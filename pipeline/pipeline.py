"""
Main STXM Pipeline Application

This module contains the main Holoscan application for STXM data processing.
It assembles operators from data_io, processing, and sink_control modules
into a complete data processing pipeline.

Pipeline Architecture:
    img_src -> decompress -> gather <- position_src
    gather -> masking_op -> publish
    gather -> [ptychography branch, when enabled]

This separates I/O operations (data_io) from computation (processing).
"""

import logging
import threading
from argparse import ArgumentParser

from holoscan.core import Application
from holoscan.schedulers import MultiThreadScheduler
from holoscan.conditions import PeriodicCondition

# Import operators from modular components
from data_io import (
    ZmqRxPositionOp,
    ZmqRxImageBatchOp,
    DecompressBatchOp,
    GatherOp
)
from processing import MaskingOp
from publish import SinkAndPublishOp
from control import ControlOp
from header_io import HeaderRxOp

# Try to import NATS (optional for testing)
try:
    from nats_async import launch_nats_instance
    HAS_NATS = True
except ImportError:
    HAS_NATS = False
    print("Warning: NATS not available, running without publishing")

logger = logging.getLogger(__name__)


class StxmApp(Application):
    """
    Main STXM data processing application.
    
    This application implements the complete STXM data processing pipeline:
    1. Receives image and position data over ZMQ
    2. Decompresses images (parallel processing)
    3. Applies circular masks to compute intensities
    4. Synchronizes positions with intensities
    5. Publishes results to NATS and files
    """
    
    def __init__(self, *args, **kwargs):
        self.num_decompress_ops = 4
        self.ptychography_enabled = False
        self.ptycho_state = None
        # Always-present shared holder (S11) for projection/frame counts, written
        # by the header op and read by both the STXM and ptycho paths. Populated
        # in main(); the frame count is filled in by configure_scan_geometry.
        self.scan_state = {
            "no_frames": 0,
            "num_projections": 1,
            "current_projection": 0,
            "transition_blocked_event": threading.Event(),
            "transition_phase": "idle",
            "ptycho_accum_flushed": False,
            "ptycho_recon_flushed": False,
            "transition_error": None,
            "max_blocked_frames": None,
        }
        super().__init__(*args, **kwargs)
        self.enable_metadata(True)

    def compose(self):
        """
        Compose the pipeline by connecting operators.
        
        Architecture:
            img_src -> decompress -> gather <- position_src
            gather -> masking_op -> publish
        """
        
        # ===== Position Data Source =====
        position_src = ZmqRxPositionOp(self,
                            name="position_src",
                            **self.kwargs('position_src'))
    
        # ===== Image Data Source =====
        img_src = ZmqRxImageBatchOp(self,
                            name="image_src",
                            num_outputs=self.num_decompress_ops,
                            dummy_img_index=False,
                            **self.kwargs('image_src'))
            
        # ===== Decompression Operators (Parallel) =====
        decompress_ops = []
        decompress_kwargs = self.kwargs('decompress_op')
        decompress_kwargs['batch_size'] = self.kwargs('image_src')['batch_size']
        
        for i in range(self.num_decompress_ops):
            decompress_op = DecompressBatchOp(self,
                                            name=f"decompress_op_{i}",
                                            **decompress_kwargs)
            decompress_ops.append(decompress_op)

        # ===== Gathering Operator (I/O - synchronizes images and positions) =====
        gather_op = GatherOp(self,
                             PeriodicCondition(self, int(0.01 * 1e9)),
                             batch_size=self.kwargs('image_src')['batch_size'],
                             scan_state=self.scan_state,
                             name="gather_op")

        # ===== Masking Operator (Processing - computes intensities) =====
        masking_kwargs = self.kwargs('masking_op')
        masking_kwargs['data_size'] = decompress_kwargs['data_size']
        masking_op = MaskingOp(self, name="masking_op", **masking_kwargs)
        
        # ===== Create Publishing Backend =====
        from publish import NatsBackend, ZmqBackend
        sink_config = self.kwargs('sink_and_publish_op')
        backend_type = sink_config.get('backend', 'nats')
        backend_endpoint = sink_config.get('backend_endpoint', None)
        
        if backend_type == "nats":
            endpoint = backend_endpoint or "localhost:6000"
            publish_backend = NatsBackend(endpoint)
        elif backend_type == "zmq":
            endpoint = backend_endpoint or "tcp://*:9999"
            publish_backend = ZmqBackend(endpoint)
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")
        
        # ===== Sink and Publish Operator =====
        tensor2subject = {
            "positions": "stxm_positions",
            "position_ids": "stxm_position_ids",
            "inner": "stxm_inner",
            "outer": "stxm_outer",
            "intensity_ids": "stxm_intensity_ids",
        }
        sink_and_publish_op = SinkAndPublishOp(self,
                                               tensor2subject=tensor2subject,
                                               publish_backend=publish_backend,
                                               scan_state=self.scan_state,
                                               **self.kwargs('sink_and_publish_op'),
                                               name="sink_and_publish_op")

        # ===== Control Operator =====
        stxm_flush_ops = [gather_op, position_src, sink_and_publish_op]
        ptycho_flush_ops = []
        ptycho_accum = None   # set below when ptychography is enabled
        ptycho_recon = None

        # ===== Ptychography Branch (conditional) =====
        if self.ptychography_enabled:
            from ptychography_ops import (
                PtychoAccumulatorOp,
                PtychoReconstructionOp,
                PtychoPublishOp,
            )

            ptycho_cfg = self.kwargs("ptychography")

            ptycho_accum = PtychoAccumulatorOp(
                self,
                ptycho_state=self.ptycho_state,
                name="ptycho_accumulator",
            )

            ptycho_recon = PtychoReconstructionOp(
                self,
                PeriodicCondition(self, int(0.01 * 1e9)),
                ptycho_state=self.ptycho_state,
                total_iterations=ptycho_cfg["total_iterations"],
                post_stream_iterations=ptycho_cfg["post_stream_iterations"],
                housekeeping_interval=ptycho_cfg["housekeeping_interval"],
                publish_interval=ptycho_cfg["publish_interval"],
                reset_probe=ptycho_cfg.get("reset_probe", False),
                publish_folder=sink_config.get("publish_folder"),
                name="ptycho_reconstruction",
            )

            ptycho_publish = PtychoPublishOp(
                self,
                publish_backend=publish_backend,
                name="ptycho_publish",
            )

            ptycho_flush_ops.append(ptycho_accum)
            ptycho_flush_ops.append(ptycho_recon)

        control_op = ControlOp(self,
                               stxm_flush_ops=stxm_flush_ops,
                               ptycho_flush_ops=ptycho_flush_ops,
                               publish_backend=publish_backend,
                               ptycho_accum=ptycho_accum,
                               ptycho_recon=ptycho_recon,
                               scan_state=self.scan_state,
                               name="control_op")

        # ===== Header Source (live scan geometry, optional) =====
        # A dedicated ZMQ SUB socket for JSON scan-geometry headers. Reconfigures
        # geometry on the fly and preempts an in-flight recon (R-4 handshake).
        header_src = None
        header_cfg = self.kwargs('header_src')
        if header_cfg:
            header_src = HeaderRxOp(self,
                                    name="header_src",
                                    scan_state=self.scan_state,
                                    ptycho_state=self.ptycho_state,
                                    **header_cfg)
        else:
            logger.warning("No header_src config — live scan headers disabled")

        # ===== Connect Operators =====
        # I/O: Image reception and decompression -> gather
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], gather_op, {("output", "images")})
        
        # I/O: Position reception -> gather
        self.add_flow(position_src, gather_op, {("positions", "positions")})
        
        # Processing: gather -> masking -> sink/publish
        self.add_flow(gather_op, masking_op, {("output", "input")})
        self.add_flow(masking_op, sink_and_publish_op, {("output", "input")})
        
        # Ptychography flows
        if self.ptychography_enabled:
            self.add_flow(gather_op, ptycho_accum, {("output", "input")})
            self.add_flow(ptycho_recon, ptycho_publish, {("output", "input")})
            # Completion signal → control (logged in PR1; drives flush in PR3)
            self.add_flow(ptycho_recon, control_op, {("complete", "input")})

        # Control path: flush signal (start message → unconditional idempotent flush)
        self.add_flow(img_src, control_op, {("flush", "input")})

        # Header path: live geometry header → control (flush for new dataset).
        # The ptycho geometry reconfigure is driven separately via the R-4
        # handshake in ptycho_state (header op sets preempt_requested).
        if header_src is not None:
            self.add_flow(header_src, control_op, {("header", "input")})


def main():
    """Main entry point for STXM pipeline."""
    parser = ArgumentParser(description="STXM Data Processing Pipeline")
    parser.add_argument("--config", type=str, default="config_test.yaml",
                       help="Configuration file path (use config_prod.yaml for production)")
    args = parser.parse_args()

    # Initialize NATS instance (if available)
    if HAS_NATS:
        nats_inst = launch_nats_instance("localhost:6000")
        print("NATS instance initialized")
    else:
        print("Running without NATS publishing")

    # Create application
    app = StxmApp()

    # Load config to make kwargs available
    app.config(args.config)

    image_src_config = app.kwargs('image_src')

    # Get scheduler parameters from config via kwargs
    scheduler_config = app.kwargs('scheduler')
    num_decompress_ops = scheduler_config.get('num_decompress_ops', 4)
    worker_threads = scheduler_config.get('worker_threads', 6)

    ptycho_cfg = app.kwargs("ptychography")
    default_blocked_frames = int(image_src_config.get("batch_size", 100)) * 10
    app.scan_state["max_blocked_frames"] = int(
        ptycho_cfg.get("max_blocked_frames", default_blocked_frames)
    ) if ptycho_cfg else default_blocked_frames

    # Set num_decompress_ops - will be used in compose() when run() is called
    app.num_decompress_ops = num_decompress_ops

    # Ptychography setup (before compose)
    if ptycho_cfg and ptycho_cfg.get("enabled", False):
        from ptychography_setup import init_ptycho_state

        logger.info("Initialising ptychography state…")
        # Pass the shared scan_state so configure_scan_geometry can mirror the
        # frame count for the STXM path and header op (S11).
        app.ptycho_state = init_ptycho_state(ptycho_cfg, app.scan_state)
        app.ptychography_enabled = True
        worker_threads = max(worker_threads, 8)
        logger.info("Ptychography enabled (worker_threads=%d)", worker_threads)

    print(f"Pipeline configuration: {num_decompress_ops} decompression operators, {worker_threads} worker threads")

    # Set up scheduler with config values
    scheduler = MultiThreadScheduler(
            app,
            worker_thread_number=worker_threads,
            check_recession_period_ms=0.001,
            stop_on_deadlock=True,
            stop_on_deadlock_timeout=500,
            name="multithread_scheduler",
        )

    app.scheduler(scheduler)
    app.run()


if __name__ == "__main__":
    main()


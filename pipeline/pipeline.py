"""
Main STXM Pipeline Application

This module contains the main Holoscan application for STXM data processing.
It assembles operators from data_io, processing, and sink_control modules
into a complete data processing pipeline.

Pipeline Architecture:
    img_src -> decompress -> gather <- position_src
    gather -> masking_op -> publish
    
This separates I/O operations (data_io) from computation (processing).
"""

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
from publish import SinkAndPublishOp, PublishToCloudOp
from control import ControlOp

# Try to import NATS (optional for testing)
try:
    from nats_async import launch_nats_instance
    HAS_NATS = True
except ImportError:
    HAS_NATS = False
    print("Warning: NATS not available, running without publishing")


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
        """
        Initialize STXM application.
        
        Note: num_decompress_ops is set from config before run() is called
        """
        self.num_decompress_ops = 4  # Default, will be overridden from config
        self.stats = {}
        super().__init__(*args, **kwargs)

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
                            stats=self.stats,
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
                             name="gather_op")

        # ===== Masking Operator (Processing - computes intensities) =====
        masking_kwargs = self.kwargs('masking_op')
        masking_kwargs['data_size'] = decompress_kwargs['data_size']
        masking_op = MaskingOp(self, name="masking_op", **masking_kwargs)
        
        # ===== Sink and Publish Operator =====
        tensor2subject = {
            "positions": "stxm_positions",
            "position_ids": "stxm_position_ids",
            "inner": "stxm_inner",
            "outer": "stxm_outer",
            "intensity_ids": "stxm_intensity_ids",
        }
        sink_and_publish_op = SinkAndPublishOp(self,
                                               stats=self.stats,
                                               tensor2subject=tensor2subject,
                                               **self.kwargs('sink_and_publish_op'),
                                               name="sink_and_publish_op")

        # ===== Cloud Publishing Operator =====
        publish_folder = self.kwargs('sink_and_publish_op')['publish_folder']
        temp_folder = self.kwargs('sink_and_publish_op')['temp_folder']

        publish_to_cloud_op = PublishToCloudOp(self,
                                               stats=self.stats,
                                               publish_folder=publish_folder,
                                               temp_folder=temp_folder,
                                               name="publish_to_cloud_op")

        # ===== Control Operator =====
        flushable_ops = [gather_op, position_src, sink_and_publish_op]

        control_op = ControlOp(self,
                               stats=self.stats,
                               flushable_ops=flushable_ops,
                               name="control_op")

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
        
        # Control path: flush and completion signals
        self.add_flow(img_src, control_op, {("flush", "input")})
        self.add_flow(sink_and_publish_op, control_op, {("processing_end", "input")})
        self.add_flow(control_op, publish_to_cloud_op, {("output", "trigger")})


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
    
    # Get scheduler parameters from config via kwargs
    scheduler_config = app.kwargs('scheduler')
    num_decompress_ops = scheduler_config.get('num_decompress_ops', 4)
    worker_threads = scheduler_config.get('worker_threads', 6)
    
    # Set num_decompress_ops - will be used in compose() when run() is called
    app.num_decompress_ops = num_decompress_ops
    
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


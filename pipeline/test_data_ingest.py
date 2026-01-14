"""
Data Ingestion Test Script for Holoscan Pipeline

This script provides test applications for validating data ingestion performance:
- Image data only
- Position data only
- Both image and position data simultaneously

Used for testing with the DAQ simulator and real hardware.
"""

import numpy as np
import time
import logging
from argparse import ArgumentParser

from holoscan.core import Application, Operator, OperatorSpec
from holoscan.schedulers import MultiThreadScheduler
from holoscan.conditions import PeriodicCondition

# Import operators from modular components
from data_io import ZmqRxImageBatchOp, DecompressBatchOp, ZmqRxPositionOp, GatherOp


class SinkOp(Operator):
    """
    Simple sink operator for testing data ingestion rates.
    
    Tracks throughput and logs statistics periodically.
    Used exclusively for testing purposes.
    """
    
    def __init__(self, fragment, *args, **kwargs):
        """
        Initialize sink operator.
        
        Args:
            fragment: Holoscan fragment
        """
        super().__init__(fragment, *args, **kwargs)
        self._total_frame_count = 0
        self._first_frame_time = None
        self.logger = logging.getLogger(kwargs.get("name", "SinkOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("input")

    def compute(self, op_input, op_output, context):
        """Measure and log throughput."""
        input_data = op_input.receive("input")
        
        if self._first_frame_time is None:
            self._first_frame_time = time.time()
        
        # Handle different input types
        if isinstance(input_data, dict):
            # Image data from DecompressBatchOp: dict with "images" and "ids"
            count = input_data["images"].shape[0]
            image_ids = np.asarray(input_data["ids"])
            
            # Log image IDs
            if len(image_ids) <= 10:
                self.logger.info(f"IMAGES: Received {len(image_ids)} with IDs: {image_ids}")
            else:
                first_5 = image_ids[:5]
                last_5 = image_ids[-5:]
                self.logger.info(f"IMAGES: Received {len(image_ids)} - first 5 IDs: {first_5}, last 5 IDs: {last_5}")
                
        elif isinstance(input_data, tuple):
            # Position data from ZmqRxPositionOp: tuple of (positions, position_ids)
            positions, position_ids = input_data
            count = positions.shape[0] if hasattr(positions, 'shape') else len(positions)
            position_ids = np.asarray(position_ids)
            # Log position IDs
            if len(position_ids) <= 10:
                self.logger.info(f"POSITIONS: Received {len(position_ids)} with IDs: {position_ids}")
            else:
                first_5 = position_ids[:5]
                last_5 = position_ids[-5:]
                self.logger.info(f"POSITIONS: Received {len(position_ids)} - first 5 IDs: {first_5}, last 5 IDs: {last_5}")
                
        else:
            # Direct array (shouldn't happen with current setup, but be safe)
            count = input_data.shape[0] if hasattr(input_data, 'shape') else 1
            
        self._total_frame_count += count
        
        # Log every 10000 frames
        # if self._total_frame_count % 100 == 0: 
        elapsed = time.time() - self._first_frame_time
        self.logger.info(
            f"{self._total_frame_count} received in {elapsed:.1f}s. "
            f"speed: {self._total_frame_count/elapsed:.1f} Hz"
        )


class SynchronizedSinkOp(Operator):
    """
    Sink operator for testing synchronized image and position data.
    
    Verifies that all images have matching positions and vice versa.
    Logs statistics about synchronization quality.
    """
    
    def __init__(self, fragment, *args, **kwargs):
        """
        Initialize synchronized sink operator.
        
        Args:
            fragment: Holoscan fragment
        """
        super().__init__(fragment, *args, **kwargs)
        self._total_count = 0
        self._first_time = None
        self._last_ids = None
        self._sync_issues = 0
        self.logger = logging.getLogger(kwargs.get("name", "SynchronizedSinkOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("input")

    def compute(self, op_input, op_output, context):
        """Receive and verify synchronized data."""
        input_data = op_input.receive("input")
        
        if self._first_time is None:
            self._first_time = time.time()
        
        # Extract synchronized data
        images = np.asarray(input_data["images"])
        positions = np.asarray(input_data["positions"])
        ids = np.asarray(input_data["ids"])
        
        count = len(ids)
        self._total_count += count
        
        # Verify synchronization
        if images.shape[0] != positions.shape[0]:
            self._sync_issues += 1
            self.logger.warning(
                f"SYNC ISSUE: Image count ({images.shape[0]}) != Position count ({positions.shape[0]})"
            )
        
        if images.shape[0] != len(ids):
            self._sync_issues += 1
            self.logger.warning(
                f"SYNC ISSUE: Image count ({images.shape[0]}) != ID count ({len(ids)})"
            )
        
        # Check for gaps in IDs
        if self._last_ids is not None:
            expected_first_id = self._last_ids[-1] + 1
            actual_first_id = ids[0]
            if actual_first_id != expected_first_id:
                gap = actual_first_id - expected_first_id
                self.logger.warning(
                    f"ID GAP: Expected {expected_first_id}, got {actual_first_id} (gap: {gap})"
                )
        
        # Check for ID consistency within batch
        sorted_ids = np.sort(ids)
        if not np.array_equal(ids, sorted_ids):
            self.logger.warning(f"SYNC ISSUE: IDs not in sorted order")
        
        # Log synchronized data
        if len(ids) <= 10:
            self.logger.info(f"SYNCHRONIZED: {len(ids)} pairs with IDs: {ids}")
        else:
            first_5 = ids[:5]
            last_5 = ids[-5:]
            self.logger.info(
                f"SYNCHRONIZED: {len(ids)} pairs - "
                f"first 5 IDs: {first_5}, last 5 IDs: {last_5}"
            )
        
        elapsed = time.time() - self._first_time
        self.logger.info(
            f"{self._total_count} synchronized pairs in {elapsed:.1f}s. "
            f"speed: {self._total_count/elapsed:.1f} Hz, "
            f"sync issues: {self._sync_issues}"
        )
        
        self._last_ids = ids


class ImageIngestApp(Application):
    """
    Application for testing image data ingestion performance.
    
    Uses same I/O structure as main pipeline:
    zmq image rx -> decompress ops -> sink
    """
    
    def __init__(self, *args, num_decompress_ops=4, **kwargs):
        """Initialize image ingestion test application."""
        self.num_decompress_ops = num_decompress_ops
        self.stats = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        """Compose image ingestion pipeline."""
        # Image source (same as main pipeline)
        img_src = ZmqRxImageBatchOp(self,
                                    stats=self.stats,
                                    name="image_src",
                                    num_outputs=self.num_decompress_ops,
                                    dummy_img_index=False,
                                    **self.kwargs('image_src'))
        
        # Decompression operators (parallel, same as main pipeline)
        decompress_ops = []
        decompress_kwargs = self.kwargs('decompress_op')
        decompress_kwargs['batch_size'] = self.kwargs('image_src')['batch_size']
        
        for i in range(self.num_decompress_ops):
            decompress_op = DecompressBatchOp(self,
                                            name=f"decompress_op_{i}",
                                            **decompress_kwargs)
            decompress_ops.append(decompress_op)
        
        # Speed measurement sink
        speed_op = SinkOp(self, name="speed_op")
        
        # Connect: img_src -> decompress -> sink
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], speed_op, {("output", "input")})


class PositionIngestApp(Application):
    """
    Application for testing position data ingestion performance.
    
    Connects to ZMQ position stream and measures throughput.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize position ingestion test application."""
        super().__init__(*args, **kwargs)

    def compose(self):
        """Compose position ingestion pipeline."""
        # Position source
        pos_src = ZmqRxPositionOp(self,
                                  name="position_src",
                                  **self.kwargs('position_src'))
        
        # Speed measurement sink
        speed_op = SinkOp(self, name="speed_op")
        
        # Connect source to sink
        self.add_flow(pos_src, speed_op, {("positions", "input")})


class BothIngestApp(Application):
    """
    Application for testing concurrent image and position data ingestion.
    
    Uses same I/O structure as main pipeline:
    zmq image rx -> decompress ops -> sink
    zmq position rx -> sink
    """
    
    def __init__(self, *args, num_decompress_ops=4, **kwargs):
        """Initialize combined ingestion test application."""
        self.num_decompress_ops = num_decompress_ops
        self.stats = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        """Compose combined ingestion pipeline."""
        # Image source (same as main pipeline)
        img_src = ZmqRxImageBatchOp(self,
                                    stats=self.stats,
                                    name="image_src",
                                    num_outputs=self.num_decompress_ops,
                                    dummy_img_index=False,
                                    **self.kwargs('image_src'))
        
        # Decompression operators (parallel, same as main pipeline)
        decompress_ops = []
        decompress_kwargs = self.kwargs('decompress_op')
        decompress_kwargs['batch_size'] = self.kwargs('image_src')['batch_size']
        
        for i in range(self.num_decompress_ops):
            decompress_op = DecompressBatchOp(self,
                                            name=f"decompress_op_{i}",
                                            **decompress_kwargs)
            decompress_ops.append(decompress_op)
        
        # Position source
        pos_src = ZmqRxPositionOp(self,
                                  name="position_src",
                                  **self.kwargs('position_src'))
        
        # Speed measurement sinks
        speed_img_op = SinkOp(self, name="speed_img_op")
        speed_pos_op = SinkOp(self, name="speed_pos_op")
        
        # Connect: img_src -> decompress -> sink
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], speed_img_op, {("output", "input")})
        
        # Connect: position src -> sink
        self.add_flow(pos_src, speed_pos_op, {("positions", "input")})


class SynchronizedIngestApp(Application):
    """
    Application for testing synchronized image and position data ingestion.
    
    Uses GatherOp to match images and positions by ID, ensuring synchronization.
    This validates that the data streams are properly aligned for downstream processing.
    
    Pipeline structure:
    zmq image rx -> decompress ops -> gather <- zmq position rx
                                      gather -> synchronized sink
    """
    
    def __init__(self, *args, num_decompress_ops=4, **kwargs):
        """Initialize synchronized ingestion test application."""
        self.num_decompress_ops = num_decompress_ops
        self.stats = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        """Compose synchronized ingestion pipeline."""
        # Image source (same as main pipeline)
        img_src = ZmqRxImageBatchOp(self,
                                    stats=self.stats,
                                    name="image_src",
                                    num_outputs=self.num_decompress_ops,
                                    dummy_img_index=False,
                                    **self.kwargs('image_src'))
        
        # Decompression operators (parallel, same as main pipeline)
        decompress_ops = []
        decompress_kwargs = self.kwargs('decompress_op')
        decompress_kwargs['batch_size'] = self.kwargs('image_src')['batch_size']
        
        for i in range(self.num_decompress_ops):
            decompress_op = DecompressBatchOp(self,
                                            name=f"decompress_op_{i}",
                                            **decompress_kwargs)
            decompress_ops.append(decompress_op)
        
        # Position source
        pos_src = ZmqRxPositionOp(self,
                                  name="position_src",
                                  **self.kwargs('position_src'))
        
        # Gather operator for synchronization
        gather_op = GatherOp(self, name="gather_op")
        
        # Synchronized sink for verification
        sync_sink = SynchronizedSinkOp(self, name="sync_sink")
        
        # Connect: img_src -> decompress -> gather
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], gather_op, {("output", "images")})
        
        # Connect: position src -> gather
        self.add_flow(pos_src, gather_op, {("positions", "positions")})
        
        # Connect: gather -> synchronized sink
        self.add_flow(gather_op, sync_sink, {("output", "input")})


def main():
    """Main entry point for data ingestion tests."""
    parser = ArgumentParser(description="Data Ingestion Test for Holoscan Pipeline")
    parser.add_argument("--mode", type=str, default="both",
                       choices=["images", "positions", "both", "synchronized"],
                       help="Test mode: images only, positions only, both separately, or synchronized")
    parser.add_argument("--config", type=str, default="config_test.yaml",
                       help="Configuration file path")
    parser.add_argument("--num-decompress-ops", type=int, default=4,
                       help="Number of parallel decompression operators (for image tests)")
    parser.add_argument("--threads", type=int, default=3,
                       help="Number of worker threads")
    parser.add_argument("--image-endpoint", type=str, default="tcp://localhost:5555",
                       help="ZMQ endpoint for image data")
    parser.add_argument("--position-endpoint", type=str, default="tcp://localhost:5556",
                       help="ZMQ endpoint for position data")
    parser.add_argument("--batch-size", type=int, default=10000,
                       help="Batch size for image data")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Select application based on mode
    if args.mode == "images":
        logging.info("Testing image data ingestion only")
        app = ImageIngestApp(num_decompress_ops=args.num_decompress_ops)
    elif args.mode == "positions":
        logging.info("Testing position data ingestion only")
        app = PositionIngestApp()
    elif args.mode == "synchronized":
        logging.info("Testing synchronized image and position data ingestion with GatherOp")
        app = SynchronizedIngestApp(num_decompress_ops=args.num_decompress_ops)
    else:
        logging.info("Testing both image and position data ingestion (separate sinks)")
        app = BothIngestApp(num_decompress_ops=args.num_decompress_ops)

    # Create scheduler
    scheduler = MultiThreadScheduler(
        app,
        worker_thread_number=args.threads,
        check_recession_period_ms=0.001,
        stop_on_deadlock=True,
        stop_on_deadlock_timeout=500,
        name="multithread_scheduler",
    )
    
    app.scheduler(scheduler)
    app.config(args.config)
    
    logging.info(f"Starting {args.mode} ingestion test")
    logging.info(f"Image endpoint: {args.image_endpoint}")
    logging.info(f"Position endpoint: {args.position_endpoint}")
    logging.info(f"Batch size: {args.batch_size}")
    logging.info(f"Threads: {args.threads}")
    logging.info("Press Ctrl+C to stop")
    
    try:
        app.run()
    except KeyboardInterrupt:
        logging.info("\nTest stopped by user")
    except Exception as e:
        logging.error(f"Test failed with error: {e}", exc_info=True)


if __name__ == "__main__":
    main()



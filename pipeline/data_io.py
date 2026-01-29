"""
Data I/O Module for Holoscan Pipeline

This module contains operators for data input/output operations:
- Image data reception and batching over ZMQ
- Position data reception over ZMQ
- CBOR message decoding and decompression
- Data synchronization (GatherOp)
- Dummy data generators for testing
"""

import numpy as np
import numpy.typing as npt
import zmq
import cbor2
import logging
import time

from dectris.compression import decompress
from holoscan.core import Operator, OperatorSpec, IOSpec, ConditionType
from holoscan.conditions import PeriodicCondition


# Tag decoders for CBOR message parsing
TAG_DECODERS = {
    69: "<u2",
    70: "<u4",
}


def receive_cbor_message(zmq_message) -> tuple[str, cbor2.CBORTag, int, dict]:
    """
    Parse a CBOR message from ZMQ.
    
    Args:
        zmq_message: Raw ZMQ message bytes
        
    Returns:
        Tuple of (message_type, data, data_id, message_content)
    """
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    
    if msg_type == "image":
        compressed_image, image_id, msg_content = msg["data"]["threshold_1"], msg["image_id"], None
    elif msg_type == "start":
        print(f"{msg_type} message content: {msg}")
        compressed_image, image_id, msg_content = None, None, msg
    elif msg_type == "end":
        print(f"{msg_type} message content: {msg}")
        compressed_image, image_id, msg_content = None, None, msg
    else:
        compressed_image, image_id, msg_content = None, None, None
        
    return msg_type, compressed_image, image_id, msg_content


def decompress_cbor_image(compressed_image: cbor2.CBORTag) -> npt.NDArray:
    """
    Decompress a CBOR-encoded image.
    
    Args:
        compressed_image: CBOR tag containing compressed image data
        
    Returns:
        Decompressed numpy array
    """
    shape, contents = compressed_image.value
    dtype = TAG_DECODERS[contents.tag]
    
    if type(contents.value) is bytes:
        # Uncompressed data
        image = np.frombuffer(contents.value, dtype=dtype).reshape(shape)
    else:
        # Compressed data
        compression_type, elem_size, image = contents.value.value
        decompressed_bytes = decompress(image, compression_type, elem_size=elem_size)
        image = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
        
    return image


def decode_cbor_image_message(zmq_message) -> tuple[str, npt.NDArray, int]:
    """
    Decode an image message from ZMQ.
    
    Args:
        zmq_message: Raw ZMQ message bytes
        
    Returns:
        Tuple of (message_type, image_array, image_id)
    """
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    
    if msg_type == "image":
        image_id = msg["image_id"]
        msg_data = msg["data"]["threshold_1"]
        shape, contents = msg_data.value
        dtype = TAG_DECODERS[contents.tag]
        
        if type(contents.value) is bytes:
            image = np.frombuffer(contents.value, dtype=dtype).reshape(shape)
        else:
            compression_type, elem_size, image_bytes = contents.value.value
            decompressed_bytes = decompress(image_bytes, compression_type, elem_size=elem_size)
            image = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
    else:
        image, image_id = None, None
        
    return msg_type, image, image_id


def decode_cbor_position_message(zmq_message) -> tuple[str, npt.NDArray, int]:
    """
    Decode a position message from ZMQ.
    
    Args:
        zmq_message: Raw ZMQ message bytes
        
    Returns:
        Tuple of (message_type, position_array, position_id)
    """
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    
    if msg_type == "position":
        data = np.array([msg["data"][k] for k in ['x', 'y', 'z', 'theta']])
        position_id = msg["position_id"]
    else:
        data = None
        position_id = None
        
    return msg_type, data, position_id


# Dictionary mapping message types to decoder functions
DECODE_CBOR_MESSAGE = {
    "image": decode_cbor_image_message,
    "position": decode_cbor_position_message,
}


class ZmqRxPositionOp(Operator):
    """
    Operator for receiving position data over ZMQ SUB socket.
    
    Receives JSON messages containing position data (x, y, z, theta) in batches.
    """
    
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 receive_timeout_ms: int = 1000,
                 **kwargs):
        """
        Initialize ZMQ position receiver.
        
        Args:
            fragment: Holoscan fragment
            zmq_endpoint: ZMQ endpoint to connect to (e.g., "tcp://localhost:5556")
            receive_timeout_ms: Timeout for receiving messages in milliseconds
        """
        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxPositionOp"))
        logging.basicConfig(level=logging.INFO)

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)
        
        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to create socket")

        # Track cumulative position count for calculating position IDs
        self.cumulative_position_count = 0
        self.last_frame_number = None

        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.output("positions")
    
    def flush(self):
        """Reset cumulative position count when starting a new series."""
        self.cumulative_position_count = 0
        self.last_frame_number = None
        self.logger.info("Position receiver flushed - cumulative count reset")

    def compute(self, op_input, op_output, context):
        """Receive and emit position data."""
        try:
            msg = self.socket.recv_json()
            
            # Check message type
            msg_type = msg.get("msg_type", "data")  # Default to "data" for backward compatibility
            
            if msg_type == "start":
                # Handle start message - reset state for new acquisition
                arm_time = msg.get("arm_time")
                start_time = msg.get("start_time")
                self.logger.info(f"Received START message: arm_time={arm_time}, start_time={start_time}")
                
                # Reset state for new acquisition
                self.cumulative_position_count = 0
                self.last_frame_number = None
                self.logger.info("Reset position tracking for new acquisition")
                return
            
            elif msg_type == "data":
                # Handle data message
                
                
                # Extract frame_number from message (number of emitted batches)
                frame_number = msg.get("frame_number", 0)
                
                # Detect wrap-around BEFORE calculating position_ids
                # When frame_number goes from N (N>0) to 0, positions have wrapped around
                if self.last_frame_number is not None and frame_number == 0 and self.last_frame_number > 0:
                    self.logger.info(f"Detected position array wrap-around (frame_number: {self.last_frame_number} -> 0), resetting cumulative count to 0")
                    self.cumulative_position_count = 0
                
                datasets = msg["datasets"]
                x = np.array(datasets["/FMC_IN.VAL1.Mean"]["data"])
                y = np.array(datasets["/FMC_IN.VAL2.Mean"]["data"])
                z = np.array(datasets["/FMC_IN.VAL3.Mean"]["data"])
                th = np.array(datasets["/INENC4.VAL.Mean"]["data"])
                positions = np.stack([x, y, z, th], axis=1)
                batch_size = positions.shape[0]
                
                # Calculate position_ids using cumulative position count (after potential wrap reset)
                # position_ids = cumulative_count + np.arange(batch_size)
                position_ids = self.cumulative_position_count + np.arange(batch_size)
                
                self.logger.info(f"Received position batch: frame_number={frame_number}, batch_size={batch_size}, position_ids={position_ids[0]}-{position_ids[-1]}")
                
                # Update tracking variables
                self.cumulative_position_count += batch_size
                self.last_frame_number = frame_number
                
                op_output.emit((positions, position_ids), "positions")
            
            else:
                # Unknown message type
                self.logger.warning(f"Received unknown message type: {msg_type}")
                
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
        except Exception as e:
            self.logger.info(f"Could not parse position message: {e}")


class ZmqRxImageBatchOp(Operator):
    """
    Operator for receiving image data over ZMQ PULL socket in batches.
    
    Receives CBOR-encoded compressed images, accumulates them into batches,
    and distributes batches across multiple output ports for parallel processing.
    """
    
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 dummy_img_index: bool = False,
                 receive_timeout_ms: int = 1000,
                 batch_size: int = 10000,
                 num_outputs: int = 2,
                 **kwargs):
        """
        Initialize ZMQ image batch receiver.
        
        Args:
            fragment: Holoscan fragment
            zmq_endpoint: ZMQ endpoint to connect to (e.g., "tcp://localhost:5555")
            dummy_img_index: Use sequential index instead of actual image_id (for testing)
            receive_timeout_ms: Timeout for receiving messages in milliseconds
            batch_size: Number of images per batch
            num_outputs: Number of output ports for load balancing
        """
        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxImageBatchOp"))
        logging.basicConfig(level=logging.INFO)

        self.batch = np.zeros((batch_size), dtype=cbor2.CBORTag)
        self.batch_ids = np.zeros((batch_size), dtype=int)
        self.current_index = 0
        self.batch_size = batch_size

        self.current_output = 0
        self.num_outputs = num_outputs
        
        # Local state tracking (not shared, will be sent via metadata)
        self.series_frame_count = 0
        self.series_start_time = 0.0
        self.first_frame_flag = True
        self.series_id = None
        self.series_finished = False
        
        self.dummy_img_index = dummy_img_index

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)

        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to create socket")

        super().__init__(fragment, *args, **kwargs)
        
    def setup(self, spec: OperatorSpec):
        # Create multiple output ports for load balancing
        for i in range(self.num_outputs):
            spec.output(f"batch_{i}")
        spec.output("flush").condition(ConditionType.NONE)

    def receive_msg(self):
        """Receive and parse a single message."""
        try:
            msg = self.socket.recv()
            cbor_type, data, data_id, msg_content = receive_cbor_message(msg)
            

            return cbor_type, data, data_id, msg_content
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
            return None
    
    def _set_series_metadata(self, include_finished=False):
        """Set common series metadata fields before emitting.
        
        Args:
            include_finished: If True, also sets series_finished flag
        """
        self.metadata.set("series_id", self.series_id)
        self.metadata.set("series_frame_count", self.series_frame_count)
        self.metadata.set("series_start_time", self.series_start_time)
        if include_finished:
            self.metadata.set("series_finished", True)
    
    def compute(self, op_input, op_output, context):
        """Receive messages and emit batches with metadata."""
        while True:
            msg = self.receive_msg()
            if msg is None:
                break
                
            cbor_type, data, data_id, msg_content = msg
            
            if cbor_type == "start":
                # Reset local state on start message
                self.series_frame_count = 0
                self.first_frame_flag = True
                self.series_finished = False
                self.series_id = msg_content["series_id"]
                
                # Set metadata for new series
                self._set_series_metadata()
                
                op_output.emit("flush", "flush")
                break
                
            if cbor_type == "end":
                # Emit remaining batch on end message
                if self.current_index > 0:
                    # Set metadata before final emit
                    self._set_series_metadata(include_finished=True)
                    
                    output_port = f"batch_{self.current_output}"
                    op_output.emit((self.batch[:self.current_index], self.batch_ids[:self.current_index]), output_port)
                    self.current_output = (self.current_output + 1) % self.num_outputs
                self.current_index = 0
                
                self.series_finished = True
                _n = self.series_frame_count
                _elapsed = time.time() - self.series_start_time
                _rate = _n/_elapsed if _elapsed > 0 else 0
                self.logger.info(f"Received {_n} messages in {_elapsed:.1f}s. at speed: {_rate:.1f} Hz")
                break
                
            else:
                # Image message
                if self.first_frame_flag:
                    self.series_start_time = time.time()
                    self.first_frame_flag = False

                self.batch[self.current_index] = data
                if self.dummy_img_index:
                    self.batch_ids[self.current_index] = self.series_frame_count - 1
                else:
                    # self.logger.info(f"Received image with id: {data_id}")
                    self.batch_ids[self.current_index] = data_id
                self.series_frame_count += 1
                self.current_index += 1

            if self.current_index >= self.batch_size:
                # Set metadata with current frame count and series info before emitting
                self._set_series_metadata()
                
                # Emit full batch through rotating output ports
                # Metadata automatically flows with the data
                output_port = f"batch_{self.current_output}"
                op_output.emit((self.batch.copy(), self.batch_ids.copy()), output_port)
                self.current_index = 0
                self.current_output = (self.current_output + 1) % self.num_outputs
                break


class DecompressBatchOp(Operator):
    """
    Operator for decompressing batches of CBOR-encoded images.
    
    Takes batches of compressed images and decompresses them in parallel.
    """
    
    def __init__(self, fragment,
                 data_size: tuple[int, int] = (192, 192),
                 data_dtype: np.dtype = np.uint16,
                 batch_size: int = 10000,
                 *args, **kwargs):
        """
        Initialize decompression operator.
        
        Args:
            fragment: Holoscan fragment
            data_size: Expected image dimensions (height, width)
            data_dtype: Expected data type
            batch_size: Batch size
        """
        self.logger = logging.getLogger(kwargs.get("name", "DecompressBatchOp"))
        self.data_size = data_size
        self.data_dtype = data_dtype
        self.batch_size = batch_size
        self.batch = np.zeros((batch_size, *data_size), dtype=np.dtype(data_dtype))
        
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("output")
    
    def flush(self):
        """Reset batch on flush."""
        self.batch = np.zeros((self.batch_size, *self.data_size), dtype=np.dtype(self.data_dtype))

    def compute(self, op_input, op_output, context):
        """Decompress batch of images."""
        compressed_images, ids = op_input.receive("input")
        batch_size = compressed_images.shape[0]
        
        for i, compressed_image in enumerate(compressed_images):
            output = decompress_cbor_image(compressed_image)
            self.batch[i] = output
            
        op_output.emit({
            "images": self.batch[:batch_size].copy(),
            "ids": ids[:batch_size].copy()
        }, "output")


class GatherOp(Operator):
    """
    Operator for synchronizing image and position data by matching IDs.
    
    This operator caches incoming image and position data, matches them by IDs,
    and emits synchronized batches for downstream processing.
    
    Architecture: img_src -> decompress -> gather <- position_src
                  gather -> masking_op -> publish
    """
    
    def __init__(self, fragment, *args, **kwargs):
        """
        Initialize gather operator.
        
        Args:
            fragment: Holoscan fragment
        """
        self.images = None
        self.image_ids = np.zeros((0,), dtype=int)
        self.positions = np.zeros((0, 4))
        self.position_ids = np.zeros((0,), dtype=int)
        self.count = 0
        
        self.logger = logging.getLogger(kwargs.get("name", "GatherOp"))
        super().__init__(fragment, *args, **kwargs)
        
    def setup(self, spec: OperatorSpec):
        spec.input("images").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.input("positions").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.output("output")

    def flush(self):
        """Reset all cached data on flush."""
        self.images = None
        self.image_ids = np.zeros((0,), dtype=int)
        self.positions = np.zeros((0, 4))
        self.position_ids = np.zeros((0,), dtype=int)
        self.count = 0

    def compute(self, op_input, op_output, context): 
        """Gather and synchronize image and position data."""
        # Receive image data
        images_dict = op_input.receive("images")
        if images_dict is not None:
            new_images = np.asarray(images_dict["images"])
            new_ids = np.asarray(images_dict["ids"])
            
            # Convert Tensors to numpy arrays if needed
            # Cache images - concatenate or initialize
            if self.images is None:
                self.images = new_images
            else:
                self.images = np.concatenate([self.images, new_images], axis=0)
            self.image_ids = np.concatenate([self.image_ids, new_ids])

        # Receive position data
        positions_tuple = op_input.receive("positions")
        if positions_tuple is not None:
            positions, position_ids = positions_tuple
            positions = np.asarray(positions)
            position_ids = np.asarray(position_ids)
                
            self.positions = np.concatenate([self.positions, positions])
            self.position_ids = np.concatenate([self.position_ids, position_ids])

        # Find common IDs between images and positions
        if self.images is not None and self.image_ids.size > 0 and self.position_ids.size > 0:
            common_ids = np.intersect1d(self.image_ids, self.position_ids).astype(int)
            
            if common_ids.size > 0:
                # Create vectorized masks for efficient filtering
                mask_positions = np.isin(self.position_ids, common_ids)
                mask_images = np.isin(self.image_ids, common_ids)
                
                # Emit synchronized data
                output = {
                    "images": self.images[mask_images],
                    "positions": self.positions[mask_positions],
                    "ids": common_ids,
                }
                op_output.emit(output, "output")

                # Remove matched data from cache
                self.positions = self.positions[~mask_positions]
                self.position_ids = self.position_ids[~mask_positions]
                self.images = self.images[~mask_images]
                self.image_ids = self.image_ids[~mask_images]
                
                # Reset images if empty
                if self.images.shape[0] == 0:
                    self.images = None
                    self.image_ids = np.zeros((0,), dtype=int)
                
                self.count += common_ids.size
            else:
                # No common IDs found - report cache state
                img_range = f"{self.image_ids.min()}-{self.image_ids.max()}" if self.image_ids.size > 0 else "none"
                pos_range = f"{self.position_ids.min()}-{self.position_ids.max()}" if self.position_ids.size > 0 else "none"
                self.logger.info(
                    f"No intersection found. Cached: {self.image_ids.size} images (IDs: {img_range}), "
                    f"{self.position_ids.size} positions (IDs: {pos_range})"
                )
                time.sleep(0.01)
        else:
            # Sleep if waiting for data
            time.sleep(0.01)



import numpy as np
import numpy.typing as npt
import cupy as cp

import zmq
import cbor2

from dectris.compression import decompress
from holoscan.core import Application, Operator, OperatorSpec, Tracker, IOSpec, ConditionType
from holoscan.decorator import create_op
from holoscan.schedulers import GreedyScheduler, MultiThreadScheduler, EventBasedScheduler
from holoscan.conditions import CountCondition, PeriodicCondition
from cupyx.scipy.interpolate import RBFInterpolator


# from nats_async import launch_nats_instance, ext_msg

from argparse import ArgumentParser
import logging
import h5py
import hdf5plugin
import time

# from nats_async import launch_nats_instance
from stxm_calc import create_circular_mask, create_circular_mask_sliced, compute_circle_sums_cupy_naive, compute_circle_sums_numpy_naive


# nats_inst = launch_nats_instance("localhost:6000")




tag_decoders = {
    69: "<u2",
    70: "<u4",
}


def receive_cbor_message(zmq_message) -> tuple[str, cbor2.CBORTag, int]:
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    # print(msg.keys())
    if msg_type == "image":
        # print(msg["image_id"])
        # msg['series_id'] - these msgs have series_id
        # msg['image_id'] - and image ids
        image_id = msg["image_id"]
        compressed_image = msg["data"]["threshold_1"]    
    else:
        compressed_image, image_id = None, None
    return msg_type, compressed_image, image_id

def decompress_cbor_image(compressed_image) -> npt.NDArray:
    shape, contents = compressed_image.value
    dtype = tag_decoders[contents.tag]
    if type(contents.value) is bytes:
        compression_type = None
        image = np.frombuffer(contents.value, dtype=dtype).reshape(shape)
    else:
        compression_type, elem_size, image = contents.value.value
        decompressed_bytes = decompress(image, compression_type, elem_size=elem_size)
        image = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
    return image

def decode_cbor_image_message(zmq_message) -> tuple[str, npt.NDArray]:
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    # print(msg.keys())
    if msg_type == "image":
        # print(msg["image_id"])
        # msg['series_id'] - these msgs have series_id
        # msg['image_id'] - and image ids
        image_id = msg["image_id"]
        msg_data = msg["data"]["threshold_1"]
        image = msg_data
        shape, contents = msg_data.value
        dtype = tag_decoders[contents.tag]
        if type(contents.value) is bytes:
            compression_type = None
            image = np.frombuffer(contents.value, dtype=dtype).reshape(shape)
        else:
            compression_type, elem_size, image = contents.value.value
            decompressed_bytes = decompress(image, compression_type, elem_size=elem_size)
            image = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
    else:
        image, image_id = None, None
    return msg_type, image, image_id


def decode_cbor_position_message(zmq_message) -> tuple[str, npt.NDArray]:
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    if msg_type == "position":
        data = np.array([msg["data"][k] for k in ['x', 'y', 'z', 'theta']])
        position_id = msg["position_id"]
    else:
        data = None
        position_id = None
    return msg_type, data, position_id


decode_cbor_message = {
    "image": decode_cbor_image_message,
    "position": decode_cbor_position_message,
}


class ZmqRxOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 msg_type: str = None,
                 receive_timeout_ms: int = 1000,  # 1 second timeout
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxOp"))
        logging.basicConfig(level=logging.INFO)
        self.logger.info(f"ZmqRxOp initialized with {msg_type} messages and endpoint {zmq_endpoint}")
        self.msg_type = msg_type
        self.decode_func = decode_cbor_message[msg_type]

        self.count = 0

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        # Set receive timeout
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)

        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to create socket")

        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.output(self.msg_type)#.condition(ConditionType.NONE)
            # ConditionType.MESSAGE_AVAILABLE

    def compute(self, op_input, op_output, context):
        try:
            msg = self.socket.recv()
            # self.logger.info(f"Received message")
            cbor_type, data, data_id = self.decode_func(msg)
            
            if cbor_type == self.msg_type:
                op_output.emit(data, self.msg_type)
                self.count += 1
            else:
               # TODO: handle start/end messages
               self.logger.info(f"Received message of type {cbor_type}, total count: {self.count}")
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")


class ZmqRxImageBatchOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 receive_timeout_ms: int = 1000,  # 1 second timeout
                 batch_size: int = 10000, #4792,
                 num_outputs: int = 2,  # Number of output ports
                 stats: dict = None,
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxOp"))
        logging.basicConfig(level=logging.INFO)

        self.batch = np.zeros((batch_size), dtype=cbor2.CBORTag)
        self.current_index = 0
        self.batch_size = batch_size

        self.current_output = 0  # Track which output port to use next
        self.num_outputs = num_outputs
        self.stats = stats
        self.stats[f"series_frame_count"] = 0
        self.stats[f"first_frame_flag"] = True

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        # Set receive timeout
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)

        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to create socket")

        super().__init__(fragment, *args, **kwargs)
        
    def setup(self, spec: OperatorSpec):
        # Create the specified number of output ports
        for i in range(self.num_outputs):
            spec.output(f"batch_{i}")

    def receive_msg(self):
        try:
            msg = self.socket.recv()
            cbor_type, data, data_id = receive_cbor_message(msg)
            
            if cbor_type == "image":
                self.stats[f"series_frame_count"] += 1
                return data, data_id
                
            else:
                self.logger.info(f"Received message of type {cbor_type}, total count: {self.stats[f'series_frame_count']}")
                return cbor_type
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
            return None
    
    def compute(self, op_input, op_output, context):
        while True:
            msg = self.receive_msg()
            if msg is None: # did not receive any message
                break
            if isinstance(msg, str) and msg == "start": # start message
                self.stats["series_frame_count"] = 0
                self.stats["first_frame_flag"] = True
                self.stats["processed_frame_count"] = 0
                self.stats["series_finished"] = False
                break
            if isinstance(msg, str) and msg == "end": # end message; emit whatever is in the batch and reset the index
                if self.current_index > 0:
                    # Emit remaining batch through current output port
                    output_port = f"batch_{self.current_output}"
                    op_output.emit(self.batch[:self.current_index], output_port)
                    self.current_output = (self.current_output + 1) % self.num_outputs
                self.current_index = 0
                
                self.stats["series_finished"] = True
                _n = self.stats[f"series_frame_count"]
                _elapsed = time.time() - self.stats[f"series_start_time"]
                _rate = _n/_elapsed
                self.logger.info(f"Received {_n} messages in {_elapsed:.1f}s. at speed: {_rate:.1f} Hz")
                break
            else: # image message
                if self.stats["first_frame_flag"]:
                    self.stats["series_start_time"] = time.time()
                    self.stats["first_frame_flag"] = False

                data, new_data_id = msg
                self.batch[self.current_index] = data
                self.current_index += 1

            if self.current_index >= self.batch_size:
                # Emit batch through alternating output ports
                output_port = f"batch_{self.current_output}"
                # self.logger.info(f"Emitting batch of size {self.batch.shape[0]} to {output_port}")
                op_output.emit(self.batch, output_port)
                self.current_index = 0
                self.current_output = (self.current_output + 1) % self.num_outputs  # Cycle through all output ports
                break


class DecompressBatchOp(Operator):
    def __init__(self, fragment,
                 data_size: tuple[int, int] = (192, 192),
                 data_dtype: np.dtype = np.uint16,
                 batch_size: int = 10000, #4792,
                 *args, **kwargs):
        self.logger = logging.getLogger(kwargs.get("name", "DecompressBatchOp"))
        self.data_size = data_size
        self.data_dtype = data_dtype
        self.batch_size = batch_size
        self.batch = np.zeros((batch_size, *data_size), dtype=np.dtype(data_dtype))
        
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("output")
        
    def compute(self, op_input, op_output, context):
        input = op_input.receive("input")
        batch_size = input.shape[0]
        for i, compressed_image in enumerate(input):
            output = decompress_cbor_image(compressed_image)
            self.batch[i] = output
        op_output.emit(self.batch[:batch_size], "output")


class MaskingOp(Operator):
    def __init__(self, fragment,
                 data_size: tuple[int, int] = (192, 192),
                 center_x: int = 100,
                 center_y: int = 100,
                 radius: int = 23,
                 *args, **kwargs):
        self.mask = create_circular_mask(data_size, radius, center_x, center_y)
        self.logger = logging.getLogger(kwargs.get("name", "MaskingOp"))
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("frames").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("intensities")

    def compute(self, op_input, op_output, context):
        frames = cp.asarray(op_input.receive("frames")) # arrives as numpy array
        # self.logger.info(f"Received frames with shape: {frames.shape}")
        inner_circle, outer_circle = compute_circle_sums_cupy_naive(frames, self.mask)
        
        output = {
            "inner": inner_circle,
            "outer": outer_circle,
        }

        op_output.emit(output, "intensities")

class SinkOp(Operator):
    def __init__(self, fragment,
                 stats: dict = None,
                 *args, **kwargs):
        super().__init__(fragment, *args, **kwargs) 
        self.logger = logging.getLogger(kwargs.get("name", "SinkOp"))
        self.stats = stats
        self.stats[f"processed_frame_count"] = 0

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        
    def compute(self, op_input, op_output, context):
        input = op_input.receive("input")
        inner_circle, outer_circle = input["inner"], input["outer"]
        # self.logger.info(f"Received intensities with shape: {inner_circle.shape} and {outer_circle.shape}")
        self.stats[f"processed_frame_count"] += inner_circle.shape[0]
        if self.stats[f"processed_frame_count"] == self.stats[f"series_frame_count"]:
            _n = self.stats[f"processed_frame_count"]
            _elapsed = time.time() - self.stats[f"series_start_time"]
            _rate = _n/_elapsed
            self.logger.info(f"{_n} processed in {_elapsed:.1f}s. speed: {_rate:.1f} Hz")
        

class StxmApp(Application): 
    def __init__(self, *args, num_decompress_ops=4, **kwargs):
        self.num_decompress_ops = num_decompress_ops
        self.stats = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        img_src_kwargs = self.kwargs('image_src')
        img_src = ZmqRxImageBatchOp(self,
                            stats=self.stats,
                            name="image_src",
                            num_outputs=self.num_decompress_ops,
                            **img_src_kwargs)
        
        # Create multiple decompress operators
        decompress_ops = []
        decompress_kwargs = self.kwargs('decompress_op')
        decompress_kwargs['batch_size'] = img_src_kwargs['batch_size']
        for i in range(self.num_decompress_ops):
            decompress_op = DecompressBatchOp(self,
                                            name=f"decompress_op_{i}",
                                            **decompress_kwargs)
            decompress_ops.append(decompress_op)

        masking_kwargs = self.kwargs('masking_op')
        masking_kwargs['data_size'] = decompress_kwargs['data_size']
        masking_op = MaskingOp(self, name="masking_op")
        sink_op = SinkOp(self,
                         stats=self.stats,
                         name="sink_op")
        
        # Connect the operators
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], masking_op, {("output", "frames")})
        
        self.add_flow(masking_op, sink_op, {("intensities", "input")})



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--config", type=str, default="holoscan_config.yaml", help="Config file")
    parser.add_argument("--num-decompress-ops", type=int, default=4, help="Number of parallel decompression operators")
    parser.add_argument("--threads", type=int, default=6, help="Number of worker threads")
    args = parser.parse_args()

    app = StxmApp(num_decompress_ops=args.num_decompress_ops)
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
    app.run()
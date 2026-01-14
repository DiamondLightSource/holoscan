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


tag_decoders = {
    69: "<u2",
    70: "<u4",
}

FIRST_FRAME_TIME = 0
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
        # shape, contents = msg_data.value
        # dtype = tag_decoders[contents.tag]
        # if type(contents.value) is bytes:
        #     compression_type = None
        #     image = np.frombuffer(contents.value, dtype=dtype).reshape(shape)
        # else:
        #     compression_type, elem_size, image = contents.value.value
        #     decompressed_bytes = decompress(image, compression_type, elem_size=elem_size)
        #     image = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
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


class ZmqRxBatchOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 msg_type: str = None,
                 receive_timeout_ms: int = 1000,  # 1 second timeout
                 data_size: tuple[int, int] = (192, 192),
                 data_dtype: np.dtype = np.uint16,
                 batch_size: int = 47920, #4792,
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxOp"))
        logging.basicConfig(level=logging.INFO)
        self.batch = np.zeros((batch_size, *data_size), dtype=np.dtype(data_dtype))
        self.current_index = 0
        self.batch_size = batch_size
        self.logger.info(f"ZmqRxOp initialized with {msg_type} messages and endpoint {zmq_endpoint}")
        self.msg_type = msg_type
        self.decode_func = decode_cbor_message[msg_type]
        self.dummy_data = np.zeros(data_size, dtype=np.uint16)
        self.count = 0
        # self.old_data_id = -1
        # self.missed_frames = 0
        self.endpoint = zmq_endpoint
        print(f"{zmq_endpoint=}")
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
        spec.output("batch")#.condition(ConditionType.NONE)
            # ConditionType.MESSAGE_AVAILABLE

    def receive_msg(self):
        global FIRST_FRAME_TIME
        try:
            msg = self.socket.recv()
            # self.logger.info(f"Received message")
            cbor_type, data, data_id = self.decode_func(msg)
            
            if cbor_type == self.msg_type:
                self.count += 1
                if FIRST_FRAME_TIME == 0:
                    FIRST_FRAME_TIME = time.time()
                return data, data_id
                
            else:
                self.logger.info(f"Received message of type {cbor_type}, total count: {self.count}")
                return cbor_type
               # TODO: handle start/end messages
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
            return None
    
    def compute(self, op_input, op_output, context):
        while True:
            msg = self.receive_msg()
            if msg is None: # did not receive any message
                break
            if isinstance(msg, str) and msg == "start": # start message
                print('start')
                break
            if isinstance(msg, str) and msg == "end": # end message; emit whatever is in the batch and reset the index
                print('end')
                if self.current_index > 0:
                    op_output.emit(self.batch[:self.current_index], "batch")
                self.current_index = 0
                # self.logger.info(f"Missed {self.missed_frames} frames")
                # self.missed_frames = 0
                break
            else: # image message
                print('image')
                data, new_data_id = msg
                data = self.dummy_data
                # if new_data_id - self.old_data_id != 1:
                    # self.missed_frames += (new_data_id - self.old_data_id - 1)
                    # self.logger.error(f"Warning: potential frame loss between {self.old_data_id} and {new_data_id}")
                self.batch[self.current_index] = data
                self.old_data_id = new_data_id
                self.current_index += 1

            if self.current_index >= self.batch_size:
                op_output.emit(self.batch, "batch")
                self.current_index = 0
                break
        


class ReplayHDF5(Operator):
    def __init__(self, fragment, *args,
                 filepath: str = None,
                 field: str = None,
                 indices: list[int] = None,
                 **kwargs):
        self.filepath = filepath
        self.field = field
        self.indices = indices
        self.current_index = 0
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.output("output")

    def start(self):
        self.hdf5_file = h5py.File(self.filepath, mode="r")

    def stop(self):
        self.hdf5_file.close()

    def compute(self, op_input, op_output, context):
        out = self.hdf5_file[self.field][self.current_index].squeeze()
        if self.indices is not None:
            out = out[self.indices]
        op_output.emit(out, "output")
        self.current_index += 1


class SpeedOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self._total_frame_count = 0
        self._first_frame_time = None
        self.logger = logging.getLogger(kwargs.get("name", "SpeedOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("input")#.connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=2**16)
        #spec.input("position")

    def compute(self, op_input, op_output, context):
        global FIRST_FRAME_TIME
        ## Performance measurement
        input = op_input.receive("input")
        if self._first_frame_time is None:
            self._first_frame_time = FIRST_FRAME_TIME
        # self._total_frame_count += 1
        self._total_frame_count += input.shape[0]
        print(input.shape)
        if self._total_frame_count % 47920 == 0:
        # if self._total_frame_count % 10000 == 0:
            elapsed = time.time() - self._first_frame_time
            # self.logger.info(f"Last frame: {frame.shape}, {frame[0,0]=}")
            self.logger.info(f"{self._total_frame_count} received in {elapsed:.1f}s. speed: {self._total_frame_count/elapsed:.1f} Hz")
        ## -----------------------
       

class DataRxApp(Application): 
    def __init__(self, *args, source="zmq", **kwargs):
        self.source = source
        super().__init__(*args, **kwargs)

    def compose(self):
        if self.source == "zmq":
            # img_src = ZmqRxOp(self,
            #                   msg_type="image",
            #                   name="simplon_zmq_rx",
            #                   **self.kwargs('simplon_zmq_rx'))
            img_src = ZmqRxBatchOp(self,
                              msg_type="image",
                              name="simplon_zmq_rx",
                              **self.kwargs('image_src'))
            # pos_src = ZmqRxOp(self,
            #                   msg_type="position",
            #                   name="positions_zmq_rx",
            #                   **self.kwargs('positions_zmq_rx'))
            # pos_src = ZmqRxBatchOp(self,
            #                   msg_type="position",
            #                   name="positions_zmq_rx",
            #                   data_size=(4,),
            #                   data_dtype=np.float64,
            #                   **self.kwargs('positions_zmq_rx'))  

            speed_img_op = SpeedOp(self, name="speed_img_op")
            # speed_pos_op = SpeedOp(self, name="speed_pos_op")
         
            # self.add_flow(img_src, speed_img_op, {("image", "input")})
            self.add_flow(img_src, speed_img_op, {("batch", "input")})
            
            # self.add_flow(pos_src, speed_pos_op, {("position", "input")})
            # self.add_flow(pos_src, speed_pos_op, {("batch", "input")})

        else:
            raise ValueError(f"Invalid source: {self.source}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--source", type=str, default="zmq", choices=["zmq", "h5"])
    args = parser.parse_args()

    app = DataRxApp(source=args.source)
    scheduler = MultiThreadScheduler(
            app,
            worker_thread_number=3,
            check_recession_period_ms=0.001,
            stop_on_deadlock=True,
            stop_on_deadlock_timeout=500,
            name="multithread_scheduler",
        )
    app.scheduler(scheduler)
    app.config("holoscan_config.yaml")
    app.run()



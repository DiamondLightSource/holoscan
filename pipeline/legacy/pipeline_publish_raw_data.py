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
import json

tag_decoders = {
    69: "<u2",
    70: "<u4",
}


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
        image = None
        image_id = None
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
        spec.output(self.msg_type).condition(ConditionType.NONE)
            # ConditionType.MESSAGE_AVAILABLE

    def compute(self, op_input, op_output, context):
        try:
            msg = self.socket.recv()
            # self.logger.info(f"Received message")
            cbor_type, data, data_id = self.decode_func(msg)
            
            if cbor_type == self.msg_type:
                out = (data, data_id)
                op_output.emit(out, self.msg_type)
                # self.logger.info(f"Emitted {self.count} {self.msg_type}s, {data.shape=}")
                # if self.count % 10000 == 0:
                #     self.logger.info(f"Emitted {self.count} images, {data.shape=}")
                self.count += 1
            # if cbor_type == "image" and self.msg_type == "frame":
                # op_output.emit(data, self.msg_type)
            else:
               # TODO: handle start/end messages
               self.logger.info(f"Received message of type {cbor_type}, total count: {self.count}")
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")


# class ZmqRxBatchOp(Operator):
class BufferOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.buffer = np.zeros((1000, 5), dtype=np.float32)
        self.last_index = 0
        self.logger = logging.getLogger(kwargs.get("name", "BufferOp"))
    
    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=32768)
        spec.output("output").condition(ConditionType.NONE)

    def compute(self, op_input, op_output, context):
        input = op_input.receive("input")
        
        # if isinstance(input, str) and (input == "flush"):
        #     self.logger.info("Flushing buffer")
        #     self.last_index = 0
        #     return
        # else:
        data, data_id = input
        
        if self.last_index < 1000:
            self.buffer[self.last_index, :4] = data
            self.buffer[self.last_index, 4] = data_id
            self.last_index +=1
        else:
            self.buffer[:-1, :] = self.buffer[1:, :]
            self.buffer[-1, :4] = data
            self.buffer[-1, 4] = data_id
            op_output.emit(self.buffer, "output")
            # self.last_index = 0


from nats_async import launch_nats_instance
nats_inst = launch_nats_instance("localhost:6000")
class SinkAndPublishImgOp(Operator):
    def __init__(self, fragment, *args, tensor2subject: dict[str, str] = None, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.tensor2subject = tensor2subject
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
    
    def setup(self, spec: OperatorSpec):
        # spec.input(self.msg_type, policy=IOSpec.QueuePolicy.REJECT).condition(
            # ConditionType.MESSAGE_AVAILABLE, min_size=1, front_stage_max_size=1
        # )
        # spec.input("input_img_sink", policy=IOSpec.QueuePolicy.REJECT).condition(
        #     ConditionType.MESSAGE_AVAILABLE, min_size=1, front_stage_max_size=1
        # )
        spec.input("input_img_sink").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=32768)

        # spec.output("output").condition(ConditionType.NONE)

    def compute(self, op_input, op_output, context):
        # global nats_inst
        if self.tensor2subject is None:
            return 
        
        input = op_input.receive("input_img_sink")
        if input is None:
            return
        data, _ = input
        # print(f"Received data: {data.shape}")
        # inner_intensity_map = cp.asarray(data["inner_intensity_map"])
        # outer_intensity_map = cp.asarray(data["outer_intensity_map"])
        # positions_grid = cp.asarray(data["positions_grid"])
        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            nats_inst.publish(subject, data)
            self.logger.info(f"Publishing {subject} with shape {data.shape}")

            
            # return

        # for tensor_key, subject in self.tensor2subject.items():
        #     if tensor_key == "theta":
        #         self.logger.info(f"Publishing theta: {cp.asarray(data[tensor_key])}")
            
        #     tensor = cp.asarray(data[tensor_key])
        #     nats_inst.publish(subject, tensor)


class SinkAndPublishPosOp(Operator):
    def __init__(self, fragment, *args, tensor2subject: dict[str, str] = None, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.tensor2subject = tensor2subject
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
    
    def setup(self, spec: OperatorSpec):
        spec.input("input_pos_sink", policy=IOSpec.QueuePolicy.REJECT).condition(
            ConditionType.MESSAGE_AVAILABLE, min_size=1, front_stage_max_size=1
        )
        # spec.input("input_pos_sink").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=32768)
        # spec.output("output").condition(ConditionType.NONE)

    def compute(self, op_input, op_output, context):
        # global nats_inst
        if self.tensor2subject is None:
            return 
        
        data = op_input.receive("input_pos_sink")
        if data is None:
            return
        # print(f"Received data: {data.shape}")
        # inner_intensity_map = cp.asarray(data["inner_intensity_map"])
        # outer_intensity_map = cp.asarray(data["outer_intensity_map"])
        # positions_grid = cp.asarray(data["positions_grid"])
        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            nats_inst.publish(subject, data)
            # payload = json.dumps(data.tolist())
            # nats_inst.js.publish("holoscan_data.raw_position", payload)

            self.logger.info(f"Publishing {subject} with shape {data.shape}")
            # op_output.emit("flush", "output")
            
            # return

        # for tensor_key, subject in self.tensor2subject.items():
        #     if tensor_key == "theta":
        #         self.logger.info(f"Publishing theta: {cp.asarray(data[tensor_key])}")
            
        #     tensor = cp.asarray(data[tensor_key])
        #     nats_inst.publish(subject, tensor)


# class SpeedOp(Operator):
#     def __init__(self, fragment, *args, **kwargs):
#         super().__init__(fragment, *args, **kwargs)
#         self._total_frame_count = 0
#         self._first_frame_time = None
#         self.logger = logging.getLogger(kwargs.get("name", "SpeedOp"))

#     def setup(self, spec: OperatorSpec):
#         spec.input("input")#.connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=2**16)
#         #spec.input("position")

#     def compute(self, op_input, op_output, context):
#         ## Performance measurement
#         input = op_input.receive("input")
#         if self._first_frame_time is None:
#             self._first_frame_time = time.time()
#         self._total_frame_count += 1
#         if self._total_frame_count % 10000 == 0:
#             elapsed = time.time() - self._first_frame_time
#             # self.logger.info(f"Last frame: {frame.shape}, {frame[0,0]=}")
#             self.logger.info(f"{self._total_frame_count} received in {elapsed:.1f}s. speed: {self._total_frame_count/elapsed:.1f} Hz")
#         ## -----------------------
       

class DataRxApp(Application): 
    def __init__(self, *args, source="zmq", **kwargs):
        self.source = source
        super().__init__(*args, **kwargs)

    def compose(self):
        if self.source == "zmq":
            img_src = ZmqRxOp(self,
                              msg_type="image",
                              name="simplon_zmq_rx",
                              **self.kwargs('simplon_zmq_rx'))
            img_sink = SinkAndPublishImgOp(self,
                                           PeriodicCondition(self, int(0.001*1e9)),
                                           tensor2subject={"image": "raw_image"},
                                           name="simplon_img_sink")
            self.add_flow(img_src, img_sink, {("image", "input_img_sink")})
            
            
            pos_src = ZmqRxOp(self,
                              msg_type="position",
                              name="positions_zmq_rx",
                              **self.kwargs('positions_zmq_rx'))
            buffer_op = BufferOp(self, name="buffer_op")

            
            pos_sink = SinkAndPublishPosOp(self,
                                           PeriodicCondition(self, int(0.001*1e9)),
                                           tensor2subject={"position": "raw_position"},
                                           name="positions_sink")
            
            
            self.add_flow(pos_src, buffer_op, {("position", "input")})
            self.add_flow(buffer_op, pos_sink, {("output", "input_pos_sink")})
            # self.add_flow(pos_sink, buffer_op, {("output", "input")})

        else:
            raise ValueError(f"Invalid source: {self.source}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--source", type=str, default="zmq", choices=["zmq", "h5"])
    args = parser.parse_args()

    app = DataRxApp(source=args.source)
    # scheduler = MultiThreadScheduler(
    #         app,
    #         worker_thread_number=3,
    #         check_recession_period_ms=0.001,
    #         stop_on_deadlock=True,
    #         stop_on_deadlock_timeout=500,
    #         name="multithread_scheduler",
    #     )
    # app.scheduler(scheduler)
    app.config("holoscan_config.yaml")
    app.run()



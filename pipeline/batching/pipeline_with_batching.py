import numpy as np
import numpy.typing as npt
import cupy as cp

import zmq
import cbor2

from dectris.compression import decompress
from holoscan.core import Application, Operator, OperatorSpec, Tracker
from holoscan.decorator import create_op
from holoscan.schedulers import GreedyScheduler, MultiThreadScheduler, EventBasedScheduler

from ptycho.ptycho import PtychoOp
from ptycho.ptycho import WriteSolutionOp
from ptycho.hdf5_io import hdf5_io
import logging

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
    return msg_type, image

def decode_cbor_positions_message(zmq_message) -> tuple[str, npt.NDArray]:
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    if msg_type == "position":
        data = np.array([msg["data"][k] for k in ['x', 'y', 'z', 'theta']])
    else:
        data = None
    return msg_type, data

decode_cbor_message = {
    "frame": decode_cbor_image_message,
    "position": decode_cbor_positions_message,
}


class ZmqRxOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint:str=None,
                 msg_type:str=None,
                 receive_timeout_ms:int=1000,  # 1 second timeout
                 **kwargs):
        
        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxOp"))
        logging.basicConfig(level=logging.INFO)

        self.msg_type = msg_type
        self.decode_func = decode_cbor_message[msg_type]

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
        spec.output(self.msg_type)

    def compute(self, op_input, op_output, context):
        try:
            msg = self.socket.recv()
            cbor_type, data = self.decode_func(msg)
            if cbor_type == self.msg_type:
                op_output.emit(data, self.msg_type)
            elif cbor_type == "image" and self.msg_type == "frame":
                op_output.emit(data, self.msg_type)
            # else:
            #    # TODO: handle start/end messages
            #    self.logger.info(f"Received message of type {msg_type}")
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")


class BatchOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self._current_frame_batch = None
        self._current_pos_batch = None
        self._current_index = 0
        self.logger = logging.getLogger(kwargs.get("name", "BatchOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("frame")
        spec.input("position")
        spec.output("frame_pos_size")
        spec.param("batch_size", 1000)

    def compute(self, op_input, op_output, context):
        frame = op_input.receive("frame")
        position = op_input.receive("position")
        inshape = frame.shape

        if self._current_frame_batch is None:
            self._current_frame_batch = cp.ndarray(shape=(self.batch_size, inshape[0], inshape[1]), dtype=frame.dtype)
            self._current_pos_batch = cp.ndarray(shape=(self.batch_size, 4))
        
        return_value = None
        if self._current_index >= self.batch_size:
            pos_mot_x = self._current_pos_batch[:,1]
            pos_mot_y = self._current_pos_batch[:,2]
            pos_mot_z = self._current_pos_batch[:,3]
            pos_t = self._current_pos_batch[:,0]

            theta = cp.mean(pos_t)
            angle_radians = cp.pi * theta / 180.0

            transformed_positions = cp.zeros((self.batch_size, 2), dtype=cp.float64)
            transformed_positions[:, 0] = (pos_mot_x[:] * cp.cos(angle_radians)) + (pos_mot_z[:] * cp.sin(angle_radians))
            transformed_positions[:, 1] = pos_mot_y

            sz_x = cp.max(transformed_positions[:, 0]) - cp.min(transformed_positions[:, 0])
            sz_y = cp.max(transformed_positions[:, 1]) - cp.min(transformed_positions[:, 1])

            return_value = {
                "frames": self._current_frame_batch,
                "positions": transformed_positions,
                "obj_size": (sz_x, sz_y),
            }
            self._current_frame_batch = cp.ndarray(shape=(self.batch_size, inshape[0], inshape[1]), dtype=frame.dtype)
            self._current_pos_batch = cp.ndarray(shape=(self.batch_size, 4))
            self._current_index = 0

        self._current_frame_batch[self._current_index] = cp.array(frame, dtype=frame.dtype)
        self._current_pos_batch[self._current_index] = cp.array(position, dtype=cp.float64)
        self._current_index += 1
        if return_value is not None:
            op_output.emit(return_value, "frame_pos_size")


@create_op(inputs=("frame_pos_size"))
def sink_func_frame_pos_size(frame_pos_size):
    logging.getLogger("sink").info(f"received batched frames with shape {frame_pos_size['frames'].shape} and positions with shape {frame_pos_size['positions'].shape} and obj_size: {frame_pos_size['obj_size']}")


def parse_args():
    # parse command line arguments
    # return the arguments
    # args are in the form of a dictionary

    import argparse

    ############################################################################
    # Command line argument parser
    ############################################################################

    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='simple ptychographic reconstruction'
        # epilog='Text at the bottom of help'
    )

    parser.add_argument('-r', '--relaxation', default=0.1, type=float, help='fourier projrction relaxation parameter')
    parser.add_argument('-d', '--dm_iter', default=0, type=int, help='difference map iteration count')
    parser.add_argument('-m', '--mlh_iter', default=0, type=int, help='maximum likelihood iteration count')
    parser.add_argument('-f', '--feedback_interval', default=0, type=int, help='feedback interval count')
    parser.add_argument('-l', '--line_search', default='bisect', help='line search method')
    parser.add_argument('measurement_file', help='measurement file or "raw"')
    parser.add_argument('initial_conditions_file', help='initial conditions or raw data directory')
    parser.add_argument('solution_file', help='solution file')
    parser.add_argument("--random_object", action="store_true", help="use random object")

    args = parser.parse_args()
    return args


class DataRxApp(Application):
    def __init__(self, prb, relaxation, solution_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prb = prb
        self.relaxation = relaxation
        self.solution_file = solution_file

    def compose(self):
        simplon_zmq_rx = ZmqRxOp(self, msg_type="frame",
                                 name="simplon_zmq_rx",
                                 **self.kwargs('simplon_zmq_rx'))
        positions_zmq_rx = ZmqRxOp(self, msg_type="position",
                                  name="positions_zmq_rx",
                                  **self.kwargs('positions_zmq_rx'))
        batch_op = BatchOp(self, self.prb, self.relaxation)
        sink_batch = sink_func_frame_pos_size(self, name="sink_batch")
        ptycho_op = PtychoOp(self, name="ptycho_op",
                                 **self.kwargs('ptycho_op'))
        write_sol_op = WriteSolutionOp(self, self.solution_file, name="write_sol_op",
                                 **self.kwargs('write_sol_op'))
        self.add_flow(simplon_zmq_rx, batch_op, {("frame", "frame")})
        self.add_flow(positions_zmq_rx, batch_op, {("position", "position")})
        # self.add_flow(batch_op, sink_batch, {("frame_pos_size", "frame_pos_size")})
        self.add_flow(batch_op, ptycho_op, {("frame_pos_size", "frame_pos_size")})
        self.add_flow(ptycho_op, write_sol_op, {("solution_vector", "solution_vector")})


if __name__ == "__main__":
    args = parse_args()

    io = hdf5_io()

    (prb, obj_read, measurements) = io.read_input(args)

    app = DataRxApp(prb, args.relaxation, args.solution_file)
    app.config("holoscan_config.yaml")
    app.run()

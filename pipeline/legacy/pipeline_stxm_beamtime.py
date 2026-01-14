import numpy as np
import numpy.typing as npt
import cupy as cp

import zmq
import cbor2
import os

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

from nats_async import launch_nats_instance
from stxm_calc import create_circular_mask, create_circular_mask_sliced, compute_circle_sums_cupy_naive, compute_circle_sums_numpy_naive


nats_inst = launch_nats_instance("localhost:6000")


tag_decoders = {
    69: "<u2",
    70: "<u4",
}


def receive_cbor_message(zmq_message) -> tuple[str, cbor2.CBORTag, int]:
    msg = cbor2.loads(zmq_message)
    msg_type = msg["type"]
    # print(msg.keys())
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


class ZmqRxPositionOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 receive_timeout_ms: int = 1000,  # 1 second timeout
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxPositionOp"))
        logging.basicConfig(level=logging.INFO)
        self.count = 0

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        # Set receive timeout
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)
        
        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to create socket")

        super().__init__(fragment, *args, **kwargs)


    def setup(self, spec: OperatorSpec):
        spec.output("positions")#.condition(ConditionType.NONE)
            # ConditionType.MESSAGE_AVAILABLE


    def flush(self):
        self.count = 0

    def compute(self, op_input, op_output, context):
        try:
            msg = self.socket.recv_json()
            datasets = msg["datasets"]
            x = np.array(datasets["/FMC_IN.VAL1.Mean"]["data"])
            y = np.array(datasets["/FMC_IN.VAL2.Mean"]["data"])
            z = np.array(datasets["/FMC_IN.VAL3.Mean"]["data"])
            th = np.array(datasets["/INENC4.VAL.Mean"]["data"])
            positions = np.stack([x, y, z, th], axis=1)
            position_ids = np.arange(positions.shape[0]) + self.count
            self.count += positions.shape[0]
            op_output.emit((positions, position_ids), "positions")
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
        except Exception as e:
            self.logger.info(f"Could not parse position message: {e}")

class DummyPositionOp(Operator):
    def __init__(self, fragment, *args,
                 batch_size: int = 10000,
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "DummyPositionOp"))
        logging.basicConfig(level=logging.INFO)
        self.count = 0
        self.batch_size = batch_size
        super().__init__(fragment, *args, **kwargs)


    def setup(self, spec: OperatorSpec):
        spec.output("positions")#.condition(ConditionType.NONE)
            # ConditionType.MESSAGE_AVAILABLE

    def flush(self):
        self.count = 0

    def compute(self, op_input, op_output, context):
        positions = np.random.randn(self.batch_size, 4)
        offset = self.count / self.batch_size / 10
        # self.logger.info(f"mean_y = {np.mean(positions[:, 1])} , Offset: {offset}")
        positions[:, 1] += offset
        position_ids = np.arange(positions.shape[0], dtype=int) + self.count
        self.count += positions.shape[0]
        op_output.emit((positions.copy(), position_ids.copy()), "positions")
        


class ZmqRxImageBatchOp(Operator):
    def __init__(self, fragment, *args,
                 zmq_endpoint: str = None,
                 dummy_img_index: bool = False,
                 receive_timeout_ms: int = 1000,  # 1 second timeout
                 batch_size: int = 10000, #4792,
                 num_outputs: int = 2,  # Number of output ports
                 stats: dict = None,
                 **kwargs):

        self.logger = logging.getLogger(kwargs.get("name", "ZmqRxOp"))
        logging.basicConfig(level=logging.INFO)

        self.batch = np.zeros((batch_size), dtype=cbor2.CBORTag)
        self.batch_ids = np.zeros((batch_size), dtype=int)
        self.current_index = 0
        self.batch_size = batch_size

        self.current_output = 0  # Track which output port to use next
        self.num_outputs = num_outputs
        self.stats = stats
        self.stats[f"series_frame_count"] = 0
        self.stats[f"series_start_time"] = 0
        self.stats[f"first_frame_flag"] = True
        self.stats[f"series_id"] = None
        
        # dealing with simulated streams for testing
        self.dummy_img_index = dummy_img_index # enumerate images instead of giving them propoer image id

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
        spec.output("flush").condition(ConditionType.NONE)

    def receive_msg(self):
        try:
            msg = self.socket.recv()
            cbor_type, data, data_id, msg_content = receive_cbor_message(msg)
            
            if cbor_type == "image":
                self.stats[f"series_frame_count"] += 1
            else:
                self.logger.info(f"Received message of type {cbor_type}")
            return cbor_type, data, data_id, msg_content
        except zmq.error.Again:
            self.logger.debug("No message received within timeout period")
            return None
    
    def compute(self, op_input, op_output, context):
        while True:
            msg = self.receive_msg()
            if msg is None: # did not receive any message
                break
            cbor_type, data, data_id, msg_content = msg
            if cbor_type == "start": # start message
                self.stats["series_frame_count"] = 0
                self.stats["first_frame_flag"] = True
                self.stats["series_finished"] = False
                self.stats["series_id"] = msg_content["series_id"]
                op_output.emit("flush", "flush")
                break
            if cbor_type == "end": # end message; emit whatever is in the batch and reset the index
                if self.current_index > 0:
                    # Emit remaining batch through current output port
                    output_port = f"batch_{self.current_output}"
                    op_output.emit((self.batch[:self.current_index], self.batch_ids[:self.current_index]), output_port)
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

                self.batch[self.current_index] = data
                if self.dummy_img_index:
                    self.batch_ids[self.current_index] = self.stats["series_frame_count"] - 1
                else:
                    self.batch_ids[self.current_index] = data_id
                
                self.current_index += 1

            if self.current_index >= self.batch_size:
                # Emit batch through alternating output ports
                output_port = f"batch_{self.current_output}"
                # self.logger.info(f"Emitting batch of size {self.batch.shape[0]} to {output_port}, ids: {self.batch_ids=}")
                op_output.emit((self.batch.copy(), self.batch_ids.copy()), output_port)
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
    
    def flush(self):
        self.batch = np.zeros((self.batch_size, *self.data_size), dtype=np.dtype(self.data_dtype))

    def compute(self, op_input, op_output, context):
        compressed_images, ids = op_input.receive("input")
        batch_size = compressed_images.shape[0]
        for i, compressed_image in enumerate(compressed_images):
            output = decompress_cbor_image(compressed_image)
            self.batch[i] = output
        op_output.emit({"images": self.batch[:batch_size].copy(),
                        "ids": ids[:batch_size].copy()},
                        "output")


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
        spec.input("images").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("intensities")

    def compute(self, op_input, op_output, context):
        input = op_input.receive("images")
        images = cp.asarray(input["images"]) # arrives as numpy array
        ids = input["ids"]
        inner_circle, outer_circle = compute_circle_sums_cupy_naive(images, self.mask)
        
        output = {
            "inner": inner_circle,
            "outer": outer_circle,
            "ids": ids,
        }

        op_output.emit(output, "intensities")

class GatherOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        self.inner_intensities = np.zeros((0,))
        self.outer_intensities = np.zeros((0,))
        self.intensity_ids = np.zeros((0,), dtype=int)
        self.positions = np.zeros((0,4))
        self.position_ids = np.zeros((0,), dtype=int)
        self.count = 0
        
        self.logger = logging.getLogger(kwargs.get("name", "GatherOp"))
        super().__init__(fragment, *args, **kwargs)
        
    def setup(self, spec: OperatorSpec):
        spec.input("intensities").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.input("positions").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.output("stxm")

    def flush(self):
        self.inner_intensities = np.zeros((0,))
        self.outer_intensities = np.zeros((0,))
        self.intensity_ids = np.zeros((0,))
        self.positions = np.zeros((0,4))
        self.position_ids = np.zeros((0,))
        self.count = 0

    def compute(self, op_input, op_output, context): 
        intensities = op_input.receive("intensities")
        if intensities is not None:
            inner = cp.asnumpy(intensities["inner"])
            outer = cp.asnumpy(intensities["outer"])
            ids = cp.asnumpy(intensities["ids"])
            self.inner_intensities = np.concatenate([self.inner_intensities, inner])
            self.outer_intensities = np.concatenate([self.outer_intensities, outer])
            self.intensity_ids = np.concatenate([self.intensity_ids, ids])
            # self.logger.info(f"Got intensity ids: {ids[0]}")

        positions_tuple = op_input.receive("positions")
        if positions_tuple is not None:
            positions, position_ids = positions_tuple
            # self.logger.info(f"{position_ids[0] = }, {position_ids[-1] = }, {np.mean(positions[:, 1])}")
            self.positions = np.concatenate([self.positions, positions])
            self.position_ids = np.concatenate([self.position_ids, position_ids])
            # self.logger.info(f"Got position ids: {position_ids[0]}")
        # self.logger.info(f"Cached {self.inner_intensities.shape[0]} intensity points and {self.positions.shape[0]} positions")

        # filter by actual indexes here
        common_ids = np.intersect1d(self.intensity_ids, self.position_ids).astype(int)
        # print(common_ids)
        if common_ids.size > 0:
            
            # Use np.isin for vectorized mask creation
            mask_positions = np.isin(self.position_ids, common_ids)
            mask_intensities = np.isin(self.intensity_ids, common_ids)
            
            output = {
                "positions": self.positions[mask_positions],
                "position_ids": self.position_ids[mask_positions],
                "inner": self.inner_intensities[mask_intensities],
                "outer": self.outer_intensities[mask_intensities],
                "intensity_ids": self.intensity_ids[mask_intensities],
            }
            # self.logger.info(f"Emitting batch with common_ids starting with {common_ids[0]}. mean_y = {np.round(np.mean(output['positions'][:, 1]), 0)}")
            op_output.emit(output, "stxm")

            self.positions = self.positions[~mask_positions]
            self.position_ids = self.position_ids[~mask_positions]
            self.inner_intensities = self.inner_intensities[~mask_intensities]
            self.outer_intensities = self.outer_intensities[~mask_intensities]
            self.intensity_ids = self.intensity_ids[~mask_intensities]
            
            self.count += common_ids.size

        # filter with the hope that everything is in sync
        # nmax = min(self.inner_intensities.shape[0], self.outer_intensities.shape[0], self.positions.shape[0])
        # if nmax > 0:
        #     output = {
        #         "positions": self.positions[:nmax],
        #         "inner": self.inner_intensities[:nmax],
        #         "outer": self.outer_intensities[:nmax],
        #     }
        #     op_output.emit(output, "stxm")
        #     self.positions = self.positions[nmax:]
        #     self.position_ids = self.position_ids[nmax:]
        #     self.inner_intensities = self.inner_intensities[nmax:]
        #     self.outer_intensities = self.outer_intensities[nmax:]
        #     self.intensity_ids = self.intensity_ids[nmax:]
        #     self.count += nmax
            


        else:
            time.sleep(0.1)

        # self.logger.info(f"Total emitted points: {self.count}. Total cached points: {self.positions.shape[0]}. Total cached intensities: {self.inner_intensities.shape[0]}.")
        # if self.intensity_ids.size > 0:
        #     self.logger.info(f"{self.intensity_ids.min()=}, {self.intensity_ids.max()=}")
        # if self.position_ids.size > 0:
        #     self.logger.info(f"{self.position_ids.min()=}, {self.position_ids.max()=}")

class SinkImageOp(Operator):
    def __init__(self, fragment,
                 stats: dict = None,
                 *args, **kwargs):
        super().__init__(fragment, *args, **kwargs) 
        self.logger = logging.getLogger(kwargs.get("name", "SinkImageOp"))
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
        

class SinkPositionsOp(Operator):
    def __init__(self, fragment,
                 stats: dict = None,
                 *args, **kwargs):
        super().__init__(fragment, *args, **kwargs) 
        self.logger = logging.getLogger(kwargs.get("name", "SinkPositionsOp"))
        self.stats = stats
        self.stats[f"received_positions_count"] = 0

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        
    def compute(self, op_input, op_output, context):
        positions = op_input.receive("input")
        self.stats[f"received_positions_count"] += positions.shape[0]
        self.logger.info(f"Received {self.stats[f'received_positions_count']} positions")


class SinkAndPublishOp(Operator):
    def __init__(self, fragment, *args,
                 stats: dict = None,
                 tensor2subject: dict[str, str] = None,
                 publish_folder=None,
                 publish_tensors: list[str] = None,
                 temp_folder: str = None,
                 **kwargs):
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
        self.stats = stats
        self.stats[f"processed_frame_count"] = 0
        self.stats[f"processed_batch_count"] = 0
        self.publish_folder = publish_folder
        self.publish_tensors = publish_tensors if publish_tensors is not None else []
        self.tensor2subject = tensor2subject
        self.temp_folder = temp_folder
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.output("processing_end").condition(ConditionType.NONE)

    def publish_to_folder(self, array_list, series_id):
        if self.publish_folder is None:
            return
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)
        
        filepath = os.path.join(self.temp_folder, f"{series_id}.h5")
        mode = 'a' if os.path.exists(filepath) else 'w'
        
        with h5py.File(filepath, mode) as f:
            dataset_key = f"batch_{self.stats['processed_batch_count']}"
            data = np.concatenate(array_list, axis=1)
            f.create_dataset(dataset_key, data=data)
        # self.logger.info(f"Temporarily published to {os.path.join(self.temp_folder, f'{series_id}.h5')}")
        # np.save(os.path.join(self.publish_folder, f"{series_id}.npy"), data)


    def flush(self):
        self.stats[f"processed_frame_count"] = 0
        self.stats[f"processed_batch_count"] = 0

    def compute(self, op_input, op_output, context):
        global nats_inst
        
        if self.tensor2subject is None:
            return

        data = op_input.receive("input")
        if data is None:
            time.sleep(0.1)
            return

        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            nats_inst.publish(subject, data)
            return
        
        if self.publish_folder is not None:
            arrays_to_publish = []
        
        for tensor_key, subject in self.tensor2subject.items():
            tensor = cp.asnumpy(data[tensor_key])
            # self.logger.info(f"Publishing {tensor_key} to {subject} with shape {tensor.shape} and {tensor.dtype}")
            # self.logger.info(f"{self.stats['processed_frame_count']=}, {self.stats[f'series_frame_count']=}")
            # if (tensor_key == "position_ids") or (tensor_key == "intensity_ids"):
            #     self.logger.info(f"Publishing {tensor_key} with {tensor[0]=}, {tensor[-1]=}")
            nats_inst.publish(subject, tensor)
            if self.publish_folder is not None:
                if tensor_key in self.publish_tensors:
                    if tensor.ndim == 1:
                        tensor = tensor.reshape(-1, 1)
                    arrays_to_publish.append(tensor)
        
        if self.publish_folder is not None:
            if len(arrays_to_publish) > 0:
                self.publish_to_folder(arrays_to_publish, self.stats["series_id"])
            
        self.stats["processed_batch_count"] += 1
        self.stats[f"processed_frame_count"] += tensor.shape[0]
        if self.stats[f"processed_frame_count"] == self.stats[f"series_frame_count"]:
            op_output.emit("processing_end", "processing_end")

            _n = self.stats[f"processed_frame_count"]
            _b = self.stats[f"processed_batch_count"]
            _elapsed = time.time() - self.stats[f"series_start_time"]
            _rate = _n/_elapsed
            self.logger.info(f"{_n} processed in {_elapsed:.1f}s. speed: {_rate:.1f} Hz (in {_b} batches)")

class PublishToCloudOp(Operator):
    def __init__(self, fragment,
                 stats: dict = None,
                 publish_folder: str = None,
                 temp_folder: str = None,
                 *args, **kwargs):
        self.logger = logging.getLogger(kwargs.get("name", "PublishToCloudOp"))
        self.stats = stats
        self.publish_folder = publish_folder
        self.temp_folder = temp_folder
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("trigger")

    def compute(self, op_input, op_output, context):
        trigger = op_input.receive("trigger")
        if trigger == "processing_end":
            if self.publish_folder is None or self.temp_folder is None:
                return

            # Get the series ID from the stats
            series_id = self.stats["series_id"]

            temp_file = os.path.join(self.temp_folder, f"{series_id}.h5")
            if not os.path.exists(temp_file):
                self.logger.warning(f"Temp file {temp_file} does not exist")
                return

            try:
                # Read and concatenate all batch arrays
                with h5py.File(temp_file, 'r') as f:
                    # Get all batch keys and sort them
                    batch_keys = sorted([k for k in f.keys() if k.startswith('batch_')])
                    if not batch_keys:
                        self.logger.warning(f"No batch arrays found in {temp_file}")
                        return

                    # Read and concatenate all batches
                    batches = [f[k][:] for k in batch_keys]
                    concatenated_data = np.concatenate(batches, axis=0)

                # Create publish folder if it doesn't exist
                os.makedirs(self.publish_folder, exist_ok=True)
                publish_file = os.path.join(self.publish_folder, f"{series_id}.h5")

                # Write concatenated data to publish folder
                with h5py.File(publish_file, 'w') as f:
                    f.create_dataset('stxm', data=concatenated_data)
                    # Copy any attributes from the original file
                    with h5py.File(temp_file, 'r') as src:
                        for key, value in src.attrs.items():
                            f.attrs[key] = value

                self.logger.info(f"Published concatenated data to {publish_file}")
                
                # Optionally remove the temp file after successful publish
                os.remove(temp_file)

            except Exception as e:
                self.logger.error(f"Error processing HDF5 file: {str(e)}")

class ControlOp(Operator):
    def __init__(self, fragment, *args,
                 stats: dict = None,
                 flushable_ops: list[Operator] = None,
                 **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "ControlOp"))
        self.flushable_ops = flushable_ops
        
    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("output")

    def compute(self, op_input, op_output, context):
        global nats_inst
        msg = op_input.receive("input")
        if msg == "flush":
            for op in self.flushable_ops:
                op.flush()
            nats_inst.publish("stxm_flush", "flush")
        
        elif msg == "processing_end":
            op_output.emit("processing_end", "output")
        
        else:
            self.logger.info(f"Received unknown message: {msg}")

class StxmApp(Application): 
    def __init__(self, *args, num_decompress_ops=4, **kwargs):
        self.num_decompress_ops = num_decompress_ops
        self.stats = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        position_src_kwargs = self.kwargs('position_src')
        pos_src = position_src_kwargs.pop("src")
        
        img_src_kwargs = self.kwargs('image_src') 
        
        if pos_src == "zmq":
            position_src = ZmqRxPositionOp(self,
                                name="position_src",
                                **position_src_kwargs)
        elif pos_src == "dummy":
            dummy_pos_batch_size = img_src_kwargs['batch_size']
            position_src = DummyPositionOp(self,
                                PeriodicCondition(self, int(0.1 * 1e9)),
                                name="position_src",
                                batch_size=dummy_pos_batch_size)
        else:
            raise ValueError(f"Invalid position source: {pos_src}")
    
        dummy_img_index = True if pos_src == "dummy" else False
        img_src = ZmqRxImageBatchOp(self,
                            stats=self.stats,
                            name="image_src",
                            num_outputs=self.num_decompress_ops,
                            dummy_img_index=dummy_img_index,
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
        masking_op = MaskingOp(self, name="masking_op", **masking_kwargs)
        # sink_image_op = SinkImageOp(self,
        #                             stats=self.stats,
        #                             name="sink_image_op")
        # sink_positions_op = SinkPositionsOp(self,
        #                                     stats=self.stats,
        #                                     name="sink_positions_op")
        
        gather_op = GatherOp(self,
                             PeriodicCondition(self, int(0.01 * 1e9)),
                             name="gather_op")
        
        tensor2subject = {
            "positions": "stxm_positions",
            "position_ids": "stxm_position_ids",
            "inner": "stxm_inner",
            "outer": "stxm_outer",
            "intensity_ids": "stxm_intensity_ids",
        }
        sink_and_publish_op = SinkAndPublishOp(self,
                                            #    PeriodicCondition(self, int(0.01 * 1e9)),
                                               stats=self.stats,
                                               tensor2subject=tensor2subject,
                                               **self.kwargs('sink_and_publish_op'),
                                               name="sink_and_publish_op")

        publish_folder = self.kwargs('sink_and_publish_op')['publish_folder']
        temp_folder = self.kwargs('sink_and_publish_op')['temp_folder']

        publish_to_cloud_op = PublishToCloudOp(self,
                                               stats=self.stats,
                                               publish_folder=publish_folder,
                                               temp_folder=temp_folder,
                                               name="publish_to_cloud_op")

        flushable_ops = [gather_op, position_src, sink_and_publish_op]

        control_op = ControlOp(self,
                               stats=self.stats,
                               flushable_ops=flushable_ops,
                               name="control_op")

        # Connect the operators
        for i in range(self.num_decompress_ops):
            self.add_flow(img_src, decompress_ops[i], {(f"batch_{i}", "input")})
            self.add_flow(decompress_ops[i], masking_op, {("output", "images")})
        
        self.add_flow(masking_op, gather_op, {("intensities", "intensities")})
        self.add_flow(position_src, gather_op, {("positions", "positions")})
        self.add_flow(gather_op, sink_and_publish_op, {("stxm", "input")})
        
        self.add_flow(img_src, control_op, {("flush", "input")})
        self.add_flow(sink_and_publish_op, control_op, {("processing_end", "input")})
        self.add_flow(control_op, publish_to_cloud_op, {("output", "trigger")})
        
        # self.add_flow(masking_op, sink_image_op, {("intensities", "input")})
        # self.add_flow(position_src, sink_positions_op, {("positions", "input")})



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



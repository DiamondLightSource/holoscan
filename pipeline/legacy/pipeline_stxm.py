import numpy as np
import cupy as cp
from cupyx.scipy.interpolate import RBFInterpolator

from holoscan.core import Application, Operator, OperatorSpec, Tracker, IOSpec, ConditionType
from holoscan.decorator import create_op, Input
from holoscan.schedulers import GreedyScheduler, MultiThreadScheduler, EventBasedScheduler
from holoscan.conditions import CountCondition, PeriodicCondition

from argparse import ArgumentParser
import logging
import h5py
import hdf5plugin
import time

from pipeline_speed import ZmqRxOp, ZmqRxBatchOp, ReplayHDF5
from nats_async import launch_nats_instance
from stxm_calc import create_circular_mask, create_circular_mask_sliced, compute_circle_sums_cupy_naive, compute_circle_sums_numpy_naive


nats_inst = launch_nats_instance("localhost:6000")


class MaskingOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        self.mask = create_circular_mask((192, 192), fragment.kwargs('masking_op')['radius'], fragment.kwargs('masking_op')['center_x'], fragment.kwargs('masking_op')['center_y'])
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("frames")
        spec.output("intensities")

    def compute(self, op_input, op_output, context):
        frames = cp.asarray(op_input.receive("frames")) # arrives as numpy array
        
        inner_circle, outer_circle = compute_circle_sums_cupy_naive(frames, self.mask)
        
        output = {
            "inner": inner_circle,
            "outer": outer_circle,
        }

        op_output.emit(output, "intensities")

class PositionTransformOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "PositionTransformOp"))
        self.transformed_positions = np.zeros((4792, 2), dtype=np.float64)

    def setup(self, spec: OperatorSpec):
        spec.input("positions")
        spec.output("transformed_positions")

    def compute(self, op_input, op_output, context):
        positions = op_input.receive("positions")
        pos_x = positions[:,0]
        pos_y = positions[:,1]
        pos_z = positions[:,2]
        pos_t = positions[:,3]

        theta = np.mean(pos_t)
        angle_radians = np.pi * theta / 180.0
        
        self.transformed_positions[:, 0] = ( (pos_x[:] * np.cos(angle_radians)) + (pos_z[:] * np.sin(angle_radians)) ) *-1
        self.transformed_positions[:, 1] = pos_y * -1
        op_output.emit(self.transformed_positions, "transformed_positions")

class GatherOp(Operator):
    def setup(self, spec: OperatorSpec):
        spec.input("positions")
        spec.input("intensities")
        spec.output("stxm")

    def compute(self, op_input, op_output, context):
        positions = op_input.receive("positions")
        intensities = op_input.receive("intensities")
        output = {
            "positions": positions,
            "outer": intensities["outer"],
            "inner": intensities["inner"],
        }
        op_output.emit(output, "stxm")

class SinkAndPublishOp(Operator):
    def __init__(self, fragment, *args, tensor2subject: dict[str, str] = None, **kwargs):
        super().__init__(fragment, *args, **kwargs)
        self.tensor2subject = tensor2subject
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
        self._first_frame_time = None
        self._total_frame_count = 0

    def setup(self, spec: OperatorSpec):
        spec.input("input")#, policy=IOSpec.QueuePolicy.REJECT).condition(


    def compute(self, op_input, op_output, context):
        # global nats_inst
        if self.tensor2subject is None:
            return 
        
        data = op_input.receive("input")
        if data is None:
            return

        if self._first_frame_time is None:
            self._first_frame_time = time.time()
        self._total_frame_count += 4792
        if self._total_frame_count % 47920 == 0:
            elapsed = time.time() - self._first_frame_time
            self.logger.info(f"{self._total_frame_count} received in {elapsed:.1f}s. speed: {self._total_frame_count/elapsed:.1f} Hz")


        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            nats_inst.publish(subject, data)
            return

        for tensor_key, subject in self.tensor2subject.items():
            if tensor_key == "theta":
                self.logger.info(f"Publishing theta: {cp.asarray(data[tensor_key])}")
            
            tensor = cp.asarray(data[tensor_key])
            nats_inst.publish(subject, tensor)



class SinkOp(Operator):
    def __init__(self, fragment, *args, **kwargs):
        super().__init__(fragment, *args, **kwargs) 
        self.logger = logging.getLogger(kwargs.get("name", "SinkOp"))
        self._first_frame_time = None
        self._total_frame_count = 0

    def setup(self, spec: OperatorSpec):
        spec.param("receivers", kind="receivers")
        

    def compute(self, op_input, op_output, context):
        receivers = op_input.receive("receivers")
        if self._first_frame_time is None:
            self._first_frame_time = time.time()
        self._total_frame_count += 4792
        if self._total_frame_count % 47920 == 0:
            elapsed = time.time() - self._first_frame_time
            self.logger.info(f"{self._total_frame_count} received in {elapsed:.1f}s. speed: {self._total_frame_count/elapsed:.1f} Hz")
        print(f"msgs received: {len(receivers)}")
        for i, receiver in enumerate(receivers):
            for key, tensor in receiver.items():
                self.logger.info(f"msg {i}: tensor {key} with shape {tensor.shape}")


@create_op(inputs=Input("batch", arg_map={"frames": "frames", "positions": "positions"}))
def sink_func(frames, positions):
    print(f"Received batch of frames with shape {frames.shape}, positions with shape {positions.shape}")


class DataRxApp(Application):
    def __init__(self, *args, source="zmq", **kwargs):
        self.source = source
        super().__init__(*args, **kwargs)

    def compose(self):
        if self.source == "zmq":
            img_src = ZmqRxBatchOp(self,
                              msg_type="image",
                              name="simplon_zmq_rx",
                              **self.kwargs('simplon_zmq_rx'))
            pos_src = ZmqRxBatchOp(self,
                              msg_type="position",
                              name="positions_zmq_rx",
                              data_size=(4,),
                              data_dtype=np.float64,
                              **self.kwargs('positions_zmq_rx'))  
 
            mask_op = MaskingOp(self, name="masking_op")
            position_transform_op = PositionTransformOp(self, name="position_transform_op")
            gather_op = GatherOp(self, name="gather_op")
            # int_op = IntensityMapRBFOp(self, name="interpolate_rbf_op")
            # int_op = IntensityMapOp(self, name="interpolate_op")
            
            # sink = SinkOp(self, name="sink_op")           
            sink_publish_stxm = SinkAndPublishOp(self,
                tensor2subject={"inner": "inner_intensity_map",
                                "outer": "outer_intensity_map",
                                "positions": "positions_grid"},
                # PeriodicCondition(self, plotting_period),
                name="sink_and_publish_stxm")

            self.add_flow(img_src, mask_op, {("batch", "frames")})
            self.add_flow(pos_src, position_transform_op, {("batch", "positions")})
            
            self.add_flow(mask_op, gather_op, {("intensities", "intensities")})
            self.add_flow(position_transform_op, gather_op, {("transformed_positions", "positions")})
            
            self.add_flow(gather_op, sink_publish_stxm, {("stxm", "input")})

        else:
            raise ValueError(f"Invalid source: {self.source}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--source", type=str, default="zmq", choices=["zmq", "h5"])
    args = parser.parse_args()

    app = DataRxApp(source=args.source)
    scheduler = MultiThreadScheduler(
            app,
            worker_thread_number=8,
            check_recession_period_ms=0.001,
            stop_on_deadlock=True,
            stop_on_deadlock_timeout=500,
            name="multithread_scheduler",
        )
    # scheduler = EventBasedScheduler(
    #         app,
    #         worker_thread_number=8,
    #         # check_recession_period_ms=0.001,
    #         stop_on_deadlock=True,
    #         stop_on_deadlock_timeout=500,
    #         name="event_based_scheduler",
    #     )
    app.scheduler(scheduler)
    
    app.config("holoscan_config.yaml")
    app.run()



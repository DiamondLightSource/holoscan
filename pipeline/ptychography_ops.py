"""
Ptychography Operators for Holoscan Pipeline

Operators:
- PtychoAccumulatorOp: fast batch accumulator (H2D + crop + position transform)
- PtychoReconstructionOp: PIE iterations on accumulated data (periodic tick)
- PtychoPublishOp: publishes reconstruction outputs
"""

import logging
import time

import numpy as np
import cupy as cp

from holoscan.core import Operator, OperatorSpec, IOSpec, ConditionType

import ptyrex.reconstruct.core.recon_processing
from ptyrex.reconstruct.core import setup
from ptyrex.reconstruct.iterator.PIE_cupy import (
    update_subset,
    combine_subsets_stream,
    from_device,
    to_device,
)


class PtychoAccumulatorOp(Operator):
    """Fast batch accumulator for ptychography.

    Receives GatherOp output, transfers images to GPU, crops, transforms
    positions to scan coordinates, and writes into pre-allocated shared
    buffers in ``ptycho_state``.
    """

    def __init__(self, fragment, *args, ptycho_state, **kwargs):
        self.ptycho_state = ptycho_state
        self.lock = ptycho_state["lock"]
        self.logger = logging.getLogger(kwargs.get("name", "PtychoAccumulatorOp"))
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(
            IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128
        ).condition(ConditionType.NONE)

    def flush(self):
        """Reset fill level and zero out GPU buffers for a new series."""
        with self.lock:
            self.ptycho_state["filled_until"] = 0
        self.ptycho_state["raw_gpu"][:] = 0
        self.ptycho_state["positions_full"][:] = 0
        self.ptycho_state["tilts_full"][:] = 0
        self.logger.info("Flushed ptychography accumulator buffers")

    def compute(self, op_input, op_output, context):
        data = op_input.receive("input")
        if data is None:
            return

        images = data["images"]       # (N, H, W) numpy on host
        positions = data["positions"]  # (N, 4) [x, y, z, theta]
        batch_size = images.shape[0]

        filled = self.ptycho_state["filled_until"]
        if filled + batch_size > self.ptycho_state["no_frames"]:
            return  # buffer full, drop batch

        pty_data = self.ptycho_state["pty_data"]

        # H2D + crop
        images_gpu = cp.asarray(images)[
            :,
            pty_data.crop_top : pty_data.crop_bottom,
            pty_data.crop_left : pty_data.crop_right,
        ]

        # Position transform: reorder (x,y,z,theta) -> (t,x,y,z)
        positions_txyz = positions[:, [3, 0, 1, 2]]
        pos_y, pos_x = self._transform_positions(positions_txyz)

        # Write into pre-allocated buffers
        new_end = filled + batch_size
        self.ptycho_state["raw_gpu"][filled:new_end] = images_gpu
        self.ptycho_state["positions_full"][0, 0, filled:new_end] = cp.asarray(pos_y)
        self.ptycho_state["positions_full"][0, 1, filled:new_end] = cp.asarray(pos_x)

        # Atomically update fill counter
        with self.lock:
            self.ptycho_state["filled_until"] = new_end

    # ------------------------------------------------------------------

    def _transform_positions(self, positions_txyz):
        """Position math from stream.calculate_positions without side effects.

        Computes pixel-space (pos_y, pos_x) from raw motor positions
        without mutating any ``pty_model`` attributes.

        Parameters
        ----------
        positions_txyz : ndarray (N, 4)
            Columns are [theta, x, y, z] in motor units.

        Returns
        -------
        pos_y, pos_x : ndarray (N,)
            Pixel-space positions (CPU numpy).
        """
        pty_model = self.ptycho_state["pty_model"]
        pty_params = self.ptycho_state["pty_params"]

        pos_t = cp.asarray(positions_txyz[:, 0])
        pos_x = cp.asarray(positions_txyz[:, 1])
        pos_y = cp.asarray(positions_txyz[:, 2])
        pos_z = cp.asarray(positions_txyz[:, 3])

        theta = cp.mean(pos_t).item()
        angle_rad = np.pi * theta / 180.0

        # Virtual scanning plane projection
        px = pos_x * np.cos(angle_rad) + pos_z * np.sin(angle_rad)
        py = pos_y

        px = -px
        py = -py

        # Orientation correction
        orientation = pty_model.scan.orientation
        if orientation == "01":
            py = -py
        elif orientation == "10":
            px = -px
        elif orientation == "11":
            px = -px
            py = -py

        # Probe positions — unit conversion to metres
        px *= -1e-6
        py *= -1e-6

        # Scale
        py *= pty_model.scan.scale[0]
        px *= pty_model.scan.scale[1]

        # Relative positions
        px -= cp.mean(px)
        py -= cp.mean(py)

        # Convert to pixel coordinates
        dx = pty_params.dx[0][0]
        px = px / dx
        py = py / dx

        # Centre in object
        py += pty_model.obj.sz_glo[0] / 2
        px += pty_model.obj.sz_glo[1] / 2

        # Offset so min is at margin (256 pixels)
        py -= cp.min(py) - 256
        px -= cp.min(px) - 256

        return cp.asnumpy(py), cp.asnumpy(px)


# ======================================================================


class PtychoReconstructionOp(Operator):
    """Run PIE iterations over accumulated data on a periodic tick.

    Fires on a ``PeriodicCondition``, snapshots the current fill level
    from ``ptycho_state``, sets views into the shared GPU buffers, and
    runs one PIE iteration per tick.
    """

    def __init__(
        self,
        fragment,
        *args,
        ptycho_state,
        total_iterations=100,
        post_stream_iterations=10,
        housekeeping_interval=10,
        publish_interval=5,
        **kwargs,
    ):
        self.ptycho_state = ptycho_state
        self.lock = ptycho_state["lock"]
        self.total_iterations = int(total_iterations)
        self.post_stream_iterations = int(post_stream_iterations)
        self.housekeeping_interval = int(housekeeping_interval)
        self.publish_interval = int(publish_interval)
        self.current_iteration = 0
        self.all_data_arrived = False
        self.post_stream_count = 0
        self.initialized_gpu = False
        self.logger = logging.getLogger(
            kwargs.get("name", "PtychoReconstructionOp")
        )
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.output("output").condition(ConditionType.NONE)

    def compute(self, op_input, op_output, context):
        # Snapshot fill level
        with self.lock:
            n_filled = self.ptycho_state["filled_until"]

        if n_filled == 0:
            return

        no_frames = self.ptycho_state["no_frames"]

        # Detect when all data has arrived
        if n_filled >= no_frames and not self.all_data_arrived:
            self.all_data_arrived = True
            self.post_stream_count = 0
            pty_model = self.ptycho_state["pty_model"]
            pty_model.scan.original = cp.copy(
                self.ptycho_state["positions_full"]
            )
            self.logger.info("All %d frames arrived", no_frames)

        # Check stopping condition
        if self.current_iteration >= self.total_iterations:
            if (
                self.all_data_arrived
                and self.post_stream_count < self.post_stream_iterations
            ):
                self.post_stream_count += 1
            else:
                return

        # One-time GPU setup on first iteration
        if not self.initialized_gpu:
            self._init_gpu()
            self.initialized_gpu = True

        # Set views into accumulated buffers
        pty_data = self.ptycho_state["pty_data"]
        pty_model = self.ptycho_state["pty_model"]
        pty_params = self.ptycho_state["pty_params"]

        pty_model.scan.positions = self.ptycho_state["positions_full"][
            :, :, :n_filled
        ]
        pty_model.scan.tilts = self.ptycho_state["tilts_full"][
            :, :, :n_filled
        ]
        pty_data.raw_expanded = self.ptycho_state["raw_gpu"][:n_filled][
            cp.newaxis, :, :, :
        ]

        pty_params.frame_IDs = np.arange(0, n_filled, dtype=np.int32)
        frame_ids_cp = cp.arange(0, n_filled, dtype=cp.int32)
        pty_params.current_iteration = self.current_iteration

        # Run one PIE iteration
        recon_data = ptyrex.reconstruct.core.recon_processing.reconstruction_data(
            pty_data, pty_model, pty_params
        )
        update_subset(pty_data, frame_ids_cp, pty_model, pty_params, recon_data)
        combine_subsets_stream(pty_model, pty_params, recon_data)

        # Restore full buffer references for next accumulator writes
        pty_model.scan.positions = self.ptycho_state["positions_full"]
        pty_model.scan.tilts = self.ptycho_state["tilts_full"]

        # Housekeeping (every N iterations or on last)
        is_last = self.current_iteration >= self.total_iterations - 1 and (
            not self.all_data_arrived
            or self.post_stream_count >= self.post_stream_iterations
        )
        if (
            self.current_iteration % self.housekeeping_interval == 0
            or is_last
        ):
            from_device(pty_model, pty_params)
            setup.after_iteration(pty_data, pty_model, pty_params, pty_plot=None)
            to_device(pty_model, pty_params, recon_data)

        # Publish (every M iterations or on last)
        if self.current_iteration % self.publish_interval == 0 or is_last:
            out = {
                "object": cp.asnumpy(pty_model.obj.array_global),
                "probe": cp.asnumpy(
                    pty_model.probe.array_states[..., 0, 0]
                ),
                "iteration": self.current_iteration,
            }
            op_output.emit(out, "output")

        self.logger.debug(
            "Iteration %d/%d  (filled=%d/%d)",
            self.current_iteration + 1,
            self.total_iterations,
            n_filled,
            no_frames,
        )
        self.current_iteration += 1

    # ------------------------------------------------------------------

    def _init_gpu(self):
        """One-time transfer of static model data to GPU."""
        pty_data = self.ptycho_state["pty_data"]
        pty_model = self.ptycho_state["pty_model"]
        pty_params = self.ptycho_state["pty_params"]

        pty_data.to_device()
        pty_model.detector.mask = cp.asarray(pty_model.detector.mask)
        pty_params.ind_binshift = cp.asarray(pty_params.ind_binshift)
        pty_params.ind_binunshift = cp.asarray(pty_params.ind_binunshift)
        pty_model.detector.min_max = cp.asarray(pty_model.detector.min_max)
        to_device(pty_model, pty_params)
        self.logger.info("GPU initialisation complete")


# ======================================================================


class PtychoPublishOp(Operator):
    """Publish ptychography outputs (object, probe) to the pipeline's
    publishing backend on dedicated subjects."""

    def __init__(
        self,
        fragment,
        *args,
        publish_backend=None,
        tensor2subject=None,
        **kwargs,
    ):
        self.backend = publish_backend
        self.tensor2subject = tensor2subject or {
            "object": "ptycho_object",
            "probe": "ptycho_probe",
        }
        self.logger = logging.getLogger(
            kwargs.get("name", "PtychoPublishOp")
        )
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(
            IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=16
        ).condition(ConditionType.NONE)

    def compute(self, op_input, op_output, context):
        data = op_input.receive("input")
        if data is None:
            return

        if self.backend is None:
            self.logger.warning("No publish backend configured, skipping")
            return

        for tensor_key, subject in self.tensor2subject.items():
            if tensor_key in data:
                tensor = np.asarray(data[tensor_key])
                self.backend.publish(subject, tensor)

        iteration = data.get("iteration", "?")
        self.logger.debug("Published ptychography results at iteration %s", iteration)

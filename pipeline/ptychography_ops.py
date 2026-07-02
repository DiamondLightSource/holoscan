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
from ptyrex.core.toolbox import setPower
from ptyrex.reconstruct.iterator.PIE_cupy import (
    update_subset,
    update_subset_profiling,
    combine_subsets_stream,
    from_device,
    to_device,
)

import os

# Route the PIE update through the optimized CUDA-Graph capture path by
# default. Opt out with PTYREX_CAPTURE_GRAPH=0; use PTYREX_PROFILING_UPDATE=1
# with capture disabled to run the eager optimized path.
_CAPTURE_GRAPH = bool(int(os.environ.get("PTYREX_CAPTURE_GRAPH", "1")))
if _CAPTURE_GRAPH:
    os.environ["PTYREX_CAPTURE_GRAPH"] = "1"

_USE_PROFILING_UPDATE = _CAPTURE_GRAPH or bool(
    int(os.environ.get("PTYREX_PROFILING_UPDATE", "0"))
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
        # Tracks whether any frames have been accumulated since the last flush,
        # so a redundant flush (e.g. the unconditional start-flush, R-2) is free.
        self._dirty = False
        # Deferred flush (see GatherOp): flush() sets this flag; the actual reset
        # happens at the top of the next compute(), so it never zeros the GPU
        # buffers while compute() is mid-write — race-safe even for a mid-stream
        # flush (PR2 header preemption).
        self._flush_requested = False
        # PR3 tomography: frames that straddle a projection boundary are split —
        # the head fills the current projection, the tail is carried here and
        # written into the next projection once the boundary advance (flush) has
        # reset filled_until to 0. None when there is no pending carry.
        self._carry = None
        # Deferred per-projection advance (PR3), distinct from a full flush.
        self._advance_requested = False
        self.logger = logging.getLogger(kwargs.get("name", "PtychoAccumulatorOp"))
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(
            IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128
        ).condition(ConditionType.NONE)

    def flush(self):
        """Request a FULL reset (scan start/end/preempt); performed at the top of
        the next compute() (deferred, so it never races the buffer writes)."""
        self._flush_requested = True

    def advance_projection(self):
        """Request a per-projection advance (PR3): reset the fill level for the
        next projection but PRESERVE the straddle carry and scan centre. Deferred
        to the top of the next compute()."""
        self._advance_requested = True

    def _perform_flush(self):
        """Reset fill level and zero the GPU buffers. Only ever called from
        compute(), so it is single-threaded w.r.t. the buffer writes. No-ops when
        nothing has been accumulated since the last flush (free redundant flush)."""
        self._flush_requested = False
        # A full flush is a scan boundary — any carried straddle-tail is stale.
        self._carry = None
        if not self._dirty:
            return
        with self.lock:
            self.ptycho_state["filled_until"] = 0
        self.ptycho_state["raw_gpu"][:] = 0
        self.ptycho_state["positions_full"][:] = 0
        self.ptycho_state["tilts_full"][:] = 0
        # Clear auto-centre so the new scan re-derives its own scan centre
        # from the first batch rather than reusing the previous scan's.
        self.ptycho_state["scan_center_py"] = None
        self.ptycho_state["scan_center_px"] = None
        self._dirty = False
        self.logger.info("Flushed ptychography accumulator buffers")

    def _perform_advance(self):
        """Per-projection advance (PR3): reset filled_until to 0 for the next
        projection while KEEPING the carry (written next compute) and the scan
        centre (all projections share the same physical scan region). Buffers are
        not zeroed — the next projection overwrites [0:no_frames] as it fills."""
        self._advance_requested = False
        with self.lock:
            self.ptycho_state["filled_until"] = 0
        self._dirty = False
        self.logger.info("Accumulator advanced to next projection (carry preserved)")

    def compute(self, op_input, op_output, context):
        # Perform any requested flush here — single-threaded w.r.t. the buffers.
        if self._flush_requested:
            self._perform_flush()
        # Per-projection advance (PR3) — after a full flush takes precedence.
        if self._advance_requested:
            self._perform_advance()

        scan_state = self.ptycho_state.get("scan_state") or {}
        num_projections = int(scan_state.get("num_projections", 1))
        no_frames = self.ptycho_state["no_frames"]

        # PR3: write a carried straddle-tail into the freshly-advanced projection
        # first (the boundary flush has reset filled_until to 0).
        if self._carry is not None and self.ptycho_state["filled_until"] == 0:
            carry = self._carry
            self._carry = None
            self._accumulate(carry["images"], carry["positions"])

        # PR3 backpressure: if this projection is full and awaiting the boundary
        # advance (recon finalize + scoped flush), stop consuming input so the
        # next projection's frames queue upstream instead of being dropped.
        if num_projections > 1 and self.ptycho_state["filled_until"] >= no_frames:
            return

        data = op_input.receive("input")
        if data is None:
            return

        images = np.asarray(data["images"])       # (N, H, W) numpy on host
        positions = np.asarray(data["positions"])  # (N, 4) [x, y, z, theta]
        batch_size = images.shape[0]

        filled = self.ptycho_state["filled_until"]
        if filled + batch_size <= no_frames:
            # Batch fits within the current projection.
            self._accumulate(images, positions)
        elif num_projections > 1:
            # PR3: batch straddles the projection boundary — write the head to
            # fill this projection, carry the tail to the next one.
            head = no_frames - filled
            self._accumulate(images[:head], positions[:head])
            self._carry = {"images": images[head:], "positions": positions[head:]}
            self.logger.info(
                "Projection boundary: wrote %d frames, carrying %d to next projection",
                head, batch_size - head,
            )
        else:
            return  # single projection, buffer full → drop batch

    def _accumulate(self, images, positions):
        """Preprocess a batch and write it into the pre-allocated buffers.

        Extracted so the projection-boundary dispatch in compute() can call it
        for a whole batch, a straddle-head, or a carried straddle-tail.
        """
        batch_size = images.shape[0]
        if batch_size == 0:
            return
        filled = self.ptycho_state["filled_until"]
        pty_data = self.ptycho_state["pty_data"]

        # H2D + crop
        images_gpu = cp.asarray(images)[
            :,
            pty_data.crop_top : pty_data.crop_bottom,
            pty_data.crop_left : pty_data.crop_right,
        ]

        # Detector preprocessing (matches PtyREX post_process / post_process_stream)
        pty_model = self.ptycho_state["pty_model"]
        det = pty_model.detector
        threshold = det.threshold
        if threshold > 0:
            images_gpu[images_gpu <= threshold] = threshold
            images_gpu = images_gpu - threshold

        orientation = det.orientation
        if orientation == "01":
            images_gpu = images_gpu[:, :, ::-1]
        elif orientation == "10":
            images_gpu = images_gpu[:, ::-1, :]
        elif orientation == "11":
            images_gpu = images_gpu[:, ::-1, ::-1]

        if det.rot != 0:
            images_gpu = cp.rot90(images_gpu, det.rot, axes=(-2, -1))

        if pty_model.geometry.modality != "near-field":
            images_gpu = cp.fft.fftshift(images_gpu, axes=(-2, -1))

        if filled == 0:
            self.logger.info(
                "Preprocessing: threshold=%s, orientation=%r, rot=%s, "
                "modality=%r, fftshift=%s, batch_sum_before=%.1f",
                threshold, orientation, det.rot,
                pty_model.geometry.modality,
                pty_model.geometry.modality != "near-field",
                float(cp.sum(images_gpu)),
            )

        # Position transform: reorder (x,y,z,theta) -> (t,x,y,z)
        positions_txyz = positions[:, [3, 0, 1, 2]]
        pos_y, pos_x = self._transform_positions(positions_txyz)

        # Write into pre-allocated buffers
        new_end = filled + batch_size
        self.ptycho_state["raw_gpu"][filled:new_end] = images_gpu
        self.ptycho_state["positions_full"][0, 0, filled:new_end] = cp.asarray(pos_y)
        self.ptycho_state["positions_full"][0, 1, filled:new_end] = cp.asarray(pos_x)

        # Diagnostics on first batch
        if filled == 0:
            pty_model = self.ptycho_state["pty_model"]
            obj_h = int(pty_model.obj.sz_glo[-2])
            obj_w = int(pty_model.obj.sz_glo[-1])
            half_h = int(pty_model.probe.array_states.shape[-2]) // 2
            half_w = int(pty_model.probe.array_states.shape[-1]) // 2
            self.logger.info(
                "Object array: %d x %d, probe: %d x %d (half: %d x %d)",
                obj_h, obj_w, half_h * 2, half_w * 2, half_h, half_w,
            )
            self.logger.info(
                "First batch positions: py=[%.2f, %.2f], px=[%.2f, %.2f]",
                pos_y.min(), pos_y.max(), pos_x.min(), pos_x.max(),
            )

        # Atomically update fill counter
        with self.lock:
            self.ptycho_state["filled_until"] = new_end
            self._dirty = True

        # Summary when buffer is full. Buffers are allocated at capacity (R-6),
        # so slice to the logical no_frames rather than the full buffer extent.
        if new_end >= self.ptycho_state["no_frames"]:
            no_frames = self.ptycho_state["no_frames"]
            all_py = cp.asnumpy(self.ptycho_state["positions_full"][0, 0, :no_frames])
            all_px = cp.asnumpy(self.ptycho_state["positions_full"][0, 1, :no_frames])
            pty_model = self.ptycho_state["pty_model"]
            obj_h = int(pty_model.obj.sz_glo[-2])
            obj_w = int(pty_model.obj.sz_glo[-1])
            half_h = int(pty_model.probe.array_states.shape[-2]) // 2
            half_w = int(pty_model.probe.array_states.shape[-1]) // 2
            oob = (
                (all_py < half_h) | (all_py + half_h > obj_h)
                | (all_px < half_w) | (all_px + half_w > obj_w)
            )
            self.logger.info(
                "All %d frames accumulated. "
                "py=[%.1f,%.1f], px=[%.1f,%.1f], "
                "out-of-bounds: %d/%d",
                new_end, all_py.min(), all_py.max(),
                all_px.min(), all_px.max(), oob.sum(), len(all_py),
            )

    # ------------------------------------------------------------------

    def _transform_positions(self, positions_txyz):
        """Position math matching PtyREX stream.calculate_positions.

        Computes pixel-space (pos_y, pos_x) from raw motor positions.
        On the first call, the mean of the batch is captured as the scan
        center so that positions are automatically centred in the object
        without requiring a pre-computed scan_range.

        Parameters
        ----------
        positions_txyz : ndarray (N, 4)
            Columns are [theta, x, y, z] in motor units (microns).

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

        # Virtual scanning plane projection (matches PtyREX streaming)
        px = pos_x * np.cos(angle_rad) + pos_z * np.sin(angle_rad)
        py = pos_y

        # Orientation correction
        orientation = pty_model.scan.orientation
        if orientation == "01":
            py = -py
        elif orientation == "10":
            px = -px
        elif orientation == "11":
            px = -px
            py = -py

        # Unit conversion to metres
        px *= 1e-6
        py *= 1e-6

        # Scale
        py *= pty_model.scan.scale[0]
        px *= pty_model.scan.scale[1]

        # Auto-centre: capture scan centre from first batch
        if self.ptycho_state["scan_center_py"] is None:
            halfview = self.ptycho_state["N"][0]/1.2/2 * 1e-6 * pty_model.scan.scale[0]
            self.ptycho_state["scan_center_py"] = float(cp.mean(py)) + halfview
            self.ptycho_state["scan_center_px"] = float(cp.mean(px))
            self.logger.info(
                "Auto-centring scan: center_py=%.6e m, center_px=%.6e m "
                "(theta=%.2f°)",
                self.ptycho_state["scan_center_py"],
                self.ptycho_state["scan_center_px"],
                theta,
            )

        py -= self.ptycho_state["scan_center_py"]
        px -= self.ptycho_state["scan_center_px"]

        # Convert to pixel coordinates
        dx = pty_params.dx[0][0]
        px = px / dx
        py = py / dx

        # Centre in object (spatial dims are at [-2], [-1])
        py += pty_model.obj.sz_glo[-2] / 2
        px += pty_model.obj.sz_glo[-1] / 2

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
        reset_probe=False,
        publish_folder=None,
        **kwargs,
    ):
        self.ptycho_state = ptycho_state
        self.lock = ptycho_state["lock"]
        # PR3: folder for per-projection reconstruction HDF5 files (tomography).
        self.publish_folder = publish_folder
        self.total_iterations = int(total_iterations)
        self.post_stream_iterations = int(post_stream_iterations)
        self.housekeeping_interval = int(housekeeping_interval)
        self.publish_interval = int(publish_interval)
        self.reset_probe = bool(reset_probe)
        self.current_iteration = 0
        self.all_data_arrived = False
        self.post_stream_count = 0
        self.initialized_gpu = False
        # Emitted exactly once per scan when the final iteration is reached;
        # reset on flush so the next scan/projection can signal again.
        self._completed = False
        # Deferred flush (see GatherOp/accumulator): flush() sets this flag; the
        # object/counter reset happens at the top of the next compute(), so it
        # never races the PIE object update — race-safe for a mid-stream flush.
        self._flush_requested = False
        # Pristine reconstruction state, snapshotted on first GPU init and
        # used to reset the object (and optionally the probe) on flush.
        self._obj_initial = None
        self._probe_initial = None
        self._flux_initial = None
        self.logger = logging.getLogger(
            kwargs.get("name", "PtychoReconstructionOp")
        )
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.output("output").condition(ConditionType.NONE)
        # Completion signal to ControlOp (plumbing for PR2/PR3).
        spec.output("complete").condition(ConditionType.NONE)

    def flush(self):
        """Request a FULL reset (scan end/header/start); performed at the top of
        the next compute() (deferred, so it never races the PIE update)."""
        self._flush_requested = True

    def _perform_advance(self):
        """Per-projection reset (PR3): fresh object + iteration counters for the
        next projection, probe carried over (warm start). Does NOT touch
        filled_until — the accumulator owns the fill level across the boundary.
        Driven by the recon itself once it observes filled_until drop below
        no_frames (i.e. the accumulator has advanced), so it never re-completes
        the same projection."""
        self.current_iteration = 0
        self.all_data_arrived = False
        self.post_stream_count = 0
        self._completed = False
        if self.initialized_gpu:
            pty_model = self.ptycho_state["pty_model"]
            pty_model.obj.array_global[:] = self._obj_initial
            pty_model.obj.array_global_old[:] = self._obj_initial
            if self.reset_probe:
                pty_model.probe.array_states[:] = self._probe_initial
                pty_model.source.flux = self._flux_initial
        self.logger.info("Recon advanced to next projection (object reset, probe carried)")

    def _perform_flush(self):
        """Reset reconstruction state for a new scan. Only ever called from
        compute(), so the object reset is single-threaded w.r.t. the PIE update.

        Resets iteration counters and the object to its initial guess. By
        default the probe (and its flux) are CARRIED OVER from the previous
        scan as a warm start, since consecutive scans usually share
        illumination. Set ``reset_probe=True`` to fully reset the probe too.
        """
        self._flush_requested = False
        self.current_iteration = 0
        self.all_data_arrived = False
        self.post_stream_count = 0
        self._completed = False
        # Clear the shared fill counter too, so this op immediately sees "no data"
        # and won't re-process the just-finished scan before the accumulator's own
        # (deferred) flush zeros the buffers. Both ops resetting it to 0 is
        # consistent; the accumulator's flush runs before it writes new frames.
        with self.lock:
            self.ptycho_state["filled_until"] = 0
        if self.initialized_gpu:
            pty_model = self.ptycho_state["pty_model"]
            pty_model.obj.array_global[:] = self._obj_initial
            pty_model.obj.array_global_old[:] = self._obj_initial
            if self.reset_probe:
                # Full reset: restore initial probe + flux so iteration 0
                # recomputes flux and re-normalises the probe.
                pty_model.probe.array_states[:] = self._probe_initial
                pty_model.source.flux = self._flux_initial
            # else: leave the previous scan's probe and flux untouched.
            # flux stays >= 0, so the iter-0 re-normalisation branch in
            # compute() does not fire and the carried probe is preserved.

        if self.reset_probe:
            self.logger.info(
                "Flushed ptycho recon state (object + probe reset to initial)"
            )
        else:
            self.logger.warning(
                "Ptycho flush: object reset, but PROBE CARRIED OVER from the "
                "previous scan (warm start). If a new scan's reconstruction "
                "looks wrong, the probe may need resetting — set "
                "ptychography.reset_probe: true in the config."
            )

    # ------------------------------------------------------------------
    # Header preemption handshake (R-4)

    def _save_and_signal_complete(self, op_output):
        """Persist the in-flight partial result, then emit recon_complete.

        Called at the top of compute() when a header preemption is requested, so
        the current reconstruction is saved (finish-current-iteration → save)
        before the geometry is reconfigured. No-ops the save when the recon has
        not produced anything yet (idle / pre-first-iteration).
        """
        if self.initialized_gpu and self.current_iteration > 0:
            pty_data = self.ptycho_state["pty_data"]
            pty_model = self.ptycho_state["pty_model"]
            pty_params = self.ptycho_state["pty_params"]
            # Bring the object/probe to host and run the end-of-iteration save
            # (writes pty_out). We are reconfiguring next, so we do not push the
            # arrays back to device (no to_device).
            from_device(pty_model, pty_params)
            setup.after_iteration(pty_data, pty_model, pty_params, pty_plot=None)
            obj_2d = np.squeeze(cp.asnumpy(pty_model.obj.array_global))
            probe_2d = np.squeeze(cp.asnumpy(pty_model.probe.array_states))
            out = {
                "object_phase": np.angle(obj_2d).astype(np.float32),
                "object_amp": np.abs(obj_2d).astype(np.float32),
                "probe_phase": np.angle(probe_2d).astype(np.float32),
                "probe_amp": np.abs(probe_2d).astype(np.float32),
                "iteration": self.current_iteration,
            }
            op_output.emit(out, "output")
            self.logger.info(
                "Saved partial result before preemption (iter %d)",
                self.current_iteration,
            )
        # Signal completion → ControlOp flushes all ops for the next dataset.
        if not self._completed:
            self._completed = True
            op_output.emit("recon_complete", "complete")

    def _apply_pending_geometry(self):
        """Apply the header's staged geometry while quiesced, then clear the
        handshake. Safe because no PIE iteration is in flight (Phase 2)."""
        from ptychography_setup import configure_scan_geometry

        with self.lock:
            pending = self.ptycho_state.get("pending_geometry")
            self.ptycho_state["pending_geometry"] = None
        if pending is not None:
            try:
                configure_scan_geometry(self.ptycho_state, **pending)
                self.logger.info("Applied new scan geometry from header")
            except Exception:
                # Never let a bad reconfigure take down the pipeline; keep the
                # previous geometry and resume. (HeaderRxOp already rejects
                # over-capacity grids; this guards anything unexpected.)
                self.logger.exception(
                    "Failed to apply new scan geometry — keeping previous geometry"
                )
                self.ptycho_state["needs_gpu_reinit"] = False
        # A full geometry reconfigure subsumes any pending flush (the object is
        # rebuilt from scratch), so drop a stale deferred flush that would
        # otherwise reference the previous geometry's initial-object snapshot.
        self._flush_requested = False
        # Clear the handshake — next compute re-inits GPU for the new object.
        self.ptycho_state["quiesced"].clear()
        self.ptycho_state["preempt_requested"].clear()

    def _reset_for_new_geometry(self):
        """Re-init reconstruction state after a geometry change.

        configure_scan_geometry rebuilt the object arrays on the host, so the
        one-time GPU transfer and the pristine-object snapshot must be redone.
        """
        self.ptycho_state["needs_gpu_reinit"] = False
        self.initialized_gpu = False
        self._obj_initial = None
        self._probe_initial = None
        self._flux_initial = None
        self.current_iteration = 0
        self.all_data_arrived = False
        self.post_stream_count = 0
        self._completed = False
        self.logger.info("Recon reset for new scan geometry")

    def compute(self, op_input, op_output, context):
        # Header preemption handshake (R-4) takes priority over any flush so the
        # in-flight partial is saved with the object still intact.
        if self.ptycho_state["preempt_requested"].is_set():
            if not self.ptycho_state["quiesced"].is_set():
                # Phase 1: the current iteration is already finished (compute is
                # atomic). Save the partial result + signal completion, then
                # quiesce so the geometry can be re-pointed without racing a
                # live PIE view.
                self._save_and_signal_complete(op_output)
                self.ptycho_state["quiesced"].set()
                self.logger.info("Recon quiesced for header preemption")
            else:
                # Phase 2: still preempted and quiesced — apply the staged
                # geometry now (nothing is touching the buffers) and clear the
                # handshake. Next compute re-inits GPU for the new object.
                self._apply_pending_geometry()
            return

        # Perform any requested flush here — single-threaded w.r.t. the PIE update.
        if self._flush_requested:
            self._perform_flush()

        # Geometry changed while quiesced — re-init GPU state for the new object.
        if self.ptycho_state.get("needs_gpu_reinit"):
            self._reset_for_new_geometry()

        scan_state = self.ptycho_state.get("scan_state") or {}
        num_projections = int(scan_state.get("num_projections", 1))
        no_frames = self.ptycho_state["no_frames"]

        # PR3 tomography: after completing a projection, idle until the accumulator
        # has advanced (filled_until dropped below no_frames). Self-advancing on
        # that observation — rather than being pushed an advance — guarantees we
        # never re-complete the same projection while its buffer is still full.
        if self._completed and num_projections > 1:
            with self.lock:
                n_now = self.ptycho_state["filled_until"]
            if n_now < no_frames:
                self._perform_advance()   # fresh object/counters for the next projection
            else:
                return                     # still holding the finished projection

        # Snapshot fill level
        with self.lock:
            n_filled = self.ptycho_state["filled_until"]

        if n_filled == 0:
            return

        # ITER_TIMING instrumentation (per-iteration timing diagnostic; INFO level,
        # ~sub-ms against a ~450ms PIE iteration)
        t_start = time.perf_counter()

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

        # Flux normalization — compute once on first iteration
        if self.current_iteration == 0 and pty_model.source.flux < 0:
            raw_cpu = cp.asnumpy(self.ptycho_state["raw_gpu"][:n_filled])
            dp = pty_data.dp
            pty_model.source.flux = float(np.sum(
                np.sum(raw_cpu, 0)[dp == 1]
            ) / raw_cpu.shape[0])
            self.logger.info("Computed flux = %.2f from %d frames", pty_model.source.flux, n_filled)
            for trial_idx in range(pty_model.scan.tris_n):
                pty_model.probe.array_states[:, :, :, :, trial_idx, :, :] = setPower(
                    pty_model.probe.array_states[:, :, :, :, trial_idx, :, :],
                    pty_model.source.flux,
                )
            self.logger.info("Probe power normalized to flux")

        pty_params.current_iteration = cp.asarray(min(
            self.current_iteration, self.total_iterations - 1
        ), dtype=cp.int32)

        # Filter to positions where the probe fits within the object.
        # PtyREX uses centre convention: both paste_e_pp and cut2 subtract
        # probe_size/2 from the position to get the top-left corner.
        obj_h = int(pty_model.obj.sz_glo[-2])
        obj_w = int(pty_model.obj.sz_glo[-1])
        half_h = int(pty_model.probe.array_states.shape[-2]) // 2
        half_w = int(pty_model.probe.array_states.shape[-1]) // 2

        pos_y = pty_model.scan.positions[0, 0, :]
        pos_x = pty_model.scan.positions[0, 1, :]
        valid_mask = (
            (pos_y >= half_h) & (pos_y + half_h <= obj_h)
            & (pos_x >= half_w) & (pos_x + half_w <= obj_w)
        )
        valid_ids = cp.where(valid_mask)[0].astype(cp.int32)
        n_oob = n_filled - int(valid_ids.size)

        if self.current_iteration == 0:
            py_min, py_max = float(cp.min(pos_y)), float(cp.max(pos_y))
            px_min, px_max = float(cp.min(pos_x)), float(cp.max(pos_x))
            self.logger.info(
                "Iter 0: %d/%d valid (object %dx%d, half-probe %dx%d), "
                "py=[%.1f,%.1f], px=[%.1f,%.1f]",
                valid_ids.size, n_filled, obj_h, obj_w, half_h, half_w,
                py_min, py_max, px_min, px_max,
            )

        if n_oob > 0:
            self.logger.warning(
                "Iter %d: %d/%d positions OUT OF BOUNDS — check R config",
                self.current_iteration, n_oob, n_filled,
            )

        if valid_ids.size == 0:
            self.logger.error(
                "No valid positions (0/%d in object bounds), skipping iteration",
                n_filled,
            )
            pty_model.scan.positions = self.ptycho_state["positions_full"]
            pty_model.scan.tilts = self.ptycho_state["tilts_full"]
            return

        pty_params.frame_IDs = cp.asnumpy(valid_ids)
        frame_ids_cp = valid_ids

        # Run one PIE iteration
        recon_data = ptyrex.reconstruct.core.recon_processing.reconstruction_data(
            pty_data, pty_model, pty_params
        )
        if _USE_PROFILING_UPDATE:
            if self.current_iteration == 0:
                self.logger.info(
                    "PIE update: update_subset_profiling (capture=%s, fpk=%s)",
                    os.environ.get("PTYREX_CAPTURE_GRAPH", "0"),
                    int(pty_params.frames_per_kernel),
                )
            update_subset_profiling(
                pty_data, frame_ids_cp, pty_model, pty_params, recon_data
            )
        else:
            update_subset(pty_data, frame_ids_cp, pty_model, pty_params, recon_data)
        combine_subsets_stream(pty_model, pty_params, recon_data)
        t_pie = time.perf_counter()

        # Restore full buffer references for next accumulator writes
        pty_model.scan.positions = self.ptycho_state["positions_full"]
        pty_model.scan.tilts = self.ptycho_state["tilts_full"]

        # Housekeeping (every N iterations or on last). For tomography
        # (num_projections > 1) a projection completes as soon as all its frames
        # are in — finish this in-flight iteration, then advance (plan PR3). For a
        # single projection, run to total_iterations + post_stream (as before).
        if num_projections > 1:
            is_last = self.all_data_arrived
        else:
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
        t_hk = time.perf_counter()

        # Publish (every M iterations or on last)
        if self.current_iteration % self.publish_interval == 0 or is_last:
            obj_2d = np.squeeze(cp.asnumpy(pty_model.obj.array_global))
            probe_2d = np.squeeze(cp.asnumpy(pty_model.probe.array_states))
            out = {
                "object_phase": np.angle(obj_2d).astype(np.float32),
                "object_amp": np.abs(obj_2d).astype(np.float32),
                "probe_phase": np.angle(probe_2d).astype(np.float32),
                "probe_amp": np.abs(probe_2d).astype(np.float32),
                "iteration": self.current_iteration,
            }
            op_output.emit(out, "output")

        # Completion signal — emitted once, only when the scan is GENUINELY
        # complete: all frames have arrived AND the final (post-stream) iteration
        # is done. Gating on all_data_arrived is essential — without it, a low
        # total_iterations exhausts mid-stream and fires "complete" while frames
        # are still arriving, which would flush GatherOp mid-compute (race) and
        # reset the object before the full scan is reconstructed. ControlOp
        # flushes on this signal (Task 3: flush after the last iteration).
        if is_last and self.all_data_arrived and not self._completed:
            self._completed = True
            if num_projections > 1:
                # Tomography: save this projection, then advance (non-final) or
                # end the scan (final projection). ControlOp does the scoped
                # advance on "projection_complete" (accum+recon only, not gather).
                self._save_projection_file()
                current_proj = int(scan_state.get("current_projection", 0))
                if current_proj >= num_projections - 1:
                    signal = "recon_complete"   # last projection → full scan end
                else:
                    signal = "projection_complete"
                op_output.emit(signal, "complete")
                self.logger.info(
                    "Projection %d/%d complete at iteration %d (signal=%s)",
                    current_proj, num_projections, self.current_iteration, signal,
                )
            else:
                op_output.emit("recon_complete", "complete")
                self.logger.info(
                    "Reconstruction complete at iteration %d", self.current_iteration
                )

        t_end = time.perf_counter()
        self.logger.info(
            "ITER_TIMING iter=%d n_filled=%d valid=%d total_ms=%.1f "
            "pie_ms=%.1f hk_ms=%.1f pub_ms=%.1f",
            self.current_iteration,
            n_filled,
            int(valid_ids.size),
            (t_end - t_start) * 1e3,
            (t_pie - t_start) * 1e3,
            (t_hk - t_pie) * 1e3,
            (t_end - t_hk) * 1e3,
        )

        self.logger.debug(
            "Iteration %d/%d  (filled=%d/%d)",
            self.current_iteration + 1,
            self.total_iterations,
            n_filled,
            no_frames,
        )
        self.current_iteration += 1

    # ------------------------------------------------------------------

    def _save_projection_file(self):
        """PR3: write this projection's reconstruction to its own HDF5 file,
        named with the shared series_id + projection index (M2 — all projections
        of a tomography scan share one series_id, so the index is mandatory)."""
        if self.publish_folder is None:
            return
        scan_state = self.ptycho_state.get("scan_state") or {}
        series_id = scan_state.get("series_id", "unknown")
        proj = int(scan_state.get("current_projection", 0))
        pty_model = self.ptycho_state["pty_model"]
        obj_2d = np.squeeze(cp.asnumpy(pty_model.obj.array_global))
        probe_2d = np.squeeze(cp.asnumpy(pty_model.probe.array_states))
        import h5py
        try:
            os.makedirs(self.publish_folder, exist_ok=True)
            path = os.path.join(
                self.publish_folder, f"{series_id}_proj{proj:02d}_recon.h5"
            )
            with h5py.File(path, "w") as f:
                f.create_dataset("object_phase", data=np.angle(obj_2d).astype(np.float32))
                f.create_dataset("object_amp", data=np.abs(obj_2d).astype(np.float32))
                f.create_dataset("probe_phase", data=np.angle(probe_2d).astype(np.float32))
                f.create_dataset("probe_amp", data=np.abs(probe_2d).astype(np.float32))
                f.attrs["projection"] = proj
                f.attrs["series_id"] = str(series_id)
                f.attrs["iteration"] = int(self.current_iteration)
            self.logger.info("Saved projection recon → %s", path)
        except Exception:
            self.logger.exception("Failed to save projection recon file")

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

        # Snapshot the pristine reconstruction state (on-device) so a new scan
        # can reset the object back to its initial guess on flush. The probe
        # snapshot is only used when reset_probe is enabled.
        self._obj_initial = pty_model.obj.array_global.copy()
        self._probe_initial = pty_model.probe.array_states.copy()
        self._flux_initial = pty_model.source.flux  # may be < 0 (auto)

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
            "object_phase": "ptycho_object_phase",
            "object_amp": "ptycho_object_amp",
            "probe_phase": "ptycho_probe_phase",
            "probe_amp": "ptycho_probe_amp",
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

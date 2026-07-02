"""
Header Input for the Holoscan Ptycho Pipeline

Defines HeaderRxOp: a dedicated ZMQ SUB operator that listens for live
scan-geometry headers and reconfigures the pipeline on the fly (PR2).

A header is a JSON object, e.g.::

    {"npoints_h": 100, "npoints_v": 100,
     "step_size_h": 0.25, "step_size_v": 0.25,
     "num_projections": 1}

On a valid header the operator:
  1. updates the always-present shared ``scan_state`` (projection count),
  2. stages the new geometry in ``ptycho_state`` and requests preemption of an
     in-flight reconstruction (R-4 handshake; the recon op applies the geometry
     once it has finished the current iteration, saved, and quiesced), and
  3. emits a ``"header"`` token to ControlOp so the STXM path flushes for the
     new dataset (works even when ptychography is disabled).

Malformed headers are logged and ignored without disturbing an in-flight scan.
"""

import logging

import zmq

from holoscan.core import Operator, OperatorSpec, ConditionType


class HeaderRxOp(Operator):
    """Receive live scan-geometry headers over a dedicated ZMQ SUB socket."""

    def __init__(
        self,
        fragment,
        *args,
        zmq_endpoint: str = None,
        receive_timeout_ms: int = 100,
        scan_state: dict = None,
        ptycho_state: dict = None,
        **kwargs,
    ):
        """
        Args:
            fragment: Holoscan fragment
            zmq_endpoint: ZMQ endpoint to connect to (e.g. "tcp://host:5557")
            receive_timeout_ms: recv timeout in ms (short so the blocking recv
                does not hold a worker thread for long)
            scan_state: always-present shared holder for projection/frame counts
            ptycho_state: ptycho state (None when ptychography is disabled)
        """
        self.logger = logging.getLogger(kwargs.get("name", "HeaderRxOp"))
        logging.basicConfig(level=logging.INFO)

        self.endpoint = zmq_endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)

        try:
            self.socket.connect(self.endpoint)
        except zmq.error.ZMQError:
            self.logger.error("Failed to connect header socket to %s", self.endpoint)

        self.scan_state = scan_state
        self.ptycho_state = ptycho_state

        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        # Token to ControlOp; NONE condition so this source runs freely.
        spec.output("header").condition(ConditionType.NONE)

    def _validate(self, msg):
        """Validate a header dict; return the parsed tuple or None if malformed."""
        if not isinstance(msg, dict):
            self.logger.warning("Header is not a JSON object: %r", msg)
            return None
        try:
            npoints_h = int(msg["npoints_h"])
            npoints_v = int(msg["npoints_v"])
            step_size_h = float(msg["step_size_h"])
            step_size_v = float(msg["step_size_v"])
            num_projections = int(msg.get("num_projections", 1))
        except (KeyError, TypeError, ValueError) as exc:
            self.logger.warning("Malformed header %r: %s", msg, exc)
            return None
        if (
            npoints_h <= 0 or npoints_v <= 0
            or step_size_h <= 0 or step_size_v <= 0
            or num_projections < 1
        ):
            self.logger.warning("Header has non-positive values: %r", msg)
            return None
        return npoints_h, npoints_v, step_size_h, step_size_v, num_projections

    def compute(self, op_input, op_output, context):
        try:
            msg = self.socket.recv_json()
        except zmq.error.Again:
            return  # recv timed out — nothing to do this tick
        except Exception as exc:  # noqa: BLE001 - don't let a bad message kill the op
            self.logger.warning("Header receive error: %s", exc)
            return

        parsed = self._validate(msg)
        if parsed is None:
            return  # malformed — ignore, leave any in-flight scan untouched

        npoints_h, npoints_v, step_size_h, step_size_v, num_projections = parsed
        self.logger.info(
            "Received header: %d x %d points, step %.4g x %.4g µm, "
            "num_projections=%d",
            npoints_h, npoints_v, step_size_h, step_size_v, num_projections,
        )

        # Reject a grid that exceeds the pre-allocated GPU capacity BEFORE staging
        # it (buffers are never realloced, R-6). Rejecting here keeps the bad
        # header off the recon's apply path, which would otherwise raise inside
        # compute() and take down the pipeline.
        if self.ptycho_state is not None:
            capacity = self.ptycho_state.get("capacity")
            if capacity is not None and npoints_h * npoints_v > capacity:
                self.logger.warning(
                    "Header grid %dx%d = %d frames exceeds capacity %d — rejected "
                    "(increase max_npoints_h/max_npoints_v). In-flight scan "
                    "untouched.",
                    npoints_h, npoints_v, npoints_h * npoints_v, capacity,
                )
                return

        # 1. Update the always-present shared holder (S11). Set no_frames here too
        #    (not only via configure_scan_geometry) so the STXM sink can segment
        #    per projection even when ptychography is disabled.
        if self.scan_state is not None:
            self.scan_state["num_projections"] = num_projections
            self.scan_state["current_projection"] = 0
            self.scan_state["no_frames"] = npoints_h * npoints_v

        # 2. Ptycho path: stage geometry + request preemption (R-4). The recon op
        #    applies configure_scan_geometry once it has quiesced, so no buffer
        #    view is swapped under a live PIE iteration.
        if self.ptycho_state is not None:
            with self.ptycho_state["lock"]:
                self.ptycho_state["pending_geometry"] = {
                    "npoints_h": npoints_h,
                    "npoints_v": npoints_v,
                    "step_size_h": step_size_h,
                    "step_size_v": step_size_v,
                }
            self.ptycho_state["preempt_requested"].set()

        # 3. Notify ControlOp so the STXM path flushes for the new dataset
        #    (works even when ptychography is disabled).
        op_output.emit("header", "header")

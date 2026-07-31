"""
Control Module for Holoscan STXM Pipeline

This module contains operators for pipeline control and synchronization.
"""

import logging
from holoscan.core import Operator, OperatorSpec, IOSpec


class ControlOp(Operator):
    """
    Control operator for managing pipeline flow.
    
    Handles the flush control message, coordinating flush state across
    multiple operators.
    """
    
    def __init__(self, fragment, *args,
                 stxm_flush_ops: list[Operator] = None,
                 ptycho_flush_ops: list[Operator] = None,
                 publish_backend = None,
                 ptycho_accum = None,
                 ptycho_recon = None,
                 scan_state: dict = None,
                 **kwargs):
        """
        Initialize control operator.

        Args:
            fragment: Holoscan fragment
            stxm_flush_ops: STXM-side operators that can be flushed
            ptycho_flush_ops: Ptycho-side operators that can be flushed
            publish_backend: Backend instance for publishing flush messages
            ptycho_accum: PtychoAccumulatorOp (for the scoped projection advance)
            ptycho_recon: PtychoReconstructionOp (for the scoped projection advance)
            scan_state: shared holder whose current_projection is advanced at a
                tomography projection boundary (PR3)
        """
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "ControlOp"))
        self.stxm_flush_ops = stxm_flush_ops or []
        self.ptycho_flush_ops = ptycho_flush_ops or []
        self.publish_backend = publish_backend
        self.ptycho_accum = ptycho_accum
        self.ptycho_recon = ptycho_recon
        self.scan_state = scan_state
        # True once a completion (recon_complete) flush has run and no new scan
        # has started since. Lets the scan-start flush skip when the buffers are
        # already clean, so we don't double-flush (Task 3 flush-check-at-start).
        self._flushed = False

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)

    def _do_stxm_flush(self):
        """Flush STXM-side operators only."""
        for op in self.stxm_flush_ops:
            op.flush()

    def _do_ptycho_flush(self):
        """Flush ptycho-side operators only."""
        for op in self.ptycho_flush_ops:
            op.flush()

    def _request_ptycho_flush(self):
        """Request ptycho-side flush without doing any STXM work."""
        self._do_ptycho_flush()

    def _do_full_flush(self):
        """Flush STXM + ptycho operators and broadcast flush signals."""
        self._do_stxm_flush()
        self._do_ptycho_flush()
        if self.publish_backend is not None:
            import numpy as np
            self.publish_backend.publish("stxm_flush", np.array([1]))  # Simple signal
            # Also signal ptycho consumers; harmless when ptycho is disabled
            # (no subscriber listens on this subject).
            self.publish_backend.publish("ptycho_flush", np.array([1]))

    def compute(self, op_input, op_output, context):
        """Handle control messages."""
        msg = op_input.receive("input")
        transition_blocked = False
        transition_phase = "idle"
        if self.scan_state is not None:
            transition_blocked = bool(self.scan_state.get("transition_blocked_event"))
            if transition_blocked:
                transition_blocked = self.scan_state["transition_blocked_event"].is_set()
            transition_phase = self.scan_state.get("transition_phase", "idle")

        if msg == "recon_complete":
            # The recon finished its final iteration and has ALREADY saved
            # (after_iteration -> pty_out) and published the result before emitting
            # this, so flushing now is safe (Task 3: flush after the last iteration).
            if transition_blocked and transition_phase == "waiting_quiesce":
                self.logger.info(
                    "Recon quiesced for header transition — requesting ptycho flush"
                )
                self._request_ptycho_flush()
                if self.scan_state is not None:
                    self.scan_state["transition_phase"] = "waiting_flush_exec"
                return

            self.logger.info("Reconstruction complete — flushing for next scan")
            self._do_full_flush()
            self._flushed = True

        # PR4: tomography projection boundaries no longer round-trip through
        # ControlOp. With double-buffering the accumulator flips write buffers
        # itself and the recon owns current_projection + the read-buffer flip
        # (avoiding a save-vs-bump race), so there is no "projection_complete"
        # signal any more — the recon emits "recon_complete" only on the FINAL
        # projection, handled above. ControlOp is kept for recon_complete / header
        # / flush.

        elif msg == "header":
            # A live header reconfigures the scan for a new dataset. Flush so the
            # STXM path saves+clears its current buffer before reconfiguration
            # (SinkAndPublishOp.flush writes any unwritten scan). Ptycho flush is
            # deferred until the recon quiesces and emits recon_complete.
            self.logger.info("Header received — flushing for reconfigured scan")
            self._do_stxm_flush()
            if self.publish_backend is not None:
                import numpy as np
                self.publish_backend.publish("stxm_flush", np.array([1]))
            self._flushed = True

        elif msg == "flush":
            # Scan-start safety flush: only flush if the buffers aren't already
            # clean from a completion flush. If the previous scan completed, this
            # no-ops (no double flush); if it was interrupted, this cleans up.
            if transition_blocked:
                self.logger.info(
                    "Start-flush deferred for blocked transition — STXM-only flush now, ptycho waits for recon quiesce"
                )
                self._do_stxm_flush()
                self._flushed = True
                return
            if self._flushed:
                self.logger.info("Start-flush skipped — already flushed on completion")
                self._flushed = False
            else:
                self._do_full_flush()

        elif msg == "transition_overflow":
            err = "Transition overflow reported by GatherOp"
            if self.scan_state is not None:
                err = self.scan_state.get("transition_error") or err
            self.logger.error(err)
            if self.publish_backend is not None:
                import numpy as np
                self.publish_backend.publish("transition_error", np.array([1]))
            raise RuntimeError(err)

        else:
            self.logger.info(f"Received unknown message: {msg}")


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
                 flushable_ops: list[Operator] = None,
                 publish_backend = None,
                 ptycho_accum = None,
                 ptycho_recon = None,
                 scan_state: dict = None,
                 **kwargs):
        """
        Initialize control operator.

        Args:
            fragment: Holoscan fragment
            flushable_ops: List of operators that can be flushed
            publish_backend: Backend instance for publishing flush messages
            ptycho_accum: PtychoAccumulatorOp (for the scoped projection advance)
            ptycho_recon: PtychoReconstructionOp (for the scoped projection advance)
            scan_state: shared holder whose current_projection is advanced at a
                tomography projection boundary (PR3)
        """
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "ControlOp"))
        self.flushable_ops = flushable_ops
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

    def _do_flush(self):
        """Flush all flushable operators and broadcast the flush signals."""
        for op in self.flushable_ops:
            op.flush()
        if self.publish_backend is not None:
            import numpy as np
            self.publish_backend.publish("stxm_flush", np.array([1]))  # Simple signal
            # Also signal ptycho consumers; harmless when ptycho is disabled
            # (no subscriber listens on this subject).
            self.publish_backend.publish("ptycho_flush", np.array([1]))

    def compute(self, op_input, op_output, context):
        """Handle control messages."""
        msg = op_input.receive("input")

        if msg == "recon_complete":
            # The recon finished its final iteration and has ALREADY saved
            # (after_iteration -> pty_out) and published the result before emitting
            # this, so flushing now is safe (Task 3: flush after the last iteration).
            self.logger.info("Reconstruction complete — flushing for next scan")
            self._do_flush()
            self._flushed = True

        elif msg == "projection_complete":
            # PR3 tomography per-projection boundary: SCOPED advance. Reset the
            # accumulator's fill level (carry preserved) and bump current_projection.
            # The recon self-advances once it observes filled_until drop (so it can't
            # re-complete the same projection). Do NOT flush GatherOp — its cached
            # next-projection frames must survive (decision #11). The STXM sink
            # segments itself by frame count (S2), so it isn't touched here.
            if self.ptycho_accum is not None:
                self.ptycho_accum.advance_projection()
            if self.scan_state is not None:
                self.scan_state["current_projection"] += 1
                self.logger.info(
                    "Projection complete — advanced to projection %d",
                    self.scan_state["current_projection"],
                )

        elif msg == "header":
            # A live header reconfigures the scan for a new dataset. Flush so the
            # STXM path saves+clears its current buffer before reconfiguration
            # (SinkAndPublishOp.flush writes any unwritten scan). This works even
            # when ptychography is disabled; when enabled, the recon's own
            # recon_complete (on quiesce) also flushes — harmless, flush is
            # idempotent. Mark _flushed so the following start-flush skips.
            self.logger.info("Header received — flushing for reconfigured scan")
            self._do_flush()
            self._flushed = True

        elif msg == "flush":
            # Scan-start safety flush: only flush if the buffers aren't already
            # clean from a completion flush. If the previous scan completed, this
            # no-ops (no double flush); if it was interrupted, this cleans up.
            if self._flushed:
                self.logger.info("Start-flush skipped — already flushed on completion")
                self._flushed = False
            else:
                self._do_flush()

        else:
            self.logger.info(f"Received unknown message: {msg}")


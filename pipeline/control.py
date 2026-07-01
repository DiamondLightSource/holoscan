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
                 **kwargs):
        """
        Initialize control operator.
        
        Args:
            fragment: Holoscan fragment
            flushable_ops: List of operators that can be flushed
            publish_backend: Backend instance for publishing flush messages
        """
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "ControlOp"))
        self.flushable_ops = flushable_ops
        self.publish_backend = publish_backend
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


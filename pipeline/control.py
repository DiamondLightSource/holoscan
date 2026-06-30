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
        
    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)

    def compute(self, op_input, op_output, context):
        """Handle control messages."""
        msg = op_input.receive("input")

        if msg == "flush":
            # Flush all flushable operators
            for op in self.flushable_ops:
                op.flush()

            # Publish flush message through the backend if available
            if self.publish_backend is not None:
                import numpy as np
                self.publish_backend.publish("stxm_flush", np.array([1]))  # Simple signal
                # Also signal ptycho consumers; harmless when ptycho is disabled
                # (no subscriber listens on this subject).
                self.publish_backend.publish("ptycho_flush", np.array([1]))

        elif msg == "recon_complete":
            # Plumbing for PR2 (header preempt) / PR3 (tomography boundary).
            # No flush here on purpose: a completed single-projection scan must
            # keep its result until the next start/header. The flush-on-
            # completion / per-projection semantics are added with the
            # tomography work.
            self.logger.info("Reconstruction complete signal received")

        else:
            self.logger.info(f"Received unknown message: {msg}")


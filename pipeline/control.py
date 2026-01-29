"""
Control Module for Holoscan STXM Pipeline

This module contains operators for pipeline control and synchronization.
"""

import logging
from holoscan.core import Operator, OperatorSpec, IOSpec


class ControlOp(Operator):
    """
    Control operator for managing pipeline flow.
    
    Handles control messages like flush and processing_end,
    coordinating state across multiple operators.
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
        spec.output("output")

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
        
        elif msg == "processing_end":
            # Forward processing_end signal
            op_output.emit("processing_end", "output")
        
        else:
            self.logger.info(f"Received unknown message: {msg}")


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
                 stats: dict = None,
                 flushable_ops: list[Operator] = None,
                 **kwargs):
        """
        Initialize control operator.
        
        Args:
            fragment: Holoscan fragment
            stats: Shared statistics dictionary
            flushable_ops: List of operators that can be flushed
        """
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "ControlOp"))
        self.flushable_ops = flushable_ops
        
    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("output")

    def compute(self, op_input, op_output, context):
        """Handle control messages."""
        # Import nats instance
        from nats_async import launch_nats_instance
        global nats_inst
        try:
            nats_inst
        except NameError:
            nats_inst = launch_nats_instance("localhost:6000")
        
        msg = op_input.receive("input")
        
        if msg == "flush":
            # Flush all flushable operators
            for op in self.flushable_ops:
                op.flush()
            nats_inst.publish("stxm_flush", "flush")
        
        elif msg == "processing_end":
            # Forward processing_end signal
            op_output.emit("processing_end", "output")
        
        else:
            self.logger.info(f"Received unknown message: {msg}")


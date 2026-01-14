"""
Processing Module for Holoscan STXM Pipeline

This module contains operators for processing synchronized image and position data:
- MaskingOp: Applies circular masks to images, computes intensities, and passes through positions
- IntensityMapOp: Creates 2D intensity maps from position-intensity pairs using regular grid binning
- IntensityMapRBFOp: Creates 2D intensity maps using RBF interpolation (requires cupyx.scipy)
"""

import numpy as np
import cupy as cp
import logging

from holoscan.core import Operator, OperatorSpec, IOSpec

# Import calculation functions from stxm package
from stxm import create_circular_mask, compute_circle_sums_cupy_naive, compute_intensity_map_grid


class MaskingOp(Operator):
    """
    Operator for applying circular masks to images and computing intensities.
    
    Receives synchronized images and positions from GatherOp, computes inner and 
    outer circle intensities using GPU-accelerated operations, and passes through
    positions for downstream publishing.
    
    Architecture: gather -> masking_op -> publish
    """
    
    def __init__(self, fragment,
                 data_size: tuple[int, int] = (192, 192),
                 center_x: int = 100,
                 center_y: int = 100,
                 radius: int = 23,
                 *args, **kwargs):
        """
        Initialize masking operator.
        
        Args:
            fragment: Holoscan fragment
            data_size: Image dimensions (height, width)
            center_x: X coordinate of circle center
            center_y: Y coordinate of circle center
            radius: Radius of circular mask in pixels
        """
        self.mask = create_circular_mask(data_size, radius, center_x, center_y)
        self.logger = logging.getLogger(kwargs.get("name", "MaskingOp"))
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128)
        spec.output("output")

    def compute(self, op_input, op_output, context):
        """Apply mask to images, compute intensities, and pass through positions."""
        data = op_input.receive("input")
        if data is None:
            return
            
        images = cp.asarray(data["images"])  # Move to GPU
        positions = data["positions"]
        ids = data["ids"]
        
        # Compute inner and outer circle intensities
        inner_circle, outer_circle = compute_circle_sums_cupy_naive(images, self.mask)
        
        # Prepare output with intensities and positions
        output = {
            "positions": positions,
            "position_ids": ids,
            "inner": inner_circle,
            "outer": outer_circle,
            "intensity_ids": ids,
        }

        op_output.emit(output, "output")


class IntensityMapOp(Operator):
    """
    Operator for creating 2D intensity maps from position-intensity pairs.
    
    Uses regular grid binning: positions are assigned to grid pixels and 
    intensities are averaged for pixels with multiple measurements.
    This is faster than RBF interpolation but produces pixelated maps.
    
    Architecture: ... -> IntensityMapOp -> visualization/saving
    """
    
    def __init__(self, fragment, *args, **kwargs):
        """
        Initialize intensity map operator.
        
        Args:
            fragment: Holoscan fragment
        """
        super().__init__(fragment, *args, **kwargs)
        self.logger = logging.getLogger(kwargs.get("name", "IntensityMapOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("intensity_batch")
        spec.output("intensity_maps")
        spec.param("x_range", (-35, 35))
        spec.param("y_range", (-15, 15))
        spec.param("pixel_size", 0.5)

    def compute(self, op_input, op_output, context):
        """Create intensity maps using regular grid binning."""
        intensity_batch = op_input.receive("intensity_batch")
        if intensity_batch is None:
            return
        
        positions = cp.array(intensity_batch["positions"])
        intensity_inner = cp.array(intensity_batch["inner"])
        intensity_outer = cp.array(intensity_batch["outer"])
        
        # Use the regular grid mapping function from stxm_calc
        output = compute_intensity_map_grid(
            positions, intensity_inner, intensity_outer,
            x_range=self.x_range,
            y_range=self.y_range,
            pixel_size=self.pixel_size
        )
        
        op_output.emit(output, "intensity_maps")


class IntensityMapRBFOp(Operator):
    """
    Operator for creating smooth 2D intensity maps using RBF interpolation.
    
    Uses Radial Basis Function interpolation to create smooth, continuous
    intensity maps from scattered position-intensity measurements.
    Slower than regular grid binning but produces smoother results.
    
    Note: Requires cupyx.scipy.interpolate
    
    Architecture: ... -> IntensityMapRBFOp -> visualization/saving
    """
    
    def __init__(self, fragment, *args, **kwargs):
        """
        Initialize RBF intensity map operator.
        
        Args:
            fragment: Holoscan fragment
        """
        super().__init__(fragment, *args, **kwargs)
        self._grid = None
        self.logger = logging.getLogger(kwargs.get("name", "IntensityMapRBFOp"))

    def setup(self, spec: OperatorSpec):
        spec.input("intensity_batch")
        spec.output("intensity_maps")
        spec.param("x_range", (-35, 35))
        spec.param("y_range", (-15, 15))
        spec.param("pixel_size", 0.5)

    def start(self):
        """Initialize the interpolation grid."""
        x_min, x_max = self.x_range
        y_min, y_max = self.y_range
        self._grid = cp.mgrid[x_min:x_max:self.pixel_size, y_min:y_max:self.pixel_size]

    def compute(self, op_input, op_output, context):
        """Create intensity maps using RBF interpolation."""
        try:
            from cupyx.scipy.interpolate import RBFInterpolator
        except ImportError:
            self.logger.error("RBF interpolation requires cupyx.scipy.interpolate")
            return
        
        intensity_batch = op_input.receive("intensity_batch")
        if intensity_batch is None:
            return
        
        positions = cp.array(intensity_batch["positions"])
        intensity_inner = cp.array(intensity_batch["inner"])
        intensity_outer = cp.array(intensity_batch["outer"])
        
        # Create RBF interpolators and evaluate on grid
        inner_intensity_map = RBFInterpolator(
            positions, intensity_inner
        )(self._grid.reshape(2, -1).T).reshape(
            (self._grid.shape[1], self._grid.shape[2])
        )
        
        outer_intensity_map = RBFInterpolator(
            positions, intensity_outer
        )(self._grid.reshape(2, -1).T).reshape(
            (self._grid.shape[1], self._grid.shape[2])
        )
        
        output = {
            "positions_grid": self._grid.T,
            "inner_intensity_map": inner_intensity_map.T,
            "outer_intensity_map": outer_intensity_map.T,
        }
        op_output.emit(output, "intensity_maps")



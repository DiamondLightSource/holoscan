"""
Ptychography State Initialization

Builds the shared ptycho_state dict at application launch by:
1. Loading PtyREX model from JSON config
2. Applying YAML overrides
3. Running one-time PtyREX setup (mirrors pre_process_reconstruct_hardcode)
4. Pre-allocating GPU buffers
"""

import threading
import logging
import time

import numpy as np

logger = logging.getLogger(__name__)


class DummyJSplitter:
    """Stub for PtyREX's JSplit when running without MPI."""
    rank = 0
    nprocs = 1

    class global_comm:
        rank = 0
        size = 1

        @staticmethod
        def Barrier():
            pass

        @staticmethod
        def Bcast(data, root=0):
            return data


class DummyPtyPlot:
    """No-op stub for PtyREX's plotting object (required by setup routines)."""

    def init(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass


def init_ptycho_state(ptycho_cfg: dict) -> dict:
    """Build ptycho_state from PtyREX JSON config + pipeline YAML overrides.

    Parameters
    ----------
    ptycho_cfg : dict
        The ``ptychography`` section of the pipeline YAML config.

    Returns
    -------
    dict
        Shared state containing PtyREX model objects and pre-allocated
        GPU buffers.
    """
    import cupy as cp
    from ptyrex.core.io import json_read
    from ptyrex.reconstruct.core import setup
    from ptyrex.reconstruct.iterator.process_pty_model import generate_grow_scan_params
    from ptyrex.reconstruct.utils import numpy as utils_np

    # 1. Load PtyREX model from JSON config
    ptyrex_config_path = ptycho_cfg["ptyrex_config"]
    scan_ID = ptycho_cfg.get("scan_ID", [1, 1, 1])
    ID = ptycho_cfg.get("ID", [1, 1, 1])
    pty_data, pty_model, pty_params = json_read.load(ptyrex_config_path, scan_ID, ID)

    # 2. Override from YAML
    no_frames = ptycho_cfg["no_frames"]
    R = ptycho_cfg["R"]
    pty_params.total_iterations = ptycho_cfg["total_iterations"]

    # Ensure string attributes expected by PtyREX save/config routines
    pty_params.recon_name = time.strftime("%Y%m%d-%H%M%S")
    pty_data.ID = str(pty_data.ID[0]) if isinstance(pty_data.ID, list) else str(pty_data.ID)
    pty_data.scan_ID = str(pty_data.scan_ID[0]) if isinstance(pty_data.scan_ID, list) else str(pty_data.scan_ID)

    # 3. Dummy jsplitter for single-rank pipeline
    pty_params.jsplitter = DummyJSplitter()

    # 4. Run PtyREX one-time setup
    #    (mirrors pre_process_reconstruct_hardcode + before_reconstruction_stream)
    pty_data.pre_load(pty_model.detector, pty_params)
    H = int(pty_data.crop_bottom - pty_data.crop_top)
    W = int(pty_data.crop_right - pty_data.crop_left)
    pty_data.raw = np.zeros((no_frames, H, W), dtype=np.uint32)
    pty_model.scan.N = [no_frames, 1]
    pty_model.scan.valid_frames = np.arange(no_frames)
    pty_model.scan.reg_ind = np.repeat([True], no_frames)
    pty_model.scan.sz = [no_frames, 1]
    pty_data.reg_ind = pty_model.scan.reg_ind

    pty_plot = DummyPtyPlot()
    pty_data, pty_model, pty_params, pty_plot = setup.before_reconstruction_stream(
        pty_data, pty_model, pty_params, pty_plot, R, no_frames
    )

    # 5. Remaining setup from pre_process_reconstruct_hardcode
    df, ff, dp = pty_data.get_pixel_mask(pty_model.detector, pty_params)
    pty_data.dp = dp[
        pty_data.crop_top : pty_data.crop_bottom,
        pty_data.crop_left : pty_data.crop_right,
    ]
    pty_model.detector = pty_data.post_process(pty_model.detector, pty_model.geometry)
    pty_model.detector.mask = np.zeros(np.shape(pty_data.dp), dtype=np.uint32)
    pty_model.detector.mask_inv = np.ones(np.shape(pty_data.dp), dtype=np.uint32)
    pty_model.detector.mask[pty_data.dp > 0] = 1
    pty_model.detector.mask_inv[pty_data.dp > 0] = 0
    pty_params.ind_binshift, pty_params.ind_binunshift = utils_np.get_binshift_ind(
        pty_model.exit_wave.array_states.shape, pty_params.upsample
    )
    pty_params.scan_trial_shift_radii = np.zeros(2, dtype=np.float32)
    pty_model.obj.array_global_old[:] = pty_model.obj.array_global[:]
    generate_grow_scan_params(pty_params)

    # 6. Pre-allocate GPU buffers
    raw_gpu = cp.zeros((no_frames, H, W), dtype=cp.uint32)
    positions_full = cp.zeros((1, 2, no_frames), dtype=cp.float32)
    tilts_full = cp.zeros((1, 2, no_frames), dtype=cp.float32)

    # Point model scan attributes at full buffers
    pty_model.scan.positions = positions_full
    pty_model.scan.tilts = tilts_full
    pty_model.scan.original = cp.zeros_like(positions_full)
    pty_model.scan.previous = cp.zeros_like(positions_full)

    logger.info(
        "ptycho_state initialized: %d frames, image size %dx%d, "
        "%d total iterations",
        no_frames, H, W, pty_params.total_iterations,
    )

    # 7. Assemble ptycho_state
    return {
        "pty_data": pty_data,
        "pty_model": pty_model,
        "pty_params": pty_params,
        "raw_gpu": raw_gpu,
        "positions_full": positions_full,
        "tilts_full": tilts_full,
        "filled_until": 0,
        "no_frames": no_frames,
        "lock": threading.Lock(),
    }

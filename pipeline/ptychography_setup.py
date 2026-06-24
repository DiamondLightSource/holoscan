"""
Ptychography State Initialization

Builds the shared ptycho_state dict at application launch by:
1. Loading PtyREX model from JSON config
2. Computing scan extent from npoints and step_size (matching PtyREX streaming)
3. Running one-time PtyREX setup (mirrors pre_process_reconstruct_stream)
4. Pre-allocating GPU buffers
"""

import threading
import logging
import time
import zmq
import numpy as np

logger = logging.getLogger(__name__)


class _DummyComm:
    """No-op MPI communicator stub (all calls are guarded by nprocs > 1)."""
    rank = 0
    size = 1

    @staticmethod
    def Barrier():
        pass

    @staticmethod
    def Bcast(data, root=0):
        return data

    @staticmethod
    def Allreduce(*args, **kwargs):
        pass


class DummyJSplitter:
    """Stub for PtyREX's JSplit when running without MPI."""
    rank = 0
    nprocs = 1
    probe_av = 0
    comm = _DummyComm()
    global_comm = _DummyComm()


class DummyPtyPlot:
    """No-op stub for PtyREX's plotting object (required by setup routines)."""

    def init(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass


def init_ptycho_state(ptycho_cfg: dict) -> dict:
    """Build ptycho_state from PtyREX JSON config + pipeline YAML overrides.

    The only scan parameters required are npoints_h, npoints_v, step_size_h,
    and step_size_v — the scan extent in pixels (R) and object size are
    derived automatically, matching PtyREX's streaming workflow.

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
    import importlib.util, os, sys
    import cupy as cp

    _spec = importlib.util.find_spec("ptyrex")
    if _spec and _spec.origin:
        _ptyrex_root = os.path.dirname(os.path.dirname(_spec.origin))
        if _ptyrex_root not in sys.path:
            sys.path.insert(0, _ptyrex_root)

    from ptyrex.core.io import json_read
    from ptyrex.reconstruct.core import setup
    from ptyrex.reconstruct.iterator.process_pty_model import generate_grow_scan_params
    from ptyrex.reconstruct.utils import numpy as utils_np

    # ── 1. Load PtyREX model from JSON config ──────────────────────────
    ptyrex_config_path = ptycho_cfg["ptyrex_config"]
    scan_ID = ptycho_cfg.get("scan_ID", [1, 1, 1])
    ID = ptycho_cfg.get("ID", [1, 1, 1])
    pty_data, pty_model, pty_params = json_read.load(ptyrex_config_path, scan_ID, ID)

    if ptycho_cfg["header"] == True: 
        # -- 2. Get scan points and step sizes from zmq stream --
        context = zmq.Context()
        socket_h = context.socket(zmq.PULL)
        socket_h.setsockopt(zmq.RCVTIMEO, 100000)
        print('endpoint header: ', ptycho_cfg["endpoint_header"])

        try: 
            socket_h.connect(ptycho_cfg["endpoint_header"])
        except zmq.error.ZMQError:
            logger.error("Failed to create socket")

        try: 
            header = socket_h.recv_json()
        except:
            logger.error("Failed to receive header from socket")
            raise

        npoints_h = int(header["nX"])
        npoints_v = int(header["nY"])
        step_size_h = float(header["dX"])
        step_size_v = float(header["dY"])

        exp_time = float(header["exp_time"]) 
        iter_time = 1.0/3000.0 # time for 1 iteration, roughly 3 kHz, ie 3000 frames/s

        if exp_time <= iter_time:
            pty_params.total_iterations = 2
        else:
            pty_params.total_iterations = np.ceil( exp_time / iter_time ) 

        socket_h.close()
    else: 
        # ── 2. Compute streaming parameters from npoints / step_size ───────
        npoints_h = ptycho_cfg["npoints_h"]
        npoints_v = ptycho_cfg["npoints_v"]
        step_size_h = ptycho_cfg["step_size_h"]
        step_size_v = ptycho_cfg["step_size_v"]
        
        pty_params.total_iterations = ptycho_cfg["total_iterations"]

    no_frames = npoints_h * npoints_v
    # Scan extent in microns with 20% padding (same formula as PtyREX streaming)
    N = [
        ((npoints_v - 1) * step_size_v) * 1.2,
        ((npoints_h - 1) * step_size_h) * 1.2,
    ]
    logger.info(
        "Scan: %d x %d points, step %.3f x %.3f µm → "
        "N = [%.2f, %.2f] µm, %d frames",
        npoints_h, npoints_v, step_size_h, step_size_v,
        N[1], N[0], no_frames,
    )
    
    #pty_params.total_iterations = ptycho_cfg["total_iterations"]

    # Ensure string attributes expected by PtyREX save/config routines
    pty_params.recon_name = time.strftime("%Y%m%d-%H%M%S")
    pty_data.ID = str(pty_data.ID[0]) if isinstance(pty_data.ID, list) else str(pty_data.ID)
    pty_data.scan_ID = str(pty_data.scan_ID[0]) if isinstance(pty_data.scan_ID, list) else str(pty_data.scan_ID)

    # ── 3. Dummy jsplitter for single-rank pipeline ────────────────────
    pty_params.jsplitter = DummyJSplitter()

    # ── 4. Initialise scan arrays (mirrors pre_process_reconstruct_stream) ─
    pty_data.pre_load(pty_model.detector, pty_params)
    H = int(pty_data.crop_bottom - pty_data.crop_top)
    W = int(pty_data.crop_right - pty_data.crop_left)
    pty_data.raw = np.zeros((no_frames, H, W), dtype=np.uint32)

    pty_model.scan.positions = np.ones([no_frames, 2], np.float32)
    pty_model.scan.tilts = np.zeros_like(pty_model.scan.positions)
    pty_model.scan.N = pty_model.scan.positions.shape
    pty_model.scan.reg_ind = np.repeat([True], no_frames)[:, np.newaxis]
    pty_model.scan.valid_frames = np.arange(no_frames)
    pty_model.scan.valid_frames = pty_model.scan.valid_frames[
        pty_model.scan.reg_ind[:, 0]
    ]
    pty_model.scan.positions = pty_model.scan.positions[
        pty_model.scan.valid_frames, :
    ]
    pty_model.scan.tilts = pty_model.scan.tilts[
        pty_model.scan.valid_frames, :
    ]
    pty_model.scan.sz = [pty_model.scan.positions.shape[0], 1]
    pty_data.reg_ind = pty_model.scan.reg_ind

    # ── 5. PtyREX one-time setup — pass N in microns (not R in pixels) ─
    pty_plot = DummyPtyPlot()
    pty_data, pty_model, pty_params, pty_plot = setup.before_reconstruction_stream(
        pty_data, pty_model, pty_params, pty_plot, N
    )

    # ── 6. Pixel mask + dp transforms (mirrors post_process_stream) ──
    df, ff, dp = pty_data.get_pixel_mask(pty_model.detector, pty_params)
    pty_data.dp = dp[
        pty_data.crop_top : pty_data.crop_bottom,
        pty_data.crop_left : pty_data.crop_right,
    ]

    # Set detector mask from ORIGINAL dp (before transforms/inversion).
    # mask=1 → good pixel, mask=0 → dead pixel.
    pty_model.detector.mask = np.ones(np.shape(pty_data.dp), dtype=np.uint32)
    pty_model.detector.mask_inv = np.zeros(np.shape(pty_data.dp), dtype=np.uint32)
    pty_model.detector.mask[pty_data.dp > 0] = 0
    pty_model.detector.mask_inv[pty_data.dp > 0] = 1

    # Now apply the same dp transforms that post_process_stream does on
    # its first iteration so that dp matches the preprocessed data layout
    # and uses the inverted convention expected by the flux computation
    # (dp == 1 → good pixel).
    det = pty_model.detector
    if det.orientation == "01":
        pty_data.dp = pty_data.dp[:, ::-1]
    elif det.orientation == "10":
        pty_data.dp = pty_data.dp[::-1, :]
    elif det.orientation == "11":
        pty_data.dp = pty_data.dp[::-1, ::-1]
    pty_data.dp = np.rot90(pty_data.dp, det.rot)
    if pty_model.geometry.modality != "near-field":
        pty_data.dp = np.fft.fftshift(pty_data.dp, axes=(0, 1))
    pty_data.dp = 1 - pty_data.dp  # invert: dp==1 now means good pixel

    pty_params.ind_binshift, pty_params.ind_binunshift = utils_np.get_binshift_ind(
        pty_model.exit_wave.array_states.shape, pty_params.upsample
    )
    pty_params.scan_trial_shift_radii = np.zeros(2, dtype=np.float32)
    pty_model.obj.array_global_old[:] = pty_model.obj.array_global[:]
    generate_grow_scan_params(pty_params)

    # ── 7. Pre-allocate GPU buffers ────────────────────────────────────
    raw_gpu = cp.zeros((no_frames, H, W), dtype=cp.uint32)
    positions_full = cp.zeros((1, 2, no_frames), dtype=cp.float32)
    tilts_full = cp.zeros((1, 2, no_frames), dtype=cp.float32)

    pty_model.scan.positions = positions_full
    pty_model.scan.tilts = tilts_full
    pty_model.scan.original = cp.zeros_like(positions_full)
    pty_model.scan.previous = cp.zeros_like(positions_full)

    logger.info(
        "ptycho_state initialized: %d frames, image size %dx%d, "
        "object size %s, %d total iterations",
        no_frames, H, W,
        tuple(int(x) for x in pty_model.obj.sz_glo),
        pty_params.total_iterations,
    )

    # ── 8. Assemble ptycho_state ───────────────────────────────────────
    return {
        "pty_data": pty_data,
        "pty_model": pty_model,
        "pty_params": pty_params,
        "raw_gpu": raw_gpu,
        "positions_full": positions_full,
        "tilts_full": tilts_full,
        "filled_until": 0,
        "no_frames": no_frames,
        "scan_center_py": None,
        "scan_center_px": None,
        "lock": threading.Lock(),
    }

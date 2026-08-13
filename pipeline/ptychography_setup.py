"""
Ptychography State Initialization

Builds the shared ptycho_state dict at application launch, split into a
grid-INDEPENDENT one-time model load and a grid-DEPENDENT geometry
configuration so scan geometry can be reconfigured on the fly from a live
header (PR2) without reallocating GPU buffers (R-6):

1. ``load_ptycho_model``      — load PtyREX model, jsplitter, detector pre-load
2. GPU buffers allocated once at the configured MAX capacity (never realloced)
3. ``configure_scan_geometry`` — grid-dependent object sizing + view re-pointing
4. ``init_ptycho_state``       — load + allocate-at-max + one default configure
"""

import threading
import logging
import time

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


def _ensure_ptyrex_on_path():
    """Make the vendored PtyREX package importable (mirrors the original
    lazy sys.path insertion so the host can import this module without PtyREX)."""
    import importlib.util, os, sys

    _spec = importlib.util.find_spec("ptyrex")
    if _spec and _spec.origin:
        _ptyrex_root = os.path.dirname(os.path.dirname(_spec.origin))
        if _ptyrex_root not in sys.path:
            sys.path.insert(0, _ptyrex_root)


def load_ptycho_model(ptycho_cfg: dict):
    """Grid-INDEPENDENT one-time load.

    Loads the PtyREX model from JSON, installs the single-rank jsplitter stub,
    and runs the detector pre-load (crop geometry). Nothing here depends on the
    scan grid, so it runs exactly once at startup.

    Parameters
    ----------
    ptycho_cfg : dict
        The ``ptychography`` section of the pipeline YAML config.

    Returns
    -------
    (pty_data, pty_model, pty_params, H, W)
        PtyREX objects plus the cropped detector frame size (H, W).
    """
    _ensure_ptyrex_on_path()
    from ptyrex.core.io import json_read

    # ── 1. Load PtyREX model from JSON config ──────────────────────────
    ptyrex_config_path = ptycho_cfg["ptyrex_config"]
    scan_ID = ptycho_cfg.get("scan_ID", [1, 1, 1])
    ID = ptycho_cfg.get("ID", [1, 1, 1])
    pty_data, pty_model, pty_params = json_read.load(ptyrex_config_path, scan_ID, ID)

    pty_params.total_iterations = ptycho_cfg["total_iterations"]

    # Ensure string attributes expected by PtyREX save/config routines
    pty_params.recon_name = time.strftime("%Y%m%d-%H%M%S")
    pty_data.ID = str(pty_data.ID[0]) if isinstance(pty_data.ID, list) else str(pty_data.ID)
    pty_data.scan_ID = str(pty_data.scan_ID[0]) if isinstance(pty_data.scan_ID, list) else str(pty_data.scan_ID)

    # ── Dummy jsplitter for single-rank pipeline ───────────────────────
    pty_params.jsplitter = DummyJSplitter()

    # ── Detector pre-load (crop geometry — grid-independent) ───────────
    pty_data.pre_load(pty_model.detector, pty_params)
    H = int(pty_data.crop_bottom - pty_data.crop_top)
    W = int(pty_data.crop_right - pty_data.crop_left)

    return pty_data, pty_model, pty_params, H, W


def configure_scan_geometry(
    ptycho_state: dict,
    npoints_h: int,
    npoints_v: int,
    step_size_h: float,
    step_size_v: float,
):
    """Grid-DEPENDENT (re)configuration of scan geometry.

    Recomputes ``no_frames`` and the scan extent ``N``, re-runs the PtyREX
    object-sizing setup, refreshes the detector pixel mask / dp transforms, and
    re-points the scan arrays at the pre-allocated GPU buffers. **No GPU
    reallocation** (R-6): a grid whose ``no_frames`` exceeds the configured
    capacity is rejected with ``ValueError``.

    Safe to call at startup (from ``init_ptycho_state``) and again on a live
    header, but ONLY while the reconstruction is quiesced (R-4 handshake) — it
    re-points views the recon reads during a PIE iteration.

    Sets ``ptycho_state["needs_gpu_reinit"] = True`` so the reconstruction op
    re-runs its one-time GPU transfer and re-snapshots the pristine object for
    the new geometry.
    """
    _ensure_ptyrex_on_path()
    from ptyrex.reconstruct.core import setup
    from ptyrex.reconstruct.iterator.process_pty_model import generate_grow_scan_params
    from ptyrex.reconstruct.utils import numpy as utils_np
    import cupy as cp

    pty_data = ptycho_state["pty_data"]
    pty_model = ptycho_state["pty_model"]
    pty_params = ptycho_state["pty_params"]
    H = ptycho_state["H"]
    W = ptycho_state["W"]
    capacity = ptycho_state["capacity"]

    # ── Compute streaming parameters from npoints / step_size ──────────
    no_frames = int(npoints_h) * int(npoints_v)
    if no_frames > capacity:
        raise ValueError(
            f"Requested grid {npoints_h}x{npoints_v} = {no_frames} frames exceeds "
            f"the pre-allocated capacity of {capacity} frames "
            f"(increase max_npoints_h/max_npoints_v in the config). "
            f"Buffers are never reallocated at runtime (R-6)."
        )

    # Scan extent in microns with 20% padding (same formula as PtyREX streaming) ## increased it on 11/09/26 to 100% padding for cases with position overshoots
    N = [
        ((npoints_v - 1) * step_size_v) * 2,
        ((npoints_h - 1) * step_size_h) * 2,
    ]
    logger.info(
        "Configuring scan: %d x %d points, step %.3f x %.3f µm → "
        "N = [%.2f, %.2f] µm, %d frames (capacity %d)",
        npoints_h, npoints_v, step_size_h, step_size_v,
        N[1], N[0], no_frames, capacity,
    )

    # If an energy scan schedule is present, seed projection 0 energy BEFORE
    # before_reconstruction_stream so all energy-dependent setup (wav, dx,
    # geometry, probe init) is built for the correct starting energy.
    scan_state = ptycho_state.get("scan_state") or {}
    energy_steps = scan_state.get("energy_steps_keV")
    if energy_steps:
        start_energy_keV = float(energy_steps[0])
        pty_model.source.energy = np.array([start_energy_keV * 1e3], dtype=np.float64)
        scan_state["current_energy_keV"] = start_energy_keV
        logger.info(
            "Applying initial energy step before geometry setup: %.6f keV",
            start_energy_keV,
        )

    # ── Initialise scan arrays (mirrors pre_process_reconstruct_stream) ─
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

    # ── PtyREX one-time setup — pass N in microns (not R in pixels) ────
    pty_plot = DummyPtyPlot()
    pty_data, pty_model, pty_params, pty_plot = setup.before_reconstruction_stream(
        pty_data, pty_model, pty_params, pty_plot, N
    )

    # ── Pixel mask + dp transforms (mirrors post_process_stream) ───────
    # Re-run every reconfigure so ordering matches the validated single-scan
    # path (before_reconstruction_stream then get_pixel_mask); cheap.
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

    # Apply the same dp transforms that post_process_stream does on its first
    # iteration so dp matches the preprocessed data layout and uses the inverted
    # convention expected by the flux computation (dp == 1 → good pixel).
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

    # ── Re-point scan arrays at the pre-allocated GPU buffers ──────────
    # The buffers are allocated once at capacity in init_ptycho_state and never
    # realloced; the accumulator/recon index them via ptycho_state directly and
    # bound their reads to [:n_filled] (n_filled <= no_frames <= capacity).
    # PR4: point the model at buffer 0 to start; the recon re-points these views
    # to positions_full[read_idx] / raw_gpu[read_idx] each compute, so this is
    # just the initial binding + the shape source for scan.original/previous.
    positions_full = ptycho_state["positions_full"]
    tilts_full = ptycho_state["tilts_full"]
    pty_model.scan.positions = positions_full[0]
    pty_model.scan.tilts = tilts_full[0]
    pty_model.scan.original = cp.zeros_like(positions_full[0])
    pty_model.scan.previous = cp.zeros_like(positions_full[0])

    # ── Update shared state ────────────────────────────────────────────
    # A (re)configure is a clean scan boundary: reset the ping-pong to buffer 0,
    # both fill levels empty, buffer 0 claimed for the first projection.
    with ptycho_state["lock"]:
        ptycho_state["no_frames"] = no_frames
        nbuf = ptycho_state["num_buffers"]
        ptycho_state["filled_until"] = [0] * nbuf
        ptycho_state["write_idx"] = 0
        ptycho_state["read_idx"] = 0
        ptycho_state["buf_free"] = [i != 0 for i in range(nbuf)]
    ptycho_state["N"] = N
    # Clear auto-centre so the new geometry re-derives its own scan centre.
    ptycho_state["scan_center_py"] = None
    ptycho_state["scan_center_px"] = None
    # Signal the reconstruction op to re-init GPU state for the new object size.
    ptycho_state["needs_gpu_reinit"] = True

    # Mirror the frame count into the always-present scan_state holder (S11) so
    # the STXM path and header op can read it even when ptycho is disabled.
    scan_state = ptycho_state.get("scan_state")
    if scan_state is not None:
        scan_state["no_frames"] = no_frames
        scan_state["npoints_h"] = int(npoints_h)
        scan_state["npoints_v"] = int(npoints_v)
        scan_state["step_size_h"] = float(step_size_h)
        scan_state["step_size_v"] = float(step_size_v)

    logger.info(
        "Scan geometry configured: %d frames, image size %dx%d, object size %s",
        no_frames, H, W,
        tuple(int(x) for x in pty_model.obj.sz_glo),
    )


def init_ptycho_state(ptycho_cfg: dict, scan_state: dict = None) -> dict:
    """Build ptycho_state: load the model, allocate GPU buffers at the configured
    MAX capacity, then configure a default scan geometry so the pipeline can run
    before any header arrives.

    Parameters
    ----------
    ptycho_cfg : dict
        The ``ptychography`` section of the pipeline YAML config. Must provide
        ``max_npoints_h``/``max_npoints_v`` (buffer capacity + default grid) and
        ``default_step_size_h``/``default_step_size_v`` (startup step sizes).
    scan_state : dict, optional
        The always-present shared holder for projection/frame counts (S11). Its
        ``no_frames`` is populated by the default configure below.

    Returns
    -------
    dict
        Shared state containing PtyREX model objects, pre-allocated GPU buffers
        (at max capacity), geometry, and the preemption handshake primitives.
    """
    import cupy as cp

    # ── 1. Grid-independent model load ─────────────────────────────────
    pty_data, pty_model, pty_params, H, W = load_ptycho_model(ptycho_cfg)

    # ── 2. Capacity + default startup grid ─────────────────────────────
    max_npoints_h = int(ptycho_cfg["max_npoints_h"])
    max_npoints_v = int(ptycho_cfg["max_npoints_v"])
    capacity = max_npoints_h * max_npoints_v
    default_step_h = float(ptycho_cfg["default_step_size_h"])
    default_step_v = float(ptycho_cfg["default_step_size_v"])

    # ── 3. Pre-allocate GPU buffers ONCE at max capacity (R-6) ─────────
    # PR4 double-buffering: TWO buffer sets (ping-pong). While the recon
    # finalizes projection N on the read buffer, the accumulator fills
    # projection N+1 into the write buffer — so the accumulator never stops
    # draining and a free-running detector is never backpressured across the
    # ~one-iteration finalize window. Single-projection scans always use
    # buffer 0 (no flip). Cost: 2× raw_gpu (~tens of MB).
    NUM_BUFFERS = 2
    raw_gpu = [cp.zeros((capacity, H, W), dtype=cp.uint32) for _ in range(NUM_BUFFERS)]
    positions_full = [cp.zeros((1, 2, capacity), dtype=cp.float32) for _ in range(NUM_BUFFERS)]
    tilts_full = [cp.zeros((1, 2, capacity), dtype=cp.float32) for _ in range(NUM_BUFFERS)]

    logger.info(
        "ptycho_state buffers allocated at capacity: %d frames × %d buffers "
        "(double-buffered), image size %dx%d, %d total iterations",
        capacity, NUM_BUFFERS, H, W, pty_params.total_iterations,
    )

    # ── 4. Assemble ptycho_state (geometry filled in by configure) ─────
    ptycho_state = {
        "pty_data": pty_data,
        "pty_model": pty_model,
        "pty_params": pty_params,
        "raw_gpu": raw_gpu,             # list[NUM_BUFFERS] of (capacity,H,W)
        "positions_full": positions_full,  # list[NUM_BUFFERS]
        "tilts_full": tilts_full,          # list[NUM_BUFFERS]
        "num_buffers": NUM_BUFFERS,
        # PR4 ping-pong state (all touched under "lock"):
        #   filled_until[i] — fill level of buffer i
        #   write_idx       — buffer the accumulator writes (accumulator owns)
        #   read_idx        — buffer the recon reads (recon owns)
        #   buf_free[i]     — buffer i holds no data the recon still needs, so the
        #                     accumulator may claim it for a new projection. Init:
        #                     buffer 0 is claimed for the first projection.
        "filled_until": [0] * NUM_BUFFERS,
        "write_idx": 0,
        "read_idx": 0,
        "buf_free": [i != 0 for i in range(NUM_BUFFERS)],
        "no_frames": 0,          # set by configure_scan_geometry
        "H": H,
        "W": W,
        "capacity": capacity,
        "scan_center_py": None,
        "scan_center_px": None,
        "N": None,               # set by configure_scan_geometry
        "lock": threading.Lock(),
        "scan_state": scan_state,
        # Preemption handshake (R-4): header stages pending_geometry + sets
        # preempt_requested; recon saves the partial, sets quiesced, then applies
        # the geometry while quiesced and clears the flags.
        "preempt_requested": threading.Event(),
        "quiesced": threading.Event(),
        "pending_geometry": None,
        "needs_gpu_reinit": False,
    }

    # ── 5. Configure the default (startup) scan geometry ───────────────
    configure_scan_geometry(
        ptycho_state,
        npoints_h=max_npoints_h,
        npoints_v=max_npoints_v,
        step_size_h=default_step_h,
        step_size_v=default_step_v,
    )
    # First configure just did startup init; no reconfigure has happened yet.
    ptycho_state["needs_gpu_reinit"] = False

    return ptycho_state

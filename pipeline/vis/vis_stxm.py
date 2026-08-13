import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from queue import Empty, Queue
import sys
from pathlib import Path
import threading
import json
import time
import matplotlib.tri as tri
import matplotlib
from argparse import ArgumentParser
import zmq


# ===================== Subscribe backends =====================

class SubscribeBackend:
    """Base class for subscription backends."""

    def get_queue(self, subject: str) -> Queue:
        raise NotImplementedError


class NatsSubscribeBackend(SubscribeBackend):
    """NATS subscription backend."""

    def __init__(self, host: str, subjects: tuple):
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from nats_async import launch_nats_instance
        self.nats_inst = launch_nats_instance(host=host, subscribe_subjects=subjects)

    def get_queue(self, subject: str) -> Queue:
        return self.nats_inst.get_rxq(subject)


class ZmqSubscribeBackend(SubscribeBackend):
    """ZMQ SUB backend."""

    def __init__(self, endpoint: str, subjects: tuple):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(endpoint)
        self.subjects = subjects
        for subject in subjects:
            self.socket.subscribe(subject.encode('utf-8'))
        self.queues = {subject: Queue() for subject in subjects}
        self.running = True
        self.thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.thread.start()
        print(f"ZMQ subscriber connected to {endpoint}")

    def _receive_loop(self):
        while self.running:
            try:
                if self.socket.poll(100):
                    parts = self.socket.recv_multipart()
                    topic_str = parts[0].decode('utf-8')
                    if topic_str not in self.queues:
                        continue
                    if len(parts) == 3:
                        header = json.loads(parts[1].decode('utf-8'))
                        arr = np.frombuffer(
                            parts[2], dtype=header['dtype']
                        ).reshape(header['shape']).copy()
                        self.queues[topic_str].put(arr, block=False)
                    elif len(parts) == 2:
                        arr = np.array(json.loads(parts[1].decode('utf-8')))
                        self.queues[topic_str].put(arr, block=False)
                    print(f"  [ZMQ] {topic_str}: shape={arr.shape} dtype={arr.dtype}")
            except Exception as e:
                print(f"Error in ZMQ receive loop: {e}")
                time.sleep(0.1)

    def get_queue(self, subject: str) -> Queue:
        return self.queues[subject]

    def close(self):
        self.running = False
        self.thread.join(timeout=1.0)
        self.socket.close()
        self.context.term()


STXM_SUBJECTS = (
    "stxm_inner", "stxm_outer", "stxm_positions",
    "stxm_position_ids", "stxm_intensity_ids", "stxm_flush",
)

PTYCHO_SUBJECTS = (
    "ptycho_object_phase", "ptycho_object_amp",
    "ptycho_probe_phase", "ptycho_probe_amp", "ptycho_flush",
)


def get_subscribe_backend(backend: str, endpoint: str, subjects: tuple):
    if backend == "nats":
        return NatsSubscribeBackend(endpoint, subjects)
    elif backend == "zmq":
        return ZmqSubscribeBackend(endpoint, subjects)
    else:
        raise ValueError(f"Unknown backend: {backend}")


# ===================== STXM data receiver =====================

stxm_dict = {
    "outer": None,
    "inner": None,
    "positions": None,
    "position_ids": None,
    "intensity_ids": None,
    "total_n": 0,
}


def receive_stxm_data(sub_backend):
    global stxm_dict
    while True:
        for key, subject in (
            ("outer", "stxm_outer"),
            ("inner", "stxm_inner"),
            ("positions", "stxm_positions"),
            ("position_ids", "stxm_position_ids"),
            ("intensity_ids", "stxm_intensity_ids"),
        ):
            try:
                arr = sub_backend.get_queue(subject).get(block=False)
                if stxm_dict[key] is None:
                    stxm_dict[key] = arr.copy()
                else:
                    stxm_dict[key] = np.concatenate([stxm_dict[key], arr.copy()])
            except Empty:
                pass

        try:
            sub_backend.get_queue("stxm_flush").get(block=False)
            print("STXM flush received")
            for k in ("outer", "inner", "positions", "position_ids", "intensity_ids"):
                stxm_dict[k] = None
            stxm_dict["total_n"] = 0
        except Empty:
            pass

        fields = ("inner", "outer", "positions", "position_ids", "intensity_ids")
        if all(stxm_dict[f] is not None for f in fields):
            stxm_dict["total_n"] = min(stxm_dict[f].shape[0] for f in fields)

        time.sleep(0.01)


# ===================== Ptycho data receiver =====================

ptycho_dict = {
    "object_phase": None,
    "object_amp": None,
    "probe_phase": None,
    "probe_amp": None,
}


def receive_ptycho_data(sub_backend):
    global ptycho_dict
    while True:
        for key, subject in (
            ("object_phase", "ptycho_object_phase"),
            ("object_amp", "ptycho_object_amp"),
            ("probe_phase", "ptycho_probe_phase"),
            ("probe_amp", "ptycho_probe_amp"),
        ):
            try:
                arr = np.squeeze(sub_backend.get_queue(subject).get(block=False))
                if arr.ndim < 2:
                    print(f"Ptycho {key}: skipping bad shape {arr.shape}")
                    continue
                ptycho_dict[key] = arr
            except Empty:
                pass

        try:
            sub_backend.get_queue("ptycho_flush").get(block=False)
            print("Ptycho flush received")
            for k in ptycho_dict:
                ptycho_dict[k] = None
        except Empty:
            pass

        time.sleep(0.05)


# ===================== Figure setup =====================

def update_tricontour(ax, x, y, intensity_map, reset_lim=True):
    triang = tri.Triangulation(x, y)
    vmin = np.percentile(intensity_map, 3.5)
    vmax = np.percentile(intensity_map, 98.5)
    im = ax.tricontourf(triang, intensity_map, 51, cmap='gray', vmin=vmin, vmax=vmax)
    if reset_lim:
        ax.set_xlim(x.min(), x.max())
        ax.set_ylim(y.min(), y.max())
    return im


def build_combined_figure():
    """3-row x 2-col layout: STXM top, ptycho object middle, ptycho probe bottom."""
    plt.style.use('dark_background')
    matplotlib.rcParams.update({'font.size': 8})

    fig = plt.figure(figsize=(10, 10))
    gs = fig.add_gridspec(3, 2, hspace=0.35, wspace=0.3)

    ax_stxm_outer = fig.add_subplot(gs[0, 0])
    ax_stxm_inner = fig.add_subplot(gs[0, 1])
    ax_obj_phase  = fig.add_subplot(gs[1, 0])
    ax_obj_amp    = fig.add_subplot(gs[1, 1])
    ax_prb_phase  = fig.add_subplot(gs[2, 0])
    ax_prb_amp    = fig.add_subplot(gs[2, 1])

    ax_stxm_outer.set_title("STXM Outer")
    ax_stxm_inner.set_title("STXM Inner")
    ax_obj_phase.set_title("Object Phase")
    ax_obj_amp.set_title("Object Amplitude")
    ax_prb_phase.set_title("Probe Phase")
    ax_prb_amp.set_title("Probe Amplitude")

    placeholder = np.zeros((64, 64)) * np.nan
    ptycho_axes_info = [
        (ax_obj_phase,  "gray", "object_phase"),
        (ax_obj_amp,    "gray",     "object_amp"),
        (ax_prb_phase,  "gray", "probe_phase"),
        (ax_prb_amp,    "gray",     "probe_amp"),
    ]
    ptycho_ims = {}
    for ax, cmap, key in ptycho_axes_info:
        im = ax.imshow(placeholder, cmap=cmap, interpolation='nearest', aspect='equal')
        ax.invert_xaxis()
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        ptycho_ims[key] = im

    return fig, ax_stxm_outer, ax_stxm_inner, ptycho_ims

def build_combined_figure_reduced():
    """2-row x 2-col layout: STXM top, ptycho object phase + probe modulus bottom."""
    plt.style.use('dark_background')
    matplotlib.rcParams.update({'font.size': 8})

    fig = plt.figure(figsize=(10, 10))
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.3)

    ax_stxm_outer = fig.add_subplot(gs[0, 0])
    ax_stxm_inner = fig.add_subplot(gs[0, 1])
    ax_obj_phase  = fig.add_subplot(gs[1, 0])
    ax_prb_amp    = fig.add_subplot(gs[1, 1])

    ax_stxm_outer.set_title("STXM Outer")
    ax_stxm_inner.set_title("STXM Inner")
    ax_obj_phase.set_title("Object Phase")
    ax_prb_amp.set_title("Probe Amplitude")

    placeholder = np.zeros((64, 64)) * np.nan
    ptycho_axes_info = [
        (ax_obj_phase,  "gray", "object_phase"),
        (ax_prb_amp,    "gray",     "probe_amp"),
    ]
    ptycho_ims = {}
    for ax, cmap, key in ptycho_axes_info:
        im = ax.imshow(placeholder, cmap=cmap, interpolation='nearest', aspect='equal')
        ax.invert_xaxis()
        #fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        fig.colorbar(im, ax=ax) #, fraction=0.046, pad=0.04)
        ptycho_ims[key] = im

    return fig, ax_stxm_outer, ax_stxm_inner, ptycho_ims


# ===================== Combined animation =====================

_stxm_n_prev = 0


def animate_combined(i):
    global stxm_dict, ptycho_dict, _stxm_n_prev
    global ax_stxm_outer, ax_stxm_inner, ptycho_ims, reset_limits, plot_batch

    # ---- STXM panels ----
    fields = ("inner", "outer", "positions", "position_ids", "intensity_ids")
    if all(stxm_dict[f] is not None for f in fields):
        n = stxm_dict["total_n"]
        if n > 0 and n != _stxm_n_prev:
            n_plot = n if plot_batch is None else min(n, plot_batch)
            _stxm_n_prev = n

            x, y, z, th = stxm_dict["positions"][:n_plot].T
            s_inner = stxm_dict["inner"][:n_plot]
            s_outer = stxm_dict["outer"][:n_plot]

            th_rad = th * np.pi / 180
            x_lab = (x * np.cos(th_rad) + z * np.sin(th_rad)) * -1
            y_lab = y * -1

            for c in ax_stxm_outer.collections:
                c.remove()
            for c in ax_stxm_inner.collections:
                c.remove()

            update_tricontour(ax_stxm_outer, x_lab, y_lab, s_outer, reset_limits)
            update_tricontour(ax_stxm_inner, x_lab, y_lab, s_inner, reset_limits)

            print(f"STXM frame {i}: {n_plot} points")

    # ---- Ptycho panels ----
    for key, im in ptycho_ims.items():
        arr = ptycho_dict[key]
        if arr is not None and arr.ndim >= 2:
            im.set_data(arr)
            im.set_clim(vmin=np.nanpercentile(arr, 2),
                        vmax=np.nanpercentile(arr, 98))


# ===================== Main =====================

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--backend", type=str, default="zmq",
                        choices=["nats", "zmq"],
                        help="Backend type")
    parser.add_argument("--endpoint", type=str, default=None,
                        help="Backend endpoint (default: 'localhost:6000' for NATS, "
                             "'tcp://localhost:6020' for ZMQ)")
    parser.add_argument("--plot_batch", type=int, default=None,
                        help="STXM plot batch size")
    parser.add_argument("--xmin", type=float, default=None)
    parser.add_argument("--xmax", type=float, default=None)
    parser.add_argument("--ymin", type=float, default=None)
    parser.add_argument("--ymax", type=float, default=None)
    args = parser.parse_args()

    plot_batch = args.plot_batch

    if args.endpoint is None:
        endpoint = "localhost:6000" if args.backend == "nats" else "tcp://localhost:6020"
    else:
        endpoint = args.endpoint

    print(f"Using {args.backend} backend at {endpoint}")

    all_subjects = STXM_SUBJECTS + PTYCHO_SUBJECTS
    sub_backend = get_subscribe_backend(args.backend, endpoint, all_subjects)

    threading.Thread(target=receive_stxm_data, args=(sub_backend,), daemon=True).start()
    threading.Thread(target=receive_ptycho_data, args=(sub_backend,), daemon=True).start()

    reduced_flag = False

    if reduced_flag:
        fig, ax_stxm_outer, ax_stxm_inner, ptycho_ims = build_combined_figure_reduced()
    else:
        fig, ax_stxm_outer, ax_stxm_inner, ptycho_ims = build_combined_figure()

    if all(v is not None for v in (args.xmin, args.xmax, args.ymin, args.ymax)):
        ax_stxm_outer.set_xlim(-args.xmax, -args.xmin)
        ax_stxm_inner.set_xlim(-args.xmax, -args.xmin)
        ax_stxm_outer.set_ylim(-args.ymax, -args.ymin)
        ax_stxm_inner.set_ylim(-args.ymax, -args.ymin)
        reset_limits = False
    else:
        reset_limits = True

    ani = animation.FuncAnimation(
        fig, animate_combined, interval=200, cache_frame_data=False,
    )
    plt.show()

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from queue import Empty
# import asyncio
import sys
from pathlib import Path
# Add parent directory to path to import nats_async
sys.path.insert(0, str(Path(__file__).parent.parent))
from nats_async import launch_nats_instance
import threading
# import time
from queue import Empty, Queue
import json
import time
# Create nats instance
import matplotlib.tri as tri    
import matplotlib
from argparse import ArgumentParser
# Create nats instance with default host (will use environment variable)


def get_nats_instance(host="localhost:6000"):
    nats_inst = launch_nats_instance(host=host,
                                 subscribe_subjects=("stxm_inner",
                                                     "stxm_outer",
                                                     "stxm_positions",
                                                     "stxm_position_ids",
                                                     "stxm_intensity_ids",
                                                     "stxm_flush"))
    return nats_inst

data_dict = {
    "outer": None,
    "inner": None,
    "positions": None,
    "position_ids": None,
    "intensity_ids": None,
    "total_n": 0,
}


def receive_data():
    global nats_inst, data_dict
    while True:
        try:
            outer = nats_inst.get_rxq("stxm_outer").get(block=False)
            outer = np.array(json.loads(outer.decode()))
            if data_dict["outer"] is None:
                data_dict["outer"] = outer.copy()
            else:
                data_dict["outer"] = np.concatenate([data_dict["outer"], outer.copy()])
        except Empty:
            pass
        try:
            inner = nats_inst.get_rxq("stxm_inner").get(block=False)
            inner = np.array(json.loads(inner.decode()))
            if data_dict["inner"] is None:
                data_dict["inner"] = inner.copy()
            else:
                data_dict["inner"] = np.concatenate([data_dict["inner"], inner.copy()])
        except Empty:
            pass
        try:
            positions = nats_inst.get_rxq("stxm_positions").get(block=False)
            positions = np.array(json.loads(positions.decode()))
            if data_dict["positions"] is None:
                data_dict["positions"] = positions.copy()
            else:
                data_dict["positions"] = np.concatenate([data_dict["positions"], positions.copy()])
        except Empty:
            pass
        try:
            position_ids = nats_inst.get_rxq("stxm_position_ids").get(block=False)
            position_ids = np.array(json.loads(position_ids.decode()))
            if data_dict["position_ids"] is None:
                data_dict["position_ids"] = position_ids.copy()
            else:
                data_dict["position_ids"] = np.concatenate([data_dict["position_ids"], position_ids.copy()])
        except Empty:
            pass
        try:
            intensity_ids = nats_inst.get_rxq("stxm_intensity_ids").get(block=False)    
            intensity_ids = np.array(json.loads(intensity_ids.decode()))
            if data_dict["intensity_ids"] is None:
                data_dict["intensity_ids"] = intensity_ids.copy()
            else:
                data_dict["intensity_ids"] = np.concatenate([data_dict["intensity_ids"], intensity_ids.copy()])
        except Empty:
            pass
        
        try:
            flush = nats_inst.get_rxq("stxm_flush").get(block=False)
            print(f"Got flush: {flush}")
            data_dict["outer"] = None
            data_dict["inner"] = None
            data_dict["positions"] = None
            data_dict["position_ids"] = None
            data_dict["intensity_ids"] = None
            data_dict["total_n"] = 0
        except Empty:
            pass
        
        if data_dict["inner"] is not None and data_dict["outer"] is not None and data_dict["positions"] is not None and data_dict["position_ids"] is not None and data_dict["intensity_ids"] is not None:
            n_inner = data_dict["inner"].shape[0]
            n_outer = data_dict["outer"].shape[0]
            n_positions = data_dict["positions"].shape[0]
            n_intensity_ids = data_dict["intensity_ids"].shape[0]
            n_position_ids = data_dict["position_ids"].shape[0]
            n = min(n_inner, n_outer, n_positions, n_intensity_ids, n_position_ids)
            
            # this is supposed to help but does not, some issue with desyncing when plotting
            # pos_order = np.argsort(data_dict["position_ids"][:n])
            # data_dict["positions"][:n] = data_dict["positions"][:n][pos_order]
            # data_dict["position_ids"][:n] = data_dict["position_ids"][:n][pos_order]
            
            # int_order = np.argsort(data_dict["intensity_ids"][:n])
            # data_dict["inner"][:n] = data_dict["inner"][:n][int_order]
            # data_dict["outer"][:n] = data_dict["outer"][:n][int_order]
            # data_dict["intensity_ids"][:n] = data_dict["intensity_ids"][:n][int_order]
            
            data_dict["total_n"] = n
        time.sleep(0.01)


def generate_figure(show_diagnostics: bool = False):
    plt.style.use('dark_background')
    fontsize = 7
    matplotlib.rcParams.update({'font.size': fontsize})
    if show_diagnostics:
        fig, ((ax1, ax2), (ax3, ax4), (ax5, ax6), (ax7, ax8)) = plt.subplots(4, 2)
    else:
        fig, ((ax1, ax2)) = plt.subplots(2, 1)

    im1 = ax1.imshow(np.zeros((80,80))*np.nan, cmap='gray', vmin=0, vmax=1)
    im2 = ax2.imshow(np.zeros((80,80))*np.nan, cmap='gray', vmin=0, vmax=1)
    
    if show_diagnostics:
        line1 = ax3.plot([], [], color='white', linewidth=1)[0]
        line2 = ax4.plot([], [], color='white', linewidth=1)[0]
        line3 = ax5.plot([], [], color='white', linewidth=1)[0]
        line4 = ax6.plot([], [], color='white', linewidth=1)[0]
        line5 = ax7.plot([], [], color='white', linewidth=1)[0]
        line6 = ax8.plot([], [], color='white', linewidth=1)[0]
        for ax in [ax3, ax4, ax5, ax6, ax7, ax8]:
            ax.set_xlim(0, 1000)
            ax.set_ylim(-2, 2)

    if show_diagnostics:
        return fig, ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, im1, im2, line1, line2, line3, line4, line5, line6
    else:
        return fig, ax1, ax2, im1, im2

def update_tricontour(ax, x, y, intensity_map):
    global reset_limits
    triang = tri.Triangulation(x, y)
    vmin = np.percentile(intensity_map, 3.5)
    vmax = np.percentile(intensity_map, 98.5)
    im = ax.tricontourf(triang, intensity_map, 51, cmap='gray', vmin=vmin, vmax=vmax)
    if reset_limits:
        ax.relim()
        ax.autoscale_view()
        ax.set_xlim(x.min(), x.max())
        ax.set_ylim(y.min(), y.max())
    return im

n_prev = 0
def animate(i):
    global im1, im2,  data_dict, n_prev, show_diagnostics, plot_batch
    if show_diagnostics:
        global  ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, line1, line2, line3, line4, line5, line6
    
    if data_dict["inner"] is None or data_dict["outer"] is None or data_dict["positions"] is None or data_dict["position_ids"] is None or data_dict["intensity_ids"] is None:
        if show_diagnostics:
            return im1, im2, line1, line2, line3, line4, line5, line6
        else:
            return im1, im2
    
    if data_dict["inner"] is not None and data_dict["outer"] is not None and data_dict["positions"] is not None and data_dict["position_ids"] is not None and data_dict["intensity_ids"] is not None:
        n_inner = data_dict["inner"].shape[0]
        n_outer = data_dict["outer"].shape[0]
        n_positions = data_dict["positions"].shape[0]
        n_intensity_ids = data_dict["intensity_ids"].shape[0]
        n_position_ids = data_dict["position_ids"].shape[0]
        n_new = min(n_inner, n_outer, n_positions, n_intensity_ids, n_position_ids)
        if plot_batch is None:
            if n_prev == n_new:
                if show_diagnostics:
                    return im1, im2, line1, line2, line3, line4, line5, line6
                else:
                    return im1, im2
            n_prev = n_new
            n_plot = n_new
        else:
            if n_new > plot_batch:
                n_plot = plot_batch
            else:
                n_plot = n_new

        x, y, z, th = data_dict["positions"][:n_plot].T
        s_inner = data_dict["inner"][:n_plot]
        s_outer = data_dict["outer"][:n_plot]
        pos_ids = data_dict["position_ids"][:n_plot]
        int_ids = data_dict["intensity_ids"][:n_plot]
        
        # clear the data if the full batch is plotted
        if (plot_batch is not None) and (n_new >= plot_batch):
            data_dict["positions"] = data_dict["positions"][plot_batch:]
            data_dict["inner"] = data_dict["inner"][plot_batch:]
            data_dict["outer"] = data_dict["outer"][plot_batch:]
            data_dict["position_ids"] = data_dict["position_ids"][plot_batch:]
            data_dict["intensity_ids"] = data_dict["intensity_ids"][plot_batch:]
            
            if data_dict["positions"].shape[0] == 0:
                data_dict["positions"] = None
            if data_dict["inner"].shape[0] == 0:
                data_dict["inner"] = None
            if data_dict["outer"].shape[0] == 0:
                data_dict["outer"] = None
            if data_dict["position_ids"].shape[0] == 0:
                data_dict["position_ids"] = None
            if data_dict["intensity_ids"].shape[0] == 0:
                data_dict["intensity_ids"] = None

        print(f"{i} - Plotting data for {n_plot} frames")  
    # print(f"Scanning within: {x.min()=}, {x.max()=}, {y.min()=}, {y.max()=}, {z.min()=}, {z.max()=}, {th.min()=}, {th.max()=}")
            
        # x_lab = x
        # y_lab = y

        th_rad = th.copy() * np.pi / 180
        x_lab = (x.copy() * np.cos(th_rad) + z.copy() * np.sin(th_rad)) * -1
        y_lab = y.copy() * -1

        im1 = update_tricontour(ax1, x_lab, y_lab, s_outer)
        im2 = update_tricontour(ax2, x_lab, y_lab, s_inner)
        index = np.arange(len(x))
        if show_diagnostics:
            for data, line, ax in zip([x, y, z, th, pos_ids, int_ids], [line1, line2, line3, line4, line5, line6], [ax3, ax4, ax5, ax6, ax7, ax8]):
            # for data, line, ax in zip([y], [line2], [ax4]):
                line.set_xdata(index)
                line.set_ydata(data)
                ax.relim()
                ax.autoscale_view()
                ax.set_xlim(index.min(), index.max())
                ax.set_ylim(data.min(), data.max())
        if show_diagnostics:
            return im1, im2, line1, line2, line3, line4, line5, line6
        else:
            return im1, im2

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost:6000", help="Config file")
    parser.add_argument("--plot_batch", type=int, default=None, help="Plot batch size")
    parser.add_argument("--show_diagnostics", type=bool, default=False, help="Show diagnostics")
    parser.add_argument("--xmin", type=float, default=None, help="xmin")
    parser.add_argument("--xmax", type=float, default=None, help="xmax")
    parser.add_argument("--ymin", type=float, default=None, help="ymin")
    parser.add_argument("--ymax", type=float, default=None, help="ymax")
    args = parser.parse_args()

    show_diagnostics = args.show_diagnostics
    plot_batch = args.plot_batch

    nats_inst = get_nats_instance(host=args.host)

    t = threading.Thread(target=receive_data)
    t.start()
    
    if show_diagnostics:    
        fig, ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, im1, im2, line1, line2, line3, line4, line5, line6 = generate_figure(show_diagnostics)
    else:
        fig, ax1, ax2, im1, im2 = generate_figure(show_diagnostics)
        
    if args.xmin is not None and args.xmax is not None and args.ymin is not None and args.ymax is not None:
        ax1.set_xlim(-args.xmax, -args.xmin)
        ax2.set_xlim(-args.xmax, -args.xmin)
        ax1.set_ylim(-args.ymax, -args.ymin)
        ax2.set_ylim(-args.ymax, -args.ymin)
        reset_limits = False
    else:
        reset_limits = True

    ani = animation.FuncAnimation(fig, animate, interval=100, blit=True, cache_frame_data=False)
    plt.show()

# print(nats_inst.rxq)



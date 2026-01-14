import numpy as np
import h5py
import sys

import matplotlib.pyplot as plt
import matplotlib.tri as tri    
import matplotlib


file_path = "./processed/1.h5"

batch_size = 4792
batch_number = 1


print("Loading data...")
with h5py.File(file_path, 'r') as f:
    data = f['stxm'][()]
    idx1 = batch_number * batch_size
    idx2 = min((batch_number + 1) * batch_size, data.shape[0])
    x, y, z, th, inner, outer = data[idx1:idx2,:].T

print(f"{x.shape=}, {y.shape=}, {z.shape=}, {th.shape=}, {inner.shape=}, {outer.shape=}")

th_rad = th * np.pi / 180
x_lab = (x*np.cos(th_rad) + z*np.sin(th_rad)) * -1
y_lab = y * -1

plt.style.use('dark_background')
fontsize = 7
matplotlib.rcParams.update({'font.size': fontsize})
fig, (ax1, ax2) = plt.subplots(2, 1)

print("Plotting...")
def plot_stxm_map(x, y, stxm_map, ax):
    triang = tri.Triangulation(x, y)
    vmin = np.percentile(stxm_map, 1.5)
    vmax = np.percentile(stxm_map, 98.5)
    im = ax.tricontourf(triang, stxm_map, 51, cmap='gray', vmin=vmin, vmax=vmax)
    ax.relim()
    ax.autoscale_view()
    ax.set_xlim(x.min(), x.max())
    ax.set_ylim(y.min(), y.max())


plot_stxm_map(x, y, inner, ax1)
plot_stxm_map(x, y, outer, ax2)
plt.show()




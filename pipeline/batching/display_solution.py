#!/usr/bin/python

import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import hsv_to_rgb
import h5py
import argparse
from concurrent import futures
import multiprocessing

def read_data(fname, dname, dims):
    """Read HDF5 dataset.

    Arguments:
    `fname` File name
    `dname` Dataset name
    `dims`  number of result dimensions

    Returns:
    numpy array with `dims` dimensions, the leading ones are set to 1 if the dataset has fewer than `dims` dimensions.
    """
    with h5py.File(fname, 'r') as f:
        res = np.array(f[dname])
    data_shape = res.shape
    res_shape = [1,] * dims
    res_shape[-len(data_shape):] = data_shape
    return res.reshape(res_shape)

def display_probes(prb):
    """Display solution probes.

    Arguments:
    `prb`       futures list with probes

    Returns:
    Dimensions (pnr, pnc) of last probe.
    """
    for p in range(len(prb)):
        data = prb[p].result()
        shape = data.shape
        pnr, pnc = shape[-2:]
        for m in range(shape[0]):
            prb_p = data[m,:,:]
            prb_phase = (np.angle(prb_p) + np.pi) / (2*np.pi)
            prb_amp = np.absolute(prb_p)
            prb_amp /= prb_amp.max()
            prb_hsv = np.ndarray((pnr, pnc, 3))
            prb_hsv[:,:,0] = prb_phase
            prb_hsv[:,:,1] = 1.0
            prb_hsv[:,:,2] = prb_amp
            plt.title(f'prb {p}m{m}')
            plt.imshow(hsv_to_rgb(prb_hsv))
            plt.show()
            plt.clf()
    return (pnr, pnc)   # dimensions of last probe

def display_objects(obj, pnr, pnc, args):
    """Display solution objects.

    Arguments:
    `obj`           futures list with objects
    `pnr`, `pnc`    number of rows and columns of some probe
    `args`          argparse arguments
    """
    for o in range(len(obj)):
        data = obj[o].result()
        if args.smul:
            data = np.apply_along_axis(np.prod, 0, data)
            data.resize((1,) + data.shape)
        dr = int(pnr/2)
        dc = int(pnc/2)
        shape = data.shape
        onr, onc = shape[-2:]
        for s in range(shape[0]):
            for m in range(shape[1]):
                obj_o = data[s,m,:,:]
                obj_phase = (np.angle(obj_o) + np.pi) / (2*np.pi)
                plt.title(f'obj {o}s{s}m{m} phs')
                plt.imshow(obj_phase, cmap=plt.cm.bone)
                oprr = obj_phase[dr:onr-dr,dc:onc-dc]
                plt.clim(np.min(oprr), np.max(oprr))
                plt.show()
                plt.clf()
                obj_amp = np.absolute(obj_o)
                obj_amp /= obj_amp.max()
                plt.title(f'obj {o}s{s}m{m} amp (max={obj_amp.max()})')
                plt.imshow(obj_amp, cmap=plt.cm.bone)
                oprr = obj_amp[dr:onr-dr,dc:onc-dc]
                plt.clim(np.min(oprr), np.max(oprr))
                plt.show()
                plt.clf()

def prepare_plot():
    """Setup global matplotlib figure for later functions.
    """
    figsz = (8, 8)
    fig = plt.figure(figsize=figsz)
    ax = plt.gca()
    ax.xaxis.set_major_locator(plt.NullLocator())
    ax.yaxis.set_major_locator(plt.NullLocator())

def parse_args():
    """Parse commandline arguments.

    Returns:
    argparse argument object
    """
    parser = argparse.ArgumentParser(description='Display solution file')
    parser.add_argument('--smul', dest='smul', action='store_true', help='multiply slices together')
    parser.add_argument('--grp', dest='group', help='solution file result group')
    parser.add_argument('sfile', help='solution file name')
    return parser.parse_args()

def main():
    """Display probes and objects in a solution file.
    """
    args = parse_args()
    prepare_plot()
    # collect dataset names
    with h5py.File(args.sfile, 'r') as f:
        probe_names = [d.name for d in (f[f'{args.group}/probes'] if args.group else f['probes']).values()]
        object_names = [d.name for d in (f[f'{args.group}/objects'] if args.group else f['objects']).values()]
    # collect np.array futures
    with futures.ProcessPoolExecutor(min(len(probe_names) + len(object_names), 2 * multiprocessing.cpu_count())) as executor:
        prb = [executor.submit(read_data, args.sfile, dataset, 3) for dataset in probe_names]
        obj = [executor.submit(read_data, args.sfile, dataset, 4) for dataset in object_names]
        (pnr, pnc) = display_probes(prb) # dimensions of last probe
        display_objects(obj, pnr, pnc, args)

if __name__ == '__main__':
    main()

#!/usr/bin/python

import h5py
import numpy as np
import numpy.fft as fft
import matplotlib.pyplot as plt
from matplotlib.colors import hsv_to_rgb
import math
import sys

def display(title, a):
    x = a.shape[0]
    y = a.shape[1]
    phase = (np.angle(a) + math.pi) / (2 * math.pi)
    amplitude = np.absolute(a)
    amplitude /= amplitude.max()
    hsv = np.ndarray((x, y, 3))
    hsv[:,:,0] = phase
    hsv[:,:,1] = 1.
    hsv[:,:,2] = amplitude
    plt.title(title)
    plt.imshow(hsv_to_rgb(hsv))
    plt.show()

class Probe:
    def __init__(self, x, y):
        self.n = 6              # how many waves per normalized distance
        self.x = x              # #pixels in x direction
        self.y = y              # #pixels in y direction
        self.cx = self.x / 2.   # center x
        self.cy = self.y / 2.   # center y
        self.data = np.zeros((self.x, self.y), dtype=complex)

    def generate_probe(self):
        for x in range(0, self.x):
            for y in range(0, self.y):
                d2 = (x - self.cx)**2 + (y - self.cy)**2         # distance^2 to center
                nf = self.cx**2 + self.cy**2                    # normalization factor
                self.data[x, y] = math.cos(d2/nf * self.n * math.pi *.5) / math.exp((self.n * d2/nf)**2) + (1 - abs(2*x/p.x -1)) * 1.j

    def display(self):
        display('Probe', self.data)

    def __str__(self):
        s = 'Probe:'
        for x in range(0, self.x):
            for y in range(0, self.y):
                s += str(self.data[x, y]) + ' '
            s += '\n'
        return s

class Object:
    def __init__(self, x, y):
        self.x = x              # #pixels in x direction
        self.y = y              # #pixels in y direction
        self.cx = self.x / 2.   # center x
        self.cy = self.y / 2.   # center y
        self.data = np.ones((self.x, self.y), dtype=complex) * .5

    def ring(self, cx, cy, ro, ri, v, add=False):
        for x in range(0, self.x):
            for y in range(0, self.y):
                d2 = (x - cx)**2 + (y - cy)**2         # distance^2 to center
                if d2 < ro**2 and d2 >= ri**2:
                    self.data[x, y] = (self.data[x, y] + v) if add else v

    def display(self):
        display('Object', self.data)

    def __str__(self):
        s = 'Object:'
        for x in range(0, self.x):
            for y in range(0, self.y):
                s += str(self.data[x, y]) + ' '
            s += '\n'
        return s

def points(p, o, spacing):
    positions = []
    for x in range(0, o.x - p.x + 1, spacing):
        for y in range(0,  o.y - p.y + 1,  spacing):
            positions.append((x,  y))
    return positions

showPic = (len(sys.argv) > 1)

f = h5py.File('/tmp/initial_conditions.h5', 'w')
pg = f.create_group('probes')
p = Probe(16, 16)
p.generate_probe()
pg.create_dataset('probe_0', data=p.data, shuffle=True, compression='gzip', compression_opts=5)
if showPic:
    p.display()
    plt.clf()
#print(p)


og = f.create_group('objects')
o = Object(32, 32)
og.create_dataset('object_0', data=o.data, shuffle=True, compression='gzip', compression_opts=5)
o.ring(7, 7, 6, 4, 0.)
o.ring(20, 20, 12, 9, 1.j)
o.ring(15, 15, 12, 10, 1.+1.j)
if showPic:
    o.display()
#print(o)
f.close()

positions = points(p, o, 2)
if showPic:
    print(positions)
    plt.scatter([x[0] for x in positions], [y[1] for y in positions], color="red", marker="o")
    plt.show()
    plt.clf()

f = h5py.File('/tmp/measurements.h5', 'w')
dg = f.create_group('detectors')
d0 = dg.create_group('detector_0')
d0.attrs.create('rows', p.x)
d0.attrs.create('columns', p.y)
m0 = d0.create_dataset('modules', (1, 4), data=[p.x, p.y, 0, 0])

f.create_dataset('probes', (1, 2), data=[p.x, p.y])
f.create_dataset('objects', (1, 2), data=[o.x, o.y])

mg = f.create_group('measurements')
i = 0
for pos in positions:
    (x, y) = pos
    view = abs(fft.fft2(p.data * o.data[x:x+p.x, y:y+p.y]))
    mig = mg.create_group('measurement_%d' % i)
    mig.attrs.create('position', [x, y])
    mig.attrs.create('detector', 0)
    mig.attrs.create('probe', 0)
    mig.attrs.create('object', 0)
    mig.create_dataset('diff_pat', data=view, shuffle=True, compression='gzip', compression_opts=5)
    i += 1
    #display('Measurement (%d, %d)' % pos, view)
f.close()

f = h5py.File('/tmp/solution.h5', 'w')
pg = f.create_group('probes')
pg.create_dataset('probe_0', data=p.data, shuffle=True, compression='gzip', compression_opts=5)
og = f.create_group('objects')
og.create_dataset('object_0', data=o.data, shuffle=True, compression='gzip', compression_opts=5)
f.close()

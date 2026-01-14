# Difference map

import numpy as np
import numpy.fft as fft
from view_vec import view_vec

eps = np.finfo(float).eps

class fourier_projection:
    def __init__(self, measurements, relaxation):
        self.m = measurements
        self.r = relaxation

    def __call__(self, v, feedback):
        global eps
        # print('pfft')
        ressq = .0      # residuals squared
        res = []
        for i in range(len(v)):
            vf = fft.fft2(v[i])
            if feedback:
                d = (abs(vf) - self.m[i].p).flatten()
                ressq += np.dot(d, d)
            vf = vf * self.r + vf * (1 - self.r) * self.m[i].p / (abs(vf) + eps)
            res.append(fft.ifft2(vf))
        if feedback:
            print('sum(sqr(residuals)) =', 2. * ressq)
        return view_vec(res)

class overlap_projection:
    def __init__(self, obj, prb, measurements):
        self.o = obj
        self.p = prb
        self.px = prb.shape[0]
        self.py = prb.shape[1]
        self.m = measurements

    def __call__(self, v, idx):
        global eps
        # print('po')
        on = np.zeros_like(self.o)
        od = np.zeros_like(self.o)
        for i in range(len(v)):
            (x, y) = (self.m[i].x, self.m[i].y)
            on[x:x+self.px,y:y+self.py] += self.p.conjugate() * v[i]
            od[x:x+self.px,y:y+self.py] += abs(self.p)**2

        self.o = on / (od + eps)
        # keep the probe constant for some iterations
        if idx > 5:
            pn = np.zeros_like(self.p)
            pd = np.zeros_like(self.p)
            for i in range(len(v)):
                (x, y) = (self.m[i].x, self.m[i].y)
                pn += self.o[x:x+self.px,y:y+self.py].conjugate() * v[i]
                pd += abs(self.o[x:x+self.px,y:y+self.py])**2
            self.p = pn / (pd + eps)
        res = []
        for i in range(len(v)):
            (x, y) = (self.m[i].x, self.m[i].y)
            res.append(self.p * self.o[x:x+self.px,y:y+self.py])
        return view_vec(res)

class difference_map:
    def __init__(self, pfft, po):
        self.pfft = pfft
        self.po = po

    def iter(self, iv, feedback, idx):
        av = self.pfft(iv, feedback)
        return self.po(av, idx)

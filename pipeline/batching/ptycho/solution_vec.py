# Solution vector

import numpy as np
from math import sqrt

class solution_vec:
    def __init__(self, obj, prb):
        self.o = obj
        self.p = prb

    def __neg__(self):
        return solution_vec(-self.o, -self.p)

    def __add__(self, v):
        return solution_vec(self.o + v.o, self.p + v.p)

    def __sub__(self, v):
        return solution_vec(self.o - v.o, self.p - v.p)

    def __mul__(self, f):
        if f.__class__ == self.__class__:
            return self.dot(f)
        return solution_vec(self.o * f, self.p * f)

    def __rmul__(self, f):
        return solution_vec(f * self.o, f * self.p)

    def __div__(self, f):
        return solution_vec(self.o / f, self.p / f)

    def __abs__(self):
        return sqrt(self.dot(self))

    def dot(self, v):
        return np.dot(np.real(self.o.flat), np.real(v.o.flat)) + \
               np.dot(np.imag(self.o.flat), np.imag(v.o.flat)) + \
               np.dot(np.real(self.p.flat), np.real(v.p.flat)) + \
               np.dot(np.imag(self.p.flat), np.imag(v.p.flat))

    def zeros_like(self):
        return solution_vec(np.zeros_like(self.o), np.zeros_like(self.p))

    def assign(self, v):
        self.o = v.o
        self.p = v.p

#!/usr/bin/python

import sys
import numpy as np
from hdf5_io import hdf5_io
from raw_io import raw_io
from view_vec import view_vec
from solution_vec import solution_vec
from diffmap import fourier_projection, overlap_projection, difference_map
from max_likelihood import max_likelihood, line_search_bisect, line_search_wolfe, conjugate_gradient_prp
from measurement import measurement_to_numpy, numpy_to_measurement
import argparse

from holoscan.core import (
    ExecutionContext,
    InputContext,
    Operator,
    OperatorSpec,
    OutputContext,
)

# holoscan operator for ptychographic reconstruction
class PtychoOp(Operator):
    def __init__(self, spec: OperatorSpec, prb, relaxation):
        super().__init__(spec)
        self.prb = prb
        self.relaxation = relaxation


    def setup(self, spec: OperatorSpec):
        # setup the operator
        spec.input("frame_pos_size", "Dictionary with Frame, Positions, and Object Size")
        spec.output("solution_vector", "Reconstruction Ouput")

    def compute(self, op_input: InputContext, op_output: OutputContext, context: ExecutionContext):
        input_dict = op_input.receive("in")
        frames = input_dict["frames"]
        positions = input_dict["positions"]
        objsize = input_dict["obj_size"]

        # prepare the object array
        obj = np.random.rand(objsize) + 1j*np.random.rand(objsize)
        obj = obj / np.abs(obj)

        # compute the operator
        sv = ptycho_reconstruct(self.prb, obj, self.relaxation, frames, positions)
        op_output.emit(sv, "solution_vector")

    def shutdown(self):
        # shutdown the operator
        pass

class WriteSolutionOp(Operator):
    def __init__(self, spec: OperatorSpec, solution_file):
        super().__init__(spec)
        self.solution_file = solution_file
        self.io = hdf5_io()

    def setup(self, spec: OperatorSpec):
        # setup the operator
        spec.input("solution_vector", "Reconstruction Ouput")

    def compute(self, op_input: InputContext, op_output: OutputContext, context: ExecutionContext):
        sv = op_input.receive("solution_vector")
        # write the solution to disk
        self.io.write_solution(self.solution_file, sv.p, sv.o)



def ptycho_reconstruct(prb, obj, relaxation, frames, positions):
    # prb is the probe
    # obj is the object
    # measurements is a list of measurement objects
    # return the solution vector

    measurements = numpy_to_measurement(positions, frames)

    px = prb.shape[0]
    py = prb.shape[1]

    vl = []
    for m in measurements:
        # convert m to int (from np.uint64)
        m.x = int(m.x)
        m.y = int(m.y)
        vl.append(prb * obj[m.x:m.x+px,m.y:m.y+py])

    iv = view_vec(vl)

    pfft = fourier_projection(measurements, relaxation)
    po = overlap_projection(obj, prb, measurements)
    diffmap = difference_map(pfft, po)

    fi = args.feedback_interval
    fb = (fi == 1)
    for n in range(args.dm_iter):
        if fb:
            print(n)
            fi = args.feedback_interval
        elif fi > 1:
            fi -= 1
        iv = diffmap.iter(iv, fb, n)
        fb = (fi == 1)

    del diffmap
    del pfft
    sv = solution_vec(po.o, po.p)
    del po

    mlh = max_likelihood(measurements, iv)
    if args.line_search == 'bisect':
        ls = line_search_bisect(mlh)
    elif args.line_search == 'wolfe':
        ls = line_search_wolfe(mlh)
    cgm = conjugate_gradient_prp(mlh, ls)

    for n in range(args.mlh_iter):
        (stop, sv) = cgm.step(sv)
        if stop:
            break

    print('mlh #fval calls: %d' % mlh.nfval)
    print('mlh #ngrad calls: %d' % mlh.ngrad)
    return sv

def parse_args():
    # parse command line arguments
    # return the arguments
    # args are in the form of a dictionary

    import argparse

    ############################################################################
    # Command line argument parser
    ############################################################################

    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='simple ptychographic reconstruction'
        # epilog='Text at the bottom of help'
    )

    parser.add_argument('-r', '--relaxation', default=0.1, type=float, help='fourier projrction relaxation parameter')
    parser.add_argument('-d', '--dm_iter', default=0, type=int, help='difference map iteration count')
    parser.add_argument('-m', '--mlh_iter', default=0, type=int, help='maximum likelihood iteration count')
    parser.add_argument('-f', '--feedback_interval', default=0, type=int, help='feedback interval count')
    parser.add_argument('-l', '--line_search', default='bisect', help='line search method')
    parser.add_argument('measurement_file', help='measurement file or "raw"')
    parser.add_argument('initial_conditions_file', help='initial conditions or raw data directory')
    parser.add_argument('solution_file', help='solution file')
    parser.add_argument("--random_object", action="store_true", help="use random object")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    if not args.line_search in [ 'wolfe', 'bisect']:
        print('unknown line search, use "wolfe" or "bisect"')
        sys.exit(1)

    if args.measurement_file == 'raw':
        io = raw_io()
    else:
        io = hdf5_io()

    (prb, obj_read, measurements) = io.read_input(args)

    # expected input: prb (read from disk), obj (H x W), measurements

    # convert measurment list to 2 numpy arrays
    positions, frames = measurement_to_numpy(measurements)

    # initialize a complex solution vector with random noise that equals 1 in magnitude
    # and has a random phase
    if args.random_object:
        obj = np.random.rand(obj_read.shape[0], obj_read.shape[1]) + 1j*np.random.rand(obj_read.shape[0], obj_read.shape[1])
        obj = obj / np.abs(obj)

    else:
        obj = obj_read

    sv = ptycho_reconstruct(prb, obj, args.relaxation, frames, positions)

    io.write_solution(args, sv.p, sv.o)

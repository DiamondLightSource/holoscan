import sys
import numpy as np
import os.path as fp
from measurement import measurement

class raw_io:
    def __init__(self):
        self.pos = []

    def parse_conf(self, input_conf):
        ic = open(input_conf, 'r')
        while ic.readable():
            line = ic.readline().strip()
            components = line.split()
            key = components[0]
            if key == 'positions':
                break
            value = components[1]
            if key == 'probe':
                self.probe_file = value
            elif key == 'object':
                self.object_file = value
            elif key == 'data':
                self.data_file = value
            elif key == 'npoints':
                self.npoints = int(value)
            elif key == 'probe_cols':
                self.probe_cols = int(value)
            elif key == 'probe_rows':
                self.probe_rows = int(value)
            elif key == 'object_cols':
                self.object_cols = int(value)
            elif key == 'object_rows':
                self.object_rows = int(value)
            elif key == 'iterations':
                self.iterations = int(value)
            else:
                print('Error: unknown parameter', key)
                sys.exit(1)
        for p in range(self.npoints):
            row_col = ic.readline().strip().split()
            self.pos.append((int(row_col[0]), int(row_col[1])))
        ic.close()

    def read_input(self, args):
        input_dir = args.initial_conditions_file
        self.parse_conf(fp.join(input_dir, 'input.conf'))
        prb = np.fromfile(fp.join(input_dir, self.probe_file), dtype='complex128')
        obj = np.fromfile(fp.join(input_dir, self.object_file), dtype='complex128')
        measurements = []
        for p in range(self.npoints):
            mf = fp.join(input_dir, "%s_%d" % (self.data_file, p))
            m = measurement(np.fromfile(mf, dtype='float64').reshape(self.probe_rows, self.probe_cols), self.pos[p][1], self.pos[p][0])
            measurements.append(m)
        return (prb.reshape(self.probe_rows, self.probe_cols),
                obj.reshape(self.object_rows, self.object_cols),
                measurements)

    def write_solution(self, args, prb, obj):
        input_dir = args.solution_file
        prb.tofile(fp.join(input_dir, 'probe_out'))
        obj.tofile(fp.join(input_dir, 'object_out'))
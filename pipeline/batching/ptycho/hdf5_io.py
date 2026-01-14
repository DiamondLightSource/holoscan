import sys
import numpy as np
import h5py
from measurement import measurement

class hdf5_io:
    def read_input(self, args):
        icf = h5py.File(args.initial_conditions_file, 'r')
        prb = np.array(icf['probes']['probe_0'])
        obj = np.array(icf['objects']['object_0'])
        icf.close()

        measurements = []
        mf = h5py.File(args.measurement_file, 'r')
        if 'measurements' in mf:
            for k in mf['measurements']:
                measurements.append(measurement(mf['measurements'][k]))
        else:
            data = np.array(mf['measurement/n0/data'])
            pos = np.array(mf['measurement/n0/positions'])
            for k in range(data.shape[0]):
                measurements.append(measurement(data[k,:,:], pos[k,0], pos[k,1]))
        mf.close()

        return (prb, obj, measurements)

    def write_solution(self, solution_file, prb, obj):
        sf = h5py.File(solution_file, 'w')
        pg = sf.create_group('probes')
        pg.create_dataset('probe_0', data=prb, shuffle=True, compression='gzip', compression_opts=5)
        og = sf.create_group('objects')
        og.create_dataset('object_0', data=obj, shuffle=True, compression='gzip', compression_opts=5)
        sf.close()

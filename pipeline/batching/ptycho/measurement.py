# Measurement

import numpy as np

class measurement:
    def __init__(self, data, row=None, col=None):
        if not row is None:
            # data is raw data
            self.x = row
            self.y = col
            self.p = data
        else :
            # data is a hdf5 group
            pos = data.attrs['position']
            self.x = pos[0]
            self.y = pos[1]
            self.p = np.array(data['diff_pat'])

    def __getitem__(self, i):
        return self.p.flat[i]

    def __getitem__(self, i, j):
        return self.p[i, j]

    def flat(self):
        return self.p.flat



def measurement_to_numpy(measurements):
    # convert measurment list to 2 numpy arrays
    positions_npy_xy = np.zeros((len(measurements),2), dtype=np.int32)
    frame_npy = np.zeros((len(measurements), measurements[0].p.shape[0], measurements[0].p.shape[1]), dtype=np.float32)
    for (i,m) in enumerate(measurements):
        positions_npy_xy[i,0] = m.x
        positions_npy_xy[i,1] = m.y
        frame_npy[i,:,:] = m.p

    return positions_npy_xy, frame_npy


def numpy_to_measurement(positions, frames):
    # convert 2 numpy arrays to measurment list
    measurements = []
    for i in range(len(frames)):
        measurements.append(measurement(frames[i], positions[i,0], positions[i,1]))
    return measurements

import h5py
import sys
import hdf5plugin

import numpy as np
import cupy as cp

CENTER_X = 100
CENTER_Y = 100
RADIUS = 23
NBINS = 8

kernel2d = cp.RawKernel('''
#define uint16_t unsigned short
#define uint8_t unsigned char
#define uint32_t unsigned int
#define UINT16_MAX 65535

extern "C" __global__ void integration(const uint16_t *image, const uint8_t *map,
                            uint32_t *bin_sum,
                             uint32_t *bin_count,
                             uint32_t npixel, uint32_t nbins) {

    extern __shared__ uint32_t shared_mem[];
    uint32_t* shared_sum = shared_mem;           // shared buffer [nbins]
    uint32_t* shared_count = &shared_sum[nbins]; // shared buffer [nbins]

    uint32_t idx = blockDim.x*blockIdx.x + threadIdx.x;

    for (uint32_t i = threadIdx.x; i < 2 * nbins; i += blockDim.x)
        shared_mem[i] = 0;
    __syncthreads();

    for (uint32_t i = idx; i < npixel; i += blockDim.x * gridDim.x) {
        uint16_t bin = map[i];
        uint16_t value = image[i];
        if (value < UINT16_MAX) {
            atomicAdd(&shared_sum[bin], value);
            atomicAdd(&shared_count[bin], 1);
        }
    }
    __syncthreads();

    for (uint32_t i = threadIdx.x; i < nbins; i += blockDim.x) {
        atomicAdd(&bin_sum[i], shared_sum[i]);
        atomicAdd(&bin_count[i], shared_count[i]);
    }
}''','integration')

kernel2d.max_dynamic_shared_size_bytes = 2 * NBINS * 4 * 32



kernel3d = cp.RawKernel('''
#define uint16_t unsigned short
#define uint8_t unsigned char
#define uint32_t unsigned int
#define UINT16_MAX 65535

extern "C" __global__ void integration(const uint16_t *images, const uint8_t *map,
                            uint32_t *bin_sum,
                             uint32_t *bin_count,
                             uint32_t npixel, uint32_t nbins, uint32_t nframes) {

    extern __shared__ uint32_t shared_mem[];
    uint32_t* shared_sum = shared_mem;           // shared buffer [nbins]
    uint32_t* shared_count = &shared_sum[nbins]; // shared buffer [nbins]

    uint32_t idx = blockDim.x*blockIdx.x + threadIdx.x;

    // Process each frame separately
    for (uint32_t frame = 0; frame < nframes; frame++) {
        // Clear shared memory for this frame
        for (uint32_t i = threadIdx.x; i < 2 * nbins; i += blockDim.x)
            shared_mem[i] = 0;
        __syncthreads();

        // Process pixels for this frame
        for (uint32_t i = idx; i < npixel; i += blockDim.x * gridDim.x) {
            uint16_t bin = map[i];
            uint16_t value = images[frame * npixel + i];  // 2D array indexing
            if (value < UINT16_MAX) {
                atomicAdd(&shared_sum[bin], value);
                atomicAdd(&shared_count[bin], 1);
            }
        }
        __syncthreads();

        // Write results for this frame
        for (uint32_t i = threadIdx.x; i < nbins; i += blockDim.x) {
            atomicAdd(&bin_sum[frame * nbins + i], shared_sum[i]);
            atomicAdd(&bin_count[frame * nbins + i], shared_count[i]);
        }
        __syncthreads();
    }
}''','integration')
kernel3d.max_dynamic_shared_size_bytes = 2 * NBINS * 4 * 32


def create_circular_mask(size, radius, center_x, center_y):
    height, width = size
    y, x = np.ogrid[:height, :width]  # Create a grid of coordinates
    distance_from_center = np.sqrt((x - center_x) ** 2 + (y - center_y) ** 2)  # Calculate Euclidean distances
    return cp.array(distance_from_center <= radius)  # Create a mask for points within the radius

# Calculate circular mask with a given image size, beam center, and radius
def create_circular_mask_sliced(size, radius, center_x, center_y):
    mask = create_circular_mask(size, radius, center_x, center_y)
    mask = mask.astype(np.uint8)  # Convert boolean mask to integers (0 and 1)

    mask[:center_y, :center_x] += 0
    mask[:center_y, center_x:] += 2

    mask[center_y:, :center_x] += 4
    mask[center_y:, center_x:] += 6

    return cp.asarray(mask.flatten())

def compute_circle_sums_cupy_naive(frames, mask):
    # inner_circle = cp.sum(frames[:, mask], axis=1)
    # outer_circle = cp.sum(frames[:, ~mask], axis=1)
    mask_3d = mask[None, :, :]
    inner_circle = cp.sum(frames * mask_3d * ~(frames == 2**16 - 1), axis=(1,2))
    outer_circle = cp.sum(frames * ~mask_3d * ~(frames == 2**16 - 1), axis=(1,2))
    return inner_circle, outer_circle

def compute_circle_sums_numpy_naive(frames, mask):
    mask_3d = mask[None, :, :]
    inner_circle = np.sum(frames * mask_3d * ~(frames == 2**16 - 1), axis=(1,2))
    outer_circle = np.sum(frames * ~mask_3d * ~(frames == 2**16 - 1), axis=(1,2))
    return inner_circle, outer_circle


def compute_intensity_map_grid(positions, intensity_inner, intensity_outer, 
                               x_range=(-35, 35), y_range=(-15, 15), pixel_size=0.5):
    """
    Compute intensity maps on a regular grid by binning position data.
    
    This function creates 2D intensity maps by assigning each (position, intensity) 
    pair to a pixel in a regular grid and averaging intensities for pixels with 
    multiple measurements.
    
    Args:
        positions: CuPy array of shape (N, 2) or (N, 4) with x, y positions
        intensity_inner: CuPy array of shape (N,) with inner circle intensities
        intensity_outer: CuPy array of shape (N,) with outer circle intensities
        x_range: Tuple of (x_min, x_max) for grid bounds
        y_range: Tuple of (y_min, y_max) for grid bounds
        pixel_size: Size of each pixel in the grid
        
    Returns:
        Dictionary with:
            - positions_grid: Grid coordinates array
            - inner_intensity_map: 2D array of averaged inner intensities
            - outer_intensity_map: 2D array of averaged outer intensities
    """
    x_min, x_max = x_range
    y_min, y_max = y_range
    
    batch_size = positions.shape[0]
    
    # Create position grid
    positions_grid = cp.mgrid[y_min:y_max:pixel_size, x_min:x_max:pixel_size]
    
    # Calculate number of pixels
    nx = int((x_max - x_min) / pixel_size)
    ny = int((y_max - y_min) / pixel_size)
    
    # Initialize accumulation arrays
    sum_inner = cp.zeros((nx, ny), dtype=cp.float64)
    sum_outer = cp.zeros((nx, ny), dtype=cp.float64)
    count_array = cp.zeros((nx, ny), dtype=cp.int32)
    
    # Bin positions and accumulate intensities
    for i in range(batch_size):
        # Calculate pixel indices
        x_idx = int((positions[i, 0] - x_min) / pixel_size)
        y_idx = int((positions[i, 1] - y_min) / pixel_size)
        
        # Skip positions outside the defined range
        if (x_idx < 0 or x_idx >= nx or y_idx < 0 or y_idx >= ny):
            continue
        
        sum_inner[x_idx, y_idx] += intensity_inner[i]
        sum_outer[x_idx, y_idx] += intensity_outer[i]
        count_array[x_idx, y_idx] += 1
    
    # Calculate mean arrays
    mean_inner = cp.zeros_like(sum_inner)
    mean_outer = cp.zeros_like(sum_outer)
    valid_pixels = count_array > 0
    mean_inner[valid_pixels] = sum_inner[valid_pixels] / count_array[valid_pixels]
    mean_outer[valid_pixels] = sum_outer[valid_pixels] / count_array[valid_pixels]
    
    return {
        "positions_grid": positions_grid.T,
        "inner_intensity_map": mean_inner.T,
        "outer_intensity_map": mean_outer.T,
    }


if __name__ == "__main__":
    mask = create_circular_mask_sliced((192, 192), 23, 100, 100)

    # Open the HDF5 file
    with h5py.File(sys.argv[1], 'r') as h5_file:  # Replace 'your_file.h5' with the actual file name
        # Navigate to the dataset
        dataset = h5_file['/entry/data/data']  # Access the 3D dataset

        # Get the shape of the image
        nimages, _, height, width = dataset.shape

        for i in range(500000):  # Assuming shape is (slow, height, width)
            frame = cp.asarray(dataset[i, 0, :, :]).flatten()

            bin_sum = cp.zeros(NBINS, dtype=cp.uint32)
            bin_count = cp.zeros(NBINS, dtype=cp.uint32)

            kernel2d((40,), (32,), (frame, mask, bin_sum, bin_count, len(frame), NBINS ))

            print(f"{i} {bin_sum[0]} {bin_sum[1]} {bin_sum[2]} {bin_sum[3]} {bin_sum[4]} {bin_sum[5]} {bin_sum[6]} {bin_sum[7]}")

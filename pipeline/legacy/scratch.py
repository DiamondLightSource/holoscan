# import cupy as cp
# import numpy as np


# CENTER_X = 100
# CENTER_Y = 100
# RADIUS = 23
# NBINS = 8

# kernel2d = cp.RawKernel('''
# #define uint16_t unsigned short
# #define uint8_t unsigned char
# #define uint32_t unsigned int
# #define UINT16_MAX 65535

# extern "C" __global__ void integration(const uint16_t *image, const uint8_t *map,
#                             uint32_t *bin_sum,
#                              uint32_t *bin_count,
#                              uint32_t npixel, uint32_t nbins) {

#     extern __shared__ uint32_t shared_mem[];
#     uint32_t* shared_sum = shared_mem;           // shared buffer [nbins]
#     uint32_t* shared_count = &shared_sum[nbins]; // shared buffer [nbins]

#     uint32_t idx = blockDim.x*blockIdx.x + threadIdx.x;

#     for (uint32_t i = threadIdx.x; i < 2 * nbins; i += blockDim.x)
#         shared_mem[i] = 0;
#     __syncthreads();

#     for (uint32_t i = idx; i < npixel; i += blockDim.x * gridDim.x) {
#         uint16_t bin = map[i];
#         uint16_t value = image[i];
#         if (value < UINT16_MAX) {
#             atomicAdd(&shared_sum[bin], value);
#             atomicAdd(&shared_count[bin], 1);
#         }
#     }
#     __syncthreads();

#     for (uint32_t i = threadIdx.x; i < nbins; i += blockDim.x) {
#         atomicAdd(&bin_sum[i], shared_sum[i]);
#         atomicAdd(&bin_count[i], shared_count[i]);
#     }
# }''','integration')

# kernel2d.max_dynamic_shared_size_bytes = 2 * NBINS * 4 * 32

# kernel3d = cp.RawKernel('''
# #define uint16_t unsigned short
# #define uint8_t unsigned char
# #define uint32_t unsigned int
# #define UINT16_MAX 65535

# extern "C" __global__ void integration(const uint16_t *images, const uint8_t *map,
#                             uint32_t *bin_sum,
#                              uint32_t *bin_count,
#                              uint32_t npixel, uint32_t nbins, uint32_t nframes) {

#     extern __shared__ uint32_t shared_mem[];
#     uint32_t* shared_sum = shared_mem;           // shared buffer [nbins]
#     uint32_t* shared_count = &shared_sum[nbins]; // shared buffer [nbins]

#     uint32_t idx = blockDim.x*blockIdx.x + threadIdx.x;

#     // Process each frame separately
#     for (uint32_t frame = 0; frame < nframes; frame++) {
#         // Clear shared memory for this frame
#         for (uint32_t i = threadIdx.x; i < 2 * nbins; i += blockDim.x)
#             shared_mem[i] = 0;
#         __syncthreads();

#         // Process pixels for this frame
#         for (uint32_t i = idx; i < npixel; i += blockDim.x * gridDim.x) {
#             uint16_t bin = map[i];
#             uint16_t value = images[frame * npixel + i];  // 2D array indexing
#             if (value < UINT16_MAX) {
#                 atomicAdd(&shared_sum[bin], value);
#                 atomicAdd(&shared_count[bin], 1);
#             }
#         }
#         __syncthreads();

#         // Write results for this frame
#         for (uint32_t i = threadIdx.x; i < nbins; i += blockDim.x) {
#             atomicAdd(&bin_sum[frame * nbins + i], shared_sum[i]);
#             atomicAdd(&bin_count[frame * nbins + i], shared_count[i]);
#         }
#         __syncthreads();
#     }
# }''','integration')
# kernel3d.max_dynamic_shared_size_bytes = 2 * NBINS * 4 * 32


# def create_circular_mask(size, radius, center_x, center_y):
#     height, width = size
#     y, x = cp.ogrid[:height, :width]  # Create a grid of coordinates
#     distance_from_center = cp.sqrt((x - center_x) ** 2 + (y - center_y) ** 2)  # Calculate Euclidean distances
#     mask = distance_from_center <= radius  # Create a mask for points within the radius
#     mask = mask.astype(cp.uint8)  # Convert boolean mask to integers (0 and 1)

#     mask[:center_y, :center_x] += 0
#     mask[:center_y, center_x:] += 2

#     mask[center_y:, :center_x] += 4
#     mask[center_y:, center_x:] += 6

#     return cp.asarray(mask.flatten())


# # N_FRAMES = 100
# N_FRAMES = 4792
# cp.random.seed(0)
# frames = cp.random.randint(0, 1000, (N_FRAMES, 192, 192), dtype=cp.uint16)
# mask = create_circular_mask((192, 192), RADIUS, CENTER_X, CENTER_Y)

# # Add debug prints at the start
# print("\nInput verification:")
# print(f"frames shape: {frames.shape}")
# print(f"frames dtype: {frames.dtype}")
# print(f"mask shape: {mask.shape}")
# print(f"mask dtype: {mask.dtype}")
# print(f"First few values of first frame: {frames[0, 0, :5].get()}")
# print(f"First few values of mask: {mask[:5].get()}")

# # First, reshape the frames array to be 2D
# frames_2d = frames.reshape(N_FRAMES, -1)  # This will be (N_FRAMES, 192*192)

# # Then modify how we call the kernel
# print("\nTesting with single frame:")
# test_frame = frames_2d[0]  # No need to flatten, already 1D
# test_bin_sum = cp.zeros((NBINS), dtype=cp.uint32)
# test_bin_count = cp.zeros((NBINS), dtype=cp.uint32)

# # Run 2D kernel
# kernel2d((40,), (32,), (test_frame, mask, test_bin_sum, test_bin_count, len(test_frame), NBINS))
# print("\n2D kernel results:")
# print(f"bin_sum: {test_bin_sum.get()}")

# # Run 3D kernel with single frame
# test_bin_sum_3d = cp.zeros((1, NBINS), dtype=cp.uint32)
# test_bin_count_3d = cp.zeros((1, NBINS), dtype=cp.uint32)
# kernel3d((40,), (32,), (test_frame[None, :], mask, test_bin_sum_3d, test_bin_count_3d, 
#          len(test_frame), NBINS, 1))
# print("\n3D kernel results (single frame):")
# print(f"bin_sum: {test_bin_sum_3d.get()}")

# # Compare results
# print("\nDifference between 2D and 3D (should be zero):")
# print(test_bin_sum.get() - test_bin_sum_3d.get())


# def compute_circle_sums_2d(frames, mask):
#     N_FRAMES = frames.shape[0]
#     NBINS = 8

#     inner_circle = cp.zeros(N_FRAMES, dtype=cp.uint32)
#     outer_circle = cp.zeros(N_FRAMES, dtype=cp.uint32)

#     for i in range(N_FRAMES):
#         frame = frames[i].flatten()
#         bin_sum = cp.zeros((NBINS), dtype=cp.uint32)
#         bin_count = cp.zeros((NBINS), dtype=cp.uint32)
#         kernel2d((40,), (32,), (frame, mask, bin_sum, bin_count, len(frame), NBINS))
#         inner_circle[i] = bin_sum[1] + bin_sum[3] + bin_sum[5] + bin_sum[7]
#         outer_circle[i] = bin_sum[0] + bin_sum[2] + bin_sum[4] + bin_sum[6]
#     return inner_circle, outer_circle


# def compute_circle_sums_3d(frames, mask):
#     N_FRAMES = frames.shape[0]
#     NBINS = 8

#     # Reshape frames to 2D array (N_FRAMES, 192*192)
#     frames_2d = frames.reshape(N_FRAMES, -1)
    
#     # Initialize output arrays
#     bin_sum = cp.zeros((N_FRAMES, NBINS), dtype=cp.uint32)
#     bin_count = cp.zeros((N_FRAMES, NBINS), dtype=cp.uint32)

#     # Call kernel once with all frames
#     npixel = 192 * 192  # Total pixels per frame
#     kernel3d((40,), (32,), (frames_2d, mask, bin_sum, bin_count, 
#              npixel, NBINS, N_FRAMES))

#     # Calculate inner and outer circle sums
#     inner_circle = bin_sum[:, 1] + bin_sum[:, 3] + bin_sum[:, 5] + bin_sum[:, 7]
#     outer_circle = bin_sum[:, 0] + bin_sum[:, 2] + bin_sum[:, 4] + bin_sum[:, 6]
    
#     return inner_circle, outer_circle
    

# def test_compute_circle_sums_2d():
#     frames_cp = cp.random.randint(0, 1000, (N_FRAMES, 192, 192), dtype=cp.uint16)
#     # mask_cp = (mask % 2==1).reshape(192, 192)
#     inner_circle_cp, outer_circle_cp = compute_circle_sums_2d(frames_cp, mask)

# def test_compute_circle_sums_3d():
#     frames_cp = cp.random.randint(0, 1000, (N_FRAMES, 192, 192), dtype=cp.uint16)
#     # mask_cp = (mask % 2==1).reshape(192, 192)
#     inner_circle_cp, outer_circle_cp = compute_circle_sums_3d(frames_cp, mask)


# inner_circle_2d, outer_circle_2d = compute_circle_sums_2d(frames, mask)
# inner_circle_3d, outer_circle_3d = compute_circle_sums_3d(frames, mask)

# print("\nDifferences (should be all zeros):")
# print("Inner differences:", (inner_circle_2d - inner_circle_3d).get())
# print("Outer differences:", (outer_circle_2d - outer_circle_3d).get())

# def compute_circle_sums_3d_cupy_naive(frames, mask):
#     inner_circle = cp.sum(frames[:, mask], axis=1)
#     outer_circle = cp.sum(frames[:, ~mask], axis=1)
#     return inner_circle, outer_circle

# mask_cp = (mask % 2==1).reshape(192, 192)
# inner_circle_cp, outer_circle_cp = compute_circle_sums_3d_cupy_naive(frames, mask_np)
# print("\nDifferences (should be all zeros) --- CUPY NAIVE:")
# print("Inner differences:", (inner_circle_2d - inner_circle_cp))
# print("Outer differences:", (outer_circle_2d - outer_circle_cp))


# def test_compute_circle_sums_3d_cupy_naive():
#     frames_cp = cp.random.randint(0, 1000, (N_FRAMES, 192, 192), dtype=cp.uint16)
#     inner_circle_cp, outer_circle_cp = compute_circle_sums_3d_cupy_naive(frames_cp, mask_cp)


# def compute_circle_sums_3d_numpy_naive(frames, mask):
#     inner_circle = np.sum(frames[:, mask], axis=1)
#     outer_circle = np.sum(frames[:, ~mask], axis=1)
#     return inner_circle, outer_circle
    
# frames_np = frames.get()
# mask_np = (mask.get() % 2==1).reshape(192, 192)
# inner_circle_np, outer_circle_np = compute_circle_sums_3d_numpy_naive(frames_np, mask_np)

# def test_compute_circle_sums_3d_numpy_naive():
#     frames_np = np.random.randint(0, 1000, (N_FRAMES, 192, 192), dtype=np.uint16)
#     # mask_np = (mask.get() % 2==1).reshape(192, 192)
#     inner_circle_np, outer_circle_np = compute_circle_sums_3d_numpy_naive(frames_np, mask_np)

# print("\nDifferences (should be all zeros) --- NUMPY NAIVE:")
# print("Inner differences:", (inner_circle_2d.get() - inner_circle_np))
# print("Outer differences:", (outer_circle_2d.get() - outer_circle_np))

# import h5py
# import matplotlib.pyplot as plt
# import numpy as np

# filepath = "/home/dleshchev/repos/dectris/dectris-hackathon/holoscan_pipeline/simplon_sim/test_data/positions_0.h5"

# plt.figure(1, clear=True)
# with h5py.File(filepath, "r") as f:
#     # print(f["entry/data/data"].shape)
#     data = f["entry/data/data"]
#     # data = np.vstack((data[:10000, :, :], data[-10000:, :, :]))
#     data = data[:, :, :]
#     p1, p2, p3, p4, theta, x, y, z = data[:,0,:].T
#     # print(x.shape)
#     # positions = f["positions"][:]
#     # print(positions.shape)
#     # print(positions)
#     # plt.plot(p1, ".-", label="p1")
#     # plt.plot(p2, ".-", label="p2")
#     # plt.plot(p3, ".-", label="p3")
#     # plt.plot(p4, ".-", label="p4")
    
#     plt.plot(theta, "-", label="theta")
#     plt.plot(x, "-", label="x")
#     plt.plot(y, "-", label="y")
#     plt.plot(z, "-", label="z")
    
#     plt.legend()
#     plt.show()


# filepath = "/home/dleshchev/repos/dectris/dectris-hackathon/holoscan_pipeline/simplon_sim/test_data/selun_test_20240519_162732_386344_master.h5"
# f = h5py.File(filepath, "r")
# plt.figure(1, clear=True)
# # with h5py.File(filepath, "r") as f:
# # print(f["entry/data/data"].shape)
# data = f["entry/data/data"][0, 0, :, :]
# plt.imshow(data)


# plt.show()

# f.close()

# files = os.listdir(directory)
# numbers = []
# for f in files:
#     if "start" in f or "end" in f:
#         continue
#     numbers.append(int(f.split("_")[-1][:-5]))

# numbers = np.sort(numbers)

# np.sort(np.array([int(f.split("_")[-1][:-5]) for f in os.listdir(directory) if ("start" not in f) or ("end" not in f)]))

## chained 0MQ proxy duplicator

# import time
# import zmq
# import argparse
# import threading
# import queue
# import signal
# import sys
# from concurrent.futures import ThreadPoolExecutor

# class ZmqProxy:
#     def __init__(self, zmq_recv_addr, zmq_send1_addr, zmq_send2_addr):
#         self.context = zmq.Context()
#         # Central queue for received messages

#         self.running = True
        
#         # Setup receive socket
#         self.sock_recv = self.context.socket(zmq.PULL)
#         self.sock_recv.connect(zmq_recv_addr)
#         self.sock_recv.setsockopt(zmq.RCVHWM, 1000)

#         self.msg_queue1 = queue.Queue(maxsize=10000) # for DAQ
#         self.msg_queue2 = queue.Queue(maxsize=500000) # for Holoscan

#         # Setup send sockets with higher HWM to accommodate bursts
#         self.sock_snd1 = self.context.socket(zmq.PUSH)
#         self.sock_snd1.bind(zmq_send1_addr)
#         self.sock_snd1.setsockopt(zmq.SNDHWM, 1000)  # Twice the receive HWM
#         self.sock_snd1.setsockopt(zmq.SNDTIMEO, 1)

#         self.sock_snd2 = self.context.socket(zmq.PUSH)
#         self.sock_snd2.bind(zmq_send2_addr)
#         self.sock_snd2.setsockopt(zmq.SNDHWM, 1000)  # Twice the receive HWM
#         self.sock_snd2.setsockopt(zmq.SNDTIMEO, 1)

#         # Setup signal handler for graceful shutdown
#         signal.signal(signal.SIGINT, self.signal_handler)
#         signal.signal(signal.SIGTERM, self.signal_handler)

#         # Statistics
#         self.stats = {
#             'received': 0,
#             'sent1': 0,
#             'sent2': 0,
#             'dropped1': 0,
#             'dropped2': 0,
#             'recv_queue_size': 0,
#             'start_time': None,
#             'last_print_time': None,
#             'receive_rate': 0.0,
#             'send1_start_time': None,
#             'send2_start_time': None,
#             'send1_rate': 0.0,
#             'send2_rate': 0.0,
#             'failed_retries2': 0
#         }

#     def signal_handler(self, signum, frame):
#         print("\nShutting down gracefully...")
#         self.running = False

#     def receive_messages(self):
#         """Thread function to receive messages and put them in the central queue"""
#         while self.running:
#             try:
#                 if self.msg_queue1.qsize() < 10000:
#                     msg = self.sock_recv.recv(zmq.NOBLOCK)    
#                     self.stats['received'] += 1
#                     self.msg_queue1.put(msg, block=True, timeout=1)
#                     self.stats['recv_queue_size'] = self.msg_queue1.qsize()
                    
#                     # Update receive rate
#                     current_time = time.time()
#                     if self.stats['start_time'] is None:
#                         self.stats['start_time'] = current_time
#                     elapsed = current_time - self.stats['start_time']
#                     if elapsed > 0:
#                         self.stats['receive_rate'] = self.stats['received'] / elapsed
#                 else:
#                     time.sleep(0.0001)
#             except zmq.Again:
#                 time.sleep(0.0001)  # Small sleep to prevent CPU spinning
#                 self.stats['recv_queue_size'] = self.msg_queue1.qsize()
#             except Exception as ex:
#                 if self.running:
#                     print(f"Error in receive thread: {ex}")
#                 break
    
#     def send_with_retry(self, socket, msg, max_retries=5, initial_delay=0.001, block=True):
#         """Helper function to send message with retry logic"""
#         delay = initial_delay
#         for attempt in range(max_retries):
#             try:
#                 socket.send(msg, int(not block))
#                 return True
#             except Exception as ex:
#                 if attempt < max_retries - 1:
#                     time.sleep(delay)
#                     delay *= 2  # Exponential backoff
#                 else:
#                     return False
#         return False

#     def send_messages_socket1(self): # for DAQ
#         """Thread function to send messages from the queue to DAQ"""
#         while self.running:
#             # for i in range(500): pass
#             try:
#                 msg = self.msg_queue1.get(block=True, timeout=1)
                
#                 # Try to send to first socket with retry
#                 if self.send_with_retry(self.sock_snd1, msg, max_retries=3, initial_delay=0.001, block=False):
#                     self.stats['sent1'] += 1
#                     if self.stats['send1_start_time'] is None:
#                         self.stats['send1_start_time'] = time.time()
#                     else:
#                         elapsed = time.time() - self.stats['send1_start_time']
#                         if elapsed > 0:
#                             self.stats['send1_rate'] = self.stats['sent1'] / elapsed
#                 else:
#                     self.stats['dropped1'] += 1
                
#                 try:
#                     self.msg_queue2.put(msg, block=True, timeout=0.001)
#                 except queue.Full:
#                     print(f"self.msg_queue2.qsize() = {self.msg_queue2.qsize()}")
#                     self.stats['dropped2'] += 1
#                 # if self.msg_queue2.qsize() < 500000:
#                 #     self.msg_queue2.put(msg, block=True, timeout=1)
#                 # else:
#                 #     print(f"self.msg_queue2.qsize() = {self.msg_queue2.qsize()}")
#                 #     self.stats['dropped2'] += 1
                    
#             except queue.Empty:
#                 time.sleep(0.001)
#             except Exception as ex:
#                 if self.running:
#                     print(f"Error in send thread: {ex}")
#                 break

#     def send_messages_socket2(self): # for Holoscan
#         """Thread function to send messages from the queue to Holoscan"""
#         while self.running:
#             try:
#                 msg = self.msg_queue2.get(block=True, timeout=1)
                
#                 # Try to send to second socket with retry
#                 if self.send_with_retry(self.sock_snd2, msg, max_retries=10, initial_delay=0.0001, block=False):
#                     self.stats['sent2'] += 1
#                     if self.stats['send2_start_time'] is None:
#                         self.stats['send2_start_time'] = time.time()
#                     else:
#                         elapsed = time.time() - self.stats['send2_start_time']
#                         if elapsed > 0:
#                             self.stats['send2_rate'] = self.stats['sent2'] / elapsed
#                 else:
#                     self.stats['failed_retries2'] += 1
#                     self.stats['dropped2'] += 1
                
#             except queue.Empty:
#                 time.sleep(0.001)
#             except Exception as ex:
#                 if self.running:
#                     print(f"Error in send thread: {ex}")
#                 break


#     def print_stats(self):
#         """Print statistics periodically"""
#         while self.running:
#             print(f"\rStats - Received: {self.stats['received']} ({self.stats['receive_rate']:.1f} Hz), "
#                   f"Sent1: {self.stats['sent1']} ({self.stats['send1_rate']:.1f} Hz), "
#                   f"Sent2: {self.stats['sent2']} ({self.stats['send2_rate']:.1f} Hz), "
#                   f"Drop1: {self.stats['dropped1']}, Drop2: {self.stats['dropped2']} ({self.stats['failed_retries2']}), "
#                   f"RecvQ: {self.stats['recv_queue_size']}", end='')
#             time.sleep(1)

#     def run(self):
#         """Main function to run the proxy"""
#         print("0MQ Proxy is starting...")
        
#         # Create thread pool
#         with ThreadPoolExecutor(max_workers=4) as executor:  # One more worker for stats
#             # Start receive thread
#             receive_future = executor.submit(self.receive_messages)

#             # Start send thread
#             send_future = executor.submit(self.send_messages_socket1)
#             send_future = executor.submit(self.send_messages_socket2)
            
#             # Start stats thread
#             stats_future = executor.submit(self.print_stats)
            
#             print("\n0MQ Proxy is running successfully.")
            
#             # Wait for threads to complete
#             receive_future.result()
#             send_future.result()
#             stats_future.result()

#         # Cleanup
#         self.sock_recv.close()
#         self.sock_snd1.close()
#         self.sock_snd2.close()
#         self.context.term()
#         print("\n0MQ Proxy has been shut down.")

# def main():
#     def formatter(prog):
#         return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

#     parser = argparse.ArgumentParser(
#         description="0MQ Proxy: Connects to a single PUSH socket and republishes all received messages.\n"
#         "to two different PUSH sockets. The proxy is used to distribute messages to multiple consumers.\n",
#         formatter_class=formatter,
#     )

#     parser.add_argument(
#         "--zmq-recv-addr",
#         dest="zmq_recv_addr",
#         action="store",
#         default=None,
#         help="Address of the remote PUSH socket, e.g. 'tcp://127.0.0.1:5557'.",
#     )

#     parser.add_argument(
#         "--zmq-send-addr1",
#         dest="zmq_send_addr1",
#         action="store",
#         default=None,
#         help="Address of the first PUSH socket, e.g. 'tcp://127.0.0.1:5558'.",
#     )

#     parser.add_argument(
#         "--zmq-send-addr2",
#         dest="zmq_send_addr2",
#         action="store",
#         default=None,
#         help="Address of the first PUSH socket, e.g. 'tcp://127.0.0.1:5559'.",
#     )

#     args = parser.parse_args()

#     try:
#         if args.zmq_recv_addr is None:
#             raise ValueError("The address of the remote PUSH socket must be provided.")
#         if args.zmq_send_addr1 is None:
#             raise ValueError("The address of the first PUSH socket must be provided.")
#         if args.zmq_send_addr2 is None:
#             raise ValueError("The address of the second PUSH socket must be provided.")

#         proxy = ZmqProxy(args.zmq_recv_addr, args.zmq_send_addr1, args.zmq_send_addr2)
#         proxy.run()

#     except ValueError as ex:
#         print(f"Error: {ex}")
#         sys.exit(1)

# if __name__ == "__main__":
#     main()

import json


with open('zmq_panda_output.log', 'r') as file:
    for line in file:
        if line.startswith('datasets'):
            line = line.replace("'", '"')
            data = json.loads(line[line.find('{'):])
            print(data)
            break
        
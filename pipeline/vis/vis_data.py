import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from queue import Empty
# import asyncio
from nats_async import launch_nats_instance
import threading
# import time
from queue import Empty, Queue
import json
import time
# Create nats instance
import matplotlib.tri as tri    
import matplotlib
# Create nats instance with default host (will use environment variable)
nats_inst = launch_nats_instance(host="localhost:6000", # will use environment variable; for connecting to a remote server, use its address/port
                                 subscribe_subjects=("raw_image",
                                                     "raw_position"))#,
                                                     # "raw_positions"))


# QUEUE_DATA = Queue()  
QUEUE_CONTROL = Queue()

R, x, y = None, None, None
is_dragging = False
is_resizing = False
drag_start = None

# Add data history variables
x_history = []
y_history = []
pos_id_history = []

def on_mouse_press(event):
    global is_dragging, is_resizing, drag_start, R, x, y
    if event.inaxes != ax1:
        return
    
    # Calculate distance from click to circle center
    dx = event.xdata - x
    dy = event.ydata - y
    distance = np.sqrt(dx*dx + dy*dy)
    
    if abs(distance - R) < 5:  # If click is near the circle's edge
        is_resizing = True
    elif distance < R:  # If click is inside the circle
        is_dragging = True
    
    drag_start = (event.xdata, event.ydata)

def on_mouse_release(event):
    global is_dragging, is_resizing
    is_dragging = False
    is_resizing = False

def on_mouse_move(event):
    global R, x, y, is_dragging, is_resizing, drag_start
    if event.inaxes != ax1:
        return
    
    if is_dragging:
        # Move circle
        dx = event.xdata - drag_start[0]
        dy = event.ydata - drag_start[1]
        x += dx
        y += dy
        drag_start = (event.xdata, event.ydata)
        QUEUE_CONTROL.put({"R": R, "x": x, "y": y})
        
    elif is_resizing:
        # Resize circle
        dx = event.xdata - x
        dy = event.ydata - y
        R = np.sqrt(dx*dx + dy*dy)
        QUEUE_CONTROL.put({"R": R, "x": x, "y": y})

# def generate_data():
#     global R, x, y
#     while True:
#         data =  np.random.rand(192, 192)
#         if R is None:
#             R = 50
#             x = 60
#             y = 50
#             # Put initial values into control queue
#             QUEUE_CONTROL.put({"R": R, "x": x, "y": y})
        
#         # Check for control updates
#         while True:
#             try:
#                 control = QUEUE_CONTROL.get(block=False)
#                 R = control["R"]
#                 x = control["x"]
#                 y = control["y"]
#                 print(f"got new control parameters: R: {R}, x: {x}, y: {y}")
#             except Empty:
#                 break
                
#         QUEUE_DATA.put({"data": data, "R": R, "x": x, "y": y})
#         time.sleep(0.5)

plt.style.use('dark_background')
fontsize = 7
matplotlib.rcParams.update({'font.size': fontsize})
# Create the plot
# fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)
fig, (ax2, ax1) = plt.subplots(1, 2)
# fig.subplots_adjust(right=0.75)

# Initialize with zeros
im1 = ax1.imshow(np.zeros((128, 128)), cmap='gray', vmin=0, vmax=50)
line2, = ax2.plot([1], [2], 'w.')
# im1 = ax1.imshow(np.zeros((80,160))*np.nan, cmap='gray', vmin=0, vmax=1)
# im2 = ax2.imshow(np.zeros((80,160))*np.nan, cmap='gray', vmin=0, vmax=1)
ax2.set_xlim(-40, 40)
ax2.set_ylim(-20, 20)
# ax2.set_xlim(-40, 40)
# ax2.set_ylim(-20, 20)
# _text = ax1.text(0.75, 0.8, f"", transform=ax1.transAxes)
# im = ax1.contourf(np.zeros((192, 192)), cmap='viridis', vmin=0, vmax=1)
# plt.colorbar(im)

# # Create initial circle
# circle = plt.Circle((0, 0), 1, color='red', fill=False, linewidth=2)
# ax1.add_patch(circle)

# # Connect event handlers
# fig.canvas.mpl_connect('button_press_event', on_mouse_press)
# fig.canvas.mpl_connect('button_release_event', on_mouse_release)
# fig.canvas.mpl_connect('motion_notify_event', on_mouse_move)

# def update_tricontour(ax, x, y, intensity_map):
#     triang = tri.Triangulation(x, y)
#     vmin = np.percentile(intensity_map, 1.5)
#     vmax = np.percentile(intensity_map, 98.5)
#     im = ax.tricontourf(triang, intensity_map, 51, cmap='gray', vmin=vmin, vmax=vmax)
#     ax.relim()
#     ax.autoscale_view()
#     ax.set_xlim(x.min(), x.max())
#     ax.set_ylim(y.min(), y.max())
#     # ax.tick_params(axis='both', which='major', labelsize=10)
#     return im

def animate(i):
    global im1, line2, x_history, y_history
    try:
        raw_image = nats_inst.get_rxq("raw_image").get(block=False)
        # Decode bytes to string, parse JSON, convert to numpy array
        raw_image = np.array(json.loads(raw_image.decode()))
        print(f"Got raw image, frame {i}, {raw_image.shape=}")
        im1.set_data(raw_image[100-64:100+64, 100-64:100+64])
        
    except Empty:
        print("Empty image queue")
    except Exception as e:
        print(f"Error: {e}")
    
    try:
        
        raw_position = nats_inst.get_rxq("raw_position").get(block=False)
        # Decode bytes to string, parse JSON, convert to numpy array
        raw_position = np.array(json.loads(raw_position.decode()))
        # print(f"Got raw image, frame {i}, {raw_image.shape=}")
        print(f"Got raw position, frame {i}, {raw_position.shape=}")
        pos_mot_x = raw_position[:,0]
        pos_mot_y = raw_position[:,1]
        pos_mot_z = raw_position[:,2]
        pos_t = raw_position[:,3]
        # pos_id = raw_position[:,4]
        theta = np.mean(pos_t)
        angle_radians = np.pi * theta / 180.0
        x = (pos_mot_x * np.cos(angle_radians)) + (pos_mot_z * np.sin(angle_radians))
        y = pos_mot_y
        x *= -1
        y *= -1
        
        # Update line with all historical data
        line2.set_xdata(x)
        line2.set_ydata(y)
 
    except Empty:
        print("Empty position queue")
    except Exception as e:
        print(f"Error: {e}")
    # try:
    #     raw_frame = nats_inst.get_rxq("raw_frame").get(block=False)
    #     raw_frame = np.array(json.loads(raw_frame.decode()))
    #     print(f"Got raw frame, frame {i}, {raw_frame.shape=}")
    #     im1 = ax1.imshow(raw_frame, cmap='viridis', vmin=0, vmax=np.percentile(raw_frame[raw_frame < 2**16], 99))
    # except Empty:
    #     print("Empty")
    # except Exception as e:
    #     print(f"Error: {e}")

    # try:
    #     raw_positions = nats_inst.get_rxq("raw_positions").get(block=False)
    #     raw_positions = np.array(json.loads(raw_positions.decode()))
    #     print(f"Got raw positions, frame {i}, {raw_positions.shape=}")
    #     line2.set_xdata(raw_positions[:, 0])
    #     line2.set_ydata(raw_positions[:, 1])
    #     ax2.relim()
    #     ax2.autoscale_view()
    #     ax2.set_xlim(x.min(), x.max())
    #     ax2.set_ylim(y.min(), y.max())
    # except Empty:
    #     print("Empty")
    # except Exception as e:
    #     print(f"Error: {e}")
    return im1, line2 # circle

# t = threading.Thread(target=generate_data, daemon=True)
# t.start()

ani = animation.FuncAnimation(fig, animate, interval=50, blit=True, cache_frame_data=False)
plt.show()

# print(nats_inst.rxq)



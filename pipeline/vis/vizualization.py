
import math
import time

try:
    import numpy as np
except ImportError:
    raise ImportError("This demo requires numpy, but it could not be imported.")

from holoscan.conditions import CountCondition
from holoscan.core import Application, Operator, OperatorSpec
from holoscan.decorator import Input, Output, create_op

import matplotlib.pyplot as plt

import multiprocessing as mp

queue = mp.Queue()

def visualize(queue):
    plt.ion()
    fig, ax = plt.subplots()
    data = np.zeros((10, 10))
    heatmap = ax.imshow(data, cmap='viridis', vmin=0, vmax=1)

    while True:
        if not queue.empty():
            data = queue.get()
            heatmap.set_data(data)
            fig.canvas.draw_idle()
            fig.canvas.flush_events()

iterations = 1000

# unsure how to use the "count" condition on this operator
@create_op(
    outputs=("frame"))
def generate(count=1000):
    for _ in range(count):
        frame = np.random.rand(10,10)
        yield (frame)


@create_op(inputs=("frame"))
def viz_frame(frame):
    queue.put(np.real(frame))

class BasicRadarFlow(Application):
    def __init__(self):
        super().__init__()

    def compose(self):
        src = generate(self, count=iterations, name="src")
        viz = viz_frame(self, name="viz")

        self.add_flow(src, viz, {("frame", "frame")})

if __name__ == "__main__":
    app = BasicRadarFlow()
    app.config("")

    viz_process = mp.Process(target=visualize, args=(queue,))
    viz_process.start()

    tstart = time.time()
    app.run()
    tstop = time.time()


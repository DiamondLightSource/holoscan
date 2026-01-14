#!/bin/bash

docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v ./pipeline:/workspace \
    ptycho-holoscan:stxm
#!/bin/bash
# Mount repo into /workdir so pipeline/ and pixi.toml are in the workspace.
# Run `pixi install` once inside the container (and when you change pixi.toml).
# Then: pixi run run-pipeline  or  pixi run python pipeline/pipeline.py --config pipeline/config_test.yaml
docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v "$(pwd):/workdir" \
    -w /workdir \
    ptycho-holoscan:stxm \
    "$@"

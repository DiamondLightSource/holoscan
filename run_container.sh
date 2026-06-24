#!/bin/bash
set -e

RT=docker
if [[ "$1" == "--podman" ]]; then
    RT=podman
    shift
elif [[ "$1" == "--docker" ]]; then
    RT=docker
    shift
fi

# GPU flags: Docker uses nvidia runtime; Podman uses device
if [[ "${RT}" == "docker" ]]; then
    GPU_OPTS=(--runtime=nvidia --gpus all)
else
    GPU_OPTS=(--device nvidia.com/gpu=all)
fi

# Mount only pipeline, PtyREX, and test_data so code can be edited on the host.
# Image already has pixi env in /workdir. Run `pixi run install-ptyrex` once per session.
"${RT}" run -it --rm --ipc=host --privileged \
    "${GPU_OPTS[@]}" \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v "$(pwd)/pipeline:/workdir/pipeline" \
    -v "$(pwd)/PtyREX:/workdir/PtyREX" \
    -v "$(pwd)/test_data:/workdir/test_data" \
    -w /workdir \
    ptycho-holoscan:ptyrex \
    "$@"

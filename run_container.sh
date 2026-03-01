#!/bin/bash
# Mount only pipeline, PtyREX, and test_data so code can be edited on the host.
# Image already has pixi env in /workdir. Run `pixi run install-ptyrex` once per session.
docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v "$(pwd)/pipeline:/workdir/pipeline" \
    -v "$(pwd)/PtyREX:/workdir/PtyREX" \
    -v "$(pwd)/test_data:/workdir/test_data" \
    -w /workdir \
    ptycho-holoscan:stxm \
    "$@"

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

"${RT}" build . -t ptycho-holoscan:stxm --network host
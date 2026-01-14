# dectris-hackathon

April 2025 Hackathon between NVIDIA, DECTRIS, DLS, PSI.

This repository contains trainings and materials to facilitate, build, and deploy a data processing pipeline for real-time reconstruction of X-ray ptychography data recorded using high-speed DECTRIS cameras, such as SELUN and EIGER.

## Contents
- Training
    -  Holoscan Bootcamp Materials -- `./bootcamp/`
        - Overview of Holoscan framework for real-time sensor data processing
        - [Learn by example](bootcamp/workspace/python/Python-Holoscan-Tutorial.ipynb)
    - CUDA Python Training -- `./cuda-python-training/`
        - Overview of GPU programming in Python
        - [Slides & Overview](cuda-python-training/slides)
        - [Learn by example](cuda-python-training/notebooks)


- Holoscan data processing pipeline -- `./holoscan_pipeline/`
    - Real-time ptychography data processing pipeline
    - [Overview](holoscan_pipeline/README.md)


## Prerequisites
 
 Materials in this repo are written to be deployed within a Docker container on a system that satisfies Holoscan SDK minimum installation requirements ([details](https://docs.nvidia.com/holoscan/sdk-user-guide/sdk_installation.html)):

- Debian distribution such as Ubuntu 22.04 or RHEL 9.x.
- Docker -- [link](https://docs.docker.com/engine/install/ubuntu/)
- NVIDIA Container Toolkit (>1.16.2) -- [link](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
- Account on NGC -- [link](https://ngc.nvidia.com/signin)


## Getting Started

1. Build the containers:
```bash
./build_containers.sh
```

2. Run the bootcamp environment:
```bash
./run_bootcamp.sh
```

3. Run the Holoscan environment:
```bash
./run_holoscan.sh
```

4. Install and activate venv:
```
. ./prepare.sh
```

For detailed instructions on specific components, please refer to the README files in their respective directories.

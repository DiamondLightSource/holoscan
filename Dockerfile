# Use the official Ubuntu base image
# FROM ubuntu:24.04
FROM docker.io/nvidia/cuda:12.6.0-devel-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HOLOSCAN_ENABLE_PROFILE=1

# Install basic dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    python3-apt \
    wget \
    hostname \
    socat \
    && rm -rf /var/lib/apt/lists/*

# Add deadsnakes PPA and universe repository
RUN add-apt-repository -y ppa:deadsnakes/ppa && \
    add-apt-repository -y universe

# Update package list and install Python 3.11 and other dependencies
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    gcc \
    libhdf5-serial-dev \
    curl \
    libopenmpi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install NATS server
RUN curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.7/nats-server-v2.10.7-linux-amd64.tar.gz -o nats-server.tar.gz
RUN tar -xzf nats-server.tar.gz
RUN mv nats-server-v2.10.7-linux-amd64/nats-server /usr/local/bin/
RUN rm -rf nats-server.tar.gz nats-server-v2.10.7-linux-amd64

# Set Python 3.11 as default Python 3 and create convenience links
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Install and upgrade pip
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11 && \
    python3 -m pip install --upgrade pip

# Install dependencies
RUN pip install holoscan pyfftw scipy scikit-learn dask zmq cbor2 dectris-compression \
    ipython jupyter jupyterlab nvidia-cuda-runtime-cu12 h5py hdf5plugin \
    mpi4py nvtx nats-py asyncio

ENV CUDA_WHL_LIB_DIR=/usr/local/lib/python3.11/dist-packages/nvidia/cuda_runtime/lib
ENV LD_LIBRARY_PATH=${CUDA_WHL_LIB_DIR}
ENV HDF5_PLUGIN_PATH=/usr/local/lib/python3.11/dist-packages/hdf5plugin/plugins/

# Install nsight-systems
RUN cd /tmp && \
    wget https://developer.nvidia.com/downloads/assets/tools/secure/nsight-systems/2025_1/nsight-systems-2025.1.1_2025.1.1.103-1_amd64.deb && \
    apt-get update && \
    apt-get install -y --fix-missing ./nsight-systems-2025.1.1_2025.1.1.103-1_amd64.deb && \
    rm -rf /tmp/* && \
    rm -rf /var/lib/apt/lists/*

# # Install OpenMPI
# RUN apt-get update && \
#     apt-get install -y libopenmpi-dev && \
#     rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy startup script that handles NATS initialization
COPY start_nats_server.sh /usr/local/bin/start_nats_server.sh
RUN chmod +x /usr/local/bin/start_nats_server.sh

# Set entrypoint to startup script
# This will automatically start NATS server with JetStream enabled
ENTRYPOINT ["/usr/local/bin/start_nats_server.sh"]
CMD ["/bin/bash"]
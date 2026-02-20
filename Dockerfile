# Combined Holoscan pipeline + PtyREX (CUDA 12.6, Ubuntu 22.04)
FROM docker.io/nvidia/cuda:12.6.0-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HOLOSCAN_ENABLE_PROFILE=1

# Base tools (merged from main + PtyREX)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    vim \
    wget \
    ca-certificates \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Pixi (used by both pipeline and PtyREX)
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_HOME=/usr/local bash

# NATS server for pipeline
RUN curl -sL https://github.com/nats-io/nats-server/releases/download/v2.10.7/nats-server-v2.10.7-linux-amd64.tar.gz | tar xz -C /tmp \
    && mv /tmp/nats-server-v2.10.7-linux-amd64/nats-server /usr/local/bin/ \
    && rm -rf /tmp/nats-server-v2.10.7-linux-amd64

    
# Nsight Systems (profiling) via Public Repo
RUN apt-get update && apt-get install -y --no-install-recommends gnupg && \
    wget -qO - https://developer.download.nvidia.com/devtools/repos/ubuntu2204/amd64/nvidia.pub | apt-key add - && \
    echo "deb https://developer.download.nvidia.com/devtools/repos/ubuntu2204/amd64/ /" > /etc/apt/sources.list.d/nvidia-devtools.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends nsight-systems-2025.1.1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workdir

#----------------------------------#
# Additional setup for PtyREX
# --- Holoscan pipeline environment (root pixi) ---
COPY pixi.toml pixi.lock ./
RUN pixi install --frozen

# --- Application and PtyREX code ---
COPY . .

# --- PtyREX environment (PtyREX pixi + install package) ---
RUN cd PtyREX && pixi install --frozen
RUN cd PtyREX && ./.pixi/envs/default/bin/python -m pip install --no-deps .

# Create a shortcut for ptyrex-python
RUN ln -s /workdir/PtyREX/.pixi/envs/default/bin/python /usr/local/bin/python-pty

# Default PATH: pipeline env (for `python pipeline/pipeline.py` and NATS)
ENV PATH="/workdir/.pixi/envs/default/bin:${PATH}"
# MPI (PtyREX): use when running ptyrex from /workdir/PtyREX
ENV OMPI_MCA_plm_rsh_agent=sh
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
ENV OPAL_PREFIX="/workdir/PtyREX/.pixi/envs/default"
#----------------------------------#

# Pipeline entrypoint: start NATS then exec
COPY start_nats_server.sh /usr/local/bin/start_nats_server.sh
RUN chmod +x /usr/local/bin/start_nats_server.sh

ENTRYPOINT ["/usr/local/bin/start_nats_server.sh"]
CMD ["/bin/bash"]

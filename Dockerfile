FROM docker.io/nvidia/cuda:12.6.0-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HOLOSCAN_ENABLE_PROFILE=1

# Base tools and pixi
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    vim \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_HOME=/usr/local bash

# NATS server binary
RUN curl -sL https://github.com/nats-io/nats-server/releases/download/v2.10.7/nats-server-v2.10.7-linux-amd64.tar.gz | tar xz -C /tmp \
    && mv /tmp/nats-server-v2.10.7-linux-amd64/nats-server /usr/local/bin/ \
    && rm -rf /tmp/nats-server-v2.10.7-linux-amd64

# Nsight Systems
RUN cd /tmp && \
    wget -q https://developer.nvidia.com/downloads/assets/tools/secure/nsight-systems/2025_1/nsight-systems-2025.1.1_2025.1.1.103-1_amd64.deb && \
    apt-get update && apt-get install -y --no-install-recommends --fix-missing ./nsight-systems-2025.1.1_2025.1.1.103-1_amd64.deb && \
    rm -rf /tmp/* /var/lib/apt/lists/*

WORKDIR /workdir

COPY start_nats_server.sh /usr/local/bin/start_nats_server.sh
RUN chmod +x /usr/local/bin/start_nats_server.sh

ENTRYPOINT ["/usr/local/bin/start_nats_server.sh"]
CMD ["/bin/bash"]

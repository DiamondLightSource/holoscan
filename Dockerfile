# Combined Holoscan pipeline + PtyREX (Unified Environment)
FROM docker.io/nvidia/cuda:12.6.0-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HOLOSCAN_ENABLE_PROFILE=1

# 1. Base tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl vim wget ca-certificates git build-essential gnupg \
    && rm -rf /var/lib/apt/lists/*

# 2. Pixi Installation (Internal)
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_HOME=/usr/local bash

# 3. NATS Server
RUN curl -sL https://github.com/nats-io/nats-server/releases/download/v2.10.7/nats-server-v2.10.7-linux-amd64.tar.gz | tar xz -C /tmp \
    && mv /tmp/nats-server-v2.10.7-linux-amd64/nats-server /usr/local/bin/ \
    && rm -rf /tmp/nats-server-v2.10.7-linux-amd64

# 4. Nsight Systems
RUN wget -qO - https://developer.download.nvidia.com/devtools/repos/ubuntu2204/amd64/nvidia.pub | apt-key add - && \
    echo "deb https://developer.download.nvidia.com/devtools/repos/ubuntu2204/amd64/ /" > /etc/apt/sources.list.d/nvidia-devtools.list && \
    apt-get update && apt-get install -y --no-install-recommends nsight-systems-2025.1.1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workdir

# 5. Unified Environment Setup
# Copy manifest only; generate lock and install. Source (pipeline, PtyREX) is mounted at runtime.
COPY pixi.toml ./

RUN --mount=type=cache,target=/root/.cache/rattler \
    pixi install

# 6. MPI & PATH CONFIGURATION
# These go here so they are set for the final image
ENV PATH="/workdir/.pixi/envs/default/bin:${PATH}"
ENV OPAL_PREFIX="/workdir/.pixi/envs/default"
ENV OMPI_MCA_plm_rsh_agent=sh
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
ENV PYTHONUNBUFFERED=1

# 7. Entrypoint
COPY start_nats_server.sh /usr/local/bin/start_nats_server.sh
RUN chmod +x /usr/local/bin/start_nats_server.sh

ENTRYPOINT ["/usr/local/bin/start_nats_server.sh"]
CMD ["/bin/bash"]
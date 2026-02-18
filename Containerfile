FROM ghcr.io/astral-sh/uv:debian

ARG KUBO_VERSION=0.39.0
ARG GO_VERSION=1.23.6
ARG NODE_VERSION=22.14.0

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Node
RUN curl -sSfL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
        | tar -xJ --strip-components=1 -C /usr/local

# Install Kubo
RUN curl -sSfL "https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/kubo_v${KUBO_VERSION}_linux-amd64.tar.gz" \
        | tar -xz && \
    cd kubo && ./install.sh && \
    cd / && rm -rf kubo

# Install Go (needed to build go-car and someguy)
RUN curl -sSfL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
        | tar -xz -C /usr/local
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

# Install go-car and someguy
RUN go install github.com/ipld/go-car/cmd/car@latest && \
    go install github.com/ipfs/someguy@latest

# Install storacha cli
RUN npm install -g @storacha/cli

# App
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --no-dev --frozen
COPY . .
RUN uv sync --no-dev --frozen

# Work directory for output CAR files, metadata DBs, etc.
VOLUME /work

# Persistent config for IPFS repo and storacha secrets
VOLUME /config
ENV IPFS_PATH="/config/ipfs"
ENV XDG_CONFIG_HOME="/config"
ENV WORKDIR="/work"

WORKDIR /work

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

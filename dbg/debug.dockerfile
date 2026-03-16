# Debug Dockerfile for Vector with headless ClickHouse support
#
# Usage:
#   1. Build vector: cargo build
#   2. Copy binary out of target/ (excluded by .dockerignore):
#      cp target/debug/vector ./vector-debug-bin
#   3. Build docker image:
#      docker build -f debug.dockerfile -t vector-debug .
#   4. Clean up:
#      rm ./vector-debug-bin

FROM debian:bookworm-slim

# Install basic runtime dependencies that Vector typically needs
# These cover common shared library requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    libc6 \
    libgcc-s1 \
    zlib1g \
    curl \
    dnsutils \
    iputils-ping \
    net-tools \
    procps \
    strace \
    lsof \
    file \
    && rm -rf /var/lib/apt/lists/*

# Create vector user and directories
RUN useradd --system --no-create-home vector && \
    mkdir -p /etc/vector /var/lib/vector /var/log/vector && \
    chown -R vector:vector /var/lib/vector /var/log/vector

# Copy the debug binary (copied from target/debug/vector to repo root to bypass .dockerignore)
COPY vector /usr/local/bin/vector
 
# Make sure the binary is executable
RUN chmod +x /usr/local/bin/vector

# Print library dependencies for debugging (useful to verify all libs are available)
RUN echo "=== Vector binary library dependencies ===" && \
    ldd /usr/local/bin/vector || true && \
    echo "=== End library dependencies ==="

# Copy the local config file
COPY local-clickhouse.toml /etc/vector/vector.toml

# Expose common Vector ports
# 8686 - Vector API
# 9000 - Prometheus metrics
EXPOSE 8686 9000

# Set environment variables for verbose logging
ENV VECTOR_LOG=info
ENV RUST_BACKTRACE=1

# Default command - run vector with the config
ENTRYPOINT ["/usr/local/bin/vector"]
CMD ["--config", "/etc/vector/vector.toml"]

# Debug Dockerfile for Vector with headless ClickHouse support
# Build the binary first: cargo build
# Then build this image: docker build -f debug.dockerfile -t vector-debug .

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

# Copy the debug binary
COPY target/debug/vector /usr/local/bin/vector

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

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8686/health || exit 1

# Run as vector user (commented out for debugging - uncomment for production)
# USER vector

# Default command - run vector with the config
ENTRYPOINT ["/usr/local/bin/vector"]
CMD ["--config", "/etc/vector/vector.toml"]

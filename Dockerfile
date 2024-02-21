# Basic Dockerfile to run ringserver in a container
# 
# Build container using this command:
#     docker build -t ringserver:latest .
#
# Run container, using host networking (may not work on non-Linux):
#     docker run --network="host" --rm -it ringserver
#
# Run container, using bridge networking (likely impossible to submit data):
#     docker run --network="bridge" -p 18000:18000 --rm -it ringserver


# Build ringserver in a separate container,
# so resulting container does not include compiler tools
FROM debian:bookworm-slim as buildenv
RUN apt update \
    && apt upgrade -y \
    && apt install -y --no-install-recommends \
        build-essential \
        automake \
        netbase \
    && rm -rf /var/lib/apt/lists/*

# Build executable
COPY . /build
RUN cd /build && CFLAGS="-O2" make

# Build ringserver container
FROM debian:bookworm-slim
RUN apt update \
    && apt upgrade -y \
    && apt install -y --no-install-recommends \
        netbase \
    && rm -rf /var/lib/apt/lists/*
# Copy executable and default config from build image
COPY --from=buildenv /build/ringserver /ringserver
COPY --from=buildenv /build/doc/ring.conf /ring.conf
# Run as non-root user
RUN adduser ringuser && \
    mkdir -p /data/ring && \
    chown -R ringuser /data
WORKDIR /data
USER ringuser

# Expose default SeedLink and DataLink ports
EXPOSE 18000
EXPOSE 16000

# Default command is "ringserver"
ENTRYPOINT [ "/ringserver" ]

# Default arguments
CMD [ "/ring.conf", "-L", "16000" ]

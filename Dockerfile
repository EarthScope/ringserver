# Basic Dockerfile to run ringserver in a container

ARG BASE=debian:bookworm-slim

# Build ringserver in a separate container, so resulting container does not
# include compiler tools
FROM $BASE AS buildenv
RUN apt update && \
    apt upgrade -y && \
    apt install -y --no-install-recommends \
        clang \
        make \
        netbase && \
    rm -rf /var/lib/apt/lists/*

# Build executable
COPY . /build
RUN cd /build && make clean -j && CFLAGS="-O3" make -j

# Build ringserver container
FROM $BASE
RUN apt update && \
    apt upgrade -y && \
    apt install -y --no-install-recommends \
        netbase && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

# Copy executable and default config from build image
COPY --from=buildenv /build/ringserver /ringserver

# Add non-root user
ARG UID=10000
ARG GID=10000
ARG USERNAME=containeruser
RUN groupadd --gid $GID $USERNAME && \
    adduser --uid $UID --gid $GID $USERNAME

# Set up ring file directory
RUN mkdir -p /data/ring && \
    chown -R $UID:$GID /data
WORKDIR /data

# Expose default SeedLink and DataLink ports
# The ports are configured with ENV below
EXPOSE 18000
EXPOSE 16000

# Drop to regular user
USER $USERNAME

# Set default configuration parameters.
# These can be overridden by setting the corresponding ENV variables
# to the desired values or to "DISABLE" when running the container.
ENV RS_RING_DIRECTORY=/data/ring
ENV RS_LISTEN_PORT=18000
ENV RS_DATALINK_PORT=16000

ENTRYPOINT [ "/ringserver" ]
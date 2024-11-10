# Basic Dockerfile to run ringserver in a container

ARG BASE=debian:bookworm-slim

# Build ringserver in a separate container, so resulting container does not 
# include compiler tools
FROM $BASE as buildenv
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
FROM $BASE
RUN apt update \
    && apt upgrade -y \
    && apt install -y --no-install-recommends \
        netbase \
    && rm -rf /var/lib/apt/lists/*

# Copy executable and default config from build image
COPY --from=buildenv /build/ringserver /ringserver
COPY --from=buildenv /build/doc/ring.conf /ring.conf
COPY ./entrypoint.sh /

# Add non-root user
ARG UID=10000
ARG GID=10001
ARG USERNAME=containeruser
RUN \
    groupadd --gid $GID $USERNAME && \
    adduser --uid $UID --gid $GID $USERNAME

# Set up ring file directory
RUN mkdir -p /data/ring && \
    chown -R $UID:$GID /data
WORKDIR /data

# Expose default SeedLink and DataLink ports
# Seedlink port 180000 is configured in ring.conf
# Datalink port 160000 is configured with ENV below
EXPOSE 18000
EXPOSE 16000

# Drop to regular user
USER $USERNAME

# Set default command line arguments expanded in ./entrypoint.sh
ENV LISTEN_PORT=16000
ENV CONFIG_FILE="/ring.conf"

ENTRYPOINT [ "/entrypoint.sh" ]
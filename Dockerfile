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
#
# Run container, using Docker Compose using persistant ring storage:
#     mkdir -p ring
#     sudo chown 10000 ring
#     docker compose up
#
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
EXPOSE 18000
EXPOSE 16000

# Drop to regular user
USER $USERNAME

# Default command is "ringserver"
ENTRYPOINT [ "/ringserver" ]

# Default arguments
CMD [ "/ring.conf", "-L", "16000" ]

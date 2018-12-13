# Basic Dockerfile to run ringserver in a container
# 
# Build container using this command:
#     docker build -t ringserver:latest .
#
# Run built container:
#     docker run --rm -it ringserver


# build ringserver in a separate container,
# so resulting container does not include compiler tools
FROM centos:7 as buildenv
# install compiler
RUN yum install -y gcc make
# build executable
COPY . /build
RUN cd /build && make


FROM centos:7
# install updates
RUN yum upgrade -y
# copy executable from build image
COPY --from=buildenv /build/ringserver /ringserver
COPY --from=buildenv /build/doc/ring.conf /ring.conf
# run as non-root user
RUN adduser ringuser && \
    mkdir -p /data/ring && \
    chown -R ringuser /data
WORKDIR /data
USER ringuser
# expose default port
EXPOSE 18000
# default command is "ringserver"
ENTRYPOINt [ "/ringserver" ]
# default arguments are config file
CMD [ "/ring.conf" ]

# ringserver - Generic packet ring buffer with network interfaces

This program is a generic, stream-oriented packet ring buffer.  The primary use
case is to transport packetized snippets of time series data.  The supported
protocols, all TCP-based, are: DataLink, SeedLink and HTTP/WebSocket.

The ring buffer is implemented as a FIFO with newly arriving packets pushing
older packets out of the buffer.  The ring packets are not format specific and
may contain any type of data.

Some features are specific to miniSEED, the standard data format in seismology,
such as the SeedLink protocol, built-in miniSEED file scanner and built-in
miniSEED archiver.  These are format-specific enhancements built on the core
transport that is agnostic to data format.

For usage information see the [ringserver manual](https://github.com/EarthScope/ringserver/tree/main/doc)
in the 'doc' directory.

## Ringserver container image

Images are created for recent releases and published in Docker Hub:

[https://hub.docker.com/r/earthscope/ringserver](https://hub.docker.com/r/earthscope/ringserver)

## Quick Start

To run the default configuration of ringserver with Docker which listens locally
on ports 16000 and 18000:

```
docker run -ti --network host -v ringserver-data:/data earthscope/ringserver:latest
```

The above configuration uses host networking to allow the default `localhost`
address to submit data to the server. This is only known to reliably work on
Linux hosts. For macOS and Windows, consider using port mapping instead:

```
docker run -ti -p 16000:16000 -p 18000:18000 -v ringserver-data:/data earthscope/ringserver:latest
```

Note: these example commands use a named volume of `ringserver-data` to store
the server state for persistence between re-starts and containers.  Docker will
create and manage that storage.

### Persisting server contents

In normal use the server contents and other state information are saved when the
server is shutdown, for system reboots, upgrades, etc.  Without further
configuration the state is saved inside of the running container, and will be
lost if the container is removed and cannot be used with another container (for
a new version of ringserver, etc.).  It is strongly recommended to use a storage
[volume](https://docs.docker.com/storage/volumes/) independent of the container
for most use of ringserver.  Options include:

1) [Recommended] A named volume managed by Docker.  This is the most robust and
   strongly recommended approach.  This is illustrated in the Quick Start
   examples.

2) A bind mount from a designated directory on the host system.  This should
   only be used when a named volume is not possible as it is not robust due to
   multiple storage system layers.  The designated directory on the host system
   must be owned by or writable by user ID 10000 (the non-root user that runs
   ringserver in the container).

The external volume must be mounted in the container at `/data`.

## Configuration options

Configuration options may be specified in three ways, in order of precedence:
command line options, environment variables, and a configuration file.

To view ringserver command line options:

```
docker run --rm earthscope/ringserver -h
```

To print a reference configuration file, including descriptions for each
parameter and their environment variable equivalents:

```
docker run --rm earthscope/ringserver -C
```

The environment variables documented in the reference configuration file may be
specified in a docker run using the `-e` option.  These can be combined with
command line options if desired.

For example to run ringserver with:
- a 10 mebibyte ring buffer
- listening on port 8080 for HTTP connections
- setting server description to "My Data Center"

```
docker run -ti --network host -v ringserver-data:/data -e RS_RING_SIZE=10m -e RS_HTTP_PORT=8080 earthscope/ringserver -I "My Data Center"
```

### Building a container image from source

A Dockerfile is included for building a ringserver image.

To build an image in a cloned repository of the source code:

```
docker build -t ringserver:latest .
```

## Licensing

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright (C) 2025 Chad Trabant, EarthScope Data Services

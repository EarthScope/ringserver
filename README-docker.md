# ringserver - Generic packet ring buffer with network interfaces

This program is a generic, stream-oriented packet ring buffer.  The primary
use case is to transport packetized snippets of time series data.  The
supported protocols are all TCP-based: DataLink, SeedLink and HTTP/WebSocket.

The ring buffer is implemented as a FIFO with newly arriving packets pushing
older packets out of the buffer.  The ring packets are not format specific
and may contain any type of data.

Some features are specific to miniSEED, the standard data format in seismology,
such as the SeedLink protocol, built-in miniSEED file scanner and built-in
miniSEED archiver.  These are format-specific enhancements built on the core
transport that is agnostic to data format.

For usage information see the [ringserver manual](https://github.com/EarthScope/ringserver/tree/main/doc)
in the 'doc' directory.

## Ringserver container image

Images are created for recent releases and published in Docker Hub:

[https://hub.docker.com/r/earthscope/ringserver](https://hub.docker.com/r/earthscope/ringserver)

## Quick start

To run the default configuration of ringserver with Docker which listens locally on ports 16000 and 18000:

```
docker run -ti --network host -p 16000:16000 -p 18000:18000 earthscope/ringserver:latest
```

The above configuration uses host networking in order to allow the default `localhost` address
to submit data to the server.  This is only know to reliably work on Linux hosts, your
mileage may vary on macOS, Windows and other hosts.

If you want the ring buffer to persist across container re-creations, you will need to attach a [volume](https://docs.docker.com/storage/volumes/) to store the ring files. This can be done in several ways. Below illustrates using a subdirectory in the current working directory. Note that by default the container runs ringserver as user id 10000 so the directory on the local file system where the ring files are stored needs to be writable by that user id.

```
mkdir -p ring
sudo chown -R 10000 ring
docker run -ti --network host -v ${PWD}/ring:/data/ring earthscope/ringserver:latest
```

Note: Docker for macOS does not allow for volumes to be bound to containers by default. This default configuration can be changed in the "File Sharing" section of the Docker settings, in which a user can specify directories that can be bound to docker containers. If you are receiving the error:
```
Error response from daemon: error while creating mount source path '/host_mnt/PATH/ringserver/ring': mkdir /host_mnt/PATH: operation not permitted
```
verify that the directories used for volumes can be bound to a docker container.

## Configuration options

Configuration options may be specified in three ways, in order of precedence: command line options, environment variables, and a configuration file.

To view ringserver command line options:

```
docker run --rm earthscope/ringserver -h
```

To print a reference configuration file, including descriptions for each parameter
and their environment variable equivalents:

```
docker run --rm earthscope/ringserver -C
```

The environment variables documented in the reference configuration file may be
specified in a docker run using the `-e` option.  These can be combined with command
line options if desired.

For example to run ringserver with:
- a 10 mebibyte ring buffer
- listen on port 8080 for HTTP connections
- Setting server description to "My Data Center"

```
docker run -ti --network host -e RS_RING_SIZE=10m -e RS_HTTP_PORT=8080 earthscope/ringserver -I "My Data Center"
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

Copyright (C) 2020 Chad Trabant, EarthScope Data Services

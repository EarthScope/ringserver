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

For usage infromation see the [ringserver manual](https://github.com/EarthScope/ringserver/tree/main/doc) in the
'doc' directory.

## Download release versions

The [releases](https://github.com/EarthScope/ringserver/releases) area contains
release versions for download.

## Building and Installing 

In most Unix/Linux environments a simple 'make' will build the program.

The CC and CFLAGS environment variables can be used to configure
the build parameters.

SunOS/Solaris:
 
In order to compile under Solaris the 'src/Makefile' needs to be edited.
See the Makefile for instructions.

For further installation simply copy the resulting binary and man page
(in the 'doc' directory) to appropriate directories.

## Ringserver Container Image

If you prefer to work with ringserver as a container, that is fully supported.
Recent releases have containers available publicy as:

[public.ecr.aws/earthscope/ringserver](https://gallery.ecr.aws/earthscope/ringserver)

To pull this image with Docker and tag it locally as "ringserver" for convenience:

```
docker pull public.ecr.aws/earthscope/ringserver:latest
docker tag public.ecr.aws/earthscope/ringserver:latest ringserver:latest
```

To run the default configuration of ringserver with Docker which listens locally on ports 16000 and 18000:

```
docker run -p 16000:16000 -p 18000:18000 ringserver
```

If you want the ring contents to persist container recreations, you will need to attach a [volume](https://docs.docker.com/storage/volumes/) to store the ring files. This can be done in several ways. Below illustrates using a subdirectory in the current working directory. Note that by default the container runs ringserver as user id 10000 so the directory on the local file system where the ring files are stored needs to be writable by that user id.

```
mkdir -p ring
sudo chown -R 10000 ring
docker run -p 16000:16000 -p 18000:18000 -v ${PWD}/ring:/data/ring ringserver
```

### Container Image Versioning

The container image version tag uses the same version as ring server with an additional ".n" suffix that indicates the build number of the container.
For example: `2020.075.2` is the second build of the container for ringserver version 2020.075.
The most recent build of the most recent version will be tagged `latest`.

### Command Line Options

To view all ringserver command line options:

```
docker run ringserver -h
```

All ringserver command line options are supported as either options passed to `docker run` as demonstrated with "-h" above, or environment variables that can be passed to the container when run.
See the entrypoint.sh script for the currnet mapping of command line options to the names of the environmental variables.
An example of running ringserver with 1 MB ring file instead of the default 1 GB using an environment variable:

```
mkdir -p ring
sudo chown -R 10000 ring
docker run -p 16000:16000 -p 18000:18000 -v ${PWD}/ring:/data/ring -e RING_SIZE=1048576 ringserver
```

### Running with a Custom Configuration File

Some configuration settings are not avalable via command line options and must be specified in a configuration file.
The default configuration that is built into the container is located in doc/ring.conf in this repository.
To specify a different configuration file stored on the local file system, you can combine volumes and environment variables described above:

```
mkdir -p local_config
cp -a doc/ring.conf local_config/
mkdir -p ring
sudo chown -R 10000 ring
docker run -p 16000:16000 -p 18000:18000 -v ${PWD}/ring:/data/ring -v ${PWD}/local_config/ring.conf:/ring.conf ringserver
```


### Building a Container Image Locally

A Dockerfile is included for building a container to run ringserver in.
To build:

```
docker build -t ringserver:latest .
```

### Running Locally with Compose

A `compose.yml` file is included for running ringserver locally using Docker Compose.
This will create a compose service that listens on ports 16000 and 18000 and writes the ring file to the ./ring directory where the docker compose command is run.
To start it:

```
mkdir -p ring
sudo chown -R 10000 ring
docker compose up -d
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

Copyright (C) 2020 Chad Trabant, IRIS Data Management Center

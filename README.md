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

For usage infromation see the [ringserver manual](doc/ringserver.md) in the
'doc' directory.

## Download release versions

The [releases](https://github.com/iris-edu/ringserver/releases) area contains
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

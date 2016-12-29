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

GNU GPL version 3.  See included LICENSE file for details.

Copyright (c) 2016 Chad Trabant

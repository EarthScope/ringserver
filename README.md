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

## Download release versions of source code

The [releases](https://github.com/EarthScope/ringserver/releases) area contains
release versions for download.

## Building and installing

In most Unix/Linux environments a simple 'make' will build the program.
A C99 compliant compiler and GNU make are required.

The `CC` and `CFLAGS` environment variables can be used to configure
the build parameters.

To installation simply copy the resulting binary and man page
(in the 'doc' directory) to appropriate directories.

## Ringserver container image

Container images are published for recent versions.  For import information see
[README-docker.md](README-docker.md)

## Quick start

The `ringserver` program can be configured in three ways, in order of
precedence: command line options, environment variables, and a configuration
file.

To view all ringserver command line options:

```
ringserver -h
```

To print a reference configuration file, including descriptions for each parameter
and their environment variable equivalents:

```
ringserver -C
```

A minimal ringserver instance can be started with default options with
the following commands:

```
mkdir ring
ringserver -Rd ring -L 18000 -DL 16000
```

which creates an empty `ring` directory and specifies that directory as the
ring buffer location using the `-Rd` argument.  The `-L` argument configures
a listener on port 18000 for all supported protocols (SeedLink, DataLink, HTTP).
The `-DL` argument configures a listening on port 16000 for DataLink connections,
commonly used for clients submitting data.

There are two mechanisms for submitting data packets to a server:
1. Via the TCP-based DataLink protocol (by default port 16000)
2. Via a built-in scanner for miniSEED data on the local file system

### Submitting data via the network using DataLink

For DataLink protocol submission, the server must be configured to allow
write-permission for connections on specified IP addresses.  By default `localhost`
is allowed to submit data.  The config file option `WriteIP` or environment
variable `RS_WRITE_IP` can be used to specify alternate host(s) allowed to
submit data.

Some programs exist to submit data from different sources, the most common are:

[slink2dali](https://github.com/EarthScope/slink2dali) for collecting packets from
a SeedLink server and submitting them to a ringserver.

[ew2ringserver](https://gitlab.com/seismic-software/earthworm/-/tree/master/src/data_exchange/ew2ringserver) for [Earthworm](http://www.earthwormcentral.org/)

[orb2ringserver](https://github.com/EarthScope/orb2ringserver) for [BRTT's Antelope](https://brtt.com/software/)

[miniseed2dmc](https://github.com/EarthScope/miniseed2dmc) for sending miniSEED in files.
Useful for build transfer and not intended for real-time streaming.

[libdali](https://github.com/EarthScope/libdali) is a C-language library implementing
the DataLink protocol including submission.

[simpledali](https://github.com/crotwell/simpledali) is a Python library implementing
the DataLink protocol including submission.

### Scanning for miniSEED from

A built-in scanner can be configured to scan files on the local file system
that contain miniSEED records.  This capability is configured with either:
- `-MSSCAN` command line option
- `RS_MSEED_SCAN` environment variable
- `MSeedScan` config file parameter

See the man page and reference config file for documentation.

### Querying the server

The [dalitool](https://github.com/earthscope/dalitool) program can be used
for diagnostic testing and general queries to a ringserver using the DataLink protocol.

The [slinktool](https://github.com/earthscope/slinktool) program can also be used
for diagnostic testing and general queries to a ringserver using the SeedLink protocol.

If HTTP protocol support is enabled on any ports, the server may also be queried
using a web browser at the following URL paths:

```
  /id           - Server identification
  /id/json      - Server identification in JSON
  /streams      - List of available streams with time range
  /streams/json - List of available streams with time range in JSON
  /streamids    - List of available streams, variable levels
  /status       - Server status, limited access*
  /status/json  - Server status in JSON, limited access*
  /connections  - List of connections, limited access*
  /connections/json - List of connections in JSON, limited access*
  /seedlink     - Initiate WebSocket connection for SeedLink
  /datalink     - Initiate WebSocket connection for DataLink
```

## Encrypted connections with TLS

The ringserver program supports encrypted connections using TLS.  To configure TLS,
certificate and key files must be specified that contain a certificate (with a public
key) and a private key.  These are specified using the `TLSCertFile`
(or `RS_TLS_CERT_FILE` envvar) and `TLSKeyFile` (or `RS_TLS_KEY_FILE` envvar) config file
options.

For public servers, the certificate should be signed by a trusted Certificate Authority
or clients will not be likely to trust the certificate.

For servers in closed environments, or for testing, a self signed certificate may be
used for the server and the clients. Such a certificate (and matching key) can be
generated with the [openssl](https://www.openssl.org/) program like this:

```
openssl req -x509 -nodes -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 \
    -subj "/C=XX/ST=State/L=City/O=Organization/OU=OrgUnit/CN=localhost"
```

These can then be used with ringserver like this:

```
RS_TLS_CERT_FILE=cert.pem TLS_KEY_FILE=key.pem ringserver -SL '18500 TLS' -DL 16000 -v
```

The `cert.pem` file can be used as a Certificate Authority for clients, e.g. with
[slinktool](https://github.com/earthscope/slinktool):

```
LIBSLINK_CA_CERT_FILE=cert.pem slinktool localhost:18500 -F ID
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

Copyright (C) 2024 Chad Trabant, EarthScope Data Services

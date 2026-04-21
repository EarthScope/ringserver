# ringserver — stream oriented packet ring buffer

- [ringserver — stream oriented packet ring buffer](#ringserver--stream-oriented-packet-ring-buffer)
  - [Synopsis](#synopsis)
  - [Description](#description)
  - [Options](#options)
  - [Config File Parameters](#config-file-parameters)
  - [Access Control](#access-control)
  - [Seedlink Support](#seedlink-support)
  - [Stream Ids](#stream-ids)
    - [Optional Stream ID Labels](#optional-stream-id-labels)
  - [Multi-Protocol Support](#multi-protocol-support)
  - [HTTP Support](#http-support)
  - [Usage Logging](#usage-logging)
    - [Transfer Logging](#transfer-logging)
    - [Access Logging](#access-logging)
  - [External Packet Ids](#external-packet-ids)
  - [Miniseed Archiving](#miniseed-archiving)
  - [Miniseed Scanning](#miniseed-scanning)
  - [Author](#author)

## <a id="synopsis">Synopsis</a>

```
ringserver [options] [configfile]
```

## <a id="description">Description</a>

<b>ringserver</b> is a streaming data server with support for SeedLink, DataLink and HTTP protocols.

The implementation is based on a ring buffer design that operates on a first-in-first-out basis with newly arriving packets pushing older packets out of the buffer. The packet payloads are not format specific and may contain any type of data.  All communications are performed via TCP interfaces.  Data streams are available to clients using the SeedLink and DataLink protocols and submitted to the server using the DataLink protocol.  WebSocket connections are supported for the SeedLink and DataLink protocols.  General server status is available via HTTP in JSON or formatted summaries.

The server is configured either with options on the command line, through environment variables, and/or by using a <b>ringserver</b> config file.  The order of precedence for configuration options is command line, environment variables, and then config file.

Only the most common options are available on the command line, all options are controllable via environment variables and the config file. Detailed descriptions for each option are included in the example config file that accompanies the source code.  Many options are dynamic, meaning that they can be changed while the server is running when using a config file.  The server will recognize that the config file has changed and re-read its configuration.  This is especially useful for updating access controls, logging verbosity and other logging parameters without restarting the server.

In normal operation packet buffer contents are saved in files when the server is shut down making the server stateful across restarts.  By default the packet buffer is managed as a memory-mapped file. The buffer can optionally be maintained completely in system memory, only reading and writing the buffer contents on startup and shutdown (useful in environments where memory-mapping is not possible).

Client access is controlled using IP addresses.  Controls include match, reject, limit, write and trust permissions. See <b>Access Control</b> for more details.

Usage logs can optionally be written to track server activity.  Transfer logs (TX/RX) record the number of data packet bytes of each unique stream transferred to or from each client connection.  Access logs record connection events and key commands (connect, disconnect, INFO requests, data streaming starts, and HTTP requests) in JSON Lines format.

The server supports streaming data with multiple protocols: SeedLink, DataLink, HTTP with WebSocket.  The server can listen on multiple network ports, and each port can be configured to support any combination of the protocols. See <b>Multi-protocol Support</b> for more information.

The server also has limited support for simple HTTP requests.  When support is enabled, server status, stream lists and other details can be accessed with simple HTTP requests. See <b>HTTP Support</b> for more details.

The dalitool(1) and slinktool(1) programs can be used to query the ringserver for various information via the DataLink and SeedLink interfaces respectively.  The dalitool program was developed in parallel with ringserver and the DataLink protocol and is the recommended query tool for ringserver admins.

## <a id="options">Options</a>

- <b>-V</b>
  Print the program version and exit.

- <b>-h</b>
  Print the program help/usage and exit.

- <b>-H</b>
  Print an extended help/usage and exit.

- <b>-C</b>
  Print a ringserver configuration file and exit.  The output contains descriptions of all available configuration parameters and the environment variables that can be used to set them.

- <b>-v</b>
  Be more verbose.  This flag can be used multiple times ("-v -v" or "-vv") for more verbosity.

- -I <i>ServerID</i>
  Server ID reported to the clients.  The value may be a quoted string containing spaces.  The default value is "Ring Server".

- -M <i>maxclientsperIP</i>
  Maximum number of concurrently connected clients per IP address.  This limit does not apply to addresses with write permission.  There is no default maximum.

- -m <i>maxclients</i>
  Maximum number of concurrently connected clients.  The default maximum is 600.

- -Rd <i>ringdir</i>
  Base directory for the server to store the packet buffer data files. This parameter must be specified via this option, environment variable, or config file; there is no default.

- -Rs <i>bytes</i>
  Size of the packet buffer in bytes, default size is 1 GiB.  The size of the ring, in combination with the ring packet size, determine how much past data is available in the buffer.  If the server is configured to memory-map the packet buffer files the maximum size of the ring is limited to how large a file the host system can support.  If the server is configured to maintain the ring in system memory the size is limited to system memory.

- -Rp <i>packetsize</i>
  Maximum ring packet data size (not including packet buffer header), default is 512 bytes.

- <b>-NOMM</b>
  No memory-mapping, maintain the ring buffer in system memory only.  On startup the ring buffer files will be read into memory and on shutdown the memory buffers will be written back to the files.  This option might be useful in environments where memory-mapping of files is not possible or is unreliable or slow (e.g. network storage).

- -L <i>port</i>
  Network port to listen for incoming connections on.  By default the server will not listen for connections, setting at least one listen port is needed to communicate with the server.  By default, a listening port will accept all supported protocols.  Restricting a port to only allow specific protocols can be done using a configuration file or adding flags to the port declaration, see <b>Multi-protocol Support</b> for more information.

- -SL <i>port</i>

- -DL <i>port</i>

- -HL <i>port</i>
  These options are shortcuts for configuring a listening port for only SeedLink, DataLink or HTTP protocols respectively.

- -U <i>logdir</i>
  Usage log base directory.  By default the server does not write usage logs.  If a directory is specified, transmission (TX), reception (RX), and access logs are all written by default; these can be toggled individually with the <b>UsageLogTX</b>, <b>UsageLogRX</b>, and <b>UsageLogAccess</b> config file parameters. The <b>-T</b> flag is accepted as a backward-compatible alias.

- -Ui <i>hours</i>
  Usage log writing interval in hours, default interval is 24 hours. The interval always starts at day boundaries, for example if the interval is set to 2 hours then the first interval in a given day will cover hours 0 through 2, the next 2 through 4, etc. The <b>-Ti</b> flag is accepted as a backward-compatible alias.

- -Up <i>prefix</i>
  Usage log file prefix, by default no prefix is added to the fixed section of the log file name. The <b>-Tp</b> flag is accepted as a backward-compatible alias.

- <b>-Uj</b>
  Enable JSON Lines format for transfer logs.  When enabled, each TX and RX log file uses a <b>.jsonl</b> extension and contains one JSON object per client session describing the protocol, streams, and byte counts. This replaces the legacy text format for transfer logs. The <b>-Tj</b> flag is accepted as a backward-compatible alias.

- <b>-STDERR</b>
  Send all diagnostic output to stderr instead of stdout.  This is useful in situations where logging output is captured by another program on stderr and to separate ringserver diagnostics from other output on the console.

- -MSWRITE <i>format</i>
  A special mode of ringserver is to write all miniSEED data records received via DataLink to a user defined directory and file structure. See <b>miniSEED Archiving</b> for more details.

- -MSSCAN <i>directory</i> [suboptions]
  A special mode of ringserver is to recursively scan a directory for files containing miniSEED formatted data records and insert them into the buffer.  Optional suboptions control scanning behavior, the StateFile suboption is highly recommended. See <b>miniSEED Scanning</b> for more details.

- <b>-VOLATILE</b>
  Create a volatile (non-stateful) ring buffer. Do not read or write packet buffer contents from ring files.  This is useful in combination with the -MSWRITE option when no stateful buffer is needed or scenarios where the buffer is not needed after a restart.

## <a id="config-file-parameters">Config File Parameters</a>

All of the command line parameters have config file and environment variable equivalents.  Many of the config file parameters are dynamic, if they are changed the server will re-read its configuration on the fly. See the detailed parameter descriptions in the documented example config file.

## <a id="access-control">Access Control</a>

Access control is based on IP addresses and user authentication. Authentication is optional and can be used in combination with IP address based access control.  Authentication can be required for clients requesting streaming data using the <b>AuthRequiredForStreams</b> config parameter, or <b>S_AUTH_REQUIRED_FOR_STREAMS</b> environment variable.

The IP-based access control is specified in the config file using the following parameters:

```
  \fBAcceptIP\fP or \fBRS_ACCEPT_IP\fP
  \fBDenyIP\fP or \fBRS_DENY_IP\fP
  \fBAllowedStreamsIP\fP or \fBRS_ALLOWED_STREAMS_IP\fP
  \fBForbiddenStreamsIP\fP or \fBRS_FORBIDDEN_STREAMS_IP\fP
  \fBWriteIP\fP or \fBRS_WRITE_IP\fP
  \fBTrustedIP\fP or \fBRS_TRUSTED_IP\fP
```

By default all clients are allowed to connect.  Specific clients can be rejected using the <b>DenyIP</b> config parameter.  If any <b>AcceptIP</b> config parameters are specified only addresses that match one of the entries, and are not rejected, are allowed to connect.

By default all clients are allowed access to all streams in the buffer, and clients with write permission are allowed to write any streams.  Specific clients can be limited to access or write subsets of streams using the <b>AllowedStreamsIP</b> config parameter.  Specific clients can be forbidden from accessing subsets streams using the <b>ForbiddenStreamsIP</b> config parameter. These parameters accept a regular expression that is used to match stream IDs that the client(s) are allowed or forbidden.

By default all clients are allowed to request the server ID, simple status and list of streams.  Specific clients can be allowed to access connection information and more detailed status using the <b>TrustedIP</b> access control.

If no client addresses are granted write permission via <b>WriteIP</b> or granted trusted status via <b>TrustedIP</b> then the 'localhost' address (local loopback) are granted those permissions.

Access control is host range (network) based, and specified as an address followed by an optional prefix in CIDR notation.  For example: "192.168.0.1/24" specifies the range of addresses from 192.168.0.1 to 192.168.0.254.  The address may be a hostname, which will be resolved on startup.  The prefix is optional and, if omitted, defaults to specifying only the single address.

The authentication-based access control is specified in the config file using the following parameters:

```
  \fBAuthCommand\fP or \fBRS_AUTH_COMMAND\fP
  \fBAuthTimeout\fP or \fBRS_AUTH_TIMEOUT\fP
```

The <b>AuthCommand</b> config parameter specifies a command to be executed when a client connects to the server and requests authentication.  The command is executed with the USERNAME and PASSWORD environment variables set to the values provided by the client.  The command should return a JSON formatted object on stdout with the following optional keys:

```
{
  "authenticated": <boolean>,
  "write_permission": <boolean>,
  "trust_permission": <boolean>,
  "allowed_streams": ["streamregex1", "streamregex2"],
  "forbidden_streams": ["streamregex3", "streamregex4"]
}
```

The <b>authenticated</b> key must be present and set to true for the client to be allowed to connect.  Missing keys are treated as false or empty.

## <a id="seedlink-support">Seedlink Support</a>

The legacy SeedLink protocol (v3) only transmits 512-byte miniSEED data records.  This server is able to transmit miniSEED records of any length via SeedLink.  If you wish to ensure compatibility with legacy clients, only 512-byte miniSEED records should be submitted to the server.

This server supports the wild-carding of network and station codes during SeedLink negotiation using the '?' and '*' characters for single or multiple character matches respectively.  Not all SeedLink clients support wild-carded network and station codes.

SeedLink v4 clients can additionally use filters to selected streams with labels.  See <b>Optional stream ID labels</b> for more details.

## <a id="stream-ids">Stream Ids</a>

Each unique data stream is identified by a stream ID.  The stream ID can be arbitrary but is commonly a combination of a data source identifier and a suffix (separated by a slash) that identifies the the payload type.  For example:

"FDSN:IU_COLA_00_B_H_Z/MSEED"

For the SeedLink protocol support, data source IDs must be valid FDSN Source IDs (https://docs.fdsn.org/projects/source-identifiers) with a format suffix of "/MSEED" or "/MSEED3".

The stream ID suffix recommendations are as follows:

```
  \fBMSEED\fP   : miniSEED v2 data records
  \fBMSEED3\fP  : miniSEED v3 data records
  \fBJSON\fP    : JSON payloads
  \fBTEXT\fP    : Text payloads, where UTF-8 is assumed
```

The maximum length of stream IDs supported by the server is 63 bytes.

### <a id="stream-ids-optional-stream-id-labels">Optional Stream ID Labels</a>

A stream ID may carry an optional label suffix, producing IDs of the form:

"FDSN:IU_COLA_00_B_H_Z/MSEED3/1SEC"

Labels are used to partition streams that would otherwise share the same FDSN Source ID (for example, multiple variants of the same channel produced by different processing paths).  Labels may be up to 11 characters long and may contain letters, digits, underscore '_' and hyphen '-'.  The tokens <b>native</b> and <b>3</b> are reserved SeedLink v4 filter keywords and cannot be used as label values.

Labeled streams are <b>not</b> served to SeedLink clients by default: a client must explicitly request them using the SeedLink v4 SELECT syntax with a <b>:LABEL</b> suffix, for example:

```
  \fBSELECT *_B_H_?:1SEC\fP
```

The '?' and '*' glob wildcards are accepted in the label portion.  A SELECT without a <b>:LABEL</b> suffix matches only unlabeled streams for the given pattern.

The sole exception is "all-station" mode: a SeedLink client that starts data flow without issuing any STATION or SELECT commands receives every packet in the ring, including labeled streams, because no stream filter is applied.

DataLink clients match stream IDs directly with client-supplied regular expressions, so labels are part of the stream ID text and are included or excluded by whatever regex the client supplies.

## <a id="multi-protocol-support">Multi-Protocol Support</a>

Network listening ports can respond to all supported protocols: SeedLink, DataLink and HTTP/WebSocket.  If more than one protocol is configured for a port, the first command received by the server is used to determine which protocol is being used by the client, all subsequent communication is expected in this protocol.

Both IPv4 and IPv6 protocol families are supported by default (if supported by the system).

The network protocols and families allowed by any given listening port can be set by adding flags to the port specification.  See the available flags in the <b>ListenPort</b> description of the reference config file printed using the <b>-C</b> command line option.

The <b>PROXYv2</b> flag enables support for the HAProxy PROXY protocol version 2. When this flag is set, the server will expect every client connection on that port to send a valid PROXY protocol v2 header before any actual protocol data. This allows the server to determine the true source address and port of the connecting client, as often required when the traffic flows through a trusted proxy or load balancer. <b>Important:</b> The PROXY protocol should only be enabled on ports that are exclusively reachable by trusted proxies since the client may specify any IP address in the PROXY header, potentially spoofing their source address. Do not use the PROXYv2 flag on publicly accessible ports.

The <b>TRUSTED</b> flag grants trusted status to all clients connecting on a port, allowing access to detailed server status and connection information.  <b>WARNING:</b> Do not use this flag on publicly accessible ports as it grants elevated access to all connecting clients regardless of their IP address.

Examples of adding flags to a port specification:

```
  \fB-L "18000 SeedLink HTTP"\fP        : CLI, SeedLink and HTTP on port 18000
  \fB-SL "18500 TLS IPv4"\fP            : CLI, SeedLink via TLS on port 18500, IPv4 only
  \fBRS_LISTEN_PORT="8080 HTTP IPv6"\fP : EnvVar, HTTP on port 8080, IPv6 only
  \fBListenPort 16000 DataLink\fP       : Config file, DataLink on port 16000
  \fBListenPort 14000 TRUSTED\fP        : Config file, all protocols trusted on port 14000
  \fBListenPort 18000 PROXYv2\fP      : Config file, all protocols with PROXYv2 on port 18000
```

## <a id="http-support">HTTP Support</a>

The server will respond to HTTP requests for a few fixed resources. If the <b>WebRoot</b> config parameter is set to a directory, the files under that directory will also be served when requested through the HTTP GET method.  Except for the fixed resources, the HTTP server implementation is limited to returning existing files and returning "index.html" files when a directory is requested.

The following fixed resources are supported:

```
  \fB/id\fP           - Server identification
  \fB/id/json\fP      - Server identification in JSON
  \fB/streams\fP      - List of available streams with time range
  \fB/streams/json\fP - List of available streams with time range in JSON
  \fB/streamids\fP    - List of available streams
  \fB/status\fP       - Server status, limited access*
  \fB/status/json\fP  - Server status in JSON, limited access*
  \fB/connections\fP  - List of connections, limited access*
  \fB/connections/json\fP - List of connections in JSON, limited access*
  \fB/seedlink\fP     - Initiate WebSocket connection for Seedlink
  \fB/datalink\fP     - Initiate WebSocket connection for DataLink
```

Access to the <b>status</b> and <b>connections</b> information is limited to clients that have trusted permission.

The <b>streams</b>, <b>streamids</b> and <b>connections</b> endpoints accept a <i>match</i> parameter that is a regular expression pattern used to limit the returned information.  For the <b>streams</b> and <b>streamids</b> endpoints the matching is applied to stream IDs.  For the <b>connections</b> endpoint the matching is applied to hostname, client IP address and client ID. For example: http://localhost/streams?match=IU_ANMO.

After a WebSocket connection has been initiated with either the <b>seedlink</b> or <b>datalink</b> end points, the requested protocol is supported exactly as it would be normally with the addition of WebSocket framing.  Each server command, including terminator(s), should be contained in a WebSocket frame.

Custom HTTP headers may be included in HTTP responses using the <b>HTTPHeader</b> config file parameter.  This can be used, for example, to enable cross-site HTTP requests via Cross-Origin Resource Sharing (CORS).

## <a id="usage-logging">Usage Logging</a>

Usage logging covers two distinct log types: transfer logs (TX/RX) that track data volume per stream per client, and access logs that record connection events and key commands.  All log types share the same base directory, file prefix, and rotation interval, configured via the <b>-U</b> command line option or the <b>UsageLogDirectory</b> config file parameter (the <b>TransferLogDirectory</b> alias is also accepted for backward compatibility).  All three log types (TX, RX, and access) are enabled by default when a usage log directory is specified.

### <a id="usage-logging-transfer-logging">Transfer Logging</a>

The <b>UsageLogTX</b> and <b>UsageLogRX</b> config file parameters (or their <b>TransferLogTX</b> / <b>TransferLogRX</b> aliases, or equivalent environment variables) control logging of data transmitted or received.  By default both TX and RX logging are enabled when a log directory is set.  The log interval and file name prefix can be changed via the <b>-Ui</b> and <b>-Up</b> command line options.

Log files are named with the interval time window, for example:

```
  \fBtxlog-20260316T0000-20260317T0000\fP
  \fBrxlog-20260316T0000-20260317T0000\fP
```

Entries are only written for clients that transmitted or received data; clients with no activity in a given direction are omitted from the respective log file.

By default transfer logs use a legacy text format.  The <b>-Uj</b> command line option or the <b>UsageLogJSONLines</b> config parameter enables JSON Lines format instead, where each line is a JSON object containing protocol details, per-stream byte counts, and client metadata.

In text format, each TX or RX log file contains entries with this pattern:

1) A "START CLIENT" line containing the host name, IP address, protocol, client ID, log time, and connection time.

2) One or more data lines of the following form:

```
\fB[Stream ID] [bytes] [packets]\fP
```

3) An "END CLIENT" line including the total bytes for this entry.

Note: the byte counts are the sum of the data payload bytes in each packet and do not include the DataLink or SeedLink protocol headers.

An example "TX" file illustrating a transmission entry in legacy text format:

```
START CLIENT host.iris.edu [192.168.255.255] (SeedLink|slinktool/4.11) @ 2018-03-30 07:00:05 (connected 2018-03-30 06:59:36) TX
FDSN:IU_SNZO_10_B_H_Z/MSEED 2560 5
FDSN:IU_SNZO_00_B_H_Z/MSEED 2048 4
END CLIENT host.iris.edu [192.168.255.255] total TX bytes: 4608
```

### <a id="usage-logging-access-logging">Access Logging</a>

Access logging is enabled by default when <b>UsageLogDirectory</b> (or the <b>-U</b> option) is set.  It can be disabled with <b>UsageLogAccess 0</b> in the config file or the <b>RS_USAGE_LOG_ACCESS</b> environment variable.

Access log files are always written in JSON Lines format, one JSON object per line, with a <b>.jsonl</b> extension.  Files are named:

```
  \fBaccesslog-20260316T0000-20260317T0000.jsonl\fP
```

Each access log record contains the event time, client metadata (IP address, hostname, server port, user agent), authentication details if applicable, protocol information (name, version, TLS, WebSocket), and the event or command details.

The following events are logged:

```
  \fBconnect\fP    - Client TCP connection established
  \fBdisconnect\fP - Client connection closed
  \fBcommand\fP    - Key protocol command received
```

Key commands recorded include:

```
  \fBINFO\fP         - SeedLink or DataLink INFO request (with item/type)
  \fBDATA\fP/\fBFETCH\fP  - SeedLink data streaming request (with stream selection criteria)
  \fBSTREAM\fP       - DataLink streaming start (with stream selection criteria)
  \fBGET\fP          - HTTP GET request (with path)
```

For DATA/FETCH and STREAM commands, the <b>match</b> and <b>reject</b> fields in the JSON record contain the regular expressions used to select streams, if any were specified by the client.

An example access log record:

```
{"log_time":"2026-03-16T12:00:00Z","connect_time":"2026-03-16T11:59:55Z",
 "client":{"ip":"192.168.1.10","server_port":18000,"hostname":"host.example.com",
           "user_agent":"slinktool/4.1"},
 "protocol":{"name":"SeedLink","version":"4.0"},
 "event":"command","command":"DATA","match":"FDSN:IU_.*",
 "service":{"name":"ringserver","version":"4.3.1"}}
```

## <a id="external-packet-ids">External Packet Ids</a>

With the DataLink v1.1 protocol a client may submit packets with a specified packet ID to use instead of a generated ID.  This is useful to implement multiple servers that share common packet IDs for use with a network load balancer, such that it does not matter to which server a client connects.

These packet IDs are used in the SeedLink and DataLink protocols by clients to track and resume data streams.  In SeedLink these are called sequence numbers.

For ringserver, packet IDs, aka sequence numbers, must be between 0 and (UINT64_MAX - 10), or 18446744073709551605.  The last 10 values of the uint64 range are reserved for internal use to indicate special conditions.  These values are not expected to be encountered in typical data streaming operation.

Furthermore, external IDs submitted with packets are strongly recommended to be unique and monotonically increasing.  Such a sequence of IDs support efficient data stream resumption and tracking.

## <a id="miniseed-archiving">Miniseed Archiving</a>

Using either the <b>-MSWRITE</b> command line option or the <b>MSeedWrite</b> config file parameter the server can be configured to write all miniSEED data records received via DataLink to a user defined directory and file structure.

The archive <i>format</i> argument is expanded for each packet processed using the following flags:

```
  \fBn\fP : network code, white space removed
  \fBs\fP : station code, white space removed
  \fBl\fP : location code, white space removed
  \fBc\fP : channel code, white space removed
  \fBq\fP : record quality indicator (D,R,Q,M), single character
  \fBY\fP : year, 4 digits
  \fBy\fP : year, 2 digits zero padded
  \fBj\fP : day of year, 3 digits zero padded
  \fBH\fP : hour, 2 digits zero padded
  \fBM\fP : minute, 2 digits zero padded
  \fBS\fP : second, 2 digits zero padded
  \fBF\fP : fractional seconds, 4 digits zero padded
  \fBD\fP : current year-day time stamp of the form YYYYDDD
  \fBL\fP : data record length in bytes
  \fBr\fP : sample rate (Hz) as a rounded integer
  \fBR\fP : sample rate (Hz) as a float with 6 digit precision
  \fBh\fP : host name of client submitting data
  \fB%\fP : the percent (%) character
  \fB#\fP : the number (#) character
```

The flags are prefaced with either the <b>%</b> or <b>#</b> modifier. The <b>%</b> modifier indicates a defining flag while the <b>#</b> indicates a non-defining flag.  All received packets with the same set of defining flags will be saved to the same file. Non-defining flags will be expanded using the values in the first packet received for the resulting file name.

Time flags are based on the start time of the given packet.

Files are created with (permission) mode 666 and directories are created with mode 777.  An operator of ringserver can control the final permissions of the files by adjusting the umask as desired.

Some preset archive layouts are available:

```
  \fBBUD\fP   : \fI%n/%s/%s.%n.%l.%c.%Y.%j\fP  (BUD layout)
  \fBCHAN\fP  : \fI%n.%s.%l.%c\fP  (channel)
  \fBQCHAN\fP : \fI%n.%s.%l.%c.%q\fP  (quality-channel-day)
  \fBCDAY\fP  : \fI%n.%s.%l.%c.%Y:%j:#H:#M:#S\fP  (channel-day)
  \fBSDAY\fP  : \fI%n.%s.%Y:%j\fP  (station-day)
  \fBHSDAY\fP : \fI%h/%n.%s.%Y:%j\fP  (host-station-day)
```

The preset archive layouts are used by prefixing a target directory with the preset identifier followed by an '@' character.  For example:

<b>BUD@/data/bud/</b>

would write a BUD like structure in the /data/bud/ directory.

Other example:

<b>/archive/%n/%s/%n.%s.%l.%c.%Y.%j</b>

would be expanded to day length files named something like:

<b>/archive/IU/ANMO/IU.ANMO..BHE.2003.055</b>

Using non-defining flags the format string:

<b>/data/%n.%s.%Y.%j.%H:#M:#S.miniseed</b>

would be expanded to:

<b>/data/IU.ANMO.2003.044.14:17:54.miniseed</b>

resulting in hour length files because the minute and second are specified with the non-defining modifier.  The minute and second fields are from the first packet in the file.

## <a id="miniseed-scanning">Miniseed Scanning</a>

Using either the <b>-MSSCAN</b> command line option or the <b>MSeedScan</b> config file parameter (or equivalent environment variable) the server can be configured to recursively scan a directory for files containing miniSEED data records and insert them into the buffer.  Intended for real-time data re-distribution, files are continuously scanned, newly added records are inserted into the buffer.

Sub-options can be used to control the scanning process.  The sub-options are specified on the same line as the scan directory as key-value pairs separated by an equals '=' character and may not contain spaces (because they are separated by spaces).  Do not use quotes for the values.  The available sub-options are:

```
  \fBStateFile\fP : File to save scanning state through restarts
  \fBMatch\fP : Regular expression to match file names
  \fBReject\fP : Regular expression to reject file names
  \fBInitCurrentState\fP : Initialize scanning to current state
  \fBMaxRecurse\fP : Maximum recursion depth (default is no limit)
```

Except for special cases the <b>StateFile</b> option should always be specified, otherwise a restart of the server could re-read data records that it has already read.

If the <b>InitCurrentState</b> option is set to '<b>y</b>' the scanning will only read new data, effectively skipping all the data discovered during the first scan, under the following conditions:

```
1) No StateFile has been specified
2) StateFile has been specified but does not exist
```

The <b>InitCurrentState</b> option is useful to avoid reading all existing data when starting a server scanning an existing large dataset.  It is also useful to reset the dataflow to current data after a lengthy downtime, simply remove the statefile(s) before starting the server.

To scan a data directory and save the scanning state to a StateFile configure the server with either a config file option or command line, respectively:

<b>MSeedScan /data/miniseed/ StateFile=/opt/ringserver/scan.state</b>

<b>-MSScan "/data/miniseed/ StateFile=/opt/ringserver/scan.state"</b>

To limit the scanning to file names matching a certain pattern use the Match option, e.g. files ending in ".mseed":

<b>MSeedScan /data/miniseed/ StateFile=/data/scan.state Match=.*\\.mseed$</b>

## <a id="author">Author</a>

```
Chad Trabant
EarthScope Data Services
```

---

*Generated from man page dated 2026/04/20.*

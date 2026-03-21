# <p >ringserver 
###  stream oriented packet ring buffer</p>

1. [Name](#)
1. [Synopsis](#synopsis)
1. [Description](#description)
1. [Options](#options)
1. [Config File Parameters](#config-file-parameters)
1. [Access Control](#access-control)
1. [Seedlink Support](#seedlink-support)
1. [Stream Ids](#stream-ids)
1. [Multi-Protocol Support](#multi-protocol-support)
1. [Http Support](#http-support)
1. [Usage Logging](#usage-logging)
1. [External Packet Ids](#external-packet-ids)
1. [Miniseed Archiving](#miniseed-archiving)
1. [Miniseed Scanning](#miniseed-scanning)
1. [Author](#author)

## <a id='synopsis'>Synopsis</a>

<pre >
ringserver [options] [configfile]
</pre>

## <a id='description'>Description</a>

<p ><b>ringserver</b> is a streaming data server with support for SeedLink, DataLink and HTTP protocols.</p>

<p >The implementation is based on a ring buffer design that operates on a first-in-first-out basis with newly arriving packets pushing older packets out of the buffer. The packet payloads are not format specific and may contain any type of data.  All communications are performed via TCP interfaces.  Data streams are available to clients using the SeedLink and DataLink protocols and submitted to the server using the DataLink protocol.  WebSocket connections are supported for the SeedLink and DataLink protocols.  General server status is available via HTTP in JSON or formatted summaries.</p>

<p >The server is configured either with options on the command line, through environment variables, and/or by using a <b>ringserver</b> config file.  The order of precedence for configuration options is command line, environment variables, and then config file.</p>

<p >Only the most common options are available on the command line, all options are controllable via environment variables and the config file. Detailed descriptions for each option are included in the example config file that accompanies the source code.  Many options are dynamic, meaning that they can be changed while the server is running when using a config file.  The server will recognize that the config file has changed and re-read its configuration.  This is especially useful for updating access controls, logging verbosity and other logging parameters without restarting the server.</p>

<p >In normal operation packet buffer contents are saved in files when the server is shut down making the server stateful across restarts.  By default the packet buffer is managed as a memory-mapped file. The buffer can optionally be maintained completely in system memory, only reading and writing the buffer contents on startup and shutdown (useful in environments where memory-mapping is not possible).</p>

<p >Client access is controlled using IP addresses.  Controls include match, reject, limit, write and trust permissions. See <b>Access Control</b> for more details.</p>

<p >Usage logs can optionally be written to track server activity.  Transfer logs (TX/RX) record the number of data packet bytes of each unique stream transferred to or from each client connection.  Access logs record connection events and key commands (connect, disconnect, INFO requests, data streaming starts, and HTTP requests) in JSON Lines format.</p>

<p >The server supports streaming data with multiple protocols: SeedLink, DataLink, HTTP with WebSocket.  The server can listen on multiple network ports, and each port can be configured to support any combination of the protocols. See <b>Multi-protocol support</b> for more information.</p>

<p >The server also has limited support for simple HTTP requests.  When support is enabled, server status, stream lists and other details can be accessed with simple HTTP requests. See <b>HTTP Support</b> for more details.</p>

<p >The dalitool(1) and slinktool(1)  programs can be used to query the ringserver for various information via the DataLink and SeedLink interfaces respectively.  The dalitool program was developed in parallel with ringserver and the DataLink protocol and is the recommended query tool for ringserver admins.</p>

## <a id='options'>Options</a>

<b>-V</b>

<p style="padding-left: 30px;">Print the program version and exit.</p>

<b>-h</b>

<p style="padding-left: 30px;">Print the program help/usage and exit.</p>

<b>-H</b>

<p style="padding-left: 30px;">Print an extended help/usage and exit.</p>

<b>-C</b>

<p style="padding-left: 30px;">Print a ringserver configuration file and exit.  The output contains descriptions of all available configuration parameters and the environment variables that can be used to set them.</p>

<b>-v</b>

<p style="padding-left: 30px;">Be more verbose.  This flag can be used multiple times ("-v -v" or "-vv") for more verbosity.</p>

<b>-I </b><i>ServerID</i>

<p style="padding-left: 30px;">Server ID reported to the clients.  The value may be a quoted string containing spaces.  The default value is "Ring Server".</p>

<b>-M </b><i>maxclientsperIP</i>

<p style="padding-left: 30px;">Maximum number of concurrently connected clients per IP address.  This limit does not apply to addresses with write permission.  There is no default maximum.</p>

<b>-m </b><i>maxclients</i>

<p style="padding-left: 30px;">Maximum number of concurrently connected clients.  The default maximum is 600.</p>

<b>-Rd </b><i>ringdir</i>

<p style="padding-left: 30px;">Base directory for the server to store the packet buffer data files. This parameter must be specified via this option, environment variable, or config file; there is no default.</p>

<b>-Rs </b><i>bytes</i>

<p style="padding-left: 30px;">Size of the packet buffer in bytes, default size is 1 GiB.  The size of the ring, in combination with the ring packet size, determine how much past data is available in the buffer.  If the server is configured to memory-map the packet buffer files the maximum size of the ring is limited to how large a file the host system can support.  If the server is configured to maintain the ring in system memory the size is limited to system memory.</p>

<b>-Rp </b><i>packetsize</i>

<p style="padding-left: 30px;">Maximum ring packet data size (not including packet buffer header), default is 512 bytes.</p>

<b>-NOMM</b>

<p style="padding-left: 30px;">No memory-mapping, maintain the ring buffer in system memory only.  On startup the ring buffer files will be read into memory and on shutdown the memory buffers will be written back to the files.  This option might be useful in environments where memory-mapping of files is not possible or is unreliable or slow (e.g. network storage).</p>

<b>-L </b><i>port</i>

<p style="padding-left: 30px;">Network port to listen for incoming connections on.  By default the server will not listen for connections, setting at least one listen port is needed to communicate with the server.  By default, a listening port will accept all supported protocols.  Restricting a port to only allow specific protocols can be done using a configuration file or adding flags to the port declaration, see <b>Multi-protocol Support</b> for more information.</p>

<b>-SL </b><i>port</i>

<b>-DL </b><i>port</i>

<b>-HL </b><i>port</i>

<p style="padding-left: 30px;">These options are shortcuts for configuring a listening port for only SeedLink, DataLink or HTTP protocols respectively.</p>

<b>-U </b><i>logdir</i>

<p style="padding-left: 30px;">Usage log base directory.  By default the server does not write usage logs.  If a directory is specified, both transmission (TX) and reception (RX) transfer logs will be written; these can be toggled individually with the <b>UsageLogTX</b> and <b>UsageLogRX</b> config file parameters. Access logging must be separately enabled with <b>-Ua</b>. The <b>-T</b> flag is accepted as a backward-compatible alias.</p>

<b>-Ui </b><i>hours</i>

<p style="padding-left: 30px;">Usage log writing interval in hours, default interval is 24 hours. The interval always starts at day boundaries, for example if the interval is set to 2 hours then the first interval in a given day will cover hours 0 through 2, the next 2 through 4, etc. The <b>-Ti</b> flag is accepted as a backward-compatible alias.</p>

<b>-Up </b><i>prefix</i>

<p style="padding-left: 30px;">Usage log file prefix, by default no prefix is added to the fixed section of the log file name. The <b>-Tp</b> flag is accepted as a backward-compatible alias.</p>

<b>-Uj</b>

<p style="padding-left: 30px;">Enable JSON Lines format for transfer logs.  When enabled, each TX and RX log file uses a <b>.jsonl</b> extension and contains one JSON object per client session describing the protocol, streams, and byte counts. This replaces the legacy text format for transfer logs. The <b>-Tj</b> flag is accepted as a backward-compatible alias.</p>

<b>-Ua</b>

<p style="padding-left: 30px;">Enable access logging.  When enabled, an access log file in JSON Lines format is written to the usage log directory, recording connection events and key commands.  The <b>-U</b> option (or <b>UsageLogDirectory</b> config parameter) must be set for access logging to function.</p>

<b>-STDERR</b>

<p style="padding-left: 30px;">Send all diagnostic output to stderr instead of stdout.  This is useful in situations where logging output is captured by another program on stderr and to separate ringserver diagnostics from other output on the console.</p>

<b>-MSWRITE </b><i>format</i>

<p style="padding-left: 30px;">A special mode of ringserver is to write all miniSEED data records received via DataLink to a user defined directory and file structure. See <b>miniSEED Archiving</b> for more details.</p>

<b>-MSSCAN </b><i>directory</i> [suboptions]

<p style="padding-left: 30px;">A special mode of ringserver is to recursively scan a directory for files containing miniSEED formatted data records and insert them into the buffer.  Optional suboptions control scanning behavior, the StateFile suboption is highly recommended. See <b>miniSEED Scanning</b> for more details.</p>

<b>-VOLATILE</b>

<p style="padding-left: 30px;">Create a volatile (non-stateful) ring buffer, in other words do not read packet buffer contents from ring files on startup or write them on shutdown.  This is useful in combination with the -MSWRITE option when no stateful buffer is needed.</p>

## <a id='config-file-parameters'>Config File Parameters</a>

<p >All of the command line parameters have config file and environment variable equivalents.  Many of the config file parameters are dynamic, if they are changed the server will re-read its configuration on the fly. See the detailed parameter descriptions in the documented example config file.</p>

## <a id='access-control'>Access Control</a>

<p >Access control is based on IP addresses and user authentication. Authentication is optional and can be used in combination with IP address based access control.  Authentication can be required for clients requesting streaming data using the <b>AuthRequiredForStreams</b> config parameter, or <b>S_AUTH_REQUIRED_FOR_STREAMS</b> environment variable.</p>

<p >The IP-based access control is specified in the config file using the following parameters:</p>

<pre >
  <b>AcceptIP</b> or <b>RS_ACCEPT_IP</b>
  <b>DenyIP</b> or <b>RS_DENY_IP</b>
  <b>AllowedStreamsIP</b> or <b>RS_ALLOWED_STREAMS_IP</b>
  <b>ForbiddenStreamsIP</b> or <b>RS_FORBIDDEN_STREAMS_IP</b>
  <b>WriteIP</b> or <b>RS_WRITE_IP</b>
  <b>TrustedIP</b> or <b>RS_TRUSTED_IP</b>
</pre>

<p >By default all clients are allowed to connect.  Specific clients can be rejected using the <b>DenyIP</b> config parameter.  If any <b>AcceptIP</b> config parameters are specified only addresses that match one of the entries, and are not rejected, are allowed to connect.</p>

<p >By default all clients are allowed access to all streams in the buffer, and clients with write permission are allowed to write any streams.  Specific clients can be limited to access or write subsets of streams using the <b>AllowedStreamsIP</b> config parameter.  Specific clients can be forbidden from accessing subsets streams using the <b>ForbiddenStreamsIP</b> config parameter. These parameters accept a regular expression that is used to match stream IDs that the client(s) are allowed or forbidden.</p>

<p >By default all clients are allowed to request the server ID, simple status and list of streams.  Specific clients can be allowed to access connection information and more detailed status using the <b>TrustedIP</b> access control.</p>

<p >If no client addresses are granted write permission via <b>WriteIP</b> or granted trusted status via <b>TrustedIP</b> then the 'localhost' address (local loopback) are granted those permissions.</p>

<p >Access control is host range (network) based, and specified as an address followed by an optional prefix in CIDR notation.  For example: "192.168.0.1/24" specifies the range of addresses from 192.168.0.1 to 192.168.0.254.  The address may be a hostname, which will be resolved on startup.  The prefix is optional and, if omitted, defaults to specifying only the single address.</p>

<p >The authentication-based access control is specified in the config file using the following parameters:</p>
<pre >
  <b>AuthCommand</b> or <b>RS_AUTH_COMMAND</b>
  <b>AuthTimeout</b> or <b>RS_AUTH_TIMEOUT</b>
</pre>

<p >The <b>AuthCommand</b> config parameter specifies a command to be executed when a client connects to the server and requests authentication.  The command is executed with the USERNAME and PASSWORD environment variables set to the values provided by the client.  The command should return a JSON formatted object on stdout with the following optional keys:</p>
<pre >
{
  "authenticated": <boolean>,
  "write_permission": <boolean>,
  "trust_permission": <boolean>,
  "allowed_streams": ["streamregex1", "streamregex2"],
  "forbidden_streams": ["streamregex3", "streamregex4"]
}
</pre>

<p >The <b>authenticated</b> key must be present and set to true for the client to be allowed to connect.  Missing keys are treated as false or empty.</p>

## <a id='seedlink-support'>Seedlink Support</a>

<p >The legacy SeedLink protocol (v3) only transmits 512-byte miniSEED data records.  This server is able to transmit miniSEED records of any length via SeedLink.  If you wish to ensure compatibility with legacy clients, only 512-byte miniSEED records should be submitted to the server.</p>

<p >This server supports the wild-carding of network and station codes during SeedLink negotiation using the '?' and '*' characters for single or multiple character matches respectively.  Not all SeedLink clients support wild-carded network and station codes.</p>

## <a id='stream-ids'>Stream Ids</a>

<p >Each unique data stream is identified by a stream ID.  The stream ID can be arbitrary but is commonly a combination of a data source identifier and a suffix (separated by a slash) that identifies the the payload type.  For example:</p>

<p >"FDSN:IU_COLA_00_B_H_Z/MSEED"</p>

<p >For the SeedLink protocol support, data source IDs must be valid FDSN Source IDs (https://docs.fdsn.org/projects/source-identifiers) with a suffix of "/MSEED" or "/MSEED3".</p>

<p >The stream ID suffix recommendations are as follows:</p>

<pre >
  <b>MSEED</b>   : miniSEED v2 data records
  <b>MSEED3</b>  : miniSEED v3 data records
  <b>JSON</b>    : JSON payloads
  <b>TEXT</b>    : Text payloads, where UTF-8 is assumed
</pre>

<p >The maximum length of stream IDs supported by the server is 63 bytes.</p>

## <a id='multi-protocol-support'>Multi-Protocol Support</a>

<p >Network listening ports can respond to all supported protocols: SeedLink, DataLink and HTTP/WebSocket.  If more than one protocol is configured for a port, the first command received by the server is used to determine which protocol is being used by the client, all subsequent communication is expected in this protocol.</p>

<p >Both IPv4 and IPv6 protocol families are supported by default (if supported by the system).</p>

<p >The network protocols and families allowed by any given listening port can be set by adding flags to the port specification.  See the available flags in the <b>ListenPort</b> description of the reference config file printed using the <b>-C</b> command line option.</p>

<p >The <b>PROXYv2</b> flag enables support for the HAProxy PROXY protocol version 2. When this flag is set, the server will expect every client connection on that port to send a valid PROXY protocol v2 header before any actual protocol data. This allows the server to determine the true source address and port of the connecting client, as often required when the traffic flows through a trusted proxy or load balancer. <b>Important:</b> The PROXY protocol should only be enabled on ports that are exclusively reachable by trusted proxies since the client may specify any IP address in the PROXY header, potentially spoofing their source address. Do not use the PROXYv2 flag on publicly accessible ports.</p>

<p >The <b>TRUSTED</b> flag grants trusted status to all clients connecting on a port, allowing access to detailed server status and connection information.  <b>WARNING:</b> Do not use this flag on publicly accessible ports as it grants elevated access to all connecting clients regardless of their IP address.</p>

<p >Examples of adding flags to a port specification:</p>

<pre >
  <b>-L "18000 SeedLink HTTP"</b>        : CLI, SeedLink and HTTP on port 18000
  <b>-SL "18500 TLS IPv4"</b>            : CLI, SeedLink via TLS on port 18500, IPv4 only
  <b>RS_LISTEN_PORT="8080 HTTP IPv6"</b> : EnvVar, HTTP on port 8080, IPv6 only
  <b>ListenPort 16000 DataLink</b>       : Config file, DataLink on port 16000
  <b>ListenPort 14000 TRUSTED</b>        : Config file, all protocols trusted on port 14000
  <b>ListenPort 18000 PROXYv2</b>      : Config file, all protocols with PROXYv2 on port 18000
</pre>

## <a id='http-support'>Http Support</a>

<p >The server will respond to HTTP requests for a few fixed resources. If the <b>WebRoot</b> config parameter is set to a directory, the files under that directory will also be served when requested through the HTTP GET method.  Except for the fixed resources, the HTTP server implementation is limited to returning existing files and returning "index.html" files when a directory is requested.</p>

<p >The following fixed resources are supported:</p>

<pre >
  <b>/id</b>           - Server identification
  <b>/id/json</b>      - Server identification in JSON
  <b>/streams</b>      - List of available streams with time range
  <b>/streams/json</b> - List of available streams with time range in JSON
  <b>/streamids</b>    - List of available streams
  <b>/status</b>       - Server status, limited access*
  <b>/status/json</b>  - Server status in JSON, limited access*
  <b>/connections</b>  - List of connections, limited access*
  <b>/connections/json</b> - List of connections in JSON, limited access*
  <b>/seedlink</b>     - Initiate WebSocket connection for Seedlink
  <b>/datalink</b>     - Initiate WebSocket connection for DataLink
</pre>

<p >Access to the <b>status</b> and <b>connections</b> information is limited to clients that have trusted permission.</p>

<p >The <b>streams</b>, <b>streamids</b> and <b>connections</b> endpoints accept a <i>match</i> parameter that is a regular expression pattern used to limit the returned information.  For the <b>streams</b> and <b>streamids</b> endpoints the matching is applied to stream IDs.  For the <b>connections</b> endpoint the matching is applied to hostname, client IP address and client ID. For example: http://localhost/streams?match=IU_ANMO.</p>

<p >After a WebSocket connection has been initiated with either the <b>seedlink</b> or <b>datalink</b> end points, the requested protocol is supported exactly as it would be normally with the addition of WebSocket framing.  Each server command, including terminator(s), should be contained in a WebSocket frame.</p>

<p >Custom HTTP headers may be included in HTTP responses using the <b>HTTPHeader</b> config file parameter.  This can be used, for example, to enable cross-site HTTP requests via Cross-Origin Resource Sharing (CORS).</p>

## <a id='usage-logging'>Usage Logging</a>

<p >Usage logging covers two distinct log types: transfer logs (TX/RX) that track data volume per stream per client, and access logs that record connection events and key commands.  All log types share the same base directory, file prefix, and rotation interval, configured via the <b>-U</b> command line option or the <b>UsageLogDirectory</b> config file parameter (the <b>TransferLogDirectory</b> alias is also accepted for backward compatibility).  Access logging is disabled by default and must be explicitly enabled.</p>

<p >.SS "Transfer logging"</p>

<p >The <b>UsageLogTX</b> and <b>UsageLogRX</b> config file parameters (or their <b>TransferLogTX</b> / <b>TransferLogRX</b> aliases, or equivalent environment variables) control logging of data transmitted or received.  By default both TX and RX logging are enabled when a log directory is set.  The log interval and file name prefix can be changed via the <b>-Ui</b> and <b>-Up</b> command line options.</p>

<p >Log files are named with the interval time window, for example:</p>
<pre >
  <b>txlog-20260316T0000-20260317T0000</b>
  <b>rxlog-20260316T0000-20260317T0000</b>
</pre>

<p >By default transfer logs use a legacy text format.  The <b>-Uj</b> command line option or the <b>UsageLogJSONLines</b> config parameter enables JSON Lines format instead, where each line is a JSON object containing protocol details, per-stream byte counts, and client metadata.</p>

<p >In text format, each TX or RX log file contains entries with this pattern:</p>

<p >1) A "START CLIENT" line containing the host name, IP address, protocol, client ID, log time, and connection time.</p>

<p >2) One or more data lines of the following form:</p>

<pre >
<b>[Stream ID] [bytes] [packets]</b>
</pre>

<p >3) An "END CLIENT" line including the total bytes for this entry.</p>

<p >Note: the byte counts are the sum of the data payload bytes in each packet and do not include the DataLink or SeedLink protocol headers.</p>

<p >An example "TX" file illustrating a transmission entry in legacy text format:</p>

<pre >
START CLIENT host.iris.edu [192.168.255.255] (SeedLink|Client) @ 2018-03-30 07:00:05 (connected 2018-03-30 06:59:36) TX
FDSN:IU_SNZO_10_B_H_Z/MSEED 2560 5
FDSN:IU_SNZO_00_B_H_Z/MSEED 2048 4
END CLIENT host.iris.edu [192.168.255.255] total TX bytes: 4608
</pre>

<p >.SS "Access logging"</p>

<p >Access logging is enabled with the <b>-Ua</b> command line option or the <b>UsageLogAccess</b> config file parameter (or the <b>RS_USAGE_LOG_ACCESS</b> environment variable).  The <b>UsageLogDirectory</b> (or <b>-U</b> option) must be set for access logging to function.</p>

<p >Access log files are always written in JSON Lines format, one JSON object per line, with a <b>.jsonl</b> extension.  Files are named:</p>
<pre >
  <b>accesslog-20260316T0000-20260317T0000.jsonl</b>
</pre>

<p >Each access log record contains the event time, client metadata (IP address, hostname, server port, user agent), authentication details if applicable, protocol information (name, version, TLS, WebSocket), and the event or command details.</p>

<p >The following events are logged:</p>

<pre >
  <b>connect</b>    - Client TCP connection established
  <b>disconnect</b> - Client connection closed
  <b>command</b>    - Key protocol command received
</pre>

<p >Key commands recorded include:</p>

<pre >
  <b>INFO</b>         - SeedLink or DataLink INFO request (with item/type)
  <b>DATA</b>/<b>FETCH</b>  - SeedLink data streaming request (with stream selection criteria)
  <b>STREAM</b>       - DataLink streaming start (with stream selection criteria)
  <b>GET</b>          - HTTP GET request (with path)
</pre>

<p >For DATA/FETCH and STREAM commands, the <b>match</b> and <b>reject</b> fields in the JSON record contain the regular expressions used to select streams, if any were specified by the client.</p>

<p >An example access log record:</p>

<pre >
{"log_time":"2026-03-16T12:00:00Z","connect_time":"2026-03-16T11:59:55Z",
 "client":{"ip":"192.168.1.10","server_port":18000,"hostname":"host.example.com",
           "user_agent":"slinktool/4.1"},
 "protocol":{"name":"SeedLink","version":"4.0"},
 "event":"command","command":"DATA","match":"FDSN:IU_.*",
 "service":{"name":"ringserver","version":"4.3.1"}}
</pre>

## <a id='external-packet-ids'>External Packet Ids</a>

<p >With the DataLink v1.1 protocol a client may submit packets with a specified packet ID to use instead of a generated ID.  This is useful to implement multiple servers that share common packet IDs for use with a network load balancer, such that it does not matter to which server a client connects.</p>

<p >These packet IDs are used in the SeedLink and DataLink protocols by clients to track and resume data streams.  In SeedLink these are called sequence numbers.</p>

<p >For ringserver, packet IDs, aka sequence numbers, must be between 0 and (UINT64_MAX - 10), or 18446744073709551605.  The last 10 values of the uint64 range are reserved for internal use to indicate special conditions.  These values are not expected to be encountered in typical data streaming operation.</p>

<p >Furthermore, external IDs submitted with packets are strongly recommended to be unique and monotonically increasing.  Such a sequence of IDs support efficient data stream resumption and tracking.</p>

## <a id='miniseed-archiving'>Miniseed Archiving</a>

<p >Using either the <b>-MSWRITE</b> command line option or the <b>MSeedWrite</b> config file parameter the server can be configured to write all miniSEED data records received via DataLink to a user defined directory and file structure.</p>

<p >The archive <i>format</i> argument is expanded for each packet processed using the following flags:</p>

<pre >
  <b>n</b> : network code, white space removed
  <b>s</b> : station code, white space removed
  <b>l</b> : location code, white space removed
  <b>c</b> : channel code, white space removed
  <b>q</b> : record quality indicator (D,R,Q,M), single character
  <b>Y</b> : year, 4 digits
  <b>y</b> : year, 2 digits zero padded
  <b>j</b> : day of year, 3 digits zero padded
  <b>H</b> : hour, 2 digits zero padded
  <b>M</b> : minute, 2 digits zero padded
  <b>S</b> : second, 2 digits zero padded
  <b>F</b> : fractional seconds, 4 digits zero padded
  <b>D</b> : current year-day time stamp of the form YYYYDDD
  <b>L</b> : data record length in bytes
  <b>r</b> : sample rate (Hz) as a rounded integer
  <b>R</b> : sample rate (Hz) as a float with 6 digit precision
  <b>h</b> : host name of client submitting data
  <b>%</b> : the percent (%) character
  <b>#</b> : the number (#) character
</pre>

<p >The flags are prefaced with either the <b>%</b> or <b>#</b> modifier. The <b>%</b> modifier indicates a defining flag while the <b>#</b> indicates a non-defining flag.  All received packets with the same set of defining flags will be saved to the same file. Non-defining flags will be expanded using the values in the first packet received for the resulting file name.</p>

<p >Time flags are based on the start time of the given packet.</p>

<p >Files are created with (permission) mode 666 and directories are created with mode 777.  An operator of ringserver can control the final permissions of the files by adjusting the umask as desired.</p>

<p >Some preset archive layouts are available:</p>

<pre >
  <b>BUD</b>   : <i>%n/%s/%s.%n.%l.%c.%Y.%j</i>  (BUD layout)
  <b>CHAN</b>  : <i>%n.%s.%l.%c</i>  (channel)
  <b>QCHAN</b> : <i>%n.%s.%l.%c.%q</i>  (quality-channel-day)
  <b>CDAY</b>  : <i>%n.%s.%l.%c.%Y:%j:#H:#M:#S</i>  (channel-day)
  <b>SDAY</b>  : <i>%n.%s.%Y:%j</i>  (station-day)
  <b>HSDAY</b> : <i>%h/%n.%s.%Y:%j</i>  (host-station-day)
</pre>

<p >The preset archive layouts are used by prefixing a target directory with the preset identifier followed by an '@' character.  For example:</p>

<p ><b>BUD@/data/bud/</b></p>

<p >would write a BUD like structure in the /data/bud/ directory.</p>

<p >Other example:</p>

<p ><b>/archive/%n/%s/%n.%s.%l.%c.%Y.%j</b></p>

<p >would be expanded to day length files named something like:</p>

<p ><b>/archive/IU/ANMO/IU.ANMO..BHE.2003.055</b></p>

<p >Using non-defining flags the format string:</p>

<p ><b>/data/%n.%s.%Y.%j.%H:#M:#S.miniseed</b></p>

<p >would be expanded to:</p>

<p ><b>/data/IU.ANMO.2003.044.14:17:54.miniseed</b></p>

<p >resulting in hour length files because the minute and second are specified with the non-defining modifier.  The minute and second fields are from the first packet in the file.</p>

## <a id='miniseed-scanning'>Miniseed Scanning</a>

<p >Using either the <b>-MSSCAN</b> command line option or the <b>MSeedScan</b> config file parameter (or equivalent environment variable) the server can be configured to recursively scan a directory for files containing miniSEED data records and insert them into the buffer.  Intended for real-time data re-distribution, files are continuously scanned, newly added records are inserted into the buffer.</p>

<p >Sub-options can be used to control the scanning process.  The sub-options are specified on the same line as the scan directory as key-value pairs separated by an equals '=' character and may not contain spaces (because they are separated by spaces).  Do not use quotes for the values.  The available sub-options are:</p>

<pre >
  <b>StateFile</b> : File to save scanning state through restarts
  <b>Match</b> : Regular expression to match file names
  <b>Reject</b> : Regular expression to reject file names
  <b>InitCurrentState</b> : Initialize scanning to current state
  <b>MaxRecurse</b> : Maximum recursion depth (default is no limit)
</pre>

<p >Except for special cases the <b>StateFile</b> option should always be specified, otherwise a restart of the server could re-read data records that it has already read.</p>

<p >If the <b>InitCurrentState</b> option is set to '<b>y</b>' the scanning will only read new data, effectively skipping all the data discovered during the first scan, under the following conditions:</p>
<pre >
1) No StateFile has been specified
2) StateFile has been specified but does not exist
</pre>

<p >The <b>InitCurrentState</b> option is useful to avoid reading all existing data when starting a server scanning an existing large dataset.  It is also useful to reset the dataflow to current data after a lengthy downtime, simply remove the statefile(s) before starting the server.</p>

<p >To scan a data directory and save the scanning state to a StateFile configure the server with either a config file option or command line, respectively:</p>

<p ><b>MSeedScan /data/miniseed/ StateFile=/opt/ringserver/scan.state</b></p>

<p ><b>-MSScan "/data/miniseed/ StateFile=/opt/ringserver/scan.state"</b></p>

<p >To limit the scanning to file names matching a certain pattern use the Match option, e.g. files ending in ".mseed":</p>

<p ><b>MSeedScan /data/miniseed/ StateFile=/data/scan.state Match=.*\\.mseed$</b></p>

## <a id='author'>Author</a>

<pre >
Chad Trabant
EarthScope Data Services
</pre>


(man page 2026/03/21)

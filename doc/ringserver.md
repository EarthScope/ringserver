# <p >ringserver 
###  Generic packet ring buffer</p>

1. [Name](#)
1. [Synopsis](#synopsis)
1. [Description](#description)
1. [Options](#options)
1. [Config File Parameters](#config-file-parameters)
1. [Access Control](#access-control)
1. [Seedlink Support](#seedlink-support)
1. [Multi-Protocol Support](#multi-protocol-support)
1. [Http Support](#http-support)
1. [Mini-Seed Archiving](#mini-seed-archiving)
1. [Mini-Seed Scanning](#mini-seed-scanning)
1. [Author](#author)

## <a id='synopsis'>Synopsis</a>

<pre >
ringserver [options] [configfile]
</pre>

## <a id='description'>Description</a>

<p ><b>ringserver</b> is a TCP-based ring buffer designed for packetized streaming data.  The ring buffer is implemented as a FIFO with newly arriving packets pushing older packets out of the buffer.  The ring packets are not format specific and may contain any type of data.  All communications are performed via TCP interfaces.  Data is available to clients using the SeedLink and DataLink protocols and submitted to the server using the DataLink protocol.</p>

<p >The server is configured either with options on the command line or by using a <b>ringserver</b> config file (or both).  Only the most common options are available on the command line, all options are tunable via the config file.  Detailed descriptions for each option are included in the example config file that accompanies the source code.  Many options are dynamic, meaning that they can be changed while the server is running.  In this case the server will recognize that the config file has changed and will re-read it's configuration.  This is especially useful for updating access control lists, logging verbosity and other logging parameters.</p>

<p >In normal operation packet buffer contents are saved in files when the server is shut down making the server stateful across restarts.  By default the packet buffer is managed as a memory-mapped file, it can optionally be maintained completely in system memory and only read and write the buffer contents on startup and shutdown (useful in environments where memory-mapping is not possible).</p>

<p >Client access is controlled using 3 IP address based lists: one list for specific IPs that must match, one list for specific IPs that are to be rejected and one for IPs that are allowed to send data to the server (write access).  See <b>Access Control</b> for more details.</p>

<p >Transfer logs can optionally be written to track the transmission and reception of data packets to and from the server.  This tracking is stream-based and identifies the number of packet bytes of each unique stream transfered to or from each client connection.</p>

<p >The server supports multiple protocols (SeedLink, DataLink and HTTP) on the same listening port.  Support can be limited to certain protocols on a by-port basis.  See <b>Multi-protocol support</b> for more information.</p>

<p >The server also has limited support for HTTP requests.  When support is enabled, server status, stream lists and other details can be accessed with simple HTTP requests.  See <b>HTTP Support</b> for more details.</p>

<p >The slinktool(1) and dalitool(1) programs can be used to query the ringserver for various information via the SeedLink and DataLink interfaces respectively.  The dalitool program was developed in parallel with ringserver and the DataLink protocol and is the recommended query tool for ringserver admins.</p>

## <a id='options'>Options</a>

<b>-V</b>

<p style="padding-left: 30px;">Print the program version and exit.</p>

<b>-h</b>

<p style="padding-left: 30px;">Print the program help/usage and exit.</p>

<b>-H</b>

<p style="padding-left: 30px;">Print an extended help/usage and exit.</p>

<b>-v</b>

<p style="padding-left: 30px;">Be more verbose.  This flag can be used multiple times ("-v -v" or "-vv") for more verbosity.</p>

<b>-I </b><i>ServerID</i>

<p style="padding-left: 30px;">Server ID reported to the clients.  The value may be a quoted string containing spaces.  The default value is "Ring Server".</p>

<b>-M </b><i>maxclientsperIP</i>

<p style="padding-left: 30px;">Maximum number of concurrently connected clients per IP address.  This limit does not apply to addresses with write permission.  There is no default maximum.</p>

<b>-m </b><i>maxclients</i>

<p style="padding-left: 30px;">Maximum number of concurrently connected clients.  The default maximum is 600.</p>

<b>-Rd </b><i>ringdir</i>

<p style="padding-left: 30px;">Base directory for the server to store the packet buffer data files. This option must be specified on the command line or in the config file, there is no default.</p>

<b>-Rs </b><i>bytes</i>

<p style="padding-left: 30px;">Size of the packet buffer in bytes, default size is 1 GB.  The size of the ring, in combination with the ring packet size, determine how much past data is available in the buffer.  If the server is configured to memory-map the packet buffer files the maximum size of the ring is limited to how large a file the host system can support.  If the server is configured to maintain the ring in system memory the size is limited to system memory.</p>

<b>-Rm </b><i>maxID</i>

<p style="padding-left: 30px;">Maximum ring packet ID used to uniquely identify a packet.  Default is 16,777,215 which the maximum supported by the SeedLink protocol, if SeedLink is not being used it can be much higher.</p>

<b>-Rp </b><i>packetsize</i>

<p style="padding-left: 30px;">Maximum ring packet data size (not including packet buffer header), default is 512 bytes.</p>

<b>-NOMM</b>

<p style="padding-left: 30px;">No memory-mapping, maintain the ring buffer in system memory only.  On startup the ring buffer files will be read into memory and on shutdown the memory buffers will be written back to the files.  This option might be useful in environments where memory-mapping of files is not possbile or is unreliable (e.g. NFS).</p>

<b>-L </b><i>port</i>

<p style="padding-left: 30px;">Network port to listen for incoming connections on.  By default the server will not listen for connections, setting at least one listen port is needed to communiate with the server.  By default a listening port will accept all supported protocols.  Restricting a port to only allow specific protocols can be done using a configuration file, see <b>Multi-protocol Support</b> for more information.</p>

<b>-DL </b><i>port</i>

<p style="padding-left: 30px;">Network port to listen for DataLink connections on.  This is a special case listening port limited to DataLink.</p>

<b>-SL </b><i>port</i>

<p style="padding-left: 30px;">Network port to listen for SeedLink connections on.  This is a special case listening port limited to SeedLink.</p>

<b>-T </b><i>logdir</i>

<p style="padding-left: 30px;">Transfer log file base directory, by default the server does not write transfer logs.  If a directory is specified both transmission and reception logs will be written, these two logs can be toggled individually in the server config file.</p>

<b>-Ti </b><i>hours</i>

<p style="padding-left: 30px;">Transfer log writing interval in hours, default interval is 24 hours. The interval always starts at day boundaries, for example if the interval is set to 2 hours then the first interval in a given day will cover hours 0 through 2, the next 2 through 4, etc.</p>

<b>-Tp </b><i>prefix</i>

<p style="padding-left: 30px;">Transfer log file prefix, by default no prefix is added to the fixed section of the log file name.</p>

<b>-STDERR</b>

<p style="padding-left: 30px;">Send all diagnostic output to stderr instead of stdout.  This is useful in situations where logging output is captured by another program on stderr and to separate ringserver diagnostics from other output on the console.</p>

<b>-MSWRITE </b><i>format</i>

<p style="padding-left: 30px;">A special mode of ringserver is to write all Mini-SEED data records received via DataLink (ring packets ending with the /MSEED suffix) to a user defined directory and file structure.  See <b>Mini-SEED Archiving</b> for more details.</p>

<b>-MSSCAN </b><i>directory</i> [suboptions]

<p style="padding-left: 30px;">A special mode of ringserver is to recursively scan a directory for files containing Mini-SEED formatted data records and insert them into the ring as packets with a /MSEED suffix.  Optional suboptions control scanning behavior, the StateFile suboption is highly recommended.  See <b>Mini-SEED Scanning</b> for more details.</p>

<b>-VOLATILE</b>

<p style="padding-left: 30px;">Create a volatile (non-stateful) ring buffer, in other words do not read packet buffer contents from ring files on startup or write them on shutdown.  This is useful in combination with the -MSWRITE option when no stateful buffer is needed.</p>

## <a id='config-file-parameters'>Config File Parameters</a>

<p >All of the command line parameters have config file equivalents, below are the config file parameters which have no command line equivalents. Many of the config file parameters are dynamic, if they are changed the server will re-read it's configuration on the fly.  See the detailed parameter descriptions in the documented example config file.</p>

<pre >
<b>AutoRecovery</b> [0|1|2] - Control autorecovery after corruption detection
<b>ResolveHostnames</b> [0|1] - Control reverse DNS lookups
<b>TimeWindowLimit</b> % - Control limit for time window searches
<b>TransferLogTX</b> [0|1] - Control writing of transmission log
<b>TransferLogRX</b> [0|1] - Control writing of reception log
<b>WriteIP</b> IP[/netmaks] - Add IP address(es) to write permission list
<b>LimitIP</b> IP[/netmaks] RegEx - Add IP address(es) to limit list
<b>MatchIP</b> IP[/netmaks] - Add IP address(es) to match list
<b>RejectIP</b> IP[/netmaks] - Add IP address(es) to reject list
</pre>

## <a id='access-control'>Access Control</a>

<p >By default all clients are allowed to connect.  Specific clients can be rejected using the <b>RejectIP</b> config parameter.  If any <b>MatchIP</b> config parameters are specified only addresses that match one of the entries and are not rejected are allowed to connect.</p>

<p >By default all clients are allowed access to all streams in the buffer.  Specific clients can be limited to subsets of streams using the <b>LimitIP</b> config parameter.  This parameter takes a regular expression that is used to match stream IDs that the client(s) are allowed access to.</p>

<p >By default all clients are allowed to request the server ID, simple status and list of streams.  Specific clients can be allowed to access connection information and more detailed status using the <b>TrustedIP</b> config parameter.</p>

<p >If no client addresses are granted write permission via <b>WriteIP</b> then the address 127.0.0.1 (local loopback) is granted write permission.</p>

## <a id='seedlink-support'>Seedlink Support</a>

<p >The SeedLink protocol only transmits 512-byte Mini-SEED data records. Therefore only 512-byte Mini-SEED packets with a '/MSEED' suffix on the stream ID will be exported via SeedLink if enabled.</p>

<p >This server supports the wildcarding of network and station codes during SeedLink negotiation using the '?' and '*' characters for single or multiple character matches respectively.  Not all SeedLink clients support wildcarded network and station codes.</p>

## <a id='multi-protocol-support'>Multi-Protocol Support</a>

<p >Network listening ports can respond to all supported protocols (SeedLink, DataLink and HTTP).  The first command received by the server is used to determine which protocol is being used by the client, all subsequent communication is expected in this protocol.</p>

<p >The protocols allowed by any given listening port can be set to any combination of the supported protocols by adding flags to the <i>Listen</i> parameter of the server configuration file.</p>

## <a id='http-support'>Http Support</a>

<p >The server will respond to HTTP requests for a few fixed resources. If the <b>WebRoot</b> config parameter is set to a directory, the files under that directory will also be served when requesed through the HTTP GET method.  Except for the fixed resources, the HTTP server implementation is limited to returning existing files and returning "index.html" files when a directory is requested.</p>

<p >The following fixed resources are supported:</p>

<pre >
  <b>/id</b>           - Return server identification
  <b>/streams</b>      - Return list of available streams
  <b>/status</b>       - Return server status, limited access*
  <b>/connections</b>  - Return list of connections, limited access*
  <b>/seedlink</b>     - Initiate WebSocket connection for Seedlink
  <b>/datalink</b>     - Initiate WebSocket connection for DataLink
</pre>

<p >Access to the <b>status</b> and <b>connections</b> information is limited to clients that have trusted permission.</p>

<p >The <b>streams</b> and <b>connections</b> endpoints accept a <i>match</i> parameter that is a pattern used to limit the returned information. For example: http://localhost/streams?match=IU_ANMO</p>

<p >The <b>streams</b> endpoint accepts a <i>level</i> parameter that limits the returned information to a unique list of stream identifiers at the specified level.  Valid values are 1 through 6.  Identifier components should be delimited with underscore characters.  To illustrate, if a ringserver contains stream IDs in the pattern of "NET_STA_LOC_CHAN/MSEED" a request for level 2 returns a unique list of "NET_STA" values.  For example: http://localhost/streams?level=2.</p>

<p >After a WebSocket connection has been initiated with either the <b>seedlink</b> or <b>datalink</b> end points, the requested protocol is supported exactly as it would be normally with the addition of WebSocket framing.  Each server command should be contained in a single WebSocket frame, independent of other commands.</p>

## <a id='mini-seed-archiving'>Mini-Seed Archiving</a>

<p >Using either the <b>-MSWRITE</b> command line option or the <b>MSeedWrite</b> config file parameter the server can be configured to write all Mini-SEED data records received via DataLink to a user defined directory and file structure.  The archive <i>format</i> argument is expanded for each packet processed using the following flags:</p>

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

## <a id='mini-seed-scanning'>Mini-Seed Scanning</a>

<p >Using either the <b>-MSSCAN</b> command line option or the <b>MSeedScan</b> config file parameter the server can be configured to recursively scan a directory for files containing Mini-SEED data records and insert them into the ring.  Intended for real-time data re-distribution files are continuously scanned, newly added records are inserted into the ring.</p>

<p >Sub-options can be used to control the scanning process.  The sub-options are specified on the same line as the scan directory as key-value pairs separated by an equals '=' character and may not contain spaces (because they are separated by spaces).  Do not use quotes for the values.  The available sub-options are:</p>

<pre >
  <b>StateFile</b> : File to save scanning state through restarts
  <b>Match</b> : Regular expression to match file names
  <b>Reject</b> : Regular epression to reject file names
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
IRIS Data Management Center
</pre>


(man page 2016/12/28)

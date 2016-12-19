/**************************************************************************
 * http.c
 *
 * HTTP handling.
 *
 * Copyright 2016 Chad Trabant, IRIS Data Management Center
 *
 * This file is part of ringserver.
 *
 * ringserver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * ringserver is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ringserver. If not, see http://www.gnu.org/licenses/.
 *
 * Modified: 2016.353
 **************************************************************************/

/* _GNU_SOURCE needed to get strcasestr() under Linux */
#define _GNU_SOURCE

#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#include "clients.h"
#include "dlclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "mseedscan.h"
#include "ring.h"
#include "ringserver.h"
#include "slclient.h"

#define SHA1_DIGEST_SIZE 20

/* type to hold the SHA1 */
typedef struct
{
  uint32_t count[2];
  uint32_t hash[5];
  uint32_t wbuf[16];
} sha1_ctx;

static int ParseHeader (char *header, char **value);
static int GenerateStreams (ClientInfo *cinfo, char **streamlist, char *path);
static int GenerateStatus (ClientInfo *cinfo, char **status);
static int GenerateConnections (ClientInfo *cinfo, char **connectionlist, char *path);
static int SendFileHTTP (ClientInfo *cinfo, char *path);
static int NegotiateWebSocket (ClientInfo *cinfo, char *version,
                               char *upgradeHeader, char *connectionHeader,
                               char *secWebSocketKeyHeader, char *secWebSocketVersionHeader);
static int Base64encode (char *encoded, const char *string, int len);
static void sha1_begin (sha1_ctx ctx[1]);
static void sha1_hash (const unsigned char data[], unsigned long len, sha1_ctx ctx[1]);
static void sha1_end (unsigned char hval[], sha1_ctx ctx[1]);
static void sha1 (unsigned char hval[], const unsigned char data[], unsigned long len);

/***************************************************************************
 * HandleHTTP:
 *
 * Handle HTTP requests using an extremely limited HTTP server
 * implementation.
 *
 * The following end points are handled:
 *   /id          - return server ID and version
 *   /streams     - return list of server streams
 *   /status      - return server status, limited via write-permissions
 *   /connections - return list of connections, limited via write-permissions
 *   /seedlink    - initiate WebSocket connection for SeedLink
 *   /datalink    - initiate WebSocket connection for SeedLink
 *
 * Returns 1 on success and should disconnect, 0 on success and -1 on
 * error which should disconnect.
 ***************************************************************************/
int
HandleHTTP (char *recvbuffer, ClientInfo *cinfo)
{
  size_t headlen;
  char method[10];
  char path[100];
  char version[100];
  int fields;
  int nread;
  int rv;

  char upgradeHeader[100] = "";
  char connectionHeader[100] = "";
  char secWebSocketKeyHeader[100] = "";
  char secWebSocketVersionHeader[100] = "";

  char *response = NULL;
  char *value = NULL;

  /* Parse HTTP request */
  memset (method, 0, sizeof (method));
  memset (path, 0, sizeof (path));
  memset (version, 0, sizeof (version));

  fields = sscanf (recvbuffer, "%9s %99s %99s", method, path, version);

  if (fields < 2)
  {
    lprintf (0, "[%s] HandleHTTP unrecognized HTTP request '%s'",
             cinfo->hostname, recvbuffer);
    return -1;
  }
  else if (strcmp (method, "GET"))
  {
    lprintf (0, "[%s] HandleHTTP unrecognized HTTP method '%s'",
             cinfo->hostname, method);

    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 501 Method %s Not Implemented\r\n"
                        "Content-Length: 0\r\n"
                        "Connection: close\r\n"
                        "\r\n",
                        method);

    if (headlen > 0)
    {
      SendData (cinfo, cinfo->sendbuf, strlen (cinfo->sendbuf));
    }
    else
    {
      lprintf (0, "Error creating response (unrecognized method)");
    }

    return -1;
  }

  /* Consume all request headers */
  while ((nread = RecvLine (cinfo)) > 0)
  {
    /* If empty line (CRLF) then there are no more headers */
    if (cinfo->recvbuf[0] == '\r' && cinfo->recvbuf[1] == '\n')
      break;

    if (ParseHeader (cinfo->recvbuf, &value))
    {
      lprintf (0, "Error parsing HTTP header: '%s'", cinfo->recvbuf);
      return -1;
    }

    /* Store values of selected headers */
    if (!strcasecmp (cinfo->recvbuf, "User-Agent"))
    {
      strncpy (cinfo->clientid, value, sizeof (cinfo->clientid) - 1);
      cinfo->clientid[sizeof (cinfo->clientid) - 1] = '\0';
    }
    else if (!strcasecmp (cinfo->recvbuf, "Upgrade"))
    {
      strncpy (upgradeHeader, value, sizeof (upgradeHeader) - 1);
      upgradeHeader[sizeof (upgradeHeader) - 1] = '\0';
    }
    else if (!strcasecmp (cinfo->recvbuf, "Connection"))
    {
      strncpy (connectionHeader, value, sizeof (connectionHeader) - 1);
      connectionHeader[sizeof (connectionHeader) - 1] = '\0';
    }
    else if (!strcasecmp (cinfo->recvbuf, "Sec-WebSocket-Key"))
    {
      strncpy (secWebSocketKeyHeader, value, sizeof (secWebSocketKeyHeader) - 1);
      secWebSocketKeyHeader[sizeof (secWebSocketKeyHeader) - 1] = '\0';
    }
    else if (!strcasecmp (cinfo->recvbuf, "Sec-WebSocket-Version"))
    {
      strncpy (secWebSocketVersionHeader, value, sizeof (secWebSocketVersionHeader) - 1);
      secWebSocketVersionHeader[sizeof (secWebSocketVersionHeader) - 1] = '\0';
    }
  }

  /* Error receiving data, -1 = orderly shutdown, -2 = error */
  if (nread < 0)
  {
    return -1;
  }

  /* Handle specific end points */
  if (!strcasecmp (path, "/id"))
  {
    /* This may be used to "ping" the server so only log at high verbosity */
    lprintf (2, "[%s] Received HTTP ID request", cinfo->hostname);

    asprintf (&response,
              "%s/%s\n"
              "Organization: %s",
              PACKAGE, VERSION, serverid);

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %zu\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? strlen (response) : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){strlen (cinfo->sendbuf), (response) ? strlen (response) : 0},
                       2);
    }
    else
    {
      lprintf (0, "Error creating response (ID request)");
      rv = -1;
    }

    if (response)
      free (response);

    return (rv) ? -1 : 0;
  } /* Done with /id request */
  else if (!strncasecmp (path, "/streams", 8))
  {
    lprintf (1, "[%s] Received HTTP STREAMS request", cinfo->hostname);

    rv = GenerateStreams (cinfo, &response, path);

    if (rv < 0)
    {
      lprintf (0, "[%s] Error generating stream list", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %zu\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? strlen (response) : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){strlen (cinfo->sendbuf), (response) ? strlen (response) : 0},
                       2);
    }
    else
    {
      lprintf (0, "Error creating response (STREAMS request)");
      rv = -1;
    }

    if (response)
      free (response);

    return (rv) ? -1 : 0;
  } /* Done with /streams request */
  else if (!strcasecmp (path, "/status"))
  {
    /* Check for write permission, required to access this resource */
    if (!cinfo->writeperm)
    {
      lprintf (1, "[%s] HTTP STATUS request from client without write permission",
               cinfo->hostname);

      response =
          "HTTP/1.1 403 Forbidden, no soup for you!\r\n"
          "Connection: close\r\n"
          "\r\n"
          "Forbidden, no soup for you!\r\n";

      rv = SendData (cinfo, response, strlen (response));

      return (rv) ? -1 : 1;
    }

    lprintf (1, "[%s] Received HTTP STATUS request", cinfo->hostname);

    rv = GenerateStatus (cinfo, &response);

    if (rv <= 0)
    {
      lprintf (0, "[%s] Error generating server status", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %zu\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? strlen (response) : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){strlen (cinfo->sendbuf), (response) ? strlen (response) : 0},
                       2);
    }
    else
    {
      lprintf (0, "Error creating response (STATUS request)");
      rv = -1;
    }

    if (response)
      free (response);

    return (rv) ? -1 : 0;
  } /* Done with /status request */
  else if (!strncasecmp (path, "/connections", 12))
  {
    /* Check for write permission, required to access this resource */
    if (!cinfo->writeperm)
    {
      lprintf (1, "[%s] HTTP CONNECTIONS request from client without write permission",
               cinfo->hostname);

      response =
          "HTTP/1.1 403 Forbidden, no soup for you!\r\n"
          "Connection: close\r\n"
          "\r\n"
          "Forbidden, no soup for you!\r\n";

      rv = SendData (cinfo, response, strlen (response));

      return (rv) ? -1 : 1;
    }

    lprintf (1, "[%s] Received HTTP CONNECTIONS request", cinfo->hostname);

    rv = GenerateConnections (cinfo, &response, path);

    if (rv <= 0)
    {
      lprintf (0, "[%s] Error generating server status", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %zu\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? strlen (response) : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){strlen (cinfo->sendbuf), (response) ? strlen (response) : 0},
                       2);
    }
    else
    {
      lprintf (0, "Error creating response (CONNECTIONS request)");
      rv = -1;
    }

    if (response)
      free (response);

    return (rv) ? -1 : 0;
  } /* Done with /connections request */
  else if (!strncasecmp (path, "/seedlink", 9))
  {
    if ((cinfo->protocols & PROTO_SEEDLINK) == 0)
    {
      lprintf (1, "[%s] Received SeedLink WebSocket request on non-SeedLink port", cinfo->hostname);

      /* Create header */
      headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                          "HTTP/1.1 400 Cannot request SeedLink WebSocket on non-SeedLink port\r\n"
                          "Connection: close\r\n"
                          "\r\n"
                          "Cannot request SeedLink WebSocket on non-SeedLink port");

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, strlen (cinfo->sendbuf));
      }
      else
      {
        lprintf (0, "Error creating response (SeedLink WebSocket request on non-SeedLink port)");
      }

      return -1;
    }

    lprintf (1, "[%s] Received WebSocket SeedLink request", cinfo->hostname);

    if (NegotiateWebSocket (cinfo, version, upgradeHeader, connectionHeader,
                            secWebSocketKeyHeader, secWebSocketVersionHeader))
    {
      lprintf (0, "[%s] Error negotiating SeedLink WebSocket", cinfo->hostname);
      return -1;
    }

    /* This is now a SeedLink client */
    cinfo->type = CLIENT_SEEDLINK;
  } /* Done with /seedlink request */
  else if (!strncasecmp (path, "/datalink", 9))
  {
    if ((cinfo->protocols & PROTO_DATALINK) == 0)
    {
      lprintf (1, "[%s] Received DataLink WebSocket request on non-DataLink port", cinfo->hostname);

      /* Create header */
      headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                          "HTTP/1.1 400 Cannot request DataLink WebSocket on non-DataLink port\r\n"
                          "Connection: close\r\n"
                          "\r\n"
                          "Cannot request DataLink WebSocket on non-DataLink port");

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, strlen (cinfo->sendbuf));
      }
      else
      {
        lprintf (0, "Error creating response (DataLink WebSocket request on non-DataLink port)");
      }

      return -1;
    }

    lprintf (1, "[%s] Received WebSocket DataLink request", cinfo->hostname);

    if (NegotiateWebSocket (cinfo, version, upgradeHeader, connectionHeader,
                            secWebSocketKeyHeader, secWebSocketVersionHeader))
    {
      lprintf (0, "[%s] Error negotiating DataLink WebSocket", cinfo->hostname);
      return -1;
    }

    /* This is now a DataLink client */
    cinfo->type = CLIENT_DATALINK;
  } /* Done with /datalink request */
  else
  {
    lprintf (1, "[%s] Received HTTP request for %s", cinfo->hostname, path);

    /* If WebRoot is configured send file */
    if (webroot && (rv = SendFileHTTP (cinfo, path)) >= 0)
    {
      lprintf (2, "[%s] Sent %s (%d bytes)", cinfo->hostname, path, rv);
    }
    else
    {
      response =
          "HTTP/1.1 404 Not Found\r\n"
          "Connection: close\r\n"
          "\r\n"
          "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">"
          "<html><head><title>404 Not Found</title></head>"
          "<body><h1>Not Found</h1></body></html>";

      rv = SendData (cinfo, response, strlen (response));

      return (rv) ? -1 : 1;
    }
  } /* Done with file system request */

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleHTTP() */

/***************************************************************************
 * RecvWSFrame:
 *
 * Receive a WebSocket frame, decoding the length and masking key (if
 * present).
 *
 * WebSocket pings and pongs are handled.  If a ping is received an
 * appropriate pong is returned.  If a pong received it is ignored.
 *
 * A WebSocket frame has the following, variable structure:
 *
 *  0               1               2               3
 *  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
 * +-+-+-+-+-------+-+-------------+-------------------------------+
 * |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
 * |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
 * |N|V|V|V|       |S|             |   (if payload len==126/127)   |
 * | |1|2|3|       |K|             |                               |
 * +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
 *  4               5               6               7
 * + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
 * |     Extended payload length continued, if payload len == 127  |
 * + - - - - - - - - - - - - - - - +-------------------------------+
 *  8               9               10              11
 * + - - - - - - - - - - - - - - - +-------------------------------+
 * |                               |Masking-key, if MASK set to 1  |
 * +-------------------------------+-------------------------------+
 *  12              13              14              15
 * +-------------------------------+-------------------------------+
 * | Masking-key (continued)       |          Payload Data         |
 * +-------------------------------- - - - - - - - - - - - - - - - +
 * :                     Payload Data continued ...                :
 * + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
 * |                     Payload Data continued ...                |
 * +---------------------------------------------------------------+
 *
 * Return number of characters read on success, 0 if no data is
 * available, -1 on connection shutdown and -2 on error.
 ***************************************************************************/
int
RecvWSFrame (ClientInfo *cinfo, uint64_t *length, uint32_t *mask)
{
  unsigned char payload[125];
  uint8_t onetwo[2];
  uint16_t length16;
  uint8_t length7;
  ssize_t nrecv;
  int totalrecv = 0;
  int opcode;

  if (!cinfo || !length || !mask)
    return -1;

  /* Recv first two bytes, no handling of fragmented receives */
  nrecv = recv (cinfo->socket, onetwo, 2, 0);

  /* The only acceptable error is EAGAIN (no data on non-blocking) */
  if (nrecv == -1 && errno != EAGAIN)
  {
    lprintf (0, "[%s] Error recv'ing data from client: %s",
             cinfo->hostname, strerror (errno));
    return -2;
  }

  /* No data on non-blocking socket, nothing to read */
  if (nrecv == -1 && errno == EAGAIN)
  {
    return 0;
  }

  /* Peer completed an orderly shutdown */
  if (nrecv == 0)
  {
    return -1;
  }

  /* Check for short read, which we do not handle */
  if (nrecv != 2)
  {
    lprintf (0, "[%s] Error, only read %zd byte of 2 byte WebSocket start",
             cinfo->hostname, nrecv);
    return -2;
  }
  totalrecv += 2;

  /* Check if FIN flag is set, bit 0 of the 1st byte */
  if (!(onetwo[0] & 0x80))
  {
    lprintf (0, "[%s] Error, the WebSocket FIN flag is not set and fragmentation is not supported",
             cinfo->hostname);
    return -2;
  }

  /* Extract payload length */
  length7 = onetwo[1] & 0x7f;

  /* If 126, the length is a 16-bit value in the next 2 bytes */
  if (length7 == 126)
  {
    nrecv = recv (cinfo->socket, &length16, 2, MSG_WAITALL);

    if (nrecv != 2)
      return -2;

    totalrecv += 2;

    if (!ms_bigendianhost ())
      ms_gswap2a (&length16);

    *length = length16;
  }
  /* If 127, the length is a 64-bit value in the next 8 bytes */
  else if (length7 == 127)
  {
    nrecv = recv (cinfo->socket, length, 8, MSG_WAITALL);

    if (nrecv != 8)
      return -2;

    totalrecv += 8;

    if (!ms_bigendianhost ())
      ms_gswap8a (length);
  }
  else
  {
    *length = length7;
  }

  /* If mask flag, the masking key is the next 4 bytes */
  if (onetwo[1] & 0x80)
  {
    nrecv = recv (cinfo->socket, mask, 4, MSG_WAITALL);

    if (nrecv != 4)
      return -2;

    totalrecv += 4;
  }

  /* Extract opcode, bits 4-7 of the 1st byte */
  opcode = onetwo[0] & 0xf;

  /* Check for ping, consume payload and send pong with same payload */
  if (opcode == 0x9)
  {
    if (*length > 125)
    {
      lprintf (0, "[%s] Error, WebSocket payload length of %llu > 125, which is not allowed for a ping",
               cinfo->hostname, (unsigned long long int)*length);
      return -2;
    }

    if (*length > 0)
    {
      nrecv = recv (cinfo->socket, payload, (size_t)*length, MSG_WAITALL);

      if (nrecv != (ssize_t)*length)
      {
        lprintf (0, "[%s] Error receiving payload for WebSocket ping, nrecv: %zu, expected length: %llu\n",
                 cinfo->hostname, (size_t)nrecv, (unsigned long long int)(*length));
        return -2;
      }

      totalrecv += nrecv;
    }

    /* Send pong with same payload data */
    onetwo[0] = 0x8a; /* Change opcode to pong */
    send (cinfo->socket, onetwo, 2, 0);
    send (cinfo->socket, payload, *length, 0);

    return 0;
  }

  /* Check for pong, consume payload and ignore */
  if (opcode == 0xa)
  {
    if (*length > 125)
    {
      lprintf (0, "[%s] Error, WebSocket payload length of %llu > 125, which is not allowed for a pong",
               cinfo->hostname, (unsigned long long int)*length);
      return -2;
    }

    if (*length > 0)
    {
      nrecv = recv (cinfo->socket, payload, (size_t)*length, MSG_WAITALL);

      if (nrecv != (ssize_t)*length)
      {
        lprintf (0, "[%s] Error receiving payload for unexpected WebSocket pong, nrecv: %zu, expected length: %llu\n",
                 cinfo->hostname, (size_t)nrecv, (unsigned long long int)*length);
        return -2;
      }

      totalrecv += nrecv;
    }
  }

  /* Check for Close frame, connection shutdown */
  if (opcode == 0x8)
  {
    /* Send Close frame in response */
    onetwo[0] = 0x88;
    onetwo[1] = 0;
    send (cinfo->socket, onetwo, 2, 0);

    return -1;
  }

  return totalrecv;
} /* End of RecvWSFrame() */

/***************************************************************************
 * ParseHeader:
 *
 * Isolate clean strings for header and value from given buffer.
 * The buffer is modified in place, string terminators are added.
 *
 * The header is terminated at the first space value before the (:)
 * separator.  The value is set to start at the first non-space
 * character after the (:) separator and trailing spaces are trimmed.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
ParseHeader (char *header, char **value)
{
  char *cp = NULL;
  size_t length;

  if (!header || !value)
    return -1;

  /* Find separator (:) and set pointer to value just after it */
  if ((cp = strchr (header, ':')) == NULL)
    return -1;

  *value = cp + 1;

  /* Terminte at separator and backwards until first non-space character */
  do
  {
    *cp = '\0';
    cp--;
  } while (*cp == ' ' && cp != header);

  /* Find first non-space character forward from separator to start value */
  while (**value == ' ' && **value != '\0')
    (*value)++;

  /* Find first non-space character backwards from end of value and terminate */
  length = strlen (*value);
  cp = *value + length - 1;
  while (*cp == ' ' && cp != *value)
    cp--;

  *(cp + 1) = '\0';

  return 0;
} /* End of ParseHeader() */

/***************************************************************************
 * GenerateStreams:
 *
 * Generate stream list and place into buffer, which will be allocated
 * to the length needed and should be free'd by the caller.
 *
 * Check for 'match' parameter in 'path' and use value as a regular
 * expression to match against stream identifiers.
 *
 * Returns length of status in bytes on sucess and -1 on error.
 ***************************************************************************/
static int
GenerateStreams (ClientInfo *cinfo, char **streamlist, char *path)
{
  Stack *streams;
  RingStream *ringstream;
  size_t headlen;
  char streaminfo[200];
  char earliest[50];
  char latest[50];
  char matchstr[50];
  char *cp;
  int matchlen = 0;

  if (!cinfo || !streamlist || !path)
    return -1;

  /* If match parameter is supplied, set reader match to limit streams */
  if ((cp = strstr (path, "match=")))
  {
    cp += 6; /* Advance to character after '=' */

    /* Copy parameter value into matchstr, stop at terminator, '&' or max length */
    for (matchlen = 0; *cp && *cp != '&' && matchlen < sizeof (matchstr); cp++, matchlen++)
    {
      matchstr[matchlen] = *cp;
    }
    matchstr[matchlen] = '\0';

    if (matchlen > 0 && cinfo->reader)
    {
      RingMatch (cinfo->reader, matchstr);
    }
  }

  /* Collect stream list and send a line for each stream */
  if ((streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)))
  {
    while ((ringstream = (RingStream *)StackPop (streams)))
    {
      ms_hptime2isotimestr (ringstream->earliestdstime, earliest, 1);
      ms_hptime2isotimestr (ringstream->latestdetime, latest, 1);

      snprintf (streaminfo, sizeof (streaminfo), "%s  %s  %s\n",
                ringstream->streamid, earliest, latest);
      streaminfo[sizeof (streaminfo) - 1] = '\0';

      /* Add a line to the response body for each stream, 8 MB maximum */
      if (AddToString (streamlist, streaminfo, "", 0, 8388608))
      {
        lprintf (0, "[%s] Error for HTTP STREAMS (cannot AddToString), too many streams",
                 cinfo->hostname);

        free (ringstream);
        StackDestroy (streams, free);

        /* Create and send error response */
        headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                            "HTTP/1.1 500 Stream list too large\r\n"
                            "Connection: close\r\n"
                            "\r\n"
                            "Stream list too large");

        if (headlen > 0)
        {
          SendData (cinfo, cinfo->sendbuf, strlen (cinfo->sendbuf));
        }
        else
        {
          lprintf (0, "Error creating response (stream list too large)");
        }

        return -1;
      }

      free (ringstream);
    }

    /* Cleanup stream stack */
    StackDestroy (streams, free);
  }

  /* Clear match expression if set for this request */
  if (matchlen > 0 && cinfo->reader)
  {
    RingMatch (cinfo->reader, NULL);
  }

  return (*streamlist) ? strlen (*streamlist) : 0;
} /* End of GenerateStreams() */

/***************************************************************************
 * GenerateStatus:
 *
 * Generate server status and place into buffer, which will be
 * allocated to the length needed and should be free'd by the caller.
 *
 * Returns length of status in bytes on sucess and -1 on error.
 ***************************************************************************/
static int
GenerateStatus (ClientInfo *cinfo, char **status)
{
  struct sthread *loopstp;
  char string[400];
  char serverstart[50];
  char ringversion[15];
  char ringsize[30];
  char packetsize[10];
  char maxpacketid[20];
  char maxpackets[20];
  char memorymapped[10];
  char volatileflag[10];
  char totalconnections[10];
  char totalstreams[10];
  char txpacketrate[10];
  char txbyterate[10];
  char rxpacketrate[10];
  char rxbyterate[10];
  char earliestpacketid[20];
  char earliestpacketcreate[50];
  char earliestpacketstart[50];
  char earliestpacketend[50];
  char latestpacketid[20];
  char latestpacketcreate[50];
  char latestpacketstart[50];
  char latestpacketend[50];
  int rv;

  if (!cinfo || !status)
    return -1;

  ms_hptime2mdtimestr (serverstarttime, serverstart, 0);
  snprintf (ringversion, sizeof (ringversion), "%u", (unsigned int)cinfo->ringparams->version);
  snprintf (ringsize, sizeof (ringsize), "%" PRIu64, cinfo->ringparams->ringsize);
  snprintf (packetsize, sizeof (packetsize), "%lu",
            (unsigned long int)(cinfo->ringparams->pktsize - sizeof (RingPacket)));
  snprintf (maxpacketid, sizeof (maxpacketid), "%" PRId64, cinfo->ringparams->maxpktid);
  snprintf (maxpackets, sizeof (maxpackets), "%" PRId64, cinfo->ringparams->maxpackets);
  snprintf (memorymapped, sizeof (memorymapped), "%s", (cinfo->ringparams->mmapflag) ? "TRUE" : "FALSE");
  snprintf (volatileflag, sizeof (volatileflag), "%s", (cinfo->ringparams->volatileflag) ? "TRUE" : "FALSE");
  snprintf (totalconnections, sizeof (totalconnections), "%d", clientcount);
  snprintf (totalstreams, sizeof (totalstreams), "%d", cinfo->ringparams->streamcount);
  snprintf (txpacketrate, sizeof (txpacketrate), "%.1f", cinfo->ringparams->txpacketrate);
  snprintf (txbyterate, sizeof (txbyterate), "%.1f", cinfo->ringparams->txbyterate);
  snprintf (rxpacketrate, sizeof (rxpacketrate), "%.1f", cinfo->ringparams->rxpacketrate);
  snprintf (rxbyterate, sizeof (rxbyterate), "%.1f", cinfo->ringparams->rxbyterate);
  snprintf (earliestpacketid, sizeof (earliestpacketid), "%" PRId64, cinfo->ringparams->earliestid);
  ms_hptime2mdtimestr (cinfo->ringparams->earliestptime, earliestpacketcreate, 1);
  ms_hptime2mdtimestr (cinfo->ringparams->earliestdstime, earliestpacketstart, 1);
  ms_hptime2mdtimestr (cinfo->ringparams->earliestdetime, earliestpacketend, 1);
  snprintf (latestpacketid, sizeof (latestpacketid), "%" PRId64, cinfo->ringparams->latestid);
  ms_hptime2mdtimestr (cinfo->ringparams->latestptime, latestpacketcreate, 1);
  ms_hptime2mdtimestr (cinfo->ringparams->latestdstime, latestpacketstart, 1);
  ms_hptime2mdtimestr (cinfo->ringparams->latestdetime, latestpacketend, 1);

  rv = asprintf (status,
                 "%s/%s\n"
                 "Organization: %s\n"
                 "Server start time (UTC): %s\n"
                 "Ring version: %s\n"
                 "Ring size: %s\n"
                 "Packet size: %s\n"
                 "Max packet ID: %s\n"
                 "Max packets: %s\n"
                 "Memory mapped ring: %s\n"
                 "Volatile ring: %s\n"
                 "Total connections: %s\n"
                 "Total streams: %s\n"
                 "TX packet rate: %s\n"
                 "TX byte rate: %s\n"
                 "RX packet rate: %s\n"
                 "RX byte rate: %s\n"
                 "Earliest packet: %s\n"
                 "  Create: %s  Data start: %s  Data end: %s\n"
                 "Latest packet: %s\n"
                 "  Create: %s  Data start: %s  Data end: %s\n",
                 PACKAGE, VERSION,
                 serverid,
                 serverstart,
                 ringversion,
                 ringsize,
                 packetsize,
                 maxpacketid,
                 maxpackets,
                 memorymapped,
                 volatileflag,
                 totalconnections,
                 totalstreams,
                 txpacketrate,
                 txbyterate,
                 rxpacketrate,
                 rxbyterate,
                 earliestpacketid,
                 earliestpacketcreate, earliestpacketstart, earliestpacketend,
                 latestpacketid,
                 latestpacketcreate, latestpacketstart, latestpacketend);

  AddToString (status, "\nServer threads:\n", "", 0, 8388608);

  /* List server threads, lock thread list while looping */
  pthread_mutex_lock (&sthreads_lock);
  loopstp = sthreads;
  while (loopstp)
  {
    if (loopstp->type == LISTEN_THREAD)
    {
      char protocolstr[100];
      ListenPortParams *lpp = loopstp->params;

      if (GenProtocolString (lpp->protocols, protocolstr, sizeof (protocolstr)) > 0)
      {
        if (snprintf (string, sizeof (string),
                      "  %s, Port: %s\n", protocolstr, lpp->portstr) > 0)
          AddToString (status, string, "", 0, 8388608);
      }
    }
    else if (loopstp->type == MSEEDSCAN_THREAD)
    {
      MSScanInfo *mssinfo = loopstp->params;

      snprintf (string, sizeof (string),
                "  Mini-SEED Scanner\n"
                "    Directory: %s\n"
                "    Max recursion: %d\n"
                "    State file: %s\n"
                "    Match: %s\n"
                "    Reject: %s\n"
                "    Scan time: %g\n"
                "    Packet rate: %g\n"
                "    Byte rate: %g\n",
                mssinfo->dirname,
                mssinfo->maxrecur,
                mssinfo->statefile,
                mssinfo->matchstr,
                mssinfo->rejectstr,
                mssinfo->scantime,
                mssinfo->rxpacketrate,
                mssinfo->rxbyterate);

      AddToString (status, string, "", 0, 8388608);
    }
    else
    {
      AddToString (status, "  Unknown Thread\n", "", 0, 8388608);
    }

    loopstp = loopstp->next;
  }
  pthread_mutex_unlock (&sthreads_lock);

  return (rv < 0) ? -1 : rv;
} /* End of GenerateStatus() */

/***************************************************************************
 * GenerateConnections:
 *
 * Generate connection list and place into buffer, which will be allocated
 * to the length needed and should be free'd by the caller.
 *
 * Check for 'match' parameter in 'path' and use value as a regular
 * expression to match against stream identifiers.
 *
 * Returns length of status in bytes on sucess and -1 on error.
 ***************************************************************************/
static int
GenerateConnections (ClientInfo *cinfo, char **connectionlist, char *path)
{
  struct cthread *loopctp;
  ClientInfo *tcinfo;
  hptime_t hpnow;
  int totalcount = 0;
  int selectedcount = 0;
  char conninfo[1024];
  char *conntype;
  char conntime[50];
  char packettime[50];
  char datastart[50];
  char dataend[50];
  char lagstr[5];

  char matchstr[50];
  char *cp;
  int matchlen = 0;
  pcre *match = 0;
  const char *errptr;
  int erroffset;

  if (!cinfo || !connectionlist || !path)
    return -1;

  /* If match parameter is supplied, set reader match to limit streams */
  if ((cp = strstr (path, "match=")))
  {
    cp += 6; /* Advance to character after '=' */

    /* Copy parameter value into matchstr, stop at terminator, '&' or max length */
    for (matchlen = 0; *cp && *cp != '&' && matchlen < sizeof (matchstr); cp++, matchlen++)
    {
      matchstr[matchlen] = *cp;
    }
    matchstr[matchlen] = '\0';

    if (matchlen > 0 && cinfo->reader)
    {
      RingMatch (cinfo->reader, matchstr);
    }
  }

  /* Compile match expression supplied with request */
  if (matchlen > 0)
  {
    match = pcre_compile (matchstr, 0, &errptr, &erroffset, NULL);
    if (errptr)
    {
      lprintf (0, "[%s] Error with pcre_compile: %s", errptr);
      matchlen = 0;
    }
  }

  /* Get current time */
  hpnow = HPnow ();

  /* List connections, lock client list while looping */
  pthread_mutex_lock (&cthreads_lock);
  loopctp = cthreads;
  while (loopctp)
  {
    /* Skip if client thread is not in ACTIVE state */
    if (!(loopctp->td->td_flags & TDF_ACTIVE))
    {
      loopctp = loopctp->next;
      continue;
    }

    totalcount++;
    tcinfo = (ClientInfo *)loopctp->td->td_prvtptr;

    /* Check matching expression against the client address string (host:port) and client ID */
    if (match)
      if (pcre_exec (match, NULL, tcinfo->hostname, strlen (tcinfo->hostname), 0, 0, NULL, 0) &&
          pcre_exec (match, NULL, tcinfo->ipstr, strlen (tcinfo->ipstr), 0, 0, NULL, 0) &&
          pcre_exec (match, NULL, tcinfo->clientid, strlen (tcinfo->clientid), 0, 0, NULL, 0))
      {
        loopctp = loopctp->next;
        continue;
      }

    /* Determine connection type */
    if (tcinfo->type == CLIENT_DATALINK)
    {
      if (tcinfo->websocket)
        conntype = "WebSocket DataLink";
      else
        conntype = "DataLink";
    }
    else if (tcinfo->type == CLIENT_SEEDLINK)
    {
      if (tcinfo->websocket)
        conntype = "WebSocket SeedLink";
      else
        conntype = "SeedLink";
    }
    else
    {
      conntype = "Unknown";
    }

    ms_hptime2mdtimestr (tcinfo->conntime, conntime, 1);
    ms_hptime2mdtimestr (tcinfo->reader->pkttime, packettime, 1);
    ms_hptime2mdtimestr (tcinfo->reader->datastart, datastart, 1);
    ms_hptime2mdtimestr (tcinfo->reader->dataend, dataend, 1);

    if (tcinfo->reader->pktid <= 0)
      strncpy (lagstr, "-", sizeof (lagstr));
    else
      snprintf (lagstr, sizeof (lagstr), "%d%%", tcinfo->percentlag);

    snprintf (conninfo, sizeof (conninfo),
              "%s [%s:%s]\n"
              "  [%s] %s  %s\n"
              "  Packet %" PRId64 " (%s)  Lag %s, %.1f\n"
              "  TX %" PRId64 " packets, %.1f packets/sec  %" PRId64 " bytes, %.1f bytes/sec\n"
              "  RX %" PRId64 " packets, %.1f packets/sec  %" PRId64 " bytes, %.1f bytes/sec\n"
              "  Stream count: %d\n"
              "  Match: %.50s\n"
              "  Reject: %.50s\n\n",
              tcinfo->hostname, tcinfo->ipstr, tcinfo->portstr,
              conntype, tcinfo->clientid, conntime,
              tcinfo->reader->pktid, packettime, lagstr,
              (double)MS_HPTIME2EPOCH ((hpnow - tcinfo->lastxchange)),
              tcinfo->txpackets[0], tcinfo->txpacketrate, tcinfo->txbytes[0], tcinfo->txbyterate,
              tcinfo->rxpackets[0], tcinfo->rxpacketrate, tcinfo->rxbytes[0], tcinfo->rxbyterate,
              tcinfo->streamscount,
              (tcinfo->matchstr) ? tcinfo->matchstr : "-",
              (tcinfo->rejectstr) ? tcinfo->rejectstr : "-");

    AddToString (connectionlist, conninfo, "", 0, 8388608);

    selectedcount++;
    loopctp = loopctp->next;
  }
  pthread_mutex_unlock (&cthreads_lock);

  snprintf (conninfo, sizeof (conninfo),
            "%d of %d connections\n",
            selectedcount, totalcount);

  AddToString (connectionlist, conninfo, "", 0, 8388608);

  /* Free compiled match expression */
  if (match)
    pcre_free (match);

  return (*connectionlist) ? strlen (*connectionlist) : 0;
} /* End of GenerateConnections() */

/***************************************************************************
 * SendFileHTTP:
 *
 * Send file specified by path via HTTP.  If path is a directory check
 * for a file named 'index.html' within the directory.
 *
 * Returns number of bytes sent on success and -1 on error or file not found.
 ***************************************************************************/
static int
SendFileHTTP (ClientInfo *cinfo, char *path)
{
  FILE *fp = NULL;
  struct stat filestat;
  char *contenttype = "application/octet-stream";
  char *response = NULL;
  char *webpath = NULL;
  char *filename = NULL;
  char *indexfile = NULL;
  char *cp = NULL;
  char filebuffer[65535];
  size_t bytes;
  size_t length;
  int rv;

  if (!path || !cinfo)
    return -1;

  /* Build path using web root and resolve absolute */
  if (asprintf (&webpath, "%s/%s", webroot, path) < 0)
    return -1;

  filename = realpath (webpath, NULL);
  if (filename == NULL)
  {
    lprintf (0, "Error resolving path to requested file: %s", webpath);
    return -1;
  }

  free (webpath);

  /* Sanity check that file is within web root */
  if (strncmp (webroot, filename, strlen (webroot)))
  {
    lprintf (0, "Refusing to send file outside of WebRoot: %s", filename);
    return -1;
  }

  if (stat (filename, &filestat))
    return -1;

  /* If directory and check for index.html */
  if (S_ISDIR (filestat.st_mode))
  {
    if (asprintf (&indexfile, "%s/index.html", filename) < 0)
      return -1;

    if (stat (indexfile, &filestat))
      return -1;

    if (filename)
      free (filename);

    filename = indexfile;
  }

  /* Check for extension and set Content-Type accordingly, hopefully it's true */
  cp = strrchr (filename, '.');
  if (cp)
  {
    length = strlen (cp);

    if (length == 5 && !strcmp (cp, ".html"))
      contenttype = "text/html";
    else if (length == 3 && !strcmp (cp, ".js"))
      contenttype = "application/javascript";
    else if (length == 4 && !strcmp (cp, ".txt"))
      contenttype = "text/plain";
  }

  if ((fp = fopen (filename, "r")) == NULL)
  {
    lprintf (0, "Error opening file %s:  %s",
             filename, strerror (errno));
    return -1;
  }

  /* Create header */
  rv = asprintf (&response, "HTTP/1.1 200\r\n"
                            "Content-Length: %llu\r\n"
                            "Content-Type: %s\n"
                            "\r\n",
                 (long long unsigned int)filestat.st_size,
                 contenttype);

  if (rv < 0)
  {
    fclose (fp);
    return -1;
  }

  /* Send header and file */
  SendData (cinfo, response, strlen (response));

  while ((bytes = fread (filebuffer, 1, sizeof (filebuffer), fp)) > 0)
  {
    if (SendData (cinfo, filebuffer, bytes) < 0)
      break;
  }

  fclose (fp);

  if (filename)
    free (filename);

  if (response)
    free (response);

  return (cinfo->socketerr) ? -1 : filestat.st_size;
} /* End of SendFileHTTP() */

/***************************************************************************
 * NegotiateWebSocket:
 *
 * Negotiate a WebSocket connection and set the ClientInfo.websocket flag.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
NegotiateWebSocket (ClientInfo *cinfo, char *version,
                    char *upgradeHeader, char *connectionHeader,
                    char *secWebSocketKeyHeader, char *secWebSocketVersionHeader)
{
  char *response = NULL;
  unsigned char *keybuf = NULL;
  unsigned char sha1digest[SHA1_DIGEST_SIZE];

  int keybufsize;
  int keylength;

  if (!cinfo || !version)
    return -1;

  /* Check for required headers and expected values:
   *   Protocol version must be HTTP/1.1
   *   Connection: Upgrade
   *   Upgrade: websocket
   *   Sec-WebSocket-Version: 13
   *   Sec-WebSocket-Key: <something> */

  if (!version || strcasecmp (version, "HTTP/1.1"))
  {
    asprintf (&response,
              "HTTP/1.1 400 Protocol Version Must Be 1.1 For WebSocket\r\n"
              "\r\n"
              "HTTP Version Must Be 1.1 For WebSocket\n"
              "Received: '%s'\n",
              (version) ? version : "");

    if (response)
    {
      SendData (cinfo, response, strlen (response));
      free (response);
    }
    else
    {
      lprintf (0, "Cannot allocate memory for response (wrong protocol)");
    }

    return -1;
  }

  if (!upgradeHeader || strcasecmp (upgradeHeader, "websocket"))
  {
    asprintf (&response,
              "HTTP/1.1 400 Upgrade Header Not Recognized\r\n"
              "\r\n"
              "The Upgrade header value must be 'websocket'\n"
              "Received: '%s'\n",
              (upgradeHeader) ? upgradeHeader : "");

    if (response)
    {
      SendData (cinfo, response, strlen (response));
      free (response);
    }
    else
    {
      lprintf (0, "Cannot allocate memory for response (wrong Upgrade)");
    }

    return -1;
  }

  if (!connectionHeader || !strcasestr (connectionHeader, "Upgrade"))
  {
    asprintf (&response,
              "HTTP/1.1 400 Connection Header Not Recognized\r\n"
              "\r\n"
              "The Connection header value must be 'Upgrade'\n"
              "Received: '%s'\n",
              (connectionHeader) ? connectionHeader : "");

    if (response)
    {
      SendData (cinfo, response, strlen (response));
      free (response);
    }
    else
    {
      lprintf (0, "Cannot allocate memory for response (wrong Connection)");
    }

    return -1;
  }

  if (!secWebSocketVersionHeader || strcasecmp (secWebSocketVersionHeader, "13"))
  {
    asprintf (&response,
              "HTTP/1.1 400 Sec-WebSocket-Version Must Be 13\r\n"
              "\r\n"
              "The Sec-WebSocket-Key header is required\n"
              "Received: '%s'\n",
              (secWebSocketVersionHeader) ? secWebSocketVersionHeader : "");

    if (response)
    {
      SendData (cinfo, response, strlen (response));
      free (response);
    }
    else
    {
      lprintf (0, "Cannot allocate memory for response (wrong Sec-WebSocket-Version)");
    }

    return -1;
  }

  if (!secWebSocketKeyHeader)
  {
    asprintf (&response,
              "HTTP/1.1 400 Sec-WebSocket-Key Header Must Be Present\r\n"
              "\r\n"
              "The Sec-WebSocket-Key header is required\n");

    if (response)
    {
      SendData (cinfo, response, strlen (response));
      free (response);
    }
    else
    {
      lprintf (0, "Cannot allocate memory for response (wrong Sec-WebSocket-Key) ");
    }

    return -1;
  }

  /* Allocate space for key + 36 bytes for magic string */
  keylength = strlen (secWebSocketKeyHeader);
  keybufsize = keylength + 36;
  if (!(keybuf = (unsigned char *)malloc (keybufsize)))
  {
    lprintf (0, "Cannot allocate memory for decoded key buffer (%zu bytes)", keybufsize);
    return -1;
  }

  /* Concatinate key and WebSocket magic string */
  memcpy (keybuf, secWebSocketKeyHeader, keylength);
  memcpy (keybuf + keylength, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11", 36);

  /* Calculate SHA-1 hash and then Base64 encode the combined value */
  sha1 (sha1digest, keybuf, keybufsize);
  Base64encode ((char *)keybuf, (char *)sha1digest, SHA1_DIGEST_SIZE);

  /* Generate response completing the upgrade to WebSocket connection */
  asprintf (&response,
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: %s\r\n"
            "\r\n",
            keybuf);

  if (response)
  {
    SendData (cinfo, response, strlen (response));
    free (response);
  }
  else
  {
    lprintf (0, "Cannot allocate memory for response (WebSocket upgrade)");
    free (keybuf);
    return -1;
  }

  cinfo->websocket = 1;

  free (keybuf);

  return 0;
} /* End of NegotiateWebSocket() */

/* The Base64encode() below were retrieved from
 * www.opensource.apple.com in November 2016.  All headers/licenses
 * remain intact, some code was removed and formatted. */

/*
 * Copyright (c) 2003 Apple Computer, Inc. All rights reserved.
 *
 * @APPLE_LICENSE_HEADER_START@
 *
 * Copyright (c) 1999-2003 Apple Computer, Inc.  All Rights Reserved.
 *
 * This file contains Original Code and/or Modifications of Original Code
 * as defined in and that are subject to the Apple Public Source License
 * Version 2.0 (the 'License'). You may not use this file except in
 * compliance with the License. Please obtain a copy of the License at
 * http://www.opensource.apple.com/apsl/ and read it before using this
 * file.
 *
 * The Original Code and all software distributed under the License are
 * distributed on an 'AS IS' basis, WITHOUT WARRANTY OF ANY KIND, EITHER
 * EXPRESS OR IMPLIED, AND APPLE HEREBY DISCLAIMS ALL SUCH WARRANTIES,
 * INCLUDING WITHOUT LIMITATION, ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, QUIET ENJOYMENT OR NON-INFRINGEMENT.
 * Please see the License for the specific language governing rights and
 * limitations under the License.
 *
 * @APPLE_LICENSE_HEADER_END@
 */
/* ====================================================================
 * Copyright (c) 1995-1999 The Apache Group.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the Apache Group
 *    for use in the Apache HTTP server project (http://www.apache.org/)."
 *
 * 4. The names "Apache Server" and "Apache Group" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache"
 *    nor may "Apache" appear in their names without prior written
 *    permission of the Apache Group.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the Apache Group
 *    for use in the Apache HTTP server project (http://www.apache.org/)."
 *
 * THIS SOFTWARE IS PROVIDED BY THE APACHE GROUP ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE APACHE GROUP OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Group and was originally based
 * on public domain software written at the National Center for
 * Supercomputing Applications, University of Illinois, Urbana-Champaign.
 * For more information on the Apache Group and the Apache HTTP server
 * project, please see <http://www.apache.org/>.
 *
 */

/* Base64 encoder/decoder. Originally Apache file ap_base64.c
 */

static const char basis_64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* Base64 encode specified string.
 * Return: length of encoded data. */
int
Base64encode (char *encoded, const char *string, int len)
{
  int i;
  char *p;

  p = encoded;
  for (i = 0; i < len - 2; i += 3)
  {
    *p++ = basis_64[(string[i] >> 2) & 0x3F];
    *p++ = basis_64[((string[i] & 0x3) << 4) |
                    ((int)(string[i + 1] & 0xF0) >> 4)];
    *p++ = basis_64[((string[i + 1] & 0xF) << 2) |
                    ((int)(string[i + 2] & 0xC0) >> 6)];
    *p++ = basis_64[string[i + 2] & 0x3F];
  }
  if (i < len)
  {
    *p++ = basis_64[(string[i] >> 2) & 0x3F];
    if (i == (len - 1))
    {
      *p++ = basis_64[((string[i] & 0x3) << 4)];
      *p++ = '=';
    }
    else
    {
      *p++ = basis_64[((string[i] & 0x3) << 4) |
                      ((int)(string[i + 1] & 0xF0) >> 4)];
      *p++ = basis_64[((string[i + 1] & 0xF) << 2)];
    }
    *p++ = '=';
  }

  *p++ = '\0';
  return p - encoded;
}

/* The sha1*() routines below were retrieved from
 * https://github.com/dottedmag/libsha1 in November 2016.  All
 * headers/licenses remain intact, some code was removed and
 * formatted. */

/*
  ---------------------------------------------------------------------------
  Copyright (c) 2002, Dr Brian Gladman, Worcester, UK.   All rights reserved.
  LICENSE TERMS
  The free distribution and use of this software in both source and binary
  form is allowed (with or without changes) provided that:
  1. distributions of this source code include the above copyright
  notice, this list of conditions and the following disclaimer;
  2. distributions in binary form include the above copyright
  notice, this list of conditions and the following disclaimer
  in the documentation and/or other associated materials;
  3. the copyright holder's name is not used to endorse products
  built using this software without specific written permission.
  ALTERNATIVELY, provided that this notice is retained in full, this product
  may be distributed under the terms of the GNU General Public License (GPL),
  in which case the provisions of the GPL apply INSTEAD OF those given above.
  DISCLAIMER
  This software is provided 'as is' with no explicit or implied warranties
  in respect of its properties, including, but not limited to, correctness
  and/or fitness for purpose.
  ---------------------------------------------------------------------------
  Issue Date: 01/08/2005
  This is a byte oriented version of SHA1 that operates on arrays of bytes
  stored in memory.
*/

#define SHA1_BLOCK_SIZE 64

#define rotl32(x, n) (((x) << n) | ((x) >> (32 - n)))
#define rotr32(x, n) (((x) >> n) | ((x) << (32 - n)))

#define bswap_32(x) ((rotr32 ((x), 24) & 0x00ff00ff) | (rotr32 ((x), 8) & 0xff00ff00))

#define bsw_32(p, n)                                        \
  {                                                         \
    int _i = (n);                                           \
    while (_i--)                                            \
      ((uint32_t *)p)[_i] = bswap_32 (((uint32_t *)p)[_i]); \
  }

#define SHA1_MASK (SHA1_BLOCK_SIZE - 1)

#if 0
#define ch(x, y, z) (((x) & (y)) ^ (~(x) & (z)))
#define parity(x, y, z) ((x) ^ (y) ^ (z))
#define maj(x, y, z) (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))
#else /* Discovered by Rich Schroeppel and Colin Plumb   */

#define ch(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
#define parity(x, y, z) ((x) ^ (y) ^ (z))
#define maj(x, y, z) (((x) & (y)) | ((z) & ((x) ^ (y))))

#endif

/* Compile 64 bytes of hash data into SHA1 context. Note    */
/* that this routine assumes that the byte order in the     */
/* ctx->wbuf[] at this point is in such an order that low   */
/* address bytes in the ORIGINAL byte stream will go in     */
/* this buffer to the high end of 32-bit words on BOTH big  */
/* and little endian systems                                */

#ifdef ARRAY
#define q(v, n) v[n]
#else
#define q(v, n) v##n
#endif

#define one_cycle(v, a, b, c, d, e, f, k, h)            \
  q (v, e) += rotr32 (q (v, a), 27) +                   \
              f (q (v, b), q (v, c), q (v, d)) + k + h; \
  q (v, b) = rotr32 (q (v, b), 2)

#define five_cycle(v, f, k, i)                    \
  one_cycle (v, 0, 1, 2, 3, 4, f, k, hf (i));     \
  one_cycle (v, 4, 0, 1, 2, 3, f, k, hf (i + 1)); \
  one_cycle (v, 3, 4, 0, 1, 2, f, k, hf (i + 2)); \
  one_cycle (v, 2, 3, 4, 0, 1, f, k, hf (i + 3)); \
  one_cycle (v, 1, 2, 3, 4, 0, f, k, hf (i + 4))

static void
sha1_compile (sha1_ctx ctx[1])
{
  uint32_t *w = ctx->wbuf;

#ifdef ARRAY
  uint32_t v[5];
  memcpy (v, ctx->hash, 5 * sizeof (uint32_t));
#else
  uint32_t v0, v1, v2, v3, v4;
  v0 = ctx->hash[0];
  v1 = ctx->hash[1];
  v2 = ctx->hash[2];
  v3 = ctx->hash[3];
  v4 = ctx->hash[4];
#endif

#define hf(i) w[i]

  five_cycle (v, ch, 0x5a827999, 0);
  five_cycle (v, ch, 0x5a827999, 5);
  five_cycle (v, ch, 0x5a827999, 10);
  one_cycle (v, 0, 1, 2, 3, 4, ch, 0x5a827999, hf (15));

#undef hf
#define hf(i) (w[(i)&15] = rotl32 ( \
                   w[((i) + 13) & 15] ^ w[((i) + 8) & 15] ^ w[((i) + 2) & 15] ^ w[(i)&15], 1))

  one_cycle (v, 4, 0, 1, 2, 3, ch, 0x5a827999, hf (16));
  one_cycle (v, 3, 4, 0, 1, 2, ch, 0x5a827999, hf (17));
  one_cycle (v, 2, 3, 4, 0, 1, ch, 0x5a827999, hf (18));
  one_cycle (v, 1, 2, 3, 4, 0, ch, 0x5a827999, hf (19));

  five_cycle (v, parity, 0x6ed9eba1, 20);
  five_cycle (v, parity, 0x6ed9eba1, 25);
  five_cycle (v, parity, 0x6ed9eba1, 30);
  five_cycle (v, parity, 0x6ed9eba1, 35);

  five_cycle (v, maj, 0x8f1bbcdc, 40);
  five_cycle (v, maj, 0x8f1bbcdc, 45);
  five_cycle (v, maj, 0x8f1bbcdc, 50);
  five_cycle (v, maj, 0x8f1bbcdc, 55);

  five_cycle (v, parity, 0xca62c1d6, 60);
  five_cycle (v, parity, 0xca62c1d6, 65);
  five_cycle (v, parity, 0xca62c1d6, 70);
  five_cycle (v, parity, 0xca62c1d6, 75);

#ifdef ARRAY
  ctx->hash[0] += v[0];
  ctx->hash[1] += v[1];
  ctx->hash[2] += v[2];
  ctx->hash[3] += v[3];
  ctx->hash[4] += v[4];
#else
  ctx->hash[0] += v0;
  ctx->hash[1] += v1;
  ctx->hash[2] += v2;
  ctx->hash[3] += v3;
  ctx->hash[4] += v4;
#endif
}

void
sha1_begin (sha1_ctx ctx[1])
{
  ctx->count[0] = ctx->count[1] = 0;
  ctx->hash[0] = 0x67452301;
  ctx->hash[1] = 0xefcdab89;
  ctx->hash[2] = 0x98badcfe;
  ctx->hash[3] = 0x10325476;
  ctx->hash[4] = 0xc3d2e1f0;
}

/* SHA1 hash data in an array of bytes into hash buffer and */
/* call the hash_compile function as required.              */

void
sha1_hash (const unsigned char data[], unsigned long len, sha1_ctx ctx[1])
{
  uint32_t pos = (uint32_t) (ctx->count[0] & SHA1_MASK),
           space = SHA1_BLOCK_SIZE - pos;
  const unsigned char *sp = data;

  if ((ctx->count[0] += len) < len)
    ++(ctx->count[1]);

  while (len >= space) /* tranfer whole blocks if possible  */
  {
    memcpy (((unsigned char *)ctx->wbuf) + pos, sp, space);
    sp += space;
    len -= space;
    space = SHA1_BLOCK_SIZE;
    pos = 0;
    if (!ms_bigendianhost ())
      bsw_32 (ctx->wbuf, SHA1_BLOCK_SIZE >> 2);
    sha1_compile (ctx);
  }

  memcpy (((unsigned char *)ctx->wbuf) + pos, sp, len);
}

/* SHA1 final padding and digest calculation  */

void
sha1_end (unsigned char hval[], sha1_ctx ctx[1])
{
  uint32_t i = (uint32_t) (ctx->count[0] & SHA1_MASK);

  /* put bytes in the buffer in an order in which references to   */
  /* 32-bit words will put bytes with lower addresses into the    */
  /* top of 32 bit words on BOTH big and little endian machines   */
  if (!ms_bigendianhost ())
    bsw_32 (ctx->wbuf, (i + 3) >> 2);

  /* we now need to mask valid bytes and add the padding which is */
  /* a single 1 bit and as many zero bits as necessary. Note that */
  /* we can always add the first padding byte here because the    */
  /* buffer always has at least one empty slot                    */
  ctx->wbuf[i >> 2] &= 0xffffff80 << 8 * (~i & 3);
  ctx->wbuf[i >> 2] |= 0x00000080 << 8 * (~i & 3);

  /* we need 9 or more empty positions, one for the padding byte  */
  /* (above) and eight for the length count. If there is not      */
  /* enough space, pad and empty the buffer                       */
  if (i > SHA1_BLOCK_SIZE - 9)
  {
    if (i < 60)
      ctx->wbuf[15] = 0;
    sha1_compile (ctx);
    i = 0;
  }
  else /* compute a word index for the empty buffer positions  */
    i = (i >> 2) + 1;

  while (i < 14) /* and zero pad all but last two positions        */
    ctx->wbuf[i++] = 0;

  /* the following 32-bit length fields are assembled in the      */
  /* wrong byte order on little endian machines but this is       */
  /* corrected later since they are only ever used as 32-bit      */
  /* word values.                                                 */
  ctx->wbuf[14] = (ctx->count[1] << 3) | (ctx->count[0] >> 29);
  ctx->wbuf[15] = ctx->count[0] << 3;
  sha1_compile (ctx);

  /* extract the hash value as bytes in case the hash buffer is   */
  /* misaligned for 32-bit words                                  */
  for (i = 0; i < SHA1_DIGEST_SIZE; ++i)
    hval[i] = (unsigned char)(ctx->hash[i >> 2] >> (8 * (~i & 3)));
}

void
sha1 (unsigned char hval[], const unsigned char data[], unsigned long len)
{
  sha1_ctx cx[1];

  sha1_begin (cx);
  sha1_hash (data, len, cx);
  sha1_end (hval, cx);
}

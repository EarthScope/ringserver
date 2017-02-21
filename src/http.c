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
 * Modified: 2017.052
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

#define MIN(X,Y) (X < Y) ? X : Y

static int ParseHeader (char *header, char **value);
static int GenerateStreams (ClientInfo *cinfo, char **streamlist, char *path);
static int GenerateStatus (ClientInfo *cinfo, char **status);
static int GenerateConnections (ClientInfo *cinfo, char **connectionlist, char *path);
static int SendFileHTTP (ClientInfo *cinfo, char *path);
static int NegotiateWebSocket (ClientInfo *cinfo, char *version,
                               char *upgradeHeader, char *connectionHeader,
                               char *secWebSocketKeyHeader, char *secWebSocketVersionHeader,
                               char *secWebSocketProtocolHeader);
static int apr_base64_encode_binary (char *encoded, const unsigned char *string, int len);
static int sha1digest(uint8_t *digest, char *hexdigest, const uint8_t *data, size_t databytes);

/***************************************************************************
 * HandleHTTP:
 *
 * Handle HTTP requests using an extremely limited HTTP server
 * implementation.
 *
 * The following end points are handled:
 *   /id          - return server ID and version
 *   /streams     - return list of server streams
 *                    match=<pattern> supported to limit streams
 *                    limit=<1-6> specified level of ID
 *   /status      - return server status, limited via trust-permissions
 *   /connections - return list of connections, limited via trust-permissions
 *                    match=<pattern> supported to limit connections
 *   /seedlink    - initiate WebSocket connection for SeedLink
 *   /datalink    - initiate WebSocket connection for DataLink
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
  char secWebSocketProtocolHeader[100] = "";

  char *response = NULL;
  char *value = NULL;
  int responsebytes;

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
      SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
    }
    else
    {
      lprintf (0, "Error creating response (unrecognized method)");
    }

    return -1;
  }

  /* Consume all request headers, the empty line '\r\n' terminates */
  while ((nread = RecvLine (cinfo)) > 0)
  {
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
    else if (!strcasecmp (cinfo->recvbuf, "Sec-WebSocket-Protocol"))
    {
      strncpy (secWebSocketProtocolHeader, value, sizeof (secWebSocketProtocolHeader) - 1);
      secWebSocketProtocolHeader[sizeof (secWebSocketProtocolHeader) - 1] = '\0';
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

    responsebytes = asprintf (&response,
                              "%s/%s\n"
                              "Organization: %s",
                              PACKAGE, VERSION, serverid);

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %d\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? responsebytes : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){MIN(headlen,cinfo->sendbuflen), (response) ? responsebytes : 0},
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

    responsebytes = GenerateStreams (cinfo, &response, path);

    if (responsebytes < 0)
    {
      lprintf (0, "[%s] Error generating stream list", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %d\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? responsebytes : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){MIN(headlen,cinfo->sendbuflen), (response) ? responsebytes : 0},
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
    /* Check for trusted flag, required to access this resource */
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] HTTP STATUS request from un-trusted client",
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

    responsebytes = GenerateStatus (cinfo, &response);

    if (responsebytes <= 0)
    {
      lprintf (0, "[%s] Error generating server status", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %d\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? responsebytes : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){MIN(headlen,cinfo->sendbuflen), (response) ? responsebytes : 0},
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
    /* Check for trusted flag, required to access this resource */
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] HTTP CONNECTIONS request from un-trusted client",
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

    responsebytes = GenerateConnections (cinfo, &response, path);

    if (responsebytes <= 0)
    {
      lprintf (0, "[%s] Error generating server status", cinfo->hostname);

      if (response)
        free (response);
      return -1;
    }

    /* Create header */
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200\r\n"
                        "Content-Length: %d\r\n"
                        "Content-Type: text/plain\r\n"
                        "\r\n",
                        (response) ? responsebytes : 0);

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){MIN(headlen,cinfo->sendbuflen), (response) ? responsebytes : 0},
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
        SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
      }
      else
      {
        lprintf (0, "Error creating response (SeedLink WebSocket request on non-SeedLink port)");
      }

      return -1;
    }

    /* Check subprotocol header for acceptable values, rewrite to echo in response */
    if (*secWebSocketProtocolHeader)
    {
      if (strstr (secWebSocketProtocolHeader, "SeedLink3.1"))
        snprintf (secWebSocketProtocolHeader, sizeof(secWebSocketProtocolHeader),
                  "SeedLink3.1");
      else
        *secWebSocketProtocolHeader = '\0';
    }

    lprintf (1, "[%s] Received WebSocket SeedLink request", cinfo->hostname);

    if (NegotiateWebSocket (cinfo, version, upgradeHeader, connectionHeader,
                            secWebSocketKeyHeader, secWebSocketVersionHeader,
                            secWebSocketProtocolHeader))
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
        SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
      }
      else
      {
        lprintf (0, "Error creating response (DataLink WebSocket request on non-DataLink port)");
      }

      return -1;
    }

    /* Check subprotocol header for acceptable values, rewrite to echo in response */
    if (*secWebSocketProtocolHeader)
    {
      if (strstr (secWebSocketProtocolHeader, "DataLink1.0"))
        snprintf (secWebSocketProtocolHeader, sizeof(secWebSocketProtocolHeader),
                  "DataLink1.0");
      else
        *secWebSocketProtocolHeader = '\0';
    }

    lprintf (1, "[%s] Received WebSocket DataLink request", cinfo->hostname);

    if (NegotiateWebSocket (cinfo, version, upgradeHeader, connectionHeader,
                            secWebSocketKeyHeader, secWebSocketVersionHeader,
                            secWebSocketProtocolHeader))
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
 * Returns length of stream list response in bytes on success and -1 on error.
 ***************************************************************************/
static int
GenerateStreams (ClientInfo *cinfo, char **streamlist, char *path)
{
  Stack *streams;
  StackNode *streamnode;
  RingStream *ringstream;
  int streamcount;
  size_t streamlistsize;
  size_t headlen;
  size_t streaminfolen;
  char streaminfo[200];
  char earliest[50];
  char latest[50];
  char matchstr[50];
  char *cp;
  int matchlen = 0;

  int level = 0;
  char levelstream[100] = {0};
  char prevlevelstream[100] = {0};
  int splitcount;
  char delim = '_';
  char id1[16];
  char id2[16];
  char id3[16];
  char id4[16];
  char id5[16];
  char id6[16];

  if (!cinfo || !streamlist || !path)
    return -1;

  /* If match parameter is supplied, set reader match to limit streams */
  if ((cp = strstr (path, "match=")))
  {
    cp += 6; /* Advance to character after '=' */

    /* Copy parameter value into matchstr, stop at terminator, '&' or max length */
    for (matchlen = 0; *cp != '\0' && *cp != '&' && matchlen < sizeof (matchstr); cp++, matchlen++)
    {
      matchstr[matchlen] = *cp;
    }
    matchstr[matchlen] = '\0';

    if (matchlen > 0 && cinfo->reader)
    {
      if (RingMatch (cinfo->reader, matchstr))
      {
        /* Create and send error response */
        headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                            "HTTP/1.1 400 Invalid match expression\r\n"
                            "Connection: close\r\n"
                            "\r\n"
                            "Invalid match expression: '%s'", matchstr);

        if (headlen > 0)
        {
          SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
        }
        else
        {
          lprintf (0, "Error creating response (invalid match expression)");
        }

        return -1;
      }
    }
  }

  /* If level parameter is supplied, parse and validate */
  if ((cp = strstr (path, "level=")))
  {
    cp += 6; /* Advance to character after '=' */

    level = strtoul (cp, NULL, 10);

    if (level < 1 || level > 6)
    {
      /* Create and send error response */
      headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                          "HTTP/1.1 400 Unsupported value for level: %d\r\n"
                          "Connection: close\r\n"
                          "\r\n"
                          "Unsupported value for level: %d", level, level);

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
      }
      else
      {
        lprintf (0, "Error creating response (unexpected level value: %d)", level);
      }

      return -1;
    }
  }

  /* Collect stream list and send a line for each stream */
  if ((streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)))
  {
    /* Count streams */
    streamcount = 0;
    streamnode = streams->top;
    while (streamnode)
    {
      streamcount++;
      streamnode = streamnode->next;
    }

    /* Allocate stream list buffer with maximum expected:
       for level-specific output, maximum per entry is 60 characters + newline
       otherwise the maximum per entry is 60 + 2x25 (time strings) plus a few spaces and newline */
    streamlistsize = (level) ? 61 : 120;
    streamlistsize *= streamcount;

    if (!(*streamlist = (char *)malloc(streamlistsize)))
    {
      lprintf (0, "[%s] Error for HTTP STREAMS (cannot allocate response buffer of size %zu)",
               cinfo->hostname, streamlistsize);

      StackDestroy (streams, free);

      /* Create and send error response */
      headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                          "HTTP/1.1 500 Internal error, cannot allocate response buffer\r\n"
                          "Connection: close\r\n"
                          "\r\n"
                          "Cannot allocate response buffer of %zu bytes", streamlistsize);

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
      }
      else
      {
        lprintf (0, "Error creating response (cannot allocate stream list buffer)");
      }

      return -1;
    }

    /* Set write pointer to beginning of buffer */
    cp = *streamlist;

    while ((ringstream = (RingStream *)StackPop (streams)))
    {
      /* If a specific level has been specified, split the stream ID into components
         and generate a list of unique entries for the specified level. */
      if (level > 0)
      {
        splitcount = SplitStreamID (ringstream->streamid, delim, 16, id1, id2, id3, id4, id5, id6, NULL);

        if (splitcount <= 0)
        {
          lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, ringstream->streamid);
          return -1;
        }

        if (level >= 6 && splitcount >= 6)
          snprintf (levelstream, sizeof(levelstream),
                    "%s%c%s%c%s%c%s%c%s%c%s\n", id1, delim, id2, delim, id3, delim, id4, delim, id5, delim, id6);
        else if (level >= 5 && splitcount >= 5)
          snprintf (levelstream, sizeof(levelstream),
                    "%s%c%s%c%s%c%s%c%s\n", id1, delim, id2, delim, id3, delim, id4, delim, id5);
        else if (level >= 4 && splitcount >= 4)
          snprintf (levelstream, sizeof(levelstream),
                    "%s%c%s%c%s%c%s\n", id1, delim, id2, delim, id3, delim, id4);
        else if (level >= 3 && splitcount >= 3)
          snprintf (levelstream, sizeof(levelstream),
                    "%s%c%s%c%s\n", id1, delim, id2, delim, id3);
        else if (level >= 2 && splitcount >= 2)
          snprintf (levelstream, sizeof(levelstream),
                    "%s%c%s\n", id1, delim, id2);
        else if (level >= 1 && splitcount >= 1)
          snprintf (levelstream, sizeof(levelstream),
                    "%s\n", id1);

        /* Determine if this level of stream information has been included yet by comparing
           to the previous entry (the Stack is sorted), if not copy to the streaminfo buffer */
        if (strcmp (levelstream, prevlevelstream))
        {
          strncpy (streaminfo, levelstream, sizeof (streaminfo));
          streaminfo[sizeof (streaminfo) - 1] = '\0';

          strcpy (prevlevelstream, levelstream);
        }
        /* Otherwise, skip this entry */
        else
        {
          free (ringstream);
          continue;
        }
      }
      /* Otherwise include the full stream ID and the earliest and latest data times */
      else
      {
        ms_hptime2isotimestr (ringstream->earliestdstime, earliest, 1);
        ms_hptime2isotimestr (ringstream->latestdetime, latest, 1);

        snprintf (streaminfo, sizeof (streaminfo), "%s  %s  %s\n",
                  ringstream->streamid, earliest, latest);
        streaminfo[sizeof (streaminfo) - 1] = '\0';
      }

      /* Add streaminfo entry to buffer */
      streaminfolen = strlen (streaminfo);
      if ((streamlistsize - (cp - *streamlist)) > streaminfolen)
      {
        memcpy (cp, streaminfo, streaminfolen);
        cp += streaminfolen;
      }
      else
      {
        lprintf (0, "[%s] Error for HTTP STREAMS (cannot allocate response buffer of size %zu)",
                 cinfo->hostname, streamlistsize);

        free (ringstream);
        StackDestroy (streams, free);

        /* Create and send error response */
        headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                            "HTTP/1.1 500 Internal error, stream list buffer too small\r\n"
                            "Connection: close\r\n"
                            "\r\n"
                            "Stream list buffer too small: %zu bytes for %d streams",
                            streamlistsize, streamcount);

        if (headlen > 0)
        {
          SendData (cinfo, cinfo->sendbuf, MIN(headlen,cinfo->sendbuflen));
        }
        else
        {
          lprintf (0, "Error creating response (stream list buffer too small)");
        }

        return -1;
      }

      free (ringstream);
    }

    /* Cleanup stream stack */
    StackDestroy (streams, free);
  }

  /* Add a final terminator to stream list buffer */
  *cp = '\0';

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
 * Returns length of status response in bytes on sucess and -1 on error.
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

  if (rv < 0)
    return -1;

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

  return (*status) ? strlen (*status) : 0;
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
 * Returns length of connection list response in bytes on sucess and -1 on error.
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
                    char *secWebSocketKeyHeader,
                    char *secWebSocketVersionHeader,
                    char *secWebSocketProtocolHeader)
{
  char *response = NULL;
  char subprotocolheader[100] = "";
  unsigned char *keybuf = NULL;
  uint8_t digest[20];

  int keybufsize;
  int keylength;

  if (!cinfo || !version)
    return -1;

  /* Check for required headers and expected values:
   *   Protocol version must be HTTP/1.1
   *   Connection: Upgrade
   *   Upgrade: websocket
   *   Sec-WebSocket-Version: 13
   *   Sec-WebSocket-Key: <value>
   *   Sec-WebSocket-Protocol: <value> */

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

  /* Calculate SHA-1 and then Base64 encode the digest */
  sha1digest (digest, NULL, keybuf, keybufsize);
  apr_base64_encode_binary ((char *)keybuf, (const unsigned char *)digest, 20);

  /* Generate subprotocol header if provided */
  if (secWebSocketProtocolHeader && *secWebSocketProtocolHeader)
  {
    snprintf (subprotocolheader, sizeof(subprotocolheader),
              "Sec-WebSocket-Protocol: %s\r\n", secWebSocketProtocolHeader);
  }

  /* Generate response completing the upgrade to WebSocket connection */
  asprintf (&response,
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "%s"
            "Sec-WebSocket-Accept: %s\r\n"
            "\r\n",
            subprotocolheader, keybuf);

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

/* The apr_base64_encode_binary() below was extracted from the Apache
   APR-util release 1.5.4 from file encoding/apr_base64.c.  The
   original code falls under the Apache License 2.0.
   Minor formatting applied. */

static const char basis_64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int
apr_base64_encode_binary (char *encoded, const unsigned char *string, int len)
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

  return (int)(p - encoded);
} /* End of apr_base64_encode_binary() */

/*******************************************************************************
 * sha1digest: https://github.com/CTrabant/teeny-sha1
 *
 * Calculate the SHA-1 value for supplied data buffer and generate a
 * text representation in hexadecimal.
 *
 * Based on https://github.com/jinqiangshou/EncryptionLibrary, credit
 * goes to @jinqiangshou, all new bugs are mine.
 *
 * @input:
 *    data      -- data to be hashed
 *    databytes -- bytes in data buffer to be hashed
 *
 * @output:
 *    digest    -- the result, MUST be at least 20 bytes
 *    hexdigest -- the result in hex, MUST be at least 41 bytes
 *
 * At least one of the output buffers must be supplied.  The other, if not
 * desired, may be set to NULL.
 *
 * @return: 0 on success and non-zero on error.
 ******************************************************************************/
static int
sha1digest (uint8_t *digest, char *hexdigest, const uint8_t *data, size_t databytes)
{
#define SHA1ROTATELEFT(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

  uint32_t W[80];
  uint32_t H[] = {0x67452301,
                  0xEFCDAB89,
                  0x98BADCFE,
                  0x10325476,
                  0xC3D2E1F0};
  uint32_t a;
  uint32_t b;
  uint32_t c;
  uint32_t d;
  uint32_t e;
  uint32_t f = 0;
  uint32_t k = 0;

  uint32_t idx;
  uint32_t lidx;
  uint32_t widx;
  uint32_t didx = 0;

  int32_t wcount;
  uint32_t temp;
  uint64_t databits = ((uint64_t)databytes) * 8;
  uint32_t loopcount = (databytes + 8) / 64 + 1;
  uint32_t tailbytes = 64 * loopcount - databytes;
  uint8_t datatail[128] = {0};

  if (!digest && !hexdigest)
    return -1;

  if (!data)
    return -1;

  /* Pre-processing of data tail (includes padding to fill out 512-bit chunk):
     Add bit '1' to end of message (big-endian)
     Add 64-bit message length in bits at very end (big-endian) */
  datatail[0] = 0x80;
  datatail[tailbytes - 8] = (uint8_t) (databits >> 56 & 0xFF);
  datatail[tailbytes - 7] = (uint8_t) (databits >> 48 & 0xFF);
  datatail[tailbytes - 6] = (uint8_t) (databits >> 40 & 0xFF);
  datatail[tailbytes - 5] = (uint8_t) (databits >> 32 & 0xFF);
  datatail[tailbytes - 4] = (uint8_t) (databits >> 24 & 0xFF);
  datatail[tailbytes - 3] = (uint8_t) (databits >> 16 & 0xFF);
  datatail[tailbytes - 2] = (uint8_t) (databits >> 8 & 0xFF);
  datatail[tailbytes - 1] = (uint8_t) (databits >> 0 & 0xFF);

  /* Process each 512-bit chunk */
  for (lidx = 0; lidx < loopcount; lidx++)
  {
    /* Compute all elements in W */
    memset (W, 0, 80 * sizeof (uint32_t));

    /* Break 512-bit chunk into sixteen 32-bit, big endian words */
    for (widx = 0; widx <= 15; widx++)
    {
      wcount = 24;

      /* Copy byte-per byte from specified buffer */
      while (didx < databytes && wcount >= 0)
      {
        W[widx] += (((uint32_t)data[didx]) << wcount);
        didx++;
        wcount -= 8;
      }
      /* Fill out W with padding as needed */
      while (wcount >= 0)
      {
        W[widx] += (((uint32_t)datatail[didx - databytes]) << wcount);
        didx++;
        wcount -= 8;
      }
    }

    /* Extend the sixteen 32-bit words into eighty 32-bit words, with potential optimization from:
       "Improving the Performance of the Secure Hash Algorithm (SHA-1)" by Max Locktyukhin */
    for (widx = 16; widx <= 31; widx++)
    {
      W[widx] = SHA1ROTATELEFT ((W[widx - 3] ^ W[widx - 8] ^ W[widx - 14] ^ W[widx - 16]), 1);
    }
    for (widx = 32; widx <= 79; widx++)
    {
      W[widx] = SHA1ROTATELEFT ((W[widx - 6] ^ W[widx - 16] ^ W[widx - 28] ^ W[widx - 32]), 2);
    }

    /* Main loop */
    a = H[0];
    b = H[1];
    c = H[2];
    d = H[3];
    e = H[4];

    for (idx = 0; idx <= 79; idx++)
    {
      if (idx <= 19)
      {
        f = (b & c) | ((~b) & d);
        k = 0x5A827999;
      }
      else if (idx >= 20 && idx <= 39)
      {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      }
      else if (idx >= 40 && idx <= 59)
      {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      }
      else if (idx >= 60 && idx <= 79)
      {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      temp = SHA1ROTATELEFT (a, 5) + f + e + k + W[idx];
      e = d;
      d = c;
      c = SHA1ROTATELEFT (b, 30);
      b = a;
      a = temp;
    }

    H[0] += a;
    H[1] += b;
    H[2] += c;
    H[3] += d;
    H[4] += e;
  }

  /* Store binary digest in supplied buffer */
  if (digest)
  {
    for (idx = 0; idx < 5; idx++)
    {
      digest[idx * 4 + 0] = (uint8_t) (H[idx] >> 24);
      digest[idx * 4 + 1] = (uint8_t) (H[idx] >> 16);
      digest[idx * 4 + 2] = (uint8_t) (H[idx] >> 8);
      digest[idx * 4 + 3] = (uint8_t) (H[idx]);
    }
  }

  /* Store hex version of digest in supplied buffer */
  if (hexdigest)
  {
    snprintf (hexdigest, 41, "%08x%08x%08x%08x%08x",
              H[0], H[1], H[2], H[3], H[4]);
  }

  return 0;
} /* End of sha1digest() */

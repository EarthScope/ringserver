/**************************************************************************
 * http.c
 *
 * HTTP handling.
 *
 * This file is part of the ringserver.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (C) 2024:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

/* _GNU_SOURCE needed to get strcasestr() under Linux */
#define _GNU_SOURCE

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#include "clients.h"
#include "dlclient.h"
#include "slclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "mseedscan.h"
#include "ring.h"
#include "ringserver.h"
#include "cJSON.h"
#include "infojson.h"

#define MIN(X, Y) (X < Y) ? X : Y

typedef enum
{
  UNSET,
  RAW,
  TEXT,
  HTML,
  JSON,
  CSS,
  JS,
  XML
} MediaType;

const char *MediaTypes[] = {
    "",
    "application/octet-stream",
    "text/plain",
    "text/html",
    "application/json",
    "text/css",
    "application/javascript",
    "application/xml"};

static int ParseHeader (char *header, char **value);
static int GenerateHeader (ClientInfo *cinfo, int status, MediaType type,
                           uint64_t contentlength, const char *message);
static int GenerateID (ClientInfo *cinfo, const char *path, char **response, MediaType *type);
static int GenerateStreams (ClientInfo *cinfo, const char *path, char **response, MediaType *type);
static int GenerateStatus (ClientInfo *cinfo, const char *path, char **response, MediaType *type);
static int GenerateConnections (ClientInfo *cinfo, const char *path, char **response, MediaType *type);
static int SendFileHTTP (ClientInfo *cinfo, char *path);
static int NegotiateWebSocket (ClientInfo *cinfo, char *version,
                               char *upgradeHeader, char *connectionHeader,
                               char *secWebSocketKeyHeader, char *secWebSocketVersionHeader,
                               char *secWebSocketProtocolHeader);
static int apr_base64_encode_binary (char *encoded, const unsigned char *string, int len);
static int sha1digest (uint8_t *digest, char *hexdigest, const uint8_t *data, size_t databytes);


/***************************************************************************
 * urldecode:
 *
 * Decode percent-encoded portions of URL.  The same buffer can be
 * used for input (src) and output (dst) as the decoded string is
 * always smaller.
 *
 * This function is from: https://stackoverflow.com/a/14530993
 ***************************************************************************/
void
urldecode (char *dst, const char *src)
{
  char a;
  char b;

  while (*src)
  {
    if ((*src == '%') &&
        ((a = src[1]) && (b = src[2])) &&
        (isxdigit (a) && isxdigit (b)))
    {
      if (a >= 'a')
        a -= 'a' - 'A';
      if (a >= 'A')
        a -= ('A' - 10);
      else
        a -= '0';
      if (b >= 'a')
        b -= 'a' - 'A';
      if (b >= 'A')
        b -= ('A' - 10);
      else
        b -= '0';
      *dst++ = 16 * a + b;
      src += 3;
    }
    else if (*src == '+')
    {
      *dst++ = ' ';
      src++;
    }
    else
    {
      *dst++ = *src++;
    }
  }

  *dst++ = '\0';
} /* End of urldecode() */

/***************************************************************************
 * HandleHTTP:
 *
 * Handle HTTP requests using an extremely limited HTTP server
 * implementation.
 *
 * The following end points are handled:
 *   /id[/json]          - return server ID and version
 *   /streams[/json]     - return list of server streams
 *   /streamids          - return list of server stream IDs
 *                           match=<pattern> supported to limit streams
 *   /status[/json]      - return server status, limited via trust-permissions
 *   /connections[/json] - return list of connections, limited via trust-permissions
 *                           match=<pattern> supported to limit connections
 *   /seedlink           - initiate WebSocket connection for SeedLink
 *   /datalink           - initiate WebSocket connection for DataLink
 *
 * Return 1 on success and should disconnect
 * Return 0 on success
 * Return -1 on error which should disconnect.
 ***************************************************************************/
int
HandleHTTP (char *recvbuffer, ClientInfo *cinfo)
{
  char method[10];
  char path[100];
  char version[100];
  int headlen;
  int fields;
  int nread;
  int rv;

  char upgradeHeader[100]              = "";
  char connectionHeader[100]           = "";
  char secWebSocketKeyHeader[100]      = "";
  char secWebSocketVersionHeader[100]  = "";
  char secWebSocketProtocolHeader[100] = "";

  MediaType type = RAW;
  char *response = NULL;
  char *value    = NULL;
  int responsebytes;

  /* Decode percent-encoding in URL */
  urldecode (recvbuffer, recvbuffer);

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

    headlen = GenerateHeader (cinfo, 501, UNSET, 0, method);

    if (headlen > 0)
    {
      SendData (cinfo, cinfo->sendbuf, headlen, 0);
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

  lprintf (2, "[%s] Received HTTP request %.20s", cinfo->hostname, path);

  /* Handle specific end points */
  if (!strncasecmp (path, "/id", 3))
  {
    responsebytes = GenerateID (cinfo, path, &response, &type);

    /* Create header */
    if (responsebytes > 0)
    {
      headlen = GenerateHeader (cinfo, 200, type, (uint64_t)responsebytes, NULL);
    }
    else if (responsebytes == 0)
    {
      headlen = GenerateHeader (cinfo, 404, type, 0, NULL);
    }
    else
    {
      lprintf (0, "Error creating response (ID request)");
      headlen = GenerateHeader (cinfo, 500, type, 0, NULL);
    }

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){(size_t)headlen, (response) ? (size_t)responsebytes : 0},
                       2, 0);
    }
    else
    {
      lprintf (0, "Error creating response header (ID request)");
      rv = -1;
    }

    free (response);

    return (rv) ? -1 : 0;
  } /* Done with /id request */
  else if (!strncasecmp (path, "/stream", 7))
  {
    responsebytes = GenerateStreams (cinfo, path, &response, &type);

    /* Create header */
    if (responsebytes > 0)
    {
      headlen = GenerateHeader (cinfo, 200, type, (uint64_t)responsebytes, NULL);
    }
    else if (responsebytes == 0)
    {
      headlen = GenerateHeader (cinfo, 404, type, 0, NULL);
    }
    else
    {
      lprintf (0, "Error creating response (STREAM[ID]S request)");
      headlen = GenerateHeader (cinfo, 500, type, 0, NULL);
    }

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){(size_t)headlen, (response) ? (size_t)responsebytes : 0},
                       2, 0);
    }
    else
    {
      lprintf (0, "Error creating response header (STREAM[ID]S request)");
      rv = -1;
    }

    free (response);

    return (rv) ? -1 : 0;
  } /* Done with /streams or /streamids request */
  else if (!strncasecmp (path, "/status", 7))
  {
    /* Check for trusted flag, required to access this resource */
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] HTTP STATUS request from un-trusted client",
               cinfo->hostname);

      /* Create header */
      headlen = GenerateHeader (cinfo, 403, UNSET, 0, "Forbidden, no soup for you!");

      rv = SendData (cinfo, cinfo->sendbuf, MIN ((size_t)headlen, cinfo->sendbuflen), 0);

      return (rv) ? -1 : 1;
    }

    responsebytes = GenerateStatus (cinfo, path, &response, &type);

    /* Create header */
    if (responsebytes > 0)
    {
      headlen = GenerateHeader (cinfo, 200, type, (uint64_t)responsebytes, NULL);
    }
    else if (responsebytes == 0)
    {
      headlen = GenerateHeader (cinfo, 404, type, 0, NULL);
    }
    else
    {
      lprintf (0, "Error creating response (STATUS request)");
      headlen = GenerateHeader (cinfo, 500, type, 0, NULL);
    }

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){(size_t)headlen, (response) ? (size_t)responsebytes : 0},
                       2, 0);
    }
    else
    {
      lprintf (0, "Error creating response (STATUS request)");
      rv = -1;
    }

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

      /* Create header */
      headlen = GenerateHeader (cinfo, 403, UNSET, 0, "Forbidden, no soup for you!");

      rv = SendData (cinfo, cinfo->sendbuf, (size_t)headlen, 0);

      return (rv) ? -1 : 1;
    }

    lprintf (1, "[%s] Received HTTP CONNECTIONS request", cinfo->hostname);

    responsebytes = GenerateConnections (cinfo, path, &response, &type);

    /* Create header */
    if (responsebytes > 0)
    {
      headlen = GenerateHeader (cinfo, 200, type, (uint64_t)responsebytes, NULL);
    }
    else if (responsebytes == 0)
    {
      headlen = GenerateHeader (cinfo, 404, type, 0, NULL);
    }
    else
    {
      lprintf (0, "Error creating response (CONNECTIONS request)");
      headlen = GenerateHeader (cinfo, 500, type, 0, NULL);
    }

    if (headlen > 0)
    {
      rv = SendDataMB (cinfo,
                       (void *[]){cinfo->sendbuf, response},
                       (size_t[]){(size_t)headlen, (response) ? (size_t)responsebytes : 0},
                       2, 0);
    }
    else
    {
      lprintf (0, "Error creating response header (CONNECTIONS request)");
      rv = -1;
    }

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
                          "%s"
                          "\r\n"
                          "Cannot request SeedLink WebSocket on non-SeedLink port",
                          (cinfo->httpheaders) ? cinfo->httpheaders : "");

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, MIN ((size_t)headlen, cinfo->sendbuflen), 0);
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
      if (strstr (secWebSocketProtocolHeader, "SeedLink4.0"))
        snprintf (secWebSocketProtocolHeader, sizeof (secWebSocketProtocolHeader),
                  "SeedLink4.0");
      else if (strstr (secWebSocketProtocolHeader, "SeedLink3.1"))
        snprintf (secWebSocketProtocolHeader, sizeof (secWebSocketProtocolHeader),
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
                          "%s"
                          "\r\n"
                          "Cannot request DataLink WebSocket on non-DataLink port",
                          (cinfo->httpheaders) ? cinfo->httpheaders : "");

      if (headlen > 0)
      {
        SendData (cinfo, cinfo->sendbuf, MIN ((size_t)headlen, cinfo->sendbuflen), 0);
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
        snprintf (secWebSocketProtocolHeader, sizeof (secWebSocketProtocolHeader),
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
    if (config.webroot && (rv = SendFileHTTP (cinfo, path)) >= 0)
    {
      lprintf (2, "[%s] Sent %s (%d bytes)", cinfo->hostname, path, rv);
    }
    else
    {
      /* Create header */
      headlen = GenerateHeader (cinfo, 404, HTML, 0, NULL);

      rv = SendData (cinfo, cinfo->sendbuf, (size_t)headlen, 0);

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
 * Return >0 as number of bytes read on success
 * Return  0 when no data is available
 * Return -1 on error or timeout, ClientInfo.socketerr is set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***************************************************************************/
int
RecvWSFrame (ClientInfo *cinfo, uint64_t *length, uint32_t *mask)
{
  unsigned char payload[125];
  uint8_t onetwo[2];
  uint16_t length16;
  uint8_t length7;
  int totalrecv = 0;
  int nrecv;
  int opcode;

  if (!cinfo || !length || !mask)
    return -1;

  /* Recv first two bytes */
  nrecv = RecvData (cinfo, onetwo, 2, 0);

  if (nrecv != 2)
  {
    return nrecv;
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
    nrecv = RecvData (cinfo, &length16, 2, 1);

    if (nrecv != 2)
      return nrecv;

    totalrecv += 2;

    if (!ms_bigendianhost ())
      ms_gswap2 (&length16);

    *length = length16;
  }
  /* If 127, the length is a 64-bit value in the next 8 bytes */
  else if (length7 == 127)
  {
    nrecv = RecvData (cinfo, length, 8, 1);

    if (nrecv != 8)
      return nrecv;

    totalrecv += 8;

    if (!ms_bigendianhost ())
      ms_gswap8 (length);
  }
  else
  {
    *length = length7;
  }

  /* If mask flag, the masking key is the next 4 bytes */
  if (onetwo[1] & 0x80)
  {
    nrecv = RecvData (cinfo, mask, 4, 1);

    if (nrecv != 4)
      return nrecv;

    totalrecv += 4;
  }

  /* Extract opcode, bits 4-7 of the 1st byte */
  opcode = onetwo[0] & 0xf;

  /* Check for ping, consume payload and send pong with same payload */
  if (opcode == 0x9)
  {
    if (*length > 125)
    {
      lprintf (0, "[%s] Error, WebSocket payload length of %" PRIu64 " > 125, which is not allowed for a ping",
               cinfo->hostname, *length);
      return -2;
    }

    if (*length > 0)
    {
      nrecv = RecvData (cinfo, payload, *length, 1);

      if (nrecv != (int)*length)
      {
        lprintf (0, "[%s] Error receiving payload for WebSocket ping, nrecv: %d, expected length: %" PRIu64 "\n",
                 cinfo->hostname, nrecv, *length);
        return -2;
      }

      totalrecv += nrecv;
    }

    /* Send pong with same payload data */
    onetwo[0] = 0x8a; /* Change opcode to pong */
    SendData (cinfo, onetwo, 2, 1);
    SendData (cinfo, payload, *length, 1);

    return 0;
  }

  /* Check for pong, consume payload and ignore */
  if (opcode == 0xa)
  {
    if (*length > 125)
    {
      lprintf (0, "[%s] Error, WebSocket payload length of %" PRIu64 " > 125, which is not allowed for a pong",
               cinfo->hostname, *length);
      return -2;
    }

    if (*length > 0)
    {
      nrecv = RecvData (cinfo, payload, *length, 1);

      if (nrecv != (int)*length)
      {
        lprintf (0, "[%s] Error receiving payload for unexpected WebSocket pong, nrecv: %d, expected length: %" PRIu64 "\n",
                 cinfo->hostname, nrecv, *length);
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
    SendData (cinfo, onetwo, 2, 1);

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
  cp     = *value + length - 1;
  while (*cp == ' ' && cp != *value)
    cp--;

  *(cp + 1) = '\0';

  return 0;
} /* End of ParseHeader() */

/***************************************************************************
 * GenerateHeader:
 *
 * Generate HTTP header for status, type, length with optional message
 * and write to the ClientInfo send buffer.
 *
 * The caller must free the header buffer allocated by this routine.
 *
 * Return >0 size of response on success
 * Return -1 on error which should disconnect
 ***************************************************************************/
static int
GenerateHeader (ClientInfo *cinfo, int status, MediaType type,
                uint64_t contentlength, const char *message)
{
  int headlen;

  if (status == 200)
  {
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Length: %" PRIu64 "\r\n"
                        "Content-Type: %s\r\n"
                        "%s"
                        "\r\n",
                        contentlength,
                        MediaTypes[type],
                        (cinfo->httpheaders) ? cinfo->httpheaders : "");
  }
  else if (status == 403)
  {
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 403 %s!\r\n"
                        "Connection: close\r\n"
                        "%s"
                        "\r\n",
                        (message) ? message : "Forbidden",
                        (cinfo->httpheaders) ? cinfo->httpheaders : "");
  }
  else if (status == 404)
  {
    const char *body = "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">"
                       "<html><head><title>404 Not Found</title></head>"
                       "<body><h1>Not Found</h1></body></html>";

    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 404 Not Found\r\n"
                        "Content-Length: %zu\r\n"
                        "Content-Type: %s\r\n"
                        "Connection: close\r\n"
                        "%s"
                        "\r\n"
                        "%s",
                        strlen (body),
                        MediaTypes[HTML],
                        (cinfo->httpheaders) ? cinfo->httpheaders : "",
                        body);
  }
  else if (status == 501)
  {
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 501 Method %s Not Implemented\r\n"
                        "Content-Length: 0\r\n"
                        "Connection: close\r\n"
                        "%s"
                        "\r\n",
                        message,
                        (cinfo->httpheaders) ? cinfo->httpheaders : "");
  }
  else
  {
    headlen = snprintf (cinfo->sendbuf, cinfo->sendbuflen,
                        "HTTP/1.1 %d %s\r\n"
                        "Content-Length: %" PRIu64 "\r\n"
                        "Content-Type: %s\r\n"
                        "%s"
                        "\r\n",
                        status, message,
                        contentlength,
                        MediaTypes[type],
                        (cinfo->httpheaders) ? cinfo->httpheaders : "");
  }

  return (headlen > cinfo->sendbuflen) ? cinfo->sendbuflen : headlen;
}

/***************************************************************************
 * GenerateID:
 *
 * Generate response for ID request.
 *
 * The caller must free the response buffer allocated by this routine.
 *
 * Return >0 size of response on success
 * Return  0 for unrecognized path
 * Return -1 on error
 ***************************************************************************/
static int
GenerateID (ClientInfo *cinfo, const char *path, char **response, MediaType *type)
{
  int responsebytes = 0;

  char *json_string;
  cJSON *json;

  if (!cinfo || !path || !response || !type)
    return -1;

  json_string = info_json (cinfo, PACKAGE "/" VERSION, INFO_ID, NULL);

  if (!json_string)
    return -1;

  if (!strcasecmp (path, "/id/json"))
  {
    *response     = json_string;
    responsebytes = (*response) ? strlen (*response) : 0;
    *type         = JSON;
  }
  else if (!strcasecmp (path, "/id"))
  {
    if ((json = cJSON_Parse (json_string)) == NULL)
    {
      free (json_string);
      return -1;
    }
    free (json_string);

    responsebytes = asprintf (response,
                              "%s\n"
                              "Organization: %s\n"
                              "Server start: %s",
                              cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")),
                              cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")),
                              cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")));

    cJSON_Delete (json);

    *type = TEXT;
  }
  else
  {
    free (json_string);
    return 0;
  }

  return (responsebytes > 0) ? responsebytes : -1;
} /* End of GenerateID() */

/***************************************************************************
 * GenerateStreams:
 *
 * Generate stream list and place into buffer, which will be allocated
 * to the length needed and should be free'd by the caller.
 *
 * If 'timeextents' is true the earliest and latest times will be
 * included in the output.  For now this only works when level > 0.
 *
 * Check for 'match' parameter in 'path' and use value as a regular
 * expression to match against stream identifiers.
 *
 * Return >0 size of response on success
 * Return  0 for unrecognized path
 * Return -1 on error
 ***************************************************************************/
static int
GenerateStreams (ClientInfo *cinfo, const char *path, char **response, MediaType *type)
{
  size_t streamcount;
  size_t streamlistsize;
  char mypath[64]   = {0};
  char matchstr[64] = {0};
  int matchlen      = 0;

  char *cp;
  char *writeptr    = NULL;
  int written       = 0;
  int responsebytes = 0;
  int just_ids      = 0;

  char *json_string;
  cJSON *json;
  cJSON *stream_array;
  cJSON *stream_iter = NULL;

  if (!cinfo || !path || !response || !type)
    return -1;

  strncpy (mypath, path, sizeof (mypath) - 1);

  /* If match parameter is supplied, extract value */
  if ((cp = strstr (mypath, "match=")))
  {
    cp += 6; /* Advance to character after '=' */

    /* Copy parameter value into matchstr, stop at terminator, '&' or max length */
    for (matchlen = 0; *cp != '\0' && *cp != '&' && matchlen < sizeof (matchstr); cp++, matchlen++)
    {
      matchstr[matchlen] = *cp;
    }
    matchstr[matchlen] = '\0';
  }

  if ((cp = strchr (mypath, '?')))
    *cp = '\0';

  json_string = info_json (cinfo, PACKAGE "/" VERSION, INFO_STREAMS, (matchlen > 0) ? matchstr : NULL);

  if (!json_string)
    return -1;

  if (!strcasecmp (mypath, "/streams/json"))
  {
    *response     = json_string;
    responsebytes = (*response) ? strlen (*response) : 0;
    *type         = JSON;
  }
  else if (!strcasecmp (mypath, "/streams") ||
           !strcasecmp (mypath, "/streamids"))
  {
    just_ids = (!strcasecmp (mypath, "/streamids")) ? 1 : 0;

    if ((json = cJSON_Parse (json_string)) == NULL)
    {
      free (json_string);
      return -1;
    }
    free (json_string);

    if ((stream_array = cJSON_GetObjectItem (json, "stream")) != NULL)
    {
      streamcount = cJSON_GetArraySize (stream_array);

      /* Allocate stream list buffer with maximum expected:
       * for level-specific output, maximum per entry is 60 characters + newline
       * otherwise the maximum per entry is 60 + 2x32 (time strings) plus a few spaces and newline */
      streamlistsize = (just_ids) ? 64 : 124;
      streamlistsize *= streamcount;

      if (!(*response = (char *)malloc (streamlistsize)))
      {
        lprintf (0, "[%s] Error for HTTP STREAM[ID]S (cannot allocate response buffer of size %zu)",
                 cinfo->hostname, streamlistsize);
        cJSON_Delete (json);
        return -1;
      }

      writeptr = *response;

      responsebytes = 0;

      cJSON_ArrayForEach (stream_iter, stream_array)
      {
        if (just_ids)
        {
          written = snprintf (writeptr, streamlistsize - responsebytes, "%s\n",
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "id")));
        }
        else
        {
          written = snprintf (writeptr, streamlistsize - responsebytes, "%s %s %s\n",
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "id")),
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "start_time")),
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "end_time")));
        }

        if ((responsebytes + written) >= streamlistsize)
        {
          lprintf (0, "[%s] Error for HTTP STREAM[ID]S (response buffer overflow)",
                   cinfo->hostname);
          cJSON_Delete (json);
          return -1;
        }

        writeptr += written;
        responsebytes += written;
      }

      /* Add a final terminator to stream list buffer */
      *writeptr = '\0';

      cJSON_Delete (json);
      *type = TEXT;
    }
  }
  else
  {
    free (json_string);
    return 0;
  }

  return responsebytes;
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
GenerateStatus (ClientInfo *cinfo, const char *path, char **response, MediaType *type)
{
  size_t responsesize;
  char *writeptr    = NULL;
  int written       = 0;
  int responsebytes = 0;

  char *json_string;
  cJSON *json;
  cJSON *server;
  cJSON *thread_array;
  cJSON *thread_iter = NULL;

  if (!cinfo || !path || !response || !type)
    return -1;

  json_string = info_json (cinfo, PACKAGE "/" VERSION, INFO_ID | INFO_STATUS, NULL);

  if (!json_string)
    return -1;

  if (!strcasecmp (path, "/status/json"))
  {
    *response     = json_string;
    responsebytes = (*response) ? strlen (*response) : 0;
    *type         = JSON;
  }
  else if (!strcasecmp (path, "/status"))
  {
    if ((json = cJSON_Parse (json_string)) == NULL)
    {
      free (json_string);
      return -1;
    }
    free (json_string);

    if ((server = cJSON_GetObjectItem (json, "server")) != NULL)
    {
      responsesize = 2048;

      if (!(*response = (char *)malloc (responsesize)))
      {
        lprintf (0, "[%s] Error for HTTP CONNECTIONS (cannot allocate response buffer of size %zu)",
                 cinfo->hostname, responsesize);
        cJSON_Delete (json);
        return -1;
      }

      writeptr = *response;

      responsebytes = 0;

      written = snprintf (writeptr, responsesize - responsebytes,
                          "%s\n"
                          "Organization: %s\n"
                          "Server start time: %s\n"
                          "Ring version: %d\n"
                          "Ring size: %d\n"
                          "Packet size: %d\n"
                          "Max packets: %d\n"
                          "Memory mapped ring: %s\n"
                          "Volatile ring: %s\n"
                          "Total connections: %d\n"
                          "Total streams: %d\n"
                          "TX packet rate: %.1f\n"
                          "TX byte rate: %.1f\n"
                          "RX packet rate: %.1f\n"
                          "RX byte rate: %.1f\n"
                          "Earliest packet: %s\n"
                          "  Create: %s  Data start: %s  Data end: %s\n"
                          "Latest packet: %s\n"
                          "  Create: %s  Data start: %s  Data end: %s\n",
                          cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")),
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "ring_version")) + 0.5),
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "ring_size")) + 0.5),
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "packet_size")) + 0.5),
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "maximum_packets")) + 0.5),
                          (cJSON_IsTrue (cJSON_GetObjectItem (server, "memory_mapped"))) ? "TRUE" : "FALSE",
                          (cJSON_IsTrue (cJSON_GetObjectItem (server, "volatile_ring"))) ? "TRUE" : "FALSE",
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "connection_count")) + 0.5),
                          (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (server, "stream_count")) + 0.5),
                          cJSON_GetNumberValue (cJSON_GetObjectItem (server, "transmit_packet_rate")),
                          cJSON_GetNumberValue (cJSON_GetObjectItem (server, "transmit_byte_rate")),
                          cJSON_GetNumberValue (cJSON_GetObjectItem (server, "receive_packet_rate")),
                          cJSON_GetNumberValue (cJSON_GetObjectItem (server, "receive_byte_rate")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "earliest_packet_id")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "earliest_packet_time")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "earliest_data_start")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "earliest_data_end")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "latest_packet_id")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "latest_packet_time")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "latest_data_start")),
                          cJSON_GetStringValue (cJSON_GetObjectItem (server, "latest_data_end")));

      writeptr += written;
      responsebytes += written;

      if ((thread_array = cJSON_GetObjectItem (server, "thread")) != NULL)
      {
        written = snprintf (writeptr, responsesize - responsebytes,
                            "\nServer threads:\n");
        writeptr += written;
        responsebytes += written;

        cJSON_ArrayForEach (thread_iter, thread_array)
        {
          char *thread_type = cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "type"));

          if (!strcasecmp (thread_type, "Listener"))
          {
            written = snprintf (writeptr, responsesize - responsebytes,
                                "  Thread type: %s\n"
                                "    Protocol: %s\n"
                                "    Port: %s\n",
                                thread_type,
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "protocol")),
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "port")));
          }
          else if (!strcasecmp (thread_type, "miniSEED scanner"))
          {
            written = snprintf (writeptr, responsesize - responsebytes,
                                "  Thread type: %s\n"
                                "    Directory: %s\n"
                                "    Max recursion: %d\n"
                                "    State file: %s\n"
                                "    Match: %s\n"
                                "    Reject: %s\n"
                                "    Scan time: %g\n"
                                "    Packet rate: %g\n"
                                "    Byte rate: %g\n",
                                thread_type,
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "directory")),
                                (int)(cJSON_GetNumberValue (cJSON_GetObjectItem (thread_iter, "max_recursion")) + 0.5),
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "state_file")),
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "match")),
                                cJSON_GetStringValue (cJSON_GetObjectItem (thread_iter, "reject")),
                                cJSON_GetNumberValue (cJSON_GetObjectItem (thread_iter, "scan_time")),
                                cJSON_GetNumberValue (cJSON_GetObjectItem (thread_iter, "packet_rate")),
                                cJSON_GetNumberValue (cJSON_GetObjectItem (thread_iter, "byte_rate")));
          }
          else
          {
            written = snprintf (writeptr, responsesize - responsebytes,
                                "  Thread type: %s\n",
                                thread_type);
          }

          if ((responsebytes + written) >= responsesize)
          {
            lprintf (0, "[%s] Error for HTTP STATUS (response buffer overflow)",
                     cinfo->hostname);
            cJSON_Delete (json);
            return -1;
          }

          writeptr += written;
          responsebytes += written;
        }
      }

      cJSON_Delete (json);
      *type = TEXT;
    }
  }
  else
  {
    free (json_string);
    return 0;
  }

  return responsebytes;
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
 * Return >0 size of response on success
 * Return  0 for unrecognized path
 * Return -1 on error
 ***************************************************************************/
static int
GenerateConnections (ClientInfo *cinfo, const char *path, char **response, MediaType *type)
{
  char mypath[64] = {0};
  size_t clientcount = 0;
  size_t responsesize;
  char matchstr[50];
  int matchlen = 0;

  char *cp;
  char *writeptr    = NULL;
  int written       = 0;
  int responsebytes = 0;

  char *json_string;
  cJSON *json;
  cJSON *connections;
  cJSON *client_array;
  cJSON *client_iter = NULL;

  if (!cinfo || !path || !response || !type)
    return -1;

  strncpy (mypath, path, sizeof (mypath) - 1);

  /* If match parameter is supplied, set reader match to limit streams */
  if ((cp = strstr (mypath, "match=")))
  {
    cp += 6; /* Advance to character after '=' */

    /* Copy parameter value into matchstr, stop at terminator, '&' or max length */
    for (matchlen = 0; *cp && *cp != '&' && matchlen < sizeof (matchstr); cp++, matchlen++)
    {
      matchstr[matchlen] = *cp;
    }
    matchstr[matchlen] = '\0';
  }

  if ((cp = strchr (mypath, '?')))
    *cp = '\0';

  json_string = info_json (cinfo, PACKAGE "/" VERSION, INFO_CONNECTIONS, (matchlen > 0) ? matchstr : NULL);

  if (!json_string)
    return -1;

  if (!strcasecmp (mypath, "/connections/json"))
  {
    *response     = json_string;
    responsebytes = (*response) ? strlen (*response) : 0;
    *type         = JSON;
  }
  else if (!strcasecmp (mypath, "/connections"))
  {
    if ((json = cJSON_Parse (json_string)) == NULL)
    {
      free (json_string);
      return -1;
    }
    free (json_string);

    connections = cJSON_GetObjectItem (json, "connections");

    if ((client_array = cJSON_GetObjectItem (connections, "client")) != NULL)
    {
      clientcount = cJSON_GetArraySize (client_array);

      /* Allocate stream list buffer with maximum expected: 1024 bytes per client */
      responsesize = clientcount * 1024;

      if (!(*response = (char *)malloc (responsesize)))
      {
        lprintf (0, "[%s] Error for HTTP CONNECTIONS (cannot allocate response buffer of size %zu)",
                 cinfo->hostname, responsesize);
        cJSON_Delete (json);
        return -1;
      }

      writeptr = *response;

      responsebytes = 0;

      cJSON_ArrayForEach (client_iter, client_array)
      {
        char *host             = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "host"));
        char *ipaddress        = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "ip_address"));
        char *port             = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "port"));
        char *client_type      = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "type"));
        char *client_id        = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "client_id"));
        char *connect_time     = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "connect_time"));
        char *current_packet   = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "current_packet"));
        char *packet_time      = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "packet_time"));
        int lag_percent        = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "lag_percent"));
        double lag_seconds     = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "lag_seconds"));
        char *transmit_packets = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "transmit_packets"));
        double tx_packet_rate  = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "transmit_packet_rate"));
        char *transmit_bytes   = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "transmit_bytes"));
        double tx_byte_rate    = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "transmit_byte_rate"));
        char *receive_packets  = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "receive_packets"));
        double rx_packet_rate  = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "receive_packet_rate"));
        char *receive_bytes    = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "receive_bytes"));
        double rx_byte_rate    = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "receive_byte_rate"));
        int stream_count       = cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "stream_count"));

        written = snprintf (writeptr, responsesize - responsebytes,
                            "%s [%s:%s]\n"
                            "  [%s] %s  %s\n"
                            "  Packet %s (%s)  Lag %d, %.1f\n"
                            "  TX %s packets, %.1f packets/sec  %s bytes, %.1f bytes/sec\n"
                            "  RX %s packets, %.1f packets/sec  %s bytes, %.1f bytes/sec\n"
                            "  Stream count: %d\n\n",
                            (host) ? host : "-",
                            (ipaddress) ? ipaddress : "-",
                            (port) ? port : "-",
                            (client_type) ? client_type : "-",
                            (client_id) ? client_id : "-",
                            (connect_time) ? connect_time : "-",
                            (current_packet) ? current_packet : "-",
                            (packet_time) ? packet_time : "-",
                            lag_percent, lag_seconds,
                            (transmit_packets) ? transmit_packets : "-",
                            tx_packet_rate,
                            (transmit_bytes) ? transmit_bytes : "-",
                            tx_byte_rate,
                            (receive_packets) ? receive_packets : "-",
                            rx_packet_rate,
                            (receive_bytes) ? receive_bytes : "-",
                            rx_byte_rate,
                            stream_count);

        if ((responsebytes + written) >= responsesize)
        {
          lprintf (0, "[%s] Error for HTTP STREAM[ID]S (response buffer overflow)",
                   cinfo->hostname);
          cJSON_Delete (json);
          return -1;
        }

        writeptr += written;
        responsebytes += written;
      }

      /* Add a final terminator to stream list buffer */
      *writeptr = '\0';

      cJSON_Delete (json);
      *type = TEXT;
    }
  }
  else
  {
    free (json_string);
    return 0;
  }

  return responsebytes;
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
  MediaType type    = RAW;
  char *response    = NULL;
  char *webpath     = NULL;
  char *filename    = NULL;
  char *indexfile   = NULL;
  char *cp          = NULL;
  char filebuffer[65535];
  size_t length;

  if (!path || !cinfo)
    return -1;

  /* Build path using web root and resolve absolute */
  if (asprintf (&webpath, "%s/%s", config.webroot, path) < 0)
    return -1;

  filename = realpath (webpath, NULL);
  if (filename == NULL)
  {
    lprintf (0, "Error resolving path to requested file: %s", webpath);
    return -1;
  }

  free (webpath);

  /* Sanity check that file is within web root */
  if (strncmp (config.webroot, filename, strlen (config.webroot)))
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
      type = HTML;
    else if (length == 4 && !strcmp (cp, ".htm"))
      type = HTML;
    else if (length == 4 && !strcmp (cp, ".css"))
      type = CSS;
    else if (length == 3 && !strcmp (cp, ".js"))
      type = JS;
    else if (length == 5 && !strcmp (cp, ".json"))
      type = JSON;
    else if (length == 5 && !strcmp (cp, ".text"))
      type = TEXT;
    else if (length == 4 && !strcmp (cp, ".txt"))
      type = TEXT;
    else if (length == 4 && !strcmp (cp, ".xml"))
      type = XML;
  }

  if ((fp = fopen (filename, "r")) == NULL)
  {
    lprintf (0, "Error opening file %s:  %s",
             filename, strerror (errno));
    return -1;
  }

  /* Create header */
  length = GenerateHeader (cinfo, 200, type, (uint64_t)filestat.st_size, NULL);

  if (length <= 0)
  {
    fclose (fp);
    return -1;
  }

  /* Send header and file */
  SendData (cinfo, cinfo->sendbuf, length, 0);

  while ((length = fread (filebuffer, 1, sizeof (filebuffer), fp)) > 0)
  {
    if (SendData (cinfo, filebuffer, length, 0))
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
  char *response              = NULL;
  char subprotocolheader[100] = "";
  unsigned char *keybuf       = NULL;
  uint8_t digest[20];

  size_t keybufsize;
  size_t keylength;

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
    if (asprintf (&response,
                  "HTTP/1.1 400 Protocol Version Must Be 1.1 For WebSocket\r\n"
                  "%s"
                  "\r\n"
                  "HTTP Version Must Be 1.1 For WebSocket\n"
                  "Received: '%s'\n",
                  (cinfo->httpheaders) ? cinfo->httpheaders : "",
                  (version) ? version : "") < 0)
    {
      lprintf (0, "Cannot allocate memory for response (wrong protocol)");
    }

    if (response)
    {
      SendData (cinfo, response, strlen (response), 0);
      free (response);
    }

    return -1;
  }

  if (!upgradeHeader || strcasecmp (upgradeHeader, "websocket"))
  {
    if (asprintf (&response,
                  "HTTP/1.1 400 Upgrade Header Not Recognized\r\n"
                  "%s"
                  "\r\n"
                  "The Upgrade header value must be 'websocket'\n"
                  "Received: '%s'\n",
                  (cinfo->httpheaders) ? cinfo->httpheaders : "",
                  (upgradeHeader) ? upgradeHeader : "") < 0)
    {
      lprintf (0, "Cannot allocate memory for response (wrong Upgrade)");
    }

    if (response)
    {
      SendData (cinfo, response, strlen (response), 0);
      free (response);
    }

    return -1;
  }

  if (!connectionHeader || !strcasestr (connectionHeader, "Upgrade"))
  {
    if (asprintf (&response,
                  "HTTP/1.1 400 Connection Header Not Recognized\r\n"
                  "%s"
                  "\r\n"
                  "The Connection header value must be 'Upgrade'\n"
                  "Received: '%s'\n",
                  (cinfo->httpheaders) ? cinfo->httpheaders : "",
                  (connectionHeader) ? connectionHeader : "") < 0)
    {
      lprintf (0, "Cannot allocate memory for response (wrong Connection)");
    }

    if (response)
    {
      SendData (cinfo, response, strlen (response), 0);
      free (response);
    }

    return -1;
  }

  if (!secWebSocketVersionHeader || strcasecmp (secWebSocketVersionHeader, "13"))
  {
    if (asprintf (&response,
                  "HTTP/1.1 400 Sec-WebSocket-Version Must Be 13\r\n"
                  "%s"
                  "\r\n"
                  "The Sec-WebSocket-Key header is required\n"
                  "Received: '%s'\n",
                  (cinfo->httpheaders) ? cinfo->httpheaders : "",
                  (secWebSocketVersionHeader) ? secWebSocketVersionHeader : "") < 0)
    {
      lprintf (0, "Cannot allocate memory for response (wrong Sec-WebSocket-Version)");
    }

    if (response)
    {
      SendData (cinfo, response, strlen (response), 0);
      free (response);
    }

    return -1;
  }

  if (!secWebSocketKeyHeader)
  {
    if (asprintf (&response,
                  "HTTP/1.1 400 Sec-WebSocket-Key Header Must Be Present\r\n"
                  "%s"
                  "\r\n"
                  "The Sec-WebSocket-Key header is required\n",
                  (cinfo->httpheaders) ? cinfo->httpheaders : "") < 0)
    {
      lprintf (0, "Cannot allocate memory for response (wrong Sec-WebSocket-Key) ");
    }

    if (response)
    {
      SendData (cinfo, response, strlen (response), 0);
      free (response);
    }

    return -1;
  }

  /* Allocate space for key + 36 bytes for magic string */
  keylength  = strlen (secWebSocketKeyHeader);
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
    snprintf (subprotocolheader, sizeof (subprotocolheader),
              "Sec-WebSocket-Protocol: %s\r\n", secWebSocketProtocolHeader);
  }

  /* Generate response completing the upgrade to WebSocket connection */
  if (asprintf (&response,
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "%s"
                "%s"
                "Sec-WebSocket-Accept: %s\r\n"
                "\r\n",
                (cinfo->httpheaders) ? cinfo->httpheaders : "",
                subprotocolheader,
                keybuf) < 0)
  {
    lprintf (0, "Cannot allocate memory for response");
    return -1;
  }

  if (response)
  {
    SendData (cinfo, response, strlen (response), 0);
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
  uint64_t databits     = ((uint64_t)databytes) * 8;
  uint32_t loopcount    = (databytes + 8) / 64 + 1;
  uint32_t tailbytes    = 64 * loopcount - databytes;
  uint8_t datatail[128] = {0};

  if (!digest && !hexdigest)
    return -1;

  if (!data)
    return -1;

  /* Pre-processing of data tail (includes padding to fill out 512-bit chunk):
     Add bit '1' to end of message (big-endian)
     Add 64-bit message length in bits at very end (big-endian) */
  datatail[0]             = 0x80;
  datatail[tailbytes - 8] = (uint8_t)(databits >> 56 & 0xFF);
  datatail[tailbytes - 7] = (uint8_t)(databits >> 48 & 0xFF);
  datatail[tailbytes - 6] = (uint8_t)(databits >> 40 & 0xFF);
  datatail[tailbytes - 5] = (uint8_t)(databits >> 32 & 0xFF);
  datatail[tailbytes - 4] = (uint8_t)(databits >> 24 & 0xFF);
  datatail[tailbytes - 3] = (uint8_t)(databits >> 16 & 0xFF);
  datatail[tailbytes - 2] = (uint8_t)(databits >> 8 & 0xFF);
  datatail[tailbytes - 1] = (uint8_t)(databits >> 0 & 0xFF);

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
      e    = d;
      d    = c;
      c    = SHA1ROTATELEFT (b, 30);
      b    = a;
      a    = temp;
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
      digest[idx * 4 + 0] = (uint8_t)(H[idx] >> 24);
      digest[idx * 4 + 1] = (uint8_t)(H[idx] >> 16);
      digest[idx * 4 + 2] = (uint8_t)(H[idx] >> 8);
      digest[idx * 4 + 3] = (uint8_t)(H[idx]);
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

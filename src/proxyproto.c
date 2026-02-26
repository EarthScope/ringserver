/**************************************************************************
 * proxyproto.c
 *
 * HAProxy PROXY protocol version 2 parser.
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
 * Copyright (C) 2025:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "logging.h"
#include "proxyproto.h"

/* PROXY protocol v2 fixed header length */
#define PP2_HEADER_LEN 16

/* PROXY protocol v2 signature: "\r\n\r\n\0\r\nQUIT\n" */
static const uint8_t PP2_SIGNATURE[12] = {
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

/* PROXY protocol v2 commands */
#define PP2_CMD_LOCAL 0x0
#define PP2_CMD_PROXY 0x1

/* PROXY protocol v2 address families */
#define PP2_FAM_UNSPEC 0x0
#define PP2_FAM_INET 0x1
#define PP2_FAM_INET6 0x2

/* Address data lengths per family (source + destination addr + ports) */
#define PP2_ADDR_LEN_INET 12  /* 4+4+2+2 */
#define PP2_ADDR_LEN_INET6 36 /* 16+16+2+2 */

/* Maximum additional data length we will drain (TLVs, etc.) */
#define PP2_MAX_ADD_LEN 512

/* recv_all: receive exactly 'len' bytes from 'sock' within 'timeout_ms'.
 * Returns 0 on success, -1 on timeout or error. */
static int
recv_all (int sock, void *buf, size_t len, int timeout_ms)
{
  uint8_t *ptr     = (uint8_t *)buf;
  size_t remaining = len;

  while (remaining > 0)
  {
    struct pollfd pfd;
    pfd.fd      = sock;
    pfd.events  = POLLIN;
    pfd.revents = 0;

    int ready = poll (&pfd, 1, timeout_ms);

    if (ready < 0)
    {
      if (errno == EINTR)
        continue;
      return -1;
    }

    if (ready == 0)
      return -1; /* timeout */

    ssize_t n = recv (sock, ptr, remaining, 0);

    if (n <= 0)
      return -1; /* error or EOF */

    ptr += n;
    remaining -= (size_t)n;
  }

  return 0;
}

/***********************************************************************
 * proxy_protocol_v2_read:
 *
 * Read and parse a HAProxy PROXY protocol v2 header from 'socket'.
 *
 * On success with a PROXY command the sockaddr_storage pointed to by
 * 'addr' and the value at 'addrlen' are updated with the original
 * client address conveyed by the header.
 *
 * Returns:
 *   0  - PROXY command; addr/addrlen updated with client address
 *   1  - LOCAL command; addr/addrlen unchanged
 *  -1  - error
 ***********************************************************************/
int
proxy_protocol_v2_read (int socket, struct sockaddr_storage *addr,
                        socklen_t *addrlen, int timeout_ms)
{
  uint8_t hdr[PP2_HEADER_LEN];
  uint8_t addrbuf[PP2_ADDR_LEN_INET6];
  uint8_t drain[PP2_MAX_ADD_LEN];

  if (!addr || !addrlen)
    return -1;

  /* Read the fixed 16-byte header */
  if (recv_all (socket, hdr, PP2_HEADER_LEN, timeout_ms) < 0)
  {
    lprintf (1, "PROXY protocol v2: timeout or error reading header");
    return -1;
  }

  /* Validate signature */
  if (memcmp (hdr, PP2_SIGNATURE, sizeof (PP2_SIGNATURE)) != 0)
  {
    lprintf (1, "PROXY protocol v2: invalid signature");
    return -1;
  }

  /* Validate version: upper nibble of byte 12 must be 0x2 */
  if ((hdr[12] & 0xF0) != 0x20)
  {
    lprintf (1, "PROXY protocol v2: unsupported version 0x%02x", hdr[12] >> 4);
    return -1;
  }

  uint8_t cmd    = hdr[12] & 0x0F;
  uint8_t family = (hdr[13] >> 4) & 0x0F;
  uint16_t extra = ntohs (*(uint16_t *)(hdr + 14));

  /* LOCAL command: health-check from proxy; ignore the address payload */
  if (cmd == PP2_CMD_LOCAL)
  {
    /* Drain any remaining bytes declared in the header */
    size_t to_drain = extra;
    while (to_drain > 0)
    {
      size_t chunk = (to_drain > sizeof (drain)) ? sizeof (drain) : to_drain;
      if (recv_all (socket, drain, chunk, timeout_ms) < 0)
      {
        lprintf (1, "PROXY protocol v2: error draining LOCAL payload");
        return -1;
      }
      to_drain -= chunk;
    }
    return 1;
  }

  if (cmd != PP2_CMD_PROXY)
  {
    lprintf (1, "PROXY protocol v2: unknown command 0x%x", cmd);
    return -1;
  }

  /* PROXY command: read address data according to address family */

  if (family == PP2_FAM_INET)
  {
    if (extra < PP2_ADDR_LEN_INET)
    {
      lprintf (1, "PROXY protocol v2: IPv4 address data too short (%u)", extra);
      return -1;
    }

    if (recv_all (socket, addrbuf, PP2_ADDR_LEN_INET, timeout_ms) < 0)
    {
      lprintf (1, "PROXY protocol v2: error reading IPv4 address data");
      return -1;
    }

    struct sockaddr_in *sin = (struct sockaddr_in *)addr;
    memset (sin, 0, sizeof (struct sockaddr_in));
    sin->sin_family = AF_INET;
    memcpy (&sin->sin_addr, addrbuf, 4);     /* source address */
    memcpy (&sin->sin_port, addrbuf + 8, 2); /* source port */
    *addrlen = sizeof (struct sockaddr_in);

    /* Drain any trailing TLV bytes */
    size_t to_drain = extra - PP2_ADDR_LEN_INET;
    while (to_drain > 0)
    {
      size_t chunk = (to_drain > sizeof (drain)) ? sizeof (drain) : to_drain;
      if (recv_all (socket, drain, chunk, timeout_ms) < 0)
      {
        lprintf (1, "PROXY protocol v2: error draining IPv4 TLV data");
        return -1;
      }
      to_drain -= chunk;
    }

    return 0;
  }
  else if (family == PP2_FAM_INET6)
  {
    if (extra < PP2_ADDR_LEN_INET6)
    {
      lprintf (1, "PROXY protocol v2: IPv6 address data too short (%u)", extra);
      return -1;
    }

    if (recv_all (socket, addrbuf, PP2_ADDR_LEN_INET6, timeout_ms) < 0)
    {
      lprintf (1, "PROXY protocol v2: error reading IPv6 address data");
      return -1;
    }

    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)addr;
    memset (sin6, 0, sizeof (struct sockaddr_in6));
    sin6->sin6_family = AF_INET6;
    memcpy (&sin6->sin6_addr, addrbuf, 16);     /* source address */
    memcpy (&sin6->sin6_port, addrbuf + 32, 2); /* source port */
    *addrlen = sizeof (struct sockaddr_in6);

    /* Drain any trailing TLV bytes */
    size_t to_drain = extra - PP2_ADDR_LEN_INET6;
    while (to_drain > 0)
    {
      size_t chunk = (to_drain > sizeof (drain)) ? sizeof (drain) : to_drain;
      if (recv_all (socket, drain, chunk, timeout_ms) < 0)
      {
        lprintf (1, "PROXY protocol v2: error draining IPv6 TLV data");
        return -1;
      }
      to_drain -= chunk;
    }

    return 0;
  }
  else
  {
    /* AF_UNSPEC or unknown family: treat as LOCAL per spec, drain payload */
    size_t to_drain = extra;
    while (to_drain > 0)
    {
      size_t chunk = (to_drain > sizeof (drain)) ? sizeof (drain) : to_drain;
      if (recv_all (socket, drain, chunk, timeout_ms) < 0)
      {
        lprintf (1, "PROXY protocol v2: error draining unspec payload");
        return -1;
      }
      to_drain -= chunk;
    }
    return 1;
  }
} /* End of proxy_protocol_v2_read() */

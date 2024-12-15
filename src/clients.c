/**************************************************************************
 * clients.c
 *
 * General client thread definition and common utility functions
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

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>

#include "clients.h"
#include "dlclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "rbtree.h"
#include "slclient.h"
#include "tls.h"

/* Progressive throttle stepping and maximum in milliseconds */
#define THROTTLE_STEPPING 50  /* 50 milliseconds */
#define THROTTLE_MAXIMUM 500  /* 1/2 second */

static int ClientRecv (ClientInfo *cinfo);

/* Test first 3 characters of buffer for HTTP methods:
   GET, HEAD, POST, PUT, DELETE, TRACE and CONNECT */
#define HTTPMETHOD(X) (                                  \
    (*X == 'G' && *(X + 1) == 'E' && *(X + 2) == 'T') || \
    (*X == 'H' && *(X + 1) == 'E' && *(X + 2) == 'A') || \
    (*X == 'P' && *(X + 1) == 'O' && *(X + 2) == 'S') || \
    (*X == 'P' && *(X + 1) == 'U' && *(X + 2) == 'T') || \
    (*X == 'D' && *(X + 1) == 'E' && *(X + 2) == 'L') || \
    (*X == 'T' && *(X + 1) == 'R' && *(X + 2) == 'A') || \
    (*X == 'C' && *(X + 1) == 'O' && *(X + 2) == 'N'))

/***********************************************************************
 * ClientThread:
 *
 * Thread to handle all communications with a client.
 *
 * Returns NULL.
 ***********************************************************************/
void *
ClientThread (void *arg)
{
  ClientInfo *cinfo;
  RingReader reader;
  struct thread_data *mytdp;
  int sentbytes;
  int sockflags;
  int setuperr = 0;
  ssize_t nrecv;
  int nread;

  /* Throttle related */
  uint32_t throttle_msec = 0; /* Throttle time in milliseconds */
  struct timespec timereq;   /* Throttle for nanosleep() */

  if (!arg)
    return NULL;

  mytdp = (struct thread_data *)arg;
  cinfo = (ClientInfo *)mytdp->td_prvtptr;

  /* Connect linked structures */
  cinfo->reader     = &reader;
  reader.ringparams = cinfo->ringparams;

  /* Initialize RingReader parameters */
  reader.pktoffset   = -1;
  reader.pktid       = RINGID_NONE;
  reader.pkttime     = NSTUNSET;
  reader.datastart   = NSTUNSET;
  reader.dataend     = NSTUNSET;
  reader.limit       = NULL;
  reader.limit_data  = NULL;
  reader.match       = NULL;
  reader.match_data  = NULL;
  reader.reject      = NULL;
  reader.reject_data = NULL;

  /* Set initial state */
  cinfo->state = STATE_COMMAND;

  /* Resolve IP address to hostname */
  if (config.resolvehosts)
  {
    if (getnameinfo (cinfo->addr, cinfo->addrlen,
                     cinfo->hostname, sizeof (cinfo->hostname), NULL, 0, 0))
    {
      /* Copy numeric IP address into hostname on failure to resolve */
      strncpy (cinfo->hostname, cinfo->ipstr, sizeof (cinfo->hostname) - 1);
    }
  }
  /* Otherwise use the numerical IP address as the hostname */
  else
  {
    /* Copy numeric IP address into hostname when not resolving */
    strncpy (cinfo->hostname, cinfo->ipstr, sizeof (cinfo->hostname) - 1);
  }

  lprintf (1, "Client connected [%s]: %s [%s] port %s",
           cinfo->serverport, cinfo->hostname, cinfo->ipstr, cinfo->portstr);

  /* Initialize stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  cinfo->streams      = RBTreeCreate (KeyCompare, free, free);
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Allocate client specific send buffer */
  cinfo->sendbufsize = 2 * cinfo->ringparams->pktsize;
  cinfo->sendbuf     = (char *)malloc (cinfo->sendbufsize);
  if (!cinfo->sendbuf)
  {
    lprintf (0, "[%s] Error allocating send buffer", cinfo->hostname);
    setuperr = 1;
  }

  /* Allocate client specific receive buffer */
  cinfo->recvbufsize = 10 * cinfo->ringparams->pktsize;
  cinfo->recvbuf     = (char *)malloc (cinfo->recvbufsize);
  if (!cinfo->recvbuf)
  {
    lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
    setuperr = 1;
  }

  /* Set client socket connection to non-blocking */
  sockflags = fcntl (cinfo->socket, F_GETFL, 0);
  sockflags |= O_NONBLOCK;
  if (fcntl (cinfo->socket, F_SETFL, sockflags) == -1)
  {
    lprintf (0, "[%s] Error setting non-blocking flag: %s", cinfo->hostname, strerror (errno));
    setuperr = 1;
  }

  /* Limit sources if specified */
  if (cinfo->limitstr)
  {
    if (RingLimit (&reader, cinfo->limitstr) < 0)
    {
      lprintf (0, "[%s] Error with RingLimit for '%s'", cinfo->hostname, cinfo->limitstr);
      setuperr = 1;
    }
  }

  if (cinfo->tls && tls_configure (cinfo))
  {
    lprintf (0, "[%s] Error negotiating TLS", cinfo->hostname);
    setuperr = 1;
  }

  /* Shutdown the client connection if there were setup errors */
  if (setuperr)
  {
    /* Set thread closing status */
    pthread_mutex_lock (&(mytdp->td_lock));
    mytdp->td_state = TDS_CLOSING;
    pthread_mutex_unlock (&(mytdp->td_lock));

    /* Close client socket */
    if (cinfo->socket)
    {
      shutdown (cinfo->socket, SHUT_RDWR);
      close (cinfo->socket);
      cinfo->socket = -1;
    }

    tls_cleanup (cinfo);

    /* Release limit related PCRE2 data
     * The limitstr is not owned by the client so not free'd */
    if (cinfo->reader->limit)
      pcre2_code_free (cinfo->reader->limit);
    if (cinfo->reader->limit_data)
      pcre2_match_data_free (cinfo->reader->limit_data);

    cinfo->reader = NULL;

    /* Release stream tracking binary tree */
    pthread_mutex_lock (&(cinfo->streams_lock));
    RBTreeDestroy (cinfo->streams);
    cinfo->streams      = NULL;
    cinfo->streamscount = 0;
    pthread_mutex_unlock (&(cinfo->streams_lock));

    free (cinfo->sendbuf);
    free (cinfo->recvbuf);
    free (cinfo->addr);
    cinfo->addr = NULL;
    free (cinfo->mswrite);

    lprintf (1, "Client setup error, disconnected: %s", cinfo->hostname);

    /* Set thread CLOSED status */
    pthread_mutex_lock (&(mytdp->td_lock));
    mytdp->td_state = TDS_CLOSED;
    pthread_mutex_unlock (&(mytdp->td_lock));

    return NULL;
  }

  /* If only one protocol is enabled set the expected client type */
  if (cinfo->protocols == PROTO_DATALINK)
    cinfo->type = CLIENT_DATALINK;
  else if (cinfo->protocols == PROTO_SEEDLINK)
    cinfo->type = CLIENT_SEEDLINK;
  else if (cinfo->protocols == PROTO_HTTP)
    cinfo->type = CLIENT_HTTP;

  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if (mytdp->td_state == TDS_SPAWNING)
    mytdp->td_state = TDS_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));

  /* Main client loop, delegating processing and data flow */
  while (mytdp->td_state != TDS_CLOSE)
  {
    /* Increment throttle if not at maximum */
    if (throttle_msec < THROTTLE_MAXIMUM)
      throttle_msec += THROTTLE_STEPPING;

    /* Determine client type from first 3 bytes of received data if not TLS */
    if (cinfo->type == CLIENT_UNDETERMINED && cinfo->tlsctx == NULL)
    {
      if ((nrecv = recv (cinfo->socket, cinfo->recvbuf, 3, MSG_PEEK)) == 3)
      {
        /* DataLink commands start with 'DL' */
        if (cinfo->protocols & PROTO_DATALINK &&
            cinfo->recvbuf[0] == 'D' &&
            cinfo->recvbuf[1] == 'L')
        {
          cinfo->type = CLIENT_DATALINK;
        }
        /* HTTP requests start with known method */
        else if (cinfo->protocols & PROTO_HTTP &&
                 HTTPMETHOD (cinfo->recvbuf))
        {
          cinfo->type = CLIENT_HTTP;
        }
        /* Everything else is SeedLink if it's allowed on this listener */
        else if (cinfo->protocols & PROTO_SEEDLINK)
        {
          cinfo->type = CLIENT_SEEDLINK;
        }
        else
        {
          lprintf (0, "[%s] Cannot determine allowed client protocol from '%c%c%c'",
                   cinfo->hostname,
                   (cinfo->recvbuf[0] < 32 || cinfo->recvbuf[0] > 126) ? '?' : cinfo->recvbuf[0],
                   (cinfo->recvbuf[1] < 32 || cinfo->recvbuf[1] > 126) ? '?' : cinfo->recvbuf[1],
                   (cinfo->recvbuf[2] < 32 || cinfo->recvbuf[2] > 126) ? '?' : cinfo->recvbuf[2]);
          break;
        }
      }
      /* Check for shutdown or errors except no data on non-blocking */
      else if (nrecv == 0 || (nrecv == -1 && errno != EAGAIN  && errno != EWOULDBLOCK))
      {
        break;
      }
    }
    else if (cinfo->type == CLIENT_UNDETERMINED && cinfo->tlsctx != NULL)
    {
      lprintf (1, "[%s] Client protocol cannot be detected on a TLS connection",
               cinfo->hostname);
      break;
    }

    /* Recv data from client */
    nread = ClientRecv (cinfo);

    /* Error receiving data, -1 = orderly shutdown, -2 = error */
    if (nread < 0)
    {
      break;
    }

    /* Data received from client */
    if (nread > 0)
    {
      /* If data was received do not throttle */
      throttle_msec = 0;

      /* Update the time of the last packet exchange */
      cinfo->lastxchange = NSnow ();

      /* Handle data from client according to client type */
      if (cinfo->type == CLIENT_DATALINK)
      {
        if (DLHandleCmd (cinfo))
        {
          break;
        }
      }
      else if (cinfo->type == CLIENT_HTTP)
      {
        if (HandleHTTP (cinfo->recvbuf, cinfo))
        {
          break;
        }
      }
      else
      {
        if (SLHandleCmd (cinfo))
        {
          break;
        }
      }
    } /* Done handling data from client */

    /* Regular, outbound data flow */
    if (cinfo->state == STATE_STREAM)
    {
      sentbytes = 0;

      if (cinfo->type == CLIENT_DATALINK)
      {
        sentbytes = DLStreamPackets (cinfo);
      }
      else if (cinfo->type == CLIENT_SEEDLINK)
      {
        sentbytes = SLStreamPackets (cinfo);
      }

      if (sentbytes < 0) /* Bail on error */
      {
        break;
      }
      if (sentbytes == 0) /* No packet sent, maximum throttle immediately */
      {
        throttle_msec = THROTTLE_MAXIMUM;
      }
      else /* If packet sent do not throttle */
      {
        throttle_msec = 0;
      }
    } /* Done with data streaming */

    /* Throttle loop and check for idle connections */
    if (throttle_msec > 0)
    {
      /* Check for connections with no communication and drop if idle
         for more than 10 seconds */
      if (throttle_msec >= THROTTLE_MAXIMUM &&
          cinfo->lastxchange == cinfo->conntime &&
          (NSnow () - cinfo->conntime) > ((nstime_t)NSTMODULUS * 10))
      {
        lprintf (0, "[%s] Non-communicating client timeout",
                 cinfo->hostname);
        break;
      }

      /* For known connection types throttle the loop until data is available */
      if (cinfo->type != CLIENT_UNDETERMINED)
      {
        PollSocket (cinfo->socket, 1, 0, throttle_msec);
      }
      /* For unknown (undetermined) connection types throttle the loop
         using nanosleep() as one or two bytes may be available but not
         enough to determine the type. */
      else
      {
        timereq.tv_sec  = 0;
        timereq.tv_nsec = throttle_msec * 1000000;

        nanosleep (&timereq, NULL);
      }
    }
  } /* End of main client loop */

  /* Set thread CLOSING status, locking entire client list */
  pthread_mutex_lock (&param.cthreads_lock);
  mytdp->td_state = TDS_CLOSING;
  pthread_mutex_unlock (&param.cthreads_lock);

  /* Close client socket */
  if (cinfo->socket)
  {
    shutdown (cinfo->socket, SHUT_RDWR);
    close (cinfo->socket);
    cinfo->socket = -1;
  }

  tls_cleanup (cinfo);

  /* Write out transmission log for this client if requested */
  if (TLogParams.tlogbasedir)
  {
    lprintf (2, "[%s] Writing transmission log", cinfo->hostname);
    WriteTLog (cinfo, 1);
  }

  /* Release limit related PCRE2 data
   * The limitstr is not owned by the client so not free'd */
  if (cinfo->reader->limit)
    pcre2_code_free (cinfo->reader->limit);
  if (cinfo->reader->limit_data)
    pcre2_match_data_free (cinfo->reader->limit_data);

  /* Release match and reject selectors strings and related PCRE2 data */
  free (cinfo->matchstr);
  if (cinfo->reader->match)
    pcre2_code_free (cinfo->reader->match);
  if (cinfo->reader->match_data)
    pcre2_match_data_free (cinfo->reader->match_data);
  free (cinfo->rejectstr);
  if (cinfo->reader->reject)
    pcre2_code_free (cinfo->reader->reject);
  if (cinfo->reader->reject_data)
    pcre2_match_data_free (cinfo->reader->reject_data);

  cinfo->reader = NULL;

  /* Release stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  RBTreeDestroy (cinfo->streams);
  cinfo->streams      = NULL;
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Release the client send and receive buffers */
  free (cinfo->sendbuf);
  free (cinfo->recvbuf);

  /* Release client socket structure, allocated in ListenThread() */
  free (cinfo->addr);
  cinfo->addr = NULL;

  /* Shutdown and release miniSEED write data stream */
  if (cinfo->mswrite)
  {
    ds_streamproc (cinfo->mswrite, NULL, NULL, cinfo->hostname);
    free (cinfo->mswrite);
    cinfo->mswrite = NULL;
  }

  if (cinfo->type == CLIENT_SEEDLINK && cinfo->extinfo)
    SLFree (cinfo);

  if (cinfo->type == CLIENT_DATALINK && cinfo->extinfo)
    DLFree (cinfo);

  lprintf (1, "Client disconnected: %s", cinfo->hostname);

  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_state = TDS_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));

  return NULL;
} /* End of ClientThread() */

/***********************************************************************
 * ClientRecv:
 *
 * Recv data in manner specific to the client.
 *
 * If WebSocket connection recv the framing and store the mask.
 *
 * Return >0 as number of bytes read on success
 * Return  0 when no data is available
 * Return -1 on error or timeout, ClientInfo.socketerr is set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***********************************************************************/
static int
ClientRecv (ClientInfo *cinfo)
{
  uint64_t wslength;
  int nread = 0;

  if (!cinfo)
    return -1;

  /* Recv a WebSocket frame if this connection is WebSocket */
  if (cinfo->websocket && cinfo->wspayload == 0)
  {
    cinfo->wsmaskidx = 0;
    nread = RecvWSFrame (cinfo, &wslength);

    if (nread < 0)
    {
      if (nread == -1)
        lprintf (0, "[%s] Error receiving WebSocket frame from client (%d)",
                 cinfo->hostname, nread);


      return nread;
    }
    else if (nread == 0)
    {
      return 0;
    }
    else
    {
      cinfo->wspayload = wslength;
    }
  }

  /* Check for data from client */
  if (cinfo->type == CLIENT_DATALINK)
  {
    nread = RecvDLCommand (cinfo);
  }
  else if (cinfo->type == CLIENT_SEEDLINK)
  {
    nread = RecvLine (cinfo);
  }
  else if (cinfo->type == CLIENT_HTTP)
  {
    nread = RecvLine (cinfo);
  }
  else
  {
    lprintf (0, "[%s] Unknown client type, cannot receive data",
             cinfo->hostname);
    return -1;
  }

  return nread;
} /* End of ClientRecv() */

/***************************************************************************
 * SendData:
 *
 * Send 'buflen' bytes from 'buffer' to 'cinfo->socket'.
 * A thin wrapper to call SendDataMB() with a single buffer.
 *
 * Returns the return value of SendDataMB().
 ***************************************************************************/
int
SendData (ClientInfo *cinfo, void *buffer, size_t buflen, int no_wsframe)
{
  return SendDataMB (cinfo, &buffer, &buflen, 1, no_wsframe);
} /* End of SendData() */

/***************************************************************************
 * SendDataMB:  Multi-buffer send
 *
 * Send all buffers to 'cinfo->socket' in order.
 *
 * If connection is a WebSocket, and no_wsframe is not set, create a single
 * frame header that represents the total of all buffers.
 *
 * Socket is set to blocking during the send operation.
 *
 * Return  0 on success
 * Return -1 on error or timeout, ClientInfo.socketerr is set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***************************************************************************/
int
SendDataMB (ClientInfo *cinfo, void *buffer[], size_t buflen[],
            int bufcount, int no_wsframe)
{
  TLSCTX *tlsctx = cinfo->tlsctx;
  size_t totalbuflen = 0;
  ssize_t nsent;
  int sockflags;
  int blockflags;
  int idx;

  uint8_t wsframe[10];
  size_t wsframelen;
  uint8_t length8;
  uint16_t length16;
  uint64_t length64;

  if (!cinfo)
    return -1;

  if (bufcount <= 0)
    return 0;

  for (idx = 0; idx < bufcount; idx++)
  {
    totalbuflen += buflen[idx];
  }

  /* Clear non-blocking flag from socket flags */
  sockflags = blockflags = fcntl (cinfo->socket, F_GETFL, 0);
  blockflags &= ~O_NONBLOCK;
  if (fcntl (cinfo->socket, F_SETFL, blockflags) == -1)
  {
    lprintf (0, "[%s] %s(): Error clearing non-blocking flag: %s",
             cinfo->hostname, __func__, strerror (errno));
    cinfo->socketerr = -1;
    return -1;
  }

  /* If connection is WebSocket, generate and send an appropriate frame */
  if (cinfo->websocket && !no_wsframe)
  {
    wsframelen = 0;
    wsframe[0] = 0x82; /* FIN=1(0x80), OPCODE=binary(0x2) */
    wsframe[1] = 0;    /* MASK=0, payload length added below */

    if (totalbuflen < 126) /* If payload length < 126 store in bits 1-7 of byte 2 */
    {
      wsframelen = 2;
      length8    = totalbuflen;
      wsframe[1] |= length8;
    }
    else if (totalbuflen < UINT16_MAX) /* If payload length < 16-bit int store in next two bytes */
    {
      wsframelen = 4;
      wsframe[1] |= 126;
      length16 = totalbuflen;
      memcpy (&wsframe[2], &length16, 2);
      if (!ms_bigendianhost ())
        ms_gswap2 (&wsframe[2]);
    }
    else if (totalbuflen < UINT64_MAX) /* If payload length < 64-bit int store in next 8 bytes */
    {
      wsframelen = 10;
      wsframe[1] |= 127;
      length64 = totalbuflen;
      memcpy (&wsframe[2], &length64, 8);
      if (!ms_bigendianhost ())
        ms_gswap8 (&wsframe[2]);
    }
    else
    {
      lprintf (0, "[%s] %s() Payload length too large for WebSocket: %zu",
               cinfo->hostname, __func__, totalbuflen);
      return -1;
    }

    /* Send WebSocket frame */
    if (wsframelen)
    {
      if (cinfo->tlsctx)
      {
        /* TLS writes can be fragmented, loop until everything has been sent */
        for (int written = 0; written < wsframelen; written += nsent)
        {
          while ((nsent = mbedtls_ssl_write (&tlsctx->ssl, wsframe + written, wsframelen - written)) <= 0)
          {
            if (nsent != MBEDTLS_ERR_SSL_WANT_READ &&
                nsent != MBEDTLS_ERR_SSL_WANT_WRITE &&
                nsent != MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS)
              break;
          }

          if (nsent < 0)
            break;
        }
      }
      else
      {
        nsent = send (cinfo->socket, wsframe, wsframelen, 0);
      }

      /* Connection closed by peer */
      if ((cinfo->tlsctx && nsent == MBEDTLS_ERR_NET_CONN_RESET) ||
          (nsent == -1 && errno == EPIPE))
      {
        cinfo->socketerr = -2; /* Indicate an orderly shutdown */
        return -2;
      }
      /* Connection error */
      else if (nsent < 0)
      {
        lprintf (0, "[%s] Error sending WebSocket frame", cinfo->hostname);
        cinfo->socketerr = -1; /* Indicate fatal socket error */
        return -1;
      }
    }
  }

  /* Send each buffer in sequence */
  for (idx = 0; idx < bufcount; idx++)
  {
    if (cinfo->tlsctx)
    {
      /* TLS writes can be fragmented, loop until everything has been sent */
      for (int written = 0; written < buflen[idx]; written += nsent)
      {
        while ((nsent = mbedtls_ssl_write (&tlsctx->ssl, buffer[idx] + written, buflen[idx] - written)) <= 0)
        {
          if (nsent != MBEDTLS_ERR_SSL_WANT_READ &&
              nsent != MBEDTLS_ERR_SSL_WANT_WRITE &&
              nsent != MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS)
            break;
        }

        if (nsent < 0)
          break;
      }
    }
    else
    {
      nsent = send (cinfo->socket, buffer[idx], buflen[idx], 0);
    }

    /* Connection closed by peer */
    if ((cinfo->tlsctx && nsent == MBEDTLS_ERR_NET_CONN_RESET) ||
        (nsent == -1 && errno == EPIPE))
    {
      cinfo->socketerr = -2; /* Indicate an orderly shutdown */
      return -2;
    }
    /* Connection error */
    else if (nsent < 0)
    {
      /* Create a limited, printable buffer for the diagnostic message */
      char pbuffer[100];
      char *cp;
      size_t maxlength = (buflen[idx] < sizeof (pbuffer)) ? buflen[idx] : sizeof (pbuffer);

      strncpy (pbuffer, (char *)buffer[idx], maxlength - 1);
      pbuffer[sizeof (pbuffer) - 1] = '\0';

      if ((cp = memchr (pbuffer, '\r', maxlength)))
        *cp = '\0';

      if ((cp = memchr (pbuffer, '\n', maxlength)))
        *cp = '\0';

      /* Replace unprintable characters with '?', */
      for (cp = pbuffer; *cp != '\0'; cp++)
      {
        if (*cp < 32 || *cp > 126)
          *cp = '?';
      }

      lprintf (0, "[%s] Error sending data: '%s'", cinfo->hostname, pbuffer);
      cinfo->socketerr = -1; /* Indicate fatal socket error */
      return -1;
    }
  } /* Done looping through supplied buffers */

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = NSnow ();

  /* Restore original socket flags */
  if (fcntl (cinfo->socket, F_SETFL, sockflags) == -1)
  {
    lprintf (0, "[%s] %s(): Error setting non-blocking flag: %s",
             cinfo->hostname, __func__, strerror (errno));
    cinfo->socketerr = -1;
    return -1;
  }

  return 0;
} /* End of SendDataMB() */

/***********************************************************************
 * RecvData:
 *
 * Read recvlen bytes from a socket and place in the specified buffer.
 * The CLentInfo.recvbuf buffer is used to store the received data.
 * If the destination buffer is _not_ ClientInfo.recvbuf then the
 * data is copied to the destination buffer after being received.
 *
 * This routine handles fragmented receives, meaning that it will
 * continue to receive data until recvlen bytes have been received
 * with the exceptions noted below.
 *
 * If fulfill is false (0) this routine will return immediately if
 * no data is available on the first read.  But, if some data are
 * read during the first read this routine will block until recvlen
 * bytes have been read.
 *
 * If fulfill is true (non-0) this routine will block until
 * recvlen bytes have been read.
 *
 * For any blocking reads this routine will poll the socket for up to
 * 10 seconds before timing out and returning -2.
 *
 * The caller _must_ consume the data requested.  It will be discarded
 * on the next call to RecvData().
 *
 * Return >0 as number of bytes for the caller to consume on success
 * Return  0 when fulfill == 0 and no data is available
 * Return -1 on error or timeout, ClientInfo.socketerr may be set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***********************************************************************/
int
RecvData (ClientInfo *cinfo, void *buffer, size_t requested, int fulfill)
{
  TLSCTX *tlsctx = cinfo->tlsctx;
  ssize_t nrecv;
  size_t nread = 0;
  size_t receivable;
  char *recvptr;
  char peekbyte[1];

  if (!cinfo || !buffer || requested == 0)
  {
    cinfo->socketerr = -1;
    return -1;
  }

  /* Check if socket has been disconnected */
  if (recv (cinfo->socket, peekbyte, 1, MSG_PEEK) == 0)
  {
    cinfo->socketerr = -2;
    return -2;
  }

  if (requested > cinfo->recvbufsize)
  {
    lprintf (0, "[%s] %s(): Requested receive length exceeds buffer size",
             cinfo->hostname, __func__);
    cinfo->socketerr = -1;
    return -1;
  }

  /* Shift previously consumed bytes from receive buffer */
  if (cinfo->recvconsumed > 0)
  {
    if (cinfo->recvconsumed < cinfo->recvlength)
    {
      memmove (cinfo->recvbuf,
               cinfo->recvbuf + cinfo->recvconsumed,
               cinfo->recvlength - cinfo->recvconsumed);

      cinfo->recvlength -= cinfo->recvconsumed;
    }
    else
    {
      cinfo->recvlength = 0;
    }

    cinfo->recvconsumed = 0;
  }

  recvptr = cinfo->recvbuf + cinfo->recvlength;
  receivable = cinfo->recvbufsize - cinfo->recvlength;

  /* Recv until requested bytes are available */
  while ((cinfo->recvlength + nread) < requested)
  {
    if (cinfo->tlsctx)
    {
      nrecv = mbedtls_ssl_read (&tlsctx->ssl, (unsigned char *)recvptr, receivable - nread);
    }
    else
    {
      nrecv = recv (cinfo->socket, recvptr, receivable - nread, 0);
    }

    /* Poll/throttle efficiently when there is no data on a non-blocking connection
     * TLS connections: return values of MBEDTLS_ERR_SSL_WANT_READ and MBEDTLS_ERR_SSL_WANT_WRITE
     * non-TLS connections: nrecv == -1 and errno is EAGAIN or EWOULDBLOCK */
    if ((cinfo->tlsctx &&
         (nrecv == MBEDTLS_ERR_SSL_WANT_READ || nrecv == MBEDTLS_ERR_SSL_WANT_WRITE)) ||
        (nrecv == -1 &&
         (errno == EAGAIN || errno != EWOULDBLOCK)))
    {
      /* Return immediately if no data is available and no data has been read yet */
      if (fulfill == 0 && nread == 0)
        return 0;

      /* Poll up to 10 seconds == 10,000 milliseconds */
      int pollret = PollSocket (cinfo->socket, 1, 0, 10000);

      if (pollret == 0)
      {
        lprintf (0, "[%s] Timeout receiving data", cinfo->hostname);
        cinfo->socketerr = -1;
        return -1;
      }
      else if (pollret == -1 && errno != EINTR)
      {
        lprintf (0, "[%s] Error polling socket", cinfo->hostname);
        cinfo->socketerr = -1;
        return -1;
      }
    }

    /* Connection closed by peer */
    else if (nrecv == 0 ||
             (cinfo->tlsctx &&
              (nrecv == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY ||
               nrecv == MBEDTLS_ERR_NET_CONN_RESET)))
    {
      cinfo->socketerr = -2; /* Indicate an orderly shutdown */
      return -2;
    }

    /* Connection error */
    else if (nrecv < 0)
    {
      lprintf (0, "[%s] Error receiving data from client: %s",
               cinfo->hostname, strerror (errno));
      cinfo->socketerr = -1; /* Indicate fatal socket error */
      return -1;
    }

    /* Update recv pointer and received byte counts */
    else if (nrecv > 0)
    {
      recvptr += nrecv;
      nread += (size_t)nrecv;
    }
  }

  cinfo->recvlength += nread;

  /* Unmask expected WebSocket payload */
  if (cinfo->wspayload > 0)
  {
    if (cinfo->recvlength >= cinfo->wspayload)
    {
      recvptr = cinfo->recvbuf;
      for (int idx = 0; idx < cinfo->wspayload; idx++, recvptr++, cinfo->wsmaskidx++)
        *recvptr = *recvptr ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];

      cinfo->wspayload = 0;
    }
    else
    {
      /* The full WebSocket payload is not (yet) available */
      return 0;
    }
  }

  /* Copy data to supplied buffer if not the receive buffer */
  if (buffer != cinfo->recvbuf)
  {
    memcpy (buffer, cinfo->recvbuf, requested);
  }

  /* The caller _must_ consume the data requested */
  cinfo->recvconsumed = requested;

  return requested;
} /* End of RecvData() */

/***********************************************************************
 * RecvDLCommand:
 *
 * Read a command from the client socket as a header-only DataLink
 * packet.  A header-only DataLink packet is composed of a pre-header
 * followed by a header body.  The pre-header is composed of 3 bytes:
 * the two ASCII characters 'DL' followed by an unsigned 8-bit integer
 * indicating the size of the header body.
 *
 * This routine handles fragmented receives after some data has been
 * read.  If no data has been read and no data is available from the
 * socket this routine will return immediately.
 *
 * The command (header body) returned in the ClientInfo.recvbuf buffer
 * will always be a NULL terminated string.
 *
 * Return >0 as number of bytes read on success
 * Return  0 when no data is available
 * Return -1 on error or timeout, ClientInfo.socketerr is set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***********************************************************************/
int
RecvDLCommand (ClientInfo *cinfo)
{
  int nread = 0;
  int nrecv;
  uint8_t headerlen;

  if (!cinfo || !cinfo->recvbuf)
  {
    cinfo->socketerr = -1;
    return -1;
  }

  /* Receive and process the 3 byte DataLink pre-header */
  nrecv = RecvData (cinfo, cinfo->recvbuf, 3, 0);

  if (nrecv != 3)
  {
    return nrecv;
  }

  nread += nrecv;

  /* Sequence bytes of 'DL' identify DataLink */
  if (cinfo->recvbuf[0] == 'D' && cinfo->recvbuf[1] == 'L')
  {
    /* Determine length of header body */
    headerlen = (uint8_t)(cinfo->recvbuf[2]);
  }
  else
  {
    lprintf (2, "[%s] Error verifying DataLink sequence bytes (%c%c)",
             cinfo->hostname, *(cinfo->recvbuf), *(cinfo->recvbuf + 1));
    cinfo->socketerr = -1;
    return -1;
  }

  /* Sanity check the header size */
  if (headerlen < 2)
  {
    lprintf (0, "[%s] Pre-header indicates a header size too small: %u",
             cinfo->hostname, headerlen);
    cinfo->socketerr = -1;
    return -1;
  }

  /* Receive command in header body, must be fulfilled */
  nrecv = RecvData (cinfo, cinfo->dlcommand, headerlen, 1);

  if (nrecv != headerlen)
  {
    return nrecv;
  }

  nread += nrecv;

  /* Make sure the command is NULL terminated. The command buffer size is the
   * the maximum header length (UINT8_MAX) plus 1, so this should be safe. */
  cinfo->dlcommand[nrecv] = '\0';

  return nread;
} /* End of RecvDLCommand() */

/***********************************************************************
 * RecvLine:
 *
 * Check the receive buffer for lines terminated by '\r' (carriage return),
 * '\n' (newline), or both adjacent are found.
 *
 * The resulting line will be left in the receive buffer and will always
 * be NULL terminated.
 *
 * If no data has been read and no data is available from the socket
 * this routine will return immediately.
 *
 * Return >0 as number of bytes in line on success
 * Return  0 when no line is available
 * Return -1 on error or timeout, ClientInfo.socketerr is set
 * Return -2 on orderly shutdown, ClientInfo.socketerr is set
 ***********************************************************************/
int
RecvLine (ClientInfo *cinfo)
{
  size_t skipped = 0;
  int nread      = 0;

  if (!cinfo || !cinfo->recvbuf)
  {
    cinfo->socketerr = -1;
    return -1;
  }

  /* This routine must search for a variable number of terminators in
   * received data.  The strategy to do this is as follows:
   *
   * Request a single byte using RecvData() and then directly search
   * for terminators in all available data in the receive buffer.
   * The number of bytes consumed is manipulated to:
   * 1) leave data in the receive buffer if no terminators are yet and
   * 2) to consume one or two terminators as they are discovered. */

  nread = RecvData (cinfo, cinfo->recvbuf, 1, 0);

  if (nread <= 0)
  {
    return nread;
  }

  /* Skip initial terminators, SeedLink v4 requires ignoring empty commands */
  while (skipped <= nread)
  {
    if (cinfo->recvbuf[skipped] == '\n' || cinfo->recvbuf[skipped] == '\r')
    {
      skipped++;
    }
    else
    {
      break;
    }

    /* If too many initial terminators were received, abort to avoid DoS */
    if (skipped >= 10)
    {
      lprintf (0, "[%s] Received too many empty lines", cinfo->hostname);
      cinfo->socketerr = -1;
      return -1;
    }
  }

  /* Consume skipped terminators */
  if (skipped > 0)
  {
    cinfo->recvconsumed = skipped;

    nread = RecvData (cinfo, cinfo->recvbuf, 1, 0);

    if (nread <= 0)
    {
      return nread;
    }
  }

  /* Search for line terminators */
  char *cr = memchr (cinfo->recvbuf, '\r', cinfo->recvlength);
  char *nl = memchr (cinfo->recvbuf, '\n', cinfo->recvlength);

  /* If no terminators, do not consume data */
  if (cr == NULL && nl == NULL)
  {
    cinfo->recvconsumed = 0;
    return 0;
  }

  /* Determine first and last terminator */
  char *firstterminator = (nl && cr) ? (nl < cr) ? nl : cr : (nl) ? nl : cr;
  char *lastterminator = (nl && cr) ? (nl > cr) ? nl : cr : (nl) ? nl : cr;

  /* Ensure the last terminator, if different, directly follows first */
  if (firstterminator != lastterminator &&
      (firstterminator + 1) != lastterminator)
  {
    lastterminator = firstterminator;
  }

  /* Set consumed to include all bytes through the last terminator */
  cinfo->recvconsumed = (lastterminator - cinfo->recvbuf) + 1;

  /* NULL-terminate line at first terminator */
  *firstterminator = '\0';

  return (int)(firstterminator - cinfo->recvbuf);
} /* End of RecvLine() */

/**********************************************************************/ /**
 * PollSocket:
 *
 * Poll the connected socket for read and/or write ability using poll()
 * for a specified amount of time.
 *
 * The timeout is specified in milliseconds.
 *
 * Interrupted select() calls are retried until the timeout expires
 * unless the thread state is not TDS_SPAWNING or TDS_ACTIVE.
 *
 * return >=1 : success
 * return   0 : if time-out expires or socket not connected
 * return  <0 : errors, check errno
 ***************************************************************************/
int
PollSocket (int socket, int readability, int writability, int timeout_ms)
{
  struct pollfd pfd;

  if (socket < 0 || timeout_ms < 0)
    return 0;

  pfd.fd     = socket;
  pfd.events = 0;

  if (readability)
    pfd.events |= POLLIN;

  if (writability)
    pfd.events |= POLLOUT;

  return poll (&pfd, 1, timeout_ms);
} /* End of PollSocket() */

/***************************************************************************
 * GetStreamNode:
 *
 * Search the specified binary tree for a given Key and return the
 * StreamNode.  If the Key does not exist create it and add it to the
 * tree.  If adding a new entry in the tree the plock mutex will be
 * locked.  If a new entry was added the value of new will be set to 1
 * otherwise it will be set to 0.
 *
 * Return a pointer to a ChanNode or 0 for error.
 ***************************************************************************/
StreamNode *
GetStreamNode (RBTree *tree, pthread_mutex_t *plock, char *streamid, int *new)
{
  Key key;
  Key *newkey;
  RBNode *rbnode;
  StreamNode *stream = NULL;

  /* Generate key */
  key = FNVhash64 (streamid);

  /* Search for a matching entry */
  if ((rbnode = RBFind (tree, &key)))
  {
    stream = (StreamNode *)rbnode->data;
    *new   = 0;
  }
  else
  {
    if ((newkey = (Key *)malloc (sizeof (Key))) == NULL)
    {
      lprintf (0, "GetStreamNode: Error allocating new key");
      return 0;
    }

    *newkey = key;

    if ((stream = (StreamNode *)malloc (sizeof (StreamNode))) == NULL)
    {
      lprintf (0, "GetStreamNode: Error allocating new node");
      return 0;
    }

    /* Initialize the new StreamNode */
    strncpy (stream->streamid, streamid, sizeof (stream->streamid) - 1);
    *(stream->streamid + sizeof (stream->streamid) - 1) = '\0';
    stream->txpackets                                   = 0;
    stream->txbytes                                     = 0;
    stream->rxpackets                                   = 0;
    stream->rxbytes                                     = 0;
    stream->endtimereached                              = 0;

    /* Add the new entry while locking the tree */
    pthread_mutex_lock (plock);
    RBTreeInsert (tree, newkey, stream, 0);
    pthread_mutex_unlock (plock);

    *new = 1;
  }

  return stream;
} /* End of GetStreamNode() */

/***************************************************************************
 * AddToString:
 *
 * Concatinate one string to another with a delimiter in-between
 * growing the target string as needed up to a maximum length.  The
 * new addition can be added to either the beggining or end of the
 * string using the where flag:
 *
 * where == 0 means add new addition to end of string
 * where != 0 means add new addition to beginning of string
 *
 * Return 0 on success, -1 on memory allocation error and -2 when
 * string would grow beyond maximum length.
 ***************************************************************************/
int
AddToString (char **string, char *add, char *delim, size_t where, size_t maxlen)
{
  size_t length;
  char *ptr;

  if (!string || !add)
    return -1;

  /* If string is empty, allocate space and copy the addition */
  if (!*string)
  {
    length = strlen (add) + 1;

    if (length > maxlen)
      return -2;

    if ((*string = (char *)malloc (length)) == NULL)
      return -1;

    strcpy (*string, add);
  }
  /* Otherwise add the addition with a delimiter */
  else
  {
    length = strlen (*string) + strlen (delim) + strlen (add) + 1;

    if (length > maxlen)
      return -2;

    if ((ptr = (char *)malloc (length)) == NULL)
      return -1;

    /* Put addition at beginning of the string */
    if (where)
    {
      snprintf (ptr, length, "%s%s%s",
                (add) ? add : "",
                (delim) ? delim : "",
                *string);
    }
    /* Put addition at end of the string */
    else
    {
      snprintf (ptr, length, "%s%s%s",
                *string,
                (delim) ? delim : "",
                (add) ? add : "");
    }

    /* Free previous string and set pointer to newly allocated space */
    free (*string);
    *string = ptr;
  }

  return 0;
} /* End of AddToString() */

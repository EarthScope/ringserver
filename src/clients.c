/**************************************************************************
 * clients.c
 *
 * General client thread definition and common utility functions
 *
 * Copyright 2016 Chad Trabant, IRIS Data Managment Center
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
 * Modified: 2017.012
 **************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "ringserver.h"
#include "clients.h"
#include "dlclient.h"
#include "slclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "rbtree.h"

/* Progressive throttle stepping and maximum in microseconds */
#define THROTTLE_STEPPING 100000  /* 1/10 second */
#define THROTTLE_MAXIMUM  500000  /* 1/2 second */

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

  struct sockaddr_in sin;
  socklen_t sinlen = sizeof (struct sockaddr_in);
  int serverport = -1;

  /* Throttle related */
  uint32_t throttleusec = 0; /* Throttle time in microseconds */
  fd_set readset;            /* File descriptor set for select() */
  struct timeval timeout;    /* Timeout throttle for select() */
  struct timespec timereq;   /* Throttle for nanosleep() */

  if (!arg)
    return NULL;

  mytdp = (struct thread_data *)arg;
  cinfo = (ClientInfo *)mytdp->td_prvtptr;

  /* Glue together linked structures */
  cinfo->reader = &reader;
  reader.ringparams = cinfo->ringparams;

  /* Initialize RingReader parameters */
  reader.pktid = 0;
  reader.pkttime = HPTERROR;
  reader.datastart = HPTERROR;
  reader.dataend = HPTERROR;
  reader.limit = 0;
  reader.limit_extra = 0;
  reader.match = 0;
  reader.match_extra = 0;
  reader.reject = 0;
  reader.reject_extra = 0;

  /* Set initial state */
  cinfo->state = STATE_COMMAND;

  /* Resolve IP address to hostname */
  if (resolvehosts)
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

  /* Find the server port used for this connection */
  if (getsockname (cinfo->socket, (struct sockaddr *)&sin, &sinlen) == 0)
  {
    serverport = ntohs (sin.sin_port);
  }

  lprintf (1, "Client connected [%d]: %s [%s] port %s",
           serverport, cinfo->hostname, cinfo->ipstr, cinfo->portstr);

  /* Initialize stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  cinfo->streams = RBTreeCreate (KeyCompare, free, free);
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Allocate client specific send buffer */
  cinfo->sendbuf = (char *)malloc (2 * cinfo->ringparams->pktsize);
  if (!cinfo->sendbuf)
  {
    lprintf (0, "[%s] Error allocating send buffer", cinfo->hostname);
    setuperr = 1;
  }
  cinfo->sendbuflen = 2 * cinfo->ringparams->pktsize;

  /* Allocate client specific receive buffer */
  cinfo->recvbuf = (char *)calloc (1, 2 * cinfo->ringparams->pktsize);
  if (!cinfo->recvbuf)
  {
    lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
    setuperr = 1;
  }
  cinfo->recvbuflen = 2 * cinfo->ringparams->pktsize;

  /* Allocate client specific packet data buffer */
  cinfo->packetdata = (char *)malloc (cinfo->ringparams->pktsize);
  if (!cinfo->packetdata)
  {
    lprintf (0, "[%s] Error allocating packet buffer", cinfo->hostname);
    setuperr = 1;
  }

  /* Set client socket connection to non-blocking */
  sockflags = fcntl (cinfo->socket, F_GETFL, 0);
  sockflags |= O_NONBLOCK;
  if (fcntl (cinfo->socket, F_SETFL, sockflags) == -1)
  {
    lprintf (0, "[%s] Error setting non-blocking flag: %s",
             cinfo->hostname, strerror (errno));
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

  /* Shutdown the client connection if there were setup errors */
  if (setuperr)
  {
    /* Close client socket */
    if (cinfo->socket)
    {
      shutdown (cinfo->socket, SHUT_RDWR);
      close (cinfo->socket);
      cinfo->socket = 0;
    }

    /* Set thread closing status */
    pthread_mutex_lock (&(mytdp->td_lock));
    mytdp->td_flags = TDF_CLOSING;
    pthread_mutex_unlock (&(mytdp->td_lock));

    if (cinfo->sendbuf)
      free (cinfo->sendbuf);

    if (cinfo->recvbuf)
      free (cinfo->recvbuf);

    if (cinfo->packetdata)
      free (cinfo->packetdata);

    if (cinfo->mswrite)
      free (cinfo->mswrite);

    return NULL;
  }

  /* If only one protocol is allowed set the expected client type */
  if (cinfo->protocols == PROTO_DATALINK)
    cinfo->type = CLIENT_DATALINK;
  else if (cinfo->protocols == PROTO_SEEDLINK)
    cinfo->type = CLIENT_SEEDLINK;
  else if (cinfo->protocols == PROTO_HTTP)
    cinfo->type = CLIENT_HTTP;

  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if (mytdp->td_flags == TDF_SPAWNING)
    mytdp->td_flags = TDF_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));

  /* Main client loop, delegating processing and data flow */
  while (mytdp->td_flags != TDF_CLOSE)
  {
    /* Increment throttle if not at maximum */
    if (throttleusec < THROTTLE_MAXIMUM)
      throttleusec += THROTTLE_STEPPING;

    /* Determine client type from first 3 bytes of request */
    if (cinfo->type == CLIENT_UNDETERMINED)
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
          lprintf (0, "[%s] Cannot determine allowed client from '%c%c%c'",
                   cinfo->hostname,
                   (cinfo->recvbuf[0] < 32 || cinfo->recvbuf[0] > 126) ? '?' : cinfo->recvbuf[0],
                   (cinfo->recvbuf[1] < 32 || cinfo->recvbuf[1] > 126) ? '?' : cinfo->recvbuf[1],
                   (cinfo->recvbuf[2] < 32 || cinfo->recvbuf[2] > 126) ? '?' : cinfo->recvbuf[2]);
          break;
        }
      }
      /* Check for shutdown or errors except EAGAIN (no data on non-blocking) */
      else if (nrecv == 0 || (nrecv == -1 && errno != EAGAIN))
      {
        break;
      }
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
      throttleusec = 0;

      /* Update the time of the last packet exchange */
      cinfo->lastxchange = HPnow ();

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
        throttleusec = THROTTLE_MAXIMUM;
      }
      else /* If packet sent do not throttle */
      {
        throttleusec = 0;
      }
    } /* Done with data streaming */

    /* Throttle loop and check for idle connections */
    if (throttleusec > 0)
    {
      /* Check for connections with no communication and drop if idle
         for more than 10 seconds */
      if (throttleusec >= THROTTLE_MAXIMUM &&
          cinfo->lastxchange == cinfo->conntime &&
          (HPnow () - cinfo->conntime) > (HPTMODULUS * 10))
      {
        lprintf (0, "[%s] Non-communicating client timeout",
                 cinfo->hostname);
        break;
      }

      /* For known connection types throttle the loop using select()
         so that data from the client is never waiting for a timeout. */
      if (cinfo->type != CLIENT_UNDETERMINED)
      {
        /* Configure the read descriptor set with only our client socket */
        FD_ZERO (&readset);
        FD_SET (cinfo->socket, &readset);

        timeout.tv_sec = 0;
        timeout.tv_usec = throttleusec;

        select (cinfo->socket + 1, &readset, NULL, NULL, &timeout);
      }
      /* For unknown (undetermined) connection types throttle the loop
         using nanosleep() as one or two bytes may be available but not
         enough to determine the type. */
      else
      {
        timereq.tv_sec = 0;
        timereq.tv_nsec = throttleusec * 1000;

        nanosleep (&timereq, NULL);
      }
    }
  } /* End of main client loop */

  /* Set thread CLOSING status, locking entire client list */
  pthread_mutex_lock (&cthreads_lock);
  mytdp->td_flags = TDF_CLOSING;
  pthread_mutex_unlock (&cthreads_lock);

  /* Close client socket */
  if (cinfo->socket)
  {
    shutdown (cinfo->socket, SHUT_RDWR);
    close (cinfo->socket);
    cinfo->socket = 0;
  }

  /* Write out transmission log for this client if requested */
  if (TLogParams.tlogbasedir)
  {
    lprintf (2, "[%s] Writing transmission log", cinfo->hostname);
    WriteTLog (cinfo, 1);
  }

  /* Release match, reject and selectors strings */
  if (cinfo->matchstr)
    free (cinfo->matchstr);
  if (cinfo->reader->match)
    pcre_free (cinfo->reader->match);
  if (cinfo->reader->match_extra)
    pcre_free (cinfo->reader->match_extra);
  if (cinfo->rejectstr)
    free (cinfo->rejectstr);
  if (cinfo->reader->reject)
    pcre_free (cinfo->reader->reject);
  if (cinfo->reader->reject_extra)
    pcre_free (cinfo->reader->reject_extra);

  /* Release stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  RBTreeDestroy (cinfo->streams);
  cinfo->streams = 0;
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Release the client send, receive and packet buffers */
  if (cinfo->sendbuf)
    free (cinfo->sendbuf);
  if (cinfo->recvbuf)
    free (cinfo->recvbuf);
  if (cinfo->packetdata)
    free (cinfo->packetdata);

  /* Release client socket structure */
  if (cinfo->addr)
    free (cinfo->addr);

  /* Shutdown and release Mini-SEED write data stream */
  if (cinfo->mswrite)
  {
    ds_streamproc (cinfo->mswrite, NULL, NULL, cinfo->hostname);
    free (cinfo->mswrite);
    cinfo->mswrite = 0;
  }

  if (cinfo->type == CLIENT_SEEDLINK && cinfo->extinfo)
    SLFree (cinfo);

  lprintf (1, "Client disconnected: %s", cinfo->hostname);

  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSED;
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
 * Return number of characters read on success, -1 on connection
 * shutdown and -2 on error.
 ***********************************************************************/
static int
ClientRecv (ClientInfo *cinfo)
{
  uint64_t wslength;
  int nread = 0;

  /* Recv a WebSocket frame if this connection is WebSocket */
  if (cinfo->websocket)
  {
    cinfo->wsmaskidx = 0;
    nread = RecvWSFrame (cinfo, &wslength, &cinfo->wsmask.one);

    if (nread < 0)
    {
      if (nread < -1)
        lprintf (0, "[%s] Error recv'ing WebSocket frame from client (%d)",
                 cinfo->hostname, nread);

      return nread;
    }
    else if (nread == 0)
    {
      return 0;
    }
  }

  /* Check for data from client */
  if (cinfo->protocols & PROTO_DATALINK &&
      cinfo->type == CLIENT_DATALINK)
  {
    nread = RecvCmd (cinfo);
  }
  else if (cinfo->protocols & PROTO_SEEDLINK &&
           cinfo->type == CLIENT_SEEDLINK)
  {
    nread = RecvLine (cinfo);
  }
  else if (cinfo->protocols & PROTO_HTTP &&
           cinfo->type == CLIENT_HTTP)
  {
    nread = RecvLine (cinfo);
  }

  /* Check WebSocket payload length against what was recv'ed, accounting for
     1 or 2 dropped terminators. */
  if (cinfo->websocket &&
      nread != wslength &&
      nread != wslength - 1 &&
      nread != wslength - 2)
  {
    lprintf (1, "[%s] WebSocket payload length (%" PRId64 ") does not match bytes read (%d)",
             cinfo->hostname, wslength, nread);
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
SendData (ClientInfo *cinfo, void *buffer, size_t buflen)
{
  return SendDataMB (cinfo, &buffer, &buflen, 1);
} /* End of SendData() */

/***************************************************************************
 * SendDataMB:  Multi-buffer send
 *
 * Send all buffers to 'cinfo->socket' in order.
 *
 * If connection is a WebSocket create a single frame header that
 * represents the total of all buffers.
 *
 * Socket is set to blocking during the send operation.
 *
 * Returns 0 on success and -1 on error, the ClientInfo.socketerr
 * value is set on socket errors.
 ***************************************************************************/
int
SendDataMB (ClientInfo *cinfo, void *buffer[], size_t buflen[], int bufcount)
{
  size_t totalbuflen = 0;
  int sockflags;
  int blockflags;
  int idx;

  uint8_t wsframe[10];
  int wsframelen;
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
    lprintf (0, "[%s] SendData(): Error clearing non-blocking flag: %s",
             cinfo->hostname, strerror (errno));
    cinfo->socketerr = 1;
    return -1;
  }

  /* If connection is WebSocket, generate and send an appropriate frame */
  if (cinfo->websocket)
  {
    wsframelen = 0;
    wsframe[0] = 0x82; /* FIN=1(0x80), OPCODE=binary(0x2) */
    wsframe[1] = 0;    /* MASK=0, payload length added below */

    if (totalbuflen < 126) /* If payload length < 126 store in bits 1-7 of byte 2 */
    {
      wsframelen = 2;
      length8 = totalbuflen;
      wsframe[1] |= length8;
    }
    else if (totalbuflen < (1 << 16)) /* If payload length < 16-bit int store in next two bytes */
    {
      wsframelen = 4;
      wsframe[1] |= 126;
      length16 = totalbuflen;
      memcpy (&wsframe[2], &length16, 2);
      if (!ms_bigendianhost ())
        ms_gswap2a (&wsframe[2]);
    }
    else if (totalbuflen < (1ull << 63)) /* If payload length < 64-bit int store in next 8 bytes */
    {
      wsframelen = 10;
      wsframe[1] |= 127;
      length64 = totalbuflen;
      memcpy (&wsframe[2], &length64, 8);
      if (!ms_bigendianhost ())
        ms_gswap8a (&wsframe[2]);
    }
    else
    {
      lprintf (0, "[%s] SendData(): payload length too large: %zu",
               cinfo->hostname, totalbuflen);
      return -1;
    }

    /* Send WebSocket frame */
    if (wsframelen)
    {
      if (send (cinfo->socket, wsframe, wsframelen, 0) < 0)
      {
        /* EPIPE indicates a client disconnect, everything else is an error */
        if (errno == EPIPE)
        {
          cinfo->socketerr = 2; /* Indicate an orderly shutdown */
        }
        else
        {
          lprintf (0, "[%s] SendData(): Error sending WebSocket frame", cinfo->hostname);
          cinfo->socketerr = 1;
        }

        return -1;
      }
    }
  }

  /* Send each buffer in sequence */
  for (idx = 0; idx < bufcount; idx++)
  {
    if (send (cinfo->socket, buffer[idx], buflen[idx], 0) < 0)
    {
      /* EPIPE indicates a client disconnect, everything else is an error */
      if (errno == EPIPE)
      {
        cinfo->socketerr = 2; /* Indicate an orderly shutdown */
      }
      else
      {
        /* Create a limited, printable buffer for the diagnostic message */
        char pbuffer[100];
        char *cp;
        int maxlength = (buflen[idx] < sizeof (pbuffer)) ? buflen[idx] : sizeof (pbuffer);

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

        lprintf (0, "[%s] SendData(): Error sending '%s': %s", cinfo->hostname,
                 pbuffer, strerror (errno));
        cinfo->socketerr = 1;
      }

      return -1;
    }
  }

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = HPnow ();

  /* Restore original socket flags */
  if (fcntl (cinfo->socket, F_SETFL, sockflags) == -1)
  {
    lprintf (0, "[%s] SendData(): Error setting non-blocking flag: %s",
             cinfo->hostname, strerror (errno));
    cinfo->socketerr = 1;
    return -1;
  }

  return 0;
} /* End of SendDataMB() */

/***********************************************************************
 * RecvData:
 *
 * Read buflen characters from a socket and place them into buffer.
 * This routine can handle fragmented receives and will continue
 * collecting data until the buffer is full.
 *
 * The buffer provided must already be allocated and have enough space
 * for buflen bytes.
 *
 * Return number of characters read on success, -1 on connection
 * shutdown and -2 on error.
 ***********************************************************************/
int
RecvData (ClientInfo *cinfo, char *buffer, size_t buflen)
{
  int nrecv;
  int nread = 0;
  char *bptr = buffer;

  fd_set readset;
  struct timeval timeout;
  int selret;

  /* Recv until buflen bytes have been read */
  while (nread < buflen)
  {
    if ((nrecv = recv (cinfo->socket, bptr, buflen - nread, 0)) < 0)
    {
      /* The only acceptable error is EAGAIN (no data on non-blocking) */
      if (nrecv == -1 && errno != EAGAIN)
      {
        lprintf (0, "[%s] Error recving data from client: %s",
                 cinfo->hostname, strerror (errno));
        return -2;
      }
      /* Throttle when no data for a non-blocking socket */
      else if (nrecv == -1 && errno == EAGAIN)
      {
        /* Configure the read descriptor set with only our client socket */
        FD_ZERO (&readset);
        FD_SET (cinfo->socket, &readset);

        /* Timeout 10 seconds */
        timeout.tv_sec = 10;
        timeout.tv_usec = 0;

        selret = select (cinfo->socket + 1, &readset, NULL, NULL, &timeout);

        if (selret == 0)
        {
          lprintf (0, "[%s] Timeout receiving data", cinfo->hostname);
          return -2;
        }
        else if (selret == -1 && errno != EINTR)
        {
          lprintf (0, "[%s] Error with select: %s", cinfo->hostname, strerror (errno));
          return -2;
        }
      }
    }

    /* Peer completed an orderly shutdown */
    if (nrecv == 0)
      return -1;

    /* Update recv pointer and byte count */
    if (nrecv > 0)
    {
      bptr += nrecv;
      nread += nrecv;
    }
  }

  /* Unmask received data if a mask was supplied as WebSocket */
  if (cinfo->wsmask.one != 0)
  {
    bptr = cinfo->recvbuf;
    for (; cinfo->wsmaskidx < nread; bptr++, cinfo->wsmaskidx++)
      *bptr = *bptr ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];
  }

  return nread;
} /* End of RecvData() */

/***********************************************************************
 * RecvCmd:
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
 * Return number of characters read on success, 0 if no data is
 * available, -1 on connection shutdown and -2 on error, the
 * ClientInfo.socketerr value is set on socket errors.
 ***********************************************************************/
int
RecvCmd (ClientInfo *cinfo)
{
  int nread = 0;
  int nreadtotal = 0;
  int nrecv;
  int pass;
  uint8_t nreq;
  char *bptr;

  fd_set readset;
  struct timeval timeout;
  int selret;

  if (!cinfo)
    return -2;

  /* First pass request 3 bytes: 2 sequence bytes + 1 length byte */
  nreq = 3;
  pass = 1;

  /* Sanity check the receive buffer length */
  if (cinfo->recvbuflen < 10)
  {
    lprintf (0, "[%s] Client receiving buffer is too small", cinfo->hostname);
    return -2;
  }

  /* Recv until the requested bytes are received or buffer length is reached */
  while (nread < (cinfo->recvbuflen - 1))
  {
    bptr = cinfo->recvbuf + nread;

    if ((nrecv = recv (cinfo->socket, bptr, nreq - nread, 0)) < 0)
    {
      /* The only acceptable error is EAGAIN (no data on non-blocking) */
      if (nrecv == -1 && errno != EAGAIN)
      {
        lprintf (0, "[%s] Error recv'ing data from client: %s",
                 cinfo->hostname, strerror (errno));
        cinfo->socketerr = 1;
        return -2;
      }
      /* Exit if no data is available and we haven't received anything yet */
      else if (nrecv == -1 && errno == EAGAIN && nreadtotal == 0)
      {
        return 0;
      }
      /* Throttle when no data for a non-blocking socket */
      else if (nrecv == -1 && errno == EAGAIN)
      {
        /* Configure the read descriptor set with only our client socket */
        FD_ZERO (&readset);
        FD_SET (cinfo->socket, &readset);

        /* Timeout 10 seconds */
        timeout.tv_sec = 10;
        timeout.tv_usec = 0;

        selret = select (cinfo->socket + 1, &readset, NULL, NULL, &timeout);

        if (selret == 0)
        {
          lprintf (0, "[%s] Timeout receiving DataLink command: %.*s",
                   cinfo->hostname, nread, cinfo->recvbuf);
          return -2;
        }
        else if (selret == -1 && errno != EINTR)
        {
          lprintf (0, "[%s] Error with select: %s",
                   cinfo->hostname, strerror (errno));
          return -2;
        }
      }
    }

    /* Peer completed an orderly shutdown */
    if (nrecv == 0)
    {
      cinfo->socketerr = 2; /* Indicate an orderly shutdown */
      return -1;
    }

    /* Update recv count and byte count */
    if (nrecv > 0)
    {
      nread += nrecv;
      nreadtotal += nrecv;
    }

    /* Determine read parameters from pre-header of 3 bytes: 'DL<size>' */
    if (pass == 1 && nread == 3)
    {
      /* Unmask received data if a mask was supplied as WebSocket */
      if (cinfo->wsmask.one != 0)
      {
        bptr = cinfo->recvbuf;
        for (; cinfo->wsmaskidx < nread; bptr++, cinfo->wsmaskidx++)
          *bptr = *bptr ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];
      }

      /* Sequence bytes of 'DL' identify DataLink */
      if (*(cinfo->recvbuf) == 'D' && *(cinfo->recvbuf + 1) == 'L')
      {
        /* Determine length of header body */
        nreq = (uint8_t) * (cinfo->recvbuf + 2);

        /* Sanity check the header size */
        if (nreq < 2 || nreq > (cinfo->recvbuflen - 1))
        {
          lprintf (0, "[%s] Pre-header indicates header size: %d",
                   cinfo->hostname, nreq);
          return -2;
        }

        /* Reset to read header body string */
        nread = 0;
        pass = 2;
      }
      else
      {
        lprintf (2, "[%s] Error verifying DataLink sequence bytes (%c%c) or HTTP",
                 cinfo->hostname, *(cinfo->recvbuf), *(cinfo->recvbuf + 1));
        return -2;
      }
    }

    /* Trap door if the requested number of bytes are received */
    else if (nread == nreq)
    {
      break;
    }
  }

  /* Unmask received data if a mask was supplied as WebSocket */
  if (cinfo->wsmask.one != 0)
  {
    bptr = cinfo->recvbuf;
    for (; cinfo->wsmaskidx < nread; bptr++, cinfo->wsmaskidx++)
      *bptr = *bptr ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];
  }

  /* Make sure buffer is NULL terminated. The command string is
   * allowed to be <= (cinfo->recvbuflen - 1), so this should be safe. */
  *(cinfo->recvbuf + nread) = '\0';

  return nreadtotal;
} /* End of RecvCmd() */

/***********************************************************************
 * RecvLine:
 *
 * Read characters from a socket until '\r' (carriage return) is
 * found, followed by an optional '\n' (newline) or the maximum buffer
 * length is reached and place them into the client's receive buffer.
 * The resulting string in buffer will always be NULL terminated.
 *
 * This routine can handle fragmented receives after some data has
 * been read.  If no data has been read and no data is available from
 * the socket this routine will return immediately.
 *
 * Return number of characters read on success, 0 if no data is
 * available, -1 on connection shutdown and -2 on error.
 ***********************************************************************/
int
RecvLine (ClientInfo *cinfo)
{
  char *bptr = NULL;
  char peek;
  int nread = 0;
  int nrecv;

  fd_set readset;
  struct timeval timeout;
  int selret;

  if (!cinfo)
    return -2;

  /* Buffer pointer tracks next input location */
  bptr = cinfo->recvbuf;

  if (!bptr)
    return -2;

  /* Recv a character at a time until \r or buflen is reached */
  while (nread < cinfo->recvbuflen)
  {
    if ((nrecv = recv (cinfo->socket, bptr, 1, 0)) < 0)
    {
      /* The only acceptable error is EAGAIN (no data on non-blocking) */
      if (nrecv == -1 && errno != EAGAIN)
      {
        lprintf (0, "[%s] Error recv'ing data from client: %s",
                 cinfo->hostname, strerror (errno));
        return -2;
      }
      /* Exit if no data is available and we haven't received anything yet */
      else if (nrecv == -1 && errno == EAGAIN && bptr == cinfo->recvbuf)
      {
        return 0;
      }
      /* Throttle when no data for a non-blocking socket */
      else if (nrecv == -1 && errno == EAGAIN)
      {
        /* Configure the read descriptor set with only our client socket */
        FD_ZERO (&readset);
        FD_SET (cinfo->socket, &readset);

        /* Timeout 10 seconds */
        timeout.tv_sec = 10;
        timeout.tv_usec = 0;

        selret = select (cinfo->socket + 1, &readset, NULL, NULL, &timeout);

        if (selret == 0)
        {
          lprintf (0, "[%s] Timeout receiving line", cinfo->hostname);
          return -2;
        }
        else if (selret == -1 && errno != EINTR)
        {
          lprintf (0, "[%s] Error with select: %s",
                   cinfo->hostname, strerror (errno));
          return -2;
        }
      }
    }

    /* Peer completed an orderly shutdown */
    if (nrecv == 0)
    {
      return -1;
    }

    if (nrecv > 0)
    {
      /* Unmask received data (payload) if a mask was supplied as WebSocket */
      if (cinfo->wsmask.one != 0)
      {
        *bptr = *bptr ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];
        cinfo->wsmaskidx++;
      }

      /* If '\r' is received the line is complete */
      if (*bptr == '\r')
      {
        /* Check for optional '\n' (newline) and consume it if present */
        if ((nrecv = recv (cinfo->socket, &peek, 1, MSG_PEEK)) == 1)
        {
          /* Unmask received data (payload) if a mask was supplied as WebSocket */
          if (cinfo->wsmask.one != 0)
          {
            peek = peek ^ cinfo->wsmask.four[cinfo->wsmaskidx % 4];
            cinfo->wsmaskidx++;
          }

          if (peek == '\n')
            recv (cinfo->socket, &peek, 1, 0);
        }

        break;
      }

      nread++;
      bptr++;
    }

    /* Check for a full buffer */
    if (nread >= cinfo->recvbuflen)
    {
      lprintf (0, "[%s] Received data too long for line, max %d bytes",
               cinfo->hostname, cinfo->recvbuflen);
      return -2;
    }
  }

  /* Make sure buffer is NULL terminated */
  *bptr = '\0';

  return nread;
} /* End of RecvLine() */

/***************************************************************************
 * GenProtocolString:
 *
 * Generate a string containing the names of the protocols specified
 * by the protocols flag.
 *
 * Return length of string in protocolstr on success or NULL for error.
 ***************************************************************************/
int
GenProtocolString (uint8_t protocols, char *protocolstr, size_t maxlength)
{
  int length;

  length = snprintf (protocolstr, maxlength,
                     "%s%s%s",
                     (protocols & PROTO_DATALINK) ? "DataLink " : "",
                     (protocols & PROTO_SEEDLINK) ? "SeedLink " : "",
                     (protocols & PROTO_HTTP) ? "HTTP " : "");

  if (length < maxlength && protocolstr[length - 1] == ' ')
    protocolstr[length - 1] = '\0';

  return (length > maxlength) ? maxlength - 1 : length;
} /* End of GenProtocolString() */

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
  StreamNode *stream = 0;

  /* Generate key */
  key = FVNhash64 (streamid);

  /* Search for a matching entry */
  if ((rbnode = RBFind (tree, &key)))
  {
    stream = (StreamNode *)rbnode->data;
    *new = 0;
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
    strncpy (stream->streamid, streamid, sizeof (stream->streamid));
    stream->txpackets = 0;
    stream->txbytes = 0;
    stream->rxpackets = 0;
    stream->rxbytes = 0;
    stream->endtimereached = 0;

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
AddToString (char **string, char *add, char *delim, int where, int maxlen)
{
  int length;
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

/**************************************************************************
 * dlclient.c
 *
 * DataLink client thread specific routines.
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
 * Copyright (C) 2020:
 * @author Chad Trabant, IRIS Data Management Center
 **************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include <libmseed.h>
#include <mxml.h>

#include "clients.h"
#include "dlclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "mseedscan.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo);
static int HandleWrite (ClientInfo *cinfo);
static int HandleRead (ClientInfo *cinfo);
static int HandleInfo (ClientInfo *cinfo, int socket);
static int SendPacket (ClientInfo *cinfo, char *header, char *data,
                       int64_t value, int addvalue, int addsize);
static int SendRingPacket (ClientInfo *cinfo);
static int SelectedStreams (RingParams *ringparams, RingReader *reader);

/***********************************************************************
 * DLHandleCmd:
 *
 * Handle DataLink command, which is expected to be in the
 * ClientInfo.recvbuf buffer.
 *
 * Returns zero on success, negative value on error.  On error the
 * client should be disconnected.
 ***********************************************************************/
int
DLHandleCmd (ClientInfo *cinfo)
{
  if (!cinfo)
    return -1;

  /* Determine if this is a data submission and handle */
  if (!strncmp (cinfo->recvbuf, "WRITE", 5))
  {
    /* Check for write permission */
    if (!cinfo->writeperm)
    {
      lprintf (1, "[%s] Data packet received from client without write permission",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Write permission not granted, no soup for you!", 0, 1, 1);
      return -1;
    }
    /* Any errors from HandleWrite are fatal */
    else if (HandleWrite (cinfo))
    {
      return -1;
    }
  }

  /* Determine if this is an INFO request and handle */
  else if (!strncmp (cinfo->recvbuf, "INFO", 4))
  {
    /* Any errors from HandleInfo are fatal */
    if (HandleInfo (cinfo, cinfo->socket))
    {
      return -1;
    }
  }

  /* Determine if this is a specific read request and handle */
  else if (!strncmp (cinfo->recvbuf, "READ", 4))
  {
    cinfo->state = STATE_COMMAND;

    /* Any errors from HandleRead are fatal */
    if (HandleRead (cinfo))
    {
      return -1;
    }
  }

  /* Determine if this is a request to start STREAMing and set state */
  else if (!strncmp (cinfo->recvbuf, "STREAM", 6))
  {
    /* Set read position to next packet if position not set */
    if (cinfo->reader->pktid == 0)
    {
      cinfo->reader->pktid = RINGNEXT;
    }

    cinfo->state = STATE_STREAM;
  }

  /* Determine if this is a request to end STREAMing and set state */
  else if (!strncmp (cinfo->recvbuf, "ENDSTREAM", 9))
  {
    /* Send ENDSTREAM */
    if (SendPacket (cinfo, "ENDSTREAM", NULL, 0, 0, 0))
    {
      return -1;
    }

    cinfo->state = STATE_COMMAND;
  }

  /* Otherwise a negotiation command */
  else
  {
    /* If this is not an ID request, set to a non-streaming state */
    if (strncmp (cinfo->recvbuf, "ID", 2))
      cinfo->state = STATE_COMMAND;

    /* Any errors from HandleNegotiation are fatal */
    if (HandleNegotiation (cinfo))
    {
      return -1;
    }
  }

  return 0;
} /* End of DLHandleCmd() */

/***********************************************************************
 * DLStreamPackets:
 *
 * Send selected ring packets to DataLink client.
 *
 * Returns packet size sent on success, zero when no packet sent,
 * negative value on error.  On error the client should disconnected.
 ***********************************************************************/
int
DLStreamPackets (ClientInfo *cinfo)
{
  int64_t readid;

  if (!cinfo)
    return -1;

  /* Read next packet from ring */
  readid = RingReadNext (cinfo->reader, &cinfo->packet, cinfo->packetdata);

  if (readid < 0)
  {
    lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
    return -1;
  }
  else if (readid > 0)
  {
    lprintf (3, "[%s] Read %s (%u bytes) packet ID %" PRId64 " from ring",
             cinfo->hostname, cinfo->packet.streamid,
             cinfo->packet.datasize, cinfo->packet.pktid);

    /* Send packet to client */
    if (SendRingPacket (cinfo))
    {
      if (cinfo->socketerr != 2)
        lprintf (1, "[%s] Error sending packet to client", cinfo->hostname);

      return -1;
    }

    /* Socket errors are fatal */
    if (cinfo->socketerr)
      return -1;
  }
  /* Otherwise there was no next packet */
  else
  {
    return 0;
  }

  return (readid) ? cinfo->packet.datasize : 0;
} /* End of DLStreamPackets() */

/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation commands implementing server-side DataLink
 * protocol, updating the connection configuration accordingly.
 *
 * DataLink commands handled:
 * ID
 * POSITION SET pktid [pkttime]
 * POSITION AFTER datatime
 * MATCH size|<match pattern of length size>
 * REJECT size|<match pattern of length size>
 *
 * All commands handled by this function will return the resulting
 * status to the client.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (ClientInfo *cinfo)
{
  char sendbuffer[255];
  int size;
  int fields;
  int selected;

  char OKGO = 1;
  char junk;

  /* ID - Return server ID, version and capability flags */
  if (!strncasecmp (cinfo->recvbuf, "ID", 2))
  {
    /* Parse client ID from command if included
     * Everything after "ID " is the client ID */
    if (strlen (cinfo->recvbuf) > 3)
    {
      strncpy (cinfo->clientid, cinfo->recvbuf + 3, sizeof (cinfo->clientid) - 1);
      *(cinfo->clientid + sizeof(cinfo->clientid) - 1) = '\0';
      lprintf (2, "[%s] Received ID (%s)", cinfo->hostname, cinfo->clientid);
    }
    else
    {
      lprintf (2, "[%s] Received ID", cinfo->hostname);
    }

    /* Create server version and capability flags string (DLCAPSFLAGS + WRITE if permission) */
    snprintf (sendbuffer, sizeof (sendbuffer),
              "ID DataLink " VERSION " :: %s PACKETSIZE:%lu%s", DLCAPFLAGS,
              (unsigned long int)(cinfo->ringparams->pktsize - sizeof (RingPacket)),
              (cinfo->writeperm) ? " WRITE" : "");

    /* Send the server ID string */
    if (SendPacket (cinfo, sendbuffer, NULL, 0, 0, 0))
      return -1;
  }

  /* POSITION <SET|AFTER> value [time]\r\n - Set ring reading position */
  else if (!strncasecmp (cinfo->recvbuf, "POSITION", 8))
  {
    char subcmd[10];
    char value[30];
    char subvalue[30];
    int64_t pktid = 0;
    hptime_t hptime;

    OKGO = 1;

    /* Parse sub-command and value from request */
    fields = sscanf (cinfo->recvbuf, "%*s %10s %30s %30s %c",
                     subcmd, value, subvalue, &junk);

    /* Make sure the subcommand, value and subvalue fields are terminated */
    subcmd[9] = '\0';
    value[29] = '\0';
    subvalue[29] = '\0';

    /* Make sure we got a single pattern or no pattern */
    if (fields < 2 || fields > 3)
    {
      if (SendPacket (cinfo, "ERROR", "POSITION requires 2 or 3 arguments", 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else
    {
      /* Process SET positioning */
      if (!strncmp (subcmd, "SET", 3))
      {
        /* Process SET <pktid> [time] */
        if (IsAllDigits (value))
        {
          pktid = strtoll (value, NULL, 10);
          hptime = (fields == 3) ? strtoll (subvalue, NULL, 10) : HPTERROR;
        }
        /* Process SET EARLIEST */
        else if (!strncmp (value, "EARLIEST", 8))
        {
          pktid = RINGEARLIEST;
          hptime = HPTERROR;
        }
        /* Process SET LATEST */
        else if (!strncmp (value, "LATEST", 6))
        {
          pktid = RINGLATEST;
          hptime = HPTERROR;
        }
        else
        {
          lprintf (0, "[%s] Error with POSITION SET value: %s",
                   cinfo->hostname, value);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION SET value", 0, 1, 1))
            return -1;
          OKGO = 0;
        }

        /* If no errors with the set value do the positioning */
        if (OKGO)
        {
          if ((pktid = RingPosition (cinfo->reader, pktid, hptime)) <= 0)
          {
            if (pktid == 0)
            {
              if (SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1))
                return -1;
            }
            else
            {
              lprintf (0, "[%s] Error with RingPosition (pktid: %" PRId64 ", hptime: %" PRId64 ")",
                       cinfo->hostname, pktid, hptime);
              if (SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1))
                return -1;
            }
          }
          else
          {
            snprintf (sendbuffer, sizeof (sendbuffer),
                      "Positioned to packet ID %" PRId64, pktid);
            if (SendPacket (cinfo, "OK", sendbuffer, pktid, 1, 1))
              return -1;
          }
        }
      }
      /* Process AFTER <time> positioning */
      else if (!strncmp (subcmd, "AFTER", 5))
      {
        if ((hptime = strtoll (value, NULL, 10)) == 0 && errno == EINVAL)
        {
          lprintf (0, "[%s] Error parsing POSITION AFTER time: %s",
                   cinfo->hostname, value);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION AFTER time", 0, 1, 1))
            return -1;
        }
        else
        {
          /* Position ring according to start time, use reverse search if limited */
          if (cinfo->timewinlimit == 1.0)
          {
            pktid = RingAfter (cinfo->reader, hptime, 1);
          }
          else if (cinfo->timewinlimit < 1.0)
          {
            int64_t pktlimit = (int64_t) (cinfo->timewinlimit * cinfo->ringparams->maxpackets);

            pktid = RingAfterRev (cinfo->reader, hptime, pktlimit, 1);
          }
          else
          {
            lprintf (0, "Time window search limit is invalid: %f", cinfo->timewinlimit);
            SendPacket (cinfo, "ERROR", "time window search limit is invalid", 0, 1, 1);
            return -1;
          }

          if (pktid == 0)
          {
            if (SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1))
              return -1;
          }
          else if (pktid < 0)
          {
            lprintf (0, "[%s] Error with RingAfter[Rev] (hptime: %" PRId64 ")",
                     cinfo->hostname, hptime);
            if (SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1))
              return -1;
          }
        }

        snprintf (sendbuffer, sizeof (sendbuffer), "Positioned to packet ID %" PRId64, pktid);
        if (SendPacket (cinfo, "OK", sendbuffer, pktid, 1, 1))
          return -1;
      }
      else
      {
        lprintf (0, "[%s] Unsupported POSITION subcommand: %s", cinfo->hostname, subcmd);
        if (SendPacket (cinfo, "ERROR", "Unsupported POSITION subcommand", 0, 1, 1))
          return -1;
      }
    }
  } /* End of POSITION */

  /* MATCH size\r\n[pattern] - Provide regex to match streamids */
  else if (!strncasecmp (cinfo->recvbuf, "MATCH", 5))
  {
    OKGO = 1;

    /* Parse size from request */
    fields = sscanf (cinfo->recvbuf, "%*s %d %c", &size, &junk);

    /* Make sure we got a single pattern or no pattern */
    if (fields > 1)
    {
      if (SendPacket (cinfo, "ERROR", "MATCH requires a single argument", 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    /* Remove current match if no pattern supplied */
    else if (fields <= 0)
    {
      if (cinfo->matchstr)
        free (cinfo->matchstr);
      cinfo->matchstr = 0;
      RingMatch (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match",
                selected);
      if (SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1))
        return -1;
    }
    else if (size > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] match expression too large (%d)", cinfo->hostname, size);

      snprintf (sendbuffer, sizeof (sendbuffer), "match expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else
    {
      if (cinfo->matchstr)
        free (cinfo->matchstr);

      /* Read regex of size bytes from socket */
      if (!(cinfo->matchstr = (char *)malloc (size + 1)))
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        return -1;
      }

      if (RecvData (cinfo, cinfo->matchstr, size) < 0)
      {
        lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
        return -1;
      }

      /* Make sure buffer is a terminated string */
      cinfo->matchstr[size] = '\0';

      /* Compile match expression */
      if (RingMatch (cinfo->reader, cinfo->matchstr))
      {
        lprintf (0, "[%s] Error with match expression", cinfo->hostname);

        if (SendPacket (cinfo, "ERROR", "Error with match expression", 0, 1, 1))
          return -1;
      }
      else
      {
        selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match",
                  selected);
        if (SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1))
          return -1;
      }
    }
  } /* End of MATCH */

  /* REJECT size\r\n[pattern] - Provide regex to reject streamids */
  else if (OKGO && !strncasecmp (cinfo->recvbuf, "REJECT", 6))
  {
    OKGO = 1;

    /* Parse size from request */
    fields = sscanf (cinfo->recvbuf, "%*s %d %c", &size, &junk);

    /* Make sure we got a single pattern or no pattern */
    if (fields > 1)
    {
      if (SendPacket (cinfo, "ERROR", "REJECT requires a single argument", 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    /* Remove current reject if no pattern supplied */
    else if (fields <= 0)
    {
      if (cinfo->rejectstr)
        free (cinfo->rejectstr);
      cinfo->rejectstr = 0;
      RingReject (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject",
                selected);
      if (SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1))
        return -1;
    }
    else if (size > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] reject expression too large (%d)", cinfo->hostname, size);

      snprintf (sendbuffer, sizeof (sendbuffer), "reject expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else
    {
      if (cinfo->rejectstr)
        free (cinfo->rejectstr);

      /* Read regex of size bytes from socket */
      if (!(cinfo->rejectstr = (char *)malloc (size + 1)))
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        return -1;
      }

      if (RecvData (cinfo, cinfo->rejectstr, size) < 0)
      {
        lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
        return -1;
      }

      /* Make sure buffer is a terminated string */
      cinfo->rejectstr[size] = '\0';

      /* Compile reject expression */
      if (RingReject (cinfo->reader, cinfo->rejectstr))
      {
        lprintf (0, "[%s] Error with reject expression", cinfo->hostname);

        if (SendPacket (cinfo, "ERROR", "Error with reject expression", 0, 1, 1))
          return -1;
      }
      else
      {
        selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject",
                  selected);
        if (SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1))
          return -1;
      }
    }
  } /* End of REJECT */

  /* BYE - End connection */
  else if (!strncasecmp (cinfo->recvbuf, "BYE", 3))
  {
    return -1;
  }

  /* Unrecognized command */
  else
  {
    lprintf (1, "[%s] Unrecognized command: %.10s",
             cinfo->hostname, cinfo->recvbuf);

    if (SendPacket (cinfo, "ERROR", "Unrecognized command", 0, 1, 1))
      return -1;
  }

  return 0;
} /* End of HandleNegotiation */

/***************************************************************************
 * HandleWrite:
 *
 * Handle DataLink WRITE request.
 *
 * The command syntax is: "WRITE <streamid> <hpdatastart> <hpdataend> <flags> <datasize>"
 *
 * The stream ID is used verbatim by the ringserver.  The hpdatastart
 * and hpdataend are high-precision time stamps (dltime_t).  The data
 * size is the size in bytes of the data portion following the header.
 * The flags are single character indicators and interpreted the
 * following way:
 *
 * flags:
 * 'N' = no acknowledgement is requested
 * 'A' = acknowledgement is requested, server will send a reply
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleWrite (ClientInfo *cinfo)
{
  StreamNode *stream;
  char replystr[200];
  char streamid[100];
  char flags[100];
  int nread;
  int newstream = 0;
  int rv;

  MSRecord *msr = 0;
  char *type;

  if (!cinfo)
    return -1;

  /* Parse command parameters: WRITE <streamid> <datastart> <dataend> <flags> <datasize> */
  if (sscanf (cinfo->recvbuf, "%*s %100s %" PRId64 " %" PRId64 " %100s %u",
              streamid, &(cinfo->packet.datastart), &(cinfo->packet.dataend),
              flags, &(cinfo->packet.datasize)) != 5)
  {
    lprintf (1, "[%s] Error parsing WRITE parameters: %.100s",
             cinfo->hostname, cinfo->recvbuf);

    SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

    return -1;
  }

  /* Check that client is allowed to write this stream ID if limit is present */
  if (cinfo->reader->limit)
  {
    if (pcre_exec (cinfo->reader->limit, cinfo->reader->limit_extra, streamid, strlen (streamid), 0, 0, NULL, 0))
    {
      lprintf (1, "[%s] Error, permission denied for WRITE of stream ID: %s",
               cinfo->hostname, streamid);

      snprintf (replystr, sizeof (replystr), "Error, permission denied for WRITE of stream ID: %s", streamid);
      SendPacket (cinfo, "ERROR", replystr, 0, 1, 1);

      return -1;
    }
  }

  /* Copy the stream ID */
  memcpy (cinfo->packet.streamid, streamid, sizeof (cinfo->packet.streamid));

  /* Make sure the streamid is terminated */
  cinfo->packet.streamid[sizeof (cinfo->packet.streamid) - 1] = '\0';

  /* Make sure this packet data would fit into the ring */
  if (cinfo->packet.datasize > cinfo->ringparams->pktsize)
  {
    lprintf (1, "[%s] Submitted packet size (%d) is greater than ring packet size (%d)",
             cinfo->hostname, cinfo->packet.datasize, cinfo->ringparams->pktsize);

    snprintf (replystr, sizeof (replystr), "Packet size (%d) is too large for ring, maximum is %d bytes",
              cinfo->packet.datasize, cinfo->ringparams->pktsize);
    SendPacket (cinfo, "ERROR", replystr, 0, 1, 1);

    return -1;
  }

  /* Recv packet data from socket */
  nread = RecvData (cinfo, cinfo->packetdata, cinfo->packet.datasize);

  if (nread < 0)
    return -1;

  /* Write received miniSEED to a disk archive if configured */
  if (cinfo->mswrite)
  {
    char filename[100];
    char *fn;

    if ((type = strrchr (streamid, '/')))
    {
      if (!strncmp (++type, "MSEED", 5))
      {
        /* Parse the miniSEED record header */
        if (msr_unpack (cinfo->packetdata, cinfo->packet.datasize, &msr, 0, 0) == MS_NOERROR)
        {
          /* Check for file name in streamid: "filename::streamid/MSEED" */
          if ((fn = strstr (streamid, "::")))
          {
            strncpy (filename, streamid, (fn - streamid));
            filename[(fn - streamid)] = '\0';
            fn = filename;
          }

          /* Write miniSEED record to disk */
          if (ds_streamproc (cinfo->mswrite, msr, fn, cinfo->hostname))
          {
            lprintf (1, "[%s] Error writing miniSEED to disk", cinfo->hostname);

            SendPacket (cinfo, "ERROR", "Error writing miniSEED to disk", 0, 1, 1);

            return -1;
          }
        }

        if (msr)
          msr_free (&msr);
      }
    }
  }

  /* Add the packet to the ring */
  if ((rv = RingWrite (cinfo->ringparams, &cinfo->packet, cinfo->packetdata, cinfo->packet.datasize)))
  {
    if (rv == -2)
      lprintf (1, "[%s] Error with RingWrite, corrupt ring, shutdown signalled", cinfo->hostname);
    else
      lprintf (1, "[%s] Error with RingWrite", cinfo->hostname);

    SendPacket (cinfo, "ERROR", "Error adding packet to ring", 0, 1, 1);

    /* Set the shutdown signal if ring corruption was detected */
    if (rv == -2)
      shutdownsig = 1;

    return -1;
  }

  /* Get (creating if needed) the StreamNode for this streamid */
  if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                               cinfo->packet.streamid, &newstream)) == 0)
  {
    lprintf (0, "[%s] Error with GetStreamNode for %s",
             cinfo->hostname, cinfo->packet.streamid);
    return -1;
  }

  if (newstream)
  {
    lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, cinfo->packet.streamid);
    cinfo->streamscount++;
  }

  /* Update StreamNode packet and byte counts */
  pthread_mutex_lock (&(cinfo->streams_lock));
  stream->rxpackets++;
  stream->rxbytes += cinfo->packet.datasize;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Update client receive counts */
  cinfo->rxpackets[0]++;
  cinfo->rxbytes[0] += cinfo->packet.datasize;

  /* Send acknowledgement if requested (flags contain 'A') */
  if (strchr (flags, 'A'))
  {
    if (SendPacket (cinfo, "OK", NULL, cinfo->packet.pktid, 1, 1))
      return -1;
  }

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleWrite */

/***************************************************************************
 * HandleRead:
 *
 * Handle DataLink READ request.
 *
 * The command syntax is: "READ <pktid>"
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleRead (ClientInfo *cinfo)
{
  int64_t reqid = 0;
  int64_t readid = 0;
  char replystr[100];

  if (!cinfo)
    return -1;

  /* Parse command parameters: READ <pktid> */
  if (sscanf (cinfo->recvbuf, "%*s %" PRId64, &reqid) != 1)
  {
    lprintf (1, "[%s] Error parsing READ parameters: %.100s",
             cinfo->hostname, cinfo->recvbuf);

    if (SendPacket (cinfo, "ERROR", "Error parsing READ command parameters", 0, 1, 1))
      return -1;
  }

  /* Read the packet from the ring */
  if ((readid = RingRead (cinfo->reader, reqid, &cinfo->packet, cinfo->packetdata)) < 0)
  {
    lprintf (1, "[%s] Error with RingRead", cinfo->hostname);

    if (SendPacket (cinfo, "ERROR", "Error reading packet from ring", 0, 1, 1))
      return -1;
  }

  /* Return packet not found error message if needed */
  if (readid == 0)
  {
    snprintf (replystr, sizeof (replystr), "Packet %" PRId64 " not found in ring", reqid);
    if (SendPacket (cinfo, "ERROR", replystr, 0, 1, 1))
      return -1;
  }
  /* Send packet to client */
  else if (SendRingPacket (cinfo))
  {
    if (cinfo->socketerr != 2)
      lprintf (1, "[%s] Error sending packet to client", cinfo->hostname);
  }

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleRead() */

/***************************************************************************
 * HandleInfo:
 *
 * Handle DataLink INFO request, returning the appropriate XML response.
 *
 * DataLink INFO requests handled:
 * STATUS
 * STREAMS
 * CONNECTIONS
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo (ClientInfo *cinfo, int socket)
{
  mxml_node_t *xmldoc = 0;
  mxml_node_t *status;
  char string[200];
  char *xmlstr = 0;
  int xmllength;
  char *type = 0;
  char *matchexpr = 0;
  char errflag = 0;

  if (!cinfo)
    return -1;

  if (!strncasecmp (cinfo->recvbuf, "INFO", 4))
  {
    /* Set level pointer to start of type identifier */
    type = cinfo->recvbuf + 4;

    /* Skip any spaces between INFO and type identifier */
    while (*type == ' ')
      type++;

    /* Skip type characters then spaces to get to match */
    matchexpr = type;
    while (*matchexpr != ' ' && *matchexpr)
      matchexpr++;
    while (*matchexpr == ' ')
      matchexpr++;
  }
  else
  {
    lprintf (0, "[%s] HandleInfo cannot detect INFO", cinfo->hostname);
    return -1;
  }

  /* Initialize the XML response */
  if (!(xmldoc = mxmlNewElement (MXML_NO_PARENT, "DataLink")))
  {
    lprintf (0, "[%s] Error initializing XML response", cinfo->hostname);
    return -1;
  }

  /* All INFO responses contain these attributes in the root DataLink element */
  mxmlElementSetAttr (xmldoc, "Version", VERSION);
  mxmlElementSetAttr (xmldoc, "ServerID", serverid);
  mxmlElementSetAttrf (xmldoc, "Capabilities", "%s PACKETSIZE:%lu%s", DLCAPFLAGS,
                       (unsigned long int)(cinfo->ringparams->pktsize - sizeof (RingPacket)),
                       (cinfo->writeperm) ? " WRITE" : "");

  /* All INFO responses contain the "Status" element */
  if (!(status = mxmlNewElement (xmldoc, "Status")))
  {
    lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
    errflag = 1;
  }
  else
  {
    /* Convert server start time to YYYY-MM-DD HH:MM:SS */
    ms_hptime2mdtimestr (serverstarttime, string, 0);
    mxmlElementSetAttr (status, "StartTime", string);
    mxmlElementSetAttrf (status, "RingVersion", "%u", (unsigned int)cinfo->ringparams->version);
    mxmlElementSetAttrf (status, "RingSize", "%" PRIu64, cinfo->ringparams->ringsize);
    mxmlElementSetAttrf (status, "PacketSize", "%lu",
                         (unsigned long int)(cinfo->ringparams->pktsize - sizeof (RingPacket)));
    mxmlElementSetAttrf (status, "MaximumPacketID", "%" PRId64, cinfo->ringparams->maxpktid);
    mxmlElementSetAttrf (status, "MaximumPackets", "%" PRId64, cinfo->ringparams->maxpackets);
    mxmlElementSetAttrf (status, "MemoryMappedRing", "%s", (cinfo->ringparams->mmapflag) ? "TRUE" : "FALSE");
    mxmlElementSetAttrf (status, "VolatileRing", "%s", (cinfo->ringparams->volatileflag) ? "TRUE" : "FALSE");
    mxmlElementSetAttrf (status, "TotalConnections", "%d", clientcount);
    mxmlElementSetAttrf (status, "TotalStreams", "%d", cinfo->ringparams->streamcount);
    mxmlElementSetAttrf (status, "TXPacketRate", "%.1f", cinfo->ringparams->txpacketrate);
    mxmlElementSetAttrf (status, "TXByteRate", "%.1f", cinfo->ringparams->txbyterate);
    mxmlElementSetAttrf (status, "RXPacketRate", "%.1f", cinfo->ringparams->rxpacketrate);
    mxmlElementSetAttrf (status, "RXByteRate", "%.1f", cinfo->ringparams->rxbyterate);
    mxmlElementSetAttrf (status, "EarliestPacketID", "%" PRId64, cinfo->ringparams->earliestid);
    ms_hptime2mdtimestr (cinfo->ringparams->earliestptime, string, 1);
    mxmlElementSetAttr (status, "EarliestPacketCreationTime",
                        (cinfo->ringparams->earliestptime != HPTERROR) ? string : "-");
    ms_hptime2mdtimestr (cinfo->ringparams->earliestdstime, string, 1);
    mxmlElementSetAttr (status, "EarliestPacketDataStartTime",
                        (cinfo->ringparams->earliestdstime != HPTERROR) ? string : "-");
    ms_hptime2mdtimestr (cinfo->ringparams->earliestdetime, string, 1);
    mxmlElementSetAttr (status, "EarliestPacketDataEndTime",
                        (cinfo->ringparams->earliestdetime != HPTERROR) ? string : "-");
    mxmlElementSetAttrf (status, "LatestPacketID", "%" PRId64, cinfo->ringparams->latestid);
    ms_hptime2mdtimestr (cinfo->ringparams->latestptime, string, 1);
    mxmlElementSetAttr (status, "LatestPacketCreationTime",
                        (cinfo->ringparams->latestptime != HPTERROR) ? string : "-");
    ms_hptime2mdtimestr (cinfo->ringparams->latestdstime, string, 1);
    mxmlElementSetAttr (status, "LatestPacketDataStartTime",
                        (cinfo->ringparams->latestdstime != HPTERROR) ? string : "-");
    ms_hptime2mdtimestr (cinfo->ringparams->latestdetime, string, 1);
    mxmlElementSetAttr (status, "LatestPacketDataEndTime",
                        (cinfo->ringparams->latestdetime != HPTERROR) ? string : "-");
  }

  /* Add contents to the XML structure depending on info request */
  if (!strncasecmp (type, "STATUS", 6))
  {
    mxml_node_t *stlist, *st;
    int totalcount = 0;
    struct sthread *loopstp;

    lprintf (1, "[%s] Received INFO STATUS request", cinfo->hostname);
    type = "INFO STATUS";

    /* Only add server threads if client is trusted */
    if (cinfo->trusted)
    {
      /* Create "ServerThreads" element */
      if (!(stlist = mxmlNewElement (xmldoc, "ServerThreads")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        errflag = 1;
      }

      /* Create a Thread element for each thread, lock thread list while looping */
      pthread_mutex_lock (&sthreads_lock);
      loopstp = sthreads;
      while (loopstp)
      {
        totalcount++;

        if (!(st = mxmlNewElement (stlist, "Thread")))
        {
          lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
          errflag = 1;
        }
        else
        {
          /* Add thread status flags to Thread element */
          string[0] = '\0';
          if (loopstp->td->td_flags & TDF_SPAWNING)
            strcat (string, " SPAWNING");
          if (loopstp->td->td_flags & TDF_ACTIVE)
            strcat (string, " ACTIVE");
          if (loopstp->td->td_flags & TDF_CLOSE)
            strcat (string, " CLOSE");
          if (loopstp->td->td_flags & TDF_CLOSING)
            strcat (string, " CLOSING");
          if (loopstp->td->td_flags & TDF_CLOSED)
            strcat (string, " CLOSED");
          mxmlElementSetAttr (st, "Flags", string);

          /* Determine server thread type and add specifics */
          if (loopstp->type == LISTEN_THREAD)
          {
            ListenPortParams *lpp = loopstp->params;
            char protocolstr[100];

            if (GenProtocolString (lpp->protocols, protocolstr, sizeof (protocolstr)) > 0)
              mxmlElementSetAttr (st, "Type", protocolstr);
            mxmlElementSetAttr (st, "Port", lpp->portstr);
          }
          else if (loopstp->type == MSEEDSCAN_THREAD)
          {
            MSScanInfo *mssinfo = loopstp->params;

            mxmlElementSetAttr (st, "Type", "miniSEED Scanner");
            mxmlElementSetAttr (st, "Directory", mssinfo->dirname);
            mxmlElementSetAttrf (st, "MaxRecursion", "%d", mssinfo->maxrecur);
            mxmlElementSetAttr (st, "StateFile", mssinfo->statefile);
            mxmlElementSetAttr (st, "Match", mssinfo->matchstr);
            mxmlElementSetAttr (st, "Reject", mssinfo->rejectstr);
            mxmlElementSetAttrf (st, "ScanTime", "%g", mssinfo->scantime);
            mxmlElementSetAttrf (st, "PacketRate", "%g", mssinfo->rxpacketrate);
            mxmlElementSetAttrf (st, "ByteRate", "%g", mssinfo->rxbyterate);
          }
          else
          {
            mxmlElementSetAttr (st, "Type", "Unknown Thread");
          }
        }

        loopstp = loopstp->next;
      }
      pthread_mutex_unlock (&sthreads_lock);

      /* Add thread count attribute to ServerThreads element */
      mxmlElementSetAttrf (stlist, "TotalServerThreads", "%d", totalcount);
    }
  } /* End of STATUS */
  else if (!strncasecmp (type, "STREAMS", 7))
  {
    mxml_node_t *streamlist, *stream;
    hptime_t hpnow;
    int selectedcount = 0;
    Stack *streams;
    RingStream *ringstream;

    lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);
    type = "INFO STREAMS";

    /* Create "StreamList" element and add attributes */
    if (!(streamlist = mxmlNewElement (xmldoc, "StreamList")))
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      errflag = 1;
    }

    /* Collect stream list */
    if ((streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)))
    {
      /* Get current time */
      hpnow = HPnow ();

      /* Create a "Stream" element for each stream */
      while ((ringstream = (RingStream *)StackPop (streams)))
      {
        if (!(stream = mxmlNewElement (streamlist, "Stream")))
        {
          lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
          errflag = 1;
        }
        else
        {
          mxmlElementSetAttr (stream, "Name", ringstream->streamid);
          mxmlElementSetAttrf (stream, "EarliestPacketID", "%" PRId64, ringstream->earliestid);
          ms_hptime2mdtimestr (ringstream->earliestdstime, string, 1);
          mxmlElementSetAttr (stream, "EarliestPacketDataStartTime", string);
          ms_hptime2mdtimestr (ringstream->earliestdetime, string, 1);
          mxmlElementSetAttr (stream, "EarliestPacketDataEndTime", string);
          mxmlElementSetAttrf (stream, "LatestPacketID", "%" PRId64, ringstream->latestid);
          ms_hptime2mdtimestr (ringstream->latestdstime, string, 1);
          mxmlElementSetAttr (stream, "LatestPacketDataStartTime", string);
          ms_hptime2mdtimestr (ringstream->latestdetime, string, 1);
          mxmlElementSetAttr (stream, "LatestPacketDataEndTime", string);

          /* DataLatency value is the difference between the current time and the time of last sample in seconds */
          mxmlElementSetAttrf (stream, "DataLatency", "%.1f", (double)MS_HPTIME2EPOCH ((hpnow - ringstream->latestdetime)));
        }

        free (ringstream);
        selectedcount++;
      }

      /* Cleanup stream stack */
      StackDestroy (streams, free);
    }
    else
    {
      lprintf (0, "[%s] Error generating Stack of streams", cinfo->hostname);
      errflag = 1;
    }

    /* Add stream count attributes to StreamList element */
    mxmlElementSetAttrf (streamlist, "TotalStreams", "%d", cinfo->ringparams->streamcount);
    mxmlElementSetAttrf (streamlist, "SelectedStreams", "%d", selectedcount);

  } /* End of STREAMS */
  else if (!strncasecmp (type, "CONNECTIONS", 11))
  {
    mxml_node_t *connlist, *conn;
    hptime_t hpnow;
    int selectedcount = 0;
    int totalcount = 0;
    struct cthread *loopctp;
    ClientInfo *tcinfo;
    char *conntype;
    pcre *match = 0;
    const char *errptr;
    int erroffset;

    /* Check for trusted flag, required to access this resource */
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] INFO CONNECTIONS request from un-trusted client",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Access to CONNECTIONS denied", 0, 1, 1);

      if (xmldoc)
        mxmlRelease (xmldoc);

      return -1;
    }

    lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);
    type = "INFO CONNECTIONS";

    /* Get current time */
    hpnow = HPnow ();

    /* Compile match expression supplied with request */
    if (matchexpr)
    {
      match = pcre_compile (matchexpr, 0, &errptr, &erroffset, NULL);
      if (errptr)
      {
        lprintf (0, "[%s] Error with pcre_compile: %s", cinfo->hostname, errptr);
        errflag = 1;
        matchexpr = 0;
      }
    }

    /* Create "ConnectionList" element */
    if (!(connlist = mxmlNewElement (xmldoc, "ConnectionList")))
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      errflag = 1;
    }

    /* Create a Connection element for each client, lock client list while looping */
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

      if (!(conn = mxmlNewElement (connlist, "Connection")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        errflag = 1;
      }
      else
      {
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

        mxmlElementSetAttr (conn, "Type", conntype);
        mxmlElementSetAttr (conn, "Host", tcinfo->hostname);
        mxmlElementSetAttr (conn, "IP", tcinfo->ipstr);
        mxmlElementSetAttr (conn, "Port", tcinfo->portstr);
        mxmlElementSetAttr (conn, "ClientID", tcinfo->clientid);
        ms_hptime2mdtimestr (tcinfo->conntime, string, 1);
        mxmlElementSetAttr (conn, "ConnectionTime", string);
        mxmlElementSetAttrf (conn, "Match", "%s", (tcinfo->matchstr) ? tcinfo->matchstr : "");
        mxmlElementSetAttrf (conn, "Reject", "%s", (tcinfo->rejectstr) ? tcinfo->rejectstr : "");
        mxmlElementSetAttrf (conn, "StreamCount", "%d", tcinfo->streamscount);
        mxmlElementSetAttrf (conn, "PacketID", "%" PRId64, tcinfo->reader->pktid);
        ms_hptime2mdtimestr (tcinfo->reader->pkttime, string, 1);
        mxmlElementSetAttr (conn, "PacketCreationTime",
                            (tcinfo->reader->pkttime != HPTERROR) ? string : "-");
        ms_hptime2mdtimestr (tcinfo->reader->datastart, string, 1);
        mxmlElementSetAttr (conn, "PacketDataStartTime",
                            (tcinfo->reader->datastart != HPTERROR) ? string : "-");
        ms_hptime2mdtimestr (tcinfo->reader->dataend, string, 1);
        mxmlElementSetAttr (conn, "PacketDataEndTime",
                            (tcinfo->reader->dataend != HPTERROR) ? string : "-");
        mxmlElementSetAttrf (conn, "TXPacketCount", "%" PRId64, tcinfo->txpackets[0]);
        mxmlElementSetAttrf (conn, "TXPacketRate", "%.1f", tcinfo->txpacketrate);
        mxmlElementSetAttrf (conn, "TXByteCount", "%" PRId64, tcinfo->txbytes[0]);
        mxmlElementSetAttrf (conn, "TXByteRate", "%.1f", tcinfo->txbyterate);
        mxmlElementSetAttrf (conn, "RXPacketCount", "%" PRId64, tcinfo->rxpackets[0]);
        mxmlElementSetAttrf (conn, "RXPacketRate", "%.1f", tcinfo->rxpacketrate);
        mxmlElementSetAttrf (conn, "RXByteCount", "%" PRId64, tcinfo->rxbytes[0]);
        mxmlElementSetAttrf (conn, "RXByteRate", "%.1f", tcinfo->rxbyterate);

        /* Latency value is the difference between the current time and the time of last packet exchange in seconds */
        mxmlElementSetAttrf (conn, "Latency", "%.1f", (double)MS_HPTIME2EPOCH ((hpnow - tcinfo->lastxchange)));

        if (tcinfo->reader->pktid <= 0)
          strncpy (string, "-", sizeof (string));
        else
          snprintf (string, sizeof (string), "%d", tcinfo->percentlag);

        mxmlElementSetAttr (conn, "PercentLag", string);

        selectedcount++;
      }

      loopctp = loopctp->next;
    }
    pthread_mutex_unlock (&cthreads_lock);

    /* Add client count attribute to ConnectionList element */
    mxmlElementSetAttrf (connlist, "TotalConnections", "%d", totalcount);
    mxmlElementSetAttrf (connlist, "SelectedConnections", "%d", selectedcount);

    /* Free compiled match expression */
    if (match)
      pcre_free (match);

  } /* End of CONNECTIONS */
  /* Unrecognized INFO request */
  else
  {
    lprintf (0, "[%s] Unrecognized INFO request type: %s", cinfo->hostname, type);
    snprintf (string, sizeof (string), "Unrecognized INFO request type: %s", type);
    SendPacket (cinfo, "ERROR", string, 0, 1, 1);
    errflag = 2;
  }

  /* Send ERROR to client if not already done */
  if (errflag == 1)
  {
    SendPacket (cinfo, "ERROR", "Error processing INFO request", 0, 1, 1);
  }
  /* Convert to XML string and send to client */
  else if (xmldoc && !errflag)
  {
    /* Do not wrap the output XML */
    mxmlSetWrapMargin (0);

    /* Convert to XML string */
    if (!(xmlstr = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK)))
    {
      lprintf (0, "[%s] Error with mxmlSaveAllocString()", cinfo->hostname);
      if (xmldoc)
        mxmlRelease (xmldoc);
      return -1;
    }

    /* Trim final newline character if present */
    xmllength = strlen (xmlstr);
    if (xmlstr[xmllength - 1] == '\n')
    {
      xmlstr[xmllength - 1] = '\0';
      xmllength--;
    }

    /* Send XML to client */
    if (SendPacket (cinfo, type, xmlstr, 0, 0, 1))
    {
      if (cinfo->socketerr != 2)
        lprintf (0, "[%s] Error sending INFO XML", cinfo->hostname);

      if (xmldoc)
        mxmlRelease (xmldoc);
      if (xmlstr)
        free (xmlstr);
      return -1;
    }
  }

  /* Free allocated memory */
  if (xmldoc)
    mxmlRelease (xmldoc);

  if (xmlstr)
    free (xmlstr);

  return (cinfo->socketerr || errflag) ? -1 : 0;
} /* End of HandleInfo */

/***************************************************************************
 * SendPacket:
 *
 * Create and send a packet from given header and packet data strings.
 * The header and packet strings must be NULL-terminated.  If the data
 * argument is NULL a header-only packet will be send.  If the
 * addvalue argument is true the value argument will be appended to
 * the header.  If the addsize argument is true the size of the packet
 * string will be appended to the header.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendPacket (ClientInfo *cinfo, char *header, char *data,
            int64_t value, int addvalue, int addsize)
{
  char *wirepacket = 0;
  char headerstr[255];
  int headerlen;
  int datalen;

  if (!cinfo || !header)
    return -1;

  /* Determine length of packet data string */
  datalen = (data) ? strlen (data) : 0;

  /* Add value and/or size of packet data to header */
  if (addvalue || addsize)
  {
    if (addvalue && addsize)
      snprintf (headerstr, sizeof (headerstr), "%s %" PRId64 " %u", header, value, datalen);
    else if (addvalue)
      snprintf (headerstr, sizeof (headerstr), "%s %" PRId64, header, value);
    else
      snprintf (headerstr, sizeof (headerstr), "%s %u", header, datalen);

    header = headerstr;
  }

  /* Determine length of header and sanity check it */
  headerlen = strlen (header);

  if (headerlen > 255)
  {
    lprintf (0, "[%s] SendPacket(): Header length is too large: %d",
             cinfo->hostname, headerlen);
    return -1;
  }

  /* Use the send buffer if large enough otherwise allocate memory for wire packet */
  if (cinfo->sendbuflen >= (3 + headerlen + datalen))
  {
    wirepacket = cinfo->sendbuf;
  }
  else
  {
    if (!(wirepacket = (char *)malloc (3 + headerlen + datalen)))
    {
      lprintf (0, "[%s] SendPacket(): Error allocating wire packet buffer",
               cinfo->hostname);
      return -1;
    }
  }

  /* Populate pre-header sequence of wire packet */
  wirepacket[0] = 'D';
  wirepacket[1] = 'L';
  wirepacket[2] = (uint8_t)headerlen;

  /* Copy header and packet data into wire packet */
  memcpy (&wirepacket[3], header, headerlen);

  if (data)
    memcpy (&wirepacket[3 + headerlen], data, datalen);

  /* Send complete wire packet */
  if (SendData (cinfo, wirepacket, (3 + headerlen + datalen)))
  {
    if (cinfo->socketerr != 2)
      lprintf (0, "[%s] SendPacket(): Error sending packet: %s",
               cinfo->hostname, strerror (errno));
    return -1;
  }

  /* Free the wire packet space if we allocated it */
  if (wirepacket && wirepacket != cinfo->sendbuf)
    free (wirepacket);

  return 0;
} /* End of SendPacket() */

/***************************************************************************
 * SendRingPacket:
 *
 * Create a packet header for a RingPacket and send() the header and
 * the packet data to the client.  Upon success update the client
 * transmission counts.
 *
 * The packet header is: "DL<size>PACKET <streamid> <pktid> <hppackettime> <hpdatastart> <hpdataend> <size>"
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendRingPacket (ClientInfo *cinfo)
{
  StreamNode *stream;
  char header[255];
  int headerlen;
  int newstream = 0;

  if (!cinfo)
    return -1;

  /* Create packet header: "PACKET <streamid> <pktid> <hppackettime> <hpdatatime> <size>" */
  headerlen = snprintf (header, sizeof (header),
                        "PACKET %s %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %u",
                        cinfo->packet.streamid, cinfo->packet.pktid, cinfo->packet.pkttime,
                        cinfo->packet.datastart, cinfo->packet.dataend, cinfo->packet.datasize);

  /* Sanity check header length */
  if (headerlen > 255)
  {
    lprintf (0, "[%s] SendRingPacket(): Header length is too large: %d",
             cinfo->hostname, headerlen);
    return -1;
  }

  /* Make sure send buffer is large enough for wire packet */
  if (cinfo->sendbuflen < (3 + headerlen + cinfo->packet.datasize))
  {
    lprintf (0, "[%s] SendRingPacket(): Send buffer not large enough (%d bytes), need %d bytes",
             cinfo->hostname, cinfo->sendbuflen, 3 + headerlen + cinfo->packet.datasize);
    return -1;
  }

  /* Populate pre-header sequence of wire packet */
  cinfo->sendbuf[0] = 'D';
  cinfo->sendbuf[1] = 'L';
  cinfo->sendbuf[2] = (uint8_t)headerlen;

  /* Copy header and packet data into wire packet */
  memcpy (&cinfo->sendbuf[3], header, headerlen);

  memcpy (&cinfo->sendbuf[3 + headerlen], cinfo->packetdata, cinfo->packet.datasize);

  /* Send complete wire packet */
  if (SendData (cinfo, cinfo->sendbuf, (3 + headerlen + cinfo->packet.datasize)))
  {
    if (cinfo->socketerr != 2)
      lprintf (0, "[%s] SendRingPacket(): Error sending packet: %s",
               cinfo->hostname, strerror (errno));
    return -1;
  }

  /* Get (creating if needed) the StreamNode for this streamid */
  if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                               cinfo->packet.streamid, &newstream)) == 0)
  {
    lprintf (0, "[%s] Error with GetStreamNode for %s",
             cinfo->hostname, cinfo->packet.streamid);
    return -1;
  }

  if (newstream)
  {
    lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, cinfo->packet.streamid);
    cinfo->streamscount++;
  }

  /* Update StreamNode packet and byte counts */
  pthread_mutex_lock (&(cinfo->streams_lock));
  stream->txpackets++;
  stream->txbytes += cinfo->packet.datasize;
  pthread_mutex_unlock (&(cinfo->streams_lock));

  /* Update client transmit and counts */
  cinfo->txpackets[0]++;
  cinfo->txbytes[0] += cinfo->packet.datasize;

  /* Update last sent packet ID */
  cinfo->lastid = cinfo->packet.pktid;

  return 0;
} /* End of SendRingPacket() */

/***************************************************************************
 * SelectedStreams:
 *
 * Determine the number of streams selected with the current match and
 * reject settings.  Since GetStreamsStack() already applies the match
 * and reject expressions the only thing left to do is count the
 * select streams returned.
 *
 * Returns selected stream count on success and -1 on error.
 ***************************************************************************/
static int
SelectedStreams (RingParams *ringparams, RingReader *reader)
{
  Stack *streams;
  RingStream *ringstream;
  int streamcnt = 0;

  if (!ringparams || !reader)
    return -1;

  /* Create a duplicate Stack of currently selected RingStreams */
  streams = GetStreamsStack (ringparams, reader);

  /* Count the selected streams */
  while ((ringstream = StackPop (streams)))
  {
    free (ringstream);
    streamcnt++;
  }

  /* Cleanup stream stack */
  StackDestroy (streams, free);

  return streamcnt;
} /* End of SelectedStreams() */

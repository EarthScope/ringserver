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
 * Copyright (C) 2024:
 * @author Chad Trabant, EarthScope Data Services
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

#include "clients.h"
#include "dlclient.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "mseedscan.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"
#include "infoxml.h"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo);
static int HandleWrite (ClientInfo *cinfo);
static int HandleRead (ClientInfo *cinfo);
static int HandleInfo (ClientInfo *cinfo);
static int SendPacket (ClientInfo *cinfo, char *header, char *data,
                       uint64_t value, int addvalue, int addsize);
static int SendRingPacket (ClientInfo *cinfo);
static int SelectedStreams (RingReader *reader);

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
  DLInfo *dlinfo;

  if (!cinfo)
    return -1;

  /* Allocate and initialize DataLink specific information */
  if (!cinfo->extinfo)
  {
    if (!(dlinfo = (DLInfo *)calloc (1, sizeof (DLInfo))))
    {
      lprintf (0, "[%s] Error allocating DLInfo", cinfo->hostname);
      return -1;
    }

    cinfo->extinfo = dlinfo;

    /* Compile the legacy miniSEED stream ID pattern */
    if (UpdatePattern (&dlinfo->legacy_mseed_streamid_match,
                       &dlinfo->legacy_mseed_streamid_data,
                       LEGACY_MSEED_STREAMID_PATTERN, "legacy miniSEED stream ID pattern"))
    {
      return -1;
    }
  }

  dlinfo = (DLInfo *)cinfo->extinfo;

  /* Determine if this is a data submission and handle */
  if (!strncmp (cinfo->dlcommand, "WRITE", 5))
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
  else if (!strncmp (cinfo->dlcommand, "INFO", 4))
  {
    /* Any errors from HandleInfo are fatal */
    if (HandleInfo (cinfo))
    {
      return -1;
    }
  }

  /* Determine if this is a specific read request and handle */
  else if (!strncmp (cinfo->dlcommand, "READ", 4))
  {
    cinfo->state = STATE_COMMAND;

    /* Any errors from HandleRead are fatal */
    if (HandleRead (cinfo))
    {
      return -1;
    }
  }

  /* Determine if this is a request to start STREAMing and set state */
  else if (!strncmp (cinfo->dlcommand, "STREAM", 6))
  {
    /* Set read position to next packet if position not set */
    if (cinfo->reader->pktid == RINGID_NONE)
    {
      cinfo->reader->pktid = RINGID_NEXT;
    }

    cinfo->state = STATE_STREAM;
  }

  /* Determine if this is a request to end STREAMing and set state */
  else if (!strncmp (cinfo->dlcommand, "ENDSTREAM", 9))
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
    if (strncmp (cinfo->dlcommand, "ID", 2))
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
  uint64_t readid;

  if (!cinfo)
    return -1;

  /* Read next packet from ring */
  readid = RingReadNext (cinfo->reader, &cinfo->packet, cinfo->sendbuf);

  if (readid == RINGID_ERROR)
  {
    lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
    return -1;
  }
  else if (readid == RINGID_NONE)
  {
    return 0;
  }
  else
  {
    lprintf (3, "[%s] Read %s (%u bytes) packet ID %" PRIu64 " from ring",
             cinfo->hostname, cinfo->packet.streamid,
             cinfo->packet.datasize, cinfo->packet.pktid);

    /* Send packet to client */
    if (SendRingPacket (cinfo))
    {
      if (cinfo->socketerr != -2)
        lprintf (1, "[%s] Error sending packet to client", cinfo->hostname);

      return -1;
    }

    /* Socket errors are fatal */
    if (cinfo->socketerr)
      return -1;
  }

  return (int)cinfo->packet.datasize;
} /* End of DLStreamPackets() */

/***********************************************************************
 * DLFree:
 *
 * Free all memory specific to a DataLink client.
 *
 ***********************************************************************/
void
DLFree (ClientInfo *cinfo)
{
  DLInfo *dlinfo;

  if (!cinfo || !cinfo->extinfo)
    return;

  dlinfo = (DLInfo *)cinfo->extinfo;

  /* Free the legacy miniSEED stream ID matching data */
  if (dlinfo->legacy_mseed_streamid_match)
    pcre2_code_free (dlinfo->legacy_mseed_streamid_match);
  if (dlinfo->legacy_mseed_streamid_data)
    pcre2_match_data_free (dlinfo->legacy_mseed_streamid_data);

  free (dlinfo);
  cinfo->extinfo = NULL;

  return;
} /* End of SLFree() */

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
  size_t size;
  int fields;
  int selected;

  char OKGO = 1;
  char junk;

  if (!cinfo)
    return -1;

  /* ID - Return server ID, version and capability flags */
  if (!strncasecmp (cinfo->dlcommand, "ID", 2))
  {
    /* Parse client ID from command if included
     * Everything after "ID " is the client ID */
    if (strlen (cinfo->dlcommand) > 3)
    {
      strncpy (cinfo->clientid, cinfo->dlcommand + 3, sizeof (cinfo->clientid) - 1);
      *(cinfo->clientid + sizeof (cinfo->clientid) - 1) = '\0';
      lprintf (2, "[%s] Received ID (%s)", cinfo->hostname, cinfo->clientid);
    }
    else
    {
      lprintf (2, "[%s] Received ID", cinfo->hostname);
    }

    /* Create server version and capability flags string (DLSERVER_ID + PACKETSIZE + WRITE if permission) */
    snprintf (sendbuffer, sizeof (sendbuffer),
              "ID " DLSERVER_ID " PACKETSIZE:%lu%s",
              (unsigned long int)(param.pktsize - sizeof (RingPacket)),
              (cinfo->writeperm) ? " WRITE" : "");

    /* Send the server ID string */
    if (SendPacket (cinfo, sendbuffer, NULL, 0, 0, 0))
      return -1;
  }

  /* POSITION <SET|AFTER> value [time]\r\n - Set ring reading position */
  else if (!strncasecmp (cinfo->dlcommand, "POSITION", 8))
  {
    char subcmd[11];
    char value[32];
    char subvalue[32];
    uint64_t pktid = 0;
    nstime_t nstime;

    OKGO = 1;

    /* Parse sub-command and value from request */
    fields = sscanf (cinfo->dlcommand, "%*s %10s %31s %31s %c",
                     subcmd, value, subvalue, &junk);

    /* Make sure the subcommand, value and subvalue fields are terminated */
    subcmd[sizeof (subcmd) - 1]     = '\0';
    value[sizeof (value) - 1]       = '\0';
    subvalue[sizeof (subvalue) - 1] = '\0';

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
        nstime = NSTUNSET;

        /* Process SET <pktid> [time] */
        if (IsAllDigits (value))
        {
          pktid = (uint64_t)strtoull (value, NULL, 10);

          if (fields == 3)
          {
            /* Wire protocol uses time in microseconds (hptime), convert to nanoseconds (nstime) */
            nstime = strtoll (subvalue, NULL, 10);
            nstime = MS_HPTIME2NSTIME (nstime);
          }
        }
        /* Process SET EARLIEST */
        else if (!strncmp (value, "EARLIEST", 8))
        {
          pktid = RINGID_EARLIEST;
        }
        /* Process SET LATEST */
        else if (!strncmp (value, "LATEST", 6))
        {
          pktid = RINGID_LATEST;
        }
        else
        {
          lprintf (0, "[%s] Error with POSITION SET value: %s", cinfo->hostname, value);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION SET value", 0, 1, 1))
            return -1;
          OKGO = 0;
        }

        /* If no errors with the set value, position the reader */
        if (OKGO)
        {
          uint64_t position = RingPosition (cinfo->reader, pktid, nstime);

          if (position == RINGID_ERROR)
          {
            lprintf (0, "[%s] Error with RingPosition (pktid: %" PRIu64 ", nstime: %" PRId64 ")",
                     cinfo->hostname, pktid, nstime);
            if (SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1))
              return -1;
          }
          else if (position == RINGID_NONE && pktid != RINGID_EARLIEST && pktid != RINGID_LATEST)
          {
            if (SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1))
              return -1;
          }
          else
          {
            if (position == RINGID_NONE && pktid == RINGID_EARLIEST)
              snprintf (sendbuffer, sizeof (sendbuffer), "Positioned to EARLIEST packet");
            else if (position == RINGID_NONE && pktid == RINGID_LATEST)
              snprintf (sendbuffer, sizeof (sendbuffer), "Positioned to LATEST packet");
            else
              snprintf (sendbuffer, sizeof (sendbuffer), "Positioned to packet ID %" PRIu64, position);

            if (SendPacket (cinfo, "OK", sendbuffer, (position <= RINGID_MAXIMUM) ? position : 0, 1, 1))
              return -1;
          }
        }
      }
      /* Process AFTER <time> positioning */
      else if (!strncmp (subcmd, "AFTER", 5))
      {
        /* Wire protocol uses time in microseconds ("hptime"), convert to nanoseconds ("nstime") */
        nstime = strtoll (value, NULL, 10);
        nstime = MS_HPTIME2NSTIME (nstime);

        if (nstime == 0 && errno == EINVAL)
        {
          lprintf (0, "[%s] Error parsing POSITION AFTER time: %s", cinfo->hostname, value);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION AFTER time", 0, 1, 1))
            return -1;
        }
        else
        {
          char timestr[32];
          ms_nstime2timestr (nstime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);

          /* Position ring according to start time, use reverse search if limited */
          if (config.timewinlimit == 1.0)
          {
            pktid = RingAfter (cinfo->reader, nstime, 1);
          }
          else if (config.timewinlimit < 1.0)
          {
            uint64_t pktlimit = (uint64_t)(config.timewinlimit * param.maxpackets);

            pktid = RingAfterRev (cinfo->reader, nstime, pktlimit, 1);
          }
          else
          {
            lprintf (0, "Time window search limit is invalid: %f", config.timewinlimit);
            SendPacket (cinfo, "ERROR", "time window search limit is invalid", 0, 1, 1);
            return -1;
          }

          if (pktid == RINGID_ERROR)
          {
            lprintf (0, "[%s] Error with RingAfter[Rev] time: %s [%" PRId64 "]",
                     cinfo->hostname, timestr, nstime);
            if (SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1))
              return -1;
          }
          else if (pktid == RINGID_NONE)
          {
            lprintf (2, "[%s] No packet found for RingAfter time: %s [%" PRId64 "]",
                     cinfo->hostname, timestr, cinfo->starttime);
            if (SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1))
              return -1;
          }
          else
          {
            lprintf (3, "[%s] Positioned to packet %" PRIu64 ", first after: %s",
                     cinfo->hostname, pktid, timestr);

            snprintf (sendbuffer, sizeof (sendbuffer), "Positioned to packet ID %" PRIu64, pktid);
            if (SendPacket (cinfo, "OK", sendbuffer, pktid, 1, 1))
              return -1;
          }
        }
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
  else if (!strncasecmp (cinfo->dlcommand, "MATCH", 5))
  {
    OKGO = 1;

    /* Parse size from request */
    fields = sscanf (cinfo->dlcommand, "%*s %zu %c", &size, &junk);

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
      free (cinfo->matchstr);
      cinfo->matchstr = NULL;
      RingMatch (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match",
                selected);
      if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
        return -1;
    }
    else if (size > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] match expression too large (%zu)", cinfo->hostname, size);

      snprintf (sendbuffer, sizeof (sendbuffer), "match expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else
    {
      free (cinfo->matchstr);

      /* Read regex of size bytes from socket */
      if (!(cinfo->matchstr = (char *)malloc (size + 1)))
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        return -1;
      }

      if (RecvData (cinfo, cinfo->matchstr, size, 1) < 0)
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
        selected = SelectedStreams (cinfo->reader);
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match",
                  selected);
        if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
          return -1;
      }
    }
  } /* End of MATCH */

  /* REJECT size\r\n[pattern] - Provide regex to reject streamids */
  else if (OKGO && !strncasecmp (cinfo->dlcommand, "REJECT", 6))
  {
    OKGO = 1;

    /* Parse size from request */
    fields = sscanf (cinfo->dlcommand, "%*s %zu %c", &size, &junk);

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
      free (cinfo->rejectstr);
      cinfo->rejectstr = NULL;
      RingReject (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject",
                selected);
      if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
        return -1;
    }
    else if (size > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] reject expression too large (%zu)", cinfo->hostname, size);

      snprintf (sendbuffer, sizeof (sendbuffer), "reject expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else
    {
      free (cinfo->rejectstr);

      /* Read regex of size bytes from socket */
      if (!(cinfo->rejectstr = (char *)malloc (size + 1)))
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        return -1;
      }

      if (RecvData (cinfo, cinfo->rejectstr, size, 1) < 0)
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
        selected = SelectedStreams (cinfo->reader);
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject",
                  selected);
        if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
          return -1;
      }
    }
  } /* End of REJECT */

  /* BYE - End connection */
  else if (!strncasecmp (cinfo->dlcommand, "BYE", 3))
  {
    return -1;
  }

  /* Unrecognized command */
  else
  {
    lprintf (1, "[%s] Unrecognized command: %.10s",
             cinfo->hostname, cinfo->dlcommand);

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
 * Legacy stream IDs for legacy miniSEED of the form: NN_SSSSS_LL_CCC/MSEED
 * are converted to FDSN Source ID form: FDSN:NN_SSSSS_LL_C_C_C/MSEED.
 * Otherwise the stream ID is used verbatim by the ringserver.
 * The hpdatastart and hpdataend are high-precision time stamps (hptime),
 * microseconds since the POSIX epoch.  The data size is the size in bytes
 * of the data portion following the header.  The flags are single character
 * indicators and interpreted the following way:
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
  DLInfo *dlinfo;
  StreamNode *stream;
  char replystr[200];
  char streamid[101];
  char flags[101];
  int nread;
  int newstream = 0;
  int rv;

  MS3Record *msr = NULL;

  if (!cinfo || !cinfo->extinfo)
    return -1;

  dlinfo = (DLInfo *)cinfo->extinfo;

  /* Parse command parameters: WRITE <streamid> <datastart> <dataend> <flags> <datasize> [pktid] */
  rv = sscanf (cinfo->dlcommand, "%*s %100s %" SCNd64 " %" SCNd64 " %100s %" SCNu32 " %" SCNu64,
               streamid,
               &(cinfo->packet.datastart),
               &(cinfo->packet.dataend),
               flags,
               &(cinfo->packet.datasize),
               &(cinfo->packet.pktid));

  if (rv < 5)
  {
    lprintf (1, "[%s] Error parsing WRITE parameters: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

    return -1;
  }

  if (strlen (streamid) > (MAXSTREAMID - 1))
  {
    lprintf (1, "[%s] Error, stream ID too long: %.100s",
             cinfo->hostname, streamid);

    SendPacket (cinfo, "ERROR", "Error, stream ID too long", 0, 1, 1);

    return -1;
  }

  /* Set packet ID to RINGID_NONE if not provided */
  if (rv == 5 || (rv == 6 && strchr (flags, 'I') == NULL))
  {
    cinfo->packet.pktid = RINGID_NONE;
  }

  /* Translate legacy stream ID: NN_SSSSS_LL_CCC/MSEED
   * to an FDSN Source ID: FDSN:NN_SSSSS_LL_C_C_C/MSEED */
  if (dlinfo->legacy_mseed_streamid_match != NULL &&
      pcre2_match (dlinfo->legacy_mseed_streamid_match, (PCRE2_SPTR8)streamid,
                   PCRE2_ZERO_TERMINATED, 0, 0,
                   dlinfo->legacy_mseed_streamid_data, NULL) > 0)
  {
    char *prechannel = strrchr (streamid, '_');

    snprintf (cinfo->packet.streamid, sizeof (cinfo->packet.streamid),
              "FDSN:%.*s_%c_%c_%c%s",
              (int)(prechannel - streamid), streamid,
              prechannel[1], prechannel[2], prechannel[3],
              &prechannel[4]);

    lprintf (3, "Translating legacy stream ID: %s -> %s",
             streamid, cinfo->packet.streamid);
  }
  /* Otherwise copy stream ID verbatim */
  else
  {
    /* Copy the stream ID verbatim */
    memcpy (cinfo->packet.streamid, streamid, sizeof (cinfo->packet.streamid));

    /* Make sure the streamid is terminated */
    cinfo->packet.streamid[sizeof (cinfo->packet.streamid) - 1] = '\0';
  }

  /* Wire protocol for DataLink uses time stamps in as microseconds since the epoch,
   * convert these to the nanosecond ticks used internally. */
  cinfo->packet.datastart = MS_HPTIME2NSTIME (cinfo->packet.datastart);
  cinfo->packet.dataend   = MS_HPTIME2NSTIME (cinfo->packet.dataend);

  /* Check that client is allowed to write this stream ID if limit is present */
  if (cinfo->reader->limit)
  {
    if (pcre2_match (cinfo->reader->limit, (PCRE2_SPTR8)cinfo->packet.streamid,
                     PCRE2_ZERO_TERMINATED, 0, 0,
                     cinfo->reader->limit_data, NULL) < 0)
    {
      lprintf (1, "[%s] Error, permission denied for WRITE of stream ID: %s",
               cinfo->hostname, cinfo->packet.streamid);

      snprintf (replystr, sizeof (replystr), "Error, permission denied for WRITE of stream ID: %s",
                cinfo->packet.streamid);
      SendPacket (cinfo, "ERROR", replystr, 0, 1, 1);

      return -1;
    }
  }

  /* Make sure this packet data would fit into the ring */
  if (cinfo->packet.datasize > param.pktsize)
  {
    lprintf (1, "[%s] Submitted packet size (%d) is greater than ring packet size (%d)",
             cinfo->hostname, cinfo->packet.datasize, param.pktsize);

    snprintf (replystr, sizeof (replystr), "Packet size (%d) is too large for ring, maximum is %d bytes",
              cinfo->packet.datasize, param.pktsize);
    SendPacket (cinfo, "ERROR", replystr, 0, 1, 1);

    return -1;
  }

  /* Recv packet data from socket */
  nread = RecvData (cinfo, cinfo->recvbuf, cinfo->packet.datasize, 1);

  if (nread < 0)
    return -1;

  /* Write received miniSEED to a disk archive if configured */
  if (cinfo->mswrite &&
      (MS2_ISVALIDHEADER (cinfo->recvbuf) ||
       MS3_ISVALIDHEADER (cinfo->recvbuf)))
  {
    char filename[100] = {0};
    char *fn;

    /* Parse the miniSEED record header */
    if (msr3_parse (cinfo->recvbuf, cinfo->packet.datasize, &msr, 0, 0) == MS_NOERROR)
    {
      /* Check for file name in streamid: e.g. "filename::streamid/MSEED" */
      if ((fn = strstr (cinfo->packet.streamid, "::")))
      {
        strncpy (filename, cinfo->packet.streamid, (fn - cinfo->packet.streamid));
        filename[(fn - cinfo->packet.streamid)] = '\0';
        fn                                      = filename;
      }

      /* Write miniSEED record to disk */
      if (ds_streamproc (cinfo->mswrite, msr, fn, cinfo->hostname))
      {
        lprintf (1, "[%s] Error writing miniSEED to disk", cinfo->hostname);

        SendPacket (cinfo, "ERROR", "Error writing miniSEED to disk", 0, 1, 1);

        return -1;
      }

      msr3_free (&msr);
    }
  }

  /* Add the packet to the ring */
  if ((rv = RingWrite (&cinfo->packet, cinfo->recvbuf, cinfo->packet.datasize)))
  {
    if (rv == -2)
      lprintf (1, "[%s] Error with RingWrite, corrupt ring, shutdown signalled", cinfo->hostname);
    else
      lprintf (1, "[%s] Error with RingWrite", cinfo->hostname);

    SendPacket (cinfo, "ERROR", "Error adding packet to ring", 0, 1, 1);

    /* Set the shutdown signal if ring corruption was detected */
    if (rv == -2)
      param.shutdownsig = 1;

    return -1;
  }

  /* Get (creating if needed) the StreamNode for this streamid */
  if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                               cinfo->packet.streamid, &newstream)) == NULL)
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
  cinfo->rxpackets0++;
  cinfo->rxbytes0 += cinfo->packet.datasize;

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
  uint64_t reqid  = 0;
  uint64_t readid = 0;
  char replystr[100];

  if (!cinfo)
    return -1;

  /* Parse command parameters: READ <pktid> */
  if (sscanf (cinfo->dlcommand, "%*s %" PRIu64, &reqid) != 1)
  {
    lprintf (1, "[%s] Error parsing READ parameters: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    if (SendPacket (cinfo, "ERROR", "Error parsing READ command parameters", 0, 1, 1))
      return -1;
  }

  /* Read the packet from the ring */
  readid = RingRead (cinfo->reader, reqid, &cinfo->packet, cinfo->sendbuf);

  if (readid == RINGID_ERROR)
  {
    lprintf (1, "[%s] Error with RingRead", cinfo->hostname);

    if (SendPacket (cinfo, "ERROR", "Error reading packet from ring", 0, 1, 1))
      return -1;
  }
  else if (readid == RINGID_NONE)
  {
    snprintf (replystr, sizeof (replystr), "Packet %" PRIu64 " not found in ring", reqid);
    if (SendPacket (cinfo, "ERROR", replystr, 0, 1, 1))
      return -1;
  }
  /* Send packet to client */
  else if (SendRingPacket (cinfo))
  {
    if (cinfo->socketerr != -2)
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
HandleInfo (ClientInfo *cinfo)
{
  char string[200];
  char *xmlstr = NULL;
  int xmllength;
  char *type      = NULL;
  char *matchexpr = NULL;

  if (!cinfo)
    return -1;

  if (!strncasecmp (cinfo->dlcommand, "INFO", 4))
  {
    /* Set level pointer to start of type identifier */
    type = cinfo->dlcommand + 4;

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

  /* Add contents to the XML structure depending on info request */
  if (!strncasecmp (type, "STATUS", 6))
  {
    lprintf (1, "[%s] Received INFO STATUS request", cinfo->hostname);
    type = "INFO STATUS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "STATUS", matchexpr, cinfo->trusted);
  } /* End of STATUS */
  else if (!strncasecmp (type, "STREAMS", 7))
  {
    lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);
    type = "INFO STREAMS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "STREAMS", matchexpr, cinfo->trusted);
  } /* End of STREAMS */
  else if (!strncasecmp (type, "CONNECTIONS", 11))
  {
    /* Check for trusted flag, required to access this resource */
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] INFO CONNECTIONS request from un-trusted client",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Access to CONNECTIONS denied", 0, 1, 1);

      return -1;
    }

    lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);
    type = "INFO CONNECTIONS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "CONNECTIONS", matchexpr, cinfo->trusted);
  } /* End of CONNECTIONS */
  /* Unrecognized INFO request */
  else
  {
    lprintf (0, "[%s] Unrecognized INFO request type: %s", cinfo->hostname, type);

    snprintf (string, sizeof (string), "Unrecognized INFO request type: %s", type);
    SendPacket (cinfo, "ERROR", string, 0, 1, 1);

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
    if (cinfo->socketerr != -2)
      lprintf (0, "[%s] Error sending INFO XML", cinfo->hostname);

    free (xmlstr);
    return -1;
  }

  free (xmlstr);

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleInfo */

/***************************************************************************
 * SendPacket:
 *
 * Create and send a packet from given header and packet data strings.
 * The header and packet strings must be NULL-terminated.  If the data
 * argument is NULL a header-only packet will be sent.  If the
 * addvalue argument is true the value argument will be appended to
 * the header.  If the addsize argument is true the size of the packet
 * string will be appended to the header.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendPacket (ClientInfo *cinfo, char *header, char *data,
            uint64_t value, int addvalue, int addsize)
{
  char *wirepacket = NULL;
  char headerstr[255];
  uint8_t headerlen_u8;
  size_t headerlen;
  size_t datalen;

  if (!cinfo || !header)
    return -1;

  /* Determine length of packet data string */
  datalen = (data) ? strlen (data) : 0;

  /* Add value and/or size of packet data to header */
  if (addvalue || addsize)
  {
    if (addvalue && addsize)
      snprintf (headerstr, sizeof (headerstr), "%s %" PRIu64 " %zu", header, value, datalen);
    else if (addvalue)
      snprintf (headerstr, sizeof (headerstr), "%s %" PRIu64, header, value);
    else
      snprintf (headerstr, sizeof (headerstr), "%s %zu", header, datalen);

    header = headerstr;
  }

  /* Determine length of header and sanity check it */
  headerlen = strlen (header);

  if (headerlen > UINT8_MAX)
  {
    lprintf (0, "[%s] SendPacket(): Header length is too large: %zu",
             cinfo->hostname, headerlen);
    return -1;
  }

  /* Use the send buffer if large enough otherwise allocate memory for wire packet */
  if (cinfo->sendbufsize >= (3 + headerlen + datalen))
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
  headerlen_u8  = (uint8_t)headerlen;
  memcpy (wirepacket + 2, &headerlen_u8, 1);

  /* Copy header and packet data into wire packet */
  memcpy (&wirepacket[3], header, headerlen);

  if (data)
    memcpy (&wirepacket[3 + headerlen], data, datalen);

  /* Send complete wire packet */
  if (SendData (cinfo, wirepacket, (3 + headerlen + datalen), 0))
  {
    if (cinfo->socketerr != -2)
      lprintf (0, "[%s] SendPacket(): Error sending packet: %s",
               cinfo->hostname, strerror (errno));
    return -1;
  }

  /* Free the wire packet space if we allocated it */
  if (wirepacket != cinfo->sendbuf)
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
  char preheader[3];
  char header[UINT8_MAX];
  uint8_t headerlen_u8;
  size_t headerlen;
  int newstream = 0;

  if (!cinfo)
    return -1;

  /* Create microsecond values for wire protocol from nanosecond values */
  int64_t uspkttime   = (cinfo->packet.pkttime) ? MS_NSTIME2HPTIME (cinfo->packet.pkttime) : 0;
  int64_t usdatastart = (cinfo->packet.datastart) ? MS_NSTIME2HPTIME (cinfo->packet.datastart) : 0;
  int64_t usdataend   = (cinfo->packet.dataend) ? MS_NSTIME2HPTIME (cinfo->packet.dataend) : 0;

  /* Create packet header: "PACKET <streamid> <pktid> <hppackettime> <hpdatatime> <size>" */
  headerlen = (size_t)snprintf (header, sizeof (header),
                                "PACKET %s %" PRIu64 " %" PRId64 " %" PRId64 " %" PRId64 " %u",
                                cinfo->packet.streamid, cinfo->packet.pktid, uspkttime,
                                usdatastart, usdataend, cinfo->packet.datasize);

  /* Sanity check header length */
  if (headerlen > UINT8_MAX)
  {
    lprintf (0, "[%s] SendRingPacket(): Header length is too large: %zu",
             cinfo->hostname, headerlen);
    return -1;
  }

  /* Populate pre-header sequence of wire packet */
  preheader[0] = 'D';
  preheader[1] = 'L';
  headerlen_u8      = (uint8_t)headerlen;
  memcpy (preheader + 2, &headerlen_u8, 1);

  /* Send complete wire packet */
  if (SendDataMB (cinfo,
                  (void *[]){preheader, header, cinfo->sendbuf},
                  (size_t[]){3, headerlen, cinfo->packet.datasize},
                  3, 0))
  {
    if (cinfo->socketerr != -2)
      lprintf (0, "[%s] SendRingPacket(): Error sending packet: %s",
               cinfo->hostname, strerror (errno));
    return -1;
  }

  /* Get (creating if needed) the StreamNode for this streamid */
  if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                               cinfo->packet.streamid, &newstream)) == NULL)
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
  cinfo->txpackets0++;
  cinfo->txbytes0 += cinfo->packet.datasize;

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
SelectedStreams (RingReader *reader)
{
  Stack *streams;
  RingStream *ringstream;
  int streamcnt = 0;

  if (!reader)
    return -1;

  /* Create a duplicate Stack of currently selected RingStreams */
  streams = GetStreamsStack (reader);

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

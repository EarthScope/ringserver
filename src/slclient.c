/**************************************************************************
 * slclient.c
 *
 * SeedLink client thread specific routines.
 *
 * Mapping SeedLink sequence numbers <-> ring packet ID's:
 *
 * SeedLink v4 sequence numbers are directly mapped to ringer buffer
 * packet IDs as both are unsigned 64-bt integers.
 *
 * SeedLink v3 sequence numbers are limited to 24-bit values.  They
 * are mapped to ring packet IDs by combining the highest 40-bits of
 * the latest ring packet ID with the 24-bit SeedLink sequence number.
 * If the packet ID series is well behaved, increasing monotonically,
 * this will result in v3 clients being able to resume from the last
 * 16,777,215 packets received.
 *
 * The SeedLink protocol is designed with the concept of a station ID
 * where sequences may be specific to each station ID.  Ringserver
 * implements a single buffer with a shared set of sequences for all
 * streams in the buffer.  This detail is important because a client
 * can request data from multiple station IDs with each specifying a
 * starting sequence number to resume a connection.  This is handled
 * in this client-handler by determining the most recent sequence
 * requested by the client and setting the ring to that position.  The
 * rationale is that the client has already received all data up to
 * the most recent sequence requested during a previous connection.
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
#include <mseedformat.h>

#include "clients.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"
#include "slclient.h"
#include "infojson.h"
#include "infoxml.h"

/* Define list of valid characters for selectors and station & network codes */
#define VALIDSELECTCHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_-!"
#define VALIDSTAIDCHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo);
static int HandleInfo_v3 (ClientInfo *cinfo);
static int HandleInfo_v4 (ClientInfo *cinfo);
static int SendReply (ClientInfo *cinfo, char *reply, ErrorCode code, char *extreply);
static int SendPacket (uint64_t pktid, char *payload, uint32_t payloadlen,
                       const char *staid, char format, char subformat, void *vcinfo);
static int SendRecord (RingPacket *packet, char *record, uint32_t reclen,
                       void *vcinfo);
static void SendInfoRecord (char *record, uint32_t reclen, void *vcinfo);
static void FreeReqStationID (void *rbnode);
static int StaKeyCompare (const void *a, const void *b);
static ReqStationID *GetReqStationID (RBTree *tree, char *staid);
static int StationToRegex (const char *staid, struct strnode *selector,
                           char **matchregex, char **rejectregex);
static int SelectToRegex (const char *staid, const char *select,
                          char **regex);

/***********************************************************************
 * SLHandleCmd:
 *
 * Handle SeedLink command, which is expected to be in the
 * ClientInfo.recvbuf buffer.
 *
 * Returns zero on success, negative value on error.  On error the
 * client should be disconnected.
 ***********************************************************************/
int
SLHandleCmd (ClientInfo *cinfo)
{
  SLInfo *slinfo;
  ReqStationID *stationid;
  uint64_t readid;
  uint64_t retval;

  if (!cinfo)
    return -1;

  /* Allocate and initialize SeedLink specific information */
  if (!cinfo->extinfo)
  {
    if (!(slinfo = (SLInfo *)calloc (1, sizeof (SLInfo))))
    {
      lprintf (0, "[%s] Error allocating SLInfo", cinfo->hostname);
      return -1;
    }

    cinfo->extinfo = slinfo;

    /* Default protocol expectation */
    slinfo->proto_major = 3;
    slinfo->proto_minor = 0;

    slinfo->stations = RBTreeCreate (StaKeyCompare, free, FreeReqStationID);
  }

  slinfo = (SLInfo *)cinfo->extinfo;

  /* Determine if this is an INFO request and handle */
  if (!strncasecmp (cinfo->recvbuf, "INFO", 4))
  {
    if (slinfo->proto_major == 4)
    {
      if (HandleInfo_v4 (cinfo))
      {
        return -1;
      }
    }
    else
    {
      if (HandleInfo_v3 (cinfo))
      {
        return -1;
      }
    }
  }

  /* Negotiation if expecting commands */
  else if (cinfo->state == STATE_COMMAND ||
           cinfo->state == STATE_STATION)
  {
    if (HandleNegotiation (cinfo))
    {
      return -1;
    }
  }

  /* Otherwise this is unexpected data from the client */
  else
  {
    lprintf (1, "[%s] Unexpected data received from client", cinfo->hostname);
  }

  /* Configure ring parameters if negotiation is complete */
  if (cinfo->state == STATE_RINGCONFIG)
  {
    lprintf (2, "[%s] Configuring ring parameters", cinfo->hostname);

    /* If no stations specified convert any global selectors to regexes */
    if (slinfo->stationcount == 0 && slinfo->selectors)
    {
      if (StationToRegex (NULL, slinfo->selectors,
                          &(cinfo->matchstr), &(cinfo->rejectstr)))
      {
        lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
        return -1;
      }
    }
    /* Loop through any specified stations to:
     * 1) Configure regexes
     * 2) Find widest time window
     * 3) Find start packet
     */
    else if (slinfo->stationcount > 0 && slinfo->stations)
    {
      Stack *stack;
      RBNode *rbnode;
      nstime_t newesttime = NSTUNSET;

      stack = StackCreate ();
      RBBuildStack (slinfo->stations, stack);

      while ((rbnode = (RBNode *)StackPop (stack)))
      {
        stationid = (ReqStationID *)rbnode->data;

        /* Configure regexes for this station */
        if (StationToRegex ((const char *)rbnode->key, stationid->selectors,
                            &(cinfo->matchstr), &(cinfo->rejectstr)))
        {
          lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
          SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
          return -1;
        }

        /* Track the widest time window requested */

        /* Set or expand the global starttime */
        if (stationid->starttime != NSTUNSET)
        {
          if (!cinfo->starttime)
            cinfo->starttime = stationid->starttime;
          else if (cinfo->starttime > stationid->starttime)
            cinfo->starttime = stationid->starttime;
        }

        /* Set or expand the global endtime */
        if (stationid->endtime != NSTUNSET)
        {
          if (!cinfo->endtime)
            cinfo->endtime = stationid->endtime;
          else if (cinfo->endtime < stationid->endtime)
            cinfo->endtime = stationid->endtime;
        }

        /* Track the newest packet ID while validating their existence */
        if (stationid->packetid != RINGID_NONE)
          retval = RingRead (cinfo->reader, stationid->packetid, &cinfo->packet, 0);
        else
          retval = 0;

        /* Requested packet must be valid and have a matching data start time
         * Limit packet time matching to integer seconds to match SeedLink syntax limits */
        if (retval == stationid->packetid &&
            (stationid->datastart == NSTUNSET ||
             (int64_t)(MS_NSTIME2EPOCH (stationid->datastart)) == (int64_t)(MS_NSTIME2EPOCH (cinfo->packet.datastart))))
        {
          /* Use this packet ID if it is newer than any previous newest */
          if (newesttime == NSTUNSET || cinfo->packet.pkttime > newesttime)
          {
            slinfo->startid = stationid->packetid;
            newesttime      = cinfo->packet.pkttime;
          }
        }
      }

      StackDestroy (stack, 0);
    }

    lprintf (2, "[%s] Requesting match: '%s', reject: '%s'", cinfo->hostname,
             (cinfo->matchstr) ? cinfo->matchstr : "", (cinfo->rejectstr) ? cinfo->rejectstr : "");

    /* Position ring to starting packet ID if specified */
    if (slinfo->startid > 0)
    {
      retval = RingPosition (cinfo->reader, slinfo->startid, NSTUNSET);

      if (retval == RINGID_ERROR)
      {
        lprintf (0, "[%s] Error with RingPosition for %" PRIu64,
                 cinfo->hostname, slinfo->startid);
        return -1;
      }
      else if (retval == RINGID_NONE)
      {
        lprintf (0, "[%s] Could not find and position to packet ID: %" PRIu64,
                 cinfo->hostname, slinfo->startid);
      }
      else
      {
        lprintf (2, "[%s] Positioned ring to packet ID: %" PRIu64,
                 cinfo->hostname, slinfo->startid);
      }
    }

    /* Select sources if any specified */
    if (cinfo->matchstr)
    {
      if (RingMatch (cinfo->reader, cinfo->matchstr) < 0)
      {
        lprintf (0, "[%s] Error with RingMatch for (%lu bytes) '%s'",
                 cinfo->hostname, (unsigned long)strlen (cinfo->matchstr), cinfo->matchstr);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "cannot compile matches (combined matches too large?)");
        return -1;
      }
    }

    /* Reject sources if any specified */
    if (cinfo->rejectstr)
    {
      if (RingReject (cinfo->reader, cinfo->rejectstr) < 0)
      {
        lprintf (0, "[%s] Error with RingReject for (%lu bytes) '%s'",
                 cinfo->hostname, (unsigned long)strlen (cinfo->rejectstr), cinfo->rejectstr);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "cannot compile rejections (combined rejection too large?)");
        return -1;
      }
    }

    /* Set ring position based on time if start time specified and not a packet ID */
    if (cinfo->starttime && cinfo->starttime != NSTUNSET && !slinfo->startid)
    {
      char timestr[32];
      ms_nstime2timestr (cinfo->starttime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);

      /* Position ring according to start time, use reverse search if limited */
      if (cinfo->timewinlimit == 1.0)
      {
        readid = RingAfter (cinfo->reader, cinfo->starttime, 0);
      }
      else if (cinfo->timewinlimit < 1.0)
      {
        uint64_t pktlimit = (uint64_t)(cinfo->timewinlimit * cinfo->ringparams->maxpackets);

        readid = RingAfterRev (cinfo->reader, cinfo->starttime, pktlimit, 0);
      }
      else
      {
        lprintf (0, "Time window search limit is invalid: %f", cinfo->timewinlimit);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "time window search limit is invalid");
        return -1;
      }

      if (readid == RINGID_ERROR)
      {
        lprintf (0, "[%s] Error with RingAfter[Rev] time: %s [%" PRId64 "]",
                 cinfo->hostname, timestr, cinfo->starttime);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error positioning reader to start of time window");
        return -1;
      }
      else if (readid == RINGID_NONE)
      {
        lprintf (2, "[%s] No packet found for RingAfter time: %s [%" PRId64 "], positioning to next packet",
                 cinfo->hostname, timestr, cinfo->starttime);
        cinfo->reader->pktid = RINGID_NEXT;
      }
      else
      {
        lprintf (3, "[%s] Positioned to packet %" PRIu64 ", first after: %s",
                 cinfo->hostname, readid, timestr);
      }
    }

    /* Set read position to next packet if not already done */
    if (cinfo->reader->pktid == RINGID_NONE)
    {
      cinfo->reader->pktid = RINGID_NEXT;
    }

    lprintf (1, "[%s] Configured ring parameters", cinfo->hostname);

    cinfo->state = STATE_STREAM;
  } /* Done configuring ring parameters */

  return 0;
} /* End of SLHandleCmd() */

/***********************************************************************
 * SLStreamPackets:
 *
 * Send selected ring packets to SeedLink client.
 *
 * The next read packet is only be sent if the type is allowed by
 * SeedLink, e.g. miniSEED, but the size is returned to the caller
 * to indicate that a packet was available.
 *
 * Return packet size processed on successful read from ring, zero
 * when no next packet is available, or negative value on error.  On
 * error the client should disconnected.
 ***********************************************************************/
int
SLStreamPackets (ClientInfo *cinfo)
{
  SLInfo *slinfo;
  StreamNode *stream;
  uint64_t readid;
  int skiprecord = 0;
  int newstream;

  if (!cinfo || !cinfo->extinfo)
    return -1;

  slinfo = (SLInfo *)cinfo->extinfo;

  /* Read next packet from ring */
  readid = RingReadNext (cinfo->reader, &cinfo->packet, cinfo->sendbuf);

  if (readid == RINGID_ERROR)
  {
    lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
    return -1;
  }
  else if (readid == RINGID_NONE)
  {
    if (slinfo->dialup)
    {
      lprintf (2, "[%s] Dial-up mode reached end of buffer", cinfo->hostname);
      SendData (cinfo, "END", 3, 0);
      return -1;
    }
    else
    {
      return 0;
    }
  }
  else if ((MS2_ISVALIDHEADER (cinfo->sendbuf) ||
            MS3_ISVALIDHEADER (cinfo->sendbuf)))
  {
    lprintf (3, "[%s] Read %s (%u bytes) packet ID %" PRIu64 " from ring",
             cinfo->hostname, cinfo->packet.streamid, cinfo->packet.datasize, cinfo->packet.pktid);

    /* Get (creating if needed) the StreamNode for this streamid */
    if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                                 cinfo->packet.streamid, &newstream)) == NULL)
    {
      lprintf (0, "[%s] Error with GetStreamNode() for %s",
               cinfo->hostname, cinfo->packet.streamid);
      return -1;
    }

    if (newstream)
    {
      lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, cinfo->packet.streamid);
      cinfo->streamscount++;
    }

    /* Perform time-windowing end time checks */
    if (cinfo->endtime != 0 && cinfo->endtime != NSTUNSET)
    {
      /* Track count of number of channels for time-windowing */
      slinfo->timewinchannels += newstream;

      /* Check if the end time has been reached */
      if (stream->endtimereached == 1)
      {
        skiprecord = 1;
      }
      else if (cinfo->packet.datastart > cinfo->endtime)
      {
        lprintf (2, "[%s] End of time window reached for %s",
                 cinfo->hostname, cinfo->packet.streamid);
        stream->endtimereached = 1;
        slinfo->timewinchannels--;

        /* Skip this record */
        skiprecord = 1;
      }

      /* If end times for each received channel have been met the time-windowing is done */
      if (slinfo->timewinchannels <= 0)
      {
        lprintf (2, "[%s] End of time window reached for all channels", cinfo->hostname);
        SendData (cinfo, "END", 3, 0);
        return -1;
      }
    }

    /* If not skipping this record send to the client and update byte count */
    if (!skiprecord)
    {
      /* Send miniSEED record to client */
      if (SendRecord (&cinfo->packet, cinfo->sendbuf, cinfo->packet.datasize, cinfo))
      {
        if (cinfo->socketerr != -2)
          lprintf (0, "[%s] Error sending record to client", cinfo->hostname);

        return -1;
      }

      /* Update StreamNode packet and byte count */
      pthread_mutex_lock (&(cinfo->streams_lock));
      stream->txpackets++;
      stream->txbytes += cinfo->packet.datasize;
      pthread_mutex_unlock (&(cinfo->streams_lock));

      /* Update client transmit and counts */
      cinfo->txpackets[0]++;
      cinfo->txbytes[0] += cinfo->packet.datasize;

      /* Update last sent packet ID */
      cinfo->lastid = cinfo->packet.pktid;
    }
    else
    {
      readid = RINGID_NONE;
    }
  }

  return (readid != RINGID_NONE) ? (int)cinfo->packet.datasize : 0;
} /* End of SLStreamPackets() */

/***********************************************************************
 * SLFree:
 *
 * Free all memory specific to a SeedLink client.
 *
 ***********************************************************************/
void
SLFree (ClientInfo *cinfo)
{
  SLInfo *slinfo;

  if (!cinfo || !cinfo->extinfo)
    return;

  slinfo = (SLInfo *)cinfo->extinfo;

  for (struct strnode *next, *node = slinfo->selectors; node; node = next)
  {
    next = node->next;
    free (node->string);
    free (node);
  }

  RBTreeDestroy (slinfo->stations);
  slinfo->stations = NULL;

  free (slinfo);
  cinfo->extinfo = NULL;

  return;
} /* End of SLFree() */

/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation command implementing server-side SeedLink
 * protocol, updating the connection configuration and state
 * specified.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (ClientInfo *cinfo)
{
  SLInfo *slinfo;
  char sendbuffer[400];
  ReqStationID *stationid;
  int fields;

  nstime_t starttime    = NSTUNSET;
  nstime_t endtime      = NSTUNSET;
  char starttimestr[51] = {0};
  char endtimestr[51]   = {0};
  char selector[64]     = {0};
  uint64_t startpacket  = RINGID_NEXT;

  char *ptr;
  char OKGO = 1;
  char junk;

  if (!cinfo || !cinfo->extinfo)
    return -1;

  slinfo = (SLInfo *)cinfo->extinfo;

  /* HELLO (v3.x and v4.0) - Return server version and ID */
  if (!strncasecmp (cinfo->recvbuf, "HELLO", 5))
  {
    int bytes;

    /* Create and send server version information */
    bytes = snprintf (sendbuffer, sizeof (sendbuffer),
                      SLSERVER_ID "\r\n%s\r\n", config.serverid);

    if (bytes >= sizeof (sendbuffer))
    {
      lprintf (0, "[%s] Response to HELLO is likely truncated: '%*s'",
               cinfo->hostname, (int)sizeof (sendbuffer), sendbuffer);
    }

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer), 0))
      return -1;
  }

  /* SLPROTO (v4.0) - Parse requested protocol version */
  else if (!strncasecmp (cinfo->recvbuf, "SLPROTO", 7))
  {
    uint8_t proto_major = 0;
    uint8_t proto_minor = 0;

    fields = sscanf (cinfo->recvbuf, "%*s %" SCNu8 ".%" SCNu8,
                     &proto_major, &proto_minor);

    if ((proto_major == 3) ||
        (proto_major == 4 && proto_minor == 0))
    {
      if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
        return -1;

      slinfo->proto_major = proto_major;
      slinfo->proto_minor = proto_minor;

      lprintf (2, "[%s] Received %s, protocol accepted", cinfo->hostname, cinfo->recvbuf);
    }
    else
    {
      lprintf (2, "[%s] Received %s, protocol rejected", cinfo->hostname, cinfo->recvbuf);

      if (!slinfo->batch && SendReply (cinfo, "ERROR UNSUPPORTED unsupported protocol version", ERROR_NONE, NULL))
        return -1;
    }
  }

  /* USERAGENT (v4.0) - Parse user agent command */
  else if (!strncasecmp (cinfo->recvbuf, "USERAGENT", 9))
  {
    ptr = cinfo->recvbuf + 9;
    while (isspace ((int)*ptr))
      ptr++;

    strncpy (cinfo->clientid, ptr, sizeof (cinfo->clientid) - 1);
    cinfo->clientid[sizeof (cinfo->clientid) - 1] = '\0';

    lprintf (2, "[%s] Received USERAGENT (%s)", cinfo->hostname, cinfo->clientid);

    if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* CAPABILITIES (v3.x) - Parse capabilities flags */
  else if (!strncasecmp (cinfo->recvbuf, "CAPABILITIES", 12))
  {
    /* Extended reply capability */
    if (strstr (cinfo->recvbuf, "EXTREPLY"))
      slinfo->extreply = 1;

    if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* CAT (v3.x) - Return text list of stations */
  else if (!strncasecmp (cinfo->recvbuf, "CAT", 3))
  {
    snprintf (sendbuffer, sizeof (sendbuffer),
              "CAT command not implemented\r\n");

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer), 0))
      return -1;
  }

  /* BATCH (v3.x) - Batch mode for subsequent commands */
  else if (!strncasecmp (cinfo->recvbuf, "BATCH", 5))
  {
    slinfo->batch = 1;

    if (SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* STATION (v3.x and v4.0) - Select specified station */
  else if (!strncasecmp (cinfo->recvbuf, "STATION", 7))
  {
    OKGO = 1;

    /* Parse station ID from request */
    slinfo->reqstaid[0] = '\0';

    if (slinfo->proto_major == 4)
    {
      /* STATION stationID */
      fields = sscanf (cinfo->recvbuf, "%*s %20s %c", slinfo->reqstaid, &junk);

      slinfo->reqstaid[sizeof (slinfo->reqstaid) - 1] = '\0';

      /* Make sure we got a station ID */
      if (fields != 1)
      {
        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "STATION requires 1 argument"))
          return -1;

        OKGO = 0;
      }
    }
    else /* Protocol 3.x */
    {
      char reqnet[10] = {0};
      char reqsta[10] = {0};

      /* STATION STA NET */
      fields = sscanf (cinfo->recvbuf, "%*s %9s %9s %c", reqsta, reqnet, &junk);

      /* Make sure we got a station code and optionally a network code */
      if (fields < 1 || fields > 2)
      {
        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "STATION requires 1 or 2 arguments"))
          return -1;

        OKGO = 0;
      }
      /* Use wildcard network if not specified */
      else if (fields == 1)
      {
        reqnet[0] = '*';
        reqnet[1] = '\0';
      }

      /* Combine network and station codes into station ID */
      snprintf (slinfo->reqstaid, sizeof (slinfo->reqstaid), "%s_%s", reqnet, reqsta);
    }

    /* Sanity check, only allowed characters in station ID */
    if (OKGO && strspn (slinfo->reqstaid, VALIDSTAIDCHARS) != strlen (slinfo->reqstaid))
    {
      lprintf (0, "[%s] Error, requested station code illegal characters: '%s'",
               cinfo->hostname, slinfo->reqstaid);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Invalid characters in station ID"))
        return -1;

      OKGO = 0;
    }

    if (OKGO)
    {
      /* Add to the stations list */
      if (GetReqStationID (slinfo->stations, slinfo->reqstaid) == NULL)
      {
        lprintf (0, "[%s] Error in GetReqStationID() for command STATION", cinfo->hostname);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetReqStationID()"))
          return -1;
      }

      if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
        return -1;

      slinfo->stationcount++;
      cinfo->state = STATE_STATION;
    }
  } /* End of STATION */

  /* SELECT (v3.x and v4.0) - Refine selection of channels for STATION */
  else if (!strncasecmp (cinfo->recvbuf, "SELECT", 6))
  {
    OKGO = 1;

    /* Parse pattern from request */
    fields = sscanf (cinfo->recvbuf, "%*s %63s %c", selector, &junk);

    /* Make sure we got a single pattern */
    if (fields != 1)
    {
      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "SELECT requires a single argument"))
        return -1;

      OKGO = 0;
    }

    /* For SeedLink v4, check for unsupported filter (conversion) requests */
    if (OKGO && slinfo->proto_major == 4 && (ptr = strrchr (selector, ':')))
    {
      /* Any filter except "native" is not supported */
      if (strcmp (ptr + 1, "native") != 0)
      {
        lprintf (0, "[%s] Error, SELECT filter '%s' not supported", cinfo->hostname, ptr + 1);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Filter not supported"))
          return -1;

        OKGO = 0;
      }

      *ptr = '\0';
    }

    /* Truncate pattern at a '.', subtypes are accepted but not supported */
    if (OKGO && (ptr = strrchr (selector, '.')))
    {
      *ptr = '\0';
    }

    /* Convert v3 style LLCCC selectors to v4 style (FDSN Source ID) for consistency */
    if (OKGO && slinfo->proto_major == 3)
    {
      char newselector[sizeof (selector)];
      char *negate     = (selector[0] == '!') ? "!" : "";
      char *v3selector = (selector[0] == '!') ? selector + 1 : selector;

      if (strlen (v3selector) == 5)
      {
        snprintf (newselector, sizeof (newselector), "%s%c%c_%c_%c_%c",
                  negate, v3selector[0], v3selector[1], v3selector[2], v3selector[3], v3selector[4]);
      }
      else if (strlen (v3selector) == 3)
      {
        snprintf (newselector, sizeof (newselector), "%s??_%c_%c_%c",
                  negate, v3selector[0], v3selector[1], v3selector[2]);
      }
      else
      {
        lprintf (0, "[%s] Error, SELECT pattern '%s' is not a valid SeedLink v3 LLCCC or CCC pattern",
                 cinfo->hostname, selector);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Invalid selector pattern"))
          return -1;

        OKGO = 0;
      }

      strncpy (selector, newselector, sizeof (selector));
    }

    /* Sanity check, only allowed characters */
    if (OKGO && strspn (selector, VALIDSELECTCHARS) != strlen (selector))
    {
      lprintf (0, "[%s] Error, select pattern contains illegal characters: '%s'",
               cinfo->hostname, selector);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Selector contains illegal characters"))
        return -1;

      OKGO = 0;
    }

    /* Allocate and populate new selector list node */
    struct strnode *newselector;
    if ((newselector = (struct strnode *)malloc (sizeof (struct strnode))) == NULL)
    {
      lprintf (0, "[%s] Error allocating memory", cinfo->hostname);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error allocating memory()"))
        return -1;

      OKGO = 0;
    }

    newselector->string = strdup (selector);

    /* If modifying a STATION add selector to it's entry */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      /* Find the appropriate station ID */
      if (!(stationid = GetReqStationID (slinfo->stations, slinfo->reqstaid)))
      {
        lprintf (0, "[%s] Error in GetReqStationID() for command SELECT", cinfo->hostname);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetReqStationID()"))
          return -1;
      }
      else
      {
        /* Add selector to the station ID selectors */
        /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
        if (selector[0] == '!' && stationid->selectors != NULL)
        {
          struct strnode *last = stationid->selectors;

          while (last->next != NULL)
            last = last->next;

          last->next        = newselector;
          newselector->next = NULL;
        }
        else
        {
          newselector->next    = stationid->selectors;
          stationid->selectors = newselector;
        }

        if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
          return -1;
      }
    }
    /* Otherwise add selector to global list */
    else if (OKGO)
    {
      /* Add selector to the global selectors */
      /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
      if (selector[0] == '!' && slinfo->selectors != NULL)
      {
        struct strnode *last = slinfo->selectors;

        while (last->next != NULL)
          last = last->next;

        last->next        = newselector;
        newselector->next = NULL;
      }
      else
      {
        newselector->next = slinfo->selectors;
        slinfo->selectors = newselector;
      }

      if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
        return -1;
    }
  } /* End of SELECT */

  /* DATA (v3.x and 4.0) or FETCH (v3.x) - Request data from a specific packet */
  else if (!strncasecmp (cinfo->recvbuf, "DATA", 4) ||
           (!strncasecmp (cinfo->recvbuf, "FETCH", 5) && slinfo->proto_major == 3))
  {
    /* Parse packet sequence, start and end times from request */
    starttimestr[0] = '\0';
    endtimestr[0]   = '\0';

    if (slinfo->proto_major == 4)
    {
      char seqstr[21] = {0};

      /* DATA [seq_decimal [start [end]]] */
      fields = sscanf (cinfo->recvbuf, "%*s %20s %50s %50s %c",
                       seqstr, starttimestr, endtimestr, &junk);

      if (strcmp (seqstr, "ALL") == 0)
        startpacket = RINGID_EARLIEST;
      else
        startpacket = (uint64_t)strtoull (seqstr, NULL, 10);
    }
    else /* Protocol 3.x */
    {
      uint32_t seq;

      /* DATA|FETCH [seq_hex [start]] */
      fields = sscanf (cinfo->recvbuf, "%*s %" SCNx32 " %50s %c",
                       &seq, starttimestr, &junk);

      if (cinfo->ringparams->latestid <= RINGID_MAXIMUM)
      {
        /* To map the 24-bit SeedLink v3 sequence into the 64-bit packet ID range
         * combine the highest 40-bits of latest ID with lowest 24-bits of requested sequence */
        startpacket = (cinfo->ringparams->latestid & 0xFFFFFFFFFF000000) | (seq & 0xFFFFFF);
      }
      else
      {
        startpacket = (seq & 0xFFFFFF);
      }
    }

    /* SeedLink clients resume data flow by requesting: lastpacket + 1
     * The ring needs to be positioned to the actual last packet ID for RingReadNext(),
     * so set the starting packet to the last actual packet received by the client.
     * An unfortunate side-effect: resuming from sequence 0 is not possible. */
    if (startpacket < RINGID_MAXIMUM && startpacket > 0)
      startpacket = startpacket - 1;

    /* Make sure we got no extra arguments */
    if ((slinfo->proto_major == 4 && fields > 3) ||
        (slinfo->proto_major == 3 && fields > 2))
    {
      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Too many arguments for DATA/FETCH"))
        return -1;

      OKGO = 0;
    }

    /* Convert start time string if specified */
    if (OKGO && fields == 2)
    {
      if ((starttime = ms_mdtimestr2nstime (starttimestr)) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing time in DATA|FETCH: %s",
                 cinfo->hostname, starttimestr);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing start time"))
          return -1;

        OKGO = 0;
      }
    }

    /* Convert end time string if specified */
    if (OKGO && fields == 3)
    {
      if ((endtime = ms_mdtimestr2nstime (endtimestr)) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing time in DATA|FETCH: %s",
                 cinfo->hostname, endtimestr);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing end time"))
          return -1;

        OKGO = 0;
      }
    }

    /* If configuring a specific station selection */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      if (fields >= 1)
      {
        /* Find the appropriate station ID and store the requested ID and time */
        if (!(stationid = GetReqStationID (slinfo->stations, slinfo->reqstaid)))
        {
          lprintf (0, "[%s] Error in GetReqStationID() for command DATA|FETCH",
                   cinfo->hostname);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetReqStationID()"))
            return -1;

          OKGO = 0;
        }
        else
        {
          stationid->packetid  = startpacket;
          stationid->datastart = starttime;
          stationid->starttime = starttime;
          stationid->endtime   = endtime;
        }
      }

      if (OKGO)
      {
        if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
          return -1;

        /* If any stations use FETCH the connection is dial-up */
        if (!strncasecmp (cinfo->recvbuf, "FETCH", 5))
          slinfo->dialup = 1;
      }

      cinfo->state = STATE_COMMAND;
    }
    /* Otherwise this is a request to start data flow */
    else if (OKGO)
    {
      /* If no stations yet we are in all-station mode */
      if (slinfo->stationcount == 0 && fields >= 1)
      {
        slinfo->startid  = startpacket;
        cinfo->starttime = starttime;
        cinfo->endtime   = endtime;
      }

      /* If FETCH the connection is dial-up */
      if (!strncasecmp (cinfo->recvbuf, "FETCH", 5))
        slinfo->dialup = 1;

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of DATA|FETCH */

  /* TIME (v3.x) - Request data in time window */
  else if (!strncasecmp (cinfo->recvbuf, "TIME", 4) && slinfo->proto_major == 3)
  {
    OKGO = 1;

    /* Parse start and end time from request */
    starttimestr[0] = '\0';
    endtimestr[0]   = '\0';

    /* TIME [start_time [end_time]] */
    fields = sscanf (cinfo->recvbuf, "%*s %50s %50s %c",
                     starttimestr, endtimestr, &junk);

    /* Make sure we got start time and optionally end time */
    if (fields <= 0 || fields > 2)
    {
      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "TIME command requires 1 or 2 arguments"))
        return -1;

      OKGO = 0;
    }

    /* Convert start time string */
    if (OKGO && fields >= 1)
    {
      if ((starttime = ms_mdtimestr2nstime (starttimestr)) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing start time for TIME: %s",
                 cinfo->hostname, starttimestr);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing start time"))
          return -1;

        OKGO = 0;
      }

      /* Sanity check for future start time */
      if ((time_t)MS_NSTIME2EPOCH (starttime) > time (NULL))
      {
        lprintf (0, "[%s] Start cannot be in future for TIME: %s",
                 cinfo->hostname, starttimestr);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Start time cannot be in the future"))
          return -1;

        OKGO = 0;
      }
    }

    /* Convert end time string if supplied */
    if (OKGO && fields == 2)
    {
      if ((endtime = ms_mdtimestr2nstime (endtimestr)) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing end time for TIME: %s",
                 cinfo->hostname, endtimestr);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing end time"))
          return -1;

        OKGO = 0;
      }
    }

    /* If configuring a specific station selection */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      if (fields >= 1)
      {
        /* Find the appropriate station ID and store the requested times */
        if (!(stationid = GetReqStationID (slinfo->stations, slinfo->reqstaid)))
        {
          lprintf (0, "[%s] Error in GetReqStationID() for command TIME",
                   cinfo->hostname);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetReqStationID()"))
            return -1;

          OKGO = 0;
        }
        else
        {
          stationid->starttime = starttime;
          stationid->endtime   = endtime;
        }
      }

      if (OKGO)
      {
        if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
          return -1;
      }

      cinfo->state = STATE_COMMAND;
    }
    /* Otherwise this is a request to start data flow */
    else if (OKGO)
    {
      /* If no stations yet we are in all-station mode */
      if (slinfo->stationcount == 0 && fields >= 1)
      {
        cinfo->starttime = starttime;
        cinfo->endtime   = endtime;
      }

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of TIME */

  /* END (v4.0) - Stop negotiating, send data, dial-up mode */
  else if (!strncasecmp (cinfo->recvbuf, "ENDFETCH", 9))
  {
    slinfo->dialup = 1;

    /* Trigger ring configuration and data flow */
    cinfo->state = STATE_RINGCONFIG;
  }

  /* END (v3.x and v4.0) - Stop negotiating, send data */
  else if (!strncasecmp (cinfo->recvbuf, "END", 3))
  {
    /* Trigger ring configuration and data flow */
    cinfo->state = STATE_RINGCONFIG;
  }

  /* BYE (v3.x and v4.0) - End connection */
  else if (!strncasecmp (cinfo->recvbuf, "BYE", 3))
  {
    return -1;
  }

  /* Unrecognized command */
  else
  {
    snprintf (sendbuffer, sizeof (sendbuffer),
              "Unrecognized command: %.50s", cinfo->recvbuf);

    lprintf (1, "[%s] %s", cinfo->hostname, sendbuffer);

    if (SendReply (cinfo, "ERROR", ERROR_UNSUPPORTED, sendbuffer))
      return -1;
  }

  return 0;
} /* End of HandleNegotiation */

/***************************************************************************
 * HandleInfo_v3:
 *
 * Handle SeedLink v3 INFO request.  Supported INFO levels are:
 * ID, CAPABILITIES, STATIONS, STREAMS, CONNECTIONS
 *
 * The response is an XML document encoded in one or more miniSEED records.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo_v3 (ClientInfo *cinfo)
{
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  char *xmlstr = NULL;
  int xmllength;
  char *level   = NULL;
  char errflag  = 0;

  char *record = NULL;
  int8_t swapflag;

  uint16_t year = 0;
  uint16_t yday = 0;
  uint8_t hour  = 0;
  uint8_t min   = 0;
  uint8_t sec   = 0;
  uint32_t nsec = 0;

  if (!strncasecmp (cinfo->recvbuf, "INFO", 4))
  {
    /* Set level pointer to start of level identifier */
    level = cinfo->recvbuf + 4;

    /* Skip any spaces between INFO and level identifier */
    while (*level == ' ')
      level++;
  }
  else if (*level == '\0' || *level == '\r' || *level == '\n')
  {
    lprintf (0, "[%s] HandleInfo: INFO requested without a level", cinfo->hostname);
    return -1;
  }
  else
  {
    lprintf (0, "[%s] HandleInfo cannot detect INFO", cinfo->hostname);
    return -1;
  }

  /* Allocate miniSEED record buffer */
  if ((record = calloc (1, SLINFORECSIZE)) == NULL)
  {
    lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
    return -1;
  }

  /* Parse INFO request to determine level */
  if (!strncasecmp (level, "ID", 2))
  {
    /* This is used to "ping" the server so only report at high verbosity */
    lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);

    xmlstr = info_xml_slv3_id (cinfo, SLSERVER_ID);
  }
  else if (!strncasecmp (level, "CAPABILITIES", 12))
  {
    lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);

    xmlstr = info_xml_slv3_capabilities (cinfo, SLSERVER_ID);
  }
  else if (!strncasecmp (level, "STATIONS", 8))
  {
    if (cinfo->state == STATE_STREAM)
    {
      lprintf (0, "[%s] Received mid-stream INFO STATIONS request, ignoring", cinfo->hostname);
      errflag = 1;
    }
    else
    {
      lprintf (1, "[%s] Received INFO STATIONS request", cinfo->hostname);

      xmlstr = info_xml_slv3_stations (cinfo, SLSERVER_ID, 0);
    }
  }
  else if (!strncasecmp (level, "STREAMS", 7))
  {
    if (cinfo->state == STATE_STREAM)
    {
      lprintf (0, "[%s] Received mid-stream INFO STREAMS request, ignoring", cinfo->hostname);
      errflag = 1;
    }
    else
    {
      lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);

      xmlstr = info_xml_slv3_stations (cinfo, SLSERVER_ID, 1);
    }
  }
  else if (!strncasecmp (level, "CONNECTIONS", 11))
  {
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] Refusing INFO CONNECTIONS request from un-trusted client", cinfo->hostname);
      errflag = 1;
    }
    else
    {
      lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);

      xmlstr = info_xml_slv3_connections (cinfo, SLSERVER_ID);
    }
  }
  /* Unrecognized INFO request */
  else
  {
    lprintf (0, "[%s] Unrecognized/unsupported INFO level: %s", cinfo->hostname, level);

    xmlstr = info_xml_slv3_id (cinfo, SLSERVER_ID);
    errflag = 1;
  }


  /* Pack XML into miniSEED and send to client */
  if (xmlstr)
  {
    /* Trim final newline character if present */
    xmllength = strlen (xmlstr);
    if (xmlstr[xmllength - 1] == '\n')
    {
      xmlstr[xmllength - 1] = '\0';
      xmllength--;
    }

    /* Check to see if byte swapping is needed, miniSEED 2 is written big endian */
    swapflag = (ms_bigendianhost ()) ? 0 : 1;

    ms_nstime2time (NSnow (), &year, &yday, &hour, &min, &sec, &nsec);

    /* Build Fixed Section Data Header */
    memcpy (pMS2FSDH_SEQNUM (record), "000000", 6);
    *pMS2FSDH_DATAQUALITY (record) = 'D';
    *pMS2FSDH_RESERVED (record)    = ' ';
    memcpy (pMS2FSDH_STATION (record), "INFO ", 5);
    memcpy (pMS2FSDH_LOCATION (record), "  ", 2);
    memcpy (pMS2FSDH_CHANNEL (record), (errflag) ? "ERR" : "INF", 3);
    memcpy (pMS2FSDH_NETWORK (record), "XX", 2);
    *pMS2FSDH_YEAR (record)            = HO2u (year, swapflag);
    *pMS2FSDH_DAY (record)             = HO2u (yday, swapflag);
    *pMS2FSDH_HOUR (record)            = hour;
    *pMS2FSDH_MIN (record)             = min;
    *pMS2FSDH_SEC (record)             = sec;
    *pMS2FSDH_UNUSED (record)          = 0;
    *pMS2FSDH_FSEC (record)            = 0;
    *pMS2FSDH_NUMSAMPLES (record)      = 0;
    *pMS2FSDH_SAMPLERATEFACT (record)  = 0;
    *pMS2FSDH_SAMPLERATEMULT (record)  = 0;
    *pMS2FSDH_ACTFLAGS (record)        = 0;
    *pMS2FSDH_IOFLAGS (record)         = 0;
    *pMS2FSDH_DQFLAGS (record)         = 0;
    *pMS2FSDH_NUMBLOCKETTES (record)   = 1;
    *pMS2FSDH_TIMECORRECT (record)     = 0;
    *pMS2FSDH_DATAOFFSET (record)      = HO2u (56, swapflag);
    *pMS2FSDH_BLOCKETTEOFFSET (record) = HO2u (48, swapflag);

    /* Build Blockette 1000 */
    *pMS2B1000_TYPE (record + 48)      = HO2u (1000, swapflag);
    *pMS2B1000_NEXT (record + 48)      = 0;
    *pMS2B1000_ENCODING (record + 48)  = DE_TEXT;
    *pMS2B1000_BYTEORDER (record + 48) = 1; /* 1 = big endian */
    *pMS2B1000_RECLEN (record + 48)    = 9; /* 2^9 = 512 byte record */
    *pMS2B1000_RESERVED (record + 48)  = 0;

    /* Pack all XML into 512-byte records and send to client */
    if (!cinfo->socketerr)
    {
      char seqnumstr[11];
      int seqnum = 1;
      int offset = 0;
      int nsamps;

      while (offset < xmllength && !cinfo->socketerr)
      {
        nsamps = ((xmllength - offset) > 456) ? 456 : (xmllength - offset);

        /* Update sequence number and number of samples */
        snprintf (seqnumstr, sizeof (seqnumstr), "%06d", seqnum);
        memcpy (pMS2FSDH_SEQNUM (record), seqnumstr, 6);

        *pMS2FSDH_NUMSAMPLES (record) = HO2u (nsamps, swapflag);

        /* Copy XML data into record */
        memcpy (record + 56, xmlstr + offset, nsamps);

        /* Pad any remaining record bytes with NULLs */
        if (nsamps + 56 < 512)
          memset (record + 56 + nsamps, 0, 512 - 56 - nsamps);

        /* Roll-over sequence number */
        if (seqnum >= 999999)
          seqnum = 1;
        else
          seqnum++;

        /* Update offset */
        offset += nsamps;

        /* Set termination flag if this is the last record */
        if (offset == xmllength)
          slinfo->terminfo = 1;
        else
          slinfo->terminfo = 0;

        /* Send INFO record to client, blind toss */
        SendInfoRecord (record, SLINFORECSIZE, cinfo);
      }
    }
  }

  /* Free allocated memory */
  if (xmlstr)
    free (xmlstr);

  if (record)
    free (record);

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleInfo_v3 */

/***************************************************************************
 * HandleInfo_v4:
 *
 * Handle SeedLink v4 INFO request.  Protocol INFO levels are:
 * ID, FORMATS, CAPABILITIES, STATIONS, STREAMS, CONNECTIONS.
 *
 * The response is an JSON document encoded.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo_v4 (ClientInfo *cinfo)
{
  char *json_string = NULL;
  char item[64]     = {0};
  char station[64]  = {0};
  char stream[64]   = {0};
  char *matchregex  = NULL;
  char *rejectregex = NULL;
  char junk;
  int fields;
  int errflag = 0;

  struct strnode selector = {.string = stream, .next = NULL};

  if (strncasecmp (cinfo->recvbuf, "INFO", 4) != 0)
  {
    lprintf (0, "[%s] %s() cannot detect INFO", __func__, cinfo->hostname);
    return -1;
  }

  /* Parse INFO item and optional station and stream patterns */
  fields = sscanf (cinfo->recvbuf, "%*s %63s %63s %63s %c", item, station, stream, &junk);

  if (fields == 0)
  {
    lprintf (0, "[%s] INFO requested without a level", cinfo->hostname);

    json_string = error_json (cinfo, SLSERVER_ID, "ARGUMENTS", "No INFO item specified");
    errflag = 1;
  }
  else if (!strncasecmp (item, "ID", 2))
  {
    /* This is used to "ping" the server so only report at high verbosity */
    lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);

    json_string = info_json (cinfo, SLSERVER_ID, INFO_ID, NULL);
  }
  else if (!strncasecmp (item, "CAPABILITIES", 12))
  {
    lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);

    json_string = info_json (cinfo, SLSERVER_ID, INFO_CAPABILITIES, NULL);
  }
  else if (!strncasecmp (item, "FORMATS", 7))
  {
    lprintf (1, "[%s] Received INFO FORMATS request", cinfo->hostname);

    json_string = info_json (cinfo, SLSERVER_ID, INFO_FORMATS | INFO_FILTERS, NULL);
  }
  else if (!strncasecmp (item, "STATIONS", 8))
  {
    lprintf (1, "[%s] Received INFO STATIONS request", cinfo->hostname);

    /* Configure regex for matching included station and stream patterns */
    if (station[0] != '\0')
    {
      if (StationToRegex (station, (stream[0] != '\0') ? &selector : NULL,
                          &matchregex, &rejectregex))
      {
        lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
        return -1;
      }
    }

    json_string = info_json (cinfo, SLSERVER_ID, INFO_STATIONS, matchregex);
  }
  else if (!strncasecmp (item, "STREAMS", 7))
  {
    lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);

    /* Configure regex for matching included station and stream patterns */
    if (station[0] != '\0')
    {
      if (StationToRegex (station, (stream[0] != '\0') ? &selector : NULL,
                          &matchregex, &rejectregex))
      {
        lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
        return -1;
      }
    }

    json_string = info_json (cinfo, SLSERVER_ID, INFO_STATION_STREAMS, matchregex);
  }
  else if (!strncasecmp (item, "CONNECTIONS", 11))
  {
    if (!cinfo->trusted)
    {
      lprintf (1, "[%s] Refusing INFO CONNECTIONS request from un-trusted client", cinfo->hostname);
      json_string = error_json (cinfo, SLSERVER_ID, "UNAUTHORIZED", "Client is not authorized to request connections");
      errflag = 1;
    }
    else
    {
      lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);

      json_string = info_json (cinfo, SLSERVER_ID, INFO_CONNECTIONS,
                               (station[0] != '\0') ? station : NULL);
    }
  }
  /* Unrecognized INFO request */
  else
  {
    json_string = error_json (cinfo, SLSERVER_ID, "ARGUMENTS", "Unrecognized INFO item");
    errflag = 1;

    lprintf (0, "[%s] Unrecognized INFO item: %s", cinfo->hostname, item);
  }

  /* Send INFO response to client */
  if (json_string)
  {
    SendPacket (0, json_string, strlen (json_string), "", 'J', (errflag) ? 'E' : 'I', cinfo);
  }
  else
  {
    lprintf (0, "[%s] Error creating INFO response", cinfo->hostname);
  }

  /* Free allocated memory */
  free (json_string);
  free (matchregex);
  free (rejectregex);

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleInfo_v4 */

/***************************************************************************
 * SendReply:
 *
 * Send a short reply (to a command) to the client, optionally
 * including the extended message.
 *
 * Each complete reply is terminated with a "\r\n" sequence, if
 * included the extended reply message is separated from the initial
 * reply with a single "\r" character.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendReply (ClientInfo *cinfo, char *reply, ErrorCode code, char *extreply)
{
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  char sendstr[100];
  char *codestr;

  switch (code)
  {
  case ERROR_INTERNAL:
    codestr = "INTERNAL";
    break;
  case ERROR_UNSUPPORTED:
    codestr = "UNSUPPORTED";
    break;
  case ERROR_UNEXPECTED:
    codestr = "UNEXPECTED";
    break;
  case ERROR_UNAUTHORIZED:
    codestr = "UNAUTHORIZED";
    break;
  case ERROR_LIMIT:
    codestr = "LIMIT";
    break;
  case ERROR_ARGUMENTS:
    codestr = "ARGUMENTS";
    break;
  case ERROR_AUTH:
    codestr = "AUTH";
    break;
  default:
    codestr = "UNKNOWN";
  }

  /* Create reply string to send */
  if (slinfo->proto_major == 4)
  {
    if (code != ERROR_NONE && extreply)
      snprintf (sendstr, sizeof (sendstr), "%s %s %s\r\n", reply, codestr, extreply);
    else if (code != ERROR_NONE)
      snprintf (sendstr, sizeof (sendstr), "%s %s\r\n", reply, codestr);
    else
      snprintf (sendstr, sizeof (sendstr), "%s\r\n", reply);
  }
  else
  {
    if (slinfo->extreply && extreply)
      snprintf (sendstr, sizeof (sendstr), "%s\r%s %s\r\n", reply, codestr, extreply);
    else
      snprintf (sendstr, sizeof (sendstr), "%s\r\n", reply);
  }

  /* Send the reply */
  if (SendData (cinfo, sendstr, strlen (sendstr), 0))
    return -1;

  return 0;
} /* End of SendReply() */

/***************************************************************************
 * SendPacket:
 *
 * Send 'length' bytes from 'payload' to 'cinfo->socket' and prefix
 * with an appropriate SeedLink header.
 *
 * Returns 0 on success and -1 on error, the ClientInfo.socketerr value
 * is set on socket errors.
 ***************************************************************************/
static int
SendPacket (uint64_t pktid, char *payload, uint32_t payloadlen,
            const char *staid, char format, char subformat, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  SLInfo *slinfo    = (SLInfo *)cinfo->extinfo;
  char header[40]   = {0};
  size_t headerlen  = 0;
  uint8_t l_staidlen;

  if (!payload || !vcinfo)
    return -1;

  if (slinfo->proto_major == 4) /* Create v4 header */
  {
    l_staidlen = (staid) ? (uint8_t)strlen (staid) : 0;

    /* V4 header values are little-endian byte order */
    if (ms_bigendianhost ())
    {
      ms_gswap4 (&payloadlen);
      ms_gswap8 (&pktid);
    }

    /* Construct v4 header */
    memcpy (header, "SE", 2);
    memcpy (header + 2, &format, 1);
    memcpy (header + 3, &subformat, 1);
    memcpy (header + 4, &payloadlen, 4);
    memcpy (header + 8, &pktid, 8);
    memcpy (header + 16, &l_staidlen, 1);
    memcpy (header + 17, staid, l_staidlen);

    headerlen = SLHEADSIZE_V4 + l_staidlen;
  }
  else /* Create v3 header */
  {
    /* Create v3 SeedLink header: signature + sequence number
     * Use ony the lowest 24-bits of pktid, maximum allowed in v3 sequence */
    snprintf (header, sizeof (header), "SL%06X", (uint32_t)(pktid & 0xFFFFFF));
    headerlen = SLHEADSIZE_V3;
  }

  if (SendDataMB (cinfo, (void *[]){header, payload}, (size_t[]){headerlen, payloadlen}, 2, 0))
    return -1;

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = NSnow ();

  return 0;
} /* End of SendPacket() */

/***************************************************************************
 * SendRecord:
 *
 * Send 'reclen' bytes from 'record' to 'cinfo->socket' and prefix
 * with an appropriate SeedLink header.
 *
 * Returns 0 on success and -1 on error, the ClientInfo.socketerr value
 * is set on socket errors.
 ***************************************************************************/
static int
SendRecord (RingPacket *packet, char *record, uint32_t reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  SLInfo *slinfo    = (SLInfo *)cinfo->extinfo;

  char staid[MAXSTREAMID] = {0};

  char format    = ' ';
  char subformat = 'D'; /* All miniSEED records are data/generic */

  if (!record || !vcinfo)
    return -1;

  /* Prepare details needed for v4 protocol header */
  if (slinfo->proto_major == 4)
  {
    char net[16];
    char sta[16];

    /* Extract network and station codes from FDSN Source ID (streamid) */
    if (strncmp (packet->streamid, "FDSN:", 5) == 0)
    {
      if (ms_sid2nslc (packet->streamid, net, sta, NULL, NULL))
      {
        lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, packet->streamid);
        return -1;
      }

      /* Create station ID as combination of network and station codes */
      snprintf (staid, sizeof (staid), "%s_%s", net, sta);
    }
    /* Otherwise use the stream ID as the station ID */
    else
    {
      memcpy (staid, packet->streamid, sizeof (staid));
    }

    if (MS3_ISVALIDHEADER (record))
      format = '3';
    else if (MS2_ISVALIDHEADER (record))
      format = '2';
    else
      return -1;
  }

  return SendPacket (packet->pktid, record, reclen, staid,
                     format, subformat, vcinfo);
} /* End of SendRecord() */

/***************************************************************************
 * SendInfoRecord:
 *
 * Send 'reclen' bytes from 'record' to 'cinfo->socket' and prefix
 * with an appropriate INFO SeedLink header.
 *
 * The ClientInfo.socketerr value is set on socket errors.
 ***************************************************************************/
static void
SendInfoRecord (char *record, uint32_t reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  SLInfo *slinfo    = (SLInfo *)cinfo->extinfo;
  char header[SLHEADSIZE_V3];

  if (!record || !vcinfo)
    return;

  /* Create INFO signature according to termination flag */
  if (slinfo->terminfo)
    memcpy (header, "SLINFO  ", SLHEADSIZE_V3);
  else
    memcpy (header, "SLINFO *", SLHEADSIZE_V3);

  SendDataMB (cinfo, (void *[]){header, record}, (size_t[]){SLHEADSIZE_V3, reclen}, 2, 0);

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = NSnow ();

  return;
} /* End of SendInfoRecord() */

/***************************************************************************
 * FreeReqStationID:
 *
 * Free all memory associated with a ReqStationID.
 *
 ***************************************************************************/
static void
FreeReqStationID (void *rbnode)
{
  ReqStationID *stationid = (ReqStationID *)rbnode;

  if (!rbnode)
    return;

  for (struct strnode *next, *node = stationid->selectors; node; node = next)
  {
    next = node->next;
    free (node->string);
    free (node);
  }

  free (rbnode);

  return;
} /* End of FreeReqStationID() */

/***************************************************************************
 * StaKeyCompare:
 *
 * Compare two station or channel binary tree keys passed as void pointers.
 *
 * Return 1 if a > b, -1 if a < b and 0 otherwise (e.g. equality).
 ***************************************************************************/
static int
StaKeyCompare (const void *a, const void *b)
{
  int cmpval;

  /* Compare station IDs */
  cmpval = strcmp (a, b);

  if (cmpval > 0)
    return 1;
  else if (cmpval < 0)
    return -1;

  return 0;
} /* End of StaKeyCompare() */

/***************************************************************************
 * GetReqStationID:
 *
 * Search the specified binary tree for a given entry.  If the entry does not
 * exist create it and add it to the tree.
 *
 * Return a pointer to the entry or 0 for error.
 ***************************************************************************/
static ReqStationID *
GetReqStationID (RBTree *tree, char *staid)
{
  char *newkey            = NULL;
  ReqStationID *stationid = NULL;
  RBNode *rbnode;

  /* Search for a matching entry */
  if ((rbnode = RBFind (tree, staid)))
  {
    stationid = (ReqStationID *)rbnode->data;
  }
  else
  {
    if ((newkey = strdup (staid)) == NULL)
    {
      lprintf (0, "%s: Error allocating new key", __func__);
      return 0;
    }

    if ((stationid = (ReqStationID *)malloc (sizeof (ReqStationID))) == NULL)
    {
      lprintf (0, "%s: Error allocating new node", __func__);
      return 0;
    }

    stationid->starttime = NSTUNSET;
    stationid->endtime   = NSTUNSET;
    stationid->packetid  = RINGID_NONE;
    stationid->datastart = NSTUNSET;
    stationid->selectors = NULL;

    RBTreeInsert (tree, newkey, stationid, 0);
  }

  return stationid;
} /* End of GetReqStationID() */

/***************************************************************************
 * StationToRegex:
 *
 * Update match and reject regexes for the specified station ID
 * and (comma delimited) selector list.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
StationToRegex (const char *staid, struct strnode *selector,
                char **matchregex, char **rejectregex)
{
  int matched = 0;

  if (!matchregex || !rejectregex)
  {
    lprintf (0, "Pointer-to-pointer match/reject regex cannot be NULL");
    return -1;
  }

  /* If a selector list is specified update regexes */
  if (selector)
  {
    for (; selector != NULL; selector = selector->next)
    {
      /* Handle negated selector */
      if (selector->string[0] == '!')
      {
        /* If no matching (non-negated) selectors are included a negation selector
           implies all data for the specified station with the execption of the
           negated selection, therefore we need to match all channels from the
           station and then reject those in the negated selector */
        if (!matched && staid)
        {
          if (SelectToRegex (staid, NULL, matchregex))
          {
            lprintf (0, "Error with SelectToRegex");
            return -1;
          }

          matched++;
        }

        if (SelectToRegex (staid, &(selector->string[1]), rejectregex))
        {
          lprintf (0, "Error with SelectToRegex");
          return -1;
        }
      }
      /* Handle regular selector */
      else
      {
        if (SelectToRegex (staid, selector->string, matchregex))
        {
          lprintf (0, "Error with SelectToRegex");
          return -1;
        }

        matched++;
      }
    }
  }
  /* Otherwise update regex for station without selectors */
  else
  {
    if (SelectToRegex (staid, NULL, matchregex))
    {
      lprintf (0, "Error with SelectToRegex");
      return -1;
    }
  }

  return 0;
} /* End of StationToRegex() */

/***************************************************************************
 * SelectToRegex:
 *
 * Create a regular expression for ring stream IDs (NET_STA_LOC_CHAN)
 * for the specified network, station and SeedLink selector and add it
 * to the string specified by regex, expanding it as needed.  The
 * regex string will only be expanded up to a maximum of SLMAXREGEXLEN
 * bytes.
 *
 * Each regex in the final string is separated with a "|" (OR) and
 * encapsulated with begin "^" and end "$" characters.
 *
 * Mapping is as follows:
 *   '?' -> '.'
 *   '*' -> '.*'
 *
 * The DECOTL subtypes of SeedLink selectors are not supported,
 * anything following a '.' in a selector will be ignored.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
SelectToRegex (const char *staid, const char *select, char **regex)
{
  const char *ptr;
  char pattern[200] = {0};
  char *build       = pattern;
  int retval;

  if (!regex)
    return -1;

  /* Sanity check lengths of input strings */
  if (staid && strlen (staid) > 50)
    return -1;
  if (select && strlen (select) > 50)
    return -1;

  /* Add starting '^' anchor and FDSN Source ID prefix */
  memcpy (build, "^(?:FDSN:)?", 11);
  build += 11;

  /* Copy station pattern if provided, translating globbing wildcards to regex */
  if (staid)
  {
    for (ptr = staid; *ptr; ptr++)
    {
      if (*ptr == '?')
      {
        *build++ = '.';
      }
      else if (*ptr == '*')
      {
        *build++ = '.';
        *build++ = '*';
      }
      else
      {
        *build++ = *ptr;
      }
    }
  }
  /* Otherwise add wildcard */
  else
  {
    *build++ = '.';
    *build++ = '*';
  }

  /* Add separator */
  *build++ = '_';

  /* Copy stream pattern if provided, translating globbing wildcards to regex */
  if (select)
  {
    /* Skip '-' at the beginning of the selector representing empty location codes */
    while (*select == '-')
      select++;

    for (ptr = select; *ptr; ptr++)
    {
      if (*ptr == '?')
      {
        *build++ = '.';
      }
      else if (*ptr == '*')
      {
        *build++ = '.';
        *build++ = '*';
      }
      else
      {
        *build++ = *ptr;
      }
    }
  }
  /* Otherwise add wildcard if station ID was added */
  else
  {
    *build++ = '.';
    *build++ = '*';
  }

  /* Finish with optional /MSEED suffix and 2 or 3 annotation and a '$' anchor */
  memcpy (build, "(?:/MSEED)?[23]?$", 17);

  /* Add new pattern to regex string, expanding as needed up to SLMAXREGEXLEN bytes*/
  if ((retval = AddToString (regex, pattern, "|", 0, SLMAXREGEXLEN)))
  {
    if (retval == -1)
    {
      lprintf (0, "Cannot allocate memory");
      return -1;
    }
    if (retval == -2)
    {
      lprintf (0, "AddToString would grow regex beyond maximum length");
      return -1;
    }
    else
    {
      lprintf (0, "Error with AddToString in SelectToRegex");
      return -1;
    }
  }

  return 0;
} /* End of SelectToRegex() */

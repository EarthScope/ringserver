/**************************************************************************
 * slclient.c
 *
 * SeedLink client thread specific routines.
 *
 * Mapping SeedLink sequence numbers <-> ring packet ID's:
 *
 * SeedLink sequence numbers are mapped directly to ring packet IDs.
 * With a large ring it is possible to have more IDs than fit into the
 * SeedLink sequence number address space (6 hexidecimal numbers,
 * maximum of 0xFFFFFF which is 16,777,215), an error will be logged
 * when IDs are encountered that are too large to map into a sequence
 * number.
 *
 * The SeedLink protocol is designed around a multi-ring backend
 * whereas the ringserver implements a single ring.  This is important
 * because a client will request data from network-station rings and
 * optionally specify starting sequence numbers for each ring to
 * resume a connection.  This is handled in this client by determining
 * the most recent packet ID requested by the client and setting the
 * ring to that position.
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
 * Copyright (C) 2023:
 * @author Chad Trabant, IRIS Data Management Center
 **************************************************************************/

/* Unsupported protocol features:
 * CAT listing (oh the irony)
 * INFO GAPS
 * INFO ALL
 */

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
#include <mxml.h>

#include "clients.h"
#include "generic.h"
#include "http.h"
#include "logging.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"
#include "slclient.h"

/* Define list of valid characters for selectors and station & network codes */
#define VALIDSELECTCHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?!-"
#define VALIDSTAIDCHARS  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo);
static int HandleInfo (ClientInfo *cinfo);
static int SendReply (ClientInfo *cinfo, char *reply, ErrorCode code, char *extreply);
static int SendRecord (RingPacket *packet, char *record, int reclen,
                       void *vcinfo);
static void SendInfoRecord (char *record, int reclen, void *vcinfo);
static void FreeStaNode (void *rbnode);
static void FreeNetStaNode (void *rbnode);
static int StaKeyCompare (const void *a, const void *b);
static SLStaNode *GetStaNode (RBTree *tree, char *staid);
static SLNetStaNode *GetNetStaNode (RBTree *tree, char *staid);
static int StationToRegex (const char *net_sta, const char *selectors,
                           char **matchregex, char **rejectregex);
static int SelectToRegex (const char *net_sta, const char *select,
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
  SLStaNode *stanode;
  int64_t readid;
  int64_t retval;

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

    slinfo->stations = RBTreeCreate (StaKeyCompare, free, FreeStaNode);
  }

  slinfo = (SLInfo *)cinfo->extinfo;

  /* Determine if this is an INFO request and handle */
  if (!strncasecmp (cinfo->recvbuf, "INFO", 4))
  {
    if (HandleInfo (cinfo))
    {
      return -1;
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
      nstime_t newesttime = 0;

      stack = StackCreate ();
      RBBuildStack (slinfo->stations, stack);

      while ((rbnode = (RBNode *)StackPop (stack)))
      {
        stanode = (SLStaNode *)rbnode->data;

        /* Configure regexes for this station */
        if (StationToRegex ((const char *)rbnode->key, stanode->selectors,
                            &(cinfo->matchstr), &(cinfo->rejectstr)))
        {
          lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
          SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
          return -1;
        }

        /* Track the widest time window requested */

        /* Set or expand the global starttime */
        if (stanode->starttime != NSTERROR)
        {
          if (!cinfo->starttime)
            cinfo->starttime = stanode->starttime;
          else if (cinfo->starttime > stanode->starttime)
            cinfo->starttime = stanode->starttime;
        }

        /* Set or expand the global endtime */
        if (stanode->endtime != NSTERROR)
        {
          if (!cinfo->endtime)
            cinfo->endtime = stanode->endtime;
          else if (cinfo->endtime < stanode->endtime)
            cinfo->endtime = stanode->endtime;
        }

        /* Track the newest packet ID while validating their existence */
        if (stanode->packetid)
          retval = RingRead (cinfo->reader, stanode->packetid, &cinfo->packet, 0);
        else
          retval = 0;

        /* Requested packet must be valid and have a matching data start time
         * Limit packet time matching to integer seconds to match SeedLink syntax limits */
        if (retval == stanode->packetid &&
            (stanode->datastart == NSTERROR ||
             (int64_t)(MS_NSTIME2EPOCH (stanode->datastart)) == (int64_t)(MS_NSTIME2EPOCH (cinfo->packet.datastart))))
        {
          /* Use this packet ID if it is newer than any previous newest */
          if (newesttime == 0 || cinfo->packet.pkttime > newesttime)
          {
            slinfo->startid = stanode->packetid;
            newesttime = cinfo->packet.pkttime;
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
      retval = RingPosition (cinfo->reader, slinfo->startid, NSTERROR);

      if (retval < 0)
      {
        lprintf (0, "[%s] Error with RingPosition for %" PRId64,
                 cinfo->hostname, slinfo->startid);
        return -1;
      }
      else if (retval == 0)
      {
        lprintf (0, "[%s] Could not find and position to packet ID: %" PRId64,
                 cinfo->hostname, slinfo->startid);
      }
      else
      {
        lprintf (2, "[%s] Positioned ring to packet ID: %" PRId64,
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
    if (cinfo->starttime && cinfo->starttime != NSTERROR && !slinfo->startid)
    {
      char timestr[31];

      ms_nstime2timestrz (cinfo->starttime, timestr, ISOMONTHDAY, NANO_MICRO_NONE);
      readid = 0;

      /* Position ring according to start time, use reverse search if limited */
      if (cinfo->timewinlimit == 1.0)
      {
        readid = RingAfter (cinfo->reader, cinfo->starttime, 0);
      }
      else if (cinfo->timewinlimit < 1.0)
      {
        int64_t pktlimit = (int64_t)(cinfo->timewinlimit * cinfo->ringparams->maxpackets);

        readid = RingAfterRev (cinfo->reader, cinfo->starttime, pktlimit, 0);
      }
      else
      {
        lprintf (0, "Time window search limit is invalid: %f", cinfo->timewinlimit);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "time window search limit is invalid");
        return -1;
      }

      if (readid < 0)
      {
        lprintf (0, "[%s] Error with RingAfter time: %s [%" PRId64 "]",
                 cinfo->hostname, timestr, cinfo->starttime);
        SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error positioning reader to start of time window");
        return -1;
      }

      if (readid == 0)
      {
        lprintf (2, "[%s] No packet found for RingAfter time: %s [%" PRId64 "], positioning to next packet",
                 cinfo->hostname, timestr, cinfo->starttime);
        cinfo->reader->pktid = RINGNEXT;
      }
      else
      {
        lprintf (2, "[%s] Positioned to packet %" PRId64 ", first after: %s",
                 cinfo->hostname, readid, timestr);
      }
    }

    /* Set read position to next packet if not already done */
    if (cinfo->reader->pktid == 0)
    {
      cinfo->reader->pktid = RINGNEXT;
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
  int64_t readid;
  int skiprecord = 0;
  int newstream;

  if (!cinfo || !cinfo->extinfo)
    return -1;

  slinfo = (SLInfo *)cinfo->extinfo;

  /* Read next packet from ring */
  readid = RingReadNext (cinfo->reader, &cinfo->packet, cinfo->packetdata);

  if (readid < 0)
  {
    lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
    return -1;
  }
  else if (readid > 0 &&
           (MS2_ISVALIDHEADER (cinfo->packetdata) ||
            MS3_ISVALIDHEADER (cinfo->packetdata)))
  {
    lprintf (3, "[%s] Read %s (%u bytes) packet ID %" PRId64 " from ring",
             cinfo->hostname, cinfo->packet.streamid, cinfo->packet.datasize, cinfo->packet.pktid);

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

    /* Perform time-windowing end time checks */
    if (cinfo->endtime != 0 && cinfo->endtime != NSTERROR)
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
        SendData (cinfo, "END", 3);
        return -1;
      }
    }

    /* If not skipping this record send to the client and update byte count */
    if (!skiprecord)
    {
      /* Send miniSEED record to client */
      if (SendRecord (&cinfo->packet, cinfo->packetdata, cinfo->packet.datasize, cinfo))
      {
        if (cinfo->socketerr != 2)
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
      readid = 0;
    }
  }
  /* If in dial-up mode check if we are at the end of the ring */
  else if (readid == 0 && slinfo->dialup)
  {
    lprintf (2, "[%s] Dial-up mode reached end of buffer", cinfo->hostname);
    SendData (cinfo, "END", 3);
    return -1;
  }

  return (readid) ? cinfo->packet.datasize : 0;
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

  RBTreeDestroy (slinfo->stations);
  slinfo->stations = 0;
  free (slinfo);

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
  SLStaNode *stanode;
  int fields;

  nstime_t starttime = NSTERROR;
  nstime_t endtime = NSTERROR;
  char starttimestr[51];
  char endtimestr[51];
  char pattern[9];
  int64_t startpacket = -1;

  char *ptr;
  char OKGO = 1;
  char junk;

  if (!cinfo || !cinfo->extinfo)
    return -1;

  slinfo = (SLInfo *)cinfo->extinfo;

  fprintf (stderr, "DEBUG %s: received '%s'\n", __func__, cinfo->recvbuf);

  /* HELLO (v3.x and v4.0) - Return server version and ID */
  if (!strncasecmp (cinfo->recvbuf, "HELLO", 5))
  {
    int bytes;

    /* Create and send server version information */
    bytes = snprintf (sendbuffer, sizeof (sendbuffer),
                      SLSERVERVER "\r\n%s\r\n", serverid);

    if (bytes >= sizeof (sendbuffer))
    {
      lprintf (0, "[%s] Response to HELLO is likely truncated: '%*s'",
               cinfo->hostname, (int)sizeof (sendbuffer), sendbuffer);
    }

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer)))
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

  /* GETCAPABILITIES (v4.0) - Return capabilities */
  else if (!strncasecmp (cinfo->recvbuf, "GETCAPABILITIES", 15))
  {
    lprintf (2, "[%s] Received GETCAPABILITIES", cinfo->hostname);

    snprintf (sendbuffer, sizeof (sendbuffer),
              "%s\r\n",
              SLCAPABILITIESv4);

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer)))
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

  /* CAT (v3.x) - Return ASCII list of stations */
  else if (!strncasecmp (cinfo->recvbuf, "CAT", 3))
  {
    snprintf (sendbuffer, sizeof (sendbuffer),
              "CAT command not implemented\r\n");

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer)))
      return -1;
  }

  /* BATCH (v3.x) - Batch mode for subsequent commands */
  else if (!strncasecmp (cinfo->recvbuf, "BATCH", 5))
  {
    slinfo->batch = 1;

    if (SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* STATION (v3.x and v4.0) - Select specified network and station */
  else if (!strncasecmp (cinfo->recvbuf, "STATION", 7))
  {
    OKGO = 1;

    /* Parse station ID from request */
    slinfo->reqstaid[0] = '\0';

    if (slinfo->proto_major == 4)
    {
      /* STATION stationID */
      fields = sscanf (cinfo->recvbuf, "%*s %20s %c", slinfo->reqstaid, &junk);
      slinfo->reqstaid[sizeof(slinfo->reqstaid) - 1] = '\0';

      /* Make sure we got a station code and optionally a network code */
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

      /* Combine network and station into station ID */
      snprintf (slinfo->reqstaid, sizeof (slinfo->reqstaid), "%s_%s", reqnet, reqsta);
    }

    fprintf (stderr, "DEBUG staid: '%s'", slinfo->reqstaid);

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
      if (!GetStaNode (slinfo->stations, slinfo->reqstaid))
      {
        lprintf (0, "[%s] Error in GetStaNode for command STATION", cinfo->hostname);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetStaNode()"))
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
    pattern[0] = '\0';
    fields = sscanf (cinfo->recvbuf, "%*s %8s %c", pattern, &junk);

    /* Make sure we got a single pattern */
    if (fields != 1)
    {
      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "SELECT requires a single argument"))
        return -1;

      OKGO = 0;
    }

    /* Truncate pattern at a '.', DECTOL subtypes are accepted but not supported */
    if (OKGO && (ptr = strrchr (pattern, '.')))
      *ptr = '\0';

    /* Sanity check, only allowed characters */
    if (OKGO && strspn (pattern, VALIDSELECTCHARS) != strlen (pattern))
    {
      lprintf (0, "[%s] Error, select pattern contains illegal characters: '%s'",
               cinfo->hostname, pattern);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Selector contains illegal characters"))
        return -1;

      OKGO = 0;
    }

    /* Sanity check, pattern can only be [!][LL][CCC], 2, 3, 4, 5 or 6 characters */
    if (OKGO && (strlen (pattern) < 2 || strlen (pattern) > 6))
    {
      lprintf (0, "[%s] Error, selector not 2-6 characters: %s",
               cinfo->hostname, pattern);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Selector must be 2-6 characters"))
        return -1;

      OKGO = 0;
    }

    /* If modifying a STATION add selector to it's Node */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      /* Find the appropriate StaNode */
      if (!(stanode = GetStaNode (slinfo->stations, slinfo->reqstaid)))
      {
        lprintf (0, "[%s] Error in GetStaNode for command SELECT", cinfo->hostname);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetStaNode()"))
          return -1;
      }
      else
      {
        /* Add selector to the StaNode.selectors, maximum of SLMAXSELECTLEN bytes */
        /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
        if (AddToString (&(stanode->selectors), pattern, ",", (pattern[0] == '!') ? 0 : 1, SLMAXSELECTLEN))
        {
          lprintf (0, "[%s] Error for command SELECT (cannot AddToString), too many selectors for %s",
                   cinfo->hostname, slinfo->reqstaid);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Too many selectors for this station"))
            return -1;
        }
        else
        {
          if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
            return -1;
        }
      }
    }
    /* Otherwise add selector to global list */
    else if (OKGO)
    {
      /* Add selector to the SLStaNode.selectors, maximum of SLMAXSELECTLEN bytes */
      /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
      if (AddToString (&(slinfo->selectors), pattern, ",", (pattern[0] == '!') ? 0 : 1, SLMAXSELECTLEN))
      {
        lprintf (0, "[%s] Error for command SELECT (cannot AddToString), too many global selectors",
                 cinfo->hostname);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Too many global selectors"))
          return -1;
      }
      else
      {
        if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
          return -1;
      }
    }
  } /* End of SELECT */

  /* DATA|FETCH (v3.x and 4.0) - Request data from a specific packet */
  else if (!strncasecmp (cinfo->recvbuf, "DATA", 4) ||
           !strncasecmp (cinfo->recvbuf, "FETCH", 5))
  {
    /* Parse packet sequence, start and end times from request */
    starttimestr[0] = '\0';
    endtimestr[0] = '\0';

    if (slinfo->proto_major == 4)
    {
      /* DATA|FETCH [seq_decimal [start [end]]] */
      fields = sscanf (cinfo->recvbuf, "%*s %" SCNd64 " %50s %50s %c",
                       &startpacket, starttimestr, endtimestr, &junk);
    }
    else /* Protocol 3.x */
    {
      unsigned int seq;

      /* DATA|FETCH [seq_hex [start]] */
      fields = sscanf (cinfo->recvbuf, "%*s %x %50s %c",
                       &seq, starttimestr, &junk);

      startpacket = seq;
    }

    /* SeedLink clients resume data flow by requesting: lastpacket + 1
     * The ring needs to be positioned to the actual last packet ID for RINGNEXT,
     * so set the starting packet to the last actual packet received by the client. */
    if (startpacket >= 0)
      startpacket = (startpacket == 1) ? cinfo->ringparams->maxpktid : (startpacket - 1);

    /* Make sure we got no extra arguments */
    if ((slinfo->proto_major == 3 && fields > 2) ||
        (slinfo->proto_major == 4 && fields > 3))
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
        /* Find the appropriate SLStaNode and store the requested ID and time */
        if (!(stanode = GetStaNode (slinfo->stations, slinfo->reqstaid)))
        {
          lprintf (0, "[%s] Error in GetStaNode for command DATA|FETCH",
                   cinfo->hostname);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetStaNode()"))
            return -1;

          OKGO = 0;
        }
        else
        {
          stanode->packetid = startpacket;
          stanode->datastart = starttime;
          stanode->starttime = starttime;
          stanode->endtime = endtime;
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
        slinfo->startid = startpacket;
        cinfo->starttime = starttime;
        cinfo->endtime = endtime;
      }

      /* If FETCH the connection is dial-up */
      if (!strncasecmp (cinfo->recvbuf, "FETCH", 5))
        slinfo->dialup = 1;

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of DATA|FETCH */

  /* TIME (v3.x) - Request data in time window */
  else if (!strncasecmp (cinfo->recvbuf, "TIME", 4))
  {
    OKGO = 1;

    /* Parse start and end time from request */
    starttimestr[0] = '\0';
    endtimestr[0] = '\0';

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
        /* Find the appropriate SLStaNode and store the requested times */
        if (!(stanode = GetStaNode (slinfo->stations, slinfo->reqstaid)))
        {
          lprintf (0, "[%s] Error in GetStaNode for command TIME",
                   cinfo->hostname);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetStaNode()"))
            return -1;

          OKGO = 0;
        }
        else
        {
          stanode->starttime = starttime;
          stanode->endtime = endtime;
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
        cinfo->endtime = endtime;
      }

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of TIME */

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
 * HandleInfo:
 *
 * Handle SeedLink INFO request.  Protocol INFO levels are:
 * ID, CAPABILITIES, STATIONS, STREAMS, GAPS, CONNECTIONS, ALL
 *
 * Levels GAPS and ALL are not supported.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo (ClientInfo *cinfo)
{
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  mxml_node_t *xmldoc = 0;
  mxml_node_t *seedlink = 0;
  char string[200];
  char *xmlstr = 0;
  int xmllength;
  char *level = 0;
  int infolevel = 0;
  char errflag = 0;

  char *record = 0;
  int8_t swapflag;

  uint16_t year = 0;
  uint16_t yday = 0;
  uint8_t hour = 0;
  uint8_t min = 0;
  uint8_t sec = 0;
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
    lprintf (0, "[%s] HandleInfo: INFO specified without a level", cinfo->hostname);
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

  /* Initialize the XML response structure */
  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    lprintf (0, "[%s] Error creating XML document", cinfo->hostname);
    if (record)
      free (record);
    return -1;
  }

  /* Create seedlink XML element */
  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    lprintf (0, "[%s] Error creating seedlink XML element", cinfo->hostname);
    if (xmldoc)
      mxmlRelease (xmldoc);
    if (record)
      free (record);
    return -1;
  }

  /* Convert server start time to YYYY-MM-DD HH:MM:SSZ */
  ms_nstime2timestrz (serverstarttime, string, ISOMONTHDAY, NONE);

  /* All responses, even the error response contain these attributes */
  mxmlElementSetAttr (seedlink, "software", SLSERVERVER);
  mxmlElementSetAttr (seedlink, "organization", serverid);
  mxmlElementSetAttr (seedlink, "started", string);

  /* Parse INFO request to determine level */
  if (!strncasecmp (level, "ID", 2))
  {
    /* This is used to "ping" the server so only report at high verbosity */
    lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);
    infolevel = SLINFO_ID;
  }
  else if (!strncasecmp (level, "CAPABILITIES", 12))
  {
    lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);
    infolevel = SLINFO_CAPABILITIES;
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
      infolevel = SLINFO_STATIONS;
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
      infolevel = SLINFO_STREAMS;
    }
  }
  else if (!strncasecmp (level, "GAPS", 7))
  {
    lprintf (1, "[%s] Received INFO GAPS request, unsupported", cinfo->hostname);
    errflag = 1;
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
      infolevel = SLINFO_CONNECTIONS;
    }
  }
  else if (!strncasecmp (level, "ALL", 3))
  {
    lprintf (1, "[%s] Received INFO ALL request, unsupported", cinfo->hostname);
    errflag = 1;
  }
  /* Unrecognized INFO request */
  else
  {
    lprintf (0, "[%s] Unrecognized INFO level: %s", cinfo->hostname, level);
    errflag = 1;
  }

  /* Add contents to the XML structure depending on info level */

  /* CAPABILITIES */
  if (infolevel == SLINFO_CAPABILITIES)
  {
    int idx;
    mxml_node_t *capability;
    char *caps[10] = {"dialup", "multistation", "window-extraction", "info:id",
                      "info:capabilities", "info:stations", "info:streams",
                      "info:gaps", "info:connections", "info:all"};

    lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);
    infolevel = SLINFO_CAPABILITIES;

    for (idx = 0; idx < 10; idx++)
    {
      if (!(capability = mxmlNewElement (seedlink, "capability")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        errflag = 1;
      }

      mxmlElementSetAttr (capability, "name", caps[idx]);
    }
  } /* End of CAPABILITIES request processing */
  /* STATIONS */
  else if (infolevel == SLINFO_STATIONS)
  {
    mxml_node_t *station;
    Stack *streams;
    RingStream *stream;

    RBTree *netsta;
    Stack *netstas;
    SLNetStaNode *netstanode;
    RBNode *tnode;

    char net[10];
    char sta[10];
    char staid[22];

    /* Get copy of streams as a Stack */
    if (!(streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)))
    {
      lprintf (0, "[%s] Error getting streams", cinfo->hostname);
      errflag = 1;
    }
    else
    {
      netsta = RBTreeCreate (StaKeyCompare, free, FreeNetStaNode);

      /* Loop through the streams and build a network-station tree */
      while ((stream = (RingStream *)StackPop (streams)))
      {
        /* Split the streamid to get the network and station codes,
           assumed stream pattern: "NET_STA_LOC_CHAN/MSEED" */
        if (SplitStreamID (stream->streamid, '_', 10, net, sta, NULL, NULL, NULL, NULL, NULL) != 2)
        {
          lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
          return -1;
        }

        /* Combine network and station into station ID */
        snprintf (staid, sizeof (staid), "%s_%s", net, sta);

        /* Find or create new netsta entry */
        netstanode = GetNetStaNode (netsta, staid);

        /* Check and update network-station values */
        if (netstanode)
        {
          strncpy (netstanode->net, net, sizeof (netstanode->net) - 1);
          strncpy (netstanode->sta, sta, sizeof (netstanode->sta) - 1);

          if (!netstanode->earliestdstime || netstanode->earliestdstime > stream->earliestdstime)
          {
            netstanode->earliestdstime = stream->earliestdstime;
            netstanode->earliestid = stream->earliestid;
          }
          if (!netstanode->latestdstime || netstanode->latestdstime < stream->latestdstime)
          {
            netstanode->latestdstime = stream->latestdstime;
            netstanode->latestid = stream->latestid;
          }
        }
        else
        {
          lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
          return -1;
        }

        /* Free the popped Stack entry */
        free (stream);
      }

      /* Free the remaining stream Stack memory */
      StackDestroy (streams, free);

      /* Create Stack of network-station entries */
      netstas = StackCreate ();
      RBBuildStack (netsta, netstas);

      /* Loop through array entries adding "station" elements */
      while ((tnode = (RBNode *)StackPop (netstas)))
      {
        netstanode = (SLNetStaNode *)tnode->data;

        if (!(station = mxmlNewElement (seedlink, "station")))
        {
          lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
          errflag = 1;
        }
        else
        {
          mxmlElementSetAttr (station, "name", netstanode->sta);
          mxmlElementSetAttr (station, "network", netstanode->net);
          mxmlElementSetAttrf (station, "description", "%s Station", netstanode->net);
          mxmlElementSetAttrf (station, "begin_seq", "%06" PRIX64, netstanode->earliestid);
          mxmlElementSetAttrf (station, "end_seq", "%06" PRIX64, netstanode->latestid);
        }
      }

      /* Cleanup network-station structures */
      RBTreeDestroy (netsta);
      StackDestroy (netstas, 0);
    }
  } /* End of STATIONS request processing */
  /* STREAMS */
  else if (infolevel == SLINFO_STREAMS)
  {
    mxml_node_t *station;
    mxml_node_t *streamxml;
    Stack *streams;
    RingStream *stream;

    RBTree *netsta;
    Stack *netstas;
    SLNetStaNode *netstanode;
    RBNode *tnode;

    char net[10];
    char sta[10];
    char loc[10];
    char chan[10];
    char staid[22];

    /* Get streams as a Stack (this is copied data) */
    if (!(streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)))
    {
      lprintf (0, "[%s] Error getting streams", cinfo->hostname);
      errflag = 1;
    }
    else
    {
      netsta = RBTreeCreate (StaKeyCompare, free, FreeNetStaNode);

      /* Loop through the streams and build a network-station tree with associated streams */
      while ((stream = (RingStream *)StackPop (streams)))
      {
        /* Split the streamid to get the network and station codes,
           assumed stream pattern: "NET_STA_LOC_CHAN/MSEED" */
        if (SplitStreamID (stream->streamid, '_', 10, net, sta, NULL, NULL, NULL, NULL, NULL) != 2)
        {
          lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
          return -1;
        }

        /* Combine network and station into station ID */
        snprintf (staid, sizeof (staid), "%s_%s", net, sta);

        /* Find or create new netsta entry */
        netstanode = GetNetStaNode (netsta, staid);

        if (netstanode)
        {
          /* Add stream to associated streams stack */
          StackUnshift (netstanode->streams, stream);

          strncpy (netstanode->net, net, sizeof (netstanode->net) - 1);
          strncpy (netstanode->sta, sta, sizeof (netstanode->sta) - 1);

          /* Check and update network-station earliest/latest values */
          if (!netstanode->earliestdstime || netstanode->earliestdstime > stream->earliestdstime)
          {
            netstanode->earliestdstime = stream->earliestdstime;
            netstanode->earliestid = stream->earliestid;
          }
          if (!netstanode->latestdstime || netstanode->latestdstime < stream->latestdstime)
          {
            netstanode->latestdstime = stream->latestdstime;
            netstanode->latestid = stream->latestid;
          }
        }
        else
        {
          lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
          return -1;
        }
      }

      /* Create Stack of network-station entries */
      netstas = StackCreate ();
      RBBuildStack (netsta, netstas);

      /* Traverse network-station entries creating "station" elements */
      while ((tnode = (RBNode *)StackPop (netstas)))
      {
        netstanode = (SLNetStaNode *)tnode->data;

        if (!(station = mxmlNewElement (seedlink, "station")))
        {
          lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
          errflag = 1;
        }
        else
        {
          mxmlElementSetAttr (station, "name", netstanode->sta);
          mxmlElementSetAttr (station, "network", netstanode->net);
          mxmlElementSetAttrf (station, "description", "%s Station", netstanode->net);
          mxmlElementSetAttrf (station, "begin_seq", "%06" PRIX64, netstanode->earliestid);
          mxmlElementSetAttrf (station, "end_seq", "%06" PRIX64, netstanode->latestid);
          mxmlElementSetAttr (station, "stream_check", "enabled");

          /* Traverse associated streams to find locations and channels creating "stream" elements */
          while ((stream = (RingStream *)StackPop (netstanode->streams)))
          {
            /* Split the streamid to get the network, station, location & channel codes
               assumed stream pattern: "NET_STA_LOC_CHAN/MSEED" */
            if (SplitStreamID (stream->streamid, '_', 10, net, sta, loc, chan, NULL, NULL, NULL) != 4)
            {
              lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
              return -1;
            }

            if (!(streamxml = mxmlNewElement (station, "stream")))
            {
              lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
              errflag = 1;
            }
            else
            {
              mxmlElementSetAttr (streamxml, "location", loc);
              mxmlElementSetAttr (streamxml, "seedname", chan);
              mxmlElementSetAttr (streamxml, "type", "D");

              /* Convert earliest and latest times to YYYY-MM-DDTHH:MM:SSZ and add them */
              ms_nstime2timestrz (stream->earliestdstime, string, ISOMONTHDAY, NONE);
              mxmlElementSetAttr (streamxml, "begin_time", string);
              ms_nstime2timestrz (stream->latestdetime, string, ISOMONTHDAY, NONE);
              mxmlElementSetAttr (streamxml, "end_time", string);
            }

            /* Free the RingStream entry, this is a copy from GetStreamsStack() above */
            free (stream);
          }
        }
      }

      /* Cleanup network-station structures */
      RBTreeDestroy (netsta);
      StackDestroy (netstas, 0);
      StackDestroy (streams, 0);
    }
  } /* End of STREAMS request processing */
  /* CONNECTIONS */
  else if (infolevel == SLINFO_CONNECTIONS)
  {
    struct cthread *loopctp;
    mxml_node_t *station, *connection, *window, *selector;
    ClientInfo *tcinfo;
    SLInfo *tslinfo;

    /* Loop through client connections, lock client list while looping  */
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

      tcinfo = (ClientInfo *)loopctp->td->td_prvtptr;
      tslinfo = (tcinfo->type == CLIENT_SEEDLINK) ? (SLInfo *)tcinfo->extinfo : 0;

      if (!(station = mxmlNewElement (seedlink, "station")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        errflag = 1;
      }
      else
      {
        mxmlElementSetAttr (station, "name", "CLIENT");
        if (tcinfo->type == CLIENT_DATALINK)
          mxmlElementSetAttr (station, "network", "DL");
        else if (tcinfo->type == CLIENT_SEEDLINK)
          mxmlElementSetAttr (station, "network", "SL");
        else
          mxmlElementSetAttr (station, "network", "RS");
        mxmlElementSetAttr (station, "description", "Ringserver Client");
        mxmlElementSetAttrf (station, "begin_seq", "%06" PRIX64, tcinfo->ringparams->earliestid);
        mxmlElementSetAttrf (station, "end_seq", "%06" PRIX64, tcinfo->ringparams->latestid);
        mxmlElementSetAttr (station, "stream_check", "enabled");

        /* Add a "connection" element */
        if (!(connection = mxmlNewElement (station, "connection")))
        {
          lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
          errflag = 1;
        }
        else
        {
          mxmlElementSetAttr (connection, "host", tcinfo->ipstr);
          mxmlElementSetAttr (connection, "port", tcinfo->portstr);

          /* Convert connect time to YYYY-MM-DDTHH:MM:SSZ */
          ms_nstime2timestrz (tcinfo->conntime, string, ISOMONTHDAY, NONE);
          mxmlElementSetAttr (connection, "ctime", string);
          mxmlElementSetAttr (connection, "begin_seq", "0");

          if (tcinfo->reader->pktid <= 0)
            mxmlElementSetAttr (connection, "current_seq", "unset");
          else
            mxmlElementSetAttrf (connection, "current_seq", "%06" PRIX64, tcinfo->reader->pktid);

          mxmlElementSetAttr (connection, "sequence_gaps", "0");
          mxmlElementSetAttrf (connection, "txcount", "%" PRIu64, tcinfo->txpackets[0]);
          mxmlElementSetAttrf (connection, "totBytes", "%" PRIu64, tcinfo->txbytes[0]);
          mxmlElementSetAttr (connection, "begin_seq_valid", "yes");
          mxmlElementSetAttr (connection, "realtime", "yes");
          mxmlElementSetAttr (connection, "end_of_data", "no");

          /* Add "window" element if start or end times are set */
          if (tcinfo->starttime || tcinfo->endtime)
          {
            if (!(window = mxmlNewElement (connection, "window")))
            {
              lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
              errflag = 1;
            }
            else
            {
              /* Convert start & end time to YYYY-MM-DD HH:MM:SS or "unset" */
              if (tcinfo->starttime)
                ms_nstime2timestrz (tcinfo->starttime, string, ISOMONTHDAY, NONE);
              else
                strncpy (string, "unset", sizeof (string));

              mxmlElementSetAttr (window, "begin_time", string);

              if (tcinfo->endtime)
                ms_nstime2timestrz (tcinfo->endtime, string, ISOMONTHDAY, NONE);
              else
                strncpy (string, "unset", sizeof (string));

              mxmlElementSetAttr (window, "end_time", string);
            }
          }

          /* Add "selector" element if match or reject strings are set */
          if (tcinfo->matchstr || tcinfo->rejectstr)
          {
            if (!(selector = mxmlNewElement (connection, "selector")))
            {
              lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
              errflag = 1;
            }
            else
            {
              if (tslinfo && tslinfo->selectors)
                mxmlElementSetAttr (selector, "pattern", tslinfo->selectors);
              if (tcinfo->matchstr)
                mxmlElementSetAttr (selector, "match", tcinfo->matchstr);
              if (tcinfo->rejectstr)
                mxmlElementSetAttr (selector, "reject", tcinfo->rejectstr);
            }
          }
        }
      }

      loopctp = loopctp->next;
    }
    pthread_mutex_unlock (&cthreads_lock);

  } /* End of CONNECTIONS request processing */

  /* Convert to XML string, pack into miniSEED and send to client */
  if (xmldoc)
  {
    /* Do not wrap the output XML */
    mxmlSetWrapMargin (0);

    /* Convert to XML string */
    if (!(xmlstr = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK)))
    {
      lprintf (0, "[%s] Error with mxmlSaveAllocString()", cinfo->hostname);
      if (xmldoc)
        mxmlRelease (xmldoc);
      if (record)
        free (record);
      return -1;
    }

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
    *pMS2FSDH_RESERVED (record) = ' ';
    memcpy (pMS2FSDH_STATION (record), "INFO ", 5);
    memcpy (pMS2FSDH_LOCATION (record), "  ", 2);
    memcpy (pMS2FSDH_CHANNEL (record), (errflag) ? "ERR" : "INF", 3);
    memcpy (pMS2FSDH_NETWORK (record), "XX", 2);
    *pMS2FSDH_YEAR (record) = HO2u (year, swapflag);
    *pMS2FSDH_DAY (record) = HO2u (yday, swapflag);
    *pMS2FSDH_HOUR (record) = hour;
    *pMS2FSDH_MIN (record) = min;
    *pMS2FSDH_SEC (record) = sec;
    *pMS2FSDH_UNUSED (record) = 0;
    *pMS2FSDH_FSEC (record) = 0;
    *pMS2FSDH_NUMSAMPLES (record) = 0;
    *pMS2FSDH_SAMPLERATEFACT (record) = 0;
    *pMS2FSDH_SAMPLERATEMULT (record) = 0;
    *pMS2FSDH_ACTFLAGS (record) = 0;
    *pMS2FSDH_IOFLAGS (record) = 0;
    *pMS2FSDH_DQFLAGS (record) = 0;
    *pMS2FSDH_NUMBLOCKETTES (record) = 1;
    *pMS2FSDH_TIMECORRECT (record) = 0;
    *pMS2FSDH_DATAOFFSET (record) = HO2u (56, swapflag);
    *pMS2FSDH_BLOCKETTEOFFSET (record) = HO2u (48, swapflag);

    /* Build Blockette 1000 */
    *pMS2B1000_TYPE (record + 48) = HO2u (1000, swapflag);
    *pMS2B1000_NEXT (record + 48) = 0;
    *pMS2B1000_ENCODING (record + 48) = DE_ASCII;
    *pMS2B1000_BYTEORDER (record + 48) = 1; /* 1 = big endian */
    *pMS2B1000_RECLEN (record + 48) = 9;    /* 2^9 = 512 byte record */
    *pMS2B1000_RESERVED (record + 48) = 0;

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
  if (xmldoc)
    mxmlRelease (xmldoc);

  if (xmlstr)
    free (xmlstr);

  if (record)
    free (record);

  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleInfo */

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

  fprintf (stderr, "DEBUG, sending response: '%.*s'\n", (int)strcspn (sendstr, "\r\n"), sendstr);

  /* Send the reply */
  if (SendData (cinfo, sendstr, strlen (sendstr)))
    return -1;

  return 0;
} /* End of SendReply() */

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
SendRecord (RingPacket *packet, char *record, int reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  char header[40] = {0};
  int headerlen = 0;

  if (!record || !vcinfo)
    return -1;

  if (slinfo->proto_major == 4) /* Create v4 header */
  {
    uint32_t ureclen = reclen;
    uint64_t upktid = packet->pktid;
    uint8_t ustationidlen = 0;
    int stationid_printed;
    char net[10];
    char sta[10];
    char staid[22];

    /* Split the streamid to get the network and station codes,
     * assumed stream pattern: "NET_STA_LOC_CHAN/MSEED" */
    if (SplitStreamID (packet->streamid, '_', 10, net, sta, NULL, NULL, NULL, NULL, NULL) != 2)
    {
      lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, packet->streamid);
      return -1;
    }

    /* Combine network and station into station ID */
    if ((stationid_printed = snprintf (staid, sizeof (staid), "%s_%s", net, sta)) <= 0)
    {
      lprintf (0, "[%s] Error building station ID from %s + %s", cinfo->hostname, net, sta);
      return -1;
    }
    else
    {
      ustationidlen = stationid_printed;
    }

    /* V4 header values are in little-endan byte order */
    if (ms_bigendianhost ())
    {
      ms_gswap4 (&ureclen);
      ms_gswap8 (&upktid);
    }

    /* Construct v4 header */
    memcpy (header, "SE", 2);

    if (MS3_ISVALIDHEADER (record))
      memcpy (header + 2, "3", 1);
    else if (MS2_ISVALIDHEADER (record))
      memcpy (header + 2, "2", 1);
    else
      return -1;

    memcpy (header + 3, "D", 1); /* Payload format subcode, D = data */
    memcpy (header + 4, &ureclen, 4);
    memcpy (header + 8, &upktid, 8);
    memcpy (header + 16, &ustationidlen, 1);
    memcpy (header + 17, staid, ustationidlen);

    headerlen = SLHEADSIZE_EXT + ustationidlen;
  }
  else /* Create v3 header */
  {
    /* Check that sequence number is not too big */
    if (packet->pktid > 0xFFFFFF)
    {
      lprintf (0, "[%s] sequence number too large for SeedLink: %" PRId64,
               cinfo->hostname, packet->pktid);
    }

    /* Create SeedLink header: signature + sequence number */
    snprintf (header, sizeof (header), "SL%06" PRIX64, packet->pktid);
    headerlen = SLHEADSIZE;
  }

  if (SendDataMB (cinfo, (void *[]){header, record}, (size_t[]){headerlen, reclen}, 2))
    return -1;

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = NSnow ();

  return 0;
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
SendInfoRecord (char *record, int reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  char header[SLHEADSIZE];

  if (!record || !vcinfo)
    return;

  /* Create INFO signature according to termination flag */
  if (slinfo->terminfo)
    memcpy (header, "SLINFO  ", SLHEADSIZE);
  else
    memcpy (header, "SLINFO *", SLHEADSIZE);

  SendDataMB (cinfo, (void *[]){header, record}, (size_t[]){SLHEADSIZE, reclen}, 2);

  /* Update the time of the last packet exchange */
  cinfo->lastxchange = NSnow ();

  return;
} /* End of SendInfoRecord() */

/***************************************************************************
 * FreeStaNode:
 *
 * Free all memory associated with a SLStaNode.
 *
 ***************************************************************************/
static void
FreeStaNode (void *rbnode)
{
  SLStaNode *stanode = (SLStaNode *)rbnode;

  if (stanode->selectors)
    free (stanode->selectors);

  free (rbnode);

  return;
} /* End of FreeStaNode() */

/***************************************************************************
 * FreeNetStaNode:
 *
 * Free all memory associated with a SLNetStaNode.
 *
 ***************************************************************************/
static void
FreeNetStaNode (void *rbnode)
{
  SLNetStaNode *netstanode = (SLNetStaNode *)rbnode;

  if (netstanode->streams)
    StackDestroy (netstanode->streams, free);

  free (rbnode);

  return;
} /* End of FreeNetStaNode() */

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
 * GetStaNode:
 *
 * Search the specified binary tree for a given entry.  If the entry does not
 * exist create it and add it to the tree.
 *
 * Return a pointer to the entry or 0 for error.
 ***************************************************************************/
static SLStaNode *
GetStaNode (RBTree *tree, char *staid)
{
  char *newkey = NULL;
  SLStaNode *stanode = 0;
  RBNode *rbnode;

  /* Search for a matching SLStaNode entry */
  if ((rbnode = RBFind (tree, staid)))
  {
    stanode = (SLStaNode *)rbnode->data;
  }
  else
  {
    if ((newkey = strdup (staid)) == NULL)
    {
      lprintf (0, "%s: Error allocating new key", __func__);
      return 0;
    }

    if ((stanode = (SLStaNode *)malloc (sizeof (SLStaNode))) == NULL)
    {
      lprintf (0, "%s: Error allocating new node", __func__);
      return 0;
    }

    stanode->starttime = NSTERROR;
    stanode->endtime = NSTERROR;
    stanode->packetid = 0;
    stanode->datastart = NSTERROR;
    stanode->selectors = NULL;

    RBTreeInsert (tree, newkey, stanode, 0);
  }

  return stanode;
} /* End of GetStaNode() */

/***************************************************************************
 * GetNetStaNode:
 *
 * Search the specified binary tree for a given entry.  If the entry does not
 * exist create it and add it to the tree.
 *
 * Return a pointer to the entry or 0 for error.
 ***************************************************************************/
static SLNetStaNode *
GetNetStaNode (RBTree *tree, char *staid)
{
  char *newkey = NULL;
  SLNetStaNode *netstanode = 0;
  RBNode *rbnode;

  /* Search for a matching SLNetStaNode entry */
  if ((rbnode = RBFind (tree, staid)))
  {
    netstanode = (SLNetStaNode *)rbnode->data;
  }
  else
  {
    if ((newkey = strdup (staid)) == NULL)
    {
      lprintf (0, "%s: Error allocating new key", __func__);
      return 0;
    }

    if ((netstanode = (SLNetStaNode *)calloc (1, sizeof (SLNetStaNode))) == NULL)
    {
      lprintf (0, "%s: Error allocating new node", __func__);
      return 0;
    }

    /* Initialize Stack of associated streams */
    netstanode->streams = StackCreate ();

    RBTreeInsert (tree, newkey, netstanode, 0);
  }

  return netstanode;
} /* End of GetNetStaNode() */

/***************************************************************************
 * StationToRegex:
 *
 * Update match and reject regexes for the specified station ID (NET_STA)
 * and selector list (comma delimited).
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
StationToRegex (const char *net_sta, const char *selectors,
                char **matchregex, char **rejectregex)
{
  char *selectorlist;
  char *selector, *nextselector;
  int matched;

  if (!matchregex || !rejectregex)
  {
    lprintf (0, "Pointer-to-pointer match/reject regex cannot be NULL");
    return -1;
  }

  /* If a selector list is specified traverse it and update regexes */
  if (selectors)
  {
    /* Copy selectors list so we can modify it while parsing */
    if (!(selectorlist = strdup (selectors)))
    {
      lprintf (0, "Cannot allocate memory to duplicate selectors");
      return -1;
    }

    /* Track count of matching selectors */
    matched = 0;

    /* Traverse list of comma separated selectors */
    selector = selectorlist;
    while (selector)
    {
      /* Find deliminting comma */
      nextselector = strchr (selector, ',');

      /* Terminate string at comma and set pointer for next selector */
      if (nextselector)
        *nextselector++ = '\0';

      /* Handle negated selector */
      if (selector[0] == '!')
      {
        /* If no matching (non-negated) selectors are included a negation selector
           implies all data for the specified station with the execption of the
           negated selection, therefore we need to match all channels from the
           station and then reject those in the negated selector */
        if (!matched && net_sta)
        {
          if (SelectToRegex (net_sta, NULL, matchregex))
          {
            lprintf (0, "Error with SelectToRegex");
            if (selectorlist)
              free (selectorlist);
            return -1;
          }

          matched++;
        }

        if (SelectToRegex (net_sta, &selector[1], rejectregex))
        {
          lprintf (0, "Error with SelectToRegex");
          if (selectorlist)
            free (selectorlist);
          return -1;
        }
      }
      /* Handle regular selector */
      else
      {
        if (SelectToRegex (net_sta, selector, matchregex))
        {
          lprintf (0, "Error with SelectToRegex");
          if (selectorlist)
            free (selectorlist);
          return -1;
        }

        matched++;
      }

      selector = nextselector;
    }

    if (selectorlist)
      free (selectorlist);
  }
  /* Otherwise update regex for station without selectors */
  else
  {
    if (SelectToRegex (net_sta, NULL, matchregex))
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
SelectToRegex (const char *net_sta, const char *select, char **regex)
{
  const char *ptr;
  char pattern[50];
  char *build = pattern;
  int idx;
  int length;
  int retval;

  if (!regex)
    return -1;

  /* Start pattern with a '^' */
  *build++ = '^';

  /* Sanity check lengths of input strings */
  if (net_sta && strlen (net_sta) > 20)
    return -1;
  if (select && strlen (select) > 7)
    return -1;

  if (net_sta)
  {
    /* Translate network */
    ptr = net_sta;
    while (*ptr)
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

      ptr++;
    }
  }
  else
  {
    *build++ = '.';
    *build++ = '*';
  }

  /* Add separator */
  *build++ = '_';

  if (select)
  {
    /* Ingore selector after any period, DECOTL subtypes are not supported */
    if ((ptr = strrchr (select, '.')))
      length = ptr - select;
    else
      length = strlen (select);

    /* If location and channel are specified */
    if (length == 5)
    {
      /* Translate location, '-' means space location which is collapsed */
      for (ptr = select, idx = 0; idx < 2; idx++, ptr++)
      {
        if (*ptr == '?')
          *build++ = '.';
        else if (*ptr != '-')
          *build++ = *ptr;
      }

      /* Add separator */
      *build++ = '_';

      /* Translate channel */
      for (ptr = &select[2], idx = 0; idx < 3; idx++, ptr++)
      {
        if (*ptr == '?')
          *build++ = '.';
        else
          *build++ = *ptr;
      }
    }
    /* If only location is specified */
    else if (length == 2)
    {
      /* Translate location, '-' means space location which is collapsed */
      for (ptr = select, idx = 0; idx < 2; idx++, ptr++)
      {
        if (*ptr == '?')
          *build++ = '.';
        else if (*ptr != '-')
          *build++ = *ptr;
      }

      /* Add separator */
      *build++ = '_';
    }
    /* If only channel is specified */
    else if (length == 3)
    {
      /* Add wildcard for location and separator */
      *build++ = '.';
      *build++ = '*';
      *build++ = '_';

      /* Translate channel */
      for (ptr = select, idx = 0; idx < 3; idx++, ptr++)
      {
        if (*ptr == '?')
          *build++ = '.';
        else
          *build++ = *ptr;
      }
    }
  }
  else
  {
    *build++ = '.';
    *build++ = '*';
  }

  /* Add final catch-all for remaining stream ID parts if not already done */
  if (select)
  {
    *build++ = '.';
    *build++ = '*';
  }

  /* End pattern with a '$' and terminate */
  *build++ = '$';
  *build++ = '\0';

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

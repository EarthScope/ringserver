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
 * Copyright (C) 2026:
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

#include "auth.h"
#include "clients.h"
#include "cmdtoken.h"
#include "generic.h"
#include "http.h"
#include "infojson.h"
#include "infoxml.h"
#include "logging.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"
#include "slclient.h"

/* Define list of valid characters for selectors, labels and station IDs */
#define VALIDSELECTCHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_-!"
#define VALIDLABELCHARS  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_-"
#define VALIDSTAIDCHARS  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*_"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo, CmdToken *cmd);
static int HandleInfo_v3 (ClientInfo *cinfo, CmdToken *cmd);
static int HandleInfo_v4 (ClientInfo *cinfo, CmdToken *cmd);
static int SendReply (ClientInfo *cinfo, char *reply, ErrorCode code, char *extreply);
static int SendPacket (uint64_t pktid, char *payload, uint32_t payloadlen,
                       const char *staid, char format, char subformat, void *vcinfo);
static int SendRecord (RingPacket *packet, char *record, uint32_t reclen,
                       void *vcinfo);
static void SendInfoRecord (char *record, uint32_t reclen, void *vcinfo);
static void FreeReqStationID (void *rbnode);
static int StaKeyCompare (const void *a, const void *b);
static ReqStationID *GetReqStationID (RBTree *tree, char *staid);
static int StationToRegex (const char *staid, Selector *selector,
                           char **matchregex, char **rejectregex);
static int SelectToRegex (const char *staid, const char *select,
                          const char *label, char **regex);

/***********************************************************************
 * SLHandleCmd:
 *
 * Handle SeedLink command, which is expected to be in the
 * ClientInfo.recvbuf buffer.
 *
 * Returns 0 on success, 1 on client error (e.g. unrecognized command),
 * and negative value on fatal error.  On fatal error the client should
 * be disconnected.
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

    slinfo->startid = RINGID_NONE;

    if ((slinfo->stations = RBTreeCreate (StaKeyCompare, free, FreeReqStationID)) == NULL)
    {
      lprintf (0, "[%s] Error allocating SeedLink stations tree", cinfo->hostname);
      free (slinfo);
      cinfo->extinfo = NULL;
      return -1;
    }
  }

  slinfo = (SLInfo *)cinfo->extinfo;

  /* Tokenize the command buffer once; all handlers receive the same CmdToken */
  CmdToken cmd;
  if (cmdtoken_parse (&cmd, cinfo->recvbuf) < 0 || cmd.argc == 0)
  {
    lprintf (1, "[%s] Error tokenizing command buffer", cinfo->hostname);
    return 1;
  }

  /* INFO and BYE are accepted in any state per the SeedLink specification */
  if (cmdtoken_eq_nocase (&cmd, 0, "BYE"))
  {
    lprintf (2, "[%s] Received BYE, closing connection", cinfo->hostname);
    return -1;
  }

  else if (cmdtoken_eq_nocase (&cmd, 0, "INFO"))
  {
    if (slinfo->proto_major == 4)
    {
      if (HandleInfo_v4 (cinfo, &cmd))
      {
        return -1;
      }
    }
    else
    {
      if (HandleInfo_v3 (cinfo, &cmd))
      {
        return -1;
      }
    }
  }

  /* Negotiation if expecting commands */
  else if (cinfo->state == STATE_COMMAND ||
           cinfo->state == STATE_STATION)
  {
    int rv = HandleNegotiation (cinfo, &cmd);
    if (rv < 0)
      return -1;
    if (rv > 0)
      return 1;
  }

  /* Otherwise this is unexpected data from the client */
  else
  {
    lprintf (1, "[%s] Unexpected data received from client", cinfo->hostname);
    return 1;
  }

  /* Configure ring parameters if negotiation is complete */
  if (cinfo->state == STATE_RINGCONFIG)
  {
    /* Check for authentication requirement */
    if (config.auth.required && !(cinfo->permissions & AUTHENTICATED))
    {
      lprintf (1, "[%s] Streaming requested from client without authentication",
               cinfo->hostname);
      SendReply (cinfo, "ERROR", ERROR_AUTH, "Authentication required for streaming");
      return -1;
    }

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
          if (cinfo->starttime == NSTUNSET)
            cinfo->starttime = stationid->starttime;
          else if (cinfo->starttime > stationid->starttime)
            cinfo->starttime = stationid->starttime;
        }

        /* Set or expand the global endtime */
        if (stationid->endtime != NSTUNSET)
        {
          if (cinfo->endtime == NSTUNSET)
            cinfo->endtime = stationid->endtime;
          else if (cinfo->endtime < stationid->endtime)
            cinfo->endtime = stationid->endtime;
        }

        /* Track the newest packet ID while validating their existence */
        if (stationid->packetid <= RINGID_MAXIMUM)
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

    /* Position ring to starting packet ID if one was identified */
    if (slinfo->startid <= RINGID_MAXIMUM)
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

    /* Set ring position based on time if start time specified and no packet ID */
    if (cinfo->starttime != NSTUNSET && slinfo->startid == RINGID_NONE)
    {
      char timestr[32];
      ms_nstime2timestr_n (cinfo->starttime, timestr, sizeof (timestr), ISOMONTHDAY_Z, NANO_MICRO_NONE);

      /* Position ring according to start time, use reverse search if limited */
      if (config.timewinlimit == 1.0)
      {
        readid = RingAfter (cinfo->reader, cinfo->starttime, 0);
      }
      else if (config.timewinlimit < 1.0)
      {
        uint64_t pktlimit = (uint64_t)(config.timewinlimit * param.maxpackets);

        readid = RingAfterRev (cinfo->reader, cinfo->starttime, pktlimit, 0);
      }
      else
      {
        lprintf (0, "Time window search limit is invalid: %f", (double)config.timewinlimit);
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

    WriteAccessLog (cinfo, "command",
                    (slinfo->dialup) ? "FETCH" : "DATA",
                    NULL, cinfo->matchstr, cinfo->rejectstr);

    cinfo->state = STATE_STREAM;
  } /* Done configuring ring parameters */

  return 0;
} /* End of SLHandleCmd() */

/***********************************************************************
 * SLFindFilter:
 *
 * Search the station and selector requests that match a stream ID
 * and determine the conversion, if any, for this stream.
 *
 * Return conversion value for this stream, or CONVERT_NONE if none.
 ***********************************************************************/
static Conversion
SLFindFilter (const SLInfo *slinfo, const char *streamid)
{
  char privateid[MAXSTREAMID] = {0};
  ReqStationID *reqstaid;
  RBNode *rbnode;
  Stack *stack;
  char *stationid   = NULL;
  char *selectid    = NULL;
  char *streamlabel = NULL;
  char *ptr         = NULL;

  if (!slinfo || !streamid)
    return CONVERT_NONE;

  /* No stations, there are no "filter" designations */
  if (slinfo->stationcount <= 0)
    return CONVERT_NONE;

  snprintf (privateid, sizeof (privateid), "%s", streamid);

  /* Decompose FDSN Source ID into SeedLink station, select, and optional label by
   * extracting "NET_STA", "LOC_B_S_SS", and optional "LABEL" from
   * "FDSN:NET_STA_LOC_B_S_SS/MSEED[3][/LABEL]" */
  if ((ptr = strchr (privateid, ':')))
  {
    stationid = ptr + 1;

    if ((ptr = strchr (stationid, '_')))
    {
      if ((ptr = strchr (ptr + 1, '_')))
      {
        selectid = ptr + 1;
        *ptr     = '\0';

        /* Find the first '/' which separates select ID from format (/MSEED or /MSEED3) */
        if ((ptr = strchr (selectid, '/')))
        {
          *ptr = '\0';

          /* Find a second '/' which separates format from optional label */
          if ((ptr = strchr (ptr + 1, '/')))
            streamlabel = ptr + 1;
        }
      }
    }
  }

  if (stationid == NULL || selectid == NULL)
    return CONVERT_NONE;

  /* Search for matching selections in station requests */
  stack = StackCreate ();
  RBBuildStack (slinfo->stations, stack);

  while ((rbnode = (RBNode *)StackPop (stack)))
  {
    if (GlobMatch (stationid, (const char *)rbnode->key))
    {
      reqstaid = (ReqStationID *)rbnode->data;

      /* Search for matching selections in station selectors */
      if (reqstaid->selectors)
      {
        Selector *selector = reqstaid->selectors;

        while (selector)
        {
          int selectid_matches = (selector->string[0] == '\0') ||
                                 GlobMatch (selectid, selector->string);

          /* Match label: empty selector label matches only unlabeled streams;
           * a non-empty selector label must match the stream's label */
          int label_matches;
          if (selector->label[0] == '\0')
            label_matches = (streamlabel == NULL || streamlabel[0] == '\0');
          else
            label_matches = (streamlabel != NULL && streamlabel[0] != '\0' &&
                             GlobMatch (streamlabel, selector->label));

          if (selectid_matches && label_matches)
          {
            StackDestroy (stack, 0);
            return selector->convert;
          }

          selector = selector->next;
        }
      }
    }
  }

  StackDestroy (stack, 0);

  return CONVERT_NONE;
} /* End of SLFindFilter() */

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
                                 cinfo->packet.streamid,
                                 cinfo->streamscount, &newstream)) == NULL)
    {
      lprintf (0, "[%s] Error with GetStreamNode() for %s",
               cinfo->hostname, cinfo->packet.streamid);
      return -1;
    }

    if (newstream)
    {
      lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, cinfo->packet.streamid);
      cinfo->streamscount++;

      /* Set conversion value for stream */
      stream->convert = SLFindFilter (slinfo, cinfo->packet.streamid);
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
      char *sendrecord    = cinfo->sendbuf;
      uint32_t sendreclen = cinfo->packet.datasize;

      /* Convert miniSEED 2 to miniSEED 3 if requested */
      if (stream->convert == CONVERT_MSEED3 && MS2_ISVALIDHEADER (cinfo->sendbuf))
      {
        /* Allocate buffer for conversion if */
        if (cinfo->convertbuf == NULL)
        {
          cinfo->convertbuflen = cinfo->sendbufsize;

          if ((cinfo->convertbuf = (char *)malloc (cinfo->convertbuflen)) == NULL)
          {
            lprintf (0, "[%s] Error allocating convert buffer", cinfo->hostname);
            return -1;
          }
        }

        /* Convert miniSEED 2 to miniSEED 3 */
        MS3Record *msr = NULL;
        if (msr3_parse (cinfo->sendbuf, cinfo->packet.datasize, &msr, 0, 0) == MS_NOERROR)
        {
          int convertedlen = msr3_repack_mseed3 (msr, cinfo->convertbuf, (uint32_t)cinfo->convertbuflen, 0);

          if (convertedlen > 0)
          {
            sendrecord = cinfo->convertbuf;
            sendreclen = convertedlen;
          }
          else
          {
            lprintf (3, "[%s] Error converting miniSEED 2 to miniSEED 3", cinfo->hostname);
          }
        }

        msr3_free (&msr);
      }

      /* Send miniSEED record to client */
      if (SendRecord (&cinfo->packet, sendrecord, sendreclen, cinfo))
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
      cinfo->txpackets0++;
      cinfo->txbytes0 += cinfo->packet.datasize;
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

  for (Selector *next, *node = slinfo->selectors; node; node = next)
  {
    next = node->next;
    free (node);
  }

  RBTreeDestroy (slinfo->stations);
  slinfo->stations = NULL;

  free (slinfo);
  cinfo->extinfo = NULL;

  return;
} /* End of SLFree() */

/***************************************************************************
 * sl_arg_error:
 *
 * Send an ERROR ARGUMENTS reply to a SeedLink client, respecting batch
 * mode.  Returns -1 if the reply itself failed (socket error), 0 otherwise.
 * Callers should set OKGO=0 after this call.
 ***************************************************************************/
static int
sl_arg_error (ClientInfo *cinfo, const char *msg)
{
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  if (!slinfo->batch)
    return SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, (char *)msg);
  return 0;
}

/***************************************************************************
 * FreeSelectorList:
 *
 * Free every node in a linked list of Selector structures.
 ***************************************************************************/
static inline void
FreeSelectorList (Selector *head)
{
  for (Selector *next, *node = head; node; node = next)
  {
    next = node->next;
    free (node);
  }
}

/***************************************************************************
 * LinkSelectorLists:
 *
 * Splice a SELECT command's parsed positive (non-negated) and negative
 * (negated) sub-lists into the destination selector list at *destlist.
 *
 * Non-negated patterns are prepended as a unit so that the first pattern
 * requested in a single SELECT command is matched first by SLFindFilter,
 * matching the v4 specification's "first match takes effect" rule for
 * filtered selectors.
 *
 * Negated patterns are appended as a unit at the end of the list.
 *
 * 'postail' must point to the last node of the 'poshead' list when
 * 'poshead' is non-NULL.  Either list may be NULL.
 ***************************************************************************/
static void
LinkSelectorLists (Selector **destlist, Selector *poshead, Selector *postail,
                   Selector *neghead)
{
  if (!destlist)
    return;

  if (poshead != NULL)
  {
    postail->next = *destlist;
    *destlist     = poshead;
  }

  if (neghead != NULL)
  {
    if (*destlist == NULL)
    {
      *destlist = neghead;
    }
    else
    {
      Selector *last = *destlist;
      while (last->next != NULL)
        last = last->next;
      last->next = neghead;
    }
  }
}

/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation command implementing server-side SeedLink
 * protocol, updating the connection configuration and state
 * specified.
 *
 * Returns 0 on success, 1 on client error (e.g. unrecognized command),
 * and -1 on fatal error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (ClientInfo *cinfo, CmdToken *cmd)
{
  SLInfo *slinfo;
  char sendbuffer[400];
  ReqStationID *stationid = NULL;

  nstime_t starttime   = NSTUNSET;
  nstime_t endtime     = NSTUNSET;
  uint64_t startpacket = RINGID_NONE;
  Conversion convert;

  char *ptr;
  char OKGO = 1;

  if (!cinfo || !cinfo->extinfo || !cmd)
    return -1;

  slinfo = (SLInfo *)cinfo->extinfo;

  /* HELLO (v3.x and v4.0) - Return server version and ID */
  if (cmdtoken_eq_nocase (cmd, 0, "HELLO"))
  {
    int bytes;

    /* Create and send server version information */
    pthread_rwlock_rdlock (&config.config_rwlock);
    bytes = snprintf (sendbuffer, sizeof (sendbuffer),
                      SLSERVER_ID "\r\n%s\r\n", config.serverid);
    pthread_rwlock_unlock (&config.config_rwlock);

    if (bytes >= sizeof (sendbuffer))
    {
      lprintf (0, "[%s] Response to HELLO is likely truncated: '%*s'",
               cinfo->hostname, (int)sizeof (sendbuffer), sendbuffer);
    }

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer), 0))
      return -1;
  }

  /* SLPROTO (v4.0) - Parse requested protocol version */
  else if (cmdtoken_eq_nocase (cmd, 0, "SLPROTO"))
  {
    uint8_t proto_major = 0;
    uint8_t proto_minor = 0;

    if (cmd->argc != 2 || cmd->overflow)
    {
      lprintf (2, "[%s] Received %s, protocol rejected (argument error)", cinfo->hostname, cinfo->recvbuf);

      if (!slinfo->batch && SendReply (cinfo, "ERROR UNSUPPORTED unsupported protocol version", ERROR_NONE, NULL))
        return -1;
    }
    else if (sscanf (cmd->argv[1], "%" SCNu8 ".%" SCNu8, &proto_major, &proto_minor) < 2)
    {
      lprintf (2, "[%s] Received %s, protocol rejected (bad version format)", cinfo->hostname, cinfo->recvbuf);

      if (!slinfo->batch && SendReply (cinfo, "ERROR UNSUPPORTED unsupported protocol version", ERROR_NONE, NULL))
        return -1;
    }
    else if ((proto_major == 3) || (proto_major == 4 && proto_minor == 0))
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
  else if (cmdtoken_eq_nocase (cmd, 0, "USERAGENT"))
  {
    const char *useragent = cmdtoken_rest_after (cmd, 1);

    if (useragent)
      strncpy (cinfo->clientid, useragent, sizeof (cinfo->clientid) - 1);

    cinfo->clientid[sizeof (cinfo->clientid) - 1] = '\0';

    lprintf (2, "[%s] Received USERAGENT (%s)", cinfo->hostname, cinfo->clientid);

    if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* CAPABILITIES (v3.x) - Parse capabilities flags */
  else if (cmdtoken_eq_nocase (cmd, 0, "CAPABILITIES"))
  {
    /* Extended reply capability: scan all arguments for EXTREPLY (case-insensitive) */
    for (int i = 1; i < cmd->argc; i++)
    {
      if (cmdtoken_eq_nocase (cmd, i, "EXTREPLY"))
      {
        slinfo->extreply = 1;
        break;
      }
    }

    if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* CAT (v3.x) - Return text list of stations */
  else if (cmdtoken_eq_nocase (cmd, 0, "CAT"))
  {
    snprintf (sendbuffer, sizeof (sendbuffer),
              "CAT command not implemented\r\n");

    if (SendData (cinfo, sendbuffer, strlen (sendbuffer), 0))
      return -1;
  }

  /* BATCH (v3.x) - Batch mode for subsequent commands */
  else if (cmdtoken_eq_nocase (cmd, 0, "BATCH"))
  {
    slinfo->batch = 1;

    if (SendReply (cinfo, "OK", ERROR_NONE, NULL))
      return -1;
  }

  /* AUTH (v4.0) - Parse auth command */
  else if (cmdtoken_eq_nocase (cmd, 0, "AUTH"))
  {
    char username[128] = {0};
    char password[128] = {0};
    char *jwtoken      = NULL;

    OKGO = 1;

    if (cmd->argc < 2)
    {
      lprintf (0, "[%s] Error parsing AUTH: no sub-command", cinfo->hostname);

      if (SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "AUTH requires a sub-command (USERPASS or JWT)"))
        return -1;

      OKGO = 0;
    }
    /* AUTH USERPASS username password (case-sensitive sub-keyword) */
    else if (cmdtoken_eq (cmd, 1, "USERPASS"))
    {
      lprintf (2, "[%s] Received AUTH USERPASS", cinfo->hostname);

      if (cmd->argc != 4 || cmd->overflow)
      {
        lprintf (0, "[%s] Error parsing AUTH USERPASS", cinfo->hostname);

        if (SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "AUTH USERPASS requires 2 arguments"))
          return -1;

        OKGO = 0;
      }
      /* Reject oversize credentials: silent truncation would let an
       * attacker authenticate as a credential's prefix. */
      else if (strlen (cmd->argv[2]) >= sizeof (username) ||
               strlen (cmd->argv[3]) >= sizeof (password))
      {
        lprintf (0, "[%s] AUTH USERPASS credentials too large", cinfo->hostname);

        if (SendReply (cinfo, "ERROR", ERROR_AUTH, "Credentials too large"))
          return -1;

        OKGO = 0;
      }
      else
      {
        strncpy (username, cmd->argv[2], sizeof (username) - 1);
        strncpy (password, cmd->argv[3], sizeof (password) - 1);
      }
    }
    /* AUTH JWT token (case-sensitive sub-keyword) */
    else if (cmdtoken_eq (cmd, 1, "JWT"))
    {
      lprintf (2, "[%s] Received AUTH JWT", cinfo->hostname);

      jwtoken = (char *)cmdtoken_rest_after (cmd, 2);

      if (!jwtoken || jwtoken[0] == '\0')
      {
        lprintf (0, "[%s] Error parsing AUTH JWT", cinfo->hostname);

        if (SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "AUTH JWT requires 1 argument"))
          return -1;

        OKGO = 0;
      }
    }
    else
    {
      lprintf (0, "[%s] Unrecognized AUTH sub-command: %s", cinfo->hostname, cmd->argv[1]);

      if (SendReply (cinfo, "ERROR", ERROR_UNSUPPORTED, "Unrecognized AUTH sub-command"))
        return -1;

      OKGO = 0;
    }

    if (OKGO && PerformAuth (cinfo, username, password, jwtoken))
    {
      lprintf (0, "[%s] Error performing authentication", cinfo->hostname);

      if (SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error performing authentication"))
        return -1;

      OKGO = 0;
    }

    if (OKGO && !(cinfo->permissions & CONNECT_PERMISSION))
    {
      lprintf (0, "[%s] Authentication failed, not allowed to connect", cinfo->hostname);

      if (SendReply (cinfo, "ERROR", ERROR_AUTH, "Authentication failed, not allowed to connect"))
        return -1;

      OKGO = 0;
    }
    else if (OKGO)
    {
      lprintf (2, "[%s] Authentication successful", cinfo->hostname);

      if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
        return -1;
    }
  } /* End of AUTH */

  /* STATION (v3.x and v4.0) - Select specified station */
  else if (cmdtoken_eq_nocase (cmd, 0, "STATION"))
  {
    OKGO = 1;

    /* Parse station ID from request */
    slinfo->reqstaid[0] = '\0';

    if (slinfo->proto_major == 4)
    {
      /* STATION stationID */
      if (cmd->argc != 2 || cmd->overflow)
      {
        if (sl_arg_error (cinfo, "STATION requires 1 argument"))
          return -1;

        OKGO = 0;
      }
      else
      {
        strncpy (slinfo->reqstaid, cmd->argv[1], sizeof (slinfo->reqstaid) - 1);
        slinfo->reqstaid[sizeof (slinfo->reqstaid) - 1] = '\0';
      }
    }
    else /* Protocol 3.x */
    {
      char reqnet[10] = {0};
      char reqsta[10] = {0};

      /* STATION STA [NET] */
      if (cmd->argc < 2 || cmd->argc > 3 || cmd->overflow)
      {
        if (sl_arg_error (cinfo, "STATION requires 1 or 2 arguments"))
          return -1;

        OKGO = 0;
      }
      else
      {
        strncpy (reqsta, cmd->argv[1], sizeof (reqsta) - 1);
        reqsta[sizeof (reqsta) - 1] = '\0';

        if (cmd->argc == 3)
        {
          strncpy (reqnet, cmd->argv[2], sizeof (reqnet) - 1);
          reqnet[sizeof (reqnet) - 1] = '\0';
        }
        else
        {
          /* Use wildcard network if not specified */
          reqnet[0] = '*';
          reqnet[1] = '\0';
        }

        /* Combine network and station codes into station ID */
        snprintf (slinfo->reqstaid, sizeof (slinfo->reqstaid), "%s_%s", reqnet, reqsta);
      }
    }

    /* Limit the number of stations per client */
    if (OKGO && slinfo->stationcount >= SLMAXSTATIONS)
    {
      lprintf (0, "[%s] Station limit of %d reached", cinfo->hostname, SLMAXSTATIONS);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_LIMIT, "Station limit reached"))
        return -1;

      OKGO = 0;
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
      else
      {
        if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
          return -1;

        slinfo->stationcount++;
        cinfo->state = STATE_STATION;
      }
    }
  } /* End of STATION */

  /* SELECT (v3.x and v4.0) - Refine selection of channels for STATION
   *
   * SeedLink v4 supports multiple space-separated patterns per command, e.g.
   *   SELECT *.*O:native *:3
   * Each pattern is parsed and validated independently.  All patterns must
   * validate before any are linked into the station/global selector list,
   * so a SELECT command is atomic with respect to its effect.
   *
   * SeedLink v3 retains its single-pattern behavior (LLCCC/CCC).
   */
  else if (cmdtoken_eq_nocase (cmd, 0, "SELECT"))
  {
    /* Two sub-lists preserve request order while supporting the
     * pre-existing rule: non-negated selectors are prepended to the
     * destination list, negated selectors are appended.  Within a single
     * SELECT command the first requested pattern is matched first, per
     * the v4 specification. */
    Selector *poshead = NULL; /* Non-negated patterns, request order */
    Selector *postail = NULL;
    Selector *neghead = NULL; /* Negated patterns, request order */
    Selector *negtail = NULL;
    char errmsg[80]   = {0};
    int alloc_failed  = 0;

    /* Empty SELECT is not allowed */
    if (cmd->argc < 2)
    {
      snprintf (errmsg, sizeof (errmsg),
                (slinfo->proto_major == 4) ? "empty SELECT is not allowed in v4"
                                           : "SELECT requires a single argument");
    }
    /* v3 allows exactly one pattern */
    else if (slinfo->proto_major == 3 && (cmd->argc > 2 || cmd->overflow))
    {
      snprintf (errmsg, sizeof (errmsg), "SELECT requires a single argument");
    }
    /* Tokenizer overflow means too many patterns in v4 */
    else if (cmd->overflow)
    {
      snprintf (errmsg, sizeof (errmsg), "Too many SELECT patterns");
    }

    /* Iterate over all parsed pattern tokens, stopping on first error */
    for (int i = 1; errmsg[0] == '\0' && i < cmd->argc; i++)
    {
      char selectortok[MAXSTREAMID] = {0};
      char label[SLMAXLABELLEN]     = {0};
      Selector *newselector;

      convert = CONVERT_NONE;

      /* Check token length */
      if (strlen (cmd->argv[i]) >= sizeof (selectortok))
      {
        snprintf (errmsg, sizeof (errmsg), "Selector pattern too long");
        break;
      }

      strncpy (selectortok, cmd->argv[i], sizeof (selectortok) - 1);
      selectortok[sizeof (selectortok) - 1] = '\0';

      /* For SeedLink v4, parse filter (conversion) or label */
      if (slinfo->proto_major == 4 && (ptr = strrchr (selectortok, ':')))
      {
        if (strcmp (ptr + 1, "native") == 0)
        {
          convert = CONVERT_NONE;
        }
        else if (strcmp (ptr + 1, "3") == 0)
        {
          convert = CONVERT_MSEED3;
        }
        /* Otherwise treat as a label selector: validate chars and length */
        else
        {
          const char *labelptr = ptr + 1;
          if (strlen (labelptr) == 0 || strlen (labelptr) >= SLMAXLABELLEN ||
              strspn (labelptr, VALIDLABELCHARS) != strlen (labelptr))
          {
            lprintf (0, "[%s] Error, SELECT filter '%s' is not a valid label",
                     cinfo->hostname, labelptr);

            snprintf (errmsg, sizeof (errmsg), "Filter not supported");
            break;
          }

          snprintf (label, sizeof (label), "%s", labelptr);
          convert = CONVERT_NONE;
        }

        *ptr = '\0';
      }

      /* Truncate pattern at a '.', subtypes are accepted but not supported */
      if ((ptr = strrchr (selectortok, '.')))
        *ptr = '\0';

      /* Convert v3 style LLCCC selectors to v4 style (FDSN Source ID) for consistency */
      if (slinfo->proto_major == 3)
      {
        char newselectorstr[sizeof (selectortok)];
        char *negate     = (selectortok[0] == '!') ? "!" : "";
        char *v3selector = (selectortok[0] == '!') ? selectortok + 1 : selectortok;

        if (strlen (v3selector) == 5)
        {
          snprintf (newselectorstr, sizeof (newselectorstr), "%s%c%c_%c_%c_%c",
                    negate, v3selector[0], v3selector[1], v3selector[2],
                    v3selector[3], v3selector[4]);
        }
        else if (strlen (v3selector) == 3)
        {
          snprintf (newselectorstr, sizeof (newselectorstr), "%s*_%c_%c_%c",
                    negate, v3selector[0], v3selector[1], v3selector[2]);
        }
        else
        {
          lprintf (0, "[%s] Error, SELECT pattern '%s' is not a valid SeedLink v3 LLCCC or CCC pattern",
                   cinfo->hostname, selectortok);

          snprintf (errmsg, sizeof (errmsg), "Invalid selector pattern");
          break;
        }

        memcpy (selectortok, newselectorstr, sizeof (selectortok));
      }

      /* Sanity check, only allowed characters */
      if (strspn (selectortok, VALIDSELECTCHARS) != strlen (selectortok))
      {
        lprintf (0, "[%s] Error, select pattern contains illegal characters: '%s'",
                 cinfo->hostname, selectortok);

        snprintf (errmsg, sizeof (errmsg), "Selector contains illegal characters");
        break;
      }

      /* Allocate and populate new stream selection list node */
      if ((newselector = (Selector *)calloc (1, sizeof (Selector))) == NULL)
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        snprintf (errmsg, sizeof (errmsg), "Error allocating memory()");
        alloc_failed = 1;
        break;
      }

      memcpy (newselector->string, selectortok, sizeof (newselector->string));
      memcpy (newselector->label, label, sizeof (newselector->label));
      newselector->convert = convert;
      newselector->next    = NULL;

      /* Append to negated or non-negated sub-list, preserving request order */
      if (selectortok[0] == '!')
      {
        if (negtail == NULL)
          neghead = newselector;
        else
          negtail->next = newselector;
        negtail = newselector;
      }
      else
      {
        if (postail == NULL)
          poshead = newselector;
        else
          postail->next = newselector;
        postail = newselector;
      }
    } /* End of pattern loop */

    /* If parsing failed, free any locally-parsed selectors and report error */
    if (errmsg[0] != '\0')
    {
      FreeSelectorList (poshead);
      FreeSelectorList (neghead);

      if (alloc_failed)
      {
        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, errmsg))
          return -1;
      }
      else if (sl_arg_error (cinfo, errmsg))
        return -1;
    }
    /* If modifying a STATION find the station ID before sending OK */
    else if (cinfo->state == STATE_STATION &&
             !(stationid = GetReqStationID (slinfo->stations, slinfo->reqstaid)))
    {
      lprintf (0, "[%s] Error in GetReqStationID() for command SELECT", cinfo->hostname);

      FreeSelectorList (poshead);
      FreeSelectorList (neghead);

      if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error in GetReqStationID()"))
        return -1;
    }
    else
    {
      /* Send the reply first; only link the selectors into the destination
       * list if the reply succeeds. */
      if (!slinfo->batch && SendReply (cinfo, "OK", ERROR_NONE, NULL))
      {
        FreeSelectorList (poshead);
        FreeSelectorList (neghead);
        return -1;
      }

      /* Splice into the per-station or global selector list */
      LinkSelectorLists ((cinfo->state == STATE_STATION) ? &stationid->selectors
                                                         : &slinfo->selectors,
                         poshead, postail, neghead);
    }
  } /* End of SELECT */

  /* DATA (v3.x and v4.0) or FETCH (v3.x) - Request data from a specific packet */
  else if (cmdtoken_eq_nocase (cmd, 0, "DATA") ||
           (cmdtoken_eq_nocase (cmd, 0, "FETCH") && slinfo->proto_major == 3))
  {
    OKGO = 1;

    if (slinfo->proto_major == 4)
    {
      /* DATA [seq_decimal [start [end]]] */
      if (cmd->argc > 4 || cmd->overflow)
      {
        if (sl_arg_error (cinfo, "Too many arguments for DATA"))
          return -1;

        OKGO = 0;
      }

      if (OKGO && cmd->argc >= 2)
      {
        if (cmdtoken_eq (cmd, 1, "ALL"))
        {
          startpacket = RINGID_EARLIEST;
        }
        else
        {
          uint64_t seq = 0;

          if (cmdtoken_u64 (cmd, 1, &seq, 10) < 0)
          {
            lprintf (0, "[%s] Error parsing sequence number for DATA: %s",
                     cinfo->hostname, cmd->argv[1]);

            if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Invalid sequence number"))
              return -1;

            OKGO = 0;
          }
          else
          {
            startpacket = seq;
          }
        }
      }
      else if (OKGO)
      {
        startpacket = RINGID_NEXT;
      }

      /* Convert start time string if specified */
      if (OKGO && cmd->argc >= 3)
      {
        if ((starttime = ms_mdtimestr2nstime (cmd->argv[2])) == NSTERROR)
        {
          lprintf (0, "[%s] Error parsing time in DATA: %s",
                   cinfo->hostname, cmd->argv[2]);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing start time"))
            return -1;

          OKGO = 0;
        }
      }

      /* Convert end time string if specified */
      if (OKGO && cmd->argc >= 4)
      {
        if ((endtime = ms_mdtimestr2nstime (cmd->argv[3])) == NSTERROR)
        {
          lprintf (0, "[%s] Error parsing time in DATA: %s",
                   cinfo->hostname, cmd->argv[3]);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing end time"))
            return -1;

          OKGO = 0;
        }
      }
    }
    else /* Protocol 3.x */
    {
      /* DATA|FETCH [seq_hex [start]] */
      if (cmd->argc > 3 || cmd->overflow)
      {
        if (sl_arg_error (cinfo, "Too many arguments for DATA/FETCH"))
          return -1;

        OKGO = 0;
      }

      if (OKGO && cmd->argc >= 2)
      {
        uint32_t seq = 0;

        if (cmdtoken_u32 (cmd, 1, &seq, 16) < 0)
        {
          lprintf (0, "[%s] Error parsing sequence number for DATA/FETCH: %s",
                   cinfo->hostname, cmd->argv[1]);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Invalid sequence number"))
            return -1;

          OKGO = 0;
        }
        else
        {
          RingPacket lookup;
          uint64_t latestid = RingReadPacket (param.latestoffset, &lookup, NULL);

          if (latestid <= RINGID_MAXIMUM)
          {
            /* To map the 24-bit SeedLink v3 sequence into the 64-bit packet ID range
             * combine the highest 40-bits of latest ID with lowest 24-bits of requested sequence */
            startpacket = (latestid & 0xFFFFFFFFFF000000) | (seq & 0xFFFFFF);
          }
          else
          {
            startpacket = (seq & 0xFFFFFF);
          }
        }
      }
      else if (OKGO)
      {
        startpacket = RINGID_NEXT;
      }

      /* Convert start time string if specified */
      if (OKGO && cmd->argc >= 3)
      {
        if ((starttime = ms_mdtimestr2nstime (cmd->argv[2])) == NSTERROR)
        {
          lprintf (0, "[%s] Error parsing time in DATA/FETCH: %s",
                   cinfo->hostname, cmd->argv[2]);

          if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing start time"))
            return -1;

          OKGO = 0;
        }
      }
    }

    /* SeedLink clients resume data flow by requesting: lastpacket + 1
     * The ring needs to be positioned to the actual last packet ID for RingReadNext(),
     * so set the starting packet to the last actual packet received by the client.
     * An unfortunate side-effect: resuming from sequence 0 is not possible. */
    if (startpacket <= RINGID_MAXIMUM && startpacket > 0)
      startpacket = startpacket - 1;

    /* If configuring a specific station selection */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      if (cmd->argc >= 2)
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
        if (cmdtoken_eq_nocase (cmd, 0, "FETCH"))
          slinfo->dialup = 1;
      }

      cinfo->state = STATE_COMMAND;
    }
    /* Otherwise this is a request to start data flow */
    else if (OKGO)
    {
      /* If no stations yet we are in all-station mode */
      if (slinfo->stationcount == 0)
      {
        slinfo->startid  = startpacket;
        cinfo->starttime = starttime;
        cinfo->endtime   = endtime;
      }

      /* If FETCH the connection is dial-up */
      if (cmdtoken_eq_nocase (cmd, 0, "FETCH"))
        slinfo->dialup = 1;

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of DATA|FETCH */

  /* TIME (v3.x) - Request data in time window */
  else if (cmdtoken_eq_nocase (cmd, 0, "TIME") && slinfo->proto_major == 3)
  {
    OKGO = 1;

    /* TIME start_time [end_time] */
    if (cmd->argc < 2 || cmd->argc > 3 || cmd->overflow)
    {
      if (sl_arg_error (cinfo, "TIME command requires 1 or 2 arguments"))
        return -1;

      OKGO = 0;
    }

    /* Convert start time string */
    if (OKGO)
    {
      if ((starttime = ms_mdtimestr2nstime (cmd->argv[1])) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing start time for TIME: %s",
                 cinfo->hostname, cmd->argv[1]);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing start time"))
          return -1;

        OKGO = 0;
      }

      /* Sanity check for future start time */
      if (OKGO && (time_t)MS_NSTIME2EPOCH (starttime) > time (NULL))
      {
        lprintf (0, "[%s] Start cannot be in future for TIME: %s",
                 cinfo->hostname, cmd->argv[1]);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Start time cannot be in the future"))
          return -1;

        OKGO = 0;
      }
    }

    /* Convert end time string if supplied */
    if (OKGO && cmd->argc == 3)
    {
      if ((endtime = ms_mdtimestr2nstime (cmd->argv[2])) == NSTERROR)
      {
        lprintf (0, "[%s] Error parsing end time for TIME: %s",
                 cinfo->hostname, cmd->argv[2]);

        if (!slinfo->batch && SendReply (cinfo, "ERROR", ERROR_ARGUMENTS, "Error parsing end time"))
          return -1;

        OKGO = 0;
      }
    }

    /* If configuring a specific station selection */
    if (OKGO && cinfo->state == STATE_STATION)
    {
      if (cmd->argc >= 2)
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
      if (slinfo->stationcount == 0 && cmd->argc >= 2)
      {
        cinfo->starttime = starttime;
        cinfo->endtime   = endtime;
      }

      /* Trigger ring configuration and data flow */
      cinfo->state = STATE_RINGCONFIG;
    }
  } /* End of TIME */

  /* ENDFETCH (v3.x) - Stop negotiating, send data, dial-up mode */
  else if (cmdtoken_eq_nocase (cmd, 0, "ENDFETCH"))
  {
    slinfo->dialup = 1;

    /* Trigger ring configuration and data flow */
    cinfo->state = STATE_RINGCONFIG;
  }

  /* END (v3.x and v4.0) - Stop negotiating, send data */
  else if (cmdtoken_eq_nocase (cmd, 0, "END"))
  {
    /* Trigger ring configuration and data flow */
    cinfo->state = STATE_RINGCONFIG;
  }

  /* Unrecognized command */
  else
  {
    snprintf (sendbuffer, sizeof (sendbuffer),
              "Unrecognized command: %.50s", cinfo->recvbuf);

    lprintf (1, "[%s] %s", cinfo->hostname, sendbuffer);

    if (SendReply (cinfo, "ERROR", ERROR_UNSUPPORTED, sendbuffer))
      return -1;

    return 1;
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
HandleInfo_v3 (ClientInfo *cinfo, CmdToken *cmd)
{
  SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
  char *xmlstr   = NULL;
  int xmllength;
  const char *level = NULL;
  char errflag      = 0;

  char *record = NULL;
  int8_t swapflag;

  uint16_t year = 0;
  uint16_t yday = 0;
  uint8_t hour  = 0;
  uint8_t min   = 0;
  uint8_t sec   = 0;
  uint32_t nsec = 0;

  if (cmd->argc < 2 || !cmd->argv[1] || cmd->argv[1][0] == '\0')
  {
    lprintf (0, "[%s] HandleInfo: INFO requested without a level", cinfo->hostname);
    return -1;
  }

  level = cmd->argv[1];

  /* Allocate miniSEED record buffer */
  if ((record = calloc (1, SLINFORECSIZE)) == NULL)
  {
    lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
    return -1;
  }

  WriteAccessLog (cinfo, "command", "INFO", level, NULL, NULL);

  /* Parse INFO request to determine level */
  if (cmdtoken_eq_nocase (cmd, 1, "ID"))
  {
    /* This is used to "ping" the server so only report at high verbosity */
    lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);

    xmlstr = info_xml_slv3_id (cinfo, SLSERVER_ID);
  }
  else if (cmdtoken_eq_nocase (cmd, 1, "CAPABILITIES"))
  {
    lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);

    xmlstr = info_xml_slv3_capabilities (cinfo, SLSERVER_ID);
  }
  else if (cmdtoken_eq_nocase (cmd, 1, "STATIONS"))
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
  else if (cmdtoken_eq_nocase (cmd, 1, "STREAMS"))
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
  else if (cmdtoken_eq_nocase (cmd, 1, "CONNECTIONS"))
  {
    if (!(cinfo->permissions & TRUST_PERMISSION))
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

    xmlstr  = info_xml_slv3_id (cinfo, SLSERVER_ID);
    errflag = 1;
  }

  /* Pack XML into miniSEED and send to client */
  if (xmlstr)
  {
    /* Trim final newline character if present */
    xmllength = strlen (xmlstr);
    if (xmllength > 0 && xmlstr[xmllength - 1] == '\n')
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
      char seqnumstr[12];
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
HandleInfo_v4 (ClientInfo *cinfo, CmdToken *cmd)
{
  char *json_string = NULL;
  const char *item    = NULL;
  const char *station = NULL;
  char *matchregex  = NULL;
  char *rejectregex = NULL;
  int errflag = 0;

  Selector selector = {.string = {0}, .convert = CONVERT_NONE, .next = NULL};

  if (cmd->argc < 2)
  {
    lprintf (0, "[%s] INFO requested without a level", cinfo->hostname);

    json_string = error_json (cinfo, SLSERVER_ID, "ARGUMENTS", "No INFO item specified");
    errflag     = 1;
  }
  else if (cmd->argc > 4 || cmd->overflow)
  {
    lprintf (0, "[%s] INFO request has too many arguments", cinfo->hostname);

    json_string = error_json (cinfo, SLSERVER_ID, "ARGUMENTS", "INFO request has too many arguments");
    errflag     = 1;
  }
  else
  {
    item    = cmd->argv[1];
    station = (cmd->argc >= 3) ? cmd->argv[2] : NULL;

    /* Copy optional stream selector pattern if provided */
    if (cmd->argc >= 4)
    {
      strncpy (selector.string, cmd->argv[3], sizeof (selector.string) - 1);
      selector.string[sizeof (selector.string) - 1] = '\0';
    }

    WriteAccessLog (cinfo, "command", "INFO", item, NULL, NULL);

    if (cmdtoken_eq_nocase (cmd, 1, "ID"))
    {
      /* This is used to "ping" the server so only report at high verbosity */
      lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);

      json_string = info_json (cinfo, SLSERVER_ID, INFO_ID, NULL);
    }
    else if (cmdtoken_eq_nocase (cmd, 1, "CAPABILITIES"))
    {
      lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);

      json_string = info_json (cinfo, SLSERVER_ID, INFO_CAPABILITIES, NULL);
    }
    else if (cmdtoken_eq_nocase (cmd, 1, "FORMATS"))
    {
      lprintf (1, "[%s] Received INFO FORMATS request", cinfo->hostname);

      json_string = info_json (cinfo, SLSERVER_ID, INFO_FORMATS | INFO_FILTERS, NULL);
    }
    else if (cmdtoken_eq_nocase (cmd, 1, "STATIONS"))
    {
      lprintf (1, "[%s] Received INFO STATIONS request", cinfo->hostname);

      /* Configure regex for matching included station and stream patterns */
      if (station != NULL)
      {
        if (StationToRegex (station, (selector.string[0] != '\0') ? &selector : NULL,
                            &matchregex, &rejectregex))
        {
          lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
          SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
          return -1;
        }
      }

      json_string = info_json (cinfo, SLSERVER_ID, INFO_STATIONS, matchregex);
    }
    else if (cmdtoken_eq_nocase (cmd, 1, "STREAMS"))
    {
      lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);

      /* Configure regex for matching included station and stream patterns */
      if (station != NULL)
      {
        if (StationToRegex (station, (selector.string[0] != '\0') ? &selector : NULL,
                            &matchregex, &rejectregex))
        {
          lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
          SendReply (cinfo, "ERROR", ERROR_INTERNAL, "Error with StationToRegex()");
          return -1;
        }
      }

      json_string = info_json (cinfo, SLSERVER_ID, INFO_STATION_STREAMS, matchregex);
    }
    else if (cmdtoken_eq_nocase (cmd, 1, "CONNECTIONS"))
    {
      if (!(cinfo->permissions & TRUST_PERMISSION))
      {
        lprintf (1, "[%s] Refusing INFO CONNECTIONS request from un-trusted client", cinfo->hostname);
        json_string = error_json (cinfo, SLSERVER_ID, "UNAUTHORIZED", "Client is not authorized to request connections");
        errflag     = 1;
      }
      else
      {
        lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);

        json_string = info_json (cinfo, SLSERVER_ID, INFO_CONNECTIONS, station);
      }
    }
    /* Unrecognized INFO request */
    else
    {
      json_string = error_json (cinfo, SLSERVER_ID, "ARGUMENTS", "Unrecognized INFO item");
      errflag     = 1;

      lprintf (0, "[%s] Unrecognized INFO item: %s", cinfo->hostname, item);
    }
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
  char header[300]  = {0};
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
      if (ms_sid2nslc_n (packet->streamid, net, sizeof (net), sta, sizeof (sta), NULL, 0, NULL, 0))
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

  for (Selector *next, *node = stationid->selectors; node; node = next)
  {
    next = node->next;
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
      free (newkey);
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

///////////////////////////////////////////////////////////////////////////////
// StationToRegex:
//
// Update match and reject regexes for the specified station ID
// and (comma delimited) selector list.
//
// The match regex is a logical OR of all the selectors in the selector list.
// The reject regex is a logical AND of all the selectors in the selector list.
//
// Station & Selector mapping to Regex patterns:
// ---------------------------------------------
// Station ID | Selector    | Regex       | pattern
// ---------------------------------------------
// XX_STA     | <NULL>      | matchregex  | FDSN:XX_STA_.*/MSEED3?$
// XX_*       | <NULL>      | matchregex  | FDSN:XX_.*_.*/MSEED3?$
// XX_STA     | *_B_H_?     | matchregex  | FDSN:XX_STA_.*_B_H_./MSEED3?$
// XX_S??     | *_B_H_?     | matchregex  | FDSN:XX_S.._.*_B_H_./MSEED3?$
// XX_STA     | 00_B_H_*    | matchregex  | FDSN:XX_STA_00_B_H_.*/MSEED3?$
// XX_STA     | 100_G_SR_D  | matchregex  | FDSN:XX_STA_100_G_SR_D/MSEED3?$
// XX_STA     | !*_B_H_?    | rejectregex | FDSN:XX_STA_.*_B_H_./MSEED3?$
// XX_S*      | !*_?_H_?    | rejectregex | FDSN:XX_S.*_.*_._H_./MSEED3?$
//
// Labels are also supported, and are matched against the stream label suffix:
// Station ID | Selector    | Label | Regex       | pattern
// ---------------------------------------------
// XX_STA     | 100_G_SR_D  | 1SEC  | matchregex  | FDSN:XX_STA_100_G_SR_D/MSEED3?/1SEC$
// XX_STA     | !100_G_SR_D | 1SEC  | rejectregex | FDSN:XX_STA_100_G_SR_D/MSEED3?/1SEC$
// ---------------------------------------------
//
// Return 0 on success and -1 on error.
///////////////////////////////////////////////////////////////////////////////
static int
StationToRegex (const char *staid, Selector *selector,
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
          if (SelectToRegex (staid, NULL, NULL, matchregex))
          {
            lprintf (0, "Error with SelectToRegex");
            return -1;
          }

          matched++;
        }

        if (SelectToRegex (staid, &(selector->string[1]), selector->label, rejectregex))
        {
          lprintf (0, "Error with SelectToRegex");
          return -1;
        }
      }
      /* Handle regular selector */
      else
      {
        if (SelectToRegex (staid, selector->string, selector->label, matchregex))
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
    if (SelectToRegex (staid, NULL, NULL, matchregex))
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
 * Create a regular expression for ring stream IDs for the specified station ID
 * (NET_STA) and SeedLink selector and add it to the string specified by regex,
 * extending it as needed.  The regex string will only be extended up to a
 * maximum of SLMAXREGEXLEN bytes.
 *
 * A regex pattern will be created that matches FDSN Source Identifiers with
 * either "/MSEED" or "/MSEED3" suffixes.
 *
 * Each regex in the final string is separated with an alternation "|" (logical
 * OR) to create a single, combined regex pattern.
 *
 * Mapping of SeedLink wildcards to regex is as follows:
 *   '?' -> '.'
 *   '*' -> '.*'
 *
 * The "DECOTL" subtypes or subformats of SeedLink selectors are not supported,
 * anything following a '.' in a selector will be ignored.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
SelectToRegex (const char *staid, const char *select, const char *label, char **regex)
{
  const char *ptr;
  char pattern[256] = {0};
  char *build       = pattern;
  char *build_end   = pattern + sizeof (pattern) - (SLMAXLABELLEN + 16); /* Reserve space for "/MSEED3?/LABEL$\0" suffix */
  int retval;

  if (!regex)
    return -1;

  /* Sanity check lengths of input strings */
  if (staid && strlen (staid) > 50)
    return -1;
  if (select && strlen (select) > 50)
    return -1;

  /* Add starting FDSN Source ID prefix */
  memcpy (build, "FDSN:", 5);
  build += 5;

  /* Copy station pattern if provided, translating globbing wildcards to regex */
  if (staid)
  {
    for (ptr = staid; *ptr; ptr++)
    {
      if (*ptr == '?')
      {
        if (build >= build_end)
          return -1;
        *build++ = '.';
      }
      else if (*ptr == '*')
      {
        if (build + 1 >= build_end)
          return -1;
        *build++ = '.';
        *build++ = '*';
      }
      else
      {
        if (build >= build_end)
          return -1;
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
  if (select && select[0] != '\0')
  {
    /* Skip '-' at the beginning of the selector representing empty location codes */
    while (*select == '-')
      select++;

    for (ptr = select; *ptr; ptr++)
    {
      if (*ptr == '?')
      {
        if (build >= build_end)
          return -1;
        *build++ = '.';
      }
      else if (*ptr == '*')
      {
        if (build + 1 >= build_end)
          return -1;
        *build++ = '.';
        *build++ = '*';
      }
      else
      {
        if (build >= build_end)
          return -1;
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

  /* Finish with optional /MSEED suffix and optional 3, then optional label, then '$' anchor */
  if (label && label[0] != '\0')
  {
    memcpy (build, "/MSEED3?/", 9);
    build += 9;

    for (ptr = label; *ptr; ptr++)
    {
      if (*ptr == '?')
      {
        if (build >= pattern + sizeof (pattern) - 2)
          return -1;
        *build++ = '.';
      }
      else if (*ptr == '*')
      {
        if (build + 1 >= pattern + sizeof (pattern) - 2)
          return -1;
        *build++ = '.';
        *build++ = '*';
      }
      else
      {
        if (build >= pattern + sizeof (pattern) - 2)
          return -1;
        *build++ = *ptr;
      }
    }

    *build++ = '$';
    *build   = '\0';
  }
  else
  {
    memcpy (build, "/MSEED3?$", 9);
  }

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

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

#include "auth.h"
#include "clients.h"
#include "cmdtoken.h"
#include "dlclient.h"
#include "generic.h"
#include "http.h"
#include "infoxml.h"
#include "logging.h"
#include "mseedscan.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int HandleNegotiation (ClientInfo *cinfo, CmdToken *cmd);
static int HandleWrite (ClientInfo *cinfo, CmdToken *cmd);
static int HandleRead (ClientInfo *cinfo, CmdToken *cmd);
static int HandleInfo (ClientInfo *cinfo, CmdToken *cmd);
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
 * Returns 0 on success, 1 on client error (e.g. unrecognized command),
 * and negative value on fatal error.  On fatal error the client should
 * be disconnected.
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

  /* Tokenize the command buffer once; all handlers receive the same CmdToken */
  CmdToken cmd;
  if (cmdtoken_parse (&cmd, cinfo->dlcommand) < 0 || cmd.argc == 0)
  {
    lprintf (1, "[%s] Error tokenizing command buffer", cinfo->hostname);
    return 1;
  }

  /* Determine if this is a data submission and handle (case-sensitive dispatch) */
  if (cmdtoken_eq_nocase (&cmd, 0, "WRITE"))
  {
    /* Check for write permission */
    if (!(cinfo->permissions & WRITE_PERMISSION))
    {
      lprintf (1, "[%s] Data packet received from client without write permission",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Write permission not granted, no soup for you!", 0, 1, 1);
      return -1;
    }
    /* Any errors from HandleWrite are fatal */
    else if (HandleWrite (cinfo, &cmd))
    {
      return -1;
    }
  }

  /* Determine if this is an INFO request and handle */
  else if (cmdtoken_eq_nocase (&cmd, 0, "INFO"))
  {
    /* Any errors from HandleInfo are fatal */
    if (HandleInfo (cinfo, &cmd))
    {
      return -1;
    }
  }

  /* Determine if this is a specific read request and handle */
  else if (cmdtoken_eq_nocase (&cmd, 0, "READ"))
  {
    cinfo->state = STATE_COMMAND;

    /* Any errors from HandleRead are fatal */
    if (HandleRead (cinfo, &cmd))
    {
      return -1;
    }
  }

  /* Determine if this is a request to end STREAMing and set state (must precede STREAM) */
  else if (cmdtoken_eq_nocase (&cmd, 0, "ENDSTREAM"))
  {
    /* Send ENDSTREAM */
    if (SendPacket (cinfo, "ENDSTREAM", NULL, 0, 0, 0))
    {
      return -1;
    }

    cinfo->state = STATE_COMMAND;
  }

  /* Determine if this is a request to start STREAMing and set state */
  else if (cmdtoken_eq_nocase (&cmd, 0, "STREAM"))
  {
    /* Check for authentication requirement */
    if (config.auth.required && !(cinfo->permissions & AUTHENTICATED))
    {
      lprintf (1, "[%s] Streaming requested from client without authentication",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Authentication required for streaming, no soup for you!", 0, 1, 1);
      return -1;
    }

    /* Set read position to next packet if position not set */
    if (cinfo->reader->pktid == RINGID_NONE)
    {
      cinfo->reader->pktid = RINGID_NEXT;
    }

    WriteAccessLog (cinfo, "command", "STREAM", NULL, cinfo->matchstr, cinfo->rejectstr);

    cinfo->state = STATE_STREAM;
  }

  /* Otherwise a negotiation command */
  else
  {
    /* If this is not an ID request, set to a non-streaming state */
    if (!cmdtoken_eq_nocase (&cmd, 0, "ID"))
      cinfo->state = STATE_COMMAND;

    int rv = HandleNegotiation (cinfo, &cmd);
    if (rv < 0)
      return -1;
    if (rv > 0)
      return 1;
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
 * dl_arg_error:
 *
 * Send an ERROR reply to a DataLink client.  Returns the result of
 * SendPacket (-1 on fatal socket error, 0 on success).
 ***************************************************************************/
static int
dl_arg_error (ClientInfo *cinfo, const char *msg)
{
  return SendPacket (cinfo, "ERROR", (char *)msg, 0, 1, 1);
}

/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation commands implementing server-side DataLink
 * protocol, updating the connection configuration accordingly.
 *
 * DataLink commands handled:
 * ID
 * AUTH USERPASS size|USER\rPASS
 * AUTH JWT size|JWT
 * POSITION SET pktid [pkttime]
 * POSITION AFTER datatime
 * MATCH size|<match pattern of length size>
 * REJECT size|<match pattern of length size>
 *
 * All commands handled by this function will return the resulting
 * status to the client.
 *
 * Returns 0 on success, 1 on client error (e.g. unrecognized command),
 * and -1 on fatal error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (ClientInfo *cinfo, CmdToken *cmd)
{
  char sendbuffer[255];
  size_t size;
  uint64_t size_u64;
  int selected;
  char OKGO = 1;

  if (!cinfo || !cmd)
    return -1;

  /* ID - Return server ID, version and capability flags */
  if (cmdtoken_eq_nocase (cmd, 0, "ID"))
  {
    /* Optional client ID is everything after "ID " */
    const char *clientid = cmdtoken_rest_after (cmd, 1);

    if (clientid)
    {
      strncpy (cinfo->clientid, clientid, sizeof (cinfo->clientid) - 1);
      cinfo->clientid[sizeof (cinfo->clientid) - 1] = '\0';
      lprintf (2, "[%s] Received ID (%s)", cinfo->hostname, cinfo->clientid);
    }
    else
    {
      lprintf (2, "[%s] Received ID", cinfo->hostname);
    }

    /* Create server version and capability flags string (DLSERVER_ID + PACKETSIZE + WRITE if permission) */
    snprintf (sendbuffer, sizeof (sendbuffer),
              "ID " DLSERVER_ID " PACKETSIZE:%lu%s%s",
              (unsigned long int)(param.pktsize - sizeof (RingPacket)),
              (cinfo->permissions & WRITE_PERMISSION) ? " WRITE" : "",
              (config.auth.program) ? " AUTH" : "");

    /* Send the server ID string */
    if (SendPacket (cinfo, sendbuffer, NULL, 0, 0, 0))
      return -1;
  }

  /* AUTH <USERPASS|JWT> size\r\n<credential-bytes> */
  else if (cmdtoken_eq_nocase (cmd, 0, "AUTH"))
  {
    char credential[4096] = {0};

    char *username = NULL;
    char *password = NULL;
    char *jwtoken  = NULL;

    OKGO = 1;

    /* AUTH requires exactly 2 arguments: sub-command and byte-size */
    if (cmd->argc != 3 || cmd->overflow)
    {
      if (dl_arg_error (cinfo, "AUTH requires 2 arguments"))
        return -1;

      OKGO = 0;
    }
    else if (cmdtoken_u64 (cmd, 2, &size_u64, 10) < 0)
    {
      if (dl_arg_error (cinfo, "AUTH size argument is invalid"))
        return -1;

      OKGO = 0;
    }
    else if (size_u64 > (sizeof (credential) - 1))
    {
      if (dl_arg_error (cinfo, "AUTH credentials are too large"))
        return -1;

      OKGO = 0;
    }
    else
    {
      size = (size_t)size_u64;

      /* Read credentials of size bytes */
      if (RecvData (cinfo, credential, size, 1) < 0)
      {
        lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
        return -1;
      }

      /* Sub-command comparison is case-sensitive (DataLink convention) */
      if (cmdtoken_eq_nocase (cmd, 1, "USERPASS"))
      {
        /* Parse USERNAME and PASSWORD from credential payload */
        char *ptr = strchr (credential, '\r');

        if (ptr == NULL)
        {
          if (SendPacket (cinfo, "ERROR", "AUTH USERPASS requires payload of USER\\rPASS", 0, 1, 1))
            return -1;

          OKGO = 0;
        }
        else
        {
          *ptr     = '\0';
          username = credential;
          password = ptr + 1;
        }
      }
      else if (cmdtoken_eq_nocase (cmd, 1, "JWT"))
      {
        jwtoken = credential;
      }
      else
      {
        lprintf (0, "[%s] Unsupported AUTH type: %s", cinfo->hostname, cmd->argv[1]);
        if (SendPacket (cinfo, "ERROR", "Unsupported AUTH type", 0, 1, 1))
          return -1;
      }
    }

    if (OKGO && PerformAuth (cinfo, username, password, jwtoken))
    {
      lprintf (0, "[%s] Error performing authentication", cinfo->hostname);

      if (SendPacket (cinfo, "ERROR", "Error performing authentication", 0, 1, 1))
        return -1;

      OKGO = 0;
    }

    if (OKGO && !(cinfo->permissions & CONNECT_PERMISSION))
    {
      lprintf (0, "[%s] Authentication failed, not allowed to connect", cinfo->hostname);

      if (SendPacket (cinfo, "ERROR", "Authentication failed, not allowed to connect", 0, 1, 1))
        return -1;

      OKGO = 0;
    }
    else if (OKGO)
    {
      lprintf (2, "[%s] Authentication successful", cinfo->hostname);

      if (SendPacket (cinfo, "OK", "Authentication successful", 0, 1, 1))
        return -1;
    }
  } /* End of AUTH */

  /* POSITION <SET|AFTER> value [time] - Set ring reading position */
  else if (cmdtoken_eq_nocase (cmd, 0, "POSITION"))
  {
    uint64_t pktid = 0;
    nstime_t nstime;

    OKGO = 1;

    /* POSITION requires 2 or 3 arguments after the command keyword */
    if (cmd->argc < 3 || cmd->argc > 4 || cmd->overflow)
    {
      if (dl_arg_error (cinfo, "POSITION requires 2 or 3 arguments"))
        return -1;

      OKGO = 0;
    }
    else
    {
      /* Process SET positioning */
      if (cmdtoken_eq_nocase (cmd, 1, "SET"))
      {
        int64_t hptime = INT64_MIN;

        /* SET EARLIEST / SET LATEST do not accept a time argument */
        if (cmdtoken_eq_nocase (cmd, 2, "EARLIEST"))
        {
          pktid = RINGID_EARLIEST;
        }
        else if (cmdtoken_eq_nocase (cmd, 2, "LATEST"))
        {
          pktid = RINGID_LATEST;
        }
        /* Otherwise SET <pktid> [time] - cmdtoken_u64 fully validates the pktid */
        else if (cmdtoken_u64 (cmd, 2, &pktid, 10) < 0)
        {
          lprintf (0, "[%s] Error with POSITION SET value: %s", cinfo->hostname, cmd->argv[2]);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION SET value", 0, 1, 1))
            return -1;
          OKGO = 0;
        }
        else if (cmd->argc == 4)
        {
          if (cmdtoken_i64 (cmd, 3, &hptime, 10) < 0)
          {
            lprintf (0, "[%s] Error with POSITION SET time: %s", cinfo->hostname, cmd->argv[3]);
            if (SendPacket (cinfo, "ERROR", "Error with POSITION SET time", 0, 1, 1))
              return -1;
            OKGO = 0;
          }
        }

        /* Position the reader. Limit packet time matching to integer seconds to allow
         * for client-supplied packet times of unknown precision. */
        if (OKGO)
        {
          int64_t saved_pktoffset = cinfo->reader->pktoffset;
          uint64_t saved_pktid    = cinfo->reader->pktid;
          nstime_t saved_pkttime  = cinfo->reader->pkttime;

          uint64_t position = RingPosition (cinfo->reader, pktid, NSTUNSET);

          if (hptime != INT64_MIN && position <= RINGID_MAXIMUM &&
              (int64_t)(MS_NSTIME2EPOCH (cinfo->reader->pkttime)) != hptime / 1000000)
          {
            cinfo->reader->pktoffset = saved_pktoffset;
            cinfo->reader->pktid     = saved_pktid;
            cinfo->reader->pkttime   = saved_pkttime;

            position = RINGID_NONE;
          }

          if (position == RINGID_ERROR)
          {
            lprintf (0, "[%s] Error with RingPosition (pktid: %" PRIu64 ", hptime: %" PRId64 ")",
                     cinfo->hostname, pktid, hptime);
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
      /* Process AFTER <time> positioning (case-sensitive sub-command) */
      else if (cmdtoken_eq_nocase (cmd, 1, "AFTER"))
      {
        int64_t hptime = 0;

        if (cmdtoken_i64 (cmd, 2, &hptime, 10) < 0)
        {
          lprintf (0, "[%s] Error parsing POSITION AFTER time: %s", cinfo->hostname, cmd->argv[2]);
          if (SendPacket (cinfo, "ERROR", "Error with POSITION AFTER time", 0, 1, 1))
            return -1;
        }
        else
        {
          char timestr[32];

          /* Wire protocol uses time in microseconds ("hptime"), convert to nanoseconds ("nstime") */
          nstime = MS_HPTIME2NSTIME (hptime);
          ms_nstime2timestr_n (nstime, timestr, sizeof (timestr), ISOMONTHDAY_Z, NANO_MICRO_NONE);

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
            lprintf (0, "Time window search limit is invalid: %f", (double)config.timewinlimit);
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
        lprintf (0, "[%s] Unsupported POSITION subcommand: %s", cinfo->hostname, cmd->argv[1]);
        if (SendPacket (cinfo, "ERROR", "Unsupported POSITION subcommand", 0, 1, 1))
          return -1;
      }
    }
  } /* End of POSITION */

  /* MATCH [size]\r\n[pattern] - Provide regex to match streamids */
  else if (cmdtoken_eq_nocase (cmd, 0, "MATCH"))
  {
    /* MATCH with no size argument removes current match */
    if (cmd->argc == 1)
    {
      free (cinfo->matchstr);
      cinfo->matchstr = NULL;
      RingMatch (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match", selected);
      if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
        return -1;
    }
    else if (cmd->argc > 2 || cmd->overflow)
    {
      if (dl_arg_error (cinfo, "MATCH requires a single argument"))
        return -1;
    }
    else if (cmdtoken_u64 (cmd, 1, &size_u64, 10) < 0)
    {
      if (dl_arg_error (cinfo, "MATCH size argument is invalid"))
        return -1;
    }
    else if (size_u64 > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] match expression too large (%" PRIu64 ")",
               cinfo->hostname, size_u64);

      snprintf (sendbuffer, sizeof (sendbuffer), "match expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;
    }
    else
    {
      size = (size_t)size_u64;

      free (cinfo->matchstr);
      cinfo->matchstr = NULL;

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
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after match", selected);
        if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
          return -1;
      }
    }
  } /* End of MATCH */

  /* REJECT [size]\r\n[pattern] - Provide regex to reject streamids */
  else if (cmdtoken_eq_nocase (cmd, 0, "REJECT"))
  {
    /* REJECT with no size argument removes current reject */
    if (cmd->argc == 1)
    {
      free (cinfo->rejectstr);
      cinfo->rejectstr = NULL;
      RingReject (cinfo->reader, 0);

      selected = SelectedStreams (cinfo->reader);
      snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject", selected);
      if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
        return -1;
    }
    else if (cmd->argc > 2 || cmd->overflow)
    {
      if (dl_arg_error (cinfo, "REJECT requires a single argument"))
        return -1;
    }
    else if (cmdtoken_u64 (cmd, 1, &size_u64, 10) < 0)
    {
      if (dl_arg_error (cinfo, "REJECT size argument is invalid"))
        return -1;
    }
    else if (size_u64 > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] reject expression too large (%" PRIu64 ")",
               cinfo->hostname, size_u64);

      snprintf (sendbuffer, sizeof (sendbuffer), "reject expression too large, must be <= %d",
                DLMAXREGEXLEN);
      if (SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1))
        return -1;
    }
    else
    {
      size = (size_t)size_u64;

      free (cinfo->rejectstr);
      cinfo->rejectstr = NULL;

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
        snprintf (sendbuffer, sizeof (sendbuffer), "%d streams selected after reject", selected);
        if (SendPacket (cinfo, "OK", sendbuffer, (selected >= 0) ? (uint64_t)selected : 0, 1, 1))
          return -1;
      }
    }
  } /* End of REJECT */

  /* BYE - End connection */
  else if (cmdtoken_eq_nocase (cmd, 0, "BYE"))
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

    return 1;
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
HandleWrite (ClientInfo *cinfo, CmdToken *cmd)
{
  DLInfo *dlinfo;
  StreamNode *stream;
  char replystr[200];
  char streamid[101];
  char flags[101];
  int nread;
  int newstream = 0;

  int64_t hpdatastart = 0;
  int64_t hpdataend   = 0;
  uint32_t datasize   = 0;
  int rv;

  MS3Record *msr = NULL;

  if (!cinfo || !cinfo->extinfo || !cmd)
    return -1;

  dlinfo = (DLInfo *)cinfo->extinfo;

  /* WRITE <streamid> <datastart> <dataend> <flags> <datasize> [pktid]
   * argc is 6 (no pktid) or 7 (with pktid) */
  if (cmd->argc < 6 || cmd->argc > 7 || cmd->overflow)
  {
    lprintf (1, "[%s] Error parsing WRITE parameters: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

    return -1;
  }

  /* Stream ID */
  if (strlen (cmd->argv[1]) > (MAXSTREAMID - 1))
  {
    lprintf (1, "[%s] Error, stream ID too long: %.100s",
             cinfo->hostname, cmd->argv[1]);

    SendPacket (cinfo, "ERROR", "Error, stream ID too long", 0, 1, 1);

    return -1;
  }
  strncpy (streamid, cmd->argv[1], sizeof (streamid) - 1);
  streamid[sizeof (streamid) - 1] = '\0';

  /* Data start and end times (hptime, microseconds since epoch) */
  if (cmdtoken_i64 (cmd, 2, &hpdatastart, 10) < 0 ||
      cmdtoken_i64 (cmd, 3, &hpdataend, 10) < 0)
  {
    lprintf (1, "[%s] Error parsing WRITE data times: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

    return -1;
  }

  /* Flags */
  strncpy (flags, cmd->argv[4], sizeof (flags) - 1);
  flags[sizeof (flags) - 1] = '\0';

  /* Data size */
  if (cmdtoken_u32 (cmd, 5, &datasize, 10) < 0)
  {
    lprintf (1, "[%s] Error parsing WRITE data size: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

    return -1;
  }
  cinfo->packet.datasize = datasize;

  /* Optional packet ID (only honored when flags contain 'I').  Parse into
   * an aligned local and assign, since cinfo->packet is a packed struct. */
  if (cmd->argc == 7 && strchr (flags, 'I') != NULL)
  {
    uint64_t pktid = 0;

    if (cmdtoken_u64 (cmd, 6, &pktid, 10) < 0)
    {
      lprintf (1, "[%s] Error parsing WRITE packet ID: %.100s",
               cinfo->hostname, cinfo->dlcommand);

      SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);

      return -1;
    }

    cinfo->packet.pktid = pktid;
  }
  else
  {
    cinfo->packet.pktid = RINGID_NONE;
  }

  /* Store parsed data start/end (convert hptime → nstime below) */
  cinfo->packet.datastart = hpdatastart;
  cinfo->packet.dataend   = hpdataend;

  /* Translate legacy stream ID: NN_SSSSS_LL_CCC/MSEED
   * to an FDSN Source ID: FDSN:NN_SSSSS_LL_C_C_C/MSEED */
  if (dlinfo->legacy_mseed_streamid_match != NULL &&
      pcre2_match (dlinfo->legacy_mseed_streamid_match, (PCRE2_SPTR8)streamid,
                   PCRE2_ZERO_TERMINATED, 0, 0,
                   dlinfo->legacy_mseed_streamid_data, GetMatchContext ()) > 0)
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
    strncpy (cinfo->packet.streamid, streamid, sizeof (cinfo->packet.streamid) - 1);
    cinfo->packet.streamid[sizeof (cinfo->packet.streamid) - 1] = '\0';
  }

  /* Wire protocol for DataLink uses time stamps in as microseconds since the epoch,
   * convert these to the nanosecond ticks used internally. */
  cinfo->packet.datastart = MS_HPTIME2NSTIME (cinfo->packet.datastart);
  cinfo->packet.dataend   = MS_HPTIME2NSTIME (cinfo->packet.dataend);

  /* Check that client is allowed to write this stream ID */
  if (cinfo->reader->allowed)
  {
    if (pcre2_match (cinfo->reader->allowed, (PCRE2_SPTR8)cinfo->packet.streamid,
                     PCRE2_ZERO_TERMINATED, 0, 0,
                     cinfo->reader->allowed_data, GetMatchContext ()) < 0)
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

        msr3_free (&msr);
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
                               cinfo->packet.streamid,
                               cinfo->streamscount, &newstream)) == NULL)
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
  stream->rxpackets++;
  stream->rxbytes += cinfo->packet.datasize;

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
HandleRead (ClientInfo *cinfo, CmdToken *cmd)
{
  uint64_t reqid  = 0;
  uint64_t readid = 0;
  char replystr[100];

  if (!cinfo || !cmd)
    return -1;

  /* Parse command parameters: READ <pktid> */
  if (cmd->argc != 2 || cmd->overflow || cmdtoken_u64 (cmd, 1, &reqid, 10) < 0)
  {
    lprintf (1, "[%s] Error parsing READ parameters: %.100s",
             cinfo->hostname, cinfo->dlcommand);

    if (SendPacket (cinfo, "ERROR", "Error parsing READ command parameters", 0, 1, 1))
      return -1;

    return 0;
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
HandleInfo (ClientInfo *cinfo, CmdToken *cmd)
{
  char string[200];
  char *xmlstr = NULL;
  int xmllength;
  const char *item = NULL;
  const char *type = NULL;
  char *matchexpr  = NULL;
  char *matchstr   = NULL;
  size_t size      = 0;

  if (!cinfo || !cmd)
    return -1;

  /* INFO requires at least a type argument */
  if (cmd->argc < 2)
  {
    lprintf (0, "[%s] HandleInfo: INFO requested without a type", cinfo->hostname);
    SendPacket (cinfo, "ERROR", "INFO request has no type", 0, 1, 1);
    return -1;
  }

  item = cmd->argv[1];
  type = item;

  /* Optional match expression size: INFO <type> <size> */
  if (cmd->argc > 3 || cmd->overflow)
  {
    lprintf (0, "[%s] INFO request has extra arguments", cinfo->hostname);
    SendPacket (cinfo, "ERROR", "INFO request has extra arguments", 0, 1, 1);
    return -1;
  }
  else if (cmd->argc == 3)
  {
    uint64_t size_u64 = 0;

    if (cmdtoken_u64 (cmd, 2, &size_u64, 10) < 0)
    {
      lprintf (0, "[%s] INFO size argument is invalid", cinfo->hostname);
      SendPacket (cinfo, "ERROR", "INFO size argument is invalid", 0, 1, 1);
      return -1;
    }
    else if (size_u64 > DLMAXREGEXLEN)
    {
      lprintf (0, "[%s] INFO match expression too large (%" PRIu64 ")",
               cinfo->hostname, size_u64);

      snprintf (string, sizeof (string), "match expression too large, must be <= %d",
                DLMAXREGEXLEN);
      SendPacket (cinfo, "ERROR", string, 0, 1, 1);
      return -1;
    }

    size = (size_t)size_u64;

    if (size > 0)
    {
      /* Read match expression of size bytes from socket */
      if (!(matchstr = (char *)malloc (size + 1)))
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        return -1;
      }

      if (RecvData (cinfo, matchstr, size, 1) < 0)
      {
        lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
        free (matchstr);
        return -1;
      }

      /* Make sure buffer is a terminated string */
      matchstr[size] = '\0';
      matchexpr      = matchstr;
    }
  }

  WriteAccessLog (cinfo, "command", "INFO", item, NULL, NULL);

  /* Add contents to the XML structure depending on info request */
  if (cmdtoken_eq_nocase (cmd, 1, "STATUS"))
  {
    lprintf (1, "[%s] Received INFO STATUS request", cinfo->hostname);
    type = "INFO STATUS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "STATUS", matchexpr, cinfo->permissions & TRUST_PERMISSION);
  } /* End of STATUS */
  else if (cmdtoken_eq_nocase (cmd, 1, "STREAMS"))
  {
    lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);
    type = "INFO STREAMS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "STREAMS", matchexpr, cinfo->permissions & TRUST_PERMISSION);
  } /* End of STREAMS */
  else if (cmdtoken_eq_nocase (cmd, 1, "CONNECTIONS"))
  {
    /* Check for trusted flag, required to access this resource */
    if (!(cinfo->permissions & TRUST_PERMISSION))
    {
      lprintf (1, "[%s] INFO CONNECTIONS request from un-trusted client",
               cinfo->hostname);
      SendPacket (cinfo, "ERROR", "Access to CONNECTIONS denied", 0, 1, 1);

      free (matchstr);
      return -1;
    }

    lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);
    type = "INFO CONNECTIONS";

    xmlstr = info_xml_dlv1 (cinfo, DLSERVER_ID, "CONNECTIONS", matchexpr, cinfo->permissions & TRUST_PERMISSION);
  } /* End of CONNECTIONS */
  /* Unrecognized INFO request */
  else
  {
    lprintf (0, "[%s] Unrecognized INFO request type: %s", cinfo->hostname, item);

    snprintf (string, sizeof (string), "Unrecognized INFO request type: %s", item);
    SendPacket (cinfo, "ERROR", string, 0, 1, 1);

    free (matchstr);
    return -1;
  }

  free (matchstr);

  if (xmlstr == NULL)
  {
    lprintf (0, "[%s] Error generating INFO XML", cinfo->hostname);
    SendPacket (cinfo, "ERROR", "Error generating INFO response", 0, 1, 1);
    return -1;
  }

  /* Trim final newline character if present */
  xmllength = strlen (xmlstr);
  if (xmllength > 0 && xmlstr[xmllength - 1] == '\n')
  {
    xmlstr[xmllength - 1] = '\0';
    xmllength--;
  }

  /* Send XML to client */
  if (SendPacket (cinfo, (char *)type, xmlstr, 0, 0, 1))
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
      lprintf (0, "[%s] SendPacket(): Error sending packet", cinfo->hostname);

    /* Free the wire packet space if we allocated it */
    if (wirepacket != cinfo->sendbuf)
      free (wirepacket);

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
  headerlen_u8 = (uint8_t)headerlen;
  memcpy (preheader + 2, &headerlen_u8, 1);

  /* Send complete wire packet */
  if (SendDataMB (cinfo,
                  (void *[]){preheader, header, cinfo->sendbuf},
                  (size_t[]){3, headerlen, cinfo->packet.datasize},
                  3, 0))
  {
    if (cinfo->socketerr != -2)
      lprintf (0, "[%s] SendRingPacket(): Error sending packet", cinfo->hostname);
    return -1;
  }

  /* Get (creating if needed) the StreamNode for this streamid */
  if ((stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
                               cinfo->packet.streamid,
                               cinfo->streamscount, &newstream)) == NULL)
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
  stream->txpackets++;
  stream->txbytes += cinfo->packet.datasize;

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

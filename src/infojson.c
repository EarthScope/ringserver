/**************************************************************************
 * infojson.c
 *
 * SeedLink client thread specific routines.
 *
 * Create JSON formatted information about the server state, including
 * stream lists, connnections, etc.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libmseed.h>

#include "cJSON.h"
#include "generic.h"
#include "infojson.h"
#include "slclient.h"
#include "ring.h"

/***************************************************************************
 * info_create_root:
 *
 * Initialize and populate root JSON object and items that are in all documents.
 *
 * Returns pointer to root document on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_create_root (const char *software)
{
  cJSON *root;

  if ((root = cJSON_CreateObject ()) == NULL)
  {
    return NULL;
  }

  if (cJSON_AddStringToObject (root, "software", software) == NULL)
  {
    return NULL;
  }

  if (cJSON_AddStringToObject (root, "organization", serverid) == NULL)
  {
    return NULL;
  }

  return root;
}

/***************************************************************************
 * info_add_id:
 *
 * Add more details of server ID to the JSON document.
 *
 * Returns pointer to root object on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_id (cJSON *root)
{
  char string[64];

  /* Add server start time */

  /* Create server start time string as YYYY-MM-DDTHH:MM:SSZ */
  ms_nstime2timestr (serverstarttime, string, ISOMONTHDAY_Z, NONE);

  if (cJSON_AddStringToObject (root, "server_start", string) == NULL)
  {
    return NULL;
  }

  return root;
}

/***************************************************************************
 * info_json_capabilities:
 *
 * Add format specifications to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_capabilities (cJSON *root)
{
  char *string;
  char *cap;
  char *rest;
  cJSON *capabilities;
  cJSON *capability_string;

  if ((string = strdup (SLCAPABILITIESv4)) == NULL)
  {
    return NULL;
  }

  if ((capabilities = cJSON_AddArrayToObject (root, "capability")) == NULL)
  {
    free (string);
    return NULL;
  }

  /* Parse capabilities string on spaces and add each capability */
  rest = string;
  while ((cap = strtok_r (rest, " ", &rest)))
  {
    if ((capability_string = cJSON_CreateString (cap)) == NULL)
    {
      free (string);
      return NULL;
    }

    cJSON_AddItemToArray (capabilities, capability_string);
  }

  free (string);

  return capabilities;
}

/***************************************************************************
 * info_add_formats:
 *
 * Add format specifications to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_formats (cJSON *root)
{
  cJSON *formats;
  cJSON *format;
  cJSON *subformat;

  if ((formats = cJSON_AddObjectToObject (root, "format")) == NULL)
  {
    return NULL;
  }

  /* Add format code for miniSEED 2 */
  if ((format = cJSON_AddObjectToObject (formats, "2")) == NULL ||
      cJSON_AddStringToObject (format, "mimetype", "application/vnd.fdsn.mseed") == NULL ||
      (subformat = cJSON_AddObjectToObject (format, "subformat")) == NULL ||
      cJSON_AddStringToObject (subformat, "D", "Data") == NULL)
  {
    return NULL;
  }

  /* Add format code for miniSEED 3 */
  if ((format = cJSON_AddObjectToObject (formats, "3")) == NULL ||
      cJSON_AddStringToObject (format, "mimetype", "application/vnd.fdsn.mseed3") == NULL ||
      (subformat = cJSON_AddObjectToObject (format, "subformat")) == NULL ||
      cJSON_AddStringToObject (subformat, "D", "Data") == NULL)
  {
    return NULL;
  }

  return formats;
}

/***************************************************************************
 * info_add_filters:
 *
 * Add filter specifications to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_filters (cJSON *root)
{
  cJSON *filters;

  if ((filters = cJSON_AddObjectToObject (root, "filter")) == NULL)
  {
    return NULL;
  }

  /* Add filter code for native */
  if (cJSON_AddStringToObject (filters, "native", "native format of data, no conversion") == NULL)
  {
    return NULL;
  }

  return filters;
}

/***************************************************************************
 * info_add_stations:
 *
 * Add station array, and optionally a stream sub-array, to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_stations (ClientInfo *cinfo, cJSON *root, int include_streams,
                   const char *matchexpr)
{
  Stack *ringstreams;
  RingStream *ringstream;

  struct station_details
  {
    char id[MAXSTREAMID];
    uint64_t earliestid;
    uint64_t latestid;
    Stack *streams;
  };

  struct stream_details
  {
    char id[MAXSTREAMID];
    char format[2];
    char subformat[2];
    nstime_t earliesttime;
    nstime_t latesttime;
  };

  struct station_details *station_details = NULL;
  struct stream_details *stream_details   = NULL;

  Stack *station_stack = NULL;

  char net[16]  = {0};
  char sta[16]  = {0};
  char loc[16]  = {0};
  char chan[16] = {0};

  char staid[MAXSTREAMID]    = {0};
  char streamid[MAXSTREAMID] = {0};

  cJSON *stations = NULL;
  cJSON *station  = NULL;
  cJSON *streams  = NULL;
  cJSON *stream   = NULL;

  int error         = 0;
  char *type        = NULL;
  char string32[32] = {0};

  pcre2_code *match_code       = NULL;
  pcre2_match_data *match_data = NULL;

  /* Compile match expression if provided */
  if (matchexpr && UpdatePattern (&match_code, &match_data, matchexpr, "stream match expression"))
  {
    return NULL;
  }

  /* Get copy of streams as a Stack, sorted by stream ID */
  if ((ringstreams = GetStreamsStack (cinfo->ringparams, cinfo->reader)) == NULL)
  {
    lprintf (0, "[%s] Error getting streams stack", cinfo->hostname);
    return NULL;
  }

  if ((station_stack = StackCreate ()) == NULL)
  {
    lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
    StackDestroy (ringstreams, free);
    return NULL;
  }

  /* Build Stacks of stations and optionally streams */
  while ((ringstream = (RingStream *)StackPop (ringstreams)))
  {
    /* Skip if stream ID does not match provided expression */
    if (match_code &&
        pcre2_match (match_code, (PCRE2_SPTR8)ringstream->streamid, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0)
    {
      free (ringstream);
      continue;
    }

    /* Truncate stream ID at type suffix */
    if ((type = strchr (ringstream->streamid, '/')))
      *type++ = '\0';

    /* Extract codes from FDSN Source ID (streamid) */
    if (strncmp (ringstream->streamid, "FDSN:", 5) == 0)
    {
      if (ms_sid2nslc (ringstream->streamid, net, sta, loc, chan))
      {
        lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, ringstream->streamid);
        free (ringstream);
        error = 1;
        break;
      }

      /* Create station ID as combination of network and station codes */
      snprintf (staid, sizeof (staid), "%s_%s", net, sta);

      /* Create stream ID as combination of location and channel codes,
       * expanding SEED channel if needed */
      if (strlen (chan) == 3)
        snprintf (streamid, sizeof (streamid), "%s_%c_%c_%c", loc, chan[0], chan[1], chan[2]);
      else
        snprintf (streamid, sizeof (streamid), "%s_%s", loc, chan);
    }
    /* Otherwise use the stream ID as the station and stream IDs */
    else
    {
      strncpy (staid, ringstream->streamid, sizeof (staid) - 1);
      strncpy (streamid, ringstream->streamid, sizeof (streamid) - 1);
    }

    /* Create new station entry and add to stack */
    if (station_details == NULL || strcmp (station_details->id, staid) != 0)
    {
      station_details = (struct station_details *)malloc (sizeof (struct station_details));
      if (station_details == NULL)
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        free (ringstream);
        error = 1;
        break;
      }

      strncpy (station_details->id, staid, sizeof (station_details->id) - 1);
      station_details->earliestid = ringstream->earliestid;
      station_details->latestid   = ringstream->latestid;

      if (include_streams)
      {
        if ((station_details->streams = StackCreate ()) == NULL)
        {
          lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
          free (ringstream);
          error = 1;
          break;
        }
      }
      else
      {
        station_details->streams = NULL;
      }

      StackUnshift (station_stack, station_details);
    }
    /* Otherwise, update station entry */
    else
    {
      if (station_details->earliestid > ringstream->earliestid)
        station_details->earliestid = ringstream->earliestid;
      if (station_details->latestid < ringstream->latestid)
        station_details->latestid = ringstream->latestid;
    }

    /* Add stream details if requested */
    if (station_details->streams != NULL)
    {
      stream_details = (struct stream_details *)malloc (sizeof (struct stream_details));
      if (stream_details == NULL)
      {
        lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
        free (ringstream);
        error = 1;
        break;
      }

      strncpy (stream_details->id, streamid, sizeof (stream_details->id) - 1);

      if (type && strstr (type, "MSEED3"))
      {
        strcpy (stream_details->format, "3");
      }
      else if (type && strstr (type, "MSEED"))
      {
        strcpy (stream_details->format, "2");
      }
      else
      {
        strcpy (stream_details->format, "?");
      }

      strcpy (stream_details->subformat, "D");

      stream_details->earliesttime = ringstream->earliestdstime;
      stream_details->latesttime   = ringstream->latestdetime;

      StackUnshift (station_details->streams, stream_details);
    }

    free (ringstream);
  }

  StackDestroy (ringstreams, free);

  if (!error)
  {
    /* Create JSON */
    if ((stations = cJSON_AddArrayToObject (root, "station")) == NULL)
    {
      return NULL;
    }

    /* Traverse station stack creating "station" entries */
    while ((station_details = (struct station_details *)StackPop (station_stack)) && !error)
    {
      if ((station = cJSON_CreateObject ()) == NULL)
      {
        error = 1;
        break;
      }
      cJSON_AddItemToArray (stations, station);

      cJSON_AddStringToObject (station, "id", station_details->id);
      snprintf (string32, sizeof (string32), "Station ID %s", station_details->id);
      cJSON_AddStringToObject (station, "description", string32);
      cJSON_AddNumberToObject (station, "start_seq", (double)station_details->earliestid);
      cJSON_AddNumberToObject (station, "end_seq", (double)station_details->latestid);

      if (station_details->streams)
      {
        if ((streams = cJSON_AddArrayToObject (station, "stream")) == NULL)
        {
          error = 1;
          break;
        }

        /* Traverse stream stack creating "stream" entries */
        while ((stream_details = (struct stream_details *)StackPop (station_details->streams)))
        {
          if ((stream = cJSON_CreateObject ()) == NULL)
          {
            error = 1;
            break;
          }
          cJSON_AddItemToArray (streams, stream);

          cJSON_AddStringToObject (stream, "id", stream_details->id);
          cJSON_AddStringToObject (stream, "format", stream_details->format);
          cJSON_AddStringToObject (stream, "subformat", stream_details->subformat);

          ms_nstime2timestr (stream_details->earliesttime, string32, ISOMONTHDAY_Z, MICRO);
          cJSON_AddStringToObject (stream, "start_time", string32);
          ms_nstime2timestr (stream_details->latesttime, string32, ISOMONTHDAY_Z, MICRO);
          cJSON_AddStringToObject (stream, "end_time", string32);

          free (stream_details);
        }

        StackDestroy (station_details->streams, free);
      }

      free (station_details);
    }
  }

  /* Free all temporary allocations */
  while ((station_details = (struct station_details *)StackPop (station_stack)))
  {
    if (station_details->streams)
    {
      StackDestroy (station_details->streams, free);
    }

    free (station_details);
  }

  StackDestroy (station_stack, free);

  if (match_code)
    pcre2_code_free (match_code);
  if (match_data)
    pcre2_match_data_free (match_data);

  return stations;
}

/***************************************************************************
 * info_add_connections:
 *
 * Add server connections to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static cJSON *
info_add_connections (ClientInfo *cinfo, cJSON *root, const char *matchexpr)
{
  struct cthread *loopctp;
  ClientInfo *tcinfo;
  nstime_t nsnow;

  char *conntype;
  char conntime[50];
  char packettime[50];

  cJSON *connections;
  cJSON *clients;
  cJSON *connection;

  pcre2_code *match_code       = NULL;
  pcre2_match_data *match_data = NULL;

  /* Compile match expression if provided */
  if (matchexpr && UpdatePattern (&match_code, &match_data, matchexpr, "connection match expression"))
  {
    return NULL;
  }

  if ((connections = cJSON_AddObjectToObject (root, "connections")) == NULL)
  {
    return NULL;
  }

  if ((clients = cJSON_AddArrayToObject (connections, "clients")) == NULL)
  {
    return NULL;
  }

  nsnow = NSnow ();

  /* List connections, lock client list while looping */
  pthread_mutex_lock (&cthreads_lock);
  loopctp = cthreads;
  while (loopctp)
  {
    tcinfo = (ClientInfo *)loopctp->td->td_prvtptr;

    /* Skip if client does not match provided expression */
    if (match_code)
      if (pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->hostname, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0 &&
          pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->ipstr, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0 &&
          pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->clientid, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0)
      {
        loopctp = loopctp->next;
        continue;
      }

    /* Skip if client thread is not in ACTIVE state */
    if (loopctp->td->td_state != TDS_ACTIVE)
    {
      loopctp = loopctp->next;
      continue;
    }

    /* Determine connection type */
    if (tcinfo->type == CLIENT_DATALINK)
    {
      if (tcinfo->websocket && tcinfo->tls)
        conntype = "DataLink:WebSocket:TLS";
      else if (tcinfo->websocket)
        conntype = "DataLink:WebSocket";
      else if (tcinfo->tls)
        conntype = "DataLink:TLS";
      else
        conntype = "DataLink";
    }
    else if (tcinfo->type == CLIENT_SEEDLINK)
    {
      if (tcinfo->websocket && tcinfo->tls)
        conntype = "SeedLink:WebSocket:TLS";
      else if (tcinfo->websocket)
        conntype = "SeedLink:WebSocket";
      else if (tcinfo->tls)
        conntype = "SeedLink:TLS";
      else
        conntype = "SeedLink";
    }
    else if (tcinfo->type == CLIENT_HTTP)
    {
      if (tcinfo->tls)
        conntype = "HTTPS";
      else
        conntype = "HTTP";
    }
    else
    {
      conntype = "Unknown";
    }

    ms_nstime2timestr (tcinfo->conntime, conntime, ISOMONTHDAY_Z, NANO_MICRO_NONE);

    if (tcinfo->reader->pkttime != NSTUNSET)
      ms_nstime2timestr (tcinfo->reader->pkttime, packettime, ISOMONTHDAY_Z, NANO_MICRO_NONE);

    if ((connection = cJSON_CreateObject ()) == NULL)
    {
      break;
    }
    cJSON_AddItemToArray (clients, connection);

    cJSON_AddStringToObject (connection, "host", tcinfo->hostname);
    cJSON_AddStringToObject (connection, "ip_address", tcinfo->ipstr);
    cJSON_AddStringToObject (connection, "port", tcinfo->portstr);
    cJSON_AddStringToObject (connection, "type", conntype);
    cJSON_AddStringToObject (connection, "client_id", tcinfo->clientid);
    cJSON_AddStringToObject (connection, "connect_time", conntime);

    cJSON_AddNumberToObject (connection, "current_packet", tcinfo->reader->pktid);

    if (tcinfo->reader->pkttime != NSTUNSET)
      cJSON_AddStringToObject (connection, "packet_time", packettime);

    cJSON_AddNumberToObject (connection, "lag_percent", (tcinfo->reader->pktid <= 0) ? 0 : tcinfo->percentlag);
    cJSON_AddNumberToObject (connection, "lag_seconds", (double)MS_NSTIME2EPOCH ((nsnow - tcinfo->lastxchange)));

    cJSON_AddNumberToObject (connection, "transmit_packets", tcinfo->txpackets[0]);
    cJSON_AddNumberToObject (connection, "transmit_packet_rate", tcinfo->txpacketrate);
    cJSON_AddNumberToObject (connection, "transmit_bytes", tcinfo->txbytes[0]);
    cJSON_AddNumberToObject (connection, "transmit_byte_rate", tcinfo->txbyterate);

    cJSON_AddNumberToObject (connection, "receive_packets", tcinfo->rxpackets[0]);
    cJSON_AddNumberToObject (connection, "receive_packet_rate", tcinfo->rxpacketrate);
    cJSON_AddNumberToObject (connection, "receive_bytes", tcinfo->rxbytes[0]);
    cJSON_AddNumberToObject (connection, "receive_byte_rate", tcinfo->rxbyterate);
    cJSON_AddNumberToObject (connection, "stream_count", (double)tcinfo->streamscount);

    if (tcinfo->matchstr)
      cJSON_AddStringToObject (connection, "match", tcinfo->matchstr);
    if (tcinfo->rejectstr)
      cJSON_AddStringToObject (connection, "reject", tcinfo->rejectstr);

    loopctp = loopctp->next;
  }
  pthread_mutex_unlock (&cthreads_lock);

  if (match_code)
    pcre2_code_free (match_code);
  if (match_data)
    pcre2_match_data_free (match_data);

  return connections;
}

/***************************************************************************
 * info_json:
 *
 * Return a JSON document with server details, conforming to the SeedLink
 * v4 JSON info schema.
 *
 * Which elements are includedis controlled by the elements bitmask.
 *
 * The returned string is minified JSON document and allocated on the heap
 * that must be freed by the caller.
 *
 * Returns pointer to JSON strng on success and NULL on error.
 ***************************************************************************/
char *
info_json (ClientInfo *cinfo, const char *software, InfoElements elements,
           const char *matchexpr)
{
  if (!cinfo)
    return NULL;

  cJSON *root = info_create_root (software);
  if (root == NULL)
  {
    return NULL;
  }

  if (elements & INFO_ID &&
      info_add_id (root) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_CAPABILITIES &&
      info_add_capabilities (root) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_FORMATS &&
      info_add_formats (root) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_FILTERS &&
      info_add_filters (root) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_STATIONS &&
      info_add_stations (cinfo, root, 0, matchexpr) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_STREAMS &&
      info_add_stations (cinfo, root, 1, matchexpr) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  if (elements & INFO_CONNECTIONS &&
      info_add_connections (cinfo, root, matchexpr) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  /* Generate JSON string, cleanup and return */
  char *json_string = cJSON_PrintUnformatted (root);

  cJSON_Delete (root);

  return json_string;
}

/***************************************************************************
 * error_json:
 *
 * Return a JSON document with an error code and message, conforming to
 * the SeedLink v4 JSON error schema.
 *
 * The returned string is minified JSON document and allocated on the heap
 * that must be freed by the caller.
 *
 * Returns pointer to JSON strng on success and NULL on error.
 ***************************************************************************/
char *
error_json (ClientInfo *cinfo, const char *software,
            const char *code, const char *message)
{
  cJSON *error;

  if (!cinfo)
    return NULL;

  cJSON *root = info_create_root (software);
  if (root == NULL)
  {
    return NULL;
  }

  if ((error = cJSON_AddObjectToObject (root, "error")) == NULL)
  {
    cJSON_Delete (root);
    return NULL;
  }

  cJSON_AddStringToObject (error, "code", code);
  cJSON_AddStringToObject (error, "message", message);

  /* Generate JSON string, cleanup and return */
  char *json_string = cJSON_PrintUnformatted (root);

  cJSON_Delete (root);

  return json_string;
}
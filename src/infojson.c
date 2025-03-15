/**************************************************************************
 * infojson.c
 *
 * SeedLink client thread specific routines.
 *
 * Create JSON formatted information about the server state, including
 * stream lists, connections, etc.
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

#include "yyjson.h"
#include "generic.h"
#include "infojson.h"
#include "slclient.h"
#include "ring.h"
#include "mseedscan.h"

/***************************************************************************
 * info_create_root:
 *
 * Initialize and populate root JSON object and items that are in all documents.
 *
 * Returns pointer to JSON document on success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_create_root (const char *software)
{
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;

  doc  = yyjson_mut_doc_new (NULL);
  root = yyjson_mut_obj (doc);
  yyjson_mut_doc_set_root (doc, root);

  if (root == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (yyjson_mut_obj_add_strcpy (doc, root, "software", software) == false)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (yyjson_mut_obj_add_strcpy (doc, root, "organization", config.serverid) == false)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  return doc;
}

/***************************************************************************
 * info_add_id:
 *
 * Add more details of server ID to the JSON document.
 *
 * Returns pointer to JSON document success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_id (yyjson_mut_doc *doc)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  char string[64];

  /* Add server start time */

  /* Create server start time string as YYYY-MM-DDTHH:MM:SSZ */
  ms_nstime2timestr (param.serverstarttime, string, ISOMONTHDAY_Z, NONE);

  if (yyjson_mut_obj_add_strcpy (doc, root, "server_start", string) == false)
  {
    return NULL;
  }

  return doc;
}

/***************************************************************************
 * info_json_capabilities:
 *
 * Add format specifications to the JSON document.
 *
 * Returns pointer to JSON document success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_capabilities (yyjson_mut_doc *doc)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *capability;
  char *string;
  char *cap;
  char *rest;

  if ((string = strdup (SLCAPABILITIESv4)) == NULL)
  {
    return NULL;
  }

  if ((capability = yyjson_mut_obj_add_arr (doc, root, "capability")) == NULL)
  {
    free (string);
    return NULL;
  }

  /* Parse capabilities string on spaces and add each capability */
  rest = string;
  while ((cap = strtok_r (rest, " ", &rest)))
  {
    if (yyjson_mut_arr_add_strcpy (doc, capability, cap) == false)
    {
      free (string);
      return NULL;
    }
  }

  free (string);

  return doc;
}

/***************************************************************************
 * info_add_formats:
 *
 * Add format specifications to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_formats (yyjson_mut_doc *doc)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *format;
  yyjson_mut_val *item;
  yyjson_mut_val *subformat;

  if ((format = yyjson_mut_obj_add_obj (doc, root, "format")) == NULL)
  {
    return NULL;
  }

  /* Add format code for miniSEED 2 */
  if ((item = yyjson_mut_obj_add_obj (doc, format, "2")) == NULL ||
      yyjson_mut_obj_add_strcpy (doc, item, "mimetype", "application/vnd.fdsn.mseed") == false ||
      ((subformat = yyjson_mut_obj_add_obj (doc, item, "subformat")) == NULL) ||
      yyjson_mut_obj_add_strcpy (doc, subformat, "D", "Data") == false)
  {
    return NULL;
  }

  /* Add format code for miniSEED 3 */
  if ((item = yyjson_mut_obj_add_obj (doc, format, "3")) == NULL ||
      yyjson_mut_obj_add_strcpy (doc, item, "mimetype", "application/vnd.fdsn.mseed3") == false ||
      ((subformat = yyjson_mut_obj_add_obj (doc, item, "subformat")) == NULL) ||
      yyjson_mut_obj_add_strcpy (doc, subformat, "D", "Data") == false)
  {
    return NULL;
  }

  return doc;
}

/***************************************************************************
 * info_add_filters:
 *
 * Add filter specifications to the JSON document.
 *
 * Returns pointer to JSON document success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_filters (yyjson_mut_doc *doc)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *filter;

  if ((filter = yyjson_mut_obj_add_obj (doc, root, "filter")) == NULL)
  {
    return NULL;
  }

  /* Add filter code for native */
  if (yyjson_mut_obj_add_strcpy (doc, filter, "native", "native format of data, no conversion") == false)
  {
    return NULL;
  }

  return doc;
}

/***************************************************************************
 * info_add_streams:
 *
 * Add stream array to the JSON document.
 *
 * Returns pointer to JSON document success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_streams (ClientInfo *cinfo, yyjson_mut_doc *doc, const char *matchexpr)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *stream_array;
  yyjson_mut_val *stream;

  Stack *ringstreams;
  RingStream *ringstream;

  char string32[32] = {0};
  uint32_t streamcount = 0;

  pcre2_code *match_code       = NULL;
  pcre2_match_data *match_data = NULL;

  /* Compile match expression if provided */
  if (matchexpr && UpdatePattern (&match_code, &match_data, matchexpr, "stream match expression"))
  {
    return NULL;
  }

  /* Get copy of streams as a Stack, sorted by stream ID */
  if ((ringstreams = GetStreamsStack (cinfo->reader)) == NULL)
  {
    lprintf (0, "[%s] Error getting streams stack", cinfo->hostname);
    return NULL;
  }

  /* Create JSON */
  if ((stream_array = yyjson_mut_obj_add_arr (doc, root, "stream")) == NULL)
  {
    return NULL;
  }

  /* Add streams to array */
  while ((ringstream = (RingStream *)StackPop (ringstreams)))
  {
    /* Skip if stream ID does not match provided expression */
    if (match_code &&
        pcre2_match (match_code, (PCRE2_SPTR8)ringstream->streamid, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0)
    {
      free (ringstream);
      continue;
    }

    stream = yyjson_mut_arr_add_obj (doc, stream_array);

    yyjson_mut_obj_add_strcpy (doc, stream, "id", ringstream->streamid);

    ms_nstime2timestr (ringstream->earliestdstime, string32, ISOMONTHDAY_Z, MICRO);
    yyjson_mut_obj_add_strcpy (doc, stream, "start_time", string32);

    ms_nstime2timestr (ringstream->latestdetime, string32, ISOMONTHDAY_Z, MICRO);
    yyjson_mut_obj_add_strcpy (doc, stream, "end_time", string32);

    yyjson_mut_obj_add_uint (doc, stream, "earliest_packet_id", ringstream->earliestid);

    ms_nstime2timestr (ringstream->earliestptime, string32, ISOMONTHDAY_Z, MICRO);
    yyjson_mut_obj_add_strcpy (doc, stream, "earliest_packet_time", string32);

    ms_nstime2timestr (ringstream->latestptime, string32, ISOMONTHDAY_Z, MICRO);
    yyjson_mut_obj_add_strcpy (doc, stream, "latest_packet_time", string32);

    yyjson_mut_obj_add_uint (doc, stream, "latest_packet_id", ringstream->latestid);

    /* Data latency: the difference between the current time and time of last sample in seconds */
    yyjson_mut_obj_add_real (doc, stream, "data_latency",
                             (double)MS_NSTIME2EPOCH ((NSnow () - ringstream->latestdetime)));

    streamcount++;
    free (ringstream);
  }

  yyjson_mut_obj_add_uint (doc, root, "stream_count", streamcount);

  StackDestroy (ringstreams, free);

  if (match_code)
    pcre2_code_free (match_code);
  if (match_data)
    pcre2_match_data_free (match_data);

  return doc;
}

/***************************************************************************
 * info_add_stations:
 *
 * Add station array, and optionally a stream sub-array, to the JSON document.
 *
 * Returns pointer to JSON document success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_stations (ClientInfo *cinfo, yyjson_mut_doc *doc, int include_streams,
                   const char *matchexpr)
{
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *station_array;
  yyjson_mut_val *station;
  yyjson_mut_val *stream_array;
  yyjson_mut_val *stream;

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

  char net[MAXSTREAMID]  = {0};
  char sta[MAXSTREAMID]  = {0};
  char loc[MAXSTREAMID]  = {0};
  char chan[MAXSTREAMID] = {0};

  char staid[MAXSTREAMID]    = {0};
  char streamid[MAXSTREAMID] = {0};

  int error         = 0;
  char *type        = NULL;
  char string96[96] = {0};

  pcre2_code *match_code       = NULL;
  pcre2_match_data *match_data = NULL;

  /* Compile match expression if provided */
  if (matchexpr && UpdatePattern (&match_code, &match_data, matchexpr, "stream match expression"))
  {
    return NULL;
  }

  /* Get copy of streams as a Stack, sorted by stream ID */
  if ((ringstreams = GetStreamsStack (cinfo->reader)) == NULL)
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
      snprintf (staid, sizeof (staid), "%.10s_%.10s", net, sta);

      /* Create stream ID as combination of location and channel codes,
       * expanding SEED channel if needed */
      if (strlen (chan) == 3)
        snprintf (streamid, sizeof (streamid), "%.10s_%c_%c_%c", loc, chan[0], chan[1], chan[2]);
      else
        snprintf (streamid, sizeof (streamid), "%.10s_%.30s", loc, chan);
    }
    /* Otherwise use the stream ID as the station and stream IDs */
    else
    {
      memcpy (staid, ringstream->streamid, sizeof (staid));
      memcpy (streamid, ringstream->streamid, sizeof (streamid));
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

      memcpy (station_details->id, staid, sizeof (station_details->id));
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

      memcpy (stream_details->id, streamid, sizeof (stream_details->id));

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
    station_array = yyjson_mut_obj_add_arr (doc, root, "station");

    /* Traverse station stack creating "station" entries */
    while ((station_details = (struct station_details *)StackPop (station_stack)) && !error)
    {
      station = yyjson_mut_arr_add_obj (doc, station_array);

      yyjson_mut_obj_add_strcpy (doc, station, "id", station_details->id);
      snprintf (string96, sizeof (string96), "Station ID %s", station_details->id);
      yyjson_mut_obj_add_strcpy (doc, station, "description", string96);
      yyjson_mut_obj_add_uint (doc, station, "start_seq", station_details->earliestid);
      yyjson_mut_obj_add_uint (doc, station, "end_seq", station_details->latestid);

      if (station_details->streams)
      {
        stream_array = yyjson_mut_obj_add_arr (doc, station, "stream");

        /* Traverse stream stack creating "stream" entries */
        while ((stream_details = (struct stream_details *)StackPop (station_details->streams)))
        {
          stream = yyjson_mut_arr_add_obj (doc, stream_array);

          yyjson_mut_obj_add_strcpy (doc, stream, "id", stream_details->id);
          yyjson_mut_obj_add_strcpy (doc, stream, "format", stream_details->format);
          yyjson_mut_obj_add_strcpy (doc, stream, "subformat", stream_details->subformat);

          ms_nstime2timestr (stream_details->earliesttime, string96, ISOMONTHDAY_Z, MICRO);
          yyjson_mut_obj_add_strcpy (doc, stream, "start_time", string96);
          ms_nstime2timestr (stream_details->latesttime, string96, ISOMONTHDAY_Z, MICRO);
          yyjson_mut_obj_add_strcpy (doc, stream, "end_time", string96);

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

  return doc;
}

/***************************************************************************
 * info_add_connections:
 *
 * Add server connections to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_connections (ClientInfo *cinfo, yyjson_mut_doc *doc, const char *matchexpr)
{
  (void)cinfo; /* Unused for now, suppress compiler warning */
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *connections;
  yyjson_mut_val *client_array;
  yyjson_mut_val *client;

  struct cthread *loopctp;
  ClientInfo *tcinfo;
  nstime_t nsnow;

  char *conntype;
  char timestring[50];
  char packettime[50];

  pcre2_code *match_code       = NULL;
  pcre2_match_data *match_data = NULL;

  /* Compile match expression if provided */
  if (matchexpr && UpdatePattern (&match_code, &match_data, matchexpr, "connection match expression"))
  {
    return NULL;
  }

  if ((connections = yyjson_mut_obj_add_obj (doc, root, "connections")) == NULL ||
      (client_array = yyjson_mut_obj_add_arr (doc, connections, "client")) == NULL)
  {
    return NULL;
  }

  yyjson_mut_obj_add_int (doc, connections, "client_count", param.clientcount);

  nsnow = NSnow ();

  /* List connections, lock client list while looping */
  pthread_mutex_lock (&param.cthreads_lock);
  for (loopctp = param.cthreads; loopctp != NULL; loopctp = loopctp->next)
  {
    tcinfo = (ClientInfo *)loopctp->td->td_prvtptr;

    /* Skip if client does not match provided expression */
    if (match_code)
      if (pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->hostname, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0 &&
          pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->ipstr, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0 &&
          pcre2_match (match_code, (PCRE2_SPTR8)tcinfo->clientid, PCRE2_ZERO_TERMINATED, 0, 0, match_data, NULL) < 0)
      {
        continue;
      }

    /* Skip if client thread is not in ACTIVE state */
    if (loopctp->td->td_state != TDS_ACTIVE)
    {
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

    client = yyjson_mut_arr_add_obj (doc, client_array);

    yyjson_mut_obj_add_strcpy (doc, client, "host", tcinfo->hostname);
    yyjson_mut_obj_add_strcpy (doc, client, "ip_address", tcinfo->ipstr);
    yyjson_mut_obj_add_strcpy (doc, client, "client_port", tcinfo->portstr);
    yyjson_mut_obj_add_strcpy (doc, client, "server_port", tcinfo->serverport);
    yyjson_mut_obj_add_strcpy (doc, client, "type", conntype);
    yyjson_mut_obj_add_strcpy (doc, client, "client_id", tcinfo->clientid);

    ms_nstime2timestr (tcinfo->conntime, timestring, ISOMONTHDAY_Z, NONE);
    yyjson_mut_obj_add_strcpy (doc, client, "connect_time", timestring);

    if (tcinfo->reader->pktid <= RINGID_MAXIMUM)
    {
      yyjson_mut_obj_add_uint (doc, client, "packet_id", tcinfo->reader->pktid);
    }

    if (tcinfo->reader->pkttime != NSTUNSET)
    {
      ms_nstime2timestr (tcinfo->reader->pkttime, packettime, ISOMONTHDAY_Z, NANO_MICRO_NONE);
      yyjson_mut_obj_add_strcpy (doc, client, "packet_creation_time", packettime);
    }

    yyjson_mut_obj_add_int (doc, client, "lag_percent", (tcinfo->reader->pktid > RINGID_MAXIMUM) ? 0 : tcinfo->percentlag);
    yyjson_mut_obj_add_real (doc, client, "lag_seconds", (double)MS_NSTIME2EPOCH ((nsnow - tcinfo->lastxchange)));

    yyjson_mut_obj_add_uint (doc, client, "transmit_packets", tcinfo->txpackets0);
    yyjson_mut_obj_add_real (doc, client, "transmit_packet_rate", tcinfo->txpacketrate);
    yyjson_mut_obj_add_uint (doc, client, "transmit_bytes", tcinfo->txbytes0);
    yyjson_mut_obj_add_real (doc, client, "transmit_byte_rate", tcinfo->txbyterate);

    yyjson_mut_obj_add_uint (doc, client, "receive_packets", tcinfo->rxpackets0);
    yyjson_mut_obj_add_real (doc, client, "receive_packet_rate", tcinfo->rxpacketrate);
    yyjson_mut_obj_add_uint (doc, client, "receive_bytes", tcinfo->rxbytes0);
    yyjson_mut_obj_add_real (doc, client, "receive_byte_rate", tcinfo->rxbyterate);

    yyjson_mut_obj_add_int (doc, client, "stream_count", tcinfo->streamscount);

    if (tcinfo->matchstr)
      yyjson_mut_obj_add_strcpy (doc, client, "match", tcinfo->matchstr);
    if (tcinfo->rejectstr)
      yyjson_mut_obj_add_strcpy (doc, client, "reject", tcinfo->rejectstr);

    /* SeedLink-specific stations and selectors */
    if (tcinfo->type == CLIENT_SEEDLINK && tcinfo->extinfo)
    {
      SLInfo *slinfo = (SLInfo *)tcinfo->extinfo;

      char string32[32] = {0};

      ReqStationID *stationid;
      Stack *stack = StackCreate ();
      RBNode *rbnode;

      snprintf (string32, sizeof (string32), "%u.%u", slinfo->proto_major, slinfo->proto_minor);
      yyjson_mut_obj_add_strcpy (doc, client, "protocol_version", string32);


      yyjson_mut_obj_add_bool (doc, client, "dialup_mode", slinfo->dialup);

      if (slinfo->proto_major == 3)
      {
        yyjson_mut_obj_add_bool (doc, client, "batch_mode", slinfo->batch);
        yyjson_mut_obj_add_bool (doc, client, "extended_reply", slinfo->extreply);
      }

      if (slinfo->selectors)
      {
        yyjson_mut_val *selector_array = yyjson_mut_obj_add_arr (doc, client, "selector");

        for (struct strnode *selector = slinfo->selectors; selector; selector = selector->next)
        {
          yyjson_mut_arr_add_str (doc, selector_array, selector->string);
        }
      }

      yyjson_mut_val *station_array = yyjson_mut_obj_add_arr (doc, client, "station");

      RBBuildStack (slinfo->stations, stack);

      while ((rbnode = (RBNode *)StackPop (stack)))
      {
        stationid = (ReqStationID *)rbnode->data;

        yyjson_mut_val *station = yyjson_mut_arr_add_obj (doc, station_array);

        yyjson_mut_obj_add_strcpy (doc, station, "id", (const char *)rbnode->key);

        if (stationid->starttime != NSTUNSET)
        {
          ms_nstime2timestr (stationid->starttime, timestring, ISOMONTHDAY_Z, NONE);
          yyjson_mut_obj_add_strcpy (doc, station, "start_time", timestring);
        }

        if (stationid->endtime != NSTUNSET)
        {
          ms_nstime2timestr (stationid->endtime, timestring, ISOMONTHDAY_Z, NONE);
          yyjson_mut_obj_add_strcpy (doc, station, "end_time", timestring);
        }

        if (stationid->packetid <= RINGID_MAXIMUM)
        {
          yyjson_mut_obj_add_uint (doc, station, "start_packet_id", stationid->packetid);
        }

        if (stationid->datastart != NSTUNSET)
        {
          ms_nstime2timestr (stationid->datastart, timestring, ISOMONTHDAY_Z, NONE);
          yyjson_mut_obj_add_uint (doc, station, "start_packet_time", stationid->datastart);
        }

        if (stationid->selectors)
        {
          yyjson_mut_val *selector_array = yyjson_mut_obj_add_arr (doc, station, "selector");

          for (struct strnode *selector = stationid->selectors; selector; selector = selector->next)
          {
            yyjson_mut_arr_add_str (doc, selector_array, selector->string);
          }
        }
      }

      StackDestroy (stack, 0);
    }
  }
  pthread_mutex_unlock (&param.cthreads_lock);

  if (match_code)
    pcre2_code_free (match_code);
  if (match_data)
    pcre2_match_data_free (match_data);

  return doc;
}

/***************************************************************************
 * info_add_status:
 *
 * Add server status to the JSON document.
 *
 * Returns pointer to JSON object added on success and NULL on error.
 ***************************************************************************/
static yyjson_mut_doc *
info_add_status (yyjson_mut_doc *doc)
{
  RingPacket packet;
  uint64_t pktid;
  yyjson_mut_val *root = yyjson_mut_doc_get_root (doc);
  yyjson_mut_val *server;
  yyjson_mut_val *thread_array;
  yyjson_mut_val *thread;

  struct sthread *loopstp;

  char timestr[50];

  if ((server = yyjson_mut_obj_add_obj (doc, root, "server")) == NULL ||
      (thread_array = yyjson_mut_obj_add_arr (doc, server, "thread")) == NULL)
  {
    return NULL;
  }

  yyjson_mut_obj_add_uint (doc, server, "ring_version", param.version);
  yyjson_mut_obj_add_uint (doc, server, "ring_size", param.ringsize);
  yyjson_mut_obj_add_uint (doc, server, "packet_size", param.pktsize);
  yyjson_mut_obj_add_uint (doc, server, "maximum_packets", param.maxpackets);
  yyjson_mut_obj_add_bool (doc, server, "memory_mapped", config.memorymapring);
  yyjson_mut_obj_add_bool (doc, server, "volatile_ring", config.volatilering);

  yyjson_mut_obj_add_int (doc, server, "connection_count", param.clientcount);
  yyjson_mut_obj_add_uint (doc, server, "stream_count", param.streamcount);

  yyjson_mut_obj_add_real (doc, server, "transmit_packet_rate", param.txpacketrate);
  yyjson_mut_obj_add_real (doc, server, "transmit_byte_rate", param.txbyterate);
  yyjson_mut_obj_add_real (doc, server, "receive_packet_rate", param.rxpacketrate);
  yyjson_mut_obj_add_real (doc, server, "receive_byte_rate", param.rxbyterate);

  int64_t earliestoffset  = atomic_load_explicit (&param.earliestoffset, memory_order_relaxed);

  pktid = RingReadPacket (earliestoffset, &packet, NULL);
  if (pktid != RINGID_NONE && pktid != RINGID_ERROR)
  {
    yyjson_mut_obj_add_uint (doc, server, "earliest_packet_id", pktid);
    ms_nstime2timestr (packet.pkttime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "earliest_packet_time", timestr);
    ms_nstime2timestr (packet.datastart, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "earliest_data_start", timestr);
    ms_nstime2timestr (packet.dataend, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "earliest_data_end", timestr);
  }

  int64_t latestoffset = atomic_load_explicit (&param.latestoffset, memory_order_acquire);

  pktid = RingReadPacket (latestoffset, &packet, NULL);
  if (pktid != RINGID_NONE && pktid != RINGID_ERROR)
  {
    yyjson_mut_obj_add_uint (doc, server, "latest_packet_id", pktid);
    ms_nstime2timestr (packet.pkttime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "latest_packet_time", timestr);
    ms_nstime2timestr (packet.datastart, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "latest_data_start", timestr);
    ms_nstime2timestr (packet.dataend, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
    yyjson_mut_obj_add_strcpy (doc, server, "latest_data_end", timestr);
  }

  /* List server threads, lock thread list while looping */
  pthread_mutex_lock (&param.sthreads_lock);
  for (loopstp = param.sthreads; loopstp != NULL; loopstp = loopstp->next)
  {
    thread = yyjson_mut_arr_add_obj (doc, thread_array);

    /* Add thread state to Thread element */
    if (loopstp->td->td_state == TDS_SPAWNING)
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "SPAWNING");
    else if (loopstp->td->td_state == TDS_ACTIVE)
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "ACTIVE");
    else if (loopstp->td->td_state == TDS_CLOSE)
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "CLOSE");
    else if (loopstp->td->td_state == TDS_CLOSING)
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "CLOSING");
    else if (loopstp->td->td_state == TDS_CLOSED)
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "CLOSED");
    else
      yyjson_mut_obj_add_strcpy (doc, thread, "state", "UNKNOWN");

    /* Determine server thread type and add specifics */
    if (loopstp->type == LISTEN_THREAD)
    {
      char protocolstr[100];
      ListenPortParams *lpp = loopstp->params;

      yyjson_mut_obj_add_strcpy (doc, thread, "type", "Listener");

      if (GenProtocolString (lpp->protocols, lpp->options, protocolstr, sizeof (protocolstr)) > 0)
      {
        yyjson_mut_obj_add_strcpy (doc, thread, "protocol", protocolstr);
      }

      yyjson_mut_obj_add_strcpy (doc, thread, "port", lpp->portstr);
    }
    else if (loopstp->type == MSEEDSCAN_THREAD)
    {
      MSScanInfo *mssinfo = loopstp->params;

      yyjson_mut_obj_add_strcpy (doc, thread, "type", "miniSEED scanner");
      yyjson_mut_obj_add_strcpy (doc, thread, "directory", mssinfo->dirname);
      yyjson_mut_obj_add_int (doc, thread, "max_recursion", mssinfo->maxrecur);
      yyjson_mut_obj_add_strcpy (doc, thread, "state_file", mssinfo->statefile);
      yyjson_mut_obj_add_strcpy (doc, thread, "match", mssinfo->matchstr);
      yyjson_mut_obj_add_strcpy (doc, thread, "reject", mssinfo->rejectstr);
      yyjson_mut_obj_add_real (doc, thread, "scan_time", mssinfo->scantime);
      yyjson_mut_obj_add_real (doc, thread, "packet_rate", mssinfo->rxpacketrate);
      yyjson_mut_obj_add_real (doc, thread, "byte_rate", mssinfo->rxbyterate);
    }
    else
    {
      yyjson_mut_obj_add_strcpy (doc, thread, "type", "Unknown");
    }
  }
  pthread_mutex_unlock (&param.sthreads_lock);

  return doc;
}

/***************************************************************************
 * info_json:
 *
 * Return a JSON document with server details, conforming to the SeedLink
 * v4 JSON info schema.
 *
 * Which elements are included is controlled by the elements bitmask.
 *
 * The returned string is minified JSON document and allocated on the heap
 * that must be free'd by the caller.
 *
 * Returns pointer to JSON string on success and NULL on error.
 ***************************************************************************/
char *
info_json (ClientInfo *cinfo, const char *software, InfoElements elements,
           const char *matchexpr)
{
  yyjson_mut_doc *doc;

  if (!cinfo)
    return NULL;

  if ((doc = info_create_root (software)) == NULL)
  {
    return NULL;
  }

  if (elements & INFO_ID &&
      info_add_id (doc) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_CAPABILITIES &&
      info_add_capabilities (doc) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_FORMATS &&
      info_add_formats (doc) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_FILTERS &&
      info_add_filters (doc) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_STATIONS &&
      info_add_stations (cinfo, doc, 0, matchexpr) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_STATION_STREAMS &&
      info_add_stations (cinfo, doc, 1, matchexpr) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_STREAMS &&
      info_add_streams (cinfo, doc, matchexpr) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_CONNECTIONS &&
      info_add_connections (cinfo, doc, matchexpr) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  if (elements & INFO_STATUS &&
      info_add_status (doc) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  /* Generate JSON string, cleanup and return */
  char *json_string = yyjson_mut_write (doc, 0, NULL);

  yyjson_mut_doc_free (doc);

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
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  yyjson_mut_val *error;

  if (!cinfo)
    return NULL;

  if ((doc = info_create_root (software)) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }
  root = yyjson_mut_doc_get_root (doc);

  if ((error = yyjson_mut_obj_add_obj (doc, root, "error")) == NULL)
  {
    yyjson_mut_doc_free (doc);
    return NULL;
  }

  yyjson_mut_obj_add_strcpy (doc, error, "code", code);
  yyjson_mut_obj_add_strcpy (doc, error, "message", message);

  char *json_string = yyjson_mut_write (doc, 0, NULL);

  yyjson_mut_doc_free (doc);

  return json_string;
}
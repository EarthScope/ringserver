/**************************************************************************
 * infoxml.c
 *
 * Convert SeedLink v4 INFO contents to v3 XML format.
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
#include <mxml.h>

#include "yyjson.h"
#include "generic.h"
#include "infojson.h"
#include "slclient.h"
#include "dlclient.h"

#define DASHNULL(x) ((x) ? (x) : "-")

/***************************************************************************
 * Return an SeedLink v3 INFO ID document in XML format.
 *
 * The returned document is allocated on the heap and should be freed by
 * the caller.
 *
 * Returns pointer to the document on success and NULL on error.
 ***************************************************************************/
char *
info_xml_slv3_id (ClientInfo *cinfo, const char *software)
{
  mxml_node_t *xmldoc   = NULL;
  mxml_node_t *seedlink = NULL;

  yyjson_doc *json;
  yyjson_val *root;
  char *json_string;
  char *xml_string = NULL;

  /* Generate JSON formatted document for INFO ID level */
  if ((json_string = info_json (cinfo, software, INFO_ID, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);
  root = yyjson_doc_get_root (json);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    yyjson_doc_free (json);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    yyjson_doc_free (json);
    mxmlRelease (xmldoc);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      yyjson_get_str (yyjson_obj_get (root, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      yyjson_get_str (yyjson_obj_get (root, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      yyjson_get_str (yyjson_obj_get (root, "server_start")));

  /* Generate XML string */
  mxml_options_t *options = mxmlOptionsNew ();
  mxmlOptionsSetWrapMargin (options, 0);
  xml_string = mxmlSaveAllocString (xmldoc, options);
  mxmlOptionsDelete (options);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Return an SeedLink v3 INFO CAPABILITIES document in XML format.
 *
 * The returned document is allocated on the heap and should be freed by
 * the caller.
 *
 * Returns pointer to the document on success and NULL on error.
 ***************************************************************************/
char *
info_xml_slv3_capabilities (ClientInfo *cinfo, const char *software)
{
  mxml_node_t *xmldoc     = NULL;
  mxml_node_t *seedlink   = NULL;
  mxml_node_t *capability = NULL;

  yyjson_doc *json;
  yyjson_val *root;
  char *json_string;
  char *xml_string = NULL;

  /* Fixed v3 capabilities */
  const char *caps[8] = {"dialup", "multistation", "window-extraction", "info:id",
                         "info:capabilities", "info:stations", "info:streams",
                         "info:connections"};

  /* Generate JSON formatted document for INFO ID level */
  if ((json_string = info_json (cinfo, software, INFO_ID, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);
  root = yyjson_doc_get_root (json);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    yyjson_doc_free (json);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    yyjson_doc_free (json);
    mxmlRelease (xmldoc);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      yyjson_get_str (yyjson_obj_get (root, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      yyjson_get_str (yyjson_obj_get (root, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      yyjson_get_str (yyjson_obj_get (root, "server_start")));

  /* Add capabilities */
  for (int idx = 0; idx < 8; idx++)
  {
    if (!(capability = mxmlNewElement (seedlink, "capability")))
    {
      break;
    }

    mxmlElementSetAttr (capability, "name", caps[idx]);
  }

  /* Generate XML string */
  mxml_options_t *options = mxmlOptionsNew ();
  mxmlOptionsSetWrapMargin (options, 0);
  xml_string = mxmlSaveAllocString (xmldoc, options);
  mxmlOptionsDelete (options);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Return an SeedLink v3 INFO STATIONS or INFO STREAMS document in XML format.
 *
 * The returned document is allocated on the heap and should be freed by
 * the caller.
 *
 * Returns pointer to the document on success and NULL on error.
 ***************************************************************************/
char *
info_xml_slv3_stations (ClientInfo *cinfo, const char *software, int include_streams)
{
  mxml_node_t *xmldoc   = NULL;
  mxml_node_t *seedlink = NULL;
  mxml_node_t *station  = NULL;
  mxml_node_t *stream   = NULL;

  yyjson_doc *json;
  yyjson_val *root;
  yyjson_val *station_array;
  yyjson_val *stream_array;
  yyjson_val *station_iter = NULL;
  yyjson_val *stream_iter  = NULL;
  size_t idx, max;
  size_t idxsub, maxsub;

  char *json_string;
  char *xml_string = NULL;

  char idstr[32] = {0};
  char *ptr;

  InfoElements elements = (include_streams) ? INFO_STATION_STREAMS : INFO_STATIONS;
  elements |= INFO_ID;

  /* Generate JSON formatted document for INFO STREAMS or STATIONS level */
  if ((json_string = info_json (cinfo, software, elements, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);
  root = yyjson_doc_get_root (json);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    yyjson_doc_free (json);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    yyjson_doc_free (json);
    mxmlRelease (xmldoc);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      yyjson_get_str (yyjson_obj_get (root, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      yyjson_get_str (yyjson_obj_get (root, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      yyjson_get_str (yyjson_obj_get (root, "server_start")));

  /* Add stations */
  if ((station_array = yyjson_obj_get (root, "station")) != NULL)
  {
    yyjson_arr_foreach (station_array, idx, max, station_iter)
    {
      if (!(station = mxmlNewElement (seedlink, "station")))
      {
        break;
      }

      strncpy (idstr, DASHNULL (yyjson_get_str (yyjson_obj_get (station_iter, "id"))),
               sizeof (idstr) - 1);

      /* Split network code from station if separated by '_' */
      if ((ptr = strchr (idstr, '_')))
        *ptr++ = '\0';
      /* Otherwise use the full ID for both */
      else
        ptr = idstr;

      mxmlElementSetAttr (station, "name", ptr);
      mxmlElementSetAttr (station, "network", idstr);
      mxmlElementSetAttr (station, "description",
                          yyjson_get_str (yyjson_obj_get (station_iter, "description")));
      mxmlElementSetAttrf (station, "begin_seq", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (station_iter, "start_seq")));
      mxmlElementSetAttrf (station, "end_seq", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (station_iter, "end_seq")));
      mxmlElementSetAttr (station, "stream_check", "enabled");

      if ((stream_array = yyjson_obj_get (station_iter, "stream")) != NULL)
      {
        yyjson_arr_foreach (stream_array, idxsub, maxsub, stream_iter)
        {
          if (!(stream = mxmlNewElement (station, "stream")))
          {
            break;
          }

          strncpy (idstr, yyjson_get_str (yyjson_obj_get (stream_iter, "id")),
                   sizeof (idstr) - 1);

          /* Split location code from station code if separated by '_' */
          if ((ptr = strchr (idstr, '_')))
            *ptr++ = '\0';
          /* Otherwise use the full ID for both */
          else
            ptr = idstr;

          /* If "seedname" is an FDSN Source ID channel ('B_S_s'), collapse to a SEED channel */
          if (strlen (ptr) == 5 && ptr[1] == '_' && ptr[3] == '_')
          {
            ptr[1] = ptr[2];
            ptr[2] = ptr[4];
            ptr[3] = '\0';
          }

          mxmlElementSetAttr (stream, "location", idstr);
          mxmlElementSetAttr (stream, "seedname", ptr);
          mxmlElementSetAttr (stream, "type",
                              yyjson_get_str (yyjson_obj_get (stream_iter, "subformat")));
          mxmlElementSetAttr (stream, "begin_time",
                              yyjson_get_str (yyjson_obj_get (stream_iter, "start_time")));
          mxmlElementSetAttr (stream, "end_time",
                              yyjson_get_str (yyjson_obj_get (stream_iter, "end_time")));
        }
      }
    }
  }

  /* Generate XML string */
  mxml_options_t *options = mxmlOptionsNew ();
  mxmlOptionsSetWrapMargin (options, 0);
  xml_string = mxmlSaveAllocString (xmldoc, options);
  mxmlOptionsDelete (options);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Return an SeedLink v3 INFO CONNECTIONS document in XML format.
 *
 * The returned document is allocated on the heap and should be freed by
 * the caller.
 *
 * Returns pointer to the document on success and NULL on error.
 ***************************************************************************/
char *
info_xml_slv3_connections (ClientInfo *cinfo, const char *software)
{
  mxml_node_t *xmldoc     = NULL;
  mxml_node_t *seedlink   = NULL;
  mxml_node_t *station    = NULL;
  mxml_node_t *connection = NULL;

  yyjson_doc *json;
  yyjson_val *root;
  yyjson_val *client_array;
  yyjson_val *client_iter = NULL;
  size_t idx, max;

  char *json_string;
  char *xml_string = NULL;

  const char *network;

  /* Generate JSON formatted document for INFO CONNECTIONS level */
  if ((json_string = info_json (cinfo, software, INFO_CONNECTIONS | INFO_ID, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);
  root = yyjson_doc_get_root (json);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    yyjson_doc_free (json);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    yyjson_doc_free (json);
    mxmlRelease (xmldoc);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      yyjson_get_str (yyjson_obj_get (root, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      yyjson_get_str (yyjson_obj_get (root, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      yyjson_get_str (yyjson_obj_get (root, "server_start")));

  /* Add stations and connections */
  if ((client_array = yyjson_ptr_get (root, "/connections/client")) != NULL)
  {
    yyjson_arr_foreach (client_array, idx, max, client_iter)
    {
      if (!(station = mxmlNewElement (seedlink, "station")))
      {
        break;
      }

      mxmlElementSetAttr (station, "name", "CLIENT");

      network = yyjson_get_str (yyjson_obj_get (client_iter, "type"));
      if (strstr (network, "DataLink"))
        mxmlElementSetAttr (station, "network", "DL");
      else if (strstr (network, "SeedLink"))
        mxmlElementSetAttr (station, "network", "SL");
      else if (strstr (network, "HTTP"))
        mxmlElementSetAttr (station, "network", "HP");
      else
        mxmlElementSetAttr (station, "network", "??");

      mxmlElementSetAttr (station, "description", "Ringserver Client");
      mxmlElementSetAttr (station, "begin_seq", "0");
      mxmlElementSetAttr (station, "end_seq", "0");
      mxmlElementSetAttr (station, "stream_check", "enabled");

      if (!(connection = mxmlNewElement (station, "connection")))
      {
        break;
      }

      mxmlElementSetAttr (connection, "host",
                          yyjson_get_str (yyjson_obj_get (client_iter, "ip_address")));
      mxmlElementSetAttr (connection, "port",
                          yyjson_get_str (yyjson_obj_get (client_iter, "client_port")));
      mxmlElementSetAttr (connection, "ctime",
                          yyjson_get_str (yyjson_obj_get (client_iter, "connect_time")));
      mxmlElementSetAttr (connection, "begin_seq", "0");
      mxmlElementSetAttrf (connection, "current_seq", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "packet_id")));
      mxmlElementSetAttr (connection, "sequence_gaps", "0");
      mxmlElementSetAttrf (connection, "txcount", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "transmit_packets")));
      mxmlElementSetAttrf (connection, "totBytes", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "transmit_bytes")));
      mxmlElementSetAttr (connection, "begin_seq_valid", "yes");
      mxmlElementSetAttr (connection, "realtime", "yes");
      mxmlElementSetAttr (connection, "end_of_data", "no");
    }
  }

  /* Generate XML string */
  mxml_options_t *options = mxmlOptionsNew ();
  mxmlOptionsSetWrapMargin (options, 0);
  xml_string = mxmlSaveAllocString (xmldoc, options);
  mxmlOptionsDelete (options);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Return an DataLink v1 INFO CONNECTIONS document in XML format.
 *
 * The returned document is allocated on the heap and should be freed by
 * the caller.
 *
 * Returns pointer to the document on success and NULL on error.
 ***************************************************************************/
char *
info_xml_dlv1 (ClientInfo *cinfo, const char *software, const char *level,
               const char *matchexpr, uint8_t trusted)
{
  mxml_node_t *xmldoc = NULL;
  mxml_node_t *status = NULL;

  yyjson_doc *json;
  yyjson_val *root;
  yyjson_val *server;
  size_t idx, max;

  char *json_string;
  char *xml_string = NULL;

  InfoElements elements;

  /* Determine the INFO level */
  if (strcasecmp (level, "STATUS") == 0)
    elements = INFO_STATUS | INFO_ID;
  else if (strcasecmp (level, "STREAMS") == 0)
    elements = INFO_STREAMS | INFO_STATUS | INFO_ID;
  else if (strcasecmp (level, "CONNECTIONS") == 0)
    elements = INFO_CONNECTIONS | INFO_STATUS | INFO_ID;
  else
    return NULL;

  /* Generate JSON formatted document for INFO CONNECTIONS level */
  if ((json_string = info_json (cinfo, software, elements, matchexpr)) == NULL)
  {
    return NULL;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);
  root = yyjson_doc_get_root (json);

  /* Initialize the XML response */
  if (!(xmldoc = mxmlNewElement (NULL, "DataLink")))
  {
    yyjson_doc_free (json);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (xmldoc, "Version",
                      yyjson_get_str (yyjson_obj_get (root, "software")));
  mxmlElementSetAttr (xmldoc, "ServerID",
                      yyjson_get_str (yyjson_obj_get (root, "organization")));
  mxmlElementSetAttrf (xmldoc, "Capabilities", "%s PACKETSIZE:%lu%s", DLCAPABILITIES_ID,
                       (unsigned long int)(param.pktsize - sizeof (RingPacket)),
                       (cinfo->permissions & WRITE_PERMISSION) ? " WRITE" : "");

  server = yyjson_obj_get (root, "server");

  /* All INFO responses contain the "Status" element */
  if (!(status = mxmlNewElement (xmldoc, "Status")))
  {
    lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
    yyjson_doc_free (json);
    mxmlRelease (xmldoc);
    return NULL;
  }
  else if (server)
  {
    /* Convert server start time to YYYY-MM-DD HH:MM:SS */
    mxmlElementSetAttr (status, "StartTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (root, "server_start"))));
    mxmlElementSetAttrf (status, "RingVersion", "%d",
                         yyjson_get_int (yyjson_obj_get (root, "ring_version")));
    mxmlElementSetAttrf (status, "RingSize", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (root, "ring_size")));
    mxmlElementSetAttrf (status, "PacketSize", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (root, "packet_size")));
    mxmlElementSetAttrf (status, "MaximumPackets", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (root, "maximum_packets")));
    mxmlElementSetAttrf (status, "MemoryMappedRing", "%s",
                         (yyjson_get_bool (yyjson_obj_get (root, "memory_mapped"))) ? "TRUE" : "FALSE");
    mxmlElementSetAttrf (status, "VolatileRing", "%s",
                         (yyjson_get_bool (yyjson_obj_get (root, "volatile_ring"))) ? "TRUE" : "FALSE");
    mxmlElementSetAttrf (status, "TotalConnections", "%d",
                         yyjson_get_int (yyjson_obj_get (server, "connection_count")));
    mxmlElementSetAttrf (status, "TotalStreams", "%d",
                          yyjson_get_int (yyjson_obj_get (server, "stream_count")));
    mxmlElementSetAttrf (status, "TXPacketRate", "%.1f",
                         yyjson_get_real (yyjson_obj_get (server, "transmit_packet_rate")));
    mxmlElementSetAttrf (status, "TXByteRate", "%.1f",
                          yyjson_get_real (yyjson_obj_get (server, "transmit_byte_rate")));
    mxmlElementSetAttrf (status, "RXPacketRate", "%.1f",
                         yyjson_get_real (yyjson_obj_get (server, "receive_packet_rate")));
    mxmlElementSetAttrf (status, "RXByteRate", "%.1f",
                         yyjson_get_real (yyjson_obj_get (server, "receive_byte_rate")));
    mxmlElementSetAttrf (status, "EarliestPacketID", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (server, "earliest_packet_id")));
    mxmlElementSetAttr (status, "EarliestPacketCreationTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "earliest_packet_time"))));
    mxmlElementSetAttr (status, "EarliestPacketDataStartTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "earliest_data_start"))));
    mxmlElementSetAttr (status, "EarliestPacketDataEndTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "earliest_data_end"))));
    mxmlElementSetAttrf (status, "LatestPacketID", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (server, "latest_packet_id")));
    mxmlElementSetAttr (status, "LatestPacketCreationTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "latest_packet_time"))));
    mxmlElementSetAttr (status, "LatestPacketDataStartTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "latest_data_start"))));
    mxmlElementSetAttr (status, "LatestPacketDataEndTime",
                        DASHNULL (yyjson_get_str (yyjson_obj_get (server, "latest_data_end"))));
  }

  if (elements == (INFO_STATUS | INFO_ID) && trusted) /* Only add server threads if client is trusted */
  {
    yyjson_val *thread_array;
    yyjson_val *thread_iter = NULL;
    mxml_node_t *stlist, *st;
    int totalcount = 0;

    /* Create "ServerThreads" element */
    if (!(stlist = mxmlNewElement (xmldoc, "ServerThreads")))
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      yyjson_doc_free (json);
      mxmlRelease (xmldoc);
      return NULL;
    }

    thread_array = yyjson_obj_get (server, "thread");

    yyjson_arr_foreach (thread_array, idx, max, thread_iter)
    {
      const char *thread_type = yyjson_get_str (yyjson_obj_get (thread_iter, "type"));
      totalcount++;

      if (!(st = mxmlNewElement (stlist, "Thread")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        yyjson_doc_free (json);
        mxmlRelease (xmldoc);
        return NULL;
      }

      mxmlElementSetAttr (st, "Flags",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "state"))));

      /* Determine server thread type and add specifics */
      if (thread_type && !strcasecmp (thread_type, "Listener"))
      {
        mxmlElementSetAttr (st, "Type",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "protocol"))));
        mxmlElementSetAttr (st, "Port",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "port"))));
      }
      else if (thread_type && !strcasecmp (thread_type, "miniSEED scanner"))
      {
        mxmlElementSetAttr (st, "Type", "miniSEED scanner");
        mxmlElementSetAttr (st, "Directory",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "directory"))));
        mxmlElementSetAttrf (st, "MaxRecursion", "%d",
                             yyjson_get_int (yyjson_obj_get (thread_iter, "max_recursion")));
        mxmlElementSetAttr (st, "StateFile",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "state_file"))));
        mxmlElementSetAttr (st, "Match",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "match"))));
        mxmlElementSetAttr (st, "Reject",
                            DASHNULL (yyjson_get_str (yyjson_obj_get (thread_iter, "reject"))));
        mxmlElementSetAttrf (st, "ScanTime", "%g",
                             yyjson_get_real (yyjson_obj_get (thread_iter, "scan_time")));
        mxmlElementSetAttrf (st, "PacketRate", "%g",
                             yyjson_get_real (yyjson_obj_get (thread_iter, "packet_rate")));
        mxmlElementSetAttrf (st, "ByteRate", "%g",
                             yyjson_get_real (yyjson_obj_get (thread_iter, "byte_rate")));
      }
      else
      {
        mxmlElementSetAttr (st, "Type", "Unknown Thread");
      }
    }

    /* Add thread count attribute to ServerThreads element */
    mxmlElementSetAttrf (stlist, "TotalServerThreads", "%d", totalcount);
  }
  else if (elements & INFO_STREAMS)
  {
    yyjson_val *stream_array;
    yyjson_val *stream_iter = NULL;
    mxml_node_t *streamlist, *stream;
    int selectedcount = 0;

    /* Create "StreamList" element and add attributes */
    if (!(streamlist = mxmlNewElement (xmldoc, "StreamList")))
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      yyjson_doc_free (json);
      mxmlRelease (xmldoc);
      return NULL;
    }

    stream_array = yyjson_obj_get (root, "stream");

    yyjson_arr_foreach (stream_array, idx, max, stream_iter)
    {
      selectedcount++;

      if (!(stream = mxmlNewElement (streamlist, "Stream")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        yyjson_doc_free (json);
        mxmlRelease (xmldoc);
        return NULL;
      }

      mxmlElementSetAttr (stream, "Name",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (stream_iter, "id"))));
      mxmlElementSetAttrf (stream, "EarliestPacketID", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (stream_iter, "earliest_packet_id")));
      mxmlElementSetAttr (stream, "EarliestPacketDataStartTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (stream_iter, "start_time"))));
      mxmlElementSetAttrf (stream, "LatestPacketID", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (stream_iter, "latest_packet_id")));
      /* Use the same time for both data start and end for legacy client support */
      mxmlElementSetAttr (stream, "LatestPacketDataStartTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (stream_iter, "end_time"))));
      mxmlElementSetAttr (stream, "LatestPacketDataEndTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (stream_iter, "end_time"))));
      mxmlElementSetAttrf (stream, "DataLatency", "%.1f",
                           yyjson_get_real (yyjson_obj_get (stream_iter, "data_latency")));
    }

    mxmlElementSetAttrf (streamlist, "TotalStreams", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (server, "stream_count")));
    mxmlElementSetAttrf (streamlist, "SelectedStreams", "%d", selectedcount);
  }
  else if (elements & INFO_CONNECTIONS)
  {
    yyjson_val *connections;
    yyjson_val *client_array;
    yyjson_val *client_iter = NULL;
    mxml_node_t *connlist, *conn;
    int selectedcount = 0;

    /* Create "ConnectionList" element */
    if (!(connlist = mxmlNewElement (xmldoc, "ConnectionList")))
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      yyjson_doc_free (json);
      mxmlRelease (xmldoc);
      return NULL;
    }

    connections = yyjson_obj_get (root, "connections");

    client_array = yyjson_obj_get (connections, "client");

    yyjson_arr_foreach (client_array, idx, max, client_iter)
    {
      selectedcount++;

      if (!(conn = mxmlNewElement (connlist, "Connection")))
      {
        lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
        yyjson_doc_free (json);
        mxmlRelease (xmldoc);
        return NULL;
      }

      mxmlElementSetAttr (conn, "Type",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "type"))));
      mxmlElementSetAttr (conn, "Host",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "host"))));
      mxmlElementSetAttr (conn, "IP",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "ip_address"))));
      mxmlElementSetAttr (conn, "Port",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "client_port"))));
      mxmlElementSetAttr (conn, "ClientID",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "client_id"))));
      mxmlElementSetAttr (conn, "ConnectionTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "connect_time"))));
      mxmlElementSetAttr (conn, "Match",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "match"))));
      mxmlElementSetAttr (conn, "Reject",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "reject"))));
      mxmlElementSetAttrf (conn, "StreamCount", "%d",
                           yyjson_get_int (yyjson_obj_get (client_iter, "stream_count")));
      mxmlElementSetAttrf (conn, "PacketID", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "packet_id")));
      mxmlElementSetAttr (conn, "PacketDataStartTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "packet_data_start_time"))));
      mxmlElementSetAttr (conn, "PacketCreationTime",
                          DASHNULL (yyjson_get_str (yyjson_obj_get (client_iter, "packet_creation_time"))));
      mxmlElementSetAttrf (conn, "TXPacketCount", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "transmit_packets")));
      mxmlElementSetAttrf (conn, "TXPacketRate", "%.1f",
                           yyjson_get_real (yyjson_obj_get (client_iter, "transmit_packet_rate")));
      mxmlElementSetAttrf (conn, "TXByteCount", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "transmit_bytes")));
      mxmlElementSetAttrf (conn, "TXByteRate", "%.1f",
                           yyjson_get_real (yyjson_obj_get (client_iter, "transmit_byte_rate")));
      mxmlElementSetAttrf (conn, "RXPacketCount", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "receive_packets")));
      mxmlElementSetAttrf (conn, "RXPacketRate", "%.1f",
                           yyjson_get_real (yyjson_obj_get (client_iter, "receive_packet_rate")));
      mxmlElementSetAttrf (conn, "RXByteCount", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "receive_bytes")));
      mxmlElementSetAttrf (conn, "RXByteRate", "%.1f",
                           yyjson_get_real (yyjson_obj_get (client_iter, "receive_byte_rate")));
      mxmlElementSetAttrf (conn, "Latency", "%.1f",
                           yyjson_get_real (yyjson_obj_get (client_iter, "lag_seconds")));
      mxmlElementSetAttrf (conn, "PercentLag", "%d",
                           yyjson_get_int (yyjson_obj_get (client_iter, "lag_percent")));
    }

    mxmlElementSetAttrf (connlist, "TotalConnections", "%" PRIu64,
                         yyjson_get_uint (yyjson_obj_get (connections, "client_count")));
    mxmlElementSetAttrf (connlist, "SelectedConnections", "%d", selectedcount);
  }

  /* Generate XML string */
  mxml_options_t *options = mxmlOptionsNew ();
  mxmlOptionsSetWrapMargin (options, 0);
  xml_string = mxmlSaveAllocString (xmldoc, options);
  mxmlOptionsDelete (options);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

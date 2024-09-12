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

#include "cJSON.h"
#include "generic.h"
#include "infojson.h"
#include "slclient.h"

/***************************************************************************
 * Create and rerturn an SeedLink v3 INFO ID document in XML format.
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

  cJSON *json;
  char *json_string;
  char *xml_string = NULL;

  /* Generate JSON formatted document for INFO ID level */
  if ((json_string = info_json (cinfo, software, INFO_ID, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = cJSON_Parse (json_string)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    free (json_string);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    mxmlRelease (xmldoc);
    free (json_string);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")));

  /* Generate XML string */
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

  cJSON_Delete (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Create and rerturn an SeedLink v3 INFO CAPABILITIES document in XML format.
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

  cJSON *json;
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

  if ((json = cJSON_Parse (json_string)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    free (json_string);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    mxmlRelease (xmldoc);
    free (json_string);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")));

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
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

  cJSON_Delete (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Create and rerturn an SeedLink v3 INFO STATIONS or INFO STREAMS
 * document in XML format.
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

  cJSON *json;
  cJSON *station_array;
  cJSON *stream_array;
  cJSON *station_iter = NULL;
  cJSON *stream_iter  = NULL;


  char *json_string;
  char *xml_string = NULL;

  char idstr[32] = {0};
  char *ptr;

  /* Generate JSON formatted document for INFO STREAMS or STATIONS level */
  if ((json_string = info_json (cinfo, software,
                                (include_streams) ? INFO_STREAMS : INFO_STATIONS,
                                NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = cJSON_Parse (json_string)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    free (json_string);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    mxmlRelease (xmldoc);
    free (json_string);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")));

  /* Add stations */
  if ((station_array = cJSON_GetObjectItem (json, "station")) != NULL)
  {
    cJSON_ArrayForEach (station_iter, station_array)
    {
      if (!(station = mxmlNewElement (seedlink, "station")))
      {
        break;
      }

      strncpy (idstr, cJSON_GetStringValue (cJSON_GetObjectItem (station_iter, "id")),
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
                          cJSON_GetStringValue (cJSON_GetObjectItem (station_iter, "description")));
      mxmlElementSetAttrf (station, "begin_seq", "%.0f",
                           cJSON_GetNumberValue (cJSON_GetObjectItem (station_iter, "start_seq")));
      mxmlElementSetAttrf (station, "end_seq", "%.0f",
                           cJSON_GetNumberValue (cJSON_GetObjectItem (station_iter, "end_seq")));
      mxmlElementSetAttr (station, "stream_check", "enabled");

      if ((stream_array = cJSON_GetObjectItem (station_iter, "stream")) != NULL)
      {
        cJSON_ArrayForEach (stream_iter, stream_array)
        {
          if (!(stream = mxmlNewElement (station, "stream")))
          {
            break;
          }

          strncpy (idstr, cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "id")),
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
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "subformat")));
          mxmlElementSetAttr (stream, "begin_time",
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "start_time")));
          mxmlElementSetAttr (stream, "end_time",
                              cJSON_GetStringValue (cJSON_GetObjectItem (stream_iter, "end_time")));
        }
      }
    }
  }

  /* Generate XML string */
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

  cJSON_Delete (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

/***************************************************************************
 * Create and rerturn an SeedLink v3 INFO CONNECTIONS document in XML format.
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

  cJSON *json;
  cJSON *connections;
  cJSON *client_array;
  cJSON *client_iter = NULL;

  char *json_string;
  char *xml_string = NULL;

  char *network;

  /* Generate JSON formatted document for INFO CONNECTIONS level */
  if ((json_string = info_json (cinfo, software, INFO_CONNECTIONS, NULL)) == NULL)
  {
    return NULL;
  }

  if ((json = cJSON_Parse (json_string)) == NULL)
  {
    free (json_string);
    return NULL;
  }
  free (json_string);

  if (!(xmldoc = mxmlNewXML ("1.0")))
  {
    free (json_string);
    return NULL;
  }

  if (!(seedlink = mxmlNewElement (xmldoc, "seedlink")))
  {
    mxmlRelease (xmldoc);
    free (json_string);
    return NULL;
  }

  /* Translate JSON to XML */
  mxmlElementSetAttr (seedlink, "software",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "software")));
  mxmlElementSetAttr (seedlink, "organization",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "organization")));
  mxmlElementSetAttr (seedlink, "started",
                      cJSON_GetStringValue (cJSON_GetObjectItem (json, "server_start")));

  connections = cJSON_GetObjectItem (json, "connections");

  /* Add stations and connections */
  if (connections && (client_array = cJSON_GetObjectItem (connections, "clients")) != NULL)
  {
    cJSON_ArrayForEach (client_iter, client_array)
    {
      if (!(station = mxmlNewElement (seedlink, "station")))
      {
        break;
      }

      mxmlElementSetAttr (station, "name", "CLIENT");

      network = cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "type"));
      if (strstr (network, "DataLink"))
        mxmlElementSetAttr (station, "network", "DL");
      else if (strstr (network, "SeedLink"))
        mxmlElementSetAttr (station, "network", "SL");
      else if (strstr (network, "HTTP"))
        mxmlElementSetAttr (station, "network", "SL");
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
                          cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "ip_address")));
      mxmlElementSetAttr (connection, "port",
                          cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "port")));
      mxmlElementSetAttr (connection, "ctime",
                          cJSON_GetStringValue (cJSON_GetObjectItem (client_iter, "connect_time")));
      mxmlElementSetAttr (connection, "begin_seq", "0");
      mxmlElementSetAttrf (connection, "current_seq", "%.0f",
                           cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "current_packet")));
      mxmlElementSetAttr (connection, "sequence_gaps", "0");
      mxmlElementSetAttrf (connection, "txcount", "%.0f",
                           cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "transmit_packets")));
      mxmlElementSetAttrf (connection, "totBytes", "%.0f",
                            cJSON_GetNumberValue (cJSON_GetObjectItem (client_iter, "transmit_bytes")));
      mxmlElementSetAttr (connection, "begin_seq_valid", "yes");
      mxmlElementSetAttr (connection, "realtime", "yes");
      mxmlElementSetAttr (connection, "end_of_data", "no");
    }
  }

  /* Generate XML string */
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

  cJSON_Delete (json);
  mxmlRelease (xmldoc);

  return xml_string;
}
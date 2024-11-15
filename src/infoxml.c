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
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

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
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

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

      strncpy (idstr, yyjson_get_str (yyjson_obj_get (station_iter, "id")),
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
        yyjson_arr_foreach (stream_array, idx, max, stream_iter)
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
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

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
                          yyjson_get_str (yyjson_obj_get (client_iter, "ip_address")));
      mxmlElementSetAttr (connection, "port",
                          yyjson_get_str (yyjson_obj_get (client_iter, "port")));
      mxmlElementSetAttr (connection, "ctime",
                          yyjson_get_str (yyjson_obj_get (client_iter, "connect_time")));
      mxmlElementSetAttr (connection, "begin_seq", "0");
      mxmlElementSetAttrf (connection, "current_seq", "%" PRIu64,
                           yyjson_get_uint (yyjson_obj_get (client_iter, "current_packet")));
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
  mxmlSetWrapMargin (0);
  xml_string = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK);

  yyjson_doc_free (json);
  mxmlRelease (xmldoc);

  return xml_string;
}

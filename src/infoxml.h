/**************************************************************************
 * infoxml.h
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

#ifndef INFOXML_H
#define INFOXML_H 1

#ifdef __cplusplus
extern "C"
{
#endif

#include "clients.h"
#include "logging.h"
#include "ring.h"
#include "ringserver.h"

extern char *info_xml_slv3_id (ClientInfo *cinfo, const char *software);
extern char *info_xml_slv3_capabilities (ClientInfo *cinfo, const char *software);
extern char *info_xml_slv3_stations (ClientInfo *cinfo, const char *software,
                                     int include_streams);
extern char *info_xml_slv3_connections (ClientInfo *cinfo, const char *software);

#ifdef __cplusplus
}
#endif

#endif /* INFOXML_H */

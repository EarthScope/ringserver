/**************************************************************************
 * infojson.h
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

#ifndef INFOJSON_H
#define INFOJSON_H 1

#ifdef __cplusplus
extern "C"
{
#endif

#include "clients.h"
#include "logging.h"
#include "ring.h"
#include "ringserver.h"

/* Info elements, most are defined in SeedLink v4 */
typedef enum
{
  INFO_ID           = 1u << 1,
  INFO_CAPABILITIES = 1u << 2,
  INFO_FORMATS      = 1u << 3,
  INFO_FILTERS      = 1u << 4,
  INFO_STATIONS     = 1u << 5,
  INFO_STREAMS      = 1u << 6,
  INFO_STREAMS_ONLY = 1u << 7,
  INFO_CONNECTIONS  = 1u << 8,
} InfoElements;

extern char *info_json (ClientInfo *cinfo, const char *software, InfoElements elements);
extern char *error_json (ClientInfo *cinfo, const char *software,
                         const char *code, const char *message);

#ifdef __cplusplus
}
#endif

#endif /* INFOJSON_H */

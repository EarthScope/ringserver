/**************************************************************************
 * dlclient.h
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
 * Copyright (C) 2020:
 * @author Chad Trabant, IRIS Data Management Center
 **************************************************************************/

#ifndef DLCLIENT_H
#define DLCLIENT_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include "rbtree.h"
#include "ringserver.h"

/* DataLink server capability flags */
#define DLCAPFLAGS "DLPROTO:1.0"

#define DLMAXREGEXLEN  1048576  /* Maximum regex pattern size */

extern int DLHandleCmd (ClientInfo *cinfo);
extern int DLStreamPackets (ClientInfo *cinfo);

#ifdef __cplusplus
}
#endif

#endif /* DLCLIENT_H */

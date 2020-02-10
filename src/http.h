/**************************************************************************
 * http.h
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

#ifndef HTTP_H
#define HTTP_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "ring.h"
#include "clients.h"

/* Extract bit range and shift to start */
#define EXTRACTBITRANGE(VALUE, STARTBIT, LENGTH) ((VALUE & (((1 << LENGTH) - 1) << STARTBIT)) >> STARTBIT)

extern int HandleHTTP (char *recvbuffer, ClientInfo *cinfo);
extern int RecvWSFrame (ClientInfo *cinfo, uint64_t *length, uint32_t *mask);

#ifdef __cplusplus
}
#endif

#endif /* HTTP_H */

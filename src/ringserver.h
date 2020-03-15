/**************************************************************************
 * ringserver.h
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

#ifndef RINGSERVER_H
#define RINGSERVER_H 1

#include <pthread.h>
#include "clients.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PACKAGE   "ringserver"
#define VERSION   "2020.075"

/* Thread data flags */
#define TDF_SPAWNING    (1<<0)          /* Thread is now spawning   */
#define TDF_ACTIVE      (1<<1)          /* If set, thread is active */
#define TDF_CLOSE       (1<<2)          /* If set, thread closes    */
#define TDF_CLOSING     (1<<3)          /* Thread in close process  */
#define TDF_CLOSED      (1<<4)          /* Thread is closed         */

/* Thread data associated with most threads */
struct thread_data {
  pthread_mutex_t td_lock;
  pthread_t       td_id;
  int             td_flags;
  int             td_done;
  void           *td_prvtptr;
};

/* Server thread types */
#define LISTEN_THREAD     1
#define MSEEDSCAN_THREAD  2

/* Listen thread protocols */
#define PROTO_DATALINK  0x01
#define PROTO_SEEDLINK  0x02
#define PROTO_HTTP      0x04
#define FAMILY_IPv4     0x08
#define FAMILY_IPv6     0x10

#define PROTO_ALL (PROTO_DATALINK | PROTO_SEEDLINK | PROTO_HTTP)

/* Doubly-linkable structure to list server threads */
struct sthread {
  struct thread_data *td;
  unsigned int    type;
  void           *params;
  struct sthread *prev;
  struct sthread *next;
};

/* Doubly-linkable structure to list client threads */
struct cthread {
  struct thread_data *td;
  struct cthread *prev;
  struct cthread *next;
};

/* A structure for server listening parameters */
typedef struct ListenPortParams_s
{
  char portstr[11];      /* Port number to listen on as string */
  uint8_t protocols;     /* Protocol flags for this connection */
  int socket;            /* Socket descriptor or -1 when not connected */
} ListenPortParams;

/* Global variables declared in ringserver.c */
extern pthread_mutex_t sthreads_lock;
extern struct sthread *sthreads;
extern pthread_mutex_t cthreads_lock;
extern struct cthread *cthreads;
extern char *serverid;
extern char *webroot;
extern hptime_t serverstarttime;
extern int clientcount;
extern int resolvehosts;
extern int shutdownsig;

#ifdef __cplusplus
}
#endif

#endif /* RINGSERVER_H */

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
 * Copyright (C) 2024:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

#ifndef RINGSERVER_H
#define RINGSERVER_H 1

#include <pthread.h>
#include "clients.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PACKAGE   "ringserver"
#define VERSION   "4.0.0"

/* Thread states */
typedef enum
{
  TDS_SPAWNING, /* Thread is now spawning */
  TDS_ACTIVE,   /* If set, thread is active */
  TDS_CLOSE,    /* If set, thread closes */
  TDS_CLOSING,  /* Thread in close process */
  TDS_CLOSED    /* Thread is closed */
} ThreadState;

/* Thread data associated with most threads */
struct thread_data {
  pthread_mutex_t td_lock;
  pthread_t       td_id;
  ThreadState     td_state;
  int             td_done;
  void           *td_prvtptr;
};

/* Server thread types */
typedef enum
{
  LISTEN_THREAD,   /* Listen for incoming network connections */
  MSEEDSCAN_THREAD /* Scan for miniSEED files */
} ServerThreadType;

/* Doubly-linked structure of server threads */
struct sthread
{
  struct thread_data *td;
  ServerThreadType type;
  void           *params;
  struct sthread *prev;
  struct sthread *next;
};

/* Listen thread protocols */
typedef enum
{
  PROTO_DATALINK = 1u << 1,
  PROTO_SEEDLINK = 1u << 2,
  PROTO_HTTP     = 1u << 3,
  PROTO_TLS      = 1u << 4,
  FAMILY_IPv4    = 1u << 5,
  FAMILY_IPv6    = 1u << 6,
  PROTO_ALL      = PROTO_DATALINK | PROTO_SEEDLINK | PROTO_HTTP
} ListenProtocols;

/* Doubly-linked structure of client threads */
struct cthread
{
  struct thread_data *td;
  struct cthread *prev;
  struct cthread *next;
};

/* A structure for server listening parameters */
typedef struct ListenPortParams
{
  char portstr[11];          /* Port number to listen on as string */
  ListenProtocols protocols; /* Protocol flags for this connection */
  int socket;                /* Socket descriptor or -1 when not connected */
} ListenPortParams;

/* Global variables declared in ringserver.c */
extern pthread_mutex_t sthreads_lock;
extern struct sthread *sthreads;
extern pthread_mutex_t cthreads_lock;
extern struct cthread *cthreads;
extern char *serverid;
extern char *webroot;
extern nstime_t serverstarttime;
extern int clientcount;
extern int resolvehosts;
extern int shutdownsig;

#ifdef __cplusplus
}
#endif

#endif /* RINGSERVER_H */

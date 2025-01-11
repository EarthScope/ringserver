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

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "clients.h"

#define PACKAGE   "ringserver"
#define VERSION   "4.0.1-DEV"

/* Thread states */
typedef enum
{
  TDS_SPAWNING, /* Thread is now spawning */
  TDS_ACTIVE,   /* Thread is active */
  TDS_CLOSE,    /* Thread close triggered */
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
  PROTO_ALL      = PROTO_DATALINK | PROTO_SEEDLINK | PROTO_HTTP
} ListenProtocols;

/* Listen thread options */
typedef enum
{
  ENCRYPTION_TLS = 1u << 1,
  FAMILY_IPv4    = 1u << 2,
  FAMILY_IPv6    = 1u << 3,
} ListenOptions;

/* Doubly-linked structure of client threads */
struct cthread
{
  struct thread_data *td;
  struct cthread *prev;
  struct cthread *next;
};

/* Singly-linked list of string values for general use */
struct strnode
{
  char *string;
  struct strnode *next;
};

/* A structure for server listening parameters */
typedef struct ListenPortParams
{
  char portstr[NI_MAXSERV];  /* Port number to listen on as string */
  ListenProtocols protocols; /* Protocol flags for this connection */
  ListenOptions options;     /* Options for this connection */
  int socket;                /* Socket descriptor or -1 when not connected */
} ListenPortParams;

#define ListenPortParams_INITIALIZER {.portstr = {0}, .protocols = 0, .options = 0, .socket = -1}

/* A structure to list IP addresses ranges */
typedef struct IPNet_s
{
  union
  {
    struct in_addr in_addr;
    struct in6_addr in6_addr;
  } network;
  union
  {
    struct in_addr in_addr;
    struct in6_addr in6_addr;
  } netmask;
  int family;
  char *limitstr;
  struct IPNet_s *next;
} IPNet;

/* Global parameters */
struct param_s
{
  nstime_t serverstarttime; /* Server start time */
  int clientcount;          /* Track number of connected clients */
  int shutdownsig;          /* Shutdown signal */
  time_t configfilemtime;   /* Modification time of configuration file */
  pthread_mutex_t sthreads_lock;
  struct sthread *sthreads; /* Server threads list */
  pthread_mutex_t cthreads_lock;
  struct cthread *cthreads; /* Client threads list */
};

extern struct param_s param;

/* Configuration parameters */
struct config_s
{
  char *configfile;         /* Configuration file */
  char *serverid;           /* Server ID */
  char *ringdir;            /* Directory for ring files */
  uint64_t ringsize;        /* Size of ring buffer file */
  uint32_t pktsize;         /* Ring packet size */
  uint32_t maxclients;      /* Enforce maximum number of clients */
  uint32_t maxclientsperip; /* Enforce maximum number of clients per IP */
  uint32_t clienttimeout;   /* Idle client threshold in seconds, then disconnect */
  uint32_t netiotimeout;    /* Network I/O timeout in seconds, then disconnect */
  float timewinlimit;       /* Time window search limit in percent */
  uint8_t resolvehosts;     /* Flag to control resolving of client hostnames */
  uint8_t memorymapring;    /* Flag to control mmap'ing of packet buffer */
  uint8_t volatilering;     /* Flag to control if ring is volatile or not */
  uint8_t autorecovery;     /* Flag to control auto recovery from corruption */
  char *webroot;            /* Web content root directory */
  char *httpheaders;        /* HTTP headers to include in each HTTP response */
  char *mseedarchive;       /* miniSEED archive definition */
  int mseedidleto;          /* miniSEED idle file timeout */
  IPNet *limitips;          /* List of limit-by-IP entries */
  IPNet *matchips;          /* List of IPs allowed to connect */
  IPNet *rejectips;         /* List of IPs not allowed to connect */
  IPNet *writeips;          /* List of IPs allowed to submit data */
  IPNet *trustedips;        /* List of IPs to trust */
  char *tlscertfile;        /* TLS certificate file */
  char *tlskeyfile;         /* TLS key file */
  int tlsverifyclientcert;  /* Verify client certificate */
};

extern struct config_s config;

extern int GenProtocolString (ListenProtocols protocols, ListenOptions options,
                              char *result, size_t maxlength);

#ifdef __cplusplus
}
#endif

#endif /* RINGSERVER_H */

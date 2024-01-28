/**************************************************************************
 * slclient.h
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

#ifndef SLCLIENT_H
#define SLCLIENT_H 1

#ifdef __cplusplus
extern "C"
{
#endif

#include "rbtree.h"
#include <pthread.h>

/* The total length of SLSERVERVER should be <= 98 bytes for compatibility
   with libslink versions < 2.0. */
#define SLCAPABILITIES_ID "SLPROTO:4.0 SLPROTO:3.1 CAP WS:13"
#define SLSERVERVER "SeedLink v4.0 (" VERSION " RingServer) :: " SLCAPABILITIES_ID

/* Server capabilities for v4 */
#define SLCAPABILITIESv4 "SLPROTO:4.0 SLPROTO:3.1 TIME WS:13"

#define SLHEADSIZE 8             /* SeedLink header size */
#define SLHEADSIZE_EXT 17        /* Extended SeedLink header fixed size */
#define SLINFORECSIZE 512        /* miniSEED record size for INFO packets */

#define SLMAXREGEXLEN 1048576 /* Maximum length of match/reject regex pattern */
#define SLMAXSELECTLEN 2048   /* Maximum length of per-station/global selector buffer */

#define SLINFO_ID 1
#define SLINFO_CAPABILITIES 2
#define SLINFO_STATIONS 3
#define SLINFO_STREAMS 4
#define SLINFO_GAPS 5
#define SLINFO_CONNECTIONS 6
#define SLINFO_ALL 7

/* Error codes */
typedef enum
{
  ERROR_NONE         = 0,
  ERROR_INTERNAL     = 1u << 1,
  ERROR_UNSUPPORTED  = 1u << 2,
  ERROR_UNEXPECTED   = 1u << 3,
  ERROR_UNAUTHORIZED = 1u << 4,
  ERROR_LIMIT        = 1u << 5,
  ERROR_ARGUMENTS    = 1u << 6,
  ERROR_AUTH         = 1u << 7,
} ErrorCode;

/* Structure to hold SeedLink specific parameters */
typedef struct SLInfo_s
{
  uint8_t proto_major;   /* Major protocol version */
  uint8_t proto_minor;   /* Minor protocol version */
  int extreply;          /* Capability flag: client can recieve extended replies */
  int dialup;            /* Connection is in dialup/fetch mode */
  int batch;             /* Connection is in batch mode */
  int terminfo;          /* Terminating INFO packet flag */
  int64_t startid;       /* Starting packet ID */
  char *selectors;       /* List of SeedLink selectors */
  int stationcount;      /* Number of stations requested with STATION */
  int timewinchannels;   /* Count of channels for time window completion check */
  RBTree *stations;      /* Binary tree of stations requested */
  char reqstaid[51];     /* Requested station ID, used during negotiation */
} SLInfo;

/* Requested station IDs, used as the data for B-tree of stations */
typedef struct SLStaNode_s
{
  nstime_t starttime; /* Requested start time for StaID */
  nstime_t endtime;   /* Requested end time for StaID */
  int64_t packetid;   /* Requested packet ID */
  nstime_t datastart; /* Data start time of requested packet */
  char *selectors;    /* List of SeedLink selectors for StaID */
} SLStaNode;

/* Stream listings, used as the data for B-tree of network-stations */
typedef struct SLNetStaNode_s
{
  char net[10];            /* Network code parsed from stream ID */
  char sta[10];            /* Station code parsed from stream ID*/
  nstime_t earliestdstime; /* Data start time of earliest packet for StaID */
  int64_t earliestid;      /* Earliest packet ID for StaID */
  nstime_t latestdstime;   /* Data start time of latest packet for StaID */
  int64_t latestid;        /* Latest packet ID for StaID */
  Stack *streams;          /* Stack of associated streams */
} SLNetStaNode;

extern int SLHandleCmd (ClientInfo *cinfo);
extern int SLStreamPackets (ClientInfo *cinfo);
extern void SLFree (ClientInfo *cinfo);

#ifdef __cplusplus
}
#endif

#endif /* SLCLIENT_H */

/**************************************************************************
 * ring.h
 *
 * Declarations for fundamental ring routines and data structures.
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

#ifndef RING_H
#define RING_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>
#include <limits.h>
#include <pthread.h>

#include <libmseed.h>

#define PCRE2_STATIC
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#define PCRE2_COMPILE_OPTIONS (PCRE2_NO_AUTO_CAPTURE | PCRE2_NEVER_UTF)

#include "rbtree.h"

/* Static ring parameters */
#define RING_SIGNATURE "RING"
#define RING_VERSION  2

/* Ring relative positioning values */
#define RINGCURRENT  -1
#define RINGEARLIEST -2
#define RINGLATEST   -3
#define RINGNEXT     -4

/* Define a maximum stream ID string length */
#define MAXSTREAMID 60

/* Macros for updating different patterns */
#define RingLimit(reader, pattern) UpdatePattern (&(reader)->limit, &(reader)->limit_data, pattern, "ring limit")
#define RingMatch(reader, pattern) UpdatePattern (&(reader)->match, &(reader)->match_data, pattern, "ring match")
#define RingReject(reader, pattern) UpdatePattern (&(reader)->reject, &(reader)->reject_data, pattern, "ring reject")

/* Ring parameters, stored at the beginning of the packet buffer file */
typedef struct RingParams
{
  char      signature[4];     /* RING_SIGNATURE */
  uint16_t  version;          /* RING_VERSION */
  uint64_t  ringsize;         /* Ring size in bytes */
  uint32_t  pktsize;          /* Packet size in bytes */
  int64_t   maxpktid;         /* Maximum packet ID */
  int64_t   maxpackets;       /* Maximum number of packets */
  int64_t   maxoffset;        /* Maximum packet offset */
  uint32_t  headersize;       /* Size of ring header */
  uint8_t   corruptflag;      /* Flag indicating the ring is corrupt */
  uint8_t   fluxflag;         /* Flag indicating the ring is in flux */
  uint8_t   mmapflag;         /* Memory mapped flag */
  uint8_t   volatileflag;     /* Volatile ring flag */
  pthread_mutex_t *writelock; /* Mutex lock for ring write access */
  RBTree   *streamidx;        /* Binary tree of streams */
  pthread_mutex_t *streamlock;/* Mutex lock for stream index */
  int32_t   streamcount;      /* Count of streams in index */
  int64_t   earliestid;       /* Earliest packet ID */
  nstime_t  earliestptime;    /* Earliest packet creation time */
  nstime_t  earliestdstime;   /* Earliest packet data start time */
  nstime_t  earliestdetime;   /* Earliest packet data end time */
  int64_t   earliestoffset;   /* Earliest packet offset in bytes */
  int64_t   latestid;         /* Latest packet ID */
  nstime_t  latestptime;      /* Latest packet creation time */
  nstime_t  latestdstime;     /* Latest packet data start time */
  nstime_t  latestdetime;     /* Latest packet data end time */
  int64_t   latestoffset;     /* Latest packet offset in bytes */
  nstime_t  ringstart;        /* Ring initialization time */
  double    txpacketrate;     /* Transmission packet rate in Hz */
  double    txbyterate;       /* Transmission byte rate in Hz */
  double    rxpacketrate;     /* Reception packet rate in Hz */
  double    rxbyterate;       /* Reception byte rate in Hz */
  char     *data;             /* Pointer to start of data buffer */
} RingParams;

/* Ring packet header structure, data follows header in the ring */
/* RW tagged values are set when packets are added to the ring */
typedef struct RingPacket
{
  int64_t   pktid;           /* RW: Packet ID */
  int64_t   offset;          /* RW: Offset in ring */
  nstime_t  pkttime;         /* RW: Packet creation time */
  int64_t   nextinstream;    /* RW: ID of next packet in stream, 0 if none */
  char      streamid[MAXSTREAMID]; /* Packet stream ID, NULL terminated */
  nstime_t  datastart;       /* Packet data start time */
  nstime_t  dataend;         /* Packet data end time */
  uint32_t  datasize;        /* Packet data size in bytes */
} RingPacket;

/* Ring stream structure used for the stream index */
typedef struct RingStream
{
  char        streamid[MAXSTREAMID]; /* Packet stream ID */
  nstime_t    earliestdstime;/* Earliest packet data start time */
  nstime_t    earliestdetime;/* Earliest packet data end time */
  nstime_t    earliestptime; /* Earliest packet creation time */
  int64_t     earliestid;    /* ID of earliest packet */
  nstime_t    latestdstime;  /* Latest packet data start time */
  nstime_t    latestdetime;  /* Latest packet data end time */
  nstime_t    latestptime;   /* Latest packet creation time */
  int64_t     latestid;      /* ID of latest packet */
} RingStream;

/* Ring reader parameters */
typedef struct RingReader
{
  RingParams *ringparams;    /* Ring parameters for this reader */
  int64_t     pktid;         /* Current packet ID position in ring */
  nstime_t    pkttime;       /* Current packet creation time */
  nstime_t    datastart;     /* Current packet data start time */
  nstime_t    dataend;       /* Current packet data end time */
  pcre2_code *limit;         /* Compiled limit expression */
  pcre2_match_data *limit_data;  /* Match data results */
  pcre2_code *match;         /* Compiled match expression */
  pcre2_match_data *match_data;  /* Match data results */
  pcre2_code *reject;        /* Compiled reject expression */
  pcre2_match_data *reject_data; /* Match data results */
} RingReader;


extern int RingInitialize (char *ringfilename, char *streamfilename,
			   uint64_t ringsize, uint32_t pktsize, int64_t maxpktid,
			   uint8_t mmapflag, uint8_t volatileflag,
			   int *ringfd, RingParams **ringparams);
extern int RingShutdown (int ringfd, char *streamfilename, RingParams *ringparams);
extern int RingWrite (RingParams *ringparams, RingPacket *packet,
		      char *packetdata, uint32_t datasize);
extern int64_t RingRead (RingReader *reader, int64_t reqid,
			 RingPacket *packet, char *packetdata);
extern int64_t RingReadNext (RingReader *reader, RingPacket *packet, char *packetdata);
extern int64_t RingPosition (RingReader *reader, int64_t pktid, nstime_t pkttime);
extern int64_t RingAfter (RingReader *reader, nstime_t reftime, int whence);
extern int64_t RingAfterRev (RingReader *reader, nstime_t reftime, int64_t pktlimit, int whence);
extern int UpdatePattern (pcre2_code **code, pcre2_match_data **data,
                          const char *pattern, const char *description);
extern Stack* GetStreamsStack (RingParams *ringparams, RingReader *reader);


#ifdef __cplusplus
}
#endif

#endif /* RING_H */

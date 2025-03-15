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
#define RING_SIGNATURE_LENGTH 4
#define RING_VERSION  3

/* Special ring packet ID values, the highest 10 values are reserved */
#define RINGID_ERROR    (UINT64_MAX)
#define RINGID_NONE     (UINT64_MAX - 1)
#define RINGID_EARLIEST (UINT64_MAX - 2)
#define RINGID_LATEST   (UINT64_MAX - 3)
#define RINGID_NEXT     (UINT64_MAX - 4)
#define RINGID_MAXIMUM  (UINT64_MAX - 10)

/* Maximum stream ID buffer length */
#define MAXSTREAMID 64

/* A regex pattern to match legacy stream IDs for miniSEED using SEED codes
   of the form:  NN_SSSSS_LL_CCC/MSEED */
#define LEGACY_MSEED_STREAMID_PATTERN "^[0-9A-Z]{1,2}_[0-9A-Z]{1,5}_[0-9A-Z]{0,2}_[0-9A-Z]{3}/MSEED$"

/* Macros for updating different patterns */
#define RingLimit(reader, pattern) UpdatePattern (&(reader)->limit, &(reader)->limit_data, pattern, "ring limit")
#define RingMatch(reader, pattern) UpdatePattern (&(reader)->match, &(reader)->match_data, pattern, "ring match")
#define RingReject(reader, pattern) UpdatePattern (&(reader)->reject, &(reader)->reject_data, pattern, "ring reject")

/* Ring header fields, defining the binary layout in a saved buffer. */
/* Offsets ensure alignment for each value type. */
#define pRBV3_SIGNATURE(ring)      ((char *)(ring))
#define pRBV3_VERSION(ring)        ((uint16_t *)((uint8_t*)ring + 4))
#define pRBV3_RINGSIZE(ring)       ((uint64_t *)((uint8_t*)ring + 8))
#define pRBV3_PKTSIZE(ring)        ((uint32_t *)((uint8_t*)ring + 16))
#define pRBV3_MAXPACKETS(ring)     ((uint64_t *)((uint8_t*)ring + 24))
#define pRBV3_MAXOFFSET(ring)      ((int64_t *)((uint8_t*)ring + 32))
#define pRBV3_HEADERSIZE(ring)     ((uint32_t *)((uint8_t*)ring + 40))
#define pRBV3_EARLIESTOFFSET(ring) ((int64_t *)((uint8_t*)ring + 48))
#define pRBV3_LATESTOFFSET(ring)   ((int64_t *)((uint8_t*)ring + 56))
#define RBV3_HEADERSIZE            64

/* Ring packet header structure, packet payload follows header in the ring.
 * RW tagged values are set when packets are added to the ring.
 * This structure is designed for 8-byte alignment of a sequence
 * of these structures in the buffer. */
typedef struct __attribute__ ((packed)) RingPacket
{
  int64_t offset;             /* RW: Offset in ring */
  uint64_t pktid;             /* RW: Packet ID assigned if RINGID_NONE */
  nstime_t pkttime;           /* RW: Packet creation time */
  nstime_t datastart;         /* Packet data start time */
  nstime_t dataend;           /* Packet data end time */
  int64_t nextinstream;       /* RW: Offset of next packet in stream, -1 if none */
  uint32_t datasize;          /* Packet data size in bytes */
  uint32_t unused;            /* Unused, for alignment */
  char streamid[MAXSTREAMID]; /* Packet stream ID, NULL terminated */
} RingPacket;

/* Ring stream structure used for the stream index */
typedef struct __attribute__ ((packed)) RingStream
{
  nstime_t    earliestdstime;/* Earliest packet data start time */
  nstime_t    earliestdetime;/* Earliest packet data end time */
  nstime_t    earliestptime; /* Earliest packet creation time */
  uint64_t    earliestid;    /* ID of earliest packet */
  int64_t     earliestoffset;/* Offset of earliest packet */
  nstime_t    latestdstime;  /* Latest packet data start time */
  nstime_t    latestdetime;  /* Latest packet data end time */
  nstime_t    latestptime;   /* Latest packet creation time */
  uint64_t    latestid;      /* ID of latest packet */
  int64_t     latestoffset;  /* Offset of latest packet */
  char        streamid[MAXSTREAMID]; /* Packet stream ID */
} RingStream;

/* Ring reader parameters */
typedef struct RingReader
{
  _Atomic int64_t pktoffset;     /* Current packet offset in ring */
  _Atomic uint64_t pktid;        /* Current packet ID */
  _Atomic nstime_t pkttime;      /* Current packet creation time */
  pcre2_code *limit;             /* Compiled limit expression */
  pcre2_match_data *limit_data;  /* Match data results */
  pcre2_code *match;             /* Compiled match expression */
  pcre2_match_data *match_data;  /* Match data results */
  pcre2_code *reject;            /* Compiled reject expression */
  pcre2_match_data *reject_data; /* Match data results */
} RingReader;

extern int RingInitialize (char *ringfilename, char *streamfilename, int *ringfd);
extern int RingShutdown (int ringfd, char *streamfilename);
extern int RingWrite (RingPacket *packet, char *packetdata, uint32_t datasize);
extern uint64_t RingReadPacket (int64_t offset, RingPacket *packet, char *packetdata);
extern uint64_t RingRead (RingReader *reader, uint64_t reqid,
                          RingPacket *packet, char *packetdata);
extern uint64_t RingReadNext (RingReader *reader, RingPacket *packet, char *packetdata);
extern uint64_t RingPosition (RingReader *reader, uint64_t pktid, nstime_t pkttime);
extern uint64_t RingAfter (RingReader *reader, nstime_t reftime, int whence);
extern uint64_t RingAfterRev (RingReader *reader, nstime_t reftime, uint64_t pktlimit, int whence);
extern int UpdatePattern (pcre2_code **code, pcre2_match_data **data,
                          const char *pattern, const char *description);
extern Stack *GetStreamsStack (RingReader *reader);

#ifdef __cplusplus
}
#endif

#endif /* RING_H */

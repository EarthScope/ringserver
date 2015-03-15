/**************************************************************************
 * ring.h
 *
 * Declarations for fundamental ring routines and data structures.
 *
 * Modified: 2015.074
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
#include <pcre.h>
#include "rbtree.h"

/* Static ring parameters */
#define RING_SIGNATURE "RING"
#define RING_VERSION  1

/* Ring relative positioning values */
#define RINGCURRENT  -1
#define RINGEARLIEST -2
#define RINGLATEST   -3
#define RINGNEXT     -4

/* Define a maximium stream ID string length */
#define MAXSTREAMID 60

/* Ring parameters, stored at the beginning of the packet buffer file */
typedef struct RingParams_s
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
  hptime_t  earliestptime;    /* Earliest packet creation time */
  hptime_t  earliestdstime;   /* Earliest packet data start time */
  hptime_t  earliestdetime;   /* Earliest packet data end time */
  int64_t   earliestoffset;   /* Earliest packet offset in bytes */
  int64_t   latestid;         /* Latest packet ID */
  hptime_t  latestptime;      /* Latest packet creation time */
  hptime_t  latestdstime;     /* Latest packet data start time */
  hptime_t  latestdetime;     /* Latest packet data end time */
  int64_t   latestoffset;     /* Latest packet offset in bytes */
  hptime_t  ringstart;        /* Ring initialization time */
  double    txpacketrate;     /* Transmission packet rate in Hz */
  double    txbyterate;       /* Transmission byte rate in Hz */
  double    rxpacketrate;     /* Reception packet rate in Hz */
  double    rxbyterate;       /* Reception byte rate in Hz */
  char     *data;             /* Pointer to start of data buffer */
} RingParams;

/* Ring packet header structure, data follows header in the ring */
/* RW tagged values are set when packets are added to the ring */
typedef struct RingPacket_s
{
  int64_t   pktid;           /* RW: Packet ID */
  int64_t   offset;          /* RW: Offset in ring */
  hptime_t  pkttime;         /* RW: Packet creation time */
  int64_t   nextinstream;    /* RW: ID of next packet in stream, 0 if none */
  char      streamid[MAXSTREAMID]; /* Packet stream ID, NULL terminated */
  hptime_t  datastart;       /* Packet data start time */
  hptime_t  dataend;         /* Packet data end time */
  uint32_t  datasize;        /* Packet data size in bytes */
} RingPacket;

/* Ring stream structure used for the stream index */
typedef struct RingStream_s
{
  char        streamid[MAXSTREAMID]; /* Packet stream ID */
  hptime_t    earliestdstime;/* Earliest packet data start time */
  hptime_t    earliestdetime;/* Earliest packet data end time */
  hptime_t    earliestptime; /* Earliest packet creation time */
  int64_t     earliestid;    /* ID of earliest packet */
  hptime_t    latestdstime;  /* Latest packet data start time */
  hptime_t    latestdetime;  /* Latest packet data end time */
  hptime_t    latestptime;   /* Latest packet creation time */
  int64_t     latestid;      /* ID of latest packet */
} RingStream;

/* Ring reader parameters */
typedef struct RingReader_s
{
  RingParams *ringparams;    /* Ring parameters for this reader */
  int64_t     pktid;         /* Current packet ID position in ring */
  hptime_t    pkttime;       /* Current packet creation time */
  hptime_t    datastart;     /* Current packet data start time */
  hptime_t    dataend;       /* Current packet data end time */
  pcre       *limit;         /* Compiled limit expression */
  pcre_extra *limit_extra;   /* Limit expression extra study information */
  pcre       *match;         /* Compiled match expression */
  pcre_extra *match_extra;   /* Match expression extra study information */
  pcre       *reject;        /* Compiled reject expression */
  pcre_extra *reject_extra;  /* Reject expression extra study information */
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
extern int64_t RingPosition (RingReader *reader, int64_t pktid, hptime_t pkttime);
extern int64_t RingAfter (RingReader *reader, hptime_t reftime, int whence);
extern int64_t RingAfterRev (RingReader *reader, hptime_t reftime, int64_t pktlimit, int whence);
extern int RingLimit (RingReader *reader, char *pattern);
extern int RingMatch (RingReader *reader, char *pattern);
extern int RingReject (RingReader *reader, char *pattern);
extern Stack* GetStreamsStack (RingParams *ringparams, RingReader *reader);


#ifdef __cplusplus
}
#endif

#endif /* RING_H */

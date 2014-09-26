/**************************************************************************
 * slclient.h
 *
 * Modified: 2014.269
 **************************************************************************/

#ifndef SLCLIENT_H
#define SLCLIENT_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include "rbtree.h"

#define SLCAPABILITIES "SLPROTO:3.1 CAP EXTREPLY NSWILDCARD BATCH"
#define SLSERVERVER "SeedLink v3.1 (" VERSION " Ring Server SeedLink) :: " SLCAPABILITIES

#define SLRECSIZE           512      /* Mini-SEED record size */
#define SLHEADSIZE          8        /* SeedLink header size */
#define SELSIZE             8        /* Maximum selector size */
#define SIGNATURE           "SL"     /* SeedLink header signature */
#define INFOSIGNATURE       "SLINFO" /* SeedLink INFO packet signature */

#define SLMAXREGEXLEN       1048576  /* Maximum length of match/reject regex pattern */
#define SLMAXSELECTLEN      2048     /* Maximum length of per-station/global selector buffer */

#define SLINFO_ID           1
#define SLINFO_CAPABILITIES 2
#define SLINFO_STATIONS     3
#define SLINFO_STREAMS      4
#define SLINFO_GAPS         5
#define SLINFO_CONNECTIONS  6
#define SLINFO_ALL          7

/* Structure to hold SeedLink specific parameters */
typedef struct SLInfo_s {
  int         extreply;     /* Extended messages should be included in reply */
  int         dialup;       /* Connection is in dialup/fetch mode */
  int         batch;        /* Connection is in batch mode */
  int         terminfo;     /* Terminating INFO packet flag */
  int64_t     startid;      /* Starting packet ID */
  char       *selectors;    /* List of SeedLink selectors */
  int         stationcount; /* Number of stations requested with STATION */
  RBTree     *stations;     /* Binary tree of stations requested */
} SLInfo;

/* The StaKey and StaNode structures form the key and data elements
 * of a balanced tree that is used to store station level parameters.
 */

/* Structure used as the key for B-tree of stations (SLStaNode) */
typedef struct SLStaKey_s {
  char net[10];
  char sta[10];
} SLStaKey;

/* Structure used as the data for B-tree of stations */
typedef struct SLStaNode_s {
  hptime_t  starttime;       /* Requested start time for NET_STA */
  hptime_t  endtime;         /* Requested end time for NET_STA */
  int64_t   packetid;        /* Requested packet ID */
  hptime_t  datastart;       /* Data start time of requested packet */
  char     *selectors;       /* List of SeedLink selectors for NET_STA */
} SLStaNode;

/* Structure used as the data for B-tree of network-stations */
typedef struct SLNetStaNode_s {
  char      net[10];         /* Network code */
  char      sta[10];         /* Station code */
  hptime_t  earliestdstime;  /* Data start time of earliest packet for NET_STA */
  int64_t   earliestid;      /* Earliest ID for NET_STA */
  hptime_t  latestdstime;    /* Data start time of latest packet for NET_STA */
  int64_t   latestid;        /* Latest ID for NET_STA */
  Stack    *streams;         /* Stack of associated streams */
} SLNetStaNode;


extern void *SL_ClientThread (void *arg);

#ifdef __cplusplus
}
#endif

#endif /* SLCLIENT_H */

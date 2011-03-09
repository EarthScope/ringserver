/**************************************************************************
 * clients.h
 *
 * Modified: 2010.070
 **************************************************************************/

#ifndef CLIENTS_H
#define CLIENTS_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "ring.h"
#include "rbtree.h"
#include "dsarchive.h"

/* Define client types */
#define DATALINK_CLIENT     1
#define SEEDLINK_CLIENT     2

/* Connection information for client threads */
typedef struct ClientInfo_s {
  int         socket;       /* Socket descriptor */
  int         socketerr;    /* Socket error flag */
  char       *sendbuf;      /* Client specific send buffer */
  int         sendbuflen;   /* Length of send buffer */
  char       *recvbuf;      /* Client specific receive buffer */
  int         recvbuflen;   /* Length of receive buffer */
  struct sockaddr *addr;    /* client socket structure */
  socklen_t   addrlen;      /* Length of client socket structure */
  char        ipstr[100];   /* Remote host IP address */
  char        portstr[32];  /* Remote host port */
  char        hostname[200];/* Remote hostname */
  char        clientid[100];/* Client identifier string */
  uint8_t     type;         /* Client type: DATALINK_CLIENT, SEEDLINK_CLIENT */
  uint8_t     writeperm;    /* Write permission flag */
  float       timewinlimit; /* Time window ring search limit in percent */
  RingParams *ringparams;   /* Ring buffer parameters */
  RingReader *reader;       /* Ring reader parameters */
  hptime_t    conntime;     /* Client connect time */
  char       *limitstr;     /* Regular expression string to limit streams */
  char       *matchstr;     /* Regular expression string to match streams */
  char       *rejectstr;    /* Regular expression string to reject streams */
  int64_t     lastid;       /* Last packet ID sent to client */
  hptime_t    starttime;    /* Requested start time */
  hptime_t    endtime;      /* Requested end time */
  DataStream *mswrite;      /* Mini-SEED data write parameters */
  RBTree     *streams;      /* Tracking of streams transferred */
  pthread_mutex_t streams_lock; /* Mutex lock for streams tree */
  int         streamscount; /* Count of streams in tree */
  int         percentlag;   /* Percent lag of client in ring buffer */
  hptime_t    lastxchange;  /* Time of last data transmission or reception */
  uint64_t    txpackets[2]; /* Track total number of packets transmitted to client */
  double      txpacketrate; /* Track rate of packet trasmission */
  uint64_t    txbytes[2];   /* Track total number of data bytes transmitted */
  double      txbyterate;   /* Track rate of data byte trasmission */
  uint64_t    rxpackets[2]; /* Track total number of packets received from client */
  double      rxpacketrate; /* Track rate of packet reception */
  uint64_t    rxbytes[2];   /* Track total number of data bytes received */
  double      rxbyterate;   /* Track rate of data byte reception */
  hptime_t    ratetime;     /* Time stamp for TX and RX rate calculations */
  void       *extinfo;      /* Extended client info, protocol specific */
} ClientInfo;

/* Structure used as the data for B-tree of stream tracking */
typedef struct StreamNode_s {
  char      streamid[MAXSTREAMID]; /* Stream ID */
  uint64_t  txpackets;      /* Total packets transmitted */
  uint64_t  txbytes;        /* Total bytes transmitted */
  uint64_t  rxpackets;      /* Total packets received */
  uint64_t  rxbytes;        /* Total bytes received */
  uint8_t   endtimereached; /* End time reached, for window requests */
} StreamNode;


extern int  RecvData (int socket, char *buffer, size_t buflen,
		      const char *ident);

extern StreamNode *GetStreamNode (RBTree *tree, pthread_mutex_t *plock,
				  char *streamid, int *new);

extern int  AddToString (char **string, char *source, char *delim,
			 int where, int maxlen);


#ifdef __cplusplus
}
#endif

#endif /* CLIENTS_H */

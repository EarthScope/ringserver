/**************************************************************************
 * clients.h
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

/* Client types */
#define CLIENT_UNDETERMINED 0
#define CLIENT_DATALINK     1
#define CLIENT_SEEDLINK     2
#define CLIENT_HTTP         3

/* Client states */
#define STATE_COMMAND       1  /* Initial, base, command state */
#define STATE_STATION       2  /* SeedLink STATION negotiation */
#define STATE_RINGCONFIG    3  /* SeedLink ring configuration */
#define STATE_STREAM        4  /* Data streaming */

/* Connection information for client threads */
typedef struct ClientInfo_s {
  int         socket;       /* Socket descriptor */
  int         socketerr;    /* Socket error flag */
  char       *sendbuf;      /* Client specific send buffer */
  int         sendbuflen;   /* Length of send buffer */
  char       *recvbuf;      /* Client specific receive buffer */
  int         recvbuflen;   /* Length of receive buffer */
  RingPacket  packet;       /* Client specific ring packet header */
  char       *packetdata;   /* Client specific packet buffer, size of RingParams.pktsize */
  struct sockaddr *addr;    /* client socket structure */
  socklen_t   addrlen;      /* Length of client socket structure */
  char        ipstr[100];   /* Remote host IP address */
  char        portstr[32];  /* Remote host port */
  char        hostname[200];/* Remote hostname */
  char        clientid[100];/* Client identifier string */
  uint8_t     state;        /* Client state flag */
  uint8_t     type;         /* Client type flag */
  uint8_t     protocols;    /* Procotol flags for this client */
  uint8_t     websocket;    /* Flag identifying websocket connection */
  union {
    uint32_t one;
    uint8_t four[4];
  } wsmask;                 /* Masking key for WebSocket message */
  size_t      wsmaskidx;    /* Index for unmasking WebSocket message */
  uint8_t     writeperm;    /* Write permission flag */
  uint8_t     trusted;      /* Trusted client flag */
  float       timewinlimit; /* Time window ring search limit in percent */
  RingParams *ringparams;   /* Ring buffer parameters */
  RingReader *reader;       /* Ring reader parameters */
  hptime_t    conntime;     /* Client connect time */
  char       *limitstr;     /* Regular expression string to limit streams */
  char       *matchstr;     /* Regular expression string to match streams */
  char       *rejectstr;    /* Regular expression string to reject streams */
  char       *httpheaders;  /* Fixed headers to add to HTTP responses */
  int64_t     lastid;       /* Last packet ID sent to client */
  hptime_t    starttime;    /* Requested start time */
  hptime_t    endtime;      /* Requested end time */
  DataStream *mswrite;      /* miniSEED data write parameters */
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

extern void *ClientThread (void *arg);

extern int SendData (ClientInfo *cinfo, void *buffer, size_t buflen);

extern int SendDataMB (ClientInfo *cinfo, void *buffer[], size_t buflen[], int bufcount);

extern int RecvCmd (ClientInfo *cinfo);

extern int RecvData (ClientInfo *cinfo, char *buffer, size_t buflen);

extern int RecvLine (ClientInfo *cinfo);

extern int GenProtocolString (uint8_t protocols, char *protocolstr, size_t maxlength);

extern StreamNode *GetStreamNode (RBTree *tree, pthread_mutex_t *plock,
				  char *streamid, int *new);

extern int  AddToString (char **string, char *source, char *delim,
			 int where, int maxlen);

#ifdef __cplusplus
}
#endif

#endif /* CLIENTS_H */

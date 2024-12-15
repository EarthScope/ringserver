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
 * Copyright (C) 2024:
 * @author Chad Trabant, EarthScope Data Services
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
#include "ringserver.h"
#include "dsarchive.h"

/* Client types */
typedef enum
{
  CLIENT_UNDETERMINED,
  CLIENT_DATALINK,
  CLIENT_SEEDLINK,
  CLIENT_HTTP
} ClientType;

/* Client states */
typedef enum
{
  STATE_COMMAND,    /* Initial command state */
  STATE_STATION,    /* SeedLink STATION negotiation */
  STATE_RINGCONFIG, /* SeedLink ring configuration */
  STATE_STREAM      /* Data streaming */
} ClientState;

/* Connection information for client threads */
typedef struct ClientInfo
{
  int         socket;       /* Socket descriptor */
  uint8_t     tls;          /* Flag identifying TLS connection */
  void       *tlsctx;       /* TLS context */
  int         socketerr;    /* Socket error flag, -1: error, -2: orderly shutdown */
  char       *sendbuf;      /* Client specific send buffer */
  size_t      sendbufsize;  /* Length of send buffer in bytes */
  char       *recvbuf;      /* Client specific receive buffer */
  size_t      recvbufsize;  /* Length of receive buffer in bytes */
  size_t      recvlength;   /* Length of data in recvbuf */
  size_t      recvconsumed; /* Bytes of recvbuf that have been consumed */
  char        dlcommand[UINT8_MAX + 1]; /* DataLink command buffer */
  RingPacket  packet;       /* Client specific ring packet header */
  struct sockaddr *addr;    /* client socket structure */
  socklen_t   addrlen;      /* Length of client socket structure */
  char        ipstr[100];   /* Client host IP address */
  char        portstr[NI_MAXSERV]; /* Client port */
  char        hostname[200];/* Client hostname, or IP is unresolvable */
  char        clientid[100];/* Client identifier string */
  char        serverport[NI_MAXSERV]; /* Server port */
  ClientState state;        /* Client state flag */
  ClientType  type;         /* Client type flag */
  ListenProtocols protocols;/* Protocol flags for this client */
  uint8_t     websocket;    /* Flag identifying websocket connection */
  union {
    uint32_t one;
    uint8_t four[4];
  } wsmask;                 /* Masking key for WebSocket message */
  size_t      wsmaskidx;    /* Index for unmasking WebSocket message */
  uint64_t    wspayload;    /* Length of WebSocket payload expected */
  uint8_t     writeperm;    /* Write permission flag */
  uint8_t     trusted;      /* Trusted client flag */
  float       timewinlimit; /* Time window ring search limit in percent */
  RingParams *ringparams;   /* Ring buffer parameters */
  RingReader *reader;       /* Ring reader parameters */
  nstime_t    conntime;     /* Client connect time */
  char       *limitstr;     /* Regular expression string to limit streams */
  char       *matchstr;     /* Regular expression string to match streams */
  char       *rejectstr;    /* Regular expression string to reject streams */
  char       *httpheaders;  /* Fixed headers to add to HTTP responses */
  uint64_t    lastid;       /* Last packet ID sent to client */
  nstime_t    starttime;    /* Requested start time */
  nstime_t    endtime;      /* Requested end time */
  DataStream *mswrite;      /* miniSEED data write parameters */
  RBTree     *streams;      /* Tracking of streams transferred */
  pthread_mutex_t streams_lock; /* Mutex lock for streams tree */
  int         streamscount; /* Count of streams in tree */
  int         percentlag;   /* Percent lag of client in ring buffer */
  nstime_t    lastxchange;  /* Time of last data transmission or reception */
  uint64_t    txpackets[2]; /* Track total number of packets transmitted to client */
  double      txpacketrate; /* Track rate of packet transmission */
  uint64_t    txbytes[2];   /* Track total number of data bytes transmitted */
  double      txbyterate;   /* Track rate of data byte transmission */
  uint64_t    rxpackets[2]; /* Track total number of packets received from client */
  double      rxpacketrate; /* Track rate of packet reception */
  uint64_t    rxbytes[2];   /* Track total number of data bytes received */
  double      rxbyterate;   /* Track rate of data byte reception */
  nstime_t    ratetime;     /* Time stamp for TX and RX rate calculations */
  void       *extinfo;      /* Extended client info, protocol specific */
} ClientInfo;

/* Structure used as the data for B-tree of stream tracking */
typedef struct StreamNode
{
  char      streamid[MAXSTREAMID]; /* Stream ID */
  uint64_t  txpackets;      /* Total packets transmitted */
  uint64_t  txbytes;        /* Total bytes transmitted */
  uint64_t  rxpackets;      /* Total packets received */
  uint64_t  rxbytes;        /* Total bytes received */
  uint8_t   endtimereached; /* End time reached, for window requests */
} StreamNode;

extern void *ClientThread (void *arg);

extern int SendData (ClientInfo *cinfo, void *buffer, size_t buflen, int no_wsframe);

extern int SendDataMB (ClientInfo *cinfo, void *buffer[], size_t buflen[],
                       int bufcount, int no_wsframe);

extern int RecvData (ClientInfo *cinfo, void *buffer, size_t requested, int fulfill);

extern int RecvDLCommand (ClientInfo *cinfo);

extern int RecvLine (ClientInfo *cinfo);

extern int PollSocket (int socket, int readability, int writability, int timeout_ms);

extern StreamNode *GetStreamNode (RBTree *tree, pthread_mutex_t *plock,
                                  char *streamid, int *new);

extern int AddToString (char **string, char *source, char *delim,
                        size_t where, size_t maxlen);

#ifdef __cplusplus
}
#endif

#endif /* CLIENTS_H */

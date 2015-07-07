/**************************************************************************
 * ringserver.h
 *
 * Modified: 2010.016
 **************************************************************************/

#ifndef RINGSERVER_H
#define RINGSERVER_H 1

#include <pthread.h>
#include "clients.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PACKAGE   "ringserver"
#define VERSION   "2015.188"

/* Thread data flags */ 
#define TDF_SPAWNING    (1<<0)          /* Thread is now spawning   */
#define TDF_ACTIVE      (1<<1)          /* If set, thread is active */
#define TDF_CLOSE       (1<<2)          /* If set, thread closes    */
#define TDF_CLOSING     (1<<3)          /* Thread in close process  */
#define TDF_CLOSED      (1<<4)          /* Thread is closed         */

/* Connection mode flags */
#define SEEDLINK_MODE   1
#define DATALINK_MODE   2

/* Thread data associated with most threads */
struct thread_data {
  pthread_mutex_t td_lock;
  pthread_t       td_id;
  int             td_flags;
  int             td_done;
  void           *td_prvtptr;
};

/* Server thread types */
#define DATALINK_LISTEN_THREAD 1
#define SEEDLINK_LISTEN_THREAD 2
#define MSEEDSCAN_THREAD       3

/* Doubly-linkable structure to list server threads */
struct sthread {
  struct thread_data *td;
  unsigned int    type;
  void           *params;
  struct sthread *prev;
  struct sthread *next;
};

/* Doubly-linkable structure to list client threads */
struct cthread {
  struct thread_data *td;
  struct cthread *prev;
  struct cthread *next;
};

/* A structure for server listening parameters */
typedef struct ListenPortParams_s
{
  char portstr[11];      /* Port number to listen on as string */
  int  socket;           /* Socket descriptor or -1 when not connected */
} ListenPortParams;

/* Global variables declared in ringserver.c */
extern pthread_mutex_t sthreads_lock;
extern struct sthread *sthreads;
extern pthread_mutex_t cthreads_lock;
extern struct cthread *cthreads;
extern char *serverid;
extern hptime_t serverstarttime;
extern int clientcount;
extern int resolvehosts;
extern int shutdownsig;

#ifdef __cplusplus
}
#endif

#endif /* RINGSERVER_H */

/**************************************************************************
 * dlclient.h
 *
 * Modified: 2013.192
 **************************************************************************/

#ifndef DLCLIENT_H
#define DLCLIENT_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include "rbtree.h"
#include "ringserver.h"

/* DataLink server capability flags */
#define DLCAPFLAGS "DLPROTO:1.0"

#define DLMAXREGEXLEN       1048576  /* Maximum regex pattern size */

extern void *DL_ClientThread (void *arg);

#ifdef __cplusplus
}
#endif

#endif /* DLCLIENT_H */

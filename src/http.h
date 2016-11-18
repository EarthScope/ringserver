/**************************************************************************
 * http.h
 *
 * Modified: 2016.312
 **************************************************************************/

#ifndef HTTP_H
#define HTTP_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "ring.h"
#include "clients.h"

/* Extract bit range and shift to start */
#define EXTRACTBITRANGE(VALUE, STARTBIT, LENGTH) ((VALUE & (((1 << LENGTH) - 1) << STARTBIT)) >> STARTBIT)

extern int HandleHTTP (char *recvbuffer, ClientInfo *cinfo);
extern int RecvWSFrame (ClientInfo *cinfo, uint64_t *length, uint32_t *mask);

#ifdef __cplusplus
}
#endif

#endif /* HTTP_H */

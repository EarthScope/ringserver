/**************************************************************************
 * generic.h
 *
 * Modified: 2008.290
 **************************************************************************/

#ifndef GENERIC_H
#define GENERIC_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <libmseed.h>

/* Key for B-trees */
typedef int64_t Key;

extern int      SplitStreamID (char *streamid,
			       char *net, char *sta, char *loc, char *chan,
			       char *type);
extern hptime_t HPnow (void);
extern int64_t  FVNhash64 (char *str);
extern int      KeyCompare (const void *a, const void *b);
extern int      IsAllDigits (char *string);

#ifdef __cplusplus
}
#endif

#endif /* GENERIC_H */

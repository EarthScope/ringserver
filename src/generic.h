/**************************************************************************
 * generic.h
 *
 * Modified: 2016.356
 **************************************************************************/

#ifndef GENERIC_H
#define GENERIC_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <libmseed.h>

/* Key for B-trees */
typedef int64_t Key;

extern int      SplitStreamID (char *streamid, char delim, int maxlength,
                               char *id1, char *id2, char *id3, char *id4, char *id5, char *id6,
                               char *type);
extern hptime_t HPnow (void);
extern int64_t  FVNhash64 (char *str);
extern int      KeyCompare (const void *a, const void *b);
extern int      IsAllDigits (char *string);

#ifdef __cplusplus
}
#endif

#endif /* GENERIC_H */

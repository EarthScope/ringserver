/**************************************************************************
 * generic.h
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
 * Copyright (C) 2026:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

#ifndef GENERIC_H
#define GENERIC_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <libmseed.h>

/* Key for B-trees */
typedef uint64_t Key;

extern int SplitStreamID (const char *streamid, char delim, int maxlength,
                          char *id1, char *id2, char *id3, char *id4, char *id5, char *id6,
                          char *type);
extern nstime_t NSnow (void);
extern uint64_t FNVhash64 (const char *str);
extern int KeyCompare (const void *a, const void *b);
extern int IsAllDigits (const char *string);
extern int HumanSizeString (uint64_t bytes, char *sizestring, size_t sizestringlen);
extern int GlobMatch (const char *string, const char *pattern);
extern int FinalizeMemStream (FILE *stream, char **buf, size_t *len);
extern int AllocPrintf (char **strp, const char *fmt, ...)
    __attribute__ ((format (printf, 2, 3)));

#ifdef __cplusplus
}
#endif

#endif /* GENERIC_H */

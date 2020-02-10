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
 * Copyright (C) 2020:
 * @author Chad Trabant, IRIS Data Management Center
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

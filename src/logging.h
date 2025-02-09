/**************************************************************************
 * logging.h
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

#ifndef LOGGING_H
#define LOGGING_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>

#include "clients.h"

#if defined(__GNUC__) || defined(__clang__)
__attribute__((__format__ (__printf__, 2, 3)))
#endif
extern int lprintf (int level, char *fmt, ...);
extern void lprint (char *message);
extern void lprint_wrapper (const char *message);

extern int WriteTLog (ClientInfo *cinfo, int reset);
extern int CalcTLogInterval (time_t reftime);


#ifdef __cplusplus
}
#endif

#endif /* LOGGING_H */

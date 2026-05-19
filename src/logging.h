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
 * See the License for the specific language governing permissions andd
 * limitations under the License.
 *
 * Copyright (C) 2026:
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
extern void lprint (const char *message);

/* Write per-client transfer counters to the TX and RX usage log files for the
 * current interval window.  Reads the pre-rendered filenames from
 * config.usagelog.{txlog,rxlog}_filename under config.usagelog.write_lock;
 * does not take config_rwlock.  Pass reset=1 to zero stream counters after
 * writing (client disconnect path), reset=0 to leave them intact (periodic
 * watchdog writes). */
extern int WriteTransferLog (ClientInfo *cinfo, int reset);

/* Append a single JSON Lines access log entry to the access log file for the
 * current interval window.  Reads the pre-rendered filename from
 * config.usagelog.accesslog_filename under config.usagelog.write_lock;
 * does not take config_rwlock.  Returns 0 if access logging is not enabled. */
extern int WriteAccessLog (ClientInfo *cinfo, const char *event,
                           const char *command, const char *detail,
                           const char *match, const char *reject);

/* Calculate a normalized interval time window and render the three cached log
 * filenames (txlog, rxlog, accesslog) into config.usagelog.  Takes
 * config_rwlock(wr) then usagelog.write_lock internally. */
extern int CalcUsageLogInterval (time_t reftime);

/* Same as CalcUsageLogInterval() but the caller must already hold
 * config.config_rwlock as a writer.  Intended for use inside an existing
 * wrlock window (e.g., immediately after a config reload) so that the cached
 * log filenames are updated atomically with the underlying config values. */
extern int CalcUsageLogInterval_locked (time_t reftime);

#ifdef __cplusplus
}
#endif

#endif /* LOGGING_H */

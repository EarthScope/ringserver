/***************************************************************************
 * mseedscan.h
 *
 * miniSEED scanning declarations.
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
 ***************************************************************************/

#ifndef MSEEDSCAN_H
#define MSEEDSCAN_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <pcre.h>

#include "rbtree.h"

/* Maximum filename length */
#define MSSCAN_MAXFILENAME 512

typedef struct MSScanInfo_s {
  /* Configuration parameters */
  char  dirname[512];     /* Base directory to scan */
  int   maxrecur;         /* Maximum level of directory recursion */
  int   nextnew;          /* Only read next new data in existing files */
  int   iostats;          /* Ouput IO stats every iostats seconds */
  int   budlatency;       /* BUD file latency check, value in days */
  int   scansleep;        /* Sleep between scans interval in seconds */
  int   scansleep0;       /* Sleep between scans interval when no records found */
  int   idledelay;        /* Check idle files every idledelay scans */
  int   idlesec;          /* Files are idle if not modified for idlesec */
  int   quietsec;         /* Files are quiet if not modified for quietsec */
  int   throttlensec;     /* Nanoseconds to sleep after reading each record */
  int   filemaxrecs;      /* Maximum records to read from each file per scan */
  int   stateint;         /* State saving interval in seconds */
  char  statefile[512];   /* State file to save/restore time stamps (abs path) */
  char  matchstr[512];    /* Filename match expression */
  char  rejectstr[512];   /* Filename reject expression */
  pcre *fnmatch;          /* Compiled match expression */
  pcre_extra *fnmatch_extra;  /* Match expression extra study information */
  pcre *fnreject;         /* Compiled reject expression */
  pcre_extra *fnreject_extra; /* Reject expression extra study information */

  /* Internal tracking parameters */
  uint32_t readbuffersize;/* Size of file read buffer */
  char    *readbuffer;    /* File read buffer */
  RingParams *ringparams; /* Ring buffer parameters */
  MSRecord *msr;          /* Parsed miniSEED record */
  RBTree  *filetree;      /* Working list of scanned files in a tree */
  int      accesserr;     /* Flag to indicate directory access errors */
  int      recurlevel;    /* Track recursion level */

  uint64_t rxpackets[2];  /* Track total number of packets sent to ring */
  double   rxpacketrate;  /* Track rate of packet reading */
  uint64_t rxbytes[2];    /* Track total number of data bytes read */
  double   rxbyterate;    /* Track rate of data byte reading */
  double   scantime;      /* Duration of last scan in seconds */
  hptime_t ratetime;      /* Time stamp for RX rate calculations */

  int scanfileschecked;   /* Track files checked per scan */
  int scanfilesread;      /* Track files read per scan */
  int scanrecordsread;    /* Track records read per scan */
  int scanrecordswritten; /* Track records written per scan */
} MSScanInfo;

extern void *MS_ScanThread (void *arg);

#ifdef __cplusplus
}
#endif

#endif /* MSEEDSCAN_H */

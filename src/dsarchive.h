/***************************************************************************
 * dsarchive.h
 *
 * Routines to archive miniSEED data records, specialized for ringserver.
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

#ifndef DSARCHIVE_H
#define DSARCHIVE_H

#include <stdio.h>
#include <time.h>

#include <libmseed.h>

/* Maximum file name length for output files */
#define MAX_FILENAME_LEN 400

/* Define pre-formatted archive layouts */
#define BUDLAYOUT   "%n/%s/%s.%n.%l.%c.%Y.%j"
#define CSSLAYOUT   "%Y/%j/%s.%c.%Y:%j:#H:#M:#S"
#define CHANLAYOUT  "%n.%s.%l.%c"
#define QCHANLAYOUT "%n.%s.%l.%c.%q"
#define CDAYLAYOUT  "%n.%s.%l.%c.%Y:%j:#H:#M:#S"
#define SDAYLAYOUT  "%n.%s.%Y:%j"
#define HSDAYLAYOUT "%h/%n.%s.%Y:%j"

typedef struct DataStreamGroup_s
{
  char   *defkey;
  int     filed;
  time_t  modtime;
  char    filename[MAX_FILENAME_LEN];
  char    postpath[MAX_FILENAME_LEN];
  struct  DataStreamGroup_s *next;
}
DataStreamGroup;

typedef struct DataStream_s
{
  char   *path;
  int     idletimeout;
  int     maxopenfiles;
  int     openfilecount;
  struct  DataStreamGroup_s *grouproot;
}
DataStream;


extern int ds_streamproc (DataStream *datastream, MSRecord *msr, char *postpath,
			  char *hostname);
extern int ds_closeidle (DataStream *datastream, int idletimeout, char *ident);


#endif

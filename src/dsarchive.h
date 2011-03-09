/***************************************************************************
 * dsarchive.h
 *
 * Routines to archive Mini-SEED data records, specialized for ringserver.
 *
 * Written by Chad Trabant
 *   IRIS Data Management Center
 *
 * modified: 2010.020
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

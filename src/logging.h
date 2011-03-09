/**************************************************************************
 * logging.h
 *
 * Modified: 2008.231
 **************************************************************************/

#ifndef LOGGING_H
#define LOGGING_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>

#include "clients.h"

struct TLogParams_s {
  char  *tlogbasedir;    /* Base directory for transfer log output */
  char  *tlogprefix;     /* Prefix for log files */
  int    txlog;          /* Flag to control transmission log */
  int    rxlog;          /* Flag to control reception log */
  int    tloginterval;   /* Log writing interval in seconds */
  time_t tlogstart;      /* Track actual start time of log window */
  time_t tlogstartint;   /* Normalized start time of log interval */
  time_t tlogendint;     /* Normalized end time of log interval */
};

/* Global logging parameters declared in logging.c */
extern int verbose;
extern struct TLogParams_s TLogParams;

extern int lprintf (int level, char *fmt, ...);
extern void lprint (char *message);

extern int WriteTLog (ClientInfo *cinfo, int reset);
extern int CalcIntWin (time_t reftime, int interval,
		       time_t *startint, time_t *endint);


#ifdef __cplusplus
}
#endif

#endif /* LOGGING_H */

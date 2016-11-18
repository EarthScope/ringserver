/**************************************************************************
 * logging.c
 *
 * Generic logging routines.
 *
 * Copyright 2016 Chad Trabant, IRIS Data Management Center
 *
 * This file is part of ringserver.
 *
 * ringserver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * ringserver is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ringserver. If not, see http://www.gnu.org/licenses/.
 *
 * Modified: 2016.345
 **************************************************************************/

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libmseed.h>

#include "clients.h"
#include "generic.h"
#include "logging.h"
#include "rbtree.h"
#include "ringserver.h"

/* Global logging parameters */
int verbose;

struct TLogParams_s TLogParams = {0, 0, 1, 1, 86400, 0, 0, 0};

/* Lock mutex for transmission log file writing */
static pthread_mutex_t tlogfile_lock = PTHREAD_MUTEX_INITIALIZER;

/***************************************************************************
 * lprintf:
 *
 * A generic log message handler, pre-pends a current date/time string
 * to each message.  This routine add a newline to the final output
 * message so it should not be included with the message.
 *
 * Returns the number of characters in the formatted message.
 ***************************************************************************/
int
lprintf (int level, char *fmt, ...)
{
  int rv = 0;
  char message[200];
  va_list argptr;
  struct tm *tp;
  time_t curtime;

  char *day[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  char *month[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
                   "Aug", "Sep", "Oct", "Nov", "Dec"};

  if (level <= verbose)
  {

    /* Build local time string and generate final output */
    curtime = time (NULL);
    tp = localtime (&curtime);

    va_start (argptr, fmt);
    rv = vsnprintf (message, sizeof (message), fmt, argptr);
    va_end (argptr);

    printf ("%3.3s %3.3s %2.2d %2.2d:%2.2d:%2.2d %4.4d - %s%s\n",
            day[tp->tm_wday], month[tp->tm_mon], tp->tm_mday,
            tp->tm_hour, tp->tm_min, tp->tm_sec, tp->tm_year + 1900,
            message, (rv > sizeof (message)) ? " ..." : "");

    fflush (stdout);
  }

  return rv;
} /* End of lprintf() */

/***************************************************************************
 * lprint:
 *
 * A simple gateway to lprintf(), trimming trailing newline characters
 * since lprintf() will add one.
 *
 ***************************************************************************/
void
lprint (char *message)
{
  int length;
  char *ptr;

  /* Set ptr to last character in the message string */
  length = strlen (message);
  ptr = message + (length - 1);

  /* Trim trailing newline characters */
  while (*ptr == '\n' && ptr != message)
  {
    *ptr-- = '\0';
  }

  /* Send message to lprintf() */
  lprintf (0, message);
} /* End of lprintf() */

/***************************************************************************
 * WriteTLog:
 *
 * Write transfer packet and byte counts to files in the log base
 * directory.  Separate files are written for transmission (TX) logs
 * and reception (RX) logs.  If the 'reset' flag is true reset all
 * counts to zero after logging.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
WriteTLog (ClientInfo *cinfo, int reset)
{
  uint64_t txtotalbytes = 0;
  uint64_t rxtotalbytes = 0;
  StreamNode *streamnode;
  RBNode *rbnode;
  Stack *stack;
  int rv = 0;

  hptime_t clock;
  struct tm starttm;
  struct tm endtm;

  char conntime[30];
  char currtime[30];
  char txfilename[500];
  char rxfilename[500];
  char *modestr = "";
  FILE *txfp = 0;
  FILE *rxfp = 0;

  /* If the base directory is not specified we are done */
  if (!TLogParams.tlogbasedir)
    return 0;

  /* If neither the TX or RX log is turned on we are done */
  if (!TLogParams.txlog && !TLogParams.rxlog)
    return 0;

  if (TLogParams.txlog)
  {
    /* Generate file path & name for log time interval file */
    localtime_r (&TLogParams.tlogstartint, &starttm);
    localtime_r (&TLogParams.tlogendint, &endtm);
    snprintf (txfilename, sizeof (txfilename),
              "%s/%s%stxlog-%04d%02d%02dT%02d:%02d-%04d%02d%02dT%02d:%02d",
              TLogParams.tlogbasedir,
              (TLogParams.tlogprefix) ? TLogParams.tlogprefix : "",
              (TLogParams.tlogprefix) ? "-" : "",
              starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
              starttm.tm_hour, starttm.tm_min,
              endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
              endtm.tm_hour, endtm.tm_min);

    /* Open TX log file */
    if ((txfp = fopen (txfilename, "a")) == NULL)
    {
      lprintf (0, "Error opening TX transfer log file %s: %s",
               txfilename, strerror (errno));
      return -1;
    }
  }

  if (TLogParams.rxlog)
  {
    /* Generate file path & name for log time interval file */
    localtime_r (&TLogParams.tlogstartint, &starttm);
    localtime_r (&TLogParams.tlogendint, &endtm);
    snprintf (rxfilename, sizeof (rxfilename),
              "%s/%s%srxlog-%04d%02d%02dT%02d:%02d-%04d%02d%02dT%02d:%02d",
              TLogParams.tlogbasedir,
              (TLogParams.tlogprefix) ? TLogParams.tlogprefix : "",
              (TLogParams.tlogprefix) ? "-" : "",
              starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
              starttm.tm_hour, starttm.tm_min,
              endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
              endtm.tm_hour, endtm.tm_min);

    /* Open RX log file */
    if ((rxfp = fopen (rxfilename, "a")) == NULL)
    {
      lprintf (0, "Error opening RX transfer log file %s: %s",
               rxfilename, strerror (errno));
      return -1;
    }
  }

  /* Generate pretty strings for current & connection time */
  clock = HPnow ();
  ms_hptime2mdtimestr (clock, currtime, 0);
  ms_hptime2mdtimestr (cinfo->conntime, conntime, 0);

  stack = StackCreate ();

  /* Lock transfer log mutex */
  pthread_mutex_lock (&tlogfile_lock);

  /* Seek to end of log files */
  if (txfp && fseek (txfp, 0, SEEK_END))
  {
    lprintf (0, "Error seeking to end of TX transfer log file %s: %s",
             txfilename, strerror (errno));
    rv = -1;
  }
  if (rxfp && fseek (rxfp, 0, SEEK_END))
  {
    lprintf (0, "Error seeking to end of RX transfer log file %s: %s",
             rxfilename, strerror (errno));
    rv = -1;
  }

  /* Write transfer log(s) */
  if (!rv)
  {
    if (cinfo->type == CLIENT_DATALINK)
      modestr = "DataLink";
    else if (cinfo->type == CLIENT_SEEDLINK)
      modestr = "SeedLink";
    else
      modestr = "Unknown";

    /* Print client header line */
    if (txfp)
      fprintf (txfp, "START CLIENT %s [%s] (%s|%s) @ %s (connected %s) TX\n",
               cinfo->hostname, cinfo->ipstr, modestr, cinfo->clientid,
               currtime, conntime);
    if (rxfp)
      fprintf (rxfp, "START CLIENT %s [%s] (%s|%s) @ %s (connected %s) RX\n",
               cinfo->hostname, cinfo->ipstr, modestr, cinfo->clientid,
               currtime, conntime);

    /* Lock stream tree and create list (Stack) of streams */
    pthread_mutex_lock (&(cinfo->streams_lock));
    if (cinfo->streams)
      RBBuildStack (cinfo->streams, stack);

    /* Loop through streams and output bytecounts */
    txtotalbytes = 0;
    rxtotalbytes = 0;
    while ((rbnode = (RBNode *)StackPop (stack)))
    {
      streamnode = (StreamNode *)rbnode->data;

      if (txfp && streamnode->txbytes > 0)
      {
        fprintf (txfp, "%s %" PRIu64 " %" PRIu64 "\n", streamnode->streamid,
                 streamnode->txbytes, streamnode->txpackets);

        txtotalbytes += streamnode->txbytes;

        /* Reset counts if requested */
        if (reset)
        {
          streamnode->txbytes = 0;
          streamnode->txpackets = 0;
        }
      }

      if (rxfp && streamnode->rxbytes > 0)
      {
        fprintf (rxfp, "%s %" PRIu64 " %" PRIu64 "\n", streamnode->streamid,
                 streamnode->rxbytes, streamnode->rxpackets);

        rxtotalbytes += streamnode->rxbytes;

        /* Reset counts if requested */
        if (reset)
        {
          streamnode->rxbytes = 0;
          streamnode->rxpackets = 0;
        }
      }
    }
    /* Unlock stream tree */
    pthread_mutex_unlock (&(cinfo->streams_lock));

    /* Print client footer line */
    if (txfp)
      fprintf (txfp, "END CLIENT %s [%s] total TX bytes: %" PRIu64 "\n",
               cinfo->hostname, cinfo->ipstr, txtotalbytes);
    if (rxfp)
      fprintf (rxfp, "END CLIENT %s [%s] total RX bytes: %" PRIu64 "\n",
               cinfo->hostname, cinfo->ipstr, rxtotalbytes);
  }

  /* Flush log files */
  if (txfp)
    fflush (txfp);
  if (rxfp)
    fflush (rxfp);

  /* Unlock tranfser lock mutex */
  pthread_mutex_unlock (&tlogfile_lock);

  /* Close log files */
  if (txfp && fclose (txfp))
  {
    lprintf (0, "Error closing TX transfer log file %s: %s",
             txfilename, strerror (errno));
    rv = -1;
  }
  if (rxfp && fclose (rxfp))
  {
    lprintf (0, "Error closing RX transfer log file %s: %s",
             rxfilename, strerror (errno));
    rv = -1;
  }

  StackDestroy (stack, free);

  return rv;
} /* End of WriteTLog() */

/***************************************************************************
 * CalcIntWin:
 *
 * Calculate a normalized interval time window for a given reference
 * time (usually the current time) and interval in seconds.  The
 * window is always normalized relative to the current day, intervals
 * which evenly divide days will work cleanly, other intervals will
 * probably work but might result in unexpected window calculations.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
CalcIntWin (time_t reftime, int interval, time_t *startint, time_t *endint)
{
  struct tm reftm;

  if (!localtime_r (&reftime, &reftm))
    return -1;

  /* Round down to current day */
  reftm.tm_sec = 0;
  reftm.tm_min = 0;
  reftm.tm_hour = 0;

  /* Calculate the new, rounded epoch time */
  *startint = mktime (&reftm);

  /* Add intervals until within the current interval */
  while ((*startint + interval) <= reftime)
    *startint += interval;

  /* Set end of interval window */
  *endint = *startint + interval;

  return 0;
} /* End of CalcIntWin() */

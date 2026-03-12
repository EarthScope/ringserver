/**************************************************************************
 * logging.c
 *
 * Generic logging routines.
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

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libmseed.h>

#include "clients.h"
#include "dlclient.h"
#include "generic.h"
#include "logging.h"
#include "rbtree.h"
#include "slclient.h"
#include "yyjson.h"

pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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
  struct tm tp;
  time_t curtime;

  const char *day[]   = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  const char *month[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
                         "Aug", "Sep", "Oct", "Nov", "Dec"};

  if (level <= config.verbose)
  {

    /* Build local time string and generate final output */
    curtime = time (NULL);
    localtime_r (&curtime, &tp);

    va_start (argptr, fmt);
    rv = vsnprintf (message, sizeof (message), fmt, argptr);
    va_end (argptr);

    pthread_mutex_lock (&log_mutex);
    printf ("%3.3s %3.3s %2.2d %2.2d:%2.2d:%2.2d %4.4d - %s%s\n",
            day[tp.tm_wday], month[tp.tm_mon], tp.tm_mday,
            tp.tm_hour, tp.tm_min, tp.tm_sec, tp.tm_year + 1900,
            message, (rv >= sizeof (message)) ? " ..." : "");

    fflush (stdout);
    pthread_mutex_unlock (&log_mutex);
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
lprint (const char *message)
{
  int length;

  if (!message)
    return;

  /* Determine length, trimming trailing newline characters */
  length = strlen (message);

  while (length > 0 && message[length - 1] == '\n')
    length--;

  if (length == 0)
    return;

  /* Send message to lprintf() with precision-limited format to exclude trailing newlines */
  lprintf (0, "%.*s", length, message);
} /* End of lprint() */

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

  int jsonlmode;
  int server_port;

  nstime_t clock = NSnow ();
  struct tm starttm;
  struct tm endtm;

  time_t tlog_startint = 0;

  char txfilename[512]  = {0};
  char rxfilename[512]  = {0};
  char conntime[32]     = {0};
  char currtime[32]     = {0};
  char logstarttime[32] = {0};
  char *modestr         = "";
  FILE *txfp            = NULL;
  FILE *rxfp            = NULL;

  /* Take a config reader lock */
  pthread_rwlock_rdlock (&config.config_rwlock);

  /* Transfer logging not enabled */
  if (config.tlog.mode == TLOG_NONE || config.tlog.basedir == NULL)
  {
    pthread_rwlock_unlock (&config.config_rwlock);
    return 0;
  }

  jsonlmode = (config.tlog.mode & TLOG_JSONL) ? 1 : 0;

  tlog_startint = config.tlog.startint;

  if (config.tlog.mode & TLOG_TX)
  {
    /* Generate file path & name for log time interval file */
    localtime_r (&config.tlog.startint, &starttm);
    time_t endint = config.tlog.endint;
    localtime_r (&endint, &endtm);
    snprintf (txfilename, sizeof (txfilename),
              "%s/%s%stxlog-%04d%02d%02dT%02d%02d-%04d%02d%02dT%02d%02d%s",
              config.tlog.basedir,
              (config.tlog.prefix) ? config.tlog.prefix : "",
              (config.tlog.prefix) ? "-" : "",
              starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
              starttm.tm_hour, starttm.tm_min,
              endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
              endtm.tm_hour, endtm.tm_min,
              jsonlmode ? ".jsonl" : "");
  }

  if (config.tlog.mode & TLOG_RX)
  {
    /* Generate file path & name for log time interval file */
    localtime_r (&config.tlog.startint, &starttm);
    time_t endint = config.tlog.endint;
    localtime_r (&endint, &endtm);
    snprintf (rxfilename, sizeof (rxfilename),
              "%s/%s%srxlog-%04d%02d%02dT%02d%02d-%04d%02d%02dT%02d%02d%s",
              config.tlog.basedir,
              (config.tlog.prefix) ? config.tlog.prefix : "",
              (config.tlog.prefix) ? "-" : "",
              starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
              starttm.tm_hour, starttm.tm_min,
              endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
              endtm.tm_hour, endtm.tm_min,
              jsonlmode ? ".jsonl" : "");
  }

  /* Release config reader lock */
  pthread_rwlock_unlock (&config.config_rwlock);

  /* Generate pretty strings for current & connection time */
  ms_nstime2timestr_n (clock, currtime, sizeof (currtime), ISOMONTHDAY_Z, NONE);
  ms_nstime2timestr_n (cinfo->conntime, conntime, sizeof (conntime), ISOMONTHDAY_Z, NONE);

  /* Compute log start time: later of interval start or client connect time */
  nstime_t intervalstart_ns = (nstime_t)tlog_startint * NSTMODULUS;
  nstime_t logstart         = (cinfo->conntime > intervalstart_ns) ? cinfo->conntime : intervalstart_ns;
  ms_nstime2timestr_n (logstart, logstarttime, sizeof (logstarttime), ISOMONTHDAY_Z, NONE);

  /* Convert server port from string to integer */
  server_port = atoi (cinfo->serverport);

  /* Lock transfer log file writing mutex */
  pthread_mutex_lock (&config.tlog.write_lock);

  /* Open TX log file and seek to end */
  if (txfilename[0])
  {
    if ((txfp = fopen (txfilename, "a")) == NULL)
    {
      lprintf (0, "Error opening TX transfer log file %s: %s",
               txfilename, strerror (errno));
      rv = -1;
    }
    else if (fseek (txfp, 0, SEEK_END))
    {
      lprintf (0, "Error seeking to end of TX transfer log file %s: %s",
               txfilename, strerror (errno));
      rv = -1;
    }
  }

  /* Open RX log file and seek to end */
  if (rxfilename[0])
  {
    if ((rxfp = fopen (rxfilename, "a")) == NULL)
    {
      lprintf (0, "Error opening RX transfer log file %s: %s",
               rxfilename, strerror (errno));
      rv = -1;
    }
    else if (fseek (rxfp, 0, SEEK_END))
    {
      lprintf (0, "Error seeking to end of RX transfer log file %s: %s",
               rxfilename, strerror (errno));
      rv = -1;
    }
  }

  /* Write transfer log(s) */
  if (!rv)
  {
    if (cinfo->type == CLIENT_DATALINK)
      modestr = "DataLink";
    else if (cinfo->type == CLIENT_SEEDLINK)
      modestr = "SeedLink";
    else if (cinfo->type == CLIENT_HTTP)
      modestr = "HTTP";
    else
      modestr = "Unknown";

    if (jsonlmode)
    {
      /* JSON Lines format: one JSON object per client per direction */
      char protoversion[16] = {0};

      /* Determine protocol version string */
      if (cinfo->type == CLIENT_SEEDLINK && cinfo->extinfo)
      {
        SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
        snprintf (protoversion, sizeof (protoversion), "%u.%u",
                  slinfo->proto_major, slinfo->proto_minor);
      }
      else if (cinfo->type == CLIENT_DATALINK)
      {
        snprintf (protoversion, sizeof (protoversion), "%u.%u",
                  DLPROTO_MAJOR, DLPROTO_MINOR);
      }

      /* Lock stream tree and build list (Stack) of streams */
      pthread_mutex_lock (&(cinfo->streams_lock));

      stack = StackCreate ();
      if (cinfo->streams)
        RBBuildStack (cinfo->streams, stack);

      /* Build TX JSON document */
      yyjson_mut_doc *txdoc     = NULL;
      yyjson_mut_doc *rxdoc     = NULL;
      yyjson_mut_val *txstreams = NULL;
      yyjson_mut_val *rxstreams = NULL;

      if (txfp)
      {
        txdoc = yyjson_mut_doc_new (NULL);
        if (txdoc)
        {
          yyjson_mut_val *root = yyjson_mut_obj (txdoc);
          yyjson_mut_doc_set_root (txdoc, root);

          yyjson_mut_obj_add_strcpy (txdoc, root, "log_time", currtime);
          yyjson_mut_obj_add_strcpy (txdoc, root, "log_start_time", logstarttime);
          yyjson_mut_obj_add_strcpy (txdoc, root, "connect_time", conntime);

          yyjson_mut_val *client = yyjson_mut_obj_add_obj (txdoc, root, "client");
          yyjson_mut_obj_add_strcpy (txdoc, client, "ip", cinfo->ipstr);
          yyjson_mut_obj_add_int (txdoc, client, "server_port", server_port);
          yyjson_mut_obj_add_strcpy (txdoc, client, "hostname", cinfo->hostname);
          yyjson_mut_obj_add_strcpy (txdoc, client, "user_agent", cinfo->clientid);

          if (cinfo->permissions & AUTHENTICATED)
          {
            const char *authmethodstr = (cinfo->auth_method == AUTH_JWT)        ? "jwt"
                                        : (cinfo->auth_method == AUTH_USERPASS) ? "userpass"
                                                                                : "unknown";
            yyjson_mut_val *auth      = yyjson_mut_obj_add_obj (txdoc, root, "auth");
            yyjson_mut_obj_add_str (txdoc, auth, "method", authmethodstr);
            if (cinfo->auth_username[0])
              yyjson_mut_obj_add_strcpy (txdoc, auth, "username", cinfo->auth_username);
          }

          yyjson_mut_val *proto = yyjson_mut_obj_add_obj (txdoc, root, "transfer_protocol");
          yyjson_mut_obj_add_strcpy (txdoc, proto, "name", modestr);
          if (protoversion[0])
            yyjson_mut_obj_add_strcpy (txdoc, proto, "version", protoversion);
          if (cinfo->tls)
            yyjson_mut_obj_add_bool (txdoc, proto, "is_tls", true);
          if (cinfo->websocket)
            yyjson_mut_obj_add_bool (txdoc, proto, "is_websocket", true);

          yyjson_mut_obj_add_strcpy (txdoc, root, "transfer_direction", "TX");

          yyjson_mut_val *svc = yyjson_mut_obj_add_obj (txdoc, root, "service");
          yyjson_mut_obj_add_strcpy (txdoc, svc, "name", PACKAGE);
          yyjson_mut_obj_add_strcpy (txdoc, svc, "version", VERSION);

          txstreams = yyjson_mut_obj_add_arr (txdoc, root, "streams");
        }
      }

      if (rxfp)
      {
        rxdoc = yyjson_mut_doc_new (NULL);
        if (rxdoc)
        {
          yyjson_mut_val *root = yyjson_mut_obj (rxdoc);
          yyjson_mut_doc_set_root (rxdoc, root);

          yyjson_mut_obj_add_strcpy (rxdoc, root, "log_time", currtime);
          yyjson_mut_obj_add_strcpy (rxdoc, root, "log_start_time", logstarttime);
          yyjson_mut_obj_add_strcpy (rxdoc, root, "connect_time", conntime);

          yyjson_mut_val *client = yyjson_mut_obj_add_obj (rxdoc, root, "client");
          yyjson_mut_obj_add_strcpy (rxdoc, client, "ip", cinfo->ipstr);
          yyjson_mut_obj_add_int (rxdoc, client, "server_port", server_port);
          yyjson_mut_obj_add_strcpy (rxdoc, client, "hostname", cinfo->hostname);
          yyjson_mut_obj_add_strcpy (rxdoc, client, "user_agent", cinfo->clientid);

          if (cinfo->permissions & AUTHENTICATED)
          {
            const char *authmethodstr = (cinfo->auth_method == AUTH_JWT)        ? "jwt"
                                        : (cinfo->auth_method == AUTH_USERPASS) ? "userpass"
                                                                                : "unknown";
            yyjson_mut_val *auth      = yyjson_mut_obj_add_obj (rxdoc, root, "auth");
            yyjson_mut_obj_add_str (rxdoc, auth, "method", authmethodstr);
            if (cinfo->auth_username[0])
              yyjson_mut_obj_add_strcpy (rxdoc, auth, "username", cinfo->auth_username);
          }

          yyjson_mut_val *proto = yyjson_mut_obj_add_obj (rxdoc, root, "transfer_protocol");
          yyjson_mut_obj_add_strcpy (rxdoc, proto, "name", modestr);
          if (protoversion[0])
            yyjson_mut_obj_add_strcpy (rxdoc, proto, "version", protoversion);
          if (cinfo->tls)
            yyjson_mut_obj_add_bool (rxdoc, proto, "is_tls", true);
          if (cinfo->websocket)
            yyjson_mut_obj_add_bool (rxdoc, proto, "is_websocket", true);

          yyjson_mut_obj_add_strcpy (rxdoc, root, "transfer_direction", "RX");

          yyjson_mut_val *svc = yyjson_mut_obj_add_obj (rxdoc, root, "service");
          yyjson_mut_obj_add_strcpy (rxdoc, svc, "name", PACKAGE);
          yyjson_mut_obj_add_strcpy (rxdoc, svc, "version", VERSION);

          rxstreams = yyjson_mut_obj_add_arr (rxdoc, root, "streams");
        }
      }

      /* Loop through streams, add JSON items */
      while ((rbnode = (RBNode *)StackPop (stack)))
      {
        streamnode = (StreamNode *)rbnode->data;

        if (txfp && txdoc && txstreams && streamnode->txbytes > 0)
        {
          yyjson_mut_val *item = yyjson_mut_arr_add_obj (txdoc, txstreams);
          yyjson_mut_obj_add_strcpy (txdoc, item, "stream_id", streamnode->streamid);
          yyjson_mut_obj_add_uint (txdoc, item, "bytes", streamnode->txbytes);
          yyjson_mut_obj_add_uint (txdoc, item, "packets", streamnode->txpackets);

          txtotalbytes += streamnode->txbytes;

          if (reset)
          {
            streamnode->txbytes   = 0;
            streamnode->txpackets = 0;
          }
        }

        if (rxfp && rxdoc && rxstreams && streamnode->rxbytes > 0)
        {
          yyjson_mut_val *item = yyjson_mut_arr_add_obj (rxdoc, rxstreams);
          yyjson_mut_obj_add_strcpy (rxdoc, item, "stream_id", streamnode->streamid);
          yyjson_mut_obj_add_uint (rxdoc, item, "bytes", streamnode->rxbytes);
          yyjson_mut_obj_add_uint (rxdoc, item, "packets", streamnode->rxpackets);

          rxtotalbytes += streamnode->rxbytes;

          if (reset)
          {
            streamnode->rxbytes   = 0;
            streamnode->rxpackets = 0;
          }
        }
      }

      StackDestroy (stack, free);

      pthread_mutex_unlock (&(cinfo->streams_lock));

      if (txdoc)
      {
        yyjson_mut_val *root = yyjson_mut_doc_get_root (txdoc);
        yyjson_mut_obj_add_uint (txdoc, root, "transfer_bytes", txtotalbytes);

        char *json = yyjson_mut_write (txdoc, 0, NULL);
        if (json)
        {
          fprintf (txfp, "%s\n", json);
          free (json);
        }
        yyjson_mut_doc_free (txdoc);
      }

      if (rxdoc)
      {
        yyjson_mut_val *root = yyjson_mut_doc_get_root (rxdoc);
        yyjson_mut_obj_add_uint (rxdoc, root, "transfer_bytes", rxtotalbytes);

        char *json = yyjson_mut_write (rxdoc, 0, NULL);
        if (json)
        {
          fprintf (rxfp, "%s\n", json);
          free (json);
        }
        yyjson_mut_doc_free (rxdoc);
      }
    }
    else
    {
      /* Legacy text format: START CLIENT / streams / END CLIENT */

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

      stack = StackCreate ();

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
            streamnode->txbytes   = 0;
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
            streamnode->rxbytes   = 0;
            streamnode->rxpackets = 0;
          }
        }
      }

      StackDestroy (stack, free);

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
  }

  /* Flush log files */
  if (txfp)
    fflush (txfp);
  if (rxfp)
    fflush (rxfp);

  /* Unlock transfer file writing lock mutex */
  pthread_mutex_unlock (&config.tlog.write_lock);

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

  return rv;
} /* End of WriteTLog() */

/***************************************************************************
 * CalcTLogInterval:
 *
 * Calculate a normalized interval transfer log file time window for a
 * given reference time (usually the current time) and interval in seconds.
 *
 * The window is always normalized relative to the current day. Intervals
 * which evenly divide days will work cleanly, other intervals will
 * probably work but might result in unexpected window calculations.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
CalcTLogInterval (time_t reftime)
{
  struct tm reftm;

  if (!localtime_r (&reftime, &reftm))
    return -1;

  /* Round down to current day */
  reftm.tm_sec  = 0;
  reftm.tm_min  = 0;
  reftm.tm_hour = 0;

  /* Take config writer lock */
  pthread_rwlock_wrlock (&config.config_rwlock);

  /* Calculate the new, rounded epoch time */
  config.tlog.startint = mktime (&reftm);

  /* Add intervals until within the current interval */
  while ((config.tlog.startint + config.tlog.interval) <= reftime)
    config.tlog.startint += config.tlog.interval;

  /* Set end of interval window */
  config.tlog.endint = config.tlog.startint + config.tlog.interval;

  pthread_rwlock_unlock (&config.config_rwlock);

  return 0;
} /* End of CalcTLogInterval() */

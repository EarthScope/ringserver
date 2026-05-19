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
 * Copyright (C) 2026:
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
#include "ringserver.h"
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
  char message[1024];
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

    /* Determine if the message was truncated, minding signed/unsigned differences */
    int truncated = (rv < 0) || ((size_t)rv >= sizeof (message));

    pthread_mutex_lock (&log_mutex);
    printf ("%3.3s %3.3s %2.2d %2.2d:%2.2d:%2.2d %4.4d - %s%s\n",
            day[tp.tm_wday], month[tp.tm_mon], tp.tm_mday,
            tp.tm_hour, tp.tm_min, tp.tm_sec, tp.tm_year + 1900,
            message, truncated ? " ..." : "");

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
 * WriteTransferLog:
 *
 * Write transfer packet and byte counts to files in the usage log base
 * directory.  Separate files are written for transmission (TX) logs
 * and reception (RX) logs.  If the 'reset' flag is true reset all
 * counts to zero after logging.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
WriteTransferLog (ClientInfo *cinfo, int reset)
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

  time_t tlog_startint = 0;

  const char *txfilename = NULL;
  const char *rxfilename = NULL;
  char conntime[32]      = {0};
  char currtime[32]      = {0};
  char logstarttime[32]  = {0};
  char *modestr          = "";
  FILE *txfp             = NULL;
  FILE *rxfp             = NULL;

  /* Quick atomic check: skip all work if transfer logging is not enabled */
  if (!(config.usagelog.mode & (USAGELOG_TX | USAGELOG_RX)))
    return 0;

  /* Generate pretty strings for current & connection time */
  ms_nstime2timestr_n (clock, currtime, sizeof (currtime), ISOMONTHDAY_Z, NONE);
  ms_nstime2timestr_n (cinfo->conntime, conntime, sizeof (conntime), ISOMONTHDAY_Z, NONE);

  /* Convert server port from string to integer */
  char *endptr = NULL;
  long parsed  = strtol (cinfo->serverport, &endptr, 10);
  server_port  = (endptr != cinfo->serverport && *endptr == '\0' &&
                  parsed >= 0 && parsed <= 65535)
                     ? (int)parsed
                     : 0;

  /* Lock usage log file writing mutex */
  pthread_mutex_lock (&config.usagelog.write_lock);

  /* Reference cached filenames directly; they are stable while we hold the lock */
  txfilename    = config.usagelog.txlog_filename;
  rxfilename    = config.usagelog.rxlog_filename;
  tlog_startint = config.usagelog.startint;
  jsonlmode     = (config.usagelog.mode & USAGELOG_JSONL) ? 1 : 0;

  /* Nothing to log if both filenames are empty (logging not configured) */
  if (!txfilename[0] && !rxfilename[0])
  {
    pthread_mutex_unlock (&config.usagelog.write_lock);
    return 0;
  }

  /* Compute log start time: later of interval start or client connect time */
  nstime_t intervalstart_ns = (nstime_t)tlog_startint * NSTMODULUS;
  nstime_t logstart         = (cinfo->conntime > intervalstart_ns) ? cinfo->conntime : intervalstart_ns;
  ms_nstime2timestr_n (logstart, logstarttime, sizeof (logstarttime), ISOMONTHDAY_Z, NONE);

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
          if (cinfo->clientid[0])
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

          if (cinfo->type != CLIENT_UNDETERMINED || cinfo->tls)
          {
            yyjson_mut_val *proto = yyjson_mut_obj_add_obj (txdoc, root, "transfer_protocol");
            if (cinfo->type != CLIENT_UNDETERMINED)
              yyjson_mut_obj_add_strcpy (txdoc, proto, "name", modestr);
            if (protoversion[0])
              yyjson_mut_obj_add_strcpy (txdoc, proto, "version", protoversion);
            if (cinfo->tls)
              yyjson_mut_obj_add_bool (txdoc, proto, "is_tls", true);
            if (cinfo->websocket)
              yyjson_mut_obj_add_bool (txdoc, proto, "is_websocket", true);
          }

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
          if (cinfo->clientid[0])
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

          if (cinfo->type != CLIENT_UNDETERMINED || cinfo->tls)
          {
            yyjson_mut_val *proto = yyjson_mut_obj_add_obj (rxdoc, root, "transfer_protocol");
            if (cinfo->type != CLIENT_UNDETERMINED)
              yyjson_mut_obj_add_strcpy (rxdoc, proto, "name", modestr);
            if (protoversion[0])
              yyjson_mut_obj_add_strcpy (rxdoc, proto, "version", protoversion);
            if (cinfo->tls)
              yyjson_mut_obj_add_bool (rxdoc, proto, "is_tls", true);
            if (cinfo->websocket)
              yyjson_mut_obj_add_bool (rxdoc, proto, "is_websocket", true);
          }

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

        /* Snapshot counters atomically.  Only exchange-with-zero (i.e. consume
         * the values) on the writer paths that actually consumed them; an
         * unconfigured log path must not silently drop counts. */
        if (txfp && txdoc && txstreams)
        {
          uint64_t txbytes   = reset ? atomic_exchange (&streamnode->txbytes, 0)
                                     : atomic_load (&streamnode->txbytes);
          uint64_t txpackets = reset ? atomic_exchange (&streamnode->txpackets, 0)
                                     : atomic_load (&streamnode->txpackets);

          if (txbytes > 0)
          {
            yyjson_mut_val *item = yyjson_mut_arr_add_obj (txdoc, txstreams);
            yyjson_mut_obj_add_strcpy (txdoc, item, "stream_id", streamnode->streamid);
            yyjson_mut_obj_add_uint (txdoc, item, "bytes", txbytes);
            yyjson_mut_obj_add_uint (txdoc, item, "packets", txpackets);

            txtotalbytes += txbytes;
          }
        }

        if (rxfp && rxdoc && rxstreams)
        {
          uint64_t rxbytes   = reset ? atomic_exchange (&streamnode->rxbytes, 0)
                                     : atomic_load (&streamnode->rxbytes);
          uint64_t rxpackets = reset ? atomic_exchange (&streamnode->rxpackets, 0)
                                     : atomic_load (&streamnode->rxpackets);

          if (rxbytes > 0)
          {
            yyjson_mut_val *item = yyjson_mut_arr_add_obj (rxdoc, rxstreams);
            yyjson_mut_obj_add_strcpy (rxdoc, item, "stream_id", streamnode->streamid);
            yyjson_mut_obj_add_uint (rxdoc, item, "bytes", rxbytes);
            yyjson_mut_obj_add_uint (rxdoc, item, "packets", rxpackets);

            rxtotalbytes += rxbytes;
          }
        }
      }

      StackDestroy (stack, free);

      pthread_mutex_unlock (&(cinfo->streams_lock));

      if (txdoc)
      {
        if (txtotalbytes > 0)
        {
          yyjson_mut_val *root = yyjson_mut_doc_get_root (txdoc);
          yyjson_mut_obj_add_uint (txdoc, root, "transfer_bytes", txtotalbytes);

          char *json = yyjson_mut_write (txdoc, 0, NULL);
          if (json)
          {
            fprintf (txfp, "%s\n", json);
            free (json);
          }
        }
        yyjson_mut_doc_free (txdoc);
      }

      if (rxdoc)
      {
        if (rxtotalbytes > 0)
        {
          yyjson_mut_val *root = yyjson_mut_doc_get_root (rxdoc);
          yyjson_mut_obj_add_uint (rxdoc, root, "transfer_bytes", rxtotalbytes);

          char *json = yyjson_mut_write (rxdoc, 0, NULL);
          if (json)
          {
            fprintf (rxfp, "%s\n", json);
            free (json);
          }
        }
        yyjson_mut_doc_free (rxdoc);
      }
    }
    else
    {
      /* Legacy text format: START CLIENT / streams / END CLIENT */

      /* Buffer stream lines in memory, then emit header+lines+footer only if
         there were transfers in the respective direction */
      char *txbuf   = NULL;
      char *rxbuf   = NULL;
      size_t txsize = 0;
      size_t rxsize = 0;
      FILE *txmem   = (txfp) ? open_memstream (&txbuf, &txsize) : NULL;
      FILE *rxmem   = (rxfp) ? open_memstream (&rxbuf, &rxsize) : NULL;
      int txok      = (!txfp || txmem != NULL);
      int rxok      = (!rxfp || rxmem != NULL);

      if (txfp && !txmem)
      {
        lprintf (0, "Error creating in-memory TX transfer log buffer: %s", strerror (errno));
        rv = -1;
      }
      if (rxfp && !rxmem)
      {
        lprintf (0, "Error creating in-memory RX transfer log buffer: %s", strerror (errno));
        rv = -1;
      }

      /* Lock stream tree and create list (Stack) of streams */
      pthread_mutex_lock (&(cinfo->streams_lock));

      stack = StackCreate ();

      if (cinfo->streams)
        RBBuildStack (cinfo->streams, stack);

      /* Loop through streams and output bytecounts into memory buffers */
      txtotalbytes = 0;
      rxtotalbytes = 0;
      while ((rbnode = (RBNode *)StackPop (stack)))
      {
        streamnode = (StreamNode *)rbnode->data;

        /* Snapshot counters atomically; consume (exchange-with-zero) only on
         * the writer paths that actually report them. */
        if (txmem)
        {
          uint64_t txbytes   = reset ? atomic_exchange (&streamnode->txbytes, 0)
                                     : atomic_load (&streamnode->txbytes);
          uint64_t txpackets = reset ? atomic_exchange (&streamnode->txpackets, 0)
                                     : atomic_load (&streamnode->txpackets);

          if (txbytes > 0)
          {
            fprintf (txmem, "%s %" PRIu64 " %" PRIu64 "\n", streamnode->streamid,
                     txbytes, txpackets);

            txtotalbytes += txbytes;
          }
        }

        if (rxmem)
        {
          uint64_t rxbytes   = reset ? atomic_exchange (&streamnode->rxbytes, 0)
                                     : atomic_load (&streamnode->rxbytes);
          uint64_t rxpackets = reset ? atomic_exchange (&streamnode->rxpackets, 0)
                                     : atomic_load (&streamnode->rxpackets);

          if (rxbytes > 0)
          {
            fprintf (rxmem, "%s %" PRIu64 " %" PRIu64 "\n", streamnode->streamid,
                     rxbytes, rxpackets);

            rxtotalbytes += rxbytes;
          }
        }
      }

      StackDestroy (stack, free);

      /* Unlock stream tree */
      pthread_mutex_unlock (&(cinfo->streams_lock));

      /* Finalise memory streams; detect write errors and transfer buffer ownership */
      if (txfp)
      {
        if (FinalizeMemStream (txmem, &txbuf, &txsize) < 0)
        {
          if (txok)
          {
            lprintf (0, "Error finalising TX transfer log buffer for %s [%s]",
                     cinfo->hostname, cinfo->ipstr);
            rv = -1;
          }
          txok = 0;
        }
      }
      if (rxfp)
      {
        if (FinalizeMemStream (rxmem, &rxbuf, &rxsize) < 0)
        {
          if (rxok)
          {
            lprintf (0, "Error finalising RX transfer log buffer for %s [%s]",
                     cinfo->hostname, cinfo->ipstr);
            rv = -1;
          }
          rxok = 0;
        }
      }

      /* Emit TX block only if there were bytes and the buffer is valid */
      if (txok && txfp && txtotalbytes > 0)
      {
        fprintf (txfp, "START CLIENT %s [%s] (%s|%s) @ %s (connected %s) TX\n",
                 cinfo->hostname, cinfo->ipstr, modestr,
                 cinfo->clientid[0] ? cinfo->clientid : "Client",
                 currtime, conntime);
        if (txbuf)
          fwrite (txbuf, 1, txsize, txfp);
        fprintf (txfp, "END CLIENT %s [%s] total TX bytes: %" PRIu64 "\n",
                 cinfo->hostname, cinfo->ipstr, txtotalbytes);
      }

      /* Emit RX block only if there were bytes and the buffer is valid */
      if (rxok && rxfp && rxtotalbytes > 0)
      {
        fprintf (rxfp, "START CLIENT %s [%s] (%s|%s) @ %s (connected %s) RX\n",
                 cinfo->hostname, cinfo->ipstr, modestr,
                 cinfo->clientid[0] ? cinfo->clientid : "Client",
                 currtime, conntime);
        if (rxbuf)
          fwrite (rxbuf, 1, rxsize, rxfp);
        fprintf (rxfp, "END CLIENT %s [%s] total RX bytes: %" PRIu64 "\n",
                 cinfo->hostname, cinfo->ipstr, rxtotalbytes);
      }

      free (txbuf);
      free (rxbuf);
    }
  }

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

  pthread_mutex_unlock (&config.usagelog.write_lock);

  return rv;
} /* End of WriteTransferLog() */

/***************************************************************************
 * WriteAccessLog:
 *
 * Write a single access log entry to the accesslog file in the usage log
 * base directory.  Each call appends one JSON Lines record describing a
 * connection event or command issued by the client.
 *
 * event   - "connect", "disconnect", or "command"
 * command - command name (e.g. "INFO", "DATA", "STREAM", "GET"), or NULL
 * detail  - command detail (e.g. INFO item, HTTP path), or NULL
 * match   - stream match expression, or NULL
 * reject  - stream reject expression, or NULL
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
WriteAccessLog (ClientInfo *cinfo, const char *event,
                const char *command, const char *detail,
                const char *match, const char *reject)
{
  const char *filename = NULL;
  char currtime[32]    = {0};
  char conntime[32]    = {0};
  int server_port;
  FILE *fp = NULL;
  int rv   = 0;

  if (!cinfo || !event)
    return -1;

  /* Quick atomic check: skip JSON building if access logging is not enabled */
  if (!(config.usagelog.mode & USAGELOG_ACCESS))
    return 0;

  /* Generate ISO timestamp strings */
  nstime_t clock = NSnow ();
  ms_nstime2timestr_n (clock, currtime, sizeof (currtime), ISOMONTHDAY_Z, NONE);
  ms_nstime2timestr_n (cinfo->conntime, conntime, sizeof (conntime), ISOMONTHDAY_Z, NONE);

  /* Convert server port from string to integer with full validation */
  char *endptr = NULL;
  long parsed  = strtol (cinfo->serverport, &endptr, 10);
  server_port  = (endptr != cinfo->serverport && *endptr == '\0' &&
                  parsed >= 0 && parsed <= 65535)
                     ? (int)parsed
                     : 0;

  /* Determine protocol name and version strings */
  const char *modestr   = "Unknown";
  char protoversion[16] = {0};

  if (cinfo->type == CLIENT_DATALINK)
  {
    modestr = "DataLink";
    snprintf (protoversion, sizeof (protoversion), "%u.%u", DLPROTO_MAJOR, DLPROTO_MINOR);
  }
  else if (cinfo->type == CLIENT_SEEDLINK)
  {
    modestr = "SeedLink";
    if (cinfo->extinfo)
    {
      SLInfo *slinfo = (SLInfo *)cinfo->extinfo;
      snprintf (protoversion, sizeof (protoversion), "%u.%u",
                slinfo->proto_major, slinfo->proto_minor);
    }
  }
  else if (cinfo->type == CLIENT_HTTP)
  {
    modestr = "HTTP";
  }

  /* Build JSON document */
  yyjson_mut_doc *doc = yyjson_mut_doc_new (NULL);
  if (!doc)
    return -1;

  yyjson_mut_val *root = yyjson_mut_obj (doc);
  yyjson_mut_doc_set_root (doc, root);

  yyjson_mut_obj_add_strcpy (doc, root, "log_time", currtime);
  yyjson_mut_obj_add_strcpy (doc, root, "connect_time", conntime);

  yyjson_mut_val *client = yyjson_mut_obj_add_obj (doc, root, "client");
  yyjson_mut_obj_add_strcpy (doc, client, "ip", cinfo->ipstr);
  yyjson_mut_obj_add_int (doc, client, "server_port", server_port);
  yyjson_mut_obj_add_strcpy (doc, client, "hostname", cinfo->hostname);
  if (cinfo->clientid[0])
    yyjson_mut_obj_add_strcpy (doc, client, "user_agent", cinfo->clientid);

  if (cinfo->permissions & AUTHENTICATED)
  {
    const char *authmethodstr = (cinfo->auth_method == AUTH_JWT)        ? "jwt"
                                : (cinfo->auth_method == AUTH_USERPASS) ? "userpass"
                                                                        : "unknown";
    yyjson_mut_val *auth      = yyjson_mut_obj_add_obj (doc, root, "auth");
    yyjson_mut_obj_add_str (doc, auth, "method", authmethodstr);
    if (cinfo->auth_username[0])
      yyjson_mut_obj_add_strcpy (doc, auth, "username", cinfo->auth_username);
  }

  if (cinfo->type != CLIENT_UNDETERMINED || cinfo->tls)
  {
    yyjson_mut_val *proto = yyjson_mut_obj_add_obj (doc, root, "protocol");
    if (cinfo->type != CLIENT_UNDETERMINED)
      yyjson_mut_obj_add_strcpy (doc, proto, "name", modestr);
    if (protoversion[0])
      yyjson_mut_obj_add_strcpy (doc, proto, "version", protoversion);
    if (cinfo->tls)
      yyjson_mut_obj_add_bool (doc, proto, "is_tls", true);
    if (cinfo->websocket)
      yyjson_mut_obj_add_bool (doc, proto, "is_websocket", true);
  }

  yyjson_mut_obj_add_strcpy (doc, root, "event", event);
  if (command)
    yyjson_mut_obj_add_strcpy (doc, root, "command", command);
  if (detail)
    yyjson_mut_obj_add_strcpy (doc, root, "detail", detail);
  if (match)
    yyjson_mut_obj_add_strcpy (doc, root, "match", match);
  if (reject)
    yyjson_mut_obj_add_strcpy (doc, root, "reject", reject);

  yyjson_mut_val *svc = yyjson_mut_obj_add_obj (doc, root, "service");
  yyjson_mut_obj_add_strcpy (doc, svc, "name", PACKAGE);
  yyjson_mut_obj_add_strcpy (doc, svc, "version", VERSION);

  /* Lock usage log file writing mutex */
  pthread_mutex_lock (&config.usagelog.write_lock);

  /* Reference cached filename directly; stable while we hold the lock.
   * Empty means access logging not currently configured (unexpected). */
  filename = config.usagelog.accesslog_filename;
  if (!filename[0])
  {
    pthread_mutex_unlock (&config.usagelog.write_lock);
    yyjson_mut_doc_free (doc);
    return 0;
  }

  if ((fp = fopen (filename, "a")) == NULL)
  {
    lprintf (0, "Error opening access log file %s: %s", filename, strerror (errno));
    rv = -1;
  }
  else if (fseek (fp, 0, SEEK_END))
  {
    lprintf (0, "Error seeking to end of access log file %s: %s", filename, strerror (errno));
    rv = -1;
  }
  else
  {
    char *json = yyjson_mut_write (doc, 0, NULL);
    if (json)
    {
      fprintf (fp, "%s\n", json);
      free (json);
    }

    if (fclose (fp))
    {
      lprintf (0, "Error closing access log file %s: %s", filename, strerror (errno));
      rv = -1;
    }
  }

  pthread_mutex_unlock (&config.usagelog.write_lock);

  yyjson_mut_doc_free (doc);

  return rv;
} /* End of WriteAccessLog() */

/***************************************************************************
 * RenderUsageLogFilenames:
 *
 * Render the three usage log filenames into config.usagelog based on the
 * current config values.  Sets an empty string for any log type whose mode
 * bit is not set or when basedir is NULL.
 *
 * Caller must hold both config.config_rwlock (writer) and
 * config.usagelog.write_lock.
 ***************************************************************************/
static void
RenderUsageLogFilenames (void)
{
  struct tm starttm;
  struct tm endtm;
  int jsonlmode;
  time_t startint;
  time_t endint;
  int rv;

  if (!config.usagelog.basedir)
  {
    config.usagelog.txlog_filename[0]     = '\0';
    config.usagelog.rxlog_filename[0]     = '\0';
    config.usagelog.accesslog_filename[0] = '\0';
    return;
  }

  jsonlmode = (config.usagelog.mode & USAGELOG_JSONL) ? 1 : 0;
  startint  = config.usagelog.startint;
  endint    = config.usagelog.endint;

  localtime_r (&startint, &starttm);
  localtime_r (&endint, &endtm);

  if (config.usagelog.mode & USAGELOG_TX)
  {
    rv = snprintf (config.usagelog.txlog_filename, sizeof (config.usagelog.txlog_filename),
                   "%s/%s%stxlog-%04d%02d%02dT%02d%02d-%04d%02d%02dT%02d%02d%s",
                   config.usagelog.basedir,
                   (config.usagelog.prefix) ? config.usagelog.prefix : "",
                   (config.usagelog.prefix) ? "-" : "",
                   starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
                   starttm.tm_hour, starttm.tm_min,
                   endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
                   endtm.tm_hour, endtm.tm_min,
                   jsonlmode ? ".jsonl" : "");
    if (rv < 0 || (size_t)rv >= sizeof (config.usagelog.txlog_filename))
    {
      lprintf (0, "%s(): txlog filename truncated (basedir/prefix too long), disabling for this interval",
               __func__);
      config.usagelog.txlog_filename[0] = '\0';
    }
  }
  else
  {
    config.usagelog.txlog_filename[0] = '\0';
  }

  if (config.usagelog.mode & USAGELOG_RX)
  {
    rv = snprintf (config.usagelog.rxlog_filename, sizeof (config.usagelog.rxlog_filename),
                   "%s/%s%srxlog-%04d%02d%02dT%02d%02d-%04d%02d%02dT%02d%02d%s",
                   config.usagelog.basedir,
                   (config.usagelog.prefix) ? config.usagelog.prefix : "",
                   (config.usagelog.prefix) ? "-" : "",
                   starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
                   starttm.tm_hour, starttm.tm_min,
                   endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
                   endtm.tm_hour, endtm.tm_min,
                   jsonlmode ? ".jsonl" : "");
    if (rv < 0 || (size_t)rv >= sizeof (config.usagelog.rxlog_filename))
    {
      lprintf (0, "%s(): rxlog filename truncated (basedir/prefix too long), disabling for this interval",
               __func__);
      config.usagelog.rxlog_filename[0] = '\0';
    }
  }
  else
  {
    config.usagelog.rxlog_filename[0] = '\0';
  }

  if (config.usagelog.mode & USAGELOG_ACCESS)
  {
    rv = snprintf (config.usagelog.accesslog_filename, sizeof (config.usagelog.accesslog_filename),
                   "%s/%s%saccesslog-%04d%02d%02dT%02d%02d-%04d%02d%02dT%02d%02d.jsonl",
                   config.usagelog.basedir,
                   (config.usagelog.prefix) ? config.usagelog.prefix : "",
                   (config.usagelog.prefix) ? "-" : "",
                   starttm.tm_year + 1900, starttm.tm_mon + 1, starttm.tm_mday,
                   starttm.tm_hour, starttm.tm_min,
                   endtm.tm_year + 1900, endtm.tm_mon + 1, endtm.tm_mday,
                   endtm.tm_hour, endtm.tm_min);
    if (rv < 0 || (size_t)rv >= sizeof (config.usagelog.accesslog_filename))
    {
      lprintf (0, "%s(): accesslog filename truncated (basedir/prefix too long), disabling for this interval",
               __func__);
      config.usagelog.accesslog_filename[0] = '\0';
    }
  }
  else
  {
    config.usagelog.accesslog_filename[0] = '\0';
  }
} /* End of RenderUsageLogFilenames() */

/***************************************************************************
 * CalcUsageLogInterval_locked:
 *
 * Internal helper.  Same as CalcUsageLogInterval() but the caller must
 * already hold config.config_rwlock as a writer.  Useful when called from
 * within an existing wrlock window (e.g., immediately after ReadConfigFile
 * on a config reload) so that the cached log filenames are updated atomically
 * with the underlying basedir/prefix/mode values, eliminating the brief
 * cache-inconsistency window that would otherwise exist between the config
 * change and the next watchdog tick.
 *
 * Returns 0 on success, and -1 on failure.
 ***************************************************************************/
int
CalcUsageLogInterval_locked (time_t reftime)
{
  struct tm reftm;
  time_t startint;
  time_t endint;

  if (!localtime_r (&reftime, &reftm))
    return -1;

  /* Round down to current day */
  reftm.tm_sec  = 0;
  reftm.tm_min  = 0;
  reftm.tm_hour = 0;

  /* Defensive guard, protect against <= 0 values */
  if (config.usagelog.interval <= 0)
  {
    lprintf (0, "%s(): invalid usage log interval: %d", __func__, config.usagelog.interval);
    return -1;
  }

  /* Calculate the new, rounded epoch time */
  startint = mktime (&reftm);

  /* Add intervals until within the current interval */
  while ((startint + config.usagelog.interval) <= reftime)
    startint += config.usagelog.interval;

  endint = startint + config.usagelog.interval;

  config.usagelog.startint = startint;
  config.usagelog.endint   = endint;

  /* Render the cached log filenames under write_lock so readers in
   * WriteTransferLog() and WriteAccessLog() see a consistent snapshot
   * without ever needing to take config_rwlock. */
  pthread_mutex_lock (&config.usagelog.write_lock);
  RenderUsageLogFilenames ();
  pthread_mutex_unlock (&config.usagelog.write_lock);

  return 0;
} /* End of CalcUsageLogInterval_locked() */

/***************************************************************************
 * CalcUsageLogInterval:
 *
 * Calculate a normalized interval usage log file time window for a
 * given reference time (usually the current time) and interval in seconds.
 * After updating the interval bounds, renders the three cached log filenames
 * (txlog, rxlog, accesslog) into config.usagelog so that WriteTransferLog()
 * and WriteAccessLog() can consume them without taking config_rwlock.
 *
 * The window is always normalized relative to the current day. Intervals
 * which evenly divide days will work cleanly, other intervals will
 * probably work but might result in unexpected window calculations.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
CalcUsageLogInterval (time_t reftime)
{
  int rv;

  pthread_rwlock_wrlock (&config.config_rwlock);
  rv = CalcUsageLogInterval_locked (reftime);
  pthread_rwlock_unlock (&config.config_rwlock);

  return rv;
} /* End of CalcUsageLogInterval() */

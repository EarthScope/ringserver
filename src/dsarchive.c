/***************************************************************************
 * dsarchive.c
 *
 * Routines to archive Mini-SEED data records.
 *
 * The philosophy: a "DataStream" describes an archive that Mini-SEED
 * records will be saved to.  Each archive can be separated into
 * "DataStreamGroup"s, each unique group will be saved into a unique
 * file.  The definition of the groups is implied by the format of the
 * archive.
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
 * modified: 2016.345
 ***************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include <libmseed.h>

#include "clients.h"
#include "dsarchive.h"
#include "logging.h"

/* Functions internal to this source file */
static DataStreamGroup *ds_getstream (DataStream *datastream, const char *defkey,
                                      char *filename, char *postpath, int nondefflags,
                                      const char *globmatch, char *hostname);
static int ds_openfile (DataStream *datastream, const char *filename, char *ident);
static void ds_shutdown (DataStream *datastream, char *ident);

/* For a linked list of strings, as filled by ds_strparse() */
typedef struct DSstrlist_s
{
  char *element;
  struct DSstrlist_s *next;
} DSstrlist;

static int ds_strparse (const char *string, const char *delim, DSstrlist **list);

/***************************************************************************
 * ds_streamproc:
 *
 * Save MiniSEED records in a custom directory/file structure.  The
 * appropriate directories and files are created if nesecessary.  If
 * files already exist they are appended to.  If 'msr' is NULL then
 * ds_shutdown() will be called to close all open files and free all
 * associated memory.
 *
 * If the postpath argument is not NULL it will be appended to the end
 * of path as constructed from the datastream archive definition.  For
 * example, the datastream archive could specify a base directory and
 * the postpath could specify a file name.
 *
 * Returns 0 on success, -1 on error.
 ***************************************************************************/
extern int
ds_streamproc (DataStream *datastream, MSRecord *msr, char *postpath,
               char *hostname)
{
  DataStreamGroup *foundgroup = NULL;
  DSstrlist *fnlist, *fnptr;
  struct tm ctm;
  time_t curtime;
  char *tptr;
  char tstr[20];
  char filename[MAX_FILENAME_LEN];
  char definition[MAX_FILENAME_LEN];
  char pathformat[MAX_FILENAME_LEN];
  char globmatch[MAX_FILENAME_LEN];
  int fnlen = 0;
  int nondefflags = 0;
  int writebytes;
  int writeloops;
  int rv;

  /* Special case for stream shutdown */
  if (!msr)
  {
    lprintf (2, "[%s] Closing archive for %s",
             hostname, datastream->path);

    ds_shutdown (datastream, hostname);
    return 0;
  }

  /* Build file path and name from pathformat */
  filename[0] = '\0';
  definition[0] = '\0';
  globmatch[0] = '\0';

  if (postpath)
    snprintf (pathformat, sizeof (pathformat), "%s/%s", datastream->path, postpath);
  else
    snprintf (pathformat, sizeof (pathformat), "%s", datastream->path);

  pathformat[sizeof (pathformat) - 1] = '\0';

  ds_strparse (pathformat, "/", &fnlist);

  fnptr = fnlist;

  /* Count all of the non-defining flags */
  tptr = pathformat;
  while ((tptr = strchr (tptr, '#')))
  {
    if (*(tptr + 1) != '#')
      nondefflags++;
    tptr++;
  }

  /* Special case of an absolute path (first entry is empty) */
  if (*fnptr->element == '\0')
  {
    if (fnptr->next != 0)
    {
      strcat (filename, "/");
      strcat (globmatch, "/");
      fnptr = fnptr->next;
    }
    else
    {
      lprintf (0, "[%s] ds_streamproc(): empty path format",
               hostname);
      ds_strparse (NULL, NULL, &fnlist);
      return -1;
    }
  }

  while (fnptr != 0)
  {
    int globlen = 0;
    int tdy;
    char *w, *p, def;

    p = fnptr->element;

    /* Special case of no file given */
    if (*p == '\0' && fnptr->next == 0)
    {
      lprintf (0, "[%s] ds_streamproc(): no file name specified, only %s",
               hostname, filename);
      ds_strparse (NULL, NULL, &fnlist);
      return -1;
    }

    while ((w = strpbrk (p, "%#")) != NULL)
    {
      def = (*w == '%');
      *w = '\0';

      strncat (filename, p, (sizeof (filename) - fnlen));
      fnlen = strlen (filename);

      if (nondefflags > 0)
      {
        strncat (globmatch, p, (sizeof (globmatch) - globlen));
        globlen = strlen (globmatch);
      }

      w += 1;

      switch (*w)
      {
      case 'n':
        strncat (filename, msr->network, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, msr->network, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, msr->network, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 's':
        strncat (filename, msr->station, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, msr->station, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, msr->station, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'l':
        strncat (filename, msr->location, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, msr->location, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, msr->location, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'c':
        strncat (filename, msr->channel, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, msr->channel, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, msr->channel, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'q':
        snprintf (tstr, sizeof (tstr), "%c", msr->fsdh->dataquality);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "?", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'Y':
        snprintf (tstr, sizeof (tstr), "%04d", (int)msr->fsdh->start_time.year);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9][0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'y':
        tdy = (int)msr->fsdh->start_time.year;
        while (tdy > 100)
        {
          tdy -= 100;
        }
        snprintf (tstr, sizeof (tstr), "%02d", tdy);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'j':
        snprintf (tstr, sizeof (tstr), "%03d", (int)msr->fsdh->start_time.day);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'H':
        snprintf (tstr, sizeof (tstr), "%02d", (int)msr->fsdh->start_time.hour);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'M':
        snprintf (tstr, sizeof (tstr), "%02d", (int)msr->fsdh->start_time.min);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'S':
        snprintf (tstr, sizeof (tstr), "%02d", (int)msr->fsdh->start_time.sec);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'F':
        snprintf (tstr, sizeof (tstr), "%04d", (int)msr->fsdh->start_time.fract);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9][0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'D':
        curtime = time (NULL);
        if (!curtime || !localtime_r (&curtime, &ctm))
        {
          lprintf (0, "[%s] error creating current year-day time stamp: %s",
                   hostname, strerror (errno));
          p = w;
          break;
        }
        snprintf (tstr, sizeof (tstr), "%04d%03d", ctm.tm_year + 1900, ctm.tm_yday + 1);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9]", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'L':
        snprintf (tstr, sizeof (tstr), "%d", msr->reclen);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'r':
        snprintf (tstr, sizeof (tstr), "%ld", (long int)(msr->samprate + 0.5));
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'R':
        snprintf (tstr, sizeof (tstr), "%.6f", msr->samprate);
        strncat (filename, tstr, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, tstr, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, tstr, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case 'h':
        if (!hostname)
          break;
        strncat (filename, hostname, (sizeof (filename) - fnlen));
        if (def)
          strncat (definition, hostname, (sizeof (definition) - fnlen));
        if (nondefflags > 0)
        {
          if (def)
            strncat (globmatch, hostname, (sizeof (globmatch) - globlen));
          else
            strncat (globmatch, "*", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      case '%':
        strncat (filename, "%", (sizeof (filename) - fnlen));
        strncat (globmatch, "%", (sizeof (globmatch) - globlen));
        fnlen = strlen (filename);
        globlen = strlen (globmatch);
        p = w + 1;
        break;
      case '#':
        strncat (filename, "#", (sizeof (filename) - fnlen));
        nondefflags--;
        if (nondefflags > 0)
        {
          strncat (globmatch, "#", (sizeof (globmatch) - globlen));
          globlen = strlen (globmatch);
        }
        fnlen = strlen (filename);
        p = w + 1;
        break;
      default:
        lprintf (0, "[%s] unknown file name format code: %c",
                 hostname, *w);
        p = w;
        break;
      }
    }

    strncat (filename, p, (sizeof (filename) - fnlen));
    fnlen = strlen (filename);

    if (nondefflags > 0)
    {
      strncat (globmatch, p, (sizeof (globmatch) - globlen));
      globlen = strlen (globmatch);
    }

    /* If not the last entry then it should be a directory */
    if (fnptr->next != 0)
    {
      if (access (filename, F_OK))
      {
        if (errno == ENOENT)
        {
          lprintf (2, "Creating directory: %s", hostname, filename);
          if (mkdir (filename, S_IRWXU | S_IRWXG | S_IRWXO)) /* Mode 0777 */
          {
            lprintf (0, "[%s] ds_streamproc: mkdir(%s) %s",
                     hostname, filename, strerror (errno));
            ds_strparse (NULL, NULL, &fnlist);
            return -1;
          }
        }
        else
        {
          lprintf (0, "[%s] %s: access denied, %s",
                   hostname, filename, strerror (errno));
          ds_strparse (NULL, NULL, &fnlist);
          return -1;
        }
      }

      strncat (filename, "/", (sizeof (filename) - fnlen));
      fnlen++;

      if (nondefflags > 0)
      {
        strncat (globmatch, "/", (sizeof (globmatch) - globlen));
        globlen++;
      }
    }

    fnptr = fnptr->next;
  }

  ds_strparse (NULL, NULL, &fnlist);

  /* Make sure the filename and definition are NULL terminated */
  *(filename + sizeof (filename) - 1) = '\0';
  *(definition + sizeof (definition) - 1) = '\0';

  /* Check for previously used stream entry, otherwise create it */
  foundgroup = ds_getstream (datastream, definition, filename, postpath,
                             nondefflags, globmatch, hostname);

  if (foundgroup != NULL)
  {
    /*  Write the record to the appropriate file */
    lprintf (3, "[%s] Writing data to data stream file %s",
             hostname, foundgroup->filename);

    /* Try up to 10 times to write the data out, could be interrupted by signal */
    writebytes = 0;
    writeloops = 0;
    while (writeloops < 10)
    {
      rv = write (foundgroup->filed, msr->record + writebytes, msr->reclen - writebytes);

      if (rv > 0)
        writebytes += rv;

      /* Done if the entire record was written */
      if (writebytes == msr->reclen)
        break;

      if (rv < 0)
      {
        if (errno != EINTR)
        {
          lprintf (0, "[%s] ds_streamproc: failed to write record: %s (%s)",
                   hostname, strerror (errno), foundgroup->filename);
          return -1;
        }
        else
        {
          lprintf (1, "[%s] ds_streamproc: Interrupted call to write (%s), retrying",
                   hostname, foundgroup->filename);
        }
      }

      writeloops++;
    }

    if (writeloops >= 10)
    {
      lprintf (0, "[%s] ds_streamproc: Tried 10 times to write record, interrupted each time",
               hostname);
      return -1;
    }

    /* Update mod time for this entry */
    foundgroup->modtime = time (NULL);

    return 0;
  }

  return -1;
} /* End of ds_streamproc() */

/***************************************************************************
 * ds_closeidle:
 *
 * Close all stream files that have not been active for the specified
 * idletimeout.
 *
 * Return the number of files closed.
 ***************************************************************************/
int
ds_closeidle (DataStream *datastream, int idletimeout, char *ident)
{
  int count = 0;
  DataStreamGroup *searchgroup = NULL;
  DataStreamGroup *prevgroup = NULL;
  DataStreamGroup *nextgroup = NULL;
  time_t curtime;

  searchgroup = datastream->grouproot;
  curtime = time (NULL);

  /* Traverse the stream chain */
  while (searchgroup != NULL)
  {
    nextgroup = searchgroup->next;

    if (searchgroup->modtime > 0 && (curtime - searchgroup->modtime) >= idletimeout)
    {
      lprintf (2, "[%s] Closing idle stream with key %s",
               ident, searchgroup->defkey);

      /* Re-link the stream chain */
      if (prevgroup != NULL)
      {
        prevgroup->next = searchgroup->next;
      }
      else
      {
        datastream->grouproot = searchgroup->next;
      }

      /* Close the associated file */
      if (close (searchgroup->filed))
        lprintf (2, "[%s] ds_closeidle(), closing data stream file, %s",
                 ident, strerror (errno));
      else
        count++;

      free (searchgroup->defkey);
      free (searchgroup);
    }
    else
    {
      prevgroup = searchgroup;
    }

    searchgroup = nextgroup;
  }

  datastream->openfilecount -= count;

  return count;
} /* End of ds_closeidle() */

/***************************************************************************
 * ds_getstream:
 *
 * Find the DataStreamGroup entry that matches the definition key, if
 * no matching entries are found allocate a new entry and open the
 * given file.
 *
 * Resource maintenance is performed here: the modification time of
 * each stream, modtime, is compared to the current time.  If the
 * stream entry has been idle for 'DataStream.idletimeout' seconds
 * then the stream will be closed (file closed and memory freed).
 *
 * Returns a pointer to a DataStreamGroup on success or NULL on error.
 ***************************************************************************/
static DataStreamGroup *
ds_getstream (DataStream *datastream, const char *defkey, char *filename,
              char *postpath, int nondefflags, const char *globmatch, char *ident)
{
  DataStreamGroup *foundgroup = NULL;
  DataStreamGroup *searchgroup = NULL;
  DataStreamGroup *prevgroup = NULL;
  time_t curtime;
  char *matchedfilename = 0;

  searchgroup = datastream->grouproot;
  curtime = time (NULL);

  /* Traverse the stream chain looking for matching streams */
  while (searchgroup != NULL)
  {
    DataStreamGroup *nextgroup = (DataStreamGroup *)searchgroup->next;

    if (!strcmp (searchgroup->defkey, defkey) &&
        (!postpath || !strcmp (searchgroup->postpath, postpath)))
    {
      if (postpath)
        lprintf (3, "[%s] Found data stream entry for key %s (%s)",
                 ident, defkey, postpath);
      else
        lprintf (3, "[%s] Found data stream entry for key %s",
                 ident, defkey);

      foundgroup = searchgroup;

      break;
    }

    prevgroup = searchgroup;
    searchgroup = nextgroup;
  }

  /* If no matching stream entry was found but the format included
     non-defining flags, try to use globmatch to find a matching file
     and resurrect a stream entry */
  if (foundgroup == NULL && nondefflags > 0)
  {
    glob_t pglob;
    int rval;

    lprintf (3, "[%s] No stream entry found, searching for: %s",
             ident, globmatch);

    rval = glob (globmatch, 0, NULL, &pglob);

    if (rval && rval != GLOB_NOMATCH)
    {
      switch (rval)
      {
      case GLOB_ABORTED:
        lprintf (1, "[%s] glob(): Unignored lower-level error", ident);
      case GLOB_NOSPACE:
        lprintf (1, "[%s] glob(): Not enough memory", ident);
      case GLOB_NOSYS:
        lprintf (1, "[%s] glob(): Function not supported", ident);
      default:
        lprintf (1, "[%s] glob(): %d", ident, rval);
      }
    }
    else if (rval == 0 && pglob.gl_pathc > 0)
    {
      if (pglob.gl_pathc > 1)
        lprintf (3, "[%s] Found %d files matching %s, using last match",
                 ident, pglob.gl_pathc, globmatch);

      matchedfilename = pglob.gl_pathv[pglob.gl_pathc - 1];
      lprintf (2, "[%s] Found matching file for non-defining flags: %s",
               ident, matchedfilename);

      /* Now that we have a match use it instead of filename */
      strncpy (filename, matchedfilename, MAX_FILENAME_LEN - 2);
      filename[MAX_FILENAME_LEN - 1] = '\0';
    }

    globfree (&pglob);
  }

  /* If not found, create a stream entry */
  if (foundgroup == NULL)
  {
    if (matchedfilename)
      lprintf (2, "Resurrecting data stream entry for key %s", ident, defkey);
    else
      lprintf (2, "Creating data stream entry for key %s", ident, defkey);

    foundgroup = (DataStreamGroup *)malloc (sizeof (DataStreamGroup));

    foundgroup->defkey = strdup (defkey);
    foundgroup->filed = 0;
    foundgroup->modtime = curtime;
    strncpy (foundgroup->filename, filename, sizeof (foundgroup->filename));
    if (postpath)
      strncpy (foundgroup->postpath, postpath, sizeof (foundgroup->postpath));
    else
      foundgroup->postpath[0] = '\0';
    foundgroup->next = NULL;

    /* Set the stream root if this is the first entry */
    if (datastream->grouproot == NULL)
    {
      datastream->grouproot = foundgroup;
    }
    /* Otherwise add to the end of the chain */
    else if (prevgroup != NULL)
    {
      prevgroup->next = foundgroup;
    }
    else
    {
      lprintf (0, "[%s] stream chain is broken!", ident);
      return NULL;
    }
  }

  /* Keep ds_closeidle() from closing this stream */
  if (foundgroup->modtime > 0)
  {
    foundgroup->modtime *= -1;
  }

  /* Close idle stream files */
  ds_closeidle (datastream, datastream->idletimeout, ident);

  /* If no file is open, well, open it */
  if (foundgroup->filed == 0)
  {
    int filepos;

    lprintf (1, "[%s] Opening data stream file %s", ident, filename);

    if ((foundgroup->filed = ds_openfile (datastream, filename, ident)) == -1)
    {
      /* Do not complain if the call was interrupted (signals are used for shutdown) */
      if (errno == EINTR)
        foundgroup->filed = 0;
      else
        lprintf (2, "[%s] cannot open data stream file, %s",
                 ident, strerror (errno));

      return NULL;
    }

    if ((filepos = (int)lseek (foundgroup->filed, (off_t)0, SEEK_END)) < 0)
    {
      lprintf (2, "[%s] cannot seek in data stream file, %s",
               ident, strerror (errno));
      return NULL;
    }
  }

  /* There used to be a further check here, but it shouldn't be reached, just in
     case this is left for the moment until I'm convinced. */
  else if (strcmp (defkey, foundgroup->defkey))
    lprintf (0, "[%s] Arg! open file for a key that no longer matches",
             ident);

  return foundgroup;
} /* End of ds_getstream() */

/***************************************************************************
 * ds_openfile:
 *
 * Open a specified file, if the open file limit has been reach try
 * once to increase the limit, if that fails or has already been done
 * start closing idle files with decreasing idle timeouts until a file
 * can be opened.
 *
 * Return the result of open(2), normally this a the file descriptor
 * on success and -1 on error.
 ***************************************************************************/
static int
ds_openfile (DataStream *datastream, const char *filename, char *ident)
{
  static char rlimit = 0;
  struct rlimit rlim;
  int idletimeout = datastream->idletimeout;
  int oret = 0;
  int flags = (O_RDWR | O_CREAT | O_APPEND);
  mode_t mode = (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH); /* Mode 0666 */

  if (!datastream)
    return -1;

  /* Lookup process open file limit and change maxopenfiles if needed */
  if (!rlimit)
  {
    rlimit = 1;

    if (getrlimit (RLIMIT_NOFILE, &rlim) == -1)
    {
      lprintf (0, "[%s] getrlimit failed to get open file limit",
               ident);
    }
    else
    {
      /* Increase process open file limit to maxopenfiles or hard limit */
      if (datastream->maxopenfiles && datastream->maxopenfiles > rlim.rlim_cur)
      {
        if (datastream->maxopenfiles > rlim.rlim_max)
          rlim.rlim_cur = rlim.rlim_max;
        else
          rlim.rlim_cur = datastream->maxopenfiles;

        lprintf (3, "[%s] Setting open file limit to %lld",
                 ident, (int64_t)rlim.rlim_cur);

        if (setrlimit (RLIMIT_NOFILE, &rlim) == -1)
        {
          lprintf (0, "[%s] setrlimit failed to set open file limit",
                   ident);
        }

        datastream->maxopenfiles = rlim.rlim_cur;
      }
      /* Set max to current soft limit if not already specified */
      else if (!datastream->maxopenfiles)
      {
        datastream->maxopenfiles = rlim.rlim_cur;
      }
    }
  }

  /* Close open files from the DataStream if already at the limit of (maxopenfiles - 10) */
  if ((datastream->openfilecount + 10) > datastream->maxopenfiles)
  {
    lprintf (2, "[%s] Maximum open archive files reached (%d), closing idle stream files",
             ident, (datastream->maxopenfiles - 10));

    /* Close idle streams until we have free descriptors */
    while (ds_closeidle (datastream, idletimeout, ident) == 0 && idletimeout >= 0)
    {
      idletimeout = (idletimeout / 2) - 1;
    }
  }

  /* Open file */
  if ((oret = open (filename, flags, mode)) != -1)
  {
    datastream->openfilecount++;
  }

  return oret;
} /* End of ds_openfile() */

/***************************************************************************
 * ds_shutdown:
 *
 * Close all stream files and release all of the DataStreamGroup memory
 * structures.
 ***************************************************************************/
static void
ds_shutdown (DataStream *datastream, char *ident)
{
  DataStreamGroup *curgroup = NULL;
  DataStreamGroup *prevgroup = NULL;

  curgroup = datastream->grouproot;

  while (curgroup != NULL)
  {
    prevgroup = curgroup;
    curgroup = curgroup->next;

    lprintf (3, "[%s] Shutting down stream with key: %s (%s)",
             ident, prevgroup->defkey, prevgroup->postpath);

    if (prevgroup->filed)
      if (close (prevgroup->filed))
        lprintf (0, "[%s] ds_shutdown(), closing data stream file, %s",
                 ident, strerror (errno));

    free (prevgroup->defkey);
    free (prevgroup);
  }
} /* End of ds_shutdown() */

/*************************************************************************
 * Parse/split a string on a specified delimiter
 *
 * Splits a 'string' on 'delim' and puts each part into a linked list
 * pointed to by 'list' (a pointer to a pointer).  The last entry has
 * it's 'next' set to 0.  All elements are NULL terminated strings.
 *
 * It is up to the caller to free the memory associated with the
 * returned list.  To facilitate freeing this special string list
 * ds_strparse() can be called with both 'string' and 'delim' set to
 * NULL and then the linked list is traversed and the memory used is
 * free'd and the list pointer is set to NULL.
 *
 * string - String to parse/split
 * delim  - Delimiter to split string on
 * list   - Returned list of sub-strings
 *
 * Returns the number of elements added to the list, or 0 when freeing
 * the linked list.
 ***************************************************************************/
int
ds_strparse (const char *string, const char *delim, DSstrlist **list)
{
  const char *beg; /* beginning of element */
  const char *del; /* delimiter */
  int stop = 0;
  int count = 0;
  int total;

  DSstrlist *curlist = 0;
  DSstrlist *tmplist = 0;

  if (string != NULL && delim != NULL)
  {
    total = strlen (string);
    beg = string;

    while (!stop)
    {

      /* Find delimiter */
      del = strstr (beg, delim);

      /* Delimiter not found or empty */
      if (del == NULL || strlen (delim) == 0)
      {
        del = string + strlen (string);
        stop = 1;
      }

      tmplist = (DSstrlist *)malloc (sizeof (DSstrlist));
      tmplist->next = 0;

      tmplist->element = (char *)malloc (del - beg + 1);
      strncpy (tmplist->element, beg, (del - beg));
      tmplist->element[(del - beg)] = '\0';

      /* Add this to the list */
      if (count++ == 0)
      {
        curlist = tmplist;
        *list = curlist;
      }
      else
      {
        curlist->next = tmplist;
        curlist = curlist->next;
      }

      /* Update 'beg' */
      beg = (del + strlen (delim));
      if ((beg - string) > total)
        break;
    }

    return count;
  }
  else
  {
    curlist = *list;
    while (curlist != NULL)
    {
      tmplist = curlist->next;
      free (curlist->element);
      free (curlist);
      curlist = tmplist;
    }
    *list = NULL;

    return 0;
  }
} /* End of ds_strparse() */

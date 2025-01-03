/***************************************************************************
 * mseedscan.c
 *
 * miniSEED scanning thread specific routines.
 *
 * Recursively scan one or more directory structures and continuously
 * check for file modifications.  The files are presumed to be
 * composed of miniSEED records.  As the files are appended or created
 * the new records will be read and inserted into the ring.
 *
 * The file scanning logic employs the concept of active, idle and
 * quiet files.  Files that have not been modified for specified
 * amounts of time are considered idle or quiet.  The idle files will
 * not be checked on every scan, instead a specified number of scans
 * will be skipped for each idle file.  The quiet files will never be
 * checked ever again.
 *
 * If a file is scanned and a valid miniSEED record was not read from
 * the file a placeholder will be kept but the file will not be read
 * from again.  This way files which do not contain miniSEED will
 * only be reported once.  This placeholder will be retained in a
 * statefile (if used), this means that the file will not be scanned
 * if the program is re-started.  In order to get the plugin to
 * re-scan a file it must be removed from the state file.
 *
 * A balanced binary-tree is used to keep track of the files processed
 * and allows for operation with 100,000s of files.
 *
 * Broken symbolic links are quietly skipped.  If they are eventually
 * re-connected to something the something will be scanned as
 * expected.
 *
 * The directory separator is assumed to be '/'.
 *
 * If the signal SIGUSR1 is received the program will print the
 * current file list to standard error.
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
 ***************************************************************************/

/* _GNU_SOURCE needed to get pread() under Linux */
#define _GNU_SOURCE

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <libmseed.h>

#include "generic.h"
#include "logging.h"
#include "mseedscan.h"
#include "rbtree.h"
#include "ring.h"
#include "ringserver.h"
#include "stack.h"

#define MSSCAN_MINRECLEN 40
#define MSSCAN_READLEN 128

/* The FileKey and FileNode structures form the key and data elements
 * of a balanced tree that is used to keep track of all files being
 * processed.
 */

/* Structure used as the key for B-tree of file entries (FileNode) */
typedef struct filekey
{
  ino_t inode;      /* Inode number, assumed to be unsigned */
  char filename[1]; /* File name, sized appropriately */
} FileKey;

/* Structure used as the data for B-tree of file entries */
typedef struct filenode
{
  off_t offset;    /* Last file read offset, must be signed */
  time_t modtime;  /* Last file modification time, assumed to be signed */
  time_t scantime; /* Last file scan time */
  int idledelay;   /* Idle file scan iteration delay */
} FileNode;

typedef struct EDIR_s
{
  struct edirent *ents;
  struct edirent *current;
} EDIR;

struct edirent
{
  struct edirent *prev;
  struct edirent *next;
  ino_t d_ino;
  char d_name[1024];
};

static int ScanFiles (MSScanInfo *mssinfo, char *targetdir, int level, time_t scantime);
static FileNode *FindFile (RBTree *filetree, FileKey *fkey);
static FileNode *AddFile (RBTree *filetree, ino_t inode, char *filename, time_t modtime);
static off_t ProcessFile (MSScanInfo *mssinfo, char *filename, FileNode *fnode,
                          off_t newsize, time_t newmodtime);
static void PruneFiles (RBTree *filetree, time_t scantime);
static void PrintFileList (RBTree *filetree, FILE *fd);
static int SaveState (RBTree *filetree, char *statefile);
static int RecoverState (RBTree *filetree, char *statefile);
static int WriteRecord (MSScanInfo *mssinfo, char *record, uint64_t reclen);
static int Initialize (MSScanInfo *mssinfo);
static int MSS_KeyCompare (const void *a, const void *b);
static time_t CalcDayTime (int year, int day);
static time_t BudFileDayTime (char *filename);

static int SortEDirEntries (EDIR *edirp);
static EDIR *EOpenDir (const char *dirname);
static struct edirent *EReadDir (EDIR *edirp);
static int ECloseDir (EDIR *edirp);

/***********************************************************************
 * MS_ScanThread:
 *
 * Thread to perform miniSEED scanning.
 *
 * Returns NULL.
 ***********************************************************************/
void *
MS_ScanThread (void *arg)
{
  struct thread_data *mytdp;
  MSScanInfo *mssinfo;
  time_t scantime;
  time_t statetime;
  struct timeval scanstarttime;
  struct timeval scanendtime;
  struct timespec treq, treq0, trem;
  struct stat st;
  double iostatsinterval;
  int iostatscount = 0;
  int scanerror    = 0;

  mytdp   = (struct thread_data *)arg;
  mssinfo = (MSScanInfo *)mytdp->td_prvtptr;

  mssinfo->filetree = RBTreeCreate (MSS_KeyCompare, free, free);

  /* Initialize scanning parameters */
  if (Initialize (mssinfo) < 0)
  {
    pthread_mutex_lock (&(mytdp->td_lock));
    mytdp->td_state = TDS_CLOSED;
    pthread_mutex_unlock (&(mytdp->td_lock));

    return 0;
  }

  /* Initilize timers */
  treq.tv_sec   = (time_t)mssinfo->scansleep;
  treq.tv_nsec  = (long)0;
  treq0.tv_sec  = (time_t)mssinfo->scansleep0;
  treq0.tv_nsec = (long)0;

  statetime = time (NULL);

  if (mssinfo->iostats)
    iostatscount = mssinfo->iostats;

  /* Initialize rate variables */
  mssinfo->rxpackets[0] = mssinfo->rxpackets[1] = 0;
  mssinfo->rxbytes[0] = mssinfo->rxbytes[1] = 0;
  mssinfo->rxpacketrate                     = 0.0;
  mssinfo->rxbyterate                       = 0.0;
  mssinfo->scantime                         = 0.0;
  mssinfo->ratetime                         = NSnow ();

  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if (mytdp->td_state == TDS_SPAWNING)
    mytdp->td_state = TDS_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));

  /* Report start of scanning */
  lprintf (1, "miniSEED scanning started [%s]", mssinfo->dirname);

  /* Start scan sequence */
  while (mytdp->td_state != TDS_CLOSE && scanerror == 0)
  {
    scantime                 = time (NULL);
    mssinfo->scanrecordsread = 0;

    if (mssinfo->iostats && mssinfo->iostats == iostatscount)
    {
      gettimeofday (&scanstarttime, NULL);
      mssinfo->scanfileschecked   = 0;
      mssinfo->scanfilesread      = 0;
      mssinfo->scanrecordswritten = 0;
    }

    /* Check for base directory existence */
    if (lstat (mssinfo->dirname, &st) < 0)
    {
      /* Only log error if this is a change */
      if (mssinfo->accesserr == 0)
        lprintf (0, "[MSeedScan] WARNING Cannot read base directory [%s]: %s",
                 mssinfo->dirname, strerror (errno));

      mssinfo->accesserr = 1;
    }
    else if (!S_ISDIR (st.st_mode))
    {
      /* Only log error if this is a change */
      if (mssinfo->accesserr == 0)
        lprintf (0, "[MSeedScan] WARNING Base directory is not a directory [%s]",
                 mssinfo->dirname);

      mssinfo->accesserr = 1;
    }
    else
    {
      if (mssinfo->accesserr == 1)
        mssinfo->accesserr = 0;

      if (ScanFiles (mssinfo, mssinfo->dirname, mssinfo->maxrecur, scantime) == -2)
        scanerror = 1;
    }

    if (mytdp->td_state != TDS_CLOSE && !scanerror)
    {
      /* Prune files that were not found from the filelist */
      PruneFiles (mssinfo->filetree, scantime);

      /* Save intermediate state file */
      if (*(mssinfo->statefile) && mssinfo->stateint && (scantime - statetime) > mssinfo->stateint)
      {
        SaveState (mssinfo->filetree, mssinfo->statefile);
        statetime = scantime;
      }

      /* Reset the next new flag, the first scan is now complete */
      if (mssinfo->nextnew)
        mssinfo->nextnew = 0;

      /* Sleep for specified interval */
      if (mssinfo->scansleep)
        nanosleep (&treq, &trem);

      /* Sleep for specified interval if no records were read */
      if (mssinfo->scansleep0 && mssinfo->scanrecordsread == 0)
        nanosleep (&treq0, &trem);
    }

    /* Rate calculation */
    if (mssinfo->rxpackets[0] > 0)
    {
      nstime_t nsnow = NSnow ();

      /* Calculate scan duration in seconds and make sure it's not zero */
      mssinfo->scantime = (double)(nsnow - mssinfo->ratetime) / NSTMODULUS;
      if (mssinfo->scantime == 0.0)
        mssinfo->scantime = 1.0;

      /* Calculate the reception rates */
      mssinfo->rxpacketrate = (double)(mssinfo->rxpackets[0] - mssinfo->rxpackets[1]) / mssinfo->scantime;
      mssinfo->rxbyterate   = (double)(mssinfo->rxbytes[0] - mssinfo->rxbytes[1]) / mssinfo->scantime;

      /* Shift current values to history values */
      mssinfo->rxpackets[1] = mssinfo->rxpackets[0];
      mssinfo->rxbytes[1]   = mssinfo->rxbytes[0];

      mssinfo->ratetime = nsnow;
    }

    if (mssinfo->iostats && iostatscount <= 1)
    {
      /* Determine run time since scanstarttime was set */
      gettimeofday (&scanendtime, NULL);

      iostatsinterval = (((double)scanendtime.tv_sec + (double)scanendtime.tv_usec / 1000000) -
                         ((double)scanstarttime.tv_sec + (double)scanstarttime.tv_usec / 1000000));

      lprintf (0, "[MSeedScan] Time: %g seconds for %d scan(s) (%g seconds/scan)",
               iostatsinterval, mssinfo->iostats,
               iostatsinterval / mssinfo->iostats);
      lprintf (0, "[MSeedScan] Files checked: %d, read: %d (%g read/sec)",
               mssinfo->scanfileschecked, mssinfo->scanfilesread,
               mssinfo->scanfilesread / iostatsinterval);
      lprintf (0, "[MSeedScan] Records read: %d, written: %d (%g written/sec)",
               mssinfo->scanrecordsread, mssinfo->scanrecordswritten,
               mssinfo->scanrecordswritten / iostatsinterval);

      /* Reset counter */
      iostatscount = mssinfo->iostats;
    }
    else if (mssinfo->iostats)
    {
      iostatscount--;
    }
  } /* End of main scan loop */

  /* Set thread closing status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_state = TDS_CLOSING;
  pthread_mutex_unlock (&(mytdp->td_lock));

  /* Save the state file */
  if (*(mssinfo->statefile) != '\0')
    SaveState (mssinfo->filetree, mssinfo->statefile);

  /* Release file tracking binary tree */
  if (mssinfo->filetree)
    RBTreeDestroy (mssinfo->filetree);

  /* Free regex matching memory */
  if (mssinfo->fnmatch)
    pcre2_code_free (mssinfo->fnmatch);
  if (mssinfo->fnmatch_data)
    pcre2_match_data_free (mssinfo->fnmatch_data);
  if (mssinfo->fnreject)
    pcre2_code_free (mssinfo->fnreject);
  if (mssinfo->fnreject_data)
    pcre2_match_data_free (mssinfo->fnreject_data);

  if (mssinfo->readbuffer)
    free (mssinfo->readbuffer);

  if (mssinfo->msr)
    msr3_free (&(mssinfo->msr));

  lprintf (1, "miniSEED scanning stopped [%s]", mssinfo->dirname);

  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_state = TDS_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));

  return 0;
} /* End of MS_ScanThread() */

/***************************************************************************
 * ScanFiles:
 *
 * Scan a directory and recursively drop into sub-directories up to the
 * maximum recursion level.
 *
 * When initially called the targetdir and basedir should be the same,
 * either an absolute or relative path.  During directory recursion
 * these values are maintained such that they are either absolute
 * paths or targetdir is a relatively path from the current working
 * directory (which changes during recursion) and basedir is a
 * relative path from the initial directory.
 *
 * If the FileNode->offset of an existing entry is -1 skip the file,
 * this happens when there was trouble reading the file, the contents
 * were not miniSEED on a previous attempt, etc.
 *
 * Return 0 on success and -1 on error and -2 on fatal error.
 ***************************************************************************/
static int
ScanFiles (MSScanInfo *mssinfo, char *targetdir, int level, time_t scantime)
{
  FileNode *fnode;
  FileKey *fkey;
  char filekeybuf[sizeof (FileKey) + MSSCAN_MAXFILENAME]; /* Room for fkey */
  struct stat st;
  struct edirent *ede;
  time_t currentday = 0;
  EDIR *dir;

  fkey = (FileKey *)&filekeybuf;

  lprintf (3, "[MSeedScan] Processing directory '%s'", targetdir);

  if ((dir = EOpenDir (targetdir)) == NULL)
  {
    lprintf (0, "[MSeedScan] Cannot open directory %s: %s", targetdir, strerror (errno));

    return -1;
  }

  if (mssinfo->budlatency)
  {
    struct tm cday;
    gmtime_r (&scantime, &cday);
    currentday = CalcDayTime (cday.tm_year + 1900, cday.tm_yday + 1);
  }

  while (param.shutdownsig == 0 && (ede = EReadDir (dir)) != NULL)
  {
    int filenamelen;

    /* Skip "." and ".." entries */
    if (!strcmp (ede->d_name, ".") || !strcmp (ede->d_name, ".."))
      continue;

    /* BUD file name latency check */
    if (mssinfo->budlatency)
    {
      time_t budfiletime = BudFileDayTime (ede->d_name);

      /* Skip this file if the BUD file name is more than budlatency days old */
      if (budfiletime && ((currentday - budfiletime) > (mssinfo->budlatency * 86400)))
      {
        if (verbose > 1)
          lprintf (0, "[MSeedScan] Skipping due to BUD file name latency: %s", ede->d_name);
        continue;
      }
    }

    /* Build a FileKey for this file */
    fkey->inode = ede->d_ino;
    filenamelen = snprintf (fkey->filename, sizeof (filekeybuf) - sizeof (FileKey),
                            "%s/%s", targetdir, ede->d_name);

    /* Make sure the filename was not truncated */
    if (filenamelen >= (sizeof (filekeybuf) - sizeof (FileKey) - 1))
    {
      lprintf (0, "[MSeedScan] Directory entry name beyond maximum of %lu characters, skipping:",
               (sizeof (filekeybuf) - sizeof (FileKey) - 1));
      lprintf (0, "  %s", ede->d_name);
      continue;
    }

    /* Search for a matching entry in the filetree */
    if ((fnode = FindFile (mssinfo->filetree, fkey)))
    {
      /* Check if the file is permanently skipped */
      if (fnode->offset == -1)
      {
        fnode->scantime = scantime;
        continue;
      }

      /* Check if file has triggered a delayed check */
      if (fnode->idledelay > 0)
      {
        fnode->scantime = scantime;
        fnode->idledelay--;
        continue;
      }
    }

    /* Stat the file */
    if (lstat (fkey->filename, &st) < 0)
    {
      if (!(param.shutdownsig && errno == EINTR))
        lprintf (0, "[MSeedScan] Cannot stat %s: %s", fkey->filename, strerror (errno));
      continue;
    }

    /* If symbolic link stat the real file, if it's a broken link continue */
    if (S_ISLNK (st.st_mode))
    {
      if (stat (fkey->filename, &st) < 0)
      {
        /* Interruption signals when the stop signal is set should break out */
        if (param.shutdownsig && errno == EINTR)
          break;

        /* Log an error if the error is anything but a disconnected link */
        if (errno != ENOENT)
          lprintf (0, "[MSeedScan] Cannot stat (linked) %s: %s", fkey->filename, strerror (errno));
        else
          continue;
      }
    }

    /* If directory recurse up to the limit */
    if (S_ISDIR (st.st_mode))
    {
      if (mssinfo->maxrecur < 0 || mssinfo->recurlevel < level)
      {
        lprintf (4, "[MSeedScan] Recursing into %s", fkey->filename);

        mssinfo->recurlevel++;
        if (ScanFiles (mssinfo, fkey->filename, level, scantime) == -2)
          return -2;
        mssinfo->recurlevel--;
      }
      continue;
    }

    /* Increment files found counter */
    if (mssinfo->iostats)
      mssinfo->scanfileschecked++;

    /* Do regex matching if an expression was specified */
    if (mssinfo->fnmatch)
      if (pcre2_match (mssinfo->fnmatch, (PCRE2_SPTR8)ede->d_name, PCRE2_ZERO_TERMINATED, 0, 0,
                       mssinfo->fnmatch_data, NULL) < 0)
        continue;

    /* Do regex rejecting if an expression was specified */
    if (mssinfo->fnreject)
      if (pcre2_match (mssinfo->fnreject, (PCRE2_SPTR8)ede->d_name, PCRE2_ZERO_TERMINATED, 0, 0,
                       mssinfo->fnreject_data, NULL) >= 0)
        continue;

    /* Sanity check for a regular file */
    if (!S_ISREG (st.st_mode))
    {
      lprintf (0, "[MSeedScan] %s is not a regular file", fkey->filename);
      continue;
    }

    /* Sanity check that the dirent inode and stat inode are the same */
    if (st.st_ino != ede->d_ino)
    {
      lprintf (0, "[MSeedScan] Inode numbers from dirent and stat do not match for %s\n", fkey->filename);
      lprintf (0, "  dirent: %llu  VS  stat: %llu\n",
               (unsigned long long int)ede->d_ino, (unsigned long long int)st.st_ino);
      continue;
    }

    lprintf (3, "[MSeedScan] Checking file %s", fkey->filename);

    /* If the file has never been seen add it to the list */
    if (!fnode)
    {
      /* Add new file to tree setting modtime to one second in the
       * past so we are triggered to look at this file the first time. */
      if (!(fnode = AddFile (mssinfo->filetree, fkey->inode, fkey->filename, st.st_mtime - 1)))
      {
        lprintf (0, "[MSeedScan] Error adding %s to file list", fkey->filename);
        continue;
      }
    }

    /* Only update the offset if skipping the first scan, otherwise process */
    if (mssinfo->nextnew)
      fnode->offset = st.st_size;

    /* Check if the file is quiet and mark to always skip if true */
    if (mssinfo->quietsec && st.st_mtime < (scantime - mssinfo->quietsec))
    {
      lprintf (2, "[MSeedScan] Marking file as quiet, no processing: %s", fkey->filename);
      fnode->offset = -1;
    }

    /* Otherwise check if the file is idle and set idledelay appropriately */
    else if (mssinfo->idledelay && fnode->idledelay == 0 &&
             st.st_mtime < (scantime - mssinfo->idlesec))
    {
      lprintf (2, "[MSeedScan] Marking file as idle, will not check for %d scans: %s",
               mssinfo->idledelay, fkey->filename);
      fnode->idledelay = (mssinfo->idledelay > 0) ? (mssinfo->idledelay - 1) : 0;
    }

    /* Process (read records from) the file if it's size has increased and
     * is not marked for permanent skipping */
    if (fnode->offset < st.st_size && fnode->offset != -1)
    {
      /* Increment files read counter */
      if (mssinfo->iostats)
        mssinfo->scanfilesread++;

      fnode->offset = ProcessFile (mssinfo, fkey->filename, fnode, st.st_size, st.st_mtime);

      /* If a proper file read but fatal error occured set the offset correctly
         and return a fatal error. */
      if (fnode->offset < -1)
      {
        fnode->offset = -fnode->offset;
        return -2;
      }
    }

    /* Update scantime */
    fnode->scantime = scantime;
  }

  ECloseDir (dir);

  return 0;
} /* End of ScanFiles() */

/***************************************************************************
 * FindFile:
 *
 * Search the filetree for a given FileKey.
 *
 * Return a pointer to a FileNode if found or 0 if no match found.
 ***************************************************************************/
static FileNode *
FindFile (RBTree *filetree, FileKey *fkey)
{
  FileNode *fnode = NULL;
  RBNode *tnode;

  /* Search for a matching inode + file name entry */
  if ((tnode = RBFind (filetree, fkey)))
  {
    fnode = (FileNode *)tnode->data;
  }

  return fnode;
} /* End of FindFile() */

/***************************************************************************
 * AddFile:
 *
 * Add a file to the file tree, no checking is done to determine if
 * this entry already exists.
 *
 * Return a pointer to the added FileNode on success and 0 on error.
 ***************************************************************************/
static FileNode *
AddFile (RBTree *filetree, ino_t inode, char *filename, time_t modtime)
{
  FileKey *newfkey;
  FileNode *newfnode;
  size_t filelen;

  lprintf (1, "[MSeedScan] Adding %s", filename);

  /* Create new tree key */
  filelen = strlen (filename);
  newfkey = (FileKey *)malloc (sizeof (FileKey) + filelen);

  /* Create new tree node */
  newfnode = (FileNode *)malloc (sizeof (FileNode));

  if (!newfkey || !newfnode)
    return 0;

  /* Populate the new key and node */
  newfkey->inode = inode;
  memcpy (newfkey->filename, filename, filelen + 1);

  newfnode->offset    = 0;
  newfnode->modtime   = modtime;
  newfnode->idledelay = 0;

  RBTreeInsert (filetree, newfkey, newfnode, 0);

  return newfnode;
} /* End of AddFile() */

/***************************************************************************
 * ProcessFile:
 *
 * Process a file by reading any data after the last offset.
 *
 * Return the new file offset on success and -1 on error reading file
 * and the negated file offset on successful read but fatal write (to ring)
 * error.
 ***************************************************************************/
static off_t
ProcessFile (MSScanInfo *mssinfo, char *filename, FileNode *fnode,
             off_t newsize, time_t newmodtime)
{
  ssize_t nread;
  ssize_t detlen;
  int fd;
  int reccnt     = 0;
  int reachedmax = 0;
  int flags;
  uint8_t msversion;
  off_t newoffset = fnode->offset;
  struct timespec treq, trem;

  lprintf (3, "[MSeedScan] Processing file %s", filename);

  /* Set the throttling sleep time */
  treq.tv_sec  = (time_t)0;
  treq.tv_nsec = (long)mssinfo->throttlensec;

/* Set open flags */
#if defined(__sun__) || defined(__sun)
  flags = O_RDONLY | O_RSYNC;
#else
  flags = O_RDONLY;
#endif

  /* Open target file */
  if ((fd = open (filename, flags, 0)) == -1)
  {
    if (!(param.shutdownsig && errno == EINTR))
    {
      lprintf (0, "[MSeedScan] Error opening %s: %s", filename, strerror (errno));
      return -1;
    }
    else
      return newoffset;
  }

  /* Read and process data while minimum record length is available */
  while ((newsize - newoffset) >= MSSCAN_MINRECLEN)
  {
    /* Jump out if we've read the maximum allowed number of records
       from this file for this scan */
    if (mssinfo->filemaxrecs && reccnt >= mssinfo->filemaxrecs)
    {
      reachedmax = 1;
      break;
    }

    /* Read up to MSSCAN_READLEN bytes into buffer */
    if ((nread = pread (fd, mssinfo->readbuffer, MSSCAN_READLEN, newoffset)) <= 0)
    {
      if (!(param.shutdownsig && errno == EINTR))
      {
        lprintf (0, "[MSeedScan] Error: cannot read data from %s", filename);
        close (fd);
        return -1;
      }
      else
      {
        return newoffset;
      }
    }

    /* Check buffer for miniSEED */
    detlen = ms3_detect (mssinfo->readbuffer, (uint64_t)nread, &msversion);

    /* If miniSEED not detected or length could not be determined */
    if (detlen <= 0)
    {
      /* If no data has ever been read from this file, ignore file */
      if (fnode->offset == 0)
      {
        lprintf (0, "[MSeedScan] %s: Not a valid miniSEED record at offset %lld, ignoring file",
                 filename, (long long)newoffset);
        close (fd);
        return -1;
      }
      /* Otherwise, if records have been read, skip until next scan */
      else
      {
        lprintf (0, "[MSeedScan] %s: Not a valid miniSEED record at offset %lld (new bytes %lld), skipping for this scan",
                 filename, (long long)newoffset, (long long)(newsize - newoffset));
        close (fd);
        return newoffset;
      }
    }
    /* Record is larger than packet payload maximum, aka read buffer size */
    else if (detlen > mssinfo->readbuffersize)
    {
      lprintf (0, "[MSeedScan] %s: Record length (%" PRId64 ") at offset %" PRId64 ", larger than packet payload size (%u), ignoring file",
               filename, (int64_t)detlen, (int64_t)newoffset, mssinfo->readbuffersize);
      close (fd);
      return -1;
    }
    /* File does not contain whole record, done for now */
    else if (detlen > (newsize - newoffset))
    {
      return newoffset;
    }

    /* Read the rest of the record */
    if (detlen > nread)
    {
      if (pread (fd, mssinfo->readbuffer + nread,
                 (size_t)(detlen - nread),
                 newoffset + nread) != detlen - nread)
      {
        if (!(param.shutdownsig && errno == EINTR))
        {
          lprintf (0, "[MSeedScan] Error: cannot read remaining record data from %s", filename);
          close (fd);
          return -1;
        }
        else
        {
          return newoffset;
        }
      }
    }

    newoffset += detlen;

    /* Increment records read counter */
    mssinfo->scanrecordsread++;

    /* Write record to ring buffer */
    if (WriteRecord (mssinfo, mssinfo->readbuffer, (uint64_t)detlen))
    {
      return -newoffset;
    }

    /* Increment records written counter */
    if (mssinfo->iostats)
    {
      mssinfo->scanrecordswritten++;
    }

    /* Sleep for specified throttle interval */
    if (mssinfo->throttlensec)
    {
      nanosleep (&treq, &trem);
    }

    reccnt++;
  }

  close (fd);

  /* Update modtime:
   * If the maximum number of records have been reached we are not necessarily
   * at the end of the file so set the modtime one second in the past so we are
   * triggered to look at this file again. */
  if (reachedmax)
    fnode->modtime = newmodtime - 1;
  else
    fnode->modtime = newmodtime;

  if (reccnt)
  {
    lprintf (2, "[MSeedScan] Read %d record(s) from %s", reccnt, filename);
  }

  return newoffset;
} /* End of ProcessFile() */

/***************************************************************************
 * PruneFiles:
 *
 * Prune file entries from the filetree that do not have a check time
 * later than that specified.
 *
 ***************************************************************************/
static void
PruneFiles (RBTree *filetree, time_t scantime)
{
  FileNode *fnode;
  FileKey *fkey;
  RBNode *tnode;
  Stack *stack;

  stack = StackCreate ();
  RBBuildStack (filetree, stack);

  while ((tnode = (RBNode *)StackPop (stack)))
  {
    fkey  = (FileKey *)tnode->key;
    fnode = (FileNode *)tnode->data;

    if (fnode->scantime < scantime)
    {
      lprintf (1, "[MSeedScan] Removing %s from file list", fkey->filename);

      RBDelete (filetree, tnode);
    }
  }

  StackDestroy (stack, free);
} /* End of PruneFiles() */

/***************************************************************************
 * PrintFileList:
 *
 * Print file tree to the specified descriptor.
 ***************************************************************************/
static void
PrintFileList (RBTree *filetree, FILE *fp)
{
  FileKey *fkey;
  FileNode *fnode;
  RBNode *tnode;
  Stack *stack;

  stack = StackCreate ();
  RBBuildStack (filetree, stack);

  while ((tnode = (RBNode *)StackPop (stack)))
  {
    fkey  = (FileKey *)tnode->key;
    fnode = (FileNode *)tnode->data;

    fprintf (fp, "%s\t%llu\t%lld\t%lld\n",
             fkey->filename,
             (unsigned long long int)fkey->inode,
             (signed long long int)fnode->offset,
             (signed long long int)fnode->modtime);
  }

  StackDestroy (stack, free);
} /* End of PrintFileList() */

/***************************************************************************
 * SaveState:
 *
 * Save state information to a specified file.  First the new state
 * file is written to a temporary file (the same statefile name with a
 * ".tmp" extension) then rename the temporary file.  This avoids
 * partial writes of the state file if the program is killed while
 * writing the state file.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SaveState (RBTree *filetree, char *statefile)
{
  char tmpstatefile[255];
  int fnsize;
  FILE *fp;

  lprintf (2, "[MSeedScan] Saving state file");

  /* Create temporary state file */
  fnsize = snprintf (tmpstatefile, sizeof (tmpstatefile), "%s.tmp", statefile);

  /* Check for truncation */
  if (fnsize >= sizeof (tmpstatefile))
  {
    lprintf (0, "[MSeedScan] Error, temporary statefile name too long (%d bytes)", fnsize);
    return -1;
  }

  /* Open temporary state file */
  if ((fp = fopen (tmpstatefile, "w")) == NULL)
  {
    lprintf (0, "[MSeedScan] Error opening temporary statefile %s: %s",
             tmpstatefile, strerror (errno));
    return -1;
  }

  /* Write file list to temporary state file */
  PrintFileList (filetree, fp);

  fclose (fp);

  /* Rename temporary state file overwriting the current state file */
  if (rename (tmpstatefile, statefile))
  {
    lprintf (0, "[MSeedScan] Error renaming temporary statefile %s->%s: %s",
             tmpstatefile, statefile, strerror (errno));
    return -1;
  }

  return 0;
} /* End of SaveState() */

/***************************************************************************
 * RecoverState:
 *
 * Recover the state information from the state file.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
RecoverState (RBTree *filetree, char *statefile)
{
  FileNode *fnode;
  char line[600];
  int fields, count;
  FILE *fp;

  char filename[MSSCAN_MAXFILENAME];
  unsigned long long int inode;
  signed long long int offset, modtime;

  if ((fp = fopen (statefile, "r")) == NULL)
  {
    lprintf (0, "[MSeedScan] Error opening statefile %s: %s", statefile, strerror (errno));
    return -1;
  }

  lprintf (1, "[MSeedScan] Recovering state");

  count = 1;

  while ((fgets (line, sizeof (line), fp)) != NULL)
  {
    fields = sscanf (line, "%s %llu %lld %lld\n",
                     filename, &inode, &offset, &modtime);

    if (fields < 0)
      continue;

    if (fields < 4)
    {
      lprintf (0, "[MSeedScan] Could not parse line %d of state file", count);
      continue;
    }

    fnode = AddFile (filetree, (ino_t)inode, filename, (time_t)modtime);

    if (fnode)
    {
      fnode->offset   = (off_t)offset;
      fnode->scantime = 0;
    }

    count++;
  }

  fclose (fp);

  return 0;
} /* End of RecoverState() */

/***************************************************************************
 * WriteRecord:
 *
 * Send the specified record to the DataLink server.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
WriteRecord (MSScanInfo *mssinfo, char *record, uint64_t reclen)
{
  char streamid[100];
  RingPacket packet;
  uint32_t flags = MSF_VALIDATECRC;
  int rv;

  /* Parse miniSEED header */
  if ((rv = msr3_parse (record, reclen, &(mssinfo->msr), flags, 0)) != MS_NOERROR)
  {
    lprintf (0, "[MSeedScan] Error unpacking record: %s", ms_errorstr (rv));
    return -1;
  }

  /* Generate stream ID for this record: SourceID/MSEED */
  snprintf (streamid, sizeof (streamid), "%s/MSEED", mssinfo->msr->sid);

  memset (&packet, 0, sizeof (RingPacket));
  memcpy (packet.streamid, streamid, sizeof (packet.streamid) - 1);
  packet.datastart = mssinfo->msr->starttime;
  packet.dataend   = msr3_endtime (mssinfo->msr);
  packet.datasize  = (uint32_t)mssinfo->msr->reclen;
  packet.pktid     = RINGID_NONE;

  /* Add the packet to the ring */
  if ((rv = RingWrite (mssinfo->ringparams, &packet, record, packet.datasize)))
  {
    if (rv == -2)
      lprintf (1, "[MSeedScan] Error with RingWrite, corrupt ring, shutdown signalled");
    else
      lprintf (1, "[MSeedScan] Error with RingWrite");

    /* Set the shutdown signal if ring corruption was detected */
    if (rv == -2)
      param.shutdownsig = 1;

    return -1;
  }

  /* Update client receive counts */
  mssinfo->rxpackets[0]++;
  mssinfo->rxbytes[0] += packet.datasize;

  return 0;
} /* End of WriteRecord() */

/***************************************************************************
 * Initialize:
 *
 * Compile regex'es, allocate buffers, and recover state.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
Initialize (MSScanInfo *mssinfo)
{
  /* Compile the match regex if specified */
  if (*(mssinfo->matchstr) != '\0' &&
      UpdatePattern (&mssinfo->fnmatch, &mssinfo->fnmatch_data,
                     mssinfo->matchstr, "msscan filename match expression"))
  {
    return -1;
  }

  /* Compile the reject regex if specified */
  if (*(mssinfo->rejectstr) != '\0' &&
      UpdatePattern (&mssinfo->fnreject, &mssinfo->fnreject_data,
                     mssinfo->rejectstr, "msscan filename reject expression"))
  {
    return -1;
  }

  /* Calculate maximum allowed record length and allocate file read buffer */
  mssinfo->readbuffersize = mssinfo->ringparams->pktsize - sizeof (RingPacket);
  if ((mssinfo->readbuffer = (char *)malloc (mssinfo->readbuffersize)) == NULL)
  {
    lprintf (0, "[MSeedScan] Cannot allocate file read buffer");
    return -1;
  }

  /* Attempt to recover sequence numbers from state file */
  if (*(mssinfo->statefile) != '\0')
  {
    if (RecoverState (mssinfo->filetree, mssinfo->statefile) < 0)
    {
      lprintf (0, "[MSeedScan] State recovery failed for %s", mssinfo->dirname);
    }
  }

  return 0;
} /* End of Initialize() */

/***************************************************************************
 * MSS_KeyCompare:
 *
 * Compare two FileKeys passed as void pointers.
 *
 * Return 1 if a > b, -1 if a < b and 0 otherwise (e.g. equality).
 ***************************************************************************/
static int
MSS_KeyCompare (const void *a, const void *b)
{
  int cmpval;

  /* Compare Inode values */
  if (((FileKey *)a)->inode > ((FileKey *)b)->inode)
    return 1;

  else if (((FileKey *)a)->inode < ((FileKey *)b)->inode)
    return -1;

  /* Compare filename values */
  cmpval = strcmp (((FileKey *)a)->filename, ((FileKey *)b)->filename);

  if (cmpval > 0)
    return 1;
  else if (cmpval < 0)
    return -1;

  return 0;
} /* End of MSS_KeyCompare() */

/***************************************************************************
 * CalcDayTime:
 *
 * Calculate an epoch time for a given year and day of year.
 *
 * Return epoch time.
 ***************************************************************************/
static time_t
CalcDayTime (int year, int day)
{
  time_t daytime;
  int shortyear;
  int a4, a100, a400;
  int intervening_leap_days;
  int days;

  shortyear = year - 1900;

  a4                    = (shortyear >> 2) + 475 - !(shortyear & 3);
  a100                  = a4 / 25 - (a4 % 25 < 0);
  a400                  = a100 >> 2;
  intervening_leap_days = (a4 - 492) - (a100 - 19) + (a400 - 4);

  days = (365 * (shortyear - 70) + intervening_leap_days + (day - 1));

  daytime = (time_t)(60 * (60 * (24 * days)));

  return daytime;
} /* End of CalcDayTime() */

/***************************************************************************
 * BudFileDayTime:
 *
 * Calculate an epoch time for the year and day of year from a BUD
 * file name where the name is: <STA>.<NET>.<LOC>.<CHAN>.<YEAR>.<DAY>
 *
 * Return epoch time on success and 0 on error (1 Jan 1970 will not work).
 ***************************************************************************/
static time_t
BudFileDayTime (char *filename)
{
  time_t daytime = 0;
  int iyear, iday, length;

  if (!filename)
    return 0;

  length = strlen (filename);

  if (length < 12)
    return 0;

  /* Check for digits and periods in the expected places at the end: ".YYYY.DDD" */
  if (*(filename + length - 9) == '.' &&
      isdigit (*(filename + length - 8)) && isdigit (*(filename + length - 7)) &&
      isdigit (*(filename + length - 6)) && isdigit (*(filename + length - 5)) &&
      *(filename + length - 4) == '.' &&
      isdigit (*(filename + length - 3)) && isdigit (*(filename + length - 2)) &&
      isdigit (*(filename + length - 1)))
  {
    iyear = strtol ((filename + length - 8), NULL, 10);
    iday  = strtol ((filename + length - 3), NULL, 10);

    daytime = CalcDayTime (iyear, iday);
  }

  return daytime;
} /* End of BudFileDayTime() */

/***************************************************************************
 * SortEDirEntries:
 *
 * Sort enhanced directory entries using the mergesort alorthim.
 * directory entries are compared using the strcmp() function.  The
 * mergesort implementation was inspired by the listsort function
 * published and copyright 2001 by Simon Tatham.
 *
 * Return the number of merges completed on success and -1 on error.
 ***************************************************************************/
static int
SortEDirEntries (EDIR *edirp)
{
  struct edirent *p, *q, *e, *top, *tail;
  int nmerges, totalmerges;
  int insize, psize, qsize, i;

  if (!edirp)
    return -1;

  top         = edirp->ents;
  totalmerges = 0;
  insize      = 1;

  for (;;)
  {
    p    = top;
    top  = NULL;
    tail = NULL;

    nmerges = 0; /* count number of merges we do in this pass */

    while (p)
    {
      nmerges++; /* there exists a merge to be done */
      totalmerges++;

      /* step `insize' places along from p */
      q     = p;
      psize = 0;
      for (i = 0; i < insize; i++)
      {
        psize++;
        q = q->next;
        if (!q)
          break;
      }

      /* if q hasn't fallen off end, we have two lists to merge */
      qsize = insize;

      /* now we have two lists; merge them */
      while (psize > 0 || (qsize > 0 && q))
      {
        /* decide whether next element of merge comes from p or q */
        if (psize == 0)
        { /* p is empty; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }
        else if (qsize == 0 || !q)
        { /* q is empty; e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else if (strcmp (p->d_name, q->d_name) <= 0)
        { /* First element of p is lower (or same), e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else
        { /* First element of q is lower; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }

        /* add the next element to the merged list */
        if (tail)
          tail->next = e;
        else
          top = e;

        e->prev = tail;
        tail    = e;
      }

      /* now p has stepped `insize' places along, and q has too */
      p = q;
    }

    tail->next = NULL;

    /* If we have done only one merge, we're finished. */
    if (nmerges <= 1) /* allow for nmerges==0, the empty list case */
    {
      edirp->ents = top;

      return totalmerges;
    }

    /* Otherwise repeat, merging lists twice the size */
    insize *= 2;
  }
} /* End of SortEDirEntries() */

/***************************************************************************
 * EOpenDir:
 *
 * Open a directory, read all entries, sort them and return an
 * enhanced directory stream pointer for use with edirread() and
 * edirclose().
 *
 * Return a pointer to an enhanced directory stream on success and
 * NULL on error.
 ***************************************************************************/
static EDIR *
EOpenDir (const char *dirname)
{
  DIR *dirp;
  EDIR *edirp;
  struct dirent *de;
  struct edirent *ede;
  struct edirent *prevede = NULL;
  int namelen;

  if (!dirname)
    return NULL;

  dirp = opendir (dirname);

  if (!dirp)
    return NULL;

  /* Allocate new EDIR */
  if (!(edirp = (EDIR *)malloc (sizeof (EDIR))))
  {
    closedir (dirp);
    return NULL;
  }

  /* Read all directory entries */
  while ((de = readdir (dirp)))
  {
    /* Allocate space for enhanced directory entry */
    if (!(ede = (struct edirent *)calloc (1, sizeof (struct edirent))))
    {
      lprintf (0, "[MSeedScan] Cannot allocate directory memory");
      closedir (dirp);
      ECloseDir (edirp);
      return NULL;
    }

    namelen = strlen (de->d_name);

    if (namelen > sizeof (ede->d_name))
    {
      lprintf (0, "[MSeedScan] directory entry name too long (%d): '%s'", namelen, de->d_name);
      closedir (dirp);
      ECloseDir (edirp);
      return NULL;
    }

    strncpy (ede->d_name, de->d_name, sizeof (ede->d_name));
    ede->d_ino = de->d_ino;
    ede->prev  = prevede;

    /* Add new enhanced directory entry to the list */
    if (prevede == NULL)
    {
      edirp->ents = ede;
    }
    else
    {
      prevede->next = ede;
    }

    prevede = ede;
  }

  closedir (dirp);

  /* Sort directory entries */
  if (SortEDirEntries (edirp) < 0)
  {
    ECloseDir (edirp);
    return NULL;
  }

  /* Set the current entry to the top of the list */
  edirp->current = edirp->ents;

  return edirp;
} /* End of EOpenDir() */

/***************************************************************************
 * EReadDir:
 *
 * Return the next directory entry from the list associated with the
 * EDIR and NULL if no more entries.
 ***************************************************************************/
static struct edirent *
EReadDir (EDIR *edirp)
{
  struct edirent *ede;

  if (!edirp)
    return NULL;

  ede = edirp->current;

  if (edirp->current)
    edirp->current = ede->next;

  return ede;
} /* End of EReadDir() */

/***************************************************************************
 * ECloseDir:
 *
 * Close an enhanced directory stream freeing all associated memory.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
ECloseDir (EDIR *edirp)
{
  struct edirent *ede, *nede;

  if (!edirp)
    return -1;

  ede = edirp->ents;

  /* Loop through associated entries and free them */
  while (ede)
  {
    nede = ede->next;

    free (ede);

    ede = nede;
  }

  free (edirp);

  return 0;
} /* End of ECloseDir() */

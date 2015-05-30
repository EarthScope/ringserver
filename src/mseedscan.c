/***************************************************************************
 * mseedscan.c
 *
 * Mini-SEED scanning thread specific routines.
 *
 * Recursively scan one or more directory structures and continuously
 * check for file modifications.  The files are presumed to be
 * composed of Mini-SEED records of length MSSCAN_RECSIZE.
 * As the files are appended or created the new records will be read
 * and inserted into the ring.
 *
 * The file scanning logic employs the concept of active, idle and
 * quiet files.  Files that have not been modified for specified
 * amounts of time are considered idle or quiet.  The idle files will
 * not be checked on every scan, instead a specified number of scans
 * will be skipped for each idle file.  The quiet files will never be
 * checked ever again.
 *
 * If a file is scanned and a valid Mini-SEED record was not read from
 * the file a placeholder will be kept but the file will not be read
 * from again.  This way files which do not contain Mini-SEED will
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
 * If the signal SIGUSR1 is recieved the program will print the
 * current file list to standard error.
 *
 * Copyright 2015 Chad Trabant, IRIS Data Management Center
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
 * Modified: 2015.074
 ***************************************************************************/

/* _GNU_SOURCE needed to get pread() under Linux */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <ctype.h>

#include <pcre.h>
#include <libmseed.h>

#include "ring.h"
#include "ringserver.h"
#include "rbtree.h"
#include "stack.h"
#include "generic.h"
#include "logging.h"
#include "mseedscan.h"


/* The FileKey and FileNode structures form the key and data elements
 * of a balanced tree that is used to keep track of all files being
 * processed.
 */

/* Structure used as the key for B-tree of file entries (FileNode) */
typedef struct filekey {
  ino_t inode;           /* Inode number, assumed to be unsigned */
  char filename[1];      /* File name, sized appropriately */
} FileKey;

/* Structure used as the data for B-tree of file entries */
typedef struct filenode {
  off_t offset;          /* Last file read offset, must be signed */
  time_t modtime;        /* Last file modification time, assumed to be signed */
  time_t scantime;       /* Last file scan time */
  int idledelay;         /* Idle file scan iteration delay */
} FileNode;

typedef struct EDIR_s {
  struct edirent *ents;
  struct edirent *current;
} EDIR;

struct edirent {
  struct edirent *prev;
  struct edirent *next;
  ino_t d_ino;
  char d_name[1024];
};

static int ScanFiles (MSScanInfo *mssinfo, char *targetdir, int level, time_t scantime);
static FileNode *FindFile (RBTree *filetree, FileKey *fkey);
static FileNode *AddFile (RBTree *filetree, ino_t inode, char *filename, time_t modtime);
static off_t  ProcessFile (MSScanInfo *mssinfo, char *filename, FileNode *fnode,
			   off_t newsize, time_t newmodtime);
static void   PruneFiles (RBTree *filetree, time_t scantime);
static void   PrintFileList (RBTree *filetree, FILE *fd);
static int    SaveState (RBTree *filetree, char *statefile);
static int    RecoverState (RBTree *filetree, char *statefile);
static int    WriteRecord (MSScanInfo *mssinfo, char *record, int reclen);
static int    Initialize (MSScanInfo *mssinfo);
static int    MSS_KeyCompare (const void *a, const void *b);
static time_t CalcDayTime (int year, int day);
static time_t BudFileDayTime (char *filename);

static int SortEDirEntries (EDIR *edirp);
static EDIR *EOpenDir (const char *dirname);
static struct edirent *EReadDir (EDIR *edirp);
static int ECloseDir (EDIR *edirp);


/***********************************************************************
 * MS_ScanThread:
 *
 * Thread to perform Mini-SEED scanning.
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
  int scanerror = 0;
  
  mytdp = (struct thread_data *) arg;
  mssinfo = (MSScanInfo *) mytdp->td_prvtptr;
  
  mssinfo->filetree = RBTreeCreate (MSS_KeyCompare, free, free);
  
  /* Initialize scanning parameters */
  if ( Initialize(mssinfo) < 0 )
    {
      pthread_mutex_lock (&(mytdp->td_lock));
      mytdp->td_flags = TDF_CLOSED;
      pthread_mutex_unlock (&(mytdp->td_lock));
      
      return 0;
    }
  
  /* Initilize timers */
  treq.tv_sec = (time_t) mssinfo->scansleep;
  treq.tv_nsec = (long) 0;
  treq0.tv_sec = (time_t) mssinfo->scansleep0;
  treq0.tv_nsec = (long) 0;
  
  statetime = time(NULL);
  
  if ( mssinfo->iostats )
    iostatscount = mssinfo->iostats;
  
  /* Initialize rate variables */
  mssinfo->rxpackets[0] = mssinfo->rxpackets[1] = 0;
  mssinfo->rxbytes[0] = mssinfo->rxbytes[1] = 0;
  mssinfo->rxpacketrate = 0.0;
  mssinfo->rxbyterate = 0.0;
  mssinfo->scantime = 0.0;
  mssinfo->ratetime = HPnow();
  
  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if ( mytdp->td_flags == TDF_SPAWNING )
    mytdp->td_flags = TDF_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  /* Report start of scanning */
  lprintf (1, "Mini-SEED scanning started [%s]", mssinfo->dirname);
  
  /* Start scan sequence */
  while ( mytdp->td_flags != TDF_CLOSE && scanerror == 0 )
    {
      scantime = time(NULL);
      mssinfo->scanrecordsread = 0;
      
      if ( mssinfo->iostats && mssinfo->iostats == iostatscount )
	{
	  gettimeofday (&scanstarttime, NULL);
	  mssinfo->scanfileschecked = 0;
	  mssinfo->scanfilesread = 0;
	  mssinfo->scanrecordswritten = 0;
	}
      
      /* Check for base directory existence */
      if ( lstat (mssinfo->dirname, &st) < 0 )
	{
	  /* Only log error if this is a change */
	  if ( mssinfo->accesserr == 0 )
	    lprintf (0, "[MSeedScan] WARINING Cannot read base directory [%s]: %s", mssinfo->dirname, strerror(errno));
	  
	  mssinfo->accesserr = 1;
	}
      else if ( ! S_ISDIR(st.st_mode) )
	{
	  /* Only log error if this is a change */
	  if ( mssinfo->accesserr == 0 )
	    lprintf (0, "[MSeedScan] WARINING Base directory is not a directory [%s]", mssinfo->dirname);
	  
	  mssinfo->accesserr = 1;
	}
      else
	{
	  if ( mssinfo->accesserr == 1 )
	    mssinfo->accesserr = 0;
	  
	  if ( ScanFiles (mssinfo, mssinfo->dirname, mssinfo->maxrecur, scantime) == -2 )
	    scanerror = 1;
	}
      
      if ( mytdp->td_flags != TDF_CLOSE && ! scanerror )
	{
	  /* Prune files that were not found from the filelist */
	  PruneFiles (mssinfo->filetree, scantime);
	  
	  /* Save intermediate state file */
	  if ( *(mssinfo->statefile) && mssinfo->stateint && (scantime - statetime) > mssinfo->stateint )
	    {
	      SaveState (mssinfo->filetree, mssinfo->statefile);
	      statetime = scantime;
	    }
	  
	  /* Reset the next new flag, the first scan is now complete */
	  if ( mssinfo->nextnew ) mssinfo->nextnew = 0;
	  
	  /* Sleep for specified interval */
	  if ( mssinfo->scansleep )
	    nanosleep (&treq, &trem);
	  
	  /* Sleep for specified interval if no records were read */
	  if ( mssinfo->scansleep0 && mssinfo->scanrecordsread == 0 )
	    nanosleep (&treq0, &trem);
	}
      
      /* Rate calculation */
      if ( mssinfo->rxpackets[0] > 0 )
	{
	  hptime_t hpnow = HPnow();
	  
	  /* Calculate scan duration in seconds and make sure it's not zero */
	  mssinfo->scantime = (double) (hpnow - mssinfo->ratetime) / HPTMODULUS;
	  if ( mssinfo->scantime == 0.0 )
	    mssinfo->scantime = 1.0;
	  
	  /* Calculate the reception rates */
	  mssinfo->rxpacketrate = (double) (mssinfo->rxpackets[0] - mssinfo->rxpackets[1]) / mssinfo->scantime;
	  mssinfo->rxbyterate = (double) (mssinfo->rxbytes[0] - mssinfo->rxbytes[1]) / mssinfo->scantime;
	  
	  /* Shift current values to history values */
	  mssinfo->rxpackets[1] = mssinfo->rxpackets[0];
	  mssinfo->rxbytes[1] = mssinfo->rxbytes[0];
	  
	  mssinfo->ratetime = hpnow;
	}
      
      if ( mssinfo->iostats && iostatscount <= 1 )
	{
          /* Determine run time since scanstarttime was set */
	  gettimeofday (&scanendtime, NULL);
	  
	  iostatsinterval = ( ((double)scanendtime.tv_sec + (double)scanendtime.tv_usec/1000000) -
			      ((double)scanstarttime.tv_sec + (double)scanstarttime.tv_usec/1000000) );
	  
	  lprintf (0, "[MSeedScan] Time: %g seconds for %d scan(s) (%g seconds/scan)",
                   iostatsinterval, mssinfo->iostats, iostatsinterval/mssinfo->iostats);
	  lprintf (0, "[MSeedScan] Files checked: %d, read: %d (%g read/sec)",
		   mssinfo->scanfileschecked, mssinfo->scanfilesread, mssinfo->scanfilesread/iostatsinterval);
	  lprintf (0, "[MSeedScan] Records read: %d, written: %d (%g written/sec)",
		   mssinfo->scanrecordsread, mssinfo->scanrecordswritten, mssinfo->scanrecordswritten/iostatsinterval);
	  
          /* Reset counter */
          iostatscount = mssinfo->iostats;
	}
      else if ( mssinfo->iostats )
        {
          iostatscount--;
        }
    } /* End of main scan loop */
  
  /* Set thread closing status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSING;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  /* Save the state file */
  if ( *(mssinfo->statefile) != '\0' )
    SaveState (mssinfo->filetree, mssinfo->statefile);
  
  /* Release file tracking binary tree */
  if ( mssinfo->filetree )
    RBTreeDestroy (mssinfo->filetree);
  
  /* Free regex matching memory */
  if ( mssinfo->fnmatch )
    pcre_free (mssinfo->fnmatch);
  if ( mssinfo->fnmatch_extra )
    pcre_free (mssinfo->fnmatch_extra);
  if ( mssinfo->fnreject )
    pcre_free (mssinfo->fnreject);
  if ( mssinfo->fnreject_extra )
    pcre_free (mssinfo->fnreject_extra);
  
  if ( mssinfo->msr )
    msr_free (&(mssinfo->msr));
  
  lprintf (1, "Mini-SEED scanning stopped [%s]", mssinfo->dirname);
  
  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  return 0;
}  /* End of MS_ScanThread() */


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
 * were not Mini-SEED on a previous attempt, etc.
 *
 * Return 0 on success and -1 on error and -2 on fatal error.
 ***************************************************************************/
static int
ScanFiles (MSScanInfo *mssinfo, char *targetdir, int level, time_t scantime)
{
  FileNode *fnode;
  FileKey *fkey;
  char filekeybuf[sizeof(FileKey)+MSSCAN_MAXFILENAME]; /* Room for fkey */
  struct stat st;
  struct edirent *ede;
  time_t currentday = 0;
  EDIR *dir;
  
  fkey = (FileKey *) &filekeybuf;
  
  lprintf (3, "[MSeedScan] Processing directory '%s'", targetdir);
  
  if ( (dir = EOpenDir (targetdir)) == NULL )
    {
      lprintf (0, "[MSeedScan] Cannot open directory %s: %s", targetdir, strerror(errno));
      
      return -1;
    }
  
  if ( mssinfo->budlatency )
    {
      struct tm cday;
      gmtime_r (&scantime, &cday);
      currentday = CalcDayTime (cday.tm_year + 1900, cday.tm_yday + 1);
    }
  
  while ( shutdownsig == 0 && (ede = EReadDir(dir)) != NULL )
    {
      int filenamelen;
      
      /* Skip "." and ".." entries */
      if ( !strcmp(ede->d_name, ".") || !strcmp(ede->d_name, "..") )
	continue;
      
      /* BUD file name latency check */
      if ( mssinfo->budlatency )
	{
	  time_t budfiletime = BudFileDayTime (ede->d_name);
	  
	  /* Skip this file if the BUD file name is more than budlatency days old */
	  if ( budfiletime && ((currentday-budfiletime) > (mssinfo->budlatency * 86400)) )
	    {
	      if ( verbose > 1 )
		lprintf (0, "[MSeedScan] Skipping due to BUD file name latency: %s", ede->d_name);
	      continue;
	    }
	}
      
      /* Build a FileKey for this file */
      fkey->inode = ede->d_ino;
      filenamelen = snprintf (fkey->filename, sizeof(filekeybuf) - sizeof(FileKey),
			      "%s/%s", targetdir, ede->d_name);
      
      /* Make sure the filename was not truncated */
      if ( filenamelen >= (sizeof(filekeybuf) - sizeof(FileKey) - 1) )
	{
	  lprintf (0, "[MSeedScan] Directory entry name beyond maximum of %d characters, skipping:",
		   (sizeof(filekeybuf) - sizeof(FileKey) - 1));
	  lprintf (0, "  %s", ede->d_name);
	  continue;
	}
      
      /* Search for a matching entry in the filetree */
      if ( (fnode = FindFile (mssinfo->filetree, fkey)) )
	{
	  /* Check if the file is permanently skipped */
	  if ( fnode->offset == -1 )
	    {
	      fnode->scantime = scantime;
	      continue;
	    }
	  
	  /* Check if file has triggered a delayed check */
	  if ( fnode->idledelay > 0 )
	    {
	      fnode->scantime = scantime;
	      fnode->idledelay--;
	      continue;
	    }
	}
      
      /* Stat the file */
      if ( lstat (fkey->filename, &st) < 0 )
	{
	  if ( ! (shutdownsig && errno == EINTR) )
	    lprintf (0, "[MSeedScan] Cannot stat %s: %s", fkey->filename, strerror(errno));
	  continue;
	}
      
      /* If symbolic link stat the real file, if it's a broken link continue */
      if ( S_ISLNK(st.st_mode) )
	{
	  if ( stat (fkey->filename, &st) < 0 )
	    {
	      /* Interruption signals when the stop signal is set should break out */
	      if ( shutdownsig && errno == EINTR )
		break;
	      
	      /* Log an error if the error is anything but a disconnected link */
	      if ( errno != ENOENT )
		lprintf (0, "[MSeedScan] Cannot stat (linked) %s: %s", fkey->filename, strerror(errno));
	      else
		continue;
	    }
	}
      
      /* If directory recurse up to the limit */
      if ( S_ISDIR(st.st_mode) )
	{
	  if ( mssinfo->maxrecur < 0 || mssinfo->recurlevel < level )
	    {
	      lprintf (4, "[MSeedScan] Recursing into %s", fkey->filename);
	      
	      mssinfo->recurlevel++;
	      if ( ScanFiles (mssinfo, fkey->filename, level, scantime) == -2 )
		return -2;
	      mssinfo->recurlevel--;
	    }
	  continue;
	}
      
      /* Increment files found counter */
      if ( mssinfo->iostats )
	mssinfo->scanfileschecked++;
      
      /* Do regex matching if an expression was specified */
      if ( mssinfo->fnmatch != 0 )
	if ( pcre_exec(mssinfo->fnmatch, NULL, ede->d_name, strlen(ede->d_name), 0, 0, NULL, 0) != 0 )
	  continue;
      
      /* Do regex rejecting if an expression was specified */
      if ( mssinfo->fnreject != 0 )
	if ( pcre_exec(mssinfo->fnreject, NULL, ede->d_name, strlen(ede->d_name), 0, 0, NULL, 0) == 0 )
	  continue;
      
      /* Sanity check for a regular file */
      if ( !S_ISREG(st.st_mode) )
	{
	  lprintf (0, "[MSeedScan] %s is not a regular file", fkey->filename);
	  continue;
	}
      
      /* Sanity check that the dirent inode and stat inode are the same */
      if ( st.st_ino != ede->d_ino )
	{
	  lprintf (0, "[MSeedScan] Inode numbers from dirent and stat do not match for %s\n", fkey->filename);
	  lprintf (0, "  dirent: %llu  VS  stat: %llu\n",
		   (unsigned long long int) ede->d_ino, (unsigned long long int) st.st_ino);
	  continue;
	}
      
      lprintf (3, "[MSeedScan] Checking file %s", fkey->filename);
      
      /* If the file has never been seen add it to the list */
      if ( ! fnode )
	{
	  /* Add new file to tree setting modtime to one second in the
	   * past so we are triggered to look at this file the first time. */
	  if ( ! (fnode = AddFile (mssinfo->filetree, fkey->inode, fkey->filename, st.st_mtime - 1)) )
	    {
	      lprintf (0, "[MSeedScan] Error adding %s to file list", fkey->filename);
	      continue;
	    }
        }
      
      /* Only update the offset if skipping the first scan, otherwise process */
      if ( mssinfo->nextnew )
        fnode->offset = st.st_size;
      
      /* Check if the file is quiet and mark to always skip if true */
      if ( mssinfo->quietsec && st.st_mtime < (scantime - mssinfo->quietsec) )
	{
	  lprintf (2, "[MSeedScan] Marking file as quiet, no processing: %s", fkey->filename);
	  fnode->offset = -1;
	}
      
      /* Otherwise check if the file is idle and set idledelay appropriately */
      else if ( mssinfo->idledelay && fnode->idledelay == 0 &&
		st.st_mtime < (scantime - mssinfo->idlesec) )
	{
	  lprintf (2, "[MSeedScan] Marking file as idle, will not check for %d scans: %s",
		   mssinfo->idledelay, fkey->filename);
	  fnode->idledelay = (mssinfo->idledelay > 0) ? (mssinfo->idledelay-1) : 0;
	}
      
      /* Process (read records from) the file if it's size has increased and
       * is not marked for permanent skipping */
      if ( fnode->offset < st.st_size && fnode->offset != -1 )
	{
	  /* Increment files read counter */
	  if ( mssinfo->iostats )
	    mssinfo->scanfilesread++;
	  
	  fnode->offset = ProcessFile (mssinfo, fkey->filename, fnode, st.st_size, st.st_mtime);
	  
	  /* If a proper file read but fatal error occured set the offset correctly
	     and return a fatal error. */
	  if ( fnode->offset < -1 )
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
}  /* End of ScanFiles() */


/***************************************************************************
 * FindFile:
 *
 * Search the filetree for a given FileKey.
 *
 * Return a pointer to a FileNode if found or 0 if no match found.
 ***************************************************************************/
static FileNode*
FindFile (RBTree *filetree, FileKey *fkey)
{
  FileNode *fnode = 0;
  RBNode *tnode;
  
  /* Search for a matching inode + file name entry */
  if ( (tnode = RBFind (filetree, fkey)) )
    {
      fnode = (FileNode *)tnode->data;
    }
  
  return fnode;
}  /* End of FindFile() */


/***************************************************************************
 * AddFile:
 *
 * Add a file to the file tree, no checking is done to determine if
 * this entry already exists.
 *
 * Return a pointer to the added FileNode on success and 0 on error.
 ***************************************************************************/
static FileNode*
AddFile (RBTree *filetree, ino_t inode, char *filename, time_t modtime)
{
  FileKey *newfkey;
  FileNode *newfnode;
  size_t filelen;
  
  lprintf (1, "[MSeedScan] Adding %s", filename);
  
  /* Create new tree key */
  filelen = strlen (filename);
  newfkey = (FileKey *) malloc (sizeof(FileKey)+filelen);
  
  /* Create new tree node */
  newfnode = (FileNode *) malloc (sizeof(FileNode));
  
  if ( ! newfkey || ! newfnode )
    return 0;
  
  /* Populate the new key and node */
  newfkey->inode = inode;
  memcpy (newfkey->filename, filename, filelen+1);
  
  newfnode->offset = 0;
  newfnode->modtime = modtime;
  newfnode->idledelay = 0;
  
  RBTreeInsert (filetree, newfkey, newfnode, 0);
  
  return newfnode;
}  /* End of AddFile() */


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
  int fd;
  int nread;
  int reccnt = 0;
  int reachedmax = 0;
  int detlen;
  int flags;
  off_t newoffset = fnode->offset;
  char mseedbuf[MSSCAN_RECSIZE];
  int mseedsize = MSSCAN_RECSIZE;
  struct timespec treq, trem;
  
  lprintf (3, "[MSeedScan] Processing file %s", filename);
  
  /* Set the throttling sleep time */
  treq.tv_sec = (time_t) 0;
  treq.tv_nsec = (long) mssinfo->throttlensec;
    
  /* Set open flags */
#if defined(__sun__) || defined(__sun)
  flags = O_RDONLY | O_RSYNC;
#else
  flags = O_RDONLY;
#endif
  
  /* Open target file */
  if ( (fd = open (filename, flags, 0)) == -1 )
    {
      if ( ! (shutdownsig && errno == EINTR) )
	{
	  lprintf (0, "[MSeedScan] Error opening %s: %s", filename, strerror(errno));
	  return -1;
	}
      else
	return newoffset;
    }
  
  /* Read MSSCAN_RECSIZE byte chunks off the end of the file */
  while ( (newsize - newoffset) >= MSSCAN_RECSIZE )
    {
      /* Jump out if we've read the maximum allowed number of records
	 from this file for this scan */
      if ( mssinfo->filemaxrecs && reccnt >= mssinfo->filemaxrecs )
	{
	  reachedmax = 1;
	  break;
	}
      
      /* Read the next record */
      if ( (nread = pread (fd, mseedbuf, MSSCAN_RECSIZE, newoffset)) != MSSCAN_RECSIZE )
	{
	  if ( ! (shutdownsig && errno == EINTR) )
	    {
	      lprintf (0, "[MSeedScan] Error: only read %d bytes from %s", nread, filename);
	      close (fd);
	      return -1;
	    }
	  else
	    return newoffset;
	}
      
      newoffset += nread;
      
      /* Increment records read counter */
      mssinfo->scanrecordsread++;
      
      /* Check record for 1000 blockette and verify SEED structure */
      if ( (detlen = ms_detect (mseedbuf, MSSCAN_RECSIZE)) <= 0 )
	{
	  /* If no data has ever been read from this file, ignore file */
	  if ( fnode->offset == 0 )
	    {
	      lprintf (0, "[MSeedScan] %s: Not a valid Mini-SEED record at offset %lld, ignoring file",
		       filename, (long long) (newoffset - nread));
	      close (fd);
	      return -1;
	    }
	  /* Otherwise, if records have been read, skip until next scan */
	  else
	    {
	      lprintf (0, "[MSeedScan] %s: Not a valid Mini-SEED record at offset %lld (new bytes %lld), skipping for this scan",
		       filename, (long long) (newoffset - nread),
		       (long long ) (newsize - newoffset + nread));
	      close (fd);
	      return (newoffset - nread);
	    }
	}
      else if ( detlen != MSSCAN_RECSIZE )
	{
	  lprintf (0, "[MSeedScan] %s: Unexpected record length: %d at offset %lld, ignoring file",
		   filename, detlen, (long long) (newoffset - nread));
	  close (fd);
	  return -1;
	}
      /* Prepare for sending the record off to the DataLink ringserver */
      else
	{
	  lprintf (4, "[MSeedScan] Sending %20.20s", mseedbuf);
	  
	  /* Send data record to the ringserver */
	  if ( WriteRecord (mssinfo, mseedbuf, mseedsize) )
	    {
	      return -newoffset;
	    }
	  
	  /* Increment records written counter */
	  if ( mssinfo->iostats )
	    mssinfo->scanrecordswritten++;
	  
	  /* Sleep for specified throttle interval */
	  if ( mssinfo->throttlensec )
	    nanosleep (&treq, &trem);
	}
      
      reccnt++;
    }
  
  close (fd);
  
  /* Update modtime:
   * If the maximum number of records have been reached we are not necessarily
   * at the end of the file so set the modtime one second in the past so we are
   * triggered to look at this file again. */
  if ( reachedmax )
    fnode->modtime = newmodtime - 1;
  else
    fnode->modtime = newmodtime;
  
  if ( reccnt )
    {
      lprintf (2, "[MSeedScan] Read %d %d-byte record(s) from %s",
	       reccnt, MSSCAN_RECSIZE, filename);
    }
  
  return newoffset;
}  /* End of ProcessFile() */


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
  FileKey  *fkey;
  RBNode   *tnode;
  Stack    *stack;
  
  stack = StackCreate();
  RBBuildStack (filetree, stack);
  
  while ( (tnode = (RBNode *) StackPop (stack)) )
    {
      fkey = (FileKey *) tnode->key;
      fnode = (FileNode *) tnode->data;
      
      if ( fnode->scantime < scantime )
      	{
	  lprintf (1, "[MSeedScan] Removing %s from file list", fkey->filename);
	  
	  RBDelete (filetree, tnode);
	}
    }
  
  StackDestroy (stack, free);
}  /* End of PruneFiles() */


/***************************************************************************
 * PrintFileList:
 *
 * Print file tree to the specified descriptor.
 ***************************************************************************/
static void
PrintFileList (RBTree *filetree, FILE *fp)
{
  FileKey  *fkey;
  FileNode *fnode;
  RBNode   *tnode;
  Stack    *stack;
  
  stack = StackCreate();
  RBBuildStack (filetree, stack);
  
  while ( (tnode = (RBNode *) StackPop (stack)) )
    {
      fkey = (FileKey *) tnode->key;
      fnode = (FileNode *) tnode->data;
      
      fprintf (fp, "%s\t%llu\t%lld\t%lld\n",
	       fkey->filename,
	       (unsigned long long int) fkey->inode,
	       (signed long long int) fnode->offset,
	       (signed long long int) fnode->modtime);
    }
  
  StackDestroy (stack, free);
}  /* End of PrintFileList() */


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
  fnsize = snprintf (tmpstatefile, sizeof(tmpstatefile), "%s.tmp", statefile);
  
  /* Check for truncation */
  if ( fnsize >= sizeof(tmpstatefile) )
    {
      lprintf (0, "[MSeedScan] Error, temporary statefile name too long (%d bytes)", fnsize);
      return -1;
    }
  
  /* Open temporary state file */
  if ( (fp = fopen (tmpstatefile, "w")) == NULL )
    {
      lprintf (0, "[MSeedScan] Error opening temporary statefile %s: %s",
	       tmpstatefile, strerror(errno));
      return -1;
    }
  
  /* Write file list to temporary state file */
  PrintFileList (filetree, fp);
  
  fclose (fp);
  
  /* Rename temporary state file overwriting the current state file */
  if ( rename (tmpstatefile, statefile) )
    {
      lprintf (0, "[MSeedScan] Error renaming temporary statefile %s->%s: %s",
	       tmpstatefile, statefile, strerror(errno));
      return -1;
    }
  
  return 0;
}  /* End of SaveState() */


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
  
  if ( (fp=fopen(statefile, "r")) == NULL )
    {
      lprintf (0, "[MSeedScan] Error opening statefile %s: %s", statefile, strerror(errno));
      return -1;
    }
  
  lprintf (1, "[MSeedScan] Recovering state");
  
  count = 1;
  
  while ( (fgets (line, sizeof(line), fp)) !=  NULL)
    {
      fields = sscanf (line, "%s %llu %lld %lld\n",
		       filename, &inode, &offset, &modtime);
      
      if ( fields < 0 )
        continue;
      
      if ( fields < 4 )
        {
          lprintf (0, "[MSeedScan] Could not parse line %d of state file", count);
	  continue;
        }
      
      fnode = AddFile (filetree, (ino_t) inode, filename, (time_t) modtime);
      
      if ( fnode )
	{
	  fnode->offset = (off_t) offset;
	  fnode->scantime = 0;
	}
      
      count++;
    }
  
  fclose (fp);
  
  return 0;
}  /* End of RecoverState() */


/***************************************************************************
 * WriteRecord:
 *
 * Send the specified record to the DataLink server.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
WriteRecord (MSScanInfo *mssinfo, char *record, int reclen)
{
  char streamid[100];
  RingPacket packet;
  int rv;
  
  /* Parse Mini-SEED header */
  if ( (rv = msr_unpack (record, reclen, &(mssinfo->msr), 0, 0)) != MS_NOERROR )
    {
      ms_recsrcname (record, streamid, 0);
      lprintf (0, "[MSeedScan] Error unpacking %s: %s", streamid, ms_errorstr(rv));
      return -1;
    }
  
  /* Generate stream ID for this record: NET_STA_LOC_CHAN/MSEED */
  msr_srcname (mssinfo->msr, streamid, 0);
  strcat (streamid, "/MSEED");
  
  memset (&packet, 0, sizeof(RingPacket));
  strncpy (packet.streamid, streamid, sizeof(packet.streamid)-1);
  packet.datastart = mssinfo->msr->starttime;
  packet.dataend = msr_endtime (mssinfo->msr);
  packet.datasize = mssinfo->msr->reclen;
  
  /* Add the packet to the ring */
  if ( (rv = RingWrite (mssinfo->ringparams, &packet, record, packet.datasize)) )
    {
      if ( rv == -2 )
	lprintf (1, "[MSeedScan] Error with RingWrite, corrupt ring, shutdown signalled");
      else
	lprintf (1, "[MSeedScan] Error with RingWrite");
      
      /* Set the shutdown signal if ring corruption was detected */
      if ( rv == -2 )
	shutdownsig = 1;
      
      return -1;
    }
  
  /* Update client receive counts */
  mssinfo->rxpackets[0]++;
  mssinfo->rxbytes[0] += packet.datasize;
  
  return 0;
}  /* End of WriteRecord() */


/***************************************************************************
 * Initialize:
 *
 * Process the command line parameters.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
Initialize (MSScanInfo *mssinfo)
{
  const char *errptr;
  int erroffset;
  
  /* Compile the match regex if specified */
  if ( *(mssinfo->matchstr) != '\0' )
    {
      /* Compile regex */
      mssinfo->fnmatch = pcre_compile (mssinfo->matchstr, 0, &errptr, &erroffset, NULL);
      if ( errptr )
        {
          lprintf (0, "[MSeedScan] Error with pcre_compile: %s (%s)", errptr, mssinfo->matchstr);
          return -1;
        }
      
      /* Study regex */
      mssinfo->fnmatch_extra = pcre_study (mssinfo->fnmatch, 0, &errptr);
      if ( errptr )
        {
          lprintf (0, "[MSeedScan] Error with pcre_study: %s (%s)", errptr, mssinfo->matchstr);
          return -1;
        }
    }
  
  /* Compile the reject regex if specified */
  if ( *(mssinfo->rejectstr) != '\0' )
    {
      /* Compile regex */
      mssinfo->fnreject = pcre_compile (mssinfo->rejectstr, 0, &errptr, &erroffset, NULL);
      if ( errptr )
        {
          lprintf (0, "[MSeedScan] Error with pcre_compile: %s (%s)", errptr, mssinfo->rejectstr);
          return -1;
        }
      
      /* Study regex */
      mssinfo->fnreject_extra = pcre_study (mssinfo->fnreject, 0, &errptr);
      if ( errptr )
        {
          lprintf (0, "[MSeedScan] Error with pcre_study: %s (%s)", errptr, mssinfo->rejectstr);
          return -1;
        }
    }
  
  /* Attempt to recover sequence numbers from state file */
  if ( *(mssinfo->statefile) != '\0' )
    {
      if ( RecoverState (mssinfo->filetree, mssinfo->statefile) < 0 )
        {
          lprintf (0, "[MSeedScan] State recovery failed for %s", mssinfo->dirname);
        }
    }
  
  return 0;
}  /* End of Initialize() */


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
  if ( ((FileKey*)a)->inode > ((FileKey*)b)->inode )
    return 1;
  
  else if ( ((FileKey*)a)->inode < ((FileKey*)b)->inode )
    return -1;
  
  /* Compare filename values */
  cmpval = strcmp ( ((FileKey*)a)->filename, ((FileKey*)b)->filename );
  
  if ( cmpval > 0 )
    return 1;
  else if ( cmpval < 0 )
    return -1;
  
  return 0;
}  /* End of MSS_KeyCompare() */


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

  a4 = (shortyear >> 2) + 475 - ! (shortyear & 3);
  a100 = a4 / 25 - (a4 % 25 < 0);
  a400 = a100 >> 2;
  intervening_leap_days = (a4 - 492) - (a100 - 19) + (a400 - 4);
  
  days = (365 * (shortyear - 70) + intervening_leap_days + (day - 1));
  
  daytime = (time_t ) (60 * (60 * (24 * days)));
  
  return daytime;
}  /* End of CalcDayTime() */


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
  
  if ( ! filename )
    return 0;
  
  length = strlen (filename);
  
  if ( length < 12 )
    return 0;
  
  /* Check for digits and periods in the expected places at the end: ".YYYY.DDD" */
  if ( *(filename + length - 9) == '.' &&
       isdigit (*(filename + length - 8)) && isdigit (*(filename + length - 7)) &&
       isdigit (*(filename + length - 6)) && isdigit (*(filename + length - 5)) &&
       *(filename + length - 4) == '.' &&
       isdigit (*(filename + length - 3)) && isdigit (*(filename + length - 2)) &&
       isdigit (*(filename + length - 1)) )
    {
      iyear = strtol ((filename + length - 8), NULL, 10);
      iday = strtol ((filename + length - 3), NULL, 10);
      
      daytime = CalcDayTime (iyear, iday);
    }
  
  return daytime;
}  /* End of BudFileDayTime() */


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
  
  if ( ! edirp )
    return -1;
  
  top = edirp->ents;
  totalmerges = 0;
  insize = 1;
  
  for (;;)
    {
      p = top;
      top = NULL;
      tail = NULL;
      
      nmerges = 0;  /* count number of merges we do in this pass */
      
      while ( p )
        {
          nmerges++;  /* there exists a merge to be done */
          totalmerges++;
          
          /* step `insize' places along from p */
          q = p;
          psize = 0;
          for (i = 0; i < insize; i++)
            {
              psize++;
              q = q->next;
              if ( ! q )
                break;
            }
          
          /* if q hasn't fallen off end, we have two lists to merge */
          qsize = insize;
          
          /* now we have two lists; merge them */
          while ( psize > 0 || (qsize > 0 && q) )
            {
              /* decide whether next element of merge comes from p or q */
              if ( psize == 0 )
                {  /* p is empty; e must come from q. */
                  e = q; q = q->next; qsize--;
                }
              else if ( qsize == 0 || ! q )
                {  /* q is empty; e must come from p. */
                  e = p; p = p->next; psize--;
                }
              else if ( strcmp (p->d_name, q->d_name) <= 0 )
                {  /* First element of p is lower (or same), e must come from p. */
                  e = p; p = p->next; psize--;
                }
              else
                {  /* First element of q is lower; e must come from q. */
                  e = q; q = q->next; qsize--;
                }
              
              /* add the next element to the merged list */
              if ( tail )
                tail->next = e;
              else
                top = e;
              
              e->prev = tail;
              tail = e;
            }
          
          /* now p has stepped `insize' places along, and q has too */
          p = q;
        }
      
      tail->next = NULL;
      
      /* If we have done only one merge, we're finished. */
      if ( nmerges <= 1 )   /* allow for nmerges==0, the empty list case */
        {
          edirp->ents = top;
	  
          return totalmerges;
        }
      
      /* Otherwise repeat, merging lists twice the size */
      insize *= 2;
    }
}  /* End of SortEDirEntries() */


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
  struct edirent *prevede = 0;
  int namelen;
  
  if ( ! dirname )
    return NULL;
  
  dirp = opendir (dirname);
  
  if ( ! dirp )
    return NULL;
  
  /* Allocate new EDIR */
  if ( ! (edirp = (EDIR *) malloc (sizeof (EDIR))) )
    {
      closedir (dirp);
      return NULL;
    }
  
  /* Read all directory entries */
  while ( (de = readdir (dirp)) )
    {
      /* Allocate space for enhanced directory entry */
      if ( ! (ede = (struct edirent *) calloc (1, sizeof(struct edirent))) )
	{
          lprintf (0, "[MSeedScan] Cannot allocate directory memory");
	  closedir (dirp);
	  ECloseDir (edirp);
	  return NULL;
	}
      
      namelen = strlen (de->d_name);
      
      if ( namelen > sizeof(ede->d_name) )
        {
          lprintf (0, "[MSeedScan] directory entry name too long (%d): '%s'", namelen, de->d_name);
	  closedir (dirp);
	  ECloseDir (edirp);
	  return NULL;
	}
      
      strncpy (ede->d_name, de->d_name, sizeof(ede->d_name));
      ede->d_ino = de->d_ino;
      ede->prev = prevede;
      
      /* Add new enhanced directory entry to the list */
      if ( prevede == 0 )
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
  if ( SortEDirEntries (edirp) < 0 )
    {
      ECloseDir (edirp);
      return NULL;
    }
  
  /* Set the current entry to the top of the list */
  edirp->current = edirp->ents;
  
  return edirp;
}  /* End of EOpenDir() */


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
  
  if ( ! edirp )
    return NULL;
  
  ede = edirp->current;
  
  if ( edirp->current )
    edirp->current = ede->next;
  
  return ede;
}  /* End of EReadDir() */


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
  
  if ( ! edirp )
    return -1;
  
  ede = edirp->ents;
  
  /* Loop through associated entries and free them */
  while ( ede )
    {
      nede = ede->next;
      
      free (ede);
      
      ede = nede;
    }
  
  free (edirp);
  
  return 0;
}  /* End of ECloseDir() */

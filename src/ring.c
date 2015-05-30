/**************************************************************************
 * ring.c
 *
 * Fundamental ring routines.  This code implements a generic ring
 * buffer with the packet buffer either in memory or as a
 * memory-mapped file.
 *
 * The ring system is generally composed of 2 components: 1) a packet
 * buffer (either in memory or a mmap'd file) and 2) a stream index
 * stored as a binary tree.  If the packet buffer is to be stored in
 * memory the packet buffer file is read on startup and written on
 * shutdown only.  If the packet buffer is to be memory mapped the
 * packet buffer file will be used directly.  The stream index file is
 * read on startup and written on shutdown existing only in memory
 * during operation. The packet buffer (and related stream index) can
 * also be volatile, created in memory on initialization and lost on
 * program or ring shutdown.
 *
 * Ring writing is governed by a mutex to avoid writers colliding,
 * only one writer may modify the ring at a time.  Ring reading is
 * lockless with post-operation checking guaranteeing consistency.
 *
 * In general, non-existent packets are represented with a packet ID
 * of 0 and an offset of -1.
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
 * Modified: 2015.149
 **************************************************************************/

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include <libmseed.h>
#include <pcre.h>

#include "rbtree.h"
#include "logging.h"
#include "ring.h"
#include "generic.h"


/* Macros to determine next and previous IDs given an ID and the maximum ID */
#define NEXTID(I,M) ( (((I)+1) > (M)) ? 1 : (I)+1 )
#define PREVID(I,M) ( ((I) == 1) ? M : (I)-1 )

static int StreamStackNodeCmp (StackNode *a, StackNode *b);
static inline RingPacket* GetPacket (RingParams *ringparams, int64_t pktid, hptime_t *pkttime);
static RingStream* AddStreamIdx (RBTree *streamidx, RingStream *stream, Key **ppkey);
static RingStream* GetStreamIdx (RBTree *streamidx, char *streamid);
static int DelStreamIdx (RBTree *streamidx, char *streamid);


/***************************************************************************
 * RingInitialize:
 *
 * Initialize ring files in specified directory either loading and
 * validating the existing ring files or creating new files.
 *
 * ring file = main packet buffer file, optionally memory mapped
 * stream file = stream index file, loaded into ringparams->streams
 *
 * Returns 0 on success, and -1 on corruption errors and -2 on
 * non-recoverable errors.
 ***************************************************************************/
int
RingInitialize (char *ringfilename, char *streamfilename, uint64_t ringsize,
		uint32_t pktsize, int64_t maxpktid, uint8_t mmapflag,
		uint8_t volatileflag, int *ringfd, RingParams **ringparams)
{
  static pthread_mutex_t writelock;
  static pthread_mutex_t streamlock;
  
  struct stat ringfilestat;
  struct stat streamfilestat;
  int streamidxfd;
  RingStream stream;
  
  long pagesize;
  uint32_t headersize;
  int64_t maxpackets;
  int64_t maxoffset;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  
  int corruptring = 0;
  int ringinit = 0;
  int rc;
  ssize_t rv;
  RingPacket *packetptr;
  RingStream *streamptr;
  char timestr[50];
  
  /* Sanity check input parameters */
  if ( ! volatileflag && ( ! ringfilename || ! streamfilename) )
    {
      lprintf (0, "RingInitialize(): ring file and stream file must be specified");
      return -2;
    }
  
  /* A volatile ring will never be memory mapped */
  if ( volatileflag )
    {
      mmapflag = 0;
    }
  
  /* Determine system page size */
  if ( (pagesize = sysconf(_SC_PAGESIZE)) < 0 )
    {
      lprintf (0, "RingInitizlize(): Error determining system page size: %s",
	       strerror(errno));
      return -2;
    }
  
  /* Determine the number of pages needed for the header */
  headersize = pagesize;
  while ( headersize < sizeof(RingParams) )
    headersize += pagesize;
  
  /* Sanity check that the ring can hold at least two packets */
  if ( ringsize < (headersize + 2*pktsize) )
    {
      lprintf (0,"RingInitialize(): ring size (%llu) must be enough for 2 packets (%u each) and header (%d)",
	       ringsize, pktsize, headersize);
      return -2;
    }
  
  /* Determine the maximum number of packets that fit after the first page */
  maxpackets = (int64_t) ((ringsize-headersize)/pktsize);
  
  /* Sanity check that the maximum packet ID is greater than the maximum number of packets */
  if ( maxpktid < maxpackets )
    {
      lprintf (0,"RingInitialize(): maximum pkt id (%lld) must be >= maximum packets (%lld)",
	       maxpktid, maxpackets);
      return -2;
    }
  
  /* Determine the maximum packet offset value */
  maxoffset = (int64_t) ((maxpackets-1)*pktsize);
  
  /* Open ring packet buffer file if non-volatile */
  if ( ! volatileflag )
    {
      /* Open ring packet buffer file, creating if necessary */
      if ( (*ringfd = open (ringfilename, O_RDWR | O_CREAT, mode)) < 0 )
	{
	  lprintf (0, "RingInitialize(): error opening %s: %s", ringfilename, strerror(errno));
	  return -1;
	}
      
      /* Stat the ring packet buffer file */
      if ( fstat (*ringfd, &ringfilestat) )
	{
	  lprintf (0, "RingInitialize(): error stating %s: %s", ringfilename, strerror(errno));
	  return -1;
	}
      
      /* If the file is new or unexpected size initialize to maximum ring file size */
      if ( ringfilestat.st_size != ringsize )
	{
	  ringinit = 1;
	  
	  if ( ringfilestat.st_size <= 0 )
	    lprintf (1, "Creating new ring packet buffer file");
	  else
	    lprintf (1, "Re-creating ring packet buffer file");
	  
	  /* Truncate file if larger than ringsize */
	  if ( ringfilestat.st_size > ringsize )
	    {
	      if ( ftruncate (*ringfd, ringsize) == -1 )
		{
		  lprintf (0, "RingInitialize(): error truncating %s: %s", ringfilename, strerror(errno));
		  return -1;
		}
	    }
	  
	  /* Go to the last byte of the desired size */
	  if ( lseek (*ringfd, ringsize - 1, SEEK_SET) == -1 )
	    {
	      lprintf (0, "RingInitialize(): error seeking in %s: %s", ringfilename, strerror(errno));
	      return -1;
	    }
	  
	  /* Write a dummy byte at the end of the ring packet buffer */
	  if ( write (*ringfd, "", 1) != 1 )
	    {
	      lprintf (0, "RingInitialize(): error writing to %s: %s", ringfilename, strerror(errno));
	      return -1;
	    }
	}
      else
	{
	  lprintf (1, "Recovering existing ring packet buffer file");
	}
    }
  
  /* Use memory-mapping to access file */
  if ( mmapflag && ! volatileflag )
    {
      lprintf (1, "Memory-mapping ring packet buffer file");
      
      /* Memory map the ring packet buffer file */
      if ( (*ringparams = (RingParams *) mmap (0, ringsize, PROT_READ | PROT_WRITE,
					       MAP_SHARED, *ringfd, 0)) == (void *) -1)
	{
	  lprintf (0, "RingInitialize(): error mmaping %s: %s", ringfilename, strerror(errno));
	  return -1;
	}
    }
  /* Read ring packet buffer into memory if not memory-mapping. */
  else
    {
      lprintf (2, "Allocating ring packet buffer memory");
      
      /* Allocate ring packet buffer */
      if ( ! (*ringparams = malloc (ringsize)) )
	{
	  lprintf (0, "RingInitialize(): error allocating %d bytes for ring packet buffer",
		   ringsize);
	  return -2;
	}
    
      /* Force ring initialization if volatile */
      if ( volatileflag )
	ringinit = 1;
      
      /* Read ring packet buffer into memory if initialization is not needed */
      if ( ! ringinit )
	{
	  lprintf (1, "Reading ring packet buffer file into memory");
	  
	  if ( read (*ringfd, *ringparams, ringsize) != ringsize )
	    {
	      lprintf (0, "RingInitialize(): error reading ring packet buffer into memory: %s",
		       strerror(errno));
	      return -1;
	    }
	}
    }
  
  /* Check ring corruption flag, if set the ring was earlier determined to be corrupt */
  if ( (*ringparams)->corruptflag && ! volatileflag )
    {
      lprintf (0, "** Packet buffer is marked as corrupt");
      return -1;
    }
  
  /* Check ring flux flag, if set the ring should be considered corrupt */
  if ( (*ringparams)->fluxflag && ! volatileflag )
    {
      lprintf (0, "** Packet buffer is marked as busy, probably corrupted");
      return -1;
    }
  
   /* Initialize locks */
  if ( (rc = pthread_mutex_init (&writelock, NULL)) )
    {
      lprintf (0, "RingInitialize(): error initializing ring write lock: %s", strerror(rc));
      return -2;
    }
  if ( (rc = pthread_mutex_init (&streamlock, NULL)) )
    {
      lprintf (0, "RingInitialize(): error initializing stream lock: %s", strerror(rc));
      return -2;
    }
  
  /* Initialize volatile ring packet buffer parameters */
  (*ringparams)->writelock = &writelock;
  (*ringparams)->streamlock = &streamlock;
  (*ringparams)->mmapflag = mmapflag;
  (*ringparams)->volatileflag = volatileflag;
  (*ringparams)->streamidx = RBTreeCreate (KeyCompare, free, free);
  (*ringparams)->streamcount = 0;
  (*ringparams)->ringstart = HPnow();
  (*ringparams)->data = ((char *) (*ringparams)) + headersize;
  
  /* Validate existing ring packet buffer parameters, resetting if needed */
  if ( ringinit ||
       memcmp((*ringparams)->signature, RING_SIGNATURE, sizeof((*ringparams)->signature)) ||
       (*ringparams)->version != RING_VERSION ||
       (*ringparams)->ringsize != ringsize ||
       (*ringparams)->pktsize != pktsize ||
       (*ringparams)->maxpktid != maxpktid ||
       (*ringparams)->maxpackets != maxpackets ||
       (*ringparams)->maxoffset != maxoffset ||
       (*ringparams)->headersize != headersize )
    {
      /* Report what triggered the parameter reset if not just initialized */
      if ( ! ringinit )
	{
	  if ( memcmp((*ringparams)->signature, RING_SIGNATURE, sizeof((*ringparams)->signature)) )
	    lprintf (0, "** Packet buffer signature mismatch: %.4s <-> %.4s", (*ringparams)->signature, RING_SIGNATURE);
	  if ( (*ringparams)->version != RING_VERSION )
	    lprintf (0, "** Packet buffer version change: %d -> %d", (*ringparams)->version, RING_VERSION);
	  if ( (*ringparams)->ringsize != ringsize )
	    lprintf (0, "** Packet buffer size change: %llu -> %llu", (*ringparams)->ringsize, ringsize);
	  if ( (*ringparams)->pktsize != pktsize )
	    lprintf (0, "** Packet size change: %u -> %u", (*ringparams)->pktsize, pktsize);
	  if ( (*ringparams)->maxpktid != maxpktid )
	    lprintf (0, "** Maximum packet ID change: %lld -> %lld", (*ringparams)->maxpktid, maxpktid);
	  if ( (*ringparams)->maxpackets != maxpackets )
	    lprintf (0, "** Maximum packets change: %lld -> %lld", (*ringparams)->maxpackets, maxpackets);
	  if ( (*ringparams)->maxoffset != maxoffset )
	    lprintf (0, "** Maximum offset change: %lld -> %lld", (*ringparams)->maxoffset, maxoffset);
	  if ( (*ringparams)->headersize != headersize )
	    lprintf (0, "** Header size change: %u -> %u", (*ringparams)->headersize, headersize);
	}
      
      lprintf (0, "Resetting ring packet buffer parameters");
      
      memcpy ((*ringparams)->signature, RING_SIGNATURE, sizeof((*ringparams)->signature));
      (*ringparams)->version = RING_VERSION;
      (*ringparams)->ringsize = ringsize;
      (*ringparams)->pktsize = pktsize;
      (*ringparams)->maxpktid = maxpktid;
      (*ringparams)->maxpackets = maxpackets;
      (*ringparams)->maxoffset = maxoffset;
      (*ringparams)->headersize = headersize;
      (*ringparams)->corruptflag = 0;
      (*ringparams)->fluxflag = 0;
      (*ringparams)->earliestid = 0;
      (*ringparams)->earliestptime = HPTERROR;
      (*ringparams)->earliestdstime = HPTERROR;
      (*ringparams)->earliestdetime = HPTERROR;
      (*ringparams)->earliestoffset = -1;
      (*ringparams)->latestid = 0;
      (*ringparams)->latestptime = HPTERROR;
      (*ringparams)->latestdstime = HPTERROR;
      (*ringparams)->latestdetime = HPTERROR;
      (*ringparams)->latestoffset = -1;
      (*ringparams)->txpacketrate = 0.0;
      (*ringparams)->txbyterate = 0.0;
      (*ringparams)->rxpacketrate = 0.0;
      (*ringparams)->rxbyterate = 0.0;
      
      /* Clear unused header space */
      memset (((char *)(*ringparams))+sizeof(RingParams), 0, headersize-sizeof(RingParams));
    }
  /* If the ring has not been reset and packets are present recover stream index */
  else if ( (*ringparams)->earliestoffset >= 0 )
    {
      lprintf (1, "Recovering stream index");
      
      /* Open stream index file */
      if ( (streamidxfd = open (streamfilename, O_RDONLY, 0)) < 0 )
	{
	  lprintf (0, "RingInitialize(): error opening %s: %s", streamfilename, strerror(errno));
	  return -1;
	}
      
      /* Stat the streams file */
      if ( fstat (streamidxfd, &streamfilestat) )
	{
	  lprintf (0, "RingInitialize(): error stating %s: %s", streamfilename, strerror(errno));
	  return -1;
	}
      
      if ( streamfilestat.st_size > 0 )
	{
	  /* Read the saved RingStreams */
	  while ( (rv = read (streamidxfd, &stream, sizeof(RingStream)) == sizeof(RingStream)) )
	    {
	      /* Re-populating streams index */
	      if ( ! AddStreamIdx ((*ringparams)->streamidx, &stream, 0) )
		{
		  lprintf (0, "RingInitialize(): error adding stream to index");
		  corruptring = 1;
		}
	      else
		{
		  (*ringparams)->streamcount++;
		}
	    }
	  
	  /* Test for read error */
	  if ( rv < 0 )
	    {
	      lprintf (0, "RingInitialize(): error reading %s: %s", streamfilename, strerror(errno));
	      return -1;
	    }
	}
      else
	{
	  lprintf (0, "RingInitialize(): stream index file empty!");
	  return -1;
	}
      
      /* Close the stream index file and release file name memory */
      close (streamidxfd);
    }
  
  /* Sanity checks: compare earliest and latest packet offsets between RingParams and lookups
   * and check the earliest and latest stream entries. */
  if ( (*ringparams)->earliestid )
    {
      if ( ! (packetptr = GetPacket ((*ringparams), (*ringparams)->earliestid, 0)) )
	{
	  lprintf (0, "RingInitialize(): error getting index of earliest packet, ring corrupted");
	  corruptring = 1;
	}
      else if ( packetptr->offset != (*ringparams)->earliestoffset )
	{
	  lprintf (0, "RingInitialize(): error comparing earliest packet offsets, ring corrupted");
	  corruptring = 1;
	}
      else if ( ! (streamptr = GetStreamIdx ((*ringparams)->streamidx, packetptr->streamid)) )
	{
	  lprintf (0, "RingInitialize(): error finding stream entry for earliest packet, ring corrupted");
	  corruptring = 1;
	}
    }
  if ( (*ringparams)->latestid )
    {
      if ( ! (packetptr = GetPacket (*ringparams, (*ringparams)->latestid, 0)) )
	{
	  lprintf (0, "RingInitialize(): error getting index of latest packet, ring corrupted");
	  corruptring = 1;
	}
      else if ( packetptr->offset != (*ringparams)->latestoffset )
	{
	  lprintf (0, "RingInitialize(): error comparing latest packet offsets, ring corrupted");
	  corruptring = 1;
	}
      else if ( ! (streamptr = GetStreamIdx ((*ringparams)->streamidx, packetptr->streamid)) )
	{
	  lprintf (0, "RingInitialize(): error finding stream entry for latest packet, ring corrupted");
	  corruptring = 1;
	}
    }
  
  /* If corruption was detected cleanup before returning */
  if ( corruptring )
    {
      RBTreeDestroy ((*ringparams)->streamidx);      
      
      /* Unmap the ring file */
      if ( munmap ((void *)(*ringparams), ringsize) )
	{
	  lprintf (0, "RingInitialize(): error unmapping ring file: %s", strerror(errno));
	  return -1;
	}
      
      /* Close the ring file and re-init the descriptor */ 
      if ( close (*ringfd) )
	{
	  lprintf (0, "RingInitialize(): error closing ring file: %s", strerror(errno));
	  return -1;
	}
      *ringfd = -1;
      
      return -1;
    }
  
  lprintf (0, "Ring initialized, ringsize: %llu, pktsize: %u (%u)",
           ringsize, pktsize, (pktsize - sizeof(RingPacket)));
  
  /* Log the critical ring parameters if verbose enough */
  lprintf (2, "   maxpackets: %lld, maxpktid: %lld", maxpackets, maxpktid);
  lprintf (2, "   maxoffset: %lld, headersize: %u", maxoffset, headersize);
  lprintf (2, "   earliest packet ID: %lld, offset: %lld",
	   (*ringparams)->earliestid, (*ringparams)->earliestoffset);
  ms_hptime2mdtimestr ((*ringparams)->earliestptime, timestr, 1);
  lprintf (2, "   earliest packet creation time: %s",
	   ((*ringparams)->earliestptime == HPTERROR)?"NONE":timestr);
  ms_hptime2mdtimestr ((*ringparams)->earliestdstime, timestr, 1);
  lprintf (2, "   earliest packet data start time: %s",
	   ((*ringparams)->earliestdstime == HPTERROR)?"NONE":timestr);
  lprintf (2, "   latest packet ID: %lld, offset: %lld",
	   (*ringparams)->latestid, (*ringparams)->latestoffset);
  ms_hptime2mdtimestr ((*ringparams)->latestptime, timestr, 1);
  lprintf (2, "   latest packet creation time: %s",
	   ((*ringparams)->latestptime == HPTERROR)?"NONE":timestr);
  ms_hptime2mdtimestr ((*ringparams)->latestdstime, timestr, 1);
  lprintf (2, "   latest packet data start time: %s",
	   ((*ringparams)->latestdstime == HPTERROR)?"NONE":timestr);
  
  return 0;
}  /* End of RingInitialize() */


/***************************************************************************
 * RingShutdown:
 *
 * Perform shutdown procedures for the ring buffer.
 *
 * After the writelock of the mmap'd ring has been obtained the
 * streams index is written to streamfilename and the ring is either
 * unmapped or written to the open ringfd and closed.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
RingShutdown (int ringfd, char *streamfilename, RingParams *ringparams)
{
  int streamidxfd;
  int rc;
  int rv = 0;
  Stack *streams;
  RingStream *stream;
  
  RBNode *tnode;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  
  if ( ! ringparams )
    return -1;
  
  if ( ! ringparams->volatileflag && ( ! ringfd || ! streamfilename ) )
    return -1;
  
  /* Free memory and return if ring is volatile */
  if ( ringparams->volatileflag )
    {
      RBTreeDestroy (ringparams->streamidx);
      free (ringparams);
      return 0;
    }
  
  /* Open stream index file */
  if ( (streamidxfd = open (streamfilename, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0 )
    {
      lprintf (0, "RingShutdown(): error opening %s: %s", streamfilename, strerror(errno));
      rv = -1;
    }
  
  /* Lock ring against writes, never give this up, destroyed later */
  pthread_mutex_lock (ringparams->writelock);
  
  /* Set ring flux flag */
  ringparams->fluxflag = 1;
  
  /* Create Stack of RingStreams */
  streams = StackCreate();
  RBBuildStack (ringparams->streamidx, streams);
  
  /* Write RingStreams to stream index file */
  lprintf (1, "Writing stream index file");
  while ( (tnode = (RBNode *) StackPop(streams)) )
    {
      stream = (RingStream *) tnode->data;
      
      if ( write (streamidxfd, stream, sizeof(RingStream)) != sizeof(RingStream) )
	{
	  lprintf (0, "RingShutdown(): error writing to %s: %s", streamfilename, strerror(errno));
	  rv = -1;
	}
    }
  
  /* Close the streams file */
  if ( close (streamidxfd) )
    {
      lprintf (0, "RingShutdown(): error closing %s: %s", streamfilename, strerror(errno));
      rv = -1;
    }
  
  /* Cleanup stream index related memory */
  RBTreeDestroy (ringparams->streamidx);
  StackDestroy (streams, 0);
  ringparams->streamidx = NULL;
  
  /* Destroy streams index lock */
  if ( (rc = pthread_mutex_destroy (ringparams->streamlock)) )
    {
      lprintf (0, "RingShutdown(): error destroying stream lock: %s", strerror(rc));
      rv = -1;
    }
  ringparams->streamlock = NULL;
  
  if ( ringparams->mmapflag )
    {
      /* Clear ring flux flag */
      ringparams->fluxflag = 0;
      
      /* Destroy ring write lock */
      pthread_mutex_unlock (ringparams->writelock);
      if ( (rc = pthread_mutex_destroy (ringparams->writelock)) )
	{
	  lprintf (0, "RingShutdown(): error destroying ring write lock: %s", strerror(rc));
	  rv = -1;
	}
      ringparams->writelock = NULL;
      
      /* Unmap the ring buffer file */
      lprintf (1, "Unmapping and closing ring buffer file");
      if ( munmap ((void *)ringparams, ringparams->ringsize) )
	{
	  lprintf (0, "RingShutdown(): error unmapping ring buffer file: %s", strerror(errno));
	  rv = -1;
	}
    }
  else
    {
      /* Write the ring buffer file */
      lprintf (1, "Writing and closing ring buffer file");
      
      if ( lseek (ringfd, 0, SEEK_SET) == -1  )
	{
	  lprintf (0, "RingShutdown(): error seeking in ring buffer file: %s", strerror(errno));
	  rv = -1;
	}
      
      /* Clear ring flux flag */
      ringparams->fluxflag = 0;
      
      /* Destroy ring write lock */
      pthread_mutex_unlock (ringparams->writelock);
      if ( (rc = pthread_mutex_destroy (ringparams->writelock)) )
	{
	  lprintf (0, "RingShutdown(): error destroying ring write lock: %s", strerror(rc));
	  rv = -1;
	}
      ringparams->writelock = NULL;
      
      if ( write (ringfd, ringparams, ringparams->ringsize) != ringparams->ringsize )
	{
	  lprintf (0, "RingShutdown(): error writing ring buffer file: %s", strerror(errno));
	  rv = -1;
	}
      
      /* Free the ring buffer memory */
      free (ringparams);
    }
  
  /* Close the ring file */
  if ( close (ringfd) )
    {
      lprintf (0, "RingShutdown(): error closing ring buffer file: %s", strerror(errno));
      rv = -1;
    }
  
  return rv;
}  /* End of RingShutdown() */


/***************************************************************************
 * RingWrite:
 *
 * Add packet to the ring including updates to the packet and stream
 * indexes.
 *
 * This routine will set the pktid, offset, pkttime, nextpacket and
 * nextstream values for the packet after they are determined.  If
 * this routine fails after starting to modify the ring constructs the
 * ring will almost certainly be out of sync and should be considered
 * corrupt, this is indicated with a return value of -2.
 *
 * If ring corruption is detected the corruptflag ring parameter will
 * be set in order to trigger auto recovery on the next start.
 *
 * Returns 0 on success, -1 on non-corruption error and -2 on corrupt
 * ring error.
 ***************************************************************************/
int
RingWrite (RingParams *ringparams, RingPacket *packet,
	   char *packetdata, uint32_t datasize)
{
  RingStream *stream;
  RingStream  newstream;
  RingPacket *earliest = 0;
  RingPacket *latest = 0;
  RingPacket *prevlatest;
  RBNode *node = 0;
  Key *skey;
  
  int64_t pktid;
  int64_t offset;
  
  if ( ! ringparams || ! packet || ! packetdata )
    return -1;
  
  /* Check packet size */
  if ( (sizeof(RingPacket) + datasize) > ringparams->pktsize )
    {
      lprintf (0, "RingWrite(): %s packet size too large (%d), maximum is %d bytes",
	       packet->streamid, (sizeof(RingPacket) + datasize), ringparams->pktsize);
      return -1;
    }
  
  /* Lock ring and streams index */
  pthread_mutex_lock (ringparams->writelock);
  pthread_mutex_lock (ringparams->streamlock);
  
  /* Set ring flux flag */
  ringparams->fluxflag = 1;
  
  /* Get packet entries for earliest and latest packets in ring */
  if ( ringparams->earliestid > 0 )
    if ( ! (earliest = GetPacket (ringparams, ringparams->earliestid, 0)) )
      {
	lprintf (0, "RingWrite(): Error getting earliest packet index");
	ringparams->corruptflag = 1;
	ringparams->fluxflag = 0;
	pthread_mutex_unlock (ringparams->writelock);
	pthread_mutex_unlock (ringparams->streamlock);
	return -2;
      }
  if ( ringparams->latestid > 0 )
    if ( ! (latest = GetPacket (ringparams, ringparams->latestid, 0)) )
      {
	lprintf (0, "RingWrite(): Error getting latest packet index");
	ringparams->corruptflag = 1;
	ringparams->fluxflag = 0;
	pthread_mutex_unlock (ringparams->writelock);
	pthread_mutex_unlock (ringparams->streamlock);
	return -2;
      }
  
  /* Determine next packet ID and offset */
  if ( ! earliest && ! latest )
    {
      pktid = 1;
      offset = 0;
    }
  else
    {
      /* Determine next ID: Increment or roll back to 1 if beyond the maxpktid */
      pktid = NEXTID(latest->pktid, ringparams->maxpktid);
      /* Increment offset by pktsize or roll back to 0 if beyond max offset */
      offset = ( (latest->offset+ringparams->pktsize) > ringparams->maxoffset ) ? 
	0 : (latest->offset+ringparams->pktsize);
    }
  
  /* Update new packet details */
  packet->pktid = pktid;
  packet->offset = offset;
  packet->pkttime = HPnow();
  packet->nextinstream = 0;
  
  /* Remove earliest packet if ring is full (next == earliest) */
  if ( earliest && latest && earliest != latest )
    {
      if ( offset == ringparams->earliestoffset )
	{
	  int64_t     nextid;             /* New earliest packet ID in ring*/
	  RingPacket *nextInRing = 0;     /* New earliest packet in ring */
	  RingPacket *nextInStream = 0;   /* New earliest packet in stream */
	  RingStream *streamOfEarliest = 0;  /* Stream of old earliest packet */
	  
	  nextid = NEXTID(earliest->pktid, ringparams->maxpktid);
	  
	  if ( ! (nextInRing = GetPacket (ringparams, nextid, 0)) )
	    {
	      lprintf (0, "RingWrite(): Error getting next earliest ID: %lld, current earliest: %lld",
		       nextid, earliest->pktid);
              ringparams->corruptflag = 1;
              ringparams->fluxflag = 0;
	      pthread_mutex_unlock (ringparams->writelock);
	      pthread_mutex_unlock (ringparams->streamlock);
	      return -2;
	    }
	  if ( earliest->nextinstream )
	    if ( ! (nextInStream = GetPacket (ringparams, earliest->nextinstream, 0)) )
	      {
		lprintf (0, "RingWrite(): Error getting new earliest stream packet index");
                ringparams->corruptflag = 1;
                ringparams->fluxflag = 0;
		pthread_mutex_unlock (ringparams->writelock);
		pthread_mutex_unlock (ringparams->streamlock);
		return -2;
	      }
	  if ( ! (streamOfEarliest = GetStreamIdx (ringparams->streamidx, earliest->streamid)) )
	    {
	      lprintf (0, "RingWrite(): Error getting earliest packet stream");
              ringparams->corruptflag = 1;
              ringparams->fluxflag = 0;
	      pthread_mutex_unlock (ringparams->writelock);
	      pthread_mutex_unlock (ringparams->streamlock);
	      return -2;
	    }
	  
	  lprintf (3, "Removing packet for stream %s (id: %lld)",
		   earliest->streamid, earliest->pktid);
	  
	  /* Update RingParams with new earliest entry */
	  ringparams->earliestid = nextInRing->pktid;
	  ringparams->earliestptime = nextInRing->pkttime;
	  ringparams->earliestdstime = nextInRing->datastart;
	  ringparams->earliestdetime = nextInRing->dataend;
	  ringparams->earliestoffset = nextInRing->offset;
	  
	  /* Delete stream entry if this is the only packet */
	  if ( earliest->pktid == streamOfEarliest->earliestid &&
	       earliest->pktid == streamOfEarliest->latestid )
	    {
	      lprintf (2, "Removing stream index entry for %s", earliest->streamid);
	      DelStreamIdx (ringparams->streamidx, earliest->streamid);
	      ringparams->streamcount--;
	    }
	  /* Else update stream entry for the next packet in the stream */
	  else if ( nextInStream )
	    {
	      streamOfEarliest->earliestdstime = nextInStream->datastart;
	      streamOfEarliest->earliestdetime = nextInStream->dataend;
	      streamOfEarliest->earliestptime = nextInStream->pkttime;
	      streamOfEarliest->earliestid = nextInStream->pktid;
	    }
	}
    }
  
  /* Find RingStream entry */
  if ( ! (stream = GetStreamIdx (ringparams->streamidx, packet->streamid)) )
    {
      /* Populate and add RingStream entry */
      memset (&newstream, 0, sizeof(RingStream));
      strncpy (newstream.streamid, packet->streamid, sizeof(newstream.streamid));
      newstream.earliestdstime = packet->datastart;
      newstream.earliestdetime = packet->dataend;
      newstream.earliestptime = packet->pkttime;
      newstream.earliestid = packet->pktid;
      newstream.latestdstime = packet->datastart;
      newstream.latestdetime = packet->dataend;
      newstream.latestptime = packet->pkttime;
      newstream.latestid = 0;  /* Latest ID will get set later */
      
      /* Add new stream to index */
      if  ( ! (stream = AddStreamIdx (ringparams->streamidx, &newstream, &skey)) )
	{
	  lprintf (0, "RingWrite(): Error adding new stream index");
	  if ( node ) { free (node->key); free (node->data); free (node); }
          ringparams->corruptflag = 1;
          ringparams->fluxflag = 0;
          pthread_mutex_unlock (ringparams->writelock);
	  pthread_mutex_unlock (ringparams->streamlock);
	  return -2;
	}
      else
	{
	  ringparams->streamcount++;
	}
      
      lprintf (2, "Added stream entry for %s (key: %lld)", packet->streamid, *skey);
    }
  
  /* Copy packet header into ring */
  memcpy ((ringparams->data+offset), packet, sizeof(RingPacket));
  
  /* Copy packet data into ring directly after header */
  memcpy ((ringparams->data+offset+sizeof(RingPacket)), packetdata, datasize);
  
  /* Update RingParams with new latest packet */
  ringparams->latestid = packet->pktid;
  ringparams->latestptime = packet->pkttime;
  ringparams->latestdstime = packet->datastart;
  ringparams->latestdetime = packet->dataend;
  ringparams->latestoffset = packet->offset;
  
  /* Update RingParams with new earliest packet (for initial packet) */
  if ( ! earliest )
    {
      ringparams->earliestid = packet->pktid;
      ringparams->earliestptime = packet->pkttime;
      ringparams->earliestdstime = packet->datastart;
      ringparams->earliestdetime = packet->dataend;
      ringparams->earliestoffset = packet->offset;
    }
  
  /* Update entry for previous packet in stream */
  if ( stream->latestid > 0 )
    {
      if ( ! (prevlatest = GetPacket (ringparams, stream->latestid, 0)) )
	{
	  lprintf (0, "RingWrite(): Error getting next packet in stream (id: %lld)", stream->latestid);
          ringparams->corruptflag = 1;
          ringparams->fluxflag = 0;
	  pthread_mutex_unlock (ringparams->writelock);
	  pthread_mutex_unlock (ringparams->streamlock);
	  return -2;
	}
      
      prevlatest->nextinstream = pktid;
    }
  
  /* Update stream entry */
  stream->latestdstime = packet->datastart;
  stream->latestdetime = packet->dataend;
  stream->latestptime = packet->pkttime;
  stream->latestid = packet->pktid;
  
  /* Clear ring flux flag */
  ringparams->fluxflag = 0;
  
  /* Unlock ring and stream index */
  pthread_mutex_unlock (ringparams->writelock);
  pthread_mutex_unlock (ringparams->streamlock);
  
  lprintf (3, "Added packet for stream %s, pktid: %lld, offset: %lld",
	   packet->streamid, packet->pktid, packet->offset);
  
  return 0;
}  /* End of RingWrite() */


/***************************************************************************
 * RingRead:
 *
 * Read a requested packet ID from the ring.  The requested ID can be
 * a positional value: RINGCURRENT, RINGEARLIEST or RINGLATEST.  The
 * packet pointer must point to already allocated memory.  The packet
 * data will only be returned if the packetdata pointer is not 0 and
 * points to already allocated memory.
 *
 * Returns positive packet ID on success, 0 when the packet was not
 * found and -1 on error.
 ***************************************************************************/
int64_t
RingRead (RingReader *reader, int64_t reqid,
	  RingPacket *packet, char *packetdata)
{
  RingParams *ringparams;
  RingPacket *pkt;
  hptime_t pkttime;
  int64_t pktid = 0;
  
  if ( ! reader || ! packet )
    return -1;
  
  ringparams = reader->ringparams;
  if ( ! ringparams )
    return -1;
  
  /* Determine packet ID to request */
  if ( reqid == RINGEARLIEST )
    {
      pktid = ringparams->earliestid;
    }
  else if ( reqid == RINGLATEST )
    {
      pktid = ringparams->latestid;
    }
  else if ( reqid == RINGCURRENT )
    {
      pktid = reader->pktid;
    }
  else if ( reqid < 0 )
    {
      lprintf (0, "RingRead(): unsupported position value: %lld", reqid);
      return -1;
    }
  else
    {
      pktid = reqid;
    }
  
  /* If requested ID is 0 we already know it's not available */
  if ( pktid == 0 )
    {
      return 0;
    }
  
  /* Get RingPacket from Index */
  if ( ! (pkt = GetPacket(ringparams, pktid, &pkttime)) )
    {
      return 0;
    }
  
  /* Copy packet header */
  memcpy (packet, pkt, sizeof(RingPacket));
  
  /* Copy packet data if a pointer is supplied */
  if ( packetdata )
    memcpy (packetdata, (char*)pkt + sizeof(RingPacket), pkt->datasize);
  
  /* Sanity check that the data was not overwritten during the copy */
  if ( pktid != pkt->pktid )
    {
      return 0;
    }
  
  /* Update reader position value */
  reader->pktid = packet->pktid;
  reader->pkttime = packet->pkttime;
  reader->datastart = packet->datastart;
  reader->dataend = packet->dataend;
  
  return pktid;
}  /* End of RingRead() */


/***************************************************************************
 * RingReadNext:
 *
 * Determine and read the next packet from the ring.  The packet
 * pointer must point to already allocated memory.  The packet data
 * will only be returned if the packetdata pointer is not 0 and points
 * to already allocated memory.
 *
 * If the packet being searched for does not exist and is not the next
 * expected packet that will enter the ring, assume that the read
 * position has fallen off the trailing edge of the ring and
 * reposition the search at the earliest packet.
 *
 * Returns positive packet ID on success, 0 when the packet was not
 * found and -1 on error.
 ***************************************************************************/
int64_t
RingReadNext (RingReader *reader, RingPacket *packet, char *packetdata)
{
  RingParams *ringparams;
  RingPacket *pkt;
  hptime_t pkttime;
  int64_t pktid = 0;
  uint8_t skip;
  uint32_t skipped;
  
  int64_t latestoffset;
  int64_t earliestid;
  int64_t latestid;
  hptime_t latestptime;
  hptime_t latestdstime;
  hptime_t latestdetime;
  
  if ( ! reader || ! packet )
    return -1;
  
  ringparams = reader->ringparams;
  if ( ! ringparams )
    return -1;
  
  latestoffset = ringparams->latestoffset;
  earliestid = ringparams->earliestid;
  
  /* Determine latest packet details directly to avoid race */
  latestid = ((RingPacket*) (ringparams->data + latestoffset))->pktid;
  latestptime = ((RingPacket*) (ringparams->data + latestoffset))->pkttime;
  latestdstime = ((RingPacket*) (ringparams->data + latestoffset))->datastart;
  latestdetime = ((RingPacket*) (ringparams->data + latestoffset))->dataend;
  
  /* Determine packet ID for relative positions */
  if ( reader->pktid < 0 )
    {
      if ( reader->pktid == RINGNEXT )
	{
	  /* If ring is empty (no latest ID) set to earliest */
	  if ( latestid == 0 )
	    {
	      reader->pktid = RINGEARLIEST;
	    }
	  /* Otherwise set to latest packet */
	  else if ( latestid > 0 )
	    {
	      reader->pktid = latestid;
	      reader->pkttime = latestptime;
	      reader->datastart = latestdstime;
	      reader->dataend = latestdetime;
	    }
	  
	  return 0;
	}
      else if ( reader->pktid == RINGEARLIEST )
	{
	  if ( earliestid > 0 )
	    {
	      pktid = earliestid;
	    }
	  else
	    {
	      return 0;
	    }
	}
      else if ( reader->pktid == RINGLATEST )
	{
	  if ( latestid > 0 )
	    {
	      pktid = latestid;
	    }
	  else
	    {
	      return 0;
	    }
 	}
      else
	{
	  lprintf (0, "RingReadNext(): unsupported position value: %lld",
		   reader->pktid);
	  return -1;
	}
    }
  /* Determine next packet ID based on current ID */
  else
    {
      /* If no current packet return immediately */
      if ( reader->pktid == 0 )
	{
	  /* Revert to latest packet */
	  if ( latestid > 0 )
	    {
	      reader->pktid = latestid;
	      reader->pkttime = latestptime;
	      reader->datastart = latestdstime;
	      reader->dataend = latestdetime;
	    }
	  
	  return 0;
	}
      /* If current packet is the latest in the ring return immediately,
       * this means we will never search for the next expected packet number. */
      else if ( reader->pktid == latestid && 
		reader->pkttime == latestptime )
	{
	  return 0;
	}
      
      /* Determine next ID: Increment or roll back to 1 if beyond the maxpktid */
      pktid = NEXTID(reader->pktid, ringparams->maxpktid);
    }
  
  /* If requested ID is 0 we already know it's not available */
  if ( pktid == 0 )
    {
      return 0;
    }
  
  /* Loop until we have a matching packet or no next */
  skip = 1;
  skipped = 0;
  while ( skip )
    {
      skip = 0;
      
      /* Get packet header from index */
      if ( ! (pkt = GetPacket(ringparams, pktid, &pkttime)) )
	{
	  /* If packet was not found, and it is already known to not be the next
	   * expected packet, then assume the reader has been lapped (fallen off 
	   * the trailing edge of the buffer) and reposition to the earliest packet */
	  
	  pktid = ringparams->earliestid;
	  skipped++;
	  
	  /* Safety value to avoid skipping off the trailing edge of the buffer forever */
	  if ( skipped > 100 )
	    {
	      lprintf (0, "RingReadNext(): skipped off trailing edge of buffer %d times", skipped);
	      return 0;
	    }
	  
	  skip = 1;
	  continue;
	}
      
      skipped = 0;
      
      /* Update reader position */
      reader->pktid = pktid;
      reader->pkttime = pkttime;
      reader->datastart = pkt->datastart;
      reader->dataend = pkt->dataend;
      
      /* Test limit expression if available */
      if ( reader->limit )
	if ( pcre_exec(reader->limit, reader->limit_extra, pkt->streamid, strlen(pkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test match expression if available and not already skipping */
      if ( reader->match && skip == 0 )
	if ( pcre_exec(reader->match, reader->match_extra, pkt->streamid, strlen(pkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test reject expression if available and not already skipping */
      if ( reader->reject && skip == 0 )
	if ( ! pcre_exec(reader->reject, reader->reject_extra, pkt->streamid, strlen(pkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Sanity check that the packet was not removed since GetPacket() */
      if ( pkt->pktid != pktid || pkt->pkttime != pkttime )
	skip = 1;
      
      /* If skipping this packet move to the next packet in the ring */
      if ( skip )
	{
	  /* If there is no next packet return immediately */
	  if ( pktid == latestid )
	    {
	      return 0;
	    }
	  /* Otherwise continue with the next packet */
	  else
	    {
	      pktid = NEXTID(reader->pktid, ringparams->maxpktid);
	    }
	}
    }
  
  /* Copy packet header */
  memcpy (packet, pkt, sizeof(RingPacket));
  
  /* Copy packet data if a pointer is supplied */
  if ( packetdata )
    memcpy (packetdata, (char*)pkt + sizeof(RingPacket), pkt->datasize);
  
  /* Sanity check that the data was not overwritten during the copy */
  if ( pktid != pkt->pktid )
    {
      return 0;
    }
  
  return pktid;
}  /* End of RingReadNext() */


/***************************************************************************
 * RingPosition:
 *
 * Set the ring reading position to the specified packet ID, checking
 * that the ID is a valid packet in the ring.  If the pkttime value is
 * not HPTERROR it will also be checked and should match the requested
 * packet ID.  The current read position is not changed if any errors
 * occur.
 *
 * If the packet is successfully found the RingReader.pktid will be
 * updated.
 *
 * Returns positive packet ID on success, 0 when the packet was not
 * found and -1 on error.
 ***************************************************************************/
int64_t
RingPosition (RingReader *reader, int64_t pktid, hptime_t pkttime)
{
  RingParams *ringparams;
  RingPacket *pkt;
  hptime_t ptime;
  hptime_t datastart, dataend;
  
  if ( ! reader )
    return -1;
  
  ringparams = reader->ringparams;
  if ( ! ringparams )
    return -1;
  
  /* Determine packet ID for relative positions */
  if ( pktid == RINGEARLIEST )
    {
      pktid = ringparams->earliestid;
    }
  else if ( pktid == RINGLATEST )
    {
      pktid = ringparams->latestid;
    }
  else if ( pktid < 0 )
    {
      lprintf (0, "RingPosition(): unsupported position value: %lld", pktid);
      return -1;
    }
  
  /* Get RingPacket from Index */
  if ( ! (pkt = GetPacket (ringparams, pktid, &ptime)) )
    {
      return 0;
    }
  
  /* Check for matching pkttime if not HPTERROR */
  if ( pkttime != HPTERROR )
    {
      if ( pkttime != pkt->pkttime )
	{
	  return 0;
	}
    }
  
  datastart = pkt->datastart;
  dataend = pkt->dataend;
  
  /* Sanity check that the data was not overwritten during the copy */
  if ( pktid != pkt->pktid )
    {
      return 0;
    }
  
  /* Update reader position value */
  reader->pktid = pktid;
  reader->pkttime = ptime;
  reader->datastart = datastart;
  reader->dataend = dataend;
  
  return pktid;
}  /* End of RingPosition() */


/***************************************************************************
 * RingAfter:
 *
 * Set the ring reading position to a matching packet (as defined by
 * the readers's match and reject expressions) based on packet data
 * time.  The ring is searched from the earliest packet forward,
 * stopping at the first matching packet with a data end time after
 * the reference time.
 *
 * The position can be set to either the first packet with a data time
 * after the reference time or the packet just before depending on the
 * whence argument.
 *
 * whence:
 * 0 = Set position to the packet just prior that found for whence == 1.
 * 1 = Set position to first packet with data end time after that specified.
 *
 * The whence == 0 option is useful if the reader will subsequently
 * call RingReadNext() and the first matched packet is desireable
 * (RingReadNext() will not return the current ID, but the next ID).
 *
 * If a packet is successfully found in the ring the reader.pktid will
 * be updated.  The current read position is not changed if any errors
 * occur.
 *
 * Returns positive packet ID on success, 0 when the packet was not
 * found and -1 on error.
 ***************************************************************************/
int64_t
RingAfter (RingReader *reader, hptime_t reftime, int whence)
{
  RingParams *ringparams;
  RingPacket *pkt0 = 0, *pkt1 = 0;
  hptime_t pkttime0 = HPTERROR;
  hptime_t pkttime1 = HPTERROR;
  hptime_t datastart, dataend;
  int64_t pktid0, pktid1;
  int64_t skipped = 0;
  uint8_t skip;
  
  if ( ! reader )
    return -1;
  
  ringparams = reader->ringparams;
  if ( ! ringparams )
    return -1;
  
  /* Start searching with the earliest packet in the ring */
  pktid1 = ringparams->earliestid;
  pktid0 = pktid1;
  
  /* Loop through packets in forward order */
  while ( skipped < ringparams->maxpackets )
    {
      skip = 0;
      
      /* Get pointer to RingPacket */
      if ( ! (pkt1 = GetPacket (ringparams, pktid1, &pkttime1)) )
	{
	  /* Avoid skipping off the bottom, continue if no packets seen */
	  if ( skipped == 0 )
	    {
	      pktid0 = pktid1 = NEXTID(pktid1, ringparams->maxpktid);
	      continue;
	    }
	  /* Otherwise this is an unrecognized problem */
	  else
	    {
	      return 0;
	    }
	}
      
      /* Test if packet is earlier than reference time, this will avoid the
       * regex tests for packets that we will eventually skip anyway */ 
      if ( pkt1->dataend < reftime )
	skip = 1;

      /* Test limit expression if available */
      if ( reader->limit && ! skip )
	if ( pcre_exec(reader->limit, reader->limit_extra, pkt1->streamid,
		       strlen(pkt1->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test match expression if available and not already skipping */
      if ( reader->match && ! skip )
	if ( pcre_exec(reader->match, reader->match_extra, pkt1->streamid,
		       strlen(pkt1->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test reject expression if available and not already skipping */
      if ( reader->reject && ! skip )
	if ( ! pcre_exec(reader->reject, reader->reject_extra, pkt1->streamid,
			 strlen(pkt1->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Done if this matching packet has a data end time after that specified */
      if ( ! skip && pkt1->dataend > reftime )
	{
	  break;
	}
      
      /* Shift skipped packet to the history value */
      pktid0 = pktid1;
      pkttime0 = pkttime1;
      pkt0 = pkt1;
      
      /* Done if we reach the latest packet */
      if ( pktid1 == ringparams->latestid )
	{
	  break;
	}
      
      pktid1 = NEXTID(pktid1, ringparams->maxpktid);
      skipped++;
    }
  
  /* Safety valve, if no packets were ever seen */
  if ( ! pkt1 )
    {
      return 0;
    }
  
  /* Position to packet before match if requested and not the first packet */
  if ( whence == 0 && skipped > 0 )
    {
      pktid1 = pktid0;
      pkttime1 = pkttime0;
      pkt1 = pkt0;
    }
  
  datastart = pkt1->datastart;
  dataend = pkt1->dataend;
  
  /* Sanity check that the data was not overwritten during the copy */
  if ( pktid1 != pkt1->pktid )
    {
      return 0;
    }
  
  /* Update reader position value */
  reader->pktid = pktid1;
  reader->pkttime = pkttime1;
  reader->datastart = datastart;
  reader->dataend = dataend;
  
  return pktid1;
}  /* End of RingAfter() */


/***************************************************************************
 * RingAfterRev:
 *
 * Set the ring reading position to a matching packet (as defined by
 * the readers's match and reject expressions) based on packet data
 * time.  The ring is searched from the latest packet backward,
 * stopping at the first matching packet with a data end time after
 * the reference time or after skipping pktlimit number of packets.
 *
 * The position can be set to either the first packet with a data time
 * after the reference time or the packet just before depending on the
 * whence argument.
 *
 * whence:
 * 0 = Set position to the packet just prior that found for whence == 1.
 * 1 = Set position to first packet with data end time after that specified.
 *
 * The whence == 0 option is useful if the reader will subsequently
 * call RingReadNext() and the first matched packet is desireable
 * (RingReadNext() will not return the current ID, but the next ID).
 *
 * If a packet is successfully found in the ring the reader.pktid will
 * be updated.  The current read position is not changed if any errors
 * occur.
 *
 * Returns positive packet ID on success, 0 when the packet was not
 * found and -1 on error.
 ***************************************************************************/
int64_t
RingAfterRev (RingReader *reader, hptime_t reftime, int64_t pktlimit,
	      int whence)
{
  RingParams *ringparams;
  RingPacket *pkt = 0;
  RingPacket *spkt = 0;
  hptime_t pkttime = HPTERROR;
  hptime_t spkttime = HPTERROR;
  hptime_t datastart, dataend;
  int64_t pktid, spktid;
  int64_t count = 0;
  uint8_t skip;
  
  if ( ! reader )
    return -1;
  
  ringparams = reader->ringparams;
  if ( ! ringparams )
    return -1;
  
  /* Start searching with the latest packet in the ring */
  pktid = ringparams->latestid;
  spktid = pktid;
  
  /* Loop through packets in reverse order */
  while ( count < pktlimit )
    {
      skip = 0;
      
      /* Get pointer RingPacket */
      if ( ! (spkt = GetPacket (ringparams, spktid, &spkttime)) )
	{
	  return 0;
	}
      
      /* Test if packet is later than reference time, this will avoid the
       * regex tests for packets that we will eventually skip anyway */ 
      if ( spkt->dataend > reftime )
	skip = 1;

      /* Test limit expression if available */
      if ( reader->limit )
	if ( pcre_exec(reader->limit, reader->limit_extra, spkt->streamid,
		       strlen(spkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test match expression if available and not already skipping */
      if ( reader->match && ! skip )
	if ( pcre_exec(reader->match, reader->match_extra, spkt->streamid,
		       strlen(spkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Test reject expression if available and not already skipping */
      if ( reader->reject && ! skip )
	if ( ! pcre_exec(reader->reject, reader->reject_extra, spkt->streamid,
			 strlen(spkt->streamid), 0, 0, NULL, 0) )
	  skip = 1;
      
      /* Set ID and time if this matching packet has a data end time after that specified */
      if ( ! skip && spkt->dataend > reftime )
	{
	  pkt = spkt;
	  pktid = spktid;
	  pkttime = spkttime;
	}
      
      /* Done if we reach the earliest packet */
      if ( spktid == ringparams->earliestid )
	{
	  break;
	}
      
      spktid = PREVID(spktid, ringparams->maxpktid);
      count++;
    }
  
  /* Safety valve, if no packets were ever seen */
  if ( ! pkt )
    {
      return 0;
    }
  
  /* Position to packet before match if requested and it exists */
  if ( whence == 0 )
    {
      /* Search for the previous packet */
      spktid = PREVID(pktid, ringparams->maxpktid); 
      
      /* Get pointer to RingPacket */
      if ( (spkt = GetPacket (ringparams, spktid, &spkttime)) )
	{
	  pkt = spkt;
	  pktid = spktid;
	  pkttime = spkttime;
	}
    }
  
  datastart = pkt->datastart;
  dataend = pkt->dataend;
  
  /* Sanity check that the data was not overwritten during the copy */
  if ( pktid != pkt->pktid )
    {
      return 0;
    }
  
  /* Update reader position value */
  reader->pktid = pktid;
  reader->pkttime = pkttime;
  reader->datastart = datastart;
  reader->dataend = dataend;
  
  return pktid;
}  /* End of RingAfterRev() */


/***************************************************************************
 * RingLimit:
 *
 * Compile the supplied limit pattern and assign it to the reader.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
RingLimit (RingReader *reader, char *pattern)
{
  const char *errptr;
  int erroffset;
  
  if ( ! reader )
    return -1;
  
  /* Compile pattern and assign to reader */
  if ( pattern )
    {
      /* Free existing complied expression */
      if ( reader->limit )
	pcre_free (reader->limit);
      
      /* Compile regex */
      reader->limit = pcre_compile (pattern, 0, &errptr, &erroffset, NULL);
      if ( errptr )
	{
	  lprintf (0, "RingLimit(): Error with pcre_compile: %s (offset: %d)", errptr, erroffset);
	  return -1;
	}
      
      /* Free existing study data */
      if ( reader->limit_extra )
	pcre_free (reader->limit_extra);
      
      /* Study regex */
      reader->limit_extra = pcre_study (reader->limit, 0, &errptr);
      if ( errptr )
	{
	  lprintf (0, "RingLimit(): Error with pcre_study: %s", errptr);
	  return -1;
	}
      
      /* Set limits on total matches and backtracking/recursion allowed */
      if ( reader->limit_extra )
	{
	  reader->limit_extra->flags = PCRE_EXTRA_MATCH_LIMIT | PCRE_EXTRA_MATCH_LIMIT_RECURSION;
	  reader->limit_extra->match_limit = 1000;
	  reader->limit_extra->match_limit_recursion = 1000;
	}
    }
  /* If no pattern, clear any existing regex */
  else
    {
      if ( reader->limit )
	pcre_free (reader->limit);
      reader->limit = 0;
      if ( reader->limit_extra )
	pcre_free (reader->limit_extra);
      reader->limit_extra = 0;
    }
  
  return 0;
}  /* End of RingLimit() */


/***************************************************************************
 * RingMatch:
 *
 * Compile the supplied match pattern and assign it to the reader.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
RingMatch (RingReader *reader, char *pattern)
{
  const char *errptr;
  int erroffset;

  if ( ! reader )
    return -1;
  
  /* Compile pattern and assign to reader */
  if ( pattern )
    {
      /* Free existing complied expression */
      if ( reader->match )
	pcre_free (reader->match);
      
      /* Compile regex */
      reader->match = pcre_compile (pattern, 0, &errptr, &erroffset, NULL);
      if ( errptr )
	{
	  lprintf (0, "RingMatch(): Error with pcre_compile: %s (offset: %d)", errptr, erroffset);
	  return -1;
	}
      
      /* Free existing study data */
      if ( reader->match_extra )
	pcre_free (reader->match_extra);
      
      /* Study regex */
      reader->match_extra = pcre_study (reader->match, 0, &errptr);
      if ( errptr )
	{
	  lprintf (0, "RingMatch(): Error with pcre_study: %s", errptr);
	  return -1;
	}
      
      /* Set limits on total matches and backtracking/recursion allowed */
      if ( reader->match_extra )
	{
	  reader->match_extra->flags = PCRE_EXTRA_MATCH_LIMIT | PCRE_EXTRA_MATCH_LIMIT_RECURSION;
	  reader->match_extra->match_limit = 1000;
	  reader->match_extra->match_limit_recursion = 1000;
	}
    }
  /* If no pattern, clear any existing regex */
  else
    {
      if ( reader->match )
	pcre_free (reader->match);
      reader->match = 0;
      if ( reader->match_extra )
	pcre_free (reader->match_extra);
      reader->match_extra = 0;
    }
  
  return 0;
}  /* End of RingMatch() */


/***************************************************************************
 * RingReject:
 *
 * Compile the supplied reject pattern and assign it to the reader.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
RingReject (RingReader *reader, char *pattern)
{
  const char *errptr;
  int erroffset;

  if ( ! reader )
    return -1;
  
  /* Compile pattern and assign to reader */
  if ( pattern )
    {
      /* Free existing complied expression */
      if ( reader->reject )
	pcre_free (reader->reject);
      
      /* Compile regex */
      reader->reject = pcre_compile (pattern, 0, &errptr, &erroffset, NULL);
      if ( errptr )
	{
	  lprintf (0, "RingReject(): Error with pcre_compile: %s (offset: %d)", errptr, erroffset);
	  return -1;
	}
      
      /* Free existing study data */
      if ( reader->reject_extra )
	pcre_free (reader->reject_extra);
      
      /* Study regex */
      reader->reject_extra = pcre_study (reader->reject, 0, &errptr);
      if ( errptr )
	{
	  lprintf (0, "RingReject(): Error with pcre_study: %s", errptr);
	  return -1;
	}

      /* Set limits on total matches and backtracking/recursion allowed */
      if ( reader->reject_extra )
	{
	  reader->reject_extra->flags = PCRE_EXTRA_MATCH_LIMIT | PCRE_EXTRA_MATCH_LIMIT_RECURSION;
	  reader->reject_extra->match_limit = 1000;
	  reader->reject_extra->match_limit_recursion = 1000;
	}
    }
  /* If no pattern, clear any existing regex */
  else
    {
      if ( reader->reject )
	pcre_free (reader->reject);
      reader->reject = 0;
      if ( reader->reject_extra )
	pcre_free (reader->reject_extra);
      reader->reject_extra = 0;
    }
  
  return 0;
}  /* End of RingReject() */


/***************************************************************************
 * StreamStackNodeCmp:
 *
 * Compare two RingStream entries contained in two StackNode entries
 * as the result of strncmp() on the two stream IDs.  This function is
 * used to sort a Stack of RingStream entries by stream ID.
 *
 * Return the result of strncmp() on the stream IDs.
 ***************************************************************************/
int
StreamStackNodeCmp (StackNode *a, StackNode *b)
{
  return strncmp (((RingStream *)(a->data))->streamid,
		  ((RingStream *)(b->data))->streamid,
		  MAXSTREAMID);
}  /* End of StreamStackNodeCmp() */


/***************************************************************************
 * GetStreamsStack:
 *
 * Build a copy of the stream index as a Stack sorted on stream ID.
 * It is up to the caller to free the Stack, i.e. using
 * StackDestroy(stack, free).
 *
 * If ringreader is not NULL only the streamids that match the
 * reader's limit and match expressions and do not match te reader's
 * reject expression will be included in the output Stack.
 *
 * Return a Stack on success and 0 on error.
 ***************************************************************************/
Stack *
GetStreamsStack (RingParams *ringparams, RingReader *reader)
{
  RingStream *stream;
  RingStream *newstream;
  RBNode *tnode;
  Stack *streams;
  Stack *newstreams;
  
  if ( ! ringparams )
    return 0;

  /* Lock the streams index */
  pthread_mutex_lock (ringparams->streamlock);
  
  streams = StackCreate();
  newstreams = StackCreate();
  
  RBBuildStack (ringparams->streamidx, streams);
  
  /* Loop through streams copying content to new Stack */
  while ( (tnode = (RBNode *) StackPop(streams)) )
    {
      stream = (RingStream *) tnode->data;
      
      /* If a RingReader is specified apply the match & reject expressions */
      if ( reader )
	{
	  /* Test limit expression if available */
	  if ( reader->limit )
	    if ( pcre_exec(reader->limit, reader->limit_extra, stream->streamid,
			   strlen(stream->streamid), 0, 0, NULL, 0) )
	      continue;
	  
	  /* Test match expression if available */
	  if ( reader->match )
	    if ( pcre_exec(reader->match, reader->match_extra, stream->streamid,
			   strlen(stream->streamid), 0, 0, NULL, 0) )
	      continue;
	  
	  /* Test reject expression if available */
	  if ( reader->reject )
	    if ( ! pcre_exec(reader->reject, reader->reject_extra, stream->streamid,
			     strlen(stream->streamid), 0, 0, NULL, 0) )
	      continue;
	}
      
      /* Allocate memory for new stream entry */
      if ( ! (newstream = (RingStream *) malloc (sizeof(RingStream))) )
	{
	  lprintf (0, "GetStreamsStack(): Error allocating memory");
	  return 0;
	}
      
      /* Copy stream entry and add to new streams Stack */
      memcpy (newstream, stream, sizeof(RingStream));
      
      /* Use unshift operation so copied Stack is in the same order */
      StackUnshift (newstreams, newstream);
    }
  
  /* Cleanup streams Stack structures but not data */
  StackDestroy (streams, 0);
  
  /* Unlock the streams index */
  pthread_mutex_unlock (ringparams->streamlock);
  
  /* Sort Stack on the stream IDs if more than one entry */
  if ( newstreams->top && newstreams->top != newstreams->tail )
    {
      if ( StackSort (newstreams, StreamStackNodeCmp) < 0 )
	{
	  lprintf (0, "GetStreamsStack(): Error sorting Stack");
	  return 0;
	}
    }
  
  return newstreams;
}  /* End of GetStreamsStack() */


/***************************************************************************
 * GetPacket:
 *
 * Determine the offset in the ring buffer to a specified packet ID
 * and return a pointer to appropriate the RingPacket.  If the pkttime
 * is not NULL it will be set to the packet time of the packet, useful
 * for later packet validation.
 *
 * Return a pointer to a RingPacket if found or 0 if no match.
 ***************************************************************************/
static inline RingPacket*
GetPacket (RingParams *ringparams, int64_t pktid, hptime_t *pkttime)
{
  int64_t offset;
  int64_t latestoffset;
  int64_t earliestid;
  int64_t latestid;
  uint64_t upktid, ulatestid;
  
  RingPacket *packet = 0;
  
  if ( ! ringparams )
    return 0;
  
  /* Sanity check requested packet ID */
  if ( pktid <= 0 || pktid > ringparams->maxpktid )
    return 0;
  
  latestoffset = ringparams->latestoffset;
  earliestid = ringparams->earliestid;
  
  /* Sanity check that ring packets exist */
  if ( latestoffset < 0 || earliestid <= 0 )
    {
      return 0;
    }
  
  /* Determine latest ID directly from packet to avoid race */
  latestid = ((RingPacket*) (ringparams->data + latestoffset))->pktid;
  
  /* Determine "unwrapped" latest and requested packet IDs */
  ulatestid = ( latestid < earliestid ) ? ringparams->maxpktid + latestid : latestid;
  upktid = ( pktid < earliestid ) ? ringparams->maxpktid + pktid : pktid;
  
  /* If packet ID is not inside the current ID space it's not available */
  if ( upktid < earliestid || upktid > ulatestid )
    return 0;
  
  /* Determine offset of requested ID */
  offset = latestoffset - ((ulatestid - upktid) * ringparams->pktsize);
  
  /* "Unwrap" offset if needed */
  if ( offset < 0 )
    offset += ringparams->maxoffset + ringparams->pktsize;
  
  /* Sanity check the offset value */
  if ( offset < 0 || offset > ringparams->maxoffset )
    {
      lprintf (0, "GetPacket() determined an impossible offset: %lld for pktid: %lld",
	       offset, pktid);
      lprintf (0, "            upktid: %lld, ulatestid: %lld, latestoffset: %lld",
	       upktid, ulatestid, latestoffset);
      return 0;
    }
  
  /* Set pointer to RingPacket in ring packet buffer */
  packet = (RingPacket *) (ringparams->data + offset);
  
  /* Set packet time if requested */
  if ( pkttime )
    *pkttime = packet->pkttime;
  
  /* Sanity check that this is the correct packet */
  if ( packet->pktid != pktid || packet->offset != offset )
    return 0;
  
  return packet;
}  /* End of GetPacket() */


/***************************************************************************
 * AddStreamIdx:
 *
 * Add a RingStream to the specified stream index, no checking is done
 * to determine if this entry already exists.  Return a pointer to the
 * newly generated Key if **ppkey is supplied.
 *
 * Return a pointer to the added RingStream on success and 0 on error.
 ***************************************************************************/
static RingStream*
AddStreamIdx (RBTree *streamidx, RingStream *stream, Key **ppkey)
{
  Key *newkey;
  RingStream *newdata;
  
  if ( ! streamidx || ! stream )
    return 0;
  
  /* Create new tree key */
  newkey = (Key *) malloc (sizeof(Key));

  /* Create new stream node */
  newdata = (RingStream *) malloc (sizeof(RingStream));
  
  if ( ! newkey || ! newdata )
    return 0;
  
  /* Populate the new key and data node */
  *newkey = FVNhash64 (stream->streamid);
  memcpy (newdata, stream, sizeof(RingStream));
  
  /* Add to the stream index */
  RBTreeInsert (streamidx, newkey, newdata, 0);
  
  /* Set pointer to hash key if requested */
  if ( ppkey )
    *ppkey = newkey;
  
  return newdata;
}  /* End of AddStreamIdx() */


/***************************************************************************
 * GetStreamIdx:
 *
 * Search the specified stream index for a given RingStream.
 *
 * Return a pointer to a RingStream if found or 0 if no match.
 ***************************************************************************/
static RingStream*
GetStreamIdx (RBTree *streamidx, char *streamid)
{
  Key key;
  RingStream *stream = 0;
  RBNode *tnode;
  
  if ( ! streamidx || ! streamid )
    return 0;
  
  /* Generate key from streamid */
  key = FVNhash64 (streamid);
  
  /* Search for a matching key */
  if ( (tnode = RBFind (streamidx, &key)) )
    {
      stream = (RingStream *)tnode->data;
    }
  
  return stream;
}  /* End of GetStreamIdx() */


/***************************************************************************
 * DelStreamIdx:
 *
 * Remove the specified stream ID from the stream index.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
DelStreamIdx (RBTree *streamidx, char *streamid)
{
  Key key;
  RBNode *tnode;
  
  if ( ! streamidx || ! streamid )
    return -1;
  
  /* Generate key from streamid */
  key = FVNhash64 (streamid);
  
  /* Search for a matching key */
  if ( (tnode = RBFind (streamidx, &key)) )
    {
      RBDelete (streamidx, tnode);
    }
  
  return (tnode) ? 0 : -1;
}  /* End of DelStreamIdx() */

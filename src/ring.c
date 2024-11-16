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
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <libmseed.h>

#include "generic.h"
#include "logging.h"
#include "rbtree.h"
#include "ring.h"

/* Macros to determine next and previous packet offsets given an
 * reference offset, maximum offset, and packet size */
#define NEXTOFFSET(O, M, S) (((O) + (S) > (M)) ? 0 : (O) + (S))
#define PREVOFFSET(O, M, S) (((O) == 0) ? (M) : (O) - (S))

static int StreamStackNodeCmp (StackNode *a, StackNode *b);
static inline int64_t FindOffsetForID (RingParams *ringparams, uint64_t pktid, nstime_t *pkttime);
static RingStream *AddStreamIdx (RBTree *streamidx, RingStream *stream, Key **ppkey);
static RingStream *GetStreamIdx (RBTree *streamidx, char *streamid);
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
 * Return >0 on buffer version mismatch, the version number is returned
 * Return  0 on success
 * Return -1 on corruption errors
 * Return -2 on non-recoverable errors
 ***************************************************************************/
int
RingInitialize (char *ringfilename, char *streamfilename, uint64_t ringsize,
                uint32_t pktsize, uint8_t mmapflag, uint8_t volatileflag,
                int *ringfd, RingParams **ringparams)
{
  static pthread_mutex_t writelock;
  static pthread_mutex_t streamlock;

  struct stat ringfilestat;
  struct stat streamfilestat;
  int streamidxfd;
  RingStream stream;

  long pagesize;
  uint32_t headersize;
  uint64_t maxpackets;
  int64_t maxoffset;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  int corruptring = 0;
  int ringinit    = 0;
  int rc;
  ssize_t rv;
  RingPacket *packetptr;
  RingStream *streamptr;

  /* Sanity check input parameters */
  if (!volatileflag && (!ringfilename || !streamfilename))
  {
    lprintf (0, "%s(): ring file and stream file must be specified", __func__);
    return -2;
  }

  /* A volatile ring will never be memory mapped */
  if (volatileflag)
  {
    mmapflag = 0;
  }

  /* Determine system page size */
  if ((pagesize = sysconf (_SC_PAGESIZE)) < 0)
  {
    lprintf (0, "%s(): Error determining system page size: %s",
             __func__, strerror (errno));
    return -2;
  }

  /* Determine the number of pages needed for the header */
  headersize = pagesize;
  while (headersize < sizeof (RingParams))
    headersize += pagesize;

  /* Sanity check that the ring can hold at least two packets */
  if (ringsize < (headersize + 2 * pktsize))
  {
    lprintf (0, "%s(): ring size (%" PRIu64 ") must be enough for 2 packets (%u each) and header (%d)",
             __func__, ringsize, pktsize, headersize);
    return -2;
  }

  /* Determine the maximum number of packets that fit after the first page */
  maxpackets = (uint64_t)((ringsize - headersize) / pktsize);

  /* Determine the maximum packet offset value */
  maxoffset = (int64_t)((maxpackets - 1) * pktsize);

  /* Open ring packet buffer file if non-volatile */
  if (!volatileflag)
  {
    /* Open ring packet buffer file, creating if necessary */
    if ((*ringfd = open (ringfilename, O_RDWR | O_CREAT, mode)) < 0)
    {
      lprintf (0, "%s(): error opening %s: %s", __func__, ringfilename, strerror (errno));
      return -1;
    }

    /* Stat the ring packet buffer file */
    if (fstat (*ringfd, &ringfilestat))
    {
      lprintf (0, "%s(): error stating %s: %s", __func__, ringfilename, strerror (errno));
      return -1;
    }

    /* If the file is new or unexpected size initialize to maximum ring file size */
    if (ringfilestat.st_size != ringsize)
    {
      ringinit = 1;

      if (ringfilestat.st_size <= 0)
        lprintf (1, "Creating new ring packet buffer file");
      else
        lprintf (1, "Re-creating ring packet buffer file");

      /* Truncate file if larger than ringsize */
      if (ringfilestat.st_size > ringsize)
      {
        if (ftruncate (*ringfd, (off_t)ringsize) == -1)
        {
          lprintf (0, "%s(): error truncating %s: %s", __func__, ringfilename, strerror (errno));
          return -1;
        }
      }

      /* Go to the last byte of the desired size */
      if (lseek (*ringfd, (off_t)ringsize - 1, SEEK_SET) == -1)
      {
        lprintf (0, "%s(): error seeking in %s: %s", __func__, ringfilename, strerror (errno));
        return -1;
      }

      /* Write a dummy byte at the end of the ring packet buffer */
      if (write (*ringfd, "", 1) != 1)
      {
        lprintf (0, "%s(): error writing to %s: %s", __func__, ringfilename, strerror (errno));
        return -1;
      }
    }
    else
    {
      lprintf (1, "Recovering existing ring packet buffer file");
    }
  }

  /* Use memory-mapping to access file */
  if (mmapflag && !volatileflag)
  {
    lprintf (1, "Memory-mapping ring packet buffer file");

    /* Memory map the ring packet buffer file */
    if ((*ringparams = (RingParams *)mmap (NULL, ringsize, PROT_READ | PROT_WRITE,
                                           MAP_SHARED, *ringfd, 0)) == (void *)-1)
    {
      lprintf (0, "%s(): error mmaping %s: %s", __func__, ringfilename, strerror (errno));
      return -1;
    }
  }
  /* Read ring packet buffer into memory if not memory-mapping. */
  else
  {
    lprintf (2, "Allocating ring packet buffer memory");

    /* Allocate ring packet buffer */
    if (!(*ringparams = malloc (ringsize)))
    {
      lprintf (0, "%s(): error allocating %" PRIu64 " bytes for ring packet buffer",
               __func__, ringsize);
      return -2;
    }

    /* Force ring initialization if volatile */
    if (volatileflag)
      ringinit = 1;

    /* Read ring packet buffer into memory if initialization is not needed */
    if (!ringinit)
    {
      lprintf (1, "Reading ring packet buffer file into memory");

      if (read (*ringfd, *ringparams, ringsize) != ringsize)
      {
        lprintf (0, "%s(): error reading ring packet buffer into memory: %s",
                 __func__, strerror (errno));
        return -1;
      }
    }
  }

  /* Check ring corruption flag, if set the ring was earlier determined to be corrupt */
  if ((*ringparams)->corruptflag && !volatileflag)
  {
    lprintf (0, "** Packet buffer is marked as corrupt");
    return -1;
  }

  /* Check ring flux flag, if set the ring should be considered corrupt */
  if ((*ringparams)->fluxflag && !volatileflag)
  {
    lprintf (0, "** Packet buffer is marked as busy, probably corrupted");
    return -1;
  }

  /* If signature match but version mismatch return current buffer version */
  if (!ringinit &&
      memcmp ((*ringparams)->signature, RING_SIGNATURE, sizeof ((*ringparams)->signature)) == 0 &&
      (*ringparams)->version != RING_VERSION)
  {
    lprintf (0, "Packet buffer version %d detected", (*ringparams)->version);
    return (*ringparams)->version;
  }

  /* Initialize locks */
  if ((rc = pthread_mutex_init (&writelock, NULL)))
  {
    lprintf (0, "%s(): error initializing ring write lock: %s", __func__, strerror (rc));
    return -2;
  }
  if ((rc = pthread_mutex_init (&streamlock, NULL)))
  {
    lprintf (0, "%s(): error initializing stream lock: %s", __func__, strerror (rc));
    return -2;
  }

  /* Initialize volatile ring packet buffer parameters */
  (*ringparams)->writelock    = &writelock;
  (*ringparams)->streamlock   = &streamlock;
  (*ringparams)->mmapflag     = mmapflag;
  (*ringparams)->volatileflag = volatileflag;
  (*ringparams)->streamidx    = RBTreeCreate (KeyCompare, free, free);
  (*ringparams)->streamcount  = 0;
  (*ringparams)->ringstart    = NSnow ();
  (*ringparams)->data         = ((uint8_t *)(*ringparams)) + headersize;

  /* Validate existing ring packet buffer parameters, resetting if needed */
  if (ringinit ||
      memcmp ((*ringparams)->signature, RING_SIGNATURE, sizeof ((*ringparams)->signature)) ||
      (*ringparams)->version != RING_VERSION ||
      (*ringparams)->ringsize != ringsize ||
      (*ringparams)->pktsize != pktsize ||
      (*ringparams)->maxpackets != maxpackets ||
      (*ringparams)->maxoffset != maxoffset ||
      (*ringparams)->headersize != headersize)
  {
    /* Report what triggered the parameter reset if not just initialized */
    if (!ringinit)
    {
      if (memcmp ((*ringparams)->signature, RING_SIGNATURE, sizeof ((*ringparams)->signature)))
        lprintf (0, "** Packet buffer signature mismatch: %.4s <-> %.4s", (*ringparams)->signature, RING_SIGNATURE);
      if ((*ringparams)->version != RING_VERSION)
        lprintf (0, "** Packet buffer version change: %u -> %u", (*ringparams)->version, RING_VERSION);
      if ((*ringparams)->ringsize != ringsize)
        lprintf (0, "** Packet buffer size change: %" PRIu64 " -> %" PRIu64, (*ringparams)->ringsize, ringsize);
      if ((*ringparams)->pktsize != pktsize)
        lprintf (0, "** Packet size change: %u -> %u", (*ringparams)->pktsize, pktsize);
      if ((*ringparams)->maxpackets != maxpackets)
        lprintf (0, "** Maximum packets change: %" PRIu64 " -> %" PRIu64, (*ringparams)->maxpackets, maxpackets);
      if ((*ringparams)->maxoffset != maxoffset)
        lprintf (0, "** Maximum offset change: %" PRIu64 " -> %" PRIu64, (*ringparams)->maxoffset, maxoffset);
      if ((*ringparams)->headersize != headersize)
        lprintf (0, "** Header size change: %u -> %u", (*ringparams)->headersize, headersize);
    }

    lprintf (0, "Resetting ring packet buffer parameters");

    memcpy ((*ringparams)->signature, RING_SIGNATURE, sizeof ((*ringparams)->signature));
    (*ringparams)->version        = RING_VERSION;
    (*ringparams)->ringsize       = ringsize;
    (*ringparams)->pktsize        = pktsize;
    (*ringparams)->maxpackets     = maxpackets;
    (*ringparams)->maxoffset      = maxoffset;
    (*ringparams)->headersize     = headersize;
    (*ringparams)->corruptflag    = 0;
    (*ringparams)->fluxflag       = 0;
    (*ringparams)->earliestid     = RINGID_NONE;
    (*ringparams)->earliestptime  = NSTUNSET;
    (*ringparams)->earliestdstime = NSTUNSET;
    (*ringparams)->earliestdetime = NSTUNSET;
    (*ringparams)->earliestoffset = -1;
    (*ringparams)->latestid       = RINGID_NONE;
    (*ringparams)->latestptime    = NSTUNSET;
    (*ringparams)->latestdstime   = NSTUNSET;
    (*ringparams)->latestdetime   = NSTUNSET;
    (*ringparams)->latestoffset   = -1;
    (*ringparams)->txpacketrate   = 0.0;
    (*ringparams)->txbyterate     = 0.0;
    (*ringparams)->rxpacketrate   = 0.0;
    (*ringparams)->rxbyterate     = 0.0;

    /* Clear unused header space */
    memset (((char *)(*ringparams)) + sizeof (RingParams), 0, headersize - sizeof (RingParams));
  }
  /* If the ring has not been reset and packets are present recover stream index */
  else if ((*ringparams)->earliestoffset >= 0)
  {
    lprintf (1, "Recovering stream index");

    /* Open stream index file */
    if ((streamidxfd = open (streamfilename, O_RDONLY, 0)) < 0)
    {
      lprintf (0, "%s(): error opening %s: %s", __func__, streamfilename, strerror (errno));
      return -1;
    }

    /* Stat the streams file */
    if (fstat (streamidxfd, &streamfilestat))
    {
      lprintf (0, "%s(): error stating %s: %s", __func__, streamfilename, strerror (errno));
      return -1;
    }

    if (streamfilestat.st_size > 0)
    {
      /* Read the saved RingStreams */
      while ((rv = read (streamidxfd, &stream, sizeof (RingStream)) == sizeof (RingStream)))
      {
        /* Re-populating streams index */
        if (!AddStreamIdx ((*ringparams)->streamidx, &stream, 0))
        {
          lprintf (0, "%s(): error adding stream to index", __func__);
          corruptring = 1;
        }
        else
        {
          (*ringparams)->streamcount++;
        }
      }

      /* Test for read error */
      if (rv < 0)
      {
        lprintf (0, "%s(): error reading %s: %s", __func__, streamfilename, strerror (errno));
        return -1;
      }
    }
    else
    {
      lprintf (0, "%s(): stream index file empty!", __func__);
      return -1;
    }

    /* Close the stream index file and release file name memory */
    close (streamidxfd);
  }

  if ((*ringparams)->earliestoffset > (*ringparams)->maxoffset)
  {
    lprintf (0, "%s(): error earliest offset > maxoffset, ring corrupted", __func__);
    corruptring = 1;
  }
  if ((*ringparams)->latestoffset > (*ringparams)->maxoffset)
  {
    lprintf (0, "%s(): error latest offset > maxoffset, ring corrupted", __func__);
    corruptring = 1;
  }

  /* Sanity checks: compare earliest and latest packet offsets between RingParams and lookups
   * and check the earliest and latest stream entries. */
  if (!corruptring && (*ringparams)->earliestoffset >= 0)
  {
    packetptr = (RingPacket *)((*ringparams)->data + (*ringparams)->earliestoffset);

    if (packetptr->offset != (*ringparams)->earliestoffset)
    {
      lprintf (0, "%s(): error comparing earliest packet offsets, ring corrupted", __func__);
      corruptring = 1;
    }
    else if (!(streamptr = GetStreamIdx ((*ringparams)->streamidx, packetptr->streamid)))
    {
      lprintf (0, "%s(): error finding stream entry for earliest packet, ring corrupted", __func__);
      corruptring = 1;
    }
  }
  if (!corruptring && (*ringparams)->latestoffset >= 0)
  {
    packetptr = (RingPacket *)((*ringparams)->data + (*ringparams)->latestoffset);

    if (packetptr->offset != (*ringparams)->latestoffset)
    {
      lprintf (0, "%s(): error comparing latest packet offsets, ring corrupted", __func__);
      corruptring = 1;
    }
    else if (!(streamptr = GetStreamIdx ((*ringparams)->streamidx, packetptr->streamid)))
    {
      lprintf (0, "%s(): error finding stream entry for latest packet, ring corrupted", __func__);
      corruptring = 1;
    }
  }

  /* If corruption was detected cleanup before returning */
  if (corruptring)
  {
    RBTreeDestroy ((*ringparams)->streamidx);

    /* Unmap the ring file */
    if (munmap ((void *)(*ringparams), ringsize))
    {
      lprintf (0, "%s(): error unmapping ring file: %s", __func__, strerror (errno));
      return -1;
    }

    /* Close the ring file and re-init the descriptor */
    if (close (*ringfd))
    {
      lprintf (0, "%s(): error closing ring file: %s", __func__, strerror (errno));
      return -1;
    }
    *ringfd = -1;

    return -1;
  }

  lprintf (0, "Ring initialized");

  return 0;
} /* End of RingInitialize() */

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

  if (!ringparams)
    return -1;

  if (!ringparams->volatileflag && (!ringfd || !streamfilename))
    return -1;

  /* Free memory and return if ring is volatile */
  if (ringparams->volatileflag)
  {
    RBTreeDestroy (ringparams->streamidx);
    free (ringparams);
    return 0;
  }

  /* Open stream index file */
  if ((streamidxfd = open (streamfilename, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0)
  {
    lprintf (0, "%s(): error opening %s: %s", __func__, streamfilename, strerror (errno));
    rv = -1;
  }

  /* Lock ring against writes, never give this up, destroyed later */
  pthread_mutex_lock (ringparams->writelock);

  /* Set ring flux flag */
  ringparams->fluxflag = 1;

  /* Create Stack of RingStreams */
  streams = StackCreate ();
  RBBuildStack (ringparams->streamidx, streams);

  /* Write RingStreams to stream index file */
  lprintf (1, "Writing stream index file");
  while ((tnode = (RBNode *)StackPop (streams)))
  {
    stream = (RingStream *)tnode->data;

    if (write (streamidxfd, stream, sizeof (RingStream)) != sizeof (RingStream))
    {
      lprintf (0, "%s(): error writing to %s: %s", __func__, streamfilename, strerror (errno));
      rv = -1;
    }
  }

  /* Close the streams file */
  if (close (streamidxfd))
  {
    lprintf (0, "%s(): error closing %s: %s", __func__, streamfilename, strerror (errno));
    rv = -1;
  }

  /* Cleanup stream index related memory */
  RBTreeDestroy (ringparams->streamidx);
  StackDestroy (streams, 0);
  ringparams->streamidx = NULL;

  /* Destroy streams index lock */
  if ((rc = pthread_mutex_destroy (ringparams->streamlock)))
  {
    lprintf (0, "%s(): error destroying stream lock: %s", __func__, strerror (rc));
    rv = -1;
  }
  ringparams->streamlock = NULL;

  if (ringparams->mmapflag)
  {
    /* Clear ring flux flag */
    ringparams->fluxflag = 0;

    /* Destroy ring write lock */
    pthread_mutex_unlock (ringparams->writelock);
    if ((rc = pthread_mutex_destroy (ringparams->writelock)))
    {
      lprintf (0, "%s(): error destroying ring write lock: %s", __func__, strerror (rc));
      rv = -1;
    }
    ringparams->writelock = NULL;

    /* Unmap the ring buffer file */
    lprintf (1, "Unmapping and closing ring buffer file");
    if (munmap ((void *)ringparams, ringparams->ringsize))
    {
      lprintf (0, "%s(): error unmapping ring buffer file: %s", __func__, strerror (errno));
      rv = -1;
    }
  }
  else
  {
    /* Write the ring buffer file */
    lprintf (1, "Writing and closing ring buffer file");

    if (lseek (ringfd, 0, SEEK_SET) == -1)
    {
      lprintf (0, "%s(): error seeking in ring buffer file: %s", __func__, strerror (errno));
      rv = -1;
    }

    /* Clear ring flux flag */
    ringparams->fluxflag = 0;

    /* Destroy ring write lock */
    pthread_mutex_unlock (ringparams->writelock);
    if ((rc = pthread_mutex_destroy (ringparams->writelock)))
    {
      lprintf (0, "%s(): error destroying ring write lock: %s", __func__, strerror (rc));
      rv = -1;
    }
    ringparams->writelock = NULL;

    if (write (ringfd, ringparams, ringparams->ringsize) != ringparams->ringsize)
    {
      lprintf (0, "%s(): error writing ring buffer file: %s", __func__, strerror (errno));
      rv = -1;
    }

    /* Free the ring buffer memory */
    free (ringparams);
  }

  /* Close the ring file */
  if (close (ringfd))
  {
    lprintf (0, "%s(): error closing ring buffer file: %s", __func__, strerror (errno));
    rv = -1;
  }

  return rv;
} /* End of RingShutdown() */

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
  RingStream newstream;
  RingPacket *earliest = NULL;
  RingPacket *latest   = NULL;
  RingPacket *prevlatest;
  RBNode *node = NULL;
  Key *skey;

  uint64_t pktid;
  int64_t offset;

  if (!ringparams || !packet || !packetdata)
    return -1;

  /* Check packet size */
  if ((sizeof (RingPacket) + datasize) > ringparams->pktsize)
  {
    lprintf (0, "%s(): %s packet size too large (%lu), maximum is %d bytes",
             __func__, packet->streamid, (sizeof (RingPacket) + datasize), ringparams->pktsize);
    return -1;
  }

  /* Lock ring and streams index */
  pthread_mutex_lock (ringparams->writelock);
  pthread_mutex_lock (ringparams->streamlock);

  /* Set ring flux flag */
  ringparams->fluxflag = 1;

  /* Set packet entries for earliest and latest packets in ring */
  if (ringparams->earliestoffset >= 0)
  {
    earliest = (RingPacket *)(ringparams->data + ringparams->earliestoffset);
  }
  if (ringparams->latestoffset >= 0)
  {
    latest = (RingPacket *)(ringparams->data + ringparams->latestoffset);
  }

  /* Determine next packet ID and offset */
  if (latest)
  {
    offset = NEXTOFFSET (latest->offset, ringparams->maxoffset, ringparams->pktsize);
    pktid  = latest->pktid + 1;

    /* In the unlikely event we reached the end of the universe start again with 1 */
    if (pktid > RINGID_MAXIMUM)
    {
      pktid = 1;
    }
  }
  /* Otherwise the buffer is empty, start from the beginning */
  else
  {
    pktid  = 1;
    offset = 0;
  }

  /* Update new packet details */
  packet->pktid        = (packet->pktid == RINGID_NONE) ? pktid : packet->pktid;
  packet->offset       = offset;
  packet->pkttime      = NSnow ();
  packet->nextinstream = -1;

  /* Remove earliest packet if ring is full (next == earliest) */
  if (earliest && latest && earliest != latest)
  {
    if (offset == ringparams->earliestoffset)
    {
      int64_t next_offset;                 /* New earliest packet offset */
      RingPacket *nextInRing       = NULL; /* New earliest packet in ring */
      RingPacket *nextInStream     = NULL; /* New earliest packet in stream */
      RingStream *streamOfEarliest = NULL; /* Stream of old earliest packet */

      next_offset = NEXTOFFSET (earliest->offset, ringparams->maxoffset, ringparams->pktsize);
      nextInRing  = (RingPacket *)(ringparams->data + next_offset);
      nextInStream = (RingPacket *)(ringparams->data + earliest->nextinstream);

      if (!(streamOfEarliest = GetStreamIdx (ringparams->streamidx, earliest->streamid)))
      {
        lprintf (0, "%s(): Error getting earliest packet stream", __func__);
        ringparams->corruptflag = 1;
        ringparams->fluxflag    = 0;
        pthread_mutex_unlock (ringparams->writelock);
        pthread_mutex_unlock (ringparams->streamlock);
        return -2;
      }

      lprintf (3, "Removing packet for stream %s (id: %" PRIu64 ", offset: %" PRId64 ")",
               earliest->streamid, earliest->pktid, earliest->offset);

      /* Update RingParams with new earliest entry */
      ringparams->earliestid     = nextInRing->pktid;
      ringparams->earliestptime  = nextInRing->pkttime;
      ringparams->earliestdstime = nextInRing->datastart;
      ringparams->earliestdetime = nextInRing->dataend;
      ringparams->earliestoffset = nextInRing->offset;

      /* Delete stream entry if this is the only packet */
      if (earliest->offset == streamOfEarliest->earliestoffset &&
          earliest->offset == streamOfEarliest->latestoffset)
      {
        lprintf (2, "Removing stream index entry for %s", earliest->streamid);
        DelStreamIdx (ringparams->streamidx, earliest->streamid);
        ringparams->streamcount--;
      }
      /* Else update stream entry for the next packet in the stream */
      else if (nextInStream)
      {
        streamOfEarliest->earliestdstime = nextInStream->datastart;
        streamOfEarliest->earliestdetime = nextInStream->dataend;
        streamOfEarliest->earliestptime  = nextInStream->pkttime;
        streamOfEarliest->earliestid     = nextInStream->pktid;
        streamOfEarliest->earliestoffset = nextInStream->offset;
      }
    }
  }

  /* Find RingStream entry, creating if not found */
  if (!(stream = GetStreamIdx (ringparams->streamidx, packet->streamid)))
  {
    /* Populate and add RingStream entry */
    memset (&newstream, 0, sizeof (RingStream));
    memcpy (newstream.streamid, packet->streamid, sizeof (newstream.streamid));
    newstream.earliestdstime = packet->datastart;
    newstream.earliestdetime = packet->dataend;
    newstream.earliestptime  = packet->pkttime;
    newstream.earliestid     = packet->pktid;
    newstream.earliestoffset = packet->offset;
    newstream.latestoffset   = -1;
    /* The "latest" fields are populated later */

    /* Add new stream to index */
    if (!(stream = AddStreamIdx (ringparams->streamidx, &newstream, &skey)))
    {
      lprintf (0, "%s(): Error adding new stream index", __func__);
      if (node)
      {
        free (node->key);
        free (node->data);
        free (node);
      }
      ringparams->corruptflag = 1;
      ringparams->fluxflag    = 0;
      pthread_mutex_unlock (ringparams->writelock);
      pthread_mutex_unlock (ringparams->streamlock);
      return -2;
    }
    else
    {
      ringparams->streamcount++;
    }

    lprintf (2, "Added stream entry for %s (key: %" PRIx64 ")", packet->streamid, *skey);
  }

  /* Copy packet header into ring */
  memcpy ((ringparams->data + offset), packet, sizeof (RingPacket));

  /* Copy packet data into ring directly after header */
  memcpy ((ringparams->data + offset + sizeof (RingPacket)), packetdata, datasize);

  /* Update RingParams with new latest packet */
  ringparams->latestid     = packet->pktid;
  ringparams->latestptime  = packet->pkttime;
  ringparams->latestdstime = packet->datastart;
  ringparams->latestdetime = packet->dataend;
  ringparams->latestoffset = packet->offset;

  /* Update RingParams with new earliest packet (for initial packet) */
  if (!earliest)
  {
    ringparams->earliestid     = packet->pktid;
    ringparams->earliestptime  = packet->pkttime;
    ringparams->earliestdstime = packet->datastart;
    ringparams->earliestdetime = packet->dataend;
    ringparams->earliestoffset = packet->offset;
  }

  /* Update entry for previous packet in stream */
  if (stream->latestoffset >= 0)
  {
    prevlatest = (RingPacket *)(ringparams->data + stream->latestoffset);

    prevlatest->nextinstream = packet->offset;
  }

  /* Update stream entry */
  stream->latestdstime = packet->datastart;
  stream->latestdetime = packet->dataend;
  stream->latestptime  = packet->pkttime;
  stream->latestid     = packet->pktid;
  stream->latestoffset = packet->offset;

  /* Clear ring flux flag */
  ringparams->fluxflag = 0;

  /* Unlock ring and stream index */
  pthread_mutex_unlock (ringparams->writelock);
  pthread_mutex_unlock (ringparams->streamlock);

  lprintf (3, "Added packet for stream %s, pktid: %" PRIu64 ", offset: %" PRIu64,
           packet->streamid, packet->pktid, packet->offset);

  return 0;
} /* End of RingWrite() */

/***************************************************************************
 * RingRead:
 *
 * Read a requested packet ID from the ring.
 *
 * For this routine an explicit packet ID is requested and returned if
 * found.  The packet stream ID matching and rejection patterns are not
 * relevant.
 *
 * The packet pointer must point to already allocated memory.  The packet
 * data will only be returned if the packetdata pointer is not 0 and
 * points to already allocated memory.
 *
 * Returns the packet ID on success, RINGID_NONE when the packet was not
 * found and RINGID_ERROR on error.
 ***************************************************************************/
uint64_t
RingRead (RingReader *reader, uint64_t reqid,
          RingPacket *packet, char *packetdata)
{
  RingParams *ringparams;
  RingPacket *pkt;
  nstime_t pkttime;
  uint64_t pktid = RINGID_NONE;
  int64_t offset = -1;

  if (!reader || !packet)
    return RINGID_ERROR;

  ringparams = reader->ringparams;
  if (!ringparams)
    return RINGID_ERROR;

  if (reqid > RINGID_MAXIMUM)
  {
    lprintf (0, "%s(): unsupported position value: %" PRIu64, __func__, reqid);
    return RINGID_ERROR;
  }
  else
  {
    pktid = reqid;
  }

  /* Find the offset to the packet if needed */
  if (offset < 0 && (offset = FindOffsetForID (ringparams, pktid, &pkttime)) < 0)
  {
    return RINGID_NONE;
  }

  pkt = (RingPacket *)(ringparams->data + offset);

  /* Copy packet header */
  memcpy (packet, pkt, sizeof (RingPacket));

  /* Copy packet data if a pointer is supplied */
  if (packetdata)
    memcpy (packetdata, (uint8_t *)pkt + sizeof (RingPacket), pkt->datasize);

  /* Sanity check that the data was not overwritten during the copy */
  if (pktid != pkt->pktid)
  {
    return RINGID_NONE;
  }

  /* Update reader position value */
  reader->pktoffset = packet->offset;
  reader->pktid     = packet->pktid;
  reader->pkttime   = packet->pkttime;
  reader->datastart = packet->datastart;
  reader->dataend   = packet->dataend;

  return pktid;
} /* End of RingRead() */

/***************************************************************************
 * RingReadNext:
 *
 * Determine and read the next packet from the ring.  The packet
 * pointer must point to already allocated memory.  The packet data (payload)
 * will be returned if the packetdata pointer is not NULL.
 *
 * If the packet being searched for does not exist and is not the next
 * expected packet that will enter the ring, assume that the read
 * position has fallen off the trailing edge of the ring and
 * reposition the search at the earliest packet.
 *
 * Returns packet ID on success, RINGID_NONE when no next packet
 * and RINGID_ERROR on error.
 ***************************************************************************/
uint64_t
RingReadNext (RingReader *reader, RingPacket *packet, char *packetdata)
{
  RingParams *ringparams;
  RingPacket *pkt;
  nstime_t pkttime;
  int64_t offset = -1;
  uint8_t skip;
  uint32_t skipped;

  int64_t earliestoffset;
  int64_t latestoffset;
  int64_t eoboffset;

  uint64_t latestid;
  nstime_t latestptime;
  nstime_t latestdstime;
  nstime_t latestdetime;

  if (!reader || !packet)
    return RINGID_ERROR;

  ringparams = reader->ringparams;
  if (!ringparams)
    return RINGID_ERROR;

  latestoffset   = ringparams->latestoffset;
  earliestoffset = ringparams->earliestoffset;

  /* If ring is empty return immediately */
  if (latestoffset < 0)
  {
    /* For readers already streaming data, position them to the eventual earliest */
    if (reader->pktoffset < 0 && reader->pktid == RINGID_NEXT)
    {
      reader->pktid = RINGID_EARLIEST;
    }

    return RINGID_NONE;
  }

  /* Determine latest packet details directly to avoid race */
  latestid     = ((RingPacket *)(ringparams->data + latestoffset))->pktid;
  latestptime  = ((RingPacket *)(ringparams->data + latestoffset))->pkttime;
  latestdstime = ((RingPacket *)(ringparams->data + latestoffset))->datastart;
  latestdetime = ((RingPacket *)(ringparams->data + latestoffset))->dataend;

  /* Determine offset for initial read or relative positions */
  if (reader->pktoffset < 0)
  {
    if (reader->pktid == RINGID_NEXT)
    {
      /* Position reader at the latest packet */
      reader->pktoffset = latestoffset;
      reader->pktid     = latestid;
      reader->pkttime   = latestptime;
      reader->datastart = latestdstime;
      reader->dataend   = latestdetime;

      /* There is no next packet so return */
      return RINGID_NONE;
    }
    else if (reader->pktid == RINGID_LATEST)
    {
      offset = latestoffset;
    }
    else if (reader->pktid == RINGID_EARLIEST)
    {
      offset = earliestoffset;
    }
    else
    {
      lprintf (0, "%s(): unsupported packet ID value: %" PRIu64, __func__, reader->pktid);
      return RINGID_ERROR;
    }
  }
  /* Otherwise determine the next packet offset */
  else
  {
    offset = NEXTOFFSET (reader->pktoffset, ringparams->maxoffset, ringparams->pktsize);
  }

  /* Determine the end-of-buffer offset as the one following the latest offset */
  eoboffset = NEXTOFFSET (latestoffset, ringparams->maxoffset, ringparams->pktsize);

  /* Loop until we have a matching packet or reached the end of the buffer */
  skip    = 1;
  skipped = 0;
  while (skip && offset != eoboffset)
  {
    skip = 0;

    pkt = (RingPacket *)(ringparams->data + offset);
    pkttime = pkt->pkttime;

    /* Determine if this is a valid packet by checking that the packet time has
     * not advanced past the lastest time */
    if (pkttime > latestptime)
    {
      /* If the packet has been replaced, assume the reader has been lapped (fallen off
       * the trailing edge of the buffer) and reposition to the earliest packet */

      offset = ringparams->earliestoffset;
      skipped++;

      /* Safety value to avoid skipping off the trailing edge of the buffer forever */
      if (skipped >= 100)
      {
        lprintf (0, "%s(): skipped off trailing edge of buffer %d times", __func__, skipped);
        return RINGID_NONE;
      }

      skip = 1;
      continue;
    }

    skipped = 0;

    /* Update reader position */
    reader->pktoffset = offset;
    reader->pktid     = pkt->pktid;
    reader->pkttime   = pkt->pkttime;
    reader->datastart = pkt->datastart;
    reader->dataend   = pkt->dataend;

    /* Test limit expression if available */
    if (reader->limit)
      if (pcre2_match (reader->limit, (PCRE2_SPTR8)pkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->limit_data, NULL) < 0)
        skip = 1;

    /* Test match expression if available and not already skipping */
    if (reader->match && skip == 0)
      if (pcre2_match (reader->match, (PCRE2_SPTR8)pkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->match_data, NULL) < 0)
        skip = 1;

    /* Test reject expression if available and not already skipping */
    if (reader->reject && skip == 0)
      if (pcre2_match (reader->reject, (PCRE2_SPTR8)pkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->reject_data, NULL) >= 0)
        skip = 1;

    /* If skipping this packet determine the next packet in the ring */
    if (skip)
    {
      offset = NEXTOFFSET (offset, ringparams->maxoffset, ringparams->pktsize);
    }
  }

  if (offset == eoboffset)
  {
    return RINGID_NONE;
  }

  /* Copy packet header */
  memcpy (packet, pkt, sizeof (RingPacket));

  /* Copy packet data if a pointer is supplied */
  if (packetdata)
    memcpy (packetdata, (uint8_t *)pkt + sizeof (RingPacket), pkt->datasize);

  /* Sanity check that the data was not overwritten during processing */
  if (pkttime != pkt->pkttime)
  {
    return RINGID_NONE;
  }

  return packet->pktid;
} /* End of RingReadNext() */

/***************************************************************************
 * RingPosition:
 *
 * Set the ring reading position to the specified packet ID, checking
 * that the ID is a valid packet in the ring.  If the pkttime value is
 * not NSTUNSET it will also be checked and should match the requested
 * packet ID.  The current read position is not changed if any errors
 * occur.
 *
 * If the packet is successfully found the RingReader.pktid will be
 * updated.
 *
 * Returns packet ID on success, RINGID_NONE when the packet was not
 * found and RINGID_ERROR on error.
 ***************************************************************************/
uint64_t
RingPosition (RingReader *reader, uint64_t pktid, nstime_t pkttime)
{
  RingParams *ringparams;
  RingPacket *pkt;
  nstime_t ptime;
  nstime_t datastart, dataend;
  int64_t offset;

  if (!reader)
    return RINGID_ERROR;

  ringparams = reader->ringparams;
  if (!ringparams)
    return RINGID_ERROR;

  /* Determine packet ID for relative positions */
  if (pktid == RINGID_EARLIEST)
  {
    pktid = ringparams->earliestid;
  }
  else if (pktid == RINGID_LATEST)
  {
    pktid = ringparams->latestid;
  }

  if (pktid > RINGID_MAXIMUM)
  {
    lprintf (0, "%s(): unsupported position value: %" PRIu64, __func__, pktid);
    return RINGID_ERROR;
  }

  /* Find the offset to the packet */
  if ((offset = FindOffsetForID (ringparams, pktid, &ptime)) < 0)
  {
    return RINGID_NONE;
  }

  pkt = (RingPacket *)(ringparams->data + offset);

  /* Check for matching pkttime if not NSTUNSET or NSTERROR */
  if (pkttime != NSTUNSET && pkttime != NSTERROR)
  {
    if (pkttime != ptime)
    {
      return RINGID_NONE;
    }
  }
  datastart = pkt->datastart;
  dataend   = pkt->dataend;

  /* Sanity check that the data was not overwritten during the copy */
  if (pktid != pkt->pktid)
  {
    return RINGID_NONE;
  }

  /* Update reader position value */
  reader->pktoffset = offset;
  reader->pktid     = pktid;
  reader->pkttime   = ptime;
  reader->datastart = datastart;
  reader->dataend   = dataend;

  return pktid;
} /* End of RingPosition() */

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
 * Returns packet ID on success, RINGID_NONE when the packet was not
 * found and RINGID_ERROR on error.
 ***************************************************************************/
uint64_t
RingAfter (RingReader *reader, nstime_t reftime, int whence)
{
  RingParams *ringparams;
  RingPacket *pkt0 = NULL;
  RingPacket *pkt1 = NULL;
  uint64_t pktid;
  nstime_t pkttime;
  nstime_t datastart;
  nstime_t dataend;
  int64_t offset;
  uint64_t skipped = 0;
  uint8_t skip;

  if (!reader)
    return RINGID_ERROR;

  ringparams = reader->ringparams;
  if (!ringparams)
    return RINGID_ERROR;

  /* Start searching with the earliest packet in the ring */
  offset = ringparams->earliestoffset;

  /* Loop through packets in forward order */
  while (skipped < ringparams->maxpackets)
  {
    skip = 0;

    /* Get pointer to RingPacket */
    pkt1 = (RingPacket *)(ringparams->data + offset);

    /* Test if packet is earlier than reference time, this will avoid the
     * regex tests for packets that we will eventually skip anyway */
    if (pkt1->dataend < reftime)
      skip = 1;

    /* Test limit expression if available */
    if (reader->limit && !skip)
      if (pcre2_match (reader->limit, (PCRE2_SPTR8)pkt1->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->limit_data, NULL) < 0)
        skip = 1;

    /* Test match expression if available and not already skipping */
    if (reader->match && !skip)
      if (pcre2_match (reader->match, (PCRE2_SPTR8)pkt1->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->match_data, NULL) < 0)
        skip = 1;

    /* Test reject expression if available and not already skipping */
    if (reader->reject && !skip)
      if (pcre2_match (reader->reject, (PCRE2_SPTR8)pkt1->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->reject_data, NULL) >= 0)
        skip = 1;

    /* Done if this matching packet has a data end time after that specified */
    if (!skip && pkt1->dataend > reftime)
    {
      break;
    }

    /* Shift skipped packet to the history value */
    pkt0 = pkt1;

    /* Done if we reach the latest packet */
    if (offset == ringparams->latestoffset)
    {
      break;
    }

    offset = NEXTOFFSET (offset, ringparams->maxoffset, ringparams->pktsize);
    skipped++;
  }

  /* Safety valve, if no packets were ever seen */
  if (!pkt1)
  {
    return RINGID_NONE;
  }

  /* Position to packet before match if requested and not the first packet */
  if (whence == 0 && skipped > 0)
  {
    pkt1 = pkt0;
  }

  offset    = pkt1->offset;
  pktid     = pkt1->pktid;
  pkttime   = pkt1->pkttime;
  datastart = pkt1->datastart;
  dataend   = pkt1->dataend;

  /* Sanity check that the data was not overwritten during the copy */
  if (pktid != pkt1->pktid)
  {
    return RINGID_NONE;
  }

  /* Update reader position value */
  reader->pktoffset = offset;
  reader->pktid     = pktid;
  reader->pkttime   = pkttime;
  reader->datastart = datastart;
  reader->dataend   = dataend;

  return pktid;
} /* End of RingAfter() */

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
 * Returns packet ID on success, RINGID_NONE when the packet was not
 * found and -1 on error.
 ***************************************************************************/
uint64_t
RingAfterRev (RingReader *reader, nstime_t reftime, uint64_t pktlimit,
              int whence)
{
  RingParams *ringparams;
  RingPacket *pkt   = NULL;
  RingPacket *spkt  = NULL;
  nstime_t pkttime  = NSTUNSET;
  nstime_t datastart;
  nstime_t dataend;
  uint64_t pktid;
  int64_t offset;
  int64_t soffset;
  uint64_t count = 0;
  uint8_t skip;

  if (!reader)
    return RINGID_ERROR;

  ringparams = reader->ringparams;
  if (!ringparams)
    return RINGID_ERROR;

  /* Start searching with the latest packet in the ring */
  offset  = ringparams->latestoffset;
  soffset = offset;

  /* Loop through packets in reverse order */
  while (count < pktlimit)
  {
    skip = 0;

    /* Get pointer to RingPacket */
    spkt = (RingPacket *)(ringparams->data + soffset);

    /* Test limit expression if available */
    if (reader->limit)
      if (pcre2_match (reader->limit, (PCRE2_SPTR8)spkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->limit_data, NULL) < 0)
        skip = 1;

    /* Test match expression if available and not already skipping */
    if (reader->match && !skip)
      if (pcre2_match (reader->match, (PCRE2_SPTR8)spkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->match_data, NULL) < 0)
        skip = 1;

    /* Test reject expression if available and not already skipping */
    if (reader->reject && !skip)
      if (pcre2_match (reader->reject, (PCRE2_SPTR8)spkt->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                       reader->reject_data, NULL) >= 0)
        skip = 1;

    if (!skip)
    {
      /* Set ID and time if this matching packet has a data end time after that specified */
      if (spkt->dataend > reftime)
      {
        offset  = soffset;
        pktid   = spkt->pktid;
        pkttime = spkt->pkttime;
        pkt     = spkt;
      }

      /* Done if we reach a matching packet with earlier start time */
      if (spkt->datastart < reftime)
      {
        break;
      }
    }

    /* Done if we reach the earliest packet */
    if (soffset == ringparams->earliestoffset)
    {
      break;
    }

    soffset = PREVOFFSET (soffset, ringparams->maxoffset, ringparams->pktsize);
    count++;
  }

  /* Safety valve, if no packets were ever seen */
  if (!pkt)
  {
    return RINGID_NONE;
  }

  /* Position to packet before match if requested */
  if (whence == 0)
  {
    /* Search for the previous packet */
    soffset = PREVOFFSET (soffset, ringparams->maxoffset, ringparams->pktsize);

    spkt = (RingPacket *)(ringparams->data + soffset);

    offset  = spkt->offset;
    pktid   = spkt->pktid;
    pkttime = spkt->pkttime;
    pkt     = spkt;
  }

  datastart = pkt->datastart;
  dataend   = pkt->dataend;

  /* Sanity check that the data was not overwritten during the copy */
  if (pktid != pkt->pktid)
  {
    return RINGID_NONE;
  }

  /* Update reader position value */
  reader->pktoffset = offset;
  reader->pktid     = pktid;
  reader->pkttime   = pkttime;
  reader->datastart = datastart;
  reader->dataend   = dataend;

  return pktid;
} /* End of RingAfterRev() */

/***************************************************************************
 * LogRingParameters:
 *
 * Log high-level ring buffer parameters.
 ***************************************************************************/
void LogRingParameters (RingParams *ringparams)
{
  char timestr[50];
  char pktidstr[50];
  char sizestr[50];

  if (!ringparams)
    return;

  HumanSizeString (ringparams->ringsize, sizestr, sizeof (sizestr));

  ms_nstime2timestr (ringparams->ringstart, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
  lprintf (1, "Ring parameters, version: %u, start: %s", ringparams->version, timestr);
  lprintf (1, "   ringsize: %" PRIu64 " (%s), pktsize: %u (%zu payload)",
           ringparams->ringsize, sizestr,
           ringparams->pktsize,
           ringparams->pktsize - sizeof (RingPacket));

  lprintf (2, "   headersize: %u", ringparams->headersize);

  lprintf (2, "   maxpackets: %" PRId64 ", maxoffset: %" PRIu64,
           ringparams->maxpackets, ringparams->maxoffset);

  lprintf (2, "   streamcount: %u", ringparams->streamcount);

  lprintf (2, "   volatile: %s, mmap: %s, corrupt: %s, flux: %s",
           (ringparams->volatileflag) ? "yes" : "no",
           (ringparams->mmapflag) ? "yes" : "no",
           (ringparams->corruptflag) ? "yes" : "no",
           (ringparams->fluxflag) ? "yes" : "no");

  snprintf (pktidstr, sizeof (pktidstr), "%" PRIu64, ringparams->earliestid);
  lprintf (2, "   earliest packet ID: %s, offset: %" PRId64,
           (ringparams->earliestid == RINGID_NONE) ? "NONE" : pktidstr,
           ringparams->earliestoffset);
  ms_nstime2timestr (ringparams->earliestptime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
  lprintf (2, "   earliest packet creation time: %s",
           (ringparams->earliestptime == NSTUNSET) ? "NONE" : timestr);
  ms_nstime2timestr (ringparams->earliestdstime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
  lprintf (2, "   earliest packet data start time: %s",
           (ringparams->earliestdstime == NSTUNSET) ? "NONE" : timestr);

  snprintf (pktidstr, sizeof (pktidstr), "%" PRIu64, ringparams->latestid);
  lprintf (2, "   latest packet ID: %s, offset: %" PRId64,
           (ringparams->latestid == RINGID_NONE) ? "NONE" : pktidstr,
           ringparams->latestoffset);
  ms_nstime2timestr (ringparams->latestptime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
  lprintf (2, "   latest packet creation time: %s",
           (ringparams->latestptime == NSTUNSET) ? "NONE" : timestr);
  ms_nstime2timestr (ringparams->latestdstime, timestr, ISOMONTHDAY_Z, NANO_MICRO_NONE);
  lprintf (2, "   latest packet data start time: %s",
           (ringparams->latestdstime == NSTUNSET) ? "NONE" : timestr);

} /* End of LogRingParameters() */

/***************************************************************************
 * UpdatePattern:
 *
 * Compile the supplied regex pattern (and data) and assign to the
 * provided pointers.
 *
 * The description is used in error messages to describe the pattern.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
UpdatePattern (pcre2_code **code, pcre2_match_data **data,
               const char *pattern, const char *description)
{
  int errcode;
  PCRE2_SIZE erroffset;
  PCRE2_UCHAR buffer[256];

  if (!code || !data)
    return -1;

  /* Compile pattern and assign to reader */
  if (pattern)
  {
    /* Free existing compiled expression */
    if (*code)
      pcre2_code_free (*code);
    if (*data)
      pcre2_match_data_free (*data);

    /* Compile regex */
    *code = pcre2_compile ((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED,
                           PCRE2_COMPILE_OPTIONS, &errcode, &erroffset, NULL);

    if (*code == NULL)
    {
      pcre2_get_error_message (errcode, buffer, sizeof (buffer));
      lprintf (0, "%s(): Error compiling %s expression at %zu: %s",
               __func__, (description ? description : ""),
               erroffset, buffer);
      return -1;
    }

    *data = pcre2_match_data_create_from_pattern (*code, NULL);
  }
  /* If no pattern, clear any existing regex */
  else
  {
    if (*code)
      pcre2_code_free (*code);
    *code = NULL;

    if (*data)
      pcre2_match_data_free (*data);
    *data = NULL;
  }

  return 0;
} /* End of UpdatePattern() */

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
} /* End of StreamStackNodeCmp() */

/***************************************************************************
 * GetStreamsStack:
 *
 * Build a copy of the stream index as a Stack sorted on stream ID.
 * It is up to the caller to free the Stack, i.e. using
 * StackDestroy(stack, free).
 *
 * If ringreader is not NULL only the streamids that match the
 * reader's limit and match expressions and do not match the reader's
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

  if (!ringparams)
    return 0;

  /* Lock the streams index */
  pthread_mutex_lock (ringparams->streamlock);

  streams    = StackCreate ();
  newstreams = StackCreate ();

  RBBuildStack (ringparams->streamidx, streams);

  /* Loop through streams copying content to new Stack */
  while ((tnode = (RBNode *)StackPop (streams)))
  {
    stream = (RingStream *)tnode->data;

    /* If a RingReader is specified apply the match & reject expressions */
    if (reader)
    {
      /* Test limit expression if available */
      if (reader->limit)
        if (pcre2_match (reader->limit, (PCRE2_SPTR8)stream->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                         reader->limit_data, NULL) < 0)
          continue;

      /* Test match expression if available */
      if (reader->match)
        if (pcre2_match (reader->match, (PCRE2_SPTR8)stream->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                         reader->match_data, NULL) < 0)
          continue;

      /* Test reject expression if available */
      if (reader->reject)
        if (pcre2_match (reader->reject, (PCRE2_SPTR8)stream->streamid, PCRE2_ZERO_TERMINATED, 0, 0,
                         reader->reject_data, NULL) >= 0)
          continue;
    }

    /* Allocate memory for new stream entry */
    if (!(newstream = (RingStream *)malloc (sizeof (RingStream))))
    {
      lprintf (0, "%s(): Error allocating memory", __func__);
      return 0;
    }

    /* Copy stream entry and add to new streams Stack */
    memcpy (newstream, stream, sizeof (RingStream));

    /* Use unshift operation so copied Stack is in the same order */
    StackUnshift (newstreams, newstream);
  }

  /* Cleanup streams Stack structures but not data */
  StackDestroy (streams, 0);

  /* Unlock the streams index */
  pthread_mutex_unlock (ringparams->streamlock);

  /* Sort Stack on the stream IDs if more than one entry */
  if (newstreams->top && newstreams->top != newstreams->tail)
  {
    if (StackSort (newstreams, StreamStackNodeCmp) < 0)
    {
      lprintf (0, "%s(): Error sorting Stack", __func__);
      return 0;
    }
  }

  return newstreams;
} /* End of GetStreamsStack() */

/***************************************************************************
 * FindOffsetForID:
 *
 * Determine the offset in the ring buffer to a specified packet ID.
 *
 * Assumptions:
 *
 * - If the earliest ID >= largest ID, they represent a range between
 * which the pktid must fall and they increase from earliest to latest.
 *
 * - If the earliest ID < largest ID, the ID set has no useful ordering.
 *
 * If pkttime is not NULL, it will be set to the time of the packet if the
 * search is successful.
 *
 * Return the offset to the RingPacket on success or -1 if no match.
 ***************************************************************************/
static inline int64_t
FindOffsetForID (RingParams *ringparams, uint64_t pktid, nstime_t *pkttime)
{
  RingPacket *packet = NULL;
  int64_t latestoffset;
  int64_t earliestoffset;
  int64_t offset;

  if (!ringparams)
    return -1;

  latestoffset   = ringparams->latestoffset;
  earliestoffset = ringparams->earliestoffset;

  /* Ring is empty */
  if (earliestoffset < 0 || latestoffset < 0)
  {
    return -1;
  }

  /* Earliest ID is less than latest ID.
   * Assume they increment from earliest to latest.
   * Assume the pktid must exist within the range. */
  if (ringparams->earliestid <= ringparams->latestid)
  {
    /* Check that requested packet ID is within earliest - latest range */
    if (pktid < ringparams->earliestid || pktid > ringparams->latestid)
    {
      return -1;
    }

    int64_t latestoffset_unwrapped = (latestoffset < earliestoffset) ? latestoffset + ringparams->maxoffset : latestoffset;

    int64_t lowpkt = 0;
    int64_t highpkt = (latestoffset_unwrapped - earliestoffset) / ringparams->pktsize;
    int64_t midpkt;

    /* Binary search for a matching ID */
    while (lowpkt <= highpkt)
    {
      midpkt = lowpkt + (highpkt - lowpkt) / 2;

      int64_t offset_unwrapped = earliestoffset + (midpkt * ringparams->pktsize);

      offset = (offset_unwrapped > ringparams->maxoffset) ? offset_unwrapped - ringparams->maxoffset : offset_unwrapped;

      packet = (RingPacket *)(ringparams->data + offset);

      /* If packet ID is found return the offset */
      if (packet->pktid == pktid)
      {
        if (pkttime)
          *pkttime = packet->pkttime;

        return offset;
      }

      if (packet->pktid < pktid)
      {
        lowpkt = midpkt + 1;
      }
      else
      {
        highpkt = midpkt - 1;
      }
    }
  }
  /* Otherwise the ID set has either wrapped or is otherwise unordered */
  else
  {
    /* Brute force search backwards from the latest */
    offset = NEXTOFFSET (latestoffset, ringparams->maxoffset, ringparams->pktsize);
    do
    {
      offset = PREVOFFSET (offset, ringparams->maxoffset, ringparams->pktsize);

      packet = (RingPacket *)(ringparams->data + offset);

      if (packet->pktid == pktid)
      {
        if (pkttime)
          *pkttime = packet->pkttime;

        return offset;
      }
    } while (offset != ringparams->earliestoffset);
  }

  return -1;
} /* End of FindOffsetForID() */

/***************************************************************************
 * AddStreamIdx:
 *
 * Add a RingStream to the specified stream index, no checking is done
 * to determine if this entry already exists.  Return a pointer to the
 * newly generated Key if **ppkey is supplied.
 *
 * Return a pointer to the added RingStream on success and 0 on error.
 ***************************************************************************/
static RingStream *
AddStreamIdx (RBTree *streamidx, RingStream *stream, Key **ppkey)
{
  Key *newkey;
  RingStream *newdata;

  if (!streamidx || !stream)
    return 0;

  /* Create new tree key */
  newkey = (Key *)malloc (sizeof (Key));

  /* Create new stream node */
  newdata = (RingStream *)malloc (sizeof (RingStream));

  if (!newkey || !newdata)
    return 0;

  /* Populate the new data node and key */
  memcpy (newdata, stream, sizeof (RingStream));
  *newkey = FNVhash64 (newdata->streamid);

  /* Add to the stream index */
  RBTreeInsert (streamidx, newkey, newdata, 0);

  /* Set pointer to hash key if requested */
  if (ppkey)
    *ppkey = newkey;

  return newdata;
} /* End of AddStreamIdx() */

/***************************************************************************
 * GetStreamIdx:
 *
 * Search the specified stream index for a given RingStream.
 *
 * Return a pointer to a RingStream if found or 0 if no match.
 ***************************************************************************/
static RingStream *
GetStreamIdx (RBTree *streamidx, char *streamid)
{
  Key key;
  RingStream *stream = 0;
  RBNode *tnode;

  if (!streamidx || !streamid)
    return 0;

  /* Generate key from streamid */
  key = FNVhash64 (streamid);

  /* Search for a matching key */
  if ((tnode = RBFind (streamidx, &key)))
  {
    stream = (RingStream *)tnode->data;
  }

  return stream;
} /* End of GetStreamIdx() */

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

  if (!streamidx || !streamid)
    return -1;

  /* Generate key from streamid */
  key = FNVhash64 (streamid);

  /* Search for a matching key */
  if ((tnode = RBFind (streamidx, &key)))
  {
    RBDelete (streamidx, tnode);
  }

  return (tnode) ? 0 : -1;
} /* End of DelStreamIdx() */

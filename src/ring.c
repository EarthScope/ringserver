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

/* Macro to calculate pointer to RingPacket in buffer */
#define PACKETPTR(O) ((RingPacket *)(param.datastart + (O)))

static int StreamStackNodeCmp (StackNode *a, StackNode *b);
static inline int64_t FindOffsetForID (uint64_t pktid, nstime_t *pkttime);
static RingStream *AddStreamIdx (RBTree *streamidx, RingStream *stream, Key **ppkey);
static RingStream *GetStreamIdx (RBTree *streamidx, char *streamid);
static int DelStreamIdx (RBTree *streamidx, char *streamid);

/***************************************************************************
 * RingInitialize:
 *
 * Initialize ring buffer files either loading and validating the existing
 * ring buffer files or creating new files.
 *
 * ring file = main packet buffer file, optionally memory mapped
 * stream file = stream index file, loaded into param.streamidx
 *
 * Return >0 on buffer version mismatch, the version number is returned
 * Return  0 on success
 * Return -1 on corruption errors
 * Return -2 on non-recoverable errors
 ***************************************************************************/
int
RingInitialize (char *ringfilename, char *streamfilename, int *ringfd)
{
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
  ssize_t rv;
  RingPacket *packetptr;
  RingStream *streamptr;

  /* Sanity check input parameters */
  if (!config.volatilering && (!ringfilename || !streamfilename))
  {
    lprintf (0, "%s(): ring file and stream file must be specified", __func__);
    return -2;
  }

  /* A volatile ring will never be memory mapped */
  if (config.volatilering)
  {
    config.memorymapring = 0;
  }

  /* Determine system page size */
  if ((pagesize = sysconf (_SC_PAGESIZE)) < 0)
  {
    lprintf (0, "%s(): Error determining system page size: %s",
             __func__, strerror (errno));
    return -2;
  }

  /* Determine the number of pages needed for the header, for alignment of data packets */
  headersize = pagesize;
  while (headersize < RBV3_HEADERSIZE)
    headersize += pagesize;

  /* Sanity check that the ring can hold at least two packets */
  if (config.ringsize < (headersize + 2 * config.pktsize))
  {
    lprintf (0, "%s(): ring size (%" PRIu64 ") must be enough for 2 packets (%u each) and header (%d)",
             __func__, config.ringsize, config.pktsize, headersize);
    return -2;
  }

  /* Determine the maximum number of packets that fit after the first page */
  maxpackets = (uint64_t)((config.ringsize - headersize) / config.pktsize);

  /* Determine the maximum packet offset value */
  maxoffset = (int64_t)((maxpackets - 1) * config.pktsize);

  /* Open ring packet buffer file if non-volatile */
  if (!config.volatilering)
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
    if (ringfilestat.st_size != config.ringsize)
    {
      ringinit = 1;

      if (ringfilestat.st_size <= 0)
        lprintf (1, "Creating new ring packet buffer file");
      else
        lprintf (1, "Re-creating ring packet buffer file");

      /* Truncate file if larger than ringsize */
      if (ringfilestat.st_size > config.ringsize)
      {
        if (ftruncate (*ringfd, (off_t)config.ringsize) == -1)
        {
          lprintf (0, "%s(): error truncating %s: %s", __func__, ringfilename, strerror (errno));
          return -1;
        }
      }

      /* Go to the last byte of the desired size */
      if (lseek (*ringfd, (off_t)config.ringsize - 1, SEEK_SET) == -1)
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
  if (config.memorymapring && !config.volatilering)
  {
    lprintf (1, "Memory-mapping ring packet buffer file");

    /* Memory map the ring packet buffer file */
    if ((param.ringbuffer = (uint8_t *)mmap (NULL, config.ringsize, PROT_READ | PROT_WRITE,
                                             MAP_SHARED, *ringfd, 0)) == MAP_FAILED)
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
    if (!(param.ringbuffer = malloc (config.ringsize)))
    {
      lprintf (0, "%s(): error allocating %" PRIu64 " bytes for ring packet buffer",
               __func__, config.ringsize);
      return -2;
    }

    /* Force ring initialization if volatile */
    if (config.volatilering)
      ringinit = 1;

    /* Read ring packet buffer into memory if initialization is not needed */
    if (!ringinit)
    {
      lprintf (1, "Reading ring packet buffer file into memory");

      if (read (*ringfd, param.ringbuffer, config.ringsize) != config.ringsize)
      {
        lprintf (0, "%s(): error reading ring packet buffer into memory: %s",
                 __func__, strerror (errno));
        return -1;
      }
    }
  }

  /* If signature match but version mismatch return current buffer version */
  if (!ringinit &&
      memcmp (pRBV3_SIGNATURE (param.ringbuffer), RING_SIGNATURE, RING_SIGNATURE_LENGTH) == 0 &&
      *pRBV3_VERSION (param.ringbuffer) != RING_VERSION)
  {
    lprintf (0, "Packet buffer version %u detected", *pRBV3_VERSION (param.ringbuffer));
    return *pRBV3_VERSION (param.ringbuffer);
  }

  /* Initialize volatile ring packet buffer parameters */
  param.streamidx = RBTreeCreate (KeyCompare, free, free);
  param.datastart = param.ringbuffer + headersize;

  /* Load parameters from stored header */
  memcpy (&param.version, pRBV3_VERSION (param.ringbuffer), 2);
  memcpy (&param.ringsize, pRBV3_RINGSIZE (param.ringbuffer), 8);
  memcpy (&param.pktsize, pRBV3_PKTSIZE (param.ringbuffer), 4);
  memcpy (&param.maxpackets, pRBV3_MAXPACKETS (param.ringbuffer), 8);
  memcpy (&param.maxoffset, pRBV3_MAXOFFSET (param.ringbuffer), 8);
  memcpy (&param.headersize, pRBV3_HEADERSIZE (param.ringbuffer), 4);
  memcpy (&param.earliestid, pRBV3_EARLIESTID (param.ringbuffer), 8);
  memcpy (&param.earliestptime, pRBV3_EARLIESTPTIME (param.ringbuffer), 8);
  memcpy (&param.earliestdstime, pRBV3_EARLIESTDSTIME (param.ringbuffer), 8);
  memcpy (&param.earliestdetime, pRBV3_EARLIESTDETIME (param.ringbuffer), 8);
  memcpy (&param.earliestoffset, pRBV3_EARLIESTOFFSET (param.ringbuffer), 8);
  memcpy (&param.latestid, pRBV3_LATESTID (param.ringbuffer), 8);
  memcpy (&param.latestptime, pRBV3_LATESTPTIME (param.ringbuffer), 8);
  memcpy (&param.latestdstime, pRBV3_LATESTDSTIME (param.ringbuffer), 8);
  memcpy (&param.latestdetime, pRBV3_LATESTDETIME (param.ringbuffer), 8);
  memcpy (&param.latestoffset, pRBV3_LATESTOFFSET (param.ringbuffer), 8);

  /* Validate existing ring packet buffer parameters, resetting if needed */
  if (ringinit ||
      memcmp (pRBV3_SIGNATURE (param.ringbuffer), RING_SIGNATURE, RING_SIGNATURE_LENGTH) ||
      param.version != RING_VERSION ||
      param.ringsize != config.ringsize ||
      param.pktsize != config.pktsize ||
      param.maxpackets != maxpackets ||
      param.maxoffset != maxoffset ||
      param.headersize != headersize)
  {
    /* Report what triggered the parameter reset if not just initialized */
    if (!ringinit)
    {
      if (memcmp (pRBV3_SIGNATURE (param.ringbuffer), RING_SIGNATURE, RING_SIGNATURE_LENGTH))
        lprintf (0, "** Packet buffer signature mismatch: %.4s <-> %.4s", pRBV3_SIGNATURE (param.ringbuffer), RING_SIGNATURE);
      if (param.version != RING_VERSION)
        lprintf (0, "** Packet buffer version change: %u -> %u", param.version, RING_VERSION);
      if (param.ringsize != config.ringsize)
        lprintf (0, "** Packet buffer size change: %" PRIu64 " -> %" PRIu64, param.ringsize, config.ringsize);
      if (param.pktsize != config.pktsize)
        lprintf (0, "** Packet size change: %u -> %u", param.pktsize, config.pktsize);
      if (param.maxpackets != maxpackets)
        lprintf (0, "** Maximum packets change: %" PRIu64 " -> %" PRIu64, param.maxpackets, maxpackets);
      if (param.maxoffset != maxoffset)
        lprintf (0, "** Maximum offset change: %" PRIu64 " -> %" PRIu64, param.maxoffset, maxoffset);
      if (param.headersize != headersize)
        lprintf (0, "** Header size change: %u -> %u", param.headersize, headersize);
    }

    lprintf (0, "Resetting ring packet buffer, contents is discarded");

    param.version        = RING_VERSION;
    param.ringsize       = config.ringsize;
    param.pktsize        = config.pktsize;
    param.maxpackets     = maxpackets;
    param.maxoffset      = maxoffset;
    param.headersize     = headersize;
    param.earliestid     = RINGID_NONE;
    param.earliestptime  = NSTUNSET;
    param.earliestdstime = NSTUNSET;
    param.earliestdetime = NSTUNSET;
    param.earliestoffset = -1;
    param.latestid       = RINGID_NONE;
    param.latestptime    = NSTUNSET;
    param.latestdstime   = NSTUNSET;
    param.latestdetime   = NSTUNSET;
    param.latestoffset   = -1;

    /* Clear unused header space */
    memset (param.ringbuffer + RBV3_HEADERSIZE, 0, headersize - RBV3_HEADERSIZE);
  }
  /* If the ring has not been reset and packets are present recover stream index */
  else if (param.earliestoffset >= 0)
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
        if (!AddStreamIdx (param.streamidx, &stream, 0))
        {
          lprintf (0, "%s(): error adding stream to index", __func__);
          corruptring = 1;
        }
        else
        {
          param.streamcount++;
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

  if (param.earliestoffset > param.maxoffset)
  {
    lprintf (0, "%s(): error earliest offset > maxoffset, ring corrupted", __func__);
    corruptring = 1;
  }
  if (param.latestoffset > param.maxoffset)
  {
    lprintf (0, "%s(): error latest offset > maxoffset, ring corrupted", __func__);
    corruptring = 1;
  }

  /* Sanity checks: compare earliest and latest packet offsets between ring params
   * and lookups and check the earliest and latest stream entries. */
  if (!corruptring && param.earliestoffset >= 0)
  {
    packetptr = PACKETPTR (param.earliestoffset);

    if (packetptr->offset != param.earliestoffset)
    {
      lprintf (0, "%s(): error comparing earliest packet offsets, ring corrupted", __func__);
      corruptring = 1;
    }
    else if (!(streamptr = GetStreamIdx (param.streamidx, packetptr->streamid)))
    {
      lprintf (0, "%s(): error finding stream entry for earliest packet, ring corrupted", __func__);
      corruptring = 1;
    }
  }
  if (!corruptring && param.latestoffset >= 0)
  {
    packetptr = PACKETPTR (param.latestoffset);

    if (packetptr->offset != param.latestoffset)
    {
      lprintf (0, "%s(): error comparing latest packet offsets, ring corrupted", __func__);
      corruptring = 1;
    }
    else if (!(streamptr = GetStreamIdx (param.streamidx, packetptr->streamid)))
    {
      lprintf (0, "%s(): error finding stream entry for latest packet, ring corrupted", __func__);
      corruptring = 1;
    }
  }

  /* If corruption was detected cleanup before returning */
  if (corruptring)
  {
    RBTreeDestroy (param.streamidx);

    /* Unmap the ring file */
    if (munmap ((void *)param.ringbuffer, config.ringsize))
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
 * After the write lock of the mmap'd ring has been obtained the
 * streams index is written to streamfilename and the ring is either
 * unmapped or written to the open ringfd and closed.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
RingShutdown (int ringfd, char *streamfilename)
{
  int streamidxfd;
  int rc;
  int rv = 0;
  Stack *streams;
  RingStream *stream;

  RBNode *tnode;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  if (!config.volatilering && (!ringfd || !streamfilename))
    return -1;

  /* Free memory and return if ring is volatile */
  if (config.volatilering)
  {
    RBTreeDestroy (param.streamidx);
    free (param.ringbuffer);
    return 0;
  }

  /* Open stream index file */
  if ((streamidxfd = open (streamfilename, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0)
  {
    lprintf (0, "%s(): error opening %s: %s", __func__, streamfilename, strerror (errno));
    rv = -1;
  }

  /* Lock ring against writes, never give this up, destroyed later */
  pthread_mutex_lock (&param.ringlock);

  /* Create Stack of RingStreams */
  streams = StackCreate ();
  RBBuildStack (param.streamidx, streams);

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
  RBTreeDestroy (param.streamidx);
  StackDestroy (streams, 0);
  param.streamidx = NULL;

  /* Destroy streams index lock */
  if ((rc = pthread_mutex_destroy (&param.streamlock)))
  {
    lprintf (0, "%s(): error destroying stream lock: %s", __func__, strerror (rc));
    rv = -1;
  }

  /* Store the header values in the ring buffer */
  memcpy (pRBV3_SIGNATURE (param.ringbuffer), RING_SIGNATURE, RING_SIGNATURE_LENGTH);
  memcpy (pRBV3_VERSION (param.ringbuffer), &param.version, 2);
  memcpy (pRBV3_RINGSIZE (param.ringbuffer), &param.ringsize, 8);
  memcpy (pRBV3_PKTSIZE (param.ringbuffer), &param.pktsize, 4);
  memcpy (pRBV3_MAXPACKETS (param.ringbuffer), &param.maxpackets, 8);
  memcpy (pRBV3_MAXOFFSET (param.ringbuffer), &param.maxoffset, 8);
  memcpy (pRBV3_HEADERSIZE (param.ringbuffer), &param.headersize, 4);
  memcpy (pRBV3_EARLIESTID (param.ringbuffer), &param.earliestid, 8);
  memcpy (pRBV3_EARLIESTPTIME (param.ringbuffer), &param.earliestptime, 8);
  memcpy (pRBV3_EARLIESTDSTIME (param.ringbuffer), &param.earliestdstime, 8);
  memcpy (pRBV3_EARLIESTDETIME (param.ringbuffer), &param.earliestdetime, 8);
  memcpy (pRBV3_EARLIESTOFFSET (param.ringbuffer), &param.earliestoffset, 8);
  memcpy (pRBV3_LATESTID (param.ringbuffer), &param.latestid, 8);
  memcpy (pRBV3_LATESTPTIME (param.ringbuffer), &param.latestptime, 8);
  memcpy (pRBV3_LATESTDSTIME (param.ringbuffer), &param.latestdstime, 8);
  memcpy (pRBV3_LATESTDETIME (param.ringbuffer), &param.latestdetime, 8);
  memcpy (pRBV3_LATESTOFFSET (param.ringbuffer), &param.latestoffset, 8);

  if (config.memorymapring)
  {
    /* Unmap the ring buffer file */
    lprintf (1, "Unmapping and closing ring buffer file");
    if (munmap ((void *)param.ringbuffer, param.ringsize))
    {
      lprintf (0, "%s(): error unmapping ring buffer file: %s", __func__, strerror (errno));
      rv = -1;
    }

    /* Destroy ring write lock */
    pthread_mutex_unlock (&param.ringlock);
    if ((rc = pthread_mutex_destroy (&param.ringlock)))
    {
      lprintf (0, "%s(): error destroying ring write lock: %s", __func__, strerror (rc));
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

    if (write (ringfd, param.ringbuffer, param.ringsize) != param.ringsize)
    {
      lprintf (0, "%s(): error writing ring buffer file: %s", __func__, strerror (errno));
      rv = -1;
    }

    /* Destroy ring write lock */
    pthread_mutex_unlock (&param.ringlock);
    if ((rc = pthread_mutex_destroy (&param.ringlock)))
    {
      lprintf (0, "%s(): error destroying ring write lock: %s", __func__, strerror (rc));
      rv = -1;
    }

    /* Free the ring buffer memory */
    free (param.ringbuffer);
    param.ringbuffer = NULL;
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
 * Returns 0 on success, -1 on non-corruption error and -2 on corrupt
 * ring error.
 ***************************************************************************/
int
RingWrite (RingPacket *packet, char *packetdata, uint32_t datasize)
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

  if (!packet || !packetdata)
    return -1;

  /* Check packet size */
  if ((sizeof (RingPacket) + datasize) > param.pktsize)
  {
    lprintf (0, "%s(): %s packet size too large (%lu), maximum is %d bytes",
             __func__, packet->streamid, (sizeof (RingPacket) + datasize), param.pktsize);
    return -1;
  }

  /* Lock ring and streams index */
  pthread_mutex_lock (&param.ringlock);
  pthread_mutex_lock (&param.streamlock);

  /* Set packet entries for earliest and latest packets in ring */
  if (param.earliestoffset >= 0)
  {
    earliest = PACKETPTR (param.earliestoffset);
  }
  if (param.latestoffset >= 0)
  {
    latest = PACKETPTR (param.latestoffset);
  }

  /* Determine next packet ID and offset */
  if (latest)
  {
    offset = NEXTOFFSET (latest->offset, param.maxoffset, config.pktsize);
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
    if (offset == param.earliestoffset)
    {
      int64_t next_offset;                 /* New earliest packet offset */
      RingPacket *nextInRing       = NULL; /* New earliest packet in ring */
      RingPacket *nextInStream     = NULL; /* New earliest packet in stream */
      RingStream *streamOfEarliest = NULL; /* Stream of old earliest packet */

      next_offset  = NEXTOFFSET (earliest->offset, param.maxoffset, config.pktsize);
      nextInRing   = PACKETPTR (next_offset);
      nextInStream = PACKETPTR (earliest->nextinstream);

      if (!(streamOfEarliest = GetStreamIdx (param.streamidx, earliest->streamid)))
      {
        lprintf (0, "%s(): Error getting earliest packet stream", __func__);
        pthread_mutex_unlock (&param.ringlock);
        pthread_mutex_unlock (&param.streamlock);
        return -2;
      }

      lprintf (3, "Removing packet for stream %s (id: %" PRIu64 ", offset: %" PRId64 ")",
               earliest->streamid, earliest->pktid, earliest->offset);

      /* Update params with new earliest entry */
      param.earliestid     = nextInRing->pktid;
      param.earliestptime  = nextInRing->pkttime;
      param.earliestdstime = nextInRing->datastart;
      param.earliestdetime = nextInRing->dataend;
      param.earliestoffset = nextInRing->offset;

      /* Delete stream entry if this is the only packet */
      if (earliest->offset == streamOfEarliest->earliestoffset &&
          earliest->offset == streamOfEarliest->latestoffset)
      {
        lprintf (2, "Removing stream index entry for %s", earliest->streamid);
        DelStreamIdx (param.streamidx, earliest->streamid);
        param.streamcount--;
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
  if (!(stream = GetStreamIdx (param.streamidx, packet->streamid)))
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
    if (!(stream = AddStreamIdx (param.streamidx, &newstream, &skey)))
    {
      lprintf (0, "%s(): Error adding new stream index", __func__);
      if (node)
      {
        free (node->key);
        free (node->data);
        free (node);
      }
      pthread_mutex_unlock (&param.ringlock);
      pthread_mutex_unlock (&param.streamlock);
      return -2;
    }
    else
    {
      param.streamcount++;
    }

    lprintf (2, "Added stream entry for %s (key: %" PRIx64 ")", packet->streamid, *skey);
  }

  /* Copy packet header into ring */
  uint8_t *writeptr = (uint8_t *)PACKETPTR (offset);
  memcpy (writeptr, packet, sizeof (RingPacket));

  /* Copy packet data into ring directly after header */
  memcpy ((writeptr + sizeof (RingPacket)), packetdata, datasize);

  /* Update ring params with new latest packet */
  param.latestid     = packet->pktid;
  param.latestptime  = packet->pkttime;
  param.latestdstime = packet->datastart;
  param.latestdetime = packet->dataend;
  param.latestoffset = packet->offset;

  /* Update ring params with new earliest packet (for initial packet) */
  if (!earliest)
  {
    param.earliestid     = packet->pktid;
    param.earliestptime  = packet->pkttime;
    param.earliestdstime = packet->datastart;
    param.earliestdetime = packet->dataend;
    param.earliestoffset = packet->offset;
  }

  /* Update entry for previous packet in stream */
  if (stream->latestoffset >= 0)
  {
    prevlatest = PACKETPTR (stream->latestoffset);

    prevlatest->nextinstream = packet->offset;
  }

  /* Update stream entry */
  stream->latestdstime = packet->datastart;
  stream->latestdetime = packet->dataend;
  stream->latestptime  = packet->pkttime;
  stream->latestid     = packet->pktid;
  stream->latestoffset = packet->offset;

  /* Unlock ring and stream index */
  pthread_mutex_unlock (&param.ringlock);
  pthread_mutex_unlock (&param.streamlock);

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
  RingPacket *pkt;
  nstime_t pkttime;
  uint64_t pktid = RINGID_NONE;
  int64_t offset = -1;

  if (!reader || !packet)
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
  if (offset < 0 && (offset = FindOffsetForID (pktid, &pkttime)) < 0)
  {
    return RINGID_NONE;
  }

  pkt = PACKETPTR (offset);

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

  latestoffset   = param.latestoffset;
  earliestoffset = param.earliestoffset;

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
  latestid     = PACKETPTR (latestoffset)->pktid;
  latestptime  = PACKETPTR (latestoffset)->pkttime;
  latestdstime = PACKETPTR (latestoffset)->datastart;
  latestdetime = PACKETPTR (latestoffset)->dataend;

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
    offset = NEXTOFFSET (reader->pktoffset, param.maxoffset, config.pktsize);
  }

  /* Determine the end-of-buffer offset as the one following the latest offset */
  eoboffset = NEXTOFFSET (latestoffset, param.maxoffset, config.pktsize);

  /* Loop until we have a matching packet or reached the end of the buffer */
  skip    = 1;
  skipped = 0;
  while (skip && offset != eoboffset)
  {
    skip = 0;

    pkt     = PACKETPTR (offset);
    pkttime = pkt->pkttime;

    /* Determine if this is a valid packet by checking that the packet time has
     * not advanced past the lastest time */
    if (pkttime > latestptime)
    {
      /* If the packet has been replaced, assume the reader has been lapped (fallen off
       * the trailing edge of the buffer) and reposition to the earliest packet */

      offset = param.earliestoffset;
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
      offset = NEXTOFFSET (offset, param.maxoffset, config.pktsize);
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
  RingPacket *pkt;
  nstime_t ptime;
  nstime_t datastart, dataend;
  int64_t offset;

  if (!reader)
    return RINGID_ERROR;

  /* Determine packet ID for relative positions */
  if (pktid == RINGID_EARLIEST)
  {
    pktid = param.earliestid;
  }
  else if (pktid == RINGID_LATEST)
  {
    pktid = param.latestid;
  }

  if (pktid > RINGID_MAXIMUM)
  {
    lprintf (0, "%s(): unsupported position value: %" PRIu64, __func__, pktid);
    return RINGID_ERROR;
  }

  /* Find the offset to the packet */
  if ((offset = FindOffsetForID (pktid, &ptime)) < 0)
  {
    return RINGID_NONE;
  }

  pkt = PACKETPTR (offset);

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

  /* Start searching with the earliest packet in the ring */
  offset = param.earliestoffset;

  /* Loop through packets in forward order */
  while (skipped < param.maxpackets)
  {
    skip = 0;

    /* Get pointer to RingPacket */
    pkt1 = PACKETPTR (offset);

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
    if (offset == param.latestoffset)
    {
      break;
    }

    offset = NEXTOFFSET (offset, param.maxoffset, config.pktsize);
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
  RingPacket *pkt   = NULL;
  RingPacket *spkt  = NULL;
  nstime_t pkttime  = NSTUNSET;
  nstime_t datastart;
  nstime_t dataend;
  uint64_t pktid = RINGID_NONE;
  int64_t offset;
  int64_t soffset;
  uint64_t count = 0;
  uint8_t skip;

  if (!reader)
    return RINGID_ERROR;

  /* Start searching with the latest packet in the ring */
  offset  = param.latestoffset;
  soffset = offset;

  /* Loop through packets in reverse order */
  while (count < pktlimit)
  {
    skip = 0;

    /* Get pointer to RingPacket */
    spkt = PACKETPTR (soffset);

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
    if (soffset == param.earliestoffset)
    {
      break;
    }

    soffset = PREVOFFSET (soffset, param.maxoffset, config.pktsize);
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
    soffset = PREVOFFSET (soffset, param.maxoffset, config.pktsize);

    spkt = PACKETPTR (soffset);

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
GetStreamsStack (RingReader *reader)
{
  RingStream *stream;
  RingStream *newstream;
  RBNode *tnode;
  Stack *streams;
  Stack *newstreams;

  /* Lock the streams index */
  pthread_mutex_lock (&param.streamlock);

  streams    = StackCreate ();
  newstreams = StackCreate ();

  RBBuildStack (param.streamidx, streams);

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
  pthread_mutex_unlock (&param.streamlock);

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
FindOffsetForID (uint64_t pktid, nstime_t *pkttime)
{
  RingPacket *packet = NULL;
  int64_t latestoffset;
  int64_t earliestoffset;
  int64_t offset;

  latestoffset   = param.latestoffset;
  earliestoffset = param.earliestoffset;

  /* Ring is empty */
  if (earliestoffset < 0 || latestoffset < 0)
  {
    return -1;
  }

  /* Earliest ID is less than latest ID.
   * Assume they increment from earliest to latest.
   * Assume the pktid must exist within the range. */
  if (param.earliestid <= param.latestid)
  {
    /* Check that requested packet ID is within earliest - latest range */
    if (pktid < param.earliestid || pktid > param.latestid)
    {
      return -1;
    }

    int64_t latestoffset_unwrapped = (latestoffset < earliestoffset) ? latestoffset + param.maxoffset : latestoffset;

    int64_t lowpkt = 0;
    int64_t highpkt = (latestoffset_unwrapped - earliestoffset) / param.pktsize;
    int64_t midpkt;

    /* Binary search for a matching ID */
    while (lowpkt <= highpkt)
    {
      midpkt = lowpkt + (highpkt - lowpkt) / 2;

      int64_t offset_unwrapped = earliestoffset + (midpkt * param.pktsize);

      offset = (offset_unwrapped > param.maxoffset) ? offset_unwrapped - param.maxoffset : offset_unwrapped;

      packet = PACKETPTR (offset);

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
    offset = NEXTOFFSET (latestoffset, param.maxoffset, param.pktsize);
    do
    {
      offset = PREVOFFSET (offset, param.maxoffset, param.pktsize);

      packet = PACKETPTR (offset);

      if (packet->pktid == pktid)
      {
        if (pkttime)
          *pkttime = packet->pkttime;

        return offset;
      }
    } while (offset != param.earliestoffset);
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

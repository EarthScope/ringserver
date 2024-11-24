/**************************************************************************
 * loadbuffer.c
 *
 * Load ring buffer data from older ring buffer versions.
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

#include "logging.h"
#include "ring.h"

/* Macros to determine next and previous packet offsets given an
 * reference offset, maximum offset, and packet size */
#define NEXTOFFSET(O, M, S) (((O) + (S) > (M)) ? 0 : (O) + (S))
#define PREVOFFSET(O, M, S) (((O) == 0) ? (M) : (O) - (S))

#define RING_SIGNATUREv1 "RING"
#define RING_VERSIONv1   1
#define MAXSTREAMIDv1    60

#define hptime_t int64_t /* Time value in microseconds */

/* Ring parameters, stored at the beginning of the packet buffer file */
typedef struct RingParamsV1_s
{
  char      signature[4];     /* RING_SIGNATURE */
  uint16_t  version;          /* RING_VERSION */
  uint64_t  ringsize;         /* Ring size in bytes */
  uint32_t  pktsize;          /* Packet size in bytes */
  int64_t   maxpktid;         /* Maximum packet ID */
  int64_t   maxpackets;       /* Maximum number of packets */
  int64_t   maxoffset;        /* Maximum packet offset */
  uint32_t  headersize;       /* Size of ring header */
  uint8_t   corruptflag;      /* Flag indicating the ring is corrupt */
  uint8_t   fluxflag;         /* Flag indicating the ring is in flux */
  uint8_t   mmapflag;         /* Memory mapped flag */
  uint8_t   volatileflag;     /* Volatile ring flag */
  pthread_mutex_t *writelock; /* Mutex lock for ring write access */
  RBTree   *streamidx;        /* Binary tree of streams */
  pthread_mutex_t *streamlock;/* Mutex lock for stream index */
  int32_t   streamcount;      /* Count of streams in index */
  int64_t   earliestid;       /* Earliest packet ID */
  hptime_t  earliestptime;    /* Earliest packet creation time */
  hptime_t  earliestdstime;   /* Earliest packet data start time */
  hptime_t  earliestdetime;   /* Earliest packet data end time */
  int64_t   earliestoffset;   /* Earliest packet offset in bytes */
  int64_t   latestid;         /* Latest packet ID */
  hptime_t  latestptime;      /* Latest packet creation time */
  hptime_t  latestdstime;     /* Latest packet data start time */
  hptime_t  latestdetime;     /* Latest packet data end time */
  int64_t   latestoffset;     /* Latest packet offset in bytes */
  hptime_t  ringstart;        /* Ring initialization time */
  double    txpacketrate;     /* Transmission packet rate in Hz */
  double    txbyterate;       /* Transmission byte rate in Hz */
  double    rxpacketrate;     /* Reception packet rate in Hz */
  double    rxbyterate;       /* Reception byte rate in Hz */
  char     *data;             /* Pointer to start of data buffer */
} RingParamsV1;


/* Ring packet header structure, data follows header in the ring */
/* RW tagged values are set when packets are added to the ring */
typedef struct RingPacketV1_s
{
  int64_t   pktid;           /* RW: Packet ID */
  int64_t   offset;          /* RW: Offset in ring */
  hptime_t  pkttime;         /* RW: Packet creation time */
  int64_t   nextinstream;    /* RW: ID of next packet in stream, 0 if none */
  char      streamid[MAXSTREAMIDv1]; /* Packet stream ID, NULL terminated */
  hptime_t  datastart;       /* Packet data start time */
  hptime_t  dataend;         /* Packet data end time */
  uint32_t  datasize;        /* Packet data size in bytes */
} RingPacketV1;


/***************************************************************************
 * LoadBufferV1:
 *
 * Open a ringserver version 1 packet buffer file and insert all data
 * packets into the current ring buffer.
 *
 * Return >=0 on success, number of packets loaded
 * Return  -1 on errors
 ***************************************************************************/
int64_t
LoadBufferV1 (char *ringfile_v1, RingParams *ringparams)
{
  RingParamsV1 ringparams_v1;
  RingPacketV1 *packet_v1;
  RingPacket packet;
  char *packetbuffer   = NULL;
  int64_t offset       = -1;
  uint8_t verbose_save = verbose;
  int64_t count        = 0;
  int ringfd_v1;

  pcre2_code *pcre_code       = NULL;
  pcre2_match_data *pcre_data = NULL;

  if (!ringfile_v1 || !ringparams)
  {
    return -1;
  }

  /* Open the version 1 ring file */
  if ((ringfd_v1 = open (ringfile_v1, O_RDONLY)) < 0)
  {
    lprintf (0, "%s(): error opening version 1 ring file %s: %s",
             __func__, ringfile_v1, strerror (errno));
    return -1;
  }

  /* Read the version 1 ring parameters */
  if (read (ringfd_v1, &ringparams_v1, sizeof (RingParamsV1)) != sizeof (RingParamsV1))
  {
    lprintf (0, "%s(): error reading version 1 ring parameters: %s",
             __func__, strerror (errno));
    close (ringfd_v1);
    return -1;
  }

  /* Check for v1 version, signature, corruption or busy flags */
  if (ringparams_v1.version != RING_VERSIONv1 ||
      memcmp (ringparams_v1.signature, RING_SIGNATUREv1, sizeof (ringparams_v1.signature)) != 0 ||
      ringparams_v1.corruptflag != 0 ||
      ringparams_v1.fluxflag != 0)
  {
    lprintf (0, "%s(): version 1 ring file %s has invalid signature, is corrupt, or marked busy",
             __func__, ringfile_v1);
    close (ringfd_v1);
    return -1;
  }

  /* Check for empty ring, in v1 offsets were -1 when not set */
  if (ringparams_v1.earliestoffset < 0)
  {
    lprintf (2, "%s(): version 1 ring file %s is empty",
             __func__, ringfile_v1);
    close (ringfd_v1);
    return 0;
  }

  /* Check for compatible packet size */
  if (ringparams_v1.pktsize > ringparams->pktsize)
  {
    lprintf (0, "%s(): version 1 ring file %s has incompatible packet size %u, expected <= %u",
             __func__, ringfile_v1, ringparams_v1.pktsize, ringparams->pktsize);
    close (ringfd_v1);
    return -1;
  }

  /* Allocate memory for packet data */
  if (!(packetbuffer = (char *)malloc (ringparams->pktsize)))
  {
    lprintf (0, "%s(): error allocating memory for packet data", __func__);
    close (ringfd_v1);
    return -1;
  }

  /* Compile the legacy miniSEED stream ID pattern */
  if (UpdatePattern (&pcre_code, &pcre_data,
                     LEGACY_MSEED_STREAMID_PATTERN,
                     "legacy miniSEED stream ID pattern"))
  {
    free (packetbuffer);
    close (ringfd_v1);
    return -1;
  }

  lprintf (0, "Loading version 1 ring file %s into current ring buffer", ringfile_v1);

  /* Disable verbose logging during load */
  verbose = 0;

  /* Traverse packet buffer from earliest to latest */
  offset = ringparams_v1.earliestoffset;
  while (offset >= 0 && offset <= ringparams_v1.maxoffset)
  {
    /* Read packet from offset */
    if (pread (ringfd_v1, packetbuffer, ringparams_v1.pktsize, ringparams_v1.headersize + offset) != ringparams_v1.pktsize)
    {
      lprintf (0, "%s(): error reading packet from version 1 ring file %s: %s",
               __func__, ringfile_v1, strerror (errno));
      break;
    }

    packet_v1 = (RingPacketV1 *)packetbuffer;

    /* Convert packet to current version */
    packet.pktid     = packet_v1->pktid;
    packet.datastart = MS_HPTIME2NSTIME (packet_v1->datastart);
    packet.dataend   = MS_HPTIME2NSTIME (packet_v1->dataend);
    packet.datasize  = packet_v1->datasize;

    /* Translate legacy stream ID: NN_SSSSS_LL_CCC/MSEED
     * to an FDSN Source ID: FDSN:NN_SSSSS_LL_C_C_C/MSEED */
    if (pcre_code != NULL &&
        pcre2_match (pcre_code, (PCRE2_SPTR8)packet_v1->streamid,
                     PCRE2_ZERO_TERMINATED, 0, 0,
                     pcre_data, NULL) > 0)
    {
      char *prechannel = strrchr (packet_v1->streamid, '_');

      snprintf (packet.streamid, sizeof (packet.streamid),
                "FDSN:%.*s_%c_%c_%c%s",
                (int)(prechannel - packet_v1->streamid), packet_v1->streamid,
                prechannel[1], prechannel[2], prechannel[3],
                &prechannel[4]);

      if (verbose_save >= 3)
        lprintf (3, "Translating legacy stream ID: %s -> %s",
                 packet_v1->streamid, packet.streamid);
    }
    /* Otherwise copy stream ID verbatim */
    else
    {
      /* Copy the stream ID verbatim */
      memcpy (packet.streamid, packet_v1->streamid, sizeof (packet.streamid));

      /* Make sure the streamid is terminated */
      packet.streamid[sizeof (packet.streamid) - 1] = '\0';
    }

    if (verbose_save >= 3)
      lprintf (0, "Loading packet ID %" PRId64 " from stream %s at offset %" PRId64,
               packet.pktid, packet.streamid, offset);

    /* Add packet to the current ring buffer */
    if (RingWrite (ringparams, &packet, packetbuffer + sizeof (RingPacketV1), packet.datasize))
    {
      lprintf (0, "%s(): error adding packet to current ring buffer", __func__);
      break;
    }

    count++;

    if (offset == ringparams_v1.latestoffset)
    {
      break;
    }

    offset = NEXTOFFSET (offset, ringparams_v1.maxoffset, ringparams_v1.pktsize);
  }

  verbose = verbose_save;

  UpdatePattern (&pcre_code, &pcre_data, NULL, NULL);
  free (packetbuffer);
  close (ringfd_v1);

  return count;
} /* End of LoadBufferV1() */

/**************************************************************************
 * ringserver.c
 *
 * Multi-threaded TCP generic ring buffer data server with interfaces
 * for SeedLink and DataLink protocols.
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
 * Copyright (C) 2020:
 * @author Chad Trabant, IRIS Data Management Center
 **************************************************************************/

/* _GNU_SOURCE needed to get strcasestr() under Linux */
#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <libmseed.h>

#include "clients.h"
#include "dlclient.h"
#include "dsarchive.h"
#include "generic.h"
#include "logging.h"
#include "mseedscan.h"
#include "ring.h"
#include "ringserver.h"
#include "slclient.h"

/* Reserve connection count, allows connections from addresses with write
 * permission even when the maximum connection count has been reached. */
#define RESERVECONNECTIONS 10

/* A structure to list IP addresses ranges */
typedef struct IPNet_s
{
  union {
    struct in_addr in_addr;
    struct in6_addr in6_addr;
  } network;
  union {
    struct in_addr in_addr;
    struct in6_addr in6_addr;
  } netmask;
  int family;
  char *limitstr;
  struct IPNet_s *next;
} IPNet;

/* Global variables defined here */
pthread_mutex_t sthreads_lock = PTHREAD_MUTEX_INITIALIZER;
struct sthread *sthreads = NULL; /* Server threads list */
pthread_mutex_t cthreads_lock = PTHREAD_MUTEX_INITIALIZER;
struct cthread *cthreads = NULL; /* Client threads list */

char *serverid = NULL;    /* Server ID */
char *webroot = NULL;     /* Web content root directory */
hptime_t serverstarttime; /* Server start time */
int clientcount = 0;      /* Track number of connected clients */
int resolvehosts = 1;     /* Flag to control resolving of client hostnames */
int shutdownsig = 0;      /* Shutdown signal */

/* Local functions and variables */
static struct thread_data *InitThreadData (void *prvtptr);
static void *ListenThread (void *arg);
static int InitServerSocket (char *portstr, uint8_t protocols);
static int ProcessParam (int argcount, char **argvec);
static char *GetOptVal (int argcount, char **argvec, int argopt);
static int ReadConfigFile (char *configfile, int dynamiconly, time_t mtime);
static int ConfigMSWrite (char *value);
static int AddListenThreads (ListenPortParams *lpp);
static int AddMSeedScanThread (char *configstr);
static int AddServerThread (unsigned int type, void *params);
static uint64_t CalcSize (char *sizestr);
static int CalcStats (ClientInfo *cinfo);
static int AddIPNet (IPNet **pplist, char *network, char *limitstr);
static IPNet *MatchIP (IPNet *list, struct sockaddr *addr);
static int ClientIPCount (struct sockaddr *addr);
static void *SignalThread (void *arg);
static void PrintHandler ();
static void Usage (int level);

static char *configfile = NULL;                      /* Configuration file */
static time_t configfilemtime = 0;                   /* Modification time of configuration file */
static uint32_t maxclients = 600;                    /* Enforce maximum number of clients */
static uint32_t maxclientsperip = 0;                 /* Enforce maximum number of clients per IP */
static uint32_t clienttimeout = 3600;                /* Drop clients if no exchange within this limit */
static char *ringdir = NULL;                         /* Directory for ring files */
static uint64_t ringsize = 1073741824;               /* Size of ring buffer file (1 gigabyte) */
static uint64_t maxpktid = 0xFFFFFF;                 /* Maximum packet ID (2^16 = 16,777,215) */
static uint32_t pktsize = sizeof (RingPacket) + 512; /* Ring packet size */
static uint8_t memorymapring = 1;                    /* Flag to control mmap'ing of packet buffer */
static uint8_t volatilering = 0;                     /* Flag to control if ring is volatile or not */
static uint8_t autorecovery = 1;                     /* Flag to control auto recovery from corruption */
static float timewinlimit = 1.0;                     /* Time window search limit in percent */
static char *mseedarchive = NULL;                    /* miniSEED archive definition */
static int mseedidleto = 300;                        /* miniSEED idle file timeout */
static sigset_t globalsigset;                        /* Signal set for signal handling */

static int tcpprotonumber = -1;

static RingParams *ringparams = NULL;

static IPNet *limitips = NULL;
static IPNet *matchips = NULL;
static IPNet *rejectips = NULL;
static IPNet *writeips = NULL;
static IPNet *trustedips = NULL;

static char *httpheaders = NULL;

int
main (int argc, char *argv[])
{
  char ringfilename[512];
  char streamfilename[512];
  hptime_t hpcurtime;
  time_t curtime;
  time_t chktime;
  struct timespec timereq;
  pthread_t stid;
  pthread_t sigtid;
  struct sthread *stp;
  struct sthread *loopstp;
  struct cthread *ctp;
  struct cthread *loopctp;
  char statusstr[100];
  int tlogwrite = 0;
  int servercount = 0;

  double txpacketrate;
  double txbyterate;
  double rxpacketrate;
  double rxbyterate;

  struct stat cfstat;
  int configreset = 0;
  int ringinit;

  struct protoent *pe_tcp;

  /* Ring descriptor */
  int ringfd = -1;

  /* Process command line parameters */
  if (ProcessParam (argc, argv) < 0)
    return 1;

  /* Redirect libmseed logging facility to lprintf() via the lprint() shim */
  ms_loginit (lprint, 0, lprint, 0);

  /* Signal handling using POSIX routines, create set of all signals */
  if (sigfillset (&globalsigset))
  {
    lprintf (0, "Error: sigfillset() failed, cannot initialize signal set");
    return 1;
  }

  /* Block signals in set, mask is inherited by all child threads */
  if (pthread_sigmask (SIG_BLOCK, &globalsigset, NULL))
  {
    lprintf (0, "Error: pthread_sigmask() failed, cannot set thread signal mask");
    return 1;
  }

  /* Start signal handling thread */
  lprintf (2, "Starting signal handling thread");

  if ((errno = pthread_create (&sigtid, NULL, SignalThread, NULL)))
  {
    lprintf (0, "Error creating signal handling thread: %s", strerror (errno));
    return 1;
  }

  /* Look up & store the system TCP protocol entry */
  if (!(pe_tcp = getprotobyname ("tcp")))
  {
    lprintf (0, "Error: cannot determine the system protocol number for TCP");
    return 1;
  }
  else
  {
    tcpprotonumber = pe_tcp->p_proto;
  }

  /* Init ring parameters */
  if (ringdir || volatilering)
  {
    if (!volatilering)
    {
      /* Create ring file path: "<ringdir>/packetbuf" */
      strncpy (ringfilename, ringdir, sizeof (ringfilename) - 20);
      strcat (ringfilename, "/packetbuf");

      /* Create stream index file path: "<ringdir>/streamidx" */
      strncpy (streamfilename, ringdir, sizeof (streamfilename) - 20);
      strcat (streamfilename, "/streamidx");
    }
    else
    {
      ringfilename[0] = '\0';
      streamfilename[0] = '\0';
    }

    /* Initialize ring system */
    if ((ringinit = RingInitialize (ringfilename, streamfilename, ringsize, pktsize, maxpktid,
                                    memorymapring, volatilering, &ringfd, &ringparams)))
    {
      /* Exit on unrecoverable errors and if no auto recovery */
      if (ringinit == -2 || !autorecovery)
      {
        lprintf (0, "Error initializing ring buffer (%d)", ringinit);
        return 1;
      }

      if (ringfd > 0)
      {
        if (close (ringfd))
        {
          lprintf (0, "Error closing ring buffer file: %s", strerror (errno));
        }
      }

      /* Move corrupt packet buffer and index to backup (.corrupt) files */
      if (autorecovery == 1)
      {
        char ringfilecorr[520];
        char streamfilecorr[520];

        lprintf (0, "Auto recovery, moving packet buffer and stream index files");

        /* Create .corrupt ring and stream file names */
        snprintf (ringfilecorr, sizeof (ringfilecorr), "%s.corrupt", ringfilename);
        snprintf (streamfilecorr, sizeof (streamfilecorr), "%s.corrupt", streamfilename);

        /* Rename original ring and stream files to the corrupt names */
        if (rename (ringfilename, ringfilecorr) && errno != ENOENT)
        {
          lprintf (0, "Error renaming %s to %s: %s", ringfilename, ringfilecorr,
                   strerror (errno));
          return 1;
        }
        if (rename (streamfilename, streamfilecorr) && errno != ENOENT)
        {
          lprintf (0, "Error renaming %s to %s: %s", streamfilename, streamfilecorr,
                   strerror (errno));
          return 1;
        }
      }
      /* Removing existing packet buffer and index */
      else if (autorecovery == 2)
      {
        lprintf (0, "Auto recovery, removing exising packet buffer and stream index files");

        /* Delete existing ring and stream files */
        if (unlink (ringfilename) && errno != ENOENT)
        {
          lprintf (0, "Error removing %s: %s", ringfilename, strerror (errno));
          return 1;
        }
        if (unlink (streamfilename) && errno != ENOENT)
        {
          lprintf (0, "Error renaming %s: %s", streamfilename, strerror (errno));
          return 1;
        }
      }
      else
      {
        lprintf (0, "Unrecognized auto recovery value: %u", autorecovery);
        return 1;
      }

      /* Re-initialize ring system */
      if ((ringinit = RingInitialize (ringfilename, streamfilename, ringsize, pktsize, maxpktid,
                                      memorymapring, volatilering, &ringfd, &ringparams)))
      {
        lprintf (0, "Error re-initializing ring buffer on auto-recovery (%d)", ringinit);
        return 1;
      }
    }
  }
  else
  {
    lprintf (0, "Error: ring directory is not set and ring is not volatile");
    return 1;
  }

  /* Set server start time */
  serverstarttime = HPnow ();

  /* Initialize watchdog loop interval timers */
  curtime = time (NULL);
  chktime = curtime;

  /* Initialize transfer log window timers */
  if (TLogParams.tlogbasedir)
  {
    TLogParams.tlogstart = curtime;

    if (CalcIntWin (curtime, TLogParams.tloginterval,
                    &TLogParams.tlogstartint, &TLogParams.tlogendint))
    {
      lprintf (0, "Error calculating interval time window");
      return 1;
    }
  }

  /* Set loop interval check tick to 1/4 second */
  timereq.tv_sec = 0;
  timereq.tv_nsec = 250000000;

  /* Watchdog loop: monitors the server and client threads
     performing restarts and cleanup when necessary. */
  for (;;)
  {
    hpcurtime = HPnow ();

    /* If shutdown is requested signal all client threads */
    if (shutdownsig == 1)
    {
      shutdownsig = 2;

      /* Set shutdown loop throttle of .1 seconds */
      timereq.tv_nsec = 100000000;

      /* Request shutdown of server threads */
      pthread_mutex_lock (&sthreads_lock);
      loopstp = sthreads;
      while (loopstp)
      {
        /* Close listening server sockets, causing the listen thread to exit too */
        if (loopstp->type == LISTEN_THREAD)
        {
          ListenPortParams *lpp = loopstp->params;

          if (lpp->socket > 0)
          {
            lprintf (3, "Closing port %s server socket", lpp->portstr);
            shutdown (lpp->socket, SHUT_RDWR);
            close (lpp->socket);
            lpp->socket = -1;
          }
        }
        /* Otherwise set thread flag to CLOSE */
        else if (loopstp->td && (!(loopstp->td->td_flags & TDF_CLOSING) && !(loopstp->td->td_flags & TDF_CLOSED)))
        {
          lprintf (3, "Requesting shutdown of server thread %lu",
                   (unsigned long int)loopstp->td->td_id);

          pthread_mutex_lock (&(loopstp->td->td_lock));
          loopstp->td->td_flags = TDF_CLOSE;
          pthread_mutex_unlock (&(loopstp->td->td_lock));
        }

        loopstp = loopstp->next;
      }
      pthread_mutex_unlock (&sthreads_lock);

      /* Request shutdown of client threads */
      pthread_mutex_lock (&cthreads_lock);
      loopctp = cthreads;
      while (loopctp)
      {
        if (!(loopctp->td->td_flags & TDF_CLOSING) && !(loopctp->td->td_flags & TDF_CLOSED))
        {
          lprintf (3, "Requesting shutdown of client thread %lu",
                   (unsigned long int)loopctp->td->td_id);

          pthread_mutex_lock (&(loopctp->td->td_lock));
          loopctp->td->td_flags = TDF_CLOSE;
          pthread_mutex_unlock (&(loopctp->td->td_lock));
        }
        loopctp = loopctp->next;
      }
      pthread_mutex_unlock (&cthreads_lock);
    } /* Done initializing shutdown sequence */

    if (shutdownsig > 1)
    {
      /* Safety valve for deadlock, should never get here */
      if (shutdownsig >= 100)
      {
        lprintf (0, "Shutdown did not complete cleanly after ~10 seconds");
        break;
      }

      shutdownsig++;
    }

    /* Transmission log writing time window check */
    if (TLogParams.tlogbasedir && !shutdownsig)
    {
      if (curtime >= TLogParams.tlogendint)
        tlogwrite = 1;
      else
        tlogwrite = 0;
    }

    /* Loop through server thread list to monitor threads, print status and perform cleanup */
    pthread_mutex_lock (&sthreads_lock);
    loopstp = sthreads;
    servercount = 0;
    while (loopstp)
    {
      char *threadtype = "";
      void *(*threadfunc) (void *) = 0;

      stp = loopstp;
      loopstp = loopstp->next;

      if (stp->type == LISTEN_THREAD)
      {
        threadtype = "Listen";
        threadfunc = &ListenThread;
      }
      else if (stp->type == MSEEDSCAN_THREAD)
      {
        threadtype = "MSeedScan";
        threadfunc = &MS_ScanThread;
      }

      /* Report status of server thread */
      if (stp->td)
      {
        sprintf (statusstr, "Server thread (%s) %lu Flags:", threadtype, (unsigned long int)stp->td->td_id);
        if (stp->td->td_flags & TDF_SPAWNING)
          strcat (statusstr, " SPAWNING");
        if (stp->td->td_flags & TDF_ACTIVE)
          strcat (statusstr, " ACTIVE");
        if (stp->td->td_flags & TDF_CLOSE)
          strcat (statusstr, " CLOSE");
        if (stp->td->td_flags & TDF_CLOSING)
          strcat (statusstr, " CLOSING");
        if (stp->td->td_flags & TDF_CLOSED)
          strcat (statusstr, " CLOSED");
        lprintf (3, "%s", statusstr);

        servercount++;
      }
      else
        lprintf (2, "Server thread (%s) not running", threadtype);

      if (stp->type == LISTEN_THREAD)
      {
        ListenPortParams *lpp = stp->params;

        /* Cleanup CLOSED listen thread */
        if (stp->td && stp->td->td_flags & TDF_CLOSED)
        {
          lprintf (1, "Joining CLOSED %s thread", threadtype);

          if ((errno = pthread_join (stp->td->td_id, NULL)))
          {
            lprintf (0, "Error joining CLOSED %s thread %lu: %s", threadtype,
                     (unsigned long int)stp->td->td_id, strerror (errno));
          }

          free (stp->td);
          stp->td = 0;
        }

        /* Start new listening thread if needed */
        if (stp->td == 0 && !shutdownsig)
        {
          /* Initialize thread data and create thread */
          if (!(stp->td = InitThreadData (lpp)))
          {
            lprintf (0, "Error initializing %s thread_data: %s", threadtype, strerror (errno));
          }
          else
          {
            lprintf (2, "Starting %s listen thread for port %s", threadtype, lpp->portstr);

            if ((errno = pthread_create (&stid, NULL, threadfunc, (void *)stp->td)))
            {
              lprintf (0, "Error creating %s thread: %s", threadtype, strerror (errno));
              if (stp->td)
                free (stp->td);
              stp->td = 0;
            }
            else
            {
              stp->td->td_id = stid;
            }
          }
        }
      } /* Done with LISTEN_THREAD handling */
      else if (stp->type == MSEEDSCAN_THREAD)
      {
        MSScanInfo *mssinfo = stp->params;

        /* Cleanup CLOSED scanning thread */
        if (stp->td && stp->td->td_flags & TDF_CLOSED)
        {
          lprintf (1, "Joining CLOSED %s thread", threadtype);

          if ((errno = pthread_join (stp->td->td_id, NULL)))
          {
            lprintf (0, "Error joining CLOSED %s thread %lu: %s", threadtype,
                     (unsigned long int)stp->td->td_id, strerror (errno));
          }

          free (stp->td);
          stp->td = 0;
        }

        /* Start new thread if needed */
        if (stp->td == 0 && !shutdownsig)
        {
          mssinfo->ringparams = ringparams;

          /* Initialize thread data and create thread */
          if (!(stp->td = InitThreadData (mssinfo)))
          {
            lprintf (0, "Error initializing %s thread_data: %s", threadtype, strerror (errno));
          }
          else
          {
            lprintf (2, "Starting %s thread [%s]", threadtype, mssinfo->dirname);

            if ((errno = pthread_create (&stid, NULL, threadfunc, (void *)stp->td)))
            {
              lprintf (0, "Error creating %s thread: %s", threadtype, strerror (errno));
              if (stp->td)
                free (stp->td);
              stp->td = 0;
            }
            else
            {
              stp->td->td_id = stid;
            }
          }
        }
      } /* Done with MSEEDSCAN_THREAD handling */
      else
      {
        lprintf (0, "Error, unrecognized server thread type: %d", stp->type);
      }

    } /* Done looping through server threads */
    pthread_mutex_unlock (&sthreads_lock);

    /* Reset total count and byte rates */
    txpacketrate = txbyterate = rxpacketrate = rxbyterate = 0.0;

    /* Loop through client thread list printing status and doing cleanup */
    pthread_mutex_lock (&cthreads_lock);
    loopctp = cthreads;
    while (loopctp)
    {
      ctp = loopctp;
      loopctp = loopctp->next;

      sprintf (statusstr, "Client thread %lu Flags:", (unsigned long int)ctp->td->td_id);
      if (ctp->td->td_flags & TDF_SPAWNING)
        strcat (statusstr, " SPAWNING");
      if (ctp->td->td_flags & TDF_ACTIVE)
        strcat (statusstr, " ACTIVE");
      if (ctp->td->td_flags & TDF_CLOSE)
        strcat (statusstr, " CLOSE");
      if (ctp->td->td_flags & TDF_CLOSING)
        strcat (statusstr, " CLOSING");
      if (ctp->td->td_flags & TDF_CLOSED)
        strcat (statusstr, " CLOSED");
      lprintf (3, "%s", statusstr);

      /* Free associated resources and join CLOSED client threads */
      if (ctp->td->td_flags & TDF_CLOSED)
      {
        lprintf (3, "Removing client thread %lu from the cthreads list",
                 (unsigned long int)ctp->td->td_id);

        /* Unlink from the cthreads list */
        if (!ctp->prev && !ctp->next)
          cthreads = 0;
        if (!ctp->prev && ctp->next)
          cthreads = ctp->next;
        if (ctp->prev)
          ctp->prev->next = ctp->next;
        if (ctp->next)
          ctp->next->prev = ctp->prev;

        if ((errno = pthread_join (ctp->td->td_id, NULL)))
        {
          lprintf (0, "Error joining CLOSED thread %lu: %s",
                   (unsigned long int)ctp->td->td_id, strerror (errno));
        }

        /* Free the ClientInfo structure stored at the prvtptr */
        if (ctp->td->td_prvtptr)
          free (ctp->td->td_prvtptr);

        /* Free thread data structure */
        if (ctp->td)
          free (ctp->td);

        /* Decrement client count */
        if (clientcount > 0)
          clientcount--;

        /* Free thread data */
        free (ctp);
      }
      else
      {
        /* Update transmission and reception rates */
        CalcStats ((ClientInfo *)ctp->td->td_prvtptr);

        /* Update packet and byte count totals */
        txpacketrate += ((ClientInfo *)ctp->td->td_prvtptr)->txpacketrate;
        txbyterate += ((ClientInfo *)ctp->td->td_prvtptr)->txbyterate;
        rxpacketrate += ((ClientInfo *)ctp->td->td_prvtptr)->rxpacketrate;
        rxbyterate += ((ClientInfo *)ctp->td->td_prvtptr)->rxbyterate;

        /* Write transfer logs and reset byte counts */
        if (tlogwrite)
          WriteTLog ((ClientInfo *)ctp->td->td_prvtptr, 1);

        /* Close idle clients if limit is set and exceeded */
        if (clienttimeout && (hpcurtime - ((ClientInfo *)ctp->td->td_prvtptr)->lastxchange) > (clienttimeout * HPTMODULUS))
        {
          pthread_mutex_lock (&(ctp->td->td_lock));
          if (!(ctp->td->td_flags & TDF_CLOSE) &&
              !(ctp->td->td_flags & TDF_CLOSING) &&
              !(ctp->td->td_flags & TDF_CLOSED))
          {
            lprintf (1, "Closing idle client connection: %s", ((ClientInfo *)ctp->td->td_prvtptr)->hostname);
            ctp->td->td_flags = TDF_CLOSE;
          }
          pthread_mutex_unlock (&(ctp->td->td_lock));
        }
      }
    } /* Done looping through client threads */
    pthread_mutex_unlock (&cthreads_lock);

    lprintf (3, "Client connections: %d", clientcount);

    /* Update count and byte rate ring parameters */
    ringparams->txpacketrate = txpacketrate;
    ringparams->txbyterate = txbyterate;
    ringparams->rxpacketrate = rxpacketrate;
    ringparams->rxbyterate = rxbyterate;

    /* Check for config file updates */
    if (configfile && !lstat (configfile, &cfstat))
    {
      if (cfstat.st_mtime > configfilemtime)
      {
        lprintf (1, "Re-reading configuration parameters from %s", configfile);
        ReadConfigFile (configfile, 1, cfstat.st_mtime);
        configreset = 1;
      }
    }

    /* Reset transfer log writing time windows using the current time as the reference */
    if (TLogParams.tlogbasedir && !shutdownsig && (tlogwrite || configreset))
    {
      tlogwrite = 0;

      if (CalcIntWin (time (NULL), TLogParams.tloginterval,
                      &TLogParams.tlogstartint, &TLogParams.tlogendint))
      {
        lprintf (0, "Error calculating interval time window");
        return 1;
      }
    }

    /* All done if shutting down and no threads left */
    if (shutdownsig >= 2 && clientcount == 0 && servercount == 0)
      break;

    /* Throttle the loop during shutdown */
    if (shutdownsig)
    {
      nanosleep (&timereq, NULL);
    }
    /* Otherwise, throttle the loop for a second, signals will interrupt */
    else
    {
      while (((curtime = time (NULL)) - chktime) < 1 && !shutdownsig)
        nanosleep (&timereq, NULL);
    }

    configreset = 0;
    chktime = curtime;
  } /* End of main watchdog loop */

  /* Shutdown ring buffer */
  if (ringdir || volatilering)
  {
    if (RingShutdown (ringfd, streamfilename, ringparams))
    {
      lprintf (0, "Error shutting down ring buffer");
      return 1;
    }
  }

  /* Cancel and re-joing the signal handling thread */
  if ((errno = pthread_cancel (sigtid)))
  {
    lprintf (0, "Error cancelling signal handling thread: %s", strerror (errno));
    return 1;
  }

  if ((errno = pthread_join (sigtid, NULL)))
  {
    lprintf (0, "Error joining signal handling thread: %s", strerror (errno));
    return 1;
  }

  return 0;
} /* End of main() */

/***********************************************************************
 * InitThreadData:
 *
 * Initialize thread data.
 *
 * Return pointer to thread_data on success and 0 on error.
 ***********************************************************************/
struct thread_data *
InitThreadData (void *prvtptr)
{
  struct thread_data *rtdp;

  rtdp = (struct thread_data *)malloc (sizeof (struct thread_data));

  if (!rtdp)
  {
    lprintf (0, "Error malloc'ing thread_data: %s", strerror (errno));
    return 0;
  }

  if (pthread_mutex_init (&(rtdp->td_lock), NULL))
  {
    lprintf (0, "Error initializing thread_data mutex: %s", strerror (errno));
    return 0;
  }

  rtdp->td_id = 0;
  rtdp->td_flags = TDF_SPAWNING;

  rtdp->td_prvtptr = prvtptr;

  return rtdp;
} /* End of InitThreadData() */

/***********************************************************************
 * ListenThread:
 *
 * Thread to accept connections and dispatch client threads.
 *
 * Return NULL.
 ***********************************************************************/
void *
ListenThread (void *arg)
{
  pthread_t ctid;
  int clientsocket;
  struct thread_data *mytdp;
  struct thread_data *tdp;
  struct cthread *ctp;
  ClientInfo *cinfo;
  ListenPortParams *lpp;

  char ipstr[100];
  char portstr[32];
  char protocolstr[100];

  struct sockaddr_storage addr_storage;
  struct sockaddr *paddr = (struct sockaddr *)&addr_storage;
  socklen_t addrlen = sizeof (addr_storage);
  int one = 1;

  mytdp = (struct thread_data *)arg;
  lpp = (ListenPortParams *)mytdp->td_prvtptr;

  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));

  /* Generate string of protocols supported by this listener */
  if (GenProtocolString (lpp->protocols, protocolstr, sizeof (protocolstr)) > 0)
    lprintf (1, "Listening for connections on port %s (%s)",
             lpp->portstr, protocolstr);
  else
    lprintf (1, "Listening for connections on port %s (unknown protocols?)",
             lpp->portstr);

  /* Enter connection dispatch loop, spawning a new thread for each incoming connection */
  while (!shutdownsig)
  {
    /* Process next connection in queue */
    clientsocket = accept (lpp->socket, paddr, &addrlen);

    /* Check for accept errors */
    if (clientsocket == -1)
    {
      /* Continue listening on these non-fatal errors */
      if (errno == ECONNABORTED || errno == EINTR)
        continue;

      /* If not shutting down this is a connection error */
      if (!shutdownsig)
        lprintf (0, "Could not accept incoming connection: %s", strerror (errno));

      break;
    }

    /* Turn off TCP delay algorithm (Nagle) */
    if (setsockopt (clientsocket, tcpprotonumber, TCP_NODELAY, (void *)&one, sizeof (one)))
    {
      lprintf (0, "Could not disable TCP delay algorithm: %s", strerror (errno));
    }

    /* Generate IP address and port number strings */
    if (getnameinfo (paddr, addrlen, ipstr, sizeof (ipstr), portstr, sizeof (portstr),
                     NI_NUMERICHOST | NI_NUMERICSERV))
    {
      lprintf (0, "Error creating IP and port strings");
      close (clientsocket);
      continue;
    }

    lprintf (2, "Incoming connection: %s port %s", ipstr, portstr);

    /* Reject clients not in matching list */
    if (matchips)
    {
      if (!MatchIP (matchips, paddr))
      {
        lprintf (1, "Rejecting non-matching connection from: %s:%s", ipstr, portstr);
        close (clientsocket);
        continue;
      }
    }

    /* Reject clients in the rejection list */
    if (rejectips)
    {
      if (MatchIP (rejectips, paddr))
      {
        lprintf (1, "Rejecting connection from: %s:%s", ipstr, portstr);
        close (clientsocket);
        continue;
      }
    }

    /* Enforce per-address connection limit for non write permission addresses */
    if (maxclientsperip)
    {
      if (!(writeips && MatchIP (writeips, paddr)))
      {
        if (ClientIPCount (paddr) >= maxclientsperip)
        {
          lprintf (1, "Too many connections from: %s:%s", ipstr, portstr);
          close (clientsocket);
          continue;
        }
      }
    }

    /* Enforce maximum number of clients if specified */
    if (maxclients && clientcount >= maxclients)
    {
      if ((writeips && MatchIP (writeips, paddr)) && clientcount <= (maxclients + RESERVECONNECTIONS))
      {
        lprintf (1, "Allowing connection in reserve space from %s:%s", ipstr, portstr);
      }
      else
      {
        lprintf (1, "Maximum number of clients exceeded: %d", maxclients);
        lprintf (1, "  Rejecting connection from: %s:%s", ipstr, portstr);
        close (clientsocket);
        continue;
      }
    }

    /* Allocate and initialize connection info struct */
    if ((cinfo = (ClientInfo *)calloc (1, sizeof (ClientInfo))) == NULL)
    {
      lprintf (0, "Error allocating memory for connection info");
      close (clientsocket);
      break;
    }

    cinfo->socket = clientsocket;
    cinfo->protocols = lpp->protocols;
    cinfo->type = CLIENT_UNDETERMINED;
    cinfo->ringparams = ringparams;

    /* Store client socket address structure */
    if ((cinfo->addr = (struct sockaddr *)malloc (addrlen)) == NULL)
    {
      lprintf (0, "Error allocating memory for socket structure");
      close (clientsocket);
      break;
    }
    memcpy (cinfo->addr, &addr_storage, addrlen);
    cinfo->addrlen = addrlen;

    /* Store IP address and port number strings */
    strncpy (cinfo->ipstr, ipstr, sizeof (cinfo->ipstr));
    cinfo->ipstr[sizeof (cinfo->ipstr) - 1] = '\0';
    strncpy (cinfo->portstr, portstr, sizeof (cinfo->portstr));
    cinfo->portstr[sizeof (cinfo->portstr) - 1] = '\0';

    /* Set initial client ID string */
    strncpy (cinfo->clientid, "Client", sizeof (cinfo->clientid));

    /* Set stream limit if specified for address */
    if (limitips)
    {
      IPNet *ipnet;

      if ((ipnet = MatchIP (limitips, paddr)))
      {
        cinfo->limitstr = ipnet->limitstr;
      }
    }

    /* Grant write permission if address is in the write list */
    if (writeips)
    {
      if (MatchIP (writeips, paddr))
      {
        cinfo->writeperm = 1;
      }
    }

    /* Set trusted flag if address is in the trusted list */
    if (trustedips)
    {
      if (MatchIP (trustedips, paddr))
      {
        cinfo->trusted = 1;
      }
    }

    /* Set configured fixed HTTP headers */
    cinfo->httpheaders = httpheaders;

    /* Set time window search limit */
    cinfo->timewinlimit = timewinlimit;

    /* Set client connect time */
    cinfo->conntime = HPnow ();

    /* Set last data exchange time to the connect time */
    cinfo->lastxchange = cinfo->conntime;

    /* Initialize streams lock */
    pthread_mutex_init (&cinfo->streams_lock, NULL);

    /* Initialize the miniSEED write parameters */
    if (mseedarchive)
    {
      if (!(cinfo->mswrite = (DataStream *)malloc (sizeof (DataStream))))
      {
        lprintf (0, "Error allocating memory for miniSEED write parameters");
        if (clientsocket)
          close (clientsocket);
        break;
      }

      cinfo->mswrite->path = mseedarchive;
      cinfo->mswrite->idletimeout = mseedidleto;
      cinfo->mswrite->maxopenfiles = 50;
      cinfo->mswrite->openfilecount = 0;
      cinfo->mswrite->grouproot = NULL;
    }

    /* Create new client thread */
    if (!(tdp = InitThreadData (cinfo)))
    {
      lprintf (0, "Error initializing thread_data: %s", strerror (errno));
      if (clientsocket)
        close (clientsocket);
      break;
    }

    if ((errno = pthread_create (&ctid, NULL, ClientThread, (void *)tdp)))
    {
      lprintf (0, "Error creating new client thread: %s", strerror (errno));
      if (clientsocket)
        close (clientsocket);
      if (tdp)
        free (tdp);
      tdp = 0;
      continue;
    }
    else
    {
      /* Update thread id, no locking, should be safe */
      tdp->td_id = ctid;

      ctp = (struct cthread *)malloc (sizeof (struct cthread));
      if (ctp == NULL)
      {
        lprintf (0, "Error malloc'ing cthread: %s", strerror (errno));
        if (clientsocket)
          close (clientsocket);
        if (tdp)
          free (tdp);
        break;
      }

      ctp->td = tdp;
      ctp->prev = 0;

      /* Add ctp to the beginning of the client threads list (cthreads) */
      pthread_mutex_lock (&cthreads_lock);
      if (cthreads)
      {
        ctp->next = cthreads;
        cthreads->prev = ctp;
      }
      else
      {
        ctp->next = 0;
      }
      cthreads = ctp;
      pthread_mutex_unlock (&cthreads_lock);

      /* Increment client count */
      clientcount++;
    }
  }

  /* Set thread closing status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));

  lprintf (1, "Listening thread closing");

  return NULL;
} /* End of ListenThread() */

/***********************************************************************
 * InitServerSocket:
 *
 * Initialize a TCP server socket on the specified port bound to all
 * local addresses/interfaces.
 *
 * Return socket descriptor on success and -1 on error.
 ***********************************************************************/
static int
InitServerSocket (char *portstr, uint8_t protocols)
{
  struct addrinfo *addr;
  struct addrinfo hints;
  char *familystr = NULL;
  int fd;
  int optval;
  int gaierror;

  if (!portstr)
    return -1;

  memset (&hints, 0, sizeof (hints));

  /* AF_INET, or AF_INET6 for IPv4 or IPv6 */
  if (protocols & FAMILY_IPv4)
  {
    hints.ai_family = AF_INET;
    familystr = "IPv4";
  }
  else if (protocols & FAMILY_IPv6)
  {
    hints.ai_family = AF_INET6;
    familystr = "IPv6";
  }
  else
  {
    hints.ai_family = AF_UNSPEC;
    familystr = "IPvUnspecified";
  }

  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if ((gaierror = getaddrinfo (NULL, portstr, &hints, &addr)))
  {
    lprintf (0, "Error with getaddrinfo(), %s port %s: %s",
             familystr, portstr, gai_strerror (gaierror));
    return -1;
  }

  /* Create a socket from first addrinfo entry */
  fd = socket (addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (fd < 0)
  {
    /* Print error only if not "unsupported" IPv6, as this is expected */
    if (!(addr->ai_family == AF_INET6 && errno == EAFNOSUPPORT))
      lprintf (0, "Error with socket(), %s port %s: %s",
               familystr, portstr, strerror (errno));
    return -1;
  }

  optval = 1;
  if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof (optval)))
  {
    lprintf (0, "Error setting SO_REUSEADDR with setsockopt(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  /* Limit IPv6 sockets to IPv6 only, avoid mapped addresses, we handle IPv4 separately */
  if (addr->ai_family == AF_INET6 &&
      setsockopt (fd, IPPROTO_IPV6, IPV6_V6ONLY, &optval, sizeof (optval)))
  {
    lprintf (0, "Error setting IPV6_V6ONLY with setsockopt(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  if (bind (fd, addr->ai_addr, addr->ai_addrlen) < 0)
  {
    lprintf (0, "Error with bind(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  if (listen (fd, 10) == -1)
  {
    lprintf (0, "Error with listen(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  freeaddrinfo (addr);

  return fd;
} /* End of InitServerSocket() */

/***************************************************************************
 * ProcessParam:
 *
 * Process the command line parameters.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
ProcessParam (int argcount, char **argvec)
{
  ListenPortParams lpp;
  int optind;

  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
  {
    if (strcmp (argvec[optind], "-V") == 0)
    {
      fprintf (stderr, "%s version: %s\n", PACKAGE, VERSION);
      exit (0);
    }
    else if (strcmp (argvec[optind], "-h") == 0)
    {
      Usage (0);
    }
    else if (strcmp (argvec[optind], "-H") == 0)
    {
      Usage (1);
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      verbose += strspn (&argvec[optind][1], "v");
    }
    else if (strcmp (argvec[optind], "-I") == 0)
    {
      serverid = strdup (GetOptVal (argcount, argvec, optind++));
    }
    else if (strcmp (argvec[optind], "-m") == 0)
    {
      maxclients = strtoul (GetOptVal (argcount, argvec, optind++), NULL, 10);
    }
    else if (strcmp (argvec[optind], "-M") == 0)
    {
      maxclientsperip = strtoul (GetOptVal (argcount, argvec, optind++), NULL, 10);
    }
    else if (strcmp (argvec[optind], "-Rd") == 0)
    {
      ringdir = GetOptVal (argcount, argvec, optind++);
    }
    else if (strcmp (argvec[optind], "-Rs") == 0)
    {
      ringsize = CalcSize (GetOptVal (argcount, argvec, optind++));
    }
    else if (strcmp (argvec[optind], "-Rm") == 0)
    {
      maxpktid = strtoull (GetOptVal (argcount, argvec, optind++), NULL, 10);
    }
    else if (strcmp (argvec[optind], "-Rp") == 0)
    {
      pktsize = sizeof (RingPacket) + strtoul (GetOptVal (argcount, argvec, optind++), NULL, 10);
    }
    else if (strcmp (argvec[optind], "-NOMM") == 0)
    {
      memorymapring = 0;
    }
    else if (strcmp (argvec[optind], "-L") == 0)
    {
      strncpy (lpp.portstr, GetOptVal (argcount, argvec, optind++), sizeof (lpp.portstr));
      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_ALL;
      lpp.socket = -1;

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding listening threads for all protocols");
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-DL") == 0)
    {
      strncpy (lpp.portstr, GetOptVal (argcount, argvec, optind++), sizeof (lpp.portstr));
      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_DATALINK;
      lpp.socket = -1;

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding listening threads for DataLink");
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-SL") == 0)
    {
      strncpy (lpp.portstr, GetOptVal (argcount, argvec, optind++), sizeof (lpp.portstr));
      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_SEEDLINK;
      lpp.socket = -1;

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding listening threads for SeedLink");
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-T") == 0)
    {
      TLogParams.tlogbasedir = GetOptVal (argcount, argvec, optind++);
    }
    else if (strcmp (argvec[optind], "-Ti") == 0)
    {
      /* Specified in hours, the actual variable should be in seconds */
      TLogParams.tloginterval = strtod (GetOptVal (argcount, argvec, optind++), NULL) * 3600;
    }
    else if (strcmp (argvec[optind], "-Tp") == 0)
    {
      TLogParams.tlogprefix = GetOptVal (argcount, argvec, optind++);
    }
    else if (strcmp (argvec[optind], "-STDERR") == 0)
    {
      /* Redirect all output destined for stdout to stderr */
      if (dup2 (fileno (stderr), fileno (stdout)) < 0)
      {
        lprintf (0, "Error redirecting stdout to stderr: %s", strerror (errno));
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-MSWRITE") == 0)
    {
      ConfigMSWrite (GetOptVal (argcount, argvec, optind++));
    }
    else if (strcmp (argvec[optind], "-MSSCAN") == 0)
    {
      if (AddMSeedScanThread (GetOptVal (argcount, argvec, optind++)))
      {
        lprintf (0, "Error with -MSSCAN option");
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-VOLATILE") == 0)
    {
      volatilering = 1;
    }
    else if (strncmp (argvec[optind], "-", 1) == 0)
    {
      lprintf (0, "Unknown option: %s", argvec[optind]);
      exit (1);
    }
    else
    {
      if (configfile)
      {
        lprintf (0, "Unknown option: %s", argvec[optind]);
        exit (1);
      }
      else
      {
        configfile = argvec[optind];

        lprintf (1, "Reading configuration from %s", configfile);

        /* Process the config file */
        if (ReadConfigFile (configfile, 0, 0))
        {
          lprintf (0, "Error reading config file");
          exit (1);
        }
      }
    }
  }

  /* Report the program version */
  lprintf (0, "%s version: %s", PACKAGE, VERSION);

  /* Set default server ID if not already set */
  if (!serverid)
  {
    serverid = strdup ("Ring Server");
  }

  /* Add localhost (loopback) to write permission list if list empty */
  if (!writeips)
  {
    if (AddIPNet (&writeips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to write permission list");
      return -1;
    }
  }

  /* Add localhost (loopback) to trusted list if list empty */
  if (!trustedips)
  {
    if (AddIPNet (&trustedips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to trusted list");
      return -1;
    }
  }

  /* Check for specified ring directory */
  if (!ringsize)
  {
    lprintf (0, "Error, ring buffer size not valid: %" PRIu64, ringsize);
    exit (1);
  }

  /* Check for specified ring directory */
  if (!ringdir && !volatilering)
  {
    lprintf (0, "Error, ring directory must be specified");
    exit (1);
  }

  /* Check transfer log directory */
  if (TLogParams.tlogbasedir)
  {
    if (access (TLogParams.tlogbasedir, W_OK))
    {
      lprintf (0, "WARNING, cannot write to transfer log directory: %s",
               TLogParams.tlogbasedir);
    }
  }

  return 0;
} /* End of ProcessParam() */

/***************************************************************************
 * GetOptVal:
 * Return the value to a command line option; checking that the value is
 * itself not an option (starting with '-') and is not past the end of
 * the argument list.
 *
 * argcount: total arguments in argvec
 * argvec: argument list
 * argopt: index of option to process, value is expected to be at argopt+1
 *
 * Returns value on success and exits with error message on failure
 ***************************************************************************/
static char *
GetOptVal (int argcount, char **argvec, int argopt)
{
  if (argvec == NULL || argvec[argopt] == NULL)
  {
    lprintf (0, "GetOptVal(): NULL option requested");
    exit (1);
  }

  if ((argopt + 1) < argcount && *argvec[argopt + 1] != '-')
    return argvec[argopt + 1];

  lprintf (0, "Option %s requires a value", argvec[argopt]);
  exit (1);
} /* End of GetOptVal() */

/***************************************************************************
 * ReadConfigFile:
 *
 * Reads the ringserver configuration from a file containing simple
 * key-value pairs.  If the dynamiconly argument is true only
 * "dynamic" parameters will be read from the file.
 *
 * Recognized parameters ("D" labeled parameters are dynamic):
 *
 * RingDirectory <dir>
 * RingSize <size>
 * MaxPacketID <id>
 * MaxPacketSize <size>
 * MemoryMapRing <1|0>
 * AutoRecovery <2|1|0>
 * ListenPort <port>
 * SeedLinkPort <port>
 * DataLinkPort <port>
 * [D] ServerID <server id>
 * [D] Verbosity <level>
 * [D] MaxClientsPerIP <max>
 * [D] MaxClients <max>
 * [D] ClientTimeout <timeout>
 * [D] ResolveHostnames <1|0>
 * [D] TimeWindowLimit <percent>
 * [D] TransferLogDirectory <dir>
 * [D] TransferLogInterval <interval>
 * [D] TransferLogPrefix <prefix>
 * [D] TransferLogTX <1|0>
 * [D] TransferLogRX <1|0>
 * [D] WriteIP <IP>[/netmask]
 * [D] LimitIP <IP>[/netmask] <streamlimit>
 * [D] MatchIP <IP[/netmask]
 * [D] RejectIP <IP[/netmask]
 * [D] WebRoot <web content root>
 * [D] HTTPHeader <HTTP header>
 * [D] MSeedWrite <format>
 * MSeedScan <directory>
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
ReadConfigFile (char *configfile, int dynamiconly, time_t mtime)
{
  FILE *cfile;
  char line[200];
  char *ptr, *chr;
  struct stat cfstat;
  IPNet *ipnet = 0;
  IPNet *nextipnet = 0;
  ListenPortParams lpp;

  char svalue[512];
  float fvalue;
  unsigned int uvalue;
  unsigned long long int lluvalue;

  if (!configfile)
    return -1;

  /* Open config file */
  if ((cfile = fopen (configfile, "r")) == NULL)
  {
    lprintf (0, "Error opening config file %s: %s",
             configfile, strerror (errno));
    return -1;
  }

  /* If no mtime was supplied, stat the file to get it */
  if (!mtime)
  {
    if (fstat (fileno (cfile), &cfstat))
    {
      lprintf (0, "Error stating config file %s: %s",
               configfile, strerror (errno));
      return -1;
    }

    mtime = cfstat.st_mtime;
  }

  /* Reset the configuration file mtime */
  configfilemtime = mtime;

  /* Clear the write, trusted, limit, match and reject IPs lists */
  ipnet = nextipnet = writeips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  writeips = NULL;
  ipnet = nextipnet = trustedips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  trustedips = NULL;
  ipnet = nextipnet = limitips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    if (ipnet->limitstr)
      free (ipnet->limitstr);
    free (ipnet);
    ipnet = nextipnet;
  }
  limitips = NULL;
  ipnet = nextipnet = matchips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  matchips = NULL;
  ipnet = nextipnet = rejectips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  rejectips = NULL;

  /* Clear existing HTTP headers */
  if (httpheaders)
  {
    free (httpheaders);
    httpheaders = NULL;
  }

  /* Read and process all lines */
  while (fgets (line, sizeof (line), cfile))
  {
    ptr = line;

    /* Skip prefixed white-space */
    while (isspace ((int)(*ptr)))
      ptr++;

    /* Skip empty lines */
    if (*ptr == '\0')
      continue;

    /* Skip comment lines */
    if (*ptr == '#' || *ptr == '*')
      continue;

    /* Truncate at newline or carriage return */
    if ((chr = strchr (ptr, '\n')))
      *chr = '\0';
    if ((chr = strchr (ptr, '\r')))
      *chr = '\0';

    /* Search for recognized parameters */
    if (!dynamiconly && !strncasecmp ("RingDirectory", ptr, 13))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with RingDirectory config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      ringdir = strdup (svalue);
    }
    else if (!dynamiconly && !strncasecmp ("RingSize", ptr, 8))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with RingSize config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      ringsize = CalcSize (svalue);
    }
    else if (!dynamiconly && !strncasecmp ("MaxPacketID", ptr, 11))
    {
      if (sscanf (ptr, "%*s %llu", &lluvalue) != 1)
      {
        lprintf (0, "Error with MaxPacketID config file line: %s", ptr);
        return -1;
      }

      maxpktid = lluvalue;
    }
    else if (!dynamiconly && !strncasecmp ("MaxPacketSize", ptr, 13))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with MaxPacketSize config file line: %s", ptr);
        return -1;
      }

      pktsize = sizeof (RingPacket) + uvalue;
    }
    else if (!dynamiconly && !strncasecmp ("AutoRecovery", ptr, 12))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with AutoRecovery config file line: %s", ptr);
        return -1;
      }

      if (uvalue != 0 && uvalue != 1 && uvalue != 2)
      {
        lprintf (0, "Invalid AutoRecovery config file value: %u", uvalue);
        return -1;
      }

      autorecovery = uvalue;
    }
    else if (!dynamiconly && !strncasecmp ("MemoryMapRing", ptr, 13))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with MemoryMapRing config file line: %s", ptr);
        return -1;
      }

      memorymapring = (uvalue) ? 1 : 0;
    }
    else if (!dynamiconly && !strncasecmp ("ListenPort", ptr, 10))
    {
      int rv;

      rv = sscanf (ptr, "%*s %10s %512[^\r\n]", lpp.portstr, svalue);

      if (rv != 1 && rv != 2)
      {
        lprintf (0, "Error with ListenPort config file line: %s", ptr);
        return -1;
      }

      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_ALL;
      lpp.socket = -1;

      /* Parse optional protocol flags to limit allowed protocols */
      if (rv == 2)
      {
        lpp.protocols = 0;

        if (strcasestr (svalue, "DataLink"))
          lpp.protocols |= PROTO_DATALINK;
        if (strcasestr (svalue, "SeedLink"))
          lpp.protocols |= PROTO_SEEDLINK;
        if (strcasestr (svalue, "HTTP"))
          lpp.protocols |= PROTO_HTTP;

        if (lpp.protocols == 0)
          lpp.protocols = PROTO_ALL;

        if (strcasestr (svalue, "IPv4"))
          lpp.protocols |= FAMILY_IPv4;
        if (strcasestr (svalue, "IPv6"))
          lpp.protocols |= FAMILY_IPv6;
      }

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding server thread for ListenPort config file line: %s", ptr);
        return -1;
      }
    }
    else if (!dynamiconly && !strncasecmp ("DataLinkPort", ptr, 12))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with DataLinkPort config file line: %s", ptr);
        return -1;
      }

      strncpy (lpp.portstr, svalue, sizeof (lpp.portstr));
      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_DATALINK;
      lpp.socket = -1;

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding server thread for DataLinkPort config file line: %s", ptr);
        return -1;
      }
    }
    else if (!dynamiconly && !strncasecmp ("SeedLinkPort", ptr, 12))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with SeedLinkPort config file line: %s", ptr);
        return -1;
      }

      strncpy (lpp.portstr, svalue, sizeof (lpp.portstr));
      lpp.portstr[sizeof (lpp.portstr) - 1] = '\0';
      lpp.protocols = PROTO_SEEDLINK;
      lpp.socket = -1;

      if (!AddListenThreads (&lpp))
      {
        lprintf (0, "Error adding server thread for SeedLinkPort config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("ServerID", ptr, 8))
    {
      char *value;
      char *tptr;
      char dchar;

      if (strlen (ptr) < 10)
      {
        lprintf (0, "Error with ServerID config file line: %s", ptr);
        return -1;
      }

      /* Find beginning of non-white-space value */
      value = ptr + 8;
      while (isspace ((int)*value))
        value++;

      /* If single or double quotes are detected eliminate them */
      if (*value == '"' || *value == '\'')
      {
        dchar = *value;
        value++;

        if ((tptr = strchr (value, dchar)))
        {
          /* Truncate string at matching quote */
          *tptr = '\0';
        }
        else
        {
          lprintf (0, "Mismatching quotes for ServerID config file line: %s", ptr);
          return -1;
        }
      }

      /* Free existing server ID if any */
      if (serverid)
        free (serverid);

      serverid = strdup (value);
    }
    else if (!strncasecmp ("Verbosity", ptr, 9))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with Verbosity config file line: %s", ptr);
        return -1;
      }

      verbose = uvalue;
    }
    else if (!strncasecmp ("MaxClientsPerIP", ptr, 15))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with MaxClientsPerIP config file line: %s", ptr);
        return -1;
      }

      maxclientsperip = uvalue;
    }
    else if (!strncasecmp ("MaxClients", ptr, 10))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with MaxClients config file line: %s", ptr);
        return -1;
      }

      maxclients = uvalue;
    }
    else if (!strncasecmp ("ClientTimeout", ptr, 13))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with ClientTimeout config file line: %s", ptr);
        return -1;
      }

      clienttimeout = uvalue;
    }
    else if (!strncasecmp ("ResolveHostnames", ptr, 16))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with ResolveHostnames config file line: %s", ptr);
        return -1;
      }

      resolvehosts = (uvalue) ? 1 : 0;
    }
    else if (!strncasecmp ("TimeWindowLimit", ptr, 15))
    {
      if (sscanf (ptr, "%*s %f", &fvalue) != 1)
      {
        lprintf (0, "Error with TimeWindowLimit config file line: %s", ptr);
        return -1;
      }

      timewinlimit = fvalue / 100.0;
    }
    else if (!strncasecmp ("TransferLogDirectory", ptr, 20))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with TransferLogDirectory config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      TLogParams.tlogbasedir = strdup (svalue);
    }
    else if (!strncasecmp ("TransferLogInterval", ptr, 19))
    {
      if (sscanf (ptr, "%*s %f", &fvalue) != 1)
      {
        lprintf (0, "Error with TransferLogInterval config file line: %s", ptr);
        return -1;
      }

      /* Parameter is specified in hours but value needs to be seconds */
      TLogParams.tloginterval = fvalue * 3600.0;
    }
    else if (!strncasecmp ("TransferLogPrefix", ptr, 17))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with TransferLogPrefix config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      TLogParams.tlogprefix = strdup (svalue);
    }
    else if (!strncasecmp ("TransferLogTX", ptr, 13))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with TransferLogTX config file line: %s", ptr);
        return -1;
      }

      TLogParams.txlog = (uvalue) ? 1 : 0;
    }
    else if (!strncasecmp ("TransferLogRX", ptr, 13))
    {
      if (sscanf (ptr, "%*s %u", &uvalue) != 1)
      {
        lprintf (0, "Error with TransferLogRX config file line: %s", ptr);
        return -1;
      }

      TLogParams.rxlog = (uvalue) ? 1 : 0;
    }
    else if (!strncasecmp ("WriteIP", ptr, 7))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with WriteIP config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      if (AddIPNet (&writeips, svalue, NULL))
      {
        lprintf (0, "Error with WriteIP config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("TrustedIP", ptr, 9))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with TrustedIP config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      if (AddIPNet (&trustedips, svalue, NULL))
      {
        lprintf (0, "Error with TrustedIP config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("LimitIP", ptr, 7))
    {
      char limitstr[512];

      limitstr[0] = '\0';
      if (sscanf (ptr, "%*s %512s %512s", svalue, limitstr) != 2)
      {
        lprintf (0, "Error with LimitIP config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';
      limitstr[sizeof (limitstr) - 1] = '\0';

      if (AddIPNet (&limitips, svalue, limitstr))
      {
        lprintf (0, "Error with LimitIP config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("MatchIP", ptr, 7))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with MatchIP config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      if (AddIPNet (&matchips, svalue, NULL))
      {
        lprintf (0, "Error with MatchIP config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("RejectIP", ptr, 8))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with RejectIP config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      if (AddIPNet (&rejectips, svalue, NULL))
      {
        lprintf (0, "Error with RejectIP config file line: %s", ptr);
        return -1;
      }
    }
    else if (!strncasecmp ("WebRoot", ptr, 7))
    {
      char *value;
      char *tptr;
      char dchar;

      if (strlen (ptr) < 9)
      {
        lprintf (0, "Error with WebRoot config file line: %s", ptr);
        return -1;
      }

      /* Find beginning of non-white-space value */
      value = ptr + 8;
      while (isspace ((int)*value))
        value++;

      /* If single or double quotes are detected eliminate them */
      if (*value == '"' || *value == '\'')
      {
        dchar = *value;
        value++;

        if ((tptr = strchr (value, dchar)))
        {
          /* Truncate string at matching quote */
          *tptr = '\0';
        }
        else
        {
          lprintf (0, "Mismatching quotes for WebRoot config file line: %s", ptr);
          return -1;
        }
      }

      if (webroot)
        free (webroot);

      webroot = realpath (value, NULL);

      if (webroot == NULL)
      {
        lprintf (0, "Error with WebRoot value: %s", value);
        return -1;
      }
    }
    else if (!strncasecmp ("HTTPHeader", ptr, 10))
    {
      char *value;
      char *tptr;
      char dchar;

      if (strlen (ptr) < 12)
      {
        lprintf (0, "Error with HTTPHeader config file line: %s", ptr);
        return -1;
      }

      /* Find beginning of non-white-space value */
      value = ptr + 10;
      while (isspace ((int)*value))
        value++;

      /* If single or double quotes are detected eliminate them */
      if (*value == '"' || *value == '\'')
      {
        dchar = *value;
        value++;

        if ((tptr = strchr (value, dchar)))
        {
          /* Truncate string at matching quote */
          *tptr = '\0';
        }
        else
        {
          lprintf (0, "Mismatching quotes for HTTPHeader config file line: %s", ptr);
          return -1;
        }
      }

      /* Append multiple headers to composite string */
      if (asprintf (&tptr, "%s%s\r\n", (httpheaders) ? httpheaders : "", value) == -1)
      {
        lprintf (0, "Error allocating memory");
        return -1;
      }

      if (httpheaders)
        free (httpheaders);
      httpheaders = tptr;
    }
    else if (!strncasecmp ("MSeedWrite", ptr, 10))
    {
      if (sscanf (ptr, "%*s %512s", svalue) != 1)
      {
        lprintf (0, "Error with MSeedWrite config file line: %s", ptr);
        return -1;
      }
      svalue[sizeof (svalue) - 1] = '\0';

      ConfigMSWrite (svalue);
    }
    else if (!dynamiconly && !strncasecmp ("MSeedScan", ptr, 9))
    {
      if (sscanf (ptr, "%*s %512[^\n]", svalue) != 1)
      {
        lprintf (0, "Error with MSeedScan config file line: %s", ptr);
        return -1;
      }

      if (AddMSeedScanThread (svalue))
      {
        lprintf (0, "Error with MSeedScan config file line: %s", ptr);
        return -1;
      }
    }
  } /* Done reading config file lines */

  /* Add localhost (loopback) to write permission list if list empty */
  if (!writeips)
  {
    if (AddIPNet (&writeips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to write permission list");
      return -1;
    }
  }

  /* Add localhost (loopback) to trusted list if list empty */
  if (!trustedips)
  {
    if (AddIPNet (&trustedips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to trusted list");
      return -1;
    }
  }

  /* Close config file */
  if (fclose (cfile))
  {
    lprintf (0, "Error closing config file %s: %s",
             configfile, strerror (errno));
    return -1;
  }

  return 0;
} /* End of ReadConfigFile() */

/***************************************************************************
 * ConfigMSWrite:
 *
 * Configure miniSEED writing parameters for a given archive definition.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
ConfigMSWrite (char *value)
{
  char archive[513];
  char *layout = 0;
  char *path = 0;

  /* Parse layout specification if present */
  if ((path = strchr (value, '@')))
  {
    *path++ = '\0';

    if (!strcmp (value, "BUD"))
    {
      layout = BUDLAYOUT;
    }
    else if (!strcmp (value, "CHAN"))
    {
      layout = CHANLAYOUT;
    }
    else if (!strcmp (value, "QCHAN"))
    {
      layout = QCHANLAYOUT;
    }
    else if (!strcmp (value, "CDAY"))
    {
      layout = CDAYLAYOUT;
    }
    else if (!strcmp (value, "SDAY"))
    {
      layout = SDAYLAYOUT;
    }
    else if (!strcmp (value, "HSDAY"))
    {
      layout = HSDAYLAYOUT;
    }
  }

  /* Free any existing data stream archive definition */
  if (mseedarchive)
  {
    free (mseedarchive);
  }

  /* Set new data stream archive definition */
  if (path && layout)
  {
    snprintf (archive, sizeof (archive), "%s/%s", path, layout);
    mseedarchive = strdup (archive);
  }
  else
  {
    mseedarchive = strdup (value);
  }

  return 0;
} /* End of ConfigMSWrite() */

/***************************************************************************
 * AddListenThreads:
 *
 * Add listen threads to the server thread list and initializing the
 * server listening sockets.
 *
 * A listening thread is created for each of the IPv4 and IPv6 network
 * protocol families, both by default.
 *
 * The params structure will be copied by AddServerThread().
 *
 * Returns number of listening threads on success and 0 on error.
 ***************************************************************************/
static int
AddListenThreads (ListenPortParams *lpp)
{
  uint8_t protocols;
  uint8_t families = 0;
  int threads = 0;

  if (!lpp)
    return 0;

  protocols = lpp->protocols;

  /* Split server protocols from network protocol families */
  if (protocols & FAMILY_IPv4)
  {
    families |= FAMILY_IPv4;
    protocols &= ~FAMILY_IPv4;
  }
  if (protocols & FAMILY_IPv6)
  {
    families |= FAMILY_IPv6;
    protocols &= ~FAMILY_IPv6;
  }

  /* Try to initialize listening for IPv4, if requested or default (neither family requested) */
  if (families == 0 || (families & FAMILY_IPv4))
  {
    lpp->protocols = protocols | FAMILY_IPv4;

    if ((lpp->socket = InitServerSocket (lpp->portstr, lpp->protocols)) > 0)
    {
      if (AddServerThread (LISTEN_THREAD, lpp))
      {
        return 0;
      }

      threads += 1;
    }
    /* If explicitly requested, an initialization error is a failure */
    else if (families & FAMILY_IPv4)
    {
      lprintf (0, "Error initializing IPv4 server listening socket for port %s", lpp->portstr);
      return 0;
    }
  }

  /* Try to initialize listening for IPv6, if requested or default (neither family requested) */
  if (families == 0 || (families & FAMILY_IPv6))
  {
    lpp->protocols = protocols | FAMILY_IPv6;

    if ((lpp->socket = InitServerSocket (lpp->portstr, lpp->protocols)) > 0)
    {
      if (AddServerThread (LISTEN_THREAD, lpp))
      {
        return 0;
      }

      threads += 1;
    }
    /* If explicitly requested, an initialization error is a failure */
    else if (families & FAMILY_IPv6)
    {
      lprintf (0, "Error initializing IPv6 server listening socket for port %s", lpp->portstr);
      return 0;
    }
  }

  lpp->protocols = protocols | families;

  return threads;
} /* End of AddListenThreads() */

/***************************************************************************
 * AddMSeedScanThread:
 *
 * Add a miniSEED scanner thread to the server thread list.  The
 * supplied configuration string should contain a base directory
 * followed by optional sub-parameters.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
AddMSeedScanThread (char *configstr)
{
  MSScanInfo mssinfo;
  char *kptr;
  char *vptr;
  char *sptr;
  int initcurrentstate = 0;

  /* Set miniSEED scanning defaults */
  memset (&mssinfo, 0, sizeof (MSScanInfo)); /* Init struct to zeros */
  mssinfo.maxrecur = -1;                     /* Maximum level of directory recursion, -1 is no limit */
  mssinfo.scansleep0 = 1;                    /* Sleep between scans interval when no records found */
  mssinfo.idledelay = 60;                    /* Check idle files every idledelay scans */
  mssinfo.idlesec = 7200;                    /* Files are idle if not modified for idlesec */
  mssinfo.throttlensec = 100;                /* Nanoseconds to sleep after reading each record */
  mssinfo.filemaxrecs = 100;                 /* Maximum records to read from each file per scan */
  mssinfo.stateint = 300;                    /* State saving interval in seconds */

  /* Skip initial whitespace */
  while (isspace ((int)(*configstr)))
    configstr++;

  /* Search for whitespace after initial string (directory) and truncate */
  vptr = configstr;
  while (*(vptr + 1) && !isspace ((int)(*vptr)))
    vptr++;
  if (isspace ((int)(*vptr)))
    *vptr++ = '\0';

  /* Initial portion of the config string is the directory to scan */
  strncpy (mssinfo.dirname, configstr, sizeof (mssinfo.dirname) - 1);

  /* Search for optional parameters */
  while (*vptr && (vptr = strchr (vptr, '=')))
  {
    /* Find key and value strings */
    kptr = vptr; /* Step back to find first non-space character */
    while (*kptr && kptr != configstr && !isspace ((int)(*kptr)))
      kptr--;
    kptr++;         /* This first non-space character should start the key */
    *vptr++ = '\0'; /* The value is directly after the equals */

    /* Truncate value string and track continuing config string */
    sptr = vptr;
    while (*(sptr + 1) && !isspace ((int)(*sptr)))
      sptr++;
    if (isspace ((int)(*sptr)))
      *sptr++ = '\0';

    if (!strncasecmp ("StateFile", kptr, 9)) /* State file name */
    {
      strncpy (mssinfo.statefile, vptr, sizeof (mssinfo.statefile) - 1);
    }
    else if (!strncasecmp ("Match", kptr, 5)) /* File name match */
    {
      strncpy (mssinfo.matchstr, vptr, sizeof (mssinfo.matchstr) - 1);
    }
    else if (!strncasecmp ("Reject", kptr, 6)) /* File name reject */
    {
      strncpy (mssinfo.rejectstr, vptr, sizeof (mssinfo.rejectstr) - 1);
    }
    else if (!strncasecmp ("InitCurrentState", kptr, 6)) /* Init current state flag */
    {
      if (*vptr == '1' || *vptr == 'Y' || *vptr == 'y')
        initcurrentstate = 1;
      else if (*vptr == '0' || *vptr == 'N' || *vptr == 'n')
        initcurrentstate = 0;
      else
      {
        lprintf (0, "Unrecognized InitCurrentState value: '%s'", vptr);
        return -1;
      }
    }
    else if (!strncasecmp ("MaxRecurse", kptr, 10)) /* Max recurion depth */
    {
      mssinfo.maxrecur = strtol (vptr, NULL, 10);
    }
    else
    {
      lprintf (0, "Unrecognized MSeedScan sub-option: '%s'", kptr);
      return -1;
    }

    vptr = sptr;
  }

  /* Perform InitCurrentState logic */
  if (initcurrentstate)
  {
    /* Set nextnew flag under two conditions:
     * 1) statefile is not specified
     * 2) statefile is specified but does not exist */

    if (!*(mssinfo.statefile) || (*(mssinfo.statefile) && access (mssinfo.statefile, F_OK)))
      mssinfo.nextnew = 1;
  }

  /* Add to server thread list */
  if (AddServerThread (MSEEDSCAN_THREAD, &mssinfo))
  {
    lprintf (0, "Error adding server thread for MSeedScan config file line: %s",
             configstr);
    return -1;
  }

  return 0;
} /* End of AddMSeedScanThread() */

/***************************************************************************
 * AddServerThread:
 *
 * Add a thread to the server thread list.  The structure type passed
 * in params is implied by the thread type.  The params structure will
 * be copied.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
AddServerThread (unsigned int type, void *params)
{
  struct sthread *stp;
  struct sthread *nstp;

  if (!(nstp = calloc (1, sizeof (struct sthread))))
  {
    lprintf (0, "Error allocating memory for server thread");
    return -1;
  }

  nstp->type = type;

  /* Copy thread parameters to new entry */
  if (type == LISTEN_THREAD)
  {
    if (!(nstp->params = malloc (sizeof (ListenPortParams))))
    {
      lprintf (0, "Error allocating memory for server parameters");
      return -1;
    }

    memcpy (nstp->params, params, sizeof (ListenPortParams));
  }
  else if (type == MSEEDSCAN_THREAD)
  {
    if (!(nstp->params = malloc (sizeof (MSScanInfo))))
    {
      lprintf (0, "Error allocating memory for MSeedScan parameters");
      return -1;
    }

    memcpy (nstp->params, params, sizeof (MSScanInfo));
  }
  else
  {
    lprintf (0, "Error, unrecognized server thread type: %d", type);
    return -1;
  }

  pthread_mutex_lock (&sthreads_lock);
  if (sthreads)
  {
    /* Find last server thread entry and add new thread to end of list */
    stp = sthreads;
    while (stp->next)
    {
      stp = stp->next;
    }

    stp->next = nstp;
    nstp->prev = stp;
  }
  else
  {
    /* Otherwise this is the first entry */
    sthreads = nstp;
  }
  nstp->next = 0;
  pthread_mutex_unlock (&sthreads_lock);

  return 0;
} /* End of AddServerThread() */

/***************************************************************************
 * CalcSize:
 *
 * Calculate a size in bytes for the specified size string.  If the
 * string is terminated with the following suffixes the specified
 * scaling will be applied:
 *
 * 'K' or 'k' : kilobytes - value * 1024
 * 'M' or 'm' : megabytes - value * 1024*1024
 * 'G' or 'g' : gigabytes - value * 1024*1024*1024
 *
 * Returns a size in bytes on success and 0 on error.
 ***************************************************************************/
static uint64_t
CalcSize (char *sizestr)
{
  uint64_t size = 0;
  char *parsestr;
  int termchar;

  if (!sizestr)
    return 0;

  if (!(parsestr = strdup (sizestr)))
    return 0;

  termchar = strlen (parsestr) - 1;

  if (termchar <= 0)
    return 0;

  /* For kilobytes */
  if (parsestr[termchar] == 'K' || parsestr[termchar] == 'k')
  {
    parsestr[termchar] = '\0';
    size = strtoull (parsestr, NULL, 10);
    if (!size)
    {
      lprintf (0, "CalcSize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1024;
  }
  /* For megabytes */
  else if (parsestr[termchar] == 'M' || parsestr[termchar] == 'm')
  {
    parsestr[termchar] = '\0';
    size = strtoull (parsestr, NULL, 10);
    if (!size)
    {
      lprintf (0, "CalcSize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1024 * 1024;
  }
  /* For gigabytes */
  else if (parsestr[termchar] == 'G' || parsestr[termchar] == 'g')
  {
    parsestr[termchar] = '\0';
    size = strtoull (parsestr, NULL, 10);
    if (!size)
    {
      lprintf (0, "CalcSize(): Error converting %s to integer", parsestr);
      return 0;
    }
    size *= 1024 * 1024 * 1024;
  }
  else
  {
    size = strtoull (parsestr, NULL, 10);
    if (!size)
    {
      lprintf (0, "CalcSize(): Error converting %s to integer", parsestr);
      return 0;
    }
  }

  if (parsestr)
    free (parsestr);

  return size;
} /* End of CalcSize() */

/***************************************************************************
 * CalcStats:
 *
 * Calculate statisics for the specified client connection.  This
 * includes the following calculations:
 *
 * 1) Percent lag in the ring buffer, with the latest packet
 * representing 0% lag and the earliest packet representing 100% lag.
 *
 * 2) Transmission and reception rates in Hz (packet count and bytes).
 *
 * This routine assumes that the packet and byte counts will always
 * increase.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
CalcStats (ClientInfo *cinfo)
{
  hptime_t hpnow = HPnow ();
  double deltasec;
  int64_t ulatestid;
  int64_t upktid;

  if (!cinfo)
    return -1;

  /* Determine percent lag if the current pktid is set */
  if (cinfo->reader && cinfo->reader->pktid > 0)
  {
    /* Determined "unwrapped" latest ring ID and current reader position ID */
    ulatestid = (ringparams->latestid < ringparams->earliestid) ? ringparams->latestid + ringparams->maxpktid : ringparams->latestid;

    upktid = (cinfo->reader->pktid < ringparams->earliestid) ? cinfo->reader->pktid + ringparams->maxpktid : cinfo->reader->pktid;

    /* Calculate percentage lag as position in ring where 0% = latest ID and 100% = earliest ID */
    cinfo->percentlag = (int)(((double)(ulatestid - upktid) / (ulatestid - ringparams->earliestid)) * 100);
  }
  else
  {
    cinfo->percentlag = 0;
  }

  /* Determine time difference since the previous history values were set in seconds */
  if (cinfo->ratetime == 0)
    deltasec = 1.0;
  else
    deltasec = (double)(hpnow - cinfo->ratetime) / HPTMODULUS;

  /* Transmission */
  if (cinfo->txpackets[0] > 0)
  {
    /* Calculate the transmission rates */
    cinfo->txpacketrate = (double)(cinfo->txpackets[0] - cinfo->txpackets[1]) / deltasec;
    cinfo->txbyterate = (double)(cinfo->txbytes[0] - cinfo->txbytes[1]) / deltasec;

    /* Shift current values to history values */
    cinfo->txpackets[1] = cinfo->txpackets[0];
    cinfo->txbytes[1] = cinfo->txbytes[0];
  }

  /* Reception */
  if (cinfo->rxpackets[0] > 0)
  {
    /* Calculate the reception rates */
    cinfo->rxpacketrate = (double)(cinfo->rxpackets[0] - cinfo->rxpackets[1]) / deltasec;
    cinfo->rxbyterate = (double)(cinfo->rxbytes[0] - cinfo->rxbytes[1]) / deltasec;

    /* Shift current values to history values */
    cinfo->rxpackets[1] = cinfo->rxpackets[0];
    cinfo->rxbytes[1] = cinfo->rxbytes[0];
  }

  /* Update time stamp of history values */
  cinfo->ratetime = hpnow;

  return 0;
} /* End of CalcStats() */

/***************************************************************************
 * AddIPNet:
 *
 * Add network and netmask to an IPNet list.  Both IPv4 and IPv6 are
 * supported.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
AddIPNet (IPNet **pplist, char *network, char *limitstr)
{
  struct addrinfo hints;
  struct addrinfo *addrlist;
  struct addrinfo *addr;
  struct sockaddr_in *sockaddr;
  struct sockaddr_in6 *sockaddr6;
  IPNet *newipnet;
  char net[100] = {0};
  char *end = NULL;
  char *prefixstr;
  unsigned long int prefix = 0;
  uint32_t v4netmask = 0;
  int rv;
  int idx;
  int jdx;

  if (!pplist || !network)
    return -1;

  /* Copy network string for manipulation */
  strncpy (net, network, sizeof (net) - 1);

  /* Split netmask/prefixlen from network if present: "IP/netmask" */
  if ((prefixstr = strchr (net, '/')))
  {
    *prefixstr++ = '\0';
  }

  /* Convert prefix string to value */
  if (IsAllDigits (prefixstr))
  {
    prefix = strtoul (prefixstr, &end, 10);

    if (*end)
    {
      lprintf (0, "AddIPNet(): Error converting prefix value (%s)", prefixstr);
      return -1;
    }
  }
  /* Convert IPv4 netmask to prefix, anything not all digits must be a mask */
  else if (prefixstr)
  {
    if (inet_pton (AF_INET, prefixstr, &v4netmask) <= 0)
    {
      lprintf (0, "AddIPNet(): Error parsing IPv4 netmask: %s", prefixstr);
      return -1;
    }

    if (v4netmask > 0)
    {
      if (ntohl(v4netmask) & (~ntohl(v4netmask) >> 1))
      {
        lprintf (0, "AddIPNet(): Invalid IPv4 netmask: %s", prefixstr);
        return -1;
      }
    }
  }
  else
  {
    prefix = 128;
  }

  /* Sanity check and catch errors from strtoul() */
  if (prefix > 128)
  {
    lprintf (0, "AddIPNet(): Error, prefix (%s) must be less than 128", prefixstr);
    return -1;
  }

  /* Convert address portion to binary address, resolving if possible */
  memset (&hints, 0, sizeof (hints));
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;     /* Either IPv4 and/or IPv6 */
  hints.ai_flags = AI_ADDRCONFIG;  /* Only return entries that could actually connect */

  if ((rv = getaddrinfo (net, NULL, &hints, &addrlist)) != 0)
  {
    lprintf (0, "AddIPNet(): Error with getaddrinfo(%s): %s", net, gai_strerror (rv));
    return -1;
  }

  /* Loop through results from getaddrinfo(), adding new entries */
  for (addr = addrlist; addr != 0; addr = addr->ai_next)
  {
    /* Allocate new IPNet */
    if (!(newipnet = (IPNet *)calloc (1, sizeof (IPNet))))
    {
      lprintf (0, "AddIPNet(): Error allocating memory for IPNet");
      return -1;
    }

    if (addr->ai_family == AF_INET)
    {
      newipnet->family = AF_INET;

      /* Use IPv4 netmask if specified directly */
      if (v4netmask > 0)
        newipnet->netmask.in_addr.s_addr = v4netmask;
      /* Calculate netmask from prefix, if the prefix > 32 use 32 as a max for IPv4 */
      else if (prefix > 0)
        newipnet->netmask.in_addr.s_addr = ~((1 << (32 - ((prefix > 32) ? 32 : prefix))) - 1);
      else
        newipnet->netmask.in_addr.s_addr = 0;

      /* Swap calculated netmask to network order, if a (swapped) mask not available */
      if (v4netmask == 0)
      {
        newipnet->netmask.in_addr.s_addr = htonl(newipnet->netmask.in_addr.s_addr);
      }

      sockaddr = (struct sockaddr_in *)addr->ai_addr;
      newipnet->network.in_addr.s_addr = sockaddr->sin_addr.s_addr;

      /* Calculate network: AND the address and netmask */
      newipnet->network.in_addr.s_addr &= newipnet->netmask.in_addr.s_addr;
    }
    else if (addr->ai_family == AF_INET6)
    {
      newipnet->family = AF_INET6;

      memset (&newipnet->netmask.in6_addr, 0, sizeof (struct in6_addr));

      /* Calculate netmask from prefix */
      if (prefix > 0)
      {
        for (idx = prefix, jdx = 0; idx > 0; idx -= 8, jdx++)
        {
          if (idx >= 8)
            newipnet->netmask.in6_addr.s6_addr[jdx] = 0xFFu;
          else
            newipnet->netmask.in6_addr.s6_addr[jdx] = (uint8_t)(0xFFu << (8 - idx));
	}
      }

      sockaddr6 = (struct sockaddr_in6 *)addr->ai_addr;
      memcpy (&newipnet->network.in6_addr.s6_addr, &sockaddr6->sin6_addr.s6_addr, sizeof(struct sockaddr_in6));

      /* Calculate network: AND the address and netmask */
      for (idx = 0; idx < 16; idx++)
      {
        newipnet->network.in6_addr.s6_addr[idx] &= newipnet->netmask.in6_addr.s6_addr[idx];
      }
    }

    /* Store any supplied limit expression */
    if (limitstr)
    {
      if (!(newipnet->limitstr = strdup (limitstr)))
      {
        lprintf (0, "AddIPNet(): Error allocating memory for limit string");
        return -1;
      }
    }

    /* Push the new entry on the top of the list */
    newipnet->next = *pplist;
    *pplist = newipnet;
  }

  freeaddrinfo (addrlist);

  return 0;
} /* End of AddIPNet() */


/***************************************************************************
 * MatchIP:
 *
 * Search the specified IPNet list for an entry that matches the given
 * IP address.
 *
 * Returns the matching IPNet entry if match found and NULL if no match found.
 ***************************************************************************/
static IPNet *
MatchIP (IPNet *list, struct sockaddr *addr)
{
  IPNet *net = list;
  struct in_addr *testnet = &((struct sockaddr_in *)addr)->sin_addr;
  struct in6_addr *testnet6 = &((struct sockaddr_in6 *)addr)->sin6_addr;

  if (!list)
    return 0;

  /* Sanity, only IPv4 and IPv6 addresses */
  if (addr->sa_family != AF_INET && addr->sa_family != AF_INET6)
    return 0;

  /* Search IPNet list for a matching entry for addr */
  while (net)
  {
    /* Check for match between test network and list network */
    if (addr->sa_family == AF_INET && net->family == AF_INET)
    {
      if ((testnet->s_addr & net->netmask.in_addr.s_addr) == net->network.in_addr.s_addr)
      {
        return net;
      }
    }
    else if (addr->sa_family == AF_INET6 && net->family == AF_INET6)
    {
      if ((testnet6->s6_addr[0] & net->netmask.in6_addr.s6_addr[0]) == net->network.in6_addr.s6_addr[0] &&
          (testnet6->s6_addr[1] & net->netmask.in6_addr.s6_addr[1]) == net->network.in6_addr.s6_addr[1] &&
          (testnet6->s6_addr[2] & net->netmask.in6_addr.s6_addr[2]) == net->network.in6_addr.s6_addr[2] &&
          (testnet6->s6_addr[3] & net->netmask.in6_addr.s6_addr[3]) == net->network.in6_addr.s6_addr[3] &&
          (testnet6->s6_addr[4] & net->netmask.in6_addr.s6_addr[4]) == net->network.in6_addr.s6_addr[4] &&
          (testnet6->s6_addr[5] & net->netmask.in6_addr.s6_addr[5]) == net->network.in6_addr.s6_addr[5] &&
          (testnet6->s6_addr[6] & net->netmask.in6_addr.s6_addr[6]) == net->network.in6_addr.s6_addr[6] &&
          (testnet6->s6_addr[7] & net->netmask.in6_addr.s6_addr[7]) == net->network.in6_addr.s6_addr[7] &&
          (testnet6->s6_addr[8] & net->netmask.in6_addr.s6_addr[8]) == net->network.in6_addr.s6_addr[8] &&
          (testnet6->s6_addr[9] & net->netmask.in6_addr.s6_addr[9]) == net->network.in6_addr.s6_addr[9] &&
          (testnet6->s6_addr[10] & net->netmask.in6_addr.s6_addr[10]) == net->network.in6_addr.s6_addr[10] &&
          (testnet6->s6_addr[11] & net->netmask.in6_addr.s6_addr[11]) == net->network.in6_addr.s6_addr[11] &&
          (testnet6->s6_addr[12] & net->netmask.in6_addr.s6_addr[12]) == net->network.in6_addr.s6_addr[12] &&
          (testnet6->s6_addr[13] & net->netmask.in6_addr.s6_addr[13]) == net->network.in6_addr.s6_addr[13] &&
          (testnet6->s6_addr[14] & net->netmask.in6_addr.s6_addr[14]) == net->network.in6_addr.s6_addr[14] &&
          (testnet6->s6_addr[15] & net->netmask.in6_addr.s6_addr[15]) == net->network.in6_addr.s6_addr[15])
      {
        return net;
      }
    }

    net = net->next;
  }

  return NULL;
} /* End of MatchIP() */

/***************************************************************************
 * ClientIPCount:
 *
 * Search the global client list and return a count of the connected
 * clients that match the specified address.
 *
 * Returns count of the client connections with a matching address.
 ***************************************************************************/
static int
ClientIPCount (struct sockaddr *addr)
{
  struct cthread *ctp;
  struct sockaddr_in *tsin[2];
  struct sockaddr_in6 *tsin6[2];
  int addrcount = 0;

  pthread_mutex_lock (&cthreads_lock);
  ctp = cthreads;
  while (ctp)
  {
    /* If the same protocol family */
    if (((ClientInfo *)ctp->td->td_prvtptr)->addr->sa_family == addr->sa_family)
    {
      /* Compare IPv4 addresses */
      if (addr->sa_family == AF_INET)
      {
        tsin[0] = (struct sockaddr_in *)((ClientInfo *)ctp->td->td_prvtptr)->addr;
        tsin[1] = (struct sockaddr_in *)addr;

        if (0 == memcmp (&tsin[0]->sin_addr.s_addr,
                         &tsin[1]->sin_addr.s_addr,
                         sizeof (tsin[0]->sin_addr.s_addr)))
        {
          addrcount++;
        }
      }
      /* Compare IPv6 addresses */
      else if (addr->sa_family == AF_INET6)
      {
        tsin6[0] = (struct sockaddr_in6 *)((ClientInfo *)ctp->td->td_prvtptr)->addr;
        tsin6[1] = (struct sockaddr_in6 *)addr;

        if (0 == memcmp (&tsin6[0]->sin6_addr.s6_addr,
                         &tsin6[1]->sin6_addr.s6_addr,
                         sizeof (tsin6[0]->sin6_addr.s6_addr)))
        {
          addrcount++;
        }
      }
    }

    ctp = ctp->next;
  }
  pthread_mutex_unlock (&cthreads_lock);

  return addrcount;
} /* End of ClientIPCount() */

/***********************************************************************
 * SignalThread:
 *
 * Thread to handle signals.
 *
 * Return NULL.
 ***********************************************************************/
void *
SignalThread (void *arg)
{
  int sig;
  int rc;

  /* Remove SIGPIPE from complete set, it will remain blocked */
  if (sigdelset (&globalsigset, SIGPIPE))
  {
    lprintf (0, "Error: sigdelset() failed, cannot remove SIGPIPE");
  }

  for (;;)
  {
    if ((rc = sigwait (&globalsigset, &sig)))
    {
      lprintf (0, "Error: sigwait() failed with %d", rc);
      continue;
    }

    switch (sig)
    {
    case SIGINT:
    case SIGTERM:
      lprintf (1, "Received termination signal");
      shutdownsig = 1; /* Set global termination flag */
      break;
    case SIGUSR1:
      PrintHandler (); /* Print global ring details */
      break;
    default:
      lprintf (0, "Summarily ignoring %s (%d) signal", strsignal (sig), sig);
      break;
    }
  }

  return NULL;
} /* End of SignalThread() */

/***************************************************************************
 * PrintHandler (USR1 signal):
 ***************************************************************************/
static void
PrintHandler (int sig)
{
  char timestr[100];

  lprintf (1, "Ring parameters, ringsize: %" PRIu64 ", pktsize: %u (%lu)",
           ringparams->ringsize, ringparams->pktsize,
           ringparams->pktsize - sizeof (RingPacket));
  lprintf (2, "   maxpackets: %" PRId64 ", maxpktid: %" PRId64,
           ringparams->maxpackets, ringparams->maxpktid);
  lprintf (2, "   maxoffset: %" PRId64 ", headersize: %u",
           ringparams->maxoffset, ringparams->headersize);
  ms_hptime2mdtimestr (ringparams->earliestptime, timestr, 1);
  lprintf (2, "   earliest packet ID: %" PRId64 ", offset: %" PRId64 ", time: %s",
           ringparams->earliestid, ringparams->earliestoffset,
           (ringparams->earliestptime == HPTERROR) ? "NONE" : timestr);
  ms_hptime2mdtimestr (ringparams->latestptime, timestr, 1);
  lprintf (2, "   latest packet ID: %" PRId64 ", offset: %" PRId64 ", time: %s",
           ringparams->latestid, ringparams->latestoffset,
           (ringparams->latestptime == HPTERROR) ? "NONE" : timestr);
  lprintf (2, "   TX packet rate: %g, TX byte rate: %g",
           ringparams->txpacketrate, ringparams->txbyterate);
  lprintf (2, "   RX packet rate: %g, RX byte rate: %g",
           ringparams->rxpacketrate, ringparams->rxbyterate);
}

/***************************************************************************
 * Usage:
 *
 * Print usage message and exit.
 ***************************************************************************/
static void
Usage (int level)
{
  fprintf (stderr, "%s version %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] [configfile]\n\n", PACKAGE);
  fprintf (stderr, " ## Options ##\n"
                   " -V             Print program version and exit\n"
                   " -h             Print this usage message\n"
                   " -H             Print an extended usage message\n"
                   " -v             Be more verbose, multiple flags can be used\n"
                   " -I serverID    Server ID (default 'Ring Server')\n"
                   " -m maxclnt     Maximum number of concurrent clients (currently %d)\n"
                   " -M maxperIP    Maximum number of concurrent clients per address (currently %d)\n"
                   " -Rd ringdir    Directory for ring buffer files, required\n"
                   " -Rs bytes      Ring packet buffer file size in bytes (default 1 Gigabyte)\n"
                   " -Rm maxid      Maximum ring packet ID (currently %" PRId64 ")\n"
                   " -Rp pktsize    Maximum ring packet data size in bytes (currently %d)\n"
                   " -NOMM          Do not memory map the packet buffer, use memory instead\n"
                   " -L port        Listen for connections on port, all protocols (default off)\n"
                   " -T logdir      Directory to write transfer logs (default is no logs)\n"
                   " -Ti hours      Transfer log writing interval (default 24 hours)\n"
                   " -Tp prefix     Prefix to add to transfer log files (default is none)\n"
                   " -STDERR        Send all console output to stderr instead of stdout\n"
                   "\n",
           maxclients, maxclientsperip, maxpktid, (int)(pktsize - sizeof (RingPacket)));

  if (level >= 1)
  {
    fprintf (stderr,
             " -MSWRITE format  Write all received miniSEED to an archive\n"
             " -MSSCAN dir      Scan directory for files containing miniSEED\n"
             " -VOLATILE        Create volatile ring, contents not saved to files\n"
             "\n");

    fprintf (stderr,
             "The 'format' argument is expanded for each record using the\n"
             "flags below.  Some preset archive layouts are available:\n"
             "\n"
             "BUD   : %%n/%%s/%%s.%%n.%%l.%%c.%%Y.%%j  (BUD layout)\n"
             "CHAN  : %%n.%%s.%%l.%%c  (channel)\n"
             "QCHAN : %%n.%%s.%%l.%%c.%%q  (quality-channel-day)\n"
             "CDAY  : %%n.%%s.%%l.%%c.%%Y:%%j:#H:#M:#S  (channel-day)\n"
             "SDAY  : %%n.%%s.%%Y:%%j  (station-day)\n"
             "HSDAY : %%h/%%n.%%s.%%Y:%%j  (host-station-day)\n"
             "\n"
             "Archive definition flags\n"
             "  n : Network code, white space removed\n"
             "  s : Station code, white space removed\n"
             "  l : Location code, white space removed\n"
             "  c : Channel code, white space removed\n"
             "  q : Record quality indicator (D, R, Q, M), single character\n"
             "  Y : Year, 4 digits\n"
             "  y : Year, 2 digits zero padded\n"
             "  j : Day of year, 3 digits zero padded\n"
             "  H : Hour, 2 digits zero padded\n"
             "  M : Minute, 2 digits zero padded\n"
             "  S : Second, 2 digits zero padded\n"
             "  F : Fractional seconds, 4 digits zero padded\n"
             "  D : Current year-day time stamp of the form YYYYDDD\n"
             "  L : Data record length in bytes\n"
             "  r : Sample rate (Hz) as a rounded integer\n"
             "  R : Sample rate (Hz) as a float with 6 digit precision\n"
             "  h : Host name of client submitting data \n"
             "  %% : The percent (%%) character\n"
             "  # : The number (#) character\n"
             "\n"
             "The flags are prefaced with either the %% or # modifier.  The %% modifier\n"
             "indicates a defining flag while the # indicates a non-defining flag.\n"
             "All records with the same set of defining flags will be written to the\n"
             "same file. Non-defining flags will be expanded using the values in the\n"
             "first record for the resulting file name.\n"
             "\n");
  }

  exit (1);
} /* End of Usage() */

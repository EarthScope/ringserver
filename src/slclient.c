/**************************************************************************
 * slclient.c
 *
 * SeedLink client thread specific routines.
 *
 * Mapping SeedLink sequence numbers <-> ring packet ID's:
 *
 * SeedLink sequence numbers are mapped directly to ring packet IDs.
 * With a large ring it is possible to have more IDs than fit into the
 * SeedLink sequence number address space (6 hexidecimal numbers,
 * maximum of 0xFFFFFF which is 16,777,215), an error will be logged
 * when IDs are encountered that are too large to map into a sequence
 * number.
 *
 * The SeedLink protocol is designed around a multi-ring backend
 * whereas the ringserver implements a single ring.  This is important
 * because a client will request data from network-station rings and
 * optionally specify starting sequence numbers for each ring to
 * resume a connection.  This is handled in this client by determining
 * the most recent packet ID requested by the client and setting the
 * ring to that position.
 *
 * Copyright 2014 Chad Trabant, IRIS Data Management Center
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
 * Modified: 2015.182
 **************************************************************************/

/* Unsupported protocol features:
 * CAT listing (oh the irony)
 * INFO GAPS
 * INFO ALL
 */

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <libmseed.h>
#include <mxml.h>

#include "ring.h"
#include "ringserver.h"
#include "rbtree.h"
#include "clients.h"
#include "generic.h"
#include "logging.h"
#include "slclient.h"

/* Define list of valid characters for selectors and station & network codes */
#define VALIDSELECTCHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?!-"
#define VALIDNETSTACHARS "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?*"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int  HandleNegotiation (char *recvbuffer, int recvlength, ClientInfo *cinfo, 
			       char *state, char *reqnet, char *reqsta);
static int  HandleInfo (char *recvbuffer, ClientInfo *cinfo, char state);
static int  RecvLine (ClientInfo *cinfo);
static int  SendReply (ClientInfo *cinfo, char *reply, char *extreply);
static int  SendData (ClientInfo *cinfo, void *buffer, size_t buflen);
static int  SendRecord (RingPacket *packet, char *record, int reclen,
			void *vcinfo);
static void SendInfoRecord (char *record, int reclen, void *vcinfo);
static void FreeStaNode (void *rbnode);
static void FreeNetStaNode (void *rbnode);
static int  StaKeyCompare (const void *a, const void *b);
static SLStaNode* GetStaNode (RBTree *tree, char *net, char *sta);
static SLNetStaNode* GetNetStaNode (RBTree *tree, char *net, char *sta);
static int  StationToRegex (char *net, char *sta, char *selectors,
			    char **matchregex, char **rejectregex);
static int  SelectToRegex (char *net, char *sta, char *select,
			   char **regex);


/***********************************************************************
 * SL_ClientThread:
 *
 * Thread to handle all communications with a SeedLink client.
 *
 * Returns NULL.
 ***********************************************************************/
void *
SL_ClientThread (void *arg)
{
  ClientInfo *cinfo;
  SLInfo *slinfo;
  RingReader reader;
  struct thread_data *mytdp;
  int64_t readid;
  int64_t retval;
  int sockflags;
  int setuperr = 0;
  int nread;
  int skiprecord;
  
  struct sockaddr_in sin;
  socklen_t sinlen = sizeof(struct sockaddr_in);
  int serverport = -1;

  /* Client thread-specific buffers used during negotiation */
  char reqnet[10];
  char reqsta[10];
  
  /* Client thread specific packet header and data buffers */
  RingPacket packet;
  char *packetdata = 0;
  
  /* Pointers to binary tree elements */
  SLStaKey *stakey;
  SLStaNode *stanode;
  StreamNode *stream;
  
  /* Throttle related */
  char throttle = 0; /* Controls throttling of main loop */
  fd_set readset;    /* File descriptor set for select() */
  struct timeval timeout;  /* Timeout throttle for select() */
  
  char state = 0;    /* 0 = negotiation
		        1 = negotiating STATION
		        2 = configuring ring connection
		        3 = data flow
		     */
  
  /* Time-windowing related */
  int newstream;
  int timewinchannels = 0;
  
  mytdp = (struct thread_data *) arg;
  cinfo = (ClientInfo *) mytdp->td_prvtptr;
  
  /* Glue together linked structures */
  cinfo->reader = &reader;
  reader.ringparams = cinfo->ringparams;
  
  /* Initialize RingReader parameters */
  reader.pktid = 0;
  reader.pkttime = HPTERROR;
  reader.datastart = HPTERROR;
  reader.dataend = HPTERROR;
  reader.limit = 0;
  reader.limit_extra = 0;
  reader.match = 0;
  reader.match_extra = 0;
  reader.reject = 0;
  reader.reject_extra = 0;
  
  /* Resolve IP address to hostname */
  if ( resolvehosts )
    {
      if ( getnameinfo (cinfo->addr, cinfo->addrlen,
			cinfo->hostname, sizeof(cinfo->hostname), NULL, 0, 0) )
	{
	  /* Copy numeric IP address into hostname on failure to resolve */
	  strncpy (cinfo->hostname, cinfo->ipstr, sizeof (cinfo->hostname)-1);
	}
    }
  /* Otherwise use the numerical IP address as the hostname */
  else
    {
      /* Copy numeric IP address into hostname when not resolving */
      strncpy (cinfo->hostname, cinfo->ipstr, sizeof (cinfo->hostname)-1);
    }
  
  /* Find the server port used for this connection */
  if ( getsockname (cinfo->socket, (struct sockaddr *)&sin, &sinlen) == 0 )
    {
      serverport = ntohs(sin.sin_port);
    }
  
  lprintf (1, "Client connected [SeedLink:%d]: %s [%s] port %s",
	   serverport, cinfo->hostname, cinfo->ipstr, cinfo->portstr);
  
  /* Initialize SLInfo structure */
  if ( ! (slinfo = (SLInfo *) calloc (1, sizeof(SLInfo))) )
    {
      lprintf (0, "[%s] Error allocating SLInfo", cinfo->hostname);
      setuperr = 1;
    }
  
  /* Set the extended client info to the new SLInfo */
  cinfo->extinfo = slinfo;
  
  /* Allocate client specific receive buffer */
  cinfo->recvbuf = (char *) calloc (1, 100);
  if ( ! cinfo->recvbuf )
    {
      lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
      setuperr = 1;
    }
  cinfo->recvbuflen = 100;
  
  /* Allocate client specific packet data buffer */
  packetdata = (char *) malloc (cinfo->ringparams->pktsize);
  if ( ! packetdata )
    {
      lprintf (0, "[%s] Error allocating packet buffer", cinfo->hostname);
      setuperr = 1;
    }
  
  /* Initialize stations and streams binary trees */
  slinfo->stations = RBTreeCreate (StaKeyCompare, free, FreeStaNode);
  
  pthread_mutex_lock (&(cinfo->streams_lock));
  cinfo->streams = RBTreeCreate (KeyCompare, free, free);
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Set client socket connection to non-blocking */
  sockflags = fcntl(cinfo->socket, F_GETFL, 0);
  sockflags |= O_NONBLOCK;
  if ( fcntl(cinfo->socket, F_SETFL, sockflags) == -1 )
    {
      lprintf (0, "[%s] Error setting non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      setuperr = 1;
    }
  
  /* Limit sources if specified */
  if ( cinfo->limitstr )
    {
      if ( RingLimit(&reader, cinfo->limitstr) < 0 )
	{
	  lprintf (0, "[%s] Error with RingLimit for '%s'", cinfo->hostname, cinfo->limitstr);
	  setuperr = 1;
	}
    }
  
  /* Shutdown the client connection if there were setup errors */
  if ( setuperr )
    {
      /* Close client socket */
      if ( cinfo->socket )
        {
          close (cinfo->socket);
          cinfo->socket = 0;
        }
      
      /* Set thread closing status */
      pthread_mutex_lock (&(mytdp->td_lock));
      mytdp->td_flags = TDF_CLOSING;
      pthread_mutex_unlock (&(mytdp->td_lock));
      
      if ( packetdata )
	free (packetdata);
      
      return NULL;
    }
  
  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if ( mytdp->td_flags == TDF_SPAWNING )
    mytdp->td_flags = TDF_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  /* Main client loop, delegating INFO requests and handling data flow */
  while ( mytdp->td_flags != TDF_CLOSE )
    {
      /* Increment throttle trigger count by default */
      if ( throttle < THROTTLE_TRIGGER )
	throttle++;
      
      /* Check for data from client */
      nread = RecvLine (cinfo);
      
      /* Error receiving data, -1 = orderly shutdown, -2 = error */
      if ( nread < 0 )
	{
	  break;
	}
      
      /* Data received from client */
      if ( nread > 0 )
	{
	  /* If data was received do not throttle */
	  throttle = 0;
	  
	  /* Update the time of the last packet exchange */
	  cinfo->lastxchange = HPnow();
	  
	  /* Determine if this is an INFO request and handle */
	  if ( ! strncasecmp (cinfo->recvbuf, "INFO", 4) )
	    {
	      if ( HandleInfo(cinfo->recvbuf, cinfo, state) )
		{
		  break;
		}
	    }
	  
	  /* Negotiation if data flow has not started */
	  else if ( state == 0 || state == 1 )
	    {
	      /* Any errors from HandleNegotiation are fatal */
	      if ( HandleNegotiation(cinfo->recvbuf, nread, cinfo, &state, reqnet, reqsta) )
		{
		  break;
		}
	    }
	  
	  /* Otherwise this is unexpected data from the client */
	  else
	    {
	      lprintf (1, "[%s] Unexpected data received from client", cinfo->hostname);
	    }
	} /* Done processing data from client */
      
      /* Configure ring parameters */
      if ( state == 2 )
	{
          lprintf (2, "[%s] Configuring ring parameters", cinfo->hostname);
	  
	  /* If no stations specified convert any global selectors to regexes */
	  if ( slinfo->stationcount == 0 && slinfo->selectors )
	    {
	      if ( StationToRegex (NULL, NULL, slinfo->selectors,
				   &(cinfo->matchstr), &(cinfo->rejectstr)) )
		{
		  lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
		  SendReply (cinfo, "ERROR", "Error with StationToRegex");
		  break;
		}
	    }
	  /* Loop through any specified stations to:
	   * 1) Configure regexes
	   * 2) Find widest time window
	   * 3) Find start packet
	   */
	  else if ( slinfo->stationcount > 0 && slinfo->stations )
	    {
	      Stack *stack;
	      RBNode *rbnode;
	      hptime_t newesttime = 0;
	      
	      stack = StackCreate();
	      RBBuildStack (slinfo->stations, stack);
	      
	      while ( (rbnode = (RBNode *) StackPop (stack)) )
		{
		  stakey = (SLStaKey *) rbnode->key;
		  stanode = (SLStaNode *) rbnode->data;
		  
		  /* Configure regexes for this station */
		  if ( StationToRegex (stakey->net, stakey->sta, stanode->selectors,
				       &(cinfo->matchstr), &(cinfo->rejectstr)) )
		    {
		      lprintf (0, "[%s] Error with StationToRegex", cinfo->hostname);
		      SendReply (cinfo, "ERROR", "Error with StationToRegex");
		      break;
		    }
		  
		  /* Track the widest time window requested */
		  
		  /* Set or expand the global starttime */
		  if ( stanode->starttime != HPTERROR )
		    {
		      if ( ! cinfo->starttime )
			cinfo->starttime = stanode->starttime;
		      else if ( cinfo->starttime > stanode->starttime )
			cinfo->starttime = stanode->starttime;
		    }
		  
		  /* Set or expand the global endtime */
		  if ( stanode->endtime != HPTERROR )
		    {
		      if ( ! cinfo->endtime )
			cinfo->endtime = stanode->endtime;
		      else if ( cinfo->endtime < stanode->endtime )
			cinfo->endtime = stanode->endtime;
		    }
		  
                  /* Track the newest packet ID while validating their existence */
		  if ( stanode->packetid )
		    retval = RingRead (&reader, stanode->packetid, &packet, 0);
		  else
		    retval = 0;

		  /* Requested packet must be valid and have a matching data start time */
		  if ( retval == stanode->packetid && (stanode->datastart == HPTERROR || stanode->datastart == packet.datastart) )
                    {
		      /* Use this packet ID if it is newer than any previous newest */
		      if ( newesttime == 0 || packet.pkttime > newesttime )
			{
			  slinfo->startid = stanode->packetid;
			  newesttime = packet.pkttime;
			}
                    }
		}
	      
	      StackDestroy (stack, 0);
	    }
	  
	  lprintf (2, "[%s] Requesting match: '%s', reject: '%s'", cinfo->hostname,
		   (cinfo->matchstr)?cinfo->matchstr:"", (cinfo->rejectstr)?cinfo->rejectstr:"");
	  
	  /* Position ring to starting packet ID if specified */
	  if ( slinfo->startid > 0 )
	    {
	      int64_t reqid;
	      
	      /* SeedLink clients always resume data flow by requesting: lastpacket + 1
	       * The ring needs to be positioned to the actual last packet ID for RINGNEXT */
	      
	      reqid = ( slinfo->startid == 1 ) ? cinfo->ringparams->maxpktid : (slinfo->startid-1);
	      
	      retval = RingPosition (&reader, reqid, HPTERROR);
	      
	      if ( retval < 0 )
		{
		  lprintf (0, "[%s] Error with RingPosition for '%lld'",
			   cinfo->hostname, reqid);
		  break;
		}
	      else if ( retval == 0 )
		{
		  lprintf (0, "[%s] Could not find and position to packet ID: %lld",
			   cinfo->hostname, reqid);
		}
	      else
		{
		  lprintf (2, "[%s] Positioned ring to packet ID: %lld",
			   cinfo->hostname, reqid);
		}
	    }
	  
	  /* Select sources if any specified */
	  if ( cinfo->matchstr )
	    {
	      if ( RingMatch(&reader, cinfo->matchstr) < 0 )
		{
		  lprintf (0, "[%s] Error with RingMatch for (%lu bytes) '%s'",
			   cinfo->hostname, (unsigned long)strlen(cinfo->matchstr), cinfo->matchstr);
		  SendReply (cinfo, "ERROR", "cannot compile matches (combined matches too large?)");
		  break;
		}
	    }
	  
	  /* Reject sources if any specified */
	  if ( cinfo->rejectstr )
	    {
	      if ( RingReject(&reader, cinfo->rejectstr) < 0 )
		{
		  lprintf (0, "[%s] Error with RingReject for (%lu bytes) '%s'",
			   cinfo->hostname, (unsigned long)strlen(cinfo->rejectstr), cinfo->rejectstr);
		  SendReply (cinfo, "ERROR", "cannot compile rejections (combined rejection too large?)");
		  break;
		}
	    }
	  
	  /* Set ring position based on time if start time specified and not a packet ID */
	  if ( cinfo->starttime && cinfo->starttime != HPTERROR && ! slinfo->startid )
	    {
	      char timestr[50];
	      
	      ms_hptime2seedtimestr (cinfo->starttime, timestr, 1);
	      readid = 0;
	      
	      /* Position ring according to start time, use reverse search if limited */
	      if ( cinfo->timewinlimit == 1.0 )
		{
		  readid = RingAfter (&reader, cinfo->starttime, 0);
		}
	      else if ( cinfo->timewinlimit < 1.0 )
		{
		  int64_t pktlimit = (int64_t) (cinfo->timewinlimit * cinfo->ringparams->maxpackets);
		  
		  readid = RingAfterRev (&reader, cinfo->starttime, pktlimit, 0);
		}
	      else
		{
		  lprintf (0, "Time window search limit is invalid: %f", cinfo->timewinlimit);
		  SendReply (cinfo, "ERROR", "time window search limit is invalid");
		  break;
		}
	      
	      if ( readid < 0 )
		{
		  lprintf (0, "[%s] Error with RingAfter time: %s [%lld]",
			   cinfo->hostname, timestr, cinfo->starttime);
		  SendReply (cinfo, "ERROR", "Error positioning reader to start of time window");
		  break;
		}
	      
	      if ( readid == 0 )
		{
		  lprintf (2, "[%s] No packet found for RingAfter time: %s, positioning to next packet",
			   cinfo->hostname, timestr, cinfo->starttime);
		  reader.pktid = RINGNEXT;
		}
	      else
		{
		  lprintf (2, "[%s] Positioned to packet %lld, first after: %s",
			   cinfo->hostname, readid, timestr);
		}
	    }
	  
	  /* Set read position to next packet if not already done */
	  if ( reader.pktid == 0 )
	    {
	      reader.pktid = RINGNEXT;
	    }
	  
          lprintf (1, "[%s] Configured ring parameters", cinfo->hostname);
	  state = 3;
	}  /* Done configuring ring parameters (state == 2) */
      
      /* Regular data flow */
      if ( state == 3 )
	{
	  /* Read next packet from ring */
	  readid = RingReadNext (&reader, &packet, packetdata);
	  
	  if ( readid < 0 )
	    {
	      lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
 	      break;
	    }
	  else if ( readid > 0 && MS_ISVALIDHEADER(packetdata) && packet.datasize == SLRECSIZE )
	    {
	      lprintf (3, "[%s] Read %s (%u bytes) packet ID %lld from ring",
 		       cinfo->hostname, packet.streamid, packet.datasize, packet.pktid);
	      
	      /* If data read do not throttle */
	      throttle = 0;
	      
	      skiprecord = 0;
	      
	      /* Get (creating if needed) the StreamNode for this streamid */
	      if ( (stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
					    packet.streamid, &newstream)) == 0 )
		{
		  lprintf (0, "[%s] Error with GetStreamNode for %s",
			   cinfo->hostname, packet.streamid);
		  break;
		}
	      
	      if ( newstream )
		{
		  lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, packet.streamid);
		  cinfo->streamscount++;
		}
	      
	      /* Perform time-windowing start time check */
	      /* This check will skip the packets containing the start time
	      if  ( cinfo->starttime != 0 && cinfo->starttime != HPTERROR )
		{
		  if ( packet.datastart < cinfo->starttime )
		    {
		      skiprecord = 1;
		    }
		}
	      */
	      
	      /* Perform time-windowing end time checks */
	      if  ( cinfo->endtime != 0 && cinfo->endtime != HPTERROR )
		{
		  /* Track count of number of channels for time-windowing */
		  timewinchannels += newstream;
		  
		  /* Check if the end time has been reached */
		  if ( stream->endtimereached == 1 )
		    {
		      skiprecord = 1;
		    }
		  else if ( packet.datastart > cinfo->endtime )
		    {
		      lprintf (2, "[%s] End of time window reached for %s",
			       cinfo->hostname, packet.streamid);
		      stream->endtimereached = 1;
		      timewinchannels--;
		      
		      /* Skip this record */
		      skiprecord = 1;
		    }
		  
		  /* If end times for each received channel have been met the time-windowing is done */
		  if ( timewinchannels <= 0 )
		    {
		      lprintf (2, "[%s] End of time window reached for all channels", cinfo->hostname);
		      SendData (cinfo, "END", 3);
		      break;
		    }
		}
	      
	      /* If not skipping this record send to the client and update byte count */
	      if ( ! skiprecord )
		{
		  /* Send Mini-SEED record to client */
		  if ( SendRecord (&packet, packetdata, SLRECSIZE, cinfo) )
		    {
		      if ( cinfo->socketerr != 2 )
			lprintf (0, "[%s] Error sending record to client", cinfo->hostname);
		      break;
		    }
		  
		  /* Update StreamNode packet and byte count */
		  pthread_mutex_lock (&(cinfo->streams_lock));
		  stream->txpackets++;
		  stream->txbytes += packet.datasize;
		  pthread_mutex_unlock (&(cinfo->streams_lock));
		  
		  /* Update client transmit and counts */
		  cinfo->txpackets[0]++;
		  cinfo->txbytes[0] += packet.datasize;
		  
		  /* Update last sent packet ID */
		  cinfo->lastid = packet.pktid;
		}
	    }
	  /* If in dial-up mode check if we are at the end of the ring */
	  else if ( readid == 0 && slinfo->dialup )
	    {
	      lprintf (2, "[%s] Dial-up mode reached end of buffer", cinfo->hostname);
	      SendData (cinfo, "END", 3);
	      break;
	    }
	} /* Done, for now, with regular data flow (state == 3) */
      
      /* Throttle loop if THROTTLE_TRIGGER or more loops without action */
      if ( throttle >= THROTTLE_TRIGGER )
	{
	  /* Throttle the loop using select() so that data from the
	     client is never waiting for a timeout */
	  
	  /* Configure the read descriptor set with only our client socket */
	  FD_ZERO(&readset);
	  FD_SET(cinfo->socket, &readset);
	  
	  /* Timeout (throttle when no data) is 1/10 of a second */
	  timeout.tv_sec = 0;
	  timeout.tv_usec = 100000;
	  
	  select (cinfo->socket+1, &readset, NULL, NULL, &timeout);
	}
    } /* End of main client loop */
  
  /* Set thread CLOSING status, locking entire client list */
  pthread_mutex_lock (&cthreads_lock);
  mytdp->td_flags = TDF_CLOSING;
  pthread_mutex_unlock (&cthreads_lock);
  
  /* Close client socket */
  if ( cinfo->socket )
    {
      close (cinfo->socket);
      cinfo->socket = 0;
    }
  
  /* Write out transmission log for this client if requested */
  if ( TLogParams.tlogbasedir )
    {
      lprintf (2, "[%s] Writing transmission log", cinfo->hostname);
      WriteTLog (cinfo, 1);
    }
  
  /* Release match, reject and selectors strings */
  if ( cinfo->matchstr )
    free (cinfo->matchstr);
  if ( cinfo->reader->match )
    pcre_free (cinfo->reader->match);
  if ( cinfo->reader->match_extra )
    pcre_free (cinfo->reader->match_extra);
  if ( cinfo->rejectstr )
    free (cinfo->rejectstr);
  if ( cinfo->reader->reject )
    pcre_free (cinfo->reader->reject);
  if ( cinfo->reader->reject_extra )
    pcre_free (cinfo->reader->reject_extra);
  
  /* Release stations and streams binary trees */
  RBTreeDestroy (slinfo->stations);
  slinfo->stations = 0;
  
  pthread_mutex_lock (&(cinfo->streams_lock));
  RBTreeDestroy (cinfo->streams);
  cinfo->streams = 0;
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Release packetdata buffer */
  if ( packetdata )
    free (packetdata);
  
  /* Release the SLInfo information structure */
  if ( slinfo )
    free (slinfo);
  
  /* Release the client receive buffer */
  if ( cinfo->recvbuf )
    free (cinfo->recvbuf);
  
  /* Release client socket structure */
  if ( cinfo->addr )
    free (cinfo->addr);
  
  lprintf (1, "Client disconnected: %s", cinfo->hostname);

  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  return NULL;
}  /* End of SL_ClientThread() */


/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation command implementing server-side SeedLink
 * protocol, updating the connection configuration and state
 * specified.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (char *recvbuffer, int recvlength, ClientInfo *cinfo,
		   char *state, char *reqnet, char *reqsta)
{
  SLInfo *slinfo = (SLInfo *) cinfo->extinfo;
  char sendbuffer[400];
  SLStaNode *stanode;
  int fields;
  
  hptime_t starttime = HPTERROR;
  hptime_t endtime = HPTERROR;
  char starttimestr[51];
  char endtimestr[51];
  char pattern[9];
  unsigned int startpacket = 0;
  
  char *ptr;
  char  OKGO = 1;
  char  junk;
  
  /* HELLO - Return server version and ID */
  if ( ! strncasecmp (recvbuffer, "HELLO", 5) )
    {
      int bytes;

      /* Create and send server version information */
      bytes = snprintf (sendbuffer, sizeof(sendbuffer),
			SLSERVERVER "\r\n%s\r\n", serverid);
      
      if ( bytes >= sizeof(sendbuffer) )
	{
	  lprintf (0, "[%s] Response to HELLO is likely truncated: '%*s'",
		   sizeof(sendbuffer), sendbuffer);
	}
      
      if ( SendData (cinfo, sendbuffer, strlen(sendbuffer)) )
	return -1;
    }
  
  /* CAPABILITIES - Parse capabilities flags */
  else if ( ! strncasecmp (recvbuffer, "CAPABILITIES", 12) )
    {
      /* Make sure recvbuffer is terminated, last char should be \r or \n */
      recvbuffer[recvlength] = '\0';
      
      /* Check for enhanced status flags*/
      if ( strstr (recvbuffer, "EXTREPLY") )
	slinfo->extreply = 1;
      
      if ( ! slinfo->batch && SendReply (cinfo, "OK", 0) )
	return -1;
    }
  
  /* CAT - Return ASCII list of stations */
  else if ( ! strncasecmp (recvbuffer, "CAT", 3) )
    {
      snprintf (sendbuffer, sizeof(sendbuffer),
		"CAT command not implemented\r\n");
      if ( SendData (cinfo, sendbuffer, strlen(sendbuffer)) )
	return -1;
    }
  
  /* BATCH - Batch mode for subsequent commands */
  else if ( ! strncasecmp (recvbuffer, "BATCH", 5) )
    {
      slinfo->batch = 1;
      
      if ( SendReply (cinfo, "OK", 0) )
	return -1;
    }
  
  /* STATION sta_code [net_code] - Select specified network and station */
  else if ( ! strncasecmp (recvbuffer, "STATION", 7) )
    {
      OKGO = 1;
      
      /* Parse station and network from request */
      reqsta[0] = '\0';
      reqnet[0] = '\0';
      fields = sscanf (recvbuffer, "%*s %9s %9s %c", reqsta, reqnet, &junk);
      
      /* Make sure we got a station code and optionally a network code */
      if ( fields <= 0 || fields > 2 )
	{
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "STATION requires 1 or 2 arguments") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* Sanity check, only allowed characters in network code */
      if ( OKGO && strspn (reqnet, VALIDNETSTACHARS) != strlen (reqnet) )
	{
	  lprintf (0, "[%s] Error, requested network code illegal characters: '%s'",
		   cinfo->hostname, reqnet);
	  
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Invalid characters in network code") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* Sanity check, only allowed characters in station code */
      if ( OKGO && strspn (reqsta, VALIDNETSTACHARS) != strlen (reqsta) )
	{
	  lprintf (0, "[%s] Error, requested station code illegal characters: '%s'",
		   cinfo->hostname, reqsta);
	  
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Invalid characters in station code") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      if ( OKGO )
	{
	  if ( fields == 1 )
	    reqnet[0] = '\0';
	  
	  /* Add to the stations list */
	  if ( ! GetStaNode (slinfo->stations, reqnet, reqsta) )
	    {
	      lprintf (0, "[%s] Error in GetStaNode for command STATION", cinfo->hostname);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error in GetStaNode") )
		return -1;
	    }
	  
	  if ( ! slinfo->batch && SendReply (cinfo, "OK", 0) )
	    return -1;
	  
	  slinfo->stationcount++;
	  *state = 1;
	}
    }  /* End of STATION */
  
  /* SELECT [pattern] - Refine selection of channels for STATION */
  else if ( ! strncasecmp (recvbuffer, "SELECT", 6) )
    {
      OKGO = 1;
      
      /* Parse pattern from request */
      pattern[0] = '\0';
      fields = sscanf (recvbuffer, "%*s %8s %c", pattern, &junk);
      
      /* Make sure we got a single pattern */
      if ( fields != 1 )
	{
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "SELECT requires a single argument") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* Truncate pattern at a '.', DECTOL subtypes are accepted but not supported */
      if ( OKGO && (ptr = strrchr (pattern, '.')) )
	*ptr = '\0';
      
      /* Sanity check, only allowed characters */
      if ( OKGO && strspn (pattern, VALIDSELECTCHARS) != strlen (pattern) )
	{
	  lprintf (0, "[%s] Error, select pattern contains illegal characters: '%s'",
		   cinfo->hostname, pattern);
	  
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Selector contains illegal characters") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* Sanity check, pattern can only be [!][LL][CCC], 2, 3, 4, 5 or 6 characters */
      if ( OKGO && (strlen (pattern) < 2 || strlen (pattern) > 6) )
	{
	  lprintf (0, "[%s] Error, selector not 2-6 characters: %s",
		   cinfo->hostname, pattern);
	  
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Selector must be 2-6 characters") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* If modifying a STATION add selector to it's Node */
      if ( OKGO && *state == 1 )
	{
	  /* Find the appropriate StaNode */
	  if ( ! (stanode = GetStaNode (slinfo->stations, reqnet, reqsta)) )
	    {
	      lprintf (0, "[%s] Error in GetStaNode for command SELECT", cinfo->hostname);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error in GetStaNode") )
		return -1;
	    }
	  else
	    {
	      /* Add selector to the StaNode.selectors, maximum of SLMAXSELECTLEN bytes */
	      /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
	      if ( AddToString (&(stanode->selectors), pattern, ",", (pattern[0]=='!')?0:1, SLMAXSELECTLEN) )
		{
		  lprintf (0, "[%s] Error for command SELECT (cannot AddToString), too many selectors for %s.%s",
			   cinfo->hostname, reqnet, reqsta);
		  
		  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Too many selectors for this station") )
		    return -1;
		}
	      else
		{
		  if ( ! slinfo->batch && SendReply (cinfo, "OK", "Station specific") )
		    return -1;
		}
	    }
	}
      /* Otherwise add selector to global list */
      else if ( OKGO )
	{
	  /* Add selector to the SLStaNode.selectors, maximum of SLMAXSELECTLEN bytes */
	  /* If selector is negated (!) add it to end of the selectors otherwise add it to the beginning */
	  if ( AddToString (&(slinfo->selectors), pattern, ",", (pattern[0]=='!')?0:1, SLMAXSELECTLEN) )
	    {
	      lprintf (0, "[%s] Error for command SELECT (cannot AddToString), too many global selectors",
		       cinfo->hostname);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Too many global selectors") )
		return -1;
	    }
	  else
	    {
	      if ( ! slinfo->batch && SendReply (cinfo, "OK", "All-stations mode") )
		return -1;
	    }
	}
    }  /* End of SELECT */
  
  /* DATA|FETCH [n [begin_time]] - Request data from a specific packet */
  else if ( ! strncasecmp (recvbuffer, "DATA", 4) ||
	    ! strncasecmp (recvbuffer, "FETCH", 5) )
    {
      /* Parse packet and start time from request */
      starttimestr[0] = '\0';
      fields = sscanf (recvbuffer, "%*s %x %50s %c",
		       &startpacket, starttimestr, &junk);
      
      /* Make sure we got zero, one or two arguments */
      if ( fields > 2 )
	{
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Too many arguments for DATA/FETCH") )
	    return -1;
	  
	  OKGO = 0;
	}

      /* Convert time string if supplied */
      if ( OKGO && fields == 2 )
	{
          /* Change commas to dashes for parsing routine */
	  while ( (ptr = strchr (starttimestr, ',')) )
            *ptr = '-';
	  
	  if ( (starttime = ms_timestr2hptime (starttimestr)) == HPTERROR )
	    {
	      lprintf (0, "[%s] Error parsing time in DATA|FETCH: %s",
		       cinfo->hostname, starttimestr);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error parsing start time") )
		return -1;
	      
	      OKGO = 0;
	    }
	}
      
      /* If configuring a specific station selection */
      if ( OKGO && *state == 1 )
	{
	  if ( fields >= 1 )
	    {
	      /* Find the appropriate SLStaNode and store the requested ID and time */
	      if ( ! (stanode = GetStaNode (slinfo->stations, reqnet, reqsta)) )
		{
		  lprintf (0, "[%s] Error in GetStaNode for command DATA|FETCH",
			   cinfo->hostname);
		  
		  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error in GetStaNode") )
		    return -1;
		  
		  OKGO = 0;
		}
	      else
		{
		  stanode->packetid = startpacket;
		  stanode->datastart = starttime;
		  stanode->starttime = starttime;
		}
	    }
	  
	  if ( OKGO )
	    {
	      if ( ! slinfo->batch && SendReply (cinfo, "OK", 0) )
		return -1;
	      
	      /* If any stations use FETCH the connection is dial-up */
	      if ( ! strncasecmp (recvbuffer, "FETCH", 5) )
		slinfo->dialup = 1;
	    }
	  
	  *state = 0;
	}
      /* Otherwise this is a request to start data flow */
      else if ( OKGO )
	{
	  /* If no stations yet we are in all-station mode */
	  if ( slinfo->stationcount == 0 && fields >= 1 )
	    {
	      slinfo->startid = startpacket;
	      cinfo->starttime = starttime;
	    }
	  
	  /* If FETCH the connection is dial-up */
	  if ( ! strncasecmp (recvbuffer, "FETCH", 5) )
	    slinfo->dialup = 1;
	  
	  /* Configure ring and start data flow */
	  *state = 2;
	}
    }  /* End of DATA|FETCH */
  
  /* TIME [begin_time [end_time]] - Request data in time window */
  else if ( ! strncasecmp (recvbuffer, "TIME", 4) )
    {
      OKGO = 1;
      
      /* Parse start and end time from request */
      starttimestr[0] = '\0';
      endtimestr[0] = '\0';
      fields = sscanf (recvbuffer, "%*s %50s %50s %c",
		       starttimestr, endtimestr, &junk);
      
      /* Make sure we got start time and optionally end time */
      if ( fields <= 0 || fields > 2 )
	{
	  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "TIME command requires 1 or 2 arguments") )
	    return -1;
	  
	  OKGO = 0;
	}
      
      /* Convert start time string */
      if ( OKGO && fields >= 1 )
	{
          /* Change commas to dashes for parsing routine */
          while ( (ptr = strchr (starttimestr, ',')) )
            *ptr = '-';
	  
	  if ( (starttime = ms_timestr2hptime (starttimestr)) == HPTERROR )
	    {
	      lprintf (0, "[%s] Error parsing start time in TIME: %s",
		       cinfo->hostname, starttimestr);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error parsing start time") )
		return -1;
	      
	      OKGO = 0;
	    }
	}
      
      /* Convert end time string if supplied */
      if ( OKGO && fields == 2 )
	{
          /* Change commas to dashes for parsing routine */
          while ( (ptr = strchr (endtimestr, ',')) )
            *ptr = '-';
	  
	  if ( (endtime = ms_timestr2hptime (endtimestr)) == HPTERROR )
	    {
	      lprintf (0, "[%s] Error parsing end time in TIME: %s",
		       cinfo->hostname, endtimestr);
	      
	      if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error parsing end time") )
		return -1;
	      
	      OKGO = 0;
	    }
	}
      
      /* If configuring a specific station selection */
      if ( OKGO && *state == 1 )
	{
	  if ( fields >= 1 )
	    {
	      /* Find the appropriate SLStaNode and store the requested times */
	      if ( ! (stanode = GetStaNode (slinfo->stations, reqnet, reqsta)) )
		{
		  lprintf (0, "[%s] Error in GetStaNode for command TIME",
			   cinfo->hostname);
		  
		  if ( ! slinfo->batch && SendReply (cinfo, "ERROR", "Error in GetStaNode") )
		    return -1;
		  
		  OKGO = 0;
		}
	      else
		{
		  stanode->starttime = starttime;
		  stanode->endtime = endtime;
		}
	    }
	  
	  if ( OKGO )
	    {
	      if ( ! slinfo->batch && SendReply (cinfo, "OK", 0) )
		return -1;
	    }
	  
	  *state = 0;
	}
      /* Otherwise this is a request to start data flow */
      else if ( OKGO )
	{
	  /* If no stations yet we are in all-station mode */
	  if ( slinfo->stationcount == 0 && fields >= 1 )
	    {
	      cinfo->starttime = starttime;
	      cinfo->endtime = endtime;
	    }
	  
	  /* Configure ring and start data flow */
	  *state = 2;
	}
    }  /* End of TIME */
  
  /* END - Stop negotiating, send data */
  else if ( ! strncasecmp (recvbuffer, "END", 3) )
    {
      /* Configure ring and start data flow */
      *state = 2;
    }
  
  /* BYE - End connection */
  else if ( ! strncasecmp (recvbuffer, "BYE", 3) )
    {
      return -1;
    }
  
  /* Unrecognized command */
  else
    {
      lprintf (1, "[%s] Unrecognized command: %.10s",
	       cinfo->hostname, recvbuffer);
      
      if ( SendReply (cinfo, "ERROR", "Unrecognized command") )
	return -1;
    }
  
  return 0;
} /* End of HandleNegotiation */


/***************************************************************************
 * HandleInfo:
 *
 * Handle SeedLink INFO request.  Protocol INFO levels are:
 * ID, CAPABILITIES, STATIONS, STREAMS, GAPS, CONNECTIONS, ALL
 *
 * Levels GAPS and ALL are not supported.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo (char *recvbuffer, ClientInfo *cinfo, char state)
{
  SLInfo *slinfo = (SLInfo *) cinfo->extinfo;
  mxml_node_t *xmldoc = 0;
  mxml_node_t *seedlink = 0;
  char   string[200];
  char  *xmlstr = 0;
  int    xmllength;
  char  *level = 0;
  int    infolevel = 0;
  char   errflag = 0;

  char *record = 0;
  struct fsdh_s *fsdh;
  struct blkt_1000_s *b1000;
  uint16_t ushort;
  
  if ( ! strncasecmp (recvbuffer, "INFO", 4) )
    {
      /* Set level pointer to start of level identifier */
      level = recvbuffer + 4;
      
      /* Skip any spaces between INFO and level identifier */
      while ( *level == ' ' )
	level++;
    }
  else if ( *level == '\0' || *level == '\r' || *level == '\n' )
    {
      lprintf (0, "[%s] HandleInfo: INFO specified without a level", cinfo->hostname);
      return -1;
    }
  else
    {
      lprintf (0, "[%s] HandleInfo cannot detect INFO", cinfo->hostname);
      return -1;
    }
  
  /* Allocate miniSEED record buffer */
  if ( (record = calloc (1, SLRECSIZE)) == NULL )
    {
      lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
      return -1;
    }
  
  /* Initialize the XML response structure */
  if ( ! (xmldoc = mxmlNewXML ("1.0")) )
    {
      lprintf (0, "[%s] Error creating XML document", cinfo->hostname);
      if ( record )
        free (record);
      return -1;
    }
  
  /* Create seedlink XML element */
  if ( ! (seedlink = mxmlNewElement (xmldoc, "seedlink")) )
    {
      lprintf (0, "[%s] Error creating seedlink XML element", cinfo->hostname);
      if ( xmldoc )
        mxmlRelease (xmldoc);
      if ( record )
        free (record);
      return -1;
    }
  
  /* Convert server start time to YYYY-MM-DD HH:MM:SS */
  ms_hptime2mdtimestr (serverstarttime, string, 0);
  
  /* All responses, even the error response contain these attributes */
  mxmlElementSetAttr (seedlink, "software", SLSERVERVER);
  mxmlElementSetAttr (seedlink, "organization", serverid);
  mxmlElementSetAttr (seedlink, "started", string);
  
  /* Parse INFO request to determine level */
  if ( ! strncasecmp (level, "ID", 2) )
    {
      /* This is used to "ping" the server so only report at high verbosity */
      lprintf (2, "[%s] Received INFO ID request", cinfo->hostname);
      infolevel = SLINFO_ID;
    }
  else if ( ! strncasecmp (level, "CAPABILITIES", 12) )
    {
      lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);
      infolevel = SLINFO_CAPABILITIES;
    }
  else if ( ! strncasecmp (level, "STATIONS", 8) )
    {
      if ( state >= 2 )
	{
	  lprintf (0, "[%s] Received mid-stream INFO STATIONS request, ignoring", cinfo->hostname);
	  errflag = 1;
	}
      else
	{
	  lprintf (1, "[%s] Received INFO STATIONS request", cinfo->hostname);
	  infolevel = SLINFO_STATIONS;
	}
    }
  else if ( ! strncasecmp (level, "STREAMS", 7) )
    {
      if ( state >= 2 )
	{
	  lprintf (0, "[%s] Received mid-stream INFO STREAMS request, ignoring", cinfo->hostname);
	  errflag = 1;
	}
      else
	{
	  lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);
	  infolevel = SLINFO_STREAMS;
	}
    }
  else if ( ! strncasecmp (level, "GAPS", 7) )
    {
      lprintf (1, "[%s] Received INFO GAPS request, unsupported", cinfo->hostname);
      errflag = 1;
    }
  else if ( ! strncasecmp (level, "CONNECTIONS", 11) )
    {
      lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);
      infolevel = SLINFO_CONNECTIONS;
    }
  else if ( ! strncasecmp (level, "ALL", 3) )
    {
      lprintf (1, "[%s] Received INFO ALL request, unsupported", cinfo->hostname);
      errflag = 1;
    }
  /* Unrecognized INFO request */
  else
    {
      lprintf (0, "[%s] Unrecognized INFO level: %s", cinfo->hostname, level);
      errflag = 1;
    }
  
  /* Add contents to the XML structure depending on info level */
  
  /* CAPABILITIES */
  if ( infolevel == SLINFO_CAPABILITIES )
    {
      int idx;
      mxml_node_t *capability;
      char *caps[10] = {"dialup","multistation", "window-extraction", "info:id",
			"info:capabilities", "info:stations", "info:streams",
			"info:gaps", "info:connections", "info:all"};
      
      lprintf (1, "[%s] Received INFO CAPABILITIES request", cinfo->hostname);
      infolevel = SLINFO_CAPABILITIES;
      
      for (idx=0; idx < 10; idx++)
	{
	  if ( ! (capability = mxmlNewElement (seedlink, "capability")) )
	    {
	      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	      errflag = 1;
	    }
	  
	  mxmlElementSetAttr (capability, "name", caps[idx]);
	}
    }  /* End of CAPABILITIES request processing */
  /* STATIONS */
  else if ( infolevel == SLINFO_STATIONS )
    {
      mxml_node_t *station;
      Stack *streams;
      RingStream *stream;
      
      RBTree *netsta;
      Stack *netstas;
      SLNetStaNode *netstanode;
      RBNode *tnode;

      char net[10], sta[10];
      
      /* Get copy of streams as a Stack */
      if ( ! (streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)) )
	{
	  lprintf (0, "[%s] Error getting streams", cinfo->hostname);
	  errflag = 1;
	}
      else
	{
	  netsta = RBTreeCreate (StaKeyCompare, free, FreeNetStaNode);
	  
	  /* Loop through the streams and build a network-station tree */
	  while ( (stream = (RingStream *) StackPop(streams)) )
	    {
	      /* Split the streamid to get the network and station codes */
	      if ( SplitStreamID (stream->streamid, net, sta, 0, 0, 0) )
		{
		  lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
		  break;
		}
	      
	      /* Find or create new netsta entry */
	      netstanode = GetNetStaNode (netsta, net, sta);
	      
	      /* Check and update network-station values */
	      if ( netstanode )
		{
		  if ( ! netstanode->earliestdstime || netstanode->earliestdstime > stream->earliestdstime )
		    {
		      netstanode->earliestdstime = stream->earliestdstime;
		      netstanode->earliestid = stream->earliestid;
		    }
		  if ( ! netstanode->latestdstime || netstanode->latestdstime < stream->latestdstime )
		    {
		      netstanode->latestdstime = stream->latestdstime;
		      netstanode->latestid = stream->latestid;
		    }
		}
	      else
		{
		  lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
		  errflag = 1;
		  break;
		}

	      /* Free the popped Stack entry */
	      free (stream);
	    }
	  
	  /* Free the remaining stream Stack memory */
	  StackDestroy (streams, free);
	  
	  /* Create Stack of network-station entries */
	  netstas = StackCreate ();
	  RBBuildStack (netsta, netstas);
	  
	  /* Loop through array entries adding "station" elements */
	  while ( (tnode = (RBNode *) StackPop(netstas)) )
	    {
	      netstanode = (SLNetStaNode *) tnode->data;
	      
	      if ( ! (station = mxmlNewElement (seedlink, "station")) )
		{
		  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
		  errflag = 1;
		}
	      else
		{
		  mxmlElementSetAttr (station, "name", netstanode->sta);
		  mxmlElementSetAttr (station, "network", netstanode->net);
		  mxmlElementSetAttrf (station, "description", "%s Station", netstanode->net);
		  mxmlElementSetAttrf (station, "begin_seq", "%06"PRIX64, netstanode->earliestid);
		  mxmlElementSetAttrf (station, "end_seq", "%06"PRIX64, netstanode->latestid);
		}
	    }
	  
	  /* Cleanup network-station structures */
	  RBTreeDestroy (netsta);
	  StackDestroy (netstas, 0);
	}
    } /* End of STATIONS request processing */
  /* STREAMS */
  else if ( infolevel == SLINFO_STREAMS )
    {
      mxml_node_t *station;
      mxml_node_t *streamxml;
      Stack *streams;
      RingStream *stream;
      
      RBTree *netsta;
      Stack *netstas;
      SLNetStaNode *netstanode;
      RBNode *tnode;
      
      char net[10], sta[10], loc[10], chan[10];
      
      /* Get streams as a Stack (this is copied data) */
      if ( ! (streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)) )
	{
	  lprintf (0, "[%s] Error getting streams", cinfo->hostname);
	  errflag = 1;
	}
      else
	{
	  netsta = RBTreeCreate (StaKeyCompare, free, FreeNetStaNode);
	  
	  /* Loop through the streams and build a network-station tree with associated streams */
	  while ( (stream = (RingStream *) StackPop(streams)) )
	    {
	      /* Split the streamid to get the network and station codes */
	      if ( SplitStreamID (stream->streamid, net, sta, 0, 0, 0) )
		{
		  lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
		  break;
		}
	      
	      /* Find or create new netsta entry */
	      netstanode = GetNetStaNode (netsta, net, sta);
	      
	      if ( netstanode )
		{
		  /* Add stream to associated streams stack */
		  StackUnshift (netstanode->streams, stream);
		  
		  /* Check and update network-station earliest/latest values */
		  if ( ! netstanode->earliestdstime || netstanode->earliestdstime > stream->earliestdstime )
		    {
		      netstanode->earliestdstime = stream->earliestdstime;
		      netstanode->earliestid = stream->earliestid;
		    }
		  if ( ! netstanode->latestdstime || netstanode->latestdstime < stream->latestdstime )
		    {
		      netstanode->latestdstime = stream->latestdstime;
		      netstanode->latestid = stream->latestid;
		    }
		}
	      else
		{
		  lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
		  errflag = 1;
		  break;
		}
	    }
	  
	  /* Create Stack of network-station entries */
	  netstas = StackCreate ();
	  RBBuildStack (netsta, netstas);
	  
	  /* Traverse network-station entries creating "station" elements */
	  while ( (tnode = (RBNode *) StackPop(netstas)) )
	    {
	      netstanode = (SLNetStaNode *) tnode->data;
	      
	      if ( ! (station = mxmlNewElement (seedlink, "station")) )
		{
		  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
		  errflag = 1;
		}
	      else
		{
		  mxmlElementSetAttr (station, "name", netstanode->sta);
		  mxmlElementSetAttr (station, "network", netstanode->net);
		  mxmlElementSetAttrf (station, "description", "%s Station", netstanode->net);
		  mxmlElementSetAttrf (station, "begin_seq", "%06"PRIX64, netstanode->earliestid);
		  mxmlElementSetAttrf (station, "end_seq", "%06"PRIX64, netstanode->latestid);
		  mxmlElementSetAttr (station, "stream_check", "enabled");
		  
		  /* Traverse associated streams to find locations and channels creating "stream" elements */
		  while ( (stream = (RingStream *) StackPop(netstanode->streams)) )
		    {
		      /* Split the streamid to get the network, station, location & channel codes */
		      if ( SplitStreamID (stream->streamid, net, sta, loc, chan, 0) )
			{
			  lprintf (0, "[%s] Error splitting stream ID: %s", cinfo->hostname, stream->streamid);
			  break;
			}
		      
		      if ( ! (streamxml = mxmlNewElement (station, "stream")) )
			{
			  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
			  errflag = 1;
			}
		      else
			{
			  mxmlElementSetAttr (streamxml, "location", loc);
			  mxmlElementSetAttr (streamxml, "seedname", chan);
			  mxmlElementSetAttr (streamxml, "type", "D");
			  
			  /* Convert earliest and latest times to YYYY-MM-DD HH:MM:SS and add them */
			  ms_hptime2mdtimestr (stream->earliestdstime, string, 0);
			  mxmlElementSetAttr (streamxml, "begin_time", string);
			  ms_hptime2mdtimestr (stream->latestdetime, string, 0);
			  mxmlElementSetAttr (streamxml, "end_time", string);
			}
		      
		      /* Free the RingStream entry, this is a copy from GetStreamsStack() above */
		      free (stream);
		    }
		}
	    }
	  
	  /* Cleanup network-station structures */
	  RBTreeDestroy (netsta);
	  StackDestroy (netstas, 0);
	  StackDestroy (streams, 0);
	}
    } /* End of STREAMS request processing */
  /* CONNECTIONS */
  else if ( infolevel == SLINFO_CONNECTIONS )
    {
      struct cthread *loopctp;
      mxml_node_t *station, *connection, *window, *selector;
      ClientInfo *tcinfo;
      SLInfo *tslinfo;
      
      /* Loop through client connections, lock client list while looping  */
      pthread_mutex_lock (&cthreads_lock);
      loopctp = cthreads;
      while ( loopctp )
	{
	  /* Skip if client thread is not in ACTIVE state */
	  if ( ! ( loopctp->td->td_flags & TDF_ACTIVE) )
	    {
	      loopctp = loopctp->next;	      
	      continue;
	    }
	  
	  tcinfo = (ClientInfo *) loopctp->td->td_prvtptr;
	  tslinfo = (tcinfo->type == SEEDLINK_CLIENT) ? (SLInfo *) tcinfo->extinfo : 0;
	  
	  if ( ! (station = mxmlNewElement (seedlink, "station")) )
	    {
	      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	      errflag = 1;
	    }
	  else
	    {
	      mxmlElementSetAttr (station, "name", "CLIENT");
	      if ( tcinfo->type == DATALINK_CLIENT )
		mxmlElementSetAttr (station, "network", "DL");
	      else if ( tcinfo->type == SEEDLINK_CLIENT )
		mxmlElementSetAttr (station, "network", "SL");
	      else
		mxmlElementSetAttr (station, "network", "RS");
	      mxmlElementSetAttr (station, "description", "Ringserver Client");
	      mxmlElementSetAttrf (station, "begin_seq", "%06"PRIX64, tcinfo->ringparams->earliestid);
	      mxmlElementSetAttrf (station, "end_seq", "%06"PRIX64, tcinfo->ringparams->latestid);
	      mxmlElementSetAttr (station, "stream_check", "enabled");
	      
	      /* Add a "connection" element */
	      if ( ! (connection = mxmlNewElement (station, "connection")) )
		{
		  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
		  errflag = 1;
		}
	      else
		{
		  mxmlElementSetAttr (connection, "host", tcinfo->ipstr);
		  mxmlElementSetAttr (connection, "port", tcinfo->portstr);
		  
		  /* Convert connect time to YYYY-MM-DD HH:MM:SS */
		  ms_hptime2mdtimestr (tcinfo->conntime, string, 0);
		  mxmlElementSetAttr (connection, "ctime", string);
		  mxmlElementSetAttr (connection, "begin_seq", "0");
		  
		  if ( tcinfo->reader->pktid <= 0 )
		    mxmlElementSetAttr (connection, "current_seq", "unset");
		  else
		    mxmlElementSetAttrf (connection, "current_seq", "%06"PRIX64, tcinfo->reader->pktid);
		  
		  mxmlElementSetAttr (connection, "sequence_gaps", "0");
		  mxmlElementSetAttrf (connection, "txcount", "%"PRIu64, tcinfo->txpackets[0]);
		  mxmlElementSetAttrf (connection, "totBytes", "%"PRIu64, tcinfo->txbytes[0]);
		  mxmlElementSetAttr (connection, "begin_seq_valid", "yes");
		  mxmlElementSetAttr (connection, "realtime", "yes");
		  mxmlElementSetAttr (connection, "end_of_data", "no");
		  
		  /* Add "window" element if start or end times are set */
		  if ( tcinfo->starttime || tcinfo->endtime )
		    {
		      if ( ! (window = mxmlNewElement (connection, "window")) )
			{
			  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
			  errflag = 1;
			}
		      else
			{
			  /* Convert start & end time to YYYY-MM-DD HH:MM:SS or "unset" */
			  if ( tcinfo->starttime )
			    ms_hptime2mdtimestr (tcinfo->starttime, string, 0);
			  else
			    strncpy (string, "unset", sizeof(string));
			  
			  mxmlElementSetAttr (window, "begin_time", string);
			  
			  if ( tcinfo->endtime )
			    ms_hptime2mdtimestr (tcinfo->endtime, string, 0);
			  else
			    strncpy (string, "unset", sizeof(string));
			  
			  mxmlElementSetAttr (window, "end_time", string);
			}
		    }
		  
		  /* Add "selector" element if match or reject strings are set */
		  if ( tcinfo->matchstr || tcinfo->rejectstr )
		    {
		      if ( ! (selector = mxmlNewElement (connection, "selector")) )
			{
			  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
			  errflag = 1;
			}
		      else
			{
			  if ( tslinfo && tslinfo->selectors )
			    mxmlElementSetAttr (selector, "pattern", tslinfo->selectors);
			  if ( tcinfo->matchstr )
			    mxmlElementSetAttr (selector, "match", tcinfo->matchstr);
			  if ( tcinfo->rejectstr )
			    mxmlElementSetAttr (selector, "reject", tcinfo->rejectstr);
			}
		    }
		}
	    }
	  
	  loopctp = loopctp->next;
	}
      pthread_mutex_unlock (&cthreads_lock);
      
    } /* End of CONNECTIONS request processing */
  
  /* Convert to XML string, pack into Mini-SEED and send to client */
  if ( xmldoc )
    {
      /* Do not wrap the output XML */
      mxmlSetWrapMargin (0);
      
      /* Convert to XML string */
      if ( ! (xmlstr = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK)) )
	{
	  lprintf (0, "[%s] Error with mxmlSaveAllocString()", cinfo->hostname);
	  if ( xmldoc )
	    mxmlRelease (xmldoc);
          if ( record )
            free (record);
	  return -1;
	}
      
      /* Trim final newline character if present */
      xmllength = strlen (xmlstr);
      if ( xmlstr[xmllength-1] == '\n' )
	{
	  xmlstr[xmllength-1] = '\0';
	  xmllength--;
	}
      
      /* Set up encapsulating Mini-SEED record template */
      fsdh = (struct fsdh_s *)record;
      
      /* Create Mini-SEED header primitive for 512-byte, ASCII encoded,
       * host byte-order, 0 sample rate record, including a Blockette 1000.
       * This leaves 456 bytes in each record for ASCII data. */
      fsdh->dataquality = 'D';
      fsdh->reserved = ' ';
      memcpy (fsdh->station, "INFO ", 5);
      memcpy (fsdh->location, "  ", 2);
      if ( errflag )
	memcpy (fsdh->channel, "ERR", 3);
      else
	memcpy (fsdh->channel, "INF", 3);
      memcpy (fsdh->network, "SL", 2);
      ms_hptime2btime (HPnow(), &(fsdh->start_time));
      fsdh->samprate_fact = 0;
      fsdh->samprate_mult = 0;
      fsdh->act_flags = 0;
      fsdh->io_flags = 0;
      fsdh->dq_flags = 0;
      fsdh->numblockettes = 1;
      fsdh->time_correct = 0;
      fsdh->data_offset = 56;
      fsdh->blockette_offset = 48;
      
      /* Create Blockette 1000 header at byte 48 */
      ushort = 1000;
      memcpy (record+48, &ushort, 2);
      ushort = 0;
      memcpy (record+50, &ushort, 2);
      
      /* Create Blockette 1000 body at byte 52 */
      b1000 = (struct blkt_1000_s *)(record + 52);
      b1000->encoding = DE_ASCII;
      b1000->byteorder = 1;
      b1000->reclen = 9;
      b1000->reserved = 0;
      
      /* Make sure data records are in big-endian byte order */
      if ( ! ms_bigendianhost() )
	{
	  MS_SWAPBTIME (&fsdh->start_time);
	  ms_gswap2 (&fsdh->data_offset);
	  ms_gswap2 (&fsdh->blockette_offset);
	  
	  /* Blockette 1000 type and next values */
	  ms_gswap2 (record+48);
	  ms_gswap2 (record+50);
	}
      
      /* Pack all XML into 512-byte records and send to client */
      if ( ! cinfo->socketerr )
	{
	  char seqnumstr[7];
	  int seqnum = 1;
	  int offset = 0;
	  int nsamps;
	  
	  while ( offset < xmllength && ! cinfo->socketerr )
	    {
	      nsamps = ( (xmllength - offset) > 456 ) ? 456 : (xmllength - offset);
	      
	      /* Update sequence number and number of samples */
	      snprintf (seqnumstr, 7, "%06d", seqnum);
	      memcpy (fsdh->sequence_number, seqnumstr, 6);
	      
	      fsdh->numsamples = nsamps;
	      if ( ! ms_bigendianhost() )
		ms_gswap2 (&fsdh->numsamples);
	      
	      /* Copy XML data into record */
	      memcpy (record+56, xmlstr+offset, nsamps);
	      
	      /* Pad any remaining record bytes with NULLs */
	      if ( nsamps+56 < 512 )
		memset (record+56+nsamps, 0, 512-56-nsamps);
	      
	      /* Roll-over sequence number */
	      if ( seqnum >= 999999)
		seqnum = 1;
	      else
		seqnum++;
	      
	      /* Update offset */
	      offset += nsamps;
	      
	      /* Set termination flag if this is the last record */
	      if ( offset == xmllength )
		slinfo->terminfo = 1;
	      else
		slinfo->terminfo = 0;
	      
	      /* Send INFO record to client, blind toss */
	      SendInfoRecord (record, SLRECSIZE, cinfo);
	    }
	}
    }
  
  /* Free allocated memory */
  if ( xmldoc )
    mxmlRelease (xmldoc);
  
  if ( xmlstr )
    free (xmlstr);
  
  if ( record )
    free (record);
  
  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleInfo */


/***********************************************************************
 * RecvLine:
 *
 * Read characters from a socket until '\r' (carriage return) is
 * found, followed by an optional '\n' (newline) or '\0' (NULL), or
 * the maximum buffer length is reached and place them into buffer.
 * The resulting string in buffer will always be NULL terminated.
 * 'ident' is a string to include in error messages for
 * identification, usually the address of the remote server.
 *
 * This routine can handle fragmented receives after some data has
 * been read.  If no data has been read and no data is available from
 * the socket this routine will return immediately.
 *
 * In order to handle variations in the SeedLink command protocol a
 * single character pre-buffer is used for receiving data.  The last
 * character in the client specific receive buffer is used for this
 * pre-buffer.  As long as this routine is the only one receiving data
 * for this client this scheme is maintained wholly within.
 *
 * Return number of characters read on success, 0 if no data is
 * available, -1 on connection shutdown and -2 on error.
 ***********************************************************************/
static int
RecvLine (ClientInfo *cinfo)
{
  int nrecv;
  int nread = 0;
  int crrecv = 0;
  
  /* Single character pre-buffer is last character of the receive buffer */
  char *cptr = cinfo->recvbuf + cinfo->recvbuflen-1;
  
  /* Buffer pointer tracks next input location */
  char *bptr = cinfo->recvbuf;
  
  fd_set readset;
  struct timeval timeout;
  int selret;
  
  if ( ! bptr )
    return -2;
  
  /* Add any previously read character to the buffer */
  if ( *cptr != '\0' && *cptr != '\n' && *cptr != '\r' )
    {
      *bptr = *cptr;
      bptr++;
      nread++;
    }
  
  /* Recv a character at a time until \r or buflen is reached */
  while ( nread < (cinfo->recvbuflen-2) )
    {
      if ( (nrecv = recv(cinfo->socket, cptr, 1, 0)) < 0 )
        {
          /* The only acceptable error is EAGAIN (no data on non-blocking) */
          if ( nrecv == -1 && errno != EAGAIN )
            {
              lprintf (0, "[%s] Error recving data from client: %s",
                       cinfo->hostname, strerror(errno));
              return -2;
            }
          /* Exit if no data is available and we haven't received anything yet */
          else if ( nrecv == -1 && errno == EAGAIN && bptr == cinfo->recvbuf )
            {
              return 0;
            }
          /* Done if no data is available and a '\r' has been recevied */
          else if ( nrecv == -1 && errno == EAGAIN && crrecv == 1 )
            {
              break;
            }
	  /* Throttle when no data for a non-blocking socket */
	  else if ( nrecv == -1 && errno == EAGAIN )
            {
	      /* Configure the read descriptor set with only our client socket */
	      FD_ZERO(&readset);
	      FD_SET(cinfo->socket, &readset);
	      
	      /* Timeout 10 seconds */
	      timeout.tv_sec = 10;
	      timeout.tv_usec = 0;
	      
	      selret = select (cinfo->socket+1, &readset, NULL, NULL, &timeout);
	      
	      if ( selret == 0 )
		{
		  lprintf (0, "[%s] Timeout receiving SeedLink command: %.*s",
			   cinfo->hostname, nread, cinfo->recvbuf);
		  return -2;
		}
	      else if ( selret == -1 && errno != EINTR )
		{
		  lprintf (0, "[%s] Error with select: %s",
			   cinfo->hostname, strerror(errno));
		  return -2;
		}
	    }
        }
      
      /* Peer completed an orderly shutdown */
      if ( nrecv == 0 )
        {
          return -1;
        }
      
      /* Done if a '\r' has already been received */
      if ( crrecv == 1 )
        {
          break;
        }
      
      /* Mark flag if a '\r' is received */
      if ( nrecv > 0 && *cptr == '\r' )
        {
          crrecv = 1;
        }
      
      /* Trap door if '\n' or '\0' is received */
      if ( nrecv > 0 && (*cptr == '\n' || *cptr == '\0') )
        {
          break;
        }
      
      /* Copy byte into buffer and update recv pointer and byte count */
      if ( nrecv > 0 )
        {
	  *bptr = *cptr;
          bptr++;
          nread++;
        }
      
      /* Check for a full buffer, assume an invalid SeedLink command */
      if ( nread == (cinfo->recvbuflen-2) )
	{
	  lprintf (0, "[%s] Bad SeedLink command (too long): '%.*s'",
		   cinfo->hostname, nread, cinfo->recvbuf);
	  return -2;
	}
    }
  
  /* Make sure buffer is NULL terminated */
  *bptr = '\0';
  
  /* Make sure \n and \r are trimmed */
  if ( (bptr = memchr(cinfo->recvbuf, '\n', nread)) )
    *bptr = '\0';
  if ( (bptr = memchr(cinfo->recvbuf, '\r', nread)) )
    *bptr = '\0';
  
  return nread;
}  /* End of RecvLine() */


/***************************************************************************
 * SendReply:
 *
 * Send a short reply (to a command) to the client, optionally
 * including the extended message.
 *
 * Each complete reply is terminated with a "\r\n" sequence, if
 * included the extended reply message is separated from the initial
 * reply with a single "\r" character.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendReply (ClientInfo *cinfo, char *reply, char *extreply)
{
  SLInfo *slinfo = (SLInfo *) cinfo->extinfo;
  char sendstr[100];
  
  /* Create reply string to send */
  if ( slinfo->extreply && extreply )
    snprintf (sendstr, sizeof(sendstr), "%s\r%s\r\n", reply, extreply);
  else
    snprintf (sendstr, sizeof(sendstr), "%s\r\n", reply);
  
  /* Send the reply */
  if ( SendData (cinfo, sendstr, strlen(sendstr)) )
    return -1;
  
  return 0;
}  /* End of SendReply() */


/***************************************************************************
 * SendData:
 *
 * send() 'buflen' bytes from 'buffer' to 'cinfo->socket'.
 *
 * Socket is set to blocking during the send operation.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendData (ClientInfo *cinfo, void *buffer, size_t buflen)
{
  int sockflags, blockflags;
  
  if ( ! buffer )
    return -1;
  
  /* Clear non-blocking flag from socket flags */
  sockflags = blockflags = fcntl(cinfo->socket, F_GETFL, 0);
  blockflags &= ~O_NONBLOCK;
  if ( fcntl(cinfo->socket, F_SETFL, blockflags) == -1 )
    {
      lprintf (0, "[%s] SendData(): Error clearing non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return -1;
    }
  
  if ( send (cinfo->socket, buffer, buflen, 0) < 0 )
    {
      /* EPIPE indicates a client disconnect, everything else is an error */
      if ( errno == EPIPE )
	{
	  cinfo->socketerr = 2;  /* Indicate an orderly shutdown */
	}
      else
	{
	  lprintf (0, "[%s] Error sending '%.*s': %s", cinfo->hostname,
		   strcspn ((char *) buffer, "\r\n"), (char *) buffer, 
		   strerror(errno));
	  cinfo->socketerr = 1;
	}
      
      return -1;
    }
  
  /* Update the time of the last packet exchange */
  cinfo->lastxchange = HPnow();
  
  /* Restore original socket flags */
  if ( fcntl(cinfo->socket, F_SETFL, sockflags) == -1 )
    {
      lprintf (0, "[%s] SendData(): Error setting non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return -1;
    }
  
  return 0;
}  /* End of SendData() */


/***************************************************************************
 * SendRecord:
 *
 * send() 'reclen' bytes from 'record' to 'cinfo->socket' and prefix
 * with an appropriate SeedLink header.
 *
 * Socket is set to blocking during the send operation.
 *
 * Returns 0 on success and -1 on error, the ClientInfo.socketerr value
 * is set on socket errors.
 ***************************************************************************/
static int
SendRecord (RingPacket *packet, char *record, int reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *)vcinfo;
  char buffer[SLRECSIZE+SLHEADSIZE];
  int sockflags, blockflags;
  int sendsize = 0;
  
  if ( ! record || ! vcinfo )
    return -1;
  
  /* Check that record is SLRECSIZE-bytes */
  if ( reclen != SLRECSIZE )
    {
      lprintf (0, "[%s] data record is not %d bytes as expected: %d",
	       cinfo->hostname, SLRECSIZE, reclen);
      return -1;
    }
  
  sendsize = SLHEADSIZE+SLRECSIZE;
  
  /* Check that sequence number is not too big */
  if ( packet->pktid > 0xFFFFFF )
    {
      lprintf (0, "[%s] sequence number too large for SeedLink: %d",
	       cinfo->hostname, packet->pktid);
    }
  
  /* Create SeedLink header: signature + sequence number */
  snprintf (buffer, SLHEADSIZE+1, "SL%06"PRIX64, packet->pktid);
  
  /* Add record to send buffer */
  memcpy (buffer+SLHEADSIZE, record, SLRECSIZE);
  
  /* Clear non-blocking flag from socket flags */
  sockflags = blockflags = fcntl(cinfo->socket, F_GETFL, 0);
  blockflags &= ~O_NONBLOCK;
  if ( fcntl(cinfo->socket, F_SETFL, blockflags) == -1 )
    {
      lprintf (0, "[%s] SendRecord(): Error clearing non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return -1;
    }

  /* Send SeedLink packet */
  if ( send (cinfo->socket, buffer, sendsize, 0) < 0 )
    {
      /* EPIPE indicates a client disconnect, everything else is an error */
      if ( errno == EPIPE )
	{
	  cinfo->socketerr = 2;  /* Indicate an orderly shutdown */
	}
      else
	{
	  lprintf (0, "[%s] Error sending record: %s",
		   cinfo->hostname, strerror(errno));
	  cinfo->socketerr = 1;
	}
      
      return -1;
    }
  
  /* Update the time of the last packet exchange */
  cinfo->lastxchange = HPnow();
  
  /* Restore original socket flags */
  if ( fcntl(cinfo->socket, F_SETFL, sockflags) == -1 )
    {
      lprintf (0, "[%s] SendRecord(): Error setting non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return -1;
    }
  
  return 0;
}  /* End of SendRecord() */


/***************************************************************************
 * SendInfoRecord:
 *
 * send() 'reclen' bytes from 'record' to 'cinfo->socket' and prefix
 * with an appropriate INFO SeedLink header.
 *
 * Socket is set to blocking during the send operation.
 *
 * The ClientInfo.socketerr value is set on socket errors.
 ***************************************************************************/
static void
SendInfoRecord (char *record, int reclen, void *vcinfo)
{
  ClientInfo *cinfo = (ClientInfo *) vcinfo;
  SLInfo *slinfo = (SLInfo *) cinfo->extinfo;
  char buffer[SLHEADSIZE+SLRECSIZE];
  int sockflags, blockflags;
  
  if ( ! record || ! vcinfo )
    return;
  
  /* Check that record is SLRECSIZE-bytes */
  if ( reclen != SLRECSIZE )
    {
      lprintf (0, "[%s] data record is not %d bytes: %d", SLRECSIZE, reclen);
      return;
    }
  
  /* Create INFO signature according to termination flag */
  if ( slinfo->terminfo )
    memcpy (buffer, "SLINFO  ", SLHEADSIZE);
  else
    memcpy (buffer, "SLINFO *", SLHEADSIZE);
  
  /* Add record to send buffer */
  memcpy (buffer+SLHEADSIZE, record, SLRECSIZE);
  
  /* Clear non-blocking flag from socket flags */
  sockflags = blockflags = fcntl(cinfo->socket, F_GETFL, 0);
  blockflags &= ~O_NONBLOCK;
  if ( fcntl(cinfo->socket, F_SETFL, blockflags) == -1 )
    {
      lprintf (0, "[%s] SendInfoRecord(): Error clearing non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return;
    }
  
  /* Send SeedLink INFO packet */
  if ( send (cinfo->socket, buffer, SLRECSIZE+SLHEADSIZE, 0) < 0 )
    {
      /* EPIPE indicates a client disconnect, everything else is an error */
      if ( errno == EPIPE )
	{
	  cinfo->socketerr = 2;  /* Indicate an orderly shutdown */
	}
      else
	{
	  lprintf (0, "[%s] Error sending INFO record: %s",
		   cinfo->hostname, strerror(errno));
	  cinfo->socketerr = 1;
	}
      
      return;
    }
  
  /* Update the time of the last packet exchange */
  cinfo->lastxchange = HPnow();
  
  /* Restore original socket flags */
  if ( fcntl(cinfo->socket, F_SETFL, sockflags) == -1 )
    {
      lprintf (0, "[%s] SendInfoRecord(): Error setting non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      cinfo->socketerr = 1;
      return;
    }
  
  return;
}  /* End of SendInfoRecord() */


/***************************************************************************
 * FreeStaNode:
 *
 * Free all memory associated with a SLStaNode.
 *
 ***************************************************************************/
static void
FreeStaNode (void *rbnode)
{
  SLStaNode *stanode = (SLStaNode *) rbnode;
  
  if ( stanode->selectors )
    free (stanode->selectors);
  
  free (rbnode);
  
  return;
}  /* End of FreeStaNode() */


/***************************************************************************
 * FreeNetStaNode:
 *
 * Free all memory associated with a SLNetStaNode.
 *
 ***************************************************************************/
static void
FreeNetStaNode (void *rbnode)
{
  SLNetStaNode *netstanode = (SLNetStaNode *) rbnode;
  
  if ( netstanode->streams )
    StackDestroy (netstanode->streams, free);
  
  free (rbnode);
  
  return;
}  /* End of FreeNetStaNode() */


/***************************************************************************
 * StaKeyCompare:
 *
 * Compare two station or channel binary tree keys passed as void pointers.
 *
 * Return 1 if a > b, -1 if a < b and 0 otherwise (e.g. equality).
 ***************************************************************************/
static int
StaKeyCompare (const void *a, const void *b)
{
  int cmpval;
  
  /* Compare network codes */
  cmpval = strcmp ( ((SLStaKey*)a)->net, ((SLStaKey*)b)->net );
  
  if ( cmpval > 0 )
    return 1;
  else if ( cmpval < 0 )
    return -1;
  
  /* Compare station codes */
  cmpval = strcmp ( ((SLStaKey*)a)->sta, ((SLStaKey*)b)->sta );
  
  if ( cmpval > 0 )
    return 1;
  else if ( cmpval < 0 )
    return -1;
  
  return 0;
}  /* End of StaKeyCompare() */


/***************************************************************************
 * GetStaNode:
 *
 * Search the specified binary tree for a given SLStaKey and return the
 * SLStaNode.  If the SLStaKey does not exist create it and add it to the
 * tree.
 *
 * Return a pointer to a SLStaNode or 0 for error.
 ***************************************************************************/
static SLStaNode*
GetStaNode (RBTree *tree, char *net, char *sta)
{
  SLStaKey   stakey;
  SLStaKey  *newstakey;
  SLStaNode *stanode = 0;
  RBNode  *rbnode;
  
  /* Create SLStaKey */
  strncpy (stakey.net, net, sizeof(stakey.net));
  strncpy (stakey.sta, sta, sizeof(stakey.sta));
  
  /* Search for a matching SLStaNode entry */
  if ( (rbnode = RBFind (tree, &stakey)) )
    {
      stanode = (SLStaNode *) rbnode->data;
    }
  else
    {
      if ( (newstakey = (SLStaKey *) malloc (sizeof(SLStaKey))) == NULL )
	{
	  lprintf (0, "GetStaNode: Error allocating new key");
	  return 0;
	}
      
      memcpy (newstakey, &stakey, sizeof(SLStaKey));
      
      if ( (stanode = (SLStaNode *) malloc (sizeof(SLStaNode))) == NULL )
	{
	  lprintf (0, "GetStaNode: Error allocating new node");
	  return 0;
	}
      
      stanode->starttime = HPTERROR;
      stanode->endtime   = HPTERROR;
      stanode->packetid  = 0;
      stanode->datastart = HPTERROR;
      stanode->selectors = NULL;
      
      RBTreeInsert (tree, newstakey, stanode, 0);
    }
  
  return stanode;
}  /* End of GetStaNode() */


/***************************************************************************
 * GetNetStaNode:
 *
 * Search the specified binary tree for a given SLStaKey and return
 * the SLNetStaNode.  If the SLStaKey does not exist create it and add
 * it to the tree.
 *
 * Return a pointer to a SLNetStaNode or 0 for error.
 ***************************************************************************/
static SLNetStaNode*
GetNetStaNode (RBTree *tree, char *net, char *sta)
{
  SLStaKey   stakey;
  SLStaKey  *newstakey;
  SLNetStaNode *netstanode = 0;
  RBNode  *rbnode;
  
  /* Create SLStaKey */
  strncpy (stakey.net, net, sizeof(stakey.net));
  strncpy (stakey.sta, sta, sizeof(stakey.sta));
  
  /* Search for a matching SLNetStaNode entry */
  if ( (rbnode = RBFind (tree, &stakey)) )
    {
      netstanode = (SLNetStaNode *) rbnode->data;
    }
  else
    {
      if ( (newstakey = (SLStaKey *) malloc (sizeof(SLStaKey))) == NULL )
	{
	  lprintf (0, "GetStaNode: Error allocating new key");
	  return 0;
	}
      
      memcpy (newstakey, &stakey, sizeof(SLStaKey));
      
      if ( (netstanode = (SLNetStaNode *) calloc (1, sizeof(SLNetStaNode))) == NULL )
	{
	  lprintf (0, "GetNetStaNode: Error allocating new node");
	  return 0;
	}
      
      strncpy (netstanode->net, net, sizeof(netstanode->net));
      strncpy (netstanode->sta, sta, sizeof(netstanode->sta));

      /* Initialize Stack of associated streams */
      netstanode->streams = StackCreate();
      
      RBTreeInsert (tree, newstakey, netstanode, 0);
    }
  
  return netstanode;
}  /* End of GetNetStaNode() */


/***************************************************************************
 * StationToRegex:
 *
 * Update match and reject regexes for the specified network, station
 * and selector list (comma delimited).
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
StationToRegex (char *net, char *sta, char *selectors,
		char **matchregex, char **rejectregex)
{
  char *selectorlist;
  char *selector, *nextselector;
  int matched;
  
  if ( ! matchregex || ! rejectregex )
    {
      lprintf (0, "Pointer-to-pointer match/reject regex cannot be NULL");
      return -1;
    }
  
  /* If a selector list is specified traverse it and update regexes */
  if ( selectors )
    {
      /* Copy selectors list so we can modify it while parsing */
      if ( ! (selectorlist = strdup (selectors)) )
	{
	  lprintf (0, "Cannot allocate memory to duplicate selectors");
	  return -1;
	}
      
      /* Track count of matching selectors */
      matched = 0;
      
      /* Traverse list of comma separated selectors */
      selector = selectorlist;
      while ( selector )
	{
	  /* Find deliminting comma */
	  nextselector = strchr (selector, ',');
	  
	  /* Terminate string at comma and set pointer for next selector */
	  if ( nextselector )
	    *nextselector++ = '\0';
	  
	  /* Handle negated selector */
	  if ( selector[0] == '!' )
	    {
	      /* If no matching (non-negated) selectors are included a negation selector
		 implies all data for the specified station with the execption of the
		 negated selection, therefore we need to match all channels from the
		 station and then reject those in the negated selector */
	      if ( ! matched && net && sta )
		{
		  if ( SelectToRegex (net, sta, NULL, matchregex) )
		    {
		      lprintf (0, "Error with SelectToRegex");
		      if ( selectorlist )
			free (selectorlist);
		      return -1;
		    }
		  
		  matched++;
		}
	      
	      if ( SelectToRegex (net, sta, &selector[1], rejectregex) )
		{
		  lprintf (0, "Error with SelectToRegex");
		  if ( selectorlist )
		    free (selectorlist);
		  return -1;
		}
	    }
	  /* Handle regular selector */
	  else
	    {
	      if ( SelectToRegex (net, sta, selector, matchregex) )
		{
		  lprintf (0, "Error with SelectToRegex");
		  if ( selectorlist )
		    free (selectorlist);
		  return -1;
		}
	      
	      matched++;
	    }
	  
	  selector = nextselector;
	}
      
      if ( selectorlist )
	free (selectorlist);
    }
  /* Otherwise update regex for station without selectors */
  else
    {
      if ( SelectToRegex (net, sta, NULL, matchregex) )
	{
	  lprintf (0, "Error with SelectToRegex");
	  return -1;
	}
    }
  
  return 0;
}  /* End of StationToRegex() */


/***************************************************************************
 * SelectToRegex:
 *
 * Create a regular expression for ring stream IDs (NET_STA_LOC_CHAN)
 * for the specified network, station and SeedLink selector and add it
 * to the string specified by regex, expanding it as needed.  The
 * regex string will only be expanded up to a maximum of SLMAXREGEXLEN
 * bytes.
 *
 * Each regex in the final string is separated with a "|" (OR) and
 * encapsulated with begin "^" and end "$" characters.
 *
 * Mapping is as follows:
 *   '?' -> '.'
 *   '*' -> '.*'
 *
 * The DECOTL subtypes of SeedLink selectors are not supported,
 * anything following a '.' in a selector will be ignored.
 *
 * Return 0 on success and -1 on error.
 ***************************************************************************/
static int
SelectToRegex (char *net, char *sta, char *select, char **regex)
{
  char  newpattern[100];
  char  loc[10];
  char  chan[10];
  char *ptr;
  int   length;
  int   retval;
  
  if ( ! regex )
    return -1;
  
  /* New pattern buffer */
  newpattern[0] = '\0';
  
  /* Start pattern with a '^' */
  strncat (newpattern, "^", 1);
  
  if ( net )
    {
      /* Sanity check of network string */
      if ( strlen (net) > 10 )
	return -1;
      
      /* Translate network */
      ptr = net;
      while ( *ptr )
	{
	  if ( *ptr == '?' )
	    strncat (newpattern, ".", 1);
	  else if ( *ptr == '*' )
	    strncat (newpattern, ".*", 2);
	  else
	    strncat (newpattern, ptr, 1);
	  
	  ptr++;
	}
    }
  else
    {
      strncat (newpattern, ".*", 2);
    }
  
  /* Add separator */
  strncat (newpattern, "_", 1);
  
  if ( sta )
    {
      /* Sanity check of station string */
      if ( strlen (sta) > 10 )
	return -1;
      
      /* Translate station */
      ptr = sta;
      while ( *ptr )
	{
	  if ( *ptr == '?' )
	    strncat (newpattern, ".", 1);
	  else if ( *ptr == '*' )
	    strncat (newpattern, ".*", 2);
	  else
	    strncat (newpattern, ptr, 1);

	  ptr++;
	}
    }
  else
    {
      strncat (newpattern, ".*", 2);
    }
  
  /* Add separator */
  strncat (newpattern, "_", 1);
  
  if ( select )
    {
      /* Truncate selector at any period, DECOTL subtypes are not supported */
      if ( (ptr = strrchr (select, '.')) )
	{
	  *ptr = '\0';
	}
      
      length = strlen (select);
      
      /* If location and channel are specified */
      if ( length == 5 )
	{
	  /* Split location and channel parts */
	  loc[0] = '\0';
	  chan[0] = '\0';
	  strncat (loc, select, 2);
	  ptr = select + 2;
	  strncat (chan, ptr, 3);
	  
	  /* Translate location, '-' means space location which is collapsed */
	  ptr = loc;
	  while ( *ptr )
	    {
	      if ( *ptr == '?' )
		strncat (newpattern, ".", 1);
	      else if ( *ptr != '-' )
		strncat (newpattern, ptr, 1);
	      
	      ptr++;
	    }
	  
	  /* Add separator */
	  strcat (newpattern, "_");

	  /* Translate channel */
	  ptr = chan;
	  while ( *ptr )
	    {
	      if ( *ptr == '?' )
		strncat (newpattern, ".", 1);
	      else
		strncat (newpattern, ptr, 1);
	      
	      ptr++;
	    }
	}
      /* If only location is specified */
      else if ( length == 2 )
	{
	  /* Translate location, '-' means space location which is collapsed */
	  ptr = select;
	  while ( *ptr )
	    {
	      if ( *ptr == '?' )
		strncat (newpattern, ".", 1);
	      else if ( *ptr != '-' )
		strncat (newpattern, ptr, 1);
	      
	      ptr++;
	    }
	  
	  /* Add separator */
	  strncat (newpattern, "_", 1);
	}
      /* If only channel is specified */
      else if ( length == 3 )
	{
	  /* Add wildcard for location and separator */
	  strcat (newpattern, ".*_");
	  
	  /* Translate channel */
	  ptr = select;
	  while ( *ptr )
	    {
	      if ( *ptr == '?' )
		strncat (newpattern, ".", 1);
	      else
		strncat (newpattern, ptr, 1);
	      
	      ptr++;
	    }
	}
    }
  else
    {
      strncat (newpattern, ".*", 2);
    }
  
  /* Add final catch-all for remaining stream ID parts if not already done */
  if ( select )
    {
      strncat (newpattern, ".*", 2);
    }

  /* End pattern with a '$' */
  strncat (newpattern, "$", 1);
  
  /* Add new pattern to regex string, expanding as needed up to SLMAXREGEXLEN bytes*/
  if ( (retval = AddToString (regex, newpattern, "|", 0, SLMAXREGEXLEN)) )
    {
      if ( retval == -1 )
	{
	  lprintf (0, "Cannot allocate memory");
	  return -1;
	}
      if ( retval == -2 )
	{
	  lprintf (0, "AddToString would grow regex beyond maximum length");
	  return -1;
	}
      else
	{
	  lprintf (0, "Error with AddToString in SelectToRegex");
	  return -1;
	}
    }
  
  return 0;
}  /* End of SelectToRegex() */

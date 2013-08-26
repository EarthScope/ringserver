/**************************************************************************
 * dlclient.c
 *
 * DataLink client thread specific routines.
 *
 * Copyright 2011 Chad Trabant, IRIS Data Management Center
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
 * Modified: 2013.161
 **************************************************************************/

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
#include "mseedscan.h"
#include "generic.h"
#include "logging.h"
#include "dlclient.h"

/* Define the number of no-action loops that trigger the throttle */
#define THROTTLE_TRIGGER 10

static int  HandleNegotiation (ClientInfo *cinfo);
static int  HandleInfo (ClientInfo *cinfo, int socket, char state);
static int  HandleWrite (ClientInfo *cinfo, RingPacket *packet, void *packetdata);
static int  HandleRead (ClientInfo *cinfo, RingPacket *packet, void *packetdata);
static int  RecvCmd (ClientInfo *cinfo);
static int  SendPacket (ClientInfo *cinfo, char *header, char *data, 
			int64_t value, int addvalue, int addsize);
static int  SendRingPacket (ClientInfo *cinfo, RingPacket *packet, void *packetdata);
static int  SendData (ClientInfo *cinfo, void *buffer, size_t buflen);
static int  SelectedStreams (RingParams *ringparams, RingReader *reader);


/***********************************************************************
 * DL_ClientThread:
 *
 * Thread to handle all communications with a DataLink client.
 *
 * Returns NULL.
 ***********************************************************************/
void *
DL_ClientThread (void *arg)
{
  ClientInfo *cinfo;
  RingReader reader;
  struct thread_data *mytdp;
  int64_t readid;
  int sockflags;
  int setuperr = 0;
  int nread;

  struct sockaddr_in sin;
  socklen_t sinlen = sizeof(struct sockaddr_in);
  int serverport = -1;
  
  /* Client thread specific packet header and data buffers */
  RingPacket packet;
  char *packetdata = 0;
  
  /* Throttle related */
  char throttle = 0; /* Controls throttling of main loop */
  fd_set readset;    /* File descriptor set for select() */
  struct timeval timeout;  /* Timeout throttle for select() */
  
  char state = 0;    /* 0 = non-STREAM state (negotiation, ADD, INFO & READ state)
		        1 = STREAM data flow state
		     */
  
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
  
  lprintf (1, "Client connected [DataLink:%d]: %s [%s] port %s",
	   serverport, cinfo->hostname, cinfo->ipstr, cinfo->portstr);
  
  /* Initialize stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  cinfo->streams = RBTreeCreate (KeyCompare, free, free);
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Allocate client specific send buffer */
  cinfo->sendbuf = (char *) malloc (3 + 255 + cinfo->ringparams->pktsize);
  if ( ! cinfo->sendbuf )
    {
      lprintf (0, "[%s] Error allocating send buffer", cinfo->hostname);
      setuperr = 1;
    }
  cinfo->sendbuflen = 3 + 255 + cinfo->ringparams->pktsize;
  
  /* Allocate client specific receive buffer */
  cinfo->recvbuf = (char *) malloc (cinfo->ringparams->pktsize);
  if ( ! cinfo->recvbuf )
    {
      lprintf (0, "[%s] Error allocating receive buffer", cinfo->hostname);
      setuperr = 1;
    }
  cinfo->recvbuflen = cinfo->ringparams->pktsize;
  
  /* Allocate client specific packet data buffer */
  packetdata = (char *) malloc (cinfo->ringparams->pktsize);
  if ( ! packetdata )
    {
      lprintf (0, "[%s] Error allocating packet buffer", cinfo->hostname);
      setuperr = 1;
    }
  
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
      
      if ( cinfo->sendbuf )
	free (cinfo->sendbuf);
      
      if ( cinfo->recvbuf )
	free (cinfo->recvbuf);
      
      if ( packetdata )
	free (packetdata);
      
      if ( cinfo->mswrite )
	free (cinfo->mswrite);
      
      return NULL;
    }
  
  /* Set thread active status */
  pthread_mutex_lock (&(mytdp->td_lock));
  if ( mytdp->td_flags == TDF_SPAWNING )
    mytdp->td_flags = TDF_ACTIVE;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  /* Main client loop, delegating requests and handling data flow */
  while ( mytdp->td_flags != TDF_CLOSE )
    {
      /* Increment throttle trigger count by default */
      if ( throttle < THROTTLE_TRIGGER )
	throttle++;
      
      /* Check for data from client */
      nread = RecvCmd (cinfo);
      
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
	  
	  /* Determine if this is a data submission and handle */
	  if ( ! strncmp (cinfo->recvbuf, "WRITE", 5) )
	    {
	      /* Check for write permission */
	      if ( ! cinfo->writeperm )
		{
		  lprintf (1, "[%s] Data packet received from client without write permission",
			   cinfo->hostname);
		  SendPacket (cinfo, "ERROR", "Write permission not granted, no soup for you!", 0, 1, 1);
		  break;
		}
	      /* Any errors from HandleWrite are fatal */
	      else if ( HandleWrite (cinfo, &packet, packetdata) )
		{
		  break;
		}
	    }
	  
	  /* Determine if this is an INFO request and handle */
	  else if ( ! strncmp (cinfo->recvbuf, "INFO", 4) )
	    {
	      /* Any errors from HandlerInfo are fatal */
	      if ( HandleInfo (cinfo, cinfo->socket, state) )
		{
		  break;
		}
	    }

	  /* Determine if this is a specific read request and handle */
	  else if ( ! strncmp (cinfo->recvbuf, "READ", 4) )
	    {
	      state = 0;
	     
	      /* Any errors from HandleRead are fatal */
	      if ( HandleRead (cinfo, &packet, packetdata) )
		{
		  break;
		}
	    }
	  
	  /* Determine if this is a request to start STREAMing and set state to 1 */
	  else if ( ! strncmp (cinfo->recvbuf, "STREAM", 6) )
	    {
	      /* Set read position to next packet if position not set */
	      if ( reader.pktid == 0  )
		{
		  reader.pktid = RINGNEXT;
		}
	      
	      state = 1;
	    }
	  
	  /* Determine if this is a request to end STREAMing and set state to 0 */
	  else if ( ! strncmp (cinfo->recvbuf, "ENDSTREAM", 9) )
	    {
	      /* Send ENDSTREAM */
	      if ( SendPacket (cinfo, "ENDSTREAM", NULL, 0, 0, 0) )
		{
		  break;
		}
	      
	      state = 0;
	    }
	  
	  /* Otherwise this must be some sort of negotiation */
	  else
	    {
	      /* If this is not an ID request, set to a non-streaming state */
	      if ( strncmp (cinfo->recvbuf, "ID", 2) )
		state = 0;
	      
	      /* Any errors from HandleNegotiation are fatal */
	      if ( HandleNegotiation (cinfo) )
		{
		  break;
		}
	    }
	} /* Done processing data from client */
      
      /* STREAM: Continuously get the next packet and return to client */
      if ( state == 1 )
	{
	  /* Read next packet from ring */
	  readid = RingReadNext (&reader, &packet, packetdata);
	  
	  if ( readid < 0 )
	    {
	      lprintf (0, "[%s] Error reading next packet from ring", cinfo->hostname);
 	      break;
	    }
	  else if ( readid > 0 )
	    {
 	      lprintf (3, "[%s] Read %s (%u bytes) packet ID %"PRId64" from ring",
 		       cinfo->hostname, packet.streamid, packet.datasize, packet.pktid);
	      
	      /* If data read do not throttle */
	      throttle = 0;
	      
	      /* Send packet to client */
	      if ( SendRingPacket (cinfo, &packet, packetdata) )
		{
		  if ( cinfo->socketerr != 2 )
		    lprintf (1, "[%s] Error sending packet to client", cinfo->hostname);
		}
	      
	      /* Socket errors are fatal */
	      if ( cinfo->socketerr )
		break;
	    }
	  /* Otherwise there was no next packet, turn on immediate throttling */
	  else
	    {
	      throttle = THROTTLE_TRIGGER;
	    }
	} /* Done, for now, with stream data flow */
      
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
  
  /* Release match and reject related memory */
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
  
  /* Release stream tracking binary tree */
  pthread_mutex_lock (&(cinfo->streams_lock));
  RBTreeDestroy (cinfo->streams);
  cinfo->streams = 0;
  cinfo->streamscount = 0;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Release packetdata buffer */
  if ( packetdata )
    free (packetdata);
  
  /* Release send and receive buffers */
  if ( cinfo->sendbuf )
    free (cinfo->sendbuf);
  if ( cinfo->recvbuf )
    free (cinfo->recvbuf);
  
  /* Release client socket structure */
  if ( cinfo->addr )
    free (cinfo->addr);
  
  /* Shutdown and release Mini-SEED write data stream */
  if ( cinfo->mswrite )
    {
      ds_streamproc (cinfo->mswrite, NULL, NULL, cinfo->hostname);
      free (cinfo->mswrite);
      cinfo->mswrite = 0;
    }
  
  lprintf (1, "Client disconnected: %s", cinfo->hostname);
  
  /* Set thread CLOSED status */
  pthread_mutex_lock (&(mytdp->td_lock));
  mytdp->td_flags = TDF_CLOSED;
  pthread_mutex_unlock (&(mytdp->td_lock));
  
  return NULL;
}  /* End of DL_ClientThread() */


/***************************************************************************
 * HandleNegotiation:
 *
 * Handle negotiation commands implementing server-side DataLink
 * protocol, updating the connection configuration accordingly.
 *
 * DataLink commands handled:
 * ID
 * POSITION SET pktid [pkttime]
 * POSITION AFTER datatime
 * MATCH size|<match pattern of length size>
 * REJECT size|<match pattern of length size>
 *
 * All commands handled by this function will return the resulting
 * status to the client.
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleNegotiation (ClientInfo *cinfo)
{
  char sendbuffer[255];
  int size;
  int fields;
  int selected;
  
  char OKGO = 1;
  char junk;
  
  /* ID - Return server ID, version and capability flags */
  if ( ! strncasecmp (cinfo->recvbuf, "ID", 2) )
    {
      /* Parse client ID from command if included
       * Everything after "ID " is the client ID */
      if ( strlen (cinfo->recvbuf) > 3 )
	{
	  strncpy (cinfo->clientid, cinfo->recvbuf+3, sizeof(cinfo->clientid));
	  lprintf (2, "[%s] Received ID (%s)", cinfo->hostname, cinfo->clientid);
	}
      else
	{
	  lprintf (2, "[%s] Received ID", cinfo->hostname);
	}
      
      /* Create server version and capability flags string (DLCAPSFLAGS + WRITE if permission) */
      snprintf (sendbuffer, sizeof(sendbuffer),
		"ID DataLink " VERSION " :: %s PACKETSIZE:%lu%s", DLCAPFLAGS,
		(unsigned long int) (cinfo->ringparams->pktsize - sizeof(RingPacket)),
		(cinfo->writeperm)?" WRITE":"");
      
      /* Send the server ID string */
      if ( SendPacket (cinfo, sendbuffer, NULL, 0, 0, 0) )
	return -1;
    }
  
  /* POSITION <SET|AFTER> value [time]\r\n - Set ring reading position */
  else if ( ! strncasecmp (cinfo->recvbuf, "POSITION", 8) )
    {
      char subcmd[10];
      char value[30];
      char subvalue[30];
      int64_t pktid = 0;
      hptime_t hptime;
      
      OKGO = 1;
      
      /* Parse sub-command and value from request */
      fields = sscanf (cinfo->recvbuf, "%*s %10s %30s %30s %c",
		       subcmd, value, subvalue, &junk);
      
      /* Make sure the subcommand, value and subvalue fields are terminated */
      subcmd[9] = '\0';
      value[29] = '\0';
      subvalue[29] = '\0';
      
      /* Make sure we got a single pattern or no pattern */
      if ( fields < 2 || fields > 3 )
	{
	  if ( SendPacket (cinfo, "ERROR", "POSITION requires 2 or 3 arguments", 0, 1, 1) )
	    return -1;
	  
	  OKGO = 0;
	}
      else
	{
	  /* Process SET positioning */
	  if ( ! strncmp (subcmd, "SET", 3) )
	    {
	      /* Process SET <pktid> [time] */
	      if ( IsAllDigits (value) )
		{
		  pktid = strtoll (value, NULL, 10);
		  hptime = (fields==3) ? strtoll (subvalue, NULL, 10) : HPTERROR;
		}
	      /* Process SET EARLIEST */
	      else if ( ! strncmp (value, "EARLIEST", 8) )
		{
		  pktid = RINGEARLIEST;
		  hptime = HPTERROR;
		}
	      /* Process SET LATEST */
	      else if ( ! strncmp (value, "LATEST", 6) )
		{
		  pktid = RINGLATEST;
		  hptime = HPTERROR;
		}
	      else
		{
		  lprintf (0, "[%s] Error with POSITION SET value: %s",
			   cinfo->hostname, value);
		  if ( SendPacket (cinfo, "ERROR", "Error with POSITION SET value", 0, 1, 1) )
		    return -1;
		  OKGO = 0;
		}
	      
	      /* If no errors with the set value do the positioning */
	      if ( OKGO )
		{
		  if ( (pktid = RingPosition (cinfo->reader, pktid, hptime)) <= 0 )
		    {
		      if ( pktid == 0 )
			{
			  if ( SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1) )
			    return -1;
			}
		      else
			{
			  lprintf (0, "[%s] Error with RingPosition (pktid: %"PRId64", hptime: %"PRId64")",
				   cinfo->hostname, pktid, hptime);
			  if ( SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1) )
			    return -1;
			}
		    }
		  else
		    {
		      snprintf (sendbuffer, sizeof(sendbuffer),
				"Positioned to packet ID %"PRId64, pktid);
		      if ( SendPacket (cinfo, "OK", sendbuffer, pktid, 1, 1) )
			return -1;
		    }
		}
	    }
	  /* Process AFTER <time> positioning */
	  else if ( ! strncmp (subcmd, "AFTER", 5) )
	    {
	      if ( (hptime = strtoll (value, NULL, 10)) == 0 && errno == EINVAL )
		{
		  lprintf (0, "[%s] Error parsing POSITION AFTER time: %s",
			   cinfo->hostname, value);
		  if ( SendPacket (cinfo, "ERROR", "Error with POSITION AFTER time", 0, 1, 1) )
		    return -1;
		}
	      else
		{
		  /* Position ring according to start time, use reverse search if limited */
		  if ( cinfo->timewinlimit == 1.0 )
		    {
		      pktid = RingAfter (cinfo->reader, hptime, 1);
		    }
		  else if ( cinfo->timewinlimit < 1.0 )
		    {
		      int64_t pktlimit = (int64_t) (cinfo->timewinlimit * cinfo->ringparams->maxpackets);
		      
		      pktid = RingAfterRev (cinfo->reader, hptime, pktlimit, 1);
		    }
		  else
		    {
		      lprintf (0, "Time window search limit is invalid: %f", cinfo->timewinlimit);
		      SendPacket (cinfo, "ERROR", "time window search limit is invalid", 0, 1, 1);
		      return -1;
		    }
		  
		  if ( pktid == 0 )
		    {
		      if ( SendPacket (cinfo, "ERROR", "Packet not found", 0, 1, 1) )
			return -1;
		    }
		  else if ( pktid < 0 )
		    {
		      lprintf (0, "[%s] Error with RingAfter[Rev] (hptime: %"PRId64")",
			       cinfo->hostname, hptime);
		      if ( SendPacket (cinfo, "ERROR", "Error positioning reader", 0, 1, 1) )
			return -1;
		    }
		}
	      
	      snprintf (sendbuffer, sizeof(sendbuffer), "Positioned to packet ID %"PRId64, pktid);
	      if ( SendPacket (cinfo, "OK", sendbuffer, pktid, 1, 1) )
		return -1;
	    }
	  else
	    {
	      lprintf (0, "[%s] Unsupported POSITION subcommand: %s", cinfo->hostname, subcmd);
	      if ( SendPacket (cinfo, "ERROR", "Unsupported POSITION subcommand", 0, 1, 1) )
		return -1;
	    }
	}
    }  /* End of POSITION */
  
  /* MATCH size\r\n[pattern] - Provide regex to match streamids */
  else if ( ! strncasecmp (cinfo->recvbuf, "MATCH", 5) )
    {
      OKGO = 1;
      
      /* Parse size from request */
      fields = sscanf (cinfo->recvbuf, "%*s %d %c", &size, &junk);
      
      /* Make sure we got a single pattern or no pattern */
      if ( fields > 1 )
	{
	  if ( SendPacket (cinfo, "ERROR", "MATCH requires a single argument", 0, 1, 1) )
	    return -1;
	  
	  OKGO = 0;
	}
      /* Remove current match if no pattern supplied */
      else if ( fields <= 0 )
	{
	  if ( cinfo->matchstr )
	    free (cinfo->matchstr);
	  cinfo->matchstr = 0;
	  RingMatch (cinfo->reader, 0);

	  selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
	  snprintf (sendbuffer, sizeof(sendbuffer), "%d streams selected after match",
		    selected);
	  if ( SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1) )
	    return -1;
	}
      else if ( size > DLMAXREGEXLEN )
	{
	  lprintf (0, "[%s] match expression too large (%d)", cinfo->hostname, size);
	  
	  snprintf (sendbuffer, sizeof(sendbuffer), "match expression too large, must be <= %d",
		    DLMAXREGEXLEN);
	  if ( SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1) )
	    return -1;
	  
	  OKGO = 0;
	}
      else
	{
	  if ( cinfo->matchstr )
	    free (cinfo->matchstr);
	  
	  /* Read regex of size bytes from socket */
	  if ( ! (cinfo->matchstr = (char *) malloc (size+1)) )
	    {
	      lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
	      return -1;
	    }
	  
	  if ( RecvData (cinfo->socket, cinfo->matchstr, size, cinfo->hostname) < 0 )
	    {
	      lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
	      return -1;	      
	    }
	  
	  /* Make sure buffer is a terminated string */
	  cinfo->matchstr[size] = '\0';
	  
	  /* Compile match expression */
	  if ( RingMatch (cinfo->reader, cinfo->matchstr) )
	    {
	      lprintf (0, "[%s] Error with match expression", cinfo->hostname);
	      
	      if ( SendPacket (cinfo, "ERROR", "Error with match expression", 0, 1, 1) )
		return -1;
	    }
	  else
	    {
	      selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
	      snprintf (sendbuffer, sizeof(sendbuffer), "%d streams selected after match",
			selected);
	      if ( SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1) )
		return -1;
	    }
	}
    }  /* End of MATCH */

  /* REJECT size\r\n[pattern] - Provide regex to reject streamids */
  else if ( OKGO &&! strncasecmp (cinfo->recvbuf, "REJECT", 6) )
    {
      OKGO = 1;
      
      /* Parse size from request */
      fields = sscanf (cinfo->recvbuf, "%*s %d %c", &size, &junk);
      
      /* Make sure we got a single pattern or no pattern */
      if ( fields > 1 )
	{
	  if ( SendPacket (cinfo, "ERROR", "REJECT requires a single argument", 0, 1, 1) )
	    return -1;
	  
	  OKGO = 0;
	}
      /* Remove current reject if no pattern supplied */
      else if ( fields <= 0 )
	{
	  if ( cinfo->rejectstr )
	    free (cinfo->rejectstr);
	  cinfo->rejectstr = 0;
	  RingReject (cinfo->reader, 0);
	  
	  selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
	  snprintf (sendbuffer, sizeof(sendbuffer), "%d streams selected after reject",
		    selected);
	  if ( SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1) )
	    return -1;
	}
      else if ( size > DLMAXREGEXLEN )
	{
	  lprintf (0, "[%s] reject expression too large (%)", cinfo->hostname, size);
	  
	  snprintf (sendbuffer, sizeof(sendbuffer), "reject expression too large, must be <= %d",
		    DLMAXREGEXLEN);
	  if ( SendPacket (cinfo, "ERROR", sendbuffer, 0, 1, 1) )
	    return -1;
	  
	  OKGO = 0;
	}
      else
	{
	  if ( cinfo->rejectstr )
	    free (cinfo->rejectstr);

	  /* Read regex of size bytes from socket */
	  if ( ! (cinfo->rejectstr = (char *) malloc (size+1)) )
	    {
	      lprintf (0, "[%s] Error allocating memory", cinfo->hostname);
	      return -1;
	    }
	  
	  if ( RecvData (cinfo->socket, cinfo->rejectstr, size, cinfo->hostname) < 0 )
	    {
	      lprintf (0, "[%s] Error Recv'ing data", cinfo->hostname);
	      return -1;	      
	    }
	  
	  /* Make sure buffer is a terminated string */
	  cinfo->rejectstr[size] = '\0';
	  
	  /* Compile reject expression */
	  if ( RingReject (cinfo->reader, cinfo->rejectstr) )
	    {
	      lprintf (0, "[%s] Error with reject expression", cinfo->hostname);
	      
	      if ( SendPacket (cinfo, "ERROR", "Error with reject expression", 0, 1, 1) )
		return -1;
	    }
	  else
	    {
	      selected = SelectedStreams (cinfo->ringparams, cinfo->reader);
	      snprintf (sendbuffer, sizeof(sendbuffer), "%d streams selected after reject",
			selected);
	      if ( SendPacket (cinfo, "OK", sendbuffer, selected, 1, 1) )
		return -1;
	    }
	}
    }  /* End of REJECT */
  
  /* BYE - End connection */
  else if ( ! strncasecmp (cinfo->recvbuf, "BYE", 3) )
    {
      return -1;
    }
  
  /* Unrecognized command */
  else
    {
      lprintf (1, "[%s] Unrecognized command: %.10s",
	       cinfo->hostname, cinfo->recvbuf);
      
      if ( SendPacket (cinfo, "ERROR", "Unrecognized command", 0, 1, 1) )
	return -1;
    }
  
  return 0;
} /* End of HandleNegotiation */


/***************************************************************************
 * HandleWrite:
 *
 * Handle DataLink WRITE request.
 *
 * The command syntax is: "WRITE <streamid> <hpdatastart> <hpdataend> <flags> <datasize>"
 *
 * The stream ID is used verbatim by the ringserver.  The hpdatastart
 * and hpdataend are high-precision time stamps (dltime_t).  The data
 * size is the size in bytes of the data portion following the header.
 * The flags are single character indicators and interpreted the
 * following way:
 *
 * flags:
 * 'N' = no acknowledgement is requested
 * 'A' = acknowledgement is requested, server will send a reply
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleWrite (ClientInfo *cinfo, RingPacket *packet, void *packetdata)
{
  StreamNode *stream;
  char replystr[100];
  char streamid[100];
  char flags[100];
  int nread;
  int newstream = 0;
  int rv;
  
  MSRecord *msr = 0;
  char *type;
  
  if ( ! cinfo || ! packet || ! packetdata )
    return -1;
  
  /* Parse command parameters: WRITE <streamid> <datastart> <dataend> <flags> <datasize> */
  if ( sscanf (cinfo->recvbuf, "%*s %100s %"PRId64" %"PRId64" %100s %u",
	       streamid, &(packet->datastart), &(packet->dataend), flags, &(packet->datasize)) != 5 )
    {
      lprintf (1, "[%s] Error parsing WRITE parameters: %.100s",
	       cinfo->hostname, cinfo->recvbuf);
      
      SendPacket (cinfo, "ERROR", "Error parsing WRITE command parameters", 0, 1, 1);
      
      return -1;
    }
  
  /* Copy the stream ID */
  memcpy (packet->streamid, streamid, sizeof(packet->streamid));
  
  /* Make sure the streamid is terminated */
  packet->streamid[sizeof(packet->streamid)-1] = '\0';
  
  /* Make sure this packet data would fit into the ring */
  if ( packet->datasize > cinfo->ringparams->pktsize )
    {
      lprintf (1, "[%s] Submitted packet size (%d) is greater than ring packet size (%d)",
	       cinfo->hostname, packet->datasize, cinfo->ringparams->pktsize);
      
      snprintf (replystr, sizeof(replystr), "Packet size (%d) is too large for ring, maximum is %d bytes",
		packet->datasize, cinfo->ringparams->pktsize);
      SendPacket (cinfo, "ERROR", replystr, 0, 1, 1);
      
      return -1;
    }
  
  /* Recv packet data from socket */
  nread = RecvData (cinfo->socket, packetdata, packet->datasize, cinfo->hostname);
  
  if ( nread < 0 )
    return -1;
  
  /* Write received Mini-SEED to a disk archive if configured */
  if ( cinfo->mswrite )
    {
      char filename[100];
      char *fn;
      
      if ( (type = strrchr (streamid, '/')) )
	{
	  if ( ! strncmp (++type, "MSEED", 5) )
	    {
	      /* Parse the Mini-SEED record header */
	      if ( msr_unpack (packetdata, packet->datasize, &msr, 0, 0) == MS_NOERROR )
		{
		  /* Check for file name in streamid: "filename::streamid/MSEED" */
		  if ( (fn = strstr (streamid, "::")) )
		    {
		      strncpy (filename, streamid, (fn - streamid));
		      filename[(fn - streamid)] = '\0';
		      fn = filename;
		    }
		  
		  /* Write Mini-SEED record to disk */
		  if ( ds_streamproc (cinfo->mswrite, msr, fn, cinfo->hostname) )
		    {
		      lprintf (1, "[%s] Error writing Mini-SEED to disk", cinfo->hostname);
		      
		      SendPacket (cinfo, "ERROR", "Error writing Mini-SEED to disk", 0, 1, 1);
		      
		      return -1;
		    }
		}
	      
	      if ( msr )
		msr_free (&msr);
	    }
	}
    }
  
  /* Add the packet to the ring */
  if ( (rv = RingWrite (cinfo->ringparams, packet, packetdata, packet->datasize)) )
    {
      if ( rv == -2 )
	lprintf (1, "[%s] Error with RingWrite, corrupt ring, shutdown signalled", cinfo->hostname);
      else
	lprintf (1, "[%s] Error with RingWrite", cinfo->hostname);
      
      SendPacket (cinfo, "ERROR", "Error adding packet to ring", 0, 1, 1);
      
      /* Set the shutdown signal if ring corruption was detected */
      if ( rv == -2 )
	shutdownsig = 1;
      
      return -1;
    }
  
  /* Get (creating if needed) the StreamNode for this streamid */
  if ( (stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
				packet->streamid, &newstream)) == 0 )
    {
      lprintf (0, "[%s] Error with GetStreamNode for %s",
	       cinfo->hostname, packet->streamid);
      return -1;
    }
  
  if ( newstream )
    {
      lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, packet->streamid);
      cinfo->streamscount++;
    }
  
  /* Update StreamNode packet and byte counts */
  pthread_mutex_lock (&(cinfo->streams_lock));
  stream->rxpackets++;
  stream->rxbytes += packet->datasize;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Update client receive counts */
  cinfo->rxpackets[0]++;
  cinfo->rxbytes[0] += packet->datasize;
  
  /* Send acknowledgement if requested (flags contain 'A') */
  if ( strchr (flags, 'A') )
    {
      if ( SendPacket (cinfo, "OK", NULL, packet->pktid, 1, 1) )
	return -1;
    }
  
  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleWrite */


/***************************************************************************
 * HandleRead:
 *
 * Handle DataLink READ request.
 *
 * The command syntax is: "READ <pktid>"
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleRead (ClientInfo *cinfo, RingPacket *packet, void *packetdata)
{
  int64_t reqid = 0;
  int64_t readid = 0;
  char replystr[100];
  
  if ( ! cinfo || ! packet || ! packetdata )
    return -1;
  
  /* Parse command parameters: READ <pktid> */
  if ( sscanf (cinfo->recvbuf, "%*s %"PRId64, &reqid) != 1 )
    {
      lprintf (1, "[%s] Error parsing READ parameters: %.100s",
	       cinfo->hostname, cinfo->recvbuf);
      
      if ( SendPacket (cinfo, "ERROR", "Error parsing READ command parameters", 0, 1, 1) )
	return -1;
    }
  
  /* Read the packet from the ring */
  if ( (readid = RingRead (cinfo->reader, reqid, packet, packetdata)) < 0 )
    {
      lprintf (1, "[%s] Error with RingRead", cinfo->hostname);
      
      if ( SendPacket (cinfo, "ERROR", "Error reading packet from ring", 0, 1, 1) )
	return -1;
    }
  
  /* Return packet not found error message if needed */
  if ( readid == 0 )
    {
      snprintf (replystr, sizeof(replystr), "Packet %"PRId64" not found in ring", reqid);
      if ( SendPacket (cinfo, "ERROR", replystr, 0, 1, 1) )
	return -1;
    }
  /* Send packet to client */
  else if ( SendRingPacket (cinfo, packet, packetdata) )
    {
      if ( cinfo->socketerr != 2 )
	lprintf (1, "[%s] Error sending packet to client", cinfo->hostname);
    }
  
  return (cinfo->socketerr) ? -1 : 0;
} /* End of HandleRead() */


/***************************************************************************
 * HandleInfo:
 *
 * Handle DataLink INFO request, returning the appropriate XML response.
 *
 * DataLink INFO requests handled:
 * STATUS
 * STREAMS
 * CONNECTIONS
 *
 * Returns 0 on success and -1 on error which should disconnect.
 ***************************************************************************/
static int
HandleInfo (ClientInfo *cinfo, int socket, char state)
{
  mxml_node_t *xmldoc = 0;
  mxml_node_t *status;
  char   string[200];
  char  *xmlstr = 0;
  int    xmllength;
  char  *type = 0;
  char  *matchexpr = 0;
  char   errflag = 0;
  
  if ( ! cinfo )
    return -1;
  
  if ( ! strncasecmp (cinfo->recvbuf, "INFO", 4) )
    {
      /* Set level pointer to start of type identifier */
      type = cinfo->recvbuf + 4;
      
      /* Skip any spaces between INFO and type identifier */
      while ( *type == ' ' )
	type++;
      
      /* Skip type characters then spaces to get to match */
      matchexpr = type;
      while ( *matchexpr != ' ' && *matchexpr )
	matchexpr++;
      while ( *matchexpr == ' ' )
	matchexpr++;
    }
  else
    {
      lprintf (0, "[%s] HandleInfo cannot detect INFO", cinfo->hostname);
      return -1;
    }
  
  /* Initialize the XML response */
  if ( ! (xmldoc = mxmlNewElement (MXML_NO_PARENT, "DataLink")) )
    {
      lprintf (0, "[%s] Error initializing XML response", cinfo->hostname);
      return -1;
    }
  
  /* All INFO responses contain these attributes in the root DataLink element */
  mxmlElementSetAttr (xmldoc, "Version", VERSION);
  mxmlElementSetAttr (xmldoc, "ServerID", serverid);
  mxmlElementSetAttrf (xmldoc, "Capabilities", "%s PACKETSIZE:%lu%s", DLCAPFLAGS,
		       (unsigned long int) (cinfo->ringparams->pktsize - sizeof(RingPacket)),
		       (cinfo->writeperm)?" WRITE":"");
  
  /* All INFO responses contain the "Status" element */
  if ( ! (status = mxmlNewElement (xmldoc, "Status")) )
    {
      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
      errflag = 1;
    }
  else
    {
      /* Convert server start time to YYYY-MM-DD HH:MM:SS */
      ms_hptime2mdtimestr (serverstarttime, string, 0);
      mxmlElementSetAttr (status, "StartTime", string);
      mxmlElementSetAttrf (status, "RingVersion", "%u", (unsigned int) cinfo->ringparams->version);
      mxmlElementSetAttrf (status, "RingSize", "%"PRIu64, cinfo->ringparams->ringsize);
      mxmlElementSetAttrf (status, "PacketSize", "%lu",
                           (unsigned long int) (cinfo->ringparams->pktsize - sizeof(RingPacket)));
      mxmlElementSetAttrf (status, "MaximumPacketID", "%"PRId64, cinfo->ringparams->maxpktid);
      mxmlElementSetAttrf (status, "MaximumPackets", "%"PRId64, cinfo->ringparams->maxpackets);
      mxmlElementSetAttrf (status, "MemoryMappedRing", "%s", (cinfo->ringparams->mmapflag) ? "TRUE" : "FALSE");
      mxmlElementSetAttrf (status, "VolatileRing", "%s", (cinfo->ringparams->volatileflag) ? "TRUE" : "FALSE");
      mxmlElementSetAttrf (status, "TotalConnections", "%d", clientcount);
      mxmlElementSetAttrf (status, "TotalStreams", "%d", cinfo->ringparams->streamcount);
      mxmlElementSetAttrf (status, "TXPacketRate", "%.1f", cinfo->ringparams->txpacketrate);
      mxmlElementSetAttrf (status, "TXByteRate", "%.1f", cinfo->ringparams->txbyterate);
      mxmlElementSetAttrf (status, "RXPacketRate", "%.1f", cinfo->ringparams->rxpacketrate);
      mxmlElementSetAttrf (status, "RXByteRate", "%.1f", cinfo->ringparams->rxbyterate);
      mxmlElementSetAttrf (status, "EarliestPacketID", "%"PRId64, cinfo->ringparams->earliestid);
      ms_hptime2mdtimestr (cinfo->ringparams->earliestptime, string, 1);
      mxmlElementSetAttr (status, "EarliestPacketCreationTime",
			  (cinfo->ringparams->earliestptime != HPTERROR)?string:"-");
      ms_hptime2mdtimestr (cinfo->ringparams->earliestdstime, string, 1);
      mxmlElementSetAttr (status, "EarliestPacketDataStartTime",
			  (cinfo->ringparams->earliestdstime != HPTERROR)?string:"-");
      ms_hptime2mdtimestr (cinfo->ringparams->earliestdetime, string, 1);
      mxmlElementSetAttr (status, "EarliestPacketDataEndTime",
			  (cinfo->ringparams->earliestdetime != HPTERROR)?string:"-");
      mxmlElementSetAttrf (status, "LatestPacketID", "%"PRId64, cinfo->ringparams->latestid);
      ms_hptime2mdtimestr (cinfo->ringparams->latestptime, string, 1);
      mxmlElementSetAttr (status, "LatestPacketCreationTime",
			  (cinfo->ringparams->latestptime != HPTERROR)?string:"-");
      ms_hptime2mdtimestr (cinfo->ringparams->latestdstime, string, 1);
      mxmlElementSetAttr (status, "LatestPacketDataStartTime",
			  (cinfo->ringparams->latestdstime != HPTERROR)?string:"-");
      ms_hptime2mdtimestr (cinfo->ringparams->latestdetime, string, 1);
      mxmlElementSetAttr (status, "LatestPacketDataEndTime",
			  (cinfo->ringparams->latestdetime != HPTERROR)?string:"-");
    }
  
  /* Add contents to the XML structure depending on info request */
  if ( ! strncasecmp (type, "STATUS", 6) )
    {
      mxml_node_t *stlist, *st;
      int totalcount = 0;
      struct sthread *loopstp;
      
      lprintf (1, "[%s] Received INFO STATUS request", cinfo->hostname);
      type = "INFO STATUS";
      
      /* Create "ServerThreads" element */
      if ( ! (stlist = mxmlNewElement (xmldoc, "ServerThreads")) )
	{
	  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	  errflag = 1;
	}
      
      /* Create a Thread element for each thread, lock thread list while looping */
      pthread_mutex_lock (&sthreads_lock);
      loopstp = sthreads;
      while ( loopstp )
	{
	  totalcount++;
	  
	  if ( ! (st = mxmlNewElement (stlist, "Thread")) )
	    {
	      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	      errflag = 1;
	    }
	  else
	    {
	      /* Add thread status flags to Thread element */
	      string[0] = '\0';
	      if ( loopstp->td->td_flags & TDF_SPAWNING ) strcat (string, " SPAWNING");
	      if ( loopstp->td->td_flags & TDF_ACTIVE ) strcat (string," ACTIVE");
	      if ( loopstp->td->td_flags & TDF_CLOSE ) strcat (string," CLOSE");
	      if ( loopstp->td->td_flags & TDF_CLOSING ) strcat (string," CLOSING");
	      if ( loopstp->td->td_flags & TDF_CLOSED ) strcat (string," CLOSED");
	      mxmlElementSetAttr (st, "Flags", string);
	      
	      /* Determine server thread type and add specifics */
	      if ( loopstp->type == DATALINK_LISTEN_THREAD || loopstp->type == SEEDLINK_LISTEN_THREAD )
		{
		  ListenPortParams *lpp = loopstp->params;
		  
		  if ( loopstp->type == DATALINK_LISTEN_THREAD )
		    mxmlElementSetAttr (st, "Type", "DataLink Server");
		  else
		    mxmlElementSetAttr (st, "Type", "SeedLink Server");
		  
		  mxmlElementSetAttr (st, "Port", lpp->portstr);
		}
	      else if  ( loopstp->type == MSEEDSCAN_THREAD )
		{
		  MSScanInfo *mssinfo = loopstp->params;
		  
		  mxmlElementSetAttr (st, "Type", "Mini-SEED Scanner");
		  mxmlElementSetAttr (st, "Directory", mssinfo->dirname);
		  mxmlElementSetAttrf (st, "MaxRecursion", "%d", mssinfo->maxrecur);
		  mxmlElementSetAttr (st, "StateFile", mssinfo->statefile);
		  mxmlElementSetAttr (st, "Match", mssinfo->matchstr);
		  mxmlElementSetAttr (st, "Reject", mssinfo->rejectstr);
		  mxmlElementSetAttrf (st, "ScanTime", "%g", mssinfo->scantime);
		  mxmlElementSetAttrf (st, "PacketRate", "%g", mssinfo->rxpacketrate);
		  mxmlElementSetAttrf (st, "ByteRate", "%g", mssinfo->rxbyterate);
		}
	      else
		{
		  mxmlElementSetAttr (st, "Type", "Unknown Thread");
		}
	    }
	  
	  loopstp = loopstp->next;
	}
      pthread_mutex_unlock (&sthreads_lock);
      
      /* Add thread count attribute to ServerThreads element */
      mxmlElementSetAttrf (stlist, "TotalServerThreads", "%d", totalcount);

    }  /* End of STATUS */
  else if ( ! strncasecmp (type, "STREAMS", 7) )
    {
      mxml_node_t *streamlist, *stream;
      hptime_t hpnow;
      int selectedcount = 0;
      Stack *streams;
      RingStream *ringstream;
      
      lprintf (1, "[%s] Received INFO STREAMS request", cinfo->hostname);
      type = "INFO STREAMS";
      
      /* Create "StreamList" element and add attributes */
      if ( ! (streamlist = mxmlNewElement (xmldoc, "StreamList")) )
	{
	  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	  errflag = 1;
	}
      
      /* Collect stream list */
      if ( (streams = GetStreamsStack (cinfo->ringparams, cinfo->reader)) )
        {
	  /* Get current time */
	  hpnow = HPnow();
	  
	  /* Create a "Stream" element for each stream */
	  while ( (ringstream = (RingStream *) StackPop(streams)) )
	    {
	      if ( ! (stream = mxmlNewElement (streamlist, "Stream")) )
		{
		  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
		  errflag = 1;
		}
	      else
		{
		  mxmlElementSetAttr (stream, "Name", ringstream->streamid);
		  mxmlElementSetAttrf (stream, "EarliestPacketID", "%"PRId64, ringstream->earliestid);
		  ms_hptime2mdtimestr (ringstream->earliestdstime, string, 1);
		  mxmlElementSetAttr (stream, "EarliestPacketDataStartTime", string);
		  ms_hptime2mdtimestr (ringstream->earliestdetime, string, 1);
		  mxmlElementSetAttr (stream, "EarliestPacketDataEndTime", string);
		  mxmlElementSetAttrf (stream, "LatestPacketID", "%"PRId64, ringstream->latestid);
		  ms_hptime2mdtimestr (ringstream->latestdstime, string, 1);
		  mxmlElementSetAttr (stream, "LatestPacketDataStartTime", string);
		  ms_hptime2mdtimestr (ringstream->latestdetime, string, 1);
		  mxmlElementSetAttr (stream, "LatestPacketDataEndTime", string);
		  
		  /* DataLatency value is the difference between the current time and the time of last sample in seconds */
		  mxmlElementSetAttrf (stream, "DataLatency", "%.1f", (double) MS_HPTIME2EPOCH((hpnow - ringstream->latestdetime)));
		}
	      
	      free (ringstream);
	      selectedcount++;
	    }
	  
	  /* Cleanup stream stack */
	  StackDestroy (streams, free);
	}
      else
	{
	  lprintf (0, "[%s] Error generating Stack of streams", cinfo->hostname);
	  errflag = 1;
	}
      
      /* Add stream count attributes to StreamList element */
      mxmlElementSetAttrf (streamlist, "TotalStreams", "%d", cinfo->ringparams->streamcount);
      mxmlElementSetAttrf (streamlist, "SelectedStreams", "%d", selectedcount);
      
    }  /* End of STREAMS */
  else if ( ! strncasecmp (type, "CONNECTIONS", 11) )
    {
      mxml_node_t *connlist, *conn;
      hptime_t hpnow;
      int selectedcount = 0;
      int totalcount = 0;
      struct cthread *loopctp;
      ClientInfo *tcinfo;
      char *conntype;
      pcre *match = 0;
      const char *errptr;
      int erroffset;
      
      lprintf (1, "[%s] Received INFO CONNECTIONS request", cinfo->hostname);
      type = "INFO CONNECTIONS";
      
      /* Get current time */
      hpnow = HPnow();
      
      /* Compile match expression supplied with request */
      if ( matchexpr )
	{
	  match = pcre_compile (matchexpr, 0, &errptr, &erroffset, NULL);
	  if ( errptr )
	    {
	      lprintf (0, "[%s] Error with pcre_compile: %s", errptr);
	      errflag = 1;
	      matchexpr = 0;
	    }
	}
      
      /* Create "ConnectionList" element */
      if ( ! (connlist = mxmlNewElement (xmldoc, "ConnectionList")) )
	{
	  lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	  errflag = 1;
	}
      
      /* Create a Connection element for each client, lock client list while looping */
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
	  
	  totalcount++;
	  tcinfo = (ClientInfo *) loopctp->td->td_prvtptr;
	  
	  /* Check matching expression against the client address string (host:port) and client ID */
	  if ( match )
	    if ( pcre_exec(match, NULL, tcinfo->hostname, strlen(tcinfo->hostname), 0, 0, NULL, 0) &&
		 pcre_exec(match, NULL, tcinfo->ipstr, strlen(tcinfo->ipstr), 0, 0, NULL, 0) &&
		 pcre_exec(match, NULL, tcinfo->clientid, strlen(tcinfo->clientid), 0, 0, NULL, 0) )
	      {
		loopctp = loopctp->next;
		continue;
	      }
	  
	  if ( ! (conn = mxmlNewElement (connlist, "Connection")) )
	    {
	      lprintf (0, "[%s] Error adding child to XML INFO response", cinfo->hostname);
	      errflag = 1;
	    }
	  else
	    {
	      /* Determine connection type */
	      if ( tcinfo->type == DATALINK_CLIENT )
		conntype = "DataLink";
	      else if ( tcinfo->type == SEEDLINK_CLIENT )
		conntype = "SeedLink";
	      else
		conntype = "Unknown";
	      
	      mxmlElementSetAttr (conn, "Type", conntype);
	      
	      mxmlElementSetAttr (conn, "Host", tcinfo->hostname);
	      mxmlElementSetAttr (conn, "IP", tcinfo->ipstr);
	      mxmlElementSetAttr (conn, "Port", tcinfo->portstr);
	      mxmlElementSetAttr (conn, "ClientID", tcinfo->clientid);
	      ms_hptime2mdtimestr (tcinfo->conntime, string, 1);
	      mxmlElementSetAttr (conn, "ConnectionTime", string);
	      mxmlElementSetAttrf (conn, "Match", "%s", (tcinfo->matchstr)?tcinfo->matchstr:"");
	      mxmlElementSetAttrf (conn, "Reject", "%s", (tcinfo->rejectstr)?tcinfo->rejectstr:"");
	      mxmlElementSetAttrf (conn, "StreamCount", "%d", tcinfo->streamscount);
	      mxmlElementSetAttrf (conn, "PacketID", "%"PRId64, tcinfo->reader->pktid);
	      ms_hptime2mdtimestr (tcinfo->reader->pkttime, string, 1);
	      mxmlElementSetAttr (conn, "PacketCreationTime",
				  (tcinfo->reader->pkttime != HPTERROR) ? string : "-");
	      ms_hptime2mdtimestr (tcinfo->reader->datastart, string, 1);
	      mxmlElementSetAttr (conn, "PacketDataStartTime",
				  (tcinfo->reader->datastart != HPTERROR) ? string : "-");
	      ms_hptime2mdtimestr (tcinfo->reader->dataend, string, 1);
	      mxmlElementSetAttr (conn, "PacketDataEndTime",
				  (tcinfo->reader->dataend != HPTERROR) ? string : "-");
	      mxmlElementSetAttrf (conn, "TXPacketCount", "%"PRId64, tcinfo->txpackets[0]);
	      mxmlElementSetAttrf (conn, "TXPacketRate", "%.1f", tcinfo->txpacketrate);
	      mxmlElementSetAttrf (conn, "TXByteCount", "%"PRId64, tcinfo->txbytes[0]);
	      mxmlElementSetAttrf (conn, "TXByteRate", "%.1f", tcinfo->txbyterate);
	      mxmlElementSetAttrf (conn, "RXPacketCount", "%"PRId64, tcinfo->rxpackets[0]);
	      mxmlElementSetAttrf (conn, "RXPacketRate", "%.1f", tcinfo->rxpacketrate);
	      mxmlElementSetAttrf (conn, "RXByteCount", "%"PRId64, tcinfo->rxbytes[0]);
	      mxmlElementSetAttrf (conn, "RXByteRate", "%.1f", tcinfo->rxbyterate);
	      
	      /* Latency value is the difference between the current time and the time of last packet exchange in seconds */
	      mxmlElementSetAttrf (conn, "Latency", "%.1f", (double) MS_HPTIME2EPOCH((hpnow - tcinfo->lastxchange)));
	      
	      if ( tcinfo->reader->pktid <= 0 )
		strncpy (string, "-", sizeof(string));
	      else
		snprintf (string, sizeof(string), "%d", tcinfo->percentlag);
	      
	      mxmlElementSetAttr (conn, "PercentLag", string);
	      
	      selectedcount++;
	    }
	  
	  loopctp = loopctp->next;
	}
      pthread_mutex_unlock (&cthreads_lock);
      
      /* Add client count attribute to ConnectionList element */
      mxmlElementSetAttrf (connlist, "TotalConnections", "%d", totalcount);
      mxmlElementSetAttrf (connlist, "SelectedConnections", "%d", selectedcount);
      
      /* Free compiled match expression */
      if ( match )
        pcre_free (match);
      
    }  /* End of CONNECTIONS */
  /* Unrecognized INFO request */
  else
    {
      lprintf (0, "[%s] Unrecognized INFO request type: %s", cinfo->hostname, type);
      snprintf (string, sizeof(string), "Unrecognized INFO request type: %s", type);
      SendPacket (cinfo, "ERROR", string, 0, 1, 1);
      errflag = 2;
    }
  
  /* Send ERROR to client if not already done */
  if ( errflag == 1 )
    {
      SendPacket (cinfo, "ERROR", "Error processing INFO request", 0, 1, 1);
    }
  /* Convert to XML string, pack into Mini-SEED and send to client */
  else if ( xmldoc && ! errflag )
    {
      /* Do not wrap the output XML */
      mxmlSetWrapMargin (0);
      
      /* Convert to XML string */
      if ( ! (xmlstr = mxmlSaveAllocString (xmldoc, MXML_NO_CALLBACK)) )
	{
	  lprintf (0, "[%s] Error with mxmlSaveAllocString()", cinfo->hostname);
	  if ( xmldoc )
	    mxmlRelease (xmldoc);
	  return -1;
	}
      
      /* Trim final newline character if present */
      xmllength = strlen (xmlstr);
      if ( xmlstr[xmllength-1] == '\n' )
	{
	  xmlstr[xmllength-1] = '\0';
	  xmllength--;
	}
      
      /* Send XML to client */
      if ( SendPacket (cinfo, type, xmlstr, 0, 0, 1) )
	{
	  if ( cinfo->socketerr != 2 )
	    lprintf (0, "[%s] Error sending INFO XML", cinfo->hostname);
	  
	  if ( xmldoc )
	    mxmlRelease (xmldoc);
	  if ( xmlstr )
	    free (xmlstr);
	  return -1;
	}
    }
  
  /* Free allocated memory */
  if ( xmldoc )
    mxmlRelease (xmldoc);
  
  if ( xmlstr )
    free (xmlstr);
  
  return ( cinfo->socketerr || errflag ) ? -1 : 0;
} /* End of HandleInfo */


/***********************************************************************
 * RecvCmd:
 *
 * Read a command from the client socket as a header-only DataLink
 * packet.  A header-only DataLink packet is composed of a pre-header
 * followed by a header body.  The pre-header is composed of 3 bytes:
 * the two ASCII characters 'DL' followed by an unsigned 8-bit integer
 * indicating the size of the header body.
 *
 * This routine handles fragmented receives after some data has been
 * read.  If no data has been read and no data is available from the
 * socket this routine will return immediately.
 *
 * The command (header body) returned in the ClientInfo.recvbuf buffer
 * will always be a NULL terminated string.
 *
 * Return number of characters read on success, 0 if no data is
 * available, -1 on connection shutdown and -2 on error, the
 * ClientInfo.socketerr value is set on socket errors.
 ***********************************************************************/
static int
RecvCmd (ClientInfo *cinfo)
{
  int nread = 0;
  int nreadtotal = 0;
  int nrecv;
  int pass;
  uint8_t nreq;
  char *bptr;
  
  fd_set readset;
  struct timeval timeout;
  int selret;
  
  if ( ! cinfo )
    return -2;
  
  /* First pass request 3 bytes: 2 sequence bytes + 1 length byte */
  nreq = 3;
  pass = 1;
  
  /* Sanity check the receive buffer length */
  if ( cinfo->recvbuflen < 10 )
    {
      lprintf (0, "[%s] Client receiving buffer is too small", cinfo->hostname);
      return -2;
    }
  
  /* Recv until the requested bytes are received or buffer length is reached */
  while ( nread < (cinfo->recvbuflen-1) )
    {
      bptr = cinfo->recvbuf + nread;
      
      if ( (nrecv = recv(cinfo->socket, bptr, nreq - nread, 0)) < 0 )
        {
	  /* The only acceptable error is EAGAIN (no data on non-blocking) */
	  if ( nrecv == -1 && errno != EAGAIN )
	    {
	      lprintf (0, "[%s] Error recv'ing data from client: %s",
		       cinfo->hostname, strerror(errno));
	      cinfo->socketerr = 1;
	      return -2;
	    }
	  /* Exit if no data is available and we haven't received anything yet */
	  else if ( nrecv == -1 && errno == EAGAIN && nreadtotal == 0 )
	    {
	      return 0;
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
		  lprintf (0, "[%s] Timeout receiving DataLink command: %.*s",
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
	  cinfo->socketerr = 2;  /* Indicate an orderly shutdown */
	  return -1;
	}
      
      /* Update recv count and byte count */
      if ( nrecv > 0 )
	{
	  nread += nrecv;
	  nreadtotal += nrecv;
	}
      
      /* Determine read parameters from pre-header of 3 bytes: 'DL<size>' */
      if ( pass == 1 && nread == 3 )
	{
	  /* Verify sequence bytes of 'DL' */
	  if ( *(cinfo->recvbuf) != 'D' || *(cinfo->recvbuf+1) != 'L' )
	    {
	      lprintf (2, "[%s] Error verifying DataLink sequence bytes (%c%c)",
		       cinfo->hostname, *(cinfo->recvbuf), *(cinfo->recvbuf+1));
	      return -2;
	    }
	  
	  /* Determine length of header body */
	  nreq = (uint8_t) *(cinfo->recvbuf+2);
	  
	  /* Sanity check the header size */
	  if ( nreq < 2 || nreq > (cinfo->recvbuflen - 1) )
	    {
	      lprintf (0, "[%s] Pre-header indicates header size: %d",
		       cinfo->hostname, nreq);
	      return -2;
	    }
	  
	  /* Reset to read header body string */
	  nread = 0;
	  pass = 2;
	}
      /* Trap door if the requested number of bytes are received */
      else if ( nread == nreq )
	{
	  break;
	}
    }
  
  /* Make sure buffer is NULL terminated. The command string is
   * allowed to be <= (cinfo->recvbuflen - 1), so this should be safe. */  
  *(cinfo->recvbuf + nread) = '\0';
  
  return nreadtotal;
}  /* End of RecvCmd() */


/***************************************************************************
 * SendPacket:
 *
 * Create and send a packet from given header and packet data strings.
 * The header and packet strings must be NULL-terminated.  If the data
 * argument is NULL a header-only packet will be send.  If the
 * addvalue argument is true the value argument will be appended to
 * the header.  If the addsize argument is true the size of the packet
 * string will be appended to the header.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendPacket (ClientInfo *cinfo, char *header, char *data,
	    int64_t value, int addvalue, int addsize)
{
  char *wirepacket = 0;
  char headerstr[255];
  int headerlen;
  int datalen;
  
  if ( ! cinfo || ! header )
    return -1;
  
  /* Determine length of packet data string */
  datalen = ( data ) ? strlen (data) : 0;
  
  /* Add value and/or size of packet data to header */
  if ( addvalue || addsize )
    {
      if ( addvalue && addsize )
	snprintf (headerstr, sizeof(headerstr), "%s %"PRId64" %u", header, value, datalen);
      else if ( addvalue )
	snprintf (headerstr, sizeof(headerstr), "%s %"PRId64, header, value);
      else
	snprintf (headerstr, sizeof(headerstr), "%s %u", header, datalen);
      
      header = headerstr;
    }
  
  /* Determine length of header and sanity check it */
  headerlen = strlen (header);
  
  if ( headerlen > 255 )
    {
      lprintf (0, "[%s] SendPacket(): Header length is too large: %d",
	       cinfo->hostname, headerlen);
      return -1;
    }
  
  /* Use the send buffer if large enough otherwise allocate memory for wire packet */
  if ( cinfo->sendbuflen >= (3 + headerlen + datalen) )
    {
      wirepacket = cinfo->sendbuf;
    }
  else
    {
      if ( ! (wirepacket = (char *) malloc (3 + headerlen + datalen)) )
	{
	  lprintf (0, "[%s] SendPacket(): Error allocating wire packet buffer",
		   cinfo->hostname);
	  return -1;
	}
    }
  
  /* Populate pre-header sequence of wire packet */
  wirepacket[0] = 'D';
  wirepacket[1] = 'L';
  wirepacket[2] = (uint8_t) headerlen;
  
  /* Copy header and packet data into wire packet */
  memcpy (&wirepacket[3], header, headerlen);  
  
  if ( data )
    memcpy (&wirepacket[3 + headerlen], data, datalen);
  
  /* Send complete wire packet */
  if ( SendData (cinfo, wirepacket, (3 + headerlen + datalen)) )
    {
      if ( cinfo->socketerr != 2 )
	lprintf (0, "[%s] SendPacket(): Error sending packet: %s",
		 cinfo->hostname, strerror(errno));
      return -1;
    }
  
  /* Free the wire packet space if we allocated it */
  if ( wirepacket && wirepacket != cinfo->sendbuf )
    free (wirepacket);
  
  return 0;
}  /* End of SendPacket() */


/***************************************************************************
 * SendRingPacket:
 *
 * Create a packet header for a RingPacket and send() the header and
 * the packet data to the client.  Upon success send update the client
 * transmission counts.
 *
 * The packet header is: "DL<size>PACKET <streamid> <pktid> <hppackettime> <hpdatastart> <hpdataend> <size>"
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
SendRingPacket (ClientInfo *cinfo, RingPacket *packet, void *packetdata)
{
  StreamNode *stream;
  char header[255];
  int headerlen;
  int newstream = 0;
  
  if ( ! cinfo || ! packet || ! packetdata )
    return -1;
  
  /* Create packet header: "PACKET <streamid> <pktid> <hppackettime> <hpdatatime> <size>" */
  headerlen = snprintf (header, sizeof(header),
			"PACKET %s %"PRId64" %"PRId64" %"PRId64" %"PRId64" %u",
			packet->streamid, packet->pktid, packet->pkttime,
			packet->datastart, packet->dataend, packet->datasize);
  
  /* Sanity check header length */
  if ( headerlen > 255 )
    {
      lprintf (0, "[%s] SendRingPacket(): Header length is too large: %d",
	       cinfo->hostname, headerlen);
      return -1;
    }
  
  /* Make sure send buffer is large enough for wire packet */
  if ( cinfo->sendbuflen < (3 + headerlen + packet->datasize) )
    {
      lprintf (0, "[%s] SendRingPacket(): Send buffer not large enough (%d bytes), need %d bytes",
	       cinfo->hostname, cinfo->sendbuflen, 3 + headerlen + packet->datasize);
      return -1;
    }
  
  /* Populate pre-header sequence of wire packet */
  cinfo->sendbuf[0] = 'D';
  cinfo->sendbuf[1] = 'L';
  cinfo->sendbuf[2] = (uint8_t) headerlen;
  
  /* Copy header and packet data into wire packet */
  memcpy (&cinfo->sendbuf[3], header, headerlen);
  
  memcpy (&cinfo->sendbuf[3 + headerlen], packetdata, packet->datasize);
  
  /* Send complete wire packet */
  if ( SendData (cinfo, cinfo->sendbuf, (3 + headerlen + packet->datasize)) )
    {
      if ( cinfo->socketerr != 2 )
	lprintf (0, "[%s] SendRingPacket(): Error sending packet: %s",
		 cinfo->hostname, strerror(errno));
      return -1;
    }
  
  /* Get (creating if needed) the StreamNode for this streamid */
  if ( (stream = GetStreamNode (cinfo->streams, &cinfo->streams_lock,
				packet->streamid, &newstream)) == 0 )
    {
      lprintf (0, "[%s] Error with GetStreamNode for %s",
	       cinfo->hostname, packet->streamid);
      return -1;
    }
  
  if ( newstream )
    {
      lprintf (3, "[%s] New stream for client: %s", cinfo->hostname, packet->streamid);
      cinfo->streamscount++;
    }
  
  /* Update StreamNode packet and byte counts */
  pthread_mutex_lock (&(cinfo->streams_lock));
  stream->txpackets++;
  stream->txbytes += packet->datasize;
  pthread_mutex_unlock (&(cinfo->streams_lock));
  
  /* Update client transmit and counts */
  cinfo->txpackets[0]++;
  cinfo->txbytes[0] += packet->datasize;
  
  /* Update last sent packet ID */
  cinfo->lastid = packet->pktid;  
  
  return 0;
}  /* End of SendRingPacket() */


/***************************************************************************
 * SendData:
 *
 * send() 'buflen' bytes from 'buffer' to 'socket'.  'ident' is
 * a string to include in error messages for identification, usually
 * the address of the remote server.
 *
 * Socket is set to blocking during the send operation.
 *
 * Returns 0 on success and -1 on error, the ClientInfo.socketerr
 * value is set on socket errors.
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
	  /* Create a limited printable buffer for the diagnostic message */
	  char *cp;
	  char pbuffer[100];
	  int maxtoprint;
	  
	  maxtoprint = strcspn ((char *) buffer, "\r\n");
	  if ( maxtoprint > sizeof(pbuffer) )
	    maxtoprint = sizeof(pbuffer);
	  
	  strncpy (pbuffer, (char *) buffer, maxtoprint-1);
	  pbuffer[maxtoprint-1] = '\0';
	  
	  /* Replace unprintable characters with '?', */
	  for ( cp=pbuffer; *cp != '\0'; cp++ )
	    {
	      if ( *cp < 32 || *cp > 126 )
		*cp = '?';
	    }
	  
	  lprintf (0, "[%s] SendData(): Error sending '%s': %s", cinfo->hostname,
		   pbuffer, strerror(errno));
	  cinfo->socketerr = 1;
	}
      
      return -1;
    }
  
  /* Restore original socket flags */
  if ( fcntl(cinfo->socket, F_SETFL, sockflags) == -1 )
    {
      lprintf (0, "[%s] SendData(): Error setting non-blocking flag: %s",
	       cinfo->hostname, strerror(errno));
      return -1;
    }
  
  /* Update the time of the last packet exchange */
  cinfo->lastxchange = HPnow();
  
  return 0;
}  /* End of SendData() */


/***************************************************************************
 * SelectedStreams:
 *
 * Determine the number of streams selected with the current match and
 * reject settings.  Since GetStreamsStack() already applies the match
 * and reject expressions the only thing left to do is count the
 * select streams returned.
 *
 * Returns selected stream count on success and -1 on error.
 ***************************************************************************/
static int
SelectedStreams (RingParams *ringparams, RingReader *reader)
{
  Stack *streams;
  RingStream *ringstream;
  int streamcnt = 0;
  
  if ( ! ringparams || ! reader )
    return -1;  
  
  /* Create a duplicate Stack of currently selected RingStreams */
  streams = GetStreamsStack (ringparams, reader);
  
  /* Count the selected streams */
  while ( (ringstream = StackPop(streams)) )
    {
      free (ringstream);
      streamcnt++;
    }
  
  /* Cleanup stream stack */
  StackDestroy (streams, free);
  
  return streamcnt;
}  /* End of SelectedStreams() */

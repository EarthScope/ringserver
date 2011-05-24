/**************************************************************************
 * clients.c
 *
 * General client related utility functions
 *
 * Copyright 2011 Chad Trabant, IRIS Data Managment Center
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
 * Modified: 2008.260
 **************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include "generic.h"
#include "clients.h"
#include "rbtree.h"
#include "logging.h"


/***********************************************************************
 * RecvData:
 *
 * Read buflen characters from a socket and place them into buffer.
 * This routine can handle fragmented receives and will continue
 * collecting data until the buffer is full.
 *
 * The buffer provided must already be allocated and have enough space
 * for buflen bytes.
 *
 * Return number of characters read on success, -1 on connection
 * shutdown and -2 on error.
 ***********************************************************************/
int
RecvData (int socket, char *buffer, size_t buflen, const char *ident)
{
  int nrecv;
  int nread = 0;
  char *bptr = buffer;
  
  fd_set readset;
  struct timeval timeout;
  int selret;
  
  /* Recv until buflen bytes have been read */
  while ( nread < buflen )
    {
      if ( (nrecv = recv(socket, bptr, buflen-nread, 0)) < 0 )
        {
	  /* The only acceptable error is EAGAIN (no data on non-blocking) */
	  if ( nrecv == -1 && errno != EAGAIN )
	    {
	      lprintf (0, "[%s] Error recving data from client: %s",
		       ident, strerror(errno));
	      return -2;
	    }
	  /* Throttle when no data for a non-blocking socket */
	  else if ( nrecv == -1 && errno == EAGAIN )
            {
	      /* Configure the read descriptor set with only our client socket */
	      FD_ZERO(&readset);
	      FD_SET(socket, &readset);
	      
	      /* Timeout 10 seconds */
	      timeout.tv_sec = 10;
	      timeout.tv_usec = 0;
	      
	      selret = select (socket+1, &readset, NULL, NULL, &timeout);
	      
	      if ( selret == 0 )
		{
		  lprintf (0, "[%s] Timeout receiving data", ident);
		  return -2;
		}
	      else if ( selret == -1 && errno != EINTR )
		{
		  lprintf (0, "[%s] Error with select: %s", ident, strerror(errno));
		  return -2;
		}
	    }
	}
      
      /* Peer completed an orderly shutdown */
      if ( nrecv == 0 )
	return -1;
      
      /* Update recv pointer and byte count */
      if ( nrecv > 0 )
	{
	  bptr += nrecv;
	  nread += nrecv;
	}
    }
  
  return nread;
}  /* End of RecvData() */


/***************************************************************************
 * GetStreamNode:
 *
 * Search the specified binary tree for a given Key and return the
 * StreamNode.  If the Key does not exist create it and add it to the
 * tree.  If adding a new entry in the tree the plock mutex will be
 * locked.  If a new entry was added the value of new will be set to 1
 * otherwise it will be set to 0.
 *
 * Return a pointer to a ChanNode or 0 for error.
 ***************************************************************************/
StreamNode *
GetStreamNode (RBTree *tree, pthread_mutex_t *plock, char *streamid, int *new)
{
  Key     key;
  Key    *newkey;
  RBNode *rbnode;
  StreamNode *stream = 0;  

  /* Generate key */
  key = FVNhash64 (streamid);
  
  /* Search for a matching entry */
  if ( (rbnode = RBFind (tree, &key)) )
    {
      stream = (StreamNode *)rbnode->data;
      *new = 0;
    }
  else
    {
      if ( (newkey = (Key *) malloc (sizeof(Key))) == NULL )
	{
	  lprintf (0, "GetStreamNode: Error allocating new key");
	  return 0;
	}
      
      *newkey = key;
      
      if ( (stream = (StreamNode *) malloc (sizeof(StreamNode))) == NULL )
	{
	  lprintf (0, "GetStreamNode: Error allocating new node");
	  return 0;
	}
      
      /* Initialize the new StreamNode */
      strncpy (stream->streamid, streamid, sizeof(stream->streamid));
      stream->txpackets = 0;
      stream->txbytes = 0;
      stream->rxpackets = 0;
      stream->rxbytes = 0;
      stream->endtimereached = 0;
      
      /* Add the new entry while locking the tree */
      pthread_mutex_lock (plock);
      RBTreeInsert (tree, newkey, stream, 0);
      pthread_mutex_unlock (plock);
      
      *new = 1;
    }
  
  return stream;
}  /* End of GetStreamNode() */


/***************************************************************************
 * AddToString:
 *
 * Concatinate one string to another with a delimiter in-between
 * growing the target string as needed up to a maximum length.  The
 * new addition can be added to either the beggining or end of the
 * string using the where flag:
 *
 * where == 0 means add new addition to end of string
 * where != 0 means add new addition to beginning of string
 * 
 * Return 0 on success, -1 on memory allocation error and -2 when
 * string would grow beyond maximum length.
 ***************************************************************************/
int
AddToString (char **string, char *add, char* delim, int where, int maxlen)
{
  int length;
  char *ptr;
  
  if ( ! string || ! add )
    return -1;
  
  /* If string is empty, allocate space and copy the addition */
  if ( ! *string )
    {
      length = strlen (add) + 1;
      
      if ( length > maxlen )
	return -2;
      
      if ( (*string = (char *) malloc (length)) == NULL )
	return -1;
      
      strcpy (*string, add);
    }
  /* Otherwise add the addition with a delimiter */
  else
    {
      length = strlen (*string) + strlen (delim) + strlen(add) + 1;
      
      if ( length > maxlen )
	return -2;
      
      if ( (ptr = (char *) malloc (length)) == NULL )
	return -1;
      
      /* Put addition at beginning of the string */
      if ( where )
	{
	  snprintf (ptr, length, "%s%s%s",
		    (add) ? add : "",
		    (delim) ? delim : "",
		    *string);
	}
      /* Put addition at end of the string */
      else
	{
	  snprintf (ptr, length, "%s%s%s",
		    *string,
		    (delim) ? delim : "",
		    (add) ? add : "");
	}
      
      /* Free previous string and set pointer to newly allocated space */
      free (*string);
      *string = ptr;
    }
  
  return 0;
}  /* End of AddToString() */

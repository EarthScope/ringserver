/**************************************************************************
 * generic.c
 *
 * Generic utility routines.
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
 * Modified: 2007.355
 **************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#include <libmseed.h>

#include "clients.h"
#include "rbtree.h"
#include "logging.h"
#include "generic.h"


/***************************************************************************
 * SplitStreamID:
 *
 * Split stream ID into separate components: "NET_STA_LOC_CHAN/TYPE".
 * Memory for each component must already be allocated.  If a specific
 * component is not desired set the appropriate argument to NULL.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
SplitStreamID (char *streamid, char *net, char *sta, char *loc, char *chan,
	       char *type)
{
  char *id;
  char *ptr, *top, *next;
  
  if ( ! streamid )
    return -1;
  
  /* Duplicate stream ID */
  if ( ! (id = strdup(streamid)) )
    {
      lprintf (0, "SplitStreamID(): Error duplicating streamid");
      return -1;
    }
  
  /* First truncate after the type if included */
  if ( (ptr = strrchr (id, '/')) )
    {
      *ptr++ = '\0';
      
      /* Copy the type if requested */
      if ( type )
	strcpy (type, ptr);
    }
  
  /* Network */
  top = id;
  if ( (ptr = strchr (top, '_')) )
    {
      next = ptr + 1;
      *ptr = '\0';
      
      if ( net )
	strcpy (net, top);
      
      top = next;
    }
  /* Station */
  if ( (ptr = strchr (top, '_')) )
    {
      next = ptr + 1;
      *ptr = '\0';
      
      if ( sta )
	strcpy (sta, top);
      
      top = next;
    }
  /* Location */
  if ( (ptr = strchr (top, '_')) )
    {
      next = ptr + 1;
      *ptr = '\0';
      
      if ( loc )
	strcpy (loc, top);
      
      top = next;
    }
  /* Channel */
  if ( *top && chan )
    {
      strcpy (chan, top);
    }
  
  /* Free duplicated stream ID */
  if ( id )
    free (id);

  return 0;
}  /* End of SplitStreamID() */


/***************************************************************************
 * HPnow:
 *
 * Return the current time as a high precision epoch on success or
 * HPTERROR on error.
 ***************************************************************************/
hptime_t
HPnow (void)
{
  hptime_t now;
  struct timeval tp;
  
  if ( gettimeofday (&tp, NULL) )
    {
      lprintf (0, "HPnow(): error with gettimeofday()");
      return HPTERROR;
    }
  
  now = ((hptime_t)tp.tv_sec * HPTMODULUS) +
    ((hptime_t)tp.tv_usec * (HPTMODULUS/1000000));
  
  return now;
}  /* End of HPnow() */


/***************************************************************************
 * FVNhash64:
 *
 * Perform a 64 bit Fowler/Noll/Vo hash on a string.  This is a
 * simplified version of the source code found here:
 * http://www.isthe.com/chongo/tech/comp/fnv/index.html.
 *
 * Returns the hash of the string.
 ***************************************************************************/
int64_t
FVNhash64 (char *str)
{
  unsigned char *s = (unsigned char *)str; /* unsigned string */
  
  /* Seed hash value with FVN1_64_INIT */
  int64_t hval = (int64_t)0xcbf29ce484222325ULL;
  
  /* FNV-1 hash each octet of the string */
  while ( *s )
    {
      /* Multiply by the 64 bit FNV magic prime mod 2^64 */
      hval *= (int64_t)0x100000001b3ULL;
      
      /* XOR the bottom with the current octet */
      hval ^= (int64_t)*s++;
    }
  
  return hval;
}  /* End of FNVhash64() */


/***************************************************************************
 * KeyCompare:
 *
 * Compare two Keys passed as void pointers.  The values must be
 * numerically comparable, i.e. numbers (not structures or arrays).
 *
 * Return 1 if a > b, -1 if a < b and 0 otherwise (e.g. equality).
 ***************************************************************************/
int
KeyCompare (const void *a, const void *b)
{
  /* Compare key values */
  if ( *(Key*)a > *(Key*)b )
    return 1;
  
  else if ( *(Key*)a < *(Key*)b )
    return -1;
  
  return 0;
}  /* End of KeyCompare() */


/*********************************************************************
 * IsAllDigits:
 *
 * Return 1 if the specified string is all digits and 0 otherwise.
 *********************************************************************/
int
IsAllDigits (char *string)
{
  int idx;
  int length;

  length = strlen (string);
  
  for ( idx=0; idx < length; idx++ )
    {
      if ( ! strchr("0123456789", string[idx]) )
	return 0;
    }
  
  return 1;
}  /* End of IsAllDigits() */

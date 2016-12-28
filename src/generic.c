/**************************************************************************
 * generic.c
 *
 * Generic utility routines.
 *
 * Copyright 2016 Chad Trabant, IRIS Data Management Center
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
 * Modified: 2016.356
 **************************************************************************/

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <libmseed.h>

#include "clients.h"
#include "generic.h"
#include "logging.h"
#include "rbtree.h"

/***************************************************************************
 * SplitStreamID:
 *
 * Split stream ID into separate components according to this pattern:
 * "id1_id2_id3_id4_id5_id6/TYPE"
 *
 * The delimiter can be set to any character, if specified as 0 (NUL)
 * the default character of underscore is used.  The TYPE is always
 * separated from the ID components with a forward slash.
 *
 * Memory for each component must already be allocated.  Up to
 * 'maxlength' characters will be copied to each component including
 * the terminator.
 *
 * If a specific component is not desired set the appropriate argument
 * to NULL.
 *
 * Returns count of identifiers returned (including type) on success
 * and -1 on error.
 ***************************************************************************/
int
SplitStreamID (char *streamid, char delim, int maxlength,
               char *id1, char *id2, char *id3, char *id4, char *id5, char *id6,
               char *type)
{
  char *ids[6] = {NULL, NULL, NULL, NULL, NULL, NULL};
  char *id;
  char *ptr;
  int idx;
  int length;
  int count = 0;

  if (!streamid)
    return -1;

  /* Set default delimiter */
  if (delim == '\0')
    delim = '_';

  /* Duplicate stream ID */
  if (!(id = strdup (streamid)))
  {
    lprintf (0, "SplitStreamID(): Error duplicating streamid");
    return -1;
  }

  /* First truncate after the type if included */
  if ((ptr = strrchr (id, '/')))
  {
    *ptr++ = '\0';

    /* Copy the type if requested */
    if (type != NULL)
    {
      for (length = 1; *ptr != '\0' && length < maxlength; ptr++, type++, length++)
        *type = *ptr;
      *type = '\0';
      count++;
    }
  }
  else if (type != NULL)
  {
    *type = '\0';
  }

  /* Find delimeters, convert to terminators and set pointer array */
  ptr = id;
  ids[0] = ptr;
  for (idx = 1; idx < 6 && *ptr != '\0'; ptr++)
  {
    if (*ptr == delim)
    {
      *ptr = '\0';
      ids[idx] = ptr + 1;
      idx++;
    }
  }

  /* Copy identifiers if they have been requested and parsed.
     If requested but not parsed, leave as empty strings.
     Only copy up to 'maxlength' characters including terminator. */
  if (id1 != NULL)
  {
    *id1 = '\0';
    if (ids[0] != NULL)
    {
      for (ptr = ids[0], length = 1; *ptr != '\0' && length < maxlength; ptr++, id1++, length++)
        *id1 = *ptr;
      *id1 = '\0';
      count++;
    }
  }
  if (id2 != NULL)
  {
    *id2 = '\0';
    if (ids[1] != NULL)
    {
      for (ptr = ids[1], length = 1; *ptr != '\0' && length < maxlength; ptr++, id2++, length++)
        *id2 = *ptr;
      *id2 = '\0';
      count++;
    }
  }
  if (id3 != NULL)
  {
    *id3 = '\0';
    if (ids[2] != NULL)
    {
      for (ptr = ids[2], length = 1; *ptr != '\0' && length < maxlength; ptr++, id3++, length++)
        *id3 = *ptr;
      *id3 = '\0';
      count++;
    }
  }
  if (id4 != NULL)
  {
    *id4 = '\0';
    if (ids[3] != NULL)
    {
      for (ptr = ids[3], length = 1; *ptr != '\0' && length < maxlength; ptr++, id4++, length++)
        *id4 = *ptr;
      *id4 = '\0';
      count++;
    }
  }
  if (id5 != NULL)
  {
    *id5 = '\0';
    if (ids[4] != NULL)
    {
      for (ptr = ids[4], length = 1; *ptr != '\0' && length < maxlength; ptr++, id5++, length++)
        *id5 = *ptr;
      *id5 = '\0';
      count++;
    }
  }
  if (id6 != NULL)
  {
    *id6 = '\0';
    if (ids[5] != NULL)
    {
      for (ptr = ids[5], length = 1; *ptr != '\0' && length < maxlength; ptr++, id6++, length++)
        *id6 = *ptr;
      *id6 = '\0';
      count++;
    }
  }

  /* Free duplicated stream ID */
  if (id)
    free (id);

  return count;
} /* End of SplitStreamID() */

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

  if (gettimeofday (&tp, NULL))
  {
    lprintf (0, "HPnow(): error with gettimeofday()");
    return HPTERROR;
  }

  now = ((hptime_t)tp.tv_sec * HPTMODULUS) +
        ((hptime_t)tp.tv_usec * (HPTMODULUS / 1000000));

  return now;
} /* End of HPnow() */

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
  while (*s)
  {
    /* Multiply by the 64 bit FNV magic prime mod 2^64 */
    hval *= (int64_t)0x100000001b3ULL;

    /* XOR the bottom with the current octet */
    hval ^= (int64_t)*s++;
  }

  return hval;
} /* End of FNVhash64() */

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
  if (*(Key *)a > *(Key *)b)
    return 1;

  else if (*(Key *)a < *(Key *)b)
    return -1;

  return 0;
} /* End of KeyCompare() */

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

  for (idx = 0; idx < length; idx++)
  {
    if (!strchr ("0123456789", string[idx]))
      return 0;
  }

  return 1;
} /* End of IsAllDigits() */

/**************************************************************************
 * generic.c
 *
 * Generic utility routines.
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
SplitStreamID (const char *streamid, char delim, int maxlength,
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
  ptr    = id;
  ids[0] = ptr;
  for (idx = 1; idx < 6 && *ptr != '\0'; ptr++)
  {
    if (*ptr == delim)
    {
      *ptr     = '\0';
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
 * NSnow:
 *
 * Return the current time as a high precision nanosecond epoch on success or
 * NSTERROR on error.
 ***************************************************************************/
nstime_t
NSnow (void)
{
  struct timeval tp;

  if (gettimeofday (&tp, NULL))
  {
    lprintf (0, "%s(): error with gettimeofday()", __func__);
    return NSTERROR;
  }

  return ((int64_t)tp.tv_sec * 1000000000 +
          (int64_t)tp.tv_usec * 1000);
} /* End of NSnow() */

/***************************************************************************
 * FNVhash64:
 *
 * Perform a 64 bit Fowler/Noll/Vo hash (FNV-1a) on a string.  This is a
 * simplified version of the source code found in draft-eastlake-fnv-21:
 * https://datatracker.ietf.org/doc/html/draft-eastlake-fnv-21
 *
 * Returns the hash of the string.
 ***************************************************************************/
uint64_t
FNVhash64 (const char *str)
{
  uint64_t hval = 0xCBF29CE484222325;
  uint8_t ch;

  if (!str)
    return 0;

  while ((ch = *str++))
    hval = 0x00000100000001B3 * (hval ^ ch);

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
IsAllDigits (const char *string)
{
  int idx;
  int length;

  if (!string)
    return 0;

  length = strlen (string);

  for (idx = 0; idx < length; idx++)
  {
    if (!strchr ("0123456789", string[idx]))
      return 0;
  }

  return 1;
} /* End of IsAllDigits() */

/*********************************************************************
 * HumanSizeString:
 *
 * Convert a size in bytes to a human readable string in KiB, MiB,
 * GiB, etc.
 *
 * A maximum of sizestringlen characters will be written to sizestring
 * and an error will be returned if the string would be longer.
 *
 * Return 0 on success and -1 on error.
 *********************************************************************/
int
HumanSizeString (uint64_t bytes, char *sizestring, size_t sizestringlen)
{
  const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};

  double size = (double)bytes;
  int printed;
  int idx = 0;

  if (!sizestring)
    return -1;

  while (size >= 1024.0 && idx < 7)
  {
    size /= 1024.0;
    idx++;
  }

  if (idx == 0)
    printed = snprintf (sizestring, sizestringlen, "%" PRIu64 " %s", bytes, units[idx]);
  else
    printed = snprintf (sizestring, sizestringlen, "%.1f %s", size, units[idx]);

  return (printed < 0 || printed >= sizestringlen) ? -1 : 0;
} /* End of HumanSizeString() */

/***********************************************************************
 * robust glob pattern matcher
 * ozan s. yigit/dec 1994
 * public domain
 *
 * glob patterns:
 *	*	matches zero or more characters
 *	?	matches any single character
 *	[set]	matches any character in the set
 *	[^set]	matches any character NOT in the set
 *		where a set is a group of characters or ranges. a range
 *		is written as two characters seperated with a hyphen: a-z denotes
 *		all characters between a to z inclusive.
 *	[-set]	set matches a literal hypen and any character in the set
 *	[]set]	matches a literal close bracket and any character in the set
 *
 *	char	matches itself except where char is '*' or '?' or '['
 *	\char	matches char, including any pattern character
 *
 * examples:
 *	a*c		ac abc abbc ...
 *	a?c		acc abc aXc ...
 *	a[a-z]c		aac abc acc ...
 *	a[-a-z]c	a-c aac abc ...
 *
 * Revision 1.4  2004/12/26  12:38:00  ct
 * Changed function name (amatch -> globmatch), variables and
 * formatting for clarity.  Also add matching header globmatch.h.
 *
 * Revision 1.3  1995/09/14  23:24:23  oz
 * removed boring test/main code.
 *
 * Revision 1.2  94/12/11  10:38:15  oz
 * charset code fixed. it is now robust and interprets all
 * variations of charset [i think] correctly, including [z-a] etc.
 *
 * Revision 1.1  94/12/08  12:45:23  oz
 * Initial revision
 ***********************************************************************/

 #define GLOBMATCH_TRUE 1
 #define GLOBMATCH_FALSE 0
 #define GLOBMATCH_NEGATE '^' /* std char set negation char */

 /***********************************************************************
  * GlobMatch:
  *
  * Check if a string matches a globbing pattern.
  *
  * Return 0 if string does not match pattern and non-zero otherwise.
  **********************************************************************/
 int
 GlobMatch (const char *string, const char *pattern)
 {
   int negate;
   int match;
   int c;

   while (*pattern)
   {
     if (!*string && *pattern != '*')
       return GLOBMATCH_FALSE;

     switch (c = *pattern++)
     {

     case '*':
       while (*pattern == '*')
         pattern++;

       if (!*pattern)
         return GLOBMATCH_TRUE;

       if (*pattern != '?' && *pattern != '[' && *pattern != '\\')
         while (*string && *pattern != *string)
           string++;

       while (*string)
       {
         if (GlobMatch (string, pattern))
           return GLOBMATCH_TRUE;
         string++;
       }
       return GLOBMATCH_FALSE;

     case '?':
       if (*string)
         break;
       return GLOBMATCH_FALSE;

       /* set specification is inclusive, that is [a-z] is a, z and
        * everything in between. this means [z-a] may be interpreted
        * as a set that contains z, a and nothing in between.
        */
     case '[':
       if (*pattern != GLOBMATCH_NEGATE)
         negate = GLOBMATCH_FALSE;
       else
       {
         negate = GLOBMATCH_TRUE;
         pattern++;
       }

       match = GLOBMATCH_FALSE;

       while (!match && (c = *pattern++))
       {
         if (!*pattern)
           return GLOBMATCH_FALSE;

         if (*pattern == '-') /* c-c */
         {
           if (!*++pattern)
             return GLOBMATCH_FALSE;
           if (*pattern != ']')
           {
             if (*string == c || *string == *pattern ||
                 (*string > c && *string < *pattern))
               match = GLOBMATCH_TRUE;
           }
           else
           { /* c-] */
             if (*string >= c)
               match = GLOBMATCH_TRUE;
             break;
           }
         }
         else /* cc or c] */
         {
           if (c == *string)
             match = GLOBMATCH_TRUE;
           if (*pattern != ']')
           {
             if (*pattern == *string)
               match = GLOBMATCH_TRUE;
           }
           else
             break;
         }
       }

       if (negate == match)
         return GLOBMATCH_FALSE;

       /* If there is a match, skip past the charset and continue on */
       while (*pattern && *pattern != ']')
         pattern++;
       if (!*pattern++) /* oops! */
         return GLOBMATCH_FALSE;
       break;

     case '\\':
       if (*pattern)
         c = *pattern++;
       break;

     default:
       if (c != *string)
         return GLOBMATCH_FALSE;
       break;
     }

     string++;
   }

   return !*string;
 } /* End of GlobMatch() */

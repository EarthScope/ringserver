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
 * Copyright (C) 2026:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
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
  char *id     = NULL;
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

  /* Find delimiters, convert to terminators and set pointer array */
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

  if (length == 0)
    return 0;

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

  while (size >= 1024.0 && idx < 6)
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

static int _match_charclass (const char **pp, unsigned char c);

/** ************************************************************************
 * @brief Check if a string matches a globbing pattern.
 *
 * Supported semantics:
 * `*` matches zero or more characters, e.g. `*.txt`
 * `?` matches a single character, e.g. `a?c`
 * `[]` matches a set of characters `[abc]`
 * `[a-z]` matches a range of characters `[A-Z]`
 * `[!abc]` negation, matches when no characters in the set, e.g. `[!ABC]` or `[^ABC]`
 * `[!a-z]` negation, matches when no characters in the range, e.g. `[!A-Z]` or `[^A-Z]`
 * `\` prefix to match a literal character, e.g. `\*`, `\?`, `\[`
 *
 * Notes / limitations:
 * - Escapes are not interpreted inside `[...]`; e.g. `[\]]` is a class
 *   containing `\` terminated by the first `]`.
 * - Descending ranges (e.g. `[z-a]`) are treated as the three literal
 *   characters rather than an error.
 * - A trailing `\` with no following character matches a literal `\`.
 *
 * @param string  The string to check.
 * @param pattern The globbing pattern to match.
 *
 * @returns 0 if string does not match pattern and non-zero otherwise.
 ***************************************************************************/
int
GlobMatch (const char *string, const char *pattern)
{
  const char *star_p      = NULL; /* position of the most recent '*' in pattern */
  const char *star_s      = NULL; /* position in string when that '*' was seen */
  unsigned char star_skip = 0;    /* byte to skip past on backtrack, or 0 if none */
  unsigned char c;

  if (string == NULL || pattern == NULL)
    return 0;

  for (;;)
  {
    c = (unsigned char)*pattern++;

    switch (c)
    {
    case '\0':
      /* End of pattern: must also be end of string unless a previous '*'
         can consume more characters. */
      if (*string == '\0')
        return 1;
      if (star_p)
        goto star_backtrack;
      return 0;

    case '?':
      if (*string == '\0')
        goto star_backtrack;
      string++;
      break;

    case '*':
      /* Collapse consecutive '*' */
      while (*pattern == '*')
        pattern++;

      /* Trailing '*' matches everything */
      if (*pattern == '\0')
        return 1;

      /* Determine the literal byte (if any) following the '*'. If it is a
         literal, we can skip string characters that cannot match it. */
      {
        unsigned char next = (unsigned char)*pattern;

        if (next == '\\' && pattern[1])
          next = (unsigned char)pattern[1];
        else if (next == '?' || next == '[')
          next = 0; /* not a literal; skip the optimization */

        star_skip = next;

        if (star_skip)
        {
          const char *found = strchr (string, star_skip);
          if (found == NULL)
            return 0; /* required literal cannot occur in remaining string */
          string = found;
        }
      }

      star_p = pattern - 1;
      star_s = string;
      continue;

    case '[':
    {
      const char *pp = pattern;
      if (*string == '\0')
        goto star_backtrack;
      if (!_match_charclass (&pp, (unsigned char)*string))
        goto star_backtrack;
      pattern = pp;
      string++;
      break;
    }

    case '\\':
      if (*pattern)
        c = (unsigned char)*pattern++;
      /* FALLTHROUGH */

    default:
      if ((unsigned char)*string != c)
        goto star_backtrack;
      string++;
      break;
    }

    continue;

  star_backtrack:
    /* If there was a previous '*', backtrack: let it consume one more
       character and retry from pattern just after that '*'. */
    if (star_p)
    {
      if (*star_s == '\0')
        return 0;

      star_s++;

      /* Reuse the saved fast-forward byte so we don't walk non-matching
         characters one at a time on each retry. */
      if (star_skip)
      {
        const char *found = strchr (star_s, star_skip);
        if (found == NULL)
          return 0;
        star_s = found;
      }
      else if (*star_s == '\0')
      {
        return 0;
      }

      string  = star_s;
      pattern = star_p + 1;
      continue;
    }
    return 0;
  }
}

/***************************************************************************
 * Character class parser helper function.
 *
 *   On entry: *pp points just past '['.
 *             If the class is negated, the next character will be '^'
 *             and is handled inside this function.
 *
 *   On return: *pp is advanced past the closing ']'.
 *
 * Return 1 if c matches the class, 0 otherwise.
 ***************************************************************************/
static int
_match_charclass (const char **pp, unsigned char c)
{
  const char *p;
  int negate  = 0;
  int matched = 0;

  if (pp == NULL || *pp == NULL)
    return 0;

  p = *pp;

  /* Handle negation */
  if (*p == '^' || *p == '!')
  {
    negate = 1;
    p++;
  }

  /* Per glob rules, leading ']' is literal */
  if (*p == ']')
  {
    matched = (c == ']');
    p++;
  }

  /* Per glob rules, leading '-' is literal */
  if (*p == '-')
  {
    matched |= (c == '-');
    p++;
  }

  /* Main loop until ']' or end of string */
  while (*p && *p != ']')
  {
    unsigned char pc = (unsigned char)*p;

    if (p[1] == '-' && p[2] && p[2] != ']' && (unsigned char)pc <= (unsigned char)p[2])
    {
      /* Range X-Y (only ascending ranges are supported) */
      unsigned char start = pc;
      unsigned char end   = (unsigned char)p[2];

      matched |= (c >= start && c <= end);

      p += 3; /* skip X-Y */
    }
    else
    {
      /* Literal character */
      matched |= (c == pc);
      p++;
    }
  }

  /* Malformed class (no closing ']') → no match */
  if (*p != ']')
  {
    *pp = p;
    return 0;
  }

  *pp = p + 1; /* skip ']' */

  return negate ? !matched : matched;
}

/***************************************************************************
 * FinalizeMemStream:
 *
 * Finalize a stream created by open_memstream().  Checks ferror() before
 * and the return value of fclose() to catch write failures on both glibc
 * and macOS libc.  Validates that the resulting byte count fits in an int
 * (required because HTTP response lengths are returned as int throughout
 * http.c).
 *
 * On success returns the byte count as int and transfers ownership of *buf
 * to the caller (caller must free it).
 *
 * On any failure fclose()s the stream (if not NULL), frees *buf, sets
 * *buf to NULL, and returns -1.
 ***************************************************************************/
int
FinalizeMemStream (FILE *stream, char **buf, size_t *len)
{
  if (stream == NULL)
  {
    free (*buf);
    *buf = NULL;
    return -1;
  }

  if (ferror (stream))
  {
    fclose (stream);
    free (*buf);
    *buf = NULL;
    return -1;
  }

  if (fclose (stream) != 0)
  {
    free (*buf);
    *buf = NULL;
    return -1;
  }

  if (*len > INT_MAX)
  {
    free (*buf);
    *buf = NULL;
    return -1;
  }

  return (int)*len;
}

/***************************************************************************
 * AllocPrintf:
 *
 * Allocate a buffer and printf-format into it using two vsnprintf passes:
 * the first measures the required length, the second fills the buffer.
 *
 * On success returns the length and sets *strp to a malloc'd,
 * NUL-terminated string (caller must free).
 *
 * On failure returns -1 and sets *strp to NULL.  Drop-in replacement for
 * the non-standard asprintf().
 ***************************************************************************/
int
AllocPrintf (char **strp, const char *fmt, ...)
{
  va_list ap, ap2;
  int len;

  *strp = NULL;

  va_start (ap, fmt);
  va_copy (ap2, ap);
  len = vsnprintf (NULL, 0, fmt, ap);
  va_end (ap);

  if (len < 0)
  {
    va_end (ap2);
    return -1;
  }

  *strp = (char *)malloc ((size_t)len + 1);
  if (*strp == NULL)
  {
    va_end (ap2);
    return -1;
  }

  vsnprintf (*strp, (size_t)len + 1, fmt, ap2);
  va_end (ap2);

  return len;
}

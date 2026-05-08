/**************************************************************************
 * cmdtoken.c
 *
 * Lightweight command-line tokenizer for command parsing.
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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "cmdtoken.h"

/* Token separators: ASCII space, tab, carriage return, line feed.
 * Callers may pass buffers with embedded CR/LF terminators (e.g. raw
 * line input) and the tokenizer treats them identically to space/tab. */
#define IS_TOKEN_SEP(c) ((c) == ' ' || (c) == '\t' || (c) == '\r' || (c) == '\n')

/***************************************************************************
 * cmdtoken_parse:
 *
 * Tokenize 'line' into 'cmd'.
 *
 * Returns 0 on success, -1 if line is NULL or too long.
 ***************************************************************************/
int
cmdtoken_parse (CmdToken *cmd, const char *line)
{
  char *p;

  if (!cmd || !line)
    return -1;

  memset (cmd, 0, sizeof (CmdToken));

  if (strlen (line) >= CMDTOKEN_MAX_LINE)
    return -1;

  /* Keep an unmodified copy for cmdtoken_rest_after() */
  memcpy (cmd->orig, line, strlen (line) + 1);

  /* Working copy to mutate */
  memcpy (cmd->buf, line, strlen (line) + 1);

  p = cmd->buf;

  while (*p)
  {
    /* Skip leading whitespace */
    while (IS_TOKEN_SEP (*p))
      p++;

    if (*p == '\0')
      break;

    /* Saturate at MAX_TOKENS, set overflow flag */
    if (cmd->argc >= CMDTOKEN_MAX_TOKENS)
    {
      cmd->overflow = 1;
      break;
    }

    /* Record start of this token */
    cmd->argv[cmd->argc++] = p;

    /* Advance to end of token */
    while (*p && !IS_TOKEN_SEP (*p))
      p++;

    /* NUL-terminate the token and advance */
    if (*p)
      *p++ = '\0';
  }

  return 0;
} /* End of cmdtoken_parse() */

/***************************************************************************
 * cmdtoken_eq:
 *
 * Return 1 if argv[i] equals 's' (case-sensitive), 0 otherwise.
 ***************************************************************************/
int
cmdtoken_eq (const CmdToken *cmd, int i, const char *s)
{
  if (!cmd || !s || i < 0 || i >= cmd->argc)
    return 0;

  return (strcmp (cmd->argv[i], s) == 0) ? 1 : 0;
} /* End of cmdtoken_eq() */

/***************************************************************************
 * cmdtoken_eq_nocase:
 *
 * Return 1 if argv[i] equals 's' (case-insensitive), 0 otherwise.
 ***************************************************************************/
int
cmdtoken_eq_nocase (const CmdToken *cmd, int i, const char *s)
{
  if (!cmd || !s || i < 0 || i >= cmd->argc)
    return 0;

  return (strcasecmp (cmd->argv[i], s) == 0) ? 1 : 0;
} /* End of cmdtoken_eq_nocase() */

/***************************************************************************
 * cmdtoken_rest_after:
 *
 * Return a pointer into cmd->orig at the start of the nth token
 * (0-based), preserving all subsequent content verbatim.
 * Returns NULL if n >= argc.
 ***************************************************************************/
const char *
cmdtoken_rest_after (const CmdToken *cmd, int n)
{
  const char *p;
  int found = 0;

  if (!cmd || n < 0 || n >= cmd->argc)
    return NULL;

  p = cmd->orig;

  while (*p)
  {
    /* Skip whitespace between tokens */
    while (IS_TOKEN_SEP (*p))
      p++;

    if (*p == '\0')
      break;

    if (found == n)
      return p;

    found++;

    /* Skip over this token */
    while (*p && !IS_TOKEN_SEP (*p))
      p++;
  }

  /* Reached end without finding the nth token (shouldn't happen if n < argc) */
  return NULL;
} /* End of cmdtoken_rest_after() */

/***************************************************************************
 * cmdtoken_u32:
 *
 * Parse argv[i] as an unsigned 32-bit integer in 'base'.
 * Returns 0 on success, -1 on failure.
 ***************************************************************************/
int
cmdtoken_u32 (const CmdToken *cmd, int i, uint32_t *out, int base)
{
  char *endptr = NULL;
  unsigned long long val;

  if (!cmd || !out || i < 0 || i >= cmd->argc)
    return -1;

  /* strtoull silently accepts a leading '-' and negates, producing huge
   * unsigned values.  Reject any sign character so that unsigned really
   * means unsigned. */
  if (cmd->argv[i][0] == '-' || cmd->argv[i][0] == '+')
    return -1;

  errno = 0;
  val   = strtoull (cmd->argv[i], &endptr, base);

  if (errno != 0 || endptr == cmd->argv[i] || *endptr != '\0')
    return -1;

  if (val > UINT32_MAX)
    return -1;

  *out = (uint32_t)val;
  return 0;
} /* End of cmdtoken_u32() */

/***************************************************************************
 * cmdtoken_u64:
 *
 * Parse argv[i] as an unsigned 64-bit integer in 'base'.
 * Returns 0 on success, -1 on failure.
 ***************************************************************************/
int
cmdtoken_u64 (const CmdToken *cmd, int i, uint64_t *out, int base)
{
  char *endptr = NULL;
  unsigned long long val;

  if (!cmd || !out || i < 0 || i >= cmd->argc)
    return -1;

  /* strtoull silently accepts a leading '-' and negates, producing huge
   * unsigned values.  Reject any sign character so that unsigned really
   * means unsigned. */
  if (cmd->argv[i][0] == '-' || cmd->argv[i][0] == '+')
    return -1;

  errno = 0;
  val   = strtoull (cmd->argv[i], &endptr, base);

  if (errno != 0 || endptr == cmd->argv[i] || *endptr != '\0')
    return -1;

  *out = (uint64_t)val;
  return 0;
} /* End of cmdtoken_u64() */

/***************************************************************************
 * cmdtoken_i64:
 *
 * Parse argv[i] as a signed 64-bit integer in 'base'.
 * Returns 0 on success, -1 on failure.
 ***************************************************************************/
int
cmdtoken_i64 (const CmdToken *cmd, int i, int64_t *out, int base)
{
  char *endptr = NULL;
  long long val;

  if (!cmd || !out || i < 0 || i >= cmd->argc)
    return -1;

  errno = 0;
  val   = strtoll (cmd->argv[i], &endptr, base);

  if (errno != 0 || endptr == cmd->argv[i] || *endptr != '\0')
    return -1;

  *out = (int64_t)val;
  return 0;
} /* End of cmdtoken_i64() */

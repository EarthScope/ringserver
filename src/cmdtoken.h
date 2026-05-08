/**************************************************************************
 * cmdtoken.h
 *
 * Lightweight command-line tokenizer for line parsing.
 *
 * Splits a null-terminated line on whitespace into at most
 * CMDTOKEN_MAX_TOKENS string tokens, and provides helpers for string
 * token comparison, rest-of-line access, and numeric token parsing.
 *
 * Usage pattern:
 *
 *   CmdToken cmd;
 *   cmdtoken_parse (&cmd, line);
 *   if (cmdtoken_eq_nocase (&cmd, 0, "COMMAND")) { ... }
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

#ifndef CMDTOKEN_H
#define CMDTOKEN_H

#include <stdint.h>

/* Maximum number of whitespace-separated tokens per command line */
#define CMDTOKEN_MAX_TOKENS 16

/* Maximum accepted line length */
#define CMDTOKEN_MAX_LINE 8096

typedef struct CmdToken
{
  char  buf[CMDTOKEN_MAX_LINE];        /* mutable working copy; tokens null-terminated here */
  char  orig[CMDTOKEN_MAX_LINE];       /* unmodified copy; used by cmdtoken_rest_after() */
  int   argc;                          /* number of tokens found; valid argv indices are [0, argc) */
  char *argv[CMDTOKEN_MAX_TOKENS];     /* pointers into buf */
  int   overflow;                      /* 1 if MAX_TOKENS was reached before end of line */
} CmdToken;

int cmdtoken_parse (CmdToken *cmd, const char *line);
int cmdtoken_eq (const CmdToken *cmd, int i, const char *s);
int cmdtoken_eq_nocase (const CmdToken *cmd, int i, const char *s);
const char *cmdtoken_rest_after (const CmdToken *cmd, int n);
int cmdtoken_u32 (const CmdToken *cmd, int i, uint32_t *out, int base);
int cmdtoken_u64 (const CmdToken *cmd, int i, uint64_t *out, int base);
int cmdtoken_i64 (const CmdToken *cmd, int i, int64_t *out, int base);

#endif /* CMDTOKEN_H */

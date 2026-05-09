/**************************************************************************
 * auth.c
 *
 * Authorization handling functions.
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
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "auth.h"
#include "logging.h"
#include "yyjson.h"

/* Upper bound on the compound regex pattern assembled from an auth-helper
 * "allowed_streams" / "forbidden_streams" array. Stream IDs
 * are short (FDSN SIDs are well under 100 chars); 64 KiB easily holds
 * hundreds of entries. */
#define AUTH_MAX_COMPOUND_PATTERN_SIZE ((size_t)(64 * 1024))

static int ApplyPermissionsJSON (ClientInfo *cinfo, const char *json_string);
static int ExecuteAuthProgram (char *envp[], uint32_t timeout_seconds,
                               char *output, size_t output_size,
                               char *error, size_t error_size);

/**************************************************************************
 * PerformAuth:
 *
 * Perform user authentication and set permissions for the client.
 * Authentication and permissions are determined by executing an external
 * program with USER/PASS and/or TOKEN provided as environment variables.
 *
 * @param cinfo: The ::ClientInfo to perform auth for and set permissions.
 * @param username: The username to authenticate.
 * @param password: The password to authenticate.
 * @param jwtoken: The JWT token to authenticate.
 *
 * @return: 0 on success, -1 on error.
 **************************************************************************/
int
PerformAuth (ClientInfo *cinfo,
             const char *username, const char *password,
             const char *jwtoken)
{
  char username_env[1024] = {0};
  char password_env[1024] = {0};
  char jwtoken_env[1024]  = {0};
#define OUTPUT_BUFFER_SIZE 4096
  char output[OUTPUT_BUFFER_SIZE] = {0};
  char error[OUTPUT_BUFFER_SIZE]  = {0};

  char *envp[4] = {NULL, NULL, NULL, NULL};
  int envidx    = 0;

  int status = -1;

  if (!cinfo)
  {
    lprintf (0, "%s() Invalid arguments", __func__);
    return -1;
  }

  /* Set environment variables for auth program.  Truncation is fatal */
  if (username && username[0])
  {
    if (snprintf (username_env, sizeof (username_env), "AUTH_USERNAME=%s", username) >= (int)sizeof (username_env))
    {
      lprintf (0, "[%s] AUTH_USERNAME too large (%zu bytes), cannot authenticate", cinfo->hostname, strlen (username));
      return -1;
    }
    envp[envidx++] = username_env;
  }

  if (password && password[0])
  {
    if (snprintf (password_env, sizeof (password_env), "AUTH_PASSWORD=%s", password) >= (int)sizeof (password_env))
    {
      lprintf (0, "[%s] AUTH_PASSWORD too large (%zu bytes), cannot authenticate", cinfo->hostname, strlen (password));
      return -1;
    }
    envp[envidx++] = password_env;
  }

  if (jwtoken && jwtoken[0])
  {
    if (snprintf (jwtoken_env, sizeof (jwtoken_env), "AUTH_JWTOKEN=%s", jwtoken) >= (int)sizeof (jwtoken_env))
    {
      lprintf (0, "[%s] AUTH_JWTOKEN too large (%zu bytes), cannot authenticate", cinfo->hostname, strlen (jwtoken));
      return -1;
    }
    envp[envidx++] = jwtoken_env;
  }

  /* Execute auth program */
  status = ExecuteAuthProgram (envp, config.auth.timeout_sec,
                               output, sizeof (output),
                               error, sizeof (error));
  if (status != 0)
  {
    lprintf (0, "[%s] Error executing auth program: %s", cinfo->hostname, config.auth.program);
    return -1;
  }

  /* Apply permissions from returned JSON */
  if (ApplyPermissionsJSON (cinfo, output) != 0)
  {
    return -1;
  }

  /* Store auth method when authentication succeeded */
  if (cinfo->permissions & AUTHENTICATED)
  {
    if (jwtoken && jwtoken[0])
      cinfo->auth_method = AUTH_JWT;
    else if (username && username[0])
      cinfo->auth_method = AUTH_USERPASS;

    /* Fall back to the provided username if not returned in the JSON response */
    if (cinfo->auth_username[0] == '\0' && username && username[0])
    {
      strncpy (cinfo->auth_username, username, sizeof (cinfo->auth_username) - 1);
      cinfo->auth_username[sizeof (cinfo->auth_username) - 1] = '\0';
    }
  }

  if (config.verbose >= 3)
  {
    lprintf (0, "Authorizations for '%s' at %s:",
             cinfo->auth_username[0] != '\0' ? cinfo->auth_username : "unknown-user",
             cinfo->hostname);
    if (cinfo->permissions & CONNECT_PERMISSION)
      lprintf (0, "  CONNECT_PERMISSION");
    if (cinfo->permissions & WRITE_PERMISSION)
      lprintf (0, "  WRITE_PERMISSION");
    if (cinfo->permissions & TRUST_PERMISSION)
      lprintf (0, "  TRUST_PERMISSION");

    if (cinfo->allowedstr)
      lprintf (0, "  Allowed streams: %s", cinfo->allowedstr);

    if (cinfo->forbiddenstr)
      lprintf (0, "  Forbidden streams: %s", cinfo->forbiddenstr);
  }

  return 0;
}

/**************************************************************************
 * ApplyPermissionsJSON:
 *
 * Parse specified JSON and set permissions for the client.
 *
 * The JSON object is expected to contain the following fields:
 * {
 *  "username": "<username or JWT sub claim>",
 *  "authenticated": <boolean>,
 *  "write_permission": <boolean>,
 *  "trust_permission": <boolean>,
 *  "allowed_streams": ["streamregex1", "streamregex2"],
 *  "forbidden_streams": ["streamregex3", "streamregex4"]
 * }
 *
 * No fields are required, and if missing are assumed to be false or empty.
 *
 * @param cinfo: The ::ClientInfo to apply the permissions to.
 * @param json_string: The JSON string to parse.
 *
 * @return: 0 on success, -1 on error.
 **************************************************************************/
static int
ApplyPermissionsJSON (ClientInfo *cinfo, const char *json_string)
{
  const char *username_str;
  yyjson_doc *json;
  yyjson_val *root;
  yyjson_val *streams_array;
  yyjson_val *thread_iter = NULL;
  size_t idx, max, element_size;
  size_t pattern_size;
  char *ptr;

  if (!cinfo || !json_string)
  {
    lprintf (0, "%s() Invalid arguments", __func__);
    return -1;
  }

  if ((json = yyjson_read (json_string, strlen (json_string), 0)) == NULL)
  {
    lprintf (0, "[%s] Error parsing permission JSON", cinfo->hostname);
    return -1;
  }
  root = yyjson_doc_get_root (json);

  cinfo->permissions = NO_PERMISSION;

  /* Store username from JSON response */
  username_str = yyjson_get_str (yyjson_obj_get (root, "username"));
  if (username_str)
  {
    strncpy (cinfo->auth_username, username_str, sizeof (cinfo->auth_username) - 1);
    cinfo->auth_username[sizeof (cinfo->auth_username) - 1] = '\0';
  }

  if (yyjson_get_bool (yyjson_obj_get (root, "authenticated")))
  {
    cinfo->permissions |= AUTHENTICATED;

    /* Successful authentication implies connect permission */
    cinfo->permissions |= CONNECT_PERMISSION;
  }

  if (yyjson_get_bool (yyjson_obj_get (root, "write_permission")))
  {
    cinfo->permissions |= WRITE_PERMISSION;
  }

  if (yyjson_get_bool (yyjson_obj_get (root, "trust_permission")))
  {
    cinfo->permissions |= TRUST_PERMISSION;
  }

  /* Build a compound regex pattern for any allowed streams returned */
  if ((streams_array = yyjson_obj_get (root, "allowed_streams")) &&
      yyjson_get_type (streams_array) == YYJSON_TYPE_ARR &&
      yyjson_get_len (streams_array) > 0)
  {
    pattern_size = 0;

    /* Calculate total size of compound pattern, for each element + 1 for '|'.
     * Skip non-string elements (yyjson_get_len() on arrays/objects returns
     * the child count, which would over-count space and, worse, lead to
     * strncat(dest, NULL, n) UB in the concat loop below).  Guard the
     * accumulator against both overflow and an excessive total. */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      if (!yyjson_is_str (thread_iter))
        continue;

      element_size = yyjson_get_len (thread_iter);

      if (element_size > AUTH_MAX_COMPOUND_PATTERN_SIZE ||
          pattern_size > AUTH_MAX_COMPOUND_PATTERN_SIZE - element_size - 1)
      {
        lprintf (0, "[%s] allowed_streams compound pattern exceeds %zu bytes, rejecting",
                 cinfo->hostname, AUTH_MAX_COMPOUND_PATTERN_SIZE);
        yyjson_doc_free (json);
        return -1;
      }

      pattern_size += element_size + 1;
    }

    /* Replace allocation for compound pattern, +1 for terminator */
    free (cinfo->allowedstr);
    if ((cinfo->allowedstr = calloc (pattern_size + 1, 1)) == NULL)
    {
      lprintf (0, "[%s] Cannot allocate memory for allowed pattern", cinfo->hostname);
      yyjson_doc_free (json);
      return -1;
    }

    /* Create compound pattern */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      if (!yyjson_is_str (thread_iter))
        continue;

      element_size = yyjson_get_len (thread_iter);

      if (element_size > 0)
      {
        strncat (cinfo->allowedstr, yyjson_get_str (thread_iter), element_size);
        strncat (cinfo->allowedstr, "|", 2);
      }
    }

    /* Replace last '|' with terminator */
    if ((ptr = strrchr (cinfo->allowedstr, '|')) != NULL)
      *ptr = '\0';

    if (cinfo->allowedstr[0] == '\0')
    {
      free (cinfo->allowedstr);
      cinfo->allowedstr = NULL;
    }

    /* Compile regular expression */
    if (RingAllowed (cinfo->reader, cinfo->allowedstr) < 0)
    {
      lprintf (0, "[%s] Error with RingAllowed for '%s'", cinfo->hostname, cinfo->allowedstr);
      yyjson_doc_free (json);
      return -1;
    }
  }

  /* Build a compound regex pattern for any forbidden streams returned */
  if ((streams_array = yyjson_obj_get (root, "forbidden_streams")) &&
      yyjson_get_type (streams_array) == YYJSON_TYPE_ARR &&
      yyjson_get_len (streams_array) > 0)
  {
    pattern_size = 0;

    /* Calculate total size of compound pattern, for each element + 1 for '|'.
     * Skip non-string elements and guard the accumulator; see the matching
     * comment in the allowed_streams block above. */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      if (!yyjson_is_str (thread_iter))
        continue;

      element_size = yyjson_get_len (thread_iter);

      if (element_size > AUTH_MAX_COMPOUND_PATTERN_SIZE ||
          pattern_size > AUTH_MAX_COMPOUND_PATTERN_SIZE - element_size - 1)
      {
        lprintf (0, "[%s] forbidden_streams compound pattern exceeds %zu bytes, rejecting",
                 cinfo->hostname, AUTH_MAX_COMPOUND_PATTERN_SIZE);
        yyjson_doc_free (json);
        return -1;
      }

      pattern_size += element_size + 1;
    }

    /* Replace allocation for compound pattern, +1 for terminator */
    free (cinfo->forbiddenstr);
    if ((cinfo->forbiddenstr = calloc (pattern_size + 1, 1)) == NULL)
    {
      lprintf (0, "[%s] Cannot allocate memory for forbidden pattern", cinfo->hostname);
      yyjson_doc_free (json);
      return -1;
    }

    /* Create compound pattern */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      if (!yyjson_is_str (thread_iter))
        continue;

      element_size = yyjson_get_len (thread_iter);

      if (element_size > 0)
      {
        strncat (cinfo->forbiddenstr, yyjson_get_str (thread_iter), element_size);
        strncat (cinfo->forbiddenstr, "|", 2);
      }
    }

    /* Replace last '|' with terminator */
    if ((ptr = strrchr (cinfo->forbiddenstr, '|')) != NULL)
      *ptr = '\0';

    if (cinfo->forbiddenstr[0] == '\0')
    {
      free (cinfo->forbiddenstr);
      cinfo->forbiddenstr = NULL;
    }

    /* Compile regular expression */
    if (RingForbidden (cinfo->reader, cinfo->forbiddenstr) < 0)
    {
      lprintf (0, "[%s] Error with RingForbidden for '%s'", cinfo->hostname, cinfo->forbiddenstr);
      yyjson_doc_free (json);
      return -1;
    }
  }

  yyjson_doc_free (json);

  return 0;
}

/**************************************************************************
 * ExecuteAuthProgram:
 *
 * Execute a program with the specified environment variables and timeout.
 *
 * @param program: The program to execute, absolute path recommended.
 * @param envp: The environment variables to set for the child process.
 * @param timeout_seconds: The timeout duration in seconds.
 * @param output: Pointer to a buffer to store the program's output.
 * @param error: Pointer to a buffer to store the program's error output.
 *
 * @return: exit status of the program, 0 on success, non-zero on error.
 **************************************************************************/
static int
ExecuteAuthProgram (char *envp[], uint32_t timeout_seconds,
                    char *output, size_t output_size,
                    char *error, size_t error_size)
{
  int stdout_pipe[2] = {-1, -1};
  int stderr_pipe[2] = {-1, -1};
  posix_spawn_file_actions_t actions;
  int actions_initialized = 0;
  int exit_status         = -1;
  pid_t pid               = -1;
  int status;
  size_t out_written  = 0;
  size_t err_written  = 0;
  size_t out_overflow = 0;
  size_t err_overflow = 0;

  if (!envp || !output || !error)
  {
    lprintf (0, "%s() Invalid arguments", __func__);
    return -1;
  }

  if (!config.auth.program)
  {
    lprintf (0, "%s() No auth program configured", __func__);
    return -1;
  }

  /* Create pipes for stdout and stderr */
  if (pipe (stdout_pipe) == -1)
  {
    lprintf (0, "%s() Error creating stdout pipe: %s", __func__, strerror (errno));
    goto cleanup;
  }
  if (pipe (stderr_pipe) == -1)
  {
    lprintf (0, "%s() Error creating stderr pipe: %s", __func__, strerror (errno));
    goto cleanup;
  }

  /* Initialize spawn file actions */
  posix_spawn_file_actions_init (&actions);
  actions_initialized = 1;

  /* Add file actions to redirect stdout and stderr */
  posix_spawn_file_actions_adddup2 (&actions, stdout_pipe[1], STDOUT_FILENO);
  posix_spawn_file_actions_adddup2 (&actions, stderr_pipe[1], STDERR_FILENO);

  /* Close read ends in the child */
  posix_spawn_file_actions_addclose (&actions, stdout_pipe[0]);
  posix_spawn_file_actions_addclose (&actions, stderr_pipe[0]);

  if (config.verbose >= 2)
  {
    lprintf (2, "Running auth program: %s", config.auth.program);

    if (config.auth.argv)
    {
      for (int idx = 0; config.auth.argv[idx]; idx++)
      {
        if (idx == 0)
          continue;
        lprintf (2, "  Arg[%d]: %s", idx, config.auth.argv[idx]);
      }
    }
  }

  /* Spawn the process */
  if ((status = posix_spawn (&pid, config.auth.program, &actions, NULL, config.auth.argv, envp)) != 0)
  {
    lprintf (0, "%s() Could not spawn process, status: %d, %s", __func__, status, strerror (status));
    pid = -1;
    goto cleanup;
  }

  /* Close write ends in the parent */
  close (stdout_pipe[1]);
  stdout_pipe[1] = -1;
  close (stderr_pipe[1]);
  stderr_pipe[1] = -1;

  /* Set read ends non-blocking so drain reads never block between polls */
  fcntl (stdout_pipe[0], F_SETFL, O_NONBLOCK);
  fcntl (stderr_pipe[0], F_SETFL, O_NONBLOCK);

  /* Drain both pipes concurrently with poll() while the child runs, then
   * waitpid once both are at EOF.  This prevents the classic deadlock where
   * the child blocks on a full pipe and waitpid() never returns. */
  {
    struct timespec deadline;
    clock_gettime (CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += timeout_seconds;

    int out_open = 1;
    int err_open = 1;
    int timed_out = 0;

    while (out_open || err_open)
    {
      struct timespec now;
      clock_gettime (CLOCK_MONOTONIC, &now);

      int64_t remaining_ms = (int64_t)(deadline.tv_sec - now.tv_sec) * 1000 +
                             (int64_t)(deadline.tv_nsec - now.tv_nsec) / 1000000;

      if (remaining_ms <= 0)
      {
        lprintf (1, "%s() Process (%s) did not exit after %u seconds, killing",
                 __func__, config.auth.program, timeout_seconds);
        kill (pid, SIGKILL);
        timed_out = 1;
        break;
      }

      struct pollfd fds[2];
      int nfds = 0;
      int out_idx = -1;

      if (out_open)
      {
        out_idx          = nfds;
        fds[nfds].fd     = stdout_pipe[0];
        fds[nfds].events = POLLIN;
        nfds++;
      }
      if (err_open)
      {
        fds[nfds].fd     = stderr_pipe[0];
        fds[nfds].events = POLLIN;
        nfds++;
      }

      int ready = poll (fds, (nfds_t)nfds, (int)remaining_ms);

      if (ready < 0)
      {
        if (errno == EINTR)
          continue;
        lprintf (0, "%s() poll() error: %s", __func__, strerror (errno));
        kill (pid, SIGKILL);
        timed_out = 1;
        break;
      }

      /* Drain each ready pipe; read in a tight loop until EAGAIN or EOF */
      for (int pi = 0; pi < nfds; pi++)
      {
        if (!(fds[pi].revents & (POLLIN | POLLHUP | POLLERR)))
          continue;

        int is_out      = (pi == out_idx);
        char *buf       = is_out ? output : error;
        size_t bufsz    = is_out ? output_size : error_size;
        size_t *written = is_out ? &out_written : &err_written;
        size_t *overflow = is_out ? &out_overflow : &err_overflow;

        for (;;)
        {
          char tmp[4096];
          ssize_t nr = read (fds[pi].fd, tmp, sizeof (tmp));

          if (nr < 0)
          {
            if (errno == EINTR)
              continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
              break; /* no more data right now */
            lprintf (0, "%s() Error reading %s from auth program: %s",
                     __func__, is_out ? "stdout" : "stderr", strerror (errno));
            break;
          }

          if (nr == 0)
          {
            /* EOF — pipe closed by child */
            close (fds[pi].fd);
            if (is_out)
            {
              stdout_pipe[0] = -1;
              out_open       = 0;
            }
            else
            {
              stderr_pipe[0] = -1;
              err_open       = 0;
            }
            break;
          }

          /* Append to buffer up to cap, count overflow past cap */
          size_t space   = (*written < bufsz - 1) ? (bufsz - 1 - *written) : 0;
          size_t to_copy = (nr < (ssize_t)space) ? (size_t)nr : space;

          if (to_copy > 0)
          {
            memcpy (buf + *written, tmp, to_copy);
            *written += to_copy;
          }

          if ((size_t)nr > to_copy)
            *overflow += (size_t)nr - to_copy;
        }
      }
    }

    /* Both pipes are at EOF (or we timed out); safe to reap the child */
    waitpid (pid, &status, 0);
    pid = -1;

    if (out_overflow > 0)
      lprintf (1, "%s() Auth program stdout truncated (%zu bytes past %zu-byte buffer)",
               __func__, out_overflow, output_size - 1);
    if (err_overflow > 0)
      lprintf (1, "%s() Auth program stderr truncated (%zu bytes past %zu-byte buffer)",
               __func__, err_overflow, error_size - 1);

    if (timed_out)
      goto cleanup;
  }

  /* NUL-terminate captured output */
  output[out_written] = '\0';
  error[err_written]  = '\0';

  /* Determine exit status */
  if (WIFEXITED (status))
  {
    lprintf (3, "%s() Process (%s) exited with status %d",
             __func__, config.auth.program, WEXITSTATUS (status));

    if (error[0])
      lprintf (0, "%s() Error from auth program: %s", __func__, error);

    exit_status = WEXITSTATUS (status);
  }
  else if (WIFSIGNALED (status))
  {
    lprintf (3, "%s() Process (%s) terminated by signal %d",
             __func__, config.auth.program, WTERMSIG (status));

    if (error[0])
      lprintf (0, "%s() Error from auth program: %s", __func__, error);
  }

cleanup:
  if (actions_initialized)
    posix_spawn_file_actions_destroy (&actions);
  if (stdout_pipe[0] != -1)
    close (stdout_pipe[0]);
  if (stdout_pipe[1] != -1)
    close (stdout_pipe[1]);
  if (stderr_pipe[0] != -1)
    close (stderr_pipe[0]);
  if (stderr_pipe[1] != -1)
    close (stderr_pipe[1]);

  return exit_status;
}

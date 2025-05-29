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
 * Copyright (C) 2025:
 * @author Chad Trabant, EarthScope Data Services
 **************************************************************************/

#include <errno.h>
#include <signal.h>
#include <spawn.h>
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

static int apply_permissions_json (ClientInfo *cinfo, const char *json_string);
static int execute_auth_program (char *envp[], uint32_t timeout_seconds,
                                 char *output, size_t output_size,
                                 char *error, size_t error_size);

/**************************************************************************
 * perform_auth:
 *
 * Perform user authentication and set permissions for the client.
 * Authentication and permissions are determined by executing an external
 * program with USER/PASS and/or TOKEN provided as environment variables.
 *
 * @param cinfo: The ::ClientInfo to perform auth for and set permissions.
 * @param user: The username to authenticate.
 * @param pass: The password to authenticate.
 * @param jwtoken: The JWT token to authenticate.
 *
 * @return: 0 on success, -1 on error.
 **************************************************************************/
int
perform_auth (ClientInfo *cinfo,
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

  /* Set environment variables for auth program */
  if (username && username[0])
  {
    snprintf (username_env, sizeof (username_env), "AUTH_USERNAME=%s", username);
    envp[envidx++] = username_env;
  }

  if (password && password[0])
  {
    snprintf (password_env, sizeof (password_env), "AUTH_PASSWORD=%s", password);
    envp[envidx++] = password_env;
  }

  if (jwtoken && jwtoken[0])
  {
    snprintf (jwtoken_env, sizeof (jwtoken_env), "AUTH_JWTOKEN=%s", jwtoken);
    envp[envidx++] = jwtoken_env;
  }

  /* Execute auth program */
  status = execute_auth_program (envp, config.auth.timeout_sec,
                                 output, sizeof (output),
                                 error, sizeof (error));
  if (status != 0)
  {
    lprintf (0, "[%s] Error executing auth program: %s", cinfo->hostname, config.auth.program);
    return -1;
  }

  /* Apply permissions from returned JSON */
  if (apply_permissions_json (cinfo, output) != 0)
  {
    return -1;
  }

  if (config.verbose >= 3)
  {
    lprintf (0, "Authorizations for %s:", cinfo->hostname);
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
 * apply_permissions_json:
 *
 * Parse specified JSON and set permissions for the client.
 *
 * The JSON object is expected to contain the following fields:
 * {
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
 * @param json: The JSON string to parse.
 *
 * @return: 0 on success, -1 on error.
 **************************************************************************/
static int
apply_permissions_json (ClientInfo *cinfo, const char *json_string)
{
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

    /* Calculate total size of compound pattern, for each element + 1 for '|' */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      pattern_size += yyjson_get_len (thread_iter) + 1;
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
      element_size = yyjson_get_len (thread_iter);

      strncat (cinfo->allowedstr, yyjson_get_str (thread_iter), element_size);
      strncat (cinfo->allowedstr, "|", 1);
    }

    /* Replace last '|' with terminator */
    if ((ptr = strrchr (cinfo->allowedstr, '|')) != NULL)
      *ptr = '\0';

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

    /* Calculate total size of compound pattern, for each element + 1 for '|' */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      pattern_size += yyjson_get_len (thread_iter) + 1;
    }

    /* Replace allocation for compound pattern, +1 for terminator */
    free (cinfo->forbiddenstr);
    if ((cinfo->forbiddenstr = calloc (pattern_size + 1, 1)) == NULL)
    {
      lprintf (0, "[%s] Cannot allocate memory for allowed pattern", cinfo->hostname);
      yyjson_doc_free (json);
      return -1;
    }

    /* Create compound pattern */
    yyjson_arr_foreach (streams_array, idx, max, thread_iter)
    {
      element_size = yyjson_get_len (thread_iter);

      strncat (cinfo->forbiddenstr, yyjson_get_str (thread_iter), element_size);
      strncat (cinfo->forbiddenstr, "|", 1);
    }

    /* Replace last '|' with terminator */
    if ((ptr = strrchr (cinfo->forbiddenstr, '|')) != NULL)
      *ptr = '\0';

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
 * execute_program:
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
execute_auth_program (char *envp[], uint32_t timeout_seconds,
                      char *output, size_t output_size,
                      char *error, size_t error_size)
{
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
  int stdout_pipe[2];
  int stderr_pipe[2];
  if (pipe (stdout_pipe) == -1 || pipe (stderr_pipe) == -1)
  {
    lprintf (0, "%s() Error creating pipes: %s", __func__, strerror (errno));
    return -1;
  }

  /* Initialize spawn file actions */
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init (&actions);

  /* Add file actions to redirect stdout and stderr */
  posix_spawn_file_actions_adddup2 (&actions, stdout_pipe[1], STDOUT_FILENO);
  posix_spawn_file_actions_adddup2 (&actions, stderr_pipe[1], STDERR_FILENO);

  /* Close write ends in the child */
  posix_spawn_file_actions_addclose (&actions, stdout_pipe[0]);
  posix_spawn_file_actions_addclose (&actions, stderr_pipe[0]);

  if (config.verbose >= 2)
  {
    lprintf (2, "Running auth program: %s", config.auth.program);

    for (int idx = 0; config.auth.argv[idx]; idx++)
    {
      if (idx == 0)
        continue; // Skip the program name
      lprintf (2, "  Arg[%d]: %s", idx, config.auth.argv[idx]);
    }
  }

  /* Spawn the process */
  pid_t pid;
  int status;
  if ((status = posix_spawn (&pid, config.auth.program, &actions, NULL, config.auth.argv, envp)) != 0)
  {
    lprintf (0, "%s() Could not spawn process, status: %d, %s", __func__, status, strerror (status));
    return -1;
  }

  /* Close write ends in the parent */
  close (stdout_pipe[1]);
  close (stderr_pipe[1]);

  /* Set up timeout loop using time-based approach */
  struct timespec start_time, current_time;
  clock_gettime (CLOCK_MONOTONIC, &start_time);

  pid_t wait_result;
  while ((wait_result = waitpid (pid, &status, WNOHANG)) == 0)
  {
    /* Check if timeout has lapsed */
    clock_gettime (CLOCK_MONOTONIC, &current_time);

    double elapsed_time = (current_time.tv_sec - start_time.tv_sec) +
                          (current_time.tv_nsec - start_time.tv_nsec) / 1e9;

    if (elapsed_time >= timeout_seconds)
    {
      lprintf (1, "%s() Process (%s) did not exit after %d seconds, killing",
               __func__, config.auth.program, timeout_seconds);

      kill (pid, SIGKILL);       // Kill the spawned process
      waitpid (pid, &status, 0); // Wait for it to terminate
      break;
    }

    usleep (100000); // Check the process every 100ms
  }

  /* Read output and error from pipes */
  read (stdout_pipe[0], output, output_size);
  read (stderr_pipe[0], error, error_size);

  /* Cleanup */
  posix_spawn_file_actions_destroy (&actions);
  close (stdout_pipe[0]);
  close (stderr_pipe[0]);

  int exit_status = -1;
  if (WIFEXITED (status))
  {
    lprintf (3, "%s() Process (%s) exited with status %d",
             __func__, config.auth.program, WEXITSTATUS (status));

    if (error[0])
    {
      lprintf (0, "%s() Error from auth program: %s", __func__, error);
    }

    exit_status = WEXITSTATUS (status);
  }
  else if (WIFSIGNALED (status))
  {
    lprintf (3, "%s() Process (%s) terminated by signal %d",
             __func__, config.auth.program, WTERMSIG (status));

    if (error[0])
    {
      lprintf (0, "%s() Error from auth program: %s", __func__, error);
    }

    exit_status = -1;
  }

  return exit_status;
}

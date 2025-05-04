/**************************************************************************
 * ringserver.c
 *
 * Multi-threaded TCP generic ring buffer data server with support
 * for SeedLink, DataLink and HTTP protocols.
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
#include <stdlib.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "clients.h"
#include "ringserver.h"
#include "mseedscan.h"
#include "generic.h"
#include "logging.h"
#include "config.h"

static const char *reference_config_file;

static char *GetOptVal (int argcount, char **argvec, int argopt);
static int ReadEnvironmentVariables (void);
static int SetParameter (const char *paramstring, int dynamiconly);
static int YesNo (const char *value);
static int InitServerSocket (char *portstr, ListenOptions options);
static int ConfigMSWrite (char *value);
static int AddListenThreads (ListenPortParams *lpp);
static uint64_t CalcSize (const char *sizestr);
static int AddMSeedScanThread (const char *configstr);
static int AddServerThread (ServerThreadType type, void *params);
static int AddIPNet (IPNet **pplist, const char *network, const char *limitstr);
static int SetAuthCommand (const char *program, char **argv, int argc);

/***************************************************************************
 * Usage:
 *
 * Print usage message and exit.
 ***************************************************************************/
static void
Usage (int level)
{
  fprintf (stderr, "%s version %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] [configfile]\n\n", PACKAGE);
  fprintf (stderr, " ## Options ##\n"
                   " -V             Print program version and exit\n"
                   " -h             Print this usage message\n"
                   " -H             Print an extended usage message\n"
                   " -C             Print configuration file options and descriptions\n"
                   " -v             Be more verbose, multiple flags can be used\n"
                   " -I serverID    Server ID (default 'Ring Server')\n"
                   " -m maxclnt     Maximum number of concurrent clients (currently %d)\n"
                   " -M maxperIP    Maximum number of concurrent clients per address (currently %d)\n"
                   " -Rd ringdir    Directory for ring buffer files, required\n"
                   " -Rs bytes      Ring packet buffer file size in bytes (default 1 Gibibyte)\n"
                   " -Rp pktsize    Maximum ring packet data size in bytes (currently %" PRIu32 ")\n"
                   " -NOMM          Do not memory map the packet buffer, use memory instead\n"
                   " -L port        Listen for connections on port, all protocols (default off)\n"
                   " -T logdir      Directory to write transfer logs (default is no logs)\n"
                   " -Ti hours      Transfer log writing interval (default 24 hours)\n"
                   " -Tp prefix     Prefix to add to transfer log files (default is none)\n"
                   " -STDERR        Send all console output to stderr instead of stdout\n"
                   "\n",
           config.maxclients,
           config.maxclientsperip,
           config.pktsize - (uint32_t)sizeof (RingPacket));

  if (level >= 1)
  {
    fprintf (stderr,
             " -MSWRITE format  Write all received miniSEED to an archive\n"
             " -MSSCAN dir      Scan directory for files containing miniSEED\n"
             " -VOLATILE        Create volatile ring, contents not saved to files\n"
             "\n");

    fprintf (stderr,
             "The 'format' argument is expanded for each record using the\n"
             "flags below.  Some preset archive layouts are available:\n"
             "\n"
             "BUD   : %%n/%%s/%%s.%%n.%%l.%%c.%%Y.%%j  (BUD layout)\n"
             "CHAN  : %%n.%%s.%%l.%%c  (channel)\n"
             "QCHAN : %%n.%%s.%%l.%%c.%%q  (quality-channel-day)\n"
             "CDAY  : %%n.%%s.%%l.%%c.%%Y:%%j:#H:#M:#S  (channel-day)\n"
             "SDAY  : %%n.%%s.%%Y:%%j  (station-day)\n"
             "HSDAY : %%h/%%n.%%s.%%Y:%%j  (host-station-day)\n"
             "\n"
             "Archive definition flags\n"
             "  n : Network code, white space removed\n"
             "  s : Station code, white space removed\n"
             "  l : Location code, white space removed\n"
             "  c : Channel code, white space removed\n"
             "  q : Record quality indicator (D, R, Q, M), single character\n"
             "  Y : Year, 4 digits\n"
             "  y : Year, 2 digits zero padded\n"
             "  j : Day of year, 3 digits zero padded\n"
             "  H : Hour, 2 digits zero padded\n"
             "  M : Minute, 2 digits zero padded\n"
             "  S : Second, 2 digits zero padded\n"
             "  F : Fractional seconds, 4 digits zero padded\n"
             "  D : Current year-day time stamp of the form YYYYDDD\n"
             "  L : Data record length in bytes\n"
             "  r : Sample rate (Hz) as a rounded integer\n"
             "  R : Sample rate (Hz) as a float with 6 digit precision\n"
             "  h : Host name of client submitting data \n"
             "  %% : The percent (%%) character\n"
             "  # : The number (#) character\n"
             "\n"
             "The flags are prefaced with either the %% or # modifier.  The %% modifier\n"
             "indicates a defining flag while the # indicates a non-defining flag.\n"
             "All records with the same set of defining flags will be written to the\n"
             "same file. Non-defining flags will be expanded using the values in the\n"
             "first record for the resulting file name.\n"
             "\n");
  }

  exit (1);
} /* End of Usage() */


/***************************************************************************
 * ProcessParam:
 *
 * Process the command line parameters.
 *
 * The configuration parameter structure is not blocked when updating
 * because this function is called before any other threads are started.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
int
ProcessParam (int argcount, char **argvec)
{
  char paramstr[512] = {0};
  int optind;
  int rv;

  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
  {
    if (strcmp (argvec[optind], "-V") == 0)
    {
      fprintf (stderr, "%s version: %s\n", PACKAGE, VERSION);
      exit (0);
    }
    else if (strcmp (argvec[optind], "-h") == 0)
    {
      Usage (0);
    }
    else if (strcmp (argvec[optind], "-H") == 0)
    {
      Usage (1);
    }
    else if (strcmp (argvec[optind], "-C") == 0)
    {
      printf ("%s", reference_config_file);
      exit (0);
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      size_t vcount = config.verbose + strspn (&argvec[optind][1], "v");
      snprintf (paramstr, sizeof (paramstr), "Verbosity %zu", vcount);
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-I") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "ServerID \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-m") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MaxClients %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-M") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MaxClientsPerIP %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-Rd") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "RingDirectory \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-Rs") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "RingSize %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-Rp") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MaxPacketSize %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-NOMM") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MemoryMapRing 0");
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-L") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "ListenPort %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-DL") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "DataLinkPort %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-SL") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "SeedLinkPort %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-HL") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "HTTPPort %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-T") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "TransferLogDirectory \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-Ti") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "TransferLogInterval %s", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-Tp") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "TransferLogPrefix \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-STDERR") == 0)
    {
      /* Redirect all output destined for stdout to stderr */
      if (dup2 (fileno (stderr), fileno (stdout)) < 0)
      {
        lprintf (0, "Error redirecting stdout to stderr: %s", strerror (errno));
        exit (1);
      }
    }
    else if (strcmp (argvec[optind], "-MSWRITE") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MSeedWrite \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-MSSCAN") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "MSeedScan \"%s\"", GetOptVal (argcount, argvec, optind++));
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strcmp (argvec[optind], "-VOLATILE") == 0)
    {
      snprintf (paramstr, sizeof (paramstr), "VolatileRing 1");
      if (SetParameter (paramstr, 0) <= 0)
        exit (1);
    }
    else if (strncmp (argvec[optind], "-", 1) == 0)
    {
      lprintf (0, "Unknown option: %s", argvec[optind]);
      exit (1);
    }
    else
    {
      if (config.configfile)
      {
        lprintf (0, "Unknown option: %s", argvec[optind]);
        exit (1);
      }
      else
      {
        config.configfile = strdup(argvec[optind]);
      }
    }
  }

  /* Report the program version */
  lprintf (0, "%s version: %s", PACKAGE, VERSION);

  /* Read environment variables */
  if ((rv = ReadEnvironmentVariables ()) < 0)
  {
    lprintf (0, "Error reading environment variables");
    exit (1);
  }
  else
  {
    lprintf (3, "Read %d configuration environment variables", rv);
  }

  /* Process the config file */
  if (config.configfile)
  {
    lprintf (1, "Reading configuration from %s", config.configfile);

    if (ReadConfigFile (config.configfile, 0, 0))
    {
      lprintf (0, "Error reading config file");
      exit (1);
    }
  }

  /* Set default server ID if not already set */
  if (!config.serverid)
  {
    config.serverid = strdup ("Ring Server");
  }

  /* Add localhost (loopback) to write permission list if list empty */
  if (!config.writeips)
  {
    if (AddIPNet (&config.writeips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to write permission list");
      return -1;
    }
  }

  /* Add localhost (loopback) to trusted list if list empty */
  if (!config.trustedips)
  {
    if (AddIPNet (&config.trustedips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to trusted list");
      return -1;
    }
  }

  /* Check that a ring directory is specified or is volatile */
  if (!config.ringdir && !config.volatilering)
  {
    lprintf (0, "Error, ring directory must be specified");
    exit (1);
  }

  return 0;
} /* End of ProcessParam() */

/***************************************************************************
 * GetOptVal:
 * Return the value to a command line option; checking that the value is
 * itself not an option (starting with '-') and is not past the end of
 * the argument list.
 *
 * argcount: total arguments in argvec
 * argvec: argument list
 * argopt: index of option to process, value is expected to be at argopt+1
 *
 * Returns value on success and exits with error message on failure
 ***************************************************************************/
static char *
GetOptVal (int argcount, char **argvec, int argopt)
{
  if (argvec == NULL || argvec[argopt] == NULL)
  {
    lprintf (0, "GetOptVal(): NULL option requested");
    exit (1);
  }

  if ((argopt + 1) < argcount && *argvec[argopt + 1] != '-')
    return argvec[argopt + 1];

  lprintf (0, "Option %s requires a value", argvec[argopt]);
  exit (1);
} /* End of GetOptVal() */

/***************************************************************************
 * ReadEnvironmentVariables:
 *
 * Check for and processes recognized environment variables.
 * If the value of an environment variable is 'DISABLE' then the
 * corresponding parameter is not set.
 *
 * Returns >=0 on the number of variables read on success
 * Returns  -1 on error
 ****************************************************************************/
static int
ReadEnvironmentVariables (void)
{
  const char *envvar;
  char paramstr[512] = {0};
  int count = 0;

  if ((envvar = getenv ("RS_CONFIG_FILE")) && strcasecmp (envvar, "DISABLE"))
  {
    free (config.configfile);
    config.configfile = strdup (envvar);
    count++;
  }

  if ((envvar = getenv ("RS_RING_DIRECTORY")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "RingDirectory \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_RING_SIZE")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "RingSize %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MAX_PACKET_SIZE")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MaxPacketSize %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MEMORY_MAP_RING")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MemoryMapRing %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_AUTO_RECOVERY")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AutoRecovery %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_LISTEN_PORT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "ListenPort %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_SEEDLINK_PORT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "SeedLinkPort %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_DATALINK_PORT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "DataLinkPort %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_HTTP_PORT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "HTTPPort %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_SERVER_ID")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "ServerID \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_VERBOSITY")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "Verbosity %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MAX_CLIENTS_PER_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MaxClientsPerIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MAX_CLIENTS")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MaxClients %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_CLIENT_TIMEOUT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "ClientTimeout %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_NETIO_TIMEOUT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "NetIOTimeout %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_RESOLVE_HOSTNAMES")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "ResolveHostnames %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TIME_WINDOW_LIMIT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TimeWindowLimit %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRANSFER_LOG_DIRECTORY")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TransferLogDirectory \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRANSFER_LOG_INTERVAL")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TransferLogInterval %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRANSFER_LOG_PREFIX")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TransferLogPrefix \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRANSFER_LOG_TX")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TransferLogTX %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRANSFER_LOG_RX")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TransferLogRX %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_AUTH_COMMAND")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AuthCommand %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_AUTH_TIMEOUT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AuthTimeout %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_WRITE_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "WriteIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TRUSTED_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TrustedIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  /* Deprecated LimitIP parameter, replaced by AllowedStreamsIP */
  if ((envvar = getenv ("RS_LIMIT_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AllowedStreamsIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;

    lprintf (1, "RS_LIMIT_IP environment variable is deprecated, use RS_ALLOWED_STREAMS_IP instead");
  }

  if ((envvar = getenv ("RS_ALLOWED_STREAMS_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AllowedStreamsIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_FORBIDDEN_STREAMS_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "ForbiddenStreamsIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  /* Deprecated LimitIP parameter, replaced by AcceptIP */
  if ((envvar = getenv ("RS_MATCH_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MatchIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;

    lprintf (1, "RS_MATCH_IP environment variable is deprecated, use RS_ACCEPT_IP instead");
  }

  if ((envvar = getenv ("RS_ACCEPT_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "AcceptIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  /* Deprecated RS_REJECT_IP parameter, replaced by DenyIP */
  if ((envvar = getenv ("RS_REJECT_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "RejectIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;

    lprintf (1, "RS_REJECT_IP environment variable is deprecated, use RS_DENY_IP instead");
  }

  if ((envvar = getenv ("RS_DENY_IP")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "DenyIP %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_WEB_ROOT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "WebRoot \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_HTTP_HEADER")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "HTTPHeader \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MSEED_WRITE")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MSeedWrite \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TLS_CERT_FILE")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TLSCertFile \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TLS_KEY_FILE")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TLSKeyFile \"%s\"", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_TLS_VERIFY_CLIENT_CERT")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "TLSVerifyClientCert %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_MSEED_SCAN")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "MSeedScan %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  if ((envvar = getenv ("RS_VOLATILE_RING")) && strcasecmp (envvar, "DISABLE"))
  {
    snprintf (paramstr, sizeof (paramstr), "VolatileRing %s", envvar);
    if (SetParameter (paramstr, 0) <= 0)
      return -1;
    count++;
  }

  return count;
}  /* End of ReadEnvironmentVariables() */

/***************************************************************************
 * ReadConfigFile:
 *
 * Reads the ringserver configuration from a file containing simple
 * key-value pairs.
 *
 * If the dynamiconly argument is true only "dynamic" parameters will be
 * read from the file.
 *
 * The mtime argument is the modification time of the file, if known.
 *
 * See SetParameter() for recognized parameters.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
int
ReadConfigFile (char *configfile, int dynamiconly, time_t mtime)
{
  FILE *cfile;
  char line[200];
  char *ptr;
  int linecount = 0;
  int rv;

  IPNet *ipnet     = NULL;
  IPNet *nextipnet = NULL;

  if (!configfile)
    return -1;

  /* Open config file */
  if ((cfile = fopen (configfile, "r")) == NULL)
  {
    lprintf (0, "Error opening config file %s: %s",
             configfile, strerror (errno));
    return -1;
  }

  /* If no mtime was supplied, stat the file to get it */
  if (!mtime)
  {
    struct stat cfstat;

    if (fstat (fileno (cfile), &cfstat))
    {
      lprintf (0, "Error stating config file %s: %s",
               configfile, strerror (errno));
      return -1;
    }

    mtime = cfstat.st_mtime;
  }

  /* Reset the configuration file mtime */
  param.configfilemtime = mtime;

  /* Clear the write, trusted, allowed, forbidden, match and reject IPs lists */
  ipnet = nextipnet = config.writeips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  config.writeips = NULL;

  ipnet = nextipnet = config.trustedips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  config.trustedips = NULL;

  ipnet = nextipnet = config.allowedips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    if (ipnet->limitstr)
      free (ipnet->limitstr);
    free (ipnet);
    ipnet = nextipnet;
  }
  config.allowedips = NULL;

  ipnet = nextipnet = config.forbiddenips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    if (ipnet->limitstr)
      free (ipnet->limitstr);
    free (ipnet);
    ipnet = nextipnet;
  }
  config.forbiddenips = NULL;

  ipnet = nextipnet = config.acceptips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  config.acceptips = NULL;

  ipnet = nextipnet = config.denyips;
  while (ipnet)
  {
    nextipnet = ipnet->next;
    free (ipnet);
    ipnet = nextipnet;
  }
  config.denyips = NULL;

  /* Clear webroot specification */
  if (config.webroot)
  {
    free (config.webroot);
    config.webroot = NULL;
  }

  /* Clear existing HTTP headers */
  if (config.httpheaders)
  {
    free (config.httpheaders);
    config.httpheaders = NULL;
  }

  /* Clear existing transfer log parameters */
  if (config.tlog.basedir)
  {
    config.tlog.mode = TLOG_NONE;
    free (config.tlog.basedir);
    config.tlog.basedir = NULL;
    free (config.tlog.prefix);
    config.tlog.prefix = NULL;
  }

  /* Read and process all lines */
  while (fgets (line, sizeof (line), cfile))
  {
    linecount++;

    ptr = line;
    while (isspace ((int)*ptr))
      ptr++;

    /* Skip blank and comment lines */
    if (*ptr == '\0' || *ptr == '#')
      continue;

    /* Remove trailing newline */
    if ((ptr = strrchr (ptr, '\n')))
      *ptr = '\0';

    /* Remove trailing carriage return */
    if ((ptr = strrchr (ptr, '\r')))
      *ptr = '\0';

    rv = SetParameter (line, dynamiconly);

    if (rv < 0)
    {
      lprintf (0, "Error processing config file line (line %d): %s", linecount, line);
      return -1;
    }
    else if (rv == 0)
    {
      lprintf (0, "Unrecognized parameter in config file (line %d): %s", linecount, line);
    }
  } /* Done reading config file lines */

  /* Close config file */
  if (fclose (cfile))
  {
    lprintf (0, "Error closing config file %s: %s",
             configfile, strerror (errno));
    return -1;
  }

  /* Add localhost (loopback) to write permission list if list empty */
  if (!config.writeips)
  {
    if (AddIPNet (&config.writeips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to write permission list");
      return -1;
    }
  }

  /* Add localhost (loopback) to trusted list if list empty */
  if (!config.trustedips)
  {
    if (AddIPNet (&config.trustedips, "localhost/128", NULL))
    {
      lprintf (0, "Error adding localhost/128 to trusted list");
      return -1;
    }
  }

  return 0;
} /* End of ReadConfigFile() */

/***************************************************************************
 * SetParameter:
 *
 * Parses a configuration parameter from a string, validates and sets the
 * appropriate config value.
 *
 * If the dynamiconly argument is true only "dynamic" parameters will be
 * updated.  The configuration parameter structure is locked when updating
 * dynamic parameters to avoid race conditions with readers.
 *
 * Recognized parameters ("D" labeled parameters are dynamic):
 *
 * RingDirectory <dir>
 * RingSize <size>
 * MaxPacketID <id>  (deprecated, parsed and prints a warning)
 * MaxPacketSize <size>
 * MemoryMapRing <1|0>
 * AutoRecovery <2|1|0>
 * ListenPort <port> [flags]
 * SeedLinkPort <port> [flags]
 * DataLinkPort <port> [flags]
 * HTTPPort <port> [flags]
 * [D] ServerID <server id>
 * [D] Verbosity <level>
 * [D] MaxClientsPerIP <max>
 * [D] MaxClients <max>
 * [D] ClientTimeout <timeout>
 * [D] NetIOTimeout <timeout>
 * [D] ResolveHostnames <1|0>
 * [D] TimeWindowLimit <percent>
 * [D] TransferLogDirectory <dir>
 * [D] TransferLogInterval <interval>
 * [D] TransferLogPrefix <prefix>
 * [D] TransferLogTX <1|0>
 * [D] TransferLogRX <1|0>
 * [D] AuthCommand <command>
 * [D] AuthTimeout <timeout>
 * [D] WriteIP <IP>[/netmask]
 * [D] TrustedIP <IP>[/netmask]
 * [D] AllowedStreamsIP <IP>[/netmask] <streamlimit>
 * [D] ForbiddenStreamsIP <IP>[/netmask] <streamlimit>
 * [D] AcceptIP <IP>[/netmask]
 * [D] DenyIP <IP>[/netmask]
 * [D] WebRoot <web content root>
 * [D] HTTPHeader <HTTP header>
 * [D[ TLSCertFile <file>
 * [D] TLSKeyFile <file>
 * [D] TLSVerifyClientCert 0|1
 * MSeedWrite <format>
 * MSeedScan <directory>
 * VolatileRing 0|1
 *
 * Returns >0 on the number of fields on success
 * Returns  0 on unrecognized parameter
 * Returns -1 on error
 ****************************************************************************/
static int
SetParameter (const char *paramstring, int dynamiconly)
{
#define MAX_FIELDS 10
  char resolved_path[PATH_MAX] = {0};
  char *field[MAX_FIELDS]      = {0};
  char parambuf[200]           = {0};

  int fieldcount = 0;

  ListenPortParams lpp = ListenPortParams_INITIALIZER;
  uint32_t u32val;
  int intval;

  if (!paramstring)
    return -1;

  if (strlen (paramstring) > sizeof (parambuf))
  {
    lprintf (0, "%s() Error, parameter string too long (%zu characters): '%.20s ...'",
             __func__, strlen (paramstring), paramstring);
    return -1;
  }

  lprintf (3, "Processing parameter: %s", paramstring);

  /* Set field pointers to space-delimited strings, while handling
   * quoted strings and trailing '#' comments. */
  strncpy (parambuf, paramstring, sizeof (parambuf) - 1);
  for (int idx = 0, start = 1, inside_quotes = 0;
       parambuf[idx] && fieldcount < MAX_FIELDS;
       idx++)
  {
    if (parambuf[idx] == '"')
    {
      inside_quotes = !inside_quotes;
      parambuf[idx] = '\0';
    }
    else if (!inside_quotes && isspace ((int)parambuf[idx]))
    {
      parambuf[idx] = '\0';
      start         = 1;
    }
    else if (!inside_quotes && parambuf[idx] == '#')
    {
      parambuf[idx] = '\0';
      break;
    }
    else if (start)
    {
      field[fieldcount] = parambuf + idx;
      fieldcount++;
      start = 0;
    }
  }

  /* Search for recognized parameters */
  if (!strcasecmp ("RingDirectory", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if (realpath (field[1], resolved_path) == NULL)
    {
      lprintf (0, "Error with %s value, cannot find path: %s",
               field[0], field[1]);
      return -1;
    }

    if (access (resolved_path, W_OK))
    {
      lprintf (0, "Error with %s value, cannot write to directory: %s",
               field[0], resolved_path);
      return -1;
    }

    free (config.ringdir);
    config.ringdir = strdup (resolved_path);
  }
  else if (!strcasecmp ("RingSize", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    config.ringsize = CalcSize (field[1]);

    if (config.ringsize == 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("MaxPacketID", field[0]))
  {
    lprintf (0, "MaxPacketID config file option no longer used, ignoring: %s", paramstring);
  }
  else if (!strcasecmp ("MaxPacketSize", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if (sscanf (field[1], "%" SCNu32, &config.pktsize) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    /* Add size of RingPacket header to specified value */
    config.pktsize += sizeof (RingPacket);
  }
  else if (!strcasecmp ("AutoRecovery", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.autorecovery = intval;
  }
  else if (!strcasecmp ("MemoryMapRing", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.memorymapring = intval;
  }
  else if ((!strcasecmp ("ListenPort", field[0]) ||
            !strcasecmp ("DataLinkPort", field[0]) ||
            !strcasecmp ("SeedLinkPort", field[0]) ||
            !strcasecmp ("HTTPPort", field[0])) &&
           fieldcount >= 2)
  {
    if (dynamiconly)
      return fieldcount;

    if (strlen (field[1]) >= sizeof (lpp.portstr))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    strcpy (lpp.portstr, field[1]);

    if (!strcasecmp ("DataLinkPort", field[0]))
      lpp.protocols = PROTO_DATALINK;
    else if (!strcasecmp ("SeedLinkPort", field[0]))
      lpp.protocols = PROTO_SEEDLINK;
    else if (!strcasecmp ("HTTPPort", field[0]))
      lpp.protocols = PROTO_HTTP;
    else
      lpp.protocols = 0;

    lpp.options   = 0;
    lpp.socket    = -1;

    /* Parse optional protocol flags to limit allowed protocols */
    for (int idx = 2, allow_protocols = (lpp.protocols == 0) ? 1 : 0;
         idx < fieldcount;
         idx++)
    {
      if (allow_protocols && strcasestr ("DataLink", field[idx]))
        lpp.protocols |= PROTO_DATALINK;
      else if (allow_protocols && strcasestr ("SeedLink", field[idx]))
        lpp.protocols |= PROTO_SEEDLINK;
      else if (allow_protocols && strcasestr ("HTTP", field[idx]))
        lpp.protocols |= PROTO_HTTP;

      else if (!strcasecmp ("TLS", field[idx]))
        lpp.options |= ENCRYPTION_TLS;

      else if (!strcasecmp ("IPv4", field[idx]))
        lpp.options |= FAMILY_IPv4;
      else if (!strcasecmp ("IPv6", field[idx]))
        lpp.options |= FAMILY_IPv6;
      else
      {
        lprintf (0, "Error with listen port config flag: %s", paramstring);
        lprintf (0, "  Unrecognized or unsupported flag: %s", field[idx]);
        return -1;
      }
    }

    if (lpp.protocols == 0)
      lpp.protocols = PROTO_ALL;

    if (!AddListenThreads (&lpp))
    {
      lprintf (0, "Error adding server thread for listen port config parameter: %s", paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("ServerID", field[0]) && fieldcount == 2)
  {
    free (config.serverid);
    config.serverid = strdup (field[1]);
  }
  else if (!strcasecmp ("TLSCertFile", field[0]) && fieldcount == 2)
  {
    if (realpath (field[1], resolved_path) == NULL)
    {
      lprintf (0, "Error with %s value, cannot find path: %s",
               field[0], field[1]);
      return -1;
    }

    if (access (resolved_path, R_OK))
    {
      lprintf (0, "Error with %s value, cannot write to directory: %s",
               field[0], resolved_path);
      return -1;
    }

    free (config.tlscertfile);
    config.tlscertfile = strdup (resolved_path);
  }
  else if (!strcasecmp ("TLSKeyFile", field[0]) && fieldcount == 2)
  {
    if (realpath (field[1], resolved_path) == NULL)
    {
      lprintf (0, "Error with %s value, cannot find path: %s",
               field[0], field[1]);
      return -1;
    }

    if (access (resolved_path, R_OK))
    {
      lprintf (0, "Error with %s value, cannot write to directory: %s",
               field[0], resolved_path);
      return -1;
    }

    free (config.tlskeyfile);
    config.tlskeyfile = strdup (resolved_path);
  }
  else if (!strcasecmp ("TLSVerifyClientCert", field[0]) && fieldcount == 2)
  {
    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.tlsverifyclientcert = intval;
  }
  else if (!strcasecmp ("Verbosity", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%d", &intval) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
    config.verbose = intval;
  }
  else if (!strcasecmp ("MaxClientsPerIP", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%" SCNu32, &u32val) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.maxclientsperip = u32val;
  }
  else if (!strcasecmp ("MaxClients", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%" SCNu32, &u32val) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.maxclients = u32val;
  }
  else if (!strcasecmp ("ClientTimeout", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%" SCNu32, &u32val) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.clienttimeout = u32val;
  }
  else if (!strcasecmp ("NetIOTimeout", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%" SCNu32, &u32val) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.netiotimeout = u32val;
  }
  else if (!strcasecmp ("ResolveHostnames", field[0]) && fieldcount == 2)
  {
    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.resolvehosts = intval;
  }
  else if (!strcasecmp ("TimeWindowLimit", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%d", &intval) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (intval < 0 || intval > 100)
    {
      lprintf (0, "Error, config parameter %s must be between 0 and 100: %s", field[0], paramstring);
      return -1;
    }

    config.timewinlimit = (intval > 0) ? intval / 100.0 : 0.0;
  }
  else if (!strcasecmp ("TransferLogDirectory", field[0]) && fieldcount == 2)
  {
    if (realpath (field[1], resolved_path) == NULL)
    {
      lprintf (0, "Error with %s value, cannot find path: %s",
               field[0], field[1]);
      return -1;
    }

    if (access (resolved_path, W_OK))
    {
      lprintf (0, "Error with %s value, cannot write to directory: %s",
               field[0], resolved_path);
      return -1;
    }

    free (config.tlog.basedir);
    char *basedir = strdup (resolved_path);
    config.tlog.basedir = basedir;

    /* Enable both TX and RX logging as defaults */
    config.tlog.mode = TLOG_TX | TLOG_RX;
  }
  else if (!strcasecmp ("TransferLogInterval", field[0]) && fieldcount == 2)
  {
    float fvalue;
    if (sscanf (field[1], "%f", &fvalue) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    /* Parameter is specified in hours but value needs to be seconds */
    config.tlog.interval = (int)(fvalue * 3600.0 + 0.5);
  }
  else if (!strcasecmp ("TransferLogPrefix", field[0]) && fieldcount == 2)
  {
    free (config.tlog.prefix);
    config.tlog.prefix = strdup (field[1]);
  }
  else if (!strcasecmp ("TransferLogTX", field[0]) && fieldcount == 2)
  {
    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (intval)
      config.tlog.mode |= TLOG_TX;
    else
      config.tlog.mode &= ~TLOG_TX;
  }
  else if (!strcasecmp ("TransferLogRX", field[0]) && fieldcount == 2)
  {
    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (intval)
      config.tlog.mode |= TLOG_RX;
    else
      config.tlog.mode &= ~TLOG_RX;
  }
  else if (!strcasecmp ("AuthCommand", field[0]) && fieldcount >= 2)
  {
    if (SetAuthCommand (field[1], &field[2], fieldcount - 2))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("AuthTimeout", field[0]) && fieldcount == 2)
  {
    if (sscanf (field[1], "%" SCNu32, &u32val) != 1)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.auth.timeout_sec = u32val;
  }
  else if (!strcasecmp ("WriteIP", field[0]) && fieldcount == 2)
  {
    if (AddIPNet (&config.writeips, field[1], NULL))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("TrustedIP", field[0]) && fieldcount == 2)
  {
    if (AddIPNet (&config.trustedips, field[1], NULL))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if ((!strcasecmp ("AllowedStreamsIP", field[0]) || !strcasecmp ("LimitIP", field[0])) && fieldcount == 3)
  {
    if (AddIPNet (&config.allowedips, field[1], field[2]))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (!strcasecmp ("LimitIP", field[0]))
      lprintf (1, "LimitIP config parameter is deprecated, use AllowedStreamsIP instead");
  }
  else if (!strcasecmp ("ForbiddenStreamsIP", field[0]) && fieldcount == 3)
  {
    if (AddIPNet (&config.forbiddenips, field[1], field[2]))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if ((!strcasecmp ("AcceptIP", field[0]) || !strcasecmp ("MatchIP", field[0])) && fieldcount == 2)
  {
    if (AddIPNet (&config.acceptips, field[1], NULL))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (!strcasecmp ("MatchIP", field[0]))
      lprintf (1, "MatchIP config parameter is deprecated, use AcceptIP instead");
  }
  else if ((!strcasecmp ("DenyIP", field[0]) || !strcasecmp ("RejectIP", field[0])) && fieldcount == 2)
  {
    if (AddIPNet (&config.denyips, field[1], NULL))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    if (!strcasecmp ("RejectIP", field[0]))
      lprintf (1, "RejectIP config parameter is deprecated, use DenyIP instead");
  }
  else if (!strcasecmp ("WebRoot", field[0]) && fieldcount == 2)
  {
    if (realpath (field[1], resolved_path) == NULL)
    {
      lprintf (0, "Error with %s value, cannot find path: %s",
               field[0], field[1]);
      return -1;
    }

    if (access (resolved_path, R_OK))
    {
      lprintf (0, "Error with %s value, cannot access directory: %s",
               field[0], resolved_path);
      return -1;
    }

    free (config.webroot);
    config.webroot = strdup (resolved_path);
  }
  else if (!strcasecmp ("HTTPHeader", field[0]) && fieldcount == 2)
  {
    char *combined_value = NULL;

    /* Append multiple headers to composite string */
    if (asprintf (&combined_value, "%s%s\r\n", (config.httpheaders) ? config.httpheaders : "", field[1]) == -1)
    {
      lprintf (0, "Error allocating memory");
      return -1;
    }

    free (config.httpheaders);
    config.httpheaders = combined_value;
  }
  else if (!strcasecmp ("MSeedWrite", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if (ConfigMSWrite (field[1]))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("MSeedScan", field[0]) && fieldcount >= 2)
  {
    if (dynamiconly)
      return fieldcount;

    /* Recombine option parameters for AddMSeedScanThread() to parse, TODO improve this */
    char scanparams[1024] = {0};
    for (int idx = 1; idx < fieldcount; idx++)
    {
      strcat (scanparams, field[idx]);
      strcat (scanparams, " ");
    }

    if (AddMSeedScanThread (scanparams))
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }
  }
  else if (!strcasecmp ("VolatileRing", field[0]) && fieldcount == 2)
  {
    if (dynamiconly)
      return fieldcount;

    if ((intval = YesNo (field[1])) < 0)
    {
      lprintf (0, "Error with %s config parameter: %s", field[0], paramstring);
      return -1;
    }

    config.volatilering = intval;
  }
  else
  {
    lprintf (0, "Unrecognized config parameter: %s", paramstring);
    return -1;
  }

  return fieldcount;
} /* End of SetParameter() */

/***********************************************************************
 * YesNo:
 *
 * Determine if a string is a "yes" or "no" value, including many
 * variations.
 *
 * Return 1 for "yes", 0 for "no, and -1 on undetermined.
 ***********************************************************************/
static int
YesNo (const char *value)
{
  if (*value == '1' ||
      !strcasecmp (value, "y") ||
      !strcasecmp (value, "yes") ||
      !strcasecmp (value, "true") ||
      !strcasecmp (value, "on"))
    return 1;
  else if (*value == '0' ||
           !strcasecmp (value, "n") ||
           !strcasecmp (value, "no") ||
           !strcasecmp (value, "false") ||
           !strcasecmp (value, "off"))
    return 0;
  else
    return -1;
}

/***********************************************************************
 * InitServerSocket:
 *
 * Initialize a TCP server socket on the specified port bound to all
 * local addresses/interfaces.
 *
 * Return socket descriptor on success and -1 on error.
 ***********************************************************************/
static int
InitServerSocket (char *portstr, ListenOptions options)
{
  struct addrinfo *addr;
  struct addrinfo hints;
  char *familystr = NULL;
  int fd;
  int optval;
  int gaierror;

  if (!portstr)
    return -1;

  memset (&hints, 0, sizeof (hints));

  /* AF_INET, or AF_INET6 for IPv4 or IPv6 */
  if (options & FAMILY_IPv4)
  {
    hints.ai_family = AF_INET;
    familystr       = "IPv4";
  }
  else if (options & FAMILY_IPv6)
  {
    hints.ai_family = AF_INET6;
    familystr       = "IPv6";
  }
  else
  {
    hints.ai_family = AF_UNSPEC;
    familystr       = "IPvUnspecified";
  }

  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags    = AI_PASSIVE;

  if ((gaierror = getaddrinfo (NULL, portstr, &hints, &addr)))
  {
    lprintf (0, "Error with getaddrinfo(), %s port %s: %s",
             familystr, portstr, gai_strerror (gaierror));
    return -1;
  }

  /* Create a socket from first addrinfo entry */
  fd = socket (addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (fd < 0)
  {
    /* Print error only if not "unsupported" IPv6, as this is expected */
    if (!(addr->ai_family == AF_INET6 && errno == EAFNOSUPPORT))
      lprintf (0, "Error with socket(), %s port %s: %s",
               familystr, portstr, strerror (errno));
    return -1;
  }

  optval = 1;
  if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof (optval)))
  {
    lprintf (0, "Error setting SO_REUSEADDR with setsockopt(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  /* Limit IPv6 sockets to IPv6 only, avoid mapped addresses, we handle IPv4 separately */
  if (addr->ai_family == AF_INET6 &&
      setsockopt (fd, IPPROTO_IPV6, IPV6_V6ONLY, &optval, sizeof (optval)))
  {
    lprintf (0, "Error setting IPV6_V6ONLY with setsockopt(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  if (bind (fd, addr->ai_addr, addr->ai_addrlen) < 0)
  {
    lprintf (0, "Error with bind(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  if (listen (fd, 10) == -1)
  {
    lprintf (0, "Error with listen(), %s port %s: %s",
             familystr, portstr, strerror (errno));
    close (fd);
    return -1;
  }

  freeaddrinfo (addr);

  return fd;
} /* End of InitServerSocket() */

/***************************************************************************
 * ConfigMSWrite:
 *
 * Configure miniSEED writing parameters for a given archive definition.
 *
 * Allow archive definition to be specified as a pre-determined layout
 * with a specified base directory, e.g. "SDAY@/data/archive".
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
ConfigMSWrite (char *archive)
{
  char *layout = NULL;
  char *path   = NULL;

  /* Parse layout specification if present */
  if ((path = strchr (archive, '@')))
  {
    *path++ = '\0';

    if (!strcmp (archive, "BUD"))
    {
      layout = BUDLAYOUT;
    }
    else if (!strcmp (archive, "CHAN"))
    {
      layout = CHANLAYOUT;
    }
    else if (!strcmp (archive, "QCHAN"))
    {
      layout = QCHANLAYOUT;
    }
    else if (!strcmp (archive, "CDAY"))
    {
      layout = CDAYLAYOUT;
    }
    else if (!strcmp (archive, "SDAY"))
    {
      layout = SDAYLAYOUT;
    }
    else if (!strcmp (archive, "HSDAY"))
    {
      layout = HSDAYLAYOUT;
    }
    else
    {
      return -1;
    }
  }

  pthread_rwlock_wrlock (&config.config_rwlock);
  free (config.mseedarchive);

  if (path && layout)
  {
    char combined[PATH_MAX];
    snprintf (combined, sizeof (combined), "%s/%s", path, layout);
    config.mseedarchive = strdup (combined);
  }
  else
  {
    config.mseedarchive = strdup (archive);
  }
  pthread_rwlock_unlock (&config.config_rwlock);

  return 0;
} /* End of ConfigMSWrite() */

/***************************************************************************
 * AddListenThreads:
 *
 * Add listen threads to the server thread list and initializing the
 * server listening sockets.
 *
 * A listening thread is created for each of the IPv4 and IPv6 network
 * protocol families, both by default.
 *
 * The params structure will be copied by AddServerThread().
 *
 * Returns number of listening threads on success and 0 on error.
 ***************************************************************************/
static int
AddListenThreads (ListenPortParams *lpp)
{
  ListenOptions options;
  ListenOptions families = 0;

  int threads = 0;

  if (!lpp)
    return 0;

  options = lpp->options;

  /* Split server options from network protocol families */
  if (options & FAMILY_IPv4)
  {
    families |= FAMILY_IPv4;
    options &= ~FAMILY_IPv4;
  }
  if (options & FAMILY_IPv6)
  {
    families |= FAMILY_IPv6;
    options &= ~FAMILY_IPv6;
  }

  /* Try to initialize listening for IPv4, if requested or default (no family specified) */
  if (families == 0 || (families & FAMILY_IPv4))
  {
    lpp->options = options | FAMILY_IPv4;

    if ((lpp->socket = InitServerSocket (lpp->portstr, lpp->options)) > 0)
    {
      if (AddServerThread (LISTEN_THREAD, lpp))
      {
        return 0;
      }

      threads += 1;
    }
    /* If explicitly requested, an initialization error is a failure */
    else if (families & FAMILY_IPv4)
    {
      lprintf (0, "Error initializing IPv4 server listening socket for port %s", lpp->portstr);
      return 0;
    }
  }

  /* Try to initialize listening for IPv6, if requested or default (no family specified) */
  if (families == 0 || (families & FAMILY_IPv6))
  {
    lpp->options = options | FAMILY_IPv6;

    if ((lpp->socket = InitServerSocket (lpp->portstr, lpp->options)) > 0)
    {
      if (AddServerThread (LISTEN_THREAD, lpp))
      {
        return 0;
      }

      threads += 1;
    }
    /* If explicitly requested, an initialization error is a failure */
    else if (families & FAMILY_IPv6)
    {
      lprintf (0, "Error initializing IPv6 server listening socket for port %s", lpp->portstr);
      return 0;
    }
  }

  lpp->options = options | families;

  return threads;
} /* End of AddListenThreads() */

/***************************************************************************
 * AddMSeedScanThread:
 *
 * Add a miniSEED scanner thread to the server thread list.  The
 * supplied configuration string should contain a base directory
 * followed by optional sub-parameters.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
AddMSeedScanThread (const char *scanconfig)
{
  char myconfig[PATH_MAX] = {0};
  MSScanInfo mssinfo;
  char *configstr;
  char *kptr;
  char *vptr;
  char *sptr;
  int initcurrentstate = 0;

  /* Set miniSEED scanning defaults */
  memset (&mssinfo, 0, sizeof (MSScanInfo)); /* Init struct to zeros */
  mssinfo.maxrecur     = -1;                 /* Maximum level of directory recursion, -1 is no limit */
  mssinfo.scansleep0   = 1;                  /* Sleep between scans interval when no records found */
  mssinfo.idledelay    = 60;                 /* Check idle files every idledelay scans */
  mssinfo.idlesec      = 7200;               /* Files are idle if not modified for idlesec */
  mssinfo.throttlensec = 100;                /* Nanoseconds to sleep after reading each record */
  mssinfo.filemaxrecs  = 100;                /* Maximum records to read from each file per scan */
  mssinfo.stateint     = 300;                /* State saving interval in seconds */

  strncpy (myconfig, scanconfig, sizeof (myconfig) - 1);
  configstr = myconfig;

  /* Skip initial whitespace */
  while (isspace ((int)(*configstr)))
    configstr++;

  /* Search for whitespace after initial string (directory) and truncate */
  vptr = configstr;
  while (*(vptr + 1) && !isspace ((int)(*vptr)))
    vptr++;
  if (isspace ((int)(*vptr)))
    *vptr++ = '\0';

  /* Initial portion of the config string is the directory to scan */
  snprintf (mssinfo.dirname, sizeof (mssinfo.dirname), "%.*s", (int)sizeof (mssinfo.dirname) - 1, configstr);

  /* Search for optional parameters */
  while (*vptr && (vptr = strchr (vptr, '=')))
  {
    /* Find key and value strings */
    kptr = vptr; /* Step back to find first non-space character */
    while (*kptr && kptr != configstr && !isspace ((int)(*kptr)))
      kptr--;
    kptr++;         /* This first non-space character should start the key */
    *vptr++ = '\0'; /* The value is directly after the equals */

    /* Truncate value string and track continuing config string */
    sptr = vptr;
    while (*(sptr + 1) && !isspace ((int)(*sptr)))
      sptr++;
    if (isspace ((int)(*sptr)))
      *sptr++ = '\0';

    if (!strncasecmp ("StateFile", kptr, 9)) /* State file name */
    {
      strncpy (mssinfo.statefile, vptr, sizeof (mssinfo.statefile) - 1);
    }
    else if (!strncasecmp ("Match", kptr, 5)) /* File name match */
    {
      strncpy (mssinfo.matchstr, vptr, sizeof (mssinfo.matchstr) - 1);
    }
    else if (!strncasecmp ("Reject", kptr, 6)) /* File name reject */
    {
      strncpy (mssinfo.rejectstr, vptr, sizeof (mssinfo.rejectstr) - 1);
    }
    else if (!strncasecmp ("InitCurrentState", kptr, 6)) /* Init current state flag */
    {
      if (*vptr == '1' || *vptr == 'Y' || *vptr == 'y')
        initcurrentstate = 1;
      else if (*vptr == '0' || *vptr == 'N' || *vptr == 'n')
        initcurrentstate = 0;
      else
      {
        lprintf (0, "Unrecognized InitCurrentState value: '%s'", vptr);
        return -1;
      }
    }
    else if (!strncasecmp ("MaxRecurse", kptr, 10)) /* Max recurion depth */
    {
      mssinfo.maxrecur = strtol (vptr, NULL, 10);
    }
    else
    {
      lprintf (0, "Unrecognized MSeedScan sub-option: '%s'", kptr);
      return -1;
    }

    vptr = sptr;
  }

  /* Perform InitCurrentState logic */
  if (initcurrentstate)
  {
    /* Set nextnew flag under two conditions:
     * 1) statefile is not specified
     * 2) statefile is specified but does not exist */

    if (!*(mssinfo.statefile) || (*(mssinfo.statefile) && access (mssinfo.statefile, F_OK)))
      mssinfo.nextnew = 1;
  }

  /* Add to server thread list */
  if (AddServerThread (MSEEDSCAN_THREAD, &mssinfo))
  {
    lprintf (0, "Error adding server thread for MSeedScan config file line: %s",
             configstr);
    return -1;
  }

  return 0;
} /* End of AddMSeedScanThread() */

/***************************************************************************
 * AddServerThread:
 *
 * Add a thread to the server thread list.  The structure type passed
 * in params is implied by the thread type.  The params structure will
 * be copied.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
AddServerThread (ServerThreadType type, void *params)
{
  struct sthread *stp;
  struct sthread *nstp;

  if (!(nstp = calloc (1, sizeof (struct sthread))))
  {
    lprintf (0, "Error allocating memory for server thread");
    return -1;
  }

  nstp->type = type;

  /* Copy thread parameters to new entry */
  if (type == LISTEN_THREAD)
  {
    if (!(nstp->params = malloc (sizeof (ListenPortParams))))
    {
      lprintf (0, "Error allocating memory for server parameters");
      return -1;
    }

    memcpy (nstp->params, params, sizeof (ListenPortParams));
  }
  else if (type == MSEEDSCAN_THREAD)
  {
    if (!(nstp->params = malloc (sizeof (MSScanInfo))))
    {
      lprintf (0, "Error allocating memory for MSeedScan parameters");
      return -1;
    }

    memcpy (nstp->params, params, sizeof (MSScanInfo));
  }
  else
  {
    lprintf (0, "%s() Error, unrecognized server thread type: %d",
             __func__, type);
    return -1;
  }

  pthread_mutex_lock (&param.sthreads_lock);
  if (param.sthreads)
  {
    /* Find last server thread entry and add new thread to end of list */
    stp = param.sthreads;
    while (stp->next)
    {
      stp = stp->next;
    }

    stp->next  = nstp;
    nstp->prev = stp;
  }
  else
  {
    /* Otherwise this is the first entry */
    param.sthreads = nstp;
  }
  nstp->next = NULL;
  pthread_mutex_unlock (&param.sthreads_lock);

  return 0;
} /* End of AddServerThread() */


/***************************************************************************
 * CalcSize:
 *
 * Calculate a size in bytes for the specified size string.  If the
 * string is terminated with the following suffixes the specified
 * scaling will be applied:
 *
 * 'K' or 'k' : kibibytes - value * 1024
 * 'M' or 'm' : mebibytes - value * 1024*1024
 * 'G' or 'g' : gibibytes - value * 1024*1024*1024
 * 'T' or 't' : tebibytes - value * 1024*1024*1024*1024
 *
 * Returns a size in bytes on success and 0 on error.
 ***************************************************************************/
static uint64_t
CalcSize (const char *sizestr)
{
  uint64_t size = 0;
  double dsize;
  size_t length;
  const char *lastchar;
  char *endptr;

  if (!sizestr)
    return 0;

  length = strlen (sizestr);

  if (length == 0)
    return 0;

  lastchar = sizestr + length - 1;

  /* For kibibytes */
  if (*lastchar == 'K' || *lastchar == 'k')
  {
    dsize = strtod (sizestr, &endptr);

    if (dsize < 0 || endptr != lastchar)
    {
      lprintf (0, "%s(): Error converting %s to positive integer", __func__, sizestr);
      return 0;
    }

    size = dsize * 1024 + 0.5;
  }
  /* For mebibytes */
  else if (*lastchar == 'M' || *lastchar == 'm')
  {
    dsize = strtod (sizestr, &endptr);

    if (dsize < 0 || endptr != lastchar)
    {
      lprintf (0, "%s(): Error converting %s to positive integer", __func__, sizestr);
      return 0;
    }

    size = dsize * 1024 * 1024 + 0.5;
  }
  /* For gibibytes */
  else if (*lastchar == 'G' || *lastchar == 'g')
  {
    dsize = strtod (sizestr, &endptr);

    if (dsize < 0 || endptr != lastchar)
    {
      lprintf (0, "%s(): Error converting %s to positive integer", __func__, sizestr);
      return 0;
    }

    size = dsize * 1024 * 1024 * 1024 + 0.5;
  }
  /* For tebibytes */
  else if (*lastchar == 'T' || *lastchar == 't')
  {
    dsize = strtod (sizestr, &endptr);

    if (dsize < 0 || endptr != lastchar)
    {
      lprintf (0, "%s(): Error converting %s to positive integer", __func__, sizestr);
      return 0;
    }

    size = dsize * 1024 * 1024 * 1024 * 1024 + 0.5;
  }
  /* Otherwise no recognized suffix */
  else
  {
    size = (uint64_t)strtoull (sizestr, &endptr, 10);

    if (size == 0 || *endptr != '\0')
    {
      lprintf (0, "%s(): Error converting %s to positive integer", __func__, sizestr);
      return 0;
    }
  }

  return size;
} /* End of CalcSize() */

/***************************************************************************
 * AddIPNet:
 *
 * Add network and netmask to an IPNet list.  Both IPv4 and IPv6 are
 * supported.
 *
 * Returns 0 on success and -1 on error.
 ***************************************************************************/
static int
AddIPNet (IPNet **pplist, const char *network, const char *limitstr)
{
  struct addrinfo hints;
  struct addrinfo *addrlist = NULL;
  struct addrinfo *addr;
  struct sockaddr_in *sockaddr;
  struct sockaddr_in6 *sockaddr6;
  IPNet *newipnet;
  char net[100]      = {0};
  char *endptr       = NULL;
  char *prefixstr    = NULL;
  uint64_t prefix    = 0;
  uint32_t v4netmask = 0;
  int rv;
  int idx;
  int jdx;

  if (!pplist || !network)
    return -1;

  /* Copy network string for manipulation */
  strncpy (net, network, sizeof (net) - 1);

  /* Split netmask/prefixlen from network if present: "IP/netmask" */
  if ((prefixstr = strchr (net, '/')))
  {
    *prefixstr++ = '\0';
  }

  /* Convert prefix string to value */
  if (IsAllDigits (prefixstr))
  {
    errno  = 0;
    prefix = strtoul (prefixstr, &endptr, 10);

    if (errno)
    {
      lprintf (0, "%s(): Error converting prefix value (%s): %s",
               __func__, prefixstr, strerror (errno));
      return -1;
    }

    if (endptr == prefixstr || *endptr != '\0')
    {
      lprintf (0, "%s(): Error converting prefix value (%s)", __func__, prefixstr);
      return -1;
    }
  }
  /* Convert IPv4 netmask to prefix, anything not all digits must be a mask */
  else if (prefixstr)
  {
    if (inet_pton (AF_INET, prefixstr, &v4netmask) <= 0)
    {
      lprintf (0, "%s(): Error parsing IPv4 netmask: %s", __func__, prefixstr);
      return -1;
    }

    if (v4netmask > 0)
    {
      if (ntohl (v4netmask) & (~ntohl (v4netmask) >> 1))
      {
        lprintf (0, "%s(): Invalid IPv4 netmask: %s", __func__, prefixstr);
        return -1;
      }
    }
  }
  else
  {
    prefix = 128;
  }

  /* Sanity check and catch errors from strtoul() */
  if (prefix > 128)
  {
    lprintf (0, "%s(): Error, prefix (%s) must be <= 128", __func__, prefixstr);
    return -1;
  }

  /* Convert address portion to binary address, resolving if possible */
  memset (&hints, 0, sizeof (hints));
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family   = AF_UNSPEC;     /* Either IPv4 and/or IPv6 */
  hints.ai_flags    = AI_ADDRCONFIG; /* Only return entries that could actually connect */

  if ((rv = getaddrinfo (net, NULL, &hints, &addrlist)) != 0)
  {
    lprintf (0, "%s(): Error with getaddrinfo(%s): %s", __func__, net, gai_strerror (rv));
    return -1;
  }

  /* Loop through results from getaddrinfo(), adding new entries */
  for (addr = addrlist; addr != NULL; addr = addr->ai_next)
  {
    /* Allocate new IPNet */
    if (!(newipnet = (IPNet *)calloc (1, sizeof (IPNet))))
    {
      lprintf (0, "%s(): Error allocating memory for IPNet", __func__);
      return -1;
    }

    if (addr->ai_family == AF_INET)
    {
      newipnet->family = AF_INET;

      /* Use IPv4 netmask if specified directly */
      if (v4netmask > 0)
        newipnet->netmask.in_addr.s_addr = v4netmask;
      /* Calculate netmask from prefix, if the prefix > 32 use 32 as a max for IPv4 */
      else if (prefix > 0)
        newipnet->netmask.in_addr.s_addr = ~((1 << (32 - ((prefix > 32) ? 32 : prefix))) - 1);
      else
        newipnet->netmask.in_addr.s_addr = 0;

      /* Swap calculated netmask to network order, if a (swapped) mask not available */
      if (v4netmask == 0)
      {
        newipnet->netmask.in_addr.s_addr = htonl (newipnet->netmask.in_addr.s_addr);
      }

      sockaddr                         = (struct sockaddr_in *)addr->ai_addr;
      newipnet->network.in_addr.s_addr = sockaddr->sin_addr.s_addr;

      /* Calculate network: AND the address and netmask */
      newipnet->network.in_addr.s_addr &= newipnet->netmask.in_addr.s_addr;
    }
    else if (addr->ai_family == AF_INET6)
    {
      newipnet->family = AF_INET6;

      memset (&newipnet->netmask.in6_addr, 0, sizeof (struct in6_addr));

      /* Calculate netmask from prefix */
      if (prefix > 0)
      {
        for (idx = prefix, jdx = 0; idx > 0; idx -= 8, jdx++)
        {
          if (idx >= 8)
            newipnet->netmask.in6_addr.s6_addr[jdx] = 0xFFu;
          else
            newipnet->netmask.in6_addr.s6_addr[jdx] = (uint8_t)(0xFFu << (8 - idx));
        }
      }

      sockaddr6 = (struct sockaddr_in6 *)addr->ai_addr;
      memcpy (&newipnet->network.in6_addr, &sockaddr6->sin6_addr, sizeof (struct in6_addr));

      /* Calculate network: AND the address and netmask */
      for (idx = 0; idx < 16; idx++)
      {
        newipnet->network.in6_addr.s6_addr[idx] &= newipnet->netmask.in6_addr.s6_addr[idx];
      }
    }

    /* Store any supplied limit expression */
    if (limitstr)
    {
      if (!(newipnet->limitstr = strdup (limitstr)))
      {
        lprintf (0, "%s(): Error allocating memory for limit string", __func__);
        return -1;
      }
    }

    /* Push the new entry on the top of the list */
    newipnet->next = *pplist;
    *pplist        = newipnet;
  }

  if (addrlist)
    freeaddrinfo (addrlist);

  return 0;
} /* End of AddIPNet() */

/***************************************************************************
 * SetAuthCommand:
 *
 * Set the auth command string, parse the program and it's arguments into
 * an argument array.
 *
 * Returns 0 on success and non zero on error.
 ***************************************************************************/
static int
SetAuthCommand (const char *program, char **argv, int argc)
{
  if (!program)
  {
    lprintf (2, "%s() Required arguments missing", __func__);
    return -1;
  }

  /* Free any existing auth command parameters */
  free (config.auth.program);
  if (config.auth.argv != NULL)
  {
    for (char **arg = config.auth.argv; *arg != NULL; arg++)
    {
      free (*arg);
    }
    free (config.auth.argv);
  }
  config.auth.program = NULL;
  config.auth.argv    = NULL;

  /* Set new parameters */
  if ((config.auth.program = strdup (program)) == NULL)
  {
    lprintf (0, "Error allocating memory for auth program");
    return -1;
  }

  /* Create argument vector */
  if (argv && argc > 0)
  {
    config.auth.argv = malloc (sizeof (char *) * (argc + 2));

    if (config.auth.argv == NULL)
    {
      lprintf (0, "Error allocating memory for auth command arguments");
      return -1;
    }

    /* Add program path as first element in argument vector */
    config.auth.argv[0] = strdup (program);
    if (config.auth.argv[0] == NULL)
    {
      lprintf (0, "Error allocating memory for auth command arguments");
      return -1;
    }

    /* Copy arguments to the argument vector */
    for (int count = 0; count < argc; count++)
    {
      config.auth.argv[count + 1] = strdup (argv[count]);

      if (config.auth.argv[count + 1] == NULL)
      {
        lprintf (0, "Error allocating memory for auth command arguments");
        return -1;
      }
    }

    /* NULL terminate the argument vector */
    config.auth.argv[argc + 1] = NULL;
  }

  /* Check if the program exists and is executable */
  if (access (config.auth.program, X_OK) < 0)
  {
    lprintf (1, "Warning: auth program %s not found or not executable", config.auth.program);
  }

  return 0;
} /* End of SetAuthCommand() */

static const char *reference_config_file = \
"# Example ringserver configuration file.\n\
#\n\
# Default values are in comments where appropriate.\n\
#\n\
# Dynamic parameters: some parameters will be re-read by ringserver\n\
# whenever the configuration file is modified.\n\
#\n\
# Config options can be set on the command line, via environment variables\n\
# and via a configuration file like this one.  The order of precedence is:\n\
# command line, environment variables, configuration file.\n\
# Equivalent variables are listed in the description of each parameter.\n\
#\n\
# A configuration file can be specified on the command line or with\n\
# the environment variable RS_CONFIG_FILE.\n\
#\n\
# Comment lines begin with a '#' character.\n\
\n\
\n\
# Specify the directory where the ringserver will store\n\
# the packet and stream buffers.  This must be specified.\n\
# Equivalent environment variable: RS_RING_DIRECTORY\n\
\n\
RingDirectory ring\n\
\n\
\n\
# Specify the ring packet buffer size in bytes.  A trailing\n\
# 'K', 'M' or 'G' may be added for kibibytes, mebibytes or gibibytes.\n\
# Equivalent environment variable: RS_RING_SIZE\n\
\n\
#RingSize 1G\n\
\n\
\n\
# Specify the maximum packet data size in bytes.\n\
# Equivalent environment variable: RS_MAX_PACKET_SIZE\n\
\n\
#MaxPacketSize 512\n\
\n\
\n\
# Listen for connections on a specified port.  By default all supported\n\
# protocols and network protocol families (IPv4 and IPv6) are allowed and\n\
# optional flags can be used to limit to specified protocols/families.\n\
#\n\
# Protocol flags are specified by including \"DataLink\", \"SeedLink\"\n\
# and/or \"HTTP\" after the port.  By default all protocols are allowed.\n\
#\n\
# Network families are specified by including \"IPv4\" or \"IPv6\" after\n\
# the port.  Default is any combination supported by the system.\n\
#\n\
# TLS (SSL) can be enabled by including \"TLS\" after the port.  A\n\
# certificate file must be specified using the TLSCertificateFile\n\
# parameter. By default TLS is not enabled.\n\
#\n\
# For example:\n\
# ListenPort <port> [DataLink] [SeedLink] [HTTP] [IPv4] [IPv6] [TLS]\n\
#\n\
# This parameter can be specified multiple times to listen for connections\n\
# on multiple ports.\n\
# Equivalent environment variable: RS_LISTEN_PORT\n\
\n\
ListenPort 18000\n\
\n\
# Port 18500 is the standard port for SeedLink v4 connections over TLS (SSL).\n\
#ListenPort 18500 TLS\n\
\n\
# Listen for DataLink connections on a specified port.  This is an alias\n\
# for a ListenPort configured with only DataLink allowed.\n\
# Equivalent environment variable: RS_DATALINK_PORT\n\
\n\
#DataLinkPort 16000\n\
\n\
\n\
# Listen for SeedLink connections on a specified port. This is an alias\n\
# for a ListenPort configured with only SeedLink allowed.\n\
# Equivalent environment variable: RS_SEEDLINK_PORT\n\
\n\
#SeedLinkPort 18000\n\
\n\
\n\
# Listen for HTTP connections on a specified port. This is an alias\n\
# for a ListenPort configured with only HTTP allowed.\n\
# Equivalent environment variable: RS_HTTP_PORT\n\
\n\
#HTTPPort 80\n\
\n\
\n\
# Certificate file for TLS connections.  This is a dynamic parameter.\n\
# Equivalent environment variable: RS_TLS_CERT_FILE\n\
\n\
#TLSCertFile /path/to/certificate.pem\n\
\n\
# Private key file for TLS connections.  This is a dynamic parameter.\n\
# Equivalent environment variable: RS_TLS_KEY_FILE\n\
\n\
#TLSKeyFile /path/to/private_key.pem\n\
\n\
# Enable client certificate verification for TLS connections.  This is\n\
# not a common option, but can be used to require clients to present\n\
# a certificate for authentication.  This is a dynamic parameter.\n\
# Equivalent environment variable: RS_TLS_VERIFY_CLIENT_CERT\n\
\n\
#TLSVerifyClientCert 0\n\
\n\
\n\
# Specify the Server ID as reported to the clients.  The parameter may\n\
# be a quoted string including spaces.  Default is \"Ring Server\".\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_SERVER_ID\n\
\n\
#ServerID \"Ring Server\"\n\
\n\
\n\
# Specify the level of verbosity for the server log output.  Valid\n\
# verbosity levels are 0 - 3.  This is a dynamic parameter.\n\
# Equivalent environment variable: RS_VERBOSITY\n\
\n\
#Verbosity 0\n\
\n\
\n\
# Specify the maximum number of clients per IP address, regardless of\n\
# protocol, allowed to be connected concurrently.  This limit does\n\
# not apply to addresses with write permission.  Set to 0 for unlimited.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_MAX_CLIENTS_PER_IP\n\
\n\
#MaxClientsPerIP 0\n\
\n\
\n\
# Specify the maximum number of clients, regardless of protocol,\n\
# allowed to be connected simultaneously, set to 0 for unlimited.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_MAX_CLIENTS\n\
\n\
#MaxClients 600\n\
\n\
\n\
# Specify an idle client timeout in seconds after which the client is\n\
# disconnected.  Set to 0 to disable.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_CLIENT_TIMEOUT\n\
\n\
#ClientTimeout 3600\n\
\n\
\n\
# Configure the network I/O timeout in seconds.  This controls the duration\n\
# that a network read or write operation will wait until failure, after\n\
# which the client is disconnected.  The default value of 10 seconds is\n\
# appropriate for most scenarios.\n\
# \n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_NETIO_TIMEOUT\n\
\n\
#NetIOTimeout 10\n\
\n\
\n\
# Control the usage of memory mapping of the ring packet buffer.  If\n\
# this parameter is 1 (or not defined) the packet buffer will be\n\
# memory-mapped directly from the packet buffer file, otherwise it\n\
# will be stored in memory during operation and only read/written\n\
# to/from the packet buffer file during startup and shutdown.\n\
# Normally memory mapping the packet buffer is the best option,\n\
# this parameter allows for operation in environments where memory\n\
# mapping is slow or not possible (e.g. NFS storage).\n\
# Equivalent environment variable: RS_MEMORY_MAP_RING\n\
\n\
#MemoryMapRing 1\n\
\n\
\n\
# Control auto-recovery after corruption detection.  Be default if\n\
# corruption is detected in the ring packet buffer file or stream\n\
# index file during initialization the ring and stream files will be\n\
# renamed with .corrupt extensions and initialization will be\n\
# attempted a 2nd time.  If this option is 0 (off) the server will\n\
# exit on these corruption errors.  If this option is 1 (the default)\n\
# the server will move the buffers to .corrupt files.  If this option\n\
# is 2 (delete) the server will delete the corrupt buffer files.\n\
# Equivalent environment variable: RS_AUTO_RECOVERY\n\
\n\
#AutoRecovery 1\n\
\n\
\n\
# Control reverse DNS lookups to resolve hostnames for client IPs.\n\
# By default a reverse lookup is performed whenever a client connects.\n\
# When a reverse DNS lookup fails a small delay will occur, this can\n\
# be avoided by setting this option to 0 (off).\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_RESOLVE_HOSTNAMES\n\
\n\
#ResolveHostnames 1\n\
\n\
\n\
# Specify a limit, in percent, of the packet buffer to search for time\n\
# windowing requests.  By default the entire packet buffer will be\n\
# searched starting from the earliest packet traversing forward.  If\n\
# this option is set, only the specified percent of the ring will be\n\
# searched starting from the latest packet traversing backward.  To\n\
# turn off time window requests set this parameter to 0.  This is a\n\
# dynamic parameter, but updated values will only apply to new\n\
# connections.\n\
# Equivalent environment variable: RS_TIME_WINDOW_LIMIT\n\
\n\
#TimeWindowLimit 100\n\
\n\
\n\
# Define the base directory for data transfer logs including both\n\
# data transmission and reception logs.  By default no logs are written.\n\
# This facility will log the number of data packet bytes and packet\n\
# count sent to and/or received from each client during the log interval.\n\
# If this parameter is specified and the directory exists, files will\n\
# be written at a user defined interval with the format:\n\
# \"<dir>/[prefix-]txlog-YYYYMMDDTHHMM-YYYYMMDDTHHMM\" and\n\
# \"<dir>/[prefix-]rxlog-YYYYMMDDTHHMM-YYYYMMDDTHHMM\"\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_TRANSFER_LOG_DIRECTORY\n\
\n\
#TransferLogDirectory tlog\n\
\n\
# Specify the transfer log interval in hours.  This is a dynamic\n\
# parameter.\n\
# Equivalent environment variable: RS_TRANSFER_LOG_INTERVAL\n\
\n\
#TransferLogInterval 24\n\
\n\
# Specify a transfer log file prefix, the default is no prefix.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_TRANSFER_LOG_PREFIX\n\
\n\
#TransferLogPrefix <prefix>\n\
\n\
# Control the logging of data transmission and reception independently,\n\
# by default both are logged.  The TransferLogDirectory must be set for\n\
# any transfer logs to be written.  To turn off logging of either\n\
# transmission (TX) or reception (RX) set the appropriate parameter to 0.\n\
# These are dynamic parameters.\n\
# Equivalent environment variables: RS_TRANSFER_LOG_TX, RS_TRANSFER_LOG_RX\n\
\n\
#TransferLogTX 1\n\
#TransferLogRX 1\n\
\n\
\n\
# Specify a program and arguments to execute to perform authentication and\n\
# return permissions (authorizations) if successful.  Credentials, either\n\
# as username and password or a JSON Web Token (JWT) are provided to the\n\
# program via environment variables: AUTH_USERNAME, AUTH_PASSWORD, AUTH_JWTOKEN.\n\
# See the manual for details on the authentication process.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_AUTH_COMMAND\n\
\n\
#AuthCommand </path/to/program> [arguments]\n\
\n\
\n\
# Specify the timeout in seconds for the authentication command to complete.\n\
# The default is 5 seconds.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_AUTH_TIMEOUT\n\
\n\
#AuthTimeout 5\n\
\n\
\n\
# Specify IP addresses or ranges which are allowed to submit (write)\n\
# data to the ringserver.  This parameter can be specified multiple\n\
# times and should be specified in address/prefix (CIDR) notation, e.g.:\n\
# \"WriteIP 192.168.0.1/24\".  The prefix may be omitted in which case\n\
# only the specific host is allowed.  If no addresses are explicitly\n\
# granted write permission, permission is granted to clients from\n\
# localhost (local loopback).\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_WRITE_IP\n\
\n\
#WriteIP <address>[/prefix]\n\
#WriteIP <address>[/prefix]\n\
\n\
\n\
# Specify IP addresses or ranges which are allowed to request and\n\
# receive server connections and detailed status.  This parameter can\n\
# be specified multiple times and should be specified in\n\
# address/prefix (CIDR) notation, e.g.: \"TrustedIP 192.168.0.1/24\".\n\
# The prefix may be omitted in which case only the specific host is\n\
# trusted. If no addresses are explicitly trusted, trust is granted to\n\
# clients from localhost (local loopback).  This is a dynamic\n\
# parameter.\n\
# Equivalent environment variable: RS_TRUSTED_IP\n\
\n\
#TrustedIP <address>[/prefix]\n\
#TrustedIP <address>[/prefix]\n\
\n\
\n\
# Allow IP addresses or ranges to access only specified stream IDs in the\n\
# ringserver.  A regular expression is used to specify which Stream\n\
# IDs the address range is allowed to read and write, the expression\n\
# may be compound and must not contain spaces.  By default clients\n\
# can access any streams in the buffer, or write any streams if write\n\
# permission is granted.  This parameter can be specified multiple times\n\
# and should be specified in address/prefix (CIDR) notation,\n\
# for example: \"AllowedIP 192.168.0.1/24\".  The prefix may be omitted\n\
# in which case only the specific host is limited. This is a dynamic\n\
# parameter.\n\
# Equivalent environment variable: RS_ALLOWED_STREAMS_IP\n\
\n\
#AllowedStreamsIP <address>[/prefix] <StreamID Pattern>\n\
#AllowedStreamsIP <address>[/prefix] <StreamID Pattern>\n\
\n\
\n\
# Forbid IP addresses or ranges from accessing specified stream IDs in the\n\
# ringserver.  A regular expression is used to specify which Stream\n\
# IDs the address range is allowed to access (and write), the\n\
# expression may be compound and must not contain spaces.  By default\n\
# clients can access any streams in the buffer, or write any streams\n\
# if write permission is granted.  This parameter can be specified\n\
# multiple times and should be specified in address/prefix (CIDR)\n\
# notation, e.g.: \"ForbiddenIP 192.168.0.1/24\".  The prefix may be omitted\n\
# in which case only the specific host is limited. This is a dynamic\n\
# parameter.\n\
# Equivalent environment variable: RS_FORBIDDEN_STREAMS_IP\n\
\n\
#ForbiddenStreamsIP <address>[/prefix] <StreamID Pattern>\n\
#ForbiddenStreamsIP <address>[/prefix] <StreamID Pattern>\n\
\n\
\n\
# Specify IP addresses or ranges which should be specifically allowed\n\
# to connect while all others will be rejected.  By default all IPs\n\
# are allowed to connect.  This parameter can be specified multiple\n\
# times and should be specified in address/prefix (CIDR) notation,\n\
# e.g.: \"MatchIP 192.168.0.1/24\".  The prefix may be omitted in which\n\
# case only the specific host is matched. This is a dynamic parameter.\n\
# Equivalent environment variable: RS_ACCEPT_IP\n\
\n\
#AcceptIP <address>[/prefix]\n\
#AcceptIP <address>[/prefix]\n\
\n\
\n\
# Specify IP addresses or ranges which should be rejected immediately\n\
# after connecting.  This parameter can be specified multiple times\n\
# and should be specified in address/prefix (CIDR) notation, e.g.:\n\
# \"RejectIP 192.168.0.1/24\".  The prefix may be omitted in which case\n\
# only the specific host is rejected.  This is a dynamic parameter.\n\
# Equivalent environment variable: RS_DENY_IP\n\
\n\
#DenyIP <address>[/prefix]\n\
#DenyIP <address>[/prefix]\n\
\n\
\n\
# Serve content via HTTP from the specified directory. The HTTP server\n\
# implementation is limited to returning existing files and returning\n\
# \"index.html\" files when a directory is requested using the HTTP GET\n\
# method. This is a dynamic parameter.\n\
# Equivalent environment variable: RS_WEB_ROOT\n\
\n\
#WebRoot <Web content root directory>\n\
\n\
\n\
# Add custom HTTP headers to HTTP responses.  This can be useful to\n\
# enable Cross-Origin Resource Sharing (CORS) for example.\n\
# This is a dynamic parameter.\n\
# Equivalent environment variable: RS_HTTP_HEADER\n\
\n\
#HTTPHeader \"Access-Control-Allow-Origin: *\"\n\
#HTTPHeader <Custom HTTP header>\n\
\n\
\n\
# Enable a special mode of operation where all miniSEED records\n\
# received using the DataLink protocol are written to user specified\n\
# directory and file structures.  See the ringserver(1) man page for\n\
# more details.\n\
# Equivalent environment variable: RS_MSEED_WRITE\n\
\n\
#MSeedWrite <format>\n\
\n\
\n\
# Enable a special mode of operation where files containing miniSEED\n\
# are scanned continuously and data records are inserted into the ring.\n\
# By default all sub-directories will be recursively scanned.  Sub-options\n\
# can be used to control the scanning, the StateFile sub-option is highly\n\
# recommended.  Values for sub-options should not be quoted and cannot\n\
# contain spaces.\n\
# Equivalent environment variable: RS_MSEED_SCAN\n\
# See the ringserver(1) man page for more details.\n\
\n\
#MSeedScan <directory> [StateFile=scan.state] [Match=pattern] [Reject=pattern] [InitCurrentState=y]\n\
";

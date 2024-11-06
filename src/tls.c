/**************************************************************************
 * tls.c
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

#include "tls.h"
#include "logging.h"

/* Debug output for TLS */
void
tls_debug (void *ctx, int level, const char *file, int line, const char *str)
{
  ((void)level);
  ((void)ctx);

  lprintf (0, "%s:%04d: %s", file, line, str);
}

/***********************************************************************
 *
 * Initialize and negotiation TLS on connected client socket.
 *
 * Return 0 on success and non-zero on error.
 ***********************************************************************/
int
tls_configure (ClientInfo *cinfo)
{
  TLSCTX *tlsctx         = NULL;
  char *evalue           = NULL;
  uint32_t flags;
  int debug_level = 0;
  int ret;
  psa_status_t status;

  if (config.tlscertfile == NULL)
  {
    lprintf (0, "[%s] No TLS certificate provided, cannot configure TLS", cinfo->hostname);
    return -1;
  }

  if (config.tlskeyfile == NULL)
  {
    lprintf (0, "[%s] No TLS key provided, cannot configure TLS", cinfo->hostname);
    return -1;
  }

  lprintf (2, "[%s] Configuring TLS", cinfo->hostname);

  /* Allocate TLS data structure context */
  if ((cinfo->tlsctx = malloc (sizeof (TLSCTX))) == NULL)
  {
    lprintf (0, "[%s] Cannot allocate memory for TLS context", cinfo->hostname);
    return -1;
  }

  tlsctx = (TLSCTX *)cinfo->tlsctx;

  /* Set debug level from environment variable if set */
  if ((evalue = getenv ("RINGSERVER_TLS_DEBUG")) != NULL)
  {
    debug_level = (int)strtol (evalue, NULL, 10);
    lprintf (1, "[%s] Configuring debug level %d (from RINGSERVER_TLS_DEBUG)",
             cinfo->hostname, debug_level);

    if (debug_level > 0)
    {
      mbedtls_debug_set_threshold (debug_level);
    }
  }

  tlsctx->client_fd.fd = cinfo->socket;
  mbedtls_ssl_init (&tlsctx->ssl);
  mbedtls_ssl_config_init (&tlsctx->conf);
  mbedtls_entropy_init (&tlsctx->entropy);
  mbedtls_x509_crt_init (&tlsctx->srvcert);
  mbedtls_pk_init (&tlsctx->pkey);
  mbedtls_ctr_drbg_init (&tlsctx->ctr_drbg);

  if ((status = psa_crypto_init ()) != PSA_SUCCESS)
  {
    lprintf (0, "[%s] Failed to initialize PSA Crypto implementation: %d",
             cinfo->hostname, (int)status);
    return -1;
  }

  if ((ret = mbedtls_ctr_drbg_seed (&tlsctx->ctr_drbg, mbedtls_entropy_func,
                                    &tlsctx->entropy,
                                    (const unsigned char *)cinfo->hostname,
                                    strlen (cinfo->hostname))) != 0)
  {
    lprintf (0, "[%s] mbedtls_ctr_drbg_seed() returned %d", cinfo->hostname, ret);
    return -1;
  }

  lprintf (2, "[%s] Reading TLS cert from '%s'", cinfo->hostname, config.tlscertfile);

  if ((ret = mbedtls_x509_crt_parse_file (&tlsctx->srvcert, config.tlscertfile)) != 0)
  {
    lprintf (0, "[%s] mbedtls_x509_crt_parse_file() returned -0x%x",
             cinfo->hostname, (unsigned int)-ret);
    return -1;
  }

  lprintf (2, "[%s] Reading TLS key from '%s'", cinfo->hostname, config.tlskeyfile);

  if ((ret = mbedtls_pk_parse_keyfile (&tlsctx->pkey, config.tlskeyfile, "",
                                       mbedtls_ctr_drbg_random, &tlsctx->ctr_drbg)) != 0)
  {
    lprintf (0, "[%s] mbedtls_pk_parse_keyfile() returned -0x%x", cinfo->hostname, (unsigned int)-ret);
    return -1;
  }

  if ((ret = mbedtls_ssl_config_defaults (&tlsctx->conf,
                                          MBEDTLS_SSL_IS_SERVER,
                                          MBEDTLS_SSL_TRANSPORT_STREAM,
                                          MBEDTLS_SSL_PRESET_DEFAULT)) != 0)
  {
    lprintf (0, "[%s] mbedtls_ssl_config_defaults() returned %d", cinfo->hostname, ret);
    return -1;
  }

  mbedtls_ssl_conf_authmode (&tlsctx->conf, MBEDTLS_SSL_VERIFY_OPTIONAL);
  mbedtls_ssl_conf_rng (&tlsctx->conf, mbedtls_ctr_drbg_random, &tlsctx->ctr_drbg);
  mbedtls_ssl_conf_dbg (&tlsctx->conf, tls_debug, NULL);

  mbedtls_ssl_conf_ca_chain (&tlsctx->conf, &tlsctx->srvcert, NULL);

  if ((ret = mbedtls_ssl_conf_own_cert (&tlsctx->conf, &tlsctx->srvcert, &tlsctx->pkey)) != 0)
  {
    lprintf (0, "[%s] mbedtls_ssl_conf_own_cert() returned %d", cinfo->hostname, ret);
    return -1;
  }

  if ((ret = mbedtls_ssl_setup (&tlsctx->ssl, &tlsctx->conf)) != 0)
  {
    lprintf (0, "[%s] mbedtls_ssl_setup() returned %d", cinfo->hostname, ret);
    return -1;
  }

  if ((ret = mbedtls_ssl_set_hostname (&tlsctx->ssl, cinfo->hostname)) != 0)
  {
    lprintf (0, "[%s] mbedtls_ssl_set_hostname() returned %d", cinfo->hostname, ret);
    return -1;
  }

  mbedtls_ssl_set_bio (&tlsctx->ssl, &tlsctx->client_fd,
                       mbedtls_net_send, mbedtls_net_recv, NULL);

  lprintf (2, "[%s] Starting TLS handshake", cinfo->hostname);

  while ((ret = mbedtls_ssl_handshake (&tlsctx->ssl)) != 0)
  {
    if (ret != MBEDTLS_ERR_SSL_WANT_READ &&
        ret != MBEDTLS_ERR_SSL_WANT_WRITE &&
        ret != MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS)
    {
      lprintf (0, "[%s] mbedtls_ssl_handshake() returned -0x%x", cinfo->hostname, (unsigned int)-ret);
      return -1;
    }

    /* Wait for socket availability for 1 second */
    PollSocket (cinfo->socket, 1, 1, 1000);
  }

  if (config.tlsverifyclientcert)
  {
    lprintf (2, "[%s] Verifying TLS client certificate", cinfo->hostname);

    if ((flags = mbedtls_ssl_get_verify_result (&tlsctx->ssl)) != 0)
    {
      char vrfy_buf[512];
      lprintf (0, "[%s] Certificate verification failed", cinfo->hostname);

      mbedtls_x509_crt_verify_info (vrfy_buf, sizeof (vrfy_buf), "  ! ", flags);
      lprintf (0, "[%s] VERIFY INFO: %s", cinfo->hostname, vrfy_buf);

      lprintf (0, "[%s] Connection refused due to client certificate verification failure", cinfo->hostname);
    }
  }

  lprintf (1, "[%s] TLS connection established", cinfo->hostname);

  return 0;
} /* End of tls_configure() */

/***********************************************************************
 *
 * Free memory allocated for TLS connections
 *
 ***********************************************************************/
void
tls_cleanup (ClientInfo *cinfo)
{
  if (cinfo->tlsctx)
  {
    TLSCTX *tlsctx = (TLSCTX *)cinfo->tlsctx;

    mbedtls_ssl_free (&tlsctx->ssl);
    mbedtls_ssl_config_free (&tlsctx->conf);
    mbedtls_entropy_free (&tlsctx->entropy);
    mbedtls_x509_crt_free (&tlsctx->srvcert);
    mbedtls_pk_free (&tlsctx->pkey);
    mbedtls_ctr_drbg_free (&tlsctx->ctr_drbg);
    mbedtls_psa_crypto_free ();

    free (cinfo->tlsctx);
    cinfo->tlsctx = NULL;
  }
}

/**************************************************************************
 * proxyproto.h
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

#ifndef PROXYPROTO_H
#define PROXYPROTO_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/socket.h>

/* Read and parse a HAProxy PROXY protocol version 2 header from a socket.
 *
 * On success the sockaddr_storage at 'addr' and the value at 'addrlen'
 * are updated to reflect the original client address conveyed by the header.
 *
 * When the proxy sends a LOCAL command (e.g. health-checks) the address
 * is left unchanged and 1 is returned so the caller may keep the address
 * obtained from accept().
 *
 * timeout_ms: maximum milliseconds to wait for the header to arrive.
 *
 * Returns:
 *   0  - PROXY command; addr/addrlen updated with client address
 *   1  - LOCAL command; addr/addrlen unchanged
 *  -1  - error (bad signature, unsupported version, timeout, I/O error)
 */
extern int proxy_protocol_v2_read (int socket, struct sockaddr_storage *addr,
                                   socklen_t *addrlen, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* PROXYPROTO_H */

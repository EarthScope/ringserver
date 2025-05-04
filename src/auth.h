/**************************************************************************
 * http.h
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

#ifndef AUTH_H
#define AUTH_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "clients.h"

extern int perform_auth (ClientInfo *cinfo,
                         const char *username, const char *password,
                         const char *jwtoken);

#ifdef __cplusplus
}
#endif

#endif /* AUTH_H */

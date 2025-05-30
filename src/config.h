/**************************************************************************
 * config.h
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

#ifndef CONFIG_H
#define CONFIG_H 1

#ifdef __cplusplus
extern "C" {
#endif

extern int ProcessParam (int argcount, char **argvec);
extern int ReadConfigFile (char *configfile, int dynamiconly, time_t mtime);

#ifdef __cplusplus
}
#endif

#endif /* CONFIG_H */

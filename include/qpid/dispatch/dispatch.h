#ifndef __dispatch_dispatch_h__
#define __dispatch_dispatch_h__ 1
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "qpid/dispatch/error.h"

#include <stdbool.h>
#include <stdint.h>

/**@file
 * Configure and prepare a dispatch instance.
 *
 * @defgroup dispatch dispatch
 * @{
 */

typedef struct qd_dispatch_t           qd_dispatch_t;
typedef struct qd_connection_manager_t qd_connection_manager_t;
typedef struct qd_policy_t             qd_policy_t;
typedef struct qd_server_t             qd_server_t;

/**
 * Initialize the Dispatch library and prepare it for operation.
 *
 * @param python_pkgdir The path to the Python files.
 * @param test_hooks Iff true, enable internal system testing features
 * @return A handle to be used in API calls for this instance.
 */
qd_dispatch_t *qd_dispatch(const char *python_pkgdir, bool test_hooks);


/**
 * Finalize the Dispatch library after it has stopped running.
 *
 * @param qd The dispatch handle returned by qd_dispatch
 */
void qd_dispatch_free(qd_dispatch_t *qd);

/**
 * Load the configuration file.
 *
 * @param qd The dispatch handle returned by qd_dispatch
 * @param config_path The path to the configuration file.
 */
qd_error_t qd_dispatch_load_config(qd_dispatch_t *qd, const char *config_path);

/**
 * Validate the configuration file.
 *
 * @param config_path The path to the configuration file.
 */
qd_error_t qd_dispatch_validate_config(const char *config_path);

/**
 * Return a reference to connection_manager
 *
 * @param qd The dispatch handle returned by qd_dispatch
 * @return pointer to qd_connection_manager_t object.
 */

qd_connection_manager_t *qd_dispatch_connection_manager(const qd_dispatch_t *qd);

/**
 * Return a reference to the policy agent
 *
 * @param qd The dispatch handle returned by qd_dispatch
 * @return pointer to qd_policy_t object.
 */
qd_policy_t *qd_dispatch_get_policy(const qd_dispatch_t *dispatch);

/**
 * Return the configured inter-router data connection count
 */
uint32_t qd_dispatch_get_data_connection_count(const qd_dispatch_t *dispatch);

/**
 * Return the routers server
 */
qd_server_t *qd_dispatch_get_server(const qd_dispatch_t *dispatch);

/**
 * @}
 */

#endif

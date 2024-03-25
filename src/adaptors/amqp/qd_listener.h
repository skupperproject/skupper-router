#ifndef __qd_listener_h__
#define __qd_listener_h__ 1
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

// Proactor Listener for accepting AMQP connection requests

#include "server_config.h"

#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/server.h"

typedef struct qd_lws_listener_t qd_lws_listener_t;
typedef struct qd_server_t       qd_server_t;
typedef struct pn_listener_t     pn_listener_t;

/**
 * Listener objects represent the desire to accept incoming AMQP transport connections.
 */
typedef struct qd_listener_t qd_listener_t;
struct qd_listener_t {
    /* May be referenced by connection_manager and pn_listener_t */
    qd_handler_context_t     type;
    sys_atomic_t             ref_count;
    qd_server_t              *server;
    qd_server_config_t        config;
    pn_listener_t            *pn_listener;
    qd_lws_listener_t        *http;
    DEQ_LINKS(qd_listener_t);
    bool                      exit_on_error;
};

DEQ_DECLARE(qd_listener_t, qd_listener_list_t);


/**
 * Listen for incoming connections, return true if listening succeeded.
 */
bool qd_listener_listen(qd_listener_t *l);
qd_listener_t *qd_server_listener(qd_server_t *server);
void qd_listener_decref(qd_listener_t* ct);
qd_lws_listener_t *qd_listener_http(const qd_listener_t *li);
const qd_server_config_t *qd_listener_config(const qd_listener_t *li);

#endif

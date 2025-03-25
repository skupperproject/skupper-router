#ifndef __dispatch_server_h__
#define __dispatch_server_h__ 1
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
#include "qpid/dispatch/failoverlist.h"

#include <proton/engine.h>
#include <proton/event.h>

typedef struct qd_server_t      qd_server_t;
typedef struct qd_container_t   qd_container_t;
typedef struct qd_http_server_t qd_http_server_t;
typedef struct qd_policy_t      qd_policy_t;
typedef struct qd_dispatch_t    qd_dispatch_t;
typedef struct sys_mutex_t      sys_mutex_t;

/**@file
 * Control server threads and connections.
 */

/**
 * @defgroup server server
 *
 * Control server threads, starting and stopping the server.
 * @{
 */

/**
 * Run the server threads until completion - The blocking version.
 *
 * Start the operation of the server, including launching all of the worker
 * threads.  Returns when all server threads have exited. The thread that calls
 * qd_server_run is used as one of the worker threads.
 *
 * @param qd The dispatch handle returned by qd_dispatch.
 */
void qd_server_run(qd_dispatch_t *qd);


/**
 * Tells the server to stop but doesn't wait for server to exit.
 * The call to qd_server_run() will exit when all server threads have exited.
 *
 * May be called from any thread or from a signal handler.
 *
 * @param qd The dispatch handle returned by qd_dispatch.
 */

void qd_server_stop(qd_dispatch_t *qd);

/**
 * @}
 * @defgroup connection connection
 *
 * Server AMQP Connection Handling
 *
 * Handling listeners, connectors, connections and events.
 * @{
 */


/**
 * Event type for the connection callback.
 */
typedef enum {
    /// The connection was closed at the transport level (not cleanly).
    QD_CONN_EVENT_CLOSE,

    /// The connection is writable
    QD_CONN_EVENT_WRITABLE
} qd_conn_event_t;


/**
 * Set the container, must be set prior to the invocation of qd_server_run.
 */
void qd_server_set_container(qd_dispatch_t *qd, struct qd_container_t *container);

pn_proactor_t *qd_server_proactor(const qd_server_t *qd_server);
qd_http_server_t *qd_server_http(const qd_server_t *qd_server);
uint64_t qd_server_allocate_connection_id(qd_server_t *server);

/**
 * Callback handler and context for proactor events
 */
typedef void (*qd_server_event_handler_t) (pn_event_t *e, qd_server_t *qd_server, void *context);

typedef struct qd_handler_context_t {
    void                      *context;
    qd_server_event_handler_t  handler;
} qd_handler_context_t;


const char *qd_server_get_container_name(const qd_server_t *server);
sys_mutex_t *qd_server_get_activation_lock(qd_server_t *server);

/**
 * @}
 */

#endif

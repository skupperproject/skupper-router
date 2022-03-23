#ifndef __tcp_adaptor_h__
#define __tcp_adaptor_h__ 1
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

#include "dispatch_private.h"
#include "timer_private.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/enum.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/protocol_log.h"

#include <proton/engine.h>
#include <proton/event.h>
#include <proton/ssl.h>

typedef struct qd_tcp_listener_t qd_tcp_listener_t;
typedef struct qd_tcp_connector_t qd_tcp_connector_t;
typedef struct qd_tcp_bridge_t qd_tcp_bridge_t;

struct qd_tcp_bridge_t
{
    /* Created and referenced by each new listener or connector.
     * Referenced by all connections created by listener or connector
     */
    sys_atomic_t  ref_count;
    // static configuration defined at listener/connector creation
    char         *name;
    char         *address;   // VAN (AMQP) address
    char         *host;
    char         *port;
    char         *site_id;
    char         *host_port; // Generated as "<host>:<port>" for convenience
    // run time statistics updated by connections
    sys_mutex_t  *stats_lock;
    uint64_t      connections_opened;
    uint64_t      connections_closed;
    uint64_t      bytes_in;
    uint64_t      bytes_out;
};

DEQ_DECLARE(qd_tcp_bridge_t, qd_bridge_config_list_t);
ALLOC_DECLARE(qd_tcp_bridge_t);

struct qd_tcp_listener_t
{
    qd_handler_context_t      context;
    /* May be referenced by connection_manager and pn_listener_t */
    sys_atomic_t              ref_count;
    qd_server_t              *server;
    qd_tcp_bridge_t          *config;
    pn_listener_t            *pn_listener;
    plog_record_t            *plog;

    DEQ_LINKS(qd_tcp_listener_t);
};

DEQ_DECLARE(qd_tcp_listener_t, qd_tcp_listener_list_t);
ALLOC_DECLARE(qd_tcp_listener_t);

struct qd_tcp_connector_t
{
    /* May be referenced by connection_manager, timer and pn_connection_t */
    sys_atomic_t              ref_count;
    qd_server_t              *server;
    qd_tcp_bridge_t          *config;
    void                     *dispatcher;
    plog_record_t            *plog;

    DEQ_LINKS(qd_tcp_connector_t);
};

DEQ_DECLARE(qd_tcp_connector_t, qd_tcp_connector_list_t);
ALLOC_DECLARE(qd_tcp_connector_t);

void qdra_tcp_connection_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset);
void qdra_tcp_connection_get_next_CT(qdr_core_t *core, qdr_query_t *query);
void qdra_tcp_connection_get_CT(qdr_core_t          *core,
                                qd_iterator_t       *name,
                                qd_iterator_t       *identity,
                                qdr_query_t         *query,
                                const char          *qdr_tcp_connection_columns[]);

#define QDR_TCP_CONNECTION_COLUMN_COUNT 10
extern const char *qdr_tcp_connection_columns[QDR_TCP_CONNECTION_COLUMN_COUNT + 1];

#endif

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
#include "adaptor_common.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/enum.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/protocol_log.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <proton/engine.h>
#include <proton/event.h>
#include <proton/ssl.h>

typedef struct qd_tcp_listener_t qd_tcp_listener_t;
typedef struct qd_tcp_connector_t qd_tcp_connector_t;
typedef struct qdr_tcp_stats_t qdr_tcp_stats_t;
typedef struct qd_tcp_adaptor_config_t qd_tcp_adaptor_config_t;
typedef struct qd_adaptor_listener_t qd_adaptor_listener_t;

struct qd_tcp_adaptor_config_t {
    qd_adaptor_config_t *adaptor_config; // Pointer to the common adaptor config used by all adaptors.
    sys_atomic_t         ref_count;
};

ALLOC_DECLARE(qd_tcp_adaptor_config_t);

struct qdr_tcp_stats_t {
    // run time statistics updated by connections
    sys_mutex_t  *stats_lock;
    uint64_t      connections_opened;
    uint64_t      connections_closed;
    uint64_t      bytes_in;
    uint64_t      bytes_out;
};

ALLOC_DECLARE(qdr_tcp_stats_t);

struct qd_tcp_listener_t
{
    qd_server_t              *server;
    qd_tcp_adaptor_config_t  *config;
    plog_record_t            *plog;
    qdr_tcp_stats_t          *tcp_stats;
    qd_adaptor_listener_t    *adaptor_listener;

    // must hold tcp_adaptor->listener_lock during list operations:
    DEQ_LINKS(qd_tcp_listener_t);
};

DEQ_DECLARE(qd_tcp_listener_t, qd_tcp_listener_list_t);
ALLOC_DECLARE(qd_tcp_listener_t);

struct qd_tcp_connector_t
{
    // ref_count: tcp_adaptor connector list, qdr_tcp_connection_t
    sys_atomic_t              ref_count;
    qd_server_t              *server;
    qd_tcp_adaptor_config_t  *config;
    void                     *dispatcher_conn;
    plog_record_t            *plog;
    qdr_tcp_stats_t           *tcp_stats;
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

void qd_free_tcp_adaptor_config(qd_tcp_adaptor_config_t *config, qd_log_source_t  *log_source);
qd_error_t qd_load_tcp_adaptor_config(qd_dispatch_t *qd, qd_tcp_adaptor_config_t *config, qd_entity_t* entity);
#define QDR_TCP_CONNECTION_COLUMN_COUNT 10
extern const char *qdr_tcp_connection_columns[QDR_TCP_CONNECTION_COLUMN_COUNT + 1];

#endif

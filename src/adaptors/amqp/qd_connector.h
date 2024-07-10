#ifndef __qd_connector_h__
#define __qd_connector_h__ 1
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

#include "server_config.h"

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/discriminator.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/server.h"

#include <stdbool.h>

typedef struct qd_timer_t  qd_timer_t;
typedef struct qd_server_t qd_server_t;
typedef struct qd_connection_t qd_connection_t;
typedef struct vflow_record_t  vflow_record_t;

typedef enum {
    CXTR_STATE_INIT = 0,
    CXTR_STATE_CONNECTING,
    CXTR_STATE_OPEN,
    CXTR_STATE_FAILED,
    CXTR_STATE_DELETED  // by management
} cxtr_state_t;

/**
 * Connector objects represent the desire to create and maintain an outgoing transport connection.
 */
typedef struct qd_connector_t {
    DEQ_LINKS(struct qd_connector_t);

    /* Referenced by connection_manager and pn_connection_t */
    sys_atomic_t              ref_count;
    qd_server_t              *server;
    qd_server_config_t        config;
    qd_timer_t               *timer;
    long                      delay;

    /* Connector state and ctx can be modified by I/O or management threads. */
    sys_mutex_t               lock;
    cxtr_state_t              state;
    qd_connection_t          *qd_conn;
    vflow_record_t           *vflow_record;
    bool                      oper_status_down;  // set when oper-status transitions to 'down' to avoid repeated error indications.

    /* This conn_list contains all the connection information needed to make a connection. It also includes failover connection information */
    qd_failover_item_list_t   conn_info_list;
    int                       conn_index; // Which connection in the connection list to connect to next.

    /* Optional policy vhost name */
    char                     *policy_vhost;

    /* Connection group state */
    bool is_data_connector;
    char group_correlator[QD_DISCRIMINATOR_SIZE];

    /* holds proton transport error condition text on connection failure */
#define QD_CXTR_CONN_MSG_BUF_SIZE 300
    char conn_msg[QD_CXTR_CONN_MSG_BUF_SIZE];
} qd_connector_t;

DEQ_DECLARE(qd_connector_t, qd_connector_list_t);

const qd_server_config_t *qd_connector_config(const qd_connector_t *c);

/**
 * Initiate an outgoing connection. Returns true if successful.
 */
bool qd_connector_connect(qd_connector_t *ct);

// KAG: todo: fixme:
qd_connector_t *qd_server_connector(qd_server_t *server);

void qd_connector_decref(qd_connector_t* ct);

bool qd_connector_has_failover_info(const qd_connector_t* ct);

const char *qd_connector_policy_vhost(const qd_connector_t* ct);
void qd_connector_handle_transport_error(qd_connector_t *connector, uint64_t connection_id, pn_condition_t *condition);
void qd_connector_remote_opened(qd_connector_t *connector);

// add a new connection to the parent connector
void qd_connector_add_connection(qd_connector_t *connector, qd_connection_t *ctx);
void qd_connector_add_link(qd_connector_t *connector);

// remove the child connection
// NOTE WELL: this may free the connector if the connection is holding the last
// reference to it
void qd_connector_remove_connection(qd_connector_t *connector, bool final, const char *condition_name, const char *condition_description);
#endif

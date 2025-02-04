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
typedef struct qd_connection_t  qd_connection_t;
typedef struct vflow_record_t   vflow_record_t;
typedef struct qd_tls_config_t  qd_tls_config_t;
typedef struct qd_admin_connector_t qd_admin_connector_t;

typedef enum {
    CXTR_STATE_INIT = 0,
    CXTR_STATE_CONNECTING,
    CXTR_STATE_OPEN,
    CXTR_STATE_FAILED,
    CXTR_STATE_DELETED  // by management
} cxtr_state_t;

/**
 * A qd_connector_t manages a single outgoing transport connection. It is responsible for re-establishing the connection
 * should it fail.  It is the child of a qd_admin_connector_t instance.
 */
typedef struct qd_connector_t {

    // Sibling connectors belonging to the same parent qd_admin_connector_t
    DEQ_LINKS(struct qd_connector_t);
    qd_admin_connector_t     *admin_conn;

    /* Referenced by parent qd_admin_connector_t and child qd_connection_t */
    sys_atomic_t              ref_count;
    qd_timer_t               *timer;
    long                      delay;

    /* Connector state and ctx can be modified by I/O or management threads. */
    sys_mutex_t               lock;
    cxtr_state_t              state;
    qd_connection_t          *qd_conn;
    vflow_record_t           *vflow_record;
    bool                      oper_status_down;  // set when oper-status transitions to 'down' to avoid repeated error indications.
    bool                      reconnect_enabled; // False: disable reconnect on connection drop
    bool                      is_data_connector; // inter-router conn for streaming messages

    /* This conn_list contains all the connection information needed to make a connection. It also includes failover connection information */
    qd_failover_item_list_t   conn_info_list;
    int                       conn_index; // Which connection in the connection list to connect to next.

    /* holds proton transport error condition text on connection failure */
#define QD_CXTR_CONN_MSG_BUF_SIZE 300
    char conn_msg[QD_CXTR_CONN_MSG_BUF_SIZE];
} qd_connector_t;

DEQ_DECLARE(qd_connector_t, qd_connector_list_t);


/**
 * An qd_admin_connector_t instance is created for each "connector" configuration object provisioned on the router. A
 * connector may have one or more outgoing connections associated with it depending on the connectors role. The purpose
 * of the qd_admin_connector_t is to manage a set of outgoing connections associated with the connector
 * configuration. An qd_admin_connector_t will instantiate a qd_connector_t for each outgoing connection required by the
 * connector configuration.
 */
struct qd_admin_connector_t {
    DEQ_LINKS(struct qd_admin_connector_t);  // connection_manager list

    /* Referenced by connection_manager and children qd_connector_t */
    sys_atomic_t              ref_count;
    qd_server_config_t        config;
    qd_server_t              *server;
    char                     *policy_vhost;  /* Optional policy vhost name */
    qd_tls_config_t          *tls_config;
    uint32_t                  data_connection_count;  // # of child inter-router data connections

    // The group correlation id for all child connections
    char                      group_correlator[QD_DISCRIMINATOR_SIZE];

    bool                      activated;     // T: activated by connection manager
    sys_mutex_t               lock;          // protect connectors list
    qd_connector_list_t       connectors;
};

DEQ_DECLARE(qd_admin_connector_t, qd_admin_connector_list_t);

/** Management call to create an Admin Connector
 */
qd_admin_connector_t *qd_admin_connector_create(qd_dispatch_t *qd, qd_entity_t *entity);

/** Management call to delete the Admin Connector
 *
 * This will close and release all child connector and connections then
 * decrement the callers reference count to the admin connector.
 */
void qd_admin_connector_delete(qd_admin_connector_t *admin_conn);

/** Management call to start all child connector connections
 */
void qd_admin_connector_connect(qd_admin_connector_t *admin_conn);

/** Drop a reference to the Admin Connector
 */
void qd_admin_connector_decref(qd_admin_connector_t *admin_conn);

/**
 * Connector API
 */

/**
 * Create a new connector.
 * Call qd_connector_connect() to initiate the outgoing connection
 */
qd_connector_t *qd_connector(qd_admin_connector_t *admin_conn, bool is_data_connector);

/**
 * Initiate an outgoing connection. Returns true if successful.
 */
bool qd_connector_connect(qd_connector_t *ct);

/**
 * Close the associated connection and deactivate the connector
 */
void qd_connector_close(qd_connector_t *ct);

void qd_connector_decref(qd_connector_t *ct);

const qd_server_config_t *qd_connector_get_config(const qd_connector_t *ct);
const char *qd_connector_get_group_correlator(const qd_connector_t *ct);
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

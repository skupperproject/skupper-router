#ifndef __qd_connection_h__
#define __qd_connection_h__ 1
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
#include "http.h"
#include "timer_private.h"
#include "router_core/router_core_private.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/discriminator.h"

#include <proton/engine.h>
#include <proton/event.h>
#include <proton/ssl.h>

#include <netdb.h>              /* For NI_MAXHOST/NI_MAXSERV */


qd_connection_t *qd_server_connection(qd_server_t *server, qd_server_config_t* config);

qd_connector_t* qd_connection_connector(const qd_connection_t *c);

bool qd_connection_handle(qd_connection_t *c, pn_event_t *e);


/**
 * Connection objects wrap Proton connection objects.
 */
struct qd_connection_t {
    DEQ_LINKS(qd_connection_t);
    char                            *name;
    qd_server_t                     *server;
    bool                            opened; // An open callback was invoked for this connection
    bool                            closed;
    bool                            closed_locally;
    int                             enqueued;
    qd_timer_t                      *timer;   // Timer for initial-setup
    pn_connection_t                 *pn_conn;
    pn_session_t                    *pn_sessions[QD_SSN_CLASS_COUNT];
    pn_ssl_t                        *ssl;
    qd_listener_t                   *listener;
    qd_connector_t                  *connector;
    void                            *context; // context from listener or connector
    void                            *user_context;
    void                            *link_context; // Context shared by this connection's links
    uint64_t                        connection_id; // A unique identifier for the qd_connection_t. The underlying pn_connection already has one but it is long and clunky.
    const char                      *user_id; // A unique identifier for the user on the connection. This is currently populated  from the client ssl cert. See ssl_uid_format in server.h for more info
    bool                            free_user_id;
    qd_policy_settings_t            *policy_settings;
    int                             n_sessions;
    int                             n_senders;
    int                             n_receivers;
    void                            *open_container;
    qd_deferred_call_list_t         deferred_calls;
    sys_mutex_t                     deferred_call_lock;
    bool                            policy_counted;
    char                           *role;  //The specified role of the connection, e.g. "normal", "inter-router", "route-container" etc.
    char                            group_correlator[QD_DISCRIMINATOR_SIZE];
    qd_pn_free_link_session_list_t  free_link_session_list;
    bool                            strip_annotations_in;
    bool                            strip_annotations_out;
    void (*wake)(qd_connection_t*); /* Wake method, different for libwebsockets vs. proactor */
    sys_atomic_t                    wake_core;                      // Atomic flag to indicate that core actions are due at next activation
    sys_atomic_t                    wake_cutthrough_outbound;       // Outbound cut-through actions are due at next activation
    sys_atomic_t                    wake_cutthrough_inbound;        // Inbound cut-through actions are due at next activation
    qdr_delivery_ref_list_t         inbound_cutthrough_worklist;    // List of outbound deliveries using cut-through
    qdr_delivery_ref_list_t         outbound_cutthrough_worklist;   // List of inbound deliveries using cut-through
    sys_spinlock_t                  inbound_cutthrough_spinlock;    // Spinlock to protect the inbound worklist
    sys_spinlock_t                  outbound_cutthrough_spinlock;   // Spinlock to protect the outbound worklist
    char rhost[NI_MAXHOST];     /* Remote host numeric IP for incoming connections */
    char rhost_port[NI_MAXHOST+NI_MAXSERV]; /* Remote host:port for incoming connections */
};

DEQ_DECLARE(qd_connection_t, qd_connection_list_t);
ALLOC_DECLARE_SAFE(qd_connection_t);

#endif

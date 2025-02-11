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


#include "server_config.h"

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/router_core.h"  // for qdr_delivery_ref_list_t
#include "qpid/dispatch/discriminator.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <netdb.h>              /* For NI_MAXHOST/NI_MAXSERV */


#ifndef NI_MAXHOST
# define NI_MAXHOST 1025
#endif

#ifndef NI_MAXSERV
# define NI_MAXSERV 32
#endif

typedef struct qd_listener_t        qd_listener_t;
typedef struct qd_connector_t       qd_connector_t;
typedef struct qd_policy_settings_t qd_policy_settings_t;
typedef struct pn_connection_t      pn_connection_t;
typedef struct qd_session_t         qd_session_t;
typedef struct qd_timer_t           qd_timer_t;
typedef struct qd_tls_session_t     qd_tls_session_t;


// defer a function call to be invoked on the connections proactor thread at
// a later time
//
typedef struct qd_deferred_call_t {
    DEQ_LINKS(struct qd_deferred_call_t);
    qd_deferred_t  call;
    void          *context;
} qd_deferred_call_t;

DEQ_DECLARE(qd_deferred_call_t, qd_deferred_call_list_t);


typedef struct qd_pn_free_link_t qd_pn_free_link_t;
DEQ_DECLARE(qd_pn_free_link_t, qd_pn_free_link_list_t);

DEQ_DECLARE(qd_session_t, qd_session_list_t);


/**
 * Connection objects wrap Proton connection objects.
 */
typedef struct qd_connection_t qd_connection_t;
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
    qd_session_t                    *qd_sessions[QD_SSN_CLASS_COUNT];  // long lived inter-router sessions
    qd_session_list_t                child_sessions;  // all active sessions
    qd_tls_session_t                *ssl;
    qd_listener_t                   *listener;
    qd_connector_t                  *connector;
    void                            *context; // context from listener or connector
    void                            *user_context;
    void                            *link_context; // Context shared by this connection's links
    uint64_t                        connection_id; // A unique identifier for the qd_connection_t. The underlying pn_connection already has one but it is long and clunky.
    char                            *user_id; // A unique identifier for the user on the connection. This is currently populated from the client ssl cert. See sslProfile.uidFormat for more info
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
    qd_pn_free_link_list_t          free_link_list;
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

ALLOC_DECLARE_SAFE(qd_connection_t);

// initialize a newly allocated qd_connection_t
void qd_connection_init(qd_connection_t *qd_conn, qd_server_t *server, const qd_server_config_t *config,
                        qd_connector_t *connector, qd_listener_t *listener);

qd_connector_t* qd_connection_connector(const qd_connection_t *c);

// for use with libwebsockets HTTP over AMQPWS
bool qd_amqpws_handle_connection_event(qd_connection_t *qd_conn, pn_event_t *e);
void qd_amqpws_end_of_batch(qd_connection_t *qd_conn);

/**
 * Get the remote host IP address of the connection.
 */
const char* qd_connection_remote_ip(const qd_connection_t *c);

bool qd_connection_strip_annotations_in(const qd_connection_t *c);

void qd_connection_wake(qd_connection_t *ctx);

uint64_t qd_connection_max_message_size(const qd_connection_t *c);

/**
 * Get the name of the connection, based on its IP address.
 */
const char* qd_connection_name(const qd_connection_t *c);

/**
 * Schedule a call to be invoked on a thread that has ownership of this connection.
 * It will be safe for the callback to perform operations related to this connection.
 *
 * @param conn Connection object
 * @param call The function to be invoked on the connection's thread
 * @param context The context to be passed back in the callback
 */
void qd_connection_invoke_deferred(qd_connection_t *conn, qd_deferred_t call, void *context);

void qd_connection_invoke_deferred_calls(qd_connection_t *conn, bool discard);

/**
 * Schedule a call to be invoked on a thread that has ownership of this connection
 * when it will be safe for the callback to perform operations related to this connection.
 * A qd_deferred_call_t object has been allocated before hand to avoid taking
 * the ENTITY_CACHE lock.
 *
 * @param conn Connection object
 * @param call The function to be invoked on the connection's thread
 * @param context The context to be passed back in the callback
 * @param dct Pointer to preallocated qd_deferred_call_t object
 */
void qd_connection_invoke_deferred_impl(qd_connection_t *conn, qd_deferred_t call, void *context, void *dct);


/**
 * Allocate a qd_deferred_call_t object
 */
void *qd_connection_new_qd_deferred_call_t(void);

/**
 * Deallocate a qd_deferred_call_t object
 *
 * @param dct Pointer to preallocated qd_deferred_call_t object
 */
void qd_connection_free_qd_deferred_call_t(void *dct);


/**
 * Get the wrapped proton-engine connection object.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return The proton connection object.
 */
pn_connection_t *qd_connection_pn(qd_connection_t *conn);


/**
 * Get the direction of establishment for this connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return true if connection came through a listener, false if through a connector.
 */
bool qd_connection_inbound(qd_connection_t *conn);


/**
 * Get the connection id of a connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return The connection_id associated with the connection.
 */
uint64_t qd_connection_connection_id(qd_connection_t *conn);


/**
 * Get the configuration that was used in the setup of this connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return A pointer to the server configuration used in the establishment of this connection.
 */
const qd_server_config_t *qd_connection_config(const qd_connection_t *conn);

/**
 * Set the user context for a connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @param context User context to be stored with the connection.
 */
void qd_connection_set_context(qd_connection_t *conn, void *context);


/**
 * Get the user context from a connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return The user context stored with the connection.
 */
void *qd_connection_get_context(qd_connection_t *conn);


/**
 * Get the configuration context (connector or listener) for this connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return The context supplied at the creation of the listener or connector.
 */
void *qd_connection_get_config_context(qd_connection_t *conn);


/**
 * Set the link context for a connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @param context Link context to be stored with the connection.
 */
void qd_connection_set_link_context(qd_connection_t *conn, void *context);


/**
 * Get the link context from a connection.
 *
 * @param conn Connection object supplied in QD_CONN_EVENT_{LISTENER,CONNECTOR}_OPEN
 * @return The link context stored with the connection.
 */
void *qd_connection_get_link_context(qd_connection_t *conn);


/**
 * Sets the user id on the connection.
 * If the sasl mech is EXTERNAL, set the user_id on the connection as the concatenated
 * list of fields specified in the uidFormat field of skrouter.json
 * If no uidFormat is specified, the user is set to the pn_transport_user
 *
 * @param conn Connection object
 */
void qd_connection_set_user(qd_connection_t *conn);

/**
 * Activate a connection for output.
 *
 * This function is used to request that the server activate the indicated
 * connection.  It is assumed that the connection is one that the caller does
 * not have permission to access (i.e. it may be owned by another thread
 * currently).  An activated connection will, when writable, appear in the
 * internal work list and be invoked for processing by a worker thread.
 *
 * @param conn The connection over which the application wishes to send data
 */
void qd_connection_activate(qd_connection_t *conn);

/**
 * Activate a connection for cut-through processing.
 *
 * @param conn The connection that is being requested to do cut-through processing
 * @param incoming The direction of flow being requested (true = INCOMING, false = OUTGOING)
 */
void qd_connection_activate_cutthrough(qd_connection_t *conn, bool incoming);

void qd_connection_transport_tracer(pn_transport_t *transport, const char *message);

bool qd_connection_handle_event(qd_server_t *qd_server, pn_event_t *e, void *context);
bool qd_connection_strip_annotations_in(const qd_connection_t *c);
#endif

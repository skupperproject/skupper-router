#ifndef __amqp_adaptor_h__
#define __amqp_adaptor_h__ 1
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

/*
 * AMQP Adaptor API
 */

#include <qpid/dispatch/alloc_pool.h>

#include <stdbool.h>
#include <inttypes.h>

typedef struct qd_message_t qd_message_t;
typedef struct pn_link_t    pn_link_t;

// These types are private to the AMQP adaptor:
typedef struct qd_connection_t qd_connection_t;
typedef struct qd_link_t       qd_link_t;
typedef struct qd_session_t    qd_session_t;
typedef struct qd_router_t     qd_router_t;

// For use by message.c

qd_connection_t *qd_link_connection(const qd_link_t *qd_link);
void qd_link_set_incoming_msg(qd_link_t *link, qd_message_t *msg);
void qd_link_q2_restart_receive(qd_alloc_safe_ptr_t context);
bool qd_link_is_q2_limit_unbounded(const qd_link_t *link);
pn_link_t *qd_link_pn(const qd_link_t *link);
bool qd_connection_strip_annotations_in(const qd_connection_t *c);
uint64_t qd_connection_max_message_size(const qd_connection_t *c);
void qd_connection_log_policy_denial(const qd_link_t *link, const char *text);
qd_session_t *qd_link_get_session(const qd_link_t *link);
size_t qd_session_get_outgoing_capacity(const qd_session_t *qd_ssn);

// Used by the log module
void qd_amqp_connection_set_tracing(bool enabled);


//
// AMQP handlers
//
/**
 * Invoked when an attach for a new outgoing (sending) link is received.
 */
int  AMQP_outgoing_link_handler(qd_router_t *router, qd_link_t *link);
/**
 * Invoked when a new or existing received delivery is available for processing.
 */
bool AMQP_rx_handler(qd_router_t *router, qd_link_t *link);
/**
 * Invoked when an existing delivery changes disposition or settlement state.
 */
void AMQP_disposition_handler(qd_router_t *router, qd_link_t *link, pn_delivery_t *pnd);
/**
 * Invoked when an attach for a new incoming (receiving) link is received.
 */
int  AMQP_incoming_link_handler(qd_router_t *router, qd_link_t *link);
/**
 * Invoked when a link we created was opened by the peer
 */
int  AMQP_link_attach_handler(qd_router_t *router, qd_link_t *link);
 /**
  * Invoked when link detached is received.
  */
int  AMQP_link_detach_handler(qd_router_t *router, qd_link_t *link);
 /**
  * Invoked when an incoming connection (via listener) is opened.
  */
int  AMQP_inbound_opened_handler(qd_router_t *router, qd_connection_t *conn, void *context);
/**
 * Invoked when an outgoing connection (via connector) is opened.
 */
int  AMQP_outbound_opened_handler(qd_router_t *router, qd_connection_t *conn, void *context);
/**
 * Invoked when a connection is closed.
 */
int  AMQP_closed_handler(qd_router_t *router, qd_connection_t *conn, void *context);
/**
 * Invoked when an activated connection is available for writing.
 */
int  AMQP_conn_wake_handler(qd_router_t *router, qd_connection_t *conn, void *context);
/** The last callback issued for the given qd_link_t. The adaptor must clean up all state related to the qd_link_t
 * as it will be freed on return from this call. The forced flag is set to true if the link is being forced closed
 * due to the parent connection/session closing or on shutdown.
 */
void AMQP_link_closed_handler(qd_router_t *router, qd_link_t *qd_link, bool forced);
/**
 * Invoked when a link receives a flow event
 */
int  AMQP_link_flow_handler(qd_router_t *router, qd_link_t *link);
#endif

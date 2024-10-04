#ifndef __amqp_container_h__
#define __amqp_container_h__ 1
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

/**@file
 * Container for nodes, links and deliveries.
 *
 * @defgroup container container
 *
 * Container for nodes, links and deliveries.
 *
 * @{
 */

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <proton/engine.h>
#include <proton/version.h>

typedef struct qd_container_t qd_container_t;

typedef uint8_t qd_dist_mode_t;
#define QD_DIST_COPY 0x01
#define QD_DIST_MOVE 0x02
#define QD_DIST_BOTH 0x03

/**
 * Node Lifetime Policy (see AMQP 3.5.9)
 */
typedef enum {
    QD_LIFE_PERMANENT,
    QD_LIFE_DELETE_CLOSE,
    QD_LIFE_DELETE_NO_LINKS,
    QD_LIFE_DELETE_NO_MESSAGES,
    QD_LIFE_DELETE_NO_LINKS_MESSAGES
} qd_lifetime_policy_t;


typedef struct qd_session_t    qd_session_t;
typedef struct qd_link_t       qd_link_t;
typedef struct qd_connection_t qd_connection_t;
typedef struct qd_node_type_t  qd_node_type_t;


ALLOC_DECLARE_SAFE(qd_link_t);
DEQ_DECLARE(qd_link_t, qd_link_list_t);

qd_container_t *qd_container(qd_router_t *router, const qd_node_type_t *node_type);
void qd_container_free(qd_container_t *container);

qd_link_t *qd_link(qd_connection_t *conn, qd_direction_t dir, const char *name, qd_session_class_t);
void qd_link_free(qd_link_t *link);

/**
 * List of reference in the qd_link used to track abandoned deliveries
 */
typedef struct qd_link_ref_t {
    DEQ_LINKS(struct qd_link_ref_t);
    void *ref;
} qd_link_ref_t;

ALLOC_DECLARE(qd_link_ref_t);
DEQ_DECLARE(qd_link_ref_t, qd_link_ref_list_t);

qd_link_ref_list_t *qd_link_get_ref_list(qd_link_t *link);

/**
 * Context associated with the link for storing link-specific state.
 */
void qd_link_set_context(qd_link_t *link, void *link_context);
void *qd_link_get_context(const qd_link_t *link);

void policy_notify_opened(void *container, qd_connection_t *conn, void *context);
qd_direction_t qd_link_direction(const qd_link_t *link);
void qd_link_set_q2_limit_unbounded(qd_link_t *link, bool q2_limit_unbounded);
pn_snd_settle_mode_t qd_link_remote_snd_settle_mode(const qd_link_t *link);
pn_terminus_t *qd_link_source(qd_link_t *link);
pn_terminus_t *qd_link_target(qd_link_t *link);
pn_terminus_t *qd_link_remote_source(qd_link_t *link);
pn_terminus_t *qd_link_remote_target(qd_link_t *link);
void qd_link_close(qd_link_t *link);
void qd_link_detach(qd_link_t *link);
void qd_link_free(qd_link_t *link);
void qd_link_q2_restart_receive(const qd_alloc_safe_ptr_t context);
void qd_link_q3_block(qd_link_t *link);
void qd_link_q3_unblock(qd_link_t *link);
uint64_t qd_link_link_id(const qd_link_t *link);
void qd_link_set_link_id(qd_link_t *link, uint64_t link_id);
struct qd_message_t;
void qd_link_set_incoming_msg(qd_link_t *link, struct qd_message_t *msg);

void qd_session_incref(qd_session_t *qd_ssn);
void qd_session_decref(qd_session_t *qd_ssn);
bool qd_session_is_q3_blocked(const qd_session_t *qd_ssn);
qd_link_list_t *qd_session_q3_blocked_links(qd_session_t *qd_ssn);
void qd_session_set_max_in_window(qd_session_t *qd_ssn, uint32_t in_window);

void qd_connection_release_sessions(qd_connection_t *qd_conn);

///@}
#endif

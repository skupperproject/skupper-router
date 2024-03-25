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


// For use by message.c

qd_connection_t *qd_link_connection(const qd_link_t *qd_link);
void qd_link_set_incoming_msg(qd_link_t *link, qd_message_t *msg);
void qd_link_q2_restart_receive(qd_alloc_safe_ptr_t context);
bool qd_link_is_q2_limit_unbounded(const qd_link_t *link);
pn_link_t *qd_link_pn(const qd_link_t *link);
bool qd_connection_strip_annotations_in(const qd_connection_t *c);
uint64_t qd_connection_max_message_size(const qd_connection_t *c);
void qd_connection_log_policy_denial(const qd_link_t *link, const char *text);

// Used by the log module
void qd_amqp_connection_set_tracing(bool enabled);

#endif

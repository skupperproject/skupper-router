#ifndef __amqp_adaptor_private_h__
#define __amqp_adaptor_private_h__ 1
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
 * APIs and types private to the AMQP adaptor.
 *
 * This file must not be included by any file external to the files in this directory!!
 * Doing so will risk threading violations and will break stuff!
 */

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/threading.h"

typedef struct qdr_core_t             qdr_core_t;
typedef struct qd_router_t            qd_router_t;
typedef struct qdr_protocol_adaptor_t qdr_protocol_adaptor_t;
typedef struct qd_connection_t        qd_connection_t;
typedef struct qd_container_t         qd_container_t;
typedef struct qd_dispatch_t          qd_dispatch_t;

DEQ_DECLARE(qd_connection_t, qd_connection_list_t);


//
// AMQP Protocol Adaptor
//
typedef struct amqp_adaptor_t amqp_adaptor_t;
struct amqp_adaptor_t {
    qdr_core_t             *core;
    qd_dispatch_t          *dispatch;
    qd_router_t            *router;
    qdr_protocol_adaptor_t *adaptor;
    qd_container_t         *container;
    sys_mutex_t             lock;
    qd_connection_list_t    conn_list;
};

extern amqp_adaptor_t amqp_adaptor;


#endif

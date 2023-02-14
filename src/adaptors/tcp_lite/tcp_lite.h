#ifndef __tcp_lite_h__
#define __tcp_lite_h__ 1
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
#include "delivery.h"
#include "adaptors/adaptor_common.h"
#include "adaptors/adaptor_listener.h"
#include "adaptors/adaptor_tls.h"


typedef struct tcplite_common_t     tcplite_common_t;
typedef struct tcplite_listener_t   tcplite_listener_t;
typedef struct tcplite_connector_t  tcplite_connector_t;
typedef struct tcplite_connection_t tcplite_connection_t;

ALLOC_DECLARE(tcplite_listener_t);
ALLOC_DECLARE(tcplite_connector_t);
ALLOC_DECLARE_SAFE(tcplite_connection_t);

DEQ_DECLARE(tcplite_listener_t,   tcplite_listener_list_t);
DEQ_DECLARE(tcplite_connector_t,  tcplite_connector_list_t);
DEQ_DECLARE(tcplite_connection_t, tcplite_connection_list_t);


typedef enum {
    TL_LISTENER,
    TL_CONNECTOR,
    TL_CONNECTION
} tcplite_context_type_t;

struct tcplite_common_t {
    tcplite_context_type_t  context_type;
    tcplite_common_t       *parent;
    vflow_record_t         *vflow;
    qdr_connection_t       *core_conn;
    uint64_t                conn_id;
};

struct tcplite_listener_t {
    tcplite_common_t           common;
    DEQ_LINKS(tcplite_listener_t);
    sys_mutex_t                lock;
    qd_timer_t                *activate_timer;
    qd_adaptor_config_t       *adaptor_config;
    qd_tls_domain_t           *tls_domain;
    uint64_t                   link_id;
    qdr_link_t                *in_link;
    qd_adaptor_listener_t     *adaptor_listener;
    tcplite_connection_list_t  connections;
    uint64_t                   connections_opened;
    uint64_t                   connections_closed;
    bool                       closing;
};


typedef struct tcplite_connector_t {
    tcplite_common_t           common;
    DEQ_LINKS(tcplite_connector_t);
    sys_mutex_t                lock;
    qd_timer_t                *activate_timer;
    qd_adaptor_config_t       *adaptor_config;
    qd_tls_domain_t           *tls_domain;
    uint64_t                   link_id;
    qdr_link_t                *out_link;
    tcplite_connection_list_t  connections;
    uint64_t                   connections_opened;
    uint64_t                   connections_closed;
    bool                       closing;
} tcplite_connector_t;


typedef enum {
    LSIDE_INITIAL,
    LSIDE_LINK_SETUP,
    LSIDE_STREAM_START,
    LSIDE_FLOW,
    CSIDE_INITIAL,
    CSIDE_RAW_CONNECTION_OPENING,
    CSIDE_LINK_SETUP,
    CSIDE_FLOW,
    XSIDE_CLOSING
} tcplite_connection_state_t;
ENUM_DECLARE(tcplite_connection_state);


//
// Important note about the polarity of the link/stream/delivery/disposition tuples:
//
//                   Listener Side      Connector Side
//               +------------------+-------------------+
//      Inbound  |      Client      |      Server       |
//               +------------------+-------------------+
//     Outbound  |      Server      |      Client       |
//               +------------------+-------------------+
//
typedef struct tcplite_connection_t {
    tcplite_common_t            common;
    DEQ_LINKS(tcplite_connection_t);
    pn_raw_connection_t        *raw_conn;
    sys_atomic_t                core_activation;
    sys_atomic_t                raw_opened;
    qd_timer_t                 *close_timer;
    qdr_link_t                 *inbound_link;
    qd_message_t               *inbound_stream;
    qdr_delivery_t             *inbound_delivery;
    uint64_t                    inbound_disposition;
    uint64_t                    inbound_link_id;
    qdr_link_t                 *outbound_link;
    qd_message_t               *outbound_stream;
    qdr_delivery_t             *outbound_delivery;
    uint64_t                    outbound_disposition;
    uint64_t                    outbound_link_id;
    uint64_t                    inbound_octets;
    uint64_t                    outbound_octets;
    qd_buffer_t                *outbound_body;
    pn_condition_t             *error;
    char                       *reply_to;
    qd_handler_context_t        context;
    tcplite_connection_state_t  state;
    bool                        listener_side;
    bool                        inbound_credit;
    bool                        inbound_first_octet;
    bool                        outbound_first_octet;
    bool                        outbound_body_complete;
} tcplite_connection_t;

#endif

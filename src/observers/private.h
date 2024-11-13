#ifndef __observers_private_h__
#define __observers_private_h__ 1
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

#include <qpid/dispatch/protocol_observer.h>
#include <qpid/dispatch/hash.h>
#include "adaptors/adaptor_common.h"
struct qdpo_config_t {
    qdpo_use_address_t           use_address;
    qd_observer_t                observer;
};


struct qdpo_t {
    qd_protocol_t  base;    // initial observed protocol
    qdpo_config_t *config;
};

/**
 * state machine for observing TCP
 */
#define TCP_PREFIX_LEN 32


typedef struct tcp_observer_state_t tcp_observer_state_t;
struct tcp_observer_state_t {
    // buffer holding enough of the stream to classify the protocol
    uint8_t prefix[TCP_PREFIX_LEN];
    int     prefix_len;

    // store incoming server data arriving prior to completing classification.
    qd_buffer_list_t  server_data;
    size_t            server_bytes;
};

/**
 * state machine for observing HTTP/1.x
 */
typedef struct qd_http1_decoder_connection_t  qd_http1_decoder_connection_t;
typedef struct http1_request_state_t          http1_request_state_t;
DEQ_DECLARE(http1_request_state_t, http1_request_state_list_t);

typedef struct qd_http2_decoder_connection_t  qd_http2_decoder_connection_t;
typedef struct http1_observer_state_t http1_observer_state_t;
struct http1_observer_state_t {
    qd_http1_decoder_connection_t *decoder;
    http1_request_state_list_t     requests;
};

/**
 * state machine for observing HTTP/2
 */
typedef struct qd_http2_decoder_connection_t  qd_http2_decoder_connection_t;
typedef struct http2_observer_state_t         http2_observer_state_t;
typedef struct qd_http2_stream_info_t         qd_http2_stream_info_t;

struct qd_http2_stream_info_t {
    DEQ_LINKS(qd_http2_stream_info_t);
    qd_http2_decoder_connection_t *conn_state;             // Reference to the stream's connection state information
    vflow_record_t                *vflow;                  // stream level vanflow. this is a vanflow per request
    uint32_t                       stream_id;              // stream_id of the stream.
    uint64_t                       bytes_in;
    uint64_t                       bytes_out;

};

ALLOC_DECLARE(qd_http2_stream_info_t);
DEQ_DECLARE(qd_http2_stream_info_t, qd_http2_stream_info_list_t);

struct http2_observer_state_t {
    qd_http2_decoder_connection_t *conn_state;
    qd_hash_t                     *stream_id_hash;
    qd_http2_stream_info_list_t    streams;    // A connection can have many streams.
};


/**
 * Per flow protocol observer
 */
struct qdpo_transport_handle_t {
    qdpo_t         *parent;
    vflow_record_t *vflow;
    void           *transport_context;
    uint64_t        conn_id;
    qd_protocol_t   protocol;   // current observed protocol

    void (*observe)(qdpo_transport_handle_t *, bool from_client, const unsigned char *buf, size_t length);

    union {
        tcp_observer_state_t   tcp;
        http1_observer_state_t http1;
        http2_observer_state_t http2;
    };
};

void qdpo_tcp_init(qdpo_transport_handle_t *handle);
void qdpo_tcp_final(qdpo_transport_handle_t *handle);

void qdpo_http1_init(qdpo_transport_handle_t *handle);
void qdpo_http1_final(qdpo_transport_handle_t *handle);

void qdpo_http2_init(qdpo_transport_handle_t *handle);
void qdpo_http2_final(qdpo_transport_handle_t *handle);

#endif

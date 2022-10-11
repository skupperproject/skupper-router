#ifndef __http2_adaptor_h__
#define __http2_adaptor_h__ 1

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
#include "adaptors/adaptor_buffer.h"
#include "adaptors/adaptor_common.h"
#include "adaptors/adaptor_tls.h"
#include "adaptors/http_common.h"
#include "server_private.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/compose.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"

#include <proton/tls.h>

#include <nghttp2/nghttp2.h>
#include <time.h>

size_t HTTP2_DATA_FRAME_HEADER_LENGTH = 9;


typedef struct qdr_http2_stream_data_t  qdr_http2_stream_data_t;
typedef struct qdr_http2_connection_t   qdr_http2_connection_t;

/**
 * Stream status
 */
typedef enum {
    QD_STREAM_OPEN,
    QD_STREAM_HALF_CLOSED,
    QD_STREAM_FULLY_CLOSED
} qd_http2_stream_status_t;

DEQ_DECLARE(qdr_http2_stream_data_t, qd_http2_stream_data_list_t);
DEQ_DECLARE(qdr_http2_connection_t,  qdr_http2_connection_list_t);

struct qdr_http2_stream_data_t {
    qdr_http2_connection_t   *conn;
    void                     *context;
    char                     *reply_to;
    char                     *remote_site; //for stats:
    char                     *method; //for stats, also used in the subject field of AMQP request message.
    char                     *request_status; //for stats, also used in the subject field of AMQP response message.
    qdr_delivery_t           *in_dlv;
    qdr_delivery_t           *out_dlv;
    uint64_t                  incoming_id;
    uint64_t                  outgoing_id;
    uint64_t                  out_dlv_local_disposition;
    qdr_link_t               *in_link;
    qdr_link_t               *out_link;
    qd_message_t             *message;
    qd_composed_field_t      *app_properties;
    qd_composed_field_t      *footer_properties;
    qd_buffer_list_t          body_buffers;
    qd_message_stream_data_t *curr_stream_data;
    qd_iterator_t            *curr_stream_data_iter; // points to the data contained in the stream_data/raw_buffers
    qd_message_stream_data_t *next_stream_data;
    qd_message_stream_data_t *footer_stream_data;
    qd_iterator_t            *footer_stream_data_iter;
    DEQ_LINKS(qdr_http2_stream_data_t);

    qd_message_stream_data_result_t  curr_stream_data_result;
    qd_message_stream_data_result_t  next_stream_data_result;
    int                            payload_handled;
    int                            in_link_credit;   // provided by router
    int32_t                        stream_id;
    qd_http2_stream_status_t       status;
    bool                     entire_footer_arrived;
    bool                     entire_header_arrived;
    bool                     out_msg_header_sent;
    bool                     out_msg_body_sent;
    bool                     use_footer_properties;
    bool                     full_payload_handled; // applies to the sending side.
    bool                     out_msg_has_body;
    bool                     out_msg_data_flag_eof;
    bool                     out_msg_has_footer;
    bool                     out_msg_send_complete; // we use this flag to save the send_complete flag because the delivery and message associated with this stream might have been freed.
    bool                     disp_updated;   // Has the disposition already been set on the out_dlv
    bool                     disp_applied;   // Has the disp been applied to the out_dlv. The stream is ready to be freed now.
    bool                     header_and_props_composed;  // true if the header and properties of the inbound message have already been composed so we don't have to do it again.
    bool                     stream_force_closed;
    bool                     in_dlv_decrefed;
    bool                     out_dlv_decrefed;
    bool                     body_data_added_to_msg;
    int                      bytes_in;
    int                      bytes_out;
    qd_timestamp_t           start;
    qd_timestamp_t           stop;
};

struct qdr_http2_connection_t {
    qd_handler_context_t     context;
    qdr_connection_t        *qdr_conn;
    pn_raw_connection_t     *pn_raw_conn;
    pn_raw_buffer_t          read_buffers[4];
    qd_timer_t               *activate_timer;
    qd_http_adaptor_config_t *config;
    qd_server_t             *server;

    char                     *remote_address;
    qdr_link_t               *stream_dispatcher;
    qdr_link_t               *dummy_link;

    qdr_http2_stream_data_t  *stream_dispatcher_stream_data;

    nghttp2_data_provider        data_prd;
    qd_adaptor_buffer_list_t     out_buffs;  // Buffers for writing
    nghttp2_session          *session;    // A pointer to the nghttp2s' session object
    qd_http2_stream_data_list_t  streams;    // A session can have many streams.
    qd_http_listener_t       *listener;
    qd_http_connector_t      *connector;
    qd_tls_t                    *tls;

    uint64_t bytes_in;   // if this is TLS conn, the decrypted bytes read from raw conn, else just raw bytes read from
                         // the raw conn
    uint64_t bytes_out;  // if this is TLS conn, the decrypted bytes before writing to raw connection, else just raw
                         // bytes written to raw conn
    uint64_t encrypted_bytes_in;  // If this is a TLS connection, the total encrypted bytes received on this connection,
                                  // zero otherwise
    uint64_t encrypted_bytes_out;  // If this is a TLS connection, the total encrypted bytes sent on this connection,
                                   // zero otherwise
    uint64_t conn_id;
    uint64_t stream_dispatcher_id;
    uint64_t dummy_link_id;

    bool                      connection_established;
    bool                      ingress;
    bool                      delete_egress_connections;  // If set to true, the egress qdr_connection_t and qdr_http2_connection_t objects will be deleted
    bool                      goaway_received;
    bool                      tls_error;
    sys_atomic_t              activate_scheduled;  // 1 == activate timer scheduled
    sys_atomic_t              raw_closed_read;
    sys_atomic_t              raw_closed_write;
    bool                      q2_blocked;      // send a connection level WINDOW_UPDATE frame to tell the client to stop sending data.
    sys_atomic_t              q2_restart;      // signal to resume receive
    sys_atomic_t              delay_buffer_write;   // if true, buffers will not be written to proton.
    bool                      require_tls;
    pn_tls_config_t          *tls_config;
    bool                      buffers_pushed_to_nghttp2;
    bool                      initial_settings_frame_sent;
    bool                      alpn_check_complete;
    DEQ_LINKS(qdr_http2_connection_t);
 };

ALLOC_DECLARE(qdr_http2_stream_data_t);
ALLOC_DECLARE(qdr_http2_connection_t);

#endif // __http2_adaptor_h__

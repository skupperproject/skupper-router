#ifndef http1_private_H
#define http1_private_H 1
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
 *
 */

//
// HTTP/1.x Adaptor Internals
//
// Nomenclature:
//  "in": for information flowing from the endpoint into the router
//        (from proactor to core)
//  "out": for information flowing from the router out to the endpoint
//         (from core to proactor)
//
#include "adaptors/http_common.h"

#include "qpid/dispatch/http1_codec.h"
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/protocol_adaptor.h"

// for debug: will dump I/O buffer content to stdout if true
#define HTTP1_DUMP_BUFFERS false

#define HTTP1_IO_BUF_SIZE 16384

typedef struct qdr_http1_out_data_t      qdr_http1_out_data_t;
typedef struct qdr_http1_request_base_t  qdr_http1_request_base_t;
typedef struct qdr_http1_connection_t    qdr_http1_connection_t;

DEQ_DECLARE(qdr_http1_connection_t, qdr_http1_connection_list_t);

typedef struct qdr_http1_adaptor_t {
    qdr_core_t                  *core;
    qdr_protocol_adaptor_t      *adaptor;
    qd_log_source_t             *log;
    sys_mutex_t                 *lock;  // for the lists and activation
    qd_http_listener_list_t      listeners;
    qd_http_connector_list_t     connectors;
    qdr_http1_connection_list_t  connections;
} qdr_http1_adaptor_t;

extern qdr_http1_adaptor_t *qdr_http1_adaptor;


// Data to be written out the raw connection.
//
// This adaptor has to cope with two different data sources: the HTTP1 encoder
// and the qd_message_stream_data_t list.  The HTTP1 encoder produces a simple
// qd_buffer_list_t for outgoing header data whose ownership is given to the
// adaptor: the adaptor is free to dequeue/free these buffers as needed.  The
// qd_message_stream_data_t buffers are shared with the owning message and the
// buffer list must not be modified by the adaptor.  The qdr_http1_out_data_t
// is used to manage both types of data sources.
//
struct qdr_http1_out_data_t {
    DEQ_LINKS(qdr_http1_out_data_t);

    // data is either in a raw buffer chain
    // or a message body data (not both!)

    qd_buffer_list_t raw_buffers;
    qd_message_stream_data_t *stream_data;

    // points to the data contained in the stream_data/raw_buffers
    qd_iterator_t *data_iter;
};
ALLOC_DECLARE(qdr_http1_out_data_t);
DEQ_DECLARE(qdr_http1_out_data_t, qdr_http1_out_data_list_t);


// Per HTTP request/response(s) state.
//
// This base class is extended for client and server-specific state, see
// http1_client.c and http1_server.c A reference is stored in
// qdr_delivery_get_context(dlv)
//
struct qdr_http1_request_base_t {
    DEQ_LINKS(qdr_http1_request_base_t);

    uint64_t                  msg_id;
    h1_codec_request_state_t *lib_rs;
    qdr_http1_connection_t   *hconn;  // parent connection
    char                     *response_addr; // request reply-to
    char                     *site;
    qd_timestamp_t            start;
    qd_timestamp_t            stop;
    uint64_t                  out_http1_octets;
    plog_record_t            *plog;
};
DEQ_DECLARE(qdr_http1_request_base_t, qdr_http1_request_list_t);


// A single HTTP adaptor connection.
//
struct qdr_http1_connection_t {
    DEQ_LINKS(qdr_http1_connection_t);
    qd_server_t           *qd_server;
    h1_codec_connection_t *http_conn;
    pn_raw_connection_t   *raw_conn;
    qdr_connection_t      *qdr_conn;
    qdr_http1_adaptor_t   *adaptor;

    uint64_t               conn_id;
    qd_handler_context_t   handler_context;
    h1_codec_connection_type_t     type;
    qd_conn_admin_status_t         admin_status;
    qd_conn_oper_status_t          oper_status;

    struct {
        char *host;
        char *port;
        char *address;
        char *site;
        char *host_port;
        bool event_channel;
        qd_http_aggregation_t aggregation;
        char *host_override;
    } cfg;

    // State if connected to an HTTP client
    //
    struct {
        qd_http_listener_t *listener;
        char               *client_ip_addr;
        char               *reply_to_addr;   // set once link is up
        uint64_t            next_msg_id;
    } client;

    // State if connected to an HTTP server
    struct {
        qd_http_connector_t *connector;
        qd_timer_t          *reconnect_timer;
        qd_timestamp_t       link_timeout;
        qd_duration_t        reconnect_pause;
    } server;

    // Outgoing link (router ==> HTTP app)
    //
    qdr_link_t            *out_link;
    uint64_t               out_link_id;
    int                    out_link_credit;  // provided by adaptor

    // Incoming link (HTTP app ==> router)
    //
    qdr_link_t            *in_link;
    uint64_t               in_link_id;
    int                    in_link_credit;  // provided by router
    sys_atomic_t           q2_restart;      // signal to resume receive
    bool                   q2_blocked;      // stop reading from raw conn

    // Oldest at HEAD
    //
    qdr_http1_request_list_t requests;

    // statistics and logging
    //
    plog_record_t *plog;
    uint64_t       in_http1_octets;
    uint64_t       out_http1_octets;

    // flags
    //
    bool trace;

    //
    //
    bool read_buf_busy;
    bool write_buf_busy;

    uint8_t read_buffer[HTTP1_IO_BUF_SIZE];
    uint8_t write_buffer[HTTP1_IO_BUF_SIZE];
};
ALLOC_DECLARE(qdr_http1_connection_t);

// special AMQP application properties keys for HTTP1 metadata headers
// ':' prefix is illegal for HTTP headers, ensures no collisions with
// HTTP headers
//
#define VERSION_PROP_KEY     ":version"       // of msg (string, e.g. "1.1"
#define PATH_PROP_KEY        ":path"          // URI target
#define REASON_PROP_KEY      ":reason"        // response reason phrase


// http1_adaptor.c
//
void qdr_http1_free_written_buffers(qdr_http1_connection_t *hconn);
void qdr_http1_enqueue_buffer_list(qdr_http1_out_data_list_t *fifo, qd_buffer_list_t *blist, uintmax_t octets);
void qdr_http1_enqueue_stream_data(qdr_http1_out_data_list_t *fifo, qd_message_stream_data_t *stream_data);
uint64_t qdr_http1_write_out_data(qdr_http1_connection_t *hconn, qdr_http1_out_data_list_t *fifo);
void qdr_http1_out_data_cleanup(qdr_http1_out_data_list_t *out_data);
int qdr_http1_grant_read_buffers(qdr_http1_connection_t *hconn);
uintmax_t qdr_http1_get_read_buffers(qdr_http1_connection_t *hconn,
                                     qd_buffer_list_t *blist);

void qdr_http1_close_connection(qdr_http1_connection_t *hconn, const char *error);
void qdr_http1_connection_free(qdr_http1_connection_t *hconn);

void qdr_http1_request_base_cleanup(qdr_http1_request_base_t *hreq);
void qdr_http1_error_response(qdr_http1_request_base_t *hreq,
                              int error_code,
                              const char *reason);
void qdr_http1_rejected_response(qdr_http1_request_base_t *hreq,
                                 const qdr_error_t *error);
void qdr_http1_q2_unblocked_handler(const qd_alloc_safe_ptr_t context);

// http1_client.c protocol adaptor callbacks
//
void qdr_http1_client_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit);
uint64_t qdr_http1_client_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled);
void qdr_http1_client_core_second_attach(qdr_http1_adaptor_t    *adaptor,
                                         qdr_http1_connection_t *hconn,
                                         qdr_link_t             *link,
                                         qdr_terminus_t         *source,
                                         qdr_terminus_t         *target);
void qdr_http1_client_core_delivery_update(qdr_http1_adaptor_t      *adaptor,
                                           qdr_http1_connection_t   *hconn,
                                           qdr_http1_request_base_t *hreqb,
                                           qdr_delivery_t           *dlv,
                                           uint64_t                  disp,
                                           bool                      settled);
void qdr_http1_client_conn_cleanup(qdr_http1_connection_t *hconn);
void qdr_http1_client_core_conn_close(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn);

// http1_server.c protocol adaptor callbacks
//
void qdr_http1_server_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit);
uint64_t qdr_http1_server_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled);
void qdr_http1_server_core_delivery_update(qdr_http1_adaptor_t      *adaptor,
                                           qdr_http1_connection_t   *hconn,
                                           qdr_http1_request_base_t *hreq,
                                           qdr_delivery_t           *dlv,
                                           uint64_t                  disp,
                                           bool                      settled);
void qdr_http1_server_conn_cleanup(qdr_http1_connection_t *hconn);
void qdr_http1_server_core_conn_close(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn);

// recording of stats:
void qdr_http1_record_client_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_base_t *request);
void qdr_http1_record_server_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_base_t *request);

#endif // http1_private_H

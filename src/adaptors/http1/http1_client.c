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

#include "python_private.h"

#include "adaptors/adaptor_common.h"
#include "http1_private.h"

#include "qpid/dispatch/protocol_adaptor.h"

#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>

//
// This file contains code specific to HTTP client processing.  The raw
// connection is terminated at an HTTP client, not an HTTP server.
//

#define DEFAULT_CAPACITY 250

const char *CONTENT_LENGTH_KEY = "Content-Length";
const char *POST_METHOD = "POST";

//
// State for a single response message to be sent to the client via the raw connection. A response is considered
// completed when the encode_complete is true and the pending out_data is empty.
//
typedef struct _client_response_msg_t {
    DEQ_LINKS(struct _client_response_msg_t);

    qdr_delivery_t *dlv;  // from core via core_link_deliver

    // HTTP encoded message data pending write to connection.  This list will reference qd_buffer_t's and
    // qd_message_stream_data_t's that belong to the message owned by dlv. Do _not_ release dlv until after all entries
    // on out_data have been processed and released!
    qdr_http1_out_data_list_t out_data;

    bool headers_encoded;  // all headers completely encoded
    bool encode_complete;  // true when full response encoded into out_data

} _client_response_msg_t;
ALLOC_DECLARE(_client_response_msg_t);
ALLOC_DEFINE(_client_response_msg_t);
DEQ_DECLARE(_client_response_msg_t, _client_response_msg_list_t);

//
// State for an HTTP/1.x Request+Response exchange, client facing.
//
// An HTTP/1.x Request/Response is considered done when:
// - The codec has completed: both the entire request and corresponding response message(s) have been fully received
// and encoded/decoded by the HTTP codec.
// - All _client_response_msg_t's owned by the _client_request_t have been released
// - The AMQP message holding the encoded HTTP Request has been settled by the core (remote state)
//
// If an unrecoverable error occurs the raw connection will be immediately closed and the HTTP connection,
// qdr_connection_t, etc will be destroyed.
//
typedef struct _client_request_t {
    qdr_http1_request_base_t base;

    // The request arrives via the raw connection.  These fields are used to
    // build the message and deliver it into the core.
    //
    qd_message_t        *request_msg;      // holds inbound message as it is built
    qdr_delivery_t      *request_dlv;      // qdr_link_deliver()
    qd_composed_field_t *request_props;    // holds HTTP headers as they arrive
    uint64_t             request_dispo;    // set by core (core_update_delivery)
    uint32_t             version_major;
    uint32_t             version_minor;

    // A single request may result in more than one response (1xx Continue for
    // example).  These responses are written to the raw connection from HEAD
    // to TAIL.
    //
    _client_response_msg_list_t responses;

    // flags:
    bool codec_completed;    // encoder/decoder done
    bool close_on_complete;  // close the conn when this request is complete
    bool conn_close_hdr;     // add Connection: close to response msg
    bool expect_continue;    // Expect: 100-Continue header found
    bool discard_request;    // T: avoid delivering request to core
    bool request_settled;    // set by core (core_update_delivery)

} _client_request_t;
ALLOC_DECLARE(_client_request_t);
ALLOC_DEFINE(_client_request_t);


static void _client_tx_buffers_cb(h1_codec_request_state_t *lib_hrs, qd_buffer_list_t *blist, unsigned int len);
static void _client_tx_stream_data_cb(h1_codec_request_state_t *lib_hrs, qd_message_stream_data_t *stream_data);
static int _client_rx_request_cb(h1_codec_request_state_t *lib_rs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor);
static int _client_rx_response_cb(h1_codec_request_state_t *lib_rs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor);
static int _client_rx_header_cb(h1_codec_request_state_t *lib_rs, const char *key, const char *value);
static int _client_rx_headers_done_cb(h1_codec_request_state_t *lib_rs, bool has_body);
static int _client_rx_body_cb(h1_codec_request_state_t *lib_rs, qd_buffer_list_t *body, size_t len, bool more);
static void _client_rx_done_cb(h1_codec_request_state_t *lib_rs);
static void _client_request_complete_cb(h1_codec_request_state_t *lib_rs, bool cancelled);
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);
static void _client_response_msg_free(_client_request_t *req, _client_response_msg_t *rmsg);
static void _client_request_free(_client_request_t *req);
static void _deliver_request(qdr_http1_connection_t *hconn, _client_request_t *req);
static bool _find_token(const char *list, const char *value);
static void _generate_response_msg(_client_request_t *hreq, int code, const char *reason, const char *text_body);

////////////////////////////////////////////////////////
// HTTP/1.x Client Listener
////////////////////////////////////////////////////////

// Create a new client connection. Runs on the proactor listener thread.
//
static qdr_http1_connection_t *_create_client_connection(qd_http_listener_t *li)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();

    ZERO(hconn);
    hconn->type = HTTP1_CONN_CLIENT;
    hconn->admin_status = QD_CONN_ADMIN_ENABLED;
    hconn->oper_status = QD_CONN_OPER_DOWN;
    hconn->qd_server = li->server;
    hconn->adaptor = qdr_http1_adaptor;
    hconn->handler_context.handler = &_handle_connection_events;
    hconn->handler_context.context = hconn;
    sys_atomic_init(&hconn->q2_restart, 0);

    hconn->client.next_msg_id = 1;

    // configure the HTTP/1.x library

    h1_codec_config_t config = {0};
    config.type             = HTTP1_CONN_CLIENT;
    config.tx_buffers       = _client_tx_buffers_cb;
    config.tx_stream_data   = _client_tx_stream_data_cb;
    config.rx_request       = _client_rx_request_cb;
    config.rx_response      = _client_rx_response_cb;
    config.rx_header        = _client_rx_header_cb;
    config.rx_headers_done  = _client_rx_headers_done_cb;
    config.rx_body          = _client_rx_body_cb;
    config.rx_done          = _client_rx_done_cb;
    config.request_complete = _client_request_complete_cb;

    hconn->http_conn = h1_codec_connection(&config, hconn);
    assert(hconn->http_conn);

    hconn->cfg.host = qd_strdup(li->config->adaptor_config->host);
    hconn->cfg.port = qd_strdup(li->config->adaptor_config->port);
    hconn->cfg.address = qd_strdup(li->config->adaptor_config->address);
    hconn->cfg.site = li->config->adaptor_config->site_id ? qd_strdup(li->config->adaptor_config->site_id) : 0;
    hconn->cfg.event_channel = li->config->event_channel;
    hconn->cfg.aggregation = li->config->aggregation;

    hconn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(hconn->raw_conn, &hconn->handler_context);

    sys_mutex_lock(&qdr_http1_adaptor->lock);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->connections, hconn);
    sys_mutex_unlock(&qdr_http1_adaptor->lock);

    // we'll create a QDR connection and links once the raw connection activates
    return hconn;
}

// Handle the proactor listener accept event. This runs on the proactor listener thread.
//
static void _handle_listener_accept(qd_adaptor_listener_t *adaptor_listener, pn_listener_t *pn_listener, void *context)
{
    qd_http_listener_t     *li    = (qd_http_listener_t *) context;
    qdr_http1_connection_t *hconn = _create_client_connection(li);
    if (hconn) {
        // Note: the proactor may schedule the hconn on another thread
        // during this call!
        pn_listener_raw_accept(pn_listener, hconn->raw_conn);
    }
}

// Management Agent API - Create
//
// Note that this runs on the Management Agent thread, which may be running concurrently with the
// I/O and timer threads.
//
qd_http_listener_t *qd_http1_configure_listener(qd_http_listener_t *li, qd_dispatch_t *qd, qd_entity_t *entity)
{
    li->adaptor_listener = qd_adaptor_listener(qd, li->config->adaptor_config, qdr_http1_adaptor->log);

    li->vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_PROTOCOL,         "http1");
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_NAME,             li->config->adaptor_config->name);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, li->config->adaptor_config->host);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, li->config->adaptor_config->port);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      li->config->adaptor_config->address);

    sys_mutex_lock(&qdr_http1_adaptor->lock);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->listeners, li);  // holds li refcount
    sys_mutex_unlock(&qdr_http1_adaptor->lock);

    qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Configured HTTP_ADAPTOR listener on %s with backlog %d",
           li->config->adaptor_config->host_port, li->config->adaptor_config->backlog);
    // Note: the proactor may execute _handle_listener_accept on another thread during this call
    qd_adaptor_listener_listen(li->adaptor_listener, _handle_listener_accept, (void *) li);

    return li;
}

// Management Agent API - Delete
//
// Note that this runs on the Management Agent thread, which may be running concurrently with the
// I/O and timer threads.
//
void qd_http1_delete_listener(qd_dispatch_t *ignore, qd_http_listener_t *li)
{
    if (li) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Deleting HttpListener for %s, %s:%s", li->config->adaptor_config->address, li->config->adaptor_config->host, li->config->adaptor_config->port);

        qd_adaptor_listener_close(li->adaptor_listener);
        li->adaptor_listener = 0;

        sys_mutex_lock(&qdr_http1_adaptor->lock);
        DEQ_REMOVE(qdr_http1_adaptor->listeners, li);
        sys_mutex_unlock(&qdr_http1_adaptor->lock);

        qd_http_listener_decref(li);
    }
}


////////////////////////////////////////////////////////
// Raw Connector Events
////////////////////////////////////////////////////////


// Raw Connection Initialization
//
static void _setup_client_connection(qdr_http1_connection_t *hconn)
{
    hconn->client.client_ip_addr = qd_raw_conn_get_address(hconn->raw_conn);
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_INCOMING, //qd_direction_t   dir,
                                                      hconn->client.client_ip_addr,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "HTTP/1.x Adaptor",    //const char      *container,
                                                      0,     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      "",                  // peer router version,
                                                      false);              // streaming links

    hconn->conn_id = qd_server_allocate_connection_id(hconn->qd_server);
    hconn->qdr_conn = qdr_connection_opened(qdr_http1_adaptor->core,
                                            qdr_http1_adaptor->adaptor,
                                            true,  // incoming
                                            QDR_ROLE_NORMAL,
                                            1,     //cost
                                            hconn->conn_id,
                                            0,  // label
                                            0,  // remote container id
                                            false,  // strip annotations in
                                            false,  // strip annotations out
                                            DEFAULT_CAPACITY,
                                            0,      // policy_spec
                                            info,
                                            0,      // bind context
                                            0);     // bind token
    qdr_connection_set_context(hconn->qdr_conn, hconn);
    hconn->oper_status = QD_CONN_OPER_UP;

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP connection to client created", hconn->conn_id);

    // simulate a client subscription for reply-to
    qdr_terminus_t *dynamic_source = qdr_terminus(0);
    qdr_terminus_set_dynamic(dynamic_source);
    hconn->out_link = qdr_link_first_attach(hconn->qdr_conn,
                                            QD_OUTGOING,
                                            dynamic_source,   //qdr_terminus_t   *source,
                                            qdr_terminus(0),  //qdr_terminus_t   *target,
                                            "http1.client.reply-to", //const char       *name,
                                            0,                  //const char       *terminus_addr,
                                            false,              // no-route
                                            NULL,               // initial delivery
                                            &(hconn->out_link_id));
    qdr_link_set_context(hconn->out_link, hconn);

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] HTTP client response link created",
           hconn->conn_id, hconn->out_link_id);

    // simulate a client publisher link to the HTTP server:
    qdr_terminus_t *target = qdr_terminus(0);
    if (hconn->cfg.event_channel) {
        //For an event channel, we always want to be able to handle
        //incoming requests. We use an anonymous publisher so that we
        //get credit regardless of there being consumers.
        qdr_terminus_set_address(target, NULL);
    } else {
        qdr_terminus_set_address(target, hconn->cfg.address);
    }
    hconn->in_link = qdr_link_first_attach(hconn->qdr_conn,
                                           QD_INCOMING,
                                           qdr_terminus(0),  //qdr_terminus_t   *source,
                                           target,           //qdr_terminus_t   *target,
                                           "http1.client.in", //const char       *name,
                                           0,                //const char       *terminus_addr,
                                           false,
                                           NULL,
                                           &(hconn->in_link_id));
    qdr_link_set_context(hconn->in_link, hconn);

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] HTTP client request link created",
           hconn->conn_id, hconn->in_link_id);

    // wait until the dynamic reply-to address is returned in the second attach
    // to grant buffers to the raw connection
}

// handle PN_RAW_CONNECTION_NEED_READ_BUFFERS
static void _replenish_empty_read_buffers(qdr_http1_connection_t *hconn)
{
    assert(hconn->raw_conn);

    if (!hconn->q2_blocked && (hconn->client.reply_to_addr || hconn->cfg.event_channel)) {
        int granted = qd_raw_connection_grant_read_buffers(hconn->raw_conn);
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] %d read buffers granted",
               hconn->conn_id, granted);
    }
}

uint64_t _get_output_data(void *context, qd_adaptor_buffer_list_t *abufs, size_t limit)
{
    uint64_t                total_octets = 0;
    qdr_http1_connection_t *hconn        = (qdr_http1_connection_t *) context;

    assert(hconn);
    assert(DEQ_IS_EMPTY(*abufs));

    _client_request_t *hreq = (_client_request_t *) DEQ_HEAD(hconn->requests);
    if (hreq) {
        _client_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
        while (rmsg && limit > DEQ_SIZE(*abufs)) {
            total_octets += qdr_http1_get_out_buffers(&rmsg->out_data, abufs, limit - DEQ_SIZE(*abufs));
            if (rmsg->encode_complete && DEQ_IS_EMPTY(rmsg->out_data)) {
                _client_response_msg_free(hreq, rmsg);
                rmsg = DEQ_HEAD(hreq->responses);
            } else {
                // full response message not yet fully arrived, wait for more
                rmsg = 0;
            }
        }
    }

    if (total_octets) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%" PRIu64 "] queueing %" PRIu64 " encoded octets for raw conn output (%zu buffers)", hconn->conn_id,
               total_octets, DEQ_SIZE(*abufs));
        hconn->out_http1_octets += total_octets;
    }
    return total_octets;
}

// Unencrypted raw connection I/O work loop
static int _do_raw_io(qdr_http1_connection_t *hconn)
{
    int error = 0;
    if (hconn->raw_conn) {
        bool work = false;
        do {
            size_t                   octets    = 0;
            qd_adaptor_buffer_list_t abuf_list = DEQ_EMPTY;
            work = qdr_http1_do_raw_io(hconn->raw_conn, _get_output_data, hconn, &abuf_list, &octets);
            if (DEQ_SIZE(abuf_list)) {
                qd_buffer_list_t qbuf_list = DEQ_EMPTY;
                qd_adaptor_buffers_copy_to_qd_buffers(&abuf_list, &qbuf_list);

                qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                       "[C%" PRIu64 "] pushing %zu received octets into codec (%zu buffers)", hconn->conn_id, octets,
                       DEQ_SIZE(qbuf_list));

                error = h1_codec_connection_rx_data(hconn->http_conn, &qbuf_list, octets);
                if (error) {
                    qdr_http1_close_connection(hconn, "Incoming response message failed to parse");
                }
            }
        } while (work && !error);
    }

    return error;
}

// Proton Connection Event Handler
//
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log_source_t *log = qdr_http1_adaptor->log;

    if (!hconn) return;

    qd_log(log, QD_LOG_TRACE, "[C%" PRIu64 "] client proactor event %s", hconn->conn_id,
           pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        _setup_client_connection(hconn);

        char *conn_addr = qd_raw_conn_get_address(hconn->raw_conn);
        if (conn_addr) {
            qd_log(log, QD_LOG_INFO, "[C%" PRIu64 "] HTTP/1.x client connection established from %s", hconn->conn_id,
                   conn_addr);
            free(conn_addr);
        }
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ: {
        hconn->q2_blocked = false;
        // TBD: proper client half-close support
        pn_raw_connection_close(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        pn_raw_connection_close(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qd_log(log, QD_LOG_INFO, "[C%"PRIu64"] HTTP/1.x client disconnected", hconn->conn_id);
        pn_raw_connection_set_context(hconn->raw_conn, 0);

        // prevent core from waking this connection
        sys_mutex_lock(&qdr_http1_adaptor->lock);
        qdr_connection_set_context(hconn->qdr_conn, 0);
        hconn->raw_conn = 0;
        sys_mutex_unlock(&qdr_http1_adaptor->lock);
        // at this point the core can no longer activate this connection

        hconn->oper_status = QD_CONN_OPER_DOWN;
        if (hconn->out_link) {
            qdr_link_set_context(hconn->out_link, 0);
            qdr_link_detach(hconn->out_link, QD_LOST, 0);
            hconn->out_link = 0;
        }
        if (hconn->in_link) {
            qdr_link_set_context(hconn->in_link, 0);
            qdr_link_detach(hconn->in_link, QD_LOST, 0);
            hconn->in_link = 0;
        }
        if (hconn->qdr_conn) {
            qdr_connection_set_context(hconn->qdr_conn, 0);
            qdr_connection_closed(hconn->qdr_conn);
            hconn->qdr_conn = 0;
        }

        qdr_http1_connection_free(hconn);
        return;  // hconn no longer valid
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        // handled below in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        qdr_http1_free_written_buffers(hconn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] Need read buffers", hconn->conn_id);
        _replenish_empty_read_buffers(hconn);
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        // handled below in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_DRAIN_BUFFERS: {
        qdr_http1_free_written_buffers(hconn);
        // discard of empty read buffers is handled below in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        if (sys_atomic_set(&hconn->q2_restart, 0)) {
            // note: unit tests grep for this log!
            qd_log(log, QD_LOG_TRACE, "[C%"PRIu64"] client link unblocked from Q2 limit", hconn->conn_id);
            hconn->q2_blocked = false;
            _replenish_empty_read_buffers(hconn);  // restart receiver flow
        }

        while (qdr_connection_process(hconn->qdr_conn)) {}

        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] Processing done", hconn->conn_id);
        break;
    }
    default:
        break;
    }

    // now process all pending raw connection I/O work

    if (_do_raw_io(hconn) == 0) {  // I/O errors are fatal and will close the connection

        // Check the head request for completion and advance to next request if
        // done.

        _client_request_t *hreq = (_client_request_t *) DEQ_HEAD(hconn->requests);
        if (hreq) {
            qd_log(log, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] client request msg-id=%" PRIu64 " status: codec=%s pending-resp-ct=%zu flags=[%s%s]",
                   hconn->conn_id, hreq->base.msg_id, hreq->codec_completed ? "Done" : "Not Done",
                   DEQ_SIZE(hreq->responses), hreq->close_on_complete ? "Close on Complete:" : ":",
                   hreq->request_settled ? "Settled:" : ":");

            if (hreq->codec_completed && DEQ_IS_EMPTY(hreq->responses) && hreq->request_settled) {
                const bool need_close = hreq->close_on_complete;

                qd_log(log, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP request msg-id=%" PRIu64 " completed!", hconn->conn_id,
                       hreq->base.msg_id);

                _client_request_free(hreq);
                if (need_close)
                    qdr_http1_close_connection(hconn, 0);
                else {
                    hreq = (_client_request_t *) DEQ_HEAD(hconn->requests);
                    if (hreq && hreq->request_msg && hconn->in_link_credit > 0) {
                        qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                               "[C%" PRIu64 "][L%" PRIu64 "] Delivering next request msg-id=%" PRIu64 " to router",
                               hconn->conn_id, hconn->in_link_id, hreq->base.msg_id);

                        hconn->in_link_credit -= 1;
                        _deliver_request(hconn, hreq);
                    }
                }
            }
        }
    }
}




////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
////////////////////////////////////////////////////////


// Encoder callback: send blist buffers (response msg) to client endpoint
//
static void _client_tx_buffers_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *blist, unsigned int len)
{
    _client_request_t       *hreq = (_client_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    if (!hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"] Discarding outgoing data - client connection closed", hconn->conn_id);
        qd_buffer_list_free_buffers(blist);
        return;
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] %u response octets encoded",
           hconn->conn_id, hconn->out_link_id, len);


    _client_response_msg_t *rmsg;
    if (hconn->cfg.aggregation == QD_AGGREGATION_NONE) {
        // responses are decoded one at a time - the current response it at the
        // tail of the response list
        rmsg = DEQ_TAIL(hreq->responses);
    } else {
        // when responses are aggregated the buffers don't need to be
        // correlated to specific responses as they will all be
        // written out together, so can just use the head of the
        // response list
        rmsg = DEQ_HEAD(hreq->responses);
    }
    assert(rmsg);
    qdr_http1_enqueue_buffer_list(&rmsg->out_data, blist, len);
}


// Encoder callback: send stream_data buffers (response msg) to client endpoint
//
static void _client_tx_stream_data_cb(h1_codec_request_state_t *hrs, qd_message_stream_data_t *stream_data)
{
    _client_request_t       *hreq = (_client_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    if (!hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"] Discarding outgoing data - client connection closed", hconn->conn_id);
        qd_message_stream_data_release(stream_data);
        return;
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] Sending body data to client",
           hconn->conn_id, hconn->out_link_id);


    _client_response_msg_t *rmsg;
    if (hconn->cfg.aggregation == QD_AGGREGATION_NONE) {
        // responses are decoded one at a time - the current response it at the
        // tail of the response list
        rmsg = DEQ_TAIL(hreq->responses);
    } else {
        // when responses are aggregated the buffers don't need to be
        // correlated to specific responses as they will all be
        // written out together, so can just use the head of the
        // response list
        rmsg = DEQ_HEAD(hreq->responses);
    }
    assert(rmsg);
    qdr_http1_enqueue_stream_data(&rmsg->out_data, stream_data);
}


// Called when decoding an HTTP request from a client.  This indicates the
// start of a new request message.
//
static int _client_rx_request_cb(h1_codec_request_state_t *hrs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor)
{
    h1_codec_connection_t    *h1c = h1_codec_request_state_get_connection(hrs);
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*)h1_codec_connection_get_context(h1c);

    _client_request_t *hreq = new__client_request_t();
    ZERO(hreq);
    hreq->base.start        = qd_timer_now();
    hreq->base.msg_id       = hconn->client.next_msg_id++;
    hreq->base.lib_rs       = hrs;
    hreq->base.hconn        = hconn;
    hreq->close_on_complete = (version_minor == 0);
    hreq->version_major     = version_major;
    hreq->version_minor     = version_minor;
    DEQ_INIT(hreq->responses);
    h1_codec_request_state_set_context(hrs, (void *) hreq);
    DEQ_INSERT_TAIL(hconn->requests, &hreq->base);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%" PRIu64 "] HTTP request received: msg-id=%" PRIu64 " method=%s target=%s version=%" PRIi32 ".%" PRIi32,
           hconn->conn_id, hreq->base.msg_id, method, target, version_major, version_minor);

    hreq->request_props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(hreq->request_props);
    {
        // OASIS specifies this value as "1.1" by default...
        char temp[64];
        snprintf(temp, sizeof(temp), "%"PRIi32".%"PRIi32, version_major, version_minor);
        qd_compose_insert_string(hreq->request_props, VERSION_PROP_KEY);
        qd_compose_insert_string(hreq->request_props, temp);

        qd_compose_insert_string(hreq->request_props, PATH_PROP_KEY);
        qd_compose_insert_string(hreq->request_props, target);
    }

    if (hconn->cfg.event_channel) {
        if (strcasecmp(method, POST_METHOD) != 0) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%" PRIu64 "] Event channel only supports POST request: %s not allowed", hconn->conn_id, method);
            hreq->discard_request   = true;
            hreq->close_on_complete = true;
            _generate_response_msg(hreq, 405, "Method Not Allowed",
                                   "Invalid method for event channel httpListener, only POST is allowed.");
        }
    }

    return 0;
}


// Cannot happen for a client connection!
static int _client_rx_response_cb(h1_codec_request_state_t *hrs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor)
{
    _client_request_t       *hreq = (_client_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
           "[C%"PRIu64"][L%"PRIu64"] Spurious HTTP response received from client",
           hconn->conn_id, hconn->in_link_id);
    return HTTP1_STATUS_BAD_REQ;
}


// called for each decoded HTTP header.
//
static int _client_rx_header_cb(h1_codec_request_state_t *hrs, const char *key, const char *value)
{
    _client_request_t       *hreq = (_client_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request header received: key='%s' value='%s'",
           hconn->conn_id, hconn->in_link_id, key, value);

    if (strcasecmp(key, "Connection") == 0) {
        // We need to filter the connection header out.  But first see if
        // client requested that the connection be closed after the response
        // arrives.
        //
        // @TODO(kgiusti): also have to remove other headers given in value!
        // @TODO(kgiusti): do we need to support keep-alive on 1.0 connections?
        //
        if (_find_token(value, "close")) {
            hreq->close_on_complete = true;
            hreq->conn_close_hdr = true;
        }

    } else {
        if (strcasecmp(key, "Expect") == 0) {
            // DISPATCH-2189: mark this message as streaming so the router will
            // not wait for it to complete before forwarding it.
            hreq->expect_continue = _find_token(value, "100-continue");
        }
        qd_compose_insert_symbol(hreq->request_props, key);
        qd_compose_insert_string(hreq->request_props, value);
    }

    return 0;
}


// Called after the last header is decoded, before decoding any body data.
// At this point there is enough data to start forwarding the message to
// the router.
//
static int _client_rx_headers_done_cb(h1_codec_request_state_t *hrs, bool has_body)
{
    _client_request_t *hreq = (_client_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request headers done.",
           hconn->conn_id, hconn->in_link_id);

    if (hreq->discard_request) {
        return 0;
    }

    // now that all the headers have been received we can construct
    // the AMQP message

    qd_composed_field_t *hdrs = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(hdrs);
    qd_compose_insert_bool(hdrs, 0);     // durable
    qd_compose_insert_null(hdrs);        // priority
    //qd_compose_insert_null(hdrs);        // ttl
    //qd_compose_insert_bool(hdrs, 0);     // first-acquirer
    //qd_compose_insert_uint(hdrs, 0);     // delivery-count
    qd_compose_end_list(hdrs);

    qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, hdrs);
    qd_compose_start_list(props);

    qd_compose_insert_ulong(props, hreq->base.msg_id);    // message-id
    qd_compose_insert_null(props);                 // user-id

    qd_compose_insert_string(props, hconn->cfg.address); // to
    qd_compose_insert_string(props, h1_codec_request_state_method(hrs));  // subject
    if (hconn->cfg.event_channel) {
        // event channel does not want replies
        qd_compose_insert_null(props);                                  // reply-to
    } else {
        qd_compose_insert_string(props, hconn->client.reply_to_addr);   // reply-to
    }
    qd_compose_insert_null(props);                      // correlation-id
    qd_compose_insert_null(props);                      // content-type
    qd_compose_insert_null(props);                      // content-encoding
    qd_compose_insert_null(props);                      // absolute-expiry-time
    qd_compose_insert_null(props);                      // creation-time
    qd_compose_insert_string(props, hconn->cfg.site);   // group-id

    qd_compose_end_list(props);

    qd_compose_end_map(hreq->request_props);

    hreq->request_msg = qd_message_compose(props, hreq->request_props, 0, !has_body);
    hreq->request_props = 0;
    qd_message_set_streaming_annotation(hreq->request_msg);

    // future-proof: ensure the message headers have not caused Q2
    // blocking.  We only check for Q2 events while adding body data.
    assert(!qd_message_is_Q2_blocked(hreq->request_msg));

    qd_alloc_safe_ptr_t hconn_sp = QD_SAFE_PTR_INIT(hconn);
    qd_message_set_q2_unblocked_handler(hreq->request_msg, qdr_http1_q2_unblocked_handler, hconn_sp);

    // Use up one credit to obtain a delivery and forward to core.  If no
    // credit is available the request is stalled until the core grants more
    // flow.
    if (hreq == (_client_request_t*) DEQ_HEAD(hconn->requests) && hconn->in_link_credit > 0) {
        hconn->in_link_credit -= 1;

        qd_log(hconn->adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"][L%"PRIu64"] Delivering request msg-id=%"PRIu64" to router",
               hconn->conn_id, hconn->in_link_id, hreq->base.msg_id);

        _deliver_request(hconn, hreq);
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int _client_rx_body_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *body, size_t len,
                              bool more)
{
    bool                    q2_blocked = false;
    _client_request_t      *hreq       = (_client_request_t *) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn      = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request body received len=%zu.",
           hconn->conn_id, hconn->in_link_id, len);

    if (hreq->discard_request) {
        qd_buffer_list_free_buffers(body);
        return 0;
    }

    qd_message_t *msg = hreq->request_msg ? hreq->request_msg : qdr_delivery_message(hreq->request_dlv);
    qd_message_stream_data_append(msg, body, &q2_blocked);
    if (q2_blocked && !hconn->q2_blocked) {
        // note: unit tests grep for this log!
        hconn->q2_blocked = true;
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] client link blocked on Q2 limit", hconn->conn_id);
    }

    //
    // Notify the router that more data is ready to be pushed out on the delivery
    //
    if (!more)
        qd_message_set_receive_complete(msg);

    if (hreq->request_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, hreq->request_dlv, false);

    return 0;
}


// Called at the completion of request message decoding.
//
static void _client_rx_done_cb(h1_codec_request_state_t *hrs)
{
    _client_request_t      *hreq  = (_client_request_t *) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%" PRIu64 "][L%" PRIu64 "] client msg-id=%" PRIu64 " request msg encode complete.", hconn->conn_id,
           hconn->in_link_id, hreq->base.msg_id);

    if (!hreq->discard_request) {
        qd_message_t *msg = hreq->request_msg ? hreq->request_msg : qdr_delivery_message(hreq->request_dlv);
        if (!qd_message_receive_complete(msg)) {
            qd_message_set_receive_complete(msg);
            if (hreq->request_dlv) {
                qdr_delivery_continue(qdr_http1_adaptor->core, hreq->request_dlv, false);
            }
        }
    } else {
        // since there is no delivery "fake" that it has been settled by the remote:
        hreq->request_dispo   = PN_ACCEPTED;
        hreq->request_settled = true;
    }
}


// The codec has completed processing the request and response messages.
//
static void _client_request_complete_cb(h1_codec_request_state_t *lib_rs, bool cancelled)
{
    _client_request_t *hreq = (_client_request_t*) h1_codec_request_state_get_context(lib_rs);
    if (hreq) {
        hreq->base.stop = qd_timer_now();
        qdr_http1_record_client_request_info(qdr_http1_adaptor, &hreq->base);
        hreq->base.lib_rs = 0;  // freed on return from this call
        hreq->codec_completed = true;

        uint64_t in_octets, out_octets;
        h1_codec_request_state_counters(lib_rs, &in_octets, &out_octets);
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%" PRIu64 "] client request msg-id=%" PRIu64 " codec complete: Octets read: %" PRIu64
               " written: %" PRIu64,
               hreq->base.hconn->conn_id, hreq->base.msg_id, in_octets, out_octets);
    }
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


void qdr_http1_client_core_second_attach(qdr_http1_adaptor_t    *adaptor,
                                         qdr_http1_connection_t *hconn,
                                         qdr_link_t             *link,
                                         qdr_terminus_t         *source,
                                         qdr_terminus_t         *target)
{
    if (link == hconn->out_link) {
        // this is the reply-to link for the client
        qd_iterator_t *reply_iter = qdr_terminus_get_address(source);
        hconn->client.reply_to_addr = (char*) qd_iterator_copy(reply_iter);

        assert(hconn->client.reply_to_addr);

        hconn->out_link_credit += DEFAULT_CAPACITY;
        qdr_link_flow(adaptor->core, link, DEFAULT_CAPACITY, false);
    }
}


void qdr_http1_client_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit)
{
    qd_log(adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] Credit granted on request link %d",
           hconn->conn_id, hconn->in_link_id, credit);

    assert(link == hconn->in_link);   // router only grants flow on incoming link

    hconn->in_link_credit += credit;
    if (hconn->in_link_credit > 0) {
        if (hconn->raw_conn)
            _replenish_empty_read_buffers(hconn);

        // is the current request message blocked by lack of credit?

        _client_request_t *hreq = (_client_request_t *)DEQ_HEAD(hconn->requests);
        if (hreq && hreq->request_msg) {
            assert(!hreq->request_dlv);
            hconn->in_link_credit -= 1;

            qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Delivering next request msg-id=%"PRIu64" to router",
                   hconn->conn_id, hconn->in_link_id, hreq->base.msg_id);

            _deliver_request(hconn, hreq);
        }
    }
}

static bool _get_multipart_content_length(_client_request_t *hreq, char *value)
{
    uint64_t total = 0;
    for (_client_response_msg_t *rmsg = DEQ_HEAD(hreq->responses); rmsg; rmsg = rmsg->next) {
        qd_message_t *msg = qdr_delivery_message(rmsg->dlv);
        uint64_t content_length = h1_codec_tx_multipart_section_boundary_length();
        bool got_body_length = false;

        qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
        if (app_props_iter) {
            qd_parsed_field_t *app_props = qd_parse(app_props_iter);
            if (app_props && qd_parse_is_map(app_props)) {
                // now send all headers in app properties
                qd_parsed_field_t *key = qd_field_first_child(app_props);
                while (key) {
                    qd_parsed_field_t *value = qd_field_next_child(key);
                    if (!value)
                        break;

                    qd_iterator_t *i_key = qd_parse_raw(key);
                    if (!i_key)
                        break;

                    if (qd_iterator_equal(i_key, (const unsigned char*) CONTENT_LENGTH_KEY)) {
                        qd_iterator_t *i_value = qd_parse_raw(value);
                        if (i_value) {
                            char *length_str = (char*) qd_iterator_copy(i_value);
                            uint64_t body_length;
                            sscanf(length_str, "%"SCNu64, &body_length);
                            free(length_str);
                            got_body_length = true;
                            content_length += body_length;
                        }
                    } else if (!qd_iterator_prefix(i_key, ":")) {
                        // ignore meta-data headers
                        qd_iterator_t *i_value = qd_parse_raw(value);
                        if (!i_value)
                            break;

                        content_length += qd_iterator_length(i_key) + 2 + qd_iterator_length(i_value) + 2;
                    }

                    key = qd_field_next_child(value);
                }
            }
            qd_parse_free(app_props);
        }
        qd_iterator_free(app_props_iter);
        if (got_body_length) {
            total += content_length;
        } else {
            return false;
        }
    }
    total += h1_codec_tx_multipart_end_boundary_length();
    sprintf(value, "%"SCNu64, total);
    return true;
}

static void _encode_json_response(_client_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    qd_log(hconn->adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] encoding json response", hconn->conn_id);
    bool ok = !h1_codec_tx_response(hreq->base.lib_rs, 200, NULL, hreq->version_major, hreq->version_minor);
    if (!ok) {
        qd_log(hconn->adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] Could not encode response", hconn->conn_id);
        return;
    }
    PyObject* msgs = 0;
    qd_json_msgs_init(&msgs);
    for (_client_response_msg_t *rmsg = DEQ_HEAD(hreq->responses); rmsg; rmsg = rmsg->next) {
        qd_message_t *msg = qdr_delivery_message(rmsg->dlv);
        qd_json_msgs_append(msgs, msg);
        rmsg->encode_complete = true;
    }
    char *body = qd_json_msgs_string(msgs);
    if (body) {
        h1_codec_tx_add_header(hreq->base.lib_rs, "Content-Type", "application/json");
        int len = strlen(body);
        char content_length[25];
        sprintf(content_length, "%i", len);
        h1_codec_tx_add_header(hreq->base.lib_rs, CONTENT_LENGTH_KEY, content_length);
        h1_codec_tx_body_str(hreq->base.lib_rs, body);
        free(body);
    } else {
        qd_log(hconn->adaptor->log, QD_LOG_ERROR, "[C%"PRIu64"] No aggregated json response returned", hconn->conn_id);
    }
    bool need_close;
    h1_codec_tx_done(hreq->base.lib_rs, &need_close);
    hreq->close_on_complete = need_close || hreq->close_on_complete;
}

static void _encode_multipart_response(_client_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    qd_log(hconn->adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] encoding multipart response", hconn->conn_id);
    bool ok = !h1_codec_tx_response(hreq->base.lib_rs, 200, NULL, hreq->version_major, hreq->version_minor);
    char content_length[25];
    if (_get_multipart_content_length(hreq, content_length)) {
        h1_codec_tx_add_header(hreq->base.lib_rs, CONTENT_LENGTH_KEY, content_length);
    }
    h1_codec_tx_begin_multipart(hreq->base.lib_rs);
    for (_client_response_msg_t *rmsg = DEQ_HEAD(hreq->responses); rmsg; rmsg = rmsg->next) {
        h1_codec_tx_begin_multipart_section(hreq->base.lib_rs);
        qd_message_t *msg = qdr_delivery_message(rmsg->dlv);

        qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
        if (app_props_iter) {
            qd_parsed_field_t *app_props = qd_parse(app_props_iter);
            if (app_props && qd_parse_is_map(app_props)) {
                // now send all headers in app properties
                qd_parsed_field_t *key = qd_field_first_child(app_props);
                while (ok && key) {
                    qd_parsed_field_t *value = qd_field_next_child(key);
                    if (!value)
                        break;

                    qd_iterator_t *i_key = qd_parse_raw(key);
                    if (!i_key)
                        break;

                    // ignore the special headers added by the mapping and
                    // content-length field (TODO: case insensitive comparison
                    // for content-length)
                    if (!qd_iterator_prefix(i_key, ":")
                        && !qd_iterator_equal(i_key, (const unsigned char*) CONTENT_LENGTH_KEY)) {

                        qd_iterator_t *i_value = qd_parse_raw(value);
                        if (!i_value)
                            break;

                        char *header_key = (char*) qd_iterator_copy(i_key);
                        char *header_value = (char*) qd_iterator_copy(i_value);
                        ok = !h1_codec_tx_add_header(hreq->base.lib_rs, header_key, header_value);

                        free(header_key);
                        free(header_value);
                    }

                    key = qd_field_next_child(value);
                }
            }
            qd_parse_free(app_props);
        }
        qd_iterator_free(app_props_iter);
        rmsg->headers_encoded = true;

        qd_message_stream_data_t *body_data = 0;
        bool done = false;
        while (ok && !done) {
            switch (qd_message_next_stream_data(msg, &body_data)) {

            case QD_MESSAGE_STREAM_DATA_BODY_OK:

                qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                       "[C%"PRIu64"][L%"PRIu64"] Encoding response body data",
                       hconn->conn_id, hconn->out_link_id);

                if (h1_codec_tx_body(hreq->base.lib_rs, body_data)) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                           "[C%"PRIu64"][L%"PRIu64"] body data encode failed",
                           hconn->conn_id, hconn->out_link_id);
                    ok = false;
                }
                break;

            case QD_MESSAGE_STREAM_DATA_NO_MORE:
                // indicate this message is complete
                qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                       "[C%"PRIu64"][L%"PRIu64"] response message encoding completed",
                       hconn->conn_id, hconn->out_link_id);
                done = true;
                break;

            case QD_MESSAGE_STREAM_DATA_INCOMPLETE:
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] Ignoring incomplete body data in aggregated response.",
                       hconn->conn_id, hconn->out_link_id);
                done = true;
                break;  // wait for more

            case QD_MESSAGE_STREAM_DATA_INVALID:
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] Ignoring corrupted body data in aggregated response.",
                       hconn->conn_id, hconn->out_link_id);
                done = true;
                break;

            case QD_MESSAGE_STREAM_DATA_FOOTER_OK:
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] Ignoring footer in aggregated response.",
                       hconn->conn_id, hconn->out_link_id);
                done = true;
                break;
            }
        }
        rmsg->encode_complete = true;
    }
    h1_codec_tx_end_multipart(hreq->base.lib_rs);
    bool need_close;
    h1_codec_tx_done(hreq->base.lib_rs, &need_close);
    hreq->close_on_complete = need_close || hreq->close_on_complete;
}

static void _encode_aggregated_response(qdr_http1_connection_t *hconn, _client_request_t *hreq)
{
    if (hconn->cfg.aggregation == QD_AGGREGATION_MULTIPART) {
        _encode_multipart_response(hreq);
    } else if (hconn->cfg.aggregation == QD_AGGREGATION_JSON) {
        _encode_json_response(hreq);
    }
}

static void _encode_empty_response(qdr_http1_connection_t *hconn, _client_request_t *hreq)
{
    qd_log(hconn->adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] encoding empty response", hconn->conn_id);
    h1_codec_tx_response(hreq->base.lib_rs, 204, NULL, hreq->version_major, hreq->version_minor);
    bool need_close;
    h1_codec_tx_done(hreq->base.lib_rs, &need_close);
    hreq->close_on_complete = need_close || hreq->close_on_complete;
}

// Handle disposition/settlement update for the outstanding request msg
//
void qdr_http1_client_core_delivery_update(qdr_http1_adaptor_t      *adaptor,
                                           qdr_http1_connection_t   *hconn,
                                           qdr_http1_request_base_t *req,
                                           qdr_delivery_t           *dlv,
                                           uint64_t                  disp,
                                           bool                      settled)
{
    _client_request_t *hreq = (_client_request_t *)req;
    assert(dlv == hreq->request_dlv);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           DLV_FMT " HTTP request msg-id=%" PRIu64 " delivery update, outcome=0x%" PRIx64 "%s", DLV_ARGS(dlv),
           hreq->base.msg_id, disp, settled ? " settled" : "");

    if (disp && disp != PN_RECEIVED && hreq->request_dispo == 0) {
        // terminal disposition
        hreq->request_dispo = disp;
        if (hconn->cfg.event_channel) {
            // For event channel the server never sends back a response. Generate one on its behalf:
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, DLV_FMT " Generating event channel POST response message",
                   DLV_ARGS(dlv));
            _generate_response_msg(hreq, 204, "No Content", 0);

        } else if (hconn->cfg.aggregation != QD_AGGREGATION_NONE) {
            // when aggregating response from a multicast request, the
            // acknowledgement of the request triggers generating the
            // output from the responses received
            if (settled) {
                if (DEQ_IS_EMPTY(hreq->responses)) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                           "[C%"PRIu64"][L%"PRIu64"] Aggregation request settled but no responses received.", hconn->conn_id, hconn->in_link_id);
                    _encode_empty_response(hconn, hreq);
                } else {
                    _encode_aggregated_response(hconn, hreq);
                }
            }

        } else if (disp != PN_ACCEPTED) {
            // no response message is going to arrive.  Now what?  For now fake
            // a response from the server by using the codec to write an error
            // response on the behalf of the server.
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] HTTP request msg-id=%"PRIu64" failure, outcome=0x%"PRIx64,
                   hconn->conn_id, hconn->in_link_id, hreq->base.msg_id, disp);

            if (DEQ_IS_EMPTY(hreq->responses) && hreq->base.out_http1_octets == 0) {
                // best effort attempt to send an error to the client if nothing has been sent back so far. This is a
                // total guess as to what the proper error code should be
                _generate_response_msg(hreq, 503, "Service Unavailable", 0);
                hreq->close_on_complete = true;  // trust nothing

            } else {
                // partial response already sent - punt:
                qdr_http1_close_connection(hconn, "HTTP request failed");
            }
        }
    }

    hreq->request_settled = settled || hreq->request_settled;
}


//
// Response message forwarding
//


// use the correlation ID from the AMQP message containing the response to look
// up the original request context
//
static _client_request_t *_lookup_request_context(qdr_http1_connection_t *hconn,
                                                  qd_message_t *msg)
{
    qdr_http1_request_base_t *req = 0;

    qd_parsed_field_t *cid_pf = 0;
    qd_iterator_t *cid_iter = qd_message_field_iterator_typed(msg, QD_FIELD_CORRELATION_ID);
    if (cid_iter) {
        cid_pf = qd_parse(cid_iter);
        if (cid_pf && qd_parse_ok(cid_pf)) {
            uint64_t cid = qd_parse_as_ulong(cid_pf);
            if (qd_parse_ok(cid_pf)) {
                req = DEQ_HEAD(hconn->requests);
                while (req) {
                    if (req->msg_id == cid)
                        break;
                    req = DEQ_NEXT(req);
                }
            }
        }
    }

    qd_parse_free(cid_pf);
    qd_iterator_free(cid_iter);

    return (_client_request_t*) req;
}


// Encode the response status and all HTTP headers.
// The message has been validated to app properties depth
//
static bool _encode_response_headers(_client_request_t *hreq,
                                     _client_response_msg_t *rmsg)
{
    bool ok = false;
    qd_message_t *msg = qdr_delivery_message(rmsg->dlv);

    if (!hreq->base.site) {
        qd_iterator_t *group_id_itr = qd_message_field_iterator(msg, QD_FIELD_GROUP_ID);
        hreq->base.site = (char*) qd_iterator_copy(group_id_itr);
        qd_iterator_free(group_id_itr);
    }

    uint32_t status_code = 0;
    qd_iterator_t *subject_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
    if (subject_iter) {
        char *status_str = (char*) qd_iterator_copy(subject_iter);
        if (status_str && sscanf(status_str, "%"PRIu32, &status_code) == 1) {

            qd_iterator_t *app_props_iter = qd_message_field_iterator(msg,
                                                                      QD_FIELD_APPLICATION_PROPERTIES);
            if (app_props_iter) {
                qd_parsed_field_t *app_props = qd_parse(app_props_iter);
                if (app_props && qd_parse_is_map(app_props)) {

                    // the value for RESPONSE_HEADER_KEY is optional and is set
                    // to a string representation of the version of the server
                    // (e.g. "1.1")
                    uint32_t major = 1;
                    uint32_t minor = 1;
                    qd_parsed_field_t *tmp = qd_parse_value_by_key(app_props, VERSION_PROP_KEY);
                    if (tmp) {
                        char *version_str = (char*) qd_iterator_copy(qd_parse_raw(tmp));
                        if (version_str) {
                            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
                            free(version_str);
                        }
                    }
                    char *reason_str = 0;
                    tmp = qd_parse_value_by_key(app_props, REASON_PROP_KEY);
                    if (tmp) {
                        reason_str = (char*) qd_iterator_copy(qd_parse_raw(tmp));
                    }

                    qd_log(hreq->base.hconn->adaptor->log, QD_LOG_TRACE,
                           "[C%"PRIu64"][L%"PRIu64"] Encoding response %d %s",
                           hreq->base.hconn->conn_id, hreq->base.hconn->out_link_id, (int)status_code,
                           reason_str ? reason_str : "");

                    ok = !h1_codec_tx_response(hreq->base.lib_rs, (int)status_code, reason_str, major, minor);
                    free(reason_str);

                    // now send all headers in app properties
                    qd_parsed_field_t *key = qd_field_first_child(app_props);
                    while (ok && key) {
                        qd_parsed_field_t *value = qd_field_next_child(key);
                        if (!value)
                            break;

                        qd_iterator_t *i_key = qd_parse_raw(key);
                        if (!i_key)
                            break;

                        // ignore the special headers added by the mapping
                        if (!qd_iterator_prefix(i_key, ":")) {

                            qd_iterator_t *i_value = qd_parse_raw(value);
                            if (!i_value)
                                break;

                            char *header_key = (char*) qd_iterator_copy(i_key);
                            char *header_value = (char*) qd_iterator_copy(i_value);

                            // @TODO(kgiusti): remove me (sensitive content)
                            qd_log(hreq->base.hconn->adaptor->log, QD_LOG_TRACE,
                                   "[C%"PRIu64"][L%"PRIu64"] Encoding response header %s:%s",
                                   hreq->base.hconn->conn_id, hreq->base.hconn->out_link_id,
                                   header_key, header_value);

                            ok = !h1_codec_tx_add_header(hreq->base.lib_rs, header_key, header_value);

                            free(header_key);
                            free(header_value);
                        }

                        key = qd_field_next_child(value);
                    }

                    // If the client has requested Connection: close respond
                    // accordingly IF this is the terminal response (not
                    // INFORMATIONAL)
                    if (ok && (status_code / 100) == 1) {
                        if (hreq->conn_close_hdr) {
                            ok = !h1_codec_tx_add_header(hreq->base.lib_rs,
                                                         "Connection", "close");
                        }
                    }
                }

                qd_parse_free(app_props);
                qd_iterator_free(app_props_iter);
            }
        }
        free(status_str);
        qd_iterator_free(subject_iter);
    }
    return ok;
}

// Encode an response. Returns a terminal outcome when encode is complete, else 0
//
static uint64_t _encode_response_message(_client_request_t *hreq,
                                         _client_response_msg_t *rmsg)
{
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    qd_message_t             *msg = qdr_delivery_message(rmsg->dlv);

    if (!rmsg->headers_encoded) {
        rmsg->headers_encoded = true;
        if (!_encode_response_headers(hreq, rmsg)) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] message headers malformed - discarding.",
                   hconn->conn_id, hconn->out_link_id);
            return PN_REJECTED;
        }
    }

    qd_message_stream_data_t *stream_data = 0;

    while (true) {
        switch (qd_message_next_stream_data(msg, &stream_data)) {

        case QD_MESSAGE_STREAM_DATA_BODY_OK:

            qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Encoding response body data",
                   hconn->conn_id, hconn->out_link_id);

            if (h1_codec_tx_body(hreq->base.lib_rs, stream_data)) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] body data encode failed",
                       hconn->conn_id, hconn->out_link_id);
                return PN_REJECTED;
            }
            break;

        case QD_MESSAGE_STREAM_DATA_FOOTER_OK:
            // ignore footers
            qd_message_stream_data_release(stream_data);
            break;

        case QD_MESSAGE_STREAM_DATA_NO_MORE:
            // indicate this message is complete
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                   "[C%"PRIu64"][L%"PRIu64"] response message encoding completed",
                   hconn->conn_id, hconn->out_link_id);
            return PN_ACCEPTED;

        case QD_MESSAGE_STREAM_DATA_INCOMPLETE:
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] body data need more",
                   hconn->conn_id, hconn->out_link_id);
            return 0;  // wait for more

        case QD_MESSAGE_STREAM_DATA_INVALID:
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Rejecting corrupted body data.",
                   hconn->conn_id, hconn->out_link_id);
            return PN_REJECTED;
        }
    }
}


// The I/O thread wants to send this delivery containing the response out the
// link.  It is unlikely that the parsing of this message will fail since the
// message was constructed by the ingress router.  However if the message fails
// to parse then there is probably no recovering as the client will now be out
// of sync.  For now close the connection if an error occurs.
//
uint64_t qdr_http1_client_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled)
{
    uint64_t           dispo = 0;
    qd_message_t      *msg   = qdr_delivery_message(delivery);
    _client_request_t *hreq  = (_client_request_t *) qdr_delivery_get_context(delivery);
    if (!hreq) {
        // new delivery - look for corresponding request via correlation_id
        switch (qd_message_check_depth(msg, QD_DEPTH_PROPERTIES)) {
        case QD_MESSAGE_DEPTH_INCOMPLETE:
            return 0;

        case QD_MESSAGE_DEPTH_INVALID:
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Malformed HTTP/1.x message",
                   hconn->conn_id, link->identity);
            qd_message_set_send_complete(msg);
            qdr_http1_close_connection(hconn, "Malformed response message");
            return PN_REJECTED;

        case QD_MESSAGE_DEPTH_OK:
            hreq = _lookup_request_context(hconn, msg);
            if (!hreq) {
                // No corresponding request found
                // @TODO(kgiusti) how to handle this?  - simply discard?
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] Discarding malformed message.", hconn->conn_id, link->identity);
                qd_message_set_send_complete(msg);
                qdr_http1_close_connection(hconn, "Cannot correlate response message");
                return PN_REJECTED;
            }

            // link request state and delivery
            _client_response_msg_t *rmsg = new__client_response_msg_t();
            ZERO(rmsg);
            rmsg->dlv = delivery;
            DEQ_INIT(rmsg->out_data);
            qdr_delivery_set_context(delivery, hreq);
            qdr_delivery_incref(delivery, "HTTP1 client referencing response delivery");
            DEQ_INSERT_TAIL(hreq->responses, rmsg);

            // workaround for Q2 stall, ISSUE #754/#758
            qd_message_Q2_holdoff_disable(msg);

            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] HTTP received response for msg-id=%"PRIu64,
                   hconn->conn_id, hconn->out_link_id, hreq->base.msg_id);
            break;
        }
    }

    // deliveries arrive one at a time and are added to the tail
    _client_response_msg_t *rmsg = DEQ_TAIL(hreq->responses);
    assert(rmsg && rmsg->dlv == delivery);

    if (hconn->cfg.aggregation == QD_AGGREGATION_NONE) {
        // for non-aggregated multicast we encode and sent the response message as it arrives
        if (!rmsg->encode_complete) {
            dispo = _encode_response_message(hreq, rmsg);
            if (dispo) {
                rmsg->encode_complete = true;
                qd_message_set_send_complete(msg);
                qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
                if (dispo == PN_ACCEPTED) {
                    bool need_close = false;
                    h1_codec_tx_done(hreq->base.lib_rs, &need_close);
                    hreq->close_on_complete = need_close || hreq->close_on_complete;

                    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][L%" PRIu64 "] HTTP response message msg-id=%" PRIu64 " encoding complete",
                           hconn->conn_id, link->identity, hreq->base.msg_id);

                } else {
                    // The response was bad.  This should not happen since the
                    // response was created by the remote HTTP/1.x adaptor.  Likely
                    // a bug? There's not much that can be done to recover, so for
                    // now just drop the connection to the client.
                    qdr_http1_close_connection(hconn, "Cannot parse response message");
                }
            }
        }
    } else {
        // for multicast aggregation we need to hold off encoding of the response until all responses have been
        // completely received. Only after all responses have arrived can we aggregate the response bodies into a single
        // HTTP1 response message and send it to the client.  We know that all responses have been received when the
        // settlement/terminal dispo update arrives for the _request_ message!  That triggers the response encode and
        // send.

        if (qd_message_receive_complete(msg)) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                   DLV_FMT " Aggregated response received (%zu responses received), settling", DLV_ARGS(delivery),
                   DEQ_SIZE(hreq->responses));
            dispo = PN_ACCEPTED;
            qd_message_set_send_complete(msg);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
        } else {
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                   DLV_FMT " Aggregated response incomplete (%zu responses received)", DLV_ARGS(delivery),
                   DEQ_SIZE(hreq->responses));
        }
    }

    // Note that returning a non-zero terminal disposition will cause the delivery to be updated and settled.
    return dispo;
}


//
// Misc
//


// return true if value appears in token list.  The compare
// is case-insensitive.
//
static bool _find_token(const char *list, const char *value)
{
    size_t len;
    const size_t token_len = strlen(value);
    const char *token = h1_codec_token_list_next(list, &len, &list);
    while (token) {
        if (len == token_len && strncasecmp(token, value, token_len) == 0) {
            return true;
        }
        token = h1_codec_token_list_next(list, &len, &list);
    }
    return false;
}


// free the response message
//
static void _client_response_msg_free(_client_request_t *req, _client_response_msg_t *rmsg)
{
    DEQ_REMOVE(req->responses, rmsg);
    qdr_http1_out_data_cleanup(&rmsg->out_data);

    if (rmsg->dlv) {
        assert(DEQ_IS_EMPTY(rmsg->out_data));  // expect no held references to message data
        qdr_delivery_set_context(rmsg->dlv, 0);
        qdr_delivery_decref(qdr_http1_adaptor->core, rmsg->dlv, "HTTP1 client response delivery settled");
        rmsg->dlv = 0;
    }

    free__client_response_msg_t(rmsg);
}

static void _client_request_free(_client_request_t *hreq)
{
    if (hreq) {
        // deactivate the Q2 callback
        qd_message_t *msg = hreq->request_dlv ? qdr_delivery_message(hreq->request_dlv) : hreq->request_msg;
        qd_message_clear_q2_unblocked_handler(msg);

        qdr_http1_request_base_cleanup(&hreq->base);
        qd_message_free(hreq->request_msg);
        if (hreq->request_dlv) {
            qdr_delivery_set_context(hreq->request_dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 client request delivery settled");
        }
        qd_compose_free(hreq->request_props);

        _client_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
        while (rmsg) {
            _client_response_msg_free(hreq, rmsg);
            rmsg = DEQ_HEAD(hreq->responses);
        }

        free__client_request_t(hreq);
    }
}


// release client-specific state
void qdr_http1_client_conn_cleanup(qdr_http1_connection_t *hconn)
{
    for (_client_request_t *hreq = (_client_request_t*) DEQ_HEAD(hconn->requests);
         hreq;
         hreq = (_client_request_t*) DEQ_HEAD(hconn->requests)) {
        _client_request_free(hreq);
    }
}


// handle connection close request from management
//
void qdr_http1_client_core_conn_close(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn)
{
    // initiate close of the raw conn.  the adaptor will call
    // qdr_connection_close() and clean up once the DISCONNECT
    // event is processed
    //
    qdr_http1_close_connection(hconn, 0);
}

static void _deliver_request(qdr_http1_connection_t *hconn, _client_request_t *hreq)
{
    if (hconn->cfg.event_channel) {
        qd_iterator_t *addr = qd_message_field_iterator(hreq->request_msg, QD_FIELD_TO);
        qd_iterator_reset_view(addr, ITER_VIEW_ADDRESS_HASH);
        hreq->request_dlv = qdr_link_deliver_to(hconn->in_link, hreq->request_msg, 0, addr, false, 0, 0, 0, 0);
    } else {
        hreq->request_dlv = qdr_link_deliver(hconn->in_link, hreq->request_msg, 0, false, 0, 0, 0, 0);
    }
    qdr_delivery_set_context(hreq->request_dlv, (void*) hreq);
    hreq->request_msg = 0;
}

// Generate an HTTP/1.1 response message and pass it to the codec. This should be used to send a response to a client
// request when/if the remote server is not going to send a response.
//
// code: HTTP/1.x response status code (e.g. 200...)
// reason: response reason phrase (e.g. "OK"...)
// text_body: optional text for response message body. If provided this method will generate a "Content-Length" header
// based on strlen(text_body) and a "Content-Type: text/plain" header.
//
static void _generate_response_msg(_client_request_t *hreq, int code, const char *reason, const char *text_body)
{
    assert(hreq->base.lib_rs);
    assert(DEQ_IS_EMPTY(hreq->responses));  // error: response already pending!

    _client_response_msg_t *rmsg = new__client_response_msg_t();
    ZERO(rmsg);
    DEQ_INIT(rmsg->out_data);
    DEQ_INSERT_TAIL(hreq->responses, rmsg);

    bool ignored;
    h1_codec_tx_response(hreq->base.lib_rs, code, reason, 1, 1);
    if (text_body) {
        const size_t len = strlen(text_body);
        char         buf[32];
        snprintf(buf, sizeof(buf), "%zu", len);
        h1_codec_tx_add_header(hreq->base.lib_rs, "Content-Length", buf);
        h1_codec_tx_add_header(hreq->base.lib_rs, "Content-Type", "text/plain");
        h1_codec_tx_body_str(hreq->base.lib_rs, text_body);
    } else {
        h1_codec_tx_add_header(hreq->base.lib_rs, "Content-Length", "0");
    }
    h1_codec_tx_done(hreq->base.lib_rs, &ignored);
    rmsg->encode_complete = true;
}

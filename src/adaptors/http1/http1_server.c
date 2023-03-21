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

#include "adaptors/adaptor_tls.h"
#include "http1_private.h"

#include <proton/proactor.h>

//
// This file contains code specific to HTTP server processing.  The raw
// connection is terminated at an HTTP server, not an HTTP client.
//


//
// State for a single response message arriving via the raw connection.  This
// message will be decoded into a single AMQP message and forwarded into the
// core.
//
// This object is instantiated when the HTTP1 codec indicates the arrival of a
// response message (See _server_rx_response_cb()).  The response is considered
// "complete" after it has been fully encoded and delivered to the core.  The
// _server_response_msg_t is freed at this point - we do not wait for dispo or
// settlement from the core since we cannot do anything meaningful should the
// delivery fail (other than log it).
//
typedef struct _server_response_msg_t {
    DEQ_LINKS(struct _server_response_msg_t);

    struct _server_request_t *hreq; // owning request

    qd_message_t        *msg;       // hold incoming message
    qd_composed_field_t *msg_props; // hold incoming headers
    qdr_delivery_t      *dlv;       // inbound to router (qdr_link_deliver)
    bool                 settled;   // by remote client
} _server_response_msg_t;
ALLOC_DECLARE(_server_response_msg_t);
ALLOC_DEFINE(_server_response_msg_t);
DEQ_DECLARE(_server_response_msg_t, _server_response_msg_list_t);

const char *HOST_KEY = "Host";

//
// State for an HTTP/1.x Request+Response exchange, server facing
//
typedef struct _server_request_t {
    qdr_http1_request_base_t   base;

    // The request arrives via the router core in an AMQP message
    // (qd_message_t).  These fields are used to encode the response and send
    // it out the raw connection.
    //
    qdr_delivery_t *request_dlv;      // outbound from core_link_deliver
    uint64_t        request_dispo;    // set by adaptor during encode
    bool            request_settled;  // settled by the adaptor
    bool            request_discard;  // drop incoming request data
    bool            headers_encoded;  // True when header encode done

    // fifo of encoded request data to be written out the raw connection.
    // Note well: cannot release request_dlv while there is pending out
    // data since it references body data from the message.
    qdr_http1_out_data_list_t out_data;

    _server_response_msg_list_t responses;  // response(s) to this request

    bool codec_completed_ok;  // Request and Response(s) encode/decode successful
    bool close_on_complete;  // close the connection when this request is complete
    bool cancelled;
} _server_request_t;
ALLOC_DECLARE(_server_request_t);
ALLOC_DEFINE(_server_request_t);

#define DEFAULT_CAPACITY     250

// Reconnection logic time values: When the HTTP server disconnects this
// adaptor will attempt to reconnect. The reconnect interval increases by
// RETRY_PAUSE_MSEC with each reconnect failure until it hits the maximum of
// RETRY_MAX_PAUSE_MSEC. If the reconnection does not succeed after
// LINK_TIMEOUT_MSEC then the qdr_link_t's are detached to prevent client
// requests from arriving for a potentially dead server.
#define RETRY_PAUSE_MSEC     ((qd_duration_t)500)
#define RETRY_MAX_PAUSE_MSEC ((qd_duration_t)3000)
#define LINK_TIMEOUT_MSEC    ((qd_duration_t)2500)

static void _server_tx_buffers_cb(h1_codec_request_state_t *lib_hrs, qd_buffer_list_t *blist, unsigned int len);
static void _server_tx_stream_data_cb(h1_codec_request_state_t *lib_hrs, qd_message_stream_data_t *stream_data);
static int  _server_rx_request_cb(h1_codec_request_state_t *hrs,
                                  const char *method,
                                  const char *target,
                                  uint32_t version_major,
                                  uint32_t version_minor);
static int  _server_rx_response_cb(h1_codec_request_state_t *hrs,
                                   int status_code,
                                   const char *reason_phrase,
                                   uint32_t version_major,
                                   uint32_t version_minor);
static int _server_rx_header_cb(h1_codec_request_state_t *hrs, const char *key, const char *value);
static int _server_rx_headers_done_cb(h1_codec_request_state_t *hrs, bool has_body);
static int _server_rx_body_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *body, size_t len, bool more);

static void _server_rx_done_cb(h1_codec_request_state_t *hrs);
static void _server_request_complete_cb(h1_codec_request_state_t *hrs, bool cancelled);
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);
static void _do_reconnect(void *context);
static void _server_response_msg_free(_server_request_t *req, _server_response_msg_t *rmsg);
static void _server_request_free(_server_request_t *hreq);               // deallocate
static void _cancel_request(_server_request_t *req, const char *error);  // mark req as cancelled
static void _finalize_request(_server_request_t *req);                   // clean up and free req

static uint64_t _encode_request_message(_server_request_t *hreq);

////////////////////////////////////////////////////////
// HTTP/1.x Server Connector
////////////////////////////////////////////////////////

// invoked when (if) the TLS handshake succeeds
//
static void _on_tls_connection_secured(qd_tls_t *tls, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t *) context;
    assert(hconn && hconn->tls);
    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%" PRIu64 "] TLS handshake succeeded: connection secure",
           hconn->conn_id);
    if (hconn->qdr_conn && hconn->qdr_conn->connection_info) {
        qd_tls_update_connection_info(hconn->tls, hconn->qdr_conn->connection_info);
    }
}

// An HttpConnector has been created.  Create an qdr_http_connection_t and a
// qdr_connection_t for it. This should only be called via the management agent.
//
static qdr_http1_connection_t *_create_server_connection(qd_http_connector_t *connector, qd_dispatch_t *qd)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();
    assert(hconn);

    ZERO(hconn);
    hconn->conn_id     = qd_server_allocate_connection_id(qd->server);
    hconn->require_tls = !!connector->tls_domain;
    if (hconn->require_tls) {
        // Pre-create the initial TLS session now. This is done so that if there is a configuration error it can be
        // detected here so the management operation can fail and report the problem to the user.
        hconn->tls = qd_tls(connector->tls_domain, hconn, hconn->conn_id, _on_tls_connection_secured);
        if (!hconn->tls) {
            // TLS was not configured successfully using the details in the connector and SSLProfile. See the logs for
            // additional detail
            qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR, "[C%" PRIu64 "] Error setting up TLS on connector %s to %s",
                   hconn->conn_id, connector->config->adaptor_config->name,
                   connector->config->adaptor_config->host_port);
            free_qdr_http1_connection_t(hconn);
            return 0;
        }
    }

    hconn->type                    = HTTP1_CONN_SERVER;
    hconn->admin_status            = QD_CONN_ADMIN_ENABLED;
    hconn->oper_status             = QD_CONN_OPER_DOWN;  // until TCP connection ready
    hconn->qd_server               = qd->server;
    hconn->adaptor                 = qdr_http1_adaptor;
    hconn->handler_context.handler = &_handle_connection_events;
    hconn->handler_context.context = hconn;
    sys_atomic_init(&hconn->q2_restart, 0);
    hconn->cfg.host    = qd_strdup(connector->config->adaptor_config->host);
    hconn->cfg.port    = qd_strdup(connector->config->adaptor_config->port);
    hconn->cfg.address = qd_strdup(connector->config->adaptor_config->address);
    hconn->cfg.site =
        connector->config->adaptor_config->site_id ? qd_strdup(connector->config->adaptor_config->site_id) : 0;
    hconn->cfg.host_port     = qd_strdup(connector->config->adaptor_config->host_port);
    hconn->server.connector  = connector;
    connector->ctx           = (void *) hconn;
    hconn->cfg.event_channel = connector->config->event_channel;
    hconn->cfg.aggregation   = connector->config->aggregation;
    hconn->cfg.host_override = connector->config->host_override ? qd_strdup(connector->config->host_override) : 0;

    // for initiating a connection to the server
    hconn->server.reconnect_timer = qd_timer(qdr_http1_adaptor->core->qd, _do_reconnect, hconn);

    //
    // Start a connection level vanflow record. The parent of the connection level
    // vanflow record is the associated connector's vanflow record.
    //
    hconn->vflow = vflow_start_record(VFLOW_RECORD_FLOW, connector->vflow);
    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);

    // Create the qdr_connection
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_OUTGOING, //qd_direction_t   dir,
                                                      hconn->cfg.host_port,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "HTTP/1.x Adaptor",    //const char      *container,
                                                      0,     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      "",                  // peer router version,
                                                      false,               // streaming links
                                                      false);              // connection trunking

    hconn->qdr_conn = qdr_connection_opened(qdr_http1_adaptor->core,
                                            qdr_http1_adaptor->adaptor,
                                            false,  // incoming
                                            QDR_ROLE_NORMAL,
                                            1,      // cost
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

    // wait for the raw connection to come up before creating the in and out links

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP connection to server created", hconn->conn_id);

    return hconn;
}


// Management Agent API - Create
//
// Note that this runs on the Management Agent thread, which may be running concurrently with the
// I/O and timer threads.
qd_http_connector_t *qd_http1_configure_connector(qd_http_connector_t *connector, qd_dispatch_t *qd,
                                                  qd_entity_t *entity)
{
    if (connector->config->adaptor_config->ssl_profile_name) {
        connector->tls_domain = qd_tls_domain(connector->config->adaptor_config, qd, qdr_http1_adaptor->log,
                                              http1_alpn_protocols, HTTP1_NUM_ALPN_PROTOCOLS, false);
        if (!connector->tls_domain) {
            // note qd_tls_domain logged the error
            qd_http_connector_decref(connector);
            return 0;
        }
    }

    qdr_http1_connection_t *hconn = _create_server_connection(connector, qd);
    if (!hconn) {
        // bad connector - no connection for you!
        qd_http_connector_decref(connector);
        return 0;
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%" PRIu64 "] Initiating connection to HTTP server %s",
           hconn->conn_id, hconn->cfg.host_port);

    connector->vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "http1");
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_NAME, connector->config->adaptor_config->name);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, connector->config->adaptor_config->host);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, connector->config->adaptor_config->port);
    vflow_set_string(connector->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS, connector->config->adaptor_config->address);

    // lock out the core activation thread.  Up until this point the core
    // thread cannot activate the qdr_connection_t since the
    // qdr_connection_t context has not been set (see
    // _core_connection_activate_CT in http1_adaptor.c). This keeps the
    // core from attempting to schedule the connection until we finish
    // setup.
    sys_mutex_lock(&qdr_http1_adaptor->lock);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->connections, hconn);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->connectors, connector);
    qdr_connection_set_context(hconn->qdr_conn, hconn);
    qd_timer_schedule(hconn->server.reconnect_timer, 0);
    sys_mutex_unlock(&qdr_http1_adaptor->lock);
    // setup complete - core thread can activate the connection

    return connector;
}

// Management Agent API - Delete
//
// Note that this runs on the Management Agent thread, which may be running concurrently with the
// I/O and timer threads.
void qd_http1_delete_connector(qd_dispatch_t *ignored, qd_http_connector_t *connector)
{
    if (connector) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Deleted HttpConnector for %s, %s:%s",
               connector->config->adaptor_config->address, connector->config->adaptor_config->host,
               connector->config->adaptor_config->port);

        sys_mutex_lock(&qdr_http1_adaptor->lock);
        DEQ_REMOVE(qdr_http1_adaptor->connectors, connector);
        qdr_http1_connection_t *hconn    = (qdr_http1_connection_t *) connector->ctx;
        qdr_connection_t *qdr_conn = 0;
        if (hconn) {
            hconn->admin_status = QD_CONN_ADMIN_DELETED;
            hconn->server.connector = 0;
            connector->ctx          = 0;
            qdr_conn = hconn->qdr_conn;
        }
        sys_mutex_unlock(&qdr_http1_adaptor->lock);

        if (qdr_conn)
            // this will cause the core thread to activate the connection and call qdr_http1_server_core_conn_close()
            qdr_core_close_connection(qdr_conn);
        qd_http_connector_decref(connector);
    }
}

// Has all raw connection I/O completed for the given hreq? Check if the codec has completed parsing both the request
// and all responses, and all pending output has been written to the network.  Note that this does not mean the hreq is
// complete (see _is_request_complete). It does mean that the request should not be cancelled if the raw connection goes
// away.
//
static inline bool _is_io_complete(const _server_request_t *hreq)
{
    assert(hreq);
    return hreq->codec_completed_ok && DEQ_IS_EMPTY(hreq->out_data);
}

// The request message has started being written out the server connection but the full exchange has not completed. This
// is significant during error handling: if part of the request has been written to the server then the server
// connection must be dropped when an error occurs in order to recover. Otherwise the unsent request can simply be
// discarded.
//
static inline bool _is_io_in_progress(const _server_request_t *hreq)
{
    assert(hreq);
    return hreq->base.out_http1_octets > 0 && !_is_io_complete(hreq);
}

// Has the HTTP request/response exchange completed successfully?
// - the codec has completed encode/decode of the HTTP request and response message
// - all pending network output data has been sent
// - the client request delivery has been settled
// - all responses have been delivered and settled
// - it hasn't been cancelled
//
static inline bool _is_complete_ok(const _server_request_t *hreq)
{
    assert(hreq);
    return !hreq->cancelled && _is_io_complete(hreq) && hreq->request_settled && DEQ_IS_EMPTY(hreq->responses);
}

// Check an in-progress request to see if all response messages have been read from the network
static inline bool _response_rx_done(const _server_request_t *hreq)
{
    return hreq->codec_completed_ok || (hreq->base.lib_rs && h1_codec_response_complete(hreq->base.lib_rs));
}

// Check an in-progress request to see if the client request message has been completely written to the network
static inline bool _request_tx_done(const _server_request_t *hreq)
{
    return DEQ_IS_EMPTY(hreq->out_data)
           && (hreq->codec_completed_ok || (hreq->base.lib_rs && h1_codec_request_complete(hreq->base.lib_rs)));
}

// Create the qdr links and HTTP codec when the server connection comes up.
// These links & codec will persist across temporary drops in the connection to
// the server (like when closing the connection to indicate end of response
// message).  However if the connection to the server cannot be re-established
// in a "reasonable" amount of time we consider the server unavailable and
// these links and codec will be closed - aborting any pending requests.  Once
// the connection to the server is reestablished these links & codec will be
// recreated.
//
static void _setup_server_links(qdr_http1_connection_t *hconn)
{
    if (!hconn->in_link) {
        // simulate an anonymous link for responses from the server
        hconn->in_link = qdr_link_first_attach(hconn->qdr_conn,
                                               QD_INCOMING,
                                               qdr_terminus(0),  //qdr_terminus_t   *source,
                                               qdr_terminus(0),  //qdr_terminus_t   *target
                                               "http1.server.in",  //const char       *name,
                                               0,                //const char       *terminus_addr,
                                               false,
                                               NULL,
                                               &(hconn->in_link_id));
        qdr_link_set_context(hconn->in_link, hconn);

        qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] HTTP server response link created",
               hconn->conn_id, hconn->in_link_id);
    }

    if (!hconn->out_link) {
        // simulate a server subscription for its service address
        qdr_terminus_t *source = qdr_terminus(0);
        qdr_terminus_set_address(source, hconn->cfg.address);
        hconn->out_link = qdr_link_first_attach(hconn->qdr_conn,
                                                QD_OUTGOING,
                                                source,           //qdr_terminus_t   *source,
                                                qdr_terminus(0),  //qdr_terminus_t   *target,
                                                "http1.server.out", //const char       *name,
                                                0,                //const char       *terminus_addr,
                                                false,
                                                0,      // initial delivery
                                                &(hconn->out_link_id));
        qdr_link_set_context(hconn->out_link, hconn);

        hconn->out_link_credit = DEFAULT_CAPACITY;
        qdr_link_flow(hconn->adaptor->core, hconn->out_link, DEFAULT_CAPACITY, false);

        qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] HTTP server request link created",
               hconn->conn_id, hconn->out_link_id);
    }

    if (!hconn->http_conn) {
        h1_codec_config_t config = {0};
        config.type             = HTTP1_CONN_SERVER;
        config.tx_buffers       = _server_tx_buffers_cb;
        config.tx_stream_data   = _server_tx_stream_data_cb;
        config.rx_request       = _server_rx_request_cb;
        config.rx_response      = _server_rx_response_cb;
        config.rx_header        = _server_rx_header_cb;
        config.rx_headers_done  = _server_rx_headers_done_cb;
        config.rx_body          = _server_rx_body_cb;
        config.rx_done          = _server_rx_done_cb;
        config.request_complete = _server_request_complete_cb;
        hconn->http_conn = h1_codec_connection(&config, hconn);
    }
}


// Tear down the qdr links and the codec.  This is called when the
// connection to the server has dropped and cannot be re-established in a
// timely manner.
//
static void _teardown_server_links(qdr_http1_connection_t *hconn)
{
    _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
    while (hreq) {
        _cancel_request(hreq, "Link lost");
        _finalize_request(hreq);
        hreq = (_server_request_t*) DEQ_HEAD(hconn->requests);
    }
    h1_codec_connection_free(hconn->http_conn);
    hconn->http_conn = 0;

    if (hconn->out_link) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Closing outgoing HTTP link",
               hconn->conn_id, hconn->out_link_id);
        qdr_link_set_context(hconn->out_link, 0);
        qdr_link_detach(hconn->out_link, QD_CLOSED, 0);
        hconn->out_link = 0;
    }

    if (hconn->in_link) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Closing incoming HTTP link",
               hconn->conn_id, hconn->in_link_id);
        qdr_link_set_context(hconn->in_link, 0);
        qdr_link_detach(hconn->in_link, QD_CLOSED, 0);
        hconn->in_link = 0;
    }
}


// Reconnection timer handler.
// This timer can be scheduled either by the event loop during the PN_RAW_CONNECTION_DISCONNECT event or by the core
// thread via _core_connection_activate_CT in http1_adaptor.c.  Since timers do not run concurrently this handler is
// guaranteed never to run concurrently with itself. Once hconn->raw_conn is set to zero by the disconnect handler it
// will remain zero until this handler creates a new raw connection. This ensures that it is impossible for the raw
// connection event handler and this timer handler to run concurrently (race).
//
static void _do_reconnect(void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    uint64_t conn_id = hconn->conn_id;

    // while timers do not run concurrently it is possible to reschedule them
    // via another thread while the timer handler is running, resulting in this
    // handler running twice
    sys_mutex_lock(&qdr_http1_adaptor->lock);
    if (hconn->raw_conn)  {
        sys_mutex_unlock(&qdr_http1_adaptor->lock);
        return;  // already ran
    }
    sys_mutex_unlock(&qdr_http1_adaptor->lock);

    // handle any qdr_connection_t processing requests that occurred since
    // this raw connection dropped.
    while (hconn->qdr_conn && qdr_connection_process(hconn->qdr_conn))
        ;

    if (!hconn->qdr_conn) {
        // the qdr_connection_t has been closed and there is no raw_conn to
        // clean up. It is now safe to delete hconn
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"] HTTP/1.x server connection closed", hconn->conn_id);
        qdr_http1_connection_free(hconn);
        return;
    }

    // Even though the raw connection is gone requests may be pending settlement updates from core. Check the head
    // requests for completion
    //
    _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
    while (hreq && (_is_complete_ok(hreq) || hreq->cancelled)) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP request msg-id=%" PRIu64 " completed%s!",
               hconn->conn_id, hreq->base.msg_id, hreq->cancelled ? " (cancelled)" : "");
        _finalize_request(hreq);                                          // frees hreq, removed from hconn->requests
        assert(hreq != (_server_request_t *) DEQ_HEAD(hconn->requests));  // shut up, coverity
        hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
    }

    sys_mutex_lock(&qdr_http1_adaptor->lock);
    bool connecting = false;
    if (hconn->admin_status == QD_CONN_ADMIN_ENABLED) {
        connecting      = true;
        hconn->raw_conn = pn_raw_connection();
        pn_raw_connection_set_context(hconn->raw_conn, &hconn->handler_context);
        // this next call may immediately reschedule the connection on another I/O
        // thread. After this call hconn may no longer be valid!
        pn_proactor_raw_connect(qd_server_proactor(hconn->qd_server), hconn->raw_conn, hconn->cfg.host_port);
        hconn->server.reconnecting = true;
    }
    sys_mutex_unlock(&qdr_http1_adaptor->lock);
    if (connecting)
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%" PRIu64 "] Connecting to HTTP server...", conn_id);
}

// handle PN_RAW_CONNECTION_NEED_READ_BUFFERS
static void _replenish_empty_read_buffers(qdr_http1_connection_t *hconn)
{
    assert(hconn->raw_conn);

    if (!hconn->q2_blocked) {
        int granted = qd_raw_connection_grant_read_buffers(hconn->raw_conn);
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%" PRIu64 "] %d read buffers granted", hconn->conn_id, granted);
    }
}

// Take buffers holding the encoded request data to be written out the raw connection. This is called by the I/O loop.
//
static int64_t _take_output_data(void *context, qd_adaptor_buffer_list_t *abufs, size_t limit)
{
    int64_t                 total_octets = 0;
    qdr_http1_connection_t *hconn        = (qdr_http1_connection_t *) context;

    assert(hconn);
    assert(DEQ_IS_EMPTY(*abufs));

    if (hconn->output_closed)
        return QD_IO_EOS;

    _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
    if (hreq && !hreq->cancelled) {
        total_octets = qdr_http1_get_out_buffers(&hreq->out_data, abufs, limit - DEQ_SIZE(*abufs));
        if (total_octets) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%" PRIu64 "] queueing %" PRIi64 " encoded bytes for output (%zu buffers)", hconn->conn_id,
                   total_octets, DEQ_SIZE(*abufs));
            hconn->out_http1_octets += total_octets;
            hreq->base.out_http1_octets += total_octets;
        } else if (hreq->close_on_complete && _request_tx_done(hreq)) {
            hconn->output_closed = true;
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%" PRIu64 "] request message written and close on complete true: closing write stream",
                   hconn->conn_id);
            total_octets = QD_IO_EOS;
        }
    }
    return total_octets;
}


// Unencrypted raw connection I/O work loop
static int _do_raw_io(qdr_http1_connection_t *hconn)
{
    bool rx_data = false;

    do {
        uint64_t                 octets   = 0;
        qd_adaptor_buffer_list_t in_abufs = DEQ_EMPTY;

        rx_data = false;
        qdr_http1_do_raw_io(hconn->conn_id, hconn->raw_conn, _take_output_data, hconn, &in_abufs, &octets);
        if (!DEQ_IS_EMPTY(in_abufs)) {
            rx_data = true;

            hconn->in_http1_octets += octets;
            vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);

            if (HTTP1_DUMP_BUFFERS) {
                fprintf(stdout, "\nServer raw buffer READ %" PRIu64 " total bytes\n", octets);
                qd_adaptor_buffer_t *bb = DEQ_HEAD(in_abufs);
                while (bb) {
                    fprintf(stdout, "  buffer='%.*s'\n", (int) qd_adaptor_buffer_size(bb),
                            (char *) qd_adaptor_buffer_base(bb));
                    bb = DEQ_NEXT(bb);
                }
                fflush(stdout);
            }

            if (hconn->http_conn && !hconn->input_closed) {
                qd_buffer_list_t qbuf_list = DEQ_EMPTY;
                qd_adaptor_buffers_copy_to_qd_buffers(&in_abufs, &qbuf_list);

                qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                       "[C%" PRIu64 "] pushing %" PRIu64 " received bytes into codec (%zu buffers)", hconn->conn_id,
                       octets, DEQ_SIZE(qbuf_list));

                int error = h1_codec_connection_rx_data(hconn->http_conn, &qbuf_list, octets);
                if (error) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                           "[C%" PRIu64 "] Incoming response message failed to parse, closing connection",
                           hconn->conn_id);
                    pn_raw_connection_read_close(hconn->raw_conn);
                    break;
                }
            } else {
                // codec closed - discard trailing input
                qd_adaptor_buffer_list_free_buffers(&in_abufs);
            }
        }
    } while (rx_data);  // codec may have generated more tx data

    // raw read has been drained, if closed notify the codec and close the write side when the current request completes
    //
    if (!hconn->input_closed && pn_raw_connection_is_read_closed(hconn->raw_conn)) {
        hconn->input_closed = true;

        bool close_output = true;
        bool truncated    = false;  // is current response not fully received?

        if (hconn->http_conn)
            truncated = !!h1_codec_connection_rx_close(hconn->http_conn);

        if (!truncated) {
            // check for server half-close: keep the write-side of the connection up until the request message has been
            // fully written out
            _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
            if (hreq && _response_rx_done(hreq) && !_request_tx_done(hreq)) {
                close_output            = false;
                hreq->close_on_complete = true;
            }
        } else {
            // truncated responses will be cancelled when the connection closes.
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%" PRIu64 "] codec detected truncated response message!",
                   hconn->conn_id);
            assert(close_output);
        }

        if (close_output && !hconn->output_closed) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%" PRIu64 "] raw connection read closed and input drained, closing raw connection write",
                   hconn->conn_id);
            hconn->output_closed = true;
            pn_raw_connection_write_close(hconn->raw_conn);
        }
    }
    return 0;
}

// Encrypted I/O work loop
static int _do_tls_io(qdr_http1_connection_t *hconn)
{
    bool rx_data = false;

    if (hconn->tls_error)
        return hconn->tls_error;

    if (!hconn->tls)
        return 0;

    do {
        int                      error    = 0;
        uint64_t                 octets   = 0;
        qd_adaptor_buffer_list_t in_abufs = DEQ_EMPTY;

        rx_data = false;
        error   = qd_tls_do_io(hconn->tls, hconn->raw_conn, _take_output_data, (void *) hconn, &in_abufs, &octets);
        if (error) {
            // note: TLS closes the raw conn on error
            hconn->tls_error = error;
            qd_adaptor_buffer_list_free_buffers(&in_abufs);
            return error;
        }

        if (!DEQ_IS_EMPTY(in_abufs)) {
            rx_data = true;

            hconn->in_http1_octets += octets;
            vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);

            if (HTTP1_DUMP_BUFFERS) {
                fprintf(stdout, "\nServer raw buffer READ %" PRIu64 " total bytes\n", octets);
                qd_adaptor_buffer_t *bb = DEQ_HEAD(in_abufs);
                while (bb) {
                    fprintf(stdout, "  buffer='%.*s'\n", (int) qd_adaptor_buffer_size(bb),
                            (char *) qd_adaptor_buffer_base(bb));
                    bb = DEQ_NEXT(bb);
                }
                fflush(stdout);
            }

            if (hconn->http_conn && !hconn->input_closed) {
                qd_buffer_list_t qbuf_list = DEQ_EMPTY;
                qd_adaptor_buffers_copy_to_qd_buffers(&in_abufs, &qbuf_list);

                qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                       "[C%" PRIu64 "] pushing %" PRIu64 " received bytes into codec (%zu buffers)", hconn->conn_id,
                       octets, DEQ_SIZE(qbuf_list));

                error = h1_codec_connection_rx_data(hconn->http_conn, &qbuf_list, octets);
                if (error) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                           "[C%" PRIu64 "] Incoming response message failed to parse, closing connection",
                           hconn->conn_id);
                    pn_raw_connection_read_close(hconn->raw_conn);
                    break;
                }
            } else {
                // codec closed - discard trailing input
                qd_adaptor_buffer_list_free_buffers(&in_abufs);
            }
        }
    } while (rx_data);  // codec may have generated more tx data

    // Check for end of input (half close). Please bear with me because things get really weird: Normally we should
    // ensure that the peer has done a clean TLS close - specifically sent a close_notify. However, according to the
    // HTTP/1.1 standard (RFC9112, section TLS Connection Closure) we should allow for a peer that closes the connection
    // without doing a close_notify IF the received message has a valid length. We can determine that by checking the
    // state of the current response message. But be careful: do this BEFORE closing the codec because IF the current
    // request is an 1.0 message without an explicit length the codec will mark it as complete. That's bad, because such
    // a response does NOT have an explicit length and if close_notify has NOT been received there is a potential for a
    // truncation attack.
    //
    // And you're wondering why this code is so overly complex. Welcome to my nightmare.

    bool close_notify;
    if (!hconn->input_closed && qd_tls_is_input_drained(hconn->tls, &close_notify)) {
        hconn->input_closed = true;

        bool close_output = true;
        bool truncated    = false;

        if (!close_notify) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%" PRIu64 "] TLS input closed without close-notify",
                   hconn->conn_id);

            // Peer may have truncated the response message
            _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
            if (hreq && _is_io_in_progress(hreq)) {
                if (!_response_rx_done(hreq)) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                           "[C%" PRIu64 "] TLS detected truncated response message!", hconn->conn_id);
                    truncated = true;
                    _cancel_request(hreq, "Invalid response message: truncated body");
                }
            }
        }

        if (hconn->http_conn)
            truncated = !!h1_codec_connection_rx_close(hconn->http_conn) || truncated;

        if (!truncated) {
            // permit half-close: close conn after request message written to output
            _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
            if (hreq && _response_rx_done(hreq) && !_request_tx_done(hreq)) {
                close_output            = false;
                hreq->close_on_complete = true;
            }
        }

        if (close_output && !hconn->output_closed) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%" PRIu64 "] TLS input closed and drained, closing TLS output", hconn->conn_id);
            hconn->output_closed = true;
            pn_raw_connection_wake(hconn->raw_conn);  // force I/O loop to run to do close_notify
        }
    }

    return 0;
}

inline static int _do_io(qdr_http1_connection_t *hconn)
{
    int error = 0;
    if (hconn->raw_conn) {
        if (hconn->require_tls) {
            error = _do_tls_io(hconn);
        } else {
            error = _do_raw_io(hconn);
        }
    }
    return error;
}

// Proton Raw Connection Events
//
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t *) context;
    qd_log_source_t        *log   = qdr_http1_adaptor->log;

    if (!hconn) return;

    qd_log(log, QD_LOG_TRACE, "[C%"PRIu64"] HTTP server proactor event %s", hconn->conn_id, pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        sys_mutex_lock(&qdr_http1_adaptor->lock);
        hconn->server.reconnecting = false;
        sys_mutex_unlock(&qdr_http1_adaptor->lock);

        qd_set_vflow_netaddr_string(hconn->vflow, hconn->raw_conn, false);
        if (hconn->oper_status == QD_CONN_OPER_DOWN) {
            hconn->oper_status = QD_CONN_OPER_UP;
            qd_log(log, QD_LOG_INFO, "[C%"PRIu64"] HTTP/1.x server %s connection established",
                   hconn->conn_id, hconn->cfg.host_port);
        }

        if (hconn->require_tls && hconn->tls == 0) {
            assert(hconn->server.connector && hconn->server.connector->tls_domain);
            hconn->tls = qd_tls(hconn->server.connector->tls_domain, hconn, hconn->conn_id, _on_tls_connection_secured);
            if (!hconn->tls) {
                hconn->tls_error = -1;
                qdr_http1_close_connection(hconn, "Failed to allocate a TLS session");
                break;
            }
        }

        hconn->server.link_timeout = 0;
        _setup_server_links(hconn);

        while (hconn->qdr_conn && qdr_connection_process(hconn->qdr_conn))
            ;
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qd_set_condition_on_vflow(hconn->raw_conn, hconn->vflow);
        pn_raw_connection_set_context(hconn->raw_conn, 0);

        // scrub the pending request queue: get rid of anything that wasn't fully complete before we lost the connection
        // to the server
        _server_request_t *hreq = (_server_request_t*) DEQ_HEAD(hconn->requests);
        while (hreq) {
            if (_is_io_in_progress(hreq)) {
                _cancel_request(hreq, "Network connection to the server has closed");
            }
            if (hreq->cancelled || _is_complete_ok(hreq)) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] HTTP request msg-id=%" PRIu64 " completed%s!", hconn->conn_id, hreq->base.msg_id,
                       hreq->cancelled ? " (cancelled)" : "");
                _finalize_request(hreq);  // frees hreq, removed from hconn->requests
                assert(hreq != (_server_request_t *) DEQ_HEAD(hconn->requests));  // shut up, coverity
                hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
            } else {
                break;
            }
        }

        // reset connection state
        qd_tls_free(hconn->tls);
        hconn->tls           = 0;
        hconn->tls_error     = 0;
        hconn->closing       = false;
        hconn->output_closed = false;
        hconn->input_closed  = false;

        // TODO: fix me:
        qd_raw_connection_drain_read_write_buffers(hconn->raw_conn);

        //
        // Try to reconnect to the server. Leave the links intact so pending
        // requests are not aborted.  If we fail to reconnect after
        // LINK_TIMEOUT_MSECS drop the links to prevent additional request from
        // arriving.
        //

        if (hconn->server.link_timeout == 0) {
            hconn->server.link_timeout = qd_timer_now() + LINK_TIMEOUT_MSEC;
            hconn->server.reconnect_pause = 0;
        } else {
            if ((qd_timer_now() - hconn->server.link_timeout) >= 0) {
                _teardown_server_links(hconn);
                // at this point we've unbound the service address so no
                // more messages will be sent to us. Notify meatspace:
                if (hconn->oper_status == QD_CONN_OPER_UP) {
                    hconn->oper_status = QD_CONN_OPER_DOWN;
                    qd_log(log, QD_LOG_INFO, "[C%"PRIu64"] HTTP/1.x server %s disconnected",
                           hconn->conn_id, hconn->cfg.host_port);
                }
            }
            if (hconn->server.reconnect_pause < RETRY_MAX_PAUSE_MSEC)
                hconn->server.reconnect_pause += RETRY_PAUSE_MSEC;
        }

        // prevent core activation
        sys_mutex_lock(&qdr_http1_adaptor->lock);
        hconn->raw_conn = 0;
        hconn->server.reconnecting = false;

        // there are two cases that need to be dealt with: the remote server
        // has dropped the connection and we need to reconnect, or the
        // connector has been deleted via management and no reconnect should be
        // done:
        if (hconn->admin_status == QD_CONN_ADMIN_ENABLED) {
            assert(hconn->qdr_conn);
            assert(hconn->server.reconnect_timer);
            qd_timer_schedule(hconn->server.reconnect_timer, hconn->server.reconnect_pause);
            // do not manipulate hconn further as it may now be processed by the
            // timer thread as soon as we drop the lock
            sys_mutex_unlock(&qdr_http1_adaptor->lock);
        } else {
            // we are not reconnecting due to management/shutdown/whatever. If
            // the qdr_conn has already been released (when
            // qdr_connection_process() was last called), then we can now free
            // the hconn
            hconn->server.link_timeout = 0;
            hconn->server.reconnect_pause = 0;
            bool free_hconn = !hconn->qdr_conn;
            if (!free_hconn)
                // ISSUE-627: If the core thread attempts to activate the connection via pn_raw_connection_wake() while
                // the raw connection is disconnecting we can miss that wake since once the disconnect event is
                // processed any pending wake events will be discarded. To avoid that make the reconnect_timer handler
                // run - it will call qdr_connection_process to satisfy the core activation but will not actually
                // reconnect since the connection is in admin disabled state. We cannot call qdr_connection_process here
                // safely: it cannot be called while holding the adaptor lock (deadlock) and once the lock is dropped it
                // is possible for the timer handler to run concurrently (race).
                qd_timer_schedule(hconn->server.reconnect_timer, 0);
            sys_mutex_unlock(&qdr_http1_adaptor->lock);
            if (free_hconn) {
                qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP/1.x server connection closed", hconn->conn_id);
                qdr_http1_connection_free(hconn);
            }
        }
        return;
    }
    case PN_RAW_CONNECTION_WAKE: {
        // Note: wake events may occur before the raw connection is established
        if (sys_atomic_set(&hconn->q2_restart, 0)) {
            // note: unit tests grep for this log!
            qd_log(log, QD_LOG_TRACE, "[C%"PRIu64"] server link unblocked from Q2 limit", hconn->conn_id);
            hconn->q2_blocked = false;
            _replenish_empty_read_buffers(hconn);  // restart receiver flow
        }

        // note that when qdr_connection_process() handles the connection close
        // the hconn->qdr_conn pointer will be zeroed.
        while (hconn->qdr_conn && qdr_connection_process(hconn->qdr_conn))
            ;
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        _replenish_empty_read_buffers(hconn);
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        // handled below in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ: {
        hconn->q2_blocked = false;
        // handled in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        // handled in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        qdr_http1_free_written_buffers(hconn);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        // handled in I/O loop
        break;
    }
    case PN_RAW_CONNECTION_DRAIN_BUFFERS: {
        qdr_http1_free_written_buffers(hconn);
        // discard of empty read buffers is handled in I/O loop
        break;
    }
    default:
        break;
    }

    // I/O loop: process the head request(s) until no more work can be done.
    //
    bool do_next_request;
    do {
        if (_do_io(hconn) != 0)
            break;  // I/O error - this will force-close the connection

        do_next_request = false;

        _server_request_t *hreq = (_server_request_t *) DEQ_HEAD(hconn->requests);
        if (hreq) {
            if (hreq->cancelled) {
                // If this request was cancelled either _before_ being written to the connection _or_ after all I/O has
                // completed then it can simply be finalized. However if it is in the process of being written/read then
                // we have to drop the server connection since there's no way to cleanly cancel an in-progress request
                // in the HTTP protocol.
                if (!_is_io_in_progress(hreq)) {
                    _finalize_request(hreq);
                    do_next_request = true;
                } else {
                    // Delete the request in the DISCONNECTED event to prevent further processing of pending requests.
                    qdr_http1_close_connection(hconn, "client request cancelled");
                }
            } else if (_is_complete_ok(hreq)) {
                if (hreq->close_on_complete) {
                    // The I/O layer will shutdown the connection after all I/O has completed. Leave this request on the
                    // queue until the DISCONNECT event to prevent sending next pending request before the connection
                    // closes.
                } else {
                    // move to the next pending request
                    _finalize_request(hreq);
                    do_next_request = true;
                }
            }
        }
    } while (do_next_request);
}

//////////////////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
//////////////////////////////////////////////////////////////////////


// Encoder has a buffer list to send to the server
//
static void _server_tx_buffers_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *blist, unsigned int len)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    if (hreq->request_discard)
        qd_buffer_list_free_buffers(blist);
    else {
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%" PRIu64 "][L%" PRIu64 "] %u request bytes encoded",
               hconn->conn_id, hconn->out_link_id, len);
        qdr_http1_enqueue_buffer_list(&hreq->out_data, blist, len);
    }
}


// Encoder has body data to send to the server
//
static void _server_tx_stream_data_cb(h1_codec_request_state_t *hrs, qd_message_stream_data_t *stream_data)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    if (hreq->request_discard)
        qd_message_stream_data_release(stream_data);
    else {
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%" PRIu64 "][L%" PRIu64 "] %zu body data bytes encoded",
               hconn->conn_id, hconn->out_link_id, qd_message_stream_data_payload_length(stream_data));
        qdr_http1_enqueue_stream_data(&hreq->out_data, stream_data);
    }
}


// Server will not be sending us HTTP requests
//
static int _server_rx_request_cb(h1_codec_request_state_t *hrs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor)
{
    assert(false);
    return HTTP1_STATUS_BAD_REQ;
}


// called when decoding an HTTP response from the server.
//
static int _server_rx_response_cb(h1_codec_request_state_t *hrs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    // expected to be in-order
    assert(hreq && hreq == (_server_request_t*) DEQ_HEAD(hconn->requests));

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP msg_id=%"PRIu64" response received: status=%d phrase=%s version=%"PRIi32".%"PRIi32,
           hconn->conn_id, hconn->in_link_id, hreq->base.msg_id, status_code, reason_phrase ? reason_phrase : "<NONE>",
           version_major, version_minor);
    //
    // We are about to start decoding the HTTP response from the server.
    // End the server side latency for the server request's vanflow.
    //
    vflow_latency_end(hreq->base.vflow);

    _server_response_msg_t *rmsg = new__server_response_msg_t();
    ZERO(rmsg);
    rmsg->hreq = hreq;
    DEQ_INSERT_TAIL(hreq->responses, rmsg);
    rmsg->msg_props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(rmsg->msg_props);
    {
        char temp[64];
        snprintf(temp, sizeof(temp), "%"PRIi32".%"PRIi32, version_major, version_minor);
        qd_compose_insert_string(rmsg->msg_props, VERSION_PROP_KEY);
        qd_compose_insert_string(rmsg->msg_props, temp);

        if (reason_phrase) {
            qd_compose_insert_string(rmsg->msg_props, REASON_PROP_KEY);
            qd_compose_insert_string(rmsg->msg_props, reason_phrase);
        }

        qd_compose_insert_string(rmsg->msg_props, PATH_PROP_KEY);
        qd_compose_insert_string(rmsg->msg_props, h1_codec_request_state_target(hrs));
    }

    hreq->close_on_complete = hreq->close_on_complete || version_minor == 0;
    return 0;
}


// called for each decoded HTTP header.
//
static int _server_rx_header_cb(h1_codec_request_state_t *hrs, const char *key, const char *value)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"]L%"PRIu64"] HTTP response header received: key='%s' value='%s'",
           hconn->conn_id, hconn->in_link_id, key, value);

    // expect: running incoming request at tail
    _server_response_msg_t *rmsg = DEQ_TAIL(hreq->responses);
    assert(rmsg);

    // We need to filter the connection header out
    // @TODO(kgiusti): also have to remove headers given in value!
    if (strcasecmp(key, "connection") != 0) {
        qd_compose_insert_symbol(rmsg->msg_props, key);
        qd_compose_insert_string(rmsg->msg_props, value);
    } else if (h1_codec_token_list_find(value, "close")) {
        hreq->close_on_complete = true;
    }

    return 0;
}


// called after the last header is decoded, before decoding any body data.
//
static int _server_rx_headers_done_cb(h1_codec_request_state_t *hrs, bool has_body)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP response headers done.",
           hconn->conn_id, hconn->in_link_id);

    // expect: running incoming request at tail
    _server_response_msg_t *rmsg = DEQ_TAIL(hreq->responses);
    assert(rmsg);

    // start building the AMQP message

    assert(!rmsg->msg);
    rmsg->msg = qd_message();

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
    qd_compose_insert_null(props);     // message-id
    qd_compose_insert_null(props);     // user-id
    qd_compose_insert_string(props, hreq->base.response_addr); // to
    {
        // subject:
        char u32_str[64];
        snprintf(u32_str, sizeof(u32_str), "%"PRIu32, h1_codec_request_state_response_code(hrs));
        qd_compose_insert_string(props, u32_str);
    }
    qd_compose_insert_null(props);   // reply-to
    qd_compose_insert_ulong(props, hreq->base.msg_id);  // correlation-id
    qd_compose_insert_null(props);                      // content-type
    qd_compose_insert_null(props);                      // content-encoding
    qd_compose_insert_null(props);                      // absolute-expiry-time
    qd_compose_insert_null(props);                      // creation-time
    qd_compose_insert_string(props, hconn->cfg.site);   // group-id
    qd_compose_end_list(props);

    qd_compose_end_map(rmsg->msg_props);

    qd_message_compose_3(rmsg->msg, props, rmsg->msg_props, !has_body);
    qd_compose_free(props);
    qd_compose_free(rmsg->msg_props);
    rmsg->msg_props = 0;

    // future-proof: ensure the message headers have not caused Q2
    // blocking.  We only check for Q2 events while adding body data.
    assert(!qd_message_is_Q2_blocked(rmsg->msg));

    qd_alloc_safe_ptr_t hconn_sp = QD_SAFE_PTR_INIT(hconn);
    qd_message_set_q2_unblocked_handler(rmsg->msg, qdr_http1_q2_unblocked_handler, hconn_sp);

    // hack: rather than special-case event_channel, simply leverage message discard
    if (hconn->cfg.event_channel) {
        // Responses coming from the server are always ignored when Event Channel is configured. The originating
        // client-facing router will generate a response instead.
        qd_message_set_discard(rmsg->msg, true);
    } else {
        // start delivery if possible
        if (hconn->in_link_credit > 0 && rmsg == DEQ_HEAD(hreq->responses)) {
            hconn->in_link_credit -= 1;

            qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                   "[C%" PRIu64 "][L%" PRIu64 "] Delivering msg-id=%" PRIu64 " response to router addr=%s",
                   hconn->conn_id, hconn->in_link_id, hreq->base.msg_id, hreq->base.response_addr);

            qd_iterator_t *addr = qd_message_field_iterator(rmsg->msg, QD_FIELD_TO);
            assert(addr);
            qd_iterator_reset_view(addr, ITER_VIEW_ADDRESS_HASH);
            rmsg->dlv = qdr_link_deliver_to(hconn->in_link, rmsg->msg, 0, addr, false, 0, 0, 0, 0);
            qdr_delivery_set_context(rmsg->dlv, (void *) hreq);
        }
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int _server_rx_body_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *body, size_t len,
                              bool more)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    bool                    q2_blocked = false;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP response body received len=%zu.",
           hconn->conn_id, hconn->in_link_id, len);

    _server_response_msg_t *rmsg  = DEQ_TAIL(hreq->responses);
    if (qd_message_is_discard(rmsg->msg)) {
        qd_buffer_list_free_buffers(body);
        return 0;
    }

    qd_message_t *msg = rmsg->msg ? rmsg->msg : qdr_delivery_message(rmsg->dlv);
    qd_message_stream_data_append(msg, body, &q2_blocked);
    if (q2_blocked && !hconn->q2_blocked) {
        // note: unit tests grep for this log!
        hconn->q2_blocked = true;
        if (rmsg->dlv)
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, DLV_FMT" server link blocked on Q2 limit", DLV_ARGS(rmsg->dlv));
        else
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE, "[C%"PRIu64"] server link blocked on Q2 limit", hconn->conn_id);
    }

    //
    // Notify the router that more data is ready to be pushed out on the delivery
    //
    if (!more)
        qd_message_set_receive_complete(msg);

    if (rmsg->dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, rmsg->dlv, false);

    return 0;
}

// Called at the completion of a single response message decoding. Note if this is an informational response (code 1XX)
// the whole HTTP request is not yet complete: more response messages are expected to arrive.
//
static void _server_rx_done_cb(h1_codec_request_state_t *hrs)
{
    _server_request_t      *hreq  = (_server_request_t *) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    _server_response_msg_t *rmsg  = DEQ_TAIL(hreq->responses);

    if (!qd_message_receive_complete(rmsg->msg)) {
        qd_message_set_receive_complete(rmsg->msg);
        if (rmsg->dlv) {
            qdr_delivery_continue(qdr_http1_adaptor->core, rmsg->dlv, false);
        }
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%" PRIu64 "][L%" PRIu64 "] HTTP response message msg-id=%" PRIu64 " decoding complete.", hconn->conn_id,
           hconn->in_link_id, hreq->base.msg_id);

    // if message aggregation is in use we cannot release the response until after the remote client has settled
    // it. Otherwise there's little use in blocking the server waiting for settlement so just drop it.
    if (rmsg->settled || (rmsg->dlv && hconn->cfg.aggregation == QD_AGGREGATION_NONE)
        || qd_message_is_discard(rmsg->msg)) {
        _server_response_msg_free(hreq, rmsg);
    }
}


// called at the completion of a full Request/Response exchange, or as a result
// of cancelling the request.  The hrs will be deleted on return from this
// call.  Any hrs related state must be released before returning from this
// callback.
//
// Note: in the case where the request had multiple response messages, this
// call occurs when the LAST response has been completely received
// (_server_rx_done_cb())
//
static void _server_request_complete_cb(h1_codec_request_state_t *hrs, bool cancelled)
{
    _server_request_t       *hreq = (_server_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    hreq->base.stop = qd_timer_now();
    qdr_http1_record_server_request_info(qdr_http1_adaptor, &hreq->base);
    hreq->base.lib_rs = 0;
    // expect: adaptor cancelled the request before cancelling the codec
    assert(!cancelled || hreq->cancelled);
    hreq->codec_completed_ok = !cancelled;

    // Can we settle the client request message delivery?
    //
    // In the non-message aggregation case we can settle early rather than waiting for the client to settle the response
    // messages. Otherwise we have to wait for the client to settle all outstanding response messages. Cancelled
    // requests will get settled with the proper outcome when they are finalized.

    if (!cancelled && (hconn->cfg.aggregation == QD_AGGREGATION_NONE || DEQ_IS_EMPTY(hreq->responses))) {
        if (!hreq->request_settled) {
            hreq->request_settled = true;
            if (hreq->request_dlv) {
                assert(hreq->request_dispo != 0);  // expected this to be set
                qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                  hreq->request_dlv,
                                                  hreq->request_dispo,
                                                  true,  // settled
                                                  0,     // delivery state
                                                  false);
            }
        }
    }

    uint64_t in_octets, out_octets;
    h1_codec_request_state_counters(hrs, &in_octets, &out_octets);
    //
    // Set the inbound octets for this request.
    //
    vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, in_octets);
    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"] HTTP request/response %s. Bytes read: %"PRIu64" written: %"PRIu64,
           hconn->conn_id,
           cancelled ? "cancelled!" : "codec done",
           in_octets, out_octets);
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


// credit has been granted - responses may now be sent to the
// router core.
//
void qdr_http1_server_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit)
{
    assert(link == hconn->in_link);   // router only grants flow on incoming link

    assert(qdr_link_is_anonymous(link));  // remove me
    hconn->in_link_credit += credit;

    qd_log(adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] Credit granted on response link: %d",
           hconn->conn_id, hconn->in_link_id, hconn->in_link_credit);

    if (hconn->in_link_credit > 0) {
        if (hconn->raw_conn)
            _replenish_empty_read_buffers(hconn);

        // check for pending responses that are blocked from delivery pending credit

        _server_request_t *hreq = (_server_request_t*) DEQ_HEAD(hconn->requests);
        if (hreq) {
            _server_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
            while (rmsg && rmsg->msg && !rmsg->dlv && hconn->in_link_credit > 0) {
                if (qd_message_is_discard(rmsg->msg))
                    break;

                hconn->in_link_credit -= 1;

                qd_log(adaptor->log, QD_LOG_TRACE,
                       "[C%"PRIu64"][L%"PRIu64"] Delivering blocked response to router addr=%s",
                       hconn->conn_id, hconn->in_link_id, hreq->base.response_addr);

                qd_iterator_t *addr = qd_message_field_iterator(rmsg->msg, QD_FIELD_TO);
                qd_iterator_reset_view(addr, ITER_VIEW_ADDRESS_HASH);
                rmsg->dlv = qdr_link_deliver_to(hconn->in_link, rmsg->msg, 0, addr, false, 0, 0, 0, 0);
                qdr_delivery_set_context(rmsg->dlv, (void*) hreq);
                if (!qd_message_receive_complete(rmsg->msg)) {
                    // stop here since response must be complete before we can deliver the next one.
                    break;
                }
                if (hconn->cfg.aggregation != QD_AGGREGATION_NONE) {
                    // stop here since response should not be freed until it is accepted
                    break;
                }
                // else the delivery is complete no need to save it
                _server_response_msg_free(hreq, rmsg);
                rmsg = DEQ_HEAD(hreq->responses);
            }
        }
    }
}


// Handle disposition/settlement update for the outstanding HTTP response.
//
void qdr_http1_server_core_delivery_update(qdr_http1_adaptor_t      *adaptor,
                                           qdr_http1_connection_t   *hconn,
                                           qdr_http1_request_base_t *hbase,
                                           qdr_delivery_t           *dlv,
                                           uint64_t                  disp,
                                           bool                      settled)
{
    _server_request_t *hreq = (_server_request_t *) hbase;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           DLV_FMT" HTTP response delivery update, outcome=0x%"PRIx64"%s",
           DLV_ARGS(dlv), disp, settled ? " settled": "");

    if (settled) {
        _server_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
        while (rmsg) {
            if (rmsg->dlv == dlv) {
                rmsg->settled = true;
                if (qd_message_receive_complete(rmsg->msg)) {
                    _server_response_msg_free(hreq, rmsg);
                } else if (disp == PN_RELEASED || disp == PN_REJECTED || disp == PN_MODIFIED) {
                    qd_log(adaptor->log, QD_LOG_WARNING,
                           DLV_FMT " Response message was not accepted by client, outcome=0x%" PRIx64, DLV_ARGS(dlv),
                           disp);
                    qd_message_set_discard(rmsg->msg, true);
                    qd_message_Q2_holdoff_disable(rmsg->msg);
                }
                break;
            }
            rmsg = DEQ_NEXT(rmsg);
        }

        // Message Aggregation: once all the response messages have been decoded and settled we can settle the client's
        // request delivery. The client will use this settlement update to trigger sending the aggreggated response.
        if (hconn->cfg.aggregation != QD_AGGREGATION_NONE && DEQ_IS_EMPTY(hreq->responses)
            && hreq->codec_completed_ok) {
            if (!hreq->request_settled) {
                hreq->request_settled = true;
                if (hreq->request_dlv) {
                    qd_log(adaptor->log, QD_LOG_TRACE, DLV_FMT " request message accepted",
                           DLV_ARGS(hreq->request_dlv));
                    qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                      hreq->request_dlv,
                                                      PN_ACCEPTED,
                                                      true,  // settled
                                                      0,     // delivery state
                                                      false);
                }
            }
        }
    }
}


//
// Request message forwarding
//


// Create a request context for a new request in msg, which is valid to a depth
// of at least QD_DEPTH_PROPERTIES
//
static _server_request_t *_create_request_context(qdr_http1_connection_t *hconn, qd_message_t *msg)
{
    uint64_t msg_id = 0;
    char *reply_to = 0;
    bool ok = false;
    qd_parsed_field_t *msg_id_pf = 0;

    qd_iterator_t *msg_id_itr = qd_message_field_iterator_typed(msg, QD_FIELD_MESSAGE_ID);  // ulong
    if (msg_id_itr) {
        msg_id_pf = qd_parse(msg_id_itr);
        if (msg_id_pf && qd_parse_ok(msg_id_pf)) {
            msg_id = qd_parse_as_ulong(msg_id_pf);
            ok = qd_parse_ok(msg_id_pf);
        }
    }
    qd_parse_free(msg_id_pf);
    qd_iterator_free(msg_id_itr);

    if (!ok) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] Rejecting message missing id.",
               hconn->conn_id, hconn->out_link_id);
        return 0;
    }

    qd_iterator_t *reply_to_itr = qd_message_field_iterator(msg, QD_FIELD_REPLY_TO);
    reply_to = (char*) qd_iterator_copy(reply_to_itr);
    qd_iterator_free(reply_to_itr);

    if (!reply_to && !hconn->cfg.event_channel) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] Rejecting message no reply-to.",
               hconn->conn_id, hconn->out_link_id);
        return 0;
    }

    qd_iterator_t *group_id_itr = qd_message_field_iterator(msg, QD_FIELD_GROUP_ID);
    char* group_id = (char*) qd_iterator_copy(group_id_itr);
    qd_iterator_free(group_id_itr);

    _server_request_t *hreq = new__server_request_t();
    ZERO(hreq);
    hreq->base.hconn = hconn;
    hreq->base.msg_id = msg_id;
    hreq->base.response_addr = reply_to;
    hreq->base.site = group_id;
    hreq->base.start = qd_timer_now();
    DEQ_INIT(hreq->out_data);
    DEQ_INIT(hreq->responses);
    DEQ_INSERT_TAIL(hconn->requests, &hreq->base);

    //
    // Start a vanflow record for the server side request. The parent of this vanflow is
    // its connection's vanflow record.
    //
    hreq->base.vflow = vflow_start_record(VFLOW_RECORD_FLOW, hconn->vflow);
    vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

    //
    // The server side of the router is about to send out a request to the http server.
    // Start the vflow latency here.
    //
    vflow_latency_start(hreq->base.vflow);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] New HTTP Request msg-id=%"PRIu64" reply-to=%s.",
           hconn->conn_id, hconn->out_link_id, msg_id, reply_to);
    return hreq;
}


// Start a new request to the server.  msg has been validated to at least
// application properties depth.  Returns 0 on success.
//
static uint64_t _send_request_headers(_server_request_t *hreq, qd_message_t *msg)
{
    // start encoding HTTP request.  Need method, target and version

    qdr_http1_connection_t *hconn = hreq->base.hconn;
    char *method_str = 0;
    char *target_str = 0;
    qd_parsed_field_t *app_props = 0;
    uint32_t major = 1;
    uint32_t minor = 1;
    uint64_t outcome = 0;

    assert(!hreq->base.lib_rs);
    assert(qd_message_check_depth(msg, QD_DEPTH_PROPERTIES) == QD_MESSAGE_DEPTH_OK);

    // method is passed in the SUBJECT field
    qd_iterator_t *method_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
    if (!method_iter) {
        return PN_REJECTED;
    }

    method_str = (char*) qd_iterator_copy(method_iter);
    qd_iterator_free(method_iter);
    if (!method_str || *method_str == 0) {
        outcome = PN_REJECTED;
        goto exit;
    }

    // target, version info and other headers are in the app properties
    qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (!app_props_iter) {
        outcome = PN_REJECTED;
        goto exit;
    }

    app_props = qd_parse(app_props_iter);
    qd_iterator_free(app_props_iter);
    if (!app_props) {
        outcome = PN_REJECTED;
        goto exit;
    }

    qd_parsed_field_t *ref = qd_parse_value_by_key(app_props, PATH_PROP_KEY);
    target_str = (char*) qd_iterator_copy(qd_parse_raw(ref));
    if (!target_str || *target_str == 0) {
        outcome = PN_REJECTED;
        goto exit;
    }

    // Pull the version info from the app properties (e.g. "1.1")
    ref = qd_parse_value_by_key(app_props, VERSION_PROP_KEY);
    if (ref) {  // optional
        char *version_str = (char*) qd_iterator_copy(qd_parse_raw(ref));
        if (version_str)
            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
        free(version_str);
    }

    // done copying and converting!

    qd_log(hconn->adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] Encoding request method=%s target=%s",
           hconn->conn_id, hconn->out_link_id, method_str, target_str);

    hreq->base.lib_rs = h1_codec_tx_request(hconn->http_conn, method_str, target_str, major, minor);
    if (!hreq->base.lib_rs) {
        outcome = PN_REJECTED;
        goto exit;
    }

    h1_codec_request_state_set_context(hreq->base.lib_rs, (void*) hreq);

    // now send all headers in app properties
    qd_parsed_field_t *key = qd_field_first_child(app_props);
    bool ok = true;
    while (ok && key) {
        qd_parsed_field_t *value = qd_field_next_child(key);
        if (!value)
            break;

        qd_iterator_t *i_key = qd_parse_raw(key);
        if (!i_key)
            break;

        if (hconn->cfg.host_override && qd_iterator_equal(i_key, (const unsigned char*) HOST_KEY)) {
            //if host override option is in use, write the configured
            //value rather than that submitted by client
            char *header_key = (char*) qd_iterator_copy(i_key);
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Encoding request header %s:%s",
                   hconn->conn_id, hconn->out_link_id,
                   header_key, hconn->cfg.host_override);

            ok = !h1_codec_tx_add_header(hreq->base.lib_rs, header_key, hconn->cfg.host_override);

            free(header_key);

        } else if (!qd_iterator_prefix(i_key, ":")) {

            // ignore the special headers added by the mapping
            qd_iterator_t *i_value = qd_parse_raw(value);
            if (!i_value)
                break;

            char *header_key = (char*) qd_iterator_copy(i_key);
            char *header_value = (char*) qd_iterator_copy(i_value);

            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Encoding request header %s:%s",
                   hconn->conn_id, hconn->out_link_id,
                   header_key, header_value);

            ok = !h1_codec_tx_add_header(hreq->base.lib_rs, header_key, header_value);

            free(header_key);
            free(header_value);
        } else if (qd_iterator_equal(i_key, (const unsigned char *) QD_AP_FLOW_ID)) {
            //
            // Get the vanflow id from the message and set that as the van counterflow.
            //
            vflow_set_ref_from_parsed(hreq->base.vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, value);
        }

        key = qd_field_next_child(value);
    }

    if (!ok)
        outcome = PN_REJECTED;

exit:

    free(method_str);
    free(target_str);
    qd_parse_free(app_props);

    return outcome;
}

// Encode an outbound AMQP message as an HTTP Request.  Sets the request message outcome
// when the encoding completes either successfully or in error.
//
static uint64_t _encode_request_message(_server_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->base.hconn;
    qd_message_t           *msg   = qdr_delivery_message(hreq->request_dlv);

    if (!hreq->headers_encoded) {
        hreq->headers_encoded = true;

        uint64_t dispo = _send_request_headers(hreq, msg);
        if (dispo) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING, DLV_FMT " Rejecting malformed message msg-id=%" PRIu64,
                   DLV_ARGS(hreq->request_dlv), hreq->base.msg_id);
            hreq->request_dispo = dispo;
            return hreq->request_dispo;
        }
    }

    while (true) {
        qd_message_stream_data_t *stream_data = 0;
        switch (qd_message_next_stream_data(msg, &stream_data)) {
            case QD_MESSAGE_STREAM_DATA_BODY_OK: {
                qd_log(hconn->adaptor->log, QD_LOG_TRACE, DLV_FMT " Encoding request body data",
                       DLV_ARGS(hreq->request_dlv));
                if (h1_codec_tx_body(hreq->base.lib_rs, stream_data)) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING, DLV_FMT " body data encode failed",
                           DLV_ARGS(hreq->request_dlv));
                    hreq->request_dispo = PN_REJECTED;
                    return hreq->request_dispo;
                }
                break;
            }

            case QD_MESSAGE_STREAM_DATA_FOOTER_OK:
                qd_message_stream_data_release(stream_data);
                break;

            case QD_MESSAGE_STREAM_DATA_NO_MORE: {
                bool ignore;
                qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                       DLV_FMT " HTTP Request msg-id=%" PRIu64 " body data encode complete",
                       DLV_ARGS(hreq->request_dlv), hreq->base.msg_id);
                hreq->request_dispo = PN_ACCEPTED;
                h1_codec_tx_done(hreq->base.lib_rs, &ignore);
                return hreq->request_dispo;
            }

            case QD_MESSAGE_STREAM_DATA_INCOMPLETE:
                return 0;  // wait for more

            case QD_MESSAGE_STREAM_DATA_INVALID:
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING, DLV_FMT " Rejecting corrupted body data.",
                       DLV_ARGS(hreq->request_dlv));
                hreq->request_dispo = PN_REJECTED;
                return hreq->request_dispo;

            case QD_MESSAGE_STREAM_DATA_ABORTED:
                qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                       DLV_FMT " HTTP Request msg-id=%" PRIu64 " message aborted", DLV_ARGS(hreq->request_dlv),
                       hreq->base.msg_id);
                hreq->request_dispo = PN_REJECTED;
                return hreq->request_dispo;
        }
    }
}


// The router wants to send this delivery out the link. This is either the
// start of a new incoming HTTP request or the continuation of an existing one.
// Note: returning a non-zero value will cause the delivery to be settled!
//
uint64_t qdr_http1_server_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled)
{
    qd_message_t      *msg  = qdr_delivery_message(delivery);
    _server_request_t *hreq = (_server_request_t*) qdr_delivery_get_context(delivery);

    if (!hreq) {
        // TODO(kgiusti): very occasionally this callback is invoked after we've completed sending the message. Need to
        // fix this, but for now I punt.
        if (qd_message_send_complete(msg))
            return 0;

        if (qd_message_aborted(msg)) {
            // can safely discard since it was yet to be processed
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   DLV_FMT" Discarding aborted request", DLV_ARGS(delivery));
            qd_message_set_send_complete(msg);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            return PN_REJECTED;
        }

        // new delivery - create new request:
        switch (qd_message_check_depth(msg, QD_DEPTH_PROPERTIES)) {
        case QD_MESSAGE_DEPTH_INCOMPLETE:
            return 0;

        case QD_MESSAGE_DEPTH_INVALID:
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   DLV_FMT" Malformed HTTP/1.x message", DLV_ARGS(delivery));
            qd_message_set_send_complete(msg);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            return PN_REJECTED;

        case QD_MESSAGE_DEPTH_OK:
            hreq = _create_request_context(hconn, msg);
            if (!hreq) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       DLV_FMT" Discarding malformed message.", DLV_ARGS(delivery));
                qd_message_set_send_complete(msg);
                qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
                return PN_REJECTED;
            }

            hreq->request_dlv = delivery;
            qdr_delivery_set_context(delivery, (void*) hreq);
            qdr_delivery_incref(delivery, "HTTP1 server referencing request delivery");

            // workaround for Q2 stall, ISSUE #754/#758
            qd_message_Q2_holdoff_disable(msg);
            break;
        }
    }

    // Encode the request into buffers pending output. Note that these buffers are not yet written to the network: that
    // happens in the I/O loop called from the proactor connection event handler

    if (!hreq->cancelled) {
        switch (_encode_request_message(hreq)) {
            case 0:
                // not done receiving yet
                break;
            case PN_ACCEPTED:
                qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][L%" PRIu64 "] HTTP request message msg-id=%" PRIu64 " encoding complete",
                       hconn->conn_id, hconn->out_link_id, hreq->base.msg_id);
                qd_message_set_send_complete(msg);
                break;
            default:
                // parse error: logged by _encode_request_message()
                qd_message_set_send_complete(msg);
                _cancel_request(hreq, "Incoming HTTP/1.x request message failed to encode properly");
                break;
        }
    }

    return 0;
}


//
// Misc
//

// free the response message
//
static void _server_response_msg_free(_server_request_t *hreq, _server_response_msg_t *rmsg)
{
    if (rmsg) {
        DEQ_REMOVE(hreq->responses, rmsg);
        qd_message_clear_q2_unblocked_handler(rmsg->msg);
        qd_compose_free(rmsg->msg_props);

        if (rmsg->dlv) {
            // dlv "owns" message, so do not free rmsg->msg directly
            qdr_delivery_set_context(rmsg->dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, rmsg->dlv, "HTTP1 server freeing response delivery");
        } else {
            qd_message_free(rmsg->msg);
        }

        free__server_response_msg_t(rmsg);
    }
}


// Release the request
//
static void _server_request_free(_server_request_t *hreq)
{
    if (hreq) {
        qdr_http1_request_base_cleanup(&hreq->base);
        qdr_http1_out_data_cleanup(&hreq->out_data);
        if (hreq->request_dlv) {
            assert(DEQ_IS_EMPTY(hreq->out_data));  // expect no held references to message data
            qdr_delivery_set_context(hreq->request_dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 server releasing request delivery");
            hreq->request_dlv = 0;
        }

        _server_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
        while (rmsg) {
            _server_response_msg_free(hreq, rmsg);
            rmsg = DEQ_HEAD(hreq->responses);
        }

        free__server_request_t(hreq);
    }
}

void qdr_http1_server_conn_cleanup(qdr_http1_connection_t *hconn)
{
    for (_server_request_t *hreq = (_server_request_t*) DEQ_HEAD(hconn->requests);
         hreq;
         hreq = (_server_request_t*) DEQ_HEAD(hconn->requests)) {
        _server_request_free(hreq);
    }
}

// Mark the given request as cancelled.
//
static void _cancel_request(_server_request_t *hreq, const char *error)
{
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    if (!hreq->cancelled) {
        hreq->cancelled = true;
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%" PRIu64 "][L%" PRIu64 "] Cancelling HTTP Request msg-id=%" PRIu64 ": %s", hconn->conn_id,
               hconn->out_link_id, hreq->base.msg_id, error);

        // destroy the associated codec state so we can continue receiving new requests from remote routers.
        if (hreq->base.lib_rs) {
            h1_codec_request_state_cancel(hreq->base.lib_rs);
        }
    }
}

// Run time cleanup of a completed (or cancelled) request. This will free hreq.
//
static void _finalize_request(_server_request_t *hreq)
{
    assert(hreq);
    qdr_http1_connection_t *hconn = hreq->base.hconn;

    // force send complete to prevent further calls to qdr_http1_server_core_link_deliver()
    //
    qd_message_t *msg = hreq->request_dlv ? qdr_delivery_message(hreq->request_dlv) : 0;
    if (msg && !qd_message_send_complete(msg)) {
        qd_message_set_send_complete(msg);
        if (hconn->out_link)
            qdr_link_complete_sent_message(qdr_http1_adaptor->core, hconn->out_link);
    }

    // Settle the request message's delivery
    //
    if (!hreq->request_settled) {
        hreq->request_settled = true;
        if (hreq->cancelled) {
            // Signal error to client. Do not overwrite a pre-existing error code
            if (hreq->request_dispo == 0 || hreq->request_dispo == PN_ACCEPTED)
                hreq->request_dispo = (_is_io_in_progress(hreq) || _is_io_complete(hreq)) ? PN_MODIFIED : PN_RELEASED;
        }
        if (hreq->request_dlv) {
            assert(hreq->request_dispo != 0);  // error: expected to be set
            qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                              hreq->request_dlv,
                                              hreq->request_dispo,
                                              true,  // settled
                                              0,     // delivery state
                                              false);
        }
    }

    // cancel any in-flight response deliveries
    //
    _server_response_msg_t *rmsg = DEQ_HEAD(hreq->responses);
    while (rmsg) {
        if (rmsg->msg && !qd_message_receive_complete(rmsg->msg)) {
            qd_message_set_aborted(rmsg->msg);  // is this necessary?
            qd_message_set_discard(rmsg->msg, true);
            qd_message_Q2_holdoff_disable(rmsg->msg);
            if (rmsg->dlv) {
                qdr_delivery_continue(qdr_http1_adaptor->core, rmsg->dlv, true);  // true == settled
            }
        }
        rmsg = DEQ_NEXT(rmsg);
    }

    _server_request_free(hreq);
    if (hconn->out_link)
        qdr_link_flow(qdr_http1_adaptor->core, hconn->out_link, 1, false);
}

// Called via qdr_connection_process() to close the connection. On return from
// this function the qdr_conn instance is no longer accessible and the core
// will never activate this hconn again.
//
void qdr_http1_server_core_conn_close(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn)
{
    // prevent activation by core thread
    sys_mutex_lock(&qdr_http1_adaptor->lock);
    qdr_connection_t *qdr_conn = hconn->qdr_conn;
    qdr_connection_set_context(hconn->qdr_conn, 0);
    hconn->qdr_conn = 0;
    sys_mutex_unlock(&qdr_http1_adaptor->lock);
    // the core thread can no longer activate this connection

    hconn->oper_status = QD_CONN_OPER_DOWN;
    _teardown_server_links(hconn);
    qdr_connection_closed(qdr_conn);
    qdr_http1_close_connection(hconn, 0);

    // it is expected that this callback is the final callback before returning
    // from qdr_connection_process(). Since qdr_conn is no longer available
    // qdr_connection_process() can never be called again for this hconn.
}

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

#include "observers/private.h"
#include "decoders/http1/http1_decoder.h"
#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/connection_counters.h"

#include <inttypes.h>

struct http1_request_state_t {
    DEQ_LINKS(http1_request_state_t);
    vflow_record_t *vflow;
    uint64_t        client_body_octets;  // total bytes received in client request msg body
    uint64_t        server_body_octets;  // total bytes received in server response msg body
    bool            latency_done:1;      // true: vflow latency timing complete
};
ALLOC_DECLARE(http1_request_state_t);
ALLOC_DEFINE(http1_request_state_t);


/*
 * HTTP/1.x decoder callbacks
 */


// A new request has arrived, start observing it
//
static int rx_request(qd_http1_decoder_connection_t *hconn, const char *method, const char *target, uint32_t version_major, uint32_t version_minor, uintptr_t *request_context)
{
    qdpo_transport_handle_t *th = (qdpo_transport_handle_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(th);

    qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG,
           "[C%" PRIu64 "] HTTP/1.1 observer rx_request(method=%s target=%s version=%"PRIu32".%"PRIu32")",
           th->conn_id, method, target, version_major, version_minor);

    http1_request_state_t *hreq = new_http1_request_state_t();
    ZERO(hreq);
    DEQ_ITEM_INIT(hreq);
    hreq->vflow = vflow_start_record(VFLOW_RECORD_BIFLOW_APP, th->vflow);
    vflow_set_string(hreq->vflow, VFLOW_ATTRIBUTE_PROTOCOL, version_minor == 1 ? "HTTP/1.1" : "HTTP/1.0");
    vflow_set_string(hreq->vflow, VFLOW_ATTRIBUTE_METHOD, method);
    vflow_set_uint64(hreq->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_set_uint64(hreq->vflow, VFLOW_ATTRIBUTE_OCTETS_REVERSE, 0);
    vflow_latency_start(hreq->vflow);
    hreq->latency_done = false;
    DEQ_INSERT_TAIL(th->http1.requests, hreq);
    *request_context = (uintptr_t) hreq;
    return 0;
}


static int rx_response(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, int status_code, const char *reason_phrase, uint32_t version_major, uint32_t version_minor)
{
    qdpo_transport_handle_t *th = (qdpo_transport_handle_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(th);

    qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG,
           "[C%" PRIu64 "] HTTP/1.1 observer rx_response(status=%d reason=%s version=%"PRIu32".%"PRIu32")",
           th->conn_id, status_code, reason_phrase, version_major, version_minor);

    http1_request_state_t *hreq = (http1_request_state_t *) request_context;
    assert(hreq);

    // stop latency even if the request is not complete (1xx response code)
    if (!hreq->latency_done) {
        vflow_latency_end(hreq->vflow, VFLOW_ATTRIBUTE_LATENCY);
        hreq->latency_done = true;
    }

    if (status_code / 100 != 1) {  // terminal response code
        char status_code_str[16];
        snprintf(status_code_str, sizeof(status_code_str), "%d", status_code);
        vflow_set_string(hreq->vflow, VFLOW_ATTRIBUTE_RESULT, status_code_str);
        vflow_set_string(hreq->vflow, VFLOW_ATTRIBUTE_REASON, reason_phrase);
    }

    return 0;
}


static int rx_body(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client, const unsigned char *body, size_t length)
{
    http1_request_state_t *hreq = (http1_request_state_t *) request_context;
    assert(hreq);

    if (from_client) {
        hreq->client_body_octets += length;
        vflow_set_uint64(hreq->vflow, VFLOW_ATTRIBUTE_OCTETS, hreq->client_body_octets);
    } else {
        hreq->server_body_octets += length;
        vflow_set_uint64(hreq->vflow, VFLOW_ATTRIBUTE_OCTETS_REVERSE, hreq->server_body_octets);
    }

    return 0;
}


static int transaction_complete(qd_http1_decoder_connection_t *hconn, uintptr_t request_context)
{
    qdpo_transport_handle_t *th = (qdpo_transport_handle_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(th);

    qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer transaction complete", th->conn_id);

    http1_request_state_t *hreq = (http1_request_state_t *) request_context;
    assert(hreq);

    DEQ_REMOVE(th->http1.requests, hreq);
    vflow_end_record(hreq->vflow);
    free_http1_request_state_t(hreq);
    return 0;
}


static void protocol_error(qd_http1_decoder_connection_t *hconn, const char *reason)
{
    qdpo_transport_handle_t *th = (qdpo_transport_handle_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(th);

    // only complain if we are failing while there are active requests.  It may be that the TCP stream is not
    // carrying HTTP/1.x data.

    if (DEQ_SIZE(th->http1.requests)) {
        qd_log(LOG_HTTP1_OBSERVER, QD_LOG_ERROR,
               "[C%" PRIu64 "] HTTP/1.1 observer disabled due to protocol error: %s", th->conn_id, reason);
    } else {
        qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer protocol error: %s", th->conn_id, reason);
    }

    // An error code will be returned to the http1_observe() call and this observer will be cleaned up there.
}


static qd_http1_decoder_config_t decoder_config = {
    .rx_request = rx_request,
    .rx_response = rx_response,
    // .rx_header = rx_header,
    // .rx_headers_done = rx_headers_done,
    .rx_body = rx_body,
    // .message_done = message_done,
    .transaction_complete = transaction_complete,
    .protocol_error = protocol_error
};


static void http1_observe(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    assert(th->http1.decoder);

    int rc = qd_http1_decoder_connection_rx_data(th->http1.decoder, from_client, data, length);
    if (rc) {
        qdpo_http1_final(th);
    }
}


void qdpo_http1_init(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer initialized", th->conn_id);

    th->protocol  = QD_PROTOCOL_HTTP1;
    th->observe   = http1_observe;

    memset(&th->http1, 0, sizeof(th->http1));
    DEQ_INIT(th->http1.requests);
    th->http1.decoder = qd_http1_decoder_connection(&decoder_config, (uintptr_t) th);

    // adjust the router's active protocol connection counter since we are replacing the parent TCP connection with an
    // HTTP/1.x connection
    qd_connection_counter_dec(QD_PROTOCOL_TCP);
    qd_connection_counter_inc(QD_PROTOCOL_HTTP1);
}


void qdpo_http1_final(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP1_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer finalized", th->conn_id);

    if (th->observe) {
        th->observe = 0;
        qd_http1_decoder_connection_free(th->http1.decoder);
        th->http1.decoder = 0;
        http1_request_state_t *hreq = DEQ_HEAD(th->http1.requests);
        while (hreq) {
            DEQ_REMOVE_HEAD(th->http1.requests);
            if (hreq->vflow) {
                vflow_end_record(hreq->vflow);
            }
            free_http1_request_state_t(hreq);
            hreq = DEQ_HEAD(th->http1.requests);
        }

        // restore the router's protocol connection counter for the parent TCP connection
        qd_connection_counter_dec(QD_PROTOCOL_HTTP1);
        qd_connection_counter_inc(QD_PROTOCOL_TCP);
    }
}


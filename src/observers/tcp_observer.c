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

#include "private.h"

#include <assert.h>
#include <inttypes.h>
#include <string.h>

#define MAX_SERVER_DATA_LEN 16384

// HTTP/2.0 protocol prefix (RFC 9113)
// "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
//
#define HTTP2_PREFIX_LEN 24
const uint8_t http2_prefix[HTTP2_PREFIX_LEN] = {
    0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
    0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a
};
STATIC_ASSERT(TCP_PREFIX_LEN >= HTTP2_PREFIX_LEN, "Increase TCP_PREFIX_LEN");


// Start a new protocol observer for the content of the tcp data stream
//
// This is a bit tricky since the handle uses an union for protocol state data we need to
// make a separate copy of the saved data for the new observer.
//
static void activate_inner(qdpo_transport_handle_t *th, qd_protocol_t inner_protocol, const unsigned char *data, size_t length)
{
    tcp_observer_state_t save = th->tcp;

    DEQ_INIT(th->tcp.server_data);  // prevent free by qdpo_tcp_final()
    qdpo_tcp_final(th);

    switch (inner_protocol) {
        case QD_PROTOCOL_HTTP1:
            qdpo_http1_init(th);
            break;
        case QD_PROTOCOL_HTTP2:
            qdpo_http2_init(th);
            break;
        default:
            assert(false);  // unsupported protocol
            break;
    }

    // Pass save data to new observer. The observer may deregister itself if a parse error is encountered so avoid
    // passing data if that occurs.

    th->observe(th, true, save.prefix, save.prefix_len);
    if (th->observe)
        th->observe(th, true, data, length);
    qd_buffer_t *buf = DEQ_HEAD(save.server_data);
    while (buf) {
        DEQ_REMOVE_HEAD(save.server_data);
        if (th->observe)
            th->observe(th, false, qd_buffer_base(buf), qd_buffer_size(buf));
        qd_buffer_free(buf);
        buf = DEQ_HEAD(save.server_data);
    }
}


static void tcp_observe(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] TCP observer classifying protocol: %zu %s octets", th->conn_id, length, from_client ? "client" : "server");

    if (from_client) {

        // fill up the protocol classification prefix buffer

        if (th->tcp.prefix_len < TCP_PREFIX_LEN) {
            size_t to_copy = MIN(length, TCP_PREFIX_LEN - th->tcp.prefix_len);
            memcpy(&th->tcp.prefix[th->tcp.prefix_len], data, to_copy);
            th->tcp.prefix_len += to_copy;
            data += to_copy;
            length -= to_copy;
        }

        // Check for HTTP/2.0

        size_t to_match = MIN(HTTP2_PREFIX_LEN, th->tcp.prefix_len);
        if (memcmp(th->tcp.prefix, http2_prefix, to_match) == 0) {
            // did we match the entire http2_prefix?
            if (to_match == HTTP2_PREFIX_LEN) {
                activate_inner(th, QD_PROTOCOL_HTTP2, data, length);
                return;
            } else {
                return; // Partial match for HTTP/2 prefix, wait for more
            }

        } else {

            // HTTP/2.0 check failed. Currently the only other supported protocol is HTTP/1.x so try that
            // unconditionally. If the HTTP/1.x observer fails to find HTTP/1.x traffic it will disable itself without
            // posting an error.

            activate_inner(th, QD_PROTOCOL_HTTP1, data, length);
            return;
        }

    } else {  // !from_client

        // hold server data until protocol is classified
        if (length + th->tcp.server_bytes < MAX_SERVER_DATA_LEN) {
            qd_buffer_list_append(&th->tcp.server_data, data, length);
            th->tcp.server_bytes += length;
            return;
        }

        // Server has sent too much data for us to hold while classifying. Protocol unknown.
        //
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] TCP observer response received before classification completed", th->conn_id);
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] TCP observer terminated: unknown protocol: %.2X %.2X %.2X %.2X ...",
           th->conn_id, th->tcp.prefix[0], th->tcp.prefix[1], th->tcp.prefix[2], th->tcp.prefix[3]);
    qdpo_tcp_final(th);
}


void qdpo_tcp_init(qdpo_transport_handle_t *th)
{
    th->protocol       = QD_PROTOCOL_TCP;
    th->observe        = tcp_observe;

    th->tcp.prefix_len   = 0;
    th->tcp.server_bytes = 0;
    DEQ_INIT(th->tcp.server_data);
    memset(th->tcp.prefix, 0, TCP_PREFIX_LEN);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] TCP observer initialized", th->conn_id);
}

void qdpo_tcp_final(qdpo_transport_handle_t *th)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] TCP observer finalized", th->conn_id);
    th->observe = 0;
    qd_buffer_list_free_buffers(&th->tcp.server_data);
}


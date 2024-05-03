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


// HTTP/2.0 protocol prefix (RFC 9113)
// "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
//
#define HTTP2_PREFIX_LEN 24
const uint8_t http2_prefix[HTTP2_PREFIX_LEN] = {
    0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
    0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a
};
STATIC_ASSERT(TCP_PREFIX_LEN >= HTTP2_PREFIX_LEN, "Increase TCP_PREFIX_LEN");


static void tcp_observe(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] TCP observer classifying protocol: %zu %s octets", th->conn_id, length, from_client ? "client" : "server");

    // wicked brain-dead classification just for POC

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
                qdpo_tcp_final(th);
                qdpo_http2_init(th);  // note: this overwrites tcp.prefix, so use http2_prefix:
                th->observe(th, from_client, http2_prefix, HTTP2_PREFIX_LEN);
                th->observe(th, from_client, data, length);
                return;
            } else {
                return; // Partial match for HTTP/2 prefix, wait for more
            }

        } else {

            // HTTP/2.0 check failed, try HTTP/1.x

            // hack for now. probably best to just start handing off the http1 observer and let it
            // try to find a proper HTTP/1.x request line.

            const char * const http1_commands[] = {
                "GET ", "HEAD ", "POST ", "PUT ", "DELETE ", 0
            };

            for (int i = 0; http1_commands[i] != 0; ++i) {
                size_t cmd_len  = strlen(http1_commands[i]);
                size_t to_match = MIN(th->tcp.prefix_len, cmd_len);
                if (memcmp(th->tcp.prefix, http1_commands[i], to_match) == 0) {
                    if (to_match == cmd_len) {
                        tcp_observer_state_t save = th->tcp;
                        qdpo_tcp_final(th);
                        qdpo_http1_init(th);
                        th->observe(th, from_client, save.prefix, save.prefix_len);
                        th->observe(th, from_client, data, length);
                        return;
                    } else {
                        return;  // partial match need more
                    }
                }
            }
        }

    } else {  // !from_client

        // Unexpected response data arrived before classification complete.
        // Protocol unknown
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
    th->tcp.prefix_len = 0;
    memset(th->tcp.prefix, 0, TCP_PREFIX_LEN);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] TCP observer initialized", th->conn_id);
}

void qdpo_tcp_final(qdpo_transport_handle_t *th)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] TCP observer finalized", th->conn_id);
    th->observe = 0;
}


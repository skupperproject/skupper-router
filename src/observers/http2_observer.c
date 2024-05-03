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

#include <inttypes.h>


static void http2_observe(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] HTTP/2.0 observer classifying protocol: %zu %s octets", th->conn_id, length, from_client ? "client" : "server");

}


void qdpo_http2_init(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,  "[C%" PRIu64 "] HTTP/2.0 observer initialized", th->conn_id);

    th->protocol = QD_PROTOCOL_HTTP2;
    th->observe = http2_observe;
    th->http2.tbd = 42;  // whatever
}


void qdpo_http2_final(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/2.0 observer finalized", th->conn_id);
    th->observe = 0;
}


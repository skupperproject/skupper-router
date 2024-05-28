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

#include <inttypes.h>

/**

   ### START RECORD
   ###
   # connection level record (listener based)
   vflow_start_record(VFLOW_RECORD_FLOW, li->vflow);
   # connection level record (connector)
   vflow_start_record(VFLOW_RECORD_FLOW, connector->vflow);
   # Client side request start flow
   vflow_start_record(VFLOW_RECORD_FLOW, hconn->vflow)
   # Server side request start flow
   vflow_start_record(VFLOW_RECORD_FLOW, hconn->vflow)

   ### END RECORD
   ###
   vflow_end_record(hreq->vflow);  End of request vflow

   # connection level flow end
   vflow_end_record(hconn->vflow);  End of connection vflow

   ### ATTRIBUTES CLIENT-FACING CONNECTION LEVEL
   ###
   ./http1_client.c:213:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
   ./http1_client.c:214:    vflow_add_rate(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
   ./http1_client.c:499:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);
   ./http1_client.c:597:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);
   ./http1_client.c:691:    qd_set_vflow_netaddr_string(hconn->vflow, hconn->raw_conn, true);
   # moves any connection condition to vflow on DISCONNECT:
   ./http1_client.c:713:    qd_set_condition_on_vflow(hconn->raw_conn, hconn->vflow);

   ### ATTRIBUTES SERVER-FACING CONNECTION LEVEL
   ###
   ./http1_server.c:276:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
   ./http1_server.c:277:    vflow_add_rate(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
   ./http1_server.c:603:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);
   ./http1_server.c:707:    vflow_set_uint64(hconn->vflow, VFLOW_ATTRIBUTE_OCTETS, hconn->in_http1_octets);
   # gets addr from raw conn
   ./http1_server.c:831:    qd_set_vflow_netaddr_string(hconn->vflow, hconn->raw_conn, false);
   # moves connection condition to vflow on DISCONNECT
   ./http1_server.c:856:    qd_set_condition_on_vflow(hconn->raw_conn, hconn->vflow);


   ### ATTRIBUTES CLIENT-FACING PER-REQUEST LEVEL
   ###
   ./http1_client.c:954:    vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
   ./http1_client.c:1209:   vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, in_octets);

   ./http1_client.c:955:    vflow_set_string(hreq->base.vflow, VFLOW_ATTRIBUTE_METHOD, method);
   ./http1_client.c:2074:   vflow_set_string(hreq->base.vflow, VFLOW_ATTRIBUTE_RESULT, code_str);
   ./http1_client.c:2076:   vflow_set_string(hreq->base.vflow, VFLOW_ATTRIBUTE_REASON, reason_phrase);

   ./http1_client.c:969:    vflow_serialize_identity(hreq->base.vflow, hreq->request_props);

   ./http1_client.c:959:    vflow_latency_start(hreq->base.vflow);
   ./http1_client.c:1855:   vflow_latency_end(hreq->base.vflow);

   ### ATTRIBUTES SERVER-FACING PER-REQUEST LEVEL
   ###
   ./http1_server.c:1381:    vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, in_octets);
   ./http1_server.c:1566:    vflow_set_uint64(hreq->base.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

   ./http1_server.c:1702:    vflow_set_ref_from_parsed(hreq->base.vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, value);

   ./http1_server.c:1572:    vflow_latency_start(hreq->base.vflow);
   ./http1_server.c:1119:    vflow_latency_end(hreq->base.vflow);

   ### WHAT ABOUT VFLOW_ATTRIBUTE_PROTOCOL ??


https://www.rfc-editor.org/rfc/rfc9112.html  (HTTP/1.1)
https://www.rfc-editor.org/rfc/rfc1945 (HTTP/1.0)
https://www.rfc-editor.org/rfc/rfc9110 (HTTP Semantics)

 */


static void http1_observe(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] HTTP/1.1 observer classifying protocol: %zu %s octets", th->conn_id, length, from_client ? "client" : "server");
}



void qdpo_http1_init(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer initialized", th->conn_id);

    th->protocol  = QD_PROTOCOL_HTTP1;
    th->observe   = http1_observe;
    th->http1.tbd = 42; // whatever;

}

void qdpo_http1_final(qdpo_transport_handle_t *th)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/1.1 observer finalized", th->conn_id);
    th->observe = 0;
}


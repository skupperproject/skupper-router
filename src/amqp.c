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

#include "qpid/dispatch/amqp.h"

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>

const char * const QD_MA_PREFIX  = "x-opt-qd.";
const char * const QD_MA_INGRESS = "x-opt-qd.ingress";
const char * const QD_MA_TRACE   = "x-opt-qd.trace";
const char * const QD_MA_TO      = "x-opt-qd.to";
const char * const QD_MA_CLASS   = "x-opt-qd.class";
const char * const QD_MA_STREAM  = "x-opt-qd.stream";
const int          QD_MA_MAX_KEY_LEN = 16;
const int          QD_MA_N_KEYS      = 5;  // max number of router annotations to send/receive
const int          QD_MA_FILTER_LEN  = 5;  // N tailing inbound entries to search for stripping

const char * const QD_CAPABILITY_ROUTER_CONTROL   = "qd.router";
const char * const QD_CAPABILITY_ROUTER_DATA      = "qd.router-data";
const char * const QD_CAPABILITY_EDGE_DOWNLINK    = "qd.router-edge-downlink";
const char * const QD_CAPABILITY_ANONYMOUS_RELAY  = "ANONYMOUS-RELAY";
const char * const QD_CAPABILITY_STREAMING_LINKS  = "qd.streaming-links";

const char * const QD_DYNAMIC_NODE_PROPERTY_ADDRESS = "x-opt-qd.address";

const char * const QD_CONNECTION_PROPERTY_PRODUCT_KEY           = "product";
const char * const QD_CONNECTION_PROPERTY_PRODUCT_VALUE         = "skupper-router";
const char * const QD_CONNECTION_PROPERTY_VERSION_KEY           = "version";
const char * const QD_CONNECTION_PROPERTY_COST_KEY              = "qd.inter-router-cost";
const char * const QD_CONNECTION_PROPERTY_CONN_ID               = "qd.conn-id";
const char * const QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY     = "failover-server-list";
const char * const QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY  = "network-host";
const char * const QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY     = "port";
const char * const QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY   = "scheme";
const char * const QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY = "hostname";
const char * const QD_CONNECTION_PROPERTY_ADAPTOR_KEY           = "qd.adaptor";
const char * const QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE     = "tcp";
const char * const QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY = "qd.annotations-version";

const char * const QD_TERMINUS_EDGE_ADDRESS_TRACKING = "_$qd.edge_addr_tracking";
const char * const QD_TERMINUS_ADDRESS_LOOKUP        = "_$qd.addr_lookup";
const char * const QD_TERMINUS_HEARTBEAT             = "_$qd.edge_heartbeat";

const qd_amqp_error_t QD_AMQP_OK = { 200, "OK" };
const qd_amqp_error_t QD_AMQP_CREATED = { 201, "Created" };
const qd_amqp_error_t QD_AMQP_NO_CONTENT = { 204, "No Content" }; // This is the response code if the delete of a manageable entity was successful.
const qd_amqp_error_t QD_AMQP_BAD_REQUEST = { 400, "Bad Request" };
const qd_amqp_error_t QD_AMQP_FORBIDDEN = { 403, "Forbidden" };
const qd_amqp_error_t QD_AMQP_NOT_FOUND = { 404, "Not Found" };
const qd_amqp_error_t QD_AMQP_NOT_IMPLEMENTED = { 501, "Not Implemented"};

const char * const QD_AMQP_COND_INTERNAL_ERROR = "amqp:internal-error";
const char * const QD_AMQP_COND_NOT_FOUND = "amqp:not-found";
const char * const QD_AMQP_COND_UNAUTHORIZED_ACCESS = "amqp:unauthorized-access";
const char * const QD_AMQP_COND_DECODE_ERROR = "amqp:decode-error";
const char * const QD_AMQP_COND_RESOURCE_LIMIT_EXCEEDED = "amqp:resource-limit-exceeded";
const char * const QD_AMQP_COND_NOT_ALLOWED = "amqp:not-allowed";
const char * const QD_AMQP_COND_INVALID_FIELD = "amqp:invalid-field";
const char * const QD_AMQP_COND_NOT_IMPLEMENTED = "amqp:not-implemented";
const char * const QD_AMQP_COND_RESOURCE_LOCKED = "amqp:resource-locked";
const char * const QD_AMQP_COND_PRECONDITION_FAILED = "amqp:precondition-failed";
const char * const QD_AMQP_COND_RESOURCE_DELETED = "amqp:resource-deleted";
const char * const QD_AMQP_COND_ILLEGAL_STATE = "amqp:illegal-state";
const char * const QD_AMQP_COND_FRAME_SIZE_TOO_SMALL = "amqp:frame-size-too-small";
const char * const QD_AMQP_COND_CONNECTION_FORCED = "amqp:connection:forced";
const char * const QD_AMQP_COND_MESSAGE_SIZE_EXCEEDED = "amqp:link:message-size-exceeded";

const char * const QD_AMQP_PORT_STR = "5672";
const char * const QD_AMQPS_PORT_STR = "5671";

const char * const QD_AMQP_DFLT_PROTO = "tcp";

/// Obtains port number from protocol name using getservbyname_r
static inline int qd_getservbyname(const char *name, const char *proto) {
    struct servent  serv_info;
    struct servent *serv_info_res;
    enum { buf_len = 4096 };
    char buf[buf_len];

    int r = getservbyname_r(name, proto, &serv_info, buf, buf_len, &serv_info_res);
    if (r == 0 && serv_info_res != NULL) {
        return ntohs(serv_info.s_port);
    } else {
        return -1;
    }
}

int qd_port_int(const char *port_str) {
    char *endptr;
    unsigned long n;

    // empty string?
    if (*port_str == '\0') return -1;

    // digits from beginning to end?
    errno = 0;
    n = strtoul(port_str, &endptr, 10);
    if (*endptr == '\0') {
        if (!errno && n >= 0 && n <= 0xFFFF)
            return n;
        else
            return -1;
    }

    // digits halfway?
    if (endptr != port_str) return -1;

    // resolve service port
    const int r = qd_getservbyname(port_str, QD_AMQP_DFLT_PROTO);
    if (r != -1) return r;

    // amqp(s) not defined in /etc/services?
    if (!strcmp(port_str, "amqp")) return QD_AMQP_PORT_INT;
    if (!strcmp(port_str, "amqps")) return QD_AMQPS_PORT_INT;

    return -1;
}

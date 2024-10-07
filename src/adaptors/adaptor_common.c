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
#include "adaptor_common.h"
#include "tcp/tcp_adaptor.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/platform.h"

#include <proton/netaddr.h>

#include <inttypes.h>
#include <sys/socket.h>
#include <stdatomic.h>

ALLOC_DEFINE(qd_adaptor_config_t);
const char* LISTENER_OBSERVER_AUTO  = "auto";
const char* LISTENER_OBSERVER_HTTP1 = "http1";
const char* LISTENER_OBSERVER_HTTP2 = "http2";
const char* LISTENER_OBSERVER_NONE = "none";

void qd_free_adaptor_config(qd_adaptor_config_t *config)
{
    if (!config)
        return;
    free(config->name);
    free(config->address);
    free(config->host);
    free(config->port);
    free(config->site_id);
    free(config->host_port);
    free(config->ssl_profile_name);
    free_qd_adaptor_config_t(config);
}

#define CHECK() if (qd_error_code()) goto error

qd_observer_t get_listener_observer(const char *observer)
{
    if (strcmp(observer, LISTENER_OBSERVER_NONE) == 0) {
        return OBSERVER_NONE;
    }

    if (strcmp(observer, LISTENER_OBSERVER_HTTP1) == 0) {
        return OBSERVER_HTTP1;
    }

    if (strcmp(observer, LISTENER_OBSERVER_HTTP2) == 0) {
        return OBSERVER_HTTP2;
    }

    return OBSERVER_AUTO;
}

qd_error_t qd_load_adaptor_config(qdr_core_t *core, qd_adaptor_config_t *config, qd_entity_t *entity)
{
    char *config_address;
    qd_error_clear();
    config->name    = qd_entity_opt_string(entity, "name", 0);                         CHECK();
    config->host    = qd_entity_get_string(entity, "host");                            CHECK();
    config->port    = qd_entity_get_string(entity, "port");                            CHECK();
    config_address  = qd_entity_get_string(entity, "address");                         CHECK();
    config->site_id = qd_entity_opt_string(entity, "siteId", 0);                       CHECK();
    config->ssl_profile_name  = qd_entity_opt_string(entity, "sslProfile", 0);         CHECK();
    config->authenticate_peer = qd_entity_opt_bool(entity, "authenticatePeer", false); CHECK();
    config->verify_host_name  = qd_entity_opt_bool(entity, "verifyHostname", false);   CHECK();
    char *observer =            qd_entity_opt_string(entity, "observer", "auto");      CHECK();
    config->observer =          get_listener_observer(observer);
    free(observer);
    config->backlog =           qd_entity_opt_long(entity, "backlog", 0);
    CHECK();
    if (config->backlog <= 0 || config->backlog > SOMAXCONN)
        config->backlog = SOMAXCONN;

    int hplen = strlen(config->host) + strlen(config->port) + 2;
    config->host_port = malloc(hplen);
    snprintf(config->host_port, hplen, "%s:%s", config->host, config->port);

    //
    // If this router is annotated with a van-id, add the van-id to the address
    //
    const char *van_id = qdr_core_van_id(core);
    if (!!van_id) {
        char *address = (char*) malloc(strlen(config_address) + strlen(van_id) + 2);
        strcpy(address, van_id);
        strcat(address, "/");
        strcat(address, config_address);
        config->address = address;
        free(config_address);
    } else {
        config->address = config_address;
    }

    return QD_ERROR_NONE;

error:
    return qd_error_code();
}

size_t qd_raw_conn_get_address_buf(pn_raw_connection_t *pn_raw_conn, char *buf, size_t buflen)
{
    assert(pn_raw_conn);
    assert(buflen);

    buf[0] = '\0';

    const pn_netaddr_t *netaddr = pn_raw_connection_remote_addr(pn_raw_conn);
    if (!netaddr)
        return 0;

    int len = pn_netaddr_str(netaddr, buf, buflen);
    if (len < 0)
        return 0;
    if (len >= buflen) {  // truncated
        len = buflen - 1;
        buf[len] = '\0';
    }

    return (size_t) len;
}


char *qd_raw_conn_get_address(pn_raw_connection_t *pn_raw_conn)
{
    char result[1024];
    qd_raw_conn_get_address_buf(pn_raw_conn, result, sizeof(result));
    return strdup(result);
}

void qd_set_vflow_netaddr_string(vflow_record_t *vflow, pn_raw_connection_t *pn_raw_conn, bool ingress)
{
    char                remote_host[200];
    char                remote_port[50];
    const pn_netaddr_t *na =
        ingress ? pn_raw_connection_remote_addr(pn_raw_conn) : pn_raw_connection_local_addr(pn_raw_conn);
    if (pn_netaddr_host_port(na, remote_host, 200, remote_port, 50) == 0) {
        vflow_set_string(vflow, ingress ? VFLOW_ATTRIBUTE_SOURCE_HOST : VFLOW_ATTRIBUTE_PROXY_HOST, remote_host);
        vflow_set_string(vflow, ingress ? VFLOW_ATTRIBUTE_SOURCE_PORT : VFLOW_ATTRIBUTE_PROXY_PORT, remote_port);
    }
}

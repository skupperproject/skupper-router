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

#include "qd_listener.h"
#include "qd_connection.h"
#include "http.h"

#include "qpid/dispatch/server.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/tls_amqp.h"

#include <proton/event.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>

#include <inttypes.h>

ALLOC_DEFINE(qd_listener_t);

static const int BACKLOG = 50;  /* Listening backlog */

static void on_accept(pn_event_t *e, qd_listener_t *listener)
{
    assert(pn_event_type(e) == PN_LISTENER_ACCEPT);
    pn_listener_t *pn_listener = pn_event_listener(e);

    qd_connection_t *ctx = new_qd_connection_t();
    ZERO(ctx);
    qd_connection_init(ctx, listener->server, &listener->config, 0, listener);
    qd_log(LOG_SERVER, QD_LOG_DEBUG, "[C%" PRIu64 "]: Accepting incoming connection to '%s'",
           ctx->connection_id, ctx->listener->config.host_port);
    /* Asynchronous accept, configure the transport on PN_CONNECTION_BOUND */
    pn_listener_accept(pn_listener, ctx->pn_conn);
    // Note well: at this point the connection may now be scheduled on another thread.
    // Do not access it.
 }


static void handle_listener(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qd_listener_t *li = (qd_listener_t*) context;
    const char *host_port = li->config.host_port;
    const char *port = li->config.port;

    switch (pn_event_type(e)) {

    case PN_LISTENER_OPEN: {

        if (strcmp(port, "0") == 0) {
            // If a 0 (zero) is specified for a port, get the actual listening port from the listener.
            pn_listener_t *l = pn_event_listener(e);
            const pn_netaddr_t *na = pn_listener_addr(l);
            char str[PN_MAX_ADDR] = "";
            pn_netaddr_str(na, str, sizeof(str));
            // "str" contains the host and port on which this listener is listening.
            if (li->config.name)
                    qd_log(LOG_SERVER, QD_LOG_INFO, "Listening on %s (%s)", str, li->config.name);
            else
                    qd_log(LOG_SERVER, QD_LOG_INFO, "Listening on %s", str);
        }
        else {
            qd_log(LOG_SERVER, QD_LOG_INFO, "Listening on %s", host_port);
        }

        break;
    }

    case PN_LISTENER_ACCEPT:
        qd_log(LOG_SERVER, QD_LOG_DEBUG, "Accepting connection on %s", host_port);
        on_accept(e, li);
        break;

    case PN_LISTENER_CLOSE:
        if (li->pn_listener) {
            pn_condition_t *cond = pn_listener_condition(li->pn_listener);
            if (pn_condition_is_set(cond)) {
                    qd_log(LOG_SERVER, QD_LOG_ERROR, "Listener error on %s: %s (%s)", host_port,
                           pn_condition_get_description(cond), pn_condition_get_name(cond));
                    if (li->exit_on_error) {
                        qd_log(LOG_SERVER, QD_LOG_CRITICAL, "Shutting down, required listener failed %s",
                               host_port);
                        exit(1);
                }
            } else {
                qd_log(LOG_SERVER, QD_LOG_DEBUG, "Listener closed on %s", host_port);
            }
            pn_listener_set_context(li->pn_listener, 0);
            li->pn_listener = 0;
            qd_listener_decref(li);
        }
        break;

    default:
        break;
    }
}

qd_listener_t *qd_listener(qd_server_t *server)
{
    qd_listener_t *li = new_qd_listener_t();
    if (!li) return 0;
    ZERO(li);
    sys_atomic_init(&li->ref_count, 1);
    sys_atomic_init(&li->connection_count, 0);
    li->server      = server;
    li->http = NULL;
    li->type.context = li;
    li->type.handler = &handle_listener;
    return li;
}

static bool qd_listener_listen_pn(qd_listener_t *li)
{
    li->pn_listener = pn_listener();
    if (li->pn_listener) {
        pn_listener_set_context(li->pn_listener, &li->type);
        pn_proactor_listen(qd_server_proactor(li->server), li->pn_listener, li->config.host_port,
                           BACKLOG);
        sys_atomic_inc(&li->ref_count); /* In use by proactor, PN_LISTENER_CLOSE will dec */
        /* Listen is asynchronous, log "listening" message on PN_LISTENER_OPEN event */
    } else {
        qd_log(LOG_SERVER, QD_LOG_CRITICAL, "No memory listening on %s", li->config.host_port);
     }
    return li->pn_listener;
}

static bool qd_listener_listen_http(qd_listener_t *li)
{
    qd_http_server_t *lws_http = qd_server_http(li->server);
    if (lws_http) {
        /* qd_lws_listener holds a reference to li, will decref when closed */
        qd_http_server_listen(lws_http, li);
        return li->http;
    } else {
        qd_log(LOG_SERVER, QD_LOG_ERROR, "No HTTP support to listen on %s", li->config.host_port);
        return false;
    }
}

bool qd_listener_listen(qd_listener_t *li)
{
    if (li->pn_listener || li->http) /* Already listening */
        return true;
    return li->config.http ? qd_listener_listen_http(li) : qd_listener_listen_pn(li);
}


void qd_listener_decref(qd_listener_t *li)
{
    if (li && sys_atomic_dec(&li->ref_count) == 1) {
        if (!!li->vflow_record) {
            vflow_end_record(li->vflow_record);
            li->vflow_record = 0;
        }
        qd_server_config_free(&li->config);
        qd_tls_config_decref(li->tls_config);

        free_qd_listener_t(li);
    }
}

qd_lws_listener_t *qd_listener_http(const qd_listener_t *li)
{
    return li->http;
}

const qd_server_config_t *qd_listener_config(const qd_listener_t *li)
{
    return &li->config;
}


void qd_listener_add_connection(qd_listener_t *li, qd_connection_t *ctx)
{
    sys_atomic_inc(&li->ref_count);
    ctx->listener = li;
}

void qd_listener_remove_connection(qd_listener_t *li, qd_connection_t *ctx)
{
    assert(ctx->listener == li);
    ctx->listener = 0;
    qd_listener_decref(li);
}

void qd_listener_add_link(qd_listener_t *li)
{
    if (!!li->vflow_record) {
        uint32_t count = sys_atomic_inc(&li->connection_count) + 1;
        vflow_set_uint64(li->vflow_record, VFLOW_ATTRIBUTE_LINK_COUNT, count);
    }
}

void qd_listener_remove_link(qd_listener_t *li)
{
    if (!!li->vflow_record) {
        uint32_t count = sys_atomic_dec(&li->connection_count) - 1;
        vflow_set_uint64(li->vflow_record, VFLOW_ATTRIBUTE_LINK_COUNT, count);
    }
}

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
#include "private.h"
#include "qd_connection.h"
#include "http.h"
#include "qpid/dispatch/entity.h"

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

static void handle_listener_ssl_profile_mgmt_update(const qd_tls_config_t *config, void *context);


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

qd_listener_t *qd_listener_create(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_error_clear();

    qd_listener_t *li = new_qd_listener_t();
    if (!li) {
        char *name = qd_entity_opt_string(entity, "name", "UNKNOWN");
        qd_error(QD_ERROR_CONFIG, "Failed to create listener %s: resource allocation failed", name);
        free(name);
        return 0;
    }

    ZERO(li);
    DEQ_ITEM_INIT(li);
    sys_atomic_init(&li->ref_count, 1);
    sys_atomic_init(&li->connection_count, 0);
    li->server       = qd_dispatch_get_server(qd);
    li->http         = NULL;
    li->type.context = li;
    li->type.handler = &handle_listener;

    if (qd_server_config_load(&li->config, entity, true) != QD_ERROR_NONE) {
        qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create listener: %s", qd_error_message());
        qd_listener_decref(li);
        return 0;
    }

    char *fol = qd_entity_opt_string(entity, "failoverUrls", 0);
    if (fol) {
        li->config.failover_list = qd_failover_list(fol);
        free(fol);
        if (li->config.failover_list == 0) {
            qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create listener, bad failover list: %s",
                   qd_error_message());
            qd_listener_decref(li);
            return 0;
        }
    } else {
        li->config.failover_list = 0;
    }

    if (li->config.ssl_profile_name) {
        li->tls_config = qd_tls_config(li->config.ssl_profile_name,
                                       QD_TLS_TYPE_PROTON_AMQP,
                                       QD_TLS_CONFIG_SERVER_MODE,
                                       li->config.verify_host_name,
                                       li->config.ssl_require_peer_authentication);
        if (!li->tls_config) {
            // qd_tls_config() sets qd_error_message():
            qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Failed to configure TLS for Listener %s: %s",
                   li->config.name, qd_error_message());
            qd_listener_decref(li);
            return 0;
        }

        li->tls_ordinal              = qd_tls_config_get_ordinal(li->tls_config);
        li->tls_oldest_valid_ordinal = qd_tls_config_get_oldest_valid_ordinal(li->tls_config);
        qd_tls_config_register_update_callback(li->tls_config, li,
                                               handle_listener_ssl_profile_mgmt_update);
    }

    //
    // Set up the vanflow record for this listener (ROUTER_ACCESS).
    // Do this only for router-to-router links: not mgmt/metrics/healthz/websockets listeners
    //
    if (strcmp(li->config.role, "inter-router") == 0 ||
        strcmp(li->config.role, "edge") == 0 ||
        strcmp(li->config.role, "inter-edge") == 0) {
        li->vflow_record = vflow_start_record(VFLOW_RECORD_ROUTER_ACCESS, 0);
        vflow_set_string(li->vflow_record, VFLOW_ATTRIBUTE_NAME, li->config.name);
        vflow_set_string(li->vflow_record, VFLOW_ATTRIBUTE_ROLE, li->config.role);
        vflow_set_uint64(li->vflow_record, VFLOW_ATTRIBUTE_LINK_COUNT, 0);
    }

    return li;
}


void qd_listener_delete(qd_listener_t *li, bool on_shutdown)
{
    if (li) {
        // Disable the listener to prevent new incoming connections. This is an asynchronous "clean close": the
        // Proton/LWS listeners will still hold their respective reference counts to this listener until the close
        // request has been processed (on another thread).
        //
        if (li->pn_listener) {
            pn_listener_close(li->pn_listener);
            if (on_shutdown) {
                // DISPATCH-1508: qd_listener_delete() does an asynchronous "clean close" which requires proton to
                // invoke the listeners event handler with a PN_LISTENER_CLOSE event on a proactor thread in order to
                // release the reference count to the listener held by the pn_listener. But since the router is going
                // down those events will not occur which means the pn_listeners reference count will not be cleaned
                // up. In this case we have to manually remove the reference before deleting the listener:
                //
                pn_listener_set_context(li->pn_listener, 0);
                li->pn_listener = 0;
                qd_listener_decref(li);  // for the pn_listener's context
            }

        } else if (li->http) {
            qd_lws_listener_close(li->http);
        }

        // immediately prevent further notfications of sslProfile config changes
        qd_tls_config_unregister_update_callback(li->tls_config);
        qd_listener_decref(li);
    }
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
        if (li->tls_config && li->vflow_record) {
            vflow_set_uint64(li->vflow_record, VFLOW_ATTRIBUTE_ACTIVE_TLS_ORDINAL, li->tls_ordinal);
        }
    }
}

void qd_listener_remove_link(qd_listener_t *li)
{
    if (!!li->vflow_record) {
        uint32_t count = sys_atomic_dec(&li->connection_count) - 1;
        vflow_set_uint64(li->vflow_record, VFLOW_ATTRIBUTE_LINK_COUNT, count);
    }
}


// This function searches for connections that need to be closed due to violating the parent listeners TLS oldest valid
// ordinal
//
static void expired_ordinal_connection_scrubber(qd_connection_t *qd_conn, void *context, bool discard)
{
    qd_listener_t *li = (qd_listener_t *)context;

    if (!discard) {
        if (qd_conn->pn_conn) {
            assert(qd_conn->pn_conn == (pn_connection_t *)sys_thread_proactor_context());
            qd_log(LOG_SERVER, QD_LOG_DEBUG,
                   "[C%"PRIu64"] Closing due to expired TLS ordinal", qd_conn->connection_id);
            pn_connection_close(qd_conn->pn_conn);
        }

        // Pre-allocate the deferred call in order to avoid allocation while holding the adaptor lock
        qd_deferred_call_t *dc = qd_connection_new_qd_deferred_call_t(expired_ordinal_connection_scrubber, li);

        // now search the global list of AMQP connections for other connections that need to be closed
        sys_mutex_lock(&amqp_adaptor.lock);
        qd_connection_t *next_conn = DEQ_NEXT(qd_conn);

        // Check if qd_conn has been removed from the global list since this function was scheduled. If it was restart
        // the scan otherwise we might leave up connections that should be removed.
        if (next_conn == 0 && DEQ_TAIL(amqp_adaptor.conn_list) != qd_conn) {
            next_conn = DEQ_HEAD(amqp_adaptor.conn_list);
        }

        // find the next victim:
        uint64_t tls_ordinal;
        while (next_conn && (next_conn->listener != li ||
                             next_conn->pn_conn == 0 ||
                             !qd_connection_get_tls_ordinal(next_conn, &tls_ordinal) ||
                             tls_ordinal >= li->tls_oldest_valid_ordinal)) {
            next_conn = DEQ_NEXT(next_conn);
        }
        if (next_conn) {
            qd_connection_invoke_deferred_impl(next_conn, dc);
            dc = 0;  // ownership passed, avoid free
        }
        sys_mutex_unlock(&amqp_adaptor.lock);

        if (dc) {
            qd_connection_free_qd_deferred_call_t(dc);
        }
    }
}


// Handler invoked by mgmt thread whenever the sslProfile is updated for a given qd_listener_t. Check for changes to the
// sslProfile oldestValidOrdinal attribute.  Note this is called with the sslProfile lock held to prevent new
// connections from being activated until after this call returns.
//
static void handle_listener_ssl_profile_mgmt_update(const qd_tls_config_t *config, void *context)
{
    uint64_t new_ordinal = qd_tls_config_get_ordinal(config);
    uint64_t new_oldest_ordinal = qd_tls_config_get_oldest_valid_ordinal(config);
    qd_listener_t *li           = (qd_listener_t *) context;
    li->tls_ordinal = new_ordinal;

    // destroy all connections whose connectors use an expired TLS ordinal:
    if (new_oldest_ordinal > li->tls_oldest_valid_ordinal) {

        qd_log(LOG_SERVER, QD_LOG_DEBUG,
               "Listeners %s new oldest valid TLS ordinal: %"PRIu64", previous: %"PRIu64,
               li->config.name, new_oldest_ordinal, li->tls_oldest_valid_ordinal);

        li->tls_oldest_valid_ordinal = new_oldest_ordinal;

        // Pre-allocate the deferred call in order to avoid allocation while holding the adaptor lock
        qd_deferred_call_t *dc = qd_connection_new_qd_deferred_call_t(expired_ordinal_connection_scrubber, li);

        // find the first expired connection and schedule it to be closed. When that connection closes it will find the
        // next connection that needs closing. This process repeats until all connections have been dealt with.
        //
        sys_mutex_lock(&amqp_adaptor.lock);
        qd_connection_t *qd_conn = DEQ_HEAD(amqp_adaptor.conn_list);
        while (qd_conn) {
            if (qd_conn->listener == li) {
                uint64_t tls_ordinal;
                if (qd_connection_get_tls_ordinal(qd_conn, &tls_ordinal) && tls_ordinal < new_oldest_ordinal) {
                    qd_connection_invoke_deferred_impl(qd_conn, dc);
                    dc = 0;  // ownership passed, avoid free
                    break;
                }
            }
            qd_conn = DEQ_NEXT(qd_conn);
        }
        sys_mutex_unlock(&amqp_adaptor.lock);

        if (dc) {
            qd_connection_free_qd_deferred_call_t(dc);
        }
    }
}

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

#include "qd_connection.h"
#include "private.h"
#include "qd_connector.h"
#include "qd_listener.h"
#include "policy.h"
#include "container.h"

#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/proton_utils.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/tls_amqp.h"

#include <proton/proactor.h>
#include <proton/sasl.h>
#include <proton/netaddr.h>

#include <inttypes.h>


ALLOC_DEFINE(qd_deferred_call_t);
ALLOC_DEFINE_SAFE(qd_connection_t);

const char *MECH_EXTERNAL = "EXTERNAL";


/**
 * This function is set as the pn_transport->tracer and is invoked when proton tries to write the log message to pn_transport->tracer
 */
static void transport_tracer(pn_transport_t *transport, const char *message)
{
    qd_connection_t *ctx = (qd_connection_t*) pn_transport_get_context(transport);
    if (ctx) {
        // The PROTOCOL module is used exclusively for logging protocol related tracing. The protocol could be AMQP, HTTP, TCP etc.
        qd_log(LOG_PROTOCOL, QD_LOG_DEBUG, "[C%" PRIu64 "]:%s", ctx->connection_id, message);
    }
}

void qd_connection_transport_tracer(pn_transport_t *transport, const char *message)
{
    qd_connection_t *ctx = (qd_connection_t*) pn_transport_get_context(transport);
    if (ctx) {
        // Unconditionally write the log at DEBUG level to the log file.
        qd_log_impl_v1(LOG_PROTOCOL, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "]:%s",
                       ctx->connection_id, message);
    }
}


/* Wake function for proactor-managed connections */
static void connection_wake(qd_connection_t *ctx)
{
    if (ctx->pn_conn) pn_connection_wake(ctx->pn_conn);
}


static void decorate_connection(qd_connection_t *ctx, const qd_server_config_t *config)
{
    qd_server_t     *qd_server = ctx->server;
    pn_connection_t *conn      = ctx->pn_conn;
    //
    // Set the container name
    //
    pn_connection_set_container(conn, qd_server_get_container_name(qd_server));

    //
    // Advertise our container capabilities.
    //
    {
        // offered: extension capabilities this router supports
        pn_data_t *ocaps = pn_connection_offered_capabilities(conn);
        pn_data_put_array(ocaps, false, PN_SYMBOL);
        pn_data_enter(ocaps);
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_ANONYMOUS_RELAY), (char*) QD_CAPABILITY_ANONYMOUS_RELAY));
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_STREAMING_LINKS), (char*) QD_CAPABILITY_STREAMING_LINKS));
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_CONNECTION_TRUNKING), (char*) QD_CAPABILITY_CONNECTION_TRUNKING));
        if (!!config && strcmp(config->role, "inter-edge") == 0) {
            pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_INTER_EDGE), (char*) QD_CAPABILITY_INTER_EDGE));
        }
        pn_data_exit(ocaps);

        // The desired-capability list defines which extension capabilities the
        // sender MAY use if the receiver offers them (i.e., they are in the
        // offered-capabilities list received by the sender of the
        // desired-capabilities). The sender MUST NOT attempt to use any
        // capabilities it did not declare in the desired-capabilities
        // field.
        ocaps = pn_connection_desired_capabilities(conn);
        pn_data_put_array(ocaps, false, PN_SYMBOL);
        pn_data_enter(ocaps);
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_ANONYMOUS_RELAY), (char*) QD_CAPABILITY_ANONYMOUS_RELAY));
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_STREAMING_LINKS), (char*) QD_CAPABILITY_STREAMING_LINKS));
        pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_CONNECTION_TRUNKING), (char*) QD_CAPABILITY_CONNECTION_TRUNKING));
        if (!!config && strcmp(config->role, "inter-edge") == 0) {
            pn_data_put_symbol(ocaps, pn_bytes(strlen(QD_CAPABILITY_INTER_EDGE), (char*) QD_CAPABILITY_INTER_EDGE));
        }
        pn_data_exit(ocaps);
    }

    //
    // Create the connection properties map
    //
    pn_data_put_map(pn_connection_properties(conn));
    pn_data_enter(pn_connection_properties(conn));

    pn_data_put_symbol(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_PRODUCT_KEY), QD_CONNECTION_PROPERTY_PRODUCT_KEY));
    pn_data_put_string(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_PRODUCT_VALUE), QD_CONNECTION_PROPERTY_PRODUCT_VALUE));

    pn_data_put_symbol(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_VERSION_KEY), QD_CONNECTION_PROPERTY_VERSION_KEY));
    pn_data_put_string(pn_connection_properties(conn),
                       pn_bytes(strlen(QPID_DISPATCH_VERSION), QPID_DISPATCH_VERSION));

    pn_data_put_symbol(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_CONN_ID), QD_CONNECTION_PROPERTY_CONN_ID));
    qd_connection_t *qd_conn = pn_connection_get_context(conn);
    pn_data_put_int(pn_connection_properties(conn), qd_conn->connection_id);

    if (config && config->inter_router_cost > 1) {
        pn_data_put_symbol(pn_connection_properties(conn),
                           pn_bytes(strlen(QD_CONNECTION_PROPERTY_COST_KEY), QD_CONNECTION_PROPERTY_COST_KEY));
        pn_data_put_int(pn_connection_properties(conn), config->inter_router_cost);
    }

    if (ctx->connector && ctx->connector->is_data_connector) {
        pn_data_put_symbol(pn_connection_properties(conn),
                           pn_bytes(strlen(QD_CONNECTION_PROPERTY_ROLE_KEY), QD_CONNECTION_PROPERTY_ROLE_KEY));
        pn_data_put_int(pn_connection_properties(conn), QDR_ROLE_INTER_ROUTER_DATA);
    }

    if (ctx->connector && (ctx->connector->is_data_connector || !!ctx->connector->ctor_config->data_connection_count)) {
        pn_data_put_symbol(pn_connection_properties(conn),
                           pn_bytes(strlen(QD_CONNECTION_PROPERTY_GROUP_CORRELATOR_KEY), QD_CONNECTION_PROPERTY_GROUP_CORRELATOR_KEY));
        pn_data_put_string(pn_connection_properties(conn),
                           pn_bytes(strnlen(ctx->group_correlator, QD_DISCRIMINATOR_SIZE - 1), ctx->group_correlator));
    }

    if (ctx->listener && !!ctx->listener->vflow_record) {
        pn_data_put_symbol(pn_connection_properties(conn),
                           pn_bytes(strlen(QD_CONNECTION_PROPERTY_ACCESS_ID), QD_CONNECTION_PROPERTY_ACCESS_ID));
        vflow_serialize_identity_pn(ctx->listener->vflow_record, pn_connection_properties(conn));
    }

    if (config) {
        if (strcmp(config->role, "inter-router") == 0 || strcmp(config->role, "edge") == 0) {
            pn_data_put_symbol(pn_connection_properties(conn),
                               pn_bytes(strlen(QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY),
                                        QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY));
            pn_data_put_int(pn_connection_properties(conn), QD_ROUTER_ANNOTATIONS_VERSION);
        }

        qd_failover_list_t *fol = config->failover_list;
        if (fol) {
            pn_data_put_symbol(pn_connection_properties(conn),
                               pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY), QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY));
            pn_data_put_list(pn_connection_properties(conn));
            pn_data_enter(pn_connection_properties(conn));
            int fol_count = qd_failover_list_size(fol);
            for (int i = 0; i < fol_count; i++) {
                pn_data_put_map(pn_connection_properties(conn));
                pn_data_enter(pn_connection_properties(conn));
                pn_data_put_symbol(pn_connection_properties(conn),
                                   pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY), QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY));
                pn_data_put_string(pn_connection_properties(conn),
                                   pn_bytes(strlen(qd_failover_list_host(fol, i)), qd_failover_list_host(fol, i)));

                pn_data_put_symbol(pn_connection_properties(conn),
                                   pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY), QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY));
                pn_data_put_string(pn_connection_properties(conn),
                                   pn_bytes(strlen(qd_failover_list_port(fol, i)), qd_failover_list_port(fol, i)));

                if (qd_failover_list_scheme(fol, i)) {
                    pn_data_put_symbol(pn_connection_properties(conn),
                                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY), QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY));
                    pn_data_put_string(pn_connection_properties(conn),
                                       pn_bytes(strlen(qd_failover_list_scheme(fol, i)), qd_failover_list_scheme(fol, i)));
                }

                if (qd_failover_list_hostname(fol, i)) {
                    pn_data_put_symbol(pn_connection_properties(conn),
                                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY), QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY));
                    pn_data_put_string(pn_connection_properties(conn),
                                       pn_bytes(strlen(qd_failover_list_hostname(fol, i)), qd_failover_list_hostname(fol, i)));
                }
                pn_data_exit(pn_connection_properties(conn));
            }
            pn_data_exit(pn_connection_properties(conn));
        }

        // Append any user-configured properties. conn_props is a pn_data_t PN_MAP
        // type. Append the map elements - not the map itself!
        //
        if (config->conn_props) {
            pn_data_t *outp = pn_connection_properties(conn);

            pn_data_rewind(config->conn_props);
            pn_data_next(config->conn_props);
            assert(pn_data_type(config->conn_props) == PN_MAP);
            const size_t count = pn_data_get_map(config->conn_props);
            pn_data_enter(config->conn_props);
            for (size_t i = 0; i < count / 2; ++i) {
                // key: the key must be of type Symbol.  The python agent has
                // validated the keys as ASCII strings, but the JSON converter does
                // not provide a Symbol type so all the keys in conn_props are
                // PN_STRING.
                pn_data_next(config->conn_props);
                assert(pn_data_type(config->conn_props) == PN_STRING);
                pn_data_put_symbol(outp, pn_data_get_string(config->conn_props));
                // put value
                pn_data_next(config->conn_props);
                qdpn_data_insert(outp, config->conn_props);
            }
        }
    }

    pn_data_exit(pn_connection_properties(conn));
}

/* Construct a new qd_connection. Thread safe.
 * Does not allocate any managed objects and therefore
 * does not take ENTITY_CACHE lock.
 */
void qd_connection_init(qd_connection_t *ctx, qd_server_t *server, const qd_server_config_t *config, qd_connector_t *connector, qd_listener_t *listener)
{
    ctx->pn_conn = pn_connection();
    assert(ctx->pn_conn);
    sys_mutex_init(&ctx->deferred_call_lock);
    ctx->role = qd_strdup(config->role);
    ctx->server = server;
    ctx->wake = connection_wake; /* Default, over-ridden for HTTP connections */
    ctx->connection_id = qd_server_allocate_connection_id(server);
    sys_atomic_init(&ctx->wake_core, 0);
    sys_atomic_init(&ctx->wake_cutthrough_inbound, 0);
    sys_atomic_init(&ctx->wake_cutthrough_outbound, 0);
    sys_spinlock_init(&ctx->inbound_cutthrough_spinlock);
    sys_spinlock_init(&ctx->outbound_cutthrough_spinlock);
    pn_connection_set_context(ctx->pn_conn, ctx);
    DEQ_ITEM_INIT(ctx);
    DEQ_INIT(ctx->deferred_calls);
    DEQ_INIT(ctx->free_link_list);
    DEQ_INIT(ctx->child_sessions);

    // note: setup connector or listener before decorating the connection since
    // decoration involves accessing the connection's parent.

    if (!!connector) {
        assert(!listener);
        qd_connector_add_connection(connector, ctx);
    } else if (!!listener) {
        qd_listener_add_connection(listener, ctx);
    }

    decorate_connection(ctx, config);

    sys_mutex_lock(&amqp_adaptor.lock);
    DEQ_INSERT_TAIL(amqp_adaptor.conn_list, ctx);
    sys_mutex_unlock(&amqp_adaptor.lock);
}


qd_connector_t* qd_connection_connector(const qd_connection_t *c) {
    return c->connector;
}


// Expose event handling for HTTP/AMQP websockets connections.
//
// Used by the libwebsockets code to handle AMQP connection events.
// Returns false at connection close.
//
bool qd_amqpws_handle_connection_event(qd_connection_t *qd_conn, pn_event_t *e)
{
    pn_connection_t *pn_conn = pn_event_connection(e);
    if (!pn_conn)
        return false;

    assert(pn_conn == qd_conn->pn_conn);

    (void) qd_connection_handle_event(qd_conn->server, e, (void *) pn_conn);

    // The call to the hander will free the associated qd_connection_t after the transport is closed.  Return false so
    // libwebsockets will drop its reference to the qd_connection_t and close the connection.

    if (pn_event_type(e) == PN_TRANSPORT_CLOSED) {
        // the qd_conn has been freed by the event handler
        return false;
    }
    return true;
}

// Signal end-of-batch when the HTTP/AMQP websockets connection driver
// drains all pending connection events
//
void qd_amqpws_end_of_batch(qd_connection_t *qd_conn)
{
    pn_connection_t *pn_conn = qd_conn->pn_conn;
    if (pn_conn) {
        (void) qd_connection_handle_event(qd_conn->server, 0, (void *) pn_conn);
    }
}

const char* qd_connection_remote_ip(const qd_connection_t *c) {
    return c->rhost;
}

void qd_connection_set_user(qd_connection_t *conn)
{
    pn_transport_t *tport = pn_connection_transport(conn->pn_conn);
    pn_sasl_t      *sasl  = pn_sasl(tport);
    if (sasl) {
        char *user_id = 0;
        const char *mech = pn_sasl_get_mech(sasl);

        // We want to set the user name only if it is not already set and the selected sasl mechanism is EXTERNAL
        if (mech && strcmp(mech, MECH_EXTERNAL) == 0 && conn->ssl) {
            user_id = qd_tls_session_get_user_id(conn->ssl);
        }

        if (!user_id) {
            const char *tuid = pn_transport_get_user(tport);
            if (tuid) {
                user_id = qd_strdup(tuid);
            }
        }

        assert(!conn->user_id);
        conn->user_id = user_id;
        if (conn->user_id) {
            qd_log(LOG_SERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] User id is '%s'", conn->connection_id, conn->user_id);
        }
    }
}

static void qd_connection_free(qd_connection_t *qd_conn, const char *condition_name, const char *condition_description)
{
    sys_mutex_lock(&amqp_adaptor.lock);
    DEQ_REMOVE(amqp_adaptor.conn_list, qd_conn);
    sys_mutex_unlock(&amqp_adaptor.lock);

    // If this connection is from a connector uncouple it and restart the re-connect timer if necessary

    if (qd_conn->connector) {
        qd_connector_remove_connection(qd_conn->connector, false, condition_name, condition_description);
        qd_conn->connector = 0;
    }

    if (qd_conn->listener) {
        qd_listener_remove_connection(qd_conn->listener, qd_conn);
        qd_conn->listener = 0;
    }

    // If counted for policy enforcement, notify it has closed
    if (qd_conn->policy_counted) {
        qd_policy_socket_close(qd_dispatch_get_policy(amqp_adaptor.dispatch), qd_conn);
    }

    qd_connection_invoke_deferred_calls(qd_conn, true);  // Discard any pending deferred calls
    sys_mutex_free(&qd_conn->deferred_call_lock);
    qd_policy_settings_free(qd_conn->policy_settings);
    free(qd_conn->user_id);
    if (qd_conn->timer) qd_timer_free(qd_conn->timer);
    free(qd_conn->name);
    free(qd_conn->role);
    for (int i = 0; i < QD_SSN_CLASS_COUNT; ++i)
        qd_session_decref(qd_conn->qd_sessions[i]);
    qd_connection_release_sessions(qd_conn);
    qd_tls_session_free(qd_conn->ssl);
    sys_atomic_destroy(&qd_conn->wake_core);
    sys_atomic_destroy(&qd_conn->wake_cutthrough_inbound);
    sys_atomic_destroy(&qd_conn->wake_cutthrough_outbound);
    sys_spinlock_free(&qd_conn->inbound_cutthrough_spinlock);
    sys_spinlock_free(&qd_conn->outbound_cutthrough_spinlock);

    free_qd_connection_t(qd_conn);

    /* Note: pn_conn is freed by the proactor */
}

void qd_connection_activate(qd_connection_t *ctx)
{
    if (ctx) ctx->wake(ctx);
}

void qd_connection_activate_cutthrough(qd_connection_t *ctx, bool incoming)
{
    if (!!ctx && !SET_ATOMIC_FLAG(incoming ? &ctx->wake_cutthrough_inbound : &ctx->wake_cutthrough_outbound)) {
        ctx->wake(ctx);
    }
}


void qd_connection_set_context(qd_connection_t *conn, void *context)
{
    conn->user_context = context;
}


void *qd_connection_get_context(qd_connection_t *conn)
{
    return conn->user_context;
}


void *qd_connection_get_config_context(qd_connection_t *conn)
{
    return conn->context;
}


void qd_connection_set_link_context(qd_connection_t *conn, void *context)
{
    conn->link_context = context;
}


void *qd_connection_get_link_context(qd_connection_t *conn)
{
    return conn->link_context;
}


pn_connection_t *qd_connection_pn(qd_connection_t *conn)
{
    return conn->pn_conn;
}


bool qd_connection_inbound(qd_connection_t *conn)
{
    return conn->listener != 0;
}


uint64_t qd_connection_connection_id(qd_connection_t *conn)
{
    return conn->connection_id;
}


const qd_server_config_t *qd_connection_config(const qd_connection_t *conn)
{
    if (conn->listener)
        return &conn->listener->config;
    if (conn->connector)
        return &conn->connector->ctor_config->config;
    return NULL;
}


void qd_connection_invoke_deferred(qd_connection_t *conn, qd_deferred_t call, void *context)
{
    if (!conn)
        return;

    qd_connection_invoke_deferred_impl(conn, call, context, new_qd_deferred_call_t());
}


void qd_connection_invoke_deferred_impl(qd_connection_t *conn, qd_deferred_t call, void *context, void *dct)
{
    if (!conn)
        return;

    qd_deferred_call_t *dc = (qd_deferred_call_t*)dct;
    DEQ_ITEM_INIT(dc);
    dc->call    = call;
    dc->context = context;

    sys_mutex_lock(&conn->deferred_call_lock);
    DEQ_INSERT_TAIL(conn->deferred_calls, dc);
    sys_mutex_unlock(&conn->deferred_call_lock);

    sys_mutex_lock(qd_server_get_activation_lock(conn->server));
    qd_connection_activate(conn);
    sys_mutex_unlock(qd_server_get_activation_lock(conn->server));
}

void *qd_connection_new_qd_deferred_call_t(void)
{
    return new_qd_deferred_call_t();
}


void qd_connection_free_qd_deferred_call_t(void *dct)
{
    free_qd_deferred_call_t((qd_deferred_call_t *)dct);
}

void qd_connection_invoke_deferred_calls(qd_connection_t *conn, bool discard)
{
    if (!conn)
        return;

    // Lock access to deferred_calls, other threads may concurrently add to it.  Invoke
    // the calls outside of the critical section.
    //
    sys_mutex_lock(&conn->deferred_call_lock);
    qd_deferred_call_t *dc;
    while ((dc = DEQ_HEAD(conn->deferred_calls))) {
        DEQ_REMOVE_HEAD(conn->deferred_calls);
        sys_mutex_unlock(&conn->deferred_call_lock);
        dc->call(dc->context, discard);
        free_qd_deferred_call_t(dc);
        sys_mutex_lock(&conn->deferred_call_lock);
    }
    sys_mutex_unlock(&conn->deferred_call_lock);
}



const char* qd_connection_name(const qd_connection_t *c)
{
    if (c->connector) {
        return c->connector->ctor_config->config.host_port;
    } else {
        return c->rhost_port;
    }
}

bool qd_connection_strip_annotations_in(const qd_connection_t *c)
{
    return c->strip_annotations_in;
}

uint64_t qd_connection_max_message_size(const qd_connection_t *c)
{
    return (c && c->policy_settings) ? c->policy_settings->spec.maxMessageSize : 0;
}


/* Log the description, set the transport condition (name, description) close the transport tail. */
void connect_fail(qd_connection_t *ctx, const char *name, const char *description, ...)
     __attribute__((format(printf, 3, 4)));
void connect_fail(qd_connection_t *ctx, const char *name, const char *description, ...)
{
    va_list ap;
    va_start(ap, description);
    qd_verror(QD_ERROR_RUNTIME, description, ap);
    va_end(ap);
    if (ctx->pn_conn) {
        pn_transport_t *t = pn_connection_transport(ctx->pn_conn);
        /* Normally this is closing the transport but if not bound close the connection. */
        pn_condition_t *cond = t ? pn_transport_condition(t) : pn_connection_condition(ctx->pn_conn);
        if (cond && !pn_condition_is_set(cond)) {
            va_start(ap, description);
            pn_condition_vformat(cond, name, description, ap);
            va_end(ap);
        }
        if (t) {
            pn_transport_close_tail(t);
        } else {
            pn_connection_close(ctx->pn_conn);
        }
    }
}

/* Get the host IP address for the remote end */
static void set_rhost_port(qd_connection_t *ctx) {
    pn_transport_t *tport  = pn_connection_transport(ctx->pn_conn);
    const struct sockaddr* sa = pn_netaddr_sockaddr(pn_transport_remote_addr(tport));
    size_t salen = pn_netaddr_socklen(pn_transport_remote_addr(tport));
    if (sa && salen) {
        char rport[NI_MAXSERV] = "";
        int err = getnameinfo(sa, salen,
                              ctx->rhost, sizeof(ctx->rhost), rport, sizeof(rport),
                              NI_NUMERICHOST | NI_NUMERICSERV);
        if (!err) {
            snprintf(ctx->rhost_port, sizeof(ctx->rhost_port), "%s:%s", ctx->rhost, rport);
        }
    }
}

static bool setup_ssl_sasl_and_open(qd_connection_t *ctx)
{
    qd_connector_t *ct = ctx->connector;
    const qd_server_config_t *config = &ct->ctor_config->config;
    pn_transport_t *tport  = pn_connection_transport(ctx->pn_conn);

    //
    // Create an SSL session if required
    //
    if (ct->ctor_config->tls_config) {
        ctx->ssl = qd_tls_session_amqp(ct->ctor_config->tls_config, tport, false);
        if (!ctx->ssl) {
            qd_log(LOG_SERVER, QD_LOG_ERROR,
                   "Failed to create TLS session for connection [C%" PRIu64 "] to %s:%s (%s)",
                   ctx->connection_id, config->host, config->port, qd_error_message());
            return false;
        }
    }

    //
    // Set up SASL
    //
    pn_sasl_t *sasl = pn_sasl(tport);
    if (config->sasl_mechanisms)
        pn_sasl_allowed_mechs(sasl, config->sasl_mechanisms);
    pn_sasl_set_allow_insecure_mechs(sasl, config->allowInsecureAuthentication);

    pn_connection_open(ctx->pn_conn);
    return true;
}


/* Configure the transport once it is bound to the connection */
static void on_connection_bound(qd_server_t *server, pn_event_t *e) {
    pn_connection_t *pn_conn = pn_event_connection(e);
    qd_connection_t *ctx = pn_connection_get_context(pn_conn);
    pn_transport_t *tport  = pn_connection_transport(pn_conn);
    pn_transport_set_context(tport, ctx); /* for transport_tracer */

    //
    // Proton pushes out its trace to transport_tracer() which in turn writes a trace
    // message to the qdrouter log
    // If trace level logging is enabled on the PROTOCOL module, set PN_TRACE_FRM as the transport trace
    // and also set the transport tracer callback.
    // Note here that if trace level logging is enabled on the DEFAULT module, all modules are logging at trace level too.
    //
    if (qd_log_enabled(LOG_PROTOCOL, QD_LOG_DEBUG)) {
        pn_transport_trace(tport, PN_TRACE_FRM);
        pn_transport_set_tracer(tport, transport_tracer);
    }

    const qd_server_config_t *config = NULL;
    if (ctx->listener) {        /* Accepting an incoming connection */
        config = &ctx->listener->config;
        const char *name = config->host_port;
        pn_transport_set_server(tport);
        set_rhost_port(ctx);

        ctx->policy_counted = qd_policy_socket_accept(qd_dispatch_get_policy(amqp_adaptor.dispatch), ctx->rhost);
        if (!ctx->policy_counted) {
            pn_transport_close_tail(tport);
            pn_transport_close_head(tport);
            return;
        }

        // Set up TLS
        if (ctx->listener->tls_config) {
            qd_log(LOG_SERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] Configuring SSL on %s", ctx->connection_id, name);

            ctx->ssl = qd_tls_session_amqp(ctx->listener->tls_config, tport, config->ssl_required == false);
            if (!ctx->ssl) {
                connect_fail(ctx, QD_AMQP_COND_INTERNAL_ERROR, "%s on %s", qd_error_message(), name);
                return;
            }
        }
        //
        // Set up SASL
        //
        pn_sasl_t *sasl = pn_sasl(tport);
        if (config->sasl_mechanisms)
            pn_sasl_allowed_mechs(sasl, config->sasl_mechanisms);
        pn_transport_require_auth(tport, config->requireAuthentication);
        pn_transport_require_encryption(tport, config->requireEncryption);
        pn_sasl_set_allow_insecure_mechs(sasl, config->allowInsecureAuthentication);

        // This log statement is kept at INFO level because this shows the inter-router
        // connections and that is useful when debugging router issues.
        qd_log(LOG_SERVER, QD_LOG_INFO, "[C%" PRIu64 "] Accepted connection to %s from %s",
               ctx->connection_id, name, ctx->rhost_port);
    } else if (ctx->connector) { /* Establishing an outgoing connection */
        config = &ctx->connector->ctor_config->config;
        if (!setup_ssl_sasl_and_open(ctx)) {
            qd_log(LOG_SERVER, QD_LOG_ERROR, "[C%" PRIu64 "] Connection aborted due to internal setup error",
                   ctx->connection_id);
            pn_transport_close_tail(tport);
            pn_transport_close_head(tport);
            return;
        }

    } else {                    /* No connector and no listener */
        connect_fail(ctx, QD_AMQP_COND_INTERNAL_ERROR, "unknown Connection");
        return;
    }

    //
    // Common transport configuration.
    //
    pn_transport_set_max_frame(tport, config->max_frame_size);
    pn_transport_set_idle_timeout(tport, config->idle_timeout_seconds * 1000);
    // pn_transport_set_channel_max sets the maximum session *identifier*, not the total number of sessions. Thus Proton
    // will allow sessions with identifiers [0..max_sessions], which is one greater than the value we pass to
    // pn_transport_set_channel_max. So to limit the maximum number of simultaineous sessions to config->max_sessions we
    // have to decrement it by one for Proton.
    pn_transport_set_channel_max(tport, config->max_sessions - 1);
}

void qd_container_handle_event(qd_container_t *container, pn_event_t *event, pn_connection_t *pn_conn, qd_connection_t *qd_conn);
void qd_conn_event_batch_complete(qd_container_t *container, qd_connection_t *qd_conn, bool conn_closed);


static void timeout_on_handshake(void *context, bool discard)
{
    if (discard)
        return;

    qd_connection_t *ctx   = (qd_connection_t*) context;
    pn_transport_t  *tport = pn_connection_transport(ctx->pn_conn);
    pn_transport_close_head(tport);
    connect_fail(ctx, QD_AMQP_COND_NOT_ALLOWED, "Timeout waiting for initial handshake");
}


static void startup_timer_handler(void *context)
{
    //
    // This timer fires for a connection if it has not had a REMOTE_OPEN
    // event in a time interval from the CONNECTION_INIT event.  Close
    // down the transport in an IO thread reserved for that connection.
    //
    qd_connection_t *ctx = (qd_connection_t*) context;
    qd_timer_free(ctx->timer);
    ctx->timer = 0;
    qd_connection_invoke_deferred(ctx, timeout_on_handshake, context);
}


/* Events involving a connection or listener are serialized by the proactor so
 * only one event per connection / listener will be processed at a time.
 */
bool qd_connection_handle_event(qd_server_t *qd_server, pn_event_t *e, void *context)
{
    pn_connection_t *pn_conn = (pn_connection_t *) context;
    qd_connection_t *ctx     = (qd_connection_t *) pn_connection_get_context(pn_conn);

    if (!e) {  // end-of-batch signal
        //
        // Notify the container that the batch is complete so it can do after-batch
        // processing.
        //
        if (ctx)
            qd_conn_event_batch_complete(amqp_adaptor.container, ctx, false);
        return true;
    }

    switch (pn_event_type(e)) {

    case PN_CONNECTION_INIT: {
        const qd_server_config_t *config = ctx && ctx->listener ? &ctx->listener->config : 0;
        if (config && config->initial_handshake_timeout_seconds > 0) {
            ctx->timer = qd_timer(amqp_adaptor.dispatch, startup_timer_handler, ctx);
            qd_timer_schedule(ctx->timer, config->initial_handshake_timeout_seconds * 1000);
        }
        break;
    }

    case PN_CONNECTION_BOUND:
        on_connection_bound(qd_server, e);
        break;

    case PN_CONNECTION_REMOTE_OPEN:
        // If we are transitioning to the open state, notify the client via callback.
        if (ctx && ctx->timer) {
            qd_timer_free(ctx->timer);
            ctx->timer = 0;
        }
        if (ctx && !ctx->opened) {
            ctx->opened = true;
            if (ctx->connector) {
                qd_connector_remote_opened(ctx->connector);
            }
        }
        break;

    case PN_CONNECTION_WAKE:
        qd_connection_invoke_deferred_calls(ctx, false);
        break;

    case PN_TRANSPORT_ERROR:
        {
            pn_transport_t *transport = pn_event_transport(e);
            pn_condition_t* condition = transport ? pn_transport_condition(transport) : NULL;
            if (ctx && ctx->connector) { /* Outgoing connection */
                qd_connector_handle_transport_error(ctx->connector, ctx->connection_id, condition);
            } else if (ctx && ctx->listener) { /* Incoming connection */
                if (condition && pn_condition_is_set(condition)) {
                    qd_log(LOG_SERVER, QD_LOG_ERROR,
                           "[C%" PRIu64 "] Connection from %s (to %s) failed: %s %s", ctx->connection_id,
                           ctx->rhost_port, ctx->listener->config.host_port, pn_condition_get_name(condition),
                           pn_condition_get_description(condition));
                }
            }
        }
        break;

    case PN_TRANSPORT_CLOSED:
        /* The last event for this connection. Free the connection after all other processing is complete */
        if (ctx) {
            qd_container_handle_event(amqp_adaptor.container, e, pn_conn, ctx);
            qd_conn_event_batch_complete(amqp_adaptor.container, ctx, true);
            pn_connection_set_context(pn_conn, NULL);
            pn_condition_t *cond = pn_event_condition(e);
            const char *condition_name        = !!cond ? pn_condition_get_name(cond) : 0;
            const char *condition_descriprion = !!cond ? pn_condition_get_description(cond) : 0;
            qd_connection_free(ctx, condition_name, condition_descriprion);
            ctx = 0;
        }
        break;

    default:
        break;
    } // Switch event type

    if (ctx)
        qd_container_handle_event(amqp_adaptor.container, e, pn_conn, ctx);

    return true;
}


/**
 * Set/clear the AMQP transport tracing callback for logging protocol traces
 */
void qd_amqp_connection_set_tracing(bool enable_tracing)
{
    // KAG: fixme: do this better. Currently wakes up the core for each connection, then the core wakes up the I/O
    // thread once for each connection to actually configure the tracer.
    if (amqp_adaptor.core) {
        sys_mutex_lock(&amqp_adaptor.lock);
        qd_connection_t *conn = DEQ_HEAD(amqp_adaptor.conn_list);
        while (conn) {
            qdr_connection_set_tracing((qdr_connection_t*) qd_connection_get_context(conn), enable_tracing);
            conn = DEQ_NEXT(conn);
        }
        sys_mutex_unlock(&amqp_adaptor.lock);
    }
}

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

#include "qd_connector.h"
#include "qd_connection.h"
#include "private.h"
#include "entity.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/tls_amqp.h"
#include "qpid/dispatch/dispatch.h"

#include <proton/proactor.h>

#include <inttypes.h>


ALLOC_DEFINE(qd_connector_t);
ALLOC_DEFINE(qd_connector_config_t);


static qd_failover_item_t *qd_connector_get_conn_info_lh(qd_connector_t *ct) TA_REQ(ct->lock)
{
    qd_failover_item_t *item = DEQ_HEAD(ct->conn_info_list);

    if (DEQ_SIZE(ct->conn_info_list) > 1) {
        for (int i=1; i < ct->conn_index; i++) {
            item = DEQ_NEXT(item);
        }
    }
    return item;
}


/* Timer callback to try/retry connection open, connector->lock held */
static void try_open_lh(qd_connector_t *connector, qd_connection_t *qd_conn) TA_REQ(connector->lock)
{
    assert(connector->state != CTOR_STATE_DELETED);

    const qd_connector_config_t *ctor_config = connector->ctor_config;
    qd_connection_init(qd_conn, ctor_config->server, &ctor_config->config, connector, 0);

    connector->state   = CTOR_STATE_OPEN;
    connector->delay   = 5000;

    //
    // Set the hostname on the pn_connection. This hostname will be used by proton as the
    // hostname in the open frame.
    //

    qd_failover_item_t *item = qd_connector_get_conn_info_lh(connector);

    char *current_host = item->host;
    char *host_port = item->host_port;

    pn_connection_set_hostname(qd_conn->pn_conn, current_host);

    // Set the sasl user name and password on the proton connection object. This has to be
    // done before pn_proactor_connect which will bind a transport to the connection
    const qd_server_config_t *config = &connector->ctor_config->config;
    if(config->sasl_username)
        pn_connection_set_user(qd_conn->pn_conn, config->sasl_username);
    if (config->sasl_password)
        pn_connection_set_password(qd_conn->pn_conn, config->sasl_password);

    qd_log(LOG_SERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] Connecting to %s", qd_conn->connection_id, host_port);
    /* Note: the transport is configured in the PN_CONNECTION_BOUND event */
    pn_proactor_connect(qd_server_proactor(connector->ctor_config->server), qd_conn->pn_conn, host_port);
    // at this point the qd_conn may now be scheduled on another thread
}


// (re)connection timer callback used by connector
//
static void try_open_cb(void *context)
{
    qd_connector_t *ct = (qd_connector_t*) context;

    // Allocate connection before taking connector lock to avoid
    // CONNECTOR - ENTITY_CACHE lock inversion deadlock window.
    qd_connection_t *ctx = new_qd_connection_t();
    ZERO(ctx);

    sys_mutex_lock(&ct->lock);

    if (ct->state == CTOR_STATE_CONNECTING || ct->state == CTOR_STATE_INIT) {
        // else deleted or failed - on failed wait until after connection is freed
        // and state is set to CTOR_STATE_CONNECTING (timer is rescheduled then)
        try_open_lh(ct, ctx);
        ctx = 0;  // owned by ct
    }

    sys_mutex_unlock(&ct->lock);

    free_qd_connection_t(ctx);  // noop if ctx == 0
}


/** Close the proton connection
 *
 * Scheduled on the target connections thread
 */
static void deferred_close(void *context, bool discard)
{
    if (!discard) {
        pn_connection_close((pn_connection_t*)context);
    }
}


const qd_server_config_t *qd_connector_get_config(const qd_connector_t *c)
{
    return &c->ctor_config->config;
}


qd_connector_t *qd_connector_create(qd_connector_config_t *ctor_config, bool is_data_connector)
{
    qd_connector_t *connector = new_qd_connector_t();
    if (!connector) return 0;

    ZERO(connector);
    sys_atomic_init(&connector->ref_count, 1);  // for caller
    DEQ_INIT(connector->conn_info_list);
    DEQ_ITEM_INIT(connector);

    sys_mutex_init(&connector->lock);
    connector->timer             = qd_timer(amqp_adaptor.dispatch, try_open_cb, connector);
    connector->reconnect_enabled = true;
    connector->is_data_connector = is_data_connector;

    connector->ctor_config = ctor_config;
    sys_atomic_inc(&ctor_config->ref_count);

    connector->conn_index = 1;
    connector->state      = CTOR_STATE_INIT;

    qd_failover_item_t *item = NEW(qd_failover_item_t);
    ZERO(item);
    if (ctor_config->config.ssl_required)
        item->scheme = strdup("amqps");
    else
        item->scheme = strdup("amqp");
    item->host = qd_strdup(ctor_config->config.host);
    item->port = qd_strdup(ctor_config->config.port);
    int hplen = strlen(item->host) + strlen(item->port) + 2;
    item->host_port = malloc(hplen);
    snprintf(item->host_port, hplen, "%s:%s", item->host , item->port);
    DEQ_INSERT_TAIL(connector->conn_info_list, item);

    //
    // Set up the vanflow record for this connector (LINK)
    // Do this only for router-to-router connectors since the record represents an inter-router link
    //
    if ((strcmp(ctor_config->config.role, "inter-router") == 0 && !is_data_connector) ||
        strcmp(ctor_config->config.role, "edge") == 0 ||
        strcmp(ctor_config->config.role, "inter-edge") == 0) {
        connector->vflow_record = vflow_start_record(VFLOW_RECORD_LINK, 0);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_NAME, ctor_config->config.name);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_ROLE, ctor_config->config.role);
        vflow_set_uint64(connector->vflow_record, VFLOW_ATTRIBUTE_LINK_COST, ctor_config->config.inter_router_cost);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_OPER_STATUS, "down");
        vflow_set_uint64(connector->vflow_record, VFLOW_ATTRIBUTE_DOWN_COUNT, 0);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_PROTOCOL, item->scheme);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_DESTINATION_HOST, item->host);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_DESTINATION_PORT, item->port);
        vflow_set_uint64(connector->vflow_record, VFLOW_ATTRIBUTE_OCTETS, 0);
        vflow_set_uint64(connector->vflow_record, VFLOW_ATTRIBUTE_OCTETS_REVERSE, 0);
    }
    return connector;
}


const char *qd_connector_policy_vhost(const qd_connector_t* ct)
{
    return ct->ctor_config->policy_vhost;
}


bool qd_connector_connect(qd_connector_t *ct)
{
    sys_mutex_lock(&ct->lock);
    if (ct->state != CTOR_STATE_DELETED) {
        // expect: do not attempt to connect an already connected qd_connection
        assert(ct->qd_conn == 0);
        ct->qd_conn = 0;
        ct->delay   = 0;
        ct->state   = CTOR_STATE_CONNECTING;
        qd_timer_schedule(ct->timer, ct->delay);
        sys_mutex_unlock(&ct->lock);
        return true;
    }
    sys_mutex_unlock(&ct->lock);
    return false;
}


// Teardown the connection associated with the connector and
// prepare the connector for deletion
//
void qd_connector_close(qd_connector_t *ct)
{
    // cannot free the timer while holding ct->lock since the
    // timer callback may be running during the call to qd_timer_free
    qd_timer_t *timer = 0;
    void       *dct = qd_connection_new_qd_deferred_call_t();

    sys_mutex_lock(&ct->lock);
    timer = ct->timer;
    ct->timer = 0;
    ct->state = CTOR_STATE_DELETED;
    qd_connection_t *conn = ct->qd_conn;
    if (conn && conn->pn_conn) {
        qd_connection_invoke_deferred_impl(conn, deferred_close, conn->pn_conn, dct);
        sys_mutex_unlock(&ct->lock);
    } else {
        sys_mutex_unlock(&ct->lock);
        qd_connection_free_qd_deferred_call_t(dct);
    }
    qd_timer_free(timer);
}


void qd_connector_decref(qd_connector_t* connector)
{
    if (!connector) return;
    if (sys_atomic_dec(&connector->ref_count) == 1) {

        // expect both mgmt and qd_connection no longer reference this
        assert(connector->state == CTOR_STATE_DELETED);
        assert(connector->qd_conn == 0);

        qd_connector_config_decref(connector->ctor_config);
        vflow_end_record(connector->vflow_record);
        connector->vflow_record = 0;
        qd_timer_free(connector->timer);
        sys_mutex_free(&connector->lock);
        sys_atomic_destroy(&connector->ref_count);

        qd_failover_item_t *item = DEQ_HEAD(connector->conn_info_list);
        while (item) {
            DEQ_REMOVE_HEAD(connector->conn_info_list);
            free(item->scheme);
            free(item->host);
            free(item->port);
            free(item->hostname);
            free(item->host_port);
            free(item);
            item = DEQ_HEAD(connector->conn_info_list);
        }
        free_qd_connector_t(connector);
    }
}


bool qd_connector_has_failover_info(const qd_connector_t* ct)
{
    if (ct && DEQ_SIZE(ct->conn_info_list) > 1)
        return true;
    return false;
}


static void increment_conn_index_lh(qd_connector_t *connector) TA_REQ(connector->lock)
{
    assert(connector);
    qd_failover_item_t *item = qd_connector_get_conn_info_lh(connector);

    if (item->retries == 1) {
        connector->conn_index += 1;
        if (connector->conn_index > DEQ_SIZE(connector->conn_info_list))
            connector->conn_index = 1;
        item->retries = 0;
    }
    else
        item->retries += 1;
}


/**
 * Handle PN_TRANSPORT_ERROR events
 */
void qd_connector_handle_transport_error(qd_connector_t *connector, uint64_t connection_id, pn_condition_t *condition)
{
    const qd_server_config_t *config = &connector->ctor_config->config;
    char conn_msg[QD_CTOR_CONN_MSG_BUF_SIZE];  // avoid holding connector lock when logging
    char conn_msg_1[QD_CTOR_CONN_MSG_BUF_SIZE]; // this connection message does not contain the connection id

    bool log_error_message = false;
    sys_mutex_lock(&connector->lock);
    if (connector->state != CTOR_STATE_DELETED) {
        increment_conn_index_lh(connector);
        // note: will transition back to STATE_CONNECTING when associated connection is freed (pn_connection_free)
        connector->state = CTOR_STATE_FAILED;
        if (condition && pn_condition_is_set(condition)) {
            qd_format_string(conn_msg, sizeof(conn_msg), "[C%"PRIu64"] Connection to %s failed: %s %s",
                             connection_id, config->host_port, pn_condition_get_name(condition),
                             pn_condition_get_description(condition));

            qd_format_string(conn_msg_1, sizeof(conn_msg_1), "Connection to %s failed: %s %s",
                             config->host_port, pn_condition_get_name(condition), pn_condition_get_description(condition));
        } else {
            qd_format_string(conn_msg, sizeof(conn_msg), "[C%"PRIu64"] Connection to %s failed",
                             connection_id, config->host_port);
            qd_format_string(conn_msg_1, sizeof(conn_msg_1), "Connection to %s failed", config->host_port);
        }
        //
        // This is a fix for https://github.com/skupperproject/skupper-router/issues/1385
        // The router will repeatedly try to connect to the host/port specified in the connector
        // If it is unable to connect, an error message will be logged only once and more error messages
        // from connection failures will only be logged if the error message changes.
        // This is done so we don't flood the log with connection failure error messages
        // Even though we restrict the number of times the error message is displayed, the
        // router will still keep trying to connect to the host/port specified in the connector.
        //

        if (strcmp(connector->conn_msg, conn_msg_1) != 0) {
            strncpy(connector->conn_msg, conn_msg_1, QD_CTOR_CONN_MSG_BUF_SIZE);
            log_error_message = true;
        }
    }
    sys_mutex_unlock(&connector->lock);
    if (log_error_message) {
        qd_log(LOG_SERVER, QD_LOG_ERROR, "%s", conn_msg);
    }
}


/**
 * Handle AMQP connection remote opened event.
 *
 * Resets the delay timer and failover retry counter
 */
void qd_connector_remote_opened(qd_connector_t *connector)
{
    sys_mutex_lock(&connector->lock);
    connector->delay = 2000;  // Delay re-connect in case there is a recurring error
    qd_failover_item_t *item = qd_connector_get_conn_info_lh(connector);
    if (item)
        item->retries = 0;
    sys_mutex_unlock(&connector->lock);
}

/**
 * Set the child connection of the connector
 */
void qd_connector_add_connection(qd_connector_t *connector, qd_connection_t *ctx)
{
    assert(ctx->connector == 0);

    sys_atomic_inc(&connector->ref_count);
    ctx->connector = connector;
    connector->qd_conn = ctx;

    strncpy(ctx->group_correlator, connector->ctor_config->group_correlator, QD_DISCRIMINATOR_SIZE);
}


void qd_connector_add_link(qd_connector_t *connector)
{
    if (!connector->is_data_connector) {
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_OPER_STATUS, "up");
        vflow_set_timestamp_now(connector->vflow_record, VFLOW_ATTRIBUTE_UP_TIMESTAMP);
        connector->oper_status_down = false;
    }
}


/**
 * The connection associated with this connector is about to be freed,
 * clean up all related state
 */
void qd_connector_remove_connection(qd_connector_t *connector, bool final, const char *condition_name, const char *condition_description)
{
    sys_mutex_lock(&connector->lock);

    qd_connection_t *ctx = connector->qd_conn;
    if (!connector->is_data_connector && !connector->oper_status_down  && !final) {
        connector->oper_status_down = true;
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_OPER_STATUS, "down");
        vflow_inc_counter(connector->vflow_record, VFLOW_ATTRIBUTE_DOWN_COUNT, 1);
        vflow_set_timestamp_now(connector->vflow_record, VFLOW_ATTRIBUTE_DOWN_TIMESTAMP);
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_RESULT, condition_name        ? condition_name : "unknown");
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_REASON, condition_description ? condition_description : "");
    }
    connector->qd_conn = 0;
    ctx->connector = 0;

    if (connector->state != CTOR_STATE_DELETED) {
        // Increment the connection index by so that we can try connecting to the failover url (if any).
        bool has_failover = qd_connector_has_failover_info(connector);
        long delay = connector->delay;

        if (has_failover) {
            // Go thru the failover list round robin.
            // IMPORTANT: Note here that we set the re-try timer to 1 second.
            // We want to quickly keep cycling thru the failover urls every second.
            delay = 1000;
        }
        connector->state = CTOR_STATE_CONNECTING;
        qd_timer_schedule(connector->timer, delay);
    }
    sys_mutex_unlock(&connector->lock);

    // Drop reference held by connection.
    qd_connector_decref(connector);
}


/**
 * Create a new qd_connector_config_t instance
 */
qd_connector_config_t *qd_connector_config_create(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_connector_config_t *ctor_config = new_qd_connector_config_t();
    if (!ctor_config) {
        char *name = qd_entity_opt_string(entity, "name", "UNKNOWN");
        qd_error(QD_ERROR_CONFIG, "Failed to create Connector %s: resource allocation failed", name);
        free(name);
        return 0;
    }

    qd_error_clear();

    ZERO(ctor_config);
    DEQ_ITEM_INIT(ctor_config);
    sys_atomic_init(&ctor_config->ref_count, 1);  // for caller
    sys_mutex_init(&ctor_config->lock);
    ctor_config->server = qd_dispatch_get_server(qd);
    DEQ_INIT(ctor_config->connectors);

    if (qd_server_config_load(&ctor_config->config, entity, false) != QD_ERROR_NONE) {
        qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create connector: %s", qd_error_message());
        qd_connector_config_decref(ctor_config);
        return 0;
    }

    ctor_config->policy_vhost = qd_entity_opt_string(entity, "policyVhost", 0);
    if (qd_error_code()) {
        qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create connector: %s", qd_error_message());
        qd_connector_config_decref(ctor_config);
        return 0;
    }

    //
    // If an sslProfile is configured allocate a TLS config to be used by all child connector's connections
    //
    if (ctor_config->config.ssl_profile_name) {
        ctor_config->tls_config = qd_tls_config(ctor_config->config.ssl_profile_name,
                                             QD_TLS_TYPE_PROTON_AMQP,
                                             QD_TLS_CONFIG_CLIENT_MODE,
                                             ctor_config->config.verify_host_name,
                                             ctor_config->config.ssl_require_peer_authentication);
        if (!ctor_config->tls_config) {
            // qd_tls2_config() has set the qd_error_message(), which is logged below
            goto error;
        }
    }

    // For inter-router connectors create associated inter-router data connectors if configured

    if (strcmp(ctor_config->config.role, "inter-router") == 0) {
        ctor_config->data_connection_count = qd_dispatch_get_data_connection_count(qd);
        if (!!ctor_config->data_connection_count) {
            qd_generate_discriminator(ctor_config->group_correlator);

            // Add any data connectors to the head of the connectors list first. This allows the
            // router control connector to be located at the head of the list.

            for (int i = 0; i < ctor_config->data_connection_count; i++) {
                qd_connector_t *dc = qd_connector_create(ctor_config, true);
                if (!dc) {
                    qd_error(QD_ERROR_CONFIG, "Failed to create data Connector %s: resource allocation failed", ctor_config->config.name);
                    goto error;
                }
                DEQ_INSERT_HEAD(ctor_config->connectors, dc);
            }
        }
    }

    // Create the primary connector associated with this configuration. It will be located
    // at the head of the connectors list

    qd_connector_t *ct = qd_connector_create(ctor_config, false);
    if (!ct) {
        qd_error(QD_ERROR_CONFIG, "Failed to create data Connector %s: resource allocation failed", ctor_config->config.name);
        goto error;
    }
    DEQ_INSERT_HEAD(ctor_config->connectors, ct);

    return ctor_config;

  error:
    if (qd_error_code())
        qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create connector: %s", qd_error_message());
    for (qd_connector_t *dc = DEQ_HEAD(ctor_config->connectors); dc; dc = DEQ_HEAD(ctor_config->connectors)) {
        DEQ_REMOVE_HEAD(ctor_config->connectors);
        dc->state = CTOR_STATE_DELETED;
        qd_connector_decref(dc);
    }
    qd_connector_config_decref(ctor_config);
    return 0;
}


void qd_connector_config_delete(qd_connector_config_t *ctor_config)
{
    qd_connector_t *ct = DEQ_HEAD(ctor_config->connectors);
    while (ct) {
        DEQ_REMOVE_HEAD(ctor_config->connectors);
        qd_connector_close(ct);
        qd_connector_decref(ct);
        ct = DEQ_HEAD(ctor_config->connectors);
    }

    // drop ref held by the caller
    qd_connector_config_decref(ctor_config);
}


void qd_connector_config_decref(qd_connector_config_t *ctor_config)
{
    if (!ctor_config)
        return;

    uint32_t rc = sys_atomic_dec(&ctor_config->ref_count);
    (void) rc;
    assert(rc > 0);  // else underflow

    if (rc == 1) {
        // Expect: all connectors hold the ref_count so this must be empty
        assert(DEQ_IS_EMPTY(ctor_config->connectors));
        sys_mutex_free(&ctor_config->lock);
        sys_atomic_destroy(&ctor_config->ref_count);
        free(ctor_config->policy_vhost);
        qd_tls_config_decref(ctor_config->tls_config);
        qd_server_config_free(&ctor_config->config);
        free_qd_connector_config_t(ctor_config);
    }
}


// Initiate connections on all child connectors
void qd_connector_config_connect(qd_connector_config_t *ctor_config)
{
    if (!ctor_config->activated) {
        ctor_config->activated = true;
        for (qd_connector_t *ct = DEQ_HEAD(ctor_config->connectors); !!ct; ct = DEQ_NEXT(ct)) {
            qd_connector_connect(ct);
        }
    }
}

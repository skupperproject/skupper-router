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

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/vanflow.h"

#include <proton/proactor.h>

#include <inttypes.h>


ALLOC_DEFINE(qd_connector_t);


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
    qd_connection_init(qd_conn, connector->server, &connector->config, connector, 0);

    connector->state   = CXTR_STATE_OPEN;
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
    const qd_server_config_t *config = &connector->config;
    if(config->sasl_username)
        pn_connection_set_user(qd_conn->pn_conn, config->sasl_username);
    if (config->sasl_password)
        pn_connection_set_password(qd_conn->pn_conn, config->sasl_password);

    qd_log(LOG_SERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] Connecting to %s", qd_conn->connection_id, host_port);
    /* Note: the transport is configured in the PN_CONNECTION_BOUND event */
    pn_proactor_connect(qd_server_proactor(connector->server), qd_conn->pn_conn, host_port);
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

    if (ct->state == CXTR_STATE_CONNECTING || ct->state == CXTR_STATE_INIT) {
        // else deleted or failed - on failed wait until after connection is freed
        // and state is set to CXTR_STATE_CONNECTING (timer is rescheduled then)
        try_open_lh(ct, ctx);
        ctx = 0;  // owned by ct
    }

    sys_mutex_unlock(&ct->lock);

    free_qd_connection_t(ctx);  // noop if ctx == 0
}


const qd_server_config_t *qd_connector_config(const qd_connector_t *c)
{
    return &c->config;
}


qd_connector_t *qd_server_connector(qd_server_t *server)
{
    qd_connector_t *connector = new_qd_connector_t();
    if (!connector) return 0;
    ZERO(connector);
    sys_atomic_init(&connector->ref_count, 1);
    DEQ_INIT(connector->conn_info_list);

    sys_mutex_init(&connector->lock);
    connector->timer = qd_timer(amqp_adaptor.dispatch, try_open_cb, connector);
    if (!connector->timer)
        goto error;

    connector->server     = server;
    connector->conn_index = 1;
    connector->state      = CXTR_STATE_INIT;

    return connector;

error:
    connector->state = CXTR_STATE_DELETED;
    qd_connector_decref(connector);
    return 0;
}


const char *qd_connector_policy_vhost(const qd_connector_t* ct)
{
    return ct->policy_vhost;
}


bool qd_connector_connect(qd_connector_t *ct)
{
    sys_mutex_lock(&ct->lock);
    // expect: do not attempt to connect an already connected qd_connection
    assert(ct->qd_conn == 0);
    ct->qd_conn = 0;
    ct->delay   = 0;
    ct->state   = CXTR_STATE_CONNECTING;
    qd_timer_schedule(ct->timer, ct->delay);
    sys_mutex_unlock(&ct->lock);
    return true;
}


void qd_connector_decref(qd_connector_t* connector)
{
    if (!connector) return;
    if (sys_atomic_dec(&connector->ref_count) == 1) {

        // expect both mgmt and qd_connection no longer reference this
        assert(connector->state == CXTR_STATE_DELETED);
        assert(connector->qd_conn == 0);

        vflow_end_record(connector->vflow_record);
        connector->vflow_record = 0;
        qd_server_config_free(&connector->config);
        qd_timer_free(connector->timer);
        sys_mutex_free(&connector->lock);

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
        if (connector->policy_vhost) free(connector->policy_vhost);
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
    const qd_server_config_t *config = &connector->config;
    char conn_msg[QD_CXTR_CONN_MSG_BUF_SIZE];  // avoid holding connector lock when logging
    char conn_msg_1[QD_CXTR_CONN_MSG_BUF_SIZE]; // this connection message does not contain the connection id

    sys_mutex_lock(&connector->lock);
    increment_conn_index_lh(connector);
    // note: will transition back to STATE_CONNECTING when associated connection is freed (pn_connection_free)
    connector->state = CXTR_STATE_FAILED;
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
    bool log_error_message = false;
    if (strcmp(connector->conn_msg, conn_msg_1) != 0) {
        strncpy(connector->conn_msg, conn_msg_1, QD_CXTR_CONN_MSG_BUF_SIZE);
        log_error_message = true;
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

    strncpy(ctx->group_correlator, connector->group_correlator, QD_DISCRIMINATOR_SIZE);
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
void qd_connector_remove_connection(qd_connector_t *connector, const char *condition_name, const char *condition_description)
{
    sys_mutex_lock(&connector->lock);

    qd_connection_t *ctx = connector->qd_conn;
    if (!connector->is_data_connector && !connector->oper_status_down) {
        connector->oper_status_down = true;
        vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_OPER_STATUS, "down");
        vflow_inc_counter(connector->vflow_record, VFLOW_ATTRIBUTE_DOWN_COUNT, 1);
        if (!!condition_name) {
            vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_RESULT, condition_name);
        }
        if (!!condition_description) {
            vflow_set_string(connector->vflow_record, VFLOW_ATTRIBUTE_REASON, condition_description);
        }
    }
    connector->qd_conn = 0;
    ctx->connector = 0;

    if (connector->state != CXTR_STATE_DELETED) {
        // Increment the connection index by so that we can try connecting to the failover url (if any).
        bool has_failover = qd_connector_has_failover_info(connector);
        long delay = connector->delay;

        if (has_failover) {
            // Go thru the failover list round robin.
            // IMPORTANT: Note here that we set the re-try timer to 1 second.
            // We want to quickly keep cycling thru the failover urls every second.
            delay = 1000;
        }
        connector->state = CXTR_STATE_CONNECTING;
        qd_timer_schedule(connector->timer, delay);
    }
    sys_mutex_unlock(&connector->lock);

    // Drop reference held by connection.
    qd_connector_decref(connector);
}

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

#include "qpid/dispatch/connection_manager.h"

#include "qd_connection.h"
#include "qd_listener.h"
#include "qd_connector.h"
#include "server_config.h"
#include "dispatch_private.h"
#include "entity.h"
#include "server_private.h"

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/failoverlist.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/tls_amqp.h"

#include <proton/listener.h>

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>


struct qd_connection_manager_t {
    qd_server_t                  *server;
    qd_listener_list_t            listeners;
    qd_connector_config_list_t    connector_configs;
};


static void log_config(qd_server_config_t *c, const char *what, bool create)
{
    // Log creation/deletion of config objects at INFO level.
    qd_log(LOG_CONN_MGR, QD_LOG_INFO, "%s %s: %s proto=%s, role=%s%s%s%s", create ? "Configured ": "Deleted ", what, c->host_port,
           c->socket_address_family ? c->socket_address_family : "any", c->role, c->http ? ", http" : "",
           c->ssl_profile_name ? ", sslProfile=" : "", c->ssl_profile_name ? c->ssl_profile_name : "");
}


QD_EXPORT qd_listener_t *qd_dispatch_configure_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_connection_manager_t *cm = qd->connection_manager;
    qd_listener_t *li = qd_listener(qd->server);
    if (!li || qd_server_config_load(&li->config, entity, true) != QD_ERROR_NONE) {
        qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create listener: %s", qd_error_message());
        qd_listener_decref(li);
        return 0;
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

    DEQ_ITEM_INIT(li);
    DEQ_INSERT_TAIL(cm->listeners, li);
    log_config(&li->config, "Listener", true);
    return li;
}


QD_EXPORT qd_error_t qd_entity_refresh_listener(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}


/**
 * Calculates the total length of the failover  list string.
 * For example, the failover list string can look like this - "amqp://0.0.0.0:62616, amqp://0.0.0.0:61616"
 * This function calculates the length of the above string by adding up the scheme (amqp or amqps) and host_port for each failover item.
 * It also assumes that there will be a comma and a space between each failover item.
 *
 */
static int get_failover_info_length(qd_failover_item_list_t   conn_info_list)
{
    int arr_length = 0;
    qd_failover_item_t *item = DEQ_HEAD(conn_info_list);

    while(item) {
        if (item->scheme) {
            // The +3 is for the '://'
            arr_length += strlen(item->scheme) + 3;
        }
        if (item->host_port) {
            arr_length += strlen(item->host_port);
        }
        item = DEQ_NEXT(item);
        if (item) {
            // This is for the comma and space between the items
            arr_length += 2;
        }
    }

    if (arr_length > 0)
        // This is for the final '\0'
        arr_length += 1;

    return arr_length;
}


/**
 *
 * Creates a failover url list. This comma separated failover list shows a list of urls that the router will attempt
 * to connect to in case the primary connection fails. The router will attempt these failover connections to urls in
 * the order that they appear in the list.
 *
 */
QD_EXPORT qd_error_t qd_entity_refresh_connector(qd_entity_t* entity, void *impl)
{
    qd_connector_config_t *ctor_config = (qd_connector_config_t *) impl;
    qd_connector_t       *connector  = 0;

    qd_error_clear();

    // TODO(kgiusti): inter-router connections may have several qd_connector_ts active due to the router data connection
    // count configuration.  However we can only report 1 connector via management. It would be more accurate to report
    // all connectors associated with this management entity
    sys_mutex_lock(&ctor_config->lock);
    connector = DEQ_HEAD(ctor_config->connectors);
    if (connector) {
        // prevent I/O thread from freeing connector while it is being accessed
        sys_atomic_inc(&connector->ref_count);
    }
    sys_mutex_unlock(&ctor_config->lock);

    if (connector) {
        int i = 1;
        int num_items = 0;

        sys_mutex_lock(&connector->lock);

        int conn_index = connector->conn_index;
        qd_failover_item_list_t   conn_info_list = connector->conn_info_list;

        int conn_info_len = DEQ_SIZE(conn_info_list);

        qd_failover_item_t *item = DEQ_HEAD(conn_info_list);

        int arr_length = get_failover_info_length(conn_info_list);

        // This is the string that will contain the comma separated failover list
        char *failover_info = qd_calloc(arr_length + 1, sizeof(char));
        while (item) {

            // Break out of the loop when we have hit all items in the list.
            if (num_items >= conn_info_len)
                break;

            if (num_items >= 1) {
                strcat(failover_info, ", ");
            }

            // We need to go to the elements in the list to get to the
            // element that matches the connection index. This is the first
            // url that the router will try to connect on failover.
            if (conn_index == i) {
                num_items += 1;
                if (item->scheme) {
                    strcat(failover_info, item->scheme);
                    strcat(failover_info, "://");
                }
                if (item->host_port) {
                    strcat(failover_info, item->host_port);
                }
            }
            else {
                if (num_items > 0) {
                    num_items += 1;
                    if (item->scheme) {
                        strcat(failover_info, item->scheme);
                        strcat(failover_info, "://");
                    }
                    if (item->host_port) {
                        strcat(failover_info, item->host_port);
                    }
                }
            }

            i += 1;

            item = DEQ_NEXT(item);
            if (item == 0)
                item = DEQ_HEAD(conn_info_list);
        }

        const char *state_info = 0;
        switch (connector->state) {
            case CTOR_STATE_CONNECTING:
                state_info = "CONNECTING";
                break;
            case CTOR_STATE_OPEN:
                state_info = "SUCCESS";
                break;
            case CTOR_STATE_FAILED:
                state_info = "FAILED";
                break;
            case CTOR_STATE_INIT:
                state_info = "INITIALIZING";
                break;
            case CTOR_STATE_DELETED:
                // deleted by management, waiting for connection to close
                state_info = "CLOSING";
                break;
            default:
                state_info = "UNKNOWN";
                break;
        }

        // stop updating entity on first failure to capture the error code
        if (qd_entity_set_string(entity, "failoverUrls", failover_info) == 0
            && qd_entity_set_string(entity, "connectionStatus", state_info) == 0
            && qd_entity_set_string(entity, "connectionMsg", connector->conn_msg) == 0) {
            // error code not set - nothing to do
        }

        sys_mutex_unlock(&connector->lock);
        qd_connector_decref(connector);  // release local reference
        free(failover_info);
    } else {
        qd_error(QD_ERROR_NOT_FOUND, "No active connector present");
    }

    return qd_error_code();
}


QD_EXPORT qd_connector_config_t *qd_dispatch_configure_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_connection_manager_t *cm       = qd->connection_manager;
    qd_connector_config_t    *ctor_config = qd_connector_config_create(qd, entity);
    if (!ctor_config) {
        return 0;
    }

    DEQ_INSERT_TAIL(cm->connector_configs, ctor_config);
    log_config(&ctor_config->config, "Connector", true);
    return ctor_config;
}


qd_connection_manager_t *qd_connection_manager(qd_dispatch_t *qd)
{
    qd_connection_manager_t *cm = NEW(qd_connection_manager_t);
    if (!cm)
        return 0;

    cm->server     = qd->server;
    DEQ_INIT(cm->listeners);
    DEQ_INIT(cm->connector_configs);

    return cm;
}


// Called on router shutdown
//
void qd_connection_manager_free(qd_connection_manager_t *cm)
{
    if (!cm) return;
    qd_listener_t *li = DEQ_HEAD(cm->listeners);
    while (li) {
        DEQ_REMOVE_HEAD(cm->listeners);
        if (li->pn_listener) {
            // DISPATCH-1508: force cleanup of pn_listener context.  This is
            // usually done in the PN_LISTENER_CLOSE event handler in server.c,
            // but since the router is going down those events will no longer
            // be generated.
            pn_listener_set_context(li->pn_listener, 0);
            pn_listener_close(li->pn_listener);
            li->pn_listener = 0;
            qd_listener_decref(li);  // for the pn_listener's context
        }
        qd_listener_decref(li);
        li = DEQ_HEAD(cm->listeners);
    }

    qd_connector_config_t *ctor_config = DEQ_HEAD(cm->connector_configs);
    while (ctor_config) {
        DEQ_REMOVE_HEAD(cm->connector_configs);
        qd_connector_config_delete(ctor_config);
        ctor_config = DEQ_HEAD(cm->connector_configs);
    }

    free(cm);
}


/** NOTE: non-static qd_connection_manager_* functions are called from the python agent */


QD_EXPORT void qd_connection_manager_start(qd_dispatch_t *qd)
{
    static bool first_start = true;
    qd_listener_t  *li = DEQ_HEAD(qd->connection_manager->listeners);
    qd_connector_config_t *ctor_config = DEQ_HEAD(qd->connection_manager->connector_configs);

    while (li) {
        if (!li->pn_listener) {
            if (!qd_listener_listen(li) && first_start) {
                qd_log(LOG_CONN_MGR, QD_LOG_CRITICAL, "Listen on %s failed during initial config",
                       li->config.host_port);
                exit(1);
            } else {
                li->exit_on_error = first_start;
            }
        }
        li = DEQ_NEXT(li);
    }

    while (ctor_config) {
        qd_connector_config_connect(ctor_config);
        ctor_config = DEQ_NEXT(ctor_config);
    }

    first_start = false;
}


QD_EXPORT void qd_connection_manager_delete_listener(qd_dispatch_t *qd, void *impl)
{
    qd_listener_t *li = (qd_listener_t*) impl;
    if (li) {
        if (li->pn_listener) {
            pn_listener_close(li->pn_listener);
        }
        else if (li->http) {
            qd_lws_listener_close(li->http);
        }

        log_config(&li->config, "Listener", false);

        DEQ_REMOVE(qd->connection_manager->listeners, li);
        qd_listener_decref(li);
    }
}


// threading: called by management thread while I/O thread may be
// referencing the qd_connector_t via the qd_connection_t
//
QD_EXPORT void qd_connection_manager_delete_connector(qd_dispatch_t *qd, void *impl)
{
    qd_connector_config_t *ctor_config = (qd_connector_config_t *) impl;
    assert(ctor_config);

    // take it off the connection manager

    log_config(&ctor_config->config, "Connector", false);
    DEQ_REMOVE(qd->connection_manager->connector_configs, ctor_config);
    qd_connector_config_delete(ctor_config);
}


const char *qd_connector_name(qd_connector_t *ct)
{
    return ct ? ct->ctor_config->config.name : 0;
}


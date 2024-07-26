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

#include <proton/listener.h>

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#define CHECK() if (qd_error_code()) goto error

DEQ_DECLARE(qd_config_ssl_profile_t, qd_config_ssl_profile_list_t);

struct qd_connection_manager_t {
    qd_server_t                  *server;
    qd_listener_list_t            listeners;
    qd_connector_list_t           connectors;
    qd_connector_list_t           data_connectors;
    qd_config_ssl_profile_list_t  config_ssl_profiles;
};


/**
 * Search the list of config_ssl_profiles for an ssl-profile that matches the passed in name
 */
qd_config_ssl_profile_t *qd_find_ssl_profile(const qd_connection_manager_t *cm, const char *ssl_profile_name)
{
    qd_config_ssl_profile_t *ssl_profile = DEQ_HEAD(cm->config_ssl_profiles);
    while (ssl_profile) {
        if (strcmp(ssl_profile->name, ssl_profile_name) == 0)
            return ssl_profile;
        ssl_profile = DEQ_NEXT(ssl_profile);
    }

    return 0;
}


static bool config_ssl_profile_free(qd_connection_manager_t *cm, qd_config_ssl_profile_t *ssl_profile)
{
    DEQ_REMOVE(cm->config_ssl_profiles, ssl_profile);

    free(ssl_profile->name);
    free(ssl_profile->ssl_password);
    free(ssl_profile->ssl_trusted_certificate_db);
    free(ssl_profile->ssl_uid_format);
    free(ssl_profile->uid_name_mapping_file);
    free(ssl_profile->ssl_certificate_file);
    free(ssl_profile->ssl_private_key_file);
    free(ssl_profile->ssl_ciphers);
    free(ssl_profile->ssl_protocols);
    free(ssl_profile);
    return true;

}


QD_EXPORT qd_config_ssl_profile_t *qd_dispatch_configure_ssl_profile(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_error_clear();
    qd_connection_manager_t *cm = qd->connection_manager;

    qd_config_ssl_profile_t *ssl_profile = NEW(qd_config_ssl_profile_t);
    ZERO(ssl_profile);
    DEQ_ITEM_INIT(ssl_profile);
    DEQ_INSERT_TAIL(cm->config_ssl_profiles, ssl_profile);
    ssl_profile->name                       = qd_entity_opt_string(entity, "name", 0); CHECK();
    ssl_profile->ssl_certificate_file       = qd_entity_opt_string(entity, "certFile", 0); CHECK();
    ssl_profile->ssl_private_key_file       = qd_entity_opt_string(entity, "privateKeyFile", 0); CHECK();
    ssl_profile->ssl_password               = qd_entity_opt_string(entity, "password", 0); CHECK();

    if (ssl_profile->ssl_password) {
        //
        // Process the password to handle any modifications or lookups needed
        //
        char *actual_pass = 0;
        bool is_file_path = 0;
        qd_server_config_process_password(&actual_pass, ssl_profile->ssl_password, &is_file_path, true);
        CHECK();
        if (actual_pass) {
            if (is_file_path) {
                qd_set_password_from_file(actual_pass, &ssl_profile->ssl_password);
                free(actual_pass);
            }
            else {
                free(ssl_profile->ssl_password);
                ssl_profile->ssl_password = actual_pass;
            }
        }
    }

    ssl_profile->ssl_ciphers   = qd_entity_opt_string(entity, "ciphers", 0);                   CHECK();
    ssl_profile->ssl_protocols = qd_entity_opt_string(entity, "protocols", 0);                 CHECK();
    ssl_profile->ssl_trusted_certificate_db = qd_entity_opt_string(entity, "caCertFile", 0);   CHECK();
    ssl_profile->ssl_uid_format             = qd_entity_opt_string(entity, "uidFormat", 0);          CHECK();
    ssl_profile->uid_name_mapping_file      = qd_entity_opt_string(entity, "uidNameMappingFile", 0); CHECK();

    qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Created SSL Profile with name %s ", ssl_profile->name);
    return ssl_profile;

    error:
    qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create ssl profile: %s", qd_error_message());
    config_ssl_profile_free(cm, ssl_profile);
    return 0;
}

static void log_config(qd_server_config_t *c, const char *what, bool create)
{
    // Log creation/deletion of config objects at INFO level.
    qd_log(LOG_CONN_MGR, QD_LOG_INFO, "%s %s: %s proto=%s, role=%s%s%s%s", create ? "Configured ": "Deleted ", what, c->host_port,
           c->socket_address_family ? c->socket_address_family : "any", c->role, c->http ? ", http" : "",
           c->ssl_profile ? ", sslProfile=" : "", c->ssl_profile ? c->ssl_profile : "");
}


QD_EXPORT qd_listener_t *qd_dispatch_configure_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_connection_manager_t *cm = qd->connection_manager;
    qd_listener_t *li = qd_listener(qd->server);
    if (!li || qd_server_config_load(qd, &li->config, entity, true, 0) != QD_ERROR_NONE) {
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
    qd_connector_t *connector = (qd_connector_t*) impl;

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
    while(item) {

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
    case CXTR_STATE_CONNECTING:
      state_info = "CONNECTING";
      break;
    case CXTR_STATE_OPEN:
      state_info = "SUCCESS";
      break;
    case CXTR_STATE_FAILED:
      state_info = "FAILED";
      break;
    case CXTR_STATE_INIT:
      state_info = "INITIALIZING";
      break;
    case CXTR_STATE_DELETED:
      // deleted by management, waiting for connection to close
      state_info = "CLOSING";
      break;
    default:
      state_info = "UNKNOWN";
      break;
    }

    if (qd_entity_set_string(entity, "failoverUrls", failover_info) == 0
        && qd_entity_set_string(entity, "connectionStatus", state_info) == 0
        && qd_entity_set_string(entity, "connectionMsg", connector->conn_msg) == 0) {

        sys_mutex_unlock(&connector->lock);
        free(failover_info);
        return QD_ERROR_NONE;
    }

    sys_mutex_unlock(&connector->lock);
    free(failover_info);
    return qd_error_code();
}


// This is used to calculate inter-router data connection
// count when the user explicitly requests 'auto'
// or lets it default to that.
static int auto_calc_connection_count(qd_dispatch_t *qd)
{
  return (qdr_core_get_worker_thread_count(qd_dispatch_router_core(qd)) + 1) / 2;
}


QD_EXPORT qd_connector_t *qd_dispatch_configure_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_connection_manager_t *cm = qd->connection_manager;
    qd_connector_t *ct = qd_server_connector(qd->server);

    qd_error_clear();

    if (ct && qd_server_config_load(qd, &ct->config, entity, false, 0) == QD_ERROR_NONE) {
        ct->policy_vhost = qd_entity_opt_string(entity, "policyVhost", 0); CHECK();
        DEQ_ITEM_INIT(ct);
        DEQ_INSERT_TAIL(cm->connectors, ct);
        log_config(&ct->config, "Connector", true);

        uint32_t connection_count = 0;
        //
        // If the user asks for automatic setting of the number
        // of data connnection count, make one data connection
        // for every two worker threads. This is the best ratio
        // for high load, as determined by throughput performance
        // testing.
        //
        if (!strcmp("auto", ct->config.data_connection_count)) {
            // The user has explicitly requested 'auto'.
            connection_count = auto_calc_connection_count(qd);
            qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Inter-router data connections calculated at %d ",
                    connection_count);
        } else if (1 == sscanf(ct->config.data_connection_count, "%u", &connection_count)) {
            // The user has requested a specific number of connections.
            qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Inter-router data connections set to %d ", connection_count);
        } else {
            // The user has entered a non-numeric value that is not 'auto'.
            // This is not a legal value. Default to 'auto' and mention it.
            qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Bad value \"%s\" for dataConnectionCount ",
                    ct->config.data_connection_count);
            connection_count = auto_calc_connection_count(qd);
            qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Inter-router data connections calculated at %d ",
                    connection_count);
        }

        //
        // If this connection has a data-connection-group, set up the group members now
        //
        if (ct->config.has_data_connectors) {
            qd_generate_discriminator(ct->group_correlator);
            for (uint32_t i = 0; i < connection_count; i++) {
                qd_connector_t *dc = qd_server_connector(qd->server);
                if (dc && qd_server_config_load(qd, &dc->config, entity, false, "inter-router-data") == QD_ERROR_NONE) {
                    strncpy(dc->group_correlator, ct->group_correlator, QD_DISCRIMINATOR_SIZE);
                    dc->is_data_connector = true;
                    DEQ_INSERT_TAIL(cm->data_connectors, dc);
                    qd_failover_item_t *item = NEW(qd_failover_item_t);
                    ZERO(item);
                    if (dc->config.ssl_required)
                        item->scheme = strdup("amqps");
                    else
                        item->scheme = strdup("amqp");
                    item->host = strdup(dc->config.host);
                    item->port = strdup(dc->config.port);
                    int hplen = strlen(item->host) + strlen(item->port) + 2;
                    item->host_port = malloc(hplen);
                    snprintf(item->host_port, hplen, "%s:%s", item->host , item->port);
                    DEQ_INSERT_TAIL(dc->conn_info_list, item);
                }
            }
        }

        //
        // Add the first item to the ct->conn_info_list
        // The initial connection information and any backup connection information is stored in the conn_info_list
        //
        qd_failover_item_t *item = NEW(qd_failover_item_t);
        ZERO(item);
        if (ct->config.ssl_required)
            item->scheme   = strdup("amqps");
        else
            item->scheme   = strdup("amqp");

        item->host     = strdup(ct->config.host);
        item->port     = strdup(ct->config.port);

        int hplen = strlen(item->host) + strlen(item->port) + 2;
        item->host_port = malloc(hplen);
        snprintf(item->host_port, hplen, "%s:%s", item->host , item->port);

        //
        // Set up the vanflow record for this connector (LINK)
        // Do this only for router-to-router connectors since the record represents an inter-router link
        //
        if (strcmp(ct->config.role, "inter-router") == 0 ||
            strcmp(ct->config.role, "edge") == 0 ||
            strcmp(ct->config.role, "inter-edge") == 0) {
            ct->vflow_record = vflow_start_record(VFLOW_RECORD_LINK, 0);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_NAME, ct->config.name);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_ROLE, ct->config.role);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_OPER_STATUS, "down");
            vflow_set_uint64(ct->vflow_record, VFLOW_ATTRIBUTE_DOWN_COUNT, 0);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_PROTOCOL, item->scheme);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_DESTINATION_HOST, item->host);
            vflow_set_string(ct->vflow_record, VFLOW_ATTRIBUTE_DESTINATION_PORT, item->port);
            vflow_set_uint64(ct->vflow_record, VFLOW_ATTRIBUTE_OCTETS, 0);
            vflow_set_uint64(ct->vflow_record, VFLOW_ATTRIBUTE_OCTETS_REVERSE, 0);
            vflow_add_rate(ct->vflow_record, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
            vflow_add_rate(ct->vflow_record, VFLOW_ATTRIBUTE_OCTETS_REVERSE, VFLOW_ATTRIBUTE_OCTET_RATE_REVERSE);
        }

        DEQ_INSERT_TAIL(ct->conn_info_list, item);
        return ct;
    }

  error:
    qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to create connector: %s", qd_error_message());
    ct->state = CXTR_STATE_DELETED;
    qd_connector_decref(ct);
    return 0;
}


qd_connection_manager_t *qd_connection_manager(qd_dispatch_t *qd)
{
    qd_connection_manager_t *cm = NEW(qd_connection_manager_t);
    if (!cm)
        return 0;

    cm->server     = qd->server;
    DEQ_INIT(cm->listeners);
    DEQ_INIT(cm->connectors);
    DEQ_INIT(cm->data_connectors);
    DEQ_INIT(cm->config_ssl_profiles);

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

    qd_connector_list_t to_free;
    DEQ_MOVE(cm->connectors, to_free);
    DEQ_APPEND(to_free, cm->data_connectors);

    qd_connector_t *connector = DEQ_HEAD(to_free);
    while (connector) {
        DEQ_REMOVE_HEAD(to_free);
        sys_mutex_lock(&connector->lock);
        // setting DELETED below ensures the timer callback
        // will not initiate a re-connect once we drop
        // the lock
        connector->state = CXTR_STATE_DELETED;
        sys_mutex_unlock(&connector->lock);
        // cannot cancel timer while holding lock since the
        // callback takes the lock
        qd_timer_cancel(connector->timer);
        qd_connector_decref(connector);

        connector = DEQ_HEAD(to_free);
    }

    qd_config_ssl_profile_t *sslp = DEQ_HEAD(cm->config_ssl_profiles);
    while (sslp) {
        config_ssl_profile_free(cm, sslp);
        sslp = DEQ_HEAD(cm->config_ssl_profiles);
    }


    free(cm);
}


/** NOTE: non-static qd_connection_manager_* functions are called from the python agent */


QD_EXPORT void qd_connection_manager_start(qd_dispatch_t *qd)
{
    static bool first_start = true;
    qd_listener_t  *li = DEQ_HEAD(qd->connection_manager->listeners);
    qd_connector_t *ct = DEQ_HEAD(qd->connection_manager->connectors);
    qd_connector_t *dc = DEQ_HEAD(qd->connection_manager->data_connectors);

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

    while (ct) {
        if (ct->state == CXTR_STATE_OPEN || ct->state == CXTR_STATE_CONNECTING) {
            ct = DEQ_NEXT(ct);
            continue;
        }

        qd_connector_connect(ct);
        ct = DEQ_NEXT(ct);
    }

    while (dc) {
        if (dc->state == CXTR_STATE_OPEN || dc->state == CXTR_STATE_CONNECTING) {
            dc = DEQ_NEXT(dc);
            continue;
        }

        qd_connector_connect(dc);
        dc = DEQ_NEXT(dc);
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


QD_EXPORT void qd_connection_manager_delete_ssl_profile(qd_dispatch_t *qd, void *impl)
{
    qd_config_ssl_profile_t *ssl_profile = (qd_config_ssl_profile_t*) impl;

    qd_log(LOG_CONN_MGR, QD_LOG_INFO, "Deleted SSL Profile with name %s ", ssl_profile->name);

    config_ssl_profile_free(qd->connection_manager, ssl_profile);
}

static void deferred_close(void *context, bool discard) {
    if (!discard) {
        pn_connection_close((pn_connection_t*)context);
    }
}


// threading: called by management thread while I/O thread may be
// referencing the qd_connector_t via the qd_connection_t
//
QD_EXPORT void qd_connection_manager_delete_connector(qd_dispatch_t *qd, void *impl)
{
    qd_connector_t *ct = (qd_connector_t*) impl;
    if (ct) {
        // cannot free the timer while holding ct->lock since the
        // timer callback may be running during the call to qd_timer_free
        qd_timer_t *timer = 0;
        bool        has_data_connectors = ct->config.has_data_connectors;
        void *dct = qd_connection_new_qd_deferred_call_t();
        sys_mutex_lock(&ct->lock);
        timer = ct->timer;
        ct->timer = 0;
        ct->state = CXTR_STATE_DELETED;
        qd_connection_t *conn = ct->qd_conn;
        if (conn && conn->pn_conn) {
            qd_connection_invoke_deferred_impl(conn, deferred_close, conn->pn_conn, dct);
            sys_mutex_unlock(&ct->lock);
        } else {
            sys_mutex_unlock(&ct->lock);
            qd_connection_free_qd_deferred_call_t(dct);
        }
        qd_timer_free(timer);
        if (ct->is_data_connector) {
            DEQ_REMOVE(qd->connection_manager->data_connectors, ct);
        } else {
            log_config(&ct->config, "Connector", false);
            DEQ_REMOVE(qd->connection_manager->connectors, ct);
        }

        //
        // Remove correlated data connectors
        //
        if (has_data_connectors) {
            qd_connector_t *dc = DEQ_HEAD(qd->connection_manager->data_connectors);
            while (!!dc) {
                qd_connector_t *next = DEQ_NEXT(dc);
                if (strncmp(dc->group_correlator, ct->group_correlator, QD_DISCRIMINATOR_SIZE) == 0) {
                    qd_connection_manager_delete_connector(qd, (void*) dc);
                }
                dc = next;
            }
        }

        qd_connector_decref(ct);
    }
}


const char *qd_connector_name(qd_connector_t *ct)
{
    return ct ? ct->config.name : 0;
}


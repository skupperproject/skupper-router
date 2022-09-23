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

#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"

#include <proton/netaddr.h>

#include <inttypes.h>
#include <sys/socket.h>

ALLOC_DEFINE(qd_adaptor_config_t);

#define NUM_ALPN_PROTOCOLS 1

bool qd_tls_initial_setup(qd_adaptor_config_t *config,
                          qd_dispatch_t       *qd,
                          pn_tls_config_t     **tls_config,
                          pn_tls_t            **tls_session,
                          qd_log_source_t     *log_source,
                          uint64_t             conn_id,
                          bool                 is_listener,
                          bool                *tls_has_output,
                          const char          *protocols[])
{
    const char *role = is_listener ? "listener" : "connector";

    qd_log(log_source, QD_LOG_INFO, "[C%"PRIu64"] %s %s configuring ssl profile %s", conn_id, role, config->name, config->ssl_profile_name);

    do {
        // find the ssl profile
        assert(qd);
        qd_connection_manager_t *cm = qd_dispatch_connection_manager(qd);
        assert(cm);
        qd_config_ssl_profile_t *config_ssl_profile = qd_find_ssl_profile(cm, config->ssl_profile_name);
        if (!config_ssl_profile) {
            qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to find ssl profile %s", conn_id, config->name, config->ssl_profile_name);
            break;
        }

        int res;
        // First free, then create pn domain
        if (*tls_config)
            pn_tls_config_free(*tls_config);
        *tls_config = pn_tls_config(is_listener ? PN_TLS_MODE_SERVER : PN_TLS_MODE_CLIENT);

        if (! *tls_config) {
            qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to create tls domain for ssl profile %s", conn_id, role, config->name, config->ssl_profile_name);
            break;
        }

        if (config_ssl_profile->ssl_trusted_certificate_db) {
            res = pn_tls_config_set_trusted_certs(*tls_config, config_ssl_profile->ssl_trusted_certificate_db);
            if (res != 0) {
                qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to set tls trusted certificates (%d)", conn_id, role, config->name, res);
                break;
            }
        }

        // Call pn_tls_config_set_credentials only if "certFile" is provided.
        if (config_ssl_profile->ssl_certificate_file) {
            res = pn_tls_config_set_credentials(*tls_config,
                                                config_ssl_profile->ssl_certificate_file,
                                                config_ssl_profile->ssl_private_key_file,
                                                config_ssl_profile->ssl_password);
            if (res != 0) {
                qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] HTTP2 %s %s unable to set tls credentials (%d)", conn_id, role, config->name, res);
                break;
            }
        }
        else {
            qd_log(log_source, QD_LOG_INFO, "[C%"PRIu64"] sslProfile %s did not provide certFile", conn_id, config->ssl_profile_name);
        }


        if (!!config_ssl_profile->ssl_ciphers) {
            res = pn_tls_config_set_impl_ciphers(*tls_config, config_ssl_profile->ssl_ciphers);
            if (res != 0) {
                qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to set tls ciphers (%d)", conn_id, role, config->name, res);
                break;
            }
        }


        if (is_listener) {
            if (config->authenticate_peer) {
                res = pn_tls_config_set_peer_authentication(*tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }
            else {
                res = pn_tls_config_set_peer_authentication(*tls_config, PN_TLS_ANONYMOUS_PEER, 0);
            }
        }
        else {
            // Connector.
            if (config->verify_host_name) {
                res = pn_tls_config_set_peer_authentication(*tls_config, PN_TLS_VERIFY_PEER_NAME, config_ssl_profile->ssl_trusted_certificate_db);
            }
            else {
                res = pn_tls_config_set_peer_authentication(*tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }

            *tls_has_output = true; // always true for initial client side TLS.
        }

        if (res != 0) {
            qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] Unable to set tls peer authentication for sslProfile %s - (%d)", conn_id, config->ssl_profile_name, res);
            break;
        }

        //
        // Provide an ordered list of application protocols for ALPN by calling pn_tls_config_set_alpn_protocols. In our case, h2 is the only supported protocol.
        // A list of protocols can be found here - https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.txt
        //
        if (protocols)
            pn_tls_config_set_alpn_protocols(*tls_config, protocols, NUM_ALPN_PROTOCOLS);

        // set up tls session
        if (*tls_session) {
            pn_tls_free(*tls_session);
        }
        *tls_session = pn_tls(*tls_config);

        if (! *tls_session) {
            qd_log(log_source, QD_LOG_ERROR, "[C%"PRIu64"] Unable to create tls session for sslProfile %s with hostname: '%s'", conn_id, config->ssl_profile_name, config->host);
            break;
        }

        int ret = pn_tls_start(*tls_session);
        if (ret != 0) {
            break;
        }


        pn_tls_set_peer_hostname(*tls_session, config->host);


        qd_log(log_source, QD_LOG_INFO, "[C%"PRIu64"] Successfully configured ssl profile %s", conn_id, config->ssl_profile_name);

        return true;


    } while (0);

    // Handle tls creation/setup failure by deleting any pn domain or session objects
    if (*tls_session) {
        pn_tls_free(*tls_session);
        *tls_session = 0;
    }

    if (*tls_config) {
        pn_tls_config_free(*tls_config);
        *tls_config = 0;
    }


    return false;
}

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

qd_error_t qd_load_adaptor_config(qd_dispatch_t *qd, qd_adaptor_config_t *config, qd_entity_t* entity, qd_log_source_t *log_source)
{
    qd_error_clear();
    config->name    = qd_entity_opt_string(entity, "name", 0);                 CHECK();
    config->host    = qd_entity_get_string(entity, "host");                    CHECK();
    config->port    = qd_entity_get_string(entity, "port");                    CHECK();
    config->address = qd_entity_get_string(entity, "address");                 CHECK();
    config->site_id = qd_entity_opt_string(entity, "siteId", 0);               CHECK();
    config->ssl_profile_name  = qd_entity_opt_string(entity, "sslProfile", 0); CHECK();
    config->authenticate_peer = qd_entity_opt_bool(entity, "authenticatePeer", false); CHECK();
    config->verify_host_name  = qd_entity_opt_bool(entity, "verifyHostname", false);   CHECK();

    config->backlog = qd_entity_opt_long(entity, "backlog", 0);
    CHECK();
    if (config->backlog <= 0 || config->backlog > SOMAXCONN)
        config->backlog = SOMAXCONN;

    int hplen = strlen(config->host) + strlen(config->port) + 2;
    config->host_port = malloc(hplen);
    snprintf(config->host_port, hplen, "%s:%s", config->host, config->port);

    if (config->ssl_profile_name) {
        qd_connection_manager_t *cm = qd_dispatch_connection_manager(qd);
        assert(cm);
        qd_config_ssl_profile_t *config_ssl_profile = qd_find_ssl_profile(cm, config->ssl_profile_name);

        if(!config_ssl_profile) {
            //
            // The sslProfile was not found, we are going to terminate the router.
            //
            qd_log(log_source, QD_LOG_CRITICAL, "sslProfile %s could not be found", config->ssl_profile_name);
            exit(1);
        }
    }

    return QD_ERROR_NONE;

error:
    return qd_error_code();
}

int qd_raw_connection_grant_read_buffers(pn_raw_connection_t *pn_raw_conn, qd_adaptor_buffer_list_t *granted_read_buffs)
{
    assert(pn_raw_conn);
    pn_raw_buffer_t raw_buffers[RAW_BUFFER_BATCH];
    size_t          desired = pn_raw_connection_read_buffers_capacity(pn_raw_conn);
    const size_t    granted = desired;

    while (desired) {
        int i;
        for (i = 0; i < desired && i < RAW_BUFFER_BATCH; ++i) {
            qd_adaptor_buffer_t *buf = qd_adaptor_buffer();
            if (granted_read_buffs)
                DEQ_INSERT_TAIL(*granted_read_buffs, buf);
            raw_buffers[i].bytes    = (char *) qd_adaptor_buffer_base(buf);
            raw_buffers[i].capacity = qd_adaptor_buffer_capacity(buf);
            raw_buffers[i].size     = 0;
            raw_buffers[i].offset   = 0;
            raw_buffers[i].context  = (uintptr_t) buf;
        }
        desired -= i;
        pn_raw_connection_give_read_buffers(pn_raw_conn, raw_buffers, i);
    }

    return granted;
}

int qd_raw_connection_write_buffers(pn_raw_connection_t *pn_raw_conn, qd_adaptor_buffer_list_t *blist)
{
    if (!pn_raw_conn)
        return 0;

    size_t pn_buffs_to_write     = pn_raw_connection_write_buffers_capacity(pn_raw_conn);
    size_t qd_raw_buffs_to_write = DEQ_SIZE(*blist);
    size_t num_buffs             = MIN(qd_raw_buffs_to_write, pn_buffs_to_write);

    if (num_buffs == 0)
        return 0;

    pn_raw_buffer_t      raw_buffers[num_buffs];
    qd_adaptor_buffer_t *qd_adaptor_buff = DEQ_HEAD(*blist);

    int i = 0;

    while (i < num_buffs) {
        assert(qd_adaptor_buff != 0);
        raw_buffers[i].bytes    = (char *) qd_adaptor_buffer_base(qd_adaptor_buff);
        size_t buffer_size      = qd_adaptor_buffer_size(qd_adaptor_buff);
        raw_buffers[i].size     = buffer_size;
        raw_buffers[i].offset   = 0;
        raw_buffers[i].capacity = 0;
        raw_buffers[i].context = (uintptr_t) qd_adaptor_buff;
        DEQ_REMOVE_HEAD(*blist);
        qd_adaptor_buff = DEQ_HEAD(*blist);
        i++;
    }

    size_t num_buffers_written = pn_raw_connection_write_buffers(pn_raw_conn, raw_buffers, num_buffs);
    assert(num_buffs == num_buffers_written);
    return num_buffers_written;
}

char *qd_raw_conn_get_address(pn_raw_connection_t *pn_raw_conn)
{
    const pn_netaddr_t *netaddr = pn_raw_connection_remote_addr(pn_raw_conn);
    char                buffer[1024];
    int                 len = pn_netaddr_str(netaddr, buffer, 1024);
    if (len <= 1024) {
        return strdup(buffer);
    } else {
        return strndup(buffer, 1024);
    }
}

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

qd_error_t qd_load_adaptor_config(qd_adaptor_config_t *config, qd_entity_t *entity)
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

    return QD_ERROR_NONE;

error:
    return qd_error_code();
}

int qd_raw_connection_grant_read_buffers(pn_raw_connection_t *pn_raw_conn)
{
    assert(pn_raw_conn);
    pn_raw_buffer_t raw_buffers[RAW_BUFFER_BATCH];
    size_t          desired = pn_raw_connection_read_buffers_capacity(pn_raw_conn);
    const size_t    granted = desired;

    while (desired) {
        int i;
        for (i = 0; i < desired && i < RAW_BUFFER_BATCH; ++i) {
            qd_adaptor_buffer_t *buf = qd_adaptor_buffer();
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

int qd_raw_connection_drain_read_write_buffers(pn_raw_connection_t *pn_raw_conn)
{
    pn_raw_buffer_t buffs[RAW_BUFFER_BATCH];
    size_t          n;
    int             buffers_drained = 0;
    while ((n = pn_raw_connection_take_written_buffers(pn_raw_conn, buffs, RAW_BUFFER_BATCH))) {
        for (size_t i = 0; i < n; ++i) {
            buffers_drained++;
            qd_adaptor_buffer_t *qd_adaptor_buffer = (qd_adaptor_buffer_t *) buffs[i].context;
            qd_adaptor_buffer_free(qd_adaptor_buffer);
        }
    }

    while ((n = pn_raw_connection_take_read_buffers(pn_raw_conn, buffs, RAW_BUFFER_BATCH))) {
        for (size_t i = 0; i < n; ++i) {
            assert(buffs[i].size == 0);
            buffers_drained++;
            qd_adaptor_buffer_t *qd_adaptor_buffer = (qd_adaptor_buffer_t *) buffs[i].context;
            qd_adaptor_buffer_free(qd_adaptor_buffer);
        }
    }
    return buffers_drained;
}

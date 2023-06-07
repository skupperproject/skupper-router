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
#include "adaptor_buffer.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/platform.h"

#include <proton/netaddr.h>

#include <inttypes.h>
#include <sys/socket.h>

ALLOC_DEFINE(qd_adaptor_config_t);

static uint64_t buffer_ceiling = 0;
static uint64_t buffer_threshold_50;
static uint64_t buffer_threshold_75;
static uint64_t buffer_threshold_85;

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

void qd_adaptor_common_init(void)
{
    if (buffer_ceiling > 0) {
        return;
    }

    char     *ceiling_string = getenv("SKUPPER_ROUTER_MEMORY_CEILING");
    uint64_t  memory_ceiling = (uint64_t) qd_platform_memory_size();

    //
    // In the event the platform module fails to return a size, just use 4Gig.
    //
    if (memory_ceiling == 0) {
        memory_ceiling = (uint64_t) 4 * (uint64_t) 1024 * (uint64_t) 1024 * (uint64_t) 1024;
    }

    if (!!ceiling_string) {
        long long convert = atoll(ceiling_string);
        if (convert > 0) {
            memory_ceiling = (uint64_t) convert;
        }
    }

    buffer_ceiling = MAX(memory_ceiling / QD_ADAPTOR_MAX_BUFFER_SIZE, 100);
    buffer_threshold_50 = buffer_ceiling / 2;
    buffer_threshold_75 = (buffer_ceiling / 20) * 15;
    buffer_threshold_85 = (buffer_ceiling / 20) * 17;

    //qd_log(LOG_ROUTER, QD_LOG_INFO, "Adaptor buffer memory ceiling: %"PRIu64" (%"PRIu64" buffers)", memory_ceiling, buffer_ceiling);
}

int qd_raw_connection_grant_read_buffers(pn_raw_connection_t *pn_raw_conn)
{
    //
    // Define the allocation tiers.  The tier values are the number of read buffers to be granted
    // to raw connections based on the percentage of usage of the router-wide buffer ceiling.
    //
#define TIER_1 8  // [0% .. 50%)
#define TIER_2 4  // [50% .. 75%)
#define TIER_3 2  // [75% .. 85%)
#define TIER_4 2  // [85% .. 100%]

    assert(pn_raw_conn);
    pn_raw_buffer_t raw_buffers[RAW_BUFFER_BATCH];

    //
    // Get the read-buffer capacity for the connection.
    //
    size_t capacity = pn_raw_connection_read_buffers_capacity(pn_raw_conn);

    //
    // If there's no capacity, exit now before doing any further wasted work.
    //
    if (capacity == 0) {
        return 0;
    }

    //
    // Since we can't query Proton for the maximum read-buffer capacity, we will infer it from
    // calls to pn_raw_connection_read_buffers_capacity.
    //
    static size_t max_capacity = 0;
    if (capacity > max_capacity) {
        max_capacity = capacity;
    }

    //
    // Get the "held_by_threads" stats for adaptor buffers as an approximation of how many
    // buffers are in-use.  This is an approximation since it also counts free buffers held
    // in the per-thread free-pools.  Since we will be dealing with large numbers here, the
    // number of buffers in free-pools will not be significant.
    //
    // Note that there is a thread race on the access of this value.  There's no danger associated
    // with getting a partial or corrupted value from time to time.
    //
    // Note also that the stats pointer may be NULL if no buffers have yet been allocated.
    //
    qd_alloc_stats_t *stats          = alloc_stats_qd_adaptor_buffer_t();
    uint64_t          buffers_in_use = !!stats ? stats->held_by_threads : 0;

    //
    // Choose the grant-allocation tier based on the number of buffers in use.
    //
    size_t desired = TIER_4;
    if (buffers_in_use < buffer_threshold_50) {
        desired = TIER_1;
    } else if (buffers_in_use < buffer_threshold_75) {
        desired = TIER_2;
    } else if (buffers_in_use < buffer_threshold_85) {
        desired = TIER_3;
    }

    //
    // Determine how many of the desired buffers are already granted.  This will always be a
    // non-negative value.
    //
    size_t already_granted = max_capacity - capacity;

    //
    // If we desire to grant additional buffers, calculate the number to grant now.
    //
    const size_t to_grant = desired > already_granted ? desired - already_granted : 0;
    size_t       count    = to_grant;

    //
    // Grant the buffers in batches.
    //
    while (count) {
        int i;
        for (i = 0; i < count && i < RAW_BUFFER_BATCH; ++i) {
            qd_adaptor_buffer_t *buf = qd_adaptor_buffer();
            raw_buffers[i].bytes    = (char *) qd_adaptor_buffer_base(buf);
            raw_buffers[i].capacity = qd_adaptor_buffer_capacity(buf);
            raw_buffers[i].size     = 0;
            raw_buffers[i].offset   = 0;
            raw_buffers[i].context  = (uintptr_t) buf;
        }
        count -= i;
        pn_raw_connection_give_read_buffers(pn_raw_conn, raw_buffers, i);
    }

    return to_grant;
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

int qd_raw_connection_drain_write_buffers(pn_raw_connection_t *pn_raw_conn)
{
    pn_raw_buffer_t buffs[RAW_BUFFER_BATCH];
    size_t          n;
    int             write_buffers_drained = 0;
    while ((n = pn_raw_connection_take_written_buffers(pn_raw_conn, buffs, RAW_BUFFER_BATCH))) {
        for (size_t i = 0; i < n; ++i) {
            write_buffers_drained++;
            qd_adaptor_buffer_t *qd_adaptor_buffer = (qd_adaptor_buffer_t *) buffs[i].context;
            qd_adaptor_buffer_free(qd_adaptor_buffer);
        }
    }
    return write_buffers_drained;
}

int qd_raw_connection_drain_read_buffers(pn_raw_connection_t *pn_raw_conn)
{
    pn_raw_buffer_t buffs[RAW_BUFFER_BATCH];
    size_t          n;
    int             read_buffers_drained = 0;
    while ((n = pn_raw_connection_take_read_buffers(pn_raw_conn, buffs, RAW_BUFFER_BATCH))) {
        for (size_t i = 0; i < n; ++i) {
            read_buffers_drained++;
            qd_adaptor_buffer_t *qd_adaptor_buffer = (qd_adaptor_buffer_t *) buffs[i].context;
            qd_adaptor_buffer_free(qd_adaptor_buffer);
        }
    }
    return read_buffers_drained;
}

int qd_raw_connection_drain_read_write_buffers(pn_raw_connection_t *pn_raw_conn)
{
    int             buffers_drained = qd_raw_connection_drain_write_buffers(pn_raw_conn);
    buffers_drained += qd_raw_connection_drain_read_buffers(pn_raw_conn);
    return buffers_drained;
}

void qd_set_vflow_netaddr_string(vflow_record_t *vflow, pn_raw_connection_t *pn_raw_conn, bool ingress)
{
    char                remote_host[200];
    char                remote_port[50];
    const pn_netaddr_t *na =
        ingress ? pn_raw_connection_remote_addr(pn_raw_conn) : pn_raw_connection_local_addr(pn_raw_conn);
    if (pn_netaddr_host_port(na, remote_host, 200, remote_port, 50) == 0) {
        vflow_set_string(vflow, VFLOW_ATTRIBUTE_SOURCE_HOST, remote_host);
        vflow_set_string(vflow, VFLOW_ATTRIBUTE_SOURCE_PORT, remote_port);
    }
}

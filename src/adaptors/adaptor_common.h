#ifndef __adaptor_common_h__
#define __adaptor_common_h__ 1

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

#include "adaptors/adaptor_buffer.h"
#include "entity.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/router_core.h"

#include <proton/raw_connection.h>
#include <proton/tls.h>

#define QD_IO_EOS     -2  // I/O layer: signal clean end-of-output stream
#define QD_IO_EOS_ERR -3  // I/O layer: signal error end-of-output stream

#define RAW_BUFFER_BATCH 4

typedef enum {
    QD_AGGREGATION_NONE,
    QD_AGGREGATION_JSON,
    QD_AGGREGATION_MULTIPART
} qd_http_aggregation_t;

typedef struct qd_adaptor_config_t qd_adaptor_config_t;

struct qd_adaptor_config_t
{
    char              *name;
    char              *host;
    char              *port;
    char              *address;
    char              *site_id;
    char              *host_port;
    int                backlog;

    //TLS related info
    char              *ssl_profile_name;
    bool               authenticate_peer;
    bool               verify_host_name;
};


ALLOC_DECLARE(qd_adaptor_config_t);

qd_error_t qd_load_adaptor_config(qdr_core_t *core, qd_adaptor_config_t *config, qd_entity_t *entity);
void qd_free_adaptor_config(qd_adaptor_config_t *config);

/**
 * Perform router-startup actions used in the common module.
 */
void qd_adaptor_common_init(void);

/**
 * Grants as many read qd_adaptor buffers as returned by pn_raw_connection_read_buffers_capacity().
 * Maximum read capacity is set to 16 in proton raw api.
 *
 * @param raw_conn - The pn_raw_connection_t to which read buffers are granted.
 */
int qd_raw_connection_grant_read_buffers(pn_raw_connection_t *pn_raw_conn);

/**
 * Writes as many adaptor buffers as allowed by pn_raw_connection_write_buffers_capacity().
 * Maximum write capacity is set to 16 in proton raw api.
 *
 * @param raw_conn - The pn_raw_connection_t to which read buffers are granted.
 * @param blist - qd_adaptor_buffer_list_t which contains that buffers that need to be written.
 */
int qd_raw_connection_write_buffers(pn_raw_connection_t *pn_raw_conn, qd_adaptor_buffer_list_t *blist);

/**
 * Get the raw connections remote address.
 * Caller must free() the result when done.
 */
char *qd_raw_conn_get_address(pn_raw_connection_t *pn_raw_conn);

/**
 * Get the raw connections remote address.
 * Like qd_raw_conn_get_address(), but address buffer is supplied by caller.
 * @return number of bytes written, zero if no address available (buf is set to the null string).
 */
size_t qd_raw_conn_get_address_buf(pn_raw_connection_t *pn_raw_conn, char *buf, size_t buflen);

/**
 * Drains write buffers held by proton raw connection.
 * @param raw_conn - The pn_raw_connection_t to which the write buffers were granted.
 */
int qd_raw_connection_drain_write_buffers(pn_raw_connection_t *pn_raw_conn);

/**
 * Drains read buffers held by proton raw connection.
 * @param raw_conn - The pn_raw_connection_t to which the read buffers were granted.
 */
int qd_raw_connection_drain_read_buffers(pn_raw_connection_t *pn_raw_conn);

/**
 * Drains read and write buffers held by proton raw connection.
 * @param raw_conn - The pn_raw_connection_t to which the read and write buffers were granted
 */
int qd_raw_connection_drain_read_write_buffers(pn_raw_connection_t *pn_raw_conn);

/**
 * Sets the net address string on the vflow record.
 * @param vflow record
 * pn_raw_conn pointer to proton raw connection
 * bool is this connection ingress or egress.
 */
void qd_set_vflow_netaddr_string(vflow_record_t *vflow, pn_raw_connection_t *pn_raw_conn, bool ingress);

/**
 * Sets the VFLOW_ATTRIBUTE_RESULT and VFLOW_ATTRIBUTE_REASON from the
 * proton pn_condition_get_name and pn_condition_get_description respectively.
 * Usually called from the proton PN_RAW_CONNECTION_DISCONNECTED event handler.
 */
void qd_set_condition_on_vflow(pn_raw_connection_t *raw_conn, vflow_record_t *vflow);

#endif // __adaptor_common_h__

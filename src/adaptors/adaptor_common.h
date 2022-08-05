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

#include "entity.h"

#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/alloc_pool.h"

#include <proton/tls.h>

extern size_t QD_ADAPTOR_MAX_BUFFER_SIZE;

typedef enum {
    QD_AGGREGATION_NONE,
    QD_AGGREGATION_JSON,
    QD_AGGREGATION_MULTIPART
} qd_http_aggregation_t;

typedef struct qd_adaptor_config_t     qd_adaptor_config_t;
typedef struct qd_adaptor_buffer_t     qd_adaptor_buffer_t;

struct qd_adaptor_buffer_t {
    unsigned int  size;     ///< Size of data content
    //unsigned char content[4096];
    //unsigned char content[8192];
    //unsigned char content[16384];
    unsigned char content[32768];
    //unsigned char content[65536];
    DEQ_LINKS(qd_adaptor_buffer_t);
};

struct qd_adaptor_config_t
{
    char              *name;
    char              *host;
    char              *port;
    char              *address;
    char              *site_id;
    char              *host_port;

    //TLS related info
    char              *ssl_profile_name;
    bool               require_tls;
    bool               authenticate_peer;
    bool               verify_host_name;
};

qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *buffer);
qd_adaptor_buffer_t *qd_adaptor_buffer();


ALLOC_DECLARE(qd_adaptor_config_t);

qd_error_t qd_load_adaptor_config(qd_dispatch_t *qd, qd_adaptor_config_t *config, qd_entity_t* entity, qd_log_source_t *log_source);
void qd_free_adaptor_config(qd_adaptor_config_t *config);

ALLOC_DECLARE(qd_adaptor_buffer_t);
DEQ_DECLARE(qd_adaptor_buffer_t, qd_adaptor_buffer_list_t);

qd_adaptor_buffer_t *qd_adaptor_buffer_list_append(qd_adaptor_buffer_list_t *buflist, const uint8_t *data, size_t len);


static inline unsigned char *qd_adaptor_buffer_base(const qd_adaptor_buffer_t *buf)
{
    return (unsigned char*) &buf->content[0];
}

/**
 * Return a pointer to the first unused byte in the buffer.
 * @param buf A pointer to an allocated buffer
 * @return A pointer to the first free octet in the buffer, the insert point for new data.
 */
static inline unsigned char *qd_adaptor_buffer_cursor(const qd_adaptor_buffer_t *buf)
{
    return ( (unsigned char*) &(buf->content[0]) ) + buf->size;
}

/**
 * Return remaining capacity at end of buffer.
 * @param buf A pointer to an allocated buffer
 * @return The number of octets in the buffer's free space, how many octets may be inserted.
 */
static inline size_t qd_adaptor_buffer_capacity(const qd_adaptor_buffer_t *buf)
{
    return QD_ADAPTOR_MAX_BUFFER_SIZE - buf->size;
}

/**
 * Return the size of the buffers data content.
 * @param buf A pointer to an allocated buffer
 * @return The number of octets of data in the buffer
 */
static inline size_t qd_adaptor_buffer_size(const qd_adaptor_buffer_t *buf)
{
    return buf->size;
}

/**
 * Notify the buffer that octets have been inserted at the buffer's cursor.  This will advance the
 * cursor by len octets.
 *
 * @param buf A pointer to an allocated buffer
 * @param len The number of octets that have been appended to the buffer
 */
static inline void qd_adaptor_buffer_insert(qd_adaptor_buffer_t *buf, size_t len)
{
    buf->size += len;
    assert(buf->size <= QD_ADAPTOR_MAX_BUFFER_SIZE);
}

#endif // __adaptor_common_h__

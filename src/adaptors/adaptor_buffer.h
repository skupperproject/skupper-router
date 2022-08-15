#ifndef __adaptor_buffer_h__
#define __adaptor_buffer_h__ 1

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

#include <proton/raw_connection.h>

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/buffer.h"

extern const size_t QD_ADAPTOR_MAX_BUFFER_SIZE;

typedef struct qd_adaptor_buffer_t     qd_adaptor_buffer_t;

struct qd_adaptor_buffer_t {
    DEQ_LINKS(qd_adaptor_buffer_t);
    size_t size; ///< Size of data content
};

qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *buffer);
qd_adaptor_buffer_t *qd_adaptor_buffer();

ALLOC_DECLARE(qd_adaptor_buffer_t);
DEQ_DECLARE(qd_adaptor_buffer_t, qd_adaptor_buffer_list_t);

// on return buflist will be empty:
void qd_adaptor_buffer_list_free_buffers(qd_adaptor_buffer_list_t *buflist);

/**
 * Copies adaptor buffers in a buffer list into the passed in list of qd_buffers
 *
 * @param qd_adaptor_buffs - adaptor buffer list that needs to be copied into qd_buffers
 * @param qd_bufs - The qd_buffer list to which the adaptor buffers will be copied into
 */
void qd_adaptor_buffers_copy_to_qd_buffers(const qd_adaptor_buffer_list_t *qd_adaptor_buffs, qd_buffer_list_t *qd_bufs);

/**
 * Copies qd_buffers in a buffer list into the passed in list of adaptor buffers
 *
 * @param qd_buffs - qd_buffer list that needs to be copied into adaptor buffers
 * @param qd_adaptor_buffs - The adaptor buffer list to which the qd_buffers will be copied into
 */
void qd_adaptor_copy_qd_buffers_to_adaptor_buffers(const qd_buffer_list_t *qd_bufs, qd_adaptor_buffer_list_t *qd_adaptor_buffs);

static inline void qd_adaptor_buffer_free(qd_adaptor_buffer_t *buf)
{
    if (!buf)
        return;
    free_qd_adaptor_buffer_t(buf);
}

void qd_adaptor_buffer_list_append(qd_adaptor_buffer_list_t *buflist, const uint8_t *data, size_t len);


static inline unsigned char *qd_adaptor_buffer_base(const qd_adaptor_buffer_t *buf)
{
    return (unsigned char *) &buf[1];
}

/**
 * Return a pointer to the first unused byte in the buffer.
 * @param buf A pointer to an allocated buffer
 * @return A pointer to the first free octet in the buffer, the insert point for new data.
 */
static inline unsigned char *qd_adaptor_buffer_cursor(const qd_adaptor_buffer_t *buf)
{
    return ((unsigned char *) &(buf[1])) + buf->size;
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

 #endif // __adaptor_buffer_h__

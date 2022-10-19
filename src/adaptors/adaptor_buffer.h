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

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/buffer.h"

#include <proton/raw_connection.h>

extern const size_t QD_ADAPTOR_MAX_BUFFER_SIZE;

typedef struct qd_adaptor_buffer_t qd_adaptor_buffer_t;

struct qd_adaptor_buffer_t {
    DEQ_LINKS(qd_adaptor_buffer_t);
    size_t size;  ///< Size of data content
};

qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *buffer);
qd_adaptor_buffer_t *qd_adaptor_buffer();

ALLOC_DECLARE(qd_adaptor_buffer_t);
DEQ_DECLARE(qd_adaptor_buffer_t, qd_adaptor_buffer_list_t);

// on return buflist will be empty:
void qd_adaptor_buffer_list_free_buffers(qd_adaptor_buffer_list_t *buflist);

/**
 * Copies adaptor buffers in a buffer list into the passed in list of qd_buffers. Frees all adaptor
 * buffers in the passed in qd_adaptor_buffs list.
 * Frees the buffers in the passed in adaptor buffer list.
 *
 * @param qd_adaptor_buffs - adaptor buffer list that needs to be copied into qd_buffers
 * @param qd_bufs - The qd_buffer list to which the adaptor buffers will be copied into
 *
 * @return the total number of bytes copied from qd_adaptor_buffs to qd_bufs
 */
size_t qd_adaptor_buffers_copy_to_qd_buffers(qd_adaptor_buffer_list_t *qd_adaptor_buffs, qd_buffer_list_t *qd_bufs);

/**
 * Copies qd_buffers in the passed qd_buffer list into the passed in list of adaptor buffers.
 * Frees the buffers in the passed in qd_buffer list.
 *
 * @param qd_buffs - qd_buffer list that needs to be copied into adaptor buffers
 * @param qd_adaptor_buffs - The adaptor buffer list to which the qd_buffers will be copied into
 *
 * @return the total number of bytes copied from qd_bufs to qd_adaptor_buffs
 */
size_t qd_adaptor_copy_qd_buffers_to_adaptor_buffers(qd_buffer_list_t         *qd_bufs,
                                                     qd_adaptor_buffer_list_t *qd_adaptor_buffs);

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

/**
 * Sets the passed in adaptor_buffer details onto the passed in pn_raw_buffer.
 * The context of the pn_raw_buffer is set to the passed in adaptor_buffer.
 *
 * @param pn_raw_buffer - pn_raw_buffer_t struct whose details are set
 * @param adaptor_buf - qd_adaptor_buffer_t whose details need to be set to the pn_raw_buffer
 */
void qd_adaptor_buffer_pn_raw_buffer(pn_raw_buffer_t *pn_raw_buffer, const qd_adaptor_buffer_t *adaptor_buf);

/**
 * Creates a new adaptor buffer and sets its values into the passed in pn_raw_buffer_t.
 * @param pn_raw_buffer - pn_raw_buffer_t struct whose details are set with the newly created adaptor buffer.
 * @see qd_adaptor_buffer_pn_raw_buffer
 */
qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *pn_raw_buffer);

/**
 * Retrieves the context of the passed in pn_raw_buffer and sets the size of the adaptor buffer to the pn_raw_buffer's
 * size.
 * @param pn_raw_buffer - pn_raw_buffer_t struct whose context is obtained as a qd_adaptor_buffer
 */
qd_adaptor_buffer_t *qd_get_adaptor_buffer_from_pn_raw_buffer(const pn_raw_buffer_t *pn_raw_buffer);

#endif  // __adaptor_buffer_h__

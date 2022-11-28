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

#include "adaptor_buffer.h"

#include "qpid/dispatch/ctools.h"

// Buffer size will be set to 32k across all adaptors.
const size_t QD_ADAPTOR_MAX_BUFFER_SIZE = 16384;
// const size_t QD_ADAPTOR_MAX_BUFFER_SIZE = 32768;

ALLOC_DEFINE_CONFIG(qd_adaptor_buffer_t, sizeof(qd_adaptor_buffer_t), &QD_ADAPTOR_MAX_BUFFER_SIZE, 0);

qd_adaptor_buffer_t *qd_adaptor_buffer(void)
{
    qd_adaptor_buffer_t *adaptor_buff = new_qd_adaptor_buffer_t();
    DEQ_ITEM_INIT(adaptor_buff);
    adaptor_buff->size = 0;
    return adaptor_buff;
}

void qd_adaptor_buffer_list_free_buffers(qd_adaptor_buffer_list_t *list)
{
    assert(list);
    qd_adaptor_buffer_t *buf = DEQ_HEAD(*list);
    while (buf) {
        qd_adaptor_buffer_t *next = DEQ_NEXT(buf);
        qd_adaptor_buffer_free(buf);
        buf = next;
    }
    DEQ_INIT(*list);
}

void qd_adaptor_buffer_pn_raw_buffer(pn_raw_buffer_t *pn_raw_buffer, const qd_adaptor_buffer_t *adaptor_buff)
{
    assert(pn_raw_buffer);
    assert(adaptor_buff);
    pn_raw_buffer->size     = qd_adaptor_buffer_size(adaptor_buff);
    pn_raw_buffer->capacity = qd_adaptor_buffer_capacity(adaptor_buff);
    pn_raw_buffer->offset   = 0;
    pn_raw_buffer->bytes    = (char *) qd_adaptor_buffer_base(adaptor_buff);
    pn_raw_buffer->context  = (uintptr_t) adaptor_buff;
}

qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *pn_raw_buffer)
{
    assert(pn_raw_buffer);
    qd_adaptor_buffer_t *adaptor_buff = qd_adaptor_buffer();
    qd_adaptor_buffer_pn_raw_buffer(pn_raw_buffer, adaptor_buff);
    return adaptor_buff;
}

qd_adaptor_buffer_t *qd_get_adaptor_buffer_from_pn_raw_buffer(const pn_raw_buffer_t *pn_raw_buffer)
{
    assert(pn_raw_buffer);
    qd_adaptor_buffer_t *adaptor_buffer = (qd_adaptor_buffer_t *) pn_raw_buffer->context;
    // set the buffer->size: don't call qd_adaptor_buffer_insert() which increments the size!
    adaptor_buffer->size = pn_raw_buffer->size;
    assert(adaptor_buffer->size <= QD_ADAPTOR_MAX_BUFFER_SIZE);
    return adaptor_buffer;
}

void qd_adaptor_buffer_list_append(qd_adaptor_buffer_list_t *buflist, const uint8_t *data, size_t len)
{
    //
    // If len is zero, there's no work to do.
    //
    if (len == 0)
        return;

    //
    // If the buffer list is empty and there's some data, add one empty buffer before we begin.
    //
    if (DEQ_SIZE(*buflist) == 0) {
        qd_adaptor_buffer_t *buf = qd_adaptor_buffer();
        DEQ_INSERT_TAIL(*buflist, buf);
    }

    qd_adaptor_buffer_t *tail = DEQ_TAIL(*buflist);

    while (len > 0) {
        size_t to_copy = MIN(len, qd_adaptor_buffer_capacity(tail));
        if (to_copy > 0) {
            memcpy(qd_adaptor_buffer_cursor(tail), data, to_copy);
            qd_adaptor_buffer_insert(tail, to_copy);
            data += to_copy;
            len -= to_copy;
        }
        if (len > 0) {
            tail = qd_adaptor_buffer();
            DEQ_INSERT_TAIL(*buflist, tail);
        }
    }
}

size_t qd_adaptor_buffers_copy_to_qd_buffers(qd_adaptor_buffer_list_t *adaptor_buffs, qd_buffer_list_t *qd_buffs)
{
    assert(adaptor_buffs);
    size_t               bytes_copied = 0;
    qd_adaptor_buffer_t *a_buf = DEQ_HEAD(*adaptor_buffs);
    while (a_buf) {
        size_t adaptor_buffer_size = qd_adaptor_buffer_size(a_buf);
        bytes_copied += adaptor_buffer_size;
        qd_buffer_list_append(qd_buffs, (uint8_t *) qd_adaptor_buffer_base(a_buf), adaptor_buffer_size);
        DEQ_REMOVE_HEAD(*adaptor_buffs);
        qd_adaptor_buffer_free(a_buf);
        a_buf = DEQ_HEAD(*adaptor_buffs);
    }
    return bytes_copied;
}

size_t qd_adaptor_copy_qd_buffers_to_adaptor_buffers(qd_buffer_list_t         *qd_buffs,
                                                     qd_adaptor_buffer_list_t *adaptor_buffs)
{
    assert(qd_buffs);
    size_t       bytes_copied = 0;
    qd_buffer_t *q_buf        = DEQ_HEAD(*qd_buffs);
    while (q_buf) {
        size_t qd_buff_size = qd_buffer_size(q_buf);
        bytes_copied += qd_buff_size;
        qd_adaptor_buffer_list_append(adaptor_buffs, (uint8_t *) qd_buffer_base(q_buf), qd_buff_size);
        DEQ_REMOVE_HEAD(*qd_buffs);
        qd_buffer_free(q_buf);
        q_buf = DEQ_HEAD(*qd_buffs);
    }
    return bytes_copied;
}

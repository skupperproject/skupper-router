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
const size_t QD_ADAPTOR_MAX_BUFFER_SIZE = 32768;

ALLOC_DEFINE_CONFIG(qd_adaptor_buffer_t, sizeof(qd_adaptor_buffer_t), &QD_ADAPTOR_MAX_BUFFER_SIZE, 0);

qd_adaptor_buffer_t *qd_adaptor_buffer()
{
    qd_adaptor_buffer_t *adaptor_buff = new_qd_adaptor_buffer_t();
    DEQ_ITEM_INIT(adaptor_buff);
    adaptor_buff->size = 0;
    return adaptor_buff;
}

void qd_adaptor_buffer_list_free_buffers(qd_adaptor_buffer_list_t *list)
{
    qd_adaptor_buffer_t *buf = DEQ_HEAD(*list);
    while (buf) {
        qd_adaptor_buffer_t *next = DEQ_NEXT(buf);
        qd_adaptor_buffer_free(buf);
        buf = next;
    }
    DEQ_INIT(*list);
}

qd_adaptor_buffer_t *qd_adaptor_buffer_raw(pn_raw_buffer_t *buffer)
{
    qd_adaptor_buffer_t *adaptor_buff = qd_adaptor_buffer();
    buffer->bytes                     = (char *) qd_adaptor_buffer_base(adaptor_buff);
    buffer->capacity                  = qd_adaptor_buffer_capacity(adaptor_buff);
    buffer->size                      = 0;
    buffer->offset                    = 0;
    buffer->context                   = (uintptr_t) adaptor_buff;
    return adaptor_buff;
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

void qd_adaptor_buffers_copy_to_qd_buffers(const qd_adaptor_buffer_list_t *adaptor_buffs, qd_buffer_list_t *qd_bufs)
{
    DEQ_INIT(*qd_bufs);

    qd_adaptor_buffer_t *a_buf = DEQ_HEAD(*adaptor_buffs);
    while (a_buf) {
        qd_buffer_list_append(qd_bufs, (uint8_t *) qd_adaptor_buffer_base(a_buf), qd_adaptor_buffer_size(a_buf));
        a_buf = DEQ_NEXT(a_buf);
    }
}

void qd_adaptor_copy_qd_buffers_to_adaptor_buffers(const qd_buffer_list_t   *qd_bufs,
                                                   qd_adaptor_buffer_list_t *adaptor_buffs)
{
    DEQ_INIT(*adaptor_buffs);
    qd_buffer_t *q_buf = DEQ_HEAD(*qd_bufs);
    while (q_buf) {
        qd_adaptor_buffer_list_append(adaptor_buffs, (uint8_t *) qd_buffer_base(q_buf), qd_buffer_size(q_buf));
        q_buf = DEQ_NEXT(q_buf);
    }
}

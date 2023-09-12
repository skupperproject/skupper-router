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

#include "qpid/dispatch/connection_counters.h"
#include "qpid/dispatch/atomic.h"

atomic_uint_fast64_t qd_connection_counters[QD_PROTOCOL_TOTAL];

void qd_connection_counter_inc(qd_protocol_t proto)
{
    assert(proto < QD_PROTOCOL_TOTAL);
    atomic_fetch_add_explicit(&qd_connection_counters[proto], 1, memory_order_relaxed);
}

void qd_connection_counter_dec(qd_protocol_t proto)
{
    assert(proto < QD_PROTOCOL_TOTAL);
    uint64_t old = atomic_fetch_sub_explicit(&qd_connection_counters[proto], 1, memory_order_relaxed);
    (void) old;
    assert(old != 0);  // underflow!
}

uint64_t qd_connection_count(qd_protocol_t proto)
{
    assert(proto < QD_PROTOCOL_TOTAL);
    return (uint64_t) atomic_load_explicit(&qd_connection_counters[proto], memory_order_relaxed);
}



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
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/router_core.h"

#include <inttypes.h>
#include <stdio.h>

#define ADDRESS_COUNT 10

static const char *address_fmt = "addr_watch/test_address/%d";

static qdr_watch_handle_t handle[ADDRESS_COUNT];

static qdr_core_t      *core_ptr   = 0;
static qd_log_source_t *log_source = 0;

static void on_watch(void     *context,
                     uint32_t  local_consumers,
                     uint32_t  in_proc_consumers,
                     uint32_t  remote_consumers,
                     uint32_t  local_producers)
{
    qd_log(log_source, QD_LOG_INFO, "on_watch(%ld): loc: %"PRIu32" rem: %"PRIu32" prod: %"PRIu32"",
           (long) context, local_consumers, remote_consumers, local_producers);
}


static void qdr_test_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    core_ptr = core;
    if (qdr_core_test_hooks_enabled(core)) {
        log_source = qd_log_source("ADDRESS_WATCH");
        char address[100];
        for (long index = 0; index < ADDRESS_COUNT; index++) {
            sprintf(address, address_fmt, index);
            handle[index] = qdr_core_watch_address(core, address, QD_ITER_HASH_PREFIX_MOBILE, QD_TREATMENT_ANYCAST_BALANCED, on_watch, (void*) index);
        }
    }
}


static void qdr_test_adaptor_final(void *adaptor_context)
{
    if (qdr_core_test_hooks_enabled(core_ptr)) {
        for (long index = 0; index < ADDRESS_COUNT; index++) {
            qdr_core_unwatch_address(core_ptr, handle[index]);
        }
    }
}


QDR_CORE_ADAPTOR_DECLARE("test-adaptor", qdr_test_adaptor_init, qdr_test_adaptor_final)

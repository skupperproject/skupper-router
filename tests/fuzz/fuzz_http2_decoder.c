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

#include <qpid/dispatch/alloc_pool.h>

#include "decoders/http2/http2_decoder.h"
#include "decoders/http2/http2_test.h"
#include "qpid/dispatch/ctools.h"

#include "libFuzzingEngine.h"

void qd_log_initialize(void);
void qd_error_initialize(void);
void qd_router_id_finalize(void);
void qd_log_finalize(void);

bool alloc_pool_initialized = false;

void set_alloc_pool_initialized(bool initialized)
{
    alloc_pool_initialized = initialized;
}

bool is_alloc_pool_initialized(void)
{
    return alloc_pool_initialized;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (!is_alloc_pool_initialized()) {
        qd_alloc_initialize();
        qd_log_initialize();
        qd_error_initialize();
        set_alloc_pool_initialized(true);
    }

    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(0, 0/*user_context*/, 1/*conn_id*/);
    decode(conn_state, true, data, size);
    qd_http2_decoder_connection_free(conn_state);
    //qd_http2_decoder_connection_final();
    return 0;
}

__attribute__((destructor)) void fuzz_http2_destructor(void)
{
  qd_log_finalize();
  qd_alloc_finalize();
  qd_router_id_finalize();
}


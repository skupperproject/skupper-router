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

#include "decoders/http1/http1_decoder.h"
#include "qpid/dispatch/ctools.h"

#include "libFuzzingEngine.h"

void qd_log_initialize(void);
void qd_error_initialize(void);
void qd_router_id_finalize(void);
void qd_log_finalize(void);

/**
 * This function is processed on exit
 */
void call_on_exit(void)
{
    qd_log_finalize();
    qd_alloc_finalize();
    qd_router_id_finalize();
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    atexit(call_on_exit);

    qd_alloc_initialize();
    qd_log_initialize();
    qd_error_initialize();
    return 0;
}

//
// Dummy callbacks for the decoder.
//

static int _rx_request(qd_http1_decoder_connection_t *hconn,
                       const char *method,
                       const char *target,
                       uint32_t   version_major,
                       uint32_t   version_minor,
                       uintptr_t  *request_context)
{
    *request_context = 1;
    return 0;
}

static int _rx_response(qd_http1_decoder_connection_t *hconn, uintptr_t request_context,
                        int status_code,
                        const char *reason_phrase,
                        uint32_t version_major,
                        uint32_t version_minor)
{ return 0; }

static int _rx_header(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client,
                      const char *key, const char *value)
{ return 0; }

static int _rx_headers_done(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client)
{ return 0; }

static int _rx_body(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client, const unsigned char *body, size_t length)
{ return 0; }

static int _message_done(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client)
{ return 0; }

static int _transaction_complete(qd_http1_decoder_connection_t *hconn, uintptr_t request_context)
{ return 0; }

static void _protocol_error(qd_http1_decoder_connection_t *hconn, const char *reason)
{ }


const struct qd_http1_decoder_config_t test_config = {
    .rx_request           = _rx_request,
    .rx_response          = _rx_response,
    .rx_header            = _rx_header,
    .rx_headers_done      = _rx_headers_done,
    .rx_body              = _rx_body,
    .message_done         = _message_done,
    .transaction_complete = _transaction_complete,
    .protocol_error       = _protocol_error
};


int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    qd_http1_decoder_connection_t *conn_state = qd_http1_decoder_connection(&test_config, 1);
    qd_http1_decoder_connection_rx_data(conn_state, true, (const unsigned char *) data, size);
    qd_http1_decoder_connection_free(conn_state);
    return 0;
}


#ifndef __observers_http2_test_h__
#define __observers_http2_test_h__ 1

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

#include "http2_decoder.h"

typedef struct qd_decoder_buffer_t   qd_decoder_buffer_t;
typedef struct qd_http2_decoder_t          qd_http2_decoder_t;

bool is_client_decoder_state_decode_connection_preface(qd_http2_decoder_connection_t *conn_state);
bool is_client_decoder_state_decode_frame_header(qd_http2_decoder_connection_t *conn_state);
bool is_client_decoder_state_skip_frame_payload(qd_http2_decoder_connection_t *conn_state);
void move_to_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer, const uint8_t *data, size_t data_length);
uint32_t get_stream_identifier(const uint8_t *data);
void reset_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer);

#endif

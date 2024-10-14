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
#include <string.h>
#include <inttypes.h>
#include "test_case.h"
#include "decoders/http2/http2_decoder.h"
#include "decoders/http2/http2_test.h"
#include "qpid/dispatch/ctools.h"

const char *HTTP_TEST_PATH = ":path";
const char *HTTP_TEST_METHOD = ":method";
const char *HTTP_TEST_STATUS = ":status";

/**
 * First send 20 bytes of the client magic and then later send another 4 bytes of the client magic
 * and check decoder state.
 */
bool method_match = false;
bool path_match   = false;
bool streams_2_seen = false;
bool streams_3_seen = false;

char* test_http2_decode_client_magic_1(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(0, 0, 1);

    bool check_state = is_client_decoder_state_decode_connection_preface(conn_state);
    if (!check_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_CONNECTION_PREFACE but it is not";
    }

    const uint8_t data1[20] = {
        0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
        0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d
    };
    check_state = is_client_decoder_state_decode_connection_preface(conn_state);
    if (!check_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_CONNECTION_PREFACE but it is not";
    }

    decode(conn_state, true, data1, 20);
    const uint8_t data2[20] = {
            0x0d, 0x0a, 0x0d, 0x0a
    };
    decode(conn_state, true, data2, 4);
    bool decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    qd_http2_decoder_connection_free(conn_state);
    return 0;
}

/**
 * Send all 24 bytes of the client magic in one shot and make sure the decoder state is HTTP2_DECODE_FRAME_HEADER.
 */
char* test_http2_decode_client_magic_2(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(0, 0, 1);
    const uint8_t data1[24] = {
        0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
        0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a
    };

    decode(conn_state, true, data1, 24);
    bool decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    qd_http2_decoder_connection_free(conn_state);
    return 0;
}

/**
 * Send all 26 bytes of the client magic in one shot and make sure the decoder state is HTTP2_DECODE_FRAME_HEADER.
 */
char* test_http2_decode_client_magic_3(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(0, 0, 1);
    const uint8_t data1[26] = {
        0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
        0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x00
    };

    decode(conn_state, true, data1, 26);
    bool decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    qd_http2_decoder_connection_free(conn_state);
    return 0;
}

int on_test_begin_header_callback(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client,
                             uint32_t stream_id)
{

    return 0;
}

int on_test_header_recv_callback(qd_http2_decoder_connection_t *conn_state,
                            uintptr_t request_context,
                            bool from_client,
                            uint32_t stream_id,
                            const uint8_t *name,
                            size_t namelen,
                            const uint8_t *value,
                            size_t valuelen)
{
    if (strcmp(HTTP_TEST_METHOD, (const char *)name) == 0 && strcmp("GET", (const char *)value) == 0) {
        method_match = true;
    }
    if (strcmp(HTTP_TEST_PATH, (const char *)name) == 0 && strcmp("/index.html", (const char *)value) == 0) {
        path_match = true;
    }
    return 0;
}

qd_http2_decoder_callbacks_t callbacks = {
    .on_header = on_test_header_recv_callback,
    .on_begin_header = on_test_begin_header_callback
    //.protocol_error = protocol_error
};

/**
 * Sends a client magic, SETTINGS and WINDOW_UPDATE frame finally followed by a HEADER frame
 * The header frame makes callbacks for headers and two headers are checked.
 * This test makes sure that the decoder is jumping thru many frames and finally looks into the
 * HTTP headers in the HEADER frame.
 */
char* test_http2_decode_request_header(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(&callbacks, 0, 1);

    const uint8_t data1[20] = {
        0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
        0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d
    };

    decode(conn_state, true, data1, 20);

    // SETTINGS frame starts with byte 5 with length of 18
    const uint8_t data2[6] = {
        0x0d, 0x0a, 0x0d, 0x0a,     /*Start SETTINGS frame*/0x00, 0x00
    };

    decode(conn_state, true, data2, 6);

    bool decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    const uint8_t data3[9] = {
        0x12, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,  /*Start SETTINGS frame payload*/ 0x00, 0x03
    };

    decode(conn_state, true, data3, 9);

    bool skip_frame_skip_payload_state = is_client_decoder_state_skip_frame_payload(conn_state);
    if (!skip_frame_skip_payload_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_SKIP_FRAME_PAYLOAD but it is not";
    }

    const uint8_t data4[11] = {
        0x00, 0x00, 0x00, 0x64, 0x00, 0x04, 0x00, 0xa0, 0x00, 0x00, 0x00
    };

    decode(conn_state, true, data4, 11);

    // WINDOW_UPDATE frame starts with 6th byte, 0x00
    const uint8_t data5[7] = {
        0x02, 0x00, 0x00, 0x00, 0x00,  /* WINDOW_UPDATE frame starts */   0x00, 0x00
    };

    decode(conn_state, true, data5, 7);


    const uint8_t data6[2] = {
        0x04, 0x08
    };

    decode(conn_state, true, data6, 2);
//
    const uint8_t data7[12] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x7f, 0x00, 0x01, /* HEADER frame starts */ 0x00, 0x00, 0x1e
    };

    decode(conn_state, true, data7, 12);
//
    const uint8_t data8[2] = {
        0x01, 0x05
    };
    decode(conn_state, true, data8, 2);

    const uint8_t data9[34] = {
            0x00, 0x00, 0x00, 0x01, /* HEADER frame payload starts */ 0x82, 0x86, 0x41, 0x8a, 0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x7c, 0x00, 0x07, 0x85, 0x7a, 0x88, 0x25, 0xb6, 0x50, 0xc3,
            0xcb, 0x89, 0x70, 0xff, 0x53, 0x03, 0x2a, 0x2f, 0x2a
    };

    decode(conn_state, true, data9, 34);

    decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    if(!method_match) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected :method to be GET but it is not";
    }

    if(!path_match) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected :path to be /index.html but it is not";
    }

    qd_http2_decoder_connection_free(conn_state);

    return 0;
}


int on_test_begin_header_callback5(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client,
                             uint32_t stream_id)
{
    return 0;
}

bool method_match5 =false;
bool path_match5 = false;
int on_test_header_recv_callback5(qd_http2_decoder_connection_t *conn_state,
                            uintptr_t request_context,
                            bool from_client,
                            uint32_t stream_id,
                            const uint8_t *name,
                            size_t namelen,
                            const uint8_t *value,
                            size_t valuelen)
{
    if (strcmp(HTTP_TEST_METHOD, (const char *)name) == 0 && strcmp("GET", (const char *)value) == 0) {
        method_match5 = true;
    }
    if (strcmp(HTTP_TEST_PATH, (const char *)name) == 0 && strcmp("/index.html", (const char *)value) == 0) {
        path_match5 = true;
    }
    return 0;
}

qd_http2_decoder_callbacks_t callbacks5 = {
    .on_header = on_test_header_recv_callback5,
    .on_begin_header = on_test_begin_header_callback5
    //.protocol_error = protocol_error
};

/**
 * Divide the frame header into many small parts and make sure the header callbacks are
 * still being called.
 */
char* test_http2_decode_request_header_fragmented(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(&callbacks5, 0, 1);

    const uint8_t data1[2] = {
        /* HEADER frame starts */ 0x00, 0x00,
    };
    decode(conn_state, false, data1, 2);

    const uint8_t data2[1] = {
        0x1e
    };
    decode(conn_state, false, data2, 1);

    const uint8_t data3[1] = {
        0x01
    };
    decode(conn_state, false, data3, 1);

    const uint8_t data4[1] = {
        0x05
    };
    decode(conn_state, false, data4, 1);


    const uint8_t data5[34] = {
            0x00, 0x00, 0x00, 0x01, /* HEADER frame payload starts */ 0x82, 0x86, 0x41, 0x8a, 0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x7c, 0x00, 0x07, 0x85, 0x7a, 0x88, 0x25, 0xb6, 0x50, 0xc3,
            0xcb, 0x89, 0x70, 0xff, 0x53, 0x03, 0x2a, 0x2f, 0x2a
    };
    decode(conn_state, false, data5, 34);


    if(!method_match5) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected :method to be GET but it is not";
    }

    if(!path_match5) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected :path to be /index.html but it is not";
    }

    qd_http2_decoder_connection_free(conn_state);
    return 0;
}

int on_test1_begin_header_callback(qd_http2_decoder_connection_t *conn_state,
                                   uintptr_t request_context,
                                   bool from_client,
                                   uint32_t stream_id)
{
    if (stream_id == 2) {
        streams_2_seen = true;
    } else if (stream_id == 3) {
        streams_3_seen = true;
    }
    return 0;
}

int on_test1_header_recv_callback(qd_http2_decoder_connection_t *conn_state,
                                  uintptr_t request_context,
                                  bool from_client,
                                  uint32_t stream_id,
                                  const uint8_t *name,
                                  size_t namelen,
                                  const uint8_t *value,
                                  size_t valuelen)
{

    return 0;
}

bool has_protocol_error = false;
static void protocol_error(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client, const char *reason)
{
    has_protocol_error = true;
}

qd_http2_decoder_callbacks_t callbacks1 = {
    .on_header = on_test1_header_recv_callback,
    .on_begin_header = on_test1_begin_header_callback,
    .on_decode_error = protocol_error
};

/**
 * Send two HEADER frames back to back, first HEADER stream has stream_id=2 and the second one has stream_id=3
 * Make sure the on_begin_header header is called twice with both stream ids.
 */
char* test_http2_decode_request_double_header(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(&callbacks1, 0, 1);

    const uint8_t data1[27] = {
        0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
        0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a, /* HEADER frame starts */ 0x00, 0x00, 0x21
    };
    decode(conn_state, true, data1, 27);

    bool decode_frame_header_state = is_client_decoder_state_decode_frame_header(conn_state);
    if (!decode_frame_header_state) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected client decoder state to be HTTP2_DECODE_FRAME_HEADER but it is not";
    }

    const uint8_t data2[77] = {
            0x01, 0x0d, 0x00, 0x00, 0x00, 0x02, /* HEADER frame payload starts */ 0x02 /*pad length is 2*/, 0x82, 0x86, 0x41, 0x8a, 0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x7c, 0x00, 0x07, 0x85, 0x7a, 0x88, 0x25, 0xb6, 0x50, 0xc3,
            0xcb, 0x89, 0x70, 0xff, 0x53, 0x03, 0x2a, 0x2f, 0x2a, /* the following two bytes are pad bytes*/0x00, 0x00,/* another HEADER frame starts */ 0x00, 0x00, 0x1e, 0x01, 0x05, 0x00, 0x00, 0x00, 0x03, /* HEADER frame payload starts */ 0x82, 0x86, 0x41,
            0x8a, 0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x7c, 0x00, 0x07, 0x85, 0x7a, 0x88, 0x25, 0xb6, 0x50, 0xc3, 0xcb, 0x89, 0x70, 0xff, 0x53, 0x03, 0x2a, 0x2f
    };
    int ret_val = decode(conn_state, true, data2, 77);

    if(ret_val != 0) {
        qd_http2_decoder_connection_free(conn_state);
        return "Call to decode() failed with ret_val -1";
    }


    const uint8_t data3[1] = {
            0x2a
    };
    decode(conn_state, true, data3, 1);

    bool streams_seen = streams_2_seen && streams_3_seen;
    if(!streams_seen) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected to see stream_id 2 and stream_id 3 but did not";
    }
    if(has_protocol_error) {
        qd_http2_decoder_connection_free(conn_state);
        return "There was a protocol error, test_http2_decode_request_double_header has failed.";
    }

    qd_http2_decoder_connection_free(conn_state);
    return 0;
}

char *test_http2_decode_response_header(void *context)
{
    return 0;
}

char *test_http2_allocate_move_to_scratch_buffer(void *context)
{
    qd_decoder_buffer_t scratch_buffer;
    ZERO(&scratch_buffer);

    const uint8_t data[2] = {
        0x04, 0x08
    };

    move_to_scratch_buffer(&scratch_buffer, data, 2);

    if(scratch_buffer.size == 0) {
        reset_scratch_buffer(&scratch_buffer);
        return "Expected scratch buffer size to be non-zero";
    }

    if(scratch_buffer.capacity == 0) {
        reset_scratch_buffer(&scratch_buffer);
        return "Expected scratch buffer capacity to be non-zero";
    }

    if(scratch_buffer.size != scratch_buffer.capacity) {
        reset_scratch_buffer(&scratch_buffer);
        return "Expected scratch buffer capacity and size to be the same but it is not";
    }

    reset_scratch_buffer(&scratch_buffer);
    return 0;
}

char *test_http2_get_stream_identifier(void *context)
{
    const uint8_t data[4] = {
        0x00, 0x01, 0x21, 0x1e
    };
    uint32_t stream_id1 = 74014;
    uint32_t stream_id = get_stream_identifier(data);
    if (stream_id != stream_id1) {
        static char error[100];
        sprintf(error, "Expected 74014 as stream_id but got %" PRIu32 "instead",stream_id);
    }

    // Note here that 0x81 has the signed bit set to 1
    // This will test the BIT_CLEAR call in get_stream_identifier
    const uint8_t data1[4] = {
         0x81, 0x01, 0x21, 0x1e
    };

    stream_id = get_stream_identifier(data1);
    if (stream_id != 16851230) {
        static char error[100];
        sprintf(error, "Expected 16851230 as stream_id but got %" PRIu32" instead",stream_id);
    }
    return 0;
}


bool decoder_error = false;
static void protocol_error1(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client, const char *reason)
{
    if (!strcmp(reason, "get_request_headers - failure, moving decoder state to HTTP2_DECODE_ERROR nghttp2 error code=-523"))
        decoder_error = true;
}

int on_test3_begin_header_callback(qd_http2_decoder_connection_t *conn_state,
                                   uintptr_t request_context,
                                   bool from_client,
                                   uint32_t stream_id)
{
    if (stream_id == 2) {
        streams_2_seen = true;
    } else if (stream_id == 3) {
        streams_3_seen = true;
    }
    return 0;
}

int on_test3_header_recv_callback(qd_http2_decoder_connection_t *conn_state,
                                  uintptr_t request_context,
                                  bool from_client,
                                  uint32_t stream_id,
                                  const uint8_t *name,
                                  size_t namelen,
                                  const uint8_t *value,
                                  size_t valuelen)
{

    return 0;
}

qd_http2_decoder_callbacks_t callbacks2 = {
        .on_header = on_test3_header_recv_callback,
        .on_begin_header = on_test3_begin_header_callback,
        .on_decode_error = protocol_error1
};

/**
 * Send a http2 HEADER frame with bad header data. The nghttp2 HPACK deflator will fail and
 * we should see the decoder error callback being called.
 */
char *test_http2_decode_compressed_header_error(void *context)
{
    qd_http2_decoder_connection_t *conn_state = qd_http2_decoder_connection(&callbacks2, 0, 1);

    const uint8_t data1[12] = {
        0x00, 0x00, 0x03, 0x01, 0x04, 0x00, 0x00, 0x00, 0x07, 0x88, 0xc0, 0xbf
    };
    decode(conn_state, false, data1, 12);
    if (!decoder_error) {
        qd_http2_decoder_connection_free(conn_state);
        return "Expected to see a decoding error but did not";
    }
    qd_http2_decoder_connection_free(conn_state);
    return 0;
}


int http2_decoder_tests(void)
{
    int result = 0;
    char *test_group = "http2_decoder_tests";
    TEST_CASE(test_http2_decode_client_magic_1, 0);
    TEST_CASE(test_http2_decode_client_magic_2, 0);
    TEST_CASE(test_http2_decode_client_magic_3, 0);
    TEST_CASE(test_http2_decode_request_header, 0);
    TEST_CASE(test_http2_decode_request_header, 0);
    TEST_CASE(test_http2_allocate_move_to_scratch_buffer, 0);
    TEST_CASE(test_http2_get_stream_identifier, 0);
    TEST_CASE(test_http2_decode_request_double_header, 0);
    TEST_CASE(test_http2_decode_response_header, 0);
    TEST_CASE(test_http2_decode_compressed_header_error, 0);
    TEST_CASE(test_http2_decode_request_header_fragmented, 0);

    return result;
}

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
#include <inttypes.h>
#include "qpid/dispatch/ctools.h"
#include <qpid/dispatch/log.h>
#include <proton/raw_connection.h>
#include <nghttp2/nghttp2.h>
#include "buffer_field_api.h"
#include "http2_decoder.h"

typedef struct qd_http2_decoder_t          qd_http2_decoder_t;
#define HTTP2_SCRATCH_BUFFER_MAX_SIZE      65535  // arbitrary max limit restricting the scratch buffer to avoid DOS attacks

#define HTTP2_CLIENT_PREFIX_LEN 24
const uint8_t http2_client_prefix[HTTP2_CLIENT_PREFIX_LEN] = {
    0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32,
    0x2e, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a
};

#define HTTP2_FRAME_LENGTH_AND_TYPE_BYTES 4  // HTTP2 frame length is 3 bytes and frame type is 1 byte
#define HTTP2_FRAME_LENGTH_BYTES 3           // HTTP2 frame length is 3 bytes
#define HTTP2_FRAME_HEADER_LENGTH 9           // This is the frame header length. Every frame in http2 has a 9 byte header
#define HTTP2_FRAME_STREAM_ID_LENGTH 4       // Stream id is 31 bytes

typedef enum qd_http2_frame_type {
    FRAME_TYPE_HEADER   = 0x01,  // We only care about the HEADER frames
    FRAME_TYPE_OTHER,            // Every other frame than the HEADER frame is of type OTHER, we will not look into these frame contents.
    FRAME_TYPE_NONE              // We have not started parsing the http2 data stream
} qd_http2_frame_type;

typedef enum {
    HTTP2_DECODE_CONNECTION_PREFACE,        // Parse the connection preface
    HTTP2_DECODE_FRAME_HEADER,              // Parsing any http2 frame header
    HTTP2_DECODE_SKIP_FRAME_PAYLOAD,        // Jumping over the frame payload
    HTTP2_DECODE_REQUEST_RESPONSE_HEADER,   // Parsing request or response header
    HTTP2_DECODE_ERROR                      // Decoding error has occurred.
} qd_http2_decoder_state_t;


struct qd_http2_decoder_t {
    qd_http2_decoder_connection_t *conn_state;             // Reference to the decoder's connection state information.
    qd_http2_decoder_state_t       state;
    bool                           is_client;              // is this the client side or the server side decoder
    qd_decoder_buffer_t            scratch_buffer;
    nghttp2_hd_inflater           *inflater;               // nghttp2 inflator that inflates the HPACK request/response headers.
    qd_http2_frame_type            frame_type;             // Frame processing related fields
    uint32_t                       frame_length;           // frame_length = 9 byte frame header + Frame payload length.
    uint32_t                       frame_length_processed; // How much of the (9 byte frame header + Frame payload length) have we already processed
    uint32_t                       frame_payload_length;   // What is the payload length for the frame ? This does not include the 9 byte frame header.
};

struct qd_http2_decoder_connection_t {
    uintptr_t                             user_context;
    qd_http2_decoder_t                    client_decoder;       // client stream decoder
    qd_http2_decoder_t                    server_decoder;       // server stream decoder
    char                                 *parse_error;
    const qd_http2_decoder_callbacks_t   *callbacks;
    uint64_t                              conn_id;             // this conn_id is used mainly for logging purposes.
};

ALLOC_DECLARE(qd_http2_decoder_connection_t);
ALLOC_DEFINE(qd_http2_decoder_connection_t);

static void decoder_new_state(qd_http2_decoder_t *decoder, qd_http2_decoder_state_t new_state)
{
    decoder->state = new_state;
}

/**
 * Clear the frame related information in the decoder so these variables can be set again by the next frame.
 */
static void reset_decoder_frame_info(qd_http2_decoder_t *decoder)
{
    decoder->frame_length = 0;
    decoder->frame_length_processed = 0;
    decoder->frame_payload_length = 0;
    decoder->frame_type = FRAME_TYPE_NONE;
}

/**
 * A protocol parsing error has occurred.  There is no recovery
 */
static void parser_error(qd_http2_decoder_t *decoder, const char *reason)
{
    qd_http2_decoder_connection_t *conn_state = decoder->conn_state;
    qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] parser_error - reason=%s", decoder->conn_state->conn_id, reason);
    if (!conn_state->parse_error) {
        conn_state->parse_error = qd_strdup(reason);
        decoder_new_state(decoder, HTTP2_DECODE_ERROR);
        if (conn_state->callbacks && conn_state->callbacks->on_decode_error) {
            conn_state->callbacks->on_decode_error(conn_state, conn_state->user_context,  decoder->is_client, reason);
        }
    }
}

/**
 * Every other frame type other than request/response header is of type FRAME_TYPE_OTHER.
 * We will jump right over FRAME_TYPE_OTHER frames.
 * We will deep parse only frame of type FRAME_TYPE_HEADER
 */
qd_http2_frame_type get_frame_type(const uint8_t frame_type)
{
    switch (frame_type) {
    case FRAME_TYPE_HEADER: {
        return FRAME_TYPE_HEADER;
    }
    default:
        break;
    }
    return FRAME_TYPE_OTHER;
}

uint8_t get_pad_length(const uint8_t *data)
{
    uint8_t pad_length = (uint8_t) ((data)[0]);
    return pad_length;
}

uint32_t get_stream_identifier(const uint8_t *data)
{
    uint32_t stream_id = (((uint32_t) (data)[0]) << 24) | (((uint32_t) (data)[1]) << 16) | (((uint32_t) (data)[2]) << 8) | ((uint32_t) (data)[3]);
    return BIT_CLEAR(stream_id, (UINT32_C(1)<<31));
}


qd_http2_frame_type get_frame_payload_length_and_type(qd_http2_decoder_t *decoder, const uint8_t *data, uint32_t *frame_payload_length)
{
    *frame_payload_length =  (((uint32_t) data[0]) << 16) | (((uint32_t) data[1]) << 8) | ((uint32_t) data[2]);
    qd_http2_frame_type frame_type = get_frame_type(data[3]);
    return frame_type;
}


static int allocate_move_to_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer, const uint8_t *data, size_t capacity)
{
    if (capacity > HTTP2_SCRATCH_BUFFER_MAX_SIZE) {
        return -1;
    }
    scratch_buffer->bytes = qd_malloc(capacity);
    ZERO(scratch_buffer->bytes);
    memcpy(scratch_buffer->bytes, data, capacity);
    scratch_buffer->capacity = capacity;
    scratch_buffer->size = capacity;
    scratch_buffer->offset = 0;
    return capacity;
}


static int reallocate_move_to_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer, const uint8_t *data, size_t data_length)
{
    if (scratch_buffer->size == 0) {
        return -1;
    }
    size_t old_size = scratch_buffer->size;
    if (old_size + data_length > HTTP2_SCRATCH_BUFFER_MAX_SIZE) {
        return -1;
    }
    scratch_buffer->bytes = qd_realloc(scratch_buffer->bytes, old_size + data_length);
    memcpy(scratch_buffer->bytes + old_size, data, data_length);
    scratch_buffer->capacity = old_size + data_length;
    scratch_buffer->size = scratch_buffer->capacity;

    return scratch_buffer->size;
}

void reset_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer)
{
    free(scratch_buffer->bytes);
    scratch_buffer->bytes = 0;
    scratch_buffer->size = 0;
    scratch_buffer->offset = 0;
}

int move_to_scratch_buffer(qd_decoder_buffer_t  *scratch_buffer, const uint8_t *data, size_t data_length)
{
    if (scratch_buffer->size > 0) {
        return reallocate_move_to_scratch_buffer(scratch_buffer, data, data_length);
    } else {
        return allocate_move_to_scratch_buffer(scratch_buffer, data, data_length);
    }
}

static bool is_scratch_buffer_empty(qd_decoder_buffer_t *scratch_buffer)
{
    if (!scratch_buffer->bytes)
        return true;
    return false;
}

uintptr_t qd_http2_decoder_connection_get_context(const qd_http2_decoder_connection_t *conn_state)
{
    return conn_state->user_context;
}

qd_http2_decoder_connection_t *qd_http2_decoder_connection(const qd_http2_decoder_callbacks_t *callbacks, uintptr_t user_context, uint64_t conn_id)
{
    qd_http2_decoder_connection_t *conn_state = new_qd_http2_decoder_connection_t();
    ZERO(conn_state);
    conn_state->conn_id                    = conn_id;
    conn_state->user_context               = user_context;
    conn_state->callbacks                  = callbacks;

    conn_state->client_decoder.is_client   = true;
    conn_state->client_decoder.state       = HTTP2_DECODE_CONNECTION_PREFACE;
    conn_state->client_decoder.conn_state  = conn_state;
    conn_state->client_decoder.frame_type  = FRAME_TYPE_NONE;
    ZERO(&conn_state->client_decoder.scratch_buffer);
    nghttp2_hd_inflate_new(&conn_state->client_decoder.inflater); // Creates a new nghttp2 inflater

    conn_state->server_decoder.is_client  = false;
    conn_state->server_decoder.state      = HTTP2_DECODE_FRAME_HEADER;
    conn_state->server_decoder.conn_state = conn_state;
    conn_state->server_decoder.frame_type  = FRAME_TYPE_NONE;
    ZERO(&conn_state->server_decoder.scratch_buffer);
    nghttp2_hd_inflate_new(&conn_state->server_decoder.inflater);

    return conn_state;
}

void qd_http2_decoder_connection_free(qd_http2_decoder_connection_t *conn_state)
{
    nghttp2_hd_inflate_del(conn_state->client_decoder.inflater);
    nghttp2_hd_inflate_del(conn_state->server_decoder.inflater);
    reset_scratch_buffer(&conn_state->client_decoder.scratch_buffer);
    reset_scratch_buffer(&conn_state->server_decoder.scratch_buffer);
    conn_state->callbacks = 0;
    free(conn_state->parse_error);
    free_qd_http2_decoder_connection_t(conn_state);
}

/**
 * Decompresses headers that were compressed using HPACK header compression.
 * Uses nghttp2 provided API to uncompress the compressed header fields.
 * The passed in pointer to data must point to the beginning of the complete header block fragment that contains the compressed header fields.
 */
static int decompress_headers(qd_http2_decoder_t *decoder, uint32_t stream_id, const uint8_t *data, size_t length)
{
    if (length == 0) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] decompress_headers - passed in length = 0", decoder->conn_state->conn_id);
        return -1;
    }
    qd_http2_decoder_connection_t *conn_state = decoder->conn_state;
    if (conn_state->callbacks && conn_state->callbacks->on_begin_header) {
        conn_state->callbacks->on_begin_header(conn_state,
                                               conn_state->user_context,
                                               decoder->is_client,
                                               stream_id);
    }
    for (;;) {
        nghttp2_nv nv;
        int inflate_flags = 0;
        size_t proclen;
        int rv = nghttp2_hd_inflate_hd2(decoder->inflater, &nv, &inflate_flags, data, length, 1);
        if (rv < 0) {
            qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] decompress_headers - decompression error rv=%i", decoder->conn_state->conn_id, rv);
            return rv;
        }
        proclen = (size_t)rv;
        data += proclen;
        length -= proclen;

        if (inflate_flags & NGHTTP2_HD_INFLATE_EMIT) {
            if (conn_state->callbacks && conn_state->callbacks->on_header) {
                // Invoke the on_header callback when we get every header name/value pair
                conn_state->callbacks->on_header(conn_state,
                                                 conn_state->user_context,
                                                 decoder->is_client,
                                                 stream_id,
                                                 nv.name,
                                                 nv.namelen,
                                                 nv.value,
                                                 nv.valuelen);
            }
        }

        if (inflate_flags & NGHTTP2_HD_INFLATE_FINAL) {
            nghttp2_hd_inflate_end_headers(decoder->inflater);
            break;
        }
        if ((inflate_flags & NGHTTP2_HD_INFLATE_EMIT) == 0 && length == 0) {
            break;
        }
    }
    return 0;
}

/**
 * Checks if the nth bit is set in the passed in unsigned byte, data
 */
static bool is_nth_bit_set(const uint8_t data, int bit)
{
    return ((uint8_t)data) & (uint8_t)(1 << bit);
}

static void get_header_flags(const uint8_t data, bool *end_stream, bool *end_headers, bool *is_padded, bool *has_priority)
{
    *end_stream   =  is_nth_bit_set(data, 0);
    *end_headers  =  is_nth_bit_set(data, 2);
    *is_padded    =  is_nth_bit_set(data, 3);
    *has_priority =  is_nth_bit_set(data, 5);
}

bool get_request_headers(qd_http2_decoder_t *decoder)
{
    qd_decoder_buffer_t *scratch_buffer = &decoder->scratch_buffer;
    if (scratch_buffer->size == 0) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] get_request_headers - failure due to zero scratch buffer size, moving decoder state to HTTP2_DECODE_ERROR", decoder->conn_state->conn_id);
        static char error[106];
        snprintf(error, sizeof(error), "get_request_headers - failure due to zero scratch buffer size, moving decoder state to HTTP2_DECODE_ERROR");
        reset_decoder_frame_info(decoder);
        reset_scratch_buffer(&decoder->scratch_buffer);
        parser_error(decoder, error);
        return false;
    }
    bool end_stream, end_headers, is_padded, has_priority;
    get_header_flags(scratch_buffer->bytes[0], &end_stream, &end_headers, &is_padded, &has_priority);
    decoder->frame_length_processed += 1;
    scratch_buffer->offset += 1;
    uint32_t stream_id = get_stream_identifier(scratch_buffer->bytes + scratch_buffer->offset);
    qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] get_request_headers - end_stream=%i, end_headers=%i, is_padded=%i, has_priority=%i, stream_id=%" PRIu32, decoder->conn_state->conn_id, end_stream, end_headers, is_padded, has_priority, stream_id);
    scratch_buffer->offset += HTTP2_FRAME_STREAM_ID_LENGTH;
    decoder->frame_length_processed += HTTP2_FRAME_STREAM_ID_LENGTH;
    uint8_t pad_length = 0;
    if (is_padded) {
        pad_length = get_pad_length(scratch_buffer->bytes + scratch_buffer->offset);
        // Move one byte to account for pad length
        scratch_buffer->offset += 1;
        decoder->frame_length_processed += 1;
    }

    if (has_priority) {
        // Skip the Stream Dependency field if the priority flag is set
        // Stream Dependency field is 4 octets.
        scratch_buffer->offset += 4;
        decoder->frame_length_processed += 4;

        // Skip the weight field if the priority field is true.
        scratch_buffer->offset += 1;
        decoder->frame_length_processed += 1;
    }

    //
    // Before the call to decompress_headers(), we need to make sure that there is some data left in the scratch buffer before we decompress it.
    //
    int buffer_data_size = scratch_buffer->size - scratch_buffer->offset;
    int contains_pad_length = scratch_buffer->size - pad_length;
    int pad_length_offset = scratch_buffer->size - pad_length - scratch_buffer->offset;
    bool valid_pad_length = contains_pad_length > 0;
    bool valid_buffer_data = buffer_data_size > 0;
    bool valid_pad_length_offset = pad_length_offset > 0;
    if (decoder->frame_payload_length == 0 || !valid_buffer_data || !valid_pad_length || !valid_pad_length_offset) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] get_request_headers - failure, moving decoder state to HTTP2_DECODE_ERROR", decoder->conn_state->conn_id);
        static char error[130];
        snprintf(error, sizeof(error), "get_request_headers - either request or response header was received with zero payload or contains bogus data, stopping decoder");
        reset_decoder_frame_info(decoder);
        reset_scratch_buffer(&decoder->scratch_buffer);
        parser_error(decoder, error);
        return false;
    }

    // Take out the padding bytes from the end of the scratch buffer
    scratch_buffer->size = scratch_buffer->size - pad_length;

    // We are now finally at a place which matters to us - The Header block fragment. We will look thru and decompress it so we can get the request/response headers.
    int rv = decompress_headers(decoder, stream_id, scratch_buffer->bytes + scratch_buffer->offset, scratch_buffer->size - scratch_buffer->offset);
    if (rv < 0) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] get_request_headers - failure, moving decoder state to HTTP2_DECODE_ERROR", decoder->conn_state->conn_id);
        static char error[105];
        snprintf(error, sizeof(error), "get_request_headers - failure, moving decoder state to HTTP2_DECODE_ERROR nghttp2 error code=%i", rv);
        reset_decoder_frame_info(decoder);
        reset_scratch_buffer(&decoder->scratch_buffer);
        parser_error(decoder, error);
        return false;
    }
    else {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] get_request_headers - success, moving decoder state to HTTP2_DECODE_FRAME_HEADER", decoder->conn_state->conn_id);
        decoder_new_state(decoder, HTTP2_DECODE_FRAME_HEADER);
        reset_decoder_frame_info(decoder);
        reset_scratch_buffer(&decoder->scratch_buffer);
        return true;
    }
}


static bool parse_request_header(qd_http2_decoder_t *decoder, const uint8_t **data, size_t *length)
{
    if (*length == 0)
        return false;
    size_t bytes_to_copy = decoder->frame_length - decoder->frame_length_processed;
    if (*length < bytes_to_copy) {
        int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, *length);
        if (allocated == -1) {
            parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
            return false;
        }
        decoder->frame_length_processed += *length;
        *data += *length;
        *length = 0;
        return false;
    } else {
        int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, bytes_to_copy);
        if (allocated == -1) {
            parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
            return false;
        }
        *data += bytes_to_copy;
        *length -= bytes_to_copy;
        if ((decoder->frame_length_processed + bytes_to_copy) == decoder->frame_length) {
            bool header_success = get_request_headers(decoder);
            if (!header_success)
                return false;
        }
        if (*length > 0) {
            return true; // More bytes remain to be processed, continue processing.
        } else {
            return false;
        }
    }
}

/**
 * Jump over all frame payloads except for the request/response headers.
 */
static bool skip_frame_payload(qd_http2_decoder_t *decoder, const uint8_t **data, size_t *length)
{
    size_t bytes_to_skip = decoder->frame_length - decoder->frame_length_processed;
    if (*length > bytes_to_skip) {
        *data += bytes_to_skip;
        *length -= bytes_to_skip;
        reset_decoder_frame_info(decoder);
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] skip_frame_payload - decoder->frame_length=%" PRIu32", decoder->frame_length_processed=%" PRIu32", moving decoder state to HTTP2_DECODE_FRAME_HEADER", decoder->conn_state->conn_id, decoder->frame_length, decoder->frame_length_processed);
        decoder_new_state(decoder, HTTP2_DECODE_FRAME_HEADER);
        return true;
    } else {
        *data += *length;
        decoder->frame_length_processed += *length;
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] skip_frame_payload - decoder->frame_length=%" PRIu32", decoder->frame_length_processed=%" PRIu32"", decoder->conn_state->conn_id, decoder->frame_length, decoder->frame_length_processed);
        *length = 0;
        return false;
    }
}

static bool parse_frame_header(qd_http2_decoder_t *decoder, const uint8_t **data, size_t *length)
{
    if (*length == 0)
        return false;
    //
    // First check to see if there is anything in the scratch buffer.
    // The strategy is to copy ONLY the first 4 bytes of every frame header to the scratch buffer.
    //
    if (decoder->scratch_buffer.size > 0) {
        size_t bytes_remaining = HTTP2_FRAME_LENGTH_AND_TYPE_BYTES - decoder->scratch_buffer.size;
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - bytes_remaining=%zu", decoder->conn_state->conn_id, bytes_remaining);

        if (*length >= bytes_remaining) {
            if (bytes_remaining > 0) {
                int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, bytes_remaining);
                if (allocated == -1) {
                    parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
                    return false;
                }
                *length -= bytes_remaining;
                *data += bytes_remaining;
            }

            decoder->frame_type = get_frame_payload_length_and_type(decoder, decoder->scratch_buffer.bytes, &decoder->frame_payload_length);
            decoder->frame_length = decoder->frame_payload_length + HTTP2_FRAME_HEADER_LENGTH;
            qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - decoder->frame_payload_length %"PRIu32"", decoder->conn_state->conn_id, decoder->frame_payload_length);
            reset_scratch_buffer(&decoder->scratch_buffer);
            decoder->frame_length_processed = HTTP2_FRAME_LENGTH_AND_TYPE_BYTES;

            if (decoder->frame_type == FRAME_TYPE_HEADER) {
                qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - moving decoder state to HTTP2_DECODE_REQUEST_RESPONSE_HEADER", decoder->conn_state->conn_id);
                decoder_new_state(decoder, HTTP2_DECODE_REQUEST_RESPONSE_HEADER);
            } else if (decoder->frame_type == FRAME_TYPE_OTHER) {
                qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - moving decoder state to HTTP2_DECODE_SKIP_FRAME_PAYLOAD", decoder->conn_state->conn_id);
                decoder->frame_length_processed = HTTP2_FRAME_LENGTH_AND_TYPE_BYTES;
                decoder_new_state(decoder, HTTP2_DECODE_SKIP_FRAME_PAYLOAD);
            }
            return true;
        } else {
            int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, *length);
            if (allocated == -1) {
                parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
                return false;
            }
            *length = 0;
            *data += *length;
            return false;
        }
    }

    if (*length < HTTP2_FRAME_LENGTH_AND_TYPE_BYTES) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - *length(%zu) < HTTP2_FRAME_LENGTH_AND_TYPE_BYTES, moving %zu bytes to scratch buffer", decoder->conn_state->conn_id, *length, *length);
        int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, *length);
        if (allocated == -1) {
            parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
            return false;
        }
        *length = 0;
        return false;
    } else {
        size_t bytes_remaining = HTTP2_FRAME_LENGTH_AND_TYPE_BYTES - decoder->scratch_buffer.size;
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_frame_header - moving bytes_remaining(%zu) to scratch buffer", decoder->conn_state->conn_id, bytes_remaining);
        int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, bytes_remaining);
        if (allocated == -1) {
            parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
            return false;
        }
        *length -= bytes_remaining;
        *data += bytes_remaining;
        return true;
    }
}


static bool parse_client_magic(qd_http2_decoder_t *decoder, bool from_client, const uint8_t **data, size_t *length)
{
    if (from_client) {
        if (is_scratch_buffer_empty(&decoder->scratch_buffer)) {
            if (*length < HTTP2_CLIENT_PREFIX_LEN) {
                // There is not enough length to check client magic.
                // Copy the data into the scratch buffer and read it from there next time.
                qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_client_magic - length(%zu) < 24 bytes, moving %zu bytes to scratch buffer", decoder->conn_state->conn_id, *length, *length);
                int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, *length);
                if (allocated == -1) {
                    parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
                    return false;
                }
                *length = 0;  // All bytes have been moved into the scratch buffer, set the length to zero since there is no more bytes to be parsed.
                return false;
            } else {
                if (memcmp(*data, http2_client_prefix, HTTP2_CLIENT_PREFIX_LEN) == 0) {
                    qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_client_magic - success, moving decoder state to HTTP2_DECODE_FRAME_HEADER", decoder->conn_state->conn_id);
                    decoder_new_state(decoder, HTTP2_DECODE_FRAME_HEADER);
                    *data += HTTP2_CLIENT_PREFIX_LEN;
                    *length -= HTTP2_CLIENT_PREFIX_LEN;
                    return true;
                } else {
                    // Invalid HTTP2 Prefix or the client did not send the prefix. This is a protocol error
                    parser_error(decoder, "protocol error: client connection prefix is not present or invalid client connection prefix");
                    return false;
                }
            }
        } else {

            // Append this data to the data in the scratch buffer
            if (decoder->scratch_buffer.size + *length >= HTTP2_CLIENT_PREFIX_LEN) {
                // There is already some data in the scratch buffer, there are enough bytes to compare the magic.
                // Just move the required number of bytes into the scratch buffer
                size_t num_bytes_to_consume = HTTP2_CLIENT_PREFIX_LEN - decoder->scratch_buffer.size;
                int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, num_bytes_to_consume);
                if (allocated == -1) {
                    parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
                    return false;
                }
                *length -= num_bytes_to_consume;
                *data += num_bytes_to_consume;

                if (memcmp(decoder->scratch_buffer.bytes, http2_client_prefix, HTTP2_CLIENT_PREFIX_LEN) == 0) {
                    qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] parse_client_magic - full client magic in scratch buffer - success , moving decoder state to HTTP2_DECODE_FRAME_HEADER", decoder->conn_state->conn_id);
                    decoder_new_state(decoder, HTTP2_DECODE_FRAME_HEADER);
                    reset_scratch_buffer(&decoder->scratch_buffer);
                    return true;
                } else {
                    // Invalid HTTP2 Prefix or the client did not send the prefix. This is a protocol error
                    parser_error(decoder, "protocol error: client connection prefix is not present or invalid client connection prefix");
                    return false;
                }
            }
            else {
                int allocated = move_to_scratch_buffer(&decoder->scratch_buffer, *data, *length);
                if (allocated == -1) {
                    parser_error(decoder, "scratch buffer size exceeded 65535 bytes, stopping decoder");
                }
                return false;
            }
        }
    }
    return false;

}


static qd_http2_decoder_state_t get_decoder_state(qd_http2_decoder_t *decoder)
{
    return decoder->state;
}


int decode(qd_http2_decoder_connection_t *conn_state, bool from_client, const uint8_t *data, size_t length)
{
    if (length == 0)
        return 0;
    qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] decode() from_client=%i, length=%zu", conn_state->conn_id, from_client, length);
    qd_http2_decoder_t *decoder = from_client ? &conn_state->client_decoder : &conn_state->server_decoder;
    bool more = true;

    while (more) {
        switch (get_decoder_state(decoder)) {
            case HTTP2_DECODE_CONNECTION_PREFACE:
                more = parse_client_magic(decoder, from_client, &data, &length);
                break;
            case HTTP2_DECODE_FRAME_HEADER:
                more = parse_frame_header(decoder, &data, &length);
                break;
            case HTTP2_DECODE_REQUEST_RESPONSE_HEADER:
                more = parse_request_header(decoder, &data, &length);
                break;
            case HTTP2_DECODE_SKIP_FRAME_PAYLOAD: // jump over frame payloads you don't care about.
                more = skip_frame_payload(decoder, &data, &length);
                break;
            default:
                break;
        }
    }

    return conn_state->parse_error ? -1 : 0;
}


//----------------------------------
// Helper functions for testing only
//----------------------------------

bool is_client_decoder_state_decode_connection_preface(qd_http2_decoder_connection_t *conn_state)
{
    if (conn_state->client_decoder.state == HTTP2_DECODE_CONNECTION_PREFACE) {
        return true;
    }
    return false;
}

bool is_client_decoder_state_decode_frame_header(qd_http2_decoder_connection_t *conn_state)
{
    if (conn_state->client_decoder.state == HTTP2_DECODE_FRAME_HEADER) {
        return true;
    }
    return false;
}

bool is_client_decoder_state_skip_frame_payload(qd_http2_decoder_connection_t *conn_state)
{
    if (conn_state->client_decoder.state == HTTP2_DECODE_SKIP_FRAME_PAYLOAD) {
        return true;
    }
    return false;
}

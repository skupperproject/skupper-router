#ifndef __observers_http2_decoder_h__
#define __observers_http2_decoder_h__ 1

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

// HTTP2 Decoder Library
//
// This library provides a limited decoder for parsing HTTP2 TCP protocol stream.
//
// At this time, the TCP HTTP2 data streams which are fed into the decoder generate only three callbacks -
//  1. When the header frame has arrived
//  2. Each HTTP2 request/response header
//  3. Decoding error.
//
// This library provides two classes:
//
//   qd_http2_decoder_connection_t - a connection state for a single TCP connection over which
//   HTTP2 streams are exchanged between a client and a server.
//
//   qd_http2_decoder_t - the decoder (can represent a client or server side decoder).
//   This decoder ignores all frames except the request/response frames. It deep processes the
//   request/response headers frame to get request and response headers. CONTINUATION frames are ignored.
//
//   The decoder does not validate the HTTP2 stream.
//
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct qd_http2_decoder_connection_t qd_http2_decoder_connection_t;
typedef struct qd_http2_decoder_callbacks_t qd_http2_decoder_callbacks_t;

typedef struct qd_decoder_buffer_t {
    uint8_t    *bytes;     /**< Pointer to the start of the raw buffer, if this is null then no buffer is represented */
    uint32_t    capacity;  /**< Count of available bytes starting at @ref bytes */
    uint32_t    size;      /**< Number of bytes read or to be written starting at @ref offset */
    uint32_t    offset;    /**< First byte in the buffer to be read or written */
} qd_decoder_buffer_t;

struct qd_http2_decoder_callbacks_t {
    //
    // Invoked on each DATA frame
    //
    int (*on_data)(qd_http2_decoder_connection_t *conn_state,
                   uintptr_t request_context,
                   bool from_client,
                   uint32_t stream_id,
                   bool end_stream,
                   uint32_t num_bytes);

    //
    // Invoked when each HEADER is received. Called as many times as there http headers
    // from_client is true if the header was read from the stream sent by the client, false if the stream is sent from the server.
    // The callback must copy the data associated with these values if they need to be saved.
    //
    int (*on_header)(qd_http2_decoder_connection_t *conn_state,
                     uintptr_t request_context,
                     bool from_client,
                     uint32_t stream_id,
                     const uint8_t *name,
                     size_t namelen,
                     const uint8_t *value,
                     size_t valuelen);

    //
    // Invoked when the decoder sees a request header frame before each header is decoded.
    // It looks up the stream id on the header frame and passes it to the callback.
    int (*on_begin_header)(qd_http2_decoder_connection_t *conn_state,
                           uintptr_t request_context,
                           bool from_client,
                           uint32_t stream_id);

    //
    // Invoked when the decoder has decoded all request headers.
    // It looks up the stream id on the header frame and passes it to the callback.
    int (*on_end_headers)(qd_http2_decoder_connection_t *conn_state,
                          uintptr_t request_context,
                          bool from_client,
                          uint32_t stream_id,
                          bool end_stream);

    //
    // Callback called when the http2 decoder runs into a decoding error.
    //
    void (*on_decode_error)(qd_http2_decoder_connection_t *conn_state,
                           uintptr_t request_context,
                           bool from_client, const char *reason);
};

/**
 * Push network data (inbound or outbound) into the http2 decoder. All callbacks occur before this call completes.  The return value is zero on
 * success.  If a parse error occurs then the protocol_error callback will be invoked and a non-zero value is returned.
 * The decoder and the observer will not recover from protocol errors, they will shut down and will no longer observe the connection.
 *
 * @param - qd_http2_decoder_connection_t - connection state
 * @param - from_client - indicates if the bytes passed in are from the client or server
 * data - a pointer to the unsigned byte array containing bytes that need to be decoded.
 * length - length of the data byte array.
 */
int decode(qd_http2_decoder_connection_t *state, bool from_client, const uint8_t *data, size_t length);


/**
 * Creates a new connection state object and assign it a user context.
 *
 * @param qd_http2_decoder_callbacks_t - a poointer to the callbacks (qd_http2_decoder_callbacks_t)
 * @param user_context - The user context that will be returned by the callbacks.
 * @param conn_id - is used for logging purposes
 */
qd_http2_decoder_connection_t *qd_http2_decoder_connection(const qd_http2_decoder_callbacks_t *callbacks, uintptr_t user_context, uint64_t conn_id);

/**
 * Frees the decoder connection state object and its related entities.
 * @param conn_state - pointer to qd_http2_decoder_connection_t that needs to be freed.
 */
void qd_http2_decoder_connection_free(qd_http2_decoder_connection_t *conn_state);

/*
* Returns the connection context given in the qd_http2_decoder_connection() call.
*/
uintptr_t qd_http2_decoder_connection_get_context(const qd_http2_decoder_connection_t *conn_state);

#endif

#ifndef __http1_decoder_h__
#define __http1_decoder_h__ 1
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
 *
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>


// HTTP/1.x Decoder Library
//
// This library provides an API for decoding HTTP/1.x protocol streams
//
// The decoder takes input containing HTTP/1.x data read from the TCP connection and issues callbacks as various parts
// (headers, body, status) of the HTTP/1.x message are parsed.
//
// This library provides two classes:
//
// * qd_http1_decoder_connection_t - a context for a single TCP connection over which HTTP/1.x messages are exchanged
//   between a client and a server.
//
// * qd_http1_decoder_config.h - a structure to configure the callbacks for a given qd_http1_decoder_connection_t. A
//   pointer to an initialized instances of this must be passed to the qd_http1_decoder_connection() constructor. It is
//   expected that the instance's lifecycle will remain valid until return of the call to the
//   qd_http1_decoder_connection_free() destructor.
//

typedef struct qd_http1_decoder_config_t      qd_http1_decoder_config_t;
typedef struct qd_http1_decoder_connection_t  qd_http1_decoder_connection_t;

struct qd_http1_decoder_config_t {
    //
    // Decoder callbacks
    //
    // Callbacks invoked when parsing incoming raw connection data. These callbacks are invoked from the
    // h1_decode_connection_rx_data() call. These callbacks should return 0 on success or non-zero on error.  A non-zero
    // return code is used as the return code from h1_decode_connection_rx_data()
    //

    // New HTTP request received. The caller should set a request_context that will be passed back in all callbacks
    // pertaining to this request/response transaction. The method and target are not preserved on return from this
    // call. The callback must copy the data associated with these values if they need to be saved.
    //
    int (*rx_request)(qd_http1_decoder_connection_t *hconn,
                      const char *method,
                      const char *target,
                      uint32_t   version_major,
                      uint32_t   version_minor,
                      uintptr_t  *request_context);

    // HTTP response received. The request_context comes from the rx_request() callback issued for the corresponding
    // request. Note well that if status_code is Informational (1xx) then this response is NOT the last response for the
    // current request (See RFC7231, 6.2 Informational 1xx).  The transaction_complete callback will be called after the
    // LAST response for the given request has been received. The reason_phrase is not preserved on return from this
    // call. The callback must copy the data associated with this value if it needs to be saved.
    //
    int (*rx_response)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context,
                       int status_code,
                       const char *reason_phrase,
                       uint32_t version_major,
                       uint32_t version_minor);

    // Invoked for each HTTP header received. from_client is true if the header was read from the stream sent by the
    // client, false if the stream is sent from the server. Neither key nor value is preserved on return from this
    // call. The callback must copy the data associated with these values if they need to be saved.
    //
    int (*rx_header)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client,
                     const char *key, const char *value);

    // Invoked after the last HTTP header (after the last rx_header() callback for this message).
    //
    int (*rx_headers_done)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client);

    // (Optional) invoked as the HTTP1 message body is parsed. length is set to the number of data octets in the
    // body buffer. The body buffer lifecycle ends on return from this call - if the caller needs to preserve the body
    // data it must copy it.
    //
    int (*rx_body)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client, const unsigned char *body, size_t length);

    // Invoked after a received HTTP message has been completely parsed.
    //
    int (*message_done)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client);

    // Invoked when the HTTP request/response messages have been completely encoded/decoded and the transaction is complete.
    // The request_context will never be used by the decoder again on return from this call.
    //
    int (*transaction_complete)(qd_http1_decoder_connection_t *hconn, uintptr_t request_context);

    // Invoked if the decoder is unable to parse the incoming stream. No further callbacks will occur for this
    // connection.  The h1_docode_connection_rx_data() will return a non-zero value. It is expected that the user will
    // clean up all context(s) associated with the connection and any in-progress transactions.
    //
    void (*protocol_error)(qd_http1_decoder_connection_t *hconn, const char *reason);

};


// Create a new connection and assign it a context. The config object lifecycle must exist for the duration of the
// qd_http1_decoder_connection_t.
//
qd_http1_decoder_connection_t *qd_http1_decoder_connection(const qd_http1_decoder_config_t *config, uintptr_t context);

// Obtain the connection context given in the h1_decode_connection() call.
//
uintptr_t qd_http1_decoder_connection_get_context(const qd_http1_decoder_connection_t *conn);

// Release the codec. Any outstanding request/response state is released immediately, including any in-progress
// requests.
//
void qd_http1_decoder_connection_free(qd_http1_decoder_connection_t *conn);

// Push inbound network data into the http1 decoder. All callbacks occur during this call.  The return value is zero on
// success.  If a parse error occurs then the protocol_error callback will be invoked and a non-zero value is returned.
//
// Errors are not recoverable: further calls will return a non-zero value.
//
int qd_http1_decoder_connection_rx_data(qd_http1_decoder_connection_t *conn, bool from_client, const unsigned char *data, size_t len);

#endif // __http1_decoder_h__

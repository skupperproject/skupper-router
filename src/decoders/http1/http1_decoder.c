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

#include "http1_decoder.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/discriminator.h"
#include "buffer_field_api.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#define DEBUG_DECODER 0  // 1 == turn on debug printf

#define HTTP1_MAX_LINE_LEN 65535  // (arbitrary) limit line parsing to avoid DOS attacks

//
// This file contains code for decoding an HTTP/1.x data stream.  See decoder.h for details.
//

typedef struct h1_decode_request_state_t h1_decode_request_state_t;
typedef struct decoder_t                 decoder_t;


static const uint8_t CR_TOKEN = '\r';
static const uint8_t LF_TOKEN = '\n';

// RFC9112 linear white space
static const char * const LWS_CHARS  = " \t";


// Classify the response codes
#define IS_INFO_RESPONSE(code)    ((code) / 100 == 1)  // 1xx
#define IS_SUCCESS_RESPONSE(code) ((code) / 100 == 2)  // 2xx

// true if response code indicates that the response will NOT contain a body
// 204 = No Content
// 205 = Reset Content
// 304 = Not Modified
#define NO_BODY_RESPONSE(code) \
    ((code) == 204 ||          \
     (code) == 205 ||          \
     (code) == 304 ||          \
     IS_INFO_RESPONSE(code))


typedef enum {
    HTTP1_INVALID_STATE = 0,
    HTTP1_DECODE_REQUEST,       // parsing request start line
    HTTP1_DECODE_RESPONSE,      // parsing response start line
    HTTP1_DECODE_HEADERS,       // parsing headers
    HTTP1_DECODE_BODY,          // parsing fixed-length body
    HTTP1_DECODE_CHUNK_HEADER,  // parsing chunk encoded body header
    HTTP1_DECODE_CHUNK_DATA,    // parsing chunk encoded body data
    HTTP1_DECODE_CHUNK_TRAILER, // parsing chunk encoded body trailer
    HTTP1_DECODE_ERROR,         // parse error (terminal state)
} http1_decoder_state_t;

#if DEBUG_DECODER
const char * const decoder_state[] = {
    "HTTP1_INVALID_STATE",
    "HTTP1_DECODE_REQUEST",
    "HTTP1_DECODE_RESPONSE",
    "HTTP1_DECODE_HEADERS",
    "HTTP1_DECODE_BODY",
    "HTTP1_DECODE_CHUNK_HEADER",
    "HTTP1_DECODE_CHUNK_DATA",
    "HTTP1_DECODE_CHUNK_TRAILER",
    "HTTP1_DECODE_ERROR"
};
#endif

// A re-sizeable buffer for holding received HTTP data while parsing.
// Currently this is used for start lines, headers, and chunk headers.
// Null terminated.
//
typedef struct parse_buffer_t {
    unsigned char *data;
    uint32_t       mem_size;  // size of allocated memory for data, not content length!
    uint32_t       length;    // amount of data in *data (not including null terminator).
} parse_buffer_t;


// State for a single request-response transaction.
//
// A new state is created when a new request appears in the client stream. Once the corresponding response message has
// completed this state is released.
//
struct h1_decode_request_state_t {
    DEQ_LINKS(h1_decode_request_state_t);
    qd_http1_decoder_connection_t *hconn;
    uintptr_t                      user_context;

#if 0  // TBD
    uint64_t client_octets;  // # total octets arriving from client for this request
    uint64_t server_octets;  // # total octets arriving from server for this response
#endif

    int32_t response_code;   // sent by server

    bool head_request:1;      // HEAD method
    bool connect_request:1;   // CONNECT method
    bool request_complete:1;  // true when request message done encoding/decoding
    bool response_complete:1; // true when response message done encoding/decoding
    bool close_expected:1;    // if true do not signal request_complete cb until closed
};
DEQ_DECLARE(h1_decode_request_state_t, h1_decode_request_state_list_t);
ALLOC_DECLARE(h1_decode_request_state_t);
ALLOC_DEFINE(h1_decode_request_state_t);


// Decodes an incoming stream of HTTP/1.x traffic.
//
// The decoder only copies incoming data where necessary. This includes start line data, headers, and the chunk
// boundaries.  Raw body data is not copied.
//
struct decoder_t {

    qd_http1_decoder_connection_t *hconn;  // parent connection
    h1_decode_request_state_t     *hrs;    // current request/response state
    http1_decoder_state_t          state;
    parse_buffer_t                 buffer;

    int64_t                    body_length;  // content length or current chunk length

    bool is_client:1;
    bool is_chunked:1;
    bool is_http10:1;

    // decoded headers
    bool hdr_transfer_encoding:1;
    bool hdr_content_length:1;
};


// The HTTP/1.1 connection
//
struct qd_http1_decoder_connection_t {
    uintptr_t                        user_context;
    const qd_http1_decoder_config_t *config;

    // Pipelining allows a client to send multiple requests before receiving any responses.  Pending request are stored
    // in order: new requests are added to TAIL. The in-progress response is at HEAD.
    //
    h1_decode_request_state_list_t hrs_queue;

    const char *parse_error;  // if set parser has failed
    decoder_t   client;       // client stream decoder
    decoder_t   server;       // server stream decoder
};
ALLOC_DECLARE(qd_http1_decoder_connection_t);
ALLOC_DEFINE(qd_http1_decoder_connection_t);


// Expand the parse buffer up to required octets.
//
static void ensure_buffer_size(parse_buffer_t *b, size_t required)
{
    if (b->mem_size < required) {
        b->mem_size = required;
        if (!b->data)
            b->data = qd_malloc(b->mem_size);
        else
            b->data = qd_realloc(b->data, b->mem_size);
    }
}


static inline void decoder_new_state(decoder_t *decoder, http1_decoder_state_t new_state)
{
#if DEBUG_DECODER
    if (decoder->state == HTTP1_INVALID_STATE) {
        fprintf(stdout, "%s decoder initial state: %s\n", decoder->is_client ? "client" : "server",
                decoder_state[new_state]);
    } else {
        fprintf(stdout, "%s decoder state %s -> %s\n", decoder->is_client ? "client" : "server",
                decoder_state[decoder->state], decoder_state[new_state]);
    }
#endif
    decoder->state = new_state;
}


// A protocol parsing error has occurred.  There is no recovery - we're dead in the water.
//
static void parser_error(qd_http1_decoder_connection_t *hconn, const char *reason)
{
    if (!hconn->parse_error) {
        hconn->parse_error = reason;
        decoder_new_state(&hconn->client, HTTP1_DECODE_ERROR);
        decoder_new_state(&hconn->server, HTTP1_DECODE_ERROR);
        if (hconn->config->protocol_error)
            hconn->config->protocol_error(hconn, reason);
    }
}


// reset the rx decoder state machine for re-use after a message completes decoding
//
static void decoder_reset(decoder_t *decoder)
{
    if (decoder->state == HTTP1_DECODE_ERROR)
        // Once the decoder has failed parsing it cannot reliably be re-used on the stream
        // since we have lost synchonization
        return;

    decoder_new_state(decoder, decoder->is_client ? HTTP1_DECODE_REQUEST : HTTP1_DECODE_RESPONSE);

    decoder->hrs           = 0;
    decoder->body_length   = 0;
    decoder->buffer.length = 0;

    // do not change is_client!

    decoder->is_chunked            = false;
    decoder->is_http10             = false;
    decoder->hdr_transfer_encoding = false;
    decoder->hdr_content_length    = false;
}


// Create a new request state - this is done when a new http request arrives.
//
static h1_decode_request_state_t *h1_decode_request_state(qd_http1_decoder_connection_t *hconn)
{
    h1_decode_request_state_t *hrs = new_h1_decode_request_state_t();
    ZERO(hrs);
    DEQ_ITEM_INIT(hrs);
    hrs->hconn = hconn;
    return hrs;
}


// Free a request/response state
//
static void h1_decode_request_state_free(h1_decode_request_state_t *hrs)
{
    if (hrs) {
        free_h1_decode_request_state_t(hrs);
    }
}


// Create a new connection state instance.
//
qd_http1_decoder_connection_t *qd_http1_decoder_connection(const qd_http1_decoder_config_t *config, uintptr_t user_context)
{
    assert(config);

    qd_http1_decoder_connection_t *hconn = new_qd_http1_decoder_connection_t();
    ZERO(hconn);

    hconn->user_context = user_context;
    hconn->config       = config;
    DEQ_INIT(hconn->hrs_queue);

    // init the decoders

    hconn->client.is_client = true;
    hconn->client.hconn     = hconn;
    decoder_reset(&hconn->client);

    hconn->server.is_client = false;
    hconn->server.hconn     = hconn;
    decoder_reset(&hconn->server);

    return hconn;
}


uintptr_t qd_http1_decoder_connection_get_context(const qd_http1_decoder_connection_t *hconn)
{
    assert(hconn);
    return hconn->user_context;
}


// Free the connection
//
void qd_http1_decoder_connection_free(qd_http1_decoder_connection_t *conn)
{
    if (conn) {
        h1_decode_request_state_t *hrs = DEQ_HEAD(conn->hrs_queue);
        while (hrs) {
            DEQ_REMOVE_HEAD(conn->hrs_queue);
            h1_decode_request_state_free(hrs);
            hrs = DEQ_HEAD(conn->hrs_queue);
        }

        decoder_reset(&conn->client);
        free(conn->client.buffer.data);

        decoder_reset(&conn->server);
        free(conn->server.buffer.data);

        free_qd_http1_decoder_connection_t(conn);
    }
}


static void debug_print_line(const char *prefix, const char *line)
{
#if DEBUG_DECODER
    fprintf(stdout, "%s '%s'\n", prefix, line);
    fflush(stdout);
#endif
}


// Read incoming http line starting at 'data' into the decoders parse buffer. Stop when a CRLF is parsed.
//
// Returns a pointer to the null-terminated line and advances *data/*length past the line. The returned line has the
// trailin CRLF stripped off.
//
// Returns null if the line is incomplete (need more data) or a parse error occurs.
//
// NOTE WELL: When a non-null pointer is returned the parsed line is stored in the decoder parse buffer. This parse
// buffer will be overwritten on the next call to read_line()!
//
static char *read_line(decoder_t *decoder, const unsigned char **data, size_t *length)
{
    const unsigned char *ptr = *data;
    const unsigned char *end = *data + *length;

    while (ptr != end) {
        if (*ptr == 0 || *ptr > 0x7E) { // invalid USASCII
            parser_error(decoder->hconn, "protocol error: non USASCII data");
            return 0;
        }

        if (*ptr == LF_TOKEN) {
            break;
        }

        ptr += 1;
    }

    // at this point if ptr == end no LF found and we need to store all the data

    const size_t to_copy = (ptr == end) ? *length : ptr - *data + 1;  // +1: skip LF as well
    const size_t total   = decoder->buffer.length + to_copy;

    // avoid infinite-line DOS attack by failing if the line is too long
    if (total > HTTP1_MAX_LINE_LEN) {
        parser_error(decoder->hconn, "protocol error: received line too long");
        return 0;
    }

    ensure_buffer_size(&decoder->buffer, total + 1);
    memcpy(&decoder->buffer.data[decoder->buffer.length], *data, to_copy);
    decoder->buffer.length += to_copy;
    *data += to_copy;
    *length -= to_copy;

    if (ptr == end) {
        // need more (will append at decoder->buffer.data[decoder->buffer.length])
        return 0;
    }

    // At this point we know that buffer contains at least a LF (so buffer.length >= 1).
    // Strip trailing CRLF and null terminate the buffer

    assert(decoder->buffer.length > 0);
    unsigned char *trim = &decoder->buffer.data[decoder->buffer.length - 1];
    while (*trim == LF_TOKEN || *trim == CR_TOKEN) {
        *trim = '\0';
        if (trim-- == &decoder->buffer.data[0])
            break;
    }
    decoder->buffer.length = 0;  // reset line parser
    return (char *) &decoder->buffer.data[0];
}


// Return the first non-linear whitespace character in line.
// Will return empty string if line contains only whitespace.
// RFC7230 defines OWS as zero or more spaces or horizontal tabs
//
static char *trim_whitespace(char *line)
{
    while (*line && (*line == ' ' || *line == '\t'))
        line++;
    return line;
}

// Remove any trailing linear whitespace characters in line.
//
static void truncate_whitespace(char *line)
{
    char *eol = &line[strlen((char *) line)];
    if (eol-- == line)
        return;  // empty line
    while (*eol == ' ' || *eol == '\t') {
        *eol = '\0';
        if (eol-- == line)
            break;
    }
}


// Called when incoming message is complete.
// Returns false on parse error, else true.
//
static bool message_done(qd_http1_decoder_connection_t *hconn, decoder_t *decoder)
{
    h1_decode_request_state_t *hrs = decoder->hrs;

    assert(hrs);

    if (!decoder->is_client) {
        // Informational 1xx response codes are NOT terminal - further responses are allowed!
        if (IS_INFO_RESPONSE(hrs->response_code)) {
            hrs->response_code = 0;
        } else {
            hrs->response_complete = true;
        }
    } else {
        hrs->request_complete = true;
    }

    // signal the message receive is complete
    int rc = hconn->config->message_done(hconn, hrs->user_context, decoder->is_client);
    if (rc) {
        parser_error(hconn, "message_done callback failed");
        return false;
    }

    decoder_reset(decoder);  // resets decode state to parse_request or parse_response

    if (hrs->response_complete && hrs->request_complete) {
        int rc = hconn->config->transaction_complete(hconn, hrs->user_context);
        assert(DEQ_HEAD(hconn->hrs_queue) == hrs);  // expect completed in-order
        DEQ_REMOVE_HEAD(hconn->hrs_queue);
        h1_decode_request_state_free(hrs);
        hrs = 0;
        if (rc) {
            parser_error(hconn, "transaction_complete callback failed");
            return false;
        }
    }

    return true;
}


//////////////////////
// Parse start line //
//////////////////////


// parse the HTTP/1.1 request line:
// "method SP request-target SP HTTP-version CRLF"
//
static bool parse_request_line(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    char *line = read_line(decoder, data, length);
    if (!line)
        return false;  // need more data

    if (*line == '\0') {
        // RFC9112 ignore blank lines before the request. Continue parsing data.
        return !!(*length);
    }

    debug_print_line("request line:", line);

    char *saveptr = 0;
    char *method  = strtok_r(line, LWS_CHARS, &saveptr);
    char *target  = strtok_r(0, LWS_CHARS, &saveptr);
    char *version = strtok_r(0, LWS_CHARS, &saveptr);

    if (!method || !target || !version) {
        parser_error(hconn, "Malformed request line");
        return false;
    }

    uint32_t minor = 0;
    if (strcmp(version, "HTTP/1.1") == 0) {
        minor = 1;
    } else if (strcmp(version, "HTTP/1.0") == 0) {
        minor = 0;
        decoder->is_http10 = true;
    } else {
        parser_error(hconn, "Unsupported HTTP/1.x version");
        return false;
    }

    h1_decode_request_state_t *hrs = h1_decode_request_state(hconn);
    DEQ_INSERT_TAIL(hconn->hrs_queue, hrs);

    // check for methods that do not support body content in the response:
    if (strcmp(method, "HEAD") == 0)
        hrs->head_request = true;
    else if (strcmp(method, "CONNECT") == 0)
        hrs->connect_request = true;

    decoder->hrs = hrs;

    int rc = hconn->config->rx_request(hconn, method, target, 1, minor, &hrs->user_context);
    if (rc) {
        parser_error(hconn, "rx_request callback failed");
        return false;
    }
    decoder_new_state(decoder, HTTP1_DECODE_HEADERS);
    return !!(*length);
}


// parse the HTTP/1.1 response line
// "HTTP-version SP status-code [SP reason-phrase] CRLF"
//
static bool parse_response_line(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    char *line = read_line(decoder, data, length);
    if (!line)
        return false;  // need more data

    const size_t in_octets = strlen(line);
    if (in_octets == 0) {
        // RFC9112 ignore blank lines before the response
        return !!(*length);
    }

    debug_print_line("response line:", line);

    // Responses arrive in the same order as requests are generated so this new
    // response corresponds to head hrs

    h1_decode_request_state_t *hrs = DEQ_HEAD(hconn->hrs_queue);
    if (!hrs) {
        // receiving a response without a corresponding request
        parser_error(hconn, "Spurious HTTP response received");
        return false;
    }

    assert(!decoder->hrs);   // state machine violation
    decoder->hrs = hrs;

    char *saveptr = 0;
    char *version = strtok_r(line, LWS_CHARS, &saveptr);
    char *status_code = strtok_r(0, LWS_CHARS, &saveptr);
    char *reason = strtok_r(0, "", &saveptr);

    if (!version || !status_code) {
        parser_error(hconn, "Malformed response line");
        return false;
    }

    uint32_t minor = 0;
    if (strcmp(version, "HTTP/1.1") == 0) {
        minor = 1;
    } else if (strcmp(version, "HTTP/1.0") == 0) {
        minor = 0;
        decoder->is_http10 = true;
    } else {
        parser_error(hconn, "Unsupported HTTP/1.x version");
        return false;
    }

    char *eoc = 0;  // set to octet past status_code
    errno = 0;
    hrs->response_code = strtol(status_code, &eoc, 10);
    if (errno || hrs->response_code < 100 || hrs->response_code > 999 || *eoc != '\0') {
        // expect 3 digit decimal terminated by a null
        parser_error(hconn, "Bad response code");
        return false;
    }

    // The reason phrase is optional

    reason = !!reason ? trim_whitespace(reason) : "";

    int rc = hconn->config->rx_response(hconn, hrs->user_context, hrs->response_code, reason, 1, minor);
    if (rc) {
        parser_error(hconn, "rx_response callback failed");
        return false;
    }

    decoder_new_state(decoder, HTTP1_DECODE_HEADERS);
    return !!(*length);
}


////////////////////
// Header parsing //
////////////////////


// Called after the last incoming header was decoded and passed to the
// application.
//
// Determines if there is any content to the message. If there is no content we proceed directly to message_done
// otherwise transition to the content consume state.
//
// Returns true on success, false on error
//
static bool headers_done(qd_http1_decoder_connection_t *hconn, struct decoder_t *decoder)
{
    h1_decode_request_state_t *hrs = decoder->hrs;
    assert(hrs);

    int rc = hconn->config->rx_headers_done(hconn, hrs->user_context, decoder->is_client);
    if (rc) {
        parser_error(hconn, "rx_headers_done callback failed");
        return false;
    }

    // Determine if a body is present.  See RFC9112 Message Body Length

    if (!decoder->is_client) {  // parsing a response message
        // Cases 1 and 2:
        if ((hrs->head_request || NO_BODY_RESPONSE(hrs->response_code))
            || (hrs->connect_request && IS_SUCCESS_RESPONSE(hrs->response_code))
            ) {
            // No response body regardless of headers, fake zero content length:
            decoder->hdr_transfer_encoding = false;
            decoder->is_chunked = false;
            decoder->hdr_content_length = true;
            decoder->body_length = 0;
            return message_done(hconn, decoder);
        }
    }

    if (decoder->hdr_transfer_encoding) {
        // Case 3: Transfer-encoding invalidates Content-Length:
        decoder->hdr_content_length = 0;
        decoder->body_length = 0;

        // Case 4a: use chunked:
        if (decoder->is_chunked) {
            decoder_new_state(decoder, HTTP1_DECODE_CHUNK_HEADER);
            return true;

        } else if (!decoder->is_client) {
            // Case 4b: non-chunked response, body is terminated by connection close
            // Hack an infinite content length so we receive "forever"
            decoder->hdr_transfer_encoding = false;
            decoder->hdr_content_length = true;
            decoder->body_length = INT64_MAX;
            decoder_new_state(decoder, HTTP1_DECODE_BODY);
            return true;

        } else {
            // Case 4c: invalid request
            parser_error(hconn, "Unrecognized Transfer-Encoding");
            return false;
        }

    } else if (!decoder->hdr_content_length) {  // No Content-length nor Transfer-Encoding
        // Case 8: Response without explict length: body until connection closed:
        if (!decoder->is_client) {
            // Hack an infinite content length so we receive forever
            decoder->hdr_content_length = true;
            decoder->body_length = INT64_MAX;
            decoder_new_state(decoder, HTTP1_DECODE_BODY);
            return true;
        } else {
            // Case 7: assume zero length request body
            decoder->hdr_content_length = true;
            decoder->body_length = 0;
        }
    }

    // at this point we have elminated chunked and should be set to decode a fixed content length which may be zero
    // (Case 6):
    assert(decoder->hdr_content_length);
    if (decoder->body_length) {
        decoder_new_state(decoder, HTTP1_DECODE_BODY);
        return true;
    } else {
        return message_done(hconn, decoder);
    }
}


// Process a received header to determine message body length, etc.
// Returns false if parse error occurs.
//
static bool process_header(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, char *key, char *value)
{
    if (strlen(key) > strlen("Transfer-Encoding"))
        // this is not the key we are looking for...
        return true;

    if (strcasecmp("Content-Length", key) == 0) {
        int64_t old = decoder->body_length;
        char *eoc = 0;

        errno = 0;
        decoder->body_length = strtol(value, &eoc, 10);
        if (errno || eoc == value || decoder->body_length < 0) {
            parser_error(hconn, "Malformed Content-Length value");
            return false;
        }
        if (old && old != decoder->body_length) {
            parser_error(hconn, "Invalid duplicate Content-Length header");
            return false;
        }
        decoder->hdr_content_length = true;

    } else if (!decoder->is_http10 && strcasecmp("Transfer-Encoding", key) == 0) {
        decoder->hdr_transfer_encoding = true;
        char *saveptr = 0;
        char *token = strtok_r(value, " ,", &saveptr);
        while (token) {
            if (strcasecmp("chunked", token) == 0) {
                decoder->is_chunked = true;
                break;
            }
            token = strtok_r(0, " ,", &saveptr);
        }
    }

    return true;
}


// Parse an HTTP header line.
// See RFC7230 for details.  If header line folding (obs-folding) is detected,
// replace the folding with spaces.
//
static bool parse_header(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    char *line = read_line(decoder, data, length);
    if (!line)
        // need more data
        return false;

    size_t in_octets = strlen(line);
    char *eol = &line[in_octets];  // eol points to null terminator

    debug_print_line("header:", line);

    if (in_octets == 0) {
        // empty header == end of headers
        bool ok = headers_done(hconn, decoder);
        if (!ok)
            return false;
        return !!(*length);
    }

    // TODO: support obsolete line folding. For now I punt:
    if (*line == ' ' || *line == '\t')
        return !!(*length);

    // parse out key/value

    char *saveptr = 0;
    char *key = strtok_r(line, ":", &saveptr);

    if (!key) {
        parser_error(hconn, "Malformed header key");
        return false;
    }

    // According to RFC9112, the key is immediately followed by ':'. Value may start and end with whitespace which must
    // be removed before value can be processed.

    char *value = &key[strlen(key)];  // value points to null at end of key
    if (value < eol) {
        value++;  // skip to start of value
        value = trim_whitespace(value);
        truncate_whitespace(value);
    }

    assert(decoder->hrs);
    int rc = hconn->config->rx_header(hconn, decoder->hrs->user_context, decoder->is_client, key, value);
    if (rc) {
        parser_error(hconn, "rx_header callback failed");
        return false;
    }

    if (!process_header(hconn, decoder, key, value))
        return false;

    return !!(*length);
}


/////////////////
// Body Parser //
/////////////////


// Helper used by both content-length and chunked encoded bodies. Caller must check decoder->body_length to determine if
// all the expected data has been consumed (decoder->body_length == 0) and *length to see if any **data remains
//
static bool consume_body(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    size_t amount = MIN(*length, decoder->body_length);

    assert(decoder->hrs);
    if (amount && hconn->config->rx_body) {
        int rc = hconn->config->rx_body(hconn, decoder->hrs->user_context, decoder->is_client, *data, amount);
        if (rc) {
            parser_error(hconn, "rx_body callback failed");
            return false;
        }
    }

    *data += amount;
    *length -= amount;
    decoder->body_length -= amount;

    return true;
}


// parsing the start of a chunked header:
// <chunk size in hex> (chunk-ext) CRLF
// chunk-ext = *( BWS ";" BWS chunk-ext-name
//      [ BWS "=" BWS chunk-ext-val ] )*(OWS*(;))
//
static bool parse_chunk_header(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    char *line = read_line(decoder, data, length);
    if (!line)
        return false;

    char *eoc = 0;
    errno = 0;
    decoder->body_length = strtol(line, &eoc, 16);
    if (errno || eoc == line || decoder->body_length < 0) {
        parser_error(hconn, "Invalid chunk length");
        return false;
    }

    if (decoder->body_length)
        decoder_new_state(decoder, HTTP1_DECODE_CHUNK_DATA);
    else
        decoder_new_state(decoder, HTTP1_DECODE_CHUNK_TRAILER);

    return !!(*length);
}


// Parse the data section of a chunk
//
static bool parse_chunk_data(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    bool ok = consume_body(hconn, decoder, data, length);
    if (!ok)
        return false;

    if (decoder->body_length == 0) {  // end of chunk data
        // consume CRLF at end of body
        char *line = read_line(decoder, data, length);
        if (!line)
            return false;  // need more data

        if (*line) {
            // expected bare line, something is wrong
            parser_error(hconn, "Unexpected chunk body end");
            return false;
        }

        decoder_new_state(decoder, HTTP1_DECODE_CHUNK_HEADER);
    }

    return !!(*length);
}


// Keep reading chunk trailers until the terminating empty line is read
//
static bool parse_chunk_trailer(qd_http1_decoder_connection_t *hconn, decoder_t *decoder, const unsigned char **data, size_t *length)
{
    char *line = read_line(decoder, data, length);
    if (!line)
        return false;  // need more

    if (*line) {  // non empty line == chunk trailer

        // update incoming data with strlen(line) + 2;

    } else {  // end of trailers and message

        // update incoming data with +2

        bool ok = message_done(hconn, decoder);
        if (!ok)
            return false;
    }

    return !!(*length);
}


// Parse a message body using content-length
//
static bool parse_body(qd_http1_decoder_connection_t *hconn, struct decoder_t *decoder, const unsigned char **data, size_t *length)
{
    bool ok = consume_body(hconn, decoder, data, length);
    if (!ok)
        return false;

    if (decoder->body_length == 0) {  // no more body
        bool ok = message_done(hconn, decoder);
        if (!ok)
            return false;
    }

    return !!(*length);
}


// Push inbound network data into the http1 protocol engine.
//
// This is the main decode loop.  All callbacks take place in the context of this call.
//
//
int qd_http1_decoder_connection_rx_data(qd_http1_decoder_connection_t *hconn, bool from_client, const unsigned char *data, size_t length)
{
    bool more = true;

    if (hconn->parse_error) {
        return -1;
    }

    struct decoder_t *decoder = from_client ? &hconn->client : &hconn->server;
    while (more) {
#if DEBUG_DECODER
        fprintf(stdout, "hconn: %p State: %s data length=%zu\n", (void *) hconn, decoder_state[decoder->state], length);
#endif
        switch (decoder->state) {
            case HTTP1_DECODE_REQUEST:
                more = parse_request_line(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_RESPONSE:
                more = parse_response_line(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_HEADERS:
                more = parse_header(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_BODY:
                more = parse_body(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_CHUNK_HEADER:
                more = parse_chunk_header(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_CHUNK_DATA:
                more = parse_chunk_data(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_CHUNK_TRAILER:
                more = parse_chunk_trailer(hconn, decoder, &data, &length);
                break;

            case HTTP1_DECODE_ERROR:
            case HTTP1_INVALID_STATE:
                more = false;
                break;
        }
    }

    return !!hconn->parse_error ? -1 : 0;
}

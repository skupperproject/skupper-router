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
#include "qpid/dispatch/hash.h"
#include "qpid/dispatch/connection_counters.h"
#include "qpid/dispatch/ctools.h"
#include "observers/private.h"
#include "decoders/http2/http2_decoder.h"

const char *HTTP_METHOD = ":method";
const char *HTTP_STATUS = ":status";
const char *X_FORWARDED_FOR = "x-forwarded-for";

ALLOC_DEFINE(qd_http2_stream_info_t);

/**
 * Inserts the passed in stream_info object into the stream_id hash table.
 */
static qd_error_t insert_stream_info_into_hashtable(qdpo_transport_handle_t *transport_handle, qd_http2_stream_info_t *stream_info, uint32_t stream_id)
{
    qd_hash_t  *stream_id_hash = transport_handle->http2.stream_id_hash;
    // Convert stream_id to string
    char stream_id_str[11];
    snprintf(stream_id_str, sizeof stream_id_str, "%" PRIu32, stream_id);
    return qd_hash_insert_str(stream_id_hash, (const unsigned char *)stream_id_str, stream_info, 0);
}

/**
 * Gets the stream_info object from the hashtable using the passed in stream_id as the key.
 */
static qd_error_t get_stream_info_from_hashtable(qdpo_transport_handle_t *transport_handle, qd_http2_stream_info_t **stream_info, uint32_t stream_id)
{
    qd_hash_t  *stream_id_hash = transport_handle->http2.stream_id_hash;
    // Convert stream_id to string
    char stream_id_str[11];
    snprintf(stream_id_str, sizeof stream_id_str, "%" PRIu32, stream_id);
    return qd_hash_retrieve_str(stream_id_hash, (const unsigned char *)stream_id_str, (void **)stream_info);
}

/**
 * Delete the stream_info object from the hashtable whose key is the passed in stream_id
 */
static qd_error_t delete_stream_info_from_hashtable(qdpo_transport_handle_t *transport_handle, uint32_t stream_id)
{
    qd_hash_t  *stream_id_hash = transport_handle->http2.stream_id_hash;
    // Convert stream_id to string
    char stream_id_str[11];
    snprintf(stream_id_str, sizeof stream_id_str, "%" PRIu32, stream_id);
    return qd_hash_remove_str(stream_id_hash, (const unsigned char *)stream_id_str);
}

/*
 * HTTP2 decoder callbacks
 */
static void on_decoder_error(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client, const char *reason)
{
    qdpo_transport_handle_t *transport_handle = (qdpo_transport_handle_t *) qd_http2_decoder_connection_get_context(conn_state);
    // An error code will be returned to the http1_observe() call and this observer will be cleaned up there. Look at the http2_observe() function below.
    qd_log(LOG_HTTP2_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/2 observer protocol error: %s", transport_handle->conn_id, reason);
}

int on_end_headers_callback(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client,
                             uint32_t stream_id,
                             bool end_stream)
{
    qdpo_transport_handle_t *transport_handle = (qdpo_transport_handle_t *) qd_http2_decoder_connection_get_context(conn_state);
    qd_http2_stream_info_t *stream_info = 0;
    qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
    if (error == QD_ERROR_NOT_FOUND) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_end_headers_callback - could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
    } else {
        if (!from_client) {
            if (end_stream) {
                vflow_end_record(stream_info->vflow);
                qd_error_t error = delete_stream_info_from_hashtable(transport_handle, stream_id);
                if (error == QD_ERROR_NOT_FOUND) {
                    qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_header_recv_callback - could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
                }
                DEQ_REMOVE(transport_handle->http2.streams, stream_info);
                free_qd_http2_stream_info_t(stream_info);
            }
        }
    }

    return 0;
}

/**
 * This callback is called once on each HEADER frame.
 * It might be called more than once per request since GRPC can contain a
 * footer frame that is also a HEADER frame.
 */
int on_begin_header_callback(qd_http2_decoder_connection_t *conn_state,
                             uintptr_t request_context,
                             bool from_client,
                             uint32_t stream_id)
{
    qdpo_transport_handle_t *transport_handle = (qdpo_transport_handle_t *) qd_http2_decoder_connection_get_context(conn_state);
    qd_http2_stream_info_t *stream_info = 0;
    qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
    if (from_client) {
        // HTTP2 can have more than one header frame come in for the same request.
        // For example, in GRPC, we can get a header at the beginning of the stream
        // and a footer at the end of the stream and both these frames are HEADER type frames.
        if (error == QD_ERROR_NOT_FOUND) {
            qd_http2_stream_info_t *stream_info = new_qd_http2_stream_info_t();
            ZERO(stream_info);
            DEQ_INSERT_TAIL(transport_handle->http2.streams, stream_info);
            // This is the first header frame in a particular stream.
            stream_info->vflow = vflow_start_record(VFLOW_RECORD_BIFLOW_APP, transport_handle->vflow);
            vflow_set_string(stream_info->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "HTTP/2");
            vflow_set_uint64(stream_info->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
            stream_info->stream_id = stream_id;
            vflow_set_uint64(stream_info->vflow, VFLOW_ATTRIBUTE_STREAM_ID, stream_info->stream_id);
            vflow_latency_start(stream_info->vflow);
            qd_error_t error = insert_stream_info_into_hashtable(transport_handle, stream_info, stream_id);
            if (error == QD_ERROR_ALREADY_EXISTS) {
                qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_begin_header_callback - already exists in hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
            }
        } else { // !stream_info
            //
            // There is already a stream info object for this stream_id, this header might be a footer(GRPC),
            // nothing to do here
            //
        }

    } else { // from_client
        if (error != QD_ERROR_NOT_FOUND && stream_info) {
            vflow_latency_end(stream_info->vflow, VFLOW_ATTRIBUTE_LATENCY);
        }
    }
    return 0;
}

/**
 * This callback is called once on each HTTP2 DATA frame.
 * num_bytes is the number of the bytes that the data frame contains.
 */
static int on_data_recv_callback(qd_http2_decoder_connection_t *conn_state,
                                 uintptr_t request_context,
                                 bool from_client,
                                 uint32_t stream_id,
                                 bool end_stream,
                                 uint32_t num_bytes)
{
    qd_http2_stream_info_t *stream_info = 0;
    qdpo_transport_handle_t *transport_handle = (qdpo_transport_handle_t *) qd_http2_decoder_connection_get_context(conn_state);
    qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] on_data_recv_callback from_client=%i, end_stream=%i, num_bytes=%"PRIu32" , stream_id=%" PRIu32, transport_handle->conn_id, from_client, end_stream, num_bytes, stream_id);
    qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
    if (error == QD_ERROR_NOT_FOUND) {
        qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_data_recv_callback - could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
    }
    else {
        if (from_client) {
            stream_info->bytes_in += num_bytes;
            vflow_set_uint64(stream_info->vflow, VFLOW_ATTRIBUTE_OCTETS, stream_info->bytes_in);
        } else {
            stream_info->bytes_out += num_bytes;
            vflow_set_uint64(stream_info->vflow, VFLOW_ATTRIBUTE_OCTETS_REVERSE, stream_info->bytes_out);
            if (end_stream) {
                vflow_end_record(stream_info->vflow);
                qd_error_t error = delete_stream_info_from_hashtable(transport_handle, stream_id);
                if (error == QD_ERROR_NOT_FOUND) {
                    qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_header_recv_callback - could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
                }
                DEQ_REMOVE(transport_handle->http2.streams, stream_info);
                free_qd_http2_stream_info_t(stream_info);
            }
        }
    }
    return 0;
}

/**
 * This callback is called for every single header.
 * We only care about the http method and the response status code.
 */
static int on_header_recv_callback(qd_http2_decoder_connection_t *conn_state,
                                   uintptr_t request_context,
                                   bool from_client,
                                   uint32_t stream_id,
                                   const uint8_t *name,
                                   size_t namelen,
                                   const uint8_t *value,
                                   size_t valuelen)
{

    qd_http2_stream_info_t *stream_info = 0;
    qdpo_transport_handle_t *transport_handle = (qdpo_transport_handle_t *) qd_http2_decoder_connection_get_context(conn_state);

    if (strcmp(HTTP_METHOD, (const char *)name) == 0) {
        // Set the http method (GET, POST, PUT, DELETE etc) on the stream's vflow object.
        qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] on_header_recv_callback - HTTP_METHOD=%s, stream_id=%" PRIu32, transport_handle->conn_id, (const char *)value, stream_id);
        qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
        if (error == QD_ERROR_NOT_FOUND) {
            qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] set_stream_vflow_attribute could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
        }
        else {
            vflow_set_string(stream_info->vflow, VFLOW_ATTRIBUTE_METHOD, (const char *)value);
        }
    } else if (strcmp(HTTP_STATUS, (const char *)name) == 0) {
        qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
        //
        // We expect that the above call to get_stream_info_from_hashtable() should return a valid stream_info object.
        //
        if (error == QD_ERROR_NOT_FOUND) {
            qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] on_header_recv_callback - HTTP_STATUS -could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
        }
        else {
            char *status_code_int = 0;  // set to octet past status_code
            int status_code = strtol((const char *)value, &status_code_int, 10);
            if (status_code / 100 != 1) {  // terminal response code
                char status_code_str[16];
                snprintf(status_code_str, sizeof(status_code_str), "%d", status_code);
                //
                // Set the http response status (200, 404 etc) on the stream's vflow object.
                //
                qd_log(LOG_HTTP2_DECODER, QD_LOG_DEBUG, "[C%"PRIu64"] on_header_recv_callback - HTTP_STATUS=%s, stream_id=%" PRIu32, transport_handle->conn_id, (const char *)value, stream_id);
                vflow_set_string(stream_info->vflow, VFLOW_ATTRIBUTE_RESULT, status_code_str);
            }
        }
    } else if (strcmp(X_FORWARDED_FOR, (const char *)name) == 0) {
        qd_error_t error = get_stream_info_from_hashtable(transport_handle, &stream_info, stream_id);
        if (error == QD_ERROR_NOT_FOUND) {
            qd_log(LOG_HTTP2_DECODER, QD_LOG_ERROR, "[C%"PRIu64"] set_stream_vflow_attribute could not find in the hashtable, stream_id=%" PRIu32, transport_handle->conn_id, stream_id);
        }
        else {
            if (!stream_info->x_forwarded_for) {
                //
                // We will capture the very first x-forwarded-for header and ignore the other x-forwarded for headers in the same request.
                // Say a request has the following three x-forwarded-for headers
                //
                // X-Forwarded-For: 2001:db8:85a3:8d3:1319:8a2e:370:7348, 197.1.773.201
                // X-Forwarded-For: 195.0.223.001
                // X-Forwarded-For: 203.0.113.195, 2007:db5:85a3:8d3:1319:8a2e:370:3221
                //
                // We will obtain the value of the first X-Forwarded-For (2001:db8:85a3:8d3:1319:8a2e:370:7348, 197.1.773.201) and ignore the
                // other two X-Forwarded-For headers.
                // The first X-Forwarded-For header is comma separated list, we will obtain the leftmost (the first) value (2001:db8:85a3:8d3:1319:8a2e:370:7348) in the list

                // const uint8_t *value passed into this function is guaranteed to be NULL-terminated
                char value_copy[valuelen+1];
                strcpy(value_copy, (const char *)value);
                // Get the first token (left-most)
                char *first_token = strtok(value_copy, ",");
                vflow_set_string(stream_info->vflow, VFLOW_ATTRIBUTE_SOURCE_HOST, first_token);
                stream_info->x_forwarded_for = true;
            }
        }
    }
    return 0;
}

/**
 * Pass these callbacks to the decoder so it can call these at the appropriate time when processing the http2 stream.
 */
static qd_http2_decoder_callbacks_t callbacks = {
    .on_header = on_header_recv_callback,
    .on_data = on_data_recv_callback,
    .on_begin_header = on_begin_header_callback,
    .on_end_headers = on_end_headers_callback,
    .on_decode_error = on_decoder_error
};


void http2_observe(qdpo_transport_handle_t *transport_handle, bool from_client, const unsigned char *data, size_t length)
{
    qd_log(LOG_HTTP2_OBSERVER, QD_LOG_DEBUG,
           "[C%" PRIu64 "] HTTP/2.0 observer classifying protocol: %zu %s octets", transport_handle->conn_id, length, from_client ? "client" : "server");

    int rc = decode(transport_handle->http2.conn_state, from_client, data, length);

    if (rc) {
        qdpo_http2_final(transport_handle);
    }
}

void qdpo_http2_init(qdpo_transport_handle_t *transport_handle)
{
    qd_log(LOG_HTTP2_OBSERVER, QD_LOG_DEBUG,  "[C%" PRIu64 "] HTTP/2.0 observer initialized", transport_handle->conn_id);

    transport_handle->protocol = QD_PROTOCOL_HTTP2;
    transport_handle->observe = http2_observe;

    memset(&transport_handle->http2, 0, sizeof(transport_handle->http2));
    transport_handle->http2.stream_id_hash = qd_hash(12, 32, 0);
    DEQ_INIT(transport_handle->http2.streams);
    transport_handle->http2.conn_state = qd_http2_decoder_connection(&callbacks, (uintptr_t) transport_handle, transport_handle->conn_id);
}

void qdpo_http2_final(qdpo_transport_handle_t *transport_handle)
{
    qd_log(LOG_HTTP2_OBSERVER, QD_LOG_DEBUG, "[C%" PRIu64 "] HTTP/2.0 observer finalized", transport_handle->conn_id);

    if (transport_handle->observe) {
        transport_handle->observe = 0;
        qd_http2_decoder_connection_free(transport_handle->http2.conn_state);
    }
    if (transport_handle->http2.stream_id_hash) {
        qd_hash_free(transport_handle->http2.stream_id_hash);
        transport_handle->http2.stream_id_hash = 0;
    }
    qd_http2_stream_info_t *stream_info = DEQ_HEAD(transport_handle->http2.streams);
    while (stream_info) {
        DEQ_REMOVE_HEAD(transport_handle->http2.streams);
        vflow_end_record(stream_info->vflow);
        free_qd_http2_stream_info_t(stream_info);
        stream_info = DEQ_HEAD(transport_handle->http2.streams);
    }
}


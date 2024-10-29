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

#include "message_private.h"
#include "test_case.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/iterator.h"

#include <proton/message.h>
#include <proton/raw_connection.h>

#include <stdio.h>
#include <string.h>

#define FLAT_BUF_SIZE (100000)
static unsigned char buffer[FLAT_BUF_SIZE];

// given a buffer list, copy the data it contains into a single contiguous
// buffer.
static size_t flatten_bufs(qd_message_content_t *content)
{
    unsigned char *cursor = buffer;
    qd_buffer_t *buf      = DEQ_HEAD(content->buffers);

    while (buf) {
        // if this asserts you need a bigger buffer!
        assert(((size_t) (cursor - buffer)) + qd_buffer_size(buf) < FLAT_BUF_SIZE);
        memcpy(cursor, qd_buffer_base(buf), qd_buffer_size(buf));
        cursor += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    return (size_t) (cursor - buffer);
}


static void set_content(qd_message_content_t *content, unsigned char *buffer, size_t len)
{
    qd_buffer_list_t blist = DEQ_EMPTY;

    qd_buffer_list_append(&blist, buffer, len);
    DEQ_APPEND(content->buffers, blist);
    SET_ATOMIC_FLAG(&content->receive_complete);
}


static void set_content_bufs(qd_message_content_t *content, int nbufs)
{
    for (; nbufs > 0; nbufs--) {
        qd_buffer_t *buf = qd_buffer();
        size_t segment   = qd_buffer_capacity(buf);
        qd_buffer_insert(buf, segment);
        DEQ_INSERT_TAIL(content->buffers, buf);
    }
}


static char* test_send_to_messenger(void *context)
{
    qd_message_t         *msg     = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);
    qd_message_compose_1(msg, "test_addr_0", 0);
    qd_buffer_t *buf = DEQ_HEAD(content->buffers);
    if (buf == 0) {
        qd_message_free(msg);
        return "Expected a buffer in the test message";
    }

    pn_message_t *pn_msg = pn_message();
    size_t len = flatten_bufs(content);
    int result = pn_message_decode(pn_msg, (char *)buffer, len);
    if (result != 0) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Error in pn_message_decode";
    }

    if (strcmp(pn_message_get_address(pn_msg), "test_addr_0") != 0) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Address mismatch in received message";
    }

    pn_message_free(pn_msg);
    qd_message_free(msg);

    return 0;
}


static char* test_receive_from_messenger(void *context)
{
    pn_message_t *pn_msg = pn_message();
    pn_message_set_address(pn_msg, "test_addr_1");

    size_t       size = 10000;
    int result = pn_message_encode(pn_msg, (char *)buffer, &size);
    if (result != 0) {
        pn_message_free(pn_msg);
        return "Error in pn_message_encode";
    }

    qd_message_t         *msg     = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);

    set_content(content, buffer, size);

    if (qd_message_check_depth(msg, QD_DEPTH_ALL) != QD_MESSAGE_DEPTH_OK) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "qd_message_check_depth returns 'invalid'";
    }

    qd_iterator_t *iter = qd_message_field_iterator(msg, QD_FIELD_TO);
    if (iter == 0) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Expected an iterator for the 'to' field";
    }

    if (!qd_iterator_equal(iter, (unsigned char*) "test_addr_1")) {
        qd_iterator_free(iter);
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Mismatched 'to' field contents";
    }
    qd_iterator_free(iter);

    ssize_t  test_len = (size_t)qd_message_field_length(msg, QD_FIELD_TO);
    if (test_len != 11) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Incorrect field length";
    }

    char test_field[100];
    size_t hdr_length;
    test_len = qd_message_field_copy(msg, QD_FIELD_TO, test_field, &hdr_length);
    if (test_len - hdr_length != 11) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Incorrect length returned from field_copy";
    }

    if (test_len < 0) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "test_len cannot be less than zero";
    }
    test_field[test_len] = '\0';
    if (strcmp(test_field + hdr_length, "test_addr_1") != 0) {
        pn_message_free(pn_msg);
        qd_message_free(msg);
        return "Incorrect field content returned from field_copy";
    }

    pn_message_free(pn_msg);
    qd_message_free(msg);

    return 0;
}


// load a few interesting message properties and validate
static char* test_message_properties(void *context)
{
    pn_atom_t id = {.type = PN_STRING,
                    .u.as_bytes.start = "messageId",
                    .u.as_bytes.size = 9};
    pn_atom_t cid = {.type = PN_STRING,
                     .u.as_bytes.start = "correlationId",
                     .u.as_bytes.size = 13};
    const char *subject = "A Subject";
    pn_message_t *pn_msg = pn_message();
    pn_message_set_id(pn_msg, id);
    pn_message_set_subject(pn_msg, subject);
    pn_message_set_correlation_id(pn_msg, cid);

    size_t       size = 10000;
    int result = pn_message_encode(pn_msg, (char *)buffer, &size);
    pn_message_free(pn_msg);

    if (result != 0) return "Error in pn_message_encode";

    qd_message_t         *msg     = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);

    set_content(content, buffer, size);

    qd_iterator_t *iter = qd_message_field_iterator(msg, QD_FIELD_CORRELATION_ID);
    if (!iter) {
        qd_message_free(msg);
        return "Expected iterator for the 'correlation-id' field";
    }
    if (qd_iterator_length(iter) != 13) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Bad length for correlation-id";
    }
    if (!qd_iterator_equal(iter, (const unsigned char *)"correlationId")) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Invalid correlation-id";
    }
    qd_iterator_free(iter);

    iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
    if (!iter) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Expected iterator for the 'subject' field";
    }
    if (!qd_iterator_equal(iter, (const unsigned char *)subject)) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Bad value for subject";
    }
    qd_iterator_free(iter);

    iter = qd_message_field_iterator(msg, QD_FIELD_MESSAGE_ID);
    if (!iter) {
        qd_message_free(msg);
        return "Expected iterator for the 'message-id' field";
    }
    if (qd_iterator_length(iter) != 9) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Bad length for message-id";
    }
    if (!qd_iterator_equal(iter, (const unsigned char *)"messageId")) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Invalid message-id";
    }
    qd_iterator_free(iter);

    iter = qd_message_field_iterator(msg, QD_FIELD_TO);
    if (iter) {
        qd_iterator_free(iter);
        qd_message_free(msg);
        return "Expected no iterator for the 'to' field";
    }
    qd_iterator_free(iter);

    qd_message_free(msg);

    return 0;
}


// run qd_message_check_depth against different legal AMQP message
//
static char* _check_all_depths(qd_message_t *msg)
{
    static const qd_message_depth_t depths[] = {
        // yep: purposely out of order
        QD_DEPTH_MESSAGE_ANNOTATIONS,
        QD_DEPTH_DELIVERY_ANNOTATIONS,
        QD_DEPTH_PROPERTIES,
        QD_DEPTH_HEADER,
        QD_DEPTH_APPLICATION_PROPERTIES,
        QD_DEPTH_BODY
    };
    static const int n_depths = 6;

    static char err[1024];

    for (int i = 0; i < n_depths; ++i) {
        if (qd_message_check_depth(msg, depths[i]) != QD_MESSAGE_DEPTH_OK) {
            snprintf(err, 1023,
                     "qd_message_check_depth returned 'invalid' for section 0x%X", (unsigned int)depths[i]);
            err[1023] = 0;
            return err;
        }
    }
    return 0;
}


static char* test_check_multiple(void *context)
{
    // case 1: a minimal encoded message
    //
    pn_message_t *pn_msg = pn_message();

    size_t size = 10000;
    int result = pn_message_encode(pn_msg, (char *)buffer, &size);
    pn_message_free(pn_msg);
    if (result != 0) return "Error in pn_message_encode";

    qd_message_t         *msg     = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);

    set_content(content, buffer, size);
    char *rc = _check_all_depths(msg);
    qd_message_free(msg);
    if (rc) return rc;

    // case 2: minimal, with address field in header
    //
    pn_msg = pn_message();
    pn_message_set_address(pn_msg, "test_addr_2");
    size = 10000;
    result = pn_message_encode(pn_msg, (char *)buffer, &size);
    pn_message_free(pn_msg);
    if (result != 0) return "Error in pn_message_encode";
    msg = qd_message();
    set_content(MSG_CONTENT(msg), buffer, size);
    rc = _check_all_depths(msg);
    qd_message_free(msg);
    if (rc) return rc;

    // case 3: null body
    //
    pn_msg = pn_message();
    pn_data_t *body = pn_message_body(pn_msg);
    pn_data_put_null(body);
    size = 10000;
    result = pn_message_encode(pn_msg, (char *)buffer, &size);
    pn_message_free(pn_msg);
    if (result != 0) return "Error in pn_message_encode";
    msg = qd_message();
    set_content(MSG_CONTENT(msg), buffer, size);
    rc = _check_all_depths(msg);
    qd_message_free(msg);
    if (rc) return rc;

    // case 4: minimal legal AMQP 1.0 message (as defined by the standard)
    // A single body field with a null value
    const unsigned char null_body[] = {0x00, 0x53, 0x77, 0x40};
    size = sizeof(null_body);
    memcpy(buffer, null_body, size);
    msg = qd_message();
    set_content(MSG_CONTENT(msg), buffer, size);
    rc = _check_all_depths(msg);
    qd_message_free(msg);
    return rc;
}


// Generate a test message with the given router annotations data
//
static qd_message_t *generate_ra_test_message(unsigned int flags,
                                              const char *to_override,
                                              const char *ingress_router,
                                              const char *trace[])
{
    // create a test message using proton
    qd_buffer_list_t pton_buffers = DEQ_EMPTY;
    pn_message_t *pn_msg = pn_message();
    pn_message_set_durable(pn_msg, true);
    pn_message_set_address(pn_msg, "test_addr_0");
    pn_data_t *pn_ma = pn_message_annotations(pn_msg);
    pn_data_clear(pn_ma);
    pn_data_put_map(pn_ma);
    pn_data_enter(pn_ma);
    pn_data_put_symbol(pn_ma, pn_bytes(strlen("ma-key1"), "ma-key1"));
    pn_data_put_string(pn_ma, pn_bytes(strlen("distress"), "distress"));
    pn_data_put_symbol(pn_ma, pn_bytes(strlen("ma-key2"), "ma-key2"));
    pn_data_put_int(pn_ma, 1);
    pn_data_exit(pn_ma);

    pn_data_t *body = pn_message_body(pn_msg);
    pn_data_put_list(body);
    pn_data_enter(body);
    pn_data_put_long(body, 1);
    pn_data_put_long(body, 2);
    pn_data_put_long(body, 3);
    pn_data_exit(body);

    size_t len = FLAT_BUF_SIZE;
    pn_message_encode(pn_msg, (char*) buffer, &len);
    qd_buffer_list_append(&pton_buffers, buffer, len);
    pn_message_free(pn_msg);

    // generate the RA
    qd_buffer_list_t ra_buffers = DEQ_EMPTY;
    qd_composed_field_t *ra = qd_compose(QD_PERFORMATIVE_ROUTER_ANNOTATIONS, 0);
    qd_compose_start_list(ra);

    qd_compose_insert_uint(ra, flags);
    if (to_override)
        qd_compose_insert_string(ra, to_override);
    else
        qd_compose_insert_null(ra);
    if (ingress_router)
        qd_compose_insert_string(ra, ingress_router);
    else
        qd_compose_insert_null(ra);
    if (!trace)
        qd_compose_empty_list(ra);
    else {
        qd_compose_start_list(ra);
        for (int i = 0; trace[i]; ++i) {
            qd_compose_insert_string(ra, trace[i]);
        }
        qd_compose_end_list(ra);
    }
    qd_compose_end_list(ra);
    qd_compose_take_buffers(ra, &ra_buffers);
    qd_compose_free(ra);

    qd_message_t         *msg     = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);
    DEQ_APPEND(content->buffers, ra_buffers);
    DEQ_APPEND(content->buffers, pton_buffers);
    SET_ATOMIC_FLAG(&content->receive_complete);
    return msg;
}


// Create a message containing router-specific annotations.
// Ensure the annotations are properly parsed.
//
static char* test_parse_router_annotations(void *context)
{
    char *error = 0;
    qd_buffer_list_t blist = DEQ_EMPTY;

    // Test: empty RA
    qd_message_t *msg = generate_ra_test_message(0, 0, 0, 0);
    error = (char*) qd_message_parse_router_annotations(msg);
    if (error) {
        goto exit;
    }

    // validate sections parsed correctly:

    if (((qd_message_pvt_t*) msg)->ra_flags != 0) {
        error = "Test0: Invalid RA flags";
        goto exit;
    }

    if (qd_message_is_streaming(msg)) {
        error = "Test0: streaming not expected";
        goto exit;
    }

    qd_parsed_field_t *pf_trace = qd_message_get_trace(msg);
    if (!pf_trace) {
        error = "Test0: trace not found!";
        goto exit;
    }
    if (qd_parse_sub_count(pf_trace) != 0) {
        error = "Test0: trace list not empty";
        goto exit;
    }

    if (qd_message_get_to_override(msg) != 0) {
        error = "Test0: expected no to override";
        goto exit;
    }

    if (qd_message_get_ingress_router(msg)) {
        error = "Test0: expected no ingress!";
        goto exit;
    }

    // set values and compose:

    qd_message_set_streaming_annotation(msg);
    qd_message_set_to_override_annotation(msg, "to/override");

    uint32_t len = _compose_router_annotations((qd_message_pvt_t*) msg, QD_MESSAGE_RA_STRIP_NONE, &blist);
    if (len == 0) {
        error = "Test1: failed to compose 1";
        goto exit;
    }

    qd_message_free(msg);

    msg = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);
    DEQ_APPEND(content->buffers, blist);
    SET_ATOMIC_FLAG(&content->receive_complete);

    // parse updated values

    error = (char*) qd_message_parse_router_annotations(msg);
    if (error) {
        goto exit;
    }

    // validate

    if (!qd_message_is_streaming(msg)) {
        error = "Test1: streaming expected";
        goto exit;
    }

    qd_parsed_field_t *pf_to = qd_message_get_to_override(msg);
    if (!pf_to || !qd_iterator_equal(qd_parse_raw(pf_to), (const unsigned char*) "to/override")) {
        error = "Test1: to override not found!";
        goto exit;
    }

    // expected _compose_router_annotations to update trace an ingress properly

    qd_parsed_field_t *pf_ingress = qd_message_get_ingress_router(msg);
    if (!pf_ingress || !qd_iterator_equal(qd_parse_raw(pf_ingress), (const unsigned char*) "0/UnitTestRouter")) {
        error = "Test1: ingress router not found!";
        goto exit;
    }

    pf_trace = qd_message_get_trace(msg);
    if (!pf_trace || qd_parse_sub_count(pf_trace) != 1) {
        error = "Test1: trace list not found!";
        goto exit;
    }
    pf_trace = qd_parse_sub_value(pf_trace, 0);
    if (!pf_trace || !qd_iterator_equal(qd_parse_raw(pf_trace), (const unsigned char*) "0/UnitTestRouter")) {
        error = "Test1: invalid trace list";
        goto exit;
    }

    qd_message_free(msg);

    // Test: populated RA
    const char *dummy_trace[] = {
        "0/OneReallyVeryLongRouterId",
        "0/AnotherShorterRouterId",
        0
    };
    msg = generate_ra_test_message(MSG_FLAG_STREAMING,
                                   "to/override/address",
                                   "0/AnIngressRouter",
                                   dummy_trace);
    error = (char*) qd_message_parse_router_annotations(msg);
    if (error) {
        goto exit;
    }

    if (!qd_message_is_streaming(msg)) {
        error = "Test2: streaming expected";
        goto exit;
    }
    pf_to = qd_message_get_to_override(msg);
    if (!pf_to || !qd_iterator_equal(qd_parse_raw(pf_to), (const unsigned char*) "to/override/address")) {
        error = "Test2: invalid to override!";
        goto exit;
    }
    pf_ingress = qd_message_get_ingress_router(msg);
    if (!pf_ingress || !qd_iterator_equal(qd_parse_raw(pf_ingress), (const unsigned char*) "0/AnIngressRouter")) {
        error = "Test2: invalid ingress router!";
        goto exit;
    }
    pf_trace = qd_message_get_trace(msg);
    if (!pf_trace || qd_parse_sub_count(pf_trace) != 2) {
        error = "Test2: invalid trace list!";
        goto exit;
    }
    if (!qd_iterator_equal(qd_parse_raw(qd_parse_sub_value(pf_trace, 0)), (const unsigned char*) dummy_trace[0])) {
        error = "Test2: invalid trace list index 0";
        goto exit;
    }
    if (!qd_iterator_equal(qd_parse_raw(qd_parse_sub_value(pf_trace, 1)), (const unsigned char*) dummy_trace[1])) {
        error = "Test2: invalid trace list index 1";
        goto exit;
    }

    // re-compose & parse, check trace list for local router id
    len = _compose_router_annotations((qd_message_pvt_t*) msg, QD_MESSAGE_RA_STRIP_NONE, &blist);
    if (len == 0) {
        error = "Test3: failed to compose 2";
        goto exit;
    }
    qd_message_free(msg);
    msg = qd_message();
    content = MSG_CONTENT(msg);
    DEQ_APPEND(content->buffers, blist);
    SET_ATOMIC_FLAG(&content->receive_complete);
    error = (char*) qd_message_parse_router_annotations(msg);
    if (error) {
        goto exit;
    }
    pf_trace = qd_message_get_trace(msg);
    if (!pf_trace || qd_parse_sub_count(pf_trace) != 3) {
        error = "Test3: invalid trace list!";
        goto exit;
    }
    if (!qd_iterator_equal(qd_parse_raw(qd_parse_sub_value(pf_trace, 2)), (const unsigned char*) "0/UnitTestRouter")) {
        error = "Test2: invalid trace list index 1";
        goto exit;
    }


exit:

    qd_message_free(msg);
    return error;
}


static char* test_q2_input_holdoff_sensing(void *context)
{
    if (QD_QLIMIT_Q2_LOWER >= QD_QLIMIT_Q2_UPPER)
        return "QD_LIMIT_Q2 lower limit is bigger than upper limit";

    for (int nbufs=1; nbufs<QD_QLIMIT_Q2_UPPER + 1; nbufs++) {
        qd_message_t         *msg     = qd_message();
        qd_message_content_t *content = MSG_CONTENT(msg);
        sys_mutex_lock(&content->lock);

        set_content_bufs(content, nbufs);
        if (_Q2_holdoff_should_block_LH(content) != (nbufs >= QD_QLIMIT_Q2_UPPER)) {
            sys_mutex_unlock(&content->lock);
            qd_message_free(msg);
            return "qd_message_holdoff_would_block was miscalculated";
        }
        if (_Q2_holdoff_should_unblock_LH(content) != (nbufs < QD_QLIMIT_Q2_LOWER)) {
            sys_mutex_unlock(&content->lock);
            qd_message_free(msg);
            return "qd_message_holdoff_would_unblock was miscalculated";
        }

        sys_mutex_unlock(&content->lock);
        qd_message_free(msg);
    }
    return 0;
}


// verify that message check does not incorrectly validate a message section
// that has not been completely received.
//
static char *test_incomplete_annotations(void *context)
{
    const char big_string[] =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    char *result = 0;
    qd_message_t *msg = 0;
    pn_message_t *out_message = pn_message();

    pn_data_t *body = pn_message_body(out_message);
    pn_data_clear(body);
    pn_data_put_list(body);
    pn_data_enter(body);
    pn_data_put_long(body, 1);
    pn_data_put_long(body, 2);
    pn_data_put_long(body, 3);
    pn_data_exit(body);

    // Add a bunch 'o user message annotations
    pn_data_t *annos = pn_message_annotations(out_message);
    pn_data_clear(annos);
    pn_data_put_map(annos);
    pn_data_enter(annos);

    pn_data_put_symbol(annos, pn_bytes(strlen("my-key"), "my-key"));
    pn_data_put_string(annos, pn_bytes(strlen("my-data"), "my-data"));

    pn_data_put_symbol(annos, pn_bytes(strlen("my-other-key"), "my-other-key"));
    pn_data_put_string(annos, pn_bytes(strlen("my-other-data"), "my-other-data"));

    // embedded map
    pn_data_put_symbol(annos, pn_bytes(strlen("my-map"), "my-map"));
    pn_data_put_map(annos);
    pn_data_enter(annos);
    pn_data_put_symbol(annos, pn_bytes(strlen("my-map-key1"), "my-map-key1"));
    pn_data_put_char(annos, 'X');
    pn_data_put_symbol(annos, pn_bytes(strlen("my-map-key2"), "my-map-key2"));
    pn_data_put_byte(annos, 0x12);
    pn_data_put_symbol(annos, pn_bytes(strlen("my-map-key3"), "my-map-key3"));
    pn_data_put_string(annos, pn_bytes(strlen("Are We Not Men?"), "Are We Not Men?"));
    pn_data_put_symbol(annos, pn_bytes(strlen("my-last-key"), "my-last-key"));
    pn_data_put_binary(annos, pn_bytes(sizeof(big_string), big_string));
    pn_data_exit(annos);

    pn_data_put_symbol(annos, pn_bytes(strlen("my-ulong"), "my-ulong"));
    pn_data_put_ulong(annos, 0xDEADBEEFCAFEBEEF);

    // embedded list
    pn_data_put_symbol(annos, pn_bytes(strlen("my-list"), "my-list"));
    pn_data_put_list(annos);
    pn_data_enter(annos);
    pn_data_put_string(annos, pn_bytes(sizeof(big_string), big_string));
    pn_data_put_double(annos, 3.1415);
    pn_data_put_short(annos, 1966);
    pn_data_exit(annos);

    pn_data_put_symbol(annos, pn_bytes(strlen("my-bool"), "my-bool"));
    pn_data_put_bool(annos, false);

    pn_data_exit(annos);

    // now encode it

    size_t encode_len = sizeof(buffer);
    int rc = pn_message_encode(out_message, (char *)buffer, &encode_len);
    if (rc) {
        if (rc == PN_OVERFLOW)
            result = "Error: sizeof(buffer) in message_test.c too small - update it!";
        else
            result = "Error encoding message";
        goto exit;
    }

    assert(encode_len > 100);  // you broke the test!

    // Verify that the message check fails unless the entire annotations are
    // present.  First copy in only the first 100 bytes: enough for the MA
    // section header but not the whole section

    msg = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);
    set_content(content, buffer, 100);
    CLEAR_ATOMIC_FLAG(&content->receive_complete);   // more data coming!
    if (qd_message_check_depth(msg, QD_DEPTH_MESSAGE_ANNOTATIONS) != QD_MESSAGE_DEPTH_INCOMPLETE) {
        result = "Error: incomplete message was not detected!";
        goto exit;
    }

    // now complete the message
    set_content(content, &buffer[100], encode_len - 100);
    if (qd_message_check_depth(msg, QD_DEPTH_MESSAGE_ANNOTATIONS) != QD_MESSAGE_DEPTH_OK) {
        result = "Error: expected message to be valid!";
    }

exit:

    if (out_message) pn_message_free(out_message);
    if (msg) qd_message_free(msg);

    return result;
}


static char *test_check_weird_messages(void *context)
{
    char *result = 0;
    qd_message_t *msg = qd_message();

    // case 1:
    // delivery annotations with empty map
    unsigned char da_map[] = {0x00, 0x80,
                              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x71,
                              0xc1, 0x01, 0x00};
    // first test an incomplete pattern:
    set_content(MSG_CONTENT(msg), da_map, 4);
    CLEAR_ATOMIC_FLAG(&(MSG_CONTENT(msg)->receive_complete));
    qd_message_depth_status_t mc = qd_message_check_depth(msg, QD_DEPTH_DELIVERY_ANNOTATIONS);
    if (mc != QD_MESSAGE_DEPTH_INCOMPLETE) {
        result = "Expected INCOMPLETE status";
        goto exit;
    }

    // full pattern, but no tag
    set_content(MSG_CONTENT(msg), &da_map[4], 6);
    CLEAR_ATOMIC_FLAG(&(MSG_CONTENT(msg)->receive_complete));
    mc = qd_message_check_depth(msg, QD_DEPTH_DELIVERY_ANNOTATIONS);
    if (mc != QD_MESSAGE_DEPTH_INCOMPLETE) {
        result = "Expected INCOMPLETE status";
        goto exit;
    }

    // add tag, but incomplete field:
    set_content(MSG_CONTENT(msg), &da_map[10], 1);
    CLEAR_ATOMIC_FLAG(&(MSG_CONTENT(msg)->receive_complete));
    mc = qd_message_check_depth(msg, QD_DEPTH_DELIVERY_ANNOTATIONS);
    if (mc != QD_MESSAGE_DEPTH_INCOMPLETE) {
        result = "Expected INCOMPLETE status";
        goto exit;
    }

    // and finish up
    set_content(MSG_CONTENT(msg), &da_map[11], 2);
    mc = qd_message_check_depth(msg, QD_DEPTH_DELIVERY_ANNOTATIONS);
    if (mc != QD_MESSAGE_DEPTH_OK) {
        result = "Expected OK status";
        goto exit;
    }

    // case 2: negative test - detect invalid tag
    unsigned char bad_hdr[] = {0x00, 0x53, 0x70, 0xC1};  // 0xc1 == map, not list!
    qd_message_free(msg);
    msg = qd_message();
    set_content(MSG_CONTENT(msg), bad_hdr, sizeof(bad_hdr));
    CLEAR_ATOMIC_FLAG(&(MSG_CONTENT(msg)->receive_complete));
    mc = qd_message_check_depth(msg, QD_DEPTH_DELIVERY_ANNOTATIONS); // looking _past_ header!
    if (mc != QD_MESSAGE_DEPTH_INVALID) {
        result = "Bad tag not detected!";
        goto exit;
    }

    // case 3: check the valid body types
    unsigned char body_bin[] = {0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x75,
                                0xA0, 0x03, 0x00, 0x01, 0x02};
    qd_message_free(msg);
    msg = qd_message();
    set_content(MSG_CONTENT(msg), body_bin, sizeof(body_bin));
    mc = qd_message_check_depth(msg, QD_DEPTH_ALL); // looking _past_ header!
    if (mc != QD_MESSAGE_DEPTH_OK) {
        result = "Expected OK bin body";
        goto exit;
    }

    unsigned char body_seq[] = {0x00, 0x53, 0x76, 0x45};
    qd_message_free(msg);
    msg = qd_message();
    set_content(MSG_CONTENT(msg), body_seq, sizeof(body_seq));
    mc = qd_message_check_depth(msg, QD_DEPTH_BODY);
    if (mc != QD_MESSAGE_DEPTH_OK) {
        result = "Expected OK seq body";
        goto exit;
    }

    unsigned char body_value[] = {0x00, 0x53, 0x77, 0x51, 0x99};
    qd_message_free(msg);
    msg = qd_message();
    set_content(MSG_CONTENT(msg), body_value, sizeof(body_value));
    mc = qd_message_check_depth(msg, QD_DEPTH_BODY);
    if (mc != QD_MESSAGE_DEPTH_OK) {
        result = "Expected OK value body";
        goto exit;
    }

exit:
    qd_message_free(msg);
    return result;
}


// for testing Q2 unblock callback
static void q2_unblocked_handler(qd_alloc_safe_ptr_t context)
{
    int *iptr = (int*) context.ptr;
    (*iptr) += 1;
}


static char *test_q2_callback_on_disable(void *context)
{
    char *result = 0;
    qd_message_t *msg = 0;
    int unblock_called = 0;

    // first test: ensure calling disable without being in Q2 does not invoke the
    // handler:
    msg = qd_message();

    qd_alloc_safe_ptr_t unblock_arg = {0};
    unblock_arg.ptr = (void*) &unblock_called;
    qd_message_set_q2_unblocked_handler(msg, q2_unblocked_handler, unblock_arg);
    qd_message_Q2_holdoff_disable(msg);

    if (unblock_called != 0) {
        result = "Unexpected call to Q2 unblock handler!";
        goto exit;
    }

    qd_message_free(msg);

    // now try it again with a message with Q2 active

    msg = qd_message();

    unblock_arg.ptr = (void*) &unblock_called;
    qd_message_set_q2_unblocked_handler(msg, q2_unblocked_handler, unblock_arg);

    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(field);
    qd_compose_insert_bool(field, 0);     // durable
    qd_compose_insert_null(field);        // priority
    qd_compose_end_list(field);
    field = qd_compose(QD_PERFORMATIVE_PROPERTIES, field);
    qd_compose_start_list(field);
    qd_compose_insert_ulong(field, 666);    // message-id
    qd_compose_insert_null(field);                 // user-id
    qd_compose_insert_string(field, "/whereevah"); // to
    qd_compose_insert_string(field, "my-subject");  // subject
    qd_compose_insert_string(field, "/reply-to");   // reply-to
    qd_compose_end_list(field);

    qd_message_compose_2(msg, field, false);
    qd_compose_free(field);

    // grow message until Q2 activates

    bool blocked = false;
    uint8_t data[1000] = {0};
    while (!blocked) {
        qd_buffer_list_t bin_data = DEQ_EMPTY;
        qd_buffer_list_append(&bin_data, data, sizeof(data));
        field = qd_compose(QD_PERFORMATIVE_BODY_DATA, 0);
        qd_compose_insert_binary_buffers(field, &bin_data);
        qd_message_extend(msg, field, &blocked);
        qd_compose_free(field);
    }

    // now ensure callback is made

    qd_message_Q2_holdoff_disable(msg);

    if (unblock_called != 1) {
        result = "Failed to invoke unblock handler";
        goto exit;
    }


exit:
    qd_message_free(msg);
    return result;
}


// Ensure that the Q2 calculation does not include header buffers.  Header
// buffers are held until the message is freed, so they should not be a factor
// in flow control (DISPATCH-2191).
//
static char *test_q2_ignore_headers(void *context)
{
    char *result = 0;
    qd_message_t *msg = qd_message();
    qd_message_content_t *content = MSG_CONTENT(msg);

    // create a message and add a bunch of headers.  Put each header in its own
    // buffer to increase the buffer count.

    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(field);
    qd_compose_insert_bool(field, 0);     // durable
    qd_compose_insert_null(field);        // priority
    qd_compose_end_list(field);
    qd_buffer_list_t field_buffers;
    qd_compose_take_buffers(field, &field_buffers);
    qd_compose_free(field);
    content->buffers = field_buffers;

    const qd_amqp_performative_t plist[3] = {
        QD_PERFORMATIVE_DELIVERY_ANNOTATIONS,
        QD_PERFORMATIVE_MESSAGE_ANNOTATIONS,
        QD_PERFORMATIVE_APPLICATION_PROPERTIES};

    for (int i = 0; i < 3; ++i) {
        field = qd_compose(plist[i], 0);
        qd_compose_start_map(field);
        qd_compose_insert_symbol(field, "Key");
        qd_compose_insert_string(field, "Value");
        qd_compose_end_map(field);
        qd_compose_take_buffers(field, &field_buffers);
        qd_compose_free(field);
        DEQ_APPEND(content->buffers, field_buffers);
    }

    // validate the message - this will mark the buffers that contain header
    // data
    if (qd_message_check_depth(msg, QD_DEPTH_APPLICATION_PROPERTIES) != QD_MESSAGE_DEPTH_OK) {
        result = "Unexpected depth check failure";
        goto exit;
    }

    const size_t header_ct = DEQ_SIZE(content->buffers);
    assert(header_ct);
    sys_mutex_lock(&content->lock);
    assert(!_Q2_holdoff_should_block_LH(content));
    sys_mutex_unlock(&content->lock);

    sys_mutex_lock(&content->lock);
    // Now append buffers until Q2 blocks
    while (!_Q2_holdoff_should_block_LH(content)) {
        qd_buffer_t *buffy = qd_buffer();
        qd_buffer_insert(buffy, qd_buffer_capacity(buffy));
        DEQ_INSERT_TAIL(content->buffers, buffy);
    }
    sys_mutex_unlock(&content->lock);

    // expect: Q2 blocking activates when the non-header buffer count exceeds QD_QLIMIT_Q2_UPPER
    if (DEQ_SIZE(content->buffers) - header_ct < QD_QLIMIT_Q2_UPPER) {
        result = "Wrong buffer length for Q2 activate!";
        goto exit;
    }

    // now remove buffers until Q2 is relieved
    sys_mutex_lock(&content->lock);
    while (!_Q2_holdoff_should_unblock_LH(content)) {
        qd_buffer_t *buffy = DEQ_TAIL(content->buffers);
        DEQ_REMOVE_TAIL(content->buffers);
        qd_buffer_free(buffy);
    }
    sys_mutex_unlock(&content->lock);

    // expect: Q2 deactivates when the non-header buffer count falls below QD_QLIMIT_Q2_LOWER
    if (DEQ_SIZE(content->buffers) - header_ct > QD_QLIMIT_Q2_LOWER) {
        result = "Wrong buffer length for Q2 deactivate!";
        goto exit;
    }

exit:

    qd_message_free(msg);
    return result;
}


int message_tests(void)
{
    int result = 0;
    char *test_group = "message_tests";

    TEST_CASE(test_send_to_messenger, 0);
    TEST_CASE(test_receive_from_messenger, 0);
    TEST_CASE(test_message_properties, 0);
    TEST_CASE(test_check_multiple, 0);
    TEST_CASE(test_parse_router_annotations, 0);
    TEST_CASE(test_q2_input_holdoff_sensing, 0);
    TEST_CASE(test_incomplete_annotations, 0);
    TEST_CASE(test_check_weird_messages, 0);
    TEST_CASE(test_q2_callback_on_disable, 0);
    TEST_CASE(test_q2_ignore_headers, 0);

    return result;
}


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

#include "qpid/dispatch/parse.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"

#include "buffer_field_api.h"

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>

DEQ_DECLARE(qd_parsed_field_t, qd_parsed_field_list_t);


typedef struct qd_amqp_field_t {
    uint8_t           tag;
    uint32_t          size;   // includes length of count!
    uint32_t          count;
    qd_buffer_field_t value;  // the raw (encoded) value
} qd_amqp_field_t;


struct qd_parsed_field_t {
    DEQ_LINKS(qd_parsed_field_t);
    const qd_parsed_field_t *parent;
    qd_parsed_field_list_t   children;
    qd_iterator_t           *typed_iter;  // iterator over the full field (header and value)
    qd_iterator_t           *raw_iter;    // iterator over just the value
    const char              *parse_error;
    qd_buffer_field_t        full_field;  // contains encoded AMQP type header and value
    qd_amqp_field_t          amqp;        // decoded header and raw value
};

ALLOC_DECLARE(qd_parsed_field_t);
ALLOC_DEFINE(qd_parsed_field_t);


qd_parsed_field_t* qd_field_first_child(qd_parsed_field_t *field)
{
    return DEQ_HEAD(field->children);
}

qd_parsed_field_t* qd_field_next_child(qd_parsed_field_t *field)
{
    return DEQ_NEXT(field);
}


// length of size and count of AMQP data fields can be determined by the value
// of the top 4 bits of the tag octet.  See AMQP 1.0 Part 1 Types.
//
static inline int tag_get_size_length(uint8_t tag)
{
    tag &= 0xF0;
    if (tag < 0xA0) return 0;
    if ((tag & 0x10) == 0) return 1;
    return 4;
}


static inline int tag_get_count_length(uint8_t tag)
{
    tag &= 0xF0;
    if (tag < 0xC0) return 0;
    if ((tag & 0x10) == 0) return 1;
    return 4;
}


/**
 * Extract an AMQP value from the encoded data held in *bfield and store it in *value.
 * bfield is expected to point to the tag octet and will be advanced past the decoded value.
 * Returns 0 on success, else an error message.
 */
static inline char *parse_amqp_field(qd_buffer_field_t *bfield, qd_amqp_field_t *value)
{
    ZERO(value);

    if (!qd_buffer_field_octet(bfield, &value->tag))
        return "Insufficient Data to Determine Tag";

    uint32_t length_of_count = tag_get_count_length(value->tag);
    uint32_t length_of_size  = tag_get_size_length(value->tag);

    // extract size and content (optional)
    switch (value->tag & 0xF0) {
    case 0x40:
        break;
    case 0x50:
        value->size = 1;
        break;
    case 0x60:
        value->size = 2;
        break;
    case 0x70:
        value->size = 4;
        break;
    case 0x80:
        value->size = 8;
        break;
    case 0x90:
        value->size = 16;
        break;
    case 0xB0:
    case 0xD0:
    case 0xF0:
    {
        (void) length_of_size; // ignore unused var error
        assert(length_of_size == 4);
        if (!qd_buffer_field_uint32(bfield, &value->size)) {
            return "Insufficient Data to Determine Length";
        }
        if (length_of_count) {
            assert(length_of_count == 4);
            if (!qd_buffer_field_uint32(bfield, &value->count)) {
                return "Insufficient Data to Determine Count";
            }
        }
    }
        break;
    case 0xA0:
    case 0xC0:
    case 0xE0:
    {
        uint8_t octet;
        assert(length_of_size == 1);
        if (!qd_buffer_field_octet(bfield, &octet)) {
            return "Insufficient Data to Determine Length";
        }
        value->size = octet;
        if (length_of_count) {
            assert(length_of_count == 1);
            if (!qd_buffer_field_octet(bfield, &octet)) {
                return "Insufficient Data to Determine Count";
            }
            value->count = octet;
        }
        break;
    }

    default:
        return "Invalid Tag - No Length Information";
    }

    if ((value->tag == QD_AMQP_MAP8 || value->tag == QD_AMQP_MAP32) && (value->count & 1))
        return "Odd Number of Elements in a Map";

    if (length_of_count > value->size)
        return "Insufficient Length to Determine Count";

    value->value = *bfield;
    value->value.remaining = value->size - length_of_count;
    size_t moved = qd_buffer_field_advance(bfield, value->value.remaining);
    if (moved != value->value.remaining)
        return "Truncated field";

    return 0;
}



// bfield contains the encoded AMQP data to be parsed.  bfield starts at the
// type tag octet and should be long enough to hold the entire AMQP data type.
// On return bfield has been advanced past the encoded AMQP data.
//
static qd_parsed_field_t *qd_parse_internal(qd_buffer_field_t *bfield, qd_parsed_field_t *p)
{
    qd_parsed_field_t *field = new_qd_parsed_field_t();
    if (!field)
        return 0;
    ZERO(field);
    DEQ_ITEM_INIT(field);
    DEQ_INIT(field->children);
    field->parent     = p;
    field->full_field = *bfield;

    field->parse_error = parse_amqp_field(bfield, &field->amqp);
    if (!field->parse_error) {
        // truncate full_field in case bfield holds multiple values.
        // since bfield has advanced past the parsed field we just subtract it.
        field->full_field.remaining -= bfield->remaining;

        // now parse out the content of any contained types:
        qd_buffer_field_t children = field->amqp.value;
        for (uint32_t idx = 0; idx < field->amqp.count; idx++) {
            qd_parsed_field_t *child = qd_parse_internal(&children, field);
            DEQ_INSERT_TAIL(field->children, child);
            if (!qd_parse_ok(child)) {
                field->parse_error = child->parse_error;
                break;
            }
        }
    }

    return field;
}


qd_parsed_field_t *qd_parse(const qd_iterator_t *iter)
{
    if (!iter)
        return 0;

    qd_buffer_field_t bfield = qd_iterator_get_view_cursor(iter);
    return qd_parse_internal(&bfield, 0);
}


void qd_parse_free(qd_parsed_field_t *field)
{
    if (!field)
        return;

    assert(field->parent == 0);
    if (field->raw_iter)
        qd_iterator_free(field->raw_iter);

    if (field->typed_iter)
        qd_iterator_free(field->typed_iter);

    qd_parsed_field_t *sub_field = DEQ_HEAD(field->children);
    while (sub_field) {
        qd_parsed_field_t *next = DEQ_NEXT(sub_field);
        DEQ_REMOVE_HEAD(field->children);
        sub_field->parent = 0;
        qd_parse_free(sub_field);
        sub_field = next;
    }

    free_qd_parsed_field_t(field);
}


static qd_parsed_field_t *qd_parse_dup_internal(const qd_parsed_field_t *field, const qd_parsed_field_t *parent)
{
    qd_parsed_field_t *dup = new_qd_parsed_field_t();

    if (dup == 0)
        return 0;

    ZERO(dup);
    dup->parent     = parent;
    dup->raw_iter   = qd_iterator_dup(field->raw_iter);
    dup->typed_iter = qd_iterator_dup(field->typed_iter);
    dup->amqp       = field->amqp;
    dup->full_field = field->full_field;

    qd_parsed_field_t *child = DEQ_HEAD(field->children);
    while (child) {
        qd_parsed_field_t *dup_child = qd_parse_dup_internal(child, field);
        DEQ_INSERT_TAIL(dup->children, dup_child);
        child = DEQ_NEXT(child);
    }

    return dup;
}


qd_parsed_field_t *qd_parse_dup(const qd_parsed_field_t *field)
{
    return field ? qd_parse_dup_internal(field, 0) : 0;
}


int qd_parse_ok(qd_parsed_field_t *field)
{
    return field && field->parse_error == 0;
}


const char *qd_parse_error(qd_parsed_field_t *field)
{
    return field ? field->parse_error : "No field";
}


uint8_t qd_parse_tag(qd_parsed_field_t *field)
{
    assert(field);
    return field->amqp.tag;
}


// just the data (no header/tag)
qd_iterator_t *qd_parse_raw(qd_parsed_field_t *field)
{
    if (!field)
        return 0;
    if (!field->raw_iter) {
        field->raw_iter = qd_iterator_buffer_field(&field->amqp.value,
                                                   ITER_VIEW_ALL);
    }

    return field->raw_iter;
}


// includes type header, tag and data
qd_iterator_t *qd_parse_typed(qd_parsed_field_t *field)
{
    if (!field)
        return 0;
    if (!field->typed_iter) {
        field->typed_iter = qd_iterator_buffer_field(&field->full_field,
                                                     ITER_VIEW_ALL);
    }
    return field->typed_iter;
}


qd_buffer_field_t qd_parse_raw_field(const qd_parsed_field_t *field)
{
    assert(field && !field->parse_error);
    return field->amqp.value;
}


qd_buffer_field_t qd_parse_typed_field(const qd_parsed_field_t *field)
{
    assert(field && !field->parse_error);
    return field->full_field;
}


uint32_t qd_parse_as_uint(qd_parsed_field_t *field)
{
    uint32_t result = 0;
    uint64_t tmp = qd_parse_as_ulong(field);
    if (qd_parse_ok(field)) {
        if (tmp <= UINT32_MAX) {
            result = tmp;
        } else {
            field->parse_error = "Integer value too large to parse as uint";
        }
    }

    return result;
}


uint64_t qd_parse_as_ulong(qd_parsed_field_t *parsed_field)
{
    uint64_t result = 0;
    uint32_t tmp32 = 0;
    uint8_t  octet = 0;

    qd_buffer_field_t field = parsed_field->amqp.value;

    switch (parsed_field->amqp.tag) {
    case QD_AMQP_ULONG:
    case QD_AMQP_TIMESTAMP:
        qd_buffer_field_uint32(&field, &tmp32);
        result = ((uint64_t) tmp32) << 32;
        qd_buffer_field_uint32(&field, &tmp32);
        result |= ((uint64_t) tmp32);
        break;

    case QD_AMQP_UINT:
        qd_buffer_field_uint32(&field, &tmp32);
        result = tmp32;
        break;

    case QD_AMQP_USHORT:
        qd_buffer_field_octet(&field, &octet);
        result = ((uint64_t) octet) << 8;
        // fallthrough

    case QD_AMQP_BOOLEAN:
    case QD_AMQP_UBYTE:
    case QD_AMQP_SMALLUINT:
    case QD_AMQP_SMALLULONG:
        qd_buffer_field_octet(&field, &octet);
        result |= (uint64_t) octet;
        break;

    case QD_AMQP_TRUE:
        result = 1;
        break;

    case QD_AMQP_FALSE:
    case QD_AMQP_UINT0:
    case QD_AMQP_ULONG0:
        // already zeroed
        break;

    case QD_AMQP_STR8_UTF8:
    case QD_AMQP_STR32_UTF8:
    case QD_AMQP_SYM8:
    case QD_AMQP_SYM32:
        {
            // conversion from string to 64 bit unsigned integer:
            char *value = qd_buffer_field_strdup(&field);
            if (sscanf(value, "%"SCNu64, &result) != 1)
                parsed_field->parse_error = "Cannot convert string to unsigned long";
            free(value);
        }
        break;

    case QD_AMQP_BYTE:
    case QD_AMQP_SHORT:
    case QD_AMQP_INT:
    case QD_AMQP_SMALLINT:
    case QD_AMQP_LONG:
    case QD_AMQP_SMALLLONG:
    {
        // if a signed integer is positive, accept it
        int64_t ltmp = qd_parse_as_long(parsed_field);
        if (qd_parse_ok(parsed_field)) {
            if (ltmp >= 0) {
                result = (uint64_t)ltmp;
            } else {
                parsed_field->parse_error = "Unable to parse negative integer as unsigned";
            }
        }
    }
    break;


    default:
        parsed_field->parse_error = "Unable to parse as an unsigned integer";
        // catch any missing types during development
        assert(false);
    }

    return result;
}


int32_t qd_parse_as_int(qd_parsed_field_t *field)
{
    int32_t result = 0;
    int64_t tmp = qd_parse_as_long(field);
    if (qd_parse_ok(field)) {
        if (INT32_MIN <= tmp && tmp <= INT32_MAX) {
            result = tmp;
        } else {
            field->parse_error = "Integer value too large to parse as int";
        }
    }

    return result;
}


int64_t qd_parse_as_long(qd_parsed_field_t *parsed_field)
{
    int64_t result = 0;

    qd_buffer_field_t field = parsed_field->amqp.value;

    switch (parsed_field->amqp.tag) {
    case QD_AMQP_LONG: {
        uint64_t convert;
        uint32_t tmp32 = 0;
        qd_buffer_field_uint32(&field, &tmp32);
        convert = ((uint64_t) tmp32) << 32;
        qd_buffer_field_uint32(&field, &tmp32);
        convert |= (uint64_t) tmp32;
        result = (int64_t) convert;
        break;
    }

    case QD_AMQP_INT: {
        uint32_t tmp = 0;
        qd_buffer_field_uint32(&field, &tmp);
        result = (int32_t) tmp;
        break;
    }

    case QD_AMQP_SHORT: {
        uint16_t convert;
        uint8_t octet = 0;
        qd_buffer_field_octet(&field, &octet);
        convert = ((uint16_t) octet) << 8;
        qd_buffer_field_octet(&field, &octet);
        convert |= ((uint16_t) octet);
        result = (int16_t) convert;
        break;
    }

    case QD_AMQP_BYTE:
    case QD_AMQP_BOOLEAN:
    case QD_AMQP_SMALLLONG:
    case QD_AMQP_SMALLINT: {
        uint8_t octet = 0;
        qd_buffer_field_octet(&field, &octet);
        result = (int8_t) octet;
        break;
    }

    case QD_AMQP_TRUE:
        result = 1;
        break;

    case QD_AMQP_FALSE:
    case QD_AMQP_UINT0:
    case QD_AMQP_ULONG0:
        // already zeroed
        break;

    case QD_AMQP_STR8_UTF8:
    case QD_AMQP_STR32_UTF8:
    case QD_AMQP_SYM8:
    case QD_AMQP_SYM32:
        {
            // conversion from string to 64 bit integer:
            char *value = qd_buffer_field_strdup(&field);
            if (sscanf(value, "%"SCNi64, &result) != 1)
                parsed_field->parse_error = "Cannot convert string to long";
            free(value);
        }
        break;

    case QD_AMQP_UBYTE:
    case QD_AMQP_SMALLUINT:
    case QD_AMQP_SMALLULONG:
    case QD_AMQP_USHORT:
    case QD_AMQP_UINT:
    case QD_AMQP_ULONG:
    {
        // if an unsigned integer "fits" accept it
        uint64_t utmp = qd_parse_as_ulong(parsed_field);
        if (qd_parse_ok(parsed_field)) {
            uint64_t max = INT8_MAX;
            switch (parsed_field->amqp.tag) {
            case QD_AMQP_USHORT:
                max = INT16_MAX;
                break;
            case QD_AMQP_UINT:
                max = INT32_MAX;
                break;
            case QD_AMQP_ULONG:
                max = INT64_MAX;
                break;
            }
            if (utmp <= max) {
                result = (int64_t)utmp;
            } else {
                parsed_field->parse_error = "Unable to parse unsigned integer as a signed integer";
            }
        }
    }
    break;

    default:
        parsed_field->parse_error = "Unable to parse as a signed integer";
        // catch any missing types during development
        assert(false);
    }

    return result;
}


bool qd_parse_as_bool(qd_parsed_field_t *parsed_field)
{
    bool result = false;

    qd_buffer_field_t field = parsed_field->amqp.value;

    switch (parsed_field->amqp.tag) {
    case QD_AMQP_BYTE:
    case QD_AMQP_BOOLEAN: {
        uint8_t octet = 0;
        qd_buffer_field_octet(&field, &octet);
        result = !!octet;
        break;
    }

    case QD_AMQP_TRUE:
        result = true;
        break;
    }

    return result;
}


uint32_t qd_parse_sub_count(qd_parsed_field_t *field)
{
    uint32_t count = DEQ_SIZE(field->children);

    if (field->amqp.tag == QD_AMQP_MAP8 || field->amqp.tag == QD_AMQP_MAP32)
        count = count >> 1;

    return count;
}


qd_parsed_field_t *qd_parse_sub_key(qd_parsed_field_t *field, uint32_t idx)
{
    if (field->amqp.tag != QD_AMQP_MAP8 && field->amqp.tag != QD_AMQP_MAP32)
        return 0;

    idx = idx << 1;
    qd_parsed_field_t *key = DEQ_HEAD(field->children);
    while (idx && key) {
        idx--;
        key = DEQ_NEXT(key);
    }

    return key;
}


qd_parsed_field_t *qd_parse_sub_value(qd_parsed_field_t *field, uint32_t idx)
{
    if (field->amqp.tag == QD_AMQP_MAP8 || field->amqp.tag == QD_AMQP_MAP32)
        idx = (idx << 1) + 1;

    qd_parsed_field_t *key = DEQ_HEAD(field->children);
    while (idx && key) {
        idx--;
        key = DEQ_NEXT(key);
    }

    return key;
}


int is_tag_a_map(uint8_t tag)
{
    return tag == QD_AMQP_MAP8 || tag == QD_AMQP_MAP32;
}


int qd_parse_is_map(qd_parsed_field_t *field)
{
    if (!field)
        return 0;

    return is_tag_a_map(field->amqp.tag);
}


int qd_parse_is_list(qd_parsed_field_t *field)
{
    if (!field)
        return 0;

    return field->amqp.tag == QD_AMQP_LIST8
        || field->amqp.tag == QD_AMQP_LIST32
        || field->amqp.tag == QD_AMQP_LIST0;
}


int qd_parse_is_scalar(qd_parsed_field_t *field)
{
    return DEQ_SIZE(field->children) == 0;
}


static inline bool qd_parse_is_string(const qd_parsed_field_t *field)
{
    return field->amqp.tag == QD_AMQP_STR8_UTF8
        || field->amqp.tag == QD_AMQP_STR32_UTF8;
}


qd_parsed_field_t *qd_parse_value_by_key(qd_parsed_field_t *field, const char *key)
{
    if (!key)
        return 0;

    uint32_t count = qd_parse_sub_count(field);

    for (uint32_t idx = 0; idx < count; idx++) {
        qd_parsed_field_t *sub  = qd_parse_sub_key(field, idx);
        if (!sub)
            return 0;

        qd_buffer_field_t value = sub->amqp.value;
        size_t len = strlen(key);

        if (qd_buffer_field_equal(&value, (const uint8_t*) key, len)) {
            return qd_parse_sub_value(field, idx);
        }
    }

    return 0;
}


/* Incoming message router annotations processing.
 * See docs/notes/router-annotations.adoc
 */
const char *qd_parse_router_annotations(
    qd_buffer_field_t  *ra_field,
    qd_parsed_field_t **ra_ingress,
    qd_parsed_field_t **ra_to_override,
    qd_parsed_field_t **ra_trace,
    qd_parsed_field_t **ra_flags)
{
    *ra_ingress     = 0;
    *ra_to_override = 0;
    *ra_trace       = 0;
    *ra_flags       = 0;

    // ra_field should be pointing to the encoded RA list
    qd_amqp_field_t ra_list;
    const char *error = parse_amqp_field(ra_field, &ra_list);
    if (error)
        return error;

    if (ra_list.tag != QD_AMQP_LIST8 && ra_list.tag != QD_AMQP_LIST32)
        return "Invalid router annotations section: non-list type";

    // Version #2 defines 4 fields, allow future extensions at the end
    // of the list.
    if (ra_list.count < 4)
        return "Invalid router annotations section: wrong list count";

    // The order of the annotations within the list is fixed and described in
    // router-annotations.adoc.  ra_fields is set to the content of the list,
    // and each time an entry is parsed ra_fields is advanced to the next list
    // entry.

    qd_buffer_field_t ra_fields = ra_list.value;

    // index 0: flags
    (*ra_flags) = qd_parse_internal(&ra_fields, 0);
    if (!qd_parse_ok((*ra_flags)))
        return (*ra_flags)->parse_error;

    // index 1: to-override (optional)
    (*ra_to_override) = qd_parse_internal(&ra_fields, 0);
    if (!qd_parse_ok((*ra_to_override)))
        return (*ra_to_override)->parse_error;
    if ((*ra_to_override)->amqp.tag == QD_AMQP_NULL) {
        // optional - no to-override present
        qd_parse_free(*ra_to_override);
        *ra_to_override = 0;
    } else if (!qd_parse_is_string(*ra_to_override))
        return "Invalid router to-override annotation: wrong type";

    // index 2: ingress router id
    (*ra_ingress) = qd_parse_internal(&ra_fields, 0);
    if (!qd_parse_ok((*ra_ingress)))
        return (*ra_ingress)->parse_error;
    if ((*ra_ingress)->amqp.tag == QD_AMQP_NULL) {
        // no router ingress for edge connections
        qd_parse_free(*ra_ingress);
        *ra_ingress = 0;
    } else if (!qd_parse_is_string(*ra_ingress))
        return "Invalid router ingress annotation: wrong type";

    // index 3: trace list
    (*ra_trace) = qd_parse_internal(&ra_fields, 0);
    if (!qd_parse_ok((*ra_trace)))
        return (*ra_trace)->parse_error;
    if (!qd_parse_is_list((*ra_trace)))
        return "Invalid router trace annotation: not a list";
    bool all_str = true;
    for (qd_parsed_field_t *node = DEQ_HEAD((*ra_trace)->children);
         node && all_str;
         node = DEQ_NEXT(node)) {
        all_str = qd_parse_is_string(node);
    }
    if (!all_str)
        return "Invalid router trace annotation: list contains non-string entries";

    return 0;
}


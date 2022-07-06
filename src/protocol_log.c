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

#include "qpid/dispatch/protocol_log.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/io_module.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/compose.h"
#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/discriminator.h"
#include "dispatch_private.h"
#include "buffer_field_api.h"
#include "stdbool.h"
#include <inttypes.h>
#include <stdlib.h>
#include <sys/time.h>

#define ROUTER_ID_SIZE 6
#define EVENT_BATCH_MAX 50
#define FLUSH_SLOT_COUNT 5
#define RATE_SLOT_COUNT 5
#define IDENTITY_MAX 27

typedef struct plog_identity_t {
    uint64_t record_id;
    char     source_id[ROUTER_ID_SIZE];
} plog_identity_t;

typedef struct plog_attribute_data_t {
    DEQ_LINKS(struct plog_attribute_data_t);
    plog_attribute_t  attribute_type;
    uint32_t          emit_ordinal;
    union {
        uint64_t  uint_val;
        char     *string_val;
    } value;
} plog_attribute_data_t;

ALLOC_DECLARE(plog_attribute_data_t);
ALLOC_DEFINE(plog_attribute_data_t);
DEQ_DECLARE(plog_attribute_data_t, plog_attribute_data_list_t);

typedef struct plog_rate_t {
    DEQ_LINKS(struct plog_rate_t);
    DEQ_LINKS_N(PER_RECORD, struct plog_rate_t);
    uint64_t               slot[RATE_SLOT_COUNT];
    int                    slot_cursor;
    uint64_t               last_rate;
    plog_record_t         *record;
    plog_attribute_t       rate_attribute;
    plog_attribute_data_t *count_attribute;
} plog_rate_t;

ALLOC_DECLARE(plog_rate_t);
ALLOC_DEFINE(plog_rate_t);
DEQ_DECLARE(plog_rate_t, plog_rate_list_t);
DEQ_DECLARE(plog_record_t, plog_record_list_t);

struct plog_record_t {
    DEQ_LINKS(plog_record_t);
    DEQ_LINKS_N(UNFLUSHED, plog_record_t);
    plog_record_type_t          record_type;
    plog_record_t              *parent;
    plog_record_list_t          children;
    plog_identity_t             identity;
    plog_attribute_data_list_t  attributes;
    plog_rate_list_t            rates;
    uint64_t                    latency_start;
    uint32_t                    emit_ordinal;
    int                         flush_slot;
    bool                        never_flushed;
    bool                        never_logged;
    bool                        force_log;
    bool                        ended;
};

ALLOC_DECLARE(plog_record_t);
ALLOC_DEFINE(plog_record_t);

typedef struct plog_work_t plog_work_t;

typedef void (*plog_work_handler_t) (plog_work_t *work, bool discard);

struct plog_work_t {
    DEQ_LINKS(plog_work_t);
    plog_work_handler_t  handler;
    plog_record_t       *record;
    uint64_t             timestamp;
    plog_attribute_t     attribute;
    union {
        char     *string_val;
        uint64_t  int_val;
        void     *pointer;
    } value;
};

ALLOC_DECLARE(plog_work_t);
ALLOC_DEFINE(plog_work_t);
DEQ_DECLARE(plog_work_t, plog_work_list_t);

static const char *event_address_all           = "mc/sfe.all";
static const char *event_address_my_prefix     = "mc/sfe.";
static       char *event_address_my            = 0;
static const int   beacon_interval_sec         = 5;
static const int   flush_interval_msec         = 200;
static const int   initial_flush_interval_msec = 2000;
static const int   rate_slot_flush_intervals   = 10;    // For a two-second slot interval
static const int   rate_span                   = 10;    // Ten-second rolling average

static qdr_core_t         *router_core;
static sys_mutex_t        *lock;
static sys_mutex_t        *id_lock;
static sys_cond_t         *condition;
static sys_thread_t       *thread;
static bool                sleeping = false;
static qd_log_source_t    *log;
static plog_work_list_t    work_list    = DEQ_EMPTY;
static plog_record_t      *local_router = 0;
static plog_record_list_t  unflushed_records[FLUSH_SLOT_COUNT];
static plog_rate_list_t    rate_trackers = DEQ_EMPTY;
static int                 current_flush_slot = 0;
static char               *site_id;
static char               *hostname;
static char                router_id[ROUTER_ID_SIZE];
static uint64_t            next_identity     = 0;
static const char         *router_area;
static const char         *router_name;
static qdr_watch_handle_t  all_address_watch_handle;
static qdr_watch_handle_t  my_address_watch_handle;
static bool                all_address_usable = false;
static bool                my_address_usable  = false;
static qd_timer_t         *beacon_timer = 0;
static qd_timer_t         *flush_timer = 0;
static uint64_t            next_message_id = 0;

static void _plog_set_string_TH(plog_work_t *work, bool discard);
static void _plog_set_int_TH(plog_work_t *work, bool discard);

/**
 * @brief Return the current timestamp in microseconds
 *
 * @return uint64_t
 */
static uint64_t _now_in_usec(void)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_usec + 1000000 * tv.tv_sec;
}


/**
 * @brief Find either the existing attribute record or the insertion point for a new attribute.
 * 
 * @param record The record with the attribute list that should be searched
 * @param attr The attribute type to search for
 * @return data Pointer to result:
 *     If 0, insert new data at head
 *     If data has the same attribute type, overwrite this data record with new values
 *     If data has a different attribute type, insert new data record after this data record
 */
static plog_attribute_data_t* _plog_find_attribute(plog_record_t *record, plog_attribute_t attr)
{
    plog_attribute_data_t *data = DEQ_TAIL(record->attributes);

    while(!!data) {
        if (data->attribute_type <= attr) {
            //
            // Indicate the overwrite or insert-before case
            //
            return data;
        }
        data = DEQ_PREV(data);
    }

    //
    // Indicate the insert-at-tail case
    //
    return 0;
}


/**
 * @brief Assign a unique identity for a locally-sourced record.
 * 
 * @param identity (out) New, unique identity
 */
static void _plog_next_id(plog_identity_t *identity)
{
    sys_mutex_lock(id_lock);
    identity->record_id = next_identity++;
    memcpy(identity->source_id, router_id, ROUTER_ID_SIZE);
    sys_mutex_unlock(id_lock);
}


static char *_plog_id_to_new_string(const plog_identity_t *identity)
{
    char *result = (char*) malloc(IDENTITY_MAX);
    snprintf(result, IDENTITY_MAX, "%s:%"PRIu64, identity->source_id, identity->record_id);
    return result;
}


/**
 * @brief Concatenate the string representation of an id onto a string.
 *
 * @param buffer Target string for concatenation
 * @param n String size limit
 * @param id Identity to be string encoded
 */
static void _plog_strncat_id(char *buffer, size_t n, const plog_identity_t *id)
{
    char text[IDENTITY_MAX + 1];
    snprintf(text, IDENTITY_MAX, "%s:%"PRIu64, id->source_id, id->record_id);
    strncat(buffer, text, n);
}


/**
 * @brief Concatenate the attribute name onto a string.
 *
 * @param buffer Target string for concatenation
 * @param n String size limit
 * @param data Data value to extrace the attribute-type from
 */
static void _plog_strncat_attribute(char *buffer, size_t n, const plog_attribute_data_t *data)
{
#define ATTR_TEXT_MAX 65
    char  text[ATTR_TEXT_MAX + 1];
    char *text_ptr = text;

    text[0] = '\0';

    if ((uint64_t) 1 << data->attribute_type & VALID_UINT_ATTRS) {
        sprintf(text, "%"PRIu64, data->value.uint_val);
    } else if ((uint64_t) 1 << data->attribute_type & (VALID_STRING_ATTRS | VALID_TRACE_ATTRS | VALID_REF_ATTRS)) {
        text_ptr = data->value.string_val;
    }

    strncat(buffer, text_ptr, n);
}


static void _plog_compose_attribute(qd_composed_field_t *field, const plog_attribute_data_t *data)
{
    if ((uint64_t) 1 << data->attribute_type & VALID_UINT_ATTRS) {
        qd_compose_insert_long(field, data->value.uint_val);
    } else if ((uint64_t) 1 << data->attribute_type & (VALID_STRING_ATTRS | VALID_TRACE_ATTRS | VALID_REF_ATTRS)) {
        qd_compose_insert_string(field, data->value.string_val);
    }
}


/**
 * @brief Work handler for plog_start_record
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _plog_start_record_TH(plog_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    //
    // If the record type is ROUTER, this is the local-router record.  Store it.
    // Otherwise, if the parent is not specified, use the local_router as the parent.
    //
    plog_record_t *record = work->record;
    if (record->record_type == PLOG_RECORD_ROUTER) {
        local_router = record;
    } else if (record->parent == 0) {
        record->parent = local_router;
    }

    //
    // Record the creation timestamp in the record.
    //
    plog_work_t sub_work;
    sub_work.attribute = PLOG_ATTRIBUTE_START_TIME;
    sub_work.record    = record;
    sub_work.value.int_val = work->timestamp;
    _plog_set_int_TH(&sub_work, false);

    //
    // Record the parent reference.
    //
    if (!!record->parent) {
        sub_work.attribute = PLOG_ATTRIBUTE_PARENT;
        sub_work.record    = record;
        sub_work.value.string_val = _plog_id_to_new_string(&record->parent->identity);
        _plog_set_string_TH(&sub_work, false);
    }

    //
    // Place the new record on the parent's list of children
    //
    if (!!record->parent) {
        DEQ_INSERT_TAIL(record->parent->children, record);
    }

    //
    // If this record has a parent and the parent has never been logged,
    // flag it as needing to be flushed and logged.
    //
    if (!!record->parent && record->parent->never_logged) {
        record->parent->force_log = true;
        if (record->parent->flush_slot == -1) {
            record->parent->flush_slot = current_flush_slot;
            DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record->parent);
        }
    }

    //
    // Place the new record on the unflushed list to be pushed out later.
    // Mark the record as never-flushed so we can avoid the situation where
    // a record that references this record gets flushed before this record
    // is initially flushed.
    //
    if (record->flush_slot == -1) {
        record->flush_slot = current_flush_slot;
        DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record);
    }
}


/**
 * @brief Work handler for plog_end_record
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _plog_end_record_TH(plog_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    plog_record_t *record = work->record;

    //
    // Record the deletion timestamp in the record.
    //
    plog_work_t sub_work;
    sub_work.attribute = PLOG_ATTRIBUTE_END_TIME;
    sub_work.record    = record;
    sub_work.value.int_val = work->timestamp;
    _plog_set_int_TH(&sub_work, false);

    //
    // Mark the record as ended to designate the lifecycle end
    //
    record->ended = true;

    //
    // If the record has been flushed, schedule it for re-flushing
    // with the updated lifecycle information.
    //
    if (record->flush_slot == -1) {
        record->flush_slot = current_flush_slot;
        DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record);
    }

    //
    // Free any rate trackers on this record
    //
    plog_rate_t *rate = DEQ_HEAD(record->rates);
    while (!!rate) {
        DEQ_REMOVE(rate_trackers, rate);
        DEQ_REMOVE_N(PER_RECORD, record->rates, rate);
        free_plog_rate_t(rate);
        rate = DEQ_HEAD(record->rates);
    }
}


/**
 * @brief Work handler for plog_set_string
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _plog_set_string_TH(plog_work_t *work, bool discard)
{
    if (discard) {
        free(work->value.string_val);
        return;
    }

    plog_record_t         *record = work->record;
    plog_attribute_data_t *insert = _plog_find_attribute(record, work->attribute);
    plog_attribute_data_t *data;

    if (!insert || insert->attribute_type != work->attribute) {
        //
        // The attribute does not exist, create a new one and insert appropriately
        //
        data = new_plog_attribute_data_t();
        ZERO(data);
        data->attribute_type   = work->attribute;
        data->emit_ordinal     = record->emit_ordinal;
        data->value.string_val = work->value.string_val;
        if (!!insert) {
            DEQ_INSERT_AFTER(record->attributes, data, insert);
        } else {
            DEQ_INSERT_HEAD(record->attributes, data);
        }
    } else {
        //
        // The attribute already exists, overwrite the value
        //
        free(insert->value.string_val);
        insert->value.string_val = work->value.string_val;
        insert->emit_ordinal     = record->emit_ordinal;
    }

    if (record->flush_slot == -1) {
        record->flush_slot = current_flush_slot;
        DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record);
    }
}


/**
 * @brief Work handler for plog_set_int
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _plog_set_int_TH(plog_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    plog_record_t         *record = work->record;
    plog_attribute_data_t *insert = _plog_find_attribute(record, work->attribute);
    plog_attribute_data_t *data;

    if (!insert || insert->attribute_type != work->attribute) {
        //
        // The attribute does not exist, create a new one and insert appropriately
        //
        data = new_plog_attribute_data_t();
        ZERO(data);
        data->attribute_type = work->attribute;
        data->emit_ordinal   = record->emit_ordinal;
        data->value.uint_val = work->value.int_val;
        if (!!insert) {
            DEQ_INSERT_AFTER(record->attributes, data, insert);
        } else {
            DEQ_INSERT_HEAD(record->attributes, data);
        }
    } else {
        //
        // The attribute already exists, overwrite the value
        //
        insert->value.uint_val = work->value.int_val;
        insert->emit_ordinal   = record->emit_ordinal;
    }

    if (record->flush_slot == -1) {
        record->flush_slot = current_flush_slot;
        DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record);
    }
}


/**
 * @brief Allocate a work object pre-loaded with a handler.
 * 
 * @param handler The handler to be called on the plog thread to do the work
 * @return plog_work_t* Pointer to the allocated work that should be posted for processing
 */
static plog_work_t *_plog_work(plog_work_handler_t handler)
{
    plog_work_t *work = new_plog_work_t();
    ZERO(work);
    work->handler = handler;
    return work;
}


/**
 * @brief Post work for processing in the plog thread
 * 
 * @param work Pointer to the work to be processed
 */
static void _plog_post_work(plog_work_t *work)
{
    sys_mutex_lock(lock);
    DEQ_INSERT_TAIL(work_list, work);
    bool need_signal = sleeping;
    sys_mutex_unlock(lock);

    if (need_signal) {
        sys_cond_signal(condition);
    }
}


/**
 * @brief Create the record that represents the local router.
 */
static void _plog_create_router_record(void)
{
    plog_record_t *router = plog_start_record(PLOG_RECORD_ROUTER, 0);

    const char *namespace  = getenv("POD_NAMESPACE");
    const char *image_name = getenv("APPLICATION_NAME");
    const char *version    = getenv("VERSION");

    char *name = (char*) malloc(strlen(router_area) + strlen(router_name) + 2);
    strcpy(name, router_area);
    strcat(name, "/");
    strcat(name, router_name);
    plog_set_string(router, PLOG_ATTRIBUTE_NAME, name);
    free(name);

    if (!!site_id) {
        plog_set_string(router, PLOG_ATTRIBUTE_PARENT, site_id);
    }

    if (!!hostname) {
        plog_set_string(router, PLOG_ATTRIBUTE_HOST_NAME, hostname);
    }

    if (!!namespace) {
        plog_set_string(router, PLOG_ATTRIBUTE_NAMESPACE, namespace);
    }

    if (!!image_name) {
        plog_set_string(router, PLOG_ATTRIBUTE_IMAGE_NAME, image_name);
    }

    if (!!version) {
        plog_set_string(router, PLOG_ATTRIBUTE_IMAGE_VERSION, version);
    }

    plog_set_string(router, PLOG_ATTRIBUTE_BUILD_VERSION, QPID_DISPATCH_VERSION);
}


/**
 * @brief Recursively free the given record and all of its children
 * 
 * @param record Pointer to the record to be freed.
 * @param recursive If true, delete recursively, otherwise just remove parent references.
 */
static void _plog_free_record_TH(plog_record_t *record, bool recursive)
{
    //
    // If this record is a child of a parent, remove it from the parent's child list
    //
    if (!!record->parent) {
        DEQ_REMOVE(record->parent->children, record);
    }

    //
    // Remove the record from the unflushed list if needed
    //
    if (record->flush_slot >= 0) {
        DEQ_REMOVE_N(UNFLUSHED, unflushed_records[record->flush_slot], record);
        record->flush_slot = -1;
    }

    if (recursive) {
        //
        // Remove all of this record's children
        //
        while (!DEQ_IS_EMPTY(record->children)) {
            _plog_free_record_TH(DEQ_HEAD(record->children), true);
        }
    } else {
        //
        // Remove the childrens' parent references
        //
        plog_record_t *child = DEQ_HEAD(record->children);
        while (!!child) {
            child->parent = 0;
            child = DEQ_NEXT(child);
        }
    }

    //
    // Free all of this record's attributes
    //
    plog_attribute_data_t *data = DEQ_HEAD(record->attributes);
    while (!!data) {
        DEQ_REMOVE_HEAD(record->attributes);
        if ((uint64_t) 1 << data->attribute_type & (VALID_STRING_ATTRS | VALID_TRACE_ATTRS | VALID_REF_ATTRS)) {
            free(data->value.string_val);
        }
        free_plog_attribute_data_t(data);
        data = DEQ_HEAD(record->attributes);
    }

    //
    // Free the record
    //
    free_plog_record_t(record);
}


static const char *_plog_record_type_name(const plog_record_t *record)
{
    switch (record->record_type) {
    case PLOG_RECORD_SITE       : return "SITE";
    case PLOG_RECORD_ROUTER     : return "ROUTER";
    case PLOG_RECORD_LINK       : return "LINK";
    case PLOG_RECORD_CONTROLLER : return "CONTROLLER";
    case PLOG_RECORD_LISTENER   : return "LISTENER";
    case PLOG_RECORD_CONNECTOR  : return "CONNECTOR";
    case PLOG_RECORD_FLOW       : return "FLOW";
    case PLOG_RECORD_PROCESS    : return "PROCESS";
    case PLOG_RECORD_IMAGE      : return "IMAGE";
    case PLOG_RECORD_INGRESS    : return "INGRESS";
    case PLOG_RECORD_EGRESS     : return "EGRESS";
    case PLOG_RECORD_COLLECTOR  : return "COLLECTOR";
    }
    return "UNKNOWN";
}


static const char *_plog_attribute_name(const plog_attribute_data_t *data)
{
    switch (data->attribute_type) {
    case PLOG_ATTRIBUTE_RECORD_TYPE      : return "recordType";
    case PLOG_ATTRIBUTE_IDENTITY         : return "identity";
    case PLOG_ATTRIBUTE_PARENT           : return "parent";
    case PLOG_ATTRIBUTE_START_TIME       : return "startTime";
    case PLOG_ATTRIBUTE_END_TIME         : return "endTime";
    case PLOG_ATTRIBUTE_COUNTERFLOW      : return "counterflow";
    case PLOG_ATTRIBUTE_PEER             : return "peer";
    case PLOG_ATTRIBUTE_PROCESS          : return "process";
    case PLOG_ATTRIBUTE_SIBLING_ORDINAL  : return "sibOrdinal";
    case PLOG_ATTRIBUTE_LOCATION         : return "location";
    case PLOG_ATTRIBUTE_PROVIDER         : return "provider";
    case PLOG_ATTRIBUTE_PLATFORM         : return "platform";
    case PLOG_ATTRIBUTE_NAMESPACE        : return "namespace";
    case PLOG_ATTRIBUTE_MODE             : return "mode";
    case PLOG_ATTRIBUTE_SOURCE_HOST      : return "sourceHost";
    case PLOG_ATTRIBUTE_DESTINATION_HOST : return "destHost";
    case PLOG_ATTRIBUTE_PROTOCOL         : return "protocol";
    case PLOG_ATTRIBUTE_SOURCE_PORT      : return "sourcePort";
    case PLOG_ATTRIBUTE_DESTINATION_PORT : return "destPort";
    case PLOG_ATTRIBUTE_VAN_ADDRESS      : return "vanAddress";
    case PLOG_ATTRIBUTE_IMAGE_NAME       : return "imageName";
    case PLOG_ATTRIBUTE_IMAGE_VERSION    : return "imageVersion";
    case PLOG_ATTRIBUTE_HOST_NAME        : return "hostname";
    case PLOG_ATTRIBUTE_OCTETS           : return "octets";
    case PLOG_ATTRIBUTE_LATENCY          : return "latency";
    case PLOG_ATTRIBUTE_TRANSIT_LATENCY  : return "transitLatency";
    case PLOG_ATTRIBUTE_BACKLOG          : return "backlog";
    case PLOG_ATTRIBUTE_METHOD           : return "method";
    case PLOG_ATTRIBUTE_RESULT           : return "result";
    case PLOG_ATTRIBUTE_REASON           : return "reason";
    case PLOG_ATTRIBUTE_NAME             : return "name";
    case PLOG_ATTRIBUTE_TRACE            : return "trace";
    case PLOG_ATTRIBUTE_BUILD_VERSION    : return "buildVersion";
    case PLOG_ATTRIBUTE_LINK_COST        : return "linkCost";
    case PLOG_ATTRIBUTE_DIRECTION        : return "direction";
    case PLOG_ATTRIBUTE_OCTET_RATE       : return "octetRate";
    case PLOG_ATTRIBUTE_OCTETS_OUT       : return "octetsOut";
    case PLOG_ATTRIBUTE_OCTETS_UNACKED   : return "octetsUnacked";
    case PLOG_ATTRIBUTE_WINDOW_CLOSURES  : return "windowClosures";
    case PLOG_ATTRIBUTE_WINDOW_SIZE      : return "windowSize";
    }
    return "UNKNOWN";
}


/**
 * @brief Extract the value of a record identity from its serialized form in an iterator
 * 
 * @param field Pointer to the parsed field containing the serialized identity
 * @param identity [out] Pointer to the identity to be overwritten
 * @return True iff the serialized identity was well formed
 */
static bool _plog_unserialize_identity(qd_parsed_field_t *field, plog_identity_t *identity)
{
    if (!qd_parse_is_list(field) || qd_parse_sub_count(field) != 2) {
        return false;
    }

    qd_parsed_field_t *router_id_field = qd_parse_sub_value(field, 0);
    qd_parsed_field_t *record_id_field = qd_parse_sub_value(field, 1);

    if (!qd_parse_is_scalar(router_id_field) || !qd_parse_is_scalar(record_id_field)) {
        return false;
    }

    identity->record_id = qd_parse_as_ulong(record_id_field);

    qd_iterator_t *iter = qd_parse_raw(router_id_field);
    qd_iterator_ncopy(iter, (uint8_t*) identity->source_id, ROUTER_ID_SIZE - 1);
    identity->source_id[ROUTER_ID_SIZE - 1] = '\0';

    return true;
}


/**
 * @brief Emit a single record as a log event
 *
 * @param record Pointer to the record to be emitted
 */
static void _plog_emit_record_as_log_TH(plog_record_t *record)
{
#define LINE_MAX 1000
    char line[LINE_MAX + 1];

    strcpy(line, _plog_record_type_name(record));
    strcat(line, " [");
    _plog_strncat_id(line, LINE_MAX, &record->identity);
    strcat(line, "]");
    if (record->never_logged) {
        strcat(line, " BEGIN");
    }
    if (record->ended) {
        strcat(line, " END");
    }

    plog_attribute_data_t *data = DEQ_HEAD(record->attributes);
    while (data) {
        if (data->attribute_type != PLOG_ATTRIBUTE_START_TIME && data->attribute_type != PLOG_ATTRIBUTE_END_TIME) {
            strncat(line, " ", LINE_MAX);
            strncat(line, _plog_attribute_name(data), LINE_MAX);
            strncat(line, "=", LINE_MAX);
            _plog_strncat_attribute(line, LINE_MAX, data);
        }
        data = DEQ_NEXT(data);
    }

    record->never_logged = false;
    qd_log(log, QD_LOG_INFO, line);
}


/**
 * @brief Emit all of the unflushed records as events, batched into message bodies.
 *
 * @param core Pointer to the core module
 */
static void _plog_emit_unflushed_as_events_TH(qdr_core_t *core)
{
    if (DEQ_SIZE(unflushed_records[current_flush_slot]) == 0) {
        return;
    }

    int                  event_count = 0;
    qd_composed_field_t *field = 0;
    plog_record_t       *record = DEQ_HEAD(unflushed_records[current_flush_slot]);

    while (!!record) {
        if (field == 0) {
            //
            // Compose a new message content starting with the properties
            //
            field = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
            qd_compose_start_list(field);
            qd_compose_insert_long(field, next_message_id++);
            qd_compose_insert_null(field);                      // user-id
            qd_compose_insert_string(field, event_address_my);  // to
            qd_compose_insert_string(field, "RECORD");          // subject
            qd_compose_end_list(field);

            //
            // Append the body section to the content
            //
            field = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, field);

            //
            // Form the body as a list of records.
            //
            qd_compose_start_list(field);
        }

        //
        // Insert the current record into the current message body.
        //
        qd_compose_start_map(field);
        qd_compose_insert_uint(field, PLOG_ATTRIBUTE_RECORD_TYPE);
        qd_compose_insert_uint(field, record->record_type);

        qd_compose_insert_uint(field, PLOG_ATTRIBUTE_IDENTITY);
        char identity[IDENTITY_MAX + 1];
        snprintf(identity, IDENTITY_MAX, "%s:%"PRIu64, record->identity.source_id, record->identity.record_id);
        qd_compose_insert_string(field, identity);

        plog_attribute_data_t *data = DEQ_HEAD(record->attributes);
        while (data) {
            if (data->emit_ordinal >= record->emit_ordinal) {
                qd_compose_insert_uint(field, data->attribute_type);
                _plog_compose_attribute(field, data);
            }
            data = DEQ_NEXT(data);
        }
        qd_compose_end_map(field);

        //
        // Count this event.  If we have reached the maximum events for a batch, or
        // we have reached the end of the unflushed list, close out the current message.
        //
        event_count++;
        if (event_count == EVENT_BATCH_MAX || record == DEQ_TAIL(unflushed_records[current_flush_slot])) {
            event_count = 0;

            //
            // Close out the event list in the message body
            //
            qd_compose_end_list(field);

            //
            // Create a message for the content
            //
            qd_message_t *event = qd_message();
            qd_message_compose_2(event, field, true);

            //
            // Send the message to all of the bound receivers
            //
            qdr_send_to2(core, event, event_address_my, true, false);

            //
            // Free up used resources
            //
            qd_compose_free(field);
            qd_message_free(event);

            //
            // Nullify the field pointer so that a new message will be started on the next
            // loop iteration.
            //
            field = 0;
        }

        record = DEQ_NEXT_N(UNFLUSHED, record);
    }

}


/**
 * @brief Emit all of the unflushed records
 * 
 * @param core Pointer to the core module
 */
static void _plog_flush_TH(qdr_core_t *core)
{
    //
    // If there is at least one collector for this router, batch up the
    // unflushed records and send them as events to the collector.
    //
    if (my_address_usable) {
        _plog_emit_unflushed_as_events_TH(core);
    }

    plog_record_t *record = DEQ_HEAD(unflushed_records[current_flush_slot]);
    while (!!record) {
        DEQ_REMOVE_HEAD_N(UNFLUSHED, unflushed_records[current_flush_slot]);
        assert(record->flush_slot >= 0);
        record->flush_slot = -1;

        //
        // If this record has been ended, emit the log line.
        //
        if (record->ended || record->force_log) {
            record->force_log = false;
            _plog_emit_record_as_log_TH(record);
        }

        record->never_flushed = false;
        record->emit_ordinal++;
        if (record->ended) {
            _plog_free_record_TH(record, false);
        }
        record = DEQ_HEAD(unflushed_records[current_flush_slot]);
    }
}


static void _plog_send_beacon_TH(plog_work_t *work, bool discard)
{
    if (!discard) {
        qdr_core_t *core = (qdr_core_t*) work->value.pointer;

        //
        // Compose the message content starting with the properties
        //
        qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(field);
        qd_compose_insert_long(field, next_message_id++);
        qd_compose_insert_null(field);                       // user-id
        qd_compose_insert_string(field, event_address_all);  // to
        qd_compose_insert_string(field, "BEACON");           // subject
        qd_compose_end_list(field);

        field = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, field);
        qd_compose_start_map(field);
        qd_compose_insert_symbol(field, "v");
        qd_compose_insert_uint(field, 1);
        qd_compose_insert_symbol(field, "sourceType");
        qd_compose_insert_string(field, "ROUTER");
        qd_compose_insert_symbol(field, "address");
        qd_compose_insert_string(field, event_address_my);
        qd_compose_end_map(field);

        //
        // Append the body section to the content
        //
        field = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, field);
        qd_compose_insert_null(field);

        //
        // Create a message for the content
        //
        qd_message_t *beacon = qd_message();
        qd_message_compose_2(beacon, field, true);

        //
        // Send the message to all of the bound receivers
        //
        qdr_send_to2(core, beacon, event_address_all, true, false);

        //
        // Free up used resources
        //
        qd_compose_free(field);
        qd_message_free(beacon);
    }
}


static void _plog_refresh_record_TH(plog_record_t *record)
{
    record->emit_ordinal = 0;
    if (record->flush_slot == -1) {
        record->flush_slot = current_flush_slot;
        DEQ_INSERT_TAIL_N(UNFLUSHED, unflushed_records[current_flush_slot], record);
    }

    plog_record_t *child = DEQ_HEAD(record->children);
    while (!!child) {
        _plog_refresh_record_TH(child);
        child = DEQ_NEXT(child);
    }
}


static void _plog_refresh_events_TH(plog_work_t *work, bool discard)
{
    if (!discard) {
        _plog_refresh_record_TH(local_router);
    }
}


static void _plog_add_rate_TH(plog_work_t *work, bool discard)
{
    if (!discard) {
        plog_rate_t *rate = new_plog_rate_t();
        ZERO(rate);
        plog_attribute_data_t *data = DEQ_HEAD(work->record->attributes);
        while (!!data) {
            if (data->attribute_type == work->attribute) {
                rate->count_attribute = data;
                break;
            }
            data = DEQ_NEXT(data);
        }
        assert(!!rate->count_attribute); // Ensure rate is created against an existing counter
        rate->rate_attribute = work->value.int_val;
        rate->record         = work->record;
        for (int i = 0; i < RATE_SLOT_COUNT; i++) {
            rate->slot[i] = rate->count_attribute->value.uint_val;
        }
        rate->slot_cursor = 1;
        DEQ_INSERT_TAIL(rate_trackers, rate);
        DEQ_INSERT_TAIL_N(PER_RECORD, rate->record->rates, rate);
    }
}


static void _plog_process_rates_TH(void)
{
    plog_rate_t *rate = DEQ_HEAD(rate_trackers);
    while(!!rate) {
        rate->slot[rate->slot_cursor] = rate->count_attribute->value.uint_val;
        uint64_t delta = rate->slot[rate->slot_cursor] - rate->slot[(rate->slot_cursor + 1) % RATE_SLOT_COUNT];
        rate->slot_cursor = (rate->slot_cursor + 1) % RATE_SLOT_COUNT;
        uint64_t average_rate = delta / rate_span;
        plog_set_uint64(rate->record, rate->rate_attribute, average_rate);
        rate = DEQ_NEXT(rate);
    }
}


static void _plog_tick_TH(plog_work_t *work, bool discard)
{
    static int tick_ordinal = 0;
    if (!discard) {
        _plog_flush_TH(router_core);
        current_flush_slot = (current_flush_slot + 1) % FLUSH_SLOT_COUNT;

        tick_ordinal = (tick_ordinal + 1) % rate_slot_flush_intervals;
        if (tick_ordinal == 0) {
            _plog_process_rates_TH();
        }
    }
}


static void _plog_on_flush(void *context)
{
    plog_work_t *work = _plog_work(_plog_tick_TH);
    _plog_post_work(work);
    qd_timer_schedule(flush_timer, flush_interval_msec);
}


static void _plog_send_beacon(qdr_core_t *core)
{
    if (!!beacon_timer) {
        plog_work_t *work = _plog_work(_plog_send_beacon_TH);
        work->value.pointer = core;
        _plog_post_work(work);
        qd_timer_schedule(beacon_timer, beacon_interval_sec * 1000);
    }
}


//=====================================================================================
// Module Thread
//=====================================================================================
/**
 * @brief Main function for the plog thread.  This thread runs for the entire
 * lifecycle of the router.
 * 
 * @param unused Unused
 * @return void* Unused
 */
static void *_plog_thread(void *context)
{
    bool running = true;
    plog_work_list_t local_work_list = DEQ_EMPTY;
    qdr_core_t *core = (qdr_core_t*) context;

    qd_log(log, QD_LOG_INFO, "Protocol logging started");

    while (running) {
        //
        // Use the lock only to protect the condition variable and the work lists
        //
        sys_mutex_lock(lock);
        for (;;) {
            if (!DEQ_IS_EMPTY(work_list)) {
                DEQ_MOVE(work_list, local_work_list);
                break;
            }

            //
            // Block on the condition variable when there is no work to do
            //
            sleeping = true;
            sys_cond_wait(condition, lock);
            sleeping = false;
        }
        sys_mutex_unlock(lock);

        //
        // Process the local work list with the lock not held
        //
        plog_work_t *work = DEQ_HEAD(local_work_list);
        while (work) {
            DEQ_REMOVE_HEAD(local_work_list);
            if (!!work->handler) {
                work->handler(work, !running);
            } else {
                //
                // The thread is signalled to exit by posting work with a null handler.
                //
                running = false;
            }
            free_plog_work_t(work);
            work = DEQ_HEAD(local_work_list);
        }
    }

    //
    // Flush out all of the slots
    //
    for (int i = 0; i < FLUSH_SLOT_COUNT; i++) {
        _plog_flush_TH(core);
        current_flush_slot = (current_flush_slot + 1) % FLUSH_SLOT_COUNT;
    }

    //
    // Free all remaining records in the tree
    //
    _plog_free_record_TH(local_router, true);

    qd_log(log, QD_LOG_INFO, "Protocol logging completed");
    return 0;
}


//=====================================================================================
// API Callbacks
//=====================================================================================
/**
 * @brief Handler for changes in reachability for the all-routers multicast address.
 *        This address is used for "beacon" messages that announce the existence of 
 *        a router in the network.
 * 
 * @param context Context for the handler (the core module pointer)
 * @param local_consumers The number of local (on this router) consumers for the address
 * @param in_proc_consumers (unused) The number of in-process consumers for the address
 * @param remote_consumers The number of remote routers with local consumers for the address
 * @param local_producers (unused) The number of local producers for the address
 */
static void _plog_on_all_address_watch(void     *context,
                                       uint32_t  local_consumers,
                                       uint32_t  in_proc_consumers,
                                       uint32_t  remote_consumers,
                                       uint32_t  local_producers)
{
    bool now_usable = local_consumers > 0 || remote_consumers > 0;

    if (now_usable && !all_address_usable) {
        //
        // Start sending beacon messages to the all_address.
        //
        qd_log(log, QD_LOG_INFO, "Event collector detected.  Begin sending beacons.");
        all_address_usable = true;
        _plog_send_beacon((qdr_core_t*) context);
    } else if (!now_usable && all_address_usable) {
        //
        // Stop sending beacons.  Nobody is listening.
        //
        qd_log(log, QD_LOG_INFO, "Event collector lost.  Stop sending beacons.");
        all_address_usable = false;
        if (!!beacon_timer) {
            qd_timer_cancel(beacon_timer);
        }
    }
}


/**
 * @brief Handler for changes in reachability for this router's event multicast address.
 *        This address is used to send the log records to collectors in the network.
 * 
 * @param context Context for the handler (the core module pointer)
 * @param local_consumers The number of local (on this router) consumers for the address
 * @param in_proc_consumers (unused) The number of in-process consumers for the address
 * @param remote_consumers The number of remote routers with local consumers for the address
 * @param local_producers (unused) The number of local producers for the address
 */
static void _plog_on_my_address_watch(void     *context,
                                      uint32_t  local_consumers,
                                      uint32_t  in_proc_consumers,
                                      uint32_t  remote_consumers,
                                      uint32_t  local_producers)
{
    bool now_usable = local_consumers > 0 || remote_consumers > 0;

    if (now_usable && !my_address_usable) {
        //
        // Start sending log records
        //
        qd_log(log, QD_LOG_INFO, "Event collector for this router detected.  Begin sending flow events.");
        my_address_usable = true;
        _plog_post_work(_plog_work(_plog_refresh_events_TH));
    } else if (!now_usable && my_address_usable) {
        //
        // Stop sending log records
        //
        qd_log(log, QD_LOG_INFO, "Event collector for this router lost.  Stop sending flow events.");
        my_address_usable = false;
    }
}


static void _plog_on_beacon(void *context)
{
    _plog_send_beacon((qdr_core_t*) context);
}


//=====================================================================================
// Public Functions
//=====================================================================================
plog_record_t *plog_start_record(plog_record_type_t record_type, plog_record_t *parent)
{
    plog_record_t *record = new_plog_record_t();
    plog_work_t   *work   = _plog_work(_plog_start_record_TH);
    ZERO(record);
    record->record_type   = record_type;
    record->parent        = parent;
    record->flush_slot    = -1;
    record->never_flushed = true;
    record->never_logged  = true;
    record->force_log     = false;
    record->ended         = false;

    work->record    = record;
    work->timestamp = _now_in_usec();

    //
    // Assign a unique identity to the new record
    //
    _plog_next_id(&record->identity);

    _plog_post_work(work);
    return record;
}


void plog_end_record(plog_record_t *record)
{
    if (!!record) {
        plog_work_t *work = _plog_work(_plog_end_record_TH);
        work->record    = record;
        work->timestamp = _now_in_usec();
        _plog_post_work(work);
    }
}


void plog_serialize_identity(const plog_record_t *record, qd_composed_field_t *field)
{
    assert(!!record);
    if (!!record) {
        qd_compose_start_list(field);
        qd_compose_insert_string(field, record->identity.source_id);
        qd_compose_insert_ulong(field, record->identity.record_id);
        qd_compose_end_list(field);
    }
}


void plog_set_ref_from_record(plog_record_t *record, plog_attribute_t attribute_type, plog_record_t *referenced_record)
{
    if (!!record && !!referenced_record) {
        assert((uint64_t) 1 << attribute_type & VALID_REF_ATTRS);
        plog_work_t *work = _plog_work(_plog_set_string_TH);
        work->record           = record;
        work->attribute        = attribute_type;
        work->value.string_val = _plog_id_to_new_string(&referenced_record->identity);
        _plog_post_work(work);
    }
}


void plog_set_ref_from_parsed(plog_record_t *record, plog_attribute_t attribute_type, qd_parsed_field_t *field)
{
    if (!!record) {
        assert((uint64_t) 1 << attribute_type & VALID_REF_ATTRS);
        plog_work_t *work = _plog_work(_plog_set_string_TH);
        work->record    = record;
        work->attribute = attribute_type;

        plog_identity_t reference;
        bool good_id = _plog_unserialize_identity(field, &reference);
        work->value.string_val = _plog_id_to_new_string(&reference);

        if (good_id) {
            _plog_post_work(work);
        } else {
            free_plog_work_t(work);
            qd_log(log, QD_LOG_WARNING, "Reference ID cannot be parsed from the received field");
        }
    }
}


void plog_set_string(plog_record_t *record, plog_attribute_t attribute_type, const char *value)
{
#define MAX_STRING_VALUE 300
    if (!!record) {
        assert((uint64_t) 1 << attribute_type & (VALID_STRING_ATTRS | VALID_REF_ATTRS));
        plog_work_t *work = _plog_work(_plog_set_string_TH);
        work->record           = record;
        work->attribute        = attribute_type;
        work->value.string_val = !!value ? strndup(value, strnlen(value, MAX_STRING_VALUE)) : 0;
        _plog_post_work(work);
    }
}


void plog_set_uint64(plog_record_t *record, plog_attribute_t attribute_type, uint64_t value)
{
    if (!!record) {
        assert((uint64_t) 1 << attribute_type & VALID_UINT_ATTRS);
        plog_work_t *work = _plog_work(_plog_set_int_TH);
        work->record        = record;
        work->attribute     = attribute_type;
        work->value.int_val = value;
        _plog_post_work(work);
    }
}


void plog_set_trace(plog_record_t *record, qd_message_t *msg)
{
#define MAX_TRACE_BUFFER 1000
    if (!record) {
        return;
    }

    char *trace_text     = "Local";
    char *trace_text_ptr = trace_text;
    char  trace_buffer[MAX_TRACE_BUFFER + 1];
    qd_parsed_field_t *trace_value = qd_message_get_trace(msg);

    if (trace_value && qd_parse_is_list(trace_value)) {
        uint32_t trace_count = qd_parse_sub_count(trace_value);
        if (trace_count > 0) {
            trace_text_ptr = trace_buffer;
            char *cursor   = trace_text_ptr;
            for (uint32_t i = 0; i < trace_count; i++) {
                qd_parsed_field_t *trace_item = qd_parse_sub_value(trace_value, i);
                if (i > 0) {
                    *(cursor++) = '|';
                }
                if (qd_parse_is_scalar(trace_item)) {
                    qd_buffer_field_t raw_trace = qd_parse_raw_field(trace_item);
                    cursor += qd_buffer_field_ncopy(&raw_trace,
                                                    (uint8_t*) cursor, MAX_TRACE_BUFFER - (cursor - trace_text_ptr));
                }
            }
            *(cursor++) = '\0';
        }
    }

    plog_work_t *work = _plog_work(_plog_set_string_TH);
    work->record    = record;
    work->attribute = PLOG_ATTRIBUTE_TRACE;
    work->value.string_val = strdup(trace_text_ptr);
    _plog_post_work(work);
}


void plog_latency_start(plog_record_t *record)
{
    if (!!record) {
        record->latency_start = _now_in_usec();
    }
}


void plog_latency_end(plog_record_t *record)
{
    if (!!record && record->latency_start > 0) {
        uint64_t now = _now_in_usec();
        plog_set_uint64(record, PLOG_ATTRIBUTE_LATENCY, now - record->latency_start);
    }
}


void plog_add_rate(plog_record_t *record, plog_attribute_t count_attribute, plog_attribute_t rate_attribute)
{
    if (!!record) {
        assert((uint64_t) 1 << count_attribute & VALID_UINT_ATTRS);
        assert((uint64_t) 1 << rate_attribute & VALID_UINT_ATTRS);
        plog_work_t *work = _plog_work(_plog_add_rate_TH);
        work->record        = record;
        work->attribute     = count_attribute;
        work->value.int_val = rate_attribute;
        _plog_post_work(work);
    }
}


//=====================================================================================
// IO Module Callbacks
//=====================================================================================

static void _plog_init_address_watch_TH(plog_work_t *work, bool discard)
{
    qdr_core_t *core = (qdr_core_t*) work->value.pointer;

    if (!discard) {
        event_address_my = (char*) malloc(71);
        strcpy(event_address_my, event_address_my_prefix);
        _plog_strncat_id(event_address_my, 70, &local_router->identity);

        all_address_watch_handle = qdr_core_watch_address(core, event_address_all, 'M',
                                                          QD_TREATMENT_MULTICAST_ONCE, _plog_on_all_address_watch, 0, core);
        my_address_watch_handle  = qdr_core_watch_address(core, event_address_my,  'M',
                                                          QD_TREATMENT_MULTICAST_ONCE, _plog_on_my_address_watch, 0, core);
    }
}


/**
 * @brief Module initializer
 * 
 * @param core Pointer to the core object
 * @param adaptor_context (out) Context set for use in finalizer
 */
static void _plog_init(qdr_core_t *core, void **adaptor_context)
{
    router_core = core;
    hostname = getenv("HOSTNAME");
    size_t hostLength = !!hostname ? strlen(hostname) : 0;

    site_id = getenv("SKUPPER_SITE_ID");

    //
    // If the hostname is in the form of a Kubernetes pod name, use the 5-character
    // suffix as the router-id.  Otherwise, generate a random router-id.
    //
    if (hostLength > ROUTER_ID_SIZE && hostname[hostLength - ROUTER_ID_SIZE] == '-') {
        //
        // This memcpy copies the suffix and the terminating null character.
        //
        memcpy(router_id, hostname + (hostLength - ROUTER_ID_SIZE) + 1, ROUTER_ID_SIZE);
    } else {
        //
        // If the router-id size is ever greater than the discriminator size, the
        // generation of router-ids will need to be re-written to use multiple
        // discriminators.
        //
        assert(QD_DISCRIMINATOR_SIZE > ROUTER_ID_SIZE);
        char discriminator[QD_DISCRIMINATOR_SIZE];
        qd_generate_discriminator(discriminator);
        memcpy(router_id, discriminator, ROUTER_ID_SIZE - 1);
        router_id[ROUTER_ID_SIZE - 1] = '\0';
    }

    router_area = qdr_core_dispatch(core)->router_area;
    router_name = qdr_core_dispatch(core)->router_id;

    for (int slot = 0; slot < FLUSH_SLOT_COUNT; slot++) {
        DEQ_INIT(unflushed_records[slot]);
    }

    log       = qd_log_source("FLOW_LOG");
    lock      = sys_mutex();
    id_lock   = sys_mutex();
    condition = sys_cond();
    thread    = sys_thread(_plog_thread, core);
    *adaptor_context = core;

    _plog_create_router_record();

    plog_work_t *work = _plog_work(_plog_init_address_watch_TH);
    work->value.pointer = core;
    _plog_post_work(work);

    beacon_timer = qd_timer(qdr_core_dispatch(core), _plog_on_beacon, core);
    flush_timer  = qd_timer(qdr_core_dispatch(core), _plog_on_flush,  core);
    qd_timer_schedule(flush_timer, initial_flush_interval_msec);
}


/**
 * @brief Module finalizer
 * 
 * @param adaptor_context Contains the core module pointer
 */
static void _plog_final(void *adaptor_context)
{
    qdr_core_t *core = (qdr_core_t*) adaptor_context;

    qd_timer_free(beacon_timer);
    beacon_timer = 0;

    qd_timer_free(flush_timer);
    flush_timer = 0;

    //
    // Cancel the address watches
    //
    qdr_core_unwatch_address(core, all_address_watch_handle);
    qdr_core_unwatch_address(core, my_address_watch_handle);

    //
    // Signal the thread to exit by posting a NULL work pointer
    //
    _plog_post_work(_plog_work(0));

    //
    // Join and free the thread
    //
    sys_thread_join(thread);
    sys_thread_free(thread);

    //
    // Free the allocated my-address
    //
    free(event_address_my);

    //
    // Free the condition and lock variables
    //
    sys_cond_free(condition);
    sys_mutex_free(lock);
    sys_mutex_free(id_lock);
}


QDR_CORE_ADAPTOR_DECLARE_ORD("Protocol Logging", _plog_init, _plog_final, 10)

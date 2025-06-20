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

#include "qpid/dispatch/vanflow.h"
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
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/error.h"
#include "entity.h"
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
#define DEFERRED_DELETION_TICKS 25  // Five seconds
#define VFLOW_ID_CUSTOM 0xffffffffffffffff

// If the number of discretionary records rises above
// this threshold, stop production of them to avoid
// excessive memory growth.
#define DISCRETIONARY_RECORDS_STOP_THRESHOLD    5000

// If the number of discretionary records falls below
// this threshold, allow their production to start again.
#define DISCRETIONARY_RECORDS_START_THRESHOLD   4990

//
// If the record_id value is VFLOW_ID_CUSTOM, use the full_id for arbitrary strings, otherwise use ${s.source_id}:${record_id}
//
typedef struct vflow_identity_t {
    uint64_t record_id;
    union {
        char  source_id[ROUTER_ID_SIZE];
        char *full_id;
    } s;
} vflow_identity_t;

typedef struct vflow_attribute_data_t {
    DEQ_LINKS(struct vflow_attribute_data_t);
    vflow_attribute_t  attribute_type;
    uint32_t          emit_ordinal;
    union {
        uint64_t  uint_val;
        char     *string_val;
    } value;
} vflow_attribute_data_t;

ALLOC_DECLARE(vflow_attribute_data_t);
ALLOC_DEFINE(vflow_attribute_data_t);
DEQ_DECLARE(vflow_attribute_data_t, vflow_attribute_data_list_t);

typedef struct vflow_rate_t {
    DEQ_LINKS(struct vflow_rate_t);
    DEQ_LINKS_N(PER_RECORD, struct vflow_rate_t);
    uint64_t                slot[RATE_SLOT_COUNT];
    int                     slot_cursor;
    uint64_t                last_rate;
    vflow_record_t         *record;
    vflow_attribute_t       rate_attribute;
    vflow_attribute_data_t *count_attribute;
} vflow_rate_t;

ALLOC_DECLARE(vflow_rate_t);
ALLOC_DEFINE(vflow_rate_t);
DEQ_DECLARE(vflow_rate_t, vflow_rate_list_t);
DEQ_DECLARE(vflow_record_t, vflow_record_list_t);

struct vflow_record_t {
    DEQ_LINKS(vflow_record_t);
    DEQ_LINKS_N(UNFLUSHED, vflow_record_t);
    vflow_record_type_t          record_type;
    vflow_record_t              *parent;
    vflow_record_t              *co_record_peer;  // Only used when record and co-record are on the same source
    vflow_record_list_t          children;
    vflow_identity_t             identity;
    vflow_attribute_data_list_t  attributes;
    vflow_rate_list_t            rates;
    uint64_t                     latency_start;
    uint32_t                     emit_ordinal;
    uint32_t                     delete_tick;
    int                          flush_slot;
    int                          default_flush_slot;
    bool                         never_logged;
    bool                         force_log;
    bool                         ended;
    bool                         co_record;
};

ALLOC_DECLARE(vflow_record_t);
ALLOC_DEFINE(vflow_record_t);

typedef struct vflow_work_t vflow_work_t;

typedef void (*vflow_work_handler_t) (vflow_work_t *work, bool discard);

struct vflow_work_t {
    DEQ_LINKS(vflow_work_t);
    vflow_work_handler_t  handler;
    vflow_record_t       *record;
    uint64_t              value64;
    vflow_attribute_t     attribute;
    union {
        char                        *string_val;
        uint64_t                     int_val;
        void                        *pointer;
        bool                         bool_val;
        vflow_attribute_data_list_t  attributes;
    } value;
};

ALLOC_DECLARE(vflow_work_t);
ALLOC_DEFINE(vflow_work_t);
DEQ_DECLARE(vflow_work_t, vflow_work_list_t);

static const char *event_address_all           = "mc/sfe.all";
static const char *event_address_my_prefix     = "mc/sfe.";
static const char *command_address_prefix      = "sfe.";
static const char *co_record_address_prefix    = "vfcr.";
static const int   heartbeat_interval_sec      = 2;
static const int   heartbeats_per_beacon       = 5;
static const int   flush_interval_msec         = 200;
static const int   initial_flush_interval_msec = 2000;
static const int   rate_slot_flush_intervals   = 10;    // For a two-second slot interval
static const int   rate_span                   = 10;    // Ten-second rolling average

static sys_atomic_t site_configured;

typedef struct {
    // How many records of types that support discretionary currently exist?
    // Their creation can be interrupted if necessary to avoid excessive memory growth.
    sys_atomic_t         discretionary_record_count;
    sys_atomic_t         emit_discretionary_records;
    qdr_core_t          *router_core;
    sys_mutex_t          lock;
    sys_mutex_t          id_lock;
    sys_cond_t           condition;
    sys_thread_t        *thread;
    char                *event_address_my;
    char                *event_address_my_flow;
    char                *event_address_my_log;
    char                *command_address;
    char                *co_record_address;
    char                *router_mode;
    bool                 sleeping;
    vflow_work_list_t    work_list;
    vflow_record_t      *local_root;
    vflow_record_t      *local_site;
    vflow_record_t      *local_router;
    char                *local_router_id;
    vflow_record_list_t  co_records;
    vflow_record_list_t  unflushed_flow_records[FLUSH_SLOT_COUNT];
    vflow_record_list_t  unflushed_log_records[FLUSH_SLOT_COUNT];
    vflow_record_list_t  unflushed_records[FLUSH_SLOT_COUNT];  // not flow or log records
    vflow_record_list_t  unflushed_co_records[FLUSH_SLOT_COUNT];
    vflow_record_list_t  to_delete_records;
    vflow_rate_list_t    rate_trackers;
    uint32_t             current_tick;
    int                  current_flush_slot;
    char                *site_id;
    char                *hostname;
    char                 router_id[ROUTER_ID_SIZE];
    uint64_t             next_identity;
    const char          *router_area;
    const char          *router_name;
    qdr_watch_handle_t   all_address_watch_handle;
    qdr_watch_handle_t   my_address_watch_handle;
    qdr_watch_handle_t   my_flow_address_watch_handle;
    qdr_watch_handle_t   my_log_address_watch_handle;
    qdr_subscription_t  *command_subscription;
    qdr_subscription_t  *co_record_subscription;
    bool                 all_address_usable;
    bool                 my_address_usable;
    bool                 my_flow_address_usable;
    bool                 my_log_address_usable;
    qd_timer_t          *heartbeat_timer;
    qd_timer_t          *flush_timer;
    uint64_t             next_message_id;
} vflow_state_t;

static vflow_state_t *state;

static void _vflow_set_string_TH(vflow_work_t *work, bool discard);
static void _vflow_set_int_TH(vflow_work_t *work, bool discard);

#define ATTR_REF      1
#define ATTR_UINT     2
#define ATTR_COUNTER  4
#define ATTR_STRING   8
#define ATTR_TRACE    16
#define ATTR_UCOUNT   (ATTR_UINT | ATTR_COUNTER)

static uint8_t valid_attribute_types[] = {
    ATTR_UINT,   ATTR_REF,    ATTR_REF,    ATTR_UINT,
    ATTR_UINT,   ATTR_REF,    ATTR_REF,    ATTR_REF,
    ATTR_UINT,   ATTR_STRING, ATTR_STRING, ATTR_STRING,
    ATTR_STRING, ATTR_STRING, ATTR_STRING, ATTR_STRING,
    ATTR_STRING, ATTR_STRING, ATTR_STRING, ATTR_STRING,
    ATTR_STRING, ATTR_STRING, ATTR_STRING, ATTR_UCOUNT,
    ATTR_UINT,   ATTR_UINT,   ATTR_UINT,   ATTR_STRING,
    ATTR_STRING, ATTR_STRING, ATTR_STRING, ATTR_TRACE,
    ATTR_STRING, ATTR_UINT,   ATTR_STRING, ATTR_UINT,
    ATTR_UCOUNT, ATTR_UINT,   ATTR_UCOUNT, ATTR_UINT,
    ATTR_UCOUNT, ATTR_UCOUNT, ATTR_UINT,   ATTR_UINT,
    ATTR_UINT,   ATTR_REF,    ATTR_REF,    ATTR_UINT,
    ATTR_UINT,   ATTR_STRING, ATTR_STRING, ATTR_UINT,
    ATTR_UCOUNT, ATTR_STRING, ATTR_STRING, ATTR_UINT,
    ATTR_UINT,   ATTR_UCOUNT, ATTR_UCOUNT, ATTR_UINT,
    ATTR_REF,    ATTR_UINT,   ATTR_STRING, ATTR_STRING,
    ATTR_STRING, ATTR_STRING, ATTR_UINT
};

/**
 * @brief Return the current timestamp in microseconds
 *
 * @return uint64_t
 */
static uint64_t _now_in_usec(void)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (uint64_t) tv.tv_usec + (uint64_t) (1000000L * (uint64_t) tv.tv_sec);
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
static vflow_attribute_data_t* _vflow_find_attribute(vflow_record_t *record, vflow_attribute_t attr)
{
    vflow_attribute_data_t *data = DEQ_TAIL(record->attributes);

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
static void _vflow_next_id(vflow_identity_t *identity)
{
    sys_mutex_lock(&state->id_lock);
    identity->record_id = state->next_identity++;
    memcpy(identity->s.source_id, state->router_id, ROUTER_ID_SIZE);
    sys_mutex_unlock(&state->id_lock);
}


static char *_vflow_id_to_new_string(const vflow_identity_t *identity)
{
    char *result;
    if (identity->record_id == VFLOW_ID_CUSTOM) {
        result = qd_strdup(identity->s.full_id);
    } else {
        result = (char*) malloc(IDENTITY_MAX);
        snprintf(result, IDENTITY_MAX, "%s:%"PRIu64, identity->s.source_id, identity->record_id);
    }
    return result;
}


/**
 * @brief Concatenate the string representation of an id onto a string.
 *
 * @param buffer Target string for concatenation
 * @param n String size limit
 * @param id Identity to be string encoded
 */
static void _vflow_strncat_id(char *buffer, size_t n, const vflow_identity_t *id)
{
    if (id->record_id == VFLOW_ID_CUSTOM) {
        strcat(buffer, id->s.full_id);
    } else {
        char text[IDENTITY_MAX + 1];
        snprintf(text, IDENTITY_MAX, "%s:%"PRIu64, id->s.source_id, id->record_id);
        strncat(buffer, text, n);
    }
}


/**
 * @brief Parse the contents of an iterator into an identity object.  Return true iff successful.
 *
 * @param identity Pointer to the identity object to write to
 * @param iter Iterator for a string representation of an identity 
 * @return true if the parse was valid
 * @return false if the parse failed
 */
static bool _vflow_parse_id_iter(vflow_identity_t *identity, qd_iterator_t *iter)
{
    identity->record_id = 0;
    size_t source_size = qd_iterator_ncopy(iter, (uint8_t*) identity->s.source_id, ROUTER_ID_SIZE - 1);
    identity->s.source_id[source_size] = '\0';
    if (source_size != ROUTER_ID_SIZE - 1) {
        return false;
    }
    const char colon = qd_iterator_octet(iter);
    if (colon != ':') {
        return false;
    }

    size_t total_size = ROUTER_ID_SIZE + 1;
    while (!qd_iterator_end(iter)) {
        total_size++;
        if (total_size > IDENTITY_MAX) {
            return false;
        }
        const unsigned char digit = qd_iterator_octet(iter);

        if (digit < '0' || digit > '9') {
            return false;
        }
        identity->record_id = (identity->record_id * 10) + (digit - '0');
    }

    return identity->record_id != VFLOW_ID_CUSTOM;
}


/**
 * @brief Concatenate the attribute name onto a string.
 *
 * @param buffer Target string for concatenation
 * @param n String size limit
 * @param data Data value to extrace the attribute-type from
 */
static void _vflow_strncat_attribute(char *buffer, size_t n, const vflow_attribute_data_t *data)
{
#define ATTR_TEXT_MAX 65
    char  text[ATTR_TEXT_MAX + 1];
    char *text_ptr = text;

    text[0] = '\0';

    if (valid_attribute_types[data->attribute_type] & ATTR_UINT) {
        sprintf(text, "%"PRIu64, data->value.uint_val);
    } else if (valid_attribute_types[data->attribute_type] & (ATTR_STRING | ATTR_TRACE | ATTR_REF)) {
        text_ptr = data->value.string_val;
    }

    strncat(buffer, text_ptr, n);
}


static void _vflow_compose_attribute(qd_composed_field_t *field, const vflow_attribute_data_t *data)
{
    if (valid_attribute_types[data->attribute_type] & ATTR_UINT) {
        qd_compose_insert_ulong(field, data->value.uint_val);
    } else if (valid_attribute_types[data->attribute_type] & (ATTR_STRING | ATTR_TRACE | ATTR_REF)) {
        qd_compose_insert_string(field, data->value.string_val);
    }
}


/**
 * @brief Schedule a record, and if needed, its ancestors for flushing.
 *
 * @param record Pointer to the record to be flushed
 */
static void _vflow_post_flush_record_TH(vflow_record_t *record)
{
    if (!!record->parent && record->parent->never_logged) {
        record->parent->force_log = true;
        if (record != record->parent) {
            _vflow_post_flush_record_TH(record->parent);
        }
    }

    //
    // Check for deferred deletion.  This happens if the record is a BIFLOW_TPORT and is not a co-record.
    // The deferral is used to improve the probability that co-record updates will arrive before the record's
    // last update is emitted.
    //
    if (record->ended && record->record_type == VFLOW_RECORD_BIFLOW_TPORT && !record->co_record) {
        if (record->flush_slot != -1) {
            DEQ_REMOVE_N(UNFLUSHED, state->unflushed_flow_records[record->flush_slot], record);
            record->flush_slot = -1;
        }

        if (record->delete_tick == 0) {
            record->delete_tick = state->current_tick + DEFERRED_DELETION_TICKS;
            DEQ_INSERT_TAIL_N(UNFLUSHED, state->to_delete_records, record);
        }
    }
    
    else if (record->flush_slot == -1) {
        if (record->default_flush_slot == -1) {
            record->default_flush_slot = state->current_flush_slot;
        }
        record->flush_slot = record->default_flush_slot;
        if (record->co_record) {
            DEQ_INSERT_TAIL_N(UNFLUSHED, state->unflushed_co_records[record->flush_slot], record);
        } else {
            switch (record->record_type) {
                case VFLOW_RECORD_FLOW:
                case VFLOW_RECORD_BIFLOW_APP:
                case VFLOW_RECORD_BIFLOW_TPORT:
                    DEQ_INSERT_TAIL_N(UNFLUSHED, state->unflushed_flow_records[record->flush_slot], record);
                    break;
                case VFLOW_RECORD_LOG:
                    DEQ_INSERT_TAIL_N(UNFLUSHED, state->unflushed_log_records[record->flush_slot], record);
                    break;
                default:
                    DEQ_INSERT_TAIL_N(UNFLUSHED, state->unflushed_records[record->flush_slot], record);
                    break;
            }
        }
    }
}


static vflow_record_t *_vflow_find_biflow_TH(uint64_t record_id)
{
    vflow_record_t *router = state->local_router;
    vflow_record_t *biflow = 0;
    vflow_record_t *tier_one = DEQ_HEAD(router->children);
    while (!!tier_one && !biflow) {
        if (tier_one->record_type == VFLOW_RECORD_LISTENER) {
            biflow = DEQ_HEAD(tier_one->children);
            while (!!biflow) {
                if (biflow->identity.record_id == record_id) {
                    return biflow;
                }
                biflow = DEQ_NEXT(biflow);
            }
        }
        tier_one = DEQ_NEXT(tier_one);
    }

    return 0;
}


/**
 * @brief Work handler for vflow_start_record
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _vflow_start_record_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    vflow_record_t *record = work->record;

    if (!record->co_record) {
        //
        // If the record type is ROUTER, this is the local-router record.  Store it.
        // If the record type is SITE, this router owns the site records as well, link it in.
        // Otherwise, if the parent is not specified, use the local_router as the parent.
        //
        // Note that the first record to be inserted will always be the ROUTER record.
        //
        if (record->record_type == VFLOW_RECORD_ROUTER) {
            state->local_router = record;
            state->local_root   = record;
            state->local_router_id = _vflow_id_to_new_string(&record->identity);
        } else if (record->record_type == VFLOW_RECORD_SITE) {
            state->local_site = record;
            state->local_root = record;

            //
            // Update the existing router record to have the site record as its parent
            //
            assert(!!state->local_router);
            state->local_router->parent = record;
            DEQ_INSERT_TAIL(record->children, state->local_router);
            _vflow_post_flush_record_TH(state->local_router);
        } else if (record->parent == 0) {
            record->parent = state->local_router;
        }

        //
        // Record the creation timestamp in the record.  Log records provide their own start timestamp
        //
        if (record->record_type != VFLOW_RECORD_LOG) {
            vflow_work_t sub_work;
            sub_work.attribute = VFLOW_ATTRIBUTE_START_TIME;
            sub_work.record    = record;
            sub_work.value.int_val = work->value64;
            _vflow_set_int_TH(&sub_work, false);
        }

        //
        // Record the parent reference.
        //
        if (!!record->parent) {
            vflow_work_t sub_work;
            sub_work.attribute = VFLOW_ATTRIBUTE_PARENT;
            sub_work.record    = record;
            sub_work.value.string_val = _vflow_id_to_new_string(&record->parent->identity);
            _vflow_set_string_TH(&sub_work, false);
        }

        //
        // Place the new record on the parent's list of children
        //
        if (!!record->parent) {
            DEQ_INSERT_TAIL(record->parent->children, record);
        }
    } else {
        //
        // It's a co-record.  Store it in the co-record list.
        //
        DEQ_INSERT_TAIL(state->co_records, record);

        //
        // Check to see if this co-record's base record is from the same source.  If so,
        // we must find the base record and link it to the co-record for local processing.
        //
        if (strncmp(record->identity.s.source_id, state->router_id, ROUTER_ID_SIZE) == 0) {
            vflow_record_t *base_record = _vflow_find_biflow_TH(record->identity.record_id);
            if (!!base_record) {
                base_record->co_record_peer = record;
                record->co_record_peer      = base_record;
            }
        }
    }

    //
    // Schedule this record for flushing
    //
    _vflow_post_flush_record_TH(record);
}


/**
 * @brief Work handler for vflow_end_record
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _vflow_end_record_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    vflow_record_t *record = work->record;

    //
    // Remove any co-record linkage
    //
    if (!!record->co_record_peer) {
        record->co_record_peer->co_record_peer = 0;
        record->co_record_peer = 0;
    }

    //
    // Record the deletion timestamp in the record. Log records provide their own end timestamp
    //
    if (record->record_type != VFLOW_RECORD_LOG && !record->co_record) {
        vflow_work_t sub_work;
        sub_work.attribute = VFLOW_ATTRIBUTE_END_TIME;
        sub_work.record    = record;
        sub_work.value.int_val = work->value64;
        _vflow_set_int_TH(&sub_work, false);
    }

    //
    // Mark the record as ended to designate the lifecycle end
    //
    record->ended = true;

    //
    // Schedule this record for flushing
    //
    _vflow_post_flush_record_TH(record);

    //
    // Free any rate trackers on this record
    //
    vflow_rate_t *rate = DEQ_HEAD(record->rates);
    while (!!rate) {
        DEQ_REMOVE(state->rate_trackers, rate);
        DEQ_REMOVE_N(PER_RECORD, record->rates, rate);
        free_vflow_rate_t(rate);
        rate = DEQ_HEAD(record->rates);
    }
}


/**
 * @brief When writing to a record, it is sometimes appropriate to write a different record than
 * was requested.  This is true in the case where a co-record's base record is in the same source.
 * When this happens, we wish to simply update the attributes of the base record.
 *
 * @param record Requested record
 * @return vflow_record_t* Actual record to modify
 */
static vflow_record_t *_vflow_record_to_use_TH(vflow_record_t *record)
{
    if (record->co_record && !!record->co_record_peer) {
        return record->co_record_peer;
    }

    return record;
}


/**
 * @brief Work handler for vflow_set_string
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _vflow_set_string_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        free(work->value.string_val);
        return;
    }

    vflow_record_t         *record = _vflow_record_to_use_TH(work->record);
    vflow_attribute_data_t *insert = _vflow_find_attribute(record, work->attribute);
    vflow_attribute_data_t *data;

    if (!insert || insert->attribute_type != work->attribute) {
        //
        // The attribute does not exist, create a new one and insert appropriately
        //
        data = new_vflow_attribute_data_t();
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

    //
    // Schedule this record for flushing
    //
    _vflow_post_flush_record_TH(record);
}


/**
 * @brief Work handler for vflow_set_int
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _vflow_set_int_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    vflow_record_t         *record = _vflow_record_to_use_TH(work->record);
    vflow_attribute_data_t *insert = _vflow_find_attribute(record, work->attribute);
    vflow_attribute_data_t *data;
    bool                    changed = true;

    if (!insert || insert->attribute_type != work->attribute) {
        //
        // The attribute does not exist, create a new one and insert appropriately
        //
        data = new_vflow_attribute_data_t();
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
        changed = insert->value.uint_val != work->value.int_val;
        insert->value.uint_val = work->value.int_val;
        insert->emit_ordinal   = record->emit_ordinal;
    }

    //
    // Schedule this record for flushing
    //
    if (changed) {
        _vflow_post_flush_record_TH(record);
    }
}


/**
 * @brief Work handler for vflow_inc_counter
 * 
 * @param work Pointer to work context
 * @param discard Indicator that this work must be discarded
 */
static void _vflow_inc_int_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    vflow_record_t         *record = _vflow_record_to_use_TH(work->record);
    vflow_attribute_data_t *insert = _vflow_find_attribute(record, work->attribute);
    vflow_attribute_data_t *data;

    if (!insert || insert->attribute_type != work->attribute) {
        //
        // The attribute does not exist, create a new one and insert appropriately
        //
        data = new_vflow_attribute_data_t();
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
        // The attribute already exists, increment the value
        //
        insert->value.uint_val += work->value.int_val;
        insert->emit_ordinal   = record->emit_ordinal;
    }

    //
    // Schedule this record for flushing
    //
    _vflow_post_flush_record_TH(record);
}


/**
 * @brief Allocate a work object pre-loaded with a handler.
 * 
 * @param handler The handler to be called on the vflow thread to do the work
 * @return vflow_work_t* Pointer to the allocated work that should be posted for processing
 */
static vflow_work_t *_vflow_work(vflow_work_handler_t handler)
{
    vflow_work_t *work = new_vflow_work_t();
    ZERO(work);
    work->handler = handler;
    return work;
}


/**
 * @brief Post work for processing in the vflow thread
 * 
 * @param work Pointer to the work to be processed
 */
static void _vflow_post_work(vflow_work_t *work)
{
    sys_mutex_lock(&state->lock);
    DEQ_INSERT_TAIL(state->work_list, work);
    bool need_signal = state->sleeping;
    sys_mutex_unlock(&state->lock);

    if (need_signal) {
        sys_cond_signal(&state->condition);
    }
}


/**
 * @brief Create the record that represents the local router.
 */
static void _vflow_create_router_record(void)
{
    vflow_record_t *router = vflow_start_record(VFLOW_RECORD_ROUTER, 0);

    const char *namespace  = getenv("POD_NAMESPACE");
    const char *image_name = getenv("APPLICATION_NAME");
    const char *version    = getenv("VERSION");

    char *name = (char*) malloc(strlen(state->router_area) + strlen(state->router_name) + 2);
    strcpy(name, state->router_area);
    strcat(name, "/");
    strcat(name, state->router_name);
    vflow_set_string(router, VFLOW_ATTRIBUTE_NAME, name);
    free(name);

    if (!!state->site_id) {
        vflow_set_string(router, VFLOW_ATTRIBUTE_PARENT, state->site_id);
    }

    if (!!state->hostname) {
        vflow_set_string(router, VFLOW_ATTRIBUTE_HOST_NAME, state->hostname);
    }

    if (!!namespace) {
        vflow_set_string(router, VFLOW_ATTRIBUTE_NAMESPACE, namespace);
    }

    if (!!image_name) {
        vflow_set_string(router, VFLOW_ATTRIBUTE_IMAGE_NAME, image_name);
    }

    if (!!version) {
        vflow_set_string(router, VFLOW_ATTRIBUTE_IMAGE_VERSION, version);
    }

    vflow_set_string(router, VFLOW_ATTRIBUTE_BUILD_VERSION, QPID_DISPATCH_VERSION);
    vflow_set_string(router, VFLOW_ATTRIBUTE_MODE, state->router_mode);
}


/**
 * @brief Recursively free the given record and all of its children
 * 
 * @param record Pointer to the record to be freed.
 * @param recursive If true, delete recursively, otherwise just remove parent references.
 */
static void _vflow_free_record_TH(vflow_record_t *record, bool recursive)
{
    // If this is one of the record types that supports discretionary functionality,
    // count it. If the number currently existing is now below the
    // resumption threshold, indicate that we should resume producing them.
    if (discretionary_records[record->record_type]) {
        if (sys_atomic_dec(&state->discretionary_record_count) <= DISCRETIONARY_RECORDS_START_THRESHOLD) {
            sys_atomic_set(&state->emit_discretionary_records, 1);
        }
    }

    //
    // If this record is a child of a parent, remove it from the parent's child list
    //
    if (!!record->parent) {
        DEQ_REMOVE(record->parent->children, record);
    }

    if (record->co_record) {
        DEQ_REMOVE(state->co_records, record);
    }

    //
    // Remove the record from the unflushed list if needed
    //
    if (record->flush_slot >= 0) {
        if (record->co_record) {
            DEQ_REMOVE_N(UNFLUSHED, state->unflushed_co_records[record->flush_slot], record);
        } else {
            switch (record->record_type) {
                case VFLOW_RECORD_FLOW:
                case VFLOW_RECORD_BIFLOW_APP:
                case VFLOW_RECORD_BIFLOW_TPORT:
                    DEQ_REMOVE_N(UNFLUSHED, state->unflushed_flow_records[record->flush_slot], record);
                    break;
                case VFLOW_RECORD_LOG:
                    DEQ_REMOVE_N(UNFLUSHED, state->unflushed_log_records[record->flush_slot], record);
                    break;
                default:
                    DEQ_REMOVE_N(UNFLUSHED, state->unflushed_records[record->flush_slot], record);
                    break;
            }
        }
        record->flush_slot = -1;
    } else if (record->delete_tick > 0) {
        DEQ_REMOVE_N(UNFLUSHED, state->to_delete_records, record);
        record->delete_tick = 0;
    }

    if (recursive) {
        //
        // Remove all of this record's children
        //
        while (!DEQ_IS_EMPTY(record->children)) {
            _vflow_free_record_TH(DEQ_HEAD(record->children), true);
        }
    } else {
        //
        // Remove the childrens' parent references
        //
        vflow_record_t *child = DEQ_HEAD(record->children);
        while (!!child) {
            child->parent = 0;
            child = DEQ_NEXT(child);
        }
    }

    //
    // Free all of this record's attributes
    //
    vflow_attribute_data_t *data = DEQ_HEAD(record->attributes);
    while (!!data) {
        DEQ_REMOVE_HEAD(record->attributes);
        if (valid_attribute_types[data->attribute_type] & (ATTR_STRING | ATTR_TRACE | ATTR_REF)) {
            free(data->value.string_val);
        }
        free_vflow_attribute_data_t(data);
        data = DEQ_HEAD(record->attributes);
    }

    //
    // Free the full-id if appropriate
    //
    if (record->identity.record_id == VFLOW_ID_CUSTOM) {
        free(record->identity.s.full_id);
    }

    //
    // Free the record
    //
    free_vflow_record_t(record);
}

// clang-format off
static const char *_vflow_record_type_name(const vflow_record_t *record)
{
    switch (record->record_type) {
    case VFLOW_RECORD_SITE          : return "SITE";
    case VFLOW_RECORD_ROUTER        : return "ROUTER";
    case VFLOW_RECORD_LINK          : return "LINK";
    case VFLOW_RECORD_CONTROLLER    : return "CONTROLLER";
    case VFLOW_RECORD_LISTENER      : return "LISTENER";
    case VFLOW_RECORD_CONNECTOR     : return "CONNECTOR";
    case VFLOW_RECORD_FLOW          : return "FLOW";
    case VFLOW_RECORD_PROCESS       : return "PROCESS";
    case VFLOW_RECORD_IMAGE         : return "IMAGE";
    case VFLOW_RECORD_INGRESS       : return "INGRESS";
    case VFLOW_RECORD_EGRESS        : return "EGRESS";
    case VFLOW_RECORD_COLLECTOR     : return "COLLECTOR";
    case VFLOW_RECORD_PROCESS_GROUP : return "PROCESS_GROUP";
    case VFLOW_RECORD_HOST          : return "HOST";
    case VFLOW_RECORD_LOG           : return "LOG";
    case VFLOW_RECORD_ROUTER_ACCESS : return "ROUTER_ACCESS";
    case VFLOW_RECORD_BIFLOW_TPORT  : return "BIFLOW_TPORT";
    case VFLOW_RECORD_BIFLOW_APP    : return "BIFLOW_APP";
    }
    return "UNKNOWN";
}


static const char *_vflow_attribute_name(const vflow_attribute_data_t *data)
{
    switch (data->attribute_type) {
    case VFLOW_ATTRIBUTE_RECORD_TYPE          : return "recordType";
    case VFLOW_ATTRIBUTE_IDENTITY             : return "identity";
    case VFLOW_ATTRIBUTE_PARENT               : return "parent";
    case VFLOW_ATTRIBUTE_START_TIME           : return "startTime";
    case VFLOW_ATTRIBUTE_END_TIME             : return "endTime";
    case VFLOW_ATTRIBUTE_COUNTERFLOW          : return "counterflow";
    case VFLOW_ATTRIBUTE_PEER                 : return "peer";
    case VFLOW_ATTRIBUTE_PROCESS              : return "process";
    case VFLOW_ATTRIBUTE_SIBLING_ORDINAL      : return "sibOrdinal";
    case VFLOW_ATTRIBUTE_LOCATION             : return "location";
    case VFLOW_ATTRIBUTE_PROVIDER             : return "provider";
    case VFLOW_ATTRIBUTE_PLATFORM             : return "platform";
    case VFLOW_ATTRIBUTE_NAMESPACE            : return "namespace";
    case VFLOW_ATTRIBUTE_MODE                 : return "mode";
    case VFLOW_ATTRIBUTE_SOURCE_HOST          : return "sourceHost";
    case VFLOW_ATTRIBUTE_DESTINATION_HOST     : return "destHost";
    case VFLOW_ATTRIBUTE_PROTOCOL             : return "protocol";
    case VFLOW_ATTRIBUTE_SOURCE_PORT          : return "sourcePort";
    case VFLOW_ATTRIBUTE_DESTINATION_PORT     : return "destPort";
    case VFLOW_ATTRIBUTE_VAN_ADDRESS          : return "vanAddress";
    case VFLOW_ATTRIBUTE_IMAGE_NAME           : return "imageName";
    case VFLOW_ATTRIBUTE_IMAGE_VERSION        : return "imageVersion";
    case VFLOW_ATTRIBUTE_HOST_NAME            : return "hostname";
    case VFLOW_ATTRIBUTE_OCTETS               : return "octets";
    case VFLOW_ATTRIBUTE_LATENCY              : return "latency";
    case VFLOW_ATTRIBUTE_TRANSIT_LATENCY      : return "transitLatency";
    case VFLOW_ATTRIBUTE_BACKLOG              : return "backlog";
    case VFLOW_ATTRIBUTE_METHOD               : return "method";
    case VFLOW_ATTRIBUTE_RESULT               : return "result";
    case VFLOW_ATTRIBUTE_REASON               : return "reason";
    case VFLOW_ATTRIBUTE_NAME                 : return "name";
    case VFLOW_ATTRIBUTE_TRACE                : return "trace";
    case VFLOW_ATTRIBUTE_BUILD_VERSION        : return "buildVersion";
    case VFLOW_ATTRIBUTE_LINK_COST            : return "linkCost";
    case VFLOW_ATTRIBUTE_DIRECTION            : return "direction";
    case VFLOW_ATTRIBUTE_OCTET_RATE           : return "octetRate";
    case VFLOW_ATTRIBUTE_OCTETS_OUT           : return "octetsOut";
    case VFLOW_ATTRIBUTE_OCTETS_UNACKED       : return "octetsUnacked";
    case VFLOW_ATTRIBUTE_WINDOW_CLOSURES      : return "windowClosures";
    case VFLOW_ATTRIBUTE_WINDOW_SIZE          : return "windowSize";
    case VFLOW_ATTRIBUTE_FLOW_COUNT_L4        : return "flowCountL4";
    case VFLOW_ATTRIBUTE_FLOW_COUNT_L7        : return "flowCountL7";
    case VFLOW_ATTRIBUTE_FLOW_RATE_L4         : return "flowRateL4";
    case VFLOW_ATTRIBUTE_FLOW_RATE_L7         : return "flowRateL7";
    case VFLOW_ATTRIBUTE_DURATION             : return "duration";
    case VFLOW_ATTRIBUTE_IMAGE                : return "image";
    case VFLOW_ATTRIBUTE_GROUP                : return "group";
    case VFLOW_ATTRIBUTE_STREAM_ID            : return "streamId";
    case VFLOW_ATTRIBUTE_LOG_SEVERITY         : return "logSeverity";
    case VFLOW_ATTRIBUTE_LOG_TEXT             : return "logText";
    case VFLOW_ATTRIBUTE_SOURCE_FILE          : return "sourceFile";
    case VFLOW_ATTRIBUTE_SOURCE_LINE          : return "sourceLine";
    case VFLOW_ATTRIBUTE_LINK_COUNT           : return "linkCount";
    case VFLOW_ATTRIBUTE_OPER_STATUS          : return "operStatus";
    case VFLOW_ATTRIBUTE_ROLE                 : return "role";
    case VFLOW_ATTRIBUTE_UP_TIMESTAMP         : return "upTimeStamp";
    case VFLOW_ATTRIBUTE_DOWN_TIMESTAMP       : return "downTimeStamp";
    case VFLOW_ATTRIBUTE_DOWN_COUNT           : return "downCount";
    case VFLOW_ATTRIBUTE_OCTETS_REVERSE       : return "octetsReverse";
    case VFLOW_ATTRIBUTE_OCTET_RATE_REVERSE   : return "octetRateReverse";
    case VFLOW_ATTRIBUTE_CONNECTOR            : return "connector";
    case VFLOW_ATTRIBUTE_PROCESS_LATENCY      : return "processLatency";
    case VFLOW_ATTRIBUTE_PROXY_HOST           : return "proxyHost";
    case VFLOW_ATTRIBUTE_PROXY_PORT           : return "proxyPort";
    case VFLOW_ATTRIBUTE_ERROR_LISTENER_SIDE  : return "errorListenerSide";
    case VFLOW_ATTRIBUTE_ERROR_CONNECTOR_SIDE : return "errorConnectorSide";
    case VFLOW_ATTRIBUTE_ACTIVE_TLS_ORDINAL   : return "activeTlsOrdinal";
    }
    return "UNKNOWN";
}
// clang-format on

/**
 * @brief Extract the value of a record identity from its serialized form in an iterator
 * 
 * @param field Pointer to the parsed field containing the serialized identity
 * @return Newly allocated string with identity, or NULL
 */
static char *_vflow_unserialize_identity(qd_parsed_field_t *field)
{
    if (!qd_parse_is_scalar(field)) {
        return 0;
    }

    qd_iterator_t *iter = qd_parse_raw(field);
    return (char*) qd_iterator_copy(iter);
}


/**
 * @brief Emit a single record as a log event
 *
 * @param record Pointer to the record to be emitted
 */
static void _vflow_emit_record_as_log_TH(vflow_record_t *record)
{
    qd_log_level_t log_level = record->record_type == VFLOW_RECORD_FLOW ? QD_LOG_DEBUG : QD_LOG_INFO; // TODO - Add BIFLOWs here
    if (!qd_log_enabled(LOG_FLOW_LOG, log_level))
        return;
#define LINE_MAX 1000
    char line[LINE_MAX + 1];

    strcpy(line, _vflow_record_type_name(record));
    strcat(line, " [");
    _vflow_strncat_id(line, LINE_MAX, &record->identity);
    strcat(line, "]");
    if (record->never_logged) {
        strcat(line, " BEGIN");
    }
    if (record->ended) {
        strcat(line, " END");
    }

    vflow_attribute_data_t *data = DEQ_HEAD(record->attributes);
    while (data) {
        if (data->attribute_type != VFLOW_ATTRIBUTE_START_TIME && data->attribute_type != VFLOW_ATTRIBUTE_END_TIME) {
            strncat(line, " ", LINE_MAX);
            strncat(line, _vflow_attribute_name(data), LINE_MAX);
            strncat(line, "=", LINE_MAX);
            _vflow_strncat_attribute(line, LINE_MAX, data);
        }
        data = DEQ_NEXT(data);
    }

    record->never_logged = false;
    qd_log(LOG_FLOW_LOG, log_level, "%s", line);
}


/**
 * @brief Emit all of the unflushed records as events, batched into message bodies.
 *
 * @param core Pointer to the core module
 */
static void _vflow_emit_unflushed_as_events_TH(qdr_core_t *core, vflow_record_list_t *unflushed_records,
                                               const char *to_address)
{
    if (DEQ_SIZE(*unflushed_records) == 0) {
        return;
    }

    int                  event_count = 0;
    qd_composed_field_t *field = 0;
    vflow_record_t      *record = DEQ_HEAD(*unflushed_records);

    while (!!record) {
        if (field == 0) {
            //
            // Compose a new message content starting with the properties
            //
            field = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
            qd_compose_start_list(field);
            qd_compose_insert_long(field, state->next_message_id++);
            qd_compose_insert_null(field);                             // user-id
            qd_compose_insert_string(field, to_address);               // to
            qd_compose_insert_string(field, "RECORD");                 // subject
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
        qd_compose_insert_uint(field, VFLOW_ATTRIBUTE_RECORD_TYPE);
        qd_compose_insert_uint(field, record->record_type);

        qd_compose_insert_uint(field, VFLOW_ATTRIBUTE_IDENTITY);
        if (record->identity.record_id == VFLOW_ID_CUSTOM) {
            qd_compose_insert_string(field, record->identity.s.full_id);
        } else {
            char identity[IDENTITY_MAX + 1];
            snprintf(identity, IDENTITY_MAX, "%s:%"PRIu64, record->identity.s.source_id, record->identity.record_id);
            qd_compose_insert_string(field, identity);
        }

        vflow_attribute_data_t *data = DEQ_HEAD(record->attributes);
        while (data) {
            if (data->emit_ordinal >= record->emit_ordinal) {
                qd_compose_insert_uint(field, data->attribute_type);
                _vflow_compose_attribute(field, data);
            }
            data = DEQ_NEXT(data);
        }
        qd_compose_end_map(field);

        //
        // Count this event.  If we have reached the maximum events for a batch, or
        // we have reached the end of the unflushed list, close out the current message.
        //
        event_count++;
        if (event_count == EVENT_BATCH_MAX || record == DEQ_TAIL(*unflushed_records)) {
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
            qdr_send_to2(core, event, to_address, true, false);

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


static void _vflow_clean_unflushed_TH(vflow_record_list_t *unflushed_records)
{
    vflow_record_t *record = DEQ_HEAD(*unflushed_records);
    while (!!record) {
        DEQ_REMOVE_HEAD_N(UNFLUSHED, *unflushed_records);
        assert(record->flush_slot >= 0 || record->delete_tick > 0);
        record->flush_slot = -1;
        record->delete_tick = 0;

        //
        // If this record has been ended, emit the log line.
        //
        if ((record->ended || record->force_log) && !record->co_record) {
            record->force_log = false;
            _vflow_emit_record_as_log_TH(record);
        }

        record->emit_ordinal++;
        if (record->ended) {
            _vflow_free_record_TH(record, false);
        }
        record = DEQ_HEAD(*unflushed_records);
    }
}


//
// Co-records are emitted in the same format as base records, except that they are sent directly to the base router.
// This means that the unflushed co-records must be sorted by base router and then sent in batches to each router.
//
static void _vflow_emit_co_records_TH(qdr_core_t *core, vflow_record_list_t *unflushed_records)
{
    vflow_record_list_t working_list;

    while (!DEQ_IS_EMPTY(*unflushed_records)) {
        vflow_record_t *start_record   = DEQ_HEAD(*unflushed_records);
        const char     *base_source_id = start_record->identity.s.source_id;

        //
        // Build a working list of unflushed co-records that belong to the same base source.
        //
        DEQ_INIT(working_list);
        vflow_record_t *record = start_record;
        vflow_record_t *next_record;
        do {
            next_record = DEQ_NEXT_N(UNFLUSHED, record);
            if (strncmp(base_source_id, record->identity.s.source_id, ROUTER_ID_SIZE) == 0) {
                DEQ_REMOVE_N(UNFLUSHED, *unflushed_records, record);
                DEQ_INSERT_TAIL_N(UNFLUSHED, working_list, record);
            }
            record = next_record;
        } while (!!next_record);

        //
        // Only send a message if the co-records have base records on a remote source.
        //
        if (strncmp(base_source_id, state->router_id, ROUTER_ID_SIZE) != 0) {
            //
            // Emit the working list to the base source
            //
            const size_t address_length = ROUTER_ID_SIZE + 7;
            char base_source_address[address_length];
            snprintf(base_source_address, address_length, "%s%s:0", co_record_address_prefix, base_source_id);
            _vflow_emit_unflushed_as_events_TH(core, &working_list, base_source_address);
        }
        _vflow_clean_unflushed_TH(&working_list);
    }
}


/**
 * @brief Emit all of the unflushed records
 * 
 * @param core Pointer to the core module
 */
static void _vflow_flush_TH(qdr_core_t *core, bool no_defer)
{
    //
    // If there is at least one collector for this router, batch up the
    // unflushed records and send them as events to the collector.
    //
    if (state->my_address_usable) {
        _vflow_emit_unflushed_as_events_TH(core, &state->unflushed_records[state->current_flush_slot],
                                           state->event_address_my);
    }

    if (state->my_flow_address_usable) {
        _vflow_emit_unflushed_as_events_TH(core, &state->unflushed_flow_records[state->current_flush_slot],
                                           state->event_address_my_flow);
    }

    if (state->my_log_address_usable) {
        _vflow_emit_unflushed_as_events_TH(core, &state->unflushed_log_records[state->current_flush_slot],
                                           state->event_address_my_log);
    }

    _vflow_emit_co_records_TH(core, &state->unflushed_co_records[state->current_flush_slot]);

    _vflow_clean_unflushed_TH(&state->unflushed_records[state->current_flush_slot]);
    _vflow_clean_unflushed_TH(&state->unflushed_flow_records[state->current_flush_slot]);
    _vflow_clean_unflushed_TH(&state->unflushed_log_records[state->current_flush_slot]);
    _vflow_clean_unflushed_TH(&state->unflushed_co_records[state->current_flush_slot]);

    //
    // Flush records on the deferred-delete list
    //
    vflow_record_list_t delete_list;
    DEQ_INIT(delete_list);
    vflow_record_t *record = DEQ_HEAD(state->to_delete_records);
    while (!!record && (record->delete_tick <= state->current_tick || no_defer)) {
        assert(record->delete_tick > 0);
        DEQ_REMOVE_HEAD_N(UNFLUSHED, state->to_delete_records);
        DEQ_INSERT_TAIL_N(UNFLUSHED, delete_list, record);
        record = DEQ_HEAD(state->to_delete_records);
    }

    //
    // Note that we only use the flow_address for event generation.  Only flow records are delete-deferred.
    //
    if (state->my_flow_address_usable) {
        _vflow_emit_unflushed_as_events_TH(core, &delete_list, state->event_address_my_flow);
    }

    _vflow_clean_unflushed_TH(&delete_list);
}


static void _vflow_send_beacon_TH(vflow_work_t *work, bool discard)
{
    if (!discard) {
        //
        // Compose the message content starting with the properties
        //
        qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(field);
        qd_compose_insert_long(field, state->next_message_id++);
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
        qd_compose_insert_symbol(field, "id");
        qd_compose_insert_string(field, state->local_router_id);
        qd_compose_insert_symbol(field, "address");
        qd_compose_insert_string(field, state->event_address_my);
        qd_compose_insert_symbol(field, "direct");
        qd_compose_insert_string(field, state->command_address);
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
        qdr_send_to2(state->router_core, beacon, event_address_all, true, false);

        //
        // Free up used resources
        //
        qd_compose_free(field);
        qd_message_free(beacon);
    }
}


static void _vflow_send_heartbeat_TH(vflow_work_t *work, bool discard)
{
    if (!discard && state->my_address_usable) {
        //
        // Compose the message content starting with the properties
        //
        qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(field);
        qd_compose_insert_long(field, state->next_message_id++);
        qd_compose_insert_null(field);                             // user-id
        qd_compose_insert_string(field, state->event_address_my);  // to
        qd_compose_insert_string(field, "HEARTBEAT");              // subject
        qd_compose_end_list(field);

        field = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, field);
        qd_compose_start_map(field);
        qd_compose_insert_symbol(field, "v");
        qd_compose_insert_uint(field, 1);
        qd_compose_insert_symbol(field, "now");
        qd_compose_insert_ulong(field, _now_in_usec());
        qd_compose_insert_symbol(field, "id");
        qd_compose_insert_string(field, state->local_router_id);
        qd_compose_end_map(field);

        //
        // Append the body section to the content
        //
        field = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, field);
        qd_compose_insert_null(field);

        //
        // Create a message for the content
        //
        qd_message_t *hbeat = qd_message();
        qd_message_compose_2(hbeat, field, true);

        //
        // Send the message to all of the bound receivers
        //
        qdr_send_to2(state->router_core, hbeat, state->event_address_my, true, false);

        //
        // Free up used resources
        //
        qd_compose_free(field);
        qd_message_free(hbeat);
    }
}


static void _vflow_refresh_record_TH(vflow_record_t *record)
{
    record->emit_ordinal = 0;

    _vflow_post_flush_record_TH(record);

    vflow_record_t *child = DEQ_HEAD(record->children);
    while (!!child) {
        _vflow_refresh_record_TH(child);
        child = DEQ_NEXT(child);
    }
}


static void _vflow_refresh_events_TH(vflow_work_t *work, bool discard)
{
    if (!discard) {
        _vflow_refresh_record_TH(state->local_root);
    }
}


static void _vflow_add_rate_TH(vflow_work_t *work, bool discard)
{
    if (!discard) {
        vflow_rate_t *rate = new_vflow_rate_t();
        ZERO(rate);
        vflow_attribute_data_t *data = DEQ_HEAD(work->record->attributes);
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
        DEQ_INSERT_TAIL(state->rate_trackers, rate);
        DEQ_INSERT_TAIL_N(PER_RECORD, rate->record->rates, rate);
    }
}


static void _vflow_process_rates_TH(void)
{
    vflow_rate_t *rate = DEQ_HEAD(state->rate_trackers);
    while(!!rate) {
        rate->slot[rate->slot_cursor] = rate->count_attribute->value.uint_val;
        uint64_t delta = rate->slot[rate->slot_cursor] - rate->slot[(rate->slot_cursor + 1) % RATE_SLOT_COUNT];
        rate->slot_cursor = (rate->slot_cursor + 1) % RATE_SLOT_COUNT;

        vflow_work_t work;
        work.record        = rate->record;
        work.attribute     = rate->rate_attribute;
        work.value.int_val = delta / rate_span;
        _vflow_set_int_TH(&work, false);

        rate = DEQ_NEXT(rate);
    }
}


static void _vflow_tick_TH(vflow_work_t *work, bool discard)
{
    static int tick_ordinal = 0;
    if (!discard) {
        state->current_tick++;
        _vflow_flush_TH(state->router_core, false);
        state->current_flush_slot = (state->current_flush_slot + 1) % FLUSH_SLOT_COUNT;

        tick_ordinal = (tick_ordinal + 1) % rate_slot_flush_intervals;
        if (tick_ordinal == 0) {
            _vflow_process_rates_TH();
        }
    }
}


static void _vflow_on_flush(void *context)
{
    vflow_work_t *work = _vflow_work(_vflow_tick_TH);
    _vflow_post_work(work);
    qd_timer_schedule(state->flush_timer, flush_interval_msec);
}


static void _vflow_send_heartbeat(void)
{
    static int counter = 0;
    if (!!state->heartbeat_timer) {
        _vflow_post_work(_vflow_work(_vflow_send_heartbeat_TH));
        counter = (counter + 1) % heartbeats_per_beacon;
        if (counter == 0) {
            _vflow_post_work(_vflow_work(_vflow_send_beacon_TH));
        }
        qd_timer_schedule(state->heartbeat_timer, heartbeat_interval_sec * 1000);
    }
}


static void _vflow_all_address_status_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    bool now_usable = work->value.bool_val;
    if (now_usable && !state->all_address_usable) {
        //
        // Start sending beacon messages to the all_address.
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collector detected.  Begin sending beacons.");
        state->all_address_usable = true;
        _vflow_send_heartbeat();
    } else if (!now_usable && state->all_address_usable) {
        //
        // Stop sending beacons.  Nobody is listening.
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collector lost.  Stop sending beacons.");
        state->all_address_usable = false;
        if (!!state->heartbeat_timer) {
            qd_timer_cancel(state->heartbeat_timer);
        }
    }
}


static void _vflow_my_address_status_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    bool now_usable = work->value.bool_val;
    if (now_usable && !state->my_address_usable) {
        //
        // Start sending log records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO,
               "Event collector for this router detected.  Begin sending flow events.");
        state->my_address_usable = true;
    } else if (!now_usable && state->my_address_usable) {
        //
        // Stop sending log records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collector for this router lost.  Stop sending flow events.");
        state->my_address_usable = false;
    }
}


static void _vflow_my_flow_address_status_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    bool now_usable = work->value.bool_val;
    if (now_usable && !state->my_flow_address_usable) {
        //
        // Start sending flow records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collection for flow events detected.  Begin sending flow events.");
        state->my_flow_address_usable = true;
    } else if (!now_usable && state->my_flow_address_usable) {
        //
        // Stop sending flow records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collection for flow events ended.  Stop sending flow events.");
        state->my_flow_address_usable = false;
    }
}

static void _vflow_my_log_address_status_TH(vflow_work_t *work, bool discard)
{
    if (discard) {
        return;
    }

    bool now_usable = work->value.bool_val;
    if (now_usable && !state->my_log_address_usable) {
        //
        // Start sending log records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collection for log events detected.  Begin sending log events.");
        state->my_log_address_usable = true;
    } else if (!now_usable && state->my_log_address_usable) {
        //
        // Stop sending log records
        //
        qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Event collection for log events ended.  Stop sending log events.");
        state->my_log_address_usable = false;
    }
}

//=====================================================================================
// Module Thread
//=====================================================================================
/**
 * @brief Main function for the vflow thread.  This thread runs for the entire
 * lifecycle of the router.
 * 
 * @param unused Unused
 * @return void* Unused
 */
static void *_vflow_thread_TH(void *context)
{
    bool running = true;
    vflow_work_list_t local_work_list = DEQ_EMPTY;
    qdr_core_t *core = (qdr_core_t*) context;

    qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Protocol logging started");

    while (running) {
        //
        // Use the lock only to protect the condition variable and the work lists
        //
        sys_mutex_lock(&state->lock);
        for (;;) {
            if (!DEQ_IS_EMPTY(state->work_list)) {
                DEQ_MOVE(state->work_list, local_work_list);
                break;
            }

            //
            // Block on the condition variable when there is no work to do
            //
            state->sleeping = true;
            sys_cond_wait(&state->condition, &state->lock);
            state->sleeping = false;
        }
        sys_mutex_unlock(&state->lock);

        //
        // Process the local work list with the lock not held
        //
        vflow_work_t *work = DEQ_HEAD(local_work_list);
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
            free_vflow_work_t(work);
            work = DEQ_HEAD(local_work_list);
        }
    }

    //
    // Flush out all of the slots
    //
    for (int i = 0; i < FLUSH_SLOT_COUNT; i++) {
        _vflow_flush_TH(core, true);
        state->current_flush_slot = (state->current_flush_slot + 1) % FLUSH_SLOT_COUNT;
    }

    //
    // Free all remaining records in the tree
    //
    _vflow_free_record_TH(state->local_root, true);

    //
    // Free all remaining co-records
    //
    while (!DEQ_IS_EMPTY(state->co_records)) {
        _vflow_free_record_TH(DEQ_HEAD(state->co_records), false);
    }

    qd_log(LOG_FLOW_LOG, QD_LOG_INFO, "Protocol logging completed");
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
static void _vflow_on_all_address_watch(void     *context,
                                        uint32_t  local_consumers,
                                        uint32_t  in_proc_consumers,
                                        uint32_t  remote_consumers,
                                        uint32_t  local_producers)
{
    vflow_work_t *work = _vflow_work(_vflow_all_address_status_TH);
    work->value.bool_val = local_consumers > 0 || remote_consumers > 0;
    _vflow_post_work(work);
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
static void _vflow_on_my_address_watch(void     *context,
                                       uint32_t  local_consumers,
                                       uint32_t  in_proc_consumers,
                                       uint32_t  remote_consumers,
                                       uint32_t  local_producers)
{
    vflow_work_t *work = _vflow_work(_vflow_my_address_status_TH);
    work->value.bool_val = local_consumers > 0 || remote_consumers > 0;
    _vflow_post_work(work);
}


static void _vflow_on_my_flow_address_watch(void     *context,
                                            uint32_t  local_consumers,
                                            uint32_t  in_proc_consumers,
                                            uint32_t  remote_consumers,
                                            uint32_t  local_producers)
{
    vflow_work_t *work = _vflow_work(_vflow_my_flow_address_status_TH);
    work->value.bool_val = local_consumers > 0 || remote_consumers > 0;
    _vflow_post_work(work);
}

static void _vflow_on_my_log_address_watch(void    *context,
                                           uint32_t local_consumers,
                                           uint32_t in_proc_consumers,
                                           uint32_t remote_consumers,
                                           uint32_t local_producers)
{
    vflow_work_t *work   = _vflow_work(_vflow_my_log_address_status_TH);
    work->value.bool_val = local_consumers > 0 || remote_consumers > 0;
    _vflow_post_work(work);
}

static void _vflow_on_heartbeat(void *context)
{
    _vflow_send_heartbeat();
}


vflow_record_t *vflow_start_record_custom_id(vflow_record_type_t record_type, vflow_record_t *parent, const char *id)
{
    vflow_record_t *record = new_vflow_record_t();
    vflow_work_t   *work   = _vflow_work(_vflow_start_record_TH);
    ZERO(record);
    record->record_type   = record_type;
    record->parent        = parent;
    record->flush_slot    = -1;
    record->never_logged  = true;
    record->force_log     = false;
    record->ended         = false;

    work->record    = record;
    work->value64 = _now_in_usec();

    record->identity.record_id = VFLOW_ID_CUSTOM;
    record->identity.s.full_id = qd_strdup(id);

    _vflow_post_work(work);
    return record;
}


//=====================================================================================
// Public Functions
//=====================================================================================
vflow_record_t *vflow_start_record(vflow_record_type_t record_type, vflow_record_t *parent)
{
    // If this is one of the record types that supports discretionary functionality,
    // and if we already have the maximum allowed number of those, just don't produce
    // another one.
    // Otherwise, produce and count it, and check if this one put us over the limit.
    if (discretionary_records[record_type]) {
        if (0 == sys_atomic_get(&state->emit_discretionary_records)) {
            return 0;
        }
        if (sys_atomic_inc(&state->discretionary_record_count) >= DISCRETIONARY_RECORDS_STOP_THRESHOLD) {
            sys_atomic_set(&state->emit_discretionary_records, 0);
        }
    }

    vflow_record_t *record = new_vflow_record_t();
    vflow_work_t   *work   = _vflow_work(_vflow_start_record_TH);
    ZERO(record);
    record->record_type   = record_type;
    record->parent        = parent;
    record->flush_slot    = -1;
    record->never_logged  = true;
    record->force_log     = false;
    record->ended         = false;

    work->record    = record;
    work->value64 = _now_in_usec();

    //
    // Assign a unique identity to the new record
    //
    _vflow_next_id(&record->identity);

    _vflow_post_work(work);
    return record;
}


vflow_record_t *vflow_start_co_record_iter(vflow_record_type_t record_type, qd_iterator_t *identity_iterator)
{
    //
    // Note:  This implementation has been built assuming that the only record-type that will be used for a co-record
    // will be BIFLOW_TPORT.  The author does not forsee any circumstance in which another record type would need a
    // co-record.  If the author is wrong and it becomes desirable to use a co-record for a different record-type, the
    // search/replacement algorithm in _vflow_process_co_record_TH will need to be re-written in a more general way.
    //
    assert(record_type == VFLOW_RECORD_BIFLOW_TPORT);
    // The above record type is 'discretionary', which
    // means we may have been told to temporarily halt
    // production of them.
    if (0 == sys_atomic_get(&state->emit_discretionary_records)) {
        return 0;
    }
    if (sys_atomic_inc(&state->discretionary_record_count) >= DISCRETIONARY_RECORDS_STOP_THRESHOLD) {
        sys_atomic_set(&state->emit_discretionary_records, 0);
    }

    vflow_record_t *record = new_vflow_record_t();
    ZERO(record);
    record->record_type        = record_type;
    record->parent             = 0;
    record->flush_slot         = -1;
    record->default_flush_slot = -1;
    record->never_logged       = true;
    record->force_log          = false;
    record->ended              = false;
    record->co_record          = true;

    bool parse_success = _vflow_parse_id_iter(&record->identity, identity_iterator);
    if (!parse_success) {
        free_vflow_record_t(record);
        return 0;
    }

    vflow_work_t *work = _vflow_work(_vflow_start_record_TH);
    work->record  = record;
    work->value64 = _now_in_usec();

    _vflow_post_work(work);
    return record;
}


void vflow_end_record(vflow_record_t *record)
{
    if (!!record) {
        vflow_work_t *work = _vflow_work(_vflow_end_record_TH);
        work->record    = record;
        work->value64 = _now_in_usec();
        _vflow_post_work(work);
    }
}


void vflow_serialize_identity(const vflow_record_t *record, qd_composed_field_t *field)
{
    char buffer[IDENTITY_MAX + 1];
    if (!!record) {
        if (record->identity.record_id == VFLOW_ID_CUSTOM) {
            qd_compose_insert_string(field, record->identity.s.full_id);
        } else {
            snprintf(buffer, IDENTITY_MAX, "%s:%"PRIu64, record->identity.s.source_id, record->identity.record_id);
            qd_compose_insert_string(field, buffer);
        }
    } else {
        qd_compose_insert_null(field);
    }
}


void vflow_serialize_identity_pn(const vflow_record_t *record, pn_data_t *data)
{
    char buffer[IDENTITY_MAX + 1];
    assert(!!record);
    if (!!record) {
        if (record->identity.record_id == VFLOW_ID_CUSTOM) {
            pn_data_put_string(data, pn_bytes(strlen(record->identity.s.full_id), record->identity.s.full_id));
        } else {
            snprintf(buffer, IDENTITY_MAX, "%s:%"PRIu64, record->identity.s.source_id, record->identity.record_id);
            pn_data_put_string(data, pn_bytes(strlen(buffer), buffer));
        }
    }
}


void vflow_set_ref_from_record(vflow_record_t *record, vflow_attribute_t attribute_type, vflow_record_t *referenced_record)
{
    if (!!record && !!referenced_record) {
        assert(valid_attribute_types[attribute_type] & ATTR_REF);
        vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
        work->record           = record;
        work->attribute        = attribute_type;
        work->value.string_val = _vflow_id_to_new_string(&referenced_record->identity);
        _vflow_post_work(work);
    }
}


void vflow_set_ref_from_parsed(vflow_record_t *record, vflow_attribute_t attribute_type, qd_parsed_field_t *field)
{
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & ATTR_REF);
        vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
        work->record    = record;
        work->attribute = attribute_type;

        work->value.string_val = _vflow_unserialize_identity(field);

        if (!!work->value.string_val) {
            _vflow_post_work(work);
        } else {
            free_vflow_work_t(work);
            qd_log(LOG_FLOW_LOG, QD_LOG_WARNING, "Reference ID cannot be parsed from the received field");
        }
    }
}


void vflow_set_ref_from_iter(vflow_record_t *record, vflow_attribute_t attribute_type, qd_iterator_t *iter)
{
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & ATTR_REF);
        vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
        work->record    = record;
        work->attribute = attribute_type;

        work->value.string_val = (char*) qd_iterator_copy(iter);

        if (!!work->value.string_val) {
            _vflow_post_work(work);
        } else {
            free_vflow_work_t(work);
            qd_log(LOG_FLOW_LOG, QD_LOG_WARNING, "Reference ID cannot be parsed from the received field");
        }
    }
}


void vflow_set_ref_from_pn(vflow_record_t *record, vflow_attribute_t attribute_type, pn_data_t *data)
{
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & ATTR_REF);
        vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
        work->record    = record;
        work->attribute = attribute_type;

        pn_bytes_t bytes = pn_data_get_string(data);
        work->value.string_val = (char*) qd_malloc(bytes.size + 1);
        strncpy(work->value.string_val, bytes.start, bytes.size);
        work->value.string_val[bytes.size] = (char) 0;

        _vflow_post_work(work);
    }
}


void vflow_set_timestamp_now(vflow_record_t *record, vflow_attribute_t attribute_type)
{
    vflow_set_uint64(record, attribute_type, _now_in_usec());
}


void vflow_set_string(vflow_record_t *record, vflow_attribute_t attribute_type, const char *value)
{
#define MAX_STRING_VALUE 300
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & (ATTR_REF | ATTR_STRING));
        vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
        work->record           = record;
        work->attribute        = attribute_type;
        work->value.string_val = !!value ? strndup(value, strnlen(value, MAX_STRING_VALUE)) : 0;
        _vflow_post_work(work);
    }
}

void vflow_set_pn_condition_string(vflow_record_t *record, vflow_attribute_t attribute_type, pn_condition_t *cond)
{
    if (!!record && !!cond) {
        assert(valid_attribute_types[attribute_type] & ATTR_STRING);
        const char *cname = pn_condition_get_name(cond);
        const char *cdesc = pn_condition_get_description(cond);

        if (!!cname && !!cdesc) {
            size_t max = strlen(cname) + strlen(cdesc) + 4;
            char *text = (char*) qd_malloc(max);
            snprintf(text, max, "%s (%s)", cname, cdesc);

            vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
            work->record           = record;
            work->attribute        = attribute_type;
            work->value.string_val = text;
            _vflow_post_work(work);
        }
    }
}


void vflow_set_uint64(vflow_record_t *record, vflow_attribute_t attribute_type, uint64_t value)
{
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & ATTR_UINT);
        vflow_work_t *work = _vflow_work(_vflow_set_int_TH);
        work->record        = record;
        work->attribute     = attribute_type;
        work->value.int_val = value;
        _vflow_post_work(work);
    }
}


void vflow_inc_counter(vflow_record_t *record, vflow_attribute_t attribute_type, uint64_t addend)
{
    if (!!record) {
        assert(valid_attribute_types[attribute_type] & ATTR_COUNTER);
        vflow_work_t *work = _vflow_work(_vflow_inc_int_TH);
        work->record        = record;
        work->attribute     = attribute_type;
        work->value.int_val = addend;
        _vflow_post_work(work);
    }
}


void vflow_set_trace(vflow_record_t *record, qd_message_t *msg)
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

    vflow_work_t *work = _vflow_work(_vflow_set_string_TH);
    work->record    = record;
    work->attribute = VFLOW_ATTRIBUTE_TRACE;
    work->value.string_val = strdup(trace_text_ptr);
    _vflow_post_work(work);
}


void vflow_latency_start(vflow_record_t *record)
{
    if (!!record) {
        record->latency_start = _now_in_usec();
    }
}


void vflow_latency_end(vflow_record_t *record, vflow_attribute_t attribute_type)
{
    if (!!record && record->latency_start > 0) {
        uint64_t now = _now_in_usec();
        vflow_set_uint64(record, attribute_type, now - record->latency_start);
        //
        // Clear the latency_start so that any subsequent calls to vflow_latency_end on the
        // same vanflow will not have any effect.
        //
        record->latency_start = 0;
    }
}


void vflow_add_rate(vflow_record_t *record, vflow_attribute_t count_attribute, vflow_attribute_t rate_attribute)
{
    if (!!record) {
        assert(valid_attribute_types[count_attribute] & ATTR_COUNTER);
        assert(valid_attribute_types[rate_attribute] & ATTR_UINT);
        vflow_work_t *work = _vflow_work(_vflow_add_rate_TH);
        work->record        = record;
        work->attribute     = count_attribute;
        work->value.int_val = rate_attribute;
        _vflow_post_work(work);
    }
}

//=====================================================================================
// Configuration Module Callbacks
//=====================================================================================

QD_EXPORT qd_error_t qd_dispatch_configure_site(qd_dispatch_t *qd, qd_entity_t *entity)
{
    bool configured = sys_atomic_set(&site_configured, 1);
    if (configured) {
        return QD_ERROR_ALREADY_EXISTS;
    }

    char *name      = qd_entity_opt_string(entity, "name", 0);
    char *location  = qd_entity_opt_string(entity, "location", 0);
    char *provider  = qd_entity_opt_string(entity, "provider", 0);
    char *platform  = qd_entity_opt_string(entity, "platform", 0);
    char *namespace = qd_entity_opt_string(entity, "namespace", 0);
    char *version   = qd_entity_opt_string(entity, "version", 0);

    if (qd_error_code() == QD_ERROR_NONE) {
        vflow_record_t *site;

        if (!!state->site_id) {
            site = vflow_start_record_custom_id(VFLOW_RECORD_SITE, 0, state->site_id);
        } else {
            site = vflow_start_record(VFLOW_RECORD_SITE, 0);
        }

        if (name)      vflow_set_string(site, VFLOW_ATTRIBUTE_NAME, name);
        if (version)   vflow_set_string(site, VFLOW_ATTRIBUTE_BUILD_VERSION, version);
        if (location)  vflow_set_string(site, VFLOW_ATTRIBUTE_LOCATION, location);
        if (provider)  vflow_set_string(site, VFLOW_ATTRIBUTE_PROVIDER, provider);
        if (platform)  vflow_set_string(site, VFLOW_ATTRIBUTE_PLATFORM, platform);
        if (namespace) vflow_set_string(site, VFLOW_ATTRIBUTE_NAMESPACE, namespace);
    }

    free(name);
    free(location);
    free(provider);
    free(platform);
    free(namespace);
    free(version);

    return qd_error_code();
}

//=====================================================================================
// IO Module Callbacks
//=====================================================================================

static uint64_t _vflow_on_command_message(void                    *context,
                                          qd_message_t            *msg,
                                          int                      link_maskbit,
                                          int                      inter_router_cost,
                                          uint64_t                 conn_id,
                                          const qd_policy_spec_t  *policy,
                                          qdr_error_t            **error)
{
    if (qd_message_check_depth(msg, QD_DEPTH_PROPERTIES) == QD_MESSAGE_DEPTH_OK) {
        qd_iterator_t *subject_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
        if (!!subject_iter) {
            if (qd_iterator_equal(subject_iter, (const unsigned char*) "FLUSH")) {
                qd_log(LOG_FLOW_LOG, QD_LOG_DEBUG, "FLUSH request received");
                _vflow_post_work(_vflow_work(_vflow_refresh_events_TH));
            }
            qd_iterator_free(subject_iter);
        }
    }
    return PN_ACCEPTED;
}


static void _vflow_process_co_record_TH(vflow_work_t *work, bool discard)
{
    //
    // Note that this algorithm is optimized to assume that the only record type for which we will receive
    // co-records is BIFLOW_TPORT.  If, in the future, additional types of co-record are introduced, these
    // search alrorithms will need to be modified and made more general (possibly less efficient).
    //
    if (!discard) {
        vflow_record_t *biflow = _vflow_find_biflow_TH(work->value64);
        if (!!biflow) {
            vflow_attribute_data_t *attribute = DEQ_HEAD(work->value.attributes);
            while (!!attribute) {
                vflow_work_t sub_work;
                ZERO(&sub_work);
                sub_work.attribute = attribute->attribute_type;
                sub_work.record    = biflow;
                if (valid_attribute_types[attribute->attribute_type] & ATTR_UCOUNT) {
                    sub_work.value.int_val = attribute->value.uint_val;
                    _vflow_set_int_TH(&sub_work, false);
                } else {
                    sub_work.value.string_val = attribute->value.string_val;
                    attribute->value.string_val = 0;
                    _vflow_set_string_TH(&sub_work, false);
                }
                attribute = DEQ_NEXT(attribute);
            }
        }
    }

    vflow_attribute_data_t *clean_attribute = DEQ_HEAD(work->value.attributes);
    while (!!clean_attribute) {
        DEQ_REMOVE_HEAD(work->value.attributes);
        if (valid_attribute_types[clean_attribute->attribute_type] & (ATTR_STRING | ATTR_REF | ATTR_TRACE)) {
            free(clean_attribute->value.string_val);
        }
        free_vflow_attribute_data_t(clean_attribute);
        clean_attribute = DEQ_HEAD(work->value.attributes);
    }
}


static void _vflow_on_co_record_map(qd_parsed_field_t *co_record)
{
    vflow_record_type_t          record_type = VFLOW_RECORD_SITE;  // Any value other than BIFLOW_TPORT
    vflow_identity_t             identity;
    vflow_attribute_data_list_t  attributes;
    uint32_t                     item_count = qd_parse_sub_count(co_record);
    bool                         input_error = false;

    identity.record_id      = 0;
    identity.s.source_id[0] = '\0';

    DEQ_INIT(attributes);
    for (uint32_t i = 0; i < item_count; i++) {
        qd_parsed_field_t *key   = qd_parse_sub_key(co_record, i);
        qd_parsed_field_t *value = qd_parse_sub_value(co_record, i);
        if (qd_parse_is_scalar(key) && qd_parse_is_scalar(value)) {
            uint32_t attribute_ordinal = qd_parse_as_uint(key);
            if (attribute_ordinal >= sizeof(valid_attribute_types)) {
                //
                // Invalid attribute ordinal
                //
                input_error = true;
                break;
            }
            if (attribute_ordinal == VFLOW_ATTRIBUTE_RECORD_TYPE) {
                record_type = (vflow_record_type_t) qd_parse_as_uint(value);
            } else if (attribute_ordinal == VFLOW_ATTRIBUTE_IDENTITY) {
                bool valid_id = _vflow_parse_id_iter(&identity, qd_parse_raw(value));
                if (!valid_id) {
                    input_error = true;
                    break;
                }
            } else {
                vflow_attribute_data_t *attribute = new_vflow_attribute_data_t();
                ZERO(attribute);
                attribute->attribute_type = attribute_ordinal;
                uint8_t tag = qd_parse_tag(value);
                if ((valid_attribute_types[attribute_ordinal] & ATTR_UCOUNT) && (tag == QD_AMQP_ULONG0 || tag == QD_AMQP_SMALLULONG || tag == QD_AMQP_ULONG)) {
                    attribute->value.uint_val = qd_parse_as_ulong(value);
                } else if ((valid_attribute_types[attribute_ordinal] & (ATTR_STRING | ATTR_REF | ATTR_TRACE)) && (tag == QD_AMQP_STR8_UTF8 || tag == QD_AMQP_STR32_UTF8)) {
                    attribute->value.string_val = (char*) qd_iterator_copy(qd_parse_raw(value));
                } else {
                    //
                    // Invalid type tag for the value
                    //
                    input_error = true;
                    break;
                }
                DEQ_INSERT_TAIL(attributes, attribute);
            }
        } else {
            //
            // A non-scalar key or value is invalid.  Don't take any action.
            //
            input_error = true;
            break;
        }
    }

    if (DEQ_IS_EMPTY(attributes)) {
        input_error = true;
    }

    if (strncmp(identity.s.source_id, state->router_id, ROUTER_ID_SIZE) != 0) {
        input_error = true;
    }

    if (record_type != VFLOW_RECORD_BIFLOW_TPORT) {
        input_error = true;
    }

    if (!input_error) {
        //
        // Post the update for handling within the thread
        //
        vflow_work_t *work = _vflow_work(_vflow_process_co_record_TH);
        work->value64 = identity.record_id;
        DEQ_MOVE(attributes, work->value.attributes);
        _vflow_post_work(work);
    } else {
        vflow_attribute_data_t *clean_attribute = DEQ_HEAD(attributes);
        while (!!clean_attribute) {
            DEQ_REMOVE_HEAD(attributes);
            if (valid_attribute_types[clean_attribute->attribute_type] & (ATTR_STRING | ATTR_REF | ATTR_TRACE)) {
                free(clean_attribute->value.string_val);
            }
            free_vflow_attribute_data_t(clean_attribute);
            clean_attribute = DEQ_HEAD(attributes);
        }
    }
}


static uint64_t _vflow_on_co_record_message(void                    *context,
                                            qd_message_t            *msg,
                                            int                      link_maskbit,
                                            int                      inter_router_cost,
                                            uint64_t                 conn_id,
                                            const qd_policy_spec_t  *policy,
                                            qdr_error_t            **error)
{
    if (qd_message_check_depth(msg, QD_DEPTH_BODY) == QD_MESSAGE_DEPTH_OK) {
        qd_iterator_t *subject_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
        if (!!subject_iter) {
            if (qd_iterator_equal(subject_iter, (const unsigned char*) "RECORD")) {
                qd_log(LOG_FLOW_LOG, QD_LOG_DEBUG, "Co-Record update received");
                qd_iterator_t *body_iter = qd_message_field_iterator(msg, QD_FIELD_BODY);
                if (!!body_iter) {
                    qd_parsed_field_t *body = qd_parse(body_iter);
                    if (qd_parse_ok(body)) {
                        if (qd_parse_is_list(body)) {
                            qd_parsed_field_t *item = qd_field_first_child(body);
                            while (!!item) {
                                if (qd_parse_is_map(item)) {
                                    _vflow_on_co_record_map(item);
                                }
                                item = qd_field_next_child(item);
                            }
                        }
                    }
                    qd_parse_free(body);
                    qd_iterator_free(body_iter);
                }
            }
            qd_iterator_free(subject_iter);
        }
    }
    return PN_ACCEPTED;
}

static void _vflow_init_address_watch_TH(vflow_work_t *work, bool discard)
{
    qdr_core_t *core = (qdr_core_t*) work->value.pointer;

    if (!discard) {
        state->event_address_my = (char*) malloc(71);
        strcpy(state->event_address_my, event_address_my_prefix);
        _vflow_strncat_id(state->event_address_my, 70, &state->local_router->identity);

        state->event_address_my_flow = (char*) malloc(71);
        strcpy(state->event_address_my_flow, event_address_my_prefix);
        _vflow_strncat_id(state->event_address_my_flow, 70, &state->local_router->identity);
        strcat(state->event_address_my_flow, ".flows");

        state->event_address_my_log = (char *) malloc(71);
        strcpy(state->event_address_my_log, event_address_my_prefix);
        _vflow_strncat_id(state->event_address_my_log, 70, &state->local_router->identity);
        strcat(state->event_address_my_log, ".logs");

        state->command_address = (char*) malloc(71);
        strcpy(state->command_address, command_address_prefix);
        _vflow_strncat_id(state->command_address, 70, &state->local_router->identity);

        state->co_record_address = (char*) malloc(71);
        strcpy(state->co_record_address, co_record_address_prefix);
        _vflow_strncat_id(state->co_record_address, 70, &state->local_router->identity);

        // clang-format off
        state->all_address_watch_handle = qdr_core_watch_address(core, event_address_all, 'M',
                                                                 QD_TREATMENT_MULTICAST_ONCE, _vflow_on_all_address_watch, 0, core);
        state->my_address_watch_handle  = qdr_core_watch_address(core, state->event_address_my,  'M',
                                                                 QD_TREATMENT_MULTICAST_ONCE, _vflow_on_my_address_watch, 0, core);
        state->my_flow_address_watch_handle = qdr_core_watch_address(core, state->event_address_my_flow,  'M',
                                                                     QD_TREATMENT_MULTICAST_ONCE, _vflow_on_my_flow_address_watch, 0, core);
        state->my_log_address_watch_handle  = qdr_core_watch_address(core, state->event_address_my_log,  'M',
                                                                     QD_TREATMENT_MULTICAST_ONCE, _vflow_on_my_log_address_watch, 0, core);

        state->command_subscription   = qdr_core_subscribe(core, state->command_address,   'M', QD_TREATMENT_ANYCAST_CLOSEST, false, true, _vflow_on_command_message, core);
        state->co_record_subscription = qdr_core_subscribe(core, state->co_record_address, 'M', QD_TREATMENT_ANYCAST_CLOSEST, false, true, _vflow_on_co_record_message, core);
        // clang-format on
    }
}


/**
 * @brief Module initializer
 * 
 * @param core Pointer to the core object
 * @param adaptor_context (out) Context set for use in finalizer
 */
static void _vflow_init(qdr_core_t *core, void **adaptor_context)
{
    sys_atomic_init(&site_configured, 0);
    state = NEW(vflow_state_t);
    ZERO(state);

    sys_atomic_init(&state->discretionary_record_count, 0);
    sys_atomic_init(&state->emit_discretionary_records, 1);
    state->router_core = core;
    state->hostname = getenv("HOSTNAME");
    size_t hostLength = !!state->hostname ? strlen(state->hostname) : 0;

    state->site_id = getenv("SKUPPER_SITE_ID");

    //
    // If the hostname is in the form of a Kubernetes pod name, use the 5-character
    // suffix as the router-id.  Otherwise, generate a random router-id.
    //
    if (hostLength > ROUTER_ID_SIZE && state->hostname[hostLength - ROUTER_ID_SIZE] == '-') {
        //
        // This memcpy copies the suffix and the terminating null character.
        //
        memcpy(state->router_id, state->hostname + (hostLength - ROUTER_ID_SIZE) + 1, ROUTER_ID_SIZE);
    } else {
        //
        // If the router-id size is ever greater than the discriminator size, the
        // generation of router-ids will need to be re-written to use multiple
        // discriminators.
        //
        assert(QD_DISCRIMINATOR_SIZE > ROUTER_ID_SIZE);
        char discriminator[QD_DISCRIMINATOR_SIZE];
        qd_generate_discriminator(discriminator);
        memcpy(state->router_id, discriminator, ROUTER_ID_SIZE - 1);
        state->router_id[ROUTER_ID_SIZE - 1] = '\0';
    }

    state->router_area = qdr_core_dispatch(core)->router_area;
    state->router_name = qdr_core_dispatch(core)->router_id;

    switch (qdr_core_dispatch(core)->router_mode) {
    case QD_ROUTER_MODE_STANDALONE: state->router_mode = "standalone"; break;
    case QD_ROUTER_MODE_INTERIOR:   state->router_mode = "interior";   break;
    case QD_ROUTER_MODE_EDGE:       state->router_mode = "edge";       break;
    case QD_ROUTER_MODE_ENDPOINT:   state->router_mode = "endpoint";   break;
    }

    for (int slot = 0; slot < FLUSH_SLOT_COUNT; slot++) {
        DEQ_INIT(state->unflushed_flow_records[slot]);
        DEQ_INIT(state->unflushed_log_records[slot]);
        DEQ_INIT(state->unflushed_records[slot]);
        DEQ_INIT(state->unflushed_co_records[slot]);
    }

    DEQ_INIT(state->to_delete_records);

    sys_mutex_init(&state->lock);
    sys_mutex_init(&state->id_lock);
    sys_cond_init(&state->condition);
    state->thread    = sys_thread(SYS_THREAD_VFLOW, _vflow_thread_TH, core);
    *adaptor_context = core;

    _vflow_create_router_record();

    vflow_work_t *work = _vflow_work(_vflow_init_address_watch_TH);
    work->value.pointer = core;
    _vflow_post_work(work);

    state->heartbeat_timer = qd_timer(qdr_core_dispatch(core), _vflow_on_heartbeat, 0);
    state->flush_timer  = qd_timer(qdr_core_dispatch(core), _vflow_on_flush, 0);
    qd_timer_schedule(state->flush_timer, initial_flush_interval_msec);

    // allow logging to publish vanflow events now
    qd_log_enable_events();
}


/**
 * @brief Module finalizer
 * 
 * @param adaptor_context Contains the core module pointer
 */
static void _vflow_final(void *adaptor_context)
{
    qdr_core_t *core = (qdr_core_t*) adaptor_context;

    // prevent logging from publishing vanflow events
    qd_log_disable_events();

    qd_timer_free(state->heartbeat_timer);
    state->heartbeat_timer = 0;

    qd_timer_free(state->flush_timer);
    state->flush_timer = 0;

    //
    // Cancel the address watches
    //
    qdr_core_unwatch_address(core, state->all_address_watch_handle);
    qdr_core_unwatch_address(core, state->my_address_watch_handle);

    //
    // Unsubscribe for command and co-record messages
    //
    qdr_core_unsubscribe(state->command_subscription);
    qdr_core_unsubscribe(state->co_record_subscription);

    //
    // Signal the thread to exit by posting a NULL work pointer
    //
    _vflow_post_work(_vflow_work(0));

    //
    // Join and free the thread
    //
    sys_thread_join(state->thread);
    sys_thread_free(state->thread);

    //
    // Free the allocated my-address
    //
    free(state->event_address_my);
    free(state->event_address_my_flow);
    free(state->event_address_my_log);
    free(state->command_address);
    free(state->co_record_address);
    free(state->local_router_id);

    //
    // Free the condition and lock variables
    //
    sys_cond_free(&state->condition);
    sys_mutex_free(&state->lock);
    sys_mutex_free(&state->id_lock);

    //
    // Free the module state
    //
    free(state);
    sys_atomic_destroy(&site_configured);
}


QDR_CORE_ADAPTOR_DECLARE_ORD("VanFlow Logging", _vflow_init, _vflow_final, 10)

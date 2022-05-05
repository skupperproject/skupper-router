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

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/router_core.h"

#include <inttypes.h>
#include <stdio.h>

#define ADDRESS_COUNT 10

typedef struct dynamic_watch_t {
    DEQ_LINKS(struct dynamic_watch_t);
    qdr_watch_handle_t  watch_handle;
    char               *address;
} dynamic_watch_t;

DEQ_DECLARE(dynamic_watch_t, dynamic_watch_list_t);

static const char *address_fmt = "addr_watch/test_address/%d";

static qdr_watch_handle_t handle[ADDRESS_COUNT];

static qdr_core_t           *core_ptr        = 0;
static qd_log_source_t      *log_source      = 0;
static qdr_subscription_t   *subscription    = 0;
static dynamic_watch_list_t  dynamic_watches = DEQ_EMPTY;

static void on_watch(void     *context,
                     uint32_t  local_consumers,
                     uint32_t  in_proc_consumers,
                     uint32_t  remote_consumers,
                     uint32_t  local_producers)
{
    qd_log(log_source, QD_LOG_INFO, "on_watch(%ld): loc: %"PRIu32" rem: %"PRIu32" prod: %"PRIu32"",
           (long) context, local_consumers, remote_consumers, local_producers);
}


static void on_dynamic_watch(void     *context,
                             uint32_t  local_consumers,
                             uint32_t  in_proc_consumers,
                             uint32_t  remote_consumers,
                             uint32_t  local_producers)
{
    dynamic_watch_t *dw = (dynamic_watch_t*) context;

    qd_log(log_source, QD_LOG_INFO, "On Dynamic Watch: %s", dw->address);

    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(field);

    qd_compose_insert_symbol(field, "address");
    qd_compose_insert_string(field, dw->address);

    qd_compose_insert_symbol(field, "local_consumers");
    qd_compose_insert_uint(field, local_consumers);

    qd_compose_insert_symbol(field, "in_proc_consumers");
    qd_compose_insert_uint(field, in_proc_consumers);

    qd_compose_insert_symbol(field, "remote_consumers");
    qd_compose_insert_uint(field, remote_consumers);

    qd_compose_insert_symbol(field, "local_producers");
    qd_compose_insert_uint(field, local_producers);

    qd_compose_end_map(field);

    qd_message_t *msg = qd_message();
    qd_message_compose_2(msg, field, true);
    qd_compose_free(field);
    qdr_send_to2(core_ptr, msg, "_local/_testhook/watch_event", true, false);

    qd_message_free(msg);
}


static void remove_dynamic_watch(dynamic_watch_t *dw)
{
    qdr_core_unwatch_address(core_ptr, dw->watch_handle);
    free(dw->address);
    DEQ_REMOVE(dynamic_watches, dw);
    free(dw);
}


static void start_watch(const char *address)
{
    qd_log(log_source, QD_LOG_INFO, "Start Watch: %s", address);

    dynamic_watch_t *dw = NEW(dynamic_watch_t);
    DEQ_ITEM_INIT(dw);
    dw->address = strdup(address);
    dw->watch_handle = qdr_core_watch_address(core_ptr, address, QD_ITER_HASH_PREFIX_MOBILE, QD_TREATMENT_ANYCAST_BALANCED, on_dynamic_watch, dw);
    DEQ_INSERT_TAIL(dynamic_watches, dw);    
}


static void stop_watch(const char *address)
{
    qd_log(log_source, QD_LOG_INFO, "Stop Watch: %s", address);

    dynamic_watch_t *dw = DEQ_HEAD(dynamic_watches);
    while (!!dw) {
        if (strcmp(address, dw->address) == 0) {
            remove_dynamic_watch(dw);
            return;
        }
        dw = DEQ_NEXT(dw);
    }
}


static uint64_t on_message(void                    *context,
                           qd_message_t            *msg,
                           int                      link_maskbit,
                           int                      inter_router_cost,
                           uint64_t                 conn_id,
                           const qd_policy_spec_t  *policy,
                           qdr_error_t            **error)
{
    if (qd_message_check_depth(msg, QD_DEPTH_APPLICATION_PROPERTIES) == QD_MESSAGE_DEPTH_OK) {
        qd_iterator_t     *ap_iter  = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
        qd_parsed_field_t *ap_field = qd_parse(ap_iter);
        if (qd_parse_is_map(ap_field)) {
            qd_parsed_field_t *opcode  = qd_parse_value_by_key(ap_field, "opcode");
            qd_parsed_field_t *address = qd_parse_value_by_key(ap_field, "address");
            if (!!opcode && !!address) {
                char *addr = (char*) qd_iterator_copy(qd_parse_raw(address));
                if (qd_iterator_equal(qd_parse_raw(opcode), (const unsigned char*) "watch-on")) {
                    start_watch(addr);
                } else if (qd_iterator_equal(qd_parse_raw(opcode), (const unsigned char*) "watch-off")) {
                    stop_watch(addr);
                }
                free(addr);
            }
        }
        qd_parse_free(ap_field);
        qd_iterator_free(ap_iter);
    }
    return PN_ACCEPTED;
}


static void qdr_test_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    core_ptr = core;
    if (qdr_core_test_hooks_enabled(core)) {
        log_source = qd_log_source("ADDRESS_WATCH");
        char address[100];
        for (long index = 0; index < ADDRESS_COUNT; index++) {
            sprintf(address, address_fmt, index);
            handle[index] = qdr_core_watch_address(core, address, QD_ITER_HASH_PREFIX_MOBILE, QD_TREATMENT_ANYCAST_BALANCED, on_watch, (void*) index);
        }

        subscription = qdr_core_subscribe(core, "_testhook/address_watch", QD_ITER_HASH_PREFIX_LOCAL,
                                          QD_TREATMENT_ANYCAST_CLOSEST, false, on_message, core);
    }
}


static void qdr_test_adaptor_final(void *adaptor_context)
{
    if (qdr_core_test_hooks_enabled(core_ptr)) {
        for (long index = 0; index < ADDRESS_COUNT; index++) {
            qdr_core_unwatch_address(core_ptr, handle[index]);
        }

        qdr_core_unsubscribe(subscription);
        dynamic_watch_t *dw = DEQ_HEAD(dynamic_watches);
        while (!!dw) {
            remove_dynamic_watch(dw);
            dw = DEQ_HEAD(dynamic_watches);
        }
    }
}


QDR_CORE_ADAPTOR_DECLARE("test-adaptor", qdr_test_adaptor_init, qdr_test_adaptor_final)

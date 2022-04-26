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

#include "router_core_private.h"
#include "qpid/dispatch/amqp.h"

struct qdr_address_watch_t {
    DEQ_LINKS(struct qdr_address_watch_t);
    qdr_watch_handle_t          watch_handle;
    qdr_address_t              *addr;
    qdr_address_watch_update_t  handler;
    void                       *context;
};

ALLOC_DECLARE(qdr_address_watch_t);
ALLOC_DEFINE(qdr_address_watch_t);

static void qdr_watch_invoker(qdr_core_t *core, qdr_general_work_t *work);
static void qdr_core_watch_address_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_core_unwatch_address_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_address_watch_free_CT(qdr_core_t *core, qdr_address_watch_t *watch);

//==================================================================================
// Core Interface Functions
//==================================================================================
qdr_watch_handle_t qdr_core_watch_address(qdr_core_t                 *core,
                                          const char                 *address,
                                          char                        aclass,
                                          qd_address_treatment_t      treatment_hint,
                                          qdr_address_watch_update_t  on_watch,
                                          void                       *context)
{
    static sys_atomic_t next_handle;
    qdr_action_t *action = qdr_action(qdr_core_watch_address_CT, "watch_address");

    action->args.io.address       = qdr_field(address);
    action->args.io.address_class = aclass;
    action->args.io.treatment     = treatment_hint;
    action->args.io.watch_handler = on_watch;
    action->args.io.context       = context;
    action->args.io.value32_1     = sys_atomic_inc(&next_handle);

    qdr_watch_handle_t retval = action->args.io.value32_1;

    qdr_action_enqueue(core, action);
    return retval;
}


void qdr_core_unwatch_address(qdr_core_t *core, qdr_watch_handle_t handle)
{
    qdr_action_t *action = qdr_action(qdr_core_unwatch_address_CT, "unwatch_address");

    action->args.io.value32_1 = handle;
    qdr_action_enqueue(core, action);
}


//==================================================================================
// In-Core API Functions
//==================================================================================
void qdr_trigger_address_watch_CT(qdr_core_t *core, qdr_address_t *addr)
{
    qdr_address_watch_t *watch = addr->watch;

    if (!!watch) {
        qdr_general_work_t *work = qdr_general_work(qdr_watch_invoker);
        work->watch_handler     = watch->handler;
        work->context           = watch->context;
        work->local_consumers   = DEQ_SIZE(addr->rlinks);
        work->in_proc_consumers = DEQ_SIZE(addr->subscriptions);
        work->remote_consumers  = qd_bitmask_cardinality(addr->rnodes);
        work->local_producers   = DEQ_SIZE(addr->inlinks);
        qdr_post_general_work_CT(core, work);
    }
}

void qdr_address_watch_shutdown(qdr_core_t *core)
{
    qdr_address_watch_t *watch = DEQ_HEAD(core->addr_watches);
    while (!!watch) {
        DEQ_REMOVE(core->addr_watches, watch);
        qdr_address_watch_free_CT(core, watch);
        watch = DEQ_HEAD(core->addr_watches);
    }
}


//==================================================================================
// Local Functions
//==================================================================================
static void qdr_address_watch_free_CT(qdr_core_t *core, qdr_address_watch_t *watch)
{
    watch->addr->watch = 0;
    qdrc_event_addr_raise(core, QDRC_EVENT_ADDR_WATCH_OFF, watch->addr);

    watch->addr->ref_count--;
    qdr_check_addr_CT(core, watch->addr);
    free_qdr_address_watch_t(watch);
}


static void qdr_watch_invoker(qdr_core_t *core, qdr_general_work_t *work)
{
    work->watch_handler(work->context,
                        work->local_consumers, work->in_proc_consumers, work->remote_consumers, work->local_producers);
}


static void qdr_core_watch_address_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard) {
        qd_iterator_t *iter = qdr_field_iterator(action->args.io.address);
        qd_iterator_annotate_prefix(iter, action->args.io.address_class);
        qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_HASH);
        qdr_address_t *addr = 0;

        qd_hash_retrieve(core->addr_hash, iter, (void**) &addr);
        if (!addr) {
            qdr_address_config_t   *addr_config;
            qd_address_treatment_t  treatment =
                qdr_treatment_for_address_hash_with_default_CT(core, iter, action->args.io.treatment, &addr_config);

            addr = qdr_address_CT(core, treatment, addr_config);
            if (!!addr) {
                qd_hash_insert(core->addr_hash, iter, addr, &addr->hash_handle);
                DEQ_ITEM_INIT(addr);
                DEQ_INSERT_TAIL(core->addrs, addr);
            } else {
                qd_log(core->log, QD_LOG_CRITICAL, "Failed to create address for watch");
                assert(false);
            }
        }

        if (!!addr) {
            if (!addr->watch) {
                qdr_address_watch_t *watch = new_qdr_address_watch_t();
                ZERO(watch);
                watch->watch_handle = action->args.io.value32_1;
                watch->addr         = addr;
                watch->handler      = action->args.io.watch_handler;
                watch->context      = action->args.io.context;
                DEQ_INSERT_TAIL(core->addr_watches, watch);

                addr->watch = watch;
                addr->ref_count++;

                qdrc_event_addr_raise(core, QDRC_EVENT_ADDR_WATCH_ON, addr);
            } else {
                qd_log(core->log, QD_LOG_CRITICAL, "Multiple watches established for the same address, later watches ignored.");
            }
        }
    }
    qdr_field_free(action->args.io.address);
}


static void qdr_core_unwatch_address_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard) {
        qdr_watch_handle_t watch_handle = action->args.io.value32_1;

        qdr_address_watch_t *watch = DEQ_HEAD(core->addr_watches);
        while (!!watch) {
            if (watch->watch_handle == watch_handle) {
                DEQ_REMOVE(core->addr_watches, watch);
                qdr_address_watch_free_CT(core, watch);
                break;
            }
            watch = DEQ_NEXT(watch);
        }
    }
}

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

#include "python_private.h"  // must be first!
#include "qpid/dispatch/python_embedded.h"

#include "qpid/dispatch/server.h"

#include "config.h"
#include "dispatch_private.h"
#include "qpid/dispatch/entity.h"
#include "policy.h"
#include "server_private.h"
#include "timer_private.h"

// KAG: todo: fix these layering violations:
#include "adaptors/amqp/qd_connection.h"
#include "adaptors/amqp/qd_listener.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/failoverlist.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/platform.h"
#include "qpid/dispatch/proton_utils.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <proton/event.h>
#include <proton/listener.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>
#include <proton/sasl.h>
#include <proton/netaddr.h>

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct qd_server_t {
    qd_dispatch_t            *qd;
    const int                 thread_count; /* Immutable */
    const char               *container_name;
    const char               *sasl_config_path;
    const char               *sasl_config_name;
    pn_proactor_t            *proactor;
    qd_log_source_t          *protocol_log_source; // Log source for the PROTOCOL module
    void                     *start_context;
    sys_cond_t                cond;
    sys_mutex_t               lock;
    int                       pause_requests;
    int                       threads_paused;
    int                       pause_next_sequence;
    int                       pause_now_serving;
    uint64_t                  next_connection_id;
    qd_http_server_t         *http;
    sys_mutex_t               conn_activation_lock;
};


//
// Proactor event handlers
//

static void handle_event_with_context(pn_event_t *e, qd_server_t *qd_server, qd_handler_context_t *context)
{
    if (context && context->handler) {
        context->handler(e, qd_server, context->context);
    }
}

static bool handle_raw_connection_event(qd_server_t *qd_server, pn_event_t *e, void *context)
{
    if (e) { // end-of-batch signal not used (yet)
        pn_raw_connection_t *pn_raw_conn = (pn_raw_connection_t *) context;
        handle_event_with_context(e, qd_server, (qd_handler_context_t*) pn_raw_connection_get_context(pn_raw_conn));
    }
    return true;
}

static bool handle_listener_event(qd_server_t *qd_server, pn_event_t *e, void *context)
{
    if (e) { // end-of-batch signal not used (yet)
        pn_listener_t *pn_listener = (pn_listener_t *) context;
        handle_event_with_context(e, qd_server, (qd_handler_context_t*) pn_listener_get_context(pn_listener));
    }
    return true;
}

static bool handle_proactor_other_event(qd_server_t *qd_server, pn_event_t *e, void *context)
{
    if (e) {
        switch (pn_event_type(e)) {

            case PN_PROACTOR_INTERRUPT:
                /* Interrupt the next thread */
                pn_proactor_interrupt(qd_server->proactor);
                /* Stop the current thread */
                return false;

            case PN_PROACTOR_TIMEOUT:
                ASSERT_PROACTOR_MODE(SYS_THREAD_PROACTOR_MODE_TIMER);
                qd_timer_visit();
                break;

            default:
                break;
        }
    }
    return true;
}

pn_proactor_t *qd_server_proactor(const qd_server_t *qd_server)
{
    return qd_server->proactor;
}

//
// Proactor  main loop
//

static void *proactor_thread(void *arg)
{
    ASSERT_THREAD_IS(SYS_THREAD_PROACTOR);

    qd_server_t      *qd_server = (qd_server_t*)arg;
    bool running = true;
    while (running) {
        pn_event_batch_t            *events            = pn_proactor_wait(qd_server->proactor);
        sys_thread_proactor_mode_t   proactor_mode     = SYS_THREAD_PROACTOR_MODE_OTHER;
        void                        *proactor_context  = 0;
        bool                        (*event_handler)(qd_server_t *, pn_event_t *, void *);

        // set the mode for the proactor thread and determine the event handler
        // clang-format off
        if ((proactor_context = pn_event_batch_connection(events)) != 0) {
            // AMQP connection
            proactor_mode = SYS_THREAD_PROACTOR_MODE_CONNECTION;
            event_handler = qd_connection_handle_event;
        } else if ((proactor_context = pn_event_batch_raw_connection(events)) != 0) {
            proactor_mode = SYS_THREAD_PROACTOR_MODE_RAW_CONNECTION;
            event_handler = handle_raw_connection_event;
        } else if ((proactor_context = pn_event_batch_listener(events)) != 0) {
            proactor_mode = SYS_THREAD_PROACTOR_MODE_LISTENER;
            event_handler = handle_listener_event;
        } else {
            assert(pn_event_batch_proactor(events));
            event_handler = handle_proactor_other_event;
        }
        // clang-format on
        sys_thread_proactor_set_mode(proactor_mode, proactor_context);

        pn_event_t *e;
        while (running && (e = pn_event_batch_next(events))) {
            running = event_handler(qd_server, e, proactor_context);
        }

        // indicate batch complete by passing no event to the event_handler

        (void) event_handler(qd_server, 0, proactor_context);
        pn_proactor_done(qd_server->proactor, events);
    }
    return NULL;
}

qd_server_t *qd_server(qd_dispatch_t *qd, int thread_count, const char *container_name,
                       const char *sasl_config_path, const char *sasl_config_name)
{
    /* Initialize const members, 0 initialize all others. */
    qd_server_t tmp = { .thread_count = thread_count };
    qd_server_t *qd_server = NEW(qd_server_t);
    if (qd_server == 0)
        return 0;
    memcpy(qd_server, &tmp, sizeof(tmp));

    qd_server->qd               = qd;
    qd_server->container_name   = container_name;
    qd_server->sasl_config_path = sasl_config_path;
    qd_server->sasl_config_name = sasl_config_name;
    qd_server->proactor         = pn_proactor();
    qd_server->start_context    = 0;

    sys_mutex_init(&qd_server->lock);
    sys_mutex_init(&qd_server->conn_activation_lock);
    sys_cond_init(&qd_server->cond);

    qd_timer_initialize();

    qd_server->pause_requests         = 0;
    qd_server->threads_paused         = 0;
    qd_server->pause_next_sequence    = 0;
    qd_server->pause_now_serving      = 0;
    qd_server->next_connection_id     = 1;

    if (qd_server->sasl_config_path)
        pn_sasl_config_path(0, qd_server->sasl_config_path);
    if (qd_server->sasl_config_name)
        pn_sasl_config_name(0, qd_server->sasl_config_name);

    qd_server->http = qd_http_server(qd_server);

    qd_log(LOG_SERVER, QD_LOG_INFO, "Container Name: %s", container_name);

    return qd_server;
}

qd_http_server_t *qd_server_http(const qd_server_t *qd_server)
{
    return qd_server->http;
}

void qd_server_free(qd_server_t *qd_server)
{
    if (!qd_server) return;

    pn_proactor_free(qd_server->proactor);
    qd_timer_finalize();
    sys_mutex_free(&qd_server->lock);
    sys_mutex_free(&qd_server->conn_activation_lock);
    sys_cond_free(&qd_server->cond);
    free(qd_server);
}


void qd_server_run(qd_dispatch_t *qd)
{
    qd_server_t *qd_server = qd->server;
    int i;
    assert(qd_server);
    qd_log(LOG_SERVER, QD_LOG_INFO, "Operational, %d Threads Running (process ID %ld)",
           qd_server->thread_count, (long) getpid());  // Log message is matched in system_tests

    const uintmax_t ram_size = qd_platform_memory_size();
    const uint64_t  vm_size  = qd_router_virtual_memory_usage();
    const uint64_t  rss_size = qd_router_rss_memory_usage();
    const char *suffix_vm  = 0;
    const char *suffix_rss = 0;
    const char *suffix_ram = 0;
    double vm  = normalize_memory_size(vm_size, &suffix_vm);
    double rss = normalize_memory_size(rss_size, &suffix_rss);
    double ram = normalize_memory_size(ram_size, &suffix_ram);
    qd_log(LOG_ROUTER, QD_LOG_INFO,
           "Process VmSize %.2f %s RSS %.2f %s (%.2f %s available memory)",
           vm, suffix_vm, rss, suffix_rss, ram, suffix_ram);

#ifndef NDEBUG
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Running in DEBUG Mode");
#endif

    qd_alloc_start_monitor(qd);  // enable periodic alloc pool usage loggin

    const int n = qd_server->thread_count;
    sys_thread_t **threads = (sys_thread_t **)qd_calloc(n, sizeof(sys_thread_t*));
    for (i = 0; i < n; i++) {
        threads[i] = sys_thread(SYS_THREAD_PROACTOR, proactor_thread, qd_server);
    }

    for (i = 0; i < n; i++) {
        sys_thread_join(threads[i]);
        sys_thread_free(threads[i]);
    }
    free(threads);

    qd_alloc_stop_monitor();

    qd_log(LOG_ROUTER, QD_LOG_INFO, "Shut Down");
}

void qd_server_stop(qd_dispatch_t *qd)
{
    /* Interrupt the proactor, async-signal-safe */
    pn_proactor_interrupt(qd->server->proactor);
}

__attribute__((weak)) // permit replacement by dummy implementation in unit_tests
void qd_server_timeout(qd_server_t *server, qd_duration_t duration) {
    pn_proactor_set_timeout(server->proactor, duration);
}

qd_dispatch_t* qd_server_dispatch(qd_server_t *server) { return server->qd; }

uint64_t qd_server_allocate_connection_id(qd_server_t *server)
{
    uint64_t id;
    sys_mutex_lock(&server->lock);
    id = server->next_connection_id++;
    sys_mutex_unlock(&server->lock);
    return id;
}

sys_mutex_t *qd_server_get_activation_lock(qd_server_t * server)
{
    return &server->conn_activation_lock;
}

const char *qd_server_get_container_name(const qd_server_t *server)
{
    return server->container_name;
}

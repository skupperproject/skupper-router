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

#include "adaptor_listener.h"
#include "adaptor_common.h"
#include "server_private.h"

#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/log.h>

#include <proton/listener.h>
#include <proton/proactor.h>


struct qd_adaptor_listener_t {

    // the following fields are mutably shared between multiple threads and
    // must be protected by holding the lock:
    sys_mutex_t                   lock;
    pn_listener_t                *pn_listener;
    qd_adaptor_listener_accept_t  on_accept;
    qd_listener_admin_status_t    admin_status;  // set by mgmt
    qd_listener_oper_status_t     oper_status;
    int                           ref_count;
    bool                          watched;

    // the following fields are immutable so they may be accessed without
    // holding the lock:

    const qd_dispatch_t          *qd;
    void                         *user_context;
    char                         *service_address;
    char                         *name;
    char                         *host_port;
    qd_log_source_t              *log_source;
    qd_handler_context_t          event_handler;
    qdr_watch_handle_t            addr_watcher;
    int                           backlog;

    // must hold _listeners_lock:
    DEQ_LINKS(qd_adaptor_listener_t);
};

DEQ_DECLARE(qd_adaptor_listener_t, qd_adaptor_listener_list_t);
ALLOC_DECLARE(qd_adaptor_listener_t);
ALLOC_DEFINE(qd_adaptor_listener_t);


// list of active listeners, lock must be held
//
static sys_mutex_t _listeners_lock;
static qd_adaptor_listener_list_t _listeners;


// true == all listeners have been cleaned up, callbacks must not reference
// them
//
static bool _finalized;

// There is a window during listener shutdown where connection attempts may occur. These connections need to be
// rejected. Proactor does not provide a "reject" api for the listener, so we have to accept them and immediately close
// them. This is a raw connect event handler that just closes the connection.
//
static void _conn_event_handler(pn_event_t *e, qd_server_t *qd_server, void *context);

static qd_handler_context_t _conn_event_context = {
    .handler = _conn_event_handler,
};

// called during shutdown: must not schedule work!
static void _listener_free(qd_adaptor_listener_t *li)
{
    free(li->name);
    free(li->host_port);
    free(li->service_address);
    sys_mutex_free(&li->lock);
    free_qd_adaptor_listener_t(li);
}

static void _listener_decref(qd_adaptor_listener_t *li)
{
    if (li) {
        sys_mutex_lock(&li->lock);
        assert(li->ref_count > 0);
        if (--li->ref_count == 0) {
            sys_mutex_unlock(&li->lock);

            sys_mutex_lock(&_listeners_lock);
            DEQ_REMOVE(_listeners, li);
            sys_mutex_unlock(&_listeners_lock);

            // expect the proton listener has been successfully closed
            assert(li->pn_listener == 0);

            _listener_free(li);
            return;
        }
        sys_mutex_unlock(&li->lock);
    }
}


// pn_listener callback invoked on the proactor listener thread, may run
// concurrently with other callbacks or management
//
static void _listener_event_handler(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    if (!_finalized) {

        qd_adaptor_listener_t *li = (qd_adaptor_listener_t*) context;
        qd_log_source_t *log = li->log_source;

        switch (pn_event_type(e)) {
            case PN_LISTENER_OPEN: {
                bool up = false;
                sys_mutex_lock(&li->lock);
                if (li->oper_status == QD_LISTENER_OPER_OPENING) {  // may have been closed
                    up              = true;
                    li->oper_status = QD_LISTENER_OPER_UP;
                }
                sys_mutex_unlock(&li->lock);
                if (up)
                    qd_log(log, QD_LOG_INFO, "Listener %s: listening for client connections on %s with backlog %d", li->name, li->host_port, li->backlog);
                break;
            }

        case PN_LISTENER_ACCEPT:
            qd_log(log, QD_LOG_INFO, "Listener %s: new incoming client connection to %s", li->name, li->host_port);

            // block qd_adapter_listener_close() from returning during the accept call:
            sys_mutex_lock(&li->lock);
            if (li->on_accept)
                li->on_accept(li, pn_event_listener(e), li->user_context);
            else {
                // the adaptor_listener is closing but a connection attempt arrived before the
                // cleanup is complete.  Need to accept it then close it when the connection
                // completes
                pn_raw_connection_t *close_me = pn_raw_connection();
                pn_raw_connection_set_context(close_me, &_conn_event_context);
                pn_listener_raw_accept(pn_event_listener(e), close_me);
            }
            sys_mutex_unlock(&li->lock);
            break;

        case PN_LISTENER_CLOSE: {
            pn_condition_t *cond = pn_listener_condition(pn_event_listener(e));
            if (cond && pn_condition_is_set(cond)) {
                qd_log(log, QD_LOG_ERROR, "Listener %s: proactor listener error on %s: %s (%s)", li->name,
                       li->host_port, pn_condition_get_name(cond), pn_condition_get_description(cond));
            } else {
                qd_log(log, QD_LOG_INFO, "Listener %s: stopped listening for client connections on %s", li->name,
                       li->host_port);
            }

            sys_mutex_lock(&li->lock);

            li->ref_count += 1;  // temporary - prevent freeing

            // the pn_listener holds a counted reference to this listener in its context.
            if (pn_listener_get_context(li->pn_listener) != 0) {
                pn_listener_set_context(li->pn_listener, 0);
                li->ref_count -= 1;
            }
            li->pn_listener = 0;

            bool re_created = false;
            if (li->admin_status == QD_LISTENER_ADMIN_ENABLED) {
                // close is due to loss of available consumers - see
                // _on_watched_address_update()
                if (li->oper_status != QD_LISTENER_OPER_DOWN) {
                    // The VAN address now has consumers - it is possible that
                    // new consumers arrived since the close was
                    // started. Re-establish the listening socket:
                    re_created = true;
                    li->pn_listener = pn_listener();
                    pn_listener_set_context(li->pn_listener, &li->event_handler);
                    li->ref_count += 1;  // reference via pn_listener context

                    // Note: the call to pn_proactor_listen may cause this
                    // listener event handler to start executing immediately on
                    // another thread.
                    pn_proactor_listen(qd_server_proactor(qd_server), li->pn_listener, li->host_port,
                                       li->backlog);
                }
            }

            sys_mutex_unlock(&li->lock);

            if (re_created) {
                qd_log(log, QD_LOG_DEBUG, "Re-creating listener %s socket on address %s for service address %s",
                       li->name, li->host_port, li->service_address);
            }

            _listener_decref(li);  // drop temporary reference
            break;
        }

        default:
            break;
        }
    }
}


// Callback when there is a change in the number of active consumers (servers)
// for the VAN address associated with a listener. The lifecycle of the
// listening socket associated with a listener is determined by the number of
// active consumers: we close the listening socket to prevent client access
// when there are no servers available.
//
// Note well that this is called on the general handler thread and may execute
// in parallel with the management agent thread which creates and deletes
// listeners, as well as the listener proactor thread.
//
static void _on_watched_address_update(void     *context,
                                       uint32_t  local_consumers,
                                       uint32_t  in_proc_consumers,
                                       uint32_t  remote_consumers,
                                       uint32_t  local_producers)
{
    if (!_finalized) {

        // the address watch holds a reference count to the listener so it cannot be
        // deleted during this call
        //
        qd_adaptor_listener_t *li = (qd_adaptor_listener_t*) context;

        qd_log(li->log_source, QD_LOG_TRACE,
               "Listener %s (%s) service address %s consumer count updates:"
               " local=%" PRIu32 " in-process=%" PRIu32 " remote=%" PRIu32,
               li->name, li->host_port, li->service_address, local_consumers, in_proc_consumers, remote_consumers);

        sys_mutex_lock(&li->lock);

        bool created = false;
        bool stopped = false;
        if (li->admin_status == QD_LISTENER_ADMIN_ENABLED) {
            const bool can_listen = (local_consumers || remote_consumers || in_proc_consumers);

            if (can_listen) {
                if (li->oper_status == QD_LISTENER_OPER_DOWN) {
                    li->oper_status = QD_LISTENER_OPER_OPENING;
                    if (!li->pn_listener) {
                        created = true;
                        li->pn_listener = pn_listener();
                        pn_listener_set_context(li->pn_listener, &li->event_handler);
                        li->ref_count += 1;  // for pn_listener context reference

                        // Note: after this call the _listener_event_handler may be called immediately on another thread:
                        pn_proactor_listen(qd_server_proactor(li->qd->server), li->pn_listener, li->host_port, li->backlog);
                    }
                }
            } else {  // close listener
                if (li->oper_status != QD_LISTENER_OPER_DOWN) {
                    li->oper_status = QD_LISTENER_OPER_DOWN;
                    if (li->pn_listener) {
                        stopped = true;
                        // Note: after this call the _listener_event_handler may be called immediately on another thread:
                        pn_listener_close(li->pn_listener);
                        // finish cleanup in PN_LISTENER_CLOSE event
                    }
                }
            }
        }

        sys_mutex_unlock(&li->lock);

        if (stopped)
            qd_log(li->log_source, QD_LOG_DEBUG, "Closing listener %s (%s) socket: no service available for address %s",
                   li->name, li->host_port, li->service_address);

        else if (created)
            qd_log(li->log_source, QD_LOG_DEBUG, "Creating listener %s (%s) socket for service address %s", li->name,
                   li->host_port, li->service_address);
    }
}


static void _on_watched_address_cancel(void *context)
{
    if (!_finalized) {
        qd_adaptor_listener_t *li = (qd_adaptor_listener_t*) context;

        sys_mutex_lock(&li->lock);
        if (li->pn_listener) {
            pn_listener_close(li->pn_listener);
        }
        sys_mutex_unlock(&li->lock);

        _listener_decref(li);
    }
}

static void _conn_event_handler(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    // the listener is closing - reject the connection
    if (pn_event_type(e) == PN_RAW_CONNECTION_CONNECTED) {
        pn_raw_connection_close(pn_event_raw_connection(e));
    }
}

qd_adaptor_listener_t *qd_adaptor_listener(const qd_dispatch_t *qd,
                                           const qd_adaptor_config_t *config,
                                           qd_log_source_t *log_source,
                                           int backlog)
{
    qd_adaptor_listener_t *li = new_qd_adaptor_listener_t();
    ZERO(li);
    li->ref_count = 1;  // for caller
    li->qd = qd;
    li->name = qd_strdup(config->name);
    li->host_port = qd_strdup(config->host_port);
    li->service_address = qd_strdup(config->address);
    li->log_source = log_source;
    li->backlog = backlog;

    sys_mutex_init(&li->lock);
    li->admin_status = QD_LISTENER_ADMIN_ENABLED;
    li->oper_status = QD_LISTENER_OPER_DOWN;

    li->event_handler.context = li;
    li->event_handler.handler = _listener_event_handler;

    sys_mutex_lock(&_listeners_lock);
    DEQ_INSERT_TAIL(_listeners, li);
    sys_mutex_unlock(&_listeners_lock);

    return li;
}


void qd_adaptor_listener_listen(qd_adaptor_listener_t *li,
                                qd_adaptor_listener_accept_t on_accept,
                                void *context)
{
    sys_mutex_lock(&li->lock);

    assert(li->ref_count > 0);
    assert(!li->watched);
    assert(!li->on_accept);

    li->on_accept = on_accept;
    li->user_context = context;
    li->watched = true;
    li->ref_count += 1;  // for watcher
    li->addr_watcher = qdr_core_watch_address(qd_router_core(li->qd),
                                              li->service_address,
                                              QD_ITER_HASH_PREFIX_MOBILE,
                                              li->qd->default_treatment,
                                              _on_watched_address_update,
                                              _on_watched_address_cancel,
                                              (void*) li);
    sys_mutex_unlock(&li->lock);
}


void qd_adaptor_listener_close(qd_adaptor_listener_t *li)
{
    if (li) {
        sys_mutex_lock(&li->lock);
        li->admin_status = QD_LISTENER_ADMIN_DELETED;
        li->oper_status = QD_LISTENER_OPER_DOWN;
        li->on_accept = 0;
        li->user_context = 0;

        // Cancel the address watcher. The cancel callback will clean up the
        // pn_listener.
        if (li->watched)
            qdr_core_unwatch_address(qd_dispatch_router_core(li->qd), li->addr_watcher);
        sys_mutex_unlock(&li->lock);

        _listener_decref(li);
    }
}


qd_listener_oper_status_t qd_adaptor_listener_oper_status(const qd_adaptor_listener_t *li)
{
    assert(li);
    sys_mutex_lock((sys_mutex_t *) &li->lock);
    const qd_listener_oper_status_t value = li->oper_status;
    sys_mutex_unlock((sys_mutex_t *) &li->lock);
    return value;
}


void qd_adaptor_listener_init()
{
    sys_mutex_init(&_listeners_lock);
    DEQ_INIT(_listeners);
    _finalized = false;
}


void qd_adaptor_listener_finalize()
{
    _finalized = true;
    qd_adaptor_listener_t *li = DEQ_HEAD(_listeners);
    while (li) {
        DEQ_REMOVE_HEAD(_listeners);
        _listener_free(li);
        li = DEQ_HEAD(_listeners);
    }
    sys_mutex_free(&_listeners_lock);
}

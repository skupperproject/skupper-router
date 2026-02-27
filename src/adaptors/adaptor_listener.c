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

// Address selection strategies for multi-address listener
const char *ADDRESS_STRATEGY_PRIORITY = "priority";

typedef enum {
    QD_ADDR_STRATEGY_NONE,
    QD_ADDR_STRATEGY_PRIORITY // select address with highest priority
} qd_multi_address_strategy_t;

struct qd_listener_address_t
{
    DEQ_LINKS(qd_listener_address_t);
    char                  *address;
    int                    priority;
    sys_atomic_t           ref_count;
    qdr_watch_handle_t     addr_watcher;
    bool                   reachable; // true if there is at least one active consumer
    qd_adaptor_listener_t *listener; // needed by the address watcher callbacks
    uint64_t               connections_opened;
    vflow_record_t        *vflow;
};

ALLOC_DEFINE(qd_listener_address_t);
DEQ_DECLARE(qd_listener_address_t, qd_listener_address_list_t);

struct qd_adaptor_listener_t {

    // the following fields are mutably shared between multiple threads and
    // must be protected by holding the lock:
    sys_mutex_t                   lock;
    pn_listener_t                *pn_listener;
    qd_adaptor_listener_accept_t  on_accept;
    qd_listener_admin_status_t    admin_status;  // set by mgmt
    qd_listener_oper_status_t     oper_status;
    char                         *error_message;
    int                           ref_count;
    qd_listener_address_list_t    addresses;
    qd_multi_address_strategy_t   address_strategy;
    uint32_t                      reachable_address_count;

    // the following fields are immutable so they may be accessed without
    // holding the lock:

    const qd_dispatch_t          *qd;
    void                         *user_context;
    char                         *name;
    char                         *host;
    char                         *port;
    char                         *host_port;
    qd_log_module_t               log_module;
    qd_handler_context_t          event_handler;
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

// Proactor does not provide a "reject" api for the listener, so we have to accept them and immediately close them. This
// is a raw connect event handler that just closes the connection. See qd_adaptor_listener_deny()
//
static void _deny_conn_handler(pn_event_t *e, qd_server_t *qd_server, void *context);
static qd_handler_context_t _deny_conn_context = {
    .handler = _deny_conn_handler,
};

static void _set_error_message_LH(qd_adaptor_listener_t *li, const char *m)
{
    free(li->error_message);

    if (!!m) {
        li->error_message = qd_strdup(m);
    } else {
        li->error_message = 0;
    }
}

static void _listener_address_free(qd_listener_address_t *addr) {
    assert(!DEQ_NEXT(addr) && !DEQ_PREV(addr));

    // This function can be called from listener adaptor finalize when the
    // vanflow adaptor is gone already. Vanflow record must be ended before
    // we get here.
    assert(!addr->vflow);

    free(addr->address);
    free_qd_listener_address_t(addr);
}

// called during shutdown: must not schedule work!
static void _listener_free(qd_adaptor_listener_t *li)
{
    qd_listener_address_t *addr = 0;
    while ((addr = DEQ_HEAD(li->addresses)))  {
        DEQ_REMOVE_HEAD(li->addresses);
        _listener_address_free(addr);
    }

    free(li->name);
    free(li->host);
    free(li->port);
    free(li->host_port);

    free(li->error_message);
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

        qd_log_module_t log_module = li->log_module;

        switch (pn_event_type(e)) {
            case PN_LISTENER_OPEN: {
                bool up = false;
                sys_mutex_lock(&li->lock);
                if (li->oper_status == QD_LISTENER_OPER_OPENING) {  // may have been closed
                    up              = true;
                    li->oper_status = QD_LISTENER_OPER_UP;
                    _set_error_message_LH(li, 0);
                }
                sys_mutex_unlock(&li->lock);
                if (up)
                    qd_log(log_module, QD_LOG_INFO,
                           "Listener %s: listening for client connections on %s with backlog %d", li->name,
                           li->host_port, li->backlog);
                break;
            }

         case PN_LISTENER_ACCEPT: {
             bool denied = false;

             // Log incoming client connections at DEBUG level since these can flood the router logs.
             qd_log(log_module, QD_LOG_DEBUG, "Listener %s: new incoming client connection to %s", li->name,
                    li->host_port);

             // block qd_adapter_listener_close() from returning during the accept call:
             sys_mutex_lock(&li->lock);
             if (li->on_accept)
                 li->on_accept(li, pn_event_listener(e), li->user_context);
             else {
                 // The adaptor_listener is closing but a connection attempt arrived before the
                 // cleanup is complete. Deny the connection attempt.
                 qd_adaptor_listener_deny_conn(li, pn_event_listener(e));
                 denied = true;
             }
             sys_mutex_unlock(&li->lock);

             if (denied)
                 qd_log(log_module, QD_LOG_DEBUG,
                        "Listener %s: denied new incoming client connection to %s: service not available",
                        li->name, li->host_port);
             break;
         }

        case PN_LISTENER_CLOSE: {
            bool hard_failure = false;
            pn_condition_t *cond = pn_listener_condition(pn_event_listener(e));
            if (cond && pn_condition_is_set(cond)) {
                qd_log(log_module, QD_LOG_ERROR, "Listener %s: proactor listener error on %s: %s (%s)", li->name,
                       li->host_port, pn_condition_get_name(cond), pn_condition_get_description(cond));
                hard_failure = true;
            } else {
                qd_log(log_module, QD_LOG_INFO, "Listener %s: stopped listening for client connections on %s",
                       li->name, li->host_port);
            }

            sys_mutex_lock(&li->lock);

            li->ref_count += 1;  // temporary - prevent freeing

            if (hard_failure) {
                _set_error_message_LH(li, pn_condition_get_description(cond));
            } else {
                _set_error_message_LH(li, 0);
            }

            // the pn_listener holds a counted reference to this listener in its context.
            if (pn_listener_get_context(li->pn_listener) != 0) {
                pn_listener_set_context(li->pn_listener, 0);
                li->ref_count -= 1;
            }
            li->pn_listener = 0;

            bool re_created = false;
            qd_listener_address_t *addr = 0;
            if (li->admin_status == QD_LISTENER_ADMIN_ENABLED) {
                // close is due to loss of available consumers - see
                // _on_watched_address_update()
                if (li->oper_status != QD_LISTENER_OPER_DOWN && !hard_failure) {
                    // The VAN address now has consumers - it is possible that
                    // new consumers arrived since the close was
                    // started. Re-establish the listening socket:
                    re_created = true;

                    addr = DEQ_HEAD(li->addresses);
                    DEQ_FIND(addr, addr->reachable);
                    assert(addr);
                    sys_atomic_inc(&addr->ref_count); // temporary ref for the log record below

                    li->pn_listener = pn_listener();
                    pn_listener_set_context(li->pn_listener, &li->event_handler);
                    li->ref_count += 1;  // reference via pn_listener context

                    // Note: the call to pn_proactor_listen may cause this
                    // listener event handler to start executing immediately on
                    // another thread.
                    pn_proactor_listen(qd_server_proactor(qd_server), li->pn_listener, li->host_port, li->backlog);
                }
            }

            sys_mutex_unlock(&li->lock);

            if (re_created) {
                qd_log(log_module, QD_LOG_DEBUG, "Re-creating listener %s socket on address %s for service address %s",
                       li->name, li->host_port, addr->address);
                qd_adaptor_listener_address_decref(addr); // drop temporary reference
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
        qd_listener_address_t *addr = (qd_listener_address_t*) context;
        assert(addr);
        qd_adaptor_listener_t *li = addr->listener;
        assert(li);

        qd_log(li->log_module, QD_LOG_DEBUG,
               "Listener %s (%s) service address %s consumer count updates:"
               " local=%" PRIu32 " in-process=%" PRIu32 " remote=%" PRIu32,
               li->name, li->host_port, addr->address, local_consumers, in_proc_consumers, remote_consumers);

        sys_mutex_lock(&li->lock);

        bool created = false;
        bool stopped = false;
        if (li->admin_status == QD_LISTENER_ADMIN_ENABLED) {
            const bool has_consumers = (local_consumers || remote_consumers || in_proc_consumers);

            if (addr->reachable && !has_consumers) {
                addr->reachable = false;
                assert(li->reachable_address_count > 0);
                li->reachable_address_count--;
            } else if (!addr->reachable && has_consumers) {
                addr->reachable = true;
                li->reachable_address_count++;
            }

            if (li->reachable_address_count > 0) {
                if (li->oper_status == QD_LISTENER_OPER_DOWN) {
                    li->oper_status = QD_LISTENER_OPER_OPENING;
                    if (!li->pn_listener) {
                        created = true;
                        li->pn_listener = pn_listener();
                        pn_listener_set_context(li->pn_listener, &li->event_handler);
                        li->ref_count += 1;  // for pn_listener context reference

                        // Note: after this call the _listener_event_handler may be called immediately on another thread:
                        pn_proactor_listen(qd_server_proactor(li->qd->server), li->pn_listener, li->host_port,
                                           li->backlog);
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

        if (li->address_strategy != QD_ADDR_STRATEGY_NONE && (stopped || created)) {
            // temporary reference to prevent freeing the address while writing the log message below
            sys_atomic_inc(&addr->ref_count);
        }

        sys_mutex_unlock(&li->lock);

        if (stopped)
            qd_log(li->log_module, QD_LOG_DEBUG, "Closing listener %s (%s) socket: no service available for address %s",
                   li->name, li->host_port, addr->address);

        else if (created)
            qd_log(li->log_module, QD_LOG_DEBUG, "Creating listener %s (%s) socket for service address %s", li->name,
                   li->host_port, addr->address);

        if (li->address_strategy != QD_ADDR_STRATEGY_NONE && (stopped || created)) {
            // release temporary reference
            qd_adaptor_listener_address_decref(addr);
        }
    }
}


static void _on_watched_address_cancel(void *context)
{
    if (!_finalized) {
        qd_listener_address_t *addr = (qd_listener_address_t*) context;
        qd_adaptor_listener_t *li = addr->listener;

        sys_mutex_lock(&li->lock);

        if (addr->reachable) {
            assert(li->reachable_address_count > 0);
            li->reachable_address_count--;
        }

        bool stopped = false;
        if (!li->reachable_address_count) {
            if (li->oper_status != QD_LISTENER_OPER_DOWN)
                li->oper_status = QD_LISTENER_OPER_DOWN;
            if (li->pn_listener) {
                stopped = true;
                pn_listener_close(li->pn_listener);
            }
        }

        DEQ_REMOVE(li->addresses, addr);

        sys_mutex_unlock(&li->lock);

        if (stopped)
            qd_log(li->log_module, QD_LOG_DEBUG, "Closing listener %s (%s) socket: no service available for address %s",
                   li->name, li->host_port, addr->address);

        qd_adaptor_listener_address_decref(addr);

        _listener_decref(li);
    }
}

// dummy event handler that is used to deny unwanted connection attempts.
//
static void _deny_conn_handler(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    if (pn_event_type(e) == PN_RAW_CONNECTION_CONNECTED) {
        pn_raw_connection_t *close_me = pn_event_raw_connection(e);
        pn_raw_connection_set_context(close_me, 0);  // prevent further callbacks
        pn_raw_connection_close(close_me);
    }
}

qd_adaptor_listener_t *qd_adaptor_listener(const qd_dispatch_t       *qd,
                                           const qd_adaptor_config_t *config,
                                           qd_log_module_t            module)
{
    qd_adaptor_listener_t *li = new_qd_adaptor_listener_t();
    ZERO(li);
    li->ref_count = 1;  // for caller
    li->qd = qd;
    li->name = qd_strdup(config->name);
    li->host = qd_strdup(config->host);
    li->port = qd_strdup(config->port);
    li->host_port  = qd_strdup(config->host_port);
    li->backlog    = config->backlog;
    li->log_module = module;

    if (!!config->multi_address_strategy &&
        !strcmp(config->multi_address_strategy, ADDRESS_STRATEGY_PRIORITY)) {
        assert(!config->address);
        li->address_strategy = QD_ADDR_STRATEGY_PRIORITY;
    }

    if (li->address_strategy == QD_ADDR_STRATEGY_NONE) {
        // this is a single address listener
        qd_listener_address_t *addr = new_qd_listener_address_t();
        ZERO(addr);
        sys_atomic_init(&addr->ref_count, 1);
        addr->address   = qd_strdup(config->address);
        addr->listener  = li;
        DEQ_INSERT_HEAD(li->addresses, addr);
    }

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
    assert(!li->on_accept);
    assert(!li->reachable_address_count);

    li->on_accept = on_accept;
    li->user_context = context;

    if (li->address_strategy == QD_ADDR_STRATEGY_NONE) {
        // single address listener
        assert(DEQ_SIZE(li->addresses) == 1);
        qd_listener_address_t *addr = DEQ_HEAD(li->addresses);
        li->ref_count += 1;  // for watcher
        addr->addr_watcher = qdr_core_watch_address(qd_router_core(li->qd),
                                                    addr->address,
                                                    QD_ITER_HASH_PREFIX_MOBILE,
                                                    li->qd->default_treatment,
                                                    _on_watched_address_update,
                                                    _on_watched_address_cancel,
                                                    (void *) addr);
    }

    sys_mutex_unlock(&li->lock);
}


qd_error_t qd_adaptor_listener_close(qd_adaptor_listener_t *li)
{
    if (li) {
        sys_mutex_lock(&li->lock);

        if (li->address_strategy != QD_ADDR_STRATEGY_NONE && !!DEQ_SIZE(li->addresses)) {

            sys_mutex_unlock(&li->lock);

            return qd_error(QD_ERROR_VALUE, "listenerAddress entities exist for  multi-address listener %s", li->name);
        }

        li->admin_status = QD_LISTENER_ADMIN_DELETED;
        li->oper_status = QD_LISTENER_OPER_DOWN;
        li->on_accept = 0;
        li->user_context = 0;

        // Cancel the address watchers. The last cancel callback will clean up the
        // pn_listener.
        qd_listener_address_t *addr = DEQ_HEAD(li->addresses);
        while (addr) {
            // End vflow record for multi-address listener. New connection will be rejected
            // after the listener lock is released below.
            if (!!addr->vflow) {
                assert(li->address_strategy != QD_ADDR_STRATEGY_NONE);
                vflow_end_record(addr->vflow);
                addr->vflow = 0;
            }
            qdr_core_unwatch_address(qd_dispatch_router_core(li->qd), addr->addr_watcher);
            addr = DEQ_NEXT(addr);
        }

        sys_mutex_unlock(&li->lock);

        _listener_decref(li);
    }

    return QD_ERROR_NONE;
}


qd_listener_oper_status_t qd_adaptor_listener_oper_status(const qd_adaptor_listener_t *li)
{
    assert(li);
    sys_mutex_lock((sys_mutex_t *) &li->lock);
    const qd_listener_oper_status_t value = li->oper_status;
    sys_mutex_unlock((sys_mutex_t *) &li->lock);
    return value;
}

char *qd_adaptor_listener_error_message(const qd_adaptor_listener_t *li)
{
    char *value = 0;
    assert(li);
    sys_mutex_lock((sys_mutex_t *) &li->lock);
    if (!!li->error_message) {
        value = qd_strdup(li->error_message);
    }
    sys_mutex_unlock((sys_mutex_t *) &li->lock);
    return value;
}


void qd_adaptor_listener_deny_conn(qd_adaptor_listener_t *listener, pn_listener_t *pn_listener)
{
    pn_raw_connection_t *close_me = pn_raw_connection();
    pn_raw_connection_set_context(close_me, &_deny_conn_context);
    pn_listener_raw_accept(pn_listener, close_me);
}

qd_listener_address_t *qd_adaptor_listener_best_address_incref_LH(qd_adaptor_listener_t *listener)
{
    //
    //  NOTE: listener->lock is held by the caller
    //

    assert(listener->address_strategy == QD_ADDR_STRATEGY_PRIORITY);

    // Addresses are ordered by priority in the address list. Find and return the first reachable one.
    qd_listener_address_t *addr = DEQ_HEAD(listener->addresses);
    assert(addr);
    DEQ_FIND(addr, addr->reachable);

    // Return an address even if none of them are reachable
    if (!addr)
        addr =  DEQ_HEAD(listener->addresses);

    // ListenerAddress entity may be deleted by mgmt right after the listener lock gets released.
    // Increase refCount for the caller.
    sys_atomic_inc(&addr->ref_count);

    return addr;
}

void *qd_adaptor_listener_add_address(qd_listener_address_config_t *config)
{
    qd_error_t config_error = QD_ERROR_NONE;

    qd_listener_address_t *new_addr = new_qd_listener_address_t();
    ZERO(new_addr);
    sys_atomic_init(&new_addr->ref_count, 1);
    new_addr->address = strdup(config->address);
    new_addr->priority = config->value;

    sys_mutex_lock(&_listeners_lock);
    qd_adaptor_listener_t *li = DEQ_HEAD(_listeners);
    DEQ_FIND(li, strcmp(li->name, config->listener_name) == 0);
    sys_mutex_unlock(&_listeners_lock);

    if (li) {
        sys_mutex_lock(&li->lock);

        if (li->address_strategy != QD_ADDR_STRATEGY_NONE &&
            li->admin_status == QD_LISTENER_ADMIN_ENABLED) {
            assert(li->ref_count > 0);
             new_addr->listener = li;

            //
            // Insert the new address into the address list.
            // The list is in decreasing order by priority.
            //
            qd_listener_address_t *addr = DEQ_HEAD(li->addresses);
            DEQ_FIND(addr, addr->priority <= new_addr->priority);
            if (addr) {
                addr = DEQ_PREV(addr);
                if (addr)
                    DEQ_INSERT_AFTER(li->addresses, new_addr, addr);
                else
                    DEQ_INSERT_HEAD(li->addresses, new_addr);
            } else {
                 DEQ_INSERT_TAIL(li->addresses, new_addr);
            }

            //
            // Start address watching
            //
            li->ref_count += 1;  // for watcher
            new_addr->addr_watcher = qdr_core_watch_address(qd_router_core(li->qd),
                                                            new_addr->address,
                                                            QD_ITER_HASH_PREFIX_MOBILE,
                                                            li->qd->default_treatment,
                                                            _on_watched_address_update,
                                                            _on_watched_address_cancel,
                                                            (void *)new_addr);
            sys_mutex_unlock(&li->lock);
        } else {
            sys_mutex_unlock(&li->lock);
            if (li->address_strategy == QD_ADDR_STRATEGY_NONE) {
                config_error = qd_error(QD_ERROR_VALUE, "Must not assign new address to non multi-address listener %s", config->listener_name);
            } else {
                config_error = qd_error(QD_ERROR_VALUE, "Listener for new address is being deleted %s", config->listener_name);
            }
        }
    } else {
        config_error = qd_error(QD_ERROR_VALUE, "Listener for new address does not exist %s", config->listener_name);
    }

    if (config_error == QD_ERROR_NONE) {
        //
        //  Create a vflow record for this listener address
        //
        new_addr->vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
        vflow_set_string(new_addr->vflow, VFLOW_ATTRIBUTE_PROTOCOL,         "tcp");
        vflow_set_string(new_addr->vflow, VFLOW_ATTRIBUTE_NAME,             new_addr->listener->name);
        vflow_set_string(new_addr->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, new_addr->listener->host);
        vflow_set_string(new_addr->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, new_addr->listener->port);
        vflow_set_string(new_addr->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      new_addr->address);
        vflow_set_uint64(new_addr->vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4,    0);
    } else {
        // error occured
        _listener_address_free(new_addr);
        new_addr = 0;
    }

    return (void *)new_addr;
}

void qd_adaptor_listener_delete_address(qd_dispatch_t *qd, void *impl)
{
    qd_listener_address_t *address = (qd_listener_address_t *) impl;
    if (address) {
        qd_adaptor_listener_t *li = address->listener;
        assert(li);

        // End the vanflow record here.
        if (address->vflow) {
            assert(li->address_strategy != QD_ADDR_STRATEGY_NONE);
            vflow_end_record(address->vflow);
            address->vflow = 0;
        }

        sys_mutex_lock(&li->lock);

        qdr_core_unwatch_address(qd_dispatch_router_core(li->qd), address->addr_watcher);

        sys_mutex_unlock(&li->lock);
        // address is freed  in the _on_watched_address_cancel callback
    }
}

void qd_adaptor_listener_address_decref(qd_listener_address_t  *addr)
{
    if (sys_atomic_dec(&addr->ref_count) == 1) {
        _listener_address_free(addr);
    }
}

char *qd_adaptor_listener_address_string(qd_listener_address_t  *addr)
{
    return addr->address;
}

vflow_record_t *qd_adaptor_listener_address_vflow(qd_listener_address_t  *addr)
{
    return addr->vflow;
}

void qd_adaptor_listener_address_vflows_end(qd_adaptor_listener_t *li)
{
    sys_mutex_lock(&li->lock);
    qd_listener_address_t *addr = DEQ_HEAD(li->addresses);
    while (addr) {
        if (addr->vflow) {
            vflow_end_record(addr->vflow);
            addr->vflow = 0;
        }
        addr = DEQ_NEXT(addr);
    }
    sys_mutex_unlock(&li->lock);
}

void qd_adaptor_listener_address_connection_opened(qd_listener_address_t  *addr)
{
    addr->connections_opened++;
    assert(!!addr->vflow);
    vflow_set_uint64(addr->vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, addr->connections_opened);
}

void qd_adaptor_listener_init(void)
{
    sys_mutex_init(&_listeners_lock);
    DEQ_INIT(_listeners);
    _finalized = false;
}

void qd_adaptor_listener_finalize(void)
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

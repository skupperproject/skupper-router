#ifndef __adaptor_listener_h__
#define __adaptor_listener_h__
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


//
// An adaptor listener controls the lifecycle of a proactor listener based on
// the availability of consumers for a given service address
//

#include <qpid/dispatch/protocol_adaptor.h>

#include "adaptor_common.h"

typedef struct qd_adaptor_listener_t qd_adaptor_listener_t;

typedef struct qd_listener_address_t qd_listener_address_t;

// Get the currently optimal listener address for a new connection and increase its refCount
qd_listener_address_t *qd_adaptor_listener_best_address_incref_LH(qd_adaptor_listener_t *listener);

// Get listener address as string
char *qd_adaptor_listener_address_string(qd_listener_address_t *addr);

// Get the vflow record of a listener address
vflow_record_t *qd_adaptor_listener_address_vflow(qd_listener_address_t *addr);

// End listener address vflow records
void qd_adaptor_listener_address_vflows_end(qd_adaptor_listener_t *li);

// Increase opened connections counter for this address
void qd_adaptor_listener_address_connection_opened(qd_listener_address_t *addr);

// Decrement the refCount of a listener address
void qd_adaptor_listener_address_decref(qd_listener_address_t *addr);

// Add a new item to the service address list of a multi-address listener
void *qd_adaptor_listener_add_address(qd_listener_address_config_t *config);

// Delete an item from service address list of a multi-address listener
void qd_adaptor_listener_delete_address(qd_dispatch_t *qd, void *imp);

// Callback invoked when a client has connected to the listener and is waiting to be accepted.  The application may
// accept the new connection during this callback by allocating a pn_raw_connection_t and passing it to
// pn_listener_raw_accept().  This callback may instead deny the new connection by calling
// qd_adaptor_listener_deny_conn(). When denying the connection this callback MUST NOT call pn_listener_raw_accept()!
//
// This callback is run from the proactor listener event handler thread.
//
typedef void (*qd_adaptor_listener_accept_t)(qd_adaptor_listener_t *listener,
                                             pn_listener_t *pn_listener,
                                             void *context);

// Create a listener for the given configuration.
//
qd_adaptor_listener_t *qd_adaptor_listener(const qd_dispatch_t       *qd,
                                           const qd_adaptor_config_t *config,
                                           qd_log_module_t            log_module);

// Start listening on the given listener. Note that the on_accept callback may
// be invoked during this call on another thread.
//
void qd_adaptor_listener_listen(qd_adaptor_listener_t *listener,
                                qd_adaptor_listener_accept_t on_accept,
                                void *context);

// Stop listening and dispose of the listener. The caller must not reference
// the listener again after this call if it returns no error. It is guaranteed that the on_accept
// callback will never be invoked after this call returns without error.
// The call returns an error if the listener cannot be deleted (e.g. a multi-address listener can only
// be deleted after all of its addresses are deleted).
qd_error_t qd_adaptor_listener_close(qd_adaptor_listener_t *listener);

// Get the operational state of the listener
//
qd_listener_oper_status_t qd_adaptor_listener_oper_status(const qd_adaptor_listener_t *listener);

// Get the error string from the listener - The returned string (if not NULL) has been allocated and must
// be freed by the caller.
//
char *qd_adaptor_listener_error_message(const qd_adaptor_listener_t *listener);

// Deny the new connection rather than accepting it. This may only be called during the
// qd_adaptor_listener_accept_t callback
//
void qd_adaptor_listener_deny_conn(qd_adaptor_listener_t *listener, pn_listener_t *pn_listener);

#endif // __adaptor_listener_h__

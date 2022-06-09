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

// Callback invoked with a client has connected to the listener and needs to be
// accepted via a call to pn_listener_raw_accept(). This callback is run from
// the proactor listener event handler thread.
//
typedef void (*qd_adaptor_listener_accept_t)(qd_adaptor_listener_t *listener,
                                             pn_listener_t *pn_listener,
                                             void *context);

// Create a listener for the given configuration.
//
qd_adaptor_listener_t *qd_adaptor_listener(const qd_dispatch_t *qd,
                                           const qd_adaptor_config_t *config,
                                           qd_log_source_t *log_source);

// Start listening on the given listener. Note that the on_accept callback may
// be invoked during this call on another thread.
//
void qd_adaptor_listener_listen(qd_adaptor_listener_t *listener,
                                qd_adaptor_listener_accept_t on_accept,
                                void *context);

// Stop listening and dispose of the listener. The caller must not reference
// the listener again after this call. It is guaranteed that the on_accept
// callback will never be invoked after this call returns.
//
void qd_adaptor_listener_close(qd_adaptor_listener_t *listener);

// Get the operational state of the listener
//
qd_listener_oper_status_t qd_adaptor_listener_oper_status(const qd_adaptor_listener_t *listener);


#endif // __adaptor_listener_h__

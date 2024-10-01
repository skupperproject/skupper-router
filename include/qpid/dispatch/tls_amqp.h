#ifndef __tls_amqp_h__
#define __tls_amqp_h__ 1
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

#include "qpid/dispatch/tls_common.h"

typedef struct pn_transport_t pn_transport_t;

/**
 * API for TLS operations specific to Proton AMQP connections.
 *
 * Note well: these APIs apply only to TLS config/sessions of type QD_TLS_TYPE_PROTON_AMQP!  Proton raw connection based
 * TLS sessions are not supported.  See tls.h and tls_raw_io.h.
 */


/**
 * Create a new TLS session
 *
 * @param config the TLS configuration used to create the session
 * @param tport transport associated with the session's connection
 * @param allow_unencrypted if true permit accepting incoming unencrypted connections
 * @return a new TLS session or 0 on error. If error qd_error() is set.
 */
qd_tls_session_t *qd_tls_session_amqp(qd_tls_config_t *config, pn_transport_t *tport, bool allow_unencrypted);


/**
 * Get the user identifier associated with the TLS session.
 *
 * @param session the active TLS session to retrieve the user id from.
 * @return string containing user name if query succeeds else 0. Caller must free() returned user name string when no
 * longer used.
 */
char *qd_tls_session_get_user_id(qd_tls_session_t *session);

#endif


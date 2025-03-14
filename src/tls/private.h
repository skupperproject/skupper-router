#ifndef __tls_private_h__
#define __tls_private__ 1
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

#include "qpid/dispatch/tls_raw.h"
#include "qpid/dispatch/tls_amqp.h"

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/log.h"

typedef struct qd_tls_context_t qd_tls_context_t;
typedef struct qd_proton_config_t qd_proton_config_t;
typedef struct pn_tls_config_t pn_tls_config_t;
typedef struct pn_tls_t pn_tls_t;
typedef struct pn_ssl_domain_t pn_ssl_domain_t;
typedef struct pn_ssl_t pn_ssl_t;


/**
 * Handle for Proton TLS configuration.
 *
 * A Proton TLS configuration cannot be destroyed until all related TLS sessions have closed and the parent TLS
 * configuration has been deleted. A reference count is used to ensure this. Additionally a mutex is provided to
 * single-thread the creation and deletion of proton sessions. This is necessary since proton sessions are
 * multi-threaded.
 */
struct qd_proton_config_t {
    // only one of the following Proton pointers will be set based on whether this configuration is for a raw connection
    // or an AMQP connection
    pn_tls_config_t *pn_raw;
    pn_ssl_domain_t *pn_amqp;
    sys_mutex_t      lock;  // held when creating and deleting sessions
    sys_atomic_t     ref_count;  // for parent qd_tls_config_t and all children qd_tls_session_t
};

/**
 * Context for a single per-connection TLS data stream
 */
struct qd_tls_session_t {
    qd_proton_config_t   *proton_tls_cfg;  // TLS Proton configuration used by session

    // only one of the following Proton session pointers will be set based on whether this session is for a raw
    // connection or an AMQP connection
    pn_tls_t             *pn_raw;
    pn_ssl_t             *pn_amqp;

    void                          *user_context;
    qd_tls_session_on_secure_cb_t *on_secure_cb;

    // copies from parent qd_tls_config_t to avoid locking during I/O:
    char                  *ssl_profile_name;
    char                  *uid_format;
    uint64_t               ordinal;

    bool                   tls_error;
    bool                   output_eos;       // pn_tls_close_output() called
    bool                   raw_read_drained; // raw conn read closed and all buffer read
    bool                   input_drained;    // no more decrypted output, raw conn read closed
    bool                   output_flushed;   // encrypt done, raw conn write closed

    uint64_t encrypted_output_bytes;
    uint64_t encrypted_input_bytes;
};

/**
 * Context for a TLS configuration object.
 *
 * Factory for creating qd_tls_session_t instances.
 *
 * Note that this differs from the qd_ssl2_profile_t object in that this object holds the run-time configuration
 * state. It is created by reading the certificate configuration provided by the sslProfile via the qd_ssl2_profile_t.
 */
struct qd_tls_config_t {
    DEQ_LINKS(qd_tls_config_t);   // for parent qd_tls_context_t list
    sys_mutex_t         lock;
    char               *ssl_profile_name;
    char               *uid_format;      // lock must be held
    qd_proton_config_t *proton_tls_cfg;  // lock must be held
    qd_tls_type_t       p_type;
    sys_atomic_t        ref_count;
    uint64_t            ordinal;               // lock must be held
    uint64_t            oldest_valid_ordinal;  // lock must be held

    // Invoked on management thread whenever sslProfile is updated
    qd_tls_config_update_cb_t  update_callback;
    void                      *update_context;

    bool                authenticate_peer;
    bool                verify_hostname;
    bool                is_listener;
};

DEQ_DECLARE(qd_tls_config_t, qd_tls_config_list_t);

/**
 * Top-level TLS context
 *
 * Maintains all TLS state associated with an sslProfile record.  Also includes the currently active qd_tls_config_t
 * instances generated from that sslProfile record.
 */

struct qd_tls_context_t {
    DEQ_LINKS(qd_tls_context_t);
    char                 *ssl_profile_name;
    qd_ssl2_profile_t     profile;
    qd_tls_config_list_t  tls_configs;
};

DEQ_DECLARE(qd_tls_context_t, qd_tls_context_list_t);

// Internal use only!
void tls_private_release_display_name_service(void);
bool tls_private_validate_uid_format(const char *format);
char *tls_private_lookup_display_name(const char *ssl_profile_name, const char *user_id);
pn_tls_config_t *tls_private_allocate_raw_config(const char *ssl_profile_name, const qd_ssl2_profile_t *config,
                                                 bool is_listener, bool verify_hostname, bool authenticate_peer);
pn_ssl_domain_t *tls_private_allocate_amqp_config(const char *ssl_profile_name, const qd_ssl2_profile_t *config,
                                                  bool is_listener, bool verify_hostname, bool authenticate_peer);
qd_proton_config_t *qd_proton_config(pn_tls_config_t *tls_cfg, pn_ssl_domain_t *ssl_cfg);
void qd_proton_config_decref(qd_proton_config_t *p_cfg);
#endif


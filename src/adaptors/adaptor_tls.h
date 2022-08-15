#ifndef __adaptor_tls_h__
#define __adaptor_tls_h__ 1

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
#include <proton/tls.h>

#include "qpid/dispatch/alloc.h"
#include "adaptor_common.h"
#include "adaptor_buffer.h"
#include "qpid/dispatch/protocol_adaptor.h"

typedef struct qd_tls_t             qd_tls_t;
typedef struct qd_raw_buffer_t      qd_raw_buffer_t;
typedef void (*qd_tls_post_handshake_action_t) (void *context);
typedef bool (*qd_tls_alpn_handler_t)          (qd_tls_t *tls, void *context);

struct qd_tls_t {
    pn_tls_t                        *tls_session;
    pn_tls_config_t                 *tls_config;
    void                            *user_context;
    // a hook into any post handshake actions you want to perform on the incoming side.
    qd_tls_post_handshake_action_t  post_handshake_action_incoming;
    // a hook into any post handshake actions you want to perform on the outgoing side.
    qd_tls_post_handshake_action_t  post_handshake_action_outgoing;
    qd_tls_alpn_handler_t           alpn_handler;
    pn_raw_connection_t             *pn_raw_conn;
    qdr_connection_info_t           *connection_info;
    qd_log_source_t                 *log_source;
    uint64_t                         conn_id;
    bool                             tls_has_output;
    bool                             tls_error;
    bool                             alpn_check_complete;
    bool                             action_incoming_called; // used to make sure post_handshake_action_incoming is called only once.
    bool                             action_outgoing_called; // used to make sure post_handshake_action_outgoing is called only once.
};

struct qd_raw_buffer_t { // Contains a single pn_raw_buffer_t
    pn_raw_buffer_t pn_raw_buff;
    DEQ_LINKS(qd_raw_buffer_t);
};

ALLOC_DECLARE(qd_tls_t);
ALLOC_DECLARE(qd_raw_buffer_t);
DEQ_DECLARE(qd_raw_buffer_t, qd_raw_buffer_list_t);

qd_tls_t *qd_tls(void *context,
                 pn_raw_connection_t *raw_conn,
                 qd_tls_post_handshake_action_t post_handshake_action_incoming,
                 qd_tls_post_handshake_action_t post_handshake_action_outgoing,
                 qd_tls_alpn_handler_t          alpn_handler,
                 uint64_t conn_id,
                 qd_log_source_t *log_source,
                 qdr_connection_info_t *connection_info);

/**
 * Configure proton pn_tls objects using passed in config
 * and start the tls session by calling the pn_tls_start function.
 *     Logs sslProfile configuration failures at INFO level.
 * On success:
 *     pn_tls_config and pn_tls_session objects in tls are set up
 *     Returns true
 * On failure:
 *     Error log is written
 *     All in-progress pn_tls objects are destroyed.
 *     Returns false.
 *
 * @param tls - Pointer to the qd_tls_t object which contains the tls session information
 * @param config   - Pointer to the qd_adaptor_config_t object which contains the config information
 * @param qd -     - Pointer to the qd_dispatch_t object.
 * @param is_listener - set this flag to true if the tls session is initialized on a listener, false otherwise.
 * @prototcols     - An array of supported TLS protocol version, TLSv1.2 or TSLv1.3 or both
 */
bool qd_tls_start(qd_tls_t                  *tls,
                  const qd_adaptor_config_t *config,
                  const qd_dispatch_t       *qd,
                  bool                       is_listener,
                  const char                *protocols[]);

/**
 * Cleans up any pending buffers and stops/frees the proton tls session and the config.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
void qd_tls_stop(qd_tls_t *tls);

/**
 * Returns the value of tls->tls_has_output flag.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool qd_tls_has_output(qd_tls_t *tls);

/**
 * Returns true if the TLS handshake has successfully completed and we are ready to send and receive data.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool qd_tls_is_secure(qd_tls_t *tls);

/**
 * Decrypts a passed in single encrypted pn_raw_buffer_t and populates the passed in qd_raw_buffer_list_t with the decrypted buffers.
 * Participates in the TLS handshake. Must always be called with a non zero encrypted_buff.
 *
 * Writes error messages at INFO level to the logger present in the the passed in tls object.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 * @param encrypted_buff - a pointer to a single encrypted raw buffer
 * @param encrypted_buffs - A pointer to a list of type qd_raw_buffer_list_t which will contain the encrypted buffs. Make sure this list is initialized and empty.
 *
 * @return true if there is no error when trying to decrypt an encrypted buffer, false otherwise
 *
 */
bool qd_tls_decrypt_incoming(qd_tls_t *tls, const pn_raw_buffer_t *encrypted_buff, qd_raw_buffer_list_t *decrypted_buffs);

/**
 * Encrypts a passed in single unencrypted pn_raw_buffer_t and populates the passed in qd_raw_buffer_list_t with the encrypted buffers.
 * Participates in the TLS handshake. Can be called with a zero unencrypted_buff during the initiation of TLS handshake when the router
 * is acting as the TLS client and needs to send out the initial hanshake frames.
 *
 * Writes error messages at INFO level to the logger present in the the passed in tls object.
 *
 * @param tls - The qd_tls_t which contains the tls session information
 * @param encrypted_buff - a pointer to a single unencrypted raw buffer
 * @param encrypted_buffs - A pointer to a list of type qd_raw_buffer_list_t which will contain the encrypted buffs. Make sure this list is initialized and empty.
 *
 *
 * @Returns true if there is no error when trying to encrypt an unencrypted buffer, false otherwise
 */
bool qd_tls_encrypt_outgoing(qd_tls_t *tls, const pn_raw_buffer_t *unencrypted_buff, qd_raw_buffer_list_t *encrypted_buffs);


/**
 * Returns true if the TLS peer supports the passed in protocol or if the peer does not support ALPN, false otherwise.
 * This is usually called after the TLS handshake is successful.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool is_alpn_protocol_match(qd_tls_t *tls, const char *protocol);

/**
 * Returns true if there is a tls error, false otherwise
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool qd_tls_is_error(qd_tls_t *tls);



#endif // __adaptor_tls_h__

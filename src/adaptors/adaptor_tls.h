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
#include "adaptor_buffer.h"
#include "adaptor_common.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/protocol_adaptor.h"

#define QD_TLS_ERROR -1

typedef struct qd_tls_t qd_tls_t;

/**
 * Constructor to create a new qd_tls_t object.
 * @param context - the context object
 * @oaram conn_id - the connection id of the connection that creates the qd_tls_t
 * @param log_source - the log source for TLS related logging.
 */
qd_tls_t *qd_tls(void *context, uint64_t conn_id, qd_log_source_t *log_source);

/**
 * Configure proton pn_tls objects using passed in config.
 * and start the tls session by calling the pn_tls_start function.
 * Searches for the sslProfile in the passed in config and loads the details from that sslProfile.
 * Logs sslProfile configuration failures at INFO level.
 *
 * @param tls - Pointer to the qd_tls_t object which contains the tls session information.
 * @param config   - Pointer to the qd_adaptor_config_t object which contains the sslProfile information.
 * @param qd -     - Pointer to the qd_dispatch_t object.
 * @param is_listener - set this flag to true if the tls session is initialized on an adaptor listener, false otherwise.
 * @param alpn_protocols - An array of protocols supported by the application layer used when performing
 * Application-Layer Protocol Negotiation (ALPN).
 * @param alpn_protocol_count - The count of elements in the alpn_protocols array.
 * @param on_secure - optional callback invoked when TLS session becomes secure
 *
 * @return
 *   On success:
 *     pn_tls_config and pn_tls_session objects in tls are set up.
 *     Returns true
 *   On failure:
 *     Error log is written
 *     All in-progress pn_tls objects are destroyed.
 *     Returns false.
 */
typedef void qd_tls_on_secure_cb_t(qd_tls_t *tls, void *user_context);
bool         qd_tls_start(qd_tls_t                  *tls,
                          const qd_adaptor_config_t *config,
                          const qd_dispatch_t       *qd,
                          bool                       is_listener,
                          const char                *alpn_protocols[],
                          size_t                     alpn_protocol_count,
                          qd_tls_on_secure_cb_t     *on_secure);

/**
 * Takes as many read buffers from the raw connection that is allowed by pn_tls_get_decrypt_input_buffer_capacity and
 * decrypts those buffers and sticks the decrypted buffers in the passed in decrypted_buffs list.
 *
 * @param tls - The qd_tls_t object which contains the proton tls session information
 * @param pn_raw_conn - pointer to the proton raw connection from which were going to take buffers from
 * @param decrypted_buffs - A pointer to qd_adaptor_buffer_list which will contain the decrypted buffs. Make sure this
 * list is initialized and empty.
 *
 * @return QD_TLS_ERROR if there is an error when trying to decrypt an encrypted buffer, otherwise return the number of
 * undecrypted bytes processed.
 *
 */
int qd_tls_decrypt(qd_tls_t *tls, pn_raw_connection_t *pn_raw_conn, qd_adaptor_buffer_list_t *decrypted_buffs);

/**
 * Encrypts a passed in single unencrypted qd_adaptor_buffer_t and populates the passed in qd_adaptor_buffer_list_t with
 * the encrypted buffers. Can be called with a zero unencrypted_buff during the initiation of TLS handshake when the
 * router is acting as the TLS client and needs to send out the initial hanshake frames.
 * Also may need to be called at any time with zero buffers if required by the TLS protocol, not just during handshake, i.e.
 * any time we need to process input from peer, there is a chance that extra protocol bytes get generated regardless of the presence of application output.
 *
 * @param tls - The qd_tls_t which contains the tls session information
 * @param encrypted_buff - a pointer to a single unencrypted qd_adaptor buffer
 * @param encrypted_buffs - A list of type qd_adaptor_buffer_list_t
 *
 * @return QD_TLS_ERROR if there is an error when trying to encrypt an decrypted buffer, otherwise return the total
 * number of encrypted bytes processed.
 */
int qd_tls_encrypt(qd_tls_t *tls, qd_adaptor_buffer_t *unencrypted_buff, qd_adaptor_buffer_list_t *encrypted_buffs);

/**
 * Returns the value of tls->tls_has_output flag.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 *
 * @return - true if the proton tls engine has output it wants to send to the other side, false otherwise.
 */
bool qd_tls_has_output(const qd_tls_t *tls);

/**
 * Returns true if the TLS handshake has successfully completed and we are ready to send and receive data.
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool qd_tls_is_secure(const qd_tls_t *tls);

/**
 * Returns true if there is a tls error, false otherwise
 *
 * @param tls - The qd_tls_t object which contains the tls session information
 */
bool qd_tls_is_error(const qd_tls_t *tls);

/**
 * Sets some fields in the passed connection_info object.
 * The details set on the connection_info object appear in the output of skstat -c command.
 */
void qd_tls_update_connection_info(qd_tls_t *tls, qdr_connection_info_t *conn_info);

/**
 * Returns the proton tls session object.
 */
pn_tls_t *qd_tls_get_pn_tls_session(qd_tls_t *tls);

/**
 * Cleans up any pending buffers held by the proton tls session and stops/frees the proton tls session and the config.
 * Finally, frees the qd_tls_t object
 */
void qd_tls_free(qd_tls_t *tls);

#endif  // __adaptor_tls_h__

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
#define QD_TLS_EOM   -2  // see qd_tls_take_output_data_cb_t()

typedef struct qd_tls_domain_t qd_tls_domain_t;
typedef struct qd_tls_t qd_tls_t;

/**
 * Constructor to create a new qd_tls_domain_t instance.
 *
 * Creates a new qd_tls_domain instance that can be used to allocate one or more qd_tls_t TLS sessions.  Searches for
 * the sslProfile in the passed in config and loads the details from that sslProfile.  Logs sslProfile configuration
 * failures to log_source at ERROR level. The returned qd_tls_domain_t instance may be used to create multiple qd_tls_t
 * instances.  The caller must call qd_tls_domain_decref() on the returned qd_tls_domain_t instance to free it.
 *
 * @param config - Pointer to the qd_adaptor_config_t object which contains the sslProfile information.
 * @param qd - Pointer to the qd_dispatch_t object.
 * @param log_source - the log source for logging. Adopted by all child qd_tls_t instances.
 * @param alpn_protocols - An array of protocols supported by the application layer used when performing
 * Application-Layer Protocol Negotiation (ALPN).
 * @param alpn_protocol_count - The count of elements in the alpn_protocols array.
 * @param is_listener - set this flag to true if the domain will operate as a client (ex. TcpListener), false otherwise.
 *
 * @return qd_tls_domain_t instance on success, else 0.
 */
qd_tls_domain_t *qd_tls_domain(const qd_adaptor_config_t *config,
                               const qd_dispatch_t       *qd,
                               qd_log_source_t           *log_source,
                               const char                *alpn_protocols[],
                               size_t                     alpn_protocol_count,
                               bool                       is_listener);

/**
 * Drop the reference to the qd_tls_domain_t instance. This can free the instance so it must not be referenced after
 * this call is made.
 *
 * @param - Pointer to the qd_tls_domain_t instance to release.
 */
void qd_tls_domain_decref(qd_tls_domain_t *tls_domain);

/**
 * Constructor to create a new TLS session using the given domain. The returned qd_tls_t instance can be freed by
 * calling qd_tls_free().
 *
 * @param tls_domain - the qd_tls_domain_t to use for this TLS session.
 * @param context - the user context object
 * @param conn_id - the connection id of the connection that creates the qd_tls_t
 * @param on_secure - optional callback invoked when TLS session becomes secure
 *
 * @returns a new qd_tls_t instance, or 0 on error.
 */
typedef void qd_tls_on_secure_cb_t(qd_tls_t *tls, void *context);
qd_tls_t    *qd_tls(qd_tls_domain_t *tls_domain, void *context, uint64_t conn_id, qd_tls_on_secure_cb_t *on_secure);

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

////////////////////////////////////////////////////////////////////////
// Experimental: alternative API for TLS I/O
////////////////////////////////////////////////////////////////////////

/**
 * Retrieve octet counters for encrypted I/O. It is expected that the adaptor maintains counters for the cleartext data
 * itself.
 */
uint64_t qd_tls_get_encrypted_output_octet_count(const qd_tls_t *tls);  // outbound to network
uint64_t qd_tls_get_encrypted_input_octet_count(const qd_tls_t *tls);   // inbound from network

/**
 * Callback into the adaptor to retrieve a list of cleartext application data that will be encrypted and sent. This
 * callback is invoked during the qd_tls_do_io() function.
 *
 * @param context - provided by the adaptor to the qd_tls_do_io() call
 * @param blist - a buffer list for the outgoing application data. The adaptor can append up to limit buffers containing
 * outgoing application data. Ownership of these buffers is given to the TLS layer and will be freed at some point: the
 * adaptor must not refer to these buffers on return from this call!
 * @param limit - at most limit buffers can be appended during this call. Appending less than limit is acceptable,
 * appending more is an error!
 *
 * @return - the total number of data octets added to the buffer list or QD_TLS_EOM to indicate that no more application
 * data will be provided. Returning QD_TLS_EOM will cause the TLS layer to append the TLS closure record to the outbound
 * encrypted data - this confirms a clean close to the peer. When QD_TLS_EOM is returned the blist MUST be empty.
 */
typedef int64_t qd_tls_take_output_data_cb_t(void *context, qd_adaptor_buffer_list_t *blist, size_t limit);

/**
 * TLS I/O work loop.
 *
 * This API will perform TLS I/O operations in both directions. Outgoing cleartext data will be fetched from the adaptor
 * as needed via the get_output_cb() callback. The cleartext data will be encrypted and written to the raw connection
 * (write buffers). On return any incoming decrypted (cleartext) data will be appended to the input_data list. Ownership
 * of the input_data buffers is transferred to the caller: the adaptor must release them when no longer needed.
 *
 * @param raw_conn - the raw connection for reading/writing encrypted buffers
 * @param take_output_cb - see qd_tls_take_output_data_ct_t
 * @param take_output_context - passed back to take_output_cb()
 * @param input_data - incoming decrypted data is appended to this list.
 * @param input_data_count - (output) total number of cleartext octets added to input_data
 *
 * @return - zero on success, otherwise an error code. Errors are unrecoverable.
 */
int qd_tls_do_io(qd_tls_t                     *tls,
                 pn_raw_connection_t          *raw_conn,
                 qd_tls_take_output_data_cb_t *take_output_cb,
                 void                         *take_output_context,
                 qd_adaptor_buffer_list_t     *input_data,
                 uint64_t                     *input_data_count);

#endif  // __adaptor_tls_h__

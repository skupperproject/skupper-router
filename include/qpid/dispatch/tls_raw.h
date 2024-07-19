#ifndef __tls_raw_h__
#define __tls_raw_h__ 1
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

#include "qpid/dispatch/tls.h"
#include "qpid/dispatch/buffer.h"

typedef struct pn_raw_connection_t pn_raw_connection_t;

/**
 * API for TLS operations specific to Proton Raw connections.
 *
 * Note well: these APIs apply only to TLS configuration/sessions of type QD_TLS_TYPE_PROTON_RAW ONLY!  AMQP-based TLS
 * encryption/decryption is done internally by Proton - these APIs are not necessary for AMQP TLS and should not be used
 * with AMQP connections!  See tls.h and tls_amqp.h.
 */


/**
 * Create a new TLS session
 *
 * @param config the TLS configuration used to create the session
 * @param peer_hostname the expected name of the peer host (for verification)
 * @param alpn_protocols optional ALPN protocols for negotiation
 * @param alpn_protocol_count length of alpn_protocols array (must be zero if no ALPN)
 * @param context passed to the on_secure callback
 * @param on_secure optional callback that is invoked if/when the TLS handshake succeeds
 * @return the new TLS session or 0 on error. If error qd_error() is set.
 */
typedef void qd_tls_session_on_secure_cb_t(qd_tls_session_t *session, void *context);
qd_tls_session_t *qd_tls_session_raw(qd_tls_config_t *config,
                                     const char *peer_hostname,
                                     const char **alpn_protocols, size_t alpn_protocol_count,
                                     void *context, qd_tls_session_on_secure_cb_t *on_secure);

/**
 * Get the negotiated ALPN value from the session.
 *
 * @param session the TLS session to query.
 * @return null terminated string containing the negotiated ALPN value. Must be free()d by caller. Return 0 if no ALPN
 * (yet) negotiated.
 */
char *qd_tls_session_get_alpn_protocol(const qd_tls_session_t *session);

/**
 * Fetch output (cleartext) buffers from the application for encryption and transmission.
 *
 * This callback is supplied by the application when calling the qd_tls_session_do_io() work loop. The work loop will
 * call this callback to get output data buffers from the application. The work loop will encrypt these buffers via TLS
 * and send them out the raw connection.
 *
 * @param context - application supplied context (see qd_tls_session_do_io())
 * @param blist - buffer list for output buffers. The application should append output buffers to the end of the list in
 *                FIFO order of transmission.
 * @param limit - limit the number of buffers that can be appended to the list during the call. The application may
 *                append up to limit buffers but not more.
 * @return - the total number of octets worth of data appended to blist. Zero if there is no output available at the
 *           time of the call. QD_IO_EOS to force the work loop to close the output side of the stream. No buffers
 *           should be appended to blist when QD_IO_EOS is returned. Once QD_IO_EOS is returned no further output
 *           buffers will be taken/sent.
 */
#define QD_IO_EOS (-1)
typedef int64_t qd_tls_take_output_buffers_cb_t(void *context, qd_buffer_list_t *blist, size_t limit);


/**
 * TLS I/O work loop.
 *
 * This API will perform TLS data encryption and decryption between a raw connection and and application. Outgoing
 * application cleartext data will be fetched from the application as needed via the take_output_cb() callback. The
 * cleartext data will be encrypted and written to the raw connection (write buffers). On return any incoming decrypted
 * (cleartext) data will be appended to the input_data list. Ownership of the input_data buffers is transferred to the
 * caller: the application must release them when no longer needed.
 *
 * This function will close the raw_conn connection when TLS cleanly closes or if a TLS error occurs.
 *
 * @param session - the TLS session context
 * @param raw_conn - the raw connection for reading/writing encrypted buffers.
 * @param take_output_cb - invoked by the I/O loop to get outgoing cleartext application data
 * @param take_output_context - passed back to take_output_cb()
 * @param input_data - incoming decrypted data is appended to this list.
 * @param input_data_count - (output) total number of cleartext octets added to input_data
 * @param log_module - log module for logging output
 * @param conn_id - connection id for logging
 *
 * @return 0 if I/O in progress, QD_TLS_DONE if the TLS session has closed, or fatal error if < 0
 */
#define QD_TLS_DONE 1
int qd_tls_session_do_io(qd_tls_session_t                *session,
                          pn_raw_connection_t              *raw_conn,
                          qd_tls_take_output_buffers_cb_t *take_output_cb,
                          void                             *take_output_context,
                          qd_buffer_list_t                 *input_data,
                          uint64_t                         *input_data_count,
                          qd_log_module_t                   log_module,
                          uint64_t                          conn_id);

/* True if the given session has failed
 */
bool qd_tls_session_is_error(const qd_tls_session_t *session);

/* True after the TLS handshake has completed successfully
 */
bool qd_tls_session_is_secure(const qd_tls_session_t *session);

/**
 * True if all input (decrypt) data has been received and the receive side of the raw connection has closed. No further
 * input data is available from this TLS session.
 *
 * Sets close_notify to true if a proper close_notify was received from the peer. A missing close_notify is allowed in
 * HTTP/1.x if the received message has an explicit length and the framing is valid (see RFC9112, section TLS Connection
 * Closure)
 */
bool qd_tls_session_is_input_drained(const qd_tls_session_t *session, bool *close_notify);

/**
 * True if the output (encrypt) side of the TLS session is closed and all pending output has been written to the raw
 * connection (including close_notify).
 */
bool qd_tls_session_is_output_flushed(const qd_tls_session_t *session);

/**
 * Retrieve octet counters for encrypted I/O. It is expected that the application maintains counters for the cleartext data
 * itself.
 */
uint64_t qd_tls_session_encrypted_output_octet_count(const qd_tls_session_t *session);  // outbound to network
uint64_t qd_tls_session_encrypted_input_octet_count(const qd_tls_session_t *session);   // inbound from network

#endif


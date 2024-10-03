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
#include "private.h"

#include "qpid/dispatch/error.h"

#include <proton/tls.h>

#include <inttypes.h>


/*
 * API for working with Proton Raw Connections running TLS
 */


pn_tls_config_t *tls_private_allocate_raw_config(const char *ssl_profile_name, const qd_ssl2_profile_t *profile,
                                                 bool is_listener, bool verify_hostname, bool authenticate_peer)
{
    pn_tls_config_t *tls_config = 0;

    qd_error_clear();

    do {
        int res = -1;  // assume failure
        tls_config = pn_tls_config(is_listener ? PN_TLS_MODE_SERVER : PN_TLS_MODE_CLIENT);
        if (!tls_config) {
            qd_error(QD_ERROR_CONFIG, "Failed to create TLS configuration using sslProfile '%s'", ssl_profile_name);
            break;
        }

        if (profile->trusted_certificate_db) {
            res = pn_tls_config_set_trusted_certs(tls_config, profile->trusted_certificate_db);
            if (res != 0) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS caCertFile %s for sslProfile '%s': (%d)",
                         profile->trusted_certificate_db, ssl_profile_name, res);
                break;
            }
        }

        //
        // Configure my self-identifying cert:
        //

        if (profile->certificate_file) {
            res = pn_tls_config_set_credentials(tls_config,
                                                profile->certificate_file,
                                                profile->private_key_file,
                                                profile->password);
            if (res != 0) {
                qd_error(QD_ERROR_CONFIG,
                         "Failed to set TLS certFile '%s' for sslProfile '%s': (%d)",
                         profile->certificate_file, ssl_profile_name, res);
                break;
            }
        }

        if (is_listener) {
            if (authenticate_peer) {
                assert(profile->trusted_certificate_db);
                res = pn_tls_config_set_peer_authentication(tls_config, PN_TLS_VERIFY_PEER, profile->trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(tls_config, PN_TLS_ANONYMOUS_PEER, 0);
            }
            if (res != 0) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS peer authentication for sslProfile '%s'", ssl_profile_name);
                break;
            }
        } else {  // Connector
            if (verify_hostname) {
                res = pn_tls_config_set_peer_authentication(tls_config, PN_TLS_VERIFY_PEER_NAME, profile->trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(tls_config, PN_TLS_VERIFY_PEER, profile->trusted_certificate_db);
            }
            if (res != 0) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS peer host name verification for sslProfile '%s'", ssl_profile_name);
                break;
            }
        }

        // Note: Proton Raw TLS does not support setting the protocol version!

        if (!!profile->ciphers) {
            res = pn_tls_config_set_impl_ciphers(tls_config, profile->ciphers);
            if (res != 0) {
                qd_error(QD_ERROR_CONFIG,
                         "Failed to configure TLS Ciphers '%s' for sslProfile '%s'. Use openssl ciphers -v <ciphers> to validate",
                         profile->ciphers, ssl_profile_name);
                break;
            }
        }

        return tls_config;

    } while (0);

    // If we get here, the configuration setup failed

    if (tls_config) {
        pn_tls_config_free(tls_config);
    }
    return 0;
}




// initialize a proton buffer descriptor to hold the given qd_buffer_t
//
static inline void _pn_buf_desc_give_buffer(pn_raw_buffer_t *pn_desc, const qd_buffer_t *buffer)
{
    pn_desc->size     = qd_buffer_size(buffer);
    pn_desc->capacity = qd_buffer_capacity(buffer);
    pn_desc->offset   = 0;
    pn_desc->bytes    = (char *) qd_buffer_base(buffer);
    pn_desc->context  = (uintptr_t) buffer;
}


// take a used buffer from the given proton buffer descriptor
//
static inline qd_buffer_t *_pn_buf_desc_take_buffer(pn_raw_buffer_t *pn_desc)
{
    qd_buffer_t *buffer = (qd_buffer_t *) pn_desc->context;
    assert(buffer);
    buffer->size = pn_desc->size;
    assert(buffer->size <= QD_BUFFER_SIZE);
    return buffer;
}


int qd_tls_session_do_io(qd_tls_session_t                *session,
                          pn_raw_connection_t              *raw_conn,
                          qd_tls_take_output_buffers_cb_t *take_output_cb,
                          void                             *take_output_context,
                          qd_buffer_list_t                 *input_data,
                          uint64_t                         *input_data_count,
                          qd_log_module_t                   log_module,
                          uint64_t                          conn_id)
{
    assert(session);
    assert(raw_conn);

    CHECK_PROACTOR_RAW_CONNECTION(raw_conn);

    bool work;
    pn_raw_buffer_t pn_buf_desc;
    const bool debug = qd_log_enabled(log_module, QD_LOG_DEBUG);

    if (input_data_count)
        *input_data_count = 0;

    if (session->tls_error)
        return -1;
    if (session->input_drained && session->output_flushed)
        return QD_TLS_DONE;

    do {
        size_t          capacity;
        size_t          taken;
        size_t          given;
        uint64_t        total_octets;

        // Loop until no more work can be done. "work" is considered true whenever the TLS layer produces output or
        // opens up capacity for more input

        work = false;

        //
        // give empty buffers for holding encrypted and decrypted output from the TLS layer
        //

        capacity = pn_tls_get_encrypt_output_buffer_capacity(session->pn_raw);
        if (capacity > 0) {
            if (debug) {
                qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] giving %zu encrypt output buffers", conn_id, capacity);
            }
            while (capacity > 0) {
                _pn_buf_desc_give_buffer(&pn_buf_desc, qd_buffer());
                given = pn_tls_give_encrypt_output_buffers(session->pn_raw, &pn_buf_desc, 1);
                (void) given;
                assert(given == 1);
                capacity -= 1;
            }
        }
        capacity = pn_tls_get_decrypt_output_buffer_capacity(session->pn_raw);
        if (capacity > 0) {
            if (debug) {
                qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] giving %zu decrypt output buffers", conn_id, capacity);
            }
            while (capacity > 0) {
                _pn_buf_desc_give_buffer(&pn_buf_desc, qd_buffer());
                given = pn_tls_give_decrypt_output_buffers(session->pn_raw, &pn_buf_desc, 1);
                (void) given;
                assert(given == 1);
                capacity -= 1;
            }
        }

        //
        // discard written output buffers to make room for new output
        //

        while (pn_raw_connection_take_written_buffers(raw_conn, &pn_buf_desc, 1) == 1) {
            qd_buffer_t *buf = (qd_buffer_t *) pn_buf_desc.context;
            qd_buffer_free(buf);
        }

        //
        // Push any unencrypted output data from the protocol adaptor into the TLS layer for encryption.
        //

        if (pn_tls_is_secure(session->pn_raw)) {
            if (take_output_cb) {
                capacity = pn_tls_get_encrypt_input_buffer_capacity(session->pn_raw);
                if (capacity > 0) {
                    qd_buffer_list_t ubufs = DEQ_EMPTY;
                    if (debug) {
                        qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] encrypt input capacity = %zu bufs", conn_id, capacity);
                    }
                    int64_t out_octets = take_output_cb(take_output_context, &ubufs, capacity);
                    if (out_octets > 0) {
                        assert(!DEQ_IS_EMPTY(ubufs) && DEQ_SIZE(ubufs) <= capacity);
                        qd_log(log_module, QD_LOG_DEBUG,
                               "[C%" PRIu64 "] %" PRIi64 " unencrypted bytes taken by TLS for encryption (%zu buffers)",
                               conn_id, out_octets, DEQ_SIZE(ubufs));
                        qd_buffer_t *abuf = DEQ_HEAD(ubufs);
                        while (abuf) {
                            DEQ_REMOVE_HEAD(ubufs);
                            _pn_buf_desc_give_buffer(&pn_buf_desc, abuf);
                            given = pn_tls_give_encrypt_input_buffers(session->pn_raw, &pn_buf_desc, 1);
                            assert(given == 1);
                            abuf = DEQ_HEAD(ubufs);
                        }
                    } else if (out_octets == 0) {
                        // currently no output, try again later
                        assert(DEQ_IS_EMPTY(ubufs));
                    } else if (session->output_eos == false) {
                        // no further output, flush and close the connection
                        assert(DEQ_IS_EMPTY(ubufs));
                        if (out_octets == QD_IO_EOS) {
                            // clean end-of-output, append TLS close notify:
                            pn_tls_close_output(session->pn_raw);
                        }
                        session->output_eos = true;
                        qd_log(log_module, QD_LOG_DEBUG, "[C%" PRIu64 "] outgoing stream EOS signalled: closing TLS output",
                               conn_id);
                    }
                }
            } else if (debug) {
                // take_output_cb is not set
                qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] output data blocked", conn_id);
            }
        }

        //
        // Push incoming encrypted data from raw conn into TLS. *Important*: once the TLS handshake is completed do not
        // take raw connection incoming buffers if the calling thread is blocked from taking the resulting decrypted
        // incoming buffers (input_data == 0). Why? Because if we happen to take the last read buffer from the raw
        // connection then proactor will generate a PN_RAW_CONNECTION_DISCONNECTED event. Once this happens we can no
        // longer call pn_raw_connection_wake() on it. This means we can no longer activate this thread when it is
        // eventually unblocked. This results in unencrypted incoming data being stuck in the TLS session and eventually
        // discarded when the session is deallocated (data truncation).
        //

        capacity = pn_tls_get_decrypt_input_buffer_capacity(session->pn_raw);
        if (capacity > 0) {
            if (debug) {
                qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] decrypt input capacity = %zu bufs", conn_id, capacity);
            }
            if (!pn_tls_is_secure(session->pn_raw) || input_data != 0) {
                size_t pushed = 0;
                total_octets  = 0;
                while (pushed < capacity) {
                    size_t took = pn_raw_connection_take_read_buffers(raw_conn, &pn_buf_desc, 1);
                    if (took != 1) {
                        // No more read buffers available. Now it is safe to check if the raw connection has closed
                        session->raw_read_drained = pn_raw_connection_is_read_closed(raw_conn);
                        break;
                    } else if (pn_buf_desc.size) {
                        total_octets += pn_buf_desc.size;
                        given = pn_tls_give_decrypt_input_buffers(session->pn_raw, &pn_buf_desc, 1);
                        assert(given == 1);
                        ++pushed;
                    } else {
                        qd_buffer_free((qd_buffer_t *) pn_buf_desc.context);
                    }
                }
                if (pushed > 0) {
                    session->encrypted_input_bytes += total_octets;
                    qd_log(log_module, QD_LOG_DEBUG,
                           "[C%" PRIu64 "] %" PRIu64
                           " encrypted bytes read from the raw connection passed to TLS for decryption (%zu buffers)",
                           conn_id, total_octets, pushed);
                }
            }
        }

        // run the tls state machine (not considered "work").
        //
        // If there is an error the TLS layer may have generated new encrypted output that contains the details about
        // the failure that needs to be sent to the remote. That is why this code does not immediately cease processing
        // if pn_tls_process returns an error: there may be more outgoing buffers that have to be written to the raw
        // connection before it can be closed. This code assumes that the proton TLS library will stop giving capacity
        // for other work once the error has occurred so it is safe to continue running this work loop.

        if (!session->tls_error) {
            const bool check_if_secure = session->on_secure_cb && !pn_tls_is_secure(session->pn_raw);
            int        err             = pn_tls_process(session->pn_raw);
            if (err) {
                session->tls_error = true;
                qd_log(log_module, QD_LOG_DEBUG, "[C%" PRIu64 "] pn_tls_process failed: error=%d", conn_id, err);
            } else if (check_if_secure && pn_tls_is_secure(session->pn_raw)) {
                session->on_secure_cb(session, session->user_context);
                session->on_secure_cb = 0;  // one shot
            }
        }

        //
        // Take encrypted TLS output and write it to the raw connection
        //

        capacity = pn_raw_connection_write_buffers_capacity(raw_conn);
        if (capacity > 0) {
            size_t pushed = 0;
            total_octets  = 0;

            if (debug) {
                qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] raw write capacity=%zu bufs", conn_id, capacity);
            }
            while (pushed < capacity && pn_tls_take_encrypt_output_buffers(session->pn_raw, &pn_buf_desc, 1) == 1) {
                total_octets += pn_buf_desc.size;
                given = pn_raw_connection_write_buffers(raw_conn, &pn_buf_desc, 1);
                assert(given == 1);
                ++pushed;
            }
            if (pushed > 0) {
                work = true;
                session->encrypted_output_bytes += total_octets;
                qd_log(log_module, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] %" PRIu64 " encrypted bytes written to the raw connection by TLS (%zu buffers)",
                       conn_id, total_octets, pushed);
            }
        } else if (pn_raw_connection_is_write_closed(raw_conn)) {
            // drain the TLS buffers - there is no place to send them!
            taken = 0;
            while (pn_tls_take_encrypt_output_buffers(session->pn_raw, &pn_buf_desc, 1) == 1) {
                assert(pn_buf_desc.context);
                qd_buffer_free(_pn_buf_desc_take_buffer(&pn_buf_desc));
                taken += 1;
            }
            if (taken) {
                work = true;
                qd_log(log_module, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] discarded %zu outgoing encrypted buffers due to raw conn write closed",
                       conn_id, taken);
            }
        }

        //
        // take decrypted output and give it to the adaptor
        //

        if (input_data) {
            assert(input_data_count);
            total_octets = 0;
            taken        = 0;
            while (pn_tls_take_decrypt_output_buffers(session->pn_raw, &pn_buf_desc, 1) == 1) {
                qd_buffer_t *abuf = _pn_buf_desc_take_buffer(&pn_buf_desc);
                if (qd_buffer_size(abuf)) {
                    total_octets += qd_buffer_size(abuf);
                    DEQ_INSERT_TAIL(*input_data, abuf);
                    ++taken;
                } else {
                    qd_buffer_free(abuf);
                }
            }
            if (taken) {
                work = true;  // more capacity for decrypted output
                *input_data_count += total_octets;
                qd_log(log_module, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] %" PRIu64 " decrypted bytes taken from TLS for adaptor input (%zu buffers)",
                       conn_id, total_octets, taken);
            }
        } else if (debug) {
            qd_log_impl(log_module, QD_LOG_DEBUG, __FILE__, __LINE__, "[C%" PRIu64 "] input_data blocked", conn_id);
        }

        //
        // Release all used TLS input buffers - they are no longer needed
        //

        taken = 0;
        while (pn_tls_take_encrypt_input_buffers(session->pn_raw, &pn_buf_desc, 1) == 1) {
            qd_buffer_t *abuf = (qd_buffer_t *) pn_buf_desc.context;
            assert(abuf);
            qd_buffer_free(abuf);
            ++taken;
        }
        while (pn_tls_take_decrypt_input_buffers(session->pn_raw, &pn_buf_desc, 1) == 1) {
            qd_buffer_t *abuf = (qd_buffer_t *) pn_buf_desc.context;
            assert(abuf);
            qd_buffer_free(abuf);
            ++taken;
        }
        if (taken)
            work = true;  // more capacity for encrypt/decrypt input buffers

    } while (work);

    // Cannot return an error until all pending outgoing encrypted data have been written to the raw conn so error info
    // can be sent to peer. See comment above.
    //
    if (session->tls_error && !pn_tls_is_decrypt_output_pending(session->pn_raw)) {
        char buf[1024] = "Unknown protocol error";  // default error msg
        int  err = pn_tls_get_session_error(session->pn_raw);
        if (err) {
            int rc = snprintf(buf, sizeof(buf), "Code: %d ", err);
            if (rc > 0 && rc < sizeof(buf)) {
                pn_tls_get_session_error_string(session->pn_raw, &buf[rc], sizeof(buf) - rc);
            }
        }
        pn_condition_t *cond = pn_raw_connection_condition(raw_conn);
        if (!!cond && !pn_condition_is_set(cond)) {
            (void) pn_condition_set_name(cond, "TLS-connection-failed");
            (void) pn_condition_set_description(cond, buf);
        }
        pn_raw_connection_close(raw_conn);
        qd_log(log_module, QD_LOG_WARNING, "[C%" PRIu64 "] TLS connection failed: %s", conn_id, buf);
        return -1;
    }

    // check if the output (encrypt) side is done
    //
    if (session->output_eos && !pn_tls_is_encrypt_output_pending(session->pn_raw)) {
        // We closed the encrypt side of the TLS connection and we've sent all output
        session->output_flushed = true;
        if (!pn_raw_connection_is_write_closed(raw_conn)) {
            qd_log(log_module, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] TLS output closed - closing write side of raw connection", conn_id);
            pn_raw_connection_write_close(raw_conn);
        }
    }

    // check for end of input (decrypt done)
    //
    if (pn_tls_is_input_closed(session->pn_raw)) {
        // TLS clean close signalled by remote. Do not read any more data (prevent truncation attack).
        //
        session->input_drained = true;
        if (!pn_raw_connection_is_read_closed(raw_conn)) {
            qd_log(log_module, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] TLS input closed - closing read side of raw connection", conn_id);
            pn_raw_connection_read_close(raw_conn);
        }
        while (pn_raw_connection_take_read_buffers(raw_conn, &pn_buf_desc, 1) == 1) {
            qd_buffer_free(_pn_buf_desc_take_buffer(&pn_buf_desc));
        }
    } else if (session->raw_read_drained && !pn_tls_is_decrypt_output_pending(session->pn_raw)) {
        // Unclean close: remote simply dropped the connection. Done reading remaining decrypted output.
        //
        session->input_drained = true;
        qd_log(log_module, QD_LOG_DEBUG, "[C%" PRIu64 "] TLS input closed", conn_id);
    }

    return session->input_drained && session->output_flushed ? QD_TLS_DONE : 0;
}


bool qd_tls_session_is_error(const qd_tls_session_t *session)
{
    if (!session || !session->pn_raw)
        return false;
    return session->tls_error;
}


bool qd_tls_session_is_secure(const qd_tls_session_t *session)
{
    if (!session || !session->pn_raw)
        return false;
    return pn_tls_is_secure(session->pn_raw);
}


bool qd_tls_session_is_input_drained(const qd_tls_session_t *session, bool *close_notify)
{
    assert(session);
    *close_notify = pn_tls_is_input_closed(session->pn_raw);
    return session->input_drained;
}


bool qd_tls_session_is_output_flushed(const qd_tls_session_t *session)
{
    assert(session);
    return session->output_flushed;
}


uint64_t qd_tls_session_encrypted_output_octet_count(const qd_tls_session_t *session)
{
    assert(session);
    return session->encrypted_output_bytes;
}


uint64_t qd_tls_session_encrypted_input_octet_count(const qd_tls_session_t *session)
{
    assert(session);
    return session->encrypted_input_bytes;
}

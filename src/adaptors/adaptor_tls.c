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

#include "adaptor_tls.h"

#include "router_core_private.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"

#define NUM_ALPN_PROTOCOLS     1
#define TLS_MAX_INPUT_CAPACITY 4

struct qd_tls_t {
    pn_tls_t        *tls_session;
    pn_tls_config_t *tls_config;
    void            *user_context;
    qd_log_source_t *log_source;
    uint64_t         conn_id;
    bool             tls_has_output;
    bool             tls_error;
};

ALLOC_DECLARE(qd_tls_t);
ALLOC_DEFINE(qd_tls_t);

qd_tls_t *qd_tls(void *context, uint64_t conn_id, qd_log_source_t *log_source)
{
    qd_tls_t *tls = new_qd_tls_t();
    ZERO(tls);
    tls->user_context = context;
    tls->conn_id      = conn_id;
    tls->log_source   = log_source;
    return tls;
}

bool qd_tls_start(qd_tls_t                  *tls,
                  const qd_adaptor_config_t *config,
                  const qd_dispatch_t       *qd,
                  bool                       is_listener,
                  const char                *alpn_protocols[])
{
    const char *role = is_listener ? "listener" : "connector";
    qd_log(tls->log_source,
           QD_LOG_INFO,
           "[C%" PRIu64 "] %s %s configuring ssl profile %s",
           tls->conn_id,
           role,
           config->name,
           config->ssl_profile_name);

    do {
        // find the ssl profile
        assert(qd);
        qd_connection_manager_t *cm = qd_dispatch_connection_manager(qd);
        assert(cm);
        qd_config_ssl_profile_t *config_ssl_profile = qd_find_ssl_profile(cm, config->ssl_profile_name);
        if (!config_ssl_profile) {
            qd_log(tls->log_source,
                   QD_LOG_ERROR,
                   "[C%" PRIu64 "] %s %s unable to find ssl profile %s",
                   tls->conn_id,
                   role,
                   config->name,
                   config->ssl_profile_name);
            break;
        }

        int res;
        // First free, then create pn domain
        if (tls->tls_config)
            pn_tls_config_free(tls->tls_config);
        tls->tls_config = pn_tls_config(is_listener ? PN_TLS_MODE_SERVER : PN_TLS_MODE_CLIENT);

        if (!tls->tls_config) {
            qd_log(tls->log_source,
                   QD_LOG_ERROR,
                   "[C%" PRIu64 "] %s %s unable to create tls domain for ssl profile %s",
                   tls->conn_id,
                   role,
                   config->name,
                   config->ssl_profile_name);
            break;
        }

        if (config_ssl_profile->ssl_trusted_certificate_db) {
            res = pn_tls_config_set_trusted_certs(tls->tls_config, config_ssl_profile->ssl_trusted_certificate_db);
            if (res != 0) {
                qd_log(tls->log_source,
                       QD_LOG_ERROR,
                       "[C%" PRIu64 "] %s %s unable to set tls trusted certificates (%d)",
                       tls->conn_id,
                       role,
                       config->name,
                       res);
                break;
            }
        }

        // Call pn_tls_config_set_credentials only if "certFile" is provided.
        if (config_ssl_profile->ssl_certificate_file) {
            res = pn_tls_config_set_credentials(tls->tls_config,
                                                config_ssl_profile->ssl_certificate_file,
                                                config_ssl_profile->ssl_private_key_file,
                                                config_ssl_profile->ssl_password);
            if (res != 0) {
                qd_log(tls->log_source,
                       QD_LOG_ERROR,
                       "[C%" PRIu64 "] adaptor %s %s unable to set tls credentials (%d)",
                       tls->conn_id,
                       role,
                       config->name,
                       res);
                break;
            }
        } else {
            qd_log(tls->log_source,
                   QD_LOG_INFO,
                   "[C%" PRIu64 "] sslProfile %s did not provide certFile",
                   tls->conn_id,
                   config->ssl_profile_name);
        }

        if (!!config_ssl_profile->ssl_ciphers) {
            res = pn_tls_config_set_impl_ciphers(tls->tls_config, config_ssl_profile->ssl_ciphers);
            if (res != 0) {
                qd_log(tls->log_source,
                       QD_LOG_ERROR,
                       "[C%" PRIu64 "] %s %s unable to set tls ciphers (%d)",
                       tls->conn_id,
                       role,
                       config->name,
                       res);
                break;
            }
        }

        if (is_listener) {
            if (config->authenticate_peer) {
                res = pn_tls_config_set_peer_authentication(
                    tls->tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(tls->tls_config, PN_TLS_ANONYMOUS_PEER, 0);
            }
        } else {
            // Connector.
            if (config->verify_host_name) {
                res = pn_tls_config_set_peer_authentication(
                    tls->tls_config, PN_TLS_VERIFY_PEER_NAME, config_ssl_profile->ssl_trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(
                    tls->tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }

            tls->tls_has_output =
                true;  // always true when the router is acting as the client initiating TLS handshake.
        }

        if (res != 0) {
            qd_log(tls->log_source,
                   QD_LOG_ERROR,
                   "[C%" PRIu64 "] Unable to set tls peer authentication for sslProfile %s - (%d)",
                   tls->conn_id,
                   config->ssl_profile_name,
                   res);
            break;
        }

        //
        // Provide an ordered list of application protocols for ALPN by calling pn_tls_config_set_alpn_protocols. In our
        // case, h2 is the only supported protocol. A list of protocols can be found here -
        // https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.txt
        //
        if (alpn_protocols)
            pn_tls_config_set_alpn_protocols(tls->tls_config, alpn_protocols, NUM_ALPN_PROTOCOLS);

        // set up tls session
        if (tls->tls_session) {
            pn_tls_free(tls->tls_session);
        }
        tls->tls_session = pn_tls(tls->tls_config);

        if (!tls->tls_session) {
            qd_log(tls->log_source,
                   QD_LOG_ERROR,
                   "[C%" PRIu64 "] Unable to create tls session for sslProfile %s with hostname: '%s'",
                   tls->conn_id,
                   config->ssl_profile_name,
                   config->host);
            break;
        }

        pn_tls_set_peer_hostname(tls->tls_session, config->host);

        int ret = pn_tls_start(tls->tls_session);
        if (ret != 0) {
            break;
        }

        qd_log(tls->log_source,
               QD_LOG_INFO,
               "[C%" PRIu64 "] Successfully configured sslProfile %s",
               tls->conn_id,
               config->ssl_profile_name);
        return true;

    } while (0);

    // Handle tls creation/setup failure by deleting any pn domain or session objects
    if (tls->tls_session) {
        pn_tls_free(tls->tls_session);
        tls->tls_session = 0;
    }

    if (tls->tls_config) {
        pn_tls_config_free(tls->tls_config);
        tls->tls_config = 0;
    }

    return false;
}

void set_qdr_connection_info_details(qd_tls_t *tls, qdr_connection_info_t *conn_info)
{
    const char *protocol_info;
    size_t      protocol_info_length;
    char       *protocol_ver    = 0;
    char       *protocol_cipher = 0;
    //
    // Ask the Proton TLS API for protocol version and protol cipher.
    //
    if (pn_tls_get_protocol_version(tls->tls_session, &protocol_info, &protocol_info_length)) {
        protocol_ver = qd_calloc(protocol_info_length + 1, sizeof(char));
        memmove(protocol_ver, protocol_info, protocol_info_length);
    }
    if (pn_tls_get_cipher(tls->tls_session, &protocol_info, &protocol_info_length)) {
        protocol_cipher = qd_calloc(protocol_info_length + 1, sizeof(char));
        memmove(protocol_cipher, protocol_info, protocol_info_length);
    }

    //
    // Lock using the connection_info_lock before setting the values on the
    // connection_info. This same lock is being used in the agent_connection.c's qdr_connection_insert_column_CT
    //
    sys_mutex_lock(&conn_info->connection_info_lock);
    free(conn_info->ssl_cipher);
    conn_info->ssl_cipher = 0;
    free(conn_info->ssl_proto);
    conn_info->ssl_proto    = 0;
    conn_info->ssl          = true;
    conn_info->is_encrypted = true;
    if (protocol_cipher) {
        conn_info->ssl_cipher = protocol_cipher;
    }
    if (protocol_ver) {
        conn_info->ssl_proto = protocol_ver;
    }

    sys_mutex_unlock(&conn_info->connection_info_lock);
}

void qd_tls_stop(qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return;

    pn_tls_stop(tls->tls_session);
    pn_raw_buffer_t raw_buffer;
    while (pn_tls_take_encrypt_output_buffers(tls->tls_session, &raw_buffer, 1)) {
        qd_adaptor_buffer_t *buf = (qd_adaptor_buffer_t *) raw_buffer.context;
        if (buf) {
            qd_adaptor_buffer_free(buf);
        }
    }
    while (pn_tls_take_encrypt_input_buffers(tls->tls_session, &raw_buffer, 1)) {
        qd_adaptor_buffer_t *buf = (qd_adaptor_buffer_t *) raw_buffer.context;
        if (buf) {
            qd_adaptor_buffer_free(buf);
        }
    }
    while (pn_tls_take_decrypt_output_buffers(tls->tls_session, &raw_buffer, 1)) {
        qd_adaptor_buffer_t *buf = (qd_adaptor_buffer_t *) raw_buffer.context;
        if (buf) {
            qd_adaptor_buffer_free(buf);
        }
    }
    while (pn_tls_take_decrypt_input_buffers(tls->tls_session, &raw_buffer, 1)) {
        qd_adaptor_buffer_t *buf = (qd_adaptor_buffer_t *) raw_buffer.context;
        if (buf) {
            qd_adaptor_buffer_free(buf);
        }
    }
    pn_tls_free(tls->tls_session);
    tls->tls_session = 0;
    pn_tls_config_free(tls->tls_config);
    tls->tls_config = 0;
}

/**
 * Call pn_tls_process and prints out a log message with an error if there is an error processing TLS.
 * @param qd_tls_t object
 * @return True if tls was processed with no errors, false otherwise.
 */
static bool process_tls(qd_tls_t *tls)
{
    int err = pn_tls_process(tls->tls_session);
    if (err && !tls->tls_error) {
        tls->tls_error = true;
        // Stop all application data processing.
        // Close input.  Continue non-application output in case we have a TLS protocol error to send to peer.
        char error_msg[256];
        pn_tls_get_session_error_string(tls->tls_session, error_msg, sizeof(error_msg));
        tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
        qd_log(tls->log_source,
               QD_LOG_ERROR,
               "[C%" PRIu64 "] Error processing TLS: %s, tls_has_output=%i",
               tls->conn_id,
               error_msg,
               tls->tls_has_output);
        return false;
    }
    return true;
}

static void take_back_input_encrypt_buffs(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t          take_input_count;

    while ((take_input_count = pn_tls_take_encrypt_input_buffers(tls->tls_session, &take_incoming_buf, 1))) {
        assert(take_input_count == 1);
        (void) take_input_count;  // prevent unused variable warning
        qd_adaptor_buffer_t *adaptor_buff = (qd_adaptor_buffer_t *) take_incoming_buf.context;
        qd_adaptor_buffer_free(adaptor_buff);
    }
}

/**
 * Takes all decrypt input buffers back from tls by repeatedly calling pn_tls_take_decrypt_input_buffers
 * in a while loop. Frees the underlying adaptor buffer.
 */
static void take_back_input_decrypt_buffs(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t          take_input_count;
    while ((take_input_count = pn_tls_take_decrypt_input_buffers(tls->tls_session, &take_incoming_buf, 1))) {
        assert(take_input_count == 1);
        (void) take_input_count;  // prevent unused variable warning
        qd_adaptor_buffer_t *adaptor_buff = (qd_adaptor_buffer_t *) take_incoming_buf.context;
        qd_adaptor_buffer_free(adaptor_buff);
    }
}

size_t get_pn_raw_buffer_fetch_size(qd_tls_t *tls)
{
    size_t tls_input_buff_capacity = pn_tls_get_decrypt_input_buffer_capacity(tls->tls_session);
    size_t fetch_size              = MIN(tls_input_buff_capacity, TLS_MAX_INPUT_CAPACITY);
    return fetch_size;
}

int qd_tls_decrypt(qd_tls_t                 *tls,
                   pn_raw_connection_t      *pn_raw_conn,
                   qd_adaptor_buffer_list_t *decrypted_buffs,
                   qd_adaptor_buffer_list_t *granted_read_buffs)
{
    if (tls->tls_error)
        return QD_TLS_ERROR;

    int encrypted_bytes_in = 0;

    // How many decrypt input buffers can TLS take ?
    size_t fetch_size = get_pn_raw_buffer_fetch_size(tls);

    // Do not proceed if TLS has no decrypt input capacity.
    if (fetch_size == 0) {
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%" PRIu64 "] qd_tls_decrypt fetch_size=0, returning", tls->conn_id);
        return 0;
    }

    pn_raw_buffer_t raw_buffers[TLS_MAX_INPUT_CAPACITY];
    size_t          n;
    while ((n = pn_raw_connection_take_read_buffers(
                pn_raw_conn,
                raw_buffers,
                fetch_size))) {  // Repeatedly get as much buffers (from raw connection) as possible
        for (size_t i = 0; i < n; ++i) {
            // Give each decrypt input buffer to proton tls
            qd_adaptor_buffer_t *adaptor_buff = (qd_adaptor_buffer_t *) raw_buffers[i].context;
            DEQ_REMOVE(*granted_read_buffs, adaptor_buff);
            if (raw_buffers[i].size > 0) {
                encrypted_bytes_in += raw_buffers[i].size;
                size_t consumed = pn_tls_give_decrypt_input_buffers(tls->tls_session, &raw_buffers[i], 1);
                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64 "] qd_tls_decrypt gave raw buffer to pn_tls_give_decrypt_input_buffers, size=%zu ",
                       tls->conn_id,
                       raw_buffers[i].size);
                (void) consumed;  // prevent unused variable warning
                assert(consumed == 1);
            } else {
                //
                // This buffer size is zero, we will free this buffer immediately.
                //
                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64 "] qd_tls_decrypt raw buffer size=%zu, did not sent this buffer to TLS, freeing it",
                       tls->conn_id,
                       raw_buffers[i].size);
                qd_adaptor_buffer_free(adaptor_buff);
            }
        }

        size_t decrypt_output_buff_capacity = pn_tls_get_decrypt_output_buffer_capacity(tls->tls_session);

        if (decrypt_output_buff_capacity == 0) {
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] qd_tls_decrypt decrypt_output_buff_capacity == 0",
                   tls->conn_id);
        } else {
            //
            // Process TLS.
            //
            if (!process_tls(tls)) {
                take_back_input_decrypt_buffs(tls);
                return QD_TLS_ERROR;
            }

            qd_log(tls->log_source, QD_LOG_TRACE, "[C%" PRIu64 "] qd_tls_decrypt process_tls successful", tls->conn_id);

        give_decrypt_output_buffers:
            while (pn_tls_need_decrypt_output_buffers(tls->tls_session)) {
                //
                // Give one raw buffer at a time to tls which it will be used to decrypt the encrypted data.
                // This loop will give enough buffers needed by tls to decrypt the data.
                //
                pn_raw_buffer_t      decrypted_raw_buffer;
                qd_adaptor_buffer_t *decrypted_adaptor_buf = qd_adaptor_buffer_raw(&decrypted_raw_buffer);
                (void) decrypted_adaptor_buf;

                size_t given_decrypt_output_buffers =
                    pn_tls_give_decrypt_output_buffers(tls->tls_session, &decrypted_raw_buffer, 1);
                (void) given_decrypt_output_buffers;  // prevent unused variable warning

                //
                // Process TLS and log an error if any.
                //
                if (!process_tls(tls)) {
                    take_back_input_decrypt_buffs(tls);
                    return QD_TLS_ERROR;
                }

                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64
                       "] qd_tls_decrypt (pn_tls_need_decrypt_output_buffers) process_tls successful, "
                       "given_decrypt_output_buffers=%zu",
                       tls->conn_id,
                       given_decrypt_output_buffers);
            }

            pn_raw_buffer_t decrypted_output_buff;
            while (pn_tls_take_decrypt_output_buffers(tls->tls_session, &decrypted_output_buff, 1)) {
                qd_adaptor_buffer_t *decrypted_adaptor_buff =
                    qd_get_adaptor_buffer_from_pn_raw_buffer(&decrypted_output_buff);
                DEQ_INSERT_TAIL(*decrypted_buffs, decrypted_adaptor_buff);
                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64
                       "] qd_tls_decrypt pn_tls_take_decrypt_output_buffers, decrypt adaptor buffer size=%zu",
                       tls->conn_id,
                       qd_adaptor_buffer_size(decrypted_adaptor_buff));
            }

            // You can take only a maximum of decrypt_output_buff_capacity. You will have to recheck
            // pn_tls_need_decrypt_output_buffers to get everything back.
            if (pn_tls_need_decrypt_output_buffers(tls->tls_session)) {
                goto give_decrypt_output_buffers;
            }
        }
        take_back_input_decrypt_buffs(tls);
        fetch_size = get_pn_raw_buffer_fetch_size(tls);
    }

    tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
    qd_log(tls->log_source,
           QD_LOG_TRACE,
           "[C%" PRIu64 "] qd_tls_decrypt tls->tls_has_output=%i",
           tls->conn_id,
           qd_tls_has_output(tls));
    take_back_input_decrypt_buffs(tls);
    return encrypted_bytes_in;
}

int qd_tls_encrypt(qd_tls_t *tls, qd_adaptor_buffer_t *unencrypted_buff, qd_adaptor_buffer_list_t *encrypted_buffs)
{
    if (tls->tls_error)
        return QD_TLS_ERROR;

    int unencrypted_bytes_out = 0;

    if (unencrypted_buff) {
        unencrypted_bytes_out += qd_adaptor_buffer_size(unencrypted_buff);
        qd_log(tls->log_source,
               QD_LOG_DEBUG,
               "[C%" PRIu64 "] qd_tls_encrypt unencrypted_buff.size=%zu",
               tls->conn_id,
               qd_adaptor_buffer_size(unencrypted_buff));
    } else {
        qd_log(tls->log_source, QD_LOG_DEBUG, "[C%" PRIu64 "] qd_tls_encrypt no unencrypted buff sent", tls->conn_id);
    }

    if (pn_tls_is_secure(tls->tls_session)) {
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%" PRIu64 "] qd_tls_encrypt tls session is secure", tls->conn_id);

        if (unencrypted_buff) {
            size_t encrypt_input_buff_capacity = pn_tls_get_encrypt_input_buffer_capacity(tls->tls_session);
            (void) encrypt_input_buff_capacity;  // prevent unused variable warning
            assert(encrypt_input_buff_capacity > 0);

            pn_raw_buffer_t pn_raw_buffer;
            qd_adaptor_buffer_pn_raw_buffer(&pn_raw_buffer, unencrypted_buff);
            size_t consumed = pn_tls_give_encrypt_input_buffers(tls->tls_session, &pn_raw_buffer, 1);
            (void) consumed;  // prevent unused variable warning
            assert(consumed == 1);
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] qd_tls_encrypt pn_tls_give_encrypt_input_buffers success",
                   tls->conn_id);

            //
            // Process TLS.
            //
            if (!process_tls(tls)) {
                take_back_input_encrypt_buffs(tls);
                return QD_TLS_ERROR;
            }
        }

        // We are immediately taking back the  encrypt input buffers which means the tls encrypt input buffer capacity
        // is restored.
        take_back_input_encrypt_buffs(tls);
        tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
    }
give_encrypt_output_buffers:
    while (pn_tls_need_encrypt_output_buffers(tls->tls_session)) {
        //
        // We will give just one result buffer
        //
        // This is the raw buffer that will hold the encrypted results
        pn_raw_buffer_t      encrypted_result_raw_buffer;
        qd_adaptor_buffer_t *adaptor_buff = qd_adaptor_buffer_raw(&encrypted_result_raw_buffer);
        (void) adaptor_buff;  // prevent unused variable warning

        //
        // Send the empty raw_buffers to proton tls. Proton tls will put the encrypted data into
        // encrypted_result_raw_buffers
        //
        size_t encrypt_result_buffers_count =
            pn_tls_give_encrypt_output_buffers(tls->tls_session, &encrypted_result_raw_buffer, 1);
        assert(encrypt_result_buffers_count == 1);
        (void) encrypt_result_buffers_count;  // prevent unused variable warning

        qd_log(tls->log_source,
               QD_LOG_TRACE,
               "[C%" PRIu64 "] qd_tls_encrypt pn_tls_give_encrypt_output_buffers success",
               tls->conn_id);

        //
        // Process TLS.
        //
        if (!process_tls(tls)) {
            take_back_input_encrypt_buffs(tls);
            return QD_TLS_ERROR;
        }

        qd_log(tls->log_source, QD_LOG_TRACE, "[C%" PRIu64 "] qd_tls_encrypt process_tls successful", tls->conn_id);
    }

    pn_raw_buffer_t encrypted_raw_output_buff;
    while (pn_tls_take_encrypt_output_buffers(tls->tls_session, &encrypted_raw_output_buff, 1)) {
        qd_adaptor_buffer_t *encrypted_adaptor_buff =
            qd_get_adaptor_buffer_from_pn_raw_buffer(&encrypted_raw_output_buff);
        DEQ_INSERT_TAIL(*encrypted_buffs, encrypted_adaptor_buff);
        qd_log(tls->log_source,
               QD_LOG_TRACE,
               "[C%" PRIu64 "] qd_tls_encrypt pn_tls_take_encrypt_output_buffers, encrypted_raw_output_buff.size=%zu",
               tls->conn_id,
               encrypted_raw_output_buff.size);
    }

    if (pn_tls_need_encrypt_output_buffers(tls->tls_session)) {
        goto give_encrypt_output_buffers;
    }

    return unencrypted_bytes_out;
}

bool qd_tls_is_error(qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return tls->tls_error;
}

bool qd_tls_is_secure(qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return pn_tls_is_secure(tls->tls_session);
}

bool qd_tls_has_output(qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return tls->tls_has_output;
}

pn_tls_t *qd_tls_get_pn_tls_session(qd_tls_t *tls)
{
    return tls->tls_session;
}

void qd_tls_free(qd_tls_t *tls)
{
    free_qd_tls_t(tls);
}

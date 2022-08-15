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
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/connection_manager.h"

ALLOC_DEFINE(qd_tls_t);
ALLOC_DEFINE(qd_raw_buffer_t);
#define NUM_ALPN_PROTOCOLS 1

/**
 * If an ALPN protocol was detected by the TLS API, we make sure that the protocol matches the provided protocol.
 * The connection is simply closed if any other protocol was detected.
 * It is ok for no protocol to be detected which means that the other side might not be doing ALPN. If this is the case,
 * we still continue to perform TLS handshake.
 *
 */
bool is_alpn_protocol_match(qd_tls_t *tls, const char *protocol) {

    const char *protocol_name;
    size_t protocol_name_length;
    if (pn_tls_get_alpn_protocol(tls->tls_session, &protocol_name, &protocol_name_length)) {
        char *alpn_protocol = qd_calloc(protocol_name_length + 1, sizeof(char));
        memmove(alpn_protocol, protocol_name, protocol_name_length);

        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] Using protocol %s obtained via ALPN", tls->conn_id, alpn_protocol);

        if (strcmp(alpn_protocol, protocol)) {
            // The protocol received from ALPN is not the same as the passed in protocol, we will log an error and close this connection.
            qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] Error in ALPN: was expecting protocol %s but got %s", tls->conn_id, protocol, alpn_protocol);
            free(alpn_protocol);
            return false;
        }
        free(alpn_protocol);
    }
    else {
        //
        // No protocol was received via ALPN. This could mean that the other side does not do ALPN and that is ok, do not close the connection.
        //
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] No ALPN protocol was returned, nothing to do, return true", tls->conn_id);
    }
    return true;
}

/**
 * Set the TLS details on the connection_info object. The details in the connection_info object are displayed
 * in the output of the skstat -c command.
 */
static void set_qdr_connection_info_details(qd_tls_t *tls)
{
    assert(tls->connection_info !=0);

    const char *protocol_info;
    size_t protocol_info_length;
    char *protocol_ver = 0;
    char *protocol_cipher = 0;

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

    qdr_connection_info_t *conn_info = tls->connection_info;

    //
    // Lock using the connection_info_lock before setting the values on the
    // connection_info. This same lock is being used in the agent_connection.c's qdr_connection_insert_column_CT
    //
    sys_mutex_lock(&conn_info->connection_info_lock);
    free(conn_info->ssl_cipher);
    conn_info->ssl_cipher = 0;
    free(conn_info->ssl_proto);
    conn_info->ssl_proto = 0;
    conn_info->ssl = true;
    conn_info->is_encrypted = true;
    if (protocol_cipher) {
        conn_info->ssl_cipher = protocol_cipher;
    }
    if (protocol_ver) {
        conn_info->ssl_proto = protocol_ver;
    }
    conn_info->is_authenticated = true;
    sys_mutex_unlock(&conn_info->connection_info_lock);
}

void qd_tls_stop(qd_tls_t *tls)
{
    pn_tls_stop(tls->tls_session);
    pn_tls_free(tls->tls_session);
    tls->tls_session = 0;
    pn_tls_config_free(tls->tls_config);
    tls->tls_config = 0;
}


qd_tls_t *qd_tls(void *context,
                 pn_raw_connection_t *raw_conn,
                 qd_tls_post_handshake_action_t post_handshake_action_incoming,
                 qd_tls_post_handshake_action_t post_handshake_action_outgoing,
                 qd_tls_alpn_handler_t          alpn_handler,
                 uint64_t conn_id,
                 qd_log_source_t *log_source,
                 qdr_connection_info_t *connection_info) {
    qd_tls_t *tls = new_qd_tls_t();
    ZERO(tls);
    tls->user_context = context;
    tls->pn_raw_conn = raw_conn;
    tls->post_handshake_action_incoming = post_handshake_action_incoming;
    tls->post_handshake_action_outgoing = post_handshake_action_outgoing;
    tls->alpn_handler = alpn_handler;
    tls->conn_id = conn_id;
    tls->log_source = log_source;
    tls->connection_info = connection_info;
    return tls;
}

bool qd_tls_start(qd_tls_t                  *tls,
                  const qd_adaptor_config_t *config,
                  const qd_dispatch_t       *qd,
                  bool                       is_listener,
                  const char                *protocols[])
{
    const char *role = is_listener ? "listener" : "connector";
    qd_log(tls->log_source, QD_LOG_INFO, "[C%"PRIu64"] %s %s configuring ssl profile %s", tls->conn_id, role, config->name, config->ssl_profile_name);

    do {
        // find the ssl profile
        assert(qd);
        qd_connection_manager_t *cm = qd_dispatch_connection_manager(qd);
        assert(cm);
        qd_config_ssl_profile_t *config_ssl_profile = qd_find_ssl_profile(cm, config->ssl_profile_name);
        if (!config_ssl_profile) {
            qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to find ssl profile %s", tls->conn_id, config->name, config->ssl_profile_name);
            break;
        }

        int res;
        // First free, then create pn domain
        if (tls->tls_config)
            pn_tls_config_free(tls->tls_config);
        tls->tls_config = pn_tls_config(is_listener ? PN_TLS_MODE_SERVER : PN_TLS_MODE_CLIENT);

        if (! tls->tls_config) {
            qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to create tls domain for ssl profile %s", tls->conn_id, role, config->name, config->ssl_profile_name);
            break;
        }

        if (config_ssl_profile->ssl_trusted_certificate_db) {
            res = pn_tls_config_set_trusted_certs(tls->tls_config, config_ssl_profile->ssl_trusted_certificate_db);
            if (res != 0) {
                qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to set tls trusted certificates (%d)", tls->conn_id, role, config->name, res);
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
                qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] adaptor %s %s unable to set tls credentials (%d)", tls->conn_id, role, config->name, res);
                break;
            }
        }
        else {
            qd_log(tls->log_source, QD_LOG_INFO, "[C%"PRIu64"] sslProfile %s did not provide certFile", tls->conn_id, config->ssl_profile_name);
        }


        if (!!config_ssl_profile->ssl_ciphers) {
            res = pn_tls_config_set_impl_ciphers(tls->tls_config, config_ssl_profile->ssl_ciphers);
            if (res != 0) {
                qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] %s %s unable to set tls ciphers (%d)", tls->conn_id, role, config->name, res);
                break;
            }
        }


        if (is_listener) {
            if (config->authenticate_peer) {
                res = pn_tls_config_set_peer_authentication(tls->tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }
            else {
                res = pn_tls_config_set_peer_authentication(tls->tls_config, PN_TLS_ANONYMOUS_PEER, 0);
            }
        }
        else {
            // Connector.
            if (config->verify_host_name) {
                res = pn_tls_config_set_peer_authentication(tls->tls_config, PN_TLS_VERIFY_PEER_NAME, config_ssl_profile->ssl_trusted_certificate_db);
            }
            else {
                res = pn_tls_config_set_peer_authentication(tls->tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }

            tls->tls_has_output = true; // always true for initial client side TLS.
        }

        if (res != 0) {
            qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] Unable to set tls peer authentication for sslProfile %s - (%d)", tls->conn_id, config->ssl_profile_name, res);
            break;
        }

        //
        // Provide an ordered list of application protocols for ALPN by calling pn_tls_config_set_alpn_protocols. In our case, h2 is the only supported protocol.
        // A list of protocols can be found here - https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.txt
        //
        if (protocols)
            pn_tls_config_set_alpn_protocols(tls->tls_config, protocols, NUM_ALPN_PROTOCOLS);

        // set up tls session
        if (tls->tls_session) {
            pn_tls_free(tls->tls_session);
        }
        tls->tls_session = pn_tls(tls->tls_config);

        if (! tls->tls_session) {
            qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] Unable to create tls session for sslProfile %s with hostname: '%s'", tls->conn_id, config->ssl_profile_name, config->host);
            break;
        }

        pn_tls_set_peer_hostname(tls->tls_session, config->host);

        int ret = pn_tls_start(tls->tls_session);
        if (ret != 0) {
            break;
        }

        qd_log(tls->log_source, QD_LOG_INFO, "[C%"PRIu64"] Successfully configured ssl profile %s", tls->conn_id, config->ssl_profile_name);
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


static void take_back_input_encrypt_buff(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t take_input_count = pn_tls_take_encrypt_input_buffers(tls->tls_session, &take_incoming_buf, 1);
    (void)take_input_count;   // prevent unused variable warning
}

static void take_back_input_decrypt_buff(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t take_input_count = pn_tls_take_decrypt_input_buffers(tls->tls_session, &take_incoming_buf, 1);
    assert(take_input_count == 1);
    (void)take_input_count;   // prevent unused variable warning
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
        qd_log(tls->log_source, QD_LOG_ERROR, "[C%"PRIu64"] Error processing TLS: %s, tls_has_output=%i", tls->conn_id, error_msg, tls->tls_has_output);
        return false;
    }
    return true;
}

bool qd_tls_encrypt_outgoing(qd_tls_t *tls, const pn_raw_buffer_t *unencrypted_buff, qd_raw_buffer_list_t *encrypted_buffs)
{
    if (tls->tls_error)
        return false;

    if (unencrypted_buff) {
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing unencrypted_buff.size=%zu", tls->conn_id, unencrypted_buff->size);
    }

    if (pn_tls_is_secure(tls->tls_session)) {
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing pn_tls_can_encrypt(tls_session)", tls->conn_id);

        //
        // This TLS session is secure which means that the TLS handhshake between peers has succeeded.
        // It is time to call post_handshake_action_outgoing
        //

        if (tls->user_context && !tls->action_outgoing_called) {
            tls->post_handshake_action_outgoing(tls->user_context);
            if (tls->connection_info)
                set_qdr_connection_info_details(tls);
            tls->action_outgoing_called = true;
            qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing called post_handshake_action_outgoing", tls->conn_id);
        }

        if (unencrypted_buff) {
            size_t encrypt_input_buff_capacity = pn_tls_get_encrypt_input_buffer_capacity(tls->tls_session);
            (void) encrypt_input_buff_capacity;
            assert (encrypt_input_buff_capacity > 0);

            qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing pn_tls_give_encrypt_input_buffers", tls->conn_id);
            size_t consumed = pn_tls_give_encrypt_input_buffers(tls->tls_session, unencrypted_buff, 1);
            (void)consumed; // prevent unused variable warning
            assert(consumed == 1);

            //
            // Process TLS.
            //
            if (!process_tls(tls)) {
                take_back_input_encrypt_buff(tls);
                return false;
            }
        }

        take_back_input_encrypt_buff(tls);
        tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
    }

    while (pn_tls_need_encrypt_output_buffers(tls->tls_session)) {
        //
        // We will give just one result buffer
        //
        // This is the raw buffer that will hold the encrypted results
        pn_raw_buffer_t encrypted_result_raw_buffer;
        qd_adaptor_buffer_t *adaptor_buff = qd_adaptor_buffer_raw(&encrypted_result_raw_buffer);
        (void)adaptor_buff;

        //
        // Send the empty raw_buffers to proton. Proton will put the encrypted data into encrypted_result_raw_buffers
        //
        size_t encrypt_result_buffers_count = pn_tls_give_encrypt_output_buffers(tls->tls_session, &encrypted_result_raw_buffer, 1);
        assert (encrypt_result_buffers_count == 1);
        (void) encrypt_result_buffers_count; // prevent unused variable warning

        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing pn_tls_give_encrypt_output_buffers", tls->conn_id);

        //
        // Process TLS.
        //
        if (!process_tls(tls)) {
            take_back_input_encrypt_buff(tls);
            return false;
        }

        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing process_tls successful", tls->conn_id);
    }

    pn_raw_buffer_t encrypted_output_buff;
    while (pn_tls_take_encrypt_output_buffers(tls->tls_session, &encrypted_output_buff, 1)) {
        qd_raw_buffer_t *qd_raw = new_qd_raw_buffer_t();
        ZERO(qd_raw);
        qd_raw->pn_raw_buff.size = encrypted_output_buff.size;
        qd_raw->pn_raw_buff.bytes = encrypted_output_buff.bytes;
        qd_raw->pn_raw_buff.capacity = encrypted_output_buff.capacity;
        qd_raw->pn_raw_buff.offset = encrypted_output_buff.offset;
        qd_raw->pn_raw_buff.context = encrypted_output_buff.context;
        DEQ_INSERT_TAIL(*encrypted_buffs, qd_raw);
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_encrypt_outgoing pn_tls_take_encrypt_output_buffers, qd_raw->pn_raw_buff.size=%zu", tls->conn_id, qd_raw->pn_raw_buff.size);
    }

    return true;
}


/**
 * Decrypts the data in the incoming_buf and puts the output in the decrypted_output_buffs
 */
bool qd_tls_decrypt_incoming(qd_tls_t *tls, const pn_raw_buffer_t *encrypted_buff, qd_raw_buffer_list_t *decrypted_buffs)
{
    assert(encrypted_buff->size > 0);
    qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming encrypted_buff.size=%"PRIu32"", tls->conn_id, encrypted_buff->size);

    if (tls->tls_error)
        return false;

    // encrypted data is in the incoming_buf raw buffer.
    size_t input_buff_capacity = pn_tls_get_decrypt_input_buffer_capacity(tls->tls_session);
    (void)input_buff_capacity;   // prevent unused variable warning
    assert(input_buff_capacity > 0);

    size_t consumed = pn_tls_give_decrypt_input_buffers(tls->tls_session, encrypted_buff, 1);
    (void)consumed;   // prevent unused variable warning
    assert(consumed == 1);

    size_t result_decrypt_buff_capacity = pn_tls_get_decrypt_output_buffer_capacity(tls->tls_session);

    if (result_decrypt_buff_capacity == 0) {
        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming result_decrypt_buff_capacity == 0", tls->conn_id);
    }
    else {
        //
        // Process TLS.
        //
        if (!process_tls(tls)) {
            take_back_input_decrypt_buff(tls);
            return false;
        }

        qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming process_tls successful", tls->conn_id);

        while (pn_tls_need_decrypt_output_buffers(tls->tls_session)) {
            //
            // Give one raw buffer to tls which will be used to decrypt.
            //
            pn_raw_buffer_t decrypted_raw_buffer;
            qd_adaptor_buffer_t *decrypted_adaptor_buf = qd_adaptor_buffer_raw(&decrypted_raw_buffer);
            (void)decrypted_adaptor_buf;

            size_t result = pn_tls_give_decrypt_output_buffers(tls->tls_session, &decrypted_raw_buffer, 1);
            (void)result;   // prevent unused variable warning

            //
            // Process TLS and log an error if any.
            //
            if (!process_tls(tls)) {
                take_back_input_decrypt_buff(tls);
                return false;
            }

            qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming (pn_tls_need_decrypt_output_buffers) process_tls successful, result=%zu", tls->conn_id, result);
        }

        if (pn_tls_is_secure(tls->tls_session)) {
            if(tls->alpn_handler && !tls->alpn_check_complete) {
                bool alpn_success = tls->alpn_handler(tls, tls->user_context);
                if (!alpn_success) {
                    assert(DEQ_IS_EMPTY(*decrypted_buffs));
                    take_back_input_decrypt_buff(tls);
                    return false;
                }
                tls->alpn_check_complete = true;
            }

            //
            // This TLS session is secure which means that the TLS handhshake between peers has succeeded.
            // It is time to call post_handshake_action_incoming but make sure we call the action only once.
            //
            if (tls->user_context && !tls->action_incoming_called) {
                tls->post_handshake_action_incoming(tls->user_context);
                if (tls->connection_info)
                    set_qdr_connection_info_details(tls);
                tls->action_incoming_called = true;
                qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming called post_handshake_action_incoming", tls->conn_id);
            }
        }

        pn_raw_buffer_t decrypted_output_buff;
        while (pn_tls_take_decrypt_output_buffers(tls->tls_session, &decrypted_output_buff, 1)) {
            qd_raw_buffer_t *qd_raw = new_qd_raw_buffer_t();
            ZERO(qd_raw);
            qd_raw->pn_raw_buff.size = decrypted_output_buff.size;
            qd_raw->pn_raw_buff.bytes = decrypted_output_buff.bytes;
            qd_raw->pn_raw_buff.capacity = decrypted_output_buff.capacity;
            qd_raw->pn_raw_buff.offset = decrypted_output_buff.offset;
            qd_raw->pn_raw_buff.context = decrypted_output_buff.context;
            DEQ_INSERT_TAIL(*decrypted_buffs, qd_raw);
            qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming pn_tls_take_decrypt_output_buffers, decrypt buffer size=%zu, decrypt buffer capacity=%zu", tls->conn_id, qd_raw->pn_raw_buff.size, qd_raw->pn_raw_buff.capacity);
        }
    }

    tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
    qd_log(tls->log_source, QD_LOG_TRACE, "[C%"PRIu64"] qd_tls_decrypt_incoming conn->tls_has_output=%i", tls->conn_id, tls->tls_has_output);
    take_back_input_decrypt_buff(tls);
    return true;

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

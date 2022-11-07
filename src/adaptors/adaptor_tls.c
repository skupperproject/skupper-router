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
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"

#include <proton/tls.h>

// Proton TLS uses 4 as its default_decrypt_buffer_count and default_encrypt_buffer_count
#define TLS_MAX_INPUT_CAPACITY 4

struct qd_tls_domain_t {
    sys_atomic_t     ref_count;
    qd_log_source_t *log_source;
    pn_tls_config_t *pn_tls_config;
    char            *ssl_profile_name;
    char            *host;
    bool             is_listener;
};

ALLOC_DECLARE(qd_tls_domain_t);
ALLOC_DEFINE(qd_tls_domain_t);

struct qd_tls_t {
    pn_tls_t              *tls_session;
    qd_tls_domain_t       *tls_domain;
    void                  *user_context;
    qd_log_source_t       *log_source;
    uint64_t               conn_id;
    qd_tls_on_secure_cb_t *on_secure_cb;
    bool                   tls_has_output;
    bool                   tls_error;

    uint64_t encrypted_output_bytes;
    uint64_t encrypted_input_bytes;
};

ALLOC_DECLARE(qd_tls_t);
ALLOC_DEFINE(qd_tls_t);

static void take_back_decrypt_input_buffs(qd_tls_t *tls);
static void take_back_encrypt_input_buffs(qd_tls_t *tls);
static void take_back_decrypt_output_buffs(qd_tls_t *tls);
static void take_back_encrypt_output_buffs(qd_tls_t *tls);

qd_tls_t *qd_tls(qd_tls_domain_t *tls_domain, void *context, uint64_t conn_id, qd_tls_on_secure_cb_t *on_secure)
{
    assert(tls_domain);

    qd_tls_t *tls = new_qd_tls_t();
    ZERO(tls);
    tls->user_context = context;
    tls->conn_id      = conn_id;
    tls->on_secure_cb = on_secure;
    tls->log_source   = tls_domain->log_source;
    tls->tls_domain   = tls_domain;
    sys_atomic_inc(&tls_domain->ref_count);

    tls->tls_session = pn_tls(tls_domain->pn_tls_config);
    if (!tls->tls_session) {
        qd_log(tls->log_source,
               QD_LOG_ERROR,
               "[C%" PRIu64 "] Failed to create TLS session for sslProfile %s with hostname: '%s'",
               tls->conn_id,
               tls_domain->ssl_profile_name,
               tls_domain->host);
        qd_tls_free(tls);
        return 0;
    }

    int ret = pn_tls_set_peer_hostname(tls->tls_session, tls_domain->host);
    if (ret != 0) {
        qd_log(tls->log_source,
               QD_LOG_ERROR,
               "[C%" PRIu64 "] sslProfile %s: Failed to configure peer hostname '%s' (%d)",
               tls->conn_id,
               tls_domain->ssl_profile_name,
               tls_domain->host,
               ret);
        qd_tls_free(tls);
        return 0;
    }

    ret = pn_tls_start(tls->tls_session);
    if (ret != 0) {
        qd_log(tls->log_source,
               QD_LOG_ERROR,
               "[C%" PRIu64 "] sslProfile %s: Failed to start TLS session (%d)",
               tls->conn_id,
               tls_domain->ssl_profile_name,
               ret);
        qd_tls_free(tls);
        return 0;
    }

    // intitalize tls_has_output to true when the router is acting as the client initiating TLS handshake.
    if (!tls_domain->is_listener) {
        tls->tls_has_output = true;
    }

    return tls;
}

qd_tls_domain_t *qd_tls_domain(const qd_adaptor_config_t *config,
                               const qd_dispatch_t       *qd,
                               qd_log_source_t           *log_source,
                               const char                *alpn_protocols[],
                               size_t                     alpn_protocol_count,
                               bool                       is_listener)
{
    const char *role = is_listener ? "listener" : "connector";

    qd_tls_domain_t *tls_domain = new_qd_tls_domain_t();
    ZERO(tls_domain);
    sys_atomic_init(&tls_domain->ref_count, 1);
    tls_domain->log_source       = log_source;
    tls_domain->is_listener      = is_listener;
    tls_domain->host             = qd_strdup(config->host);
    tls_domain->ssl_profile_name = qd_strdup(config->ssl_profile_name);

    qd_log(log_source,
           QD_LOG_DEBUG,
           "Configuring adaptor %s %s sslProfile %s",
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
            qd_log(log_source,
                   QD_LOG_ERROR,
                   "Adaptor %s %s configuration error: failed to find sslProfile '%s'",
                   role,
                   config->name,
                   config->ssl_profile_name);
            break;
        }

        int res                   = -1;  // assume failure
        tls_domain->pn_tls_config = pn_tls_config(is_listener ? PN_TLS_MODE_SERVER : PN_TLS_MODE_CLIENT);

        if (!tls_domain->pn_tls_config) {
            qd_log(log_source,
                   QD_LOG_ERROR,
                   "Adaptor %s %s sslProfile %s: failed to create TLS domain",
                   role,
                   config->name,
                   config->ssl_profile_name);
            break;
        }

        if (config_ssl_profile->ssl_trusted_certificate_db) {
            res = pn_tls_config_set_trusted_certs(tls_domain->pn_tls_config,
                                                  config_ssl_profile->ssl_trusted_certificate_db);
            if (res != 0) {
                qd_log(log_source,
                       QD_LOG_ERROR,
                       "Adaptor %s %s sslProfile %s: failed to set TLS caCertFile %s: (%d)",
                       role,
                       config->name,
                       config->ssl_profile_name,
                       config_ssl_profile->ssl_trusted_certificate_db,
                       res);
                break;
            }
        }

        // Call pn_tls_config_set_credentials only if "certFile" is provided.
        if (config_ssl_profile->ssl_certificate_file) {
            res = pn_tls_config_set_credentials(tls_domain->pn_tls_config,
                                                config_ssl_profile->ssl_certificate_file,
                                                config_ssl_profile->ssl_private_key_file,
                                                config_ssl_profile->ssl_password);
            if (res != 0) {
                qd_log(log_source,
                       QD_LOG_ERROR,
                       "Adaptor %s %s sslProfile %s: failed to set TLS certificate configuration (certFile) %s: (%d)",
                       role,
                       config->name,
                       config->ssl_profile_name,
                       config_ssl_profile->ssl_certificate_file,
                       res);
                break;
            }
        } else {
            qd_log(log_source,
                   QD_LOG_INFO,
                   "Adaptor %s %s sslProfile %s: did not provide a certFile configuration",
                   role,
                   config->name,
                   config->ssl_profile_name);
        }

        if (!!config_ssl_profile->ssl_ciphers) {
            res = pn_tls_config_set_impl_ciphers(tls_domain->pn_tls_config, config_ssl_profile->ssl_ciphers);
            if (res != 0) {
                qd_log(log_source,
                       QD_LOG_ERROR,
                       "Adaptor %s %s sslProfile %s: failed to configure ciphers %s (%d)",
                       role,
                       config->name,
                       config->ssl_profile_name,
                       config_ssl_profile->ssl_ciphers,
                       res);
                break;
            }
        }

        if (is_listener) {
            if (config->authenticate_peer) {
                res = pn_tls_config_set_peer_authentication(
                    tls_domain->pn_tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(tls_domain->pn_tls_config, PN_TLS_ANONYMOUS_PEER, 0);
            }
        } else {
            // Connector.
            if (config->verify_host_name) {
                res = pn_tls_config_set_peer_authentication(
                    tls_domain->pn_tls_config, PN_TLS_VERIFY_PEER_NAME, config_ssl_profile->ssl_trusted_certificate_db);
            } else {
                res = pn_tls_config_set_peer_authentication(
                    tls_domain->pn_tls_config, PN_TLS_VERIFY_PEER, config_ssl_profile->ssl_trusted_certificate_db);
            }
        }

        if (res != 0) {
            qd_log(log_source,
                   QD_LOG_ERROR,
                   "Adaptor %s %s sslProfile %s: failed to configure TLS peer authentication (%d)",
                   role,
                   config->name,
                   config->ssl_profile_name,
                   res);
            break;
        }

        //
        // Provide an ordered list of application protocols for ALPN by calling pn_tls_config_set_alpn_protocols. In our
        // case, h2 is the only supported protocol. A list of protocols can be found here -
        // https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.txt
        //
        if (alpn_protocols) {
            res = pn_tls_config_set_alpn_protocols(tls_domain->pn_tls_config, alpn_protocols, alpn_protocol_count);
            if (res != 0) {
                qd_log(log_source,
                       QD_LOG_ERROR,
                       "Adaptor %s %s sslProfile %s: failed to configure ALPN protocols (%d)",
                       role,
                       config->name,
                       config->ssl_profile_name,
                       res);
                break;
            }
        }

        qd_log(log_source,
               QD_LOG_INFO,
               "Adaptor %s %s successfully configured sslProfile %s",
               role,
               config->name,
               config->ssl_profile_name);
        return tls_domain;

    } while (0);

    // If we get here, the configuration setup failed

    qd_tls_domain_decref(tls_domain);
    return 0;
}

void qd_tls_domain_decref(qd_tls_domain_t *tls_domain)
{
    if (tls_domain) {
        uint32_t rc = sys_atomic_dec(&tls_domain->ref_count);
        assert(rc != 0);
        if (rc == 1) {
            if (tls_domain->pn_tls_config)
                pn_tls_config_free(tls_domain->pn_tls_config);
            sys_atomic_destroy(&tls_domain->ref_count);
            free(tls_domain->host);
            free(tls_domain->ssl_profile_name);
            free_qd_tls_domain_t(tls_domain);
        }
    }
}

void qd_tls_update_connection_info(qd_tls_t *tls, qdr_connection_info_t *conn_info)
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

void qd_tls_free(qd_tls_t *tls)
{
    if (tls) {
        if (tls->tls_session) {
            pn_tls_stop(tls->tls_session);
            take_back_encrypt_output_buffs(tls);
            take_back_decrypt_output_buffs(tls);
            take_back_encrypt_input_buffs(tls);
            take_back_decrypt_input_buffs(tls);
            pn_tls_free(tls->tls_session);
            tls->tls_session = 0;
        }
        qd_tls_domain_decref(tls->tls_domain);
        free_qd_tls_t(tls);
    }
}

/**
 * Call pn_tls_process and prints out a log message with an error if there is an error processing TLS.
 * @param qd_tls_t object
 * @return True if tls was processed with no errors, false otherwise.
 */
static bool process_tls(qd_tls_t *tls)
{
    const bool check_if_secure = tls->on_secure_cb && !pn_tls_is_secure(tls->tls_session);

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

    if (check_if_secure && pn_tls_is_secure(tls->tls_session)) {
        tls->on_secure_cb(tls, tls->user_context);
        tls->on_secure_cb = 0;  // one shot
    }
    return true;
}

static void take_back_encrypt_output_buffs(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t          take_input_count;

    while ((take_input_count = pn_tls_take_encrypt_output_buffers(tls->tls_session, &take_incoming_buf, 1))) {
        assert(take_input_count == 1);
        (void) take_input_count;  // prevent unused variable warning
        qd_adaptor_buffer_t *adaptor_buff = (qd_adaptor_buffer_t *) take_incoming_buf.context;
        qd_adaptor_buffer_free(adaptor_buff);
    }
}

static void take_back_decrypt_output_buffs(qd_tls_t *tls)
{
    pn_raw_buffer_t take_incoming_buf;
    size_t          take_input_count;

    while ((take_input_count = pn_tls_take_decrypt_output_buffers(tls->tls_session, &take_incoming_buf, 1))) {
        assert(take_input_count == 1);
        (void) take_input_count;  // prevent unused variable warning
        qd_adaptor_buffer_t *adaptor_buff = (qd_adaptor_buffer_t *) take_incoming_buf.context;
        qd_adaptor_buffer_free(adaptor_buff);
    }
}

static void take_back_encrypt_input_buffs(qd_tls_t *tls)
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
static void take_back_decrypt_input_buffs(qd_tls_t *tls)
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

static size_t get_pn_raw_buffer_fetch_size(qd_tls_t *tls)
{
    size_t tls_input_buff_capacity = pn_tls_get_decrypt_input_buffer_capacity(tls->tls_session);
    size_t fetch_size              = MIN(tls_input_buff_capacity, TLS_MAX_INPUT_CAPACITY);
    return fetch_size;
}

int qd_tls_decrypt(qd_tls_t *tls, pn_raw_connection_t *pn_raw_conn, qd_adaptor_buffer_list_t *decrypted_buffs)
{
    int    encrypted_bytes_in = 0;
    size_t fetch_size         = get_pn_raw_buffer_fetch_size(tls);

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
                       "[C%" PRIu64 "] qd_tls_decrypt raw buffer size=0, did not sent this buffer to TLS, freeing it",
                       tls->conn_id);
                qd_adaptor_buffer_free(adaptor_buff);
            }
        }

        //
        // Process TLS.
        //
        if (!process_tls(tls)) {
            take_back_decrypt_input_buffs(tls);
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
                take_back_decrypt_input_buffs(tls);
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
                   "[C%" PRIu64 "] qd_tls_decrypt pn_tls_take_decrypt_output_buffers, decrypt adaptor buffer size=%zu",
                   tls->conn_id,
                   qd_adaptor_buffer_size(decrypted_adaptor_buff));
        }

        // You can take only a maximum of decrypt_output_buff_capacity. You will have to recheck
        // pn_tls_need_decrypt_output_buffers to get everything back.
        if (pn_tls_need_decrypt_output_buffers(tls->tls_session)) {
            goto give_decrypt_output_buffers;
        }
        take_back_decrypt_input_buffs(tls);
        fetch_size = get_pn_raw_buffer_fetch_size(tls);
    }

    tls->tls_has_output = pn_tls_is_encrypt_output_pending(tls->tls_session);
    qd_log(tls->log_source,
           QD_LOG_TRACE,
           "[C%" PRIu64 "] qd_tls_decrypt tls->tls_has_output=%i",
           tls->conn_id,
           qd_tls_has_output(tls));
    take_back_decrypt_input_buffs(tls);
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
                take_back_encrypt_input_buffs(tls);
                return QD_TLS_ERROR;
            }
        }

        // We are immediately taking back the  encrypt input buffers which means the tls encrypt input buffer capacity
        // is restored.
        take_back_encrypt_input_buffs(tls);
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
            take_back_encrypt_input_buffs(tls);
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

bool qd_tls_is_error(const qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return tls->tls_error;
}

bool qd_tls_is_secure(const qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return pn_tls_is_secure(tls->tls_session);
}

bool qd_tls_has_output(const qd_tls_t *tls)
{
    if (!tls || !tls->tls_session)
        return false;
    return tls->tls_has_output;
}

pn_tls_t *qd_tls_get_pn_tls_session(qd_tls_t *tls)
{
    return tls->tls_session;
}

// note: only use with qd_tls_do_io
uint64_t qd_tls_get_encrypted_output_octet_count(const qd_tls_t *tls)
{
    assert(tls);
    return tls->encrypted_output_bytes;
}

// note: only use with qd_tls_do_io
uint64_t qd_tls_get_encrypted_input_octet_count(const qd_tls_t *tls)
{
    assert(tls);
    return tls->encrypted_input_bytes;
}

int qd_tls_do_io(qd_tls_t                     *tls,
                 pn_raw_connection_t          *raw_conn,
                 qd_tls_take_output_data_cb_t *take_output_cb,
                 void                         *take_output_context,
                 qd_adaptor_buffer_list_t     *input_data,
                 uint64_t                     *input_data_count)
{
    bool work;
    *input_data_count = 0;

    do {
        size_t          capacity;
        size_t          taken;
        size_t          given;
        pn_raw_buffer_t pn_buf_desc;
        uint64_t        total_octets;

        work = false;

        // give empty buffers for holding encrypted output from TLS

        capacity = pn_tls_get_encrypt_output_buffer_capacity(tls->tls_session);
        if (capacity > 0) {
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] giving TLS %zu empty buffers for encrypted output",
                   tls->conn_id,
                   capacity);
            work = true;
            while (capacity-- > 0) {
                (void) qd_adaptor_buffer_raw(&pn_buf_desc);
                given = pn_tls_give_encrypt_output_buffers(tls->tls_session, &pn_buf_desc, 1);
                (void) given;
                assert(given == 1);
            }
        }

        // push any unencrypted output data from the protocol adaptor into TLS for encryption.

        if (pn_tls_is_secure(tls->tls_session)) {
            assert(take_output_cb);
            capacity = pn_tls_get_encrypt_input_buffer_capacity(tls->tls_session);
            if (capacity > 0) {
                qd_adaptor_buffer_list_t ubufs = DEQ_EMPTY;

                int64_t out_octets = take_output_cb(take_output_context, &ubufs, capacity);
                if (out_octets > 0) {
                    work = true;
                    assert(!DEQ_IS_EMPTY(ubufs) && DEQ_SIZE(ubufs) <= capacity);
                    qd_log(tls->log_source,
                           QD_LOG_TRACE,
                           "[C%" PRIu64 "] %" PRIi64 " unencrypted octets taken by TLS for encryption (%zu buffers)",
                           tls->conn_id,
                           out_octets,
                           DEQ_SIZE(ubufs));
                    qd_adaptor_buffer_t *abuf = DEQ_HEAD(ubufs);
                    while (abuf) {
                        DEQ_REMOVE_HEAD(ubufs);
                        qd_adaptor_buffer_pn_raw_buffer(&pn_buf_desc, abuf);
                        given = pn_tls_give_encrypt_input_buffers(tls->tls_session, &pn_buf_desc, 1);
                        assert(given == 1);
                        abuf = DEQ_HEAD(ubufs);
                    }
                } else {
                    assert(DEQ_IS_EMPTY(ubufs));
                    if (out_octets == QD_TLS_EOM) {
                        work = true;
                        pn_tls_close_output(tls->tls_session);
                    }
                }
            }
        }

        // give empty output buffers to be filled with decrypted data

        capacity = pn_tls_get_decrypt_output_buffer_capacity(tls->tls_session);
        if (capacity > 0) {
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] giving TLS %zu empty buffers for decrypted output",
                   tls->conn_id,
                   capacity);
            work = true;
            while (capacity-- > 0) {
                (void) qd_adaptor_buffer_raw(&pn_buf_desc);
                given = pn_tls_give_decrypt_output_buffers(tls->tls_session, &pn_buf_desc, 1);
                assert(given == 1);
            }
        }

        // push incoming encrypted data from raw conn into TLS

        capacity = pn_tls_get_decrypt_input_buffer_capacity(tls->tls_session);
        if (capacity > 0) {
            size_t pushed = 0;
            total_octets  = 0;
            while (pushed < capacity) {
                taken = pn_raw_connection_take_read_buffers(raw_conn, &pn_buf_desc, 1);
                if (taken == 0) {
                    // no available encrypted data or read closed
                    break;
                }
                if (pn_buf_desc.size) {
                    total_octets += pn_buf_desc.size;
                    given = pn_tls_give_decrypt_input_buffers(tls->tls_session, &pn_buf_desc, 1);
                    assert(given == 1);
                    ++pushed;
                } else {
                    qd_adaptor_buffer_free((qd_adaptor_buffer_t *) pn_buf_desc.context);
                }
            }

            if (pushed > 0) {
                work = true;
                tls->encrypted_input_bytes += total_octets;
                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64 "] %" PRIu64
                       " encrypted octets read from the raw connection passed to TLS for decryption (%zu buffers)",
                       tls->conn_id,
                       total_octets,
                       pushed);
            }
        } else if (pn_tls_get_session_error(tls->tls_session) || pn_tls_is_input_closed(tls->tls_session)) {
            // TLS will not take any more encrypted input - drain the raw conn input
            int drained = qd_raw_connection_drain_read_buffers(raw_conn);
            if (drained) {
                work = true;  // I assume we need to keep draining until raw conn is closed
                qd_log(tls->log_source, QD_LOG_TRACE,
                       "[C%" PRIu64 "] discarded %d read raw buffers due to TLS session closed", tls->conn_id, drained);
            }
        }

        // run the tls state machine (not considered "work").
        //
        // If there is an error the TLS layer may have generated new encrypted output that contains the details about
        // the failure that needs to be sent to the remote. That is why this code does not immediately cease processing
        // if pn_tls_process returns an error: there may be more outgoing buffers that have to be written to the raw
        // connection before it can be closed. This code assumes that the proton TLS library will stop giving capacity
        // for other work once the error has occurred so it is safe to continue running this work loop.

        if (!tls->tls_error) {
            const bool check_if_secure = tls->on_secure_cb && !pn_tls_is_secure(tls->tls_session);
            int        err             = pn_tls_process(tls->tls_session);
            if (err) {
                tls->tls_error = true;
                qd_log(
                    tls->log_source, QD_LOG_TRACE, "[C%" PRIu64 "] pn_tls_process failed: error=%d", tls->conn_id, err);
            } else if (check_if_secure && pn_tls_is_secure(tls->tls_session)) {
                tls->on_secure_cb(tls, tls->user_context);
                tls->on_secure_cb = 0;  // one shot
            }
        }

        // Take encrypted TLS output and write it to the raw connection

        capacity = pn_raw_connection_write_buffers_capacity(raw_conn);
        if (capacity > 0) {
            size_t pushed = 0;
            total_octets  = 0;
            while (pushed < capacity) {
                taken = pn_tls_take_encrypt_output_buffers(tls->tls_session, &pn_buf_desc, 1);
                if (taken != 1) {
                    break;
                }
                total_octets += pn_buf_desc.size;
                given = pn_raw_connection_write_buffers(raw_conn, &pn_buf_desc, 1);
                assert(given == 1);
                ++pushed;
            }
            if (pushed > 0) {
                work = true;
                tls->encrypted_output_bytes += total_octets;
                qd_log(tls->log_source,
                       QD_LOG_TRACE,
                       "[C%" PRIu64 "] %" PRIu64 " encrypted octets written to the raw connection by TLS (%zu buffers)",
                       tls->conn_id,
                       total_octets,
                       pushed);
            }
        } else if (pn_raw_connection_is_write_closed(raw_conn)) {
            // drain the TLS buffers - there is no place to send them!
            taken = 0;
            while (pn_tls_take_encrypt_output_buffers(tls->tls_session, &pn_buf_desc, 1) == 1) {
                assert(pn_buf_desc.context);
                qd_adaptor_buffer_free((qd_adaptor_buffer_t *) pn_buf_desc.context);
                taken += 1;
            }
            if (taken) {
                work = true;  // keep draining encrypted output
                qd_log(tls->log_source, QD_LOG_TRACE,
                       "[C%" PRIu64 "] discarded %zu outgoing encrypted buffers due to raw conn write closed",
                       tls->conn_id, taken);
            }
        }

        // free all old unencrypted input buffers

        taken = 0;
        while (pn_tls_take_encrypt_input_buffers(tls->tls_session, &pn_buf_desc, 1) == 1) {
            qd_adaptor_buffer_t *abuf = (qd_adaptor_buffer_t *) pn_buf_desc.context;
            assert(abuf);
            qd_adaptor_buffer_free(abuf);
            ++taken;
        }
        if (taken > 0) {
            work = true;
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] freed %zu old TLS unencrypted input buffers",
                   tls->conn_id,
                   taken);
        }

        // take decrypted output and give it to the adaptor

        total_octets = 0;
        taken        = 0;
        while (pn_tls_take_decrypt_output_buffers(tls->tls_session, &pn_buf_desc, 1) == 1) {
            qd_adaptor_buffer_t *abuf = qd_get_adaptor_buffer_from_pn_raw_buffer(&pn_buf_desc);
            if (pn_buf_desc.size) {
                total_octets += pn_buf_desc.size;
                DEQ_INSERT_TAIL(*input_data, abuf);
                ++taken;
            } else {
                qd_adaptor_buffer_free(abuf);
            }
        }
        if (taken) {
            work = true;
            *input_data_count += total_octets;
            qd_log(tls->log_source, QD_LOG_TRACE,
                   "[C%" PRIu64 "] %" PRIu64 " decrypted octets taken from TLS for adaptor input (%zu buffers)",
                   tls->conn_id, total_octets, taken);
        }

        // free old decryption input buffers

        taken = 0;
        while (pn_tls_take_decrypt_input_buffers(tls->tls_session, &pn_buf_desc, 1) == 1) {
            qd_adaptor_buffer_t *abuf = (qd_adaptor_buffer_t *) pn_buf_desc.context;
            assert(abuf);
            qd_adaptor_buffer_free(abuf);
            ++taken;
        }
        if (taken > 0) {
            work = true;
            qd_log(tls->log_source,
                   QD_LOG_TRACE,
                   "[C%" PRIu64 "] freed %zu old TLS encrypted input buffers",
                   tls->conn_id,
                   taken);
        }

    } while (work);

    // Cannot return an error until all pending outgoing encrypted data have been written to the raw conn so error info
    // can be sent to peer. See comment above.
    //
    if (tls->tls_error && pn_tls_get_decrypt_output_buffer_count(tls->tls_session) == 0) {
        char buf[1024];
        int  err = pn_tls_get_session_error(tls->tls_session);
        pn_tls_get_session_error_string(tls->tls_session, buf, sizeof(buf));
        qd_log(tls->log_source, QD_LOG_ERROR, "[C%" PRIu64 "] TLS connection failed (%d): %s", tls->conn_id, err, buf);
        return err;
    }
    return 0;
}

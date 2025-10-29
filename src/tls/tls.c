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
#include "private.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/error.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/buffer.h"
#include "qpid/dispatch/entity.h"

#include <proton/tls.h>
#include <proton/ssl.h>
#include <proton/transport.h>

#include <inttypes.h>

/*
 * Manages TLS configuration and stream lifecycle
 */

#define CHECKED_STRDUP(S) (!!(S) ? qd_strdup(S) : 0)


ALLOC_DECLARE(qd_proton_config_t);
ALLOC_DEFINE(qd_proton_config_t);

ALLOC_DECLARE(qd_tls_session_t);
ALLOC_DEFINE(qd_tls_session_t);

ALLOC_DECLARE(qd_tls_config_t);
ALLOC_DEFINE(qd_tls_config_t);

ALLOC_DECLARE(qd_tls_context_t);
ALLOC_DEFINE(qd_tls_context_t);


/**
 * Master list of all active TLS context instances. Only accessed by the management thread so no locking necessary.
 */
static qd_tls_context_list_t context_list;


// Internals:
static qd_error_t _read_tls_profile(qd_entity_t *entity, qd_ssl2_profile_t *profile);
static void _cleanup_tls_profile(qd_ssl2_profile_t *profile);
static qd_tls_context_t *_find_tls_context(const char *profile_name);
static void _tls_context_free(qd_tls_context_t *ctxt);
static qd_error_t _update_tls_config(qd_tls_config_t *tls_config, const qd_ssl2_profile_t *profile);
static qd_error_t _validate_config(const qd_ssl2_profile_t *profile, const char *profile_name, bool is_listener,
                                   bool authenticate_peer);

// TODO: these should be moved somewhere public as they are called from multiple places
extern void qd_server_config_process_password(char **actual_val, char *pw, bool *is_file, bool allow_literal_prefix);
extern void qd_set_password_from_file(const char *password_file, char **password_field);



/* Thread verification
 *
 * We can avoid locking of the context_list IF all the functions that access the list are running on the same
 * thread. Initial configuration file load occurs on main thread, management updates runs via a zero timer.
 */
#define ASSERT_MGMT_THREAD assert(sys_thread_role(0) == SYS_THREAD_MAIN || sys_thread_proactor_mode() == SYS_THREAD_PROACTOR_MODE_TIMER)

void qd_tls_initialize(void)
{
    DEQ_INIT(context_list);
}


void qd_tls_finalize(void)
{
    qd_tls_context_t *ctxt = DEQ_HEAD(context_list);
    while (ctxt) {
        DEQ_REMOVE_HEAD(context_list);
        _tls_context_free(ctxt);
        ctxt = DEQ_HEAD(context_list);
    }
    tls_private_release_display_name_service();
}


/**
 * Handle sslProfile record create request from management
 */
QD_EXPORT void *qd_tls_configure_ssl_profile(qd_dispatch_t *qd, qd_entity_t *entity)
{
    ASSERT_MGMT_THREAD;

    qd_error_clear();
    char *name = qd_entity_opt_string(entity, "name", 0);
    if (!name || qd_error_code()) {
        free(name);
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Unable to create sslProfile: %s", qd_error_message());
        return 0;
    }

    qd_tls_context_t *tls_context = new_qd_tls_context_t();
    ZERO(tls_context);
    DEQ_ITEM_INIT(tls_context);
    DEQ_INIT(tls_context->tls_configs);
    tls_context->ssl_profile_name = name;

    if (_read_tls_profile(entity, &tls_context->profile) != QD_ERROR_NONE) {
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Unable to create sslProfile '%s': %s", name, qd_error_message());
        _tls_context_free(tls_context);
        return 0;
    }

    DEQ_INSERT_TAIL(context_list, tls_context);
    qd_log(LOG_AGENT, QD_LOG_INFO, "Created sslProfile %s", tls_context->ssl_profile_name);
    return tls_context;
}


/**
 * Handle sslProfile record delete request from management.
 */
QD_EXPORT void qd_tls_delete_ssl_profile(qd_dispatch_t *qd, void *impl)
{
    ASSERT_MGMT_THREAD;

    qd_tls_context_t *tls_context = (qd_tls_context_t *) impl;
    assert(tls_context);

    DEQ_REMOVE(context_list, tls_context);

    qd_log(LOG_AGENT, QD_LOG_INFO, "Deleted sslProfile %s", tls_context->ssl_profile_name);

    _tls_context_free(tls_context);
}


/**
 * Handle sslProfile record update request from management.
 */
QD_EXPORT void *qd_tls_update_ssl_profile(qd_dispatch_t *qd, qd_entity_t *entity, void *impl)
{
    ASSERT_MGMT_THREAD;

    qd_tls_context_t  *tls_context = (qd_tls_context_t *) impl;
    qd_ssl2_profile_t  new_profile;

    qd_error_clear();

    assert(tls_context);
    if (_read_tls_profile(entity, &new_profile) != QD_ERROR_NONE) {
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Unable to update sslProfile '%s': %s", tls_context->ssl_profile_name, qd_error_message());
        return 0;
    }

    qd_tls_config_t *config = DEQ_HEAD(tls_context->tls_configs);
    while (config) {
        if (_update_tls_config(config, &new_profile) != QD_ERROR_NONE) {
            // There is a problem with the new configuration. Discard the change and return 0 to force the management
            // operation to fail
            qd_log(LOG_AGENT, QD_LOG_ERROR, "Failed to update sslProfile '%s': %s", tls_context->ssl_profile_name, qd_error_message());
            _cleanup_tls_profile(&new_profile);
            return 0;
        }
        config = DEQ_NEXT(config);
    }

    _cleanup_tls_profile(&tls_context->profile);
    tls_context->profile = new_profile;
    qd_log(LOG_AGENT, QD_LOG_INFO, "Updated sslProfile %s ", tls_context->ssl_profile_name);
    return impl;
}


/**
 * Dummy stub since the Python agent expects a "qd_entity_refresh_BLAH" for every
 * entity that has a C implementation (see CImplementation in agent.py)
 */
QD_EXPORT qd_error_t qd_entity_refresh_sslProfile(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}


qd_proton_config_t *qd_proton_config(pn_tls_config_t *tls_cfg, pn_ssl_domain_t *ssl_cfg)
{
    qd_proton_config_t *p_cfg = new_qd_proton_config_t();
    ZERO(p_cfg);
    p_cfg->pn_raw  = tls_cfg;
    p_cfg->pn_amqp = ssl_cfg;
    sys_mutex_init(&p_cfg->lock);
    sys_atomic_init(&p_cfg->ref_count, 1);
    return p_cfg;
}


void qd_proton_config_decref(qd_proton_config_t *p_cfg)
{
    if (p_cfg) {
        uint32_t rc = sys_atomic_dec(&p_cfg->ref_count);
        assert(rc != 0);  // underflow!
        if (rc == 1) {
            if (p_cfg->pn_raw) {
                pn_tls_config_free(p_cfg->pn_raw);
            }
            if (p_cfg->pn_amqp) {
                pn_ssl_domain_free(p_cfg->pn_amqp);
            }
            sys_mutex_free(&p_cfg->lock);
            sys_atomic_destroy(&p_cfg->ref_count);
            free_qd_proton_config_t(p_cfg);
        }
    }
}


qd_tls_config_t *qd_tls_config(const char *ssl_profile_name,
                               qd_tls_type_t p_type,
                               qd_tls_config_mode_t mode,
                               bool verify_hostname,
                               bool authenticate_peer)
{
    ASSERT_MGMT_THREAD;  // called from listener/connector create callback

    pn_tls_config_t *pn_raw_config = 0;
    pn_ssl_domain_t *pn_amqp_config = 0;
    const bool is_listener = mode == QD_TLS_CONFIG_SERVER_MODE;

    qd_error_clear();

    qd_tls_context_t *tls_context = _find_tls_context(ssl_profile_name);
    if (!tls_context) {
        qd_error(QD_ERROR_NOT_FOUND, "sslProfile '%s' not found", ssl_profile_name);
        return 0;
    }

    if (_validate_config(&tls_context->profile, ssl_profile_name, is_listener, authenticate_peer) != QD_ERROR_NONE) {
        return 0;  // validate_config sets qd_error()
    }

    switch (p_type) {
        case QD_TLS_TYPE_PROTON_AMQP:
            pn_amqp_config = tls_private_allocate_amqp_config(ssl_profile_name, &tls_context->profile, is_listener,
                                                              verify_hostname, authenticate_peer);
            if (!pn_amqp_config) {
                // allocation function set qd_error()
                return 0;
            }
            break;
        case QD_TLS_TYPE_PROTON_RAW:
            pn_raw_config = tls_private_allocate_raw_config(ssl_profile_name, &tls_context->profile, is_listener,
                                                            verify_hostname, authenticate_peer);
            if (!pn_raw_config) {
                // allocation function set qd_error()
                return 0;
            }
            break;
        default:
            assert(false);
            break;
    }

    qd_tls_config_t *tls_config = new_qd_tls_config_t();
    ZERO(tls_config);
    sys_mutex_init(&tls_config->lock);

    // 1 reference for caller and another for the tls_configs list
    sys_atomic_init(&tls_config->ref_count, 2);

    tls_config->ssl_profile_name     = qd_strdup(ssl_profile_name);
    tls_config->uid_format           = CHECKED_STRDUP(tls_context->profile.uid_format);
    tls_config->ordinal              = tls_context->profile.ordinal;
    tls_config->oldest_valid_ordinal = tls_context->profile.oldest_valid_ordinal;
    tls_config->authenticate_peer    = authenticate_peer;
    tls_config->verify_hostname      = verify_hostname;
    tls_config->is_listener          = is_listener;
    tls_config->p_type               = p_type;
    tls_config->proton_tls_cfg       = qd_proton_config(pn_raw_config, pn_amqp_config);

    DEQ_INSERT_TAIL(tls_context->tls_configs, tls_config);

    return tls_config;
}


void qd_tls_config_decref(qd_tls_config_t *tls_config)
{
    if (tls_config) {
        uint32_t rc = sys_atomic_dec(&tls_config->ref_count);
        assert(rc != 0);  // underflow!
        if (rc == 1) {
            // Last reference: can assume it has already been removed from parent tls_context config list and no
            // other threads are accessing it
            qd_proton_config_decref(tls_config->proton_tls_cfg);
            free(tls_config->ssl_profile_name);
            free(tls_config->uid_format);
            sys_atomic_destroy(&tls_config->ref_count);
            sys_mutex_free(&tls_config->lock);
            free_qd_tls_config_t(tls_config);
        }
    }
}


uint64_t qd_tls_config_get_ordinal(const qd_tls_config_t *config)
{
    // config->ordinal can only be changed by the management thread. Calling this from any other thread risks returning
    // a stale value.
    ASSERT_MGMT_THREAD;

    return config->ordinal;
}


uint64_t qd_tls_config_get_oldest_valid_ordinal(const qd_tls_config_t *config)
{
    // config->oldest_valid_ordinal can only be changed by the management thread. Calling this from any other thread
    // risks returning a stale value.
    ASSERT_MGMT_THREAD;

    return config->oldest_valid_ordinal;
}


void qd_tls_config_register_update_callback(qd_tls_config_t *config, void *update_cb_context,
                                            qd_tls_config_update_cb_t update_cb)
{
    // Since this function can only be called on the mgmt thread there is no chance that the update fields are being
    // accessed by another thread
    ASSERT_MGMT_THREAD;

    config->update_callback = update_cb;
    config->update_context  = update_cb_context;
}


void qd_tls_config_unregister_update_callback(qd_tls_config_t *config)
{
    // Since this function can only be called on the mgmt thread there is no chance that the handler is being run while
    // it is being cancelled.
    ASSERT_MGMT_THREAD;

    if (config) {
        config->update_callback = 0;
        config->update_context  = 0;
    }
}


qd_tls_session_t *qd_tls_session_raw(qd_tls_config_t *tls_config, const char *peer_hostname,
                                     const char **alpn_protocols, size_t alpn_protocol_count,
                                     void *context, qd_tls_session_on_secure_cb_t *on_secure)
{
    assert(tls_config->p_type == QD_TLS_TYPE_PROTON_RAW);

    qd_error_clear();

    qd_tls_session_t *tls_session = new_qd_tls_session_t();
    ZERO(tls_session);

    tls_session->user_context = context;
    tls_session->on_secure_cb = on_secure;
    tls_session->ssl_profile_name = qd_strdup(tls_config->ssl_profile_name);

    // Copy out those config elements that can be modified by the management thread. Do this under lock to prevent the
    // management thread update handler from modifying those config elements during this copy

    qd_proton_config_t *p_cfg = 0;

    sys_mutex_lock(&tls_config->lock);
    p_cfg = tls_config->proton_tls_cfg;
    assert(p_cfg);
    sys_atomic_inc(&p_cfg->ref_count);  // prevents free after we drop lock
    tls_session->uid_format = CHECKED_STRDUP(tls_config->uid_format);  // may be changed by mgmt thread
    tls_session->ordinal    = tls_config->ordinal;  // may be changed by mgmt thread
    sys_mutex_unlock(&tls_config->lock);

    tls_session->proton_tls_cfg = p_cfg;

    // Must hold the proton config lock during the session initialization. Initialization is not thread safe since the
    // proton config is modified during this process.

    sys_mutex_lock(&p_cfg->lock);
    assert(p_cfg->pn_raw);

    // if ALPN needs to be configured it must be done on the proton config. Since the config lock is held this will not
    // effect other sessions.

    if (alpn_protocol_count) {
        assert(alpn_protocols);
        int rc = pn_tls_config_set_alpn_protocols(p_cfg->pn_raw, alpn_protocols, alpn_protocol_count);
        if (rc != 0) {
            sys_mutex_unlock(&p_cfg->lock);
            qd_error(QD_ERROR_CONFIG, "Failed to configure ALPN settings for new TLS session with sslProfile %s (%d)",
                     tls_session->ssl_profile_name, rc);
            goto error;
        }
    }

    bool ok = true;
    tls_session->pn_raw = pn_tls(p_cfg->pn_raw);
    if (!tls_session->pn_raw) {
        ok = false;
        qd_error(QD_ERROR_CONFIG, "Failed to create new TLS session with sslProfile %s", tls_session->ssl_profile_name);

    } else {
        if (peer_hostname) {
            int rc = pn_tls_set_peer_hostname(tls_session->pn_raw, peer_hostname);
            if (rc != 0) {
                ok = false;
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS peer hostname '%s' for sslProfile %s (%d)",
                         peer_hostname, tls_session->ssl_profile_name, rc);
            }
        }

        if (ok) {
            int rc = pn_tls_start(tls_session->pn_raw);
            if (rc != 0) {
                ok = false;
                qd_error(QD_ERROR_CONFIG, "Failed to start TLS session for sslProfile %s (%d)", tls_session->ssl_profile_name, rc);
            }
        }
    }

    // must restore the ALPN settings even if the setup of the session failed

    if (alpn_protocol_count) {
        int rc = pn_tls_config_set_alpn_protocols(p_cfg->pn_raw, 0, 0);
        if (rc != 0) {
            // I have no idea how to recover from this:
            ok = false;
            qd_error(QD_ERROR_CONFIG, "Failed to clear ALPN settings for new TLS session with sslProfile %s (%d)",
                     tls_session->ssl_profile_name, rc);
        }
    }

    sys_mutex_unlock(&p_cfg->lock);

    if (ok)
        return tls_session;
    // else fall through

error:
    qd_tls_session_free(tls_session);
    return 0;
}


qd_tls_session_t *qd_tls_session_amqp(qd_tls_config_t *tls_config, pn_transport_t *tport, bool allow_unencrypted)
{
    assert(tls_config->p_type == QD_TLS_TYPE_PROTON_AMQP);

    qd_error_clear();

    qd_tls_session_t *tls_session = new_qd_tls_session_t();
    ZERO(tls_session);

    tls_session->ssl_profile_name = qd_strdup(tls_config->ssl_profile_name);

    // Copy out those config elements that can be modified by the management thread. Do this under lock to prevent the
    // management thread update handler from modifying those config elements during this copy

    qd_proton_config_t *p_cfg = 0;

    sys_mutex_lock(&tls_config->lock);
    p_cfg = tls_config->proton_tls_cfg;
    assert(p_cfg);
    sys_atomic_inc(&p_cfg->ref_count);  // prevents free after we drop lock
    tls_session->uid_format = CHECKED_STRDUP(tls_config->uid_format);  // may be changed by mgmt thread
    tls_session->ordinal = tls_config->ordinal;  // may be changed by mgmt thread
    sys_mutex_unlock(&tls_config->lock);

    tls_session->proton_tls_cfg = p_cfg;

    // Must hold the proton config lock during the session initialization. Initialization is not thread safe since the
    // proton config is modified during this process.

    sys_mutex_lock(&p_cfg->lock);
    assert(p_cfg->pn_amqp);

    tls_session->pn_amqp = pn_ssl(tport);
    if (!tls_session->pn_amqp) {
        sys_mutex_unlock(&p_cfg->lock);
        qd_error(QD_ERROR_RUNTIME, "Failed to create an AMQP TLS session");
        goto error;
    }

    int rc = pn_ssl_init(tls_session->pn_amqp, p_cfg->pn_amqp, 0);
    if (rc) {
        sys_mutex_unlock(&p_cfg->lock);
        qd_error(QD_ERROR_RUNTIME, "Failed to initialize AMQP TLS session (%d)", rc);
        goto error;
    }

    sys_mutex_unlock(&p_cfg->lock);

    // By default adding tls to a transport forces encryption to be required, so if it's not set that here
    if (allow_unencrypted) {
       pn_transport_require_encryption(tport, false);
    }

    return tls_session;

error:
    qd_tls_session_free(tls_session);
    return 0;
}


void qd_tls_session_free(qd_tls_session_t *tls_session)
{
    pn_raw_buffer_t buf_desc;

    if (tls_session) {
        if (tls_session->pn_raw) {
            pn_tls_stop(tls_session->pn_raw);

            while (pn_tls_take_encrypt_output_buffers(tls_session->pn_raw, &buf_desc, 1) == 1) {
                qd_buffer_free((qd_buffer_t *) buf_desc.context);
            }
            while (pn_tls_take_encrypt_input_buffers(tls_session->pn_raw, &buf_desc, 1) == 1) {
                qd_buffer_free((qd_buffer_t *) buf_desc.context);
            }
            while (pn_tls_take_decrypt_output_buffers(tls_session->pn_raw, &buf_desc, 1) == 1) {
                qd_buffer_free((qd_buffer_t *) buf_desc.context);
            }
            while (pn_tls_take_decrypt_input_buffers(tls_session->pn_raw, &buf_desc, 1) == 1) {
                qd_buffer_free((qd_buffer_t *) buf_desc.context);
            }
        }

        // Need to lock the proton config when releasing the proton stream since the proton config is not thread safe
        // due to use of a non-atomic reference counter.

        sys_mutex_lock(&tls_session->proton_tls_cfg->lock);
        if (tls_session->pn_raw) {
            pn_tls_free(tls_session->pn_raw);
        } else {
            // Proton does not provide a way to explicitly free the AMQP TLS session. It is owned by the parent
            // pn_transport_t and will be released when the transport is closed (I think).
        }
        sys_mutex_unlock(&tls_session->proton_tls_cfg->lock);

        qd_proton_config_decref(tls_session->proton_tls_cfg);
        free(tls_session->ssl_profile_name);
        free(tls_session->uid_format);
        free_qd_tls_session_t(tls_session);
    }
}


char *qd_tls_session_get_alpn_protocol(const qd_tls_session_t *tls_session)
{
    char       *protocol = 0;
    const char *protocol_name;
    size_t      protocol_name_length;

    assert(tls_session->pn_raw);
    if (pn_tls_get_alpn_protocol(tls_session->pn_raw, &protocol_name, &protocol_name_length)) {
        protocol = (char *) qd_calloc(protocol_name_length + 1, sizeof(char));
        memmove(protocol, protocol_name, protocol_name_length);
        protocol[protocol_name_length] = '\0';
    }
    return protocol;
}


char *qd_tls_session_get_protocol_version(const qd_tls_session_t *tls_session)
{
    char  *version = 0;
    size_t version_len;

    if (tls_session->pn_raw) {
        const char *protocol_version;
        if (pn_tls_get_protocol_version(tls_session->pn_raw, &protocol_version, &version_len)) {
            version = (char *) qd_calloc(version_len + 1, sizeof(char));
            memmove(version, protocol_version, version_len);
            version[version_len] = '\0';
        }
    } else {
        version_len = 50;  // 50 was used in the old code, so whatever...
        version = (char *) qd_calloc(version_len, sizeof(char));
        if (!pn_ssl_get_protocol_name(tls_session->pn_amqp, version, version_len)) {
            free(version);
            version = 0;
        }
    }
    return version;
}

char *qd_tls_session_get_protocol_ciphers(const qd_tls_session_t *tls_session)
{
    char   *ciphers = 0;
    size_t  ciphers_len;

    if (tls_session->pn_raw) {
        const char *protocol_ciphers;
        if (pn_tls_get_cipher(tls_session->pn_raw, &protocol_ciphers, &ciphers_len)) {
            ciphers = (char *) qd_calloc(ciphers_len + 1, sizeof(char));
            memmove(ciphers, protocol_ciphers, ciphers_len);
            ciphers[ciphers_len] = '\0';
        }
    } else {
        ciphers_len = 50;  // 50 was used in the old code, so whatever...
        ciphers = (char *) qd_calloc(ciphers_len, sizeof(char));
        if (!pn_ssl_get_cipher_name(tls_session->pn_amqp, ciphers, ciphers_len)) {
            free(ciphers);
            ciphers = 0;
        }
    }
    return ciphers;
}


int qd_tls_session_get_ssf(const qd_tls_session_t *tls_session)
{
    if (tls_session->pn_raw) {
        return pn_tls_get_ssf(tls_session->pn_raw);
    } else {
        return pn_ssl_get_ssf(tls_session->pn_amqp);
    }
}


uint64_t qd_tls_session_get_ssl_profile_ordinal(const qd_tls_session_t *session)
{
    if (session)
        return session->ordinal;
    return 0;
}


qd_ssl2_profile_t *qd_tls_read_ssl_profile(const char *ssl_profile_name, qd_ssl2_profile_t *profile)
{
    ASSERT_MGMT_THREAD;

    qd_tls_context_t *tls_context = _find_tls_context(ssl_profile_name);
    if (!tls_context) {
        ZERO(profile);
        return 0;
    }

    profile->ciphers                = CHECKED_STRDUP(tls_context->profile.ciphers);
    profile->protocols              = CHECKED_STRDUP(tls_context->profile.protocols);
    profile->password               = CHECKED_STRDUP(tls_context->profile.password);
    profile->uid_format             = CHECKED_STRDUP(tls_context->profile.uid_format);
    profile->certificate_file       = CHECKED_STRDUP(tls_context->profile.certificate_file);
    profile->private_key_file       = CHECKED_STRDUP(tls_context->profile.private_key_file);
    profile->uid_name_mapping_file  = CHECKED_STRDUP(tls_context->profile.uid_name_mapping_file);
    profile->trusted_certificate_db = CHECKED_STRDUP(tls_context->profile.trusted_certificate_db);
    profile->ordinal                = tls_context->profile.ordinal;
    profile->oldest_valid_ordinal   = tls_context->profile.oldest_valid_ordinal;

    return profile;
}


void qd_tls_cleanup_ssl_profile(qd_ssl2_profile_t *profile)
{
    if (profile) {
        free(profile->ciphers);
        free(profile->protocols);
        free(profile->trusted_certificate_db);
        free(profile->certificate_file);
        free(profile->private_key_file);
        free(profile->password);
        free(profile->uid_format);
        free(profile->uid_name_mapping_file);
        ZERO(profile);
    }
}


/**
 * Read the sslProfile configuration record from entity and copy it into config
 */
static qd_error_t _read_tls_profile(qd_entity_t *entity, qd_ssl2_profile_t *profile)
{
    ZERO(profile);

    long ordinal;
    long oldest_valid_ordinal;
    char *name = 0;
    name = qd_entity_opt_string(entity, "name", "<NONE>");
    if (qd_error_code()) goto error;

    profile->ciphers                = qd_entity_opt_string(entity, "ciphers", 0);
    if (qd_error_code()) goto error;
    profile->protocols              = qd_entity_opt_string(entity, "protocols", 0);
    if (qd_error_code()) goto error;
    profile->trusted_certificate_db = qd_entity_opt_string(entity, "caCertFile", 0);
    if (qd_error_code()) goto error;
    profile->certificate_file       = qd_entity_opt_string(entity, "certFile", 0);
    if (qd_error_code()) goto error;
    profile->private_key_file       = qd_entity_opt_string(entity, "privateKeyFile", 0);
    if (qd_error_code()) goto error;
    profile->password               = qd_entity_opt_string(entity, "password", 0);
    if (qd_error_code()) goto error;
    profile->uid_format             = qd_entity_opt_string(entity, "uidFormat", 0);
    if (qd_error_code()) goto error;
    profile->uid_name_mapping_file      = qd_entity_opt_string(entity, "uidNameMappingFile", 0);
    if (qd_error_code()) goto error;
    ordinal                             = qd_entity_opt_long(entity, "ordinal", 0);
    if (qd_error_code()) goto error;
    oldest_valid_ordinal                = qd_entity_opt_long(entity, "oldestValidOrdinal", 0);
    if (qd_error_code()) goto error;

    if (profile->uid_format) {
        if (!tls_private_validate_uid_format(profile->uid_format)) {
            // backward compatibility: this isn't treated as a hard error - the fallback behavior is to use the user
            // name from the transport. I have no idea why that is the case but changing it to a hard error results in
            // CI test failures so for now I go along to get along:
            qd_log(LOG_AGENT, QD_LOG_ERROR, "Invalid format for uidFormat field in sslProfile '%s': %s",
                   name, profile->uid_format);
            free(profile->uid_format);
            profile->uid_format = 0;
        }
    }

    if (profile->password) {
        //
        // Process the password to handle any modifications or lookups needed
        //
        char *actual_pass = 0;
        bool is_file_path = 0;
        qd_server_config_process_password(&actual_pass, profile->password, &is_file_path, true);
        if (qd_error_code()) goto error;

        if (actual_pass) {
            if (is_file_path) {
                qd_set_password_from_file(actual_pass, &profile->password);
                free(actual_pass);
            }
            else {
                free(profile->password);
                profile->password = actual_pass;
            }
        }
    }

    // simple validation of ordinal fields:
    if (ordinal < 0 || oldest_valid_ordinal < 0) {
        qd_error(QD_ERROR_CONFIG, "Negative ordinal field values are invalid (sslProfile '%s')", name);
        goto error;
    }
    if (ordinal < oldest_valid_ordinal) {
        qd_error(QD_ERROR_CONFIG, "ordinal must be >= oldestValidOrdinal (sslProfile '%s')", name);
        goto error;
    }

    profile->ordinal = (uint64_t) ordinal;
    profile->oldest_valid_ordinal = (uint64_t) oldest_valid_ordinal;

    free(name);
    return QD_ERROR_NONE;

error:
    free(name);
    _cleanup_tls_profile(profile);
    return qd_error_code();
}


/** Release the contents of a configuration instance
 */
static void _cleanup_tls_profile(qd_ssl2_profile_t *profile)
{
    free(profile->ciphers);
    free(profile->protocols);
    free(profile->trusted_certificate_db);
    free(profile->certificate_file);
    free(profile->private_key_file);
    free(profile->password);
    free(profile->uid_format);
    free(profile->uid_name_mapping_file);
    ZERO(profile);
}


/** Instantiate a new Proton Raw TLS config
 */


/**
 * Reload the sslProfile configuration for the given tls_config
 *
 * Note: this function blocks the caller while loading certificate files from the filesystem. It is *SLOW* - assume it
 * will block for at least hundreds of milliseconds!
 */
static qd_error_t _update_tls_config(qd_tls_config_t *tls_config, const qd_ssl2_profile_t *profile)
{
    ASSERT_MGMT_THREAD;  // runs too slow to block an I/O thread (see ISSUE-1572)

    pn_tls_config_t *pn_raw_config = 0;
    pn_ssl_domain_t *pn_amqp_config = 0;

    if (_validate_config(profile, tls_config->ssl_profile_name, tls_config->is_listener, tls_config->authenticate_peer) != QD_ERROR_NONE) {
        return qd_error_code();  // validate_config sets qd_error()
    }

    //
    // Generate a new proton configuration using the updated configuration from the parent tls_context and the existing
    // tls_config.  Do not hold the tls_config lock because loading certs takes a loooong time and we do not want to block
    // new connections.
    //

    switch (tls_config->p_type) {
        case QD_TLS_TYPE_PROTON_AMQP:
            pn_amqp_config = tls_private_allocate_amqp_config(tls_config->ssl_profile_name, profile, tls_config->is_listener,
                                                              tls_config->verify_hostname, tls_config->authenticate_peer);
            if (!pn_amqp_config) {
                return qd_error_code();  // allocation function set qd_error()
            }
            break;
        case QD_TLS_TYPE_PROTON_RAW:
            pn_raw_config = tls_private_allocate_raw_config(tls_config->ssl_profile_name, profile, tls_config->is_listener,
                                                            tls_config->verify_hostname, tls_config->authenticate_peer);
            if (!pn_raw_config) {
                return qd_error_code();  // allocation function set qd_error()
            }
            break;
        default:
            assert(false);
            break;
    }

    qd_proton_config_t *new_cfg = qd_proton_config(pn_raw_config, pn_amqp_config);
    qd_proton_config_t *old_cfg = 0;

    // Pull the old switcheroo on the old proton configuration.  Do this under lock to prevent I/O threads from creating
    // new sessions while we change pointers.

    sys_mutex_lock(&tls_config->lock);
    old_cfg                    = tls_config->proton_tls_cfg;
    tls_config->proton_tls_cfg = new_cfg;

    // And refresh any parameters that must be used when creating new sessions:
    tls_config->ordinal              = profile->ordinal;
    tls_config->oldest_valid_ordinal = profile->oldest_valid_ordinal;
    free(tls_config->uid_format);
    tls_config->uid_format = CHECKED_STRDUP(profile->uid_format);

    // Calling the callback under lock ensures that no new TLS sessions can be created using the updated
    // configuration until after the callback is run.
    if (tls_config->update_callback) {
        tls_config->update_callback(tls_config, tls_config->update_context);
    }
    sys_mutex_unlock(&tls_config->lock);


    // no need to hold the lock here because the decref is atomic and if this
    // is the last reference then by definition there are no other threads involved.
    qd_proton_config_decref(old_cfg);

    return QD_ERROR_NONE;
}


/** Find the TLS context associated with the given sslProfile name
 */
static qd_tls_context_t *_find_tls_context(const char *profile_name)
{
    ASSERT_MGMT_THREAD;

    qd_tls_context_t *ctxt = DEQ_HEAD(context_list);
    while (ctxt) {
        if (strcmp(ctxt->ssl_profile_name, profile_name) == 0)
            return ctxt;
        ctxt = DEQ_NEXT(ctxt);
    }
    return 0;
}


/** Free the TLS context. Assumes context is no longer on context_list
 */
static void _tls_context_free(qd_tls_context_t *ctxt)
{
    if (ctxt) {
        qd_tls_config_t *tls_config = DEQ_HEAD(ctxt->tls_configs);
        while (tls_config) {
            DEQ_REMOVE_HEAD(ctxt->tls_configs);
            qd_tls_config_decref(tls_config);
            tls_config = DEQ_HEAD(ctxt->tls_configs);
        }
        free(ctxt->ssl_profile_name);
        _cleanup_tls_profile(&ctxt->profile);
        free_qd_tls_context_t(ctxt);
    }
}


/***
 * Basic sanity checking that the sslProfile is valid
 */
static qd_error_t _validate_config(const qd_ssl2_profile_t *profile, const char *ssl_profile_name, bool is_listener, bool authenticate_peer)
{
    if (is_listener) {
        // self identifying certificate is required for a listener:
        if (!profile->certificate_file) {
            qd_error(QD_ERROR_CONFIG, "Listener requires a self-identifying certificate (sslProfile: %s)", ssl_profile_name);
            return QD_ERROR_CONFIG;
        }
        if (authenticate_peer) {
            if (!profile->trusted_certificate_db) {
                qd_error(QD_ERROR_CONFIG, "Listener requires a CA for peer authentication (sslProfile: %s)", ssl_profile_name);
                return QD_ERROR_CONFIG;
            }
        }
    } else if (!profile->trusted_certificate_db) {
        // CA must be provided for a connector:
        qd_error(QD_ERROR_CONFIG, "Connector requires a CA certificate (sslProfile: %s)", ssl_profile_name);
        return QD_ERROR_CONFIG;
    }

    if (profile->certificate_file && !profile->private_key_file) {
        // missing private key file
        qd_error(QD_ERROR_CONFIG, "Missing Private Keyfile (sslProfile: %s)", ssl_profile_name);
        return QD_ERROR_CONFIG;
    }

    return QD_ERROR_NONE;
}

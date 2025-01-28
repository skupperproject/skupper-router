#ifndef __tls_common_h__
#define __tls_common_h__ 1
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

/**@file
 * Management of TLS configuration and state
 */


#include "qpid/dispatch/log.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>


typedef struct qd_tls_config_t   qd_tls_config_t;    // run-time TLS configuration state
typedef struct qd_tls_session_t  qd_tls_session_t;  // per connection TLS state
typedef struct qd_ssl2_profile_t qd_ssl2_profile_t;  // sslProfile configuration record

// Proton has two different TLS implementations: one for AMQP and a buffer-based one for use with Raw Connections:
typedef enum {
    QD_TLS_TYPE_NONE = 0,     // unset
    QD_TLS_TYPE_PROTON_AMQP,  // for use with AMQP transport
    QD_TLS_TYPE_PROTON_RAW,   // use raw connection/qd_buffer_t interface
} qd_tls_type_t;

typedef enum {
    QD_TLS_CONFIG_MODE_NONE = 0,  // unset
    QD_TLS_CONFIG_SERVER_MODE,    // Operate as a TLS server (i.e. listener socket)
    QD_TLS_CONFIG_CLIENT_MODE,    // Operate as an TLS client (i.e. outgoing connections)
} qd_tls_config_mode_t;

// sslProfile configuration record
struct qd_ssl2_profile_t {
    char *ciphers;
    char *protocols;
    char *trusted_certificate_db;
    char *certificate_file;
    char *private_key_file;
    char *password;

    /**
     * Holds the list of component fields of the client certificate from which a unique identifier is constructed.  For
     * e.g, this field could have the format of 'cou' indicating that the uid will consist of c - common name
     * concatenated with o - organization-company name concatenated with u - organization unit
     *
     * Allowed values can be any combination of the comma separated codes (no duplicates):
     * 'c'( ISO3166 two character country code),
     * 's'(state or province),
     * 'l'(Locality; generally - city),
     * 'o'(Organization - Company Name),
     * 'u'(Organization Unit - typically certificate type or brand),
     * 'n'(CommonName - typically a user name for client certificates)
     *
     * and one of the following:
     * '1'(sha1 certificate fingerprint, the fingerprint, as displayed in the fingerprints section when looking at a certificate
     *  with say a web browser is the hash of the entire certificate in DER form)
     * '2'(sha256 certificate fingerprint)
     * '5'(sha512 certificate fingerprint)
     */
    char *uid_format;

    /**
     * Full path to the file that contains the uid to display name mapping.
     */
    char *uid_name_mapping_file;

    /**
     * Certificate Rotation
     * ordinal: Identifies the current configuration's revision
     * oldest_valid_ordinal: Previous configurations with ordinal values < oldest_valid_ordinal have expired.
     */
    uint64_t ordinal;
    uint64_t oldest_valid_ordinal;
};

/**
 * Create a new TLS qd_tls_config_t instance with the given configuration. This is called when a listener/connector
 * record is created.
 *
 * @param ssl_profile_name the name of the sslProfile configuration to use
 * @param p_type protocol type for the child connections (TCP or AMQP)
 * @param mode the operational use case (TLS Server or Client)
 * @param verify_hostname enforce host name checking (Client mode)
 * @param authenticate_peer validate peer's certificate (Server mode)
 *
 * @return a new qd_tls_config_t instance or 0 on error. qd_error() set if error.
 */
qd_tls_config_t *qd_tls_config(const char *ssl_profile_name,
                               qd_tls_type_t p_type,
                               qd_tls_config_mode_t mode,
                               bool verify_hostname,
                               bool authenticate_peer);


/**
 * Release a reference to the qd_tls_config_t
 *
 * @param config to be released. The config pointer must no longer be referenced
 */
void qd_tls_config_decref(qd_tls_config_t *config);


/**
 * Get the values of the ordinal/oldestValidOrdinal assocated with the TLS configuration.
 *
 * Note: To avoid races this function can only be called from the context of the management thread.
 *
 * @param config The qd_tls_config_t to query.
 * @return the value for the ordinal/lastValidOrdinal sslProfile attributes used by this config
 */
uint64_t qd_tls_config_get_ordinal(const qd_tls_config_t *config);
uint64_t qd_tls_config_get_oldest_valid_ordinal(const qd_tls_config_t *config);


/** Register a callback to monitor updates to the TLS configuration
 *
 * Register a callback function that will be invoked by the management thread whenever the sslProfile record associated
 * with the qd_tls_config_t is updated.  Note that the update callback is invoked on the management thread while the
 * qd_tls_config is locked. This prevents new TLS sessions from being created using the updated configuration until
 * after the update callback returns.
 *
 * @param update_cb_context Opaque handle passed to update callback function.
 * @param update_cb Optional callback when the sslProfile has been updated by management.
 */
typedef void (*qd_tls_config_update_cb_t)(const qd_tls_config_t *config,
                                          void *update_cb_context);
void qd_tls_config_register_update_callback(qd_tls_config_t *config, void *update_cb_context,
                                            qd_tls_config_update_cb_t update_cb);


/**
 * Cancel the update callback.
 *
 * Deregisters the update callback provided in qd_tls_config(). No further calls to the callback will occur on return
 * from this call. Can only be called from the context of the management thread.
 *
 * @param config The qd_tls_config_t whose callback will be cancelled
 */
void qd_tls_config_cancel_update_callback(qd_tls_config_t *config);


/**
 * Release a TLS session context.
 *
 * See the session constructor API in tls_amqp.h and tls_raw.h
 *
 * @param session the session to free. It must no longer be referenced after this call.
 */
void qd_tls_session_free(qd_tls_session_t *session);


/**
 * Get the version of TLS in use by the session.
 *
 * @param session to be queried.
 * @return Null terminated string containing the TLS version description. Returned string buffer must be free()d by
 * caller. Return 0 if version not known.
 */
char *qd_tls_session_get_protocol_version(const qd_tls_session_t *session);

/**
 * Get the cipher in use by the session.
 *
 * @param session to be queried.
 * @return Null terminated string containing a description of the active cipher. Returned string buffer must be free()d
 * by caller. Return 0 if version not known.
 */
char *qd_tls_session_get_protocol_ciphers(const qd_tls_session_t *session);

/**
 * Get the Security Strength Factor (SSF) of the Cipher in use by the session
 *
 * @param session to be queried.
 * @return the SSF value of the session
 */
int qd_tls_session_get_ssf(const qd_tls_session_t *session);

/**
 * Get the ordinal of the sslProfile used to create this session
 *
 * @param session to be queried
 * @return the value of the sslProfile ordinal associated with this session.
 */
uint64_t qd_tls_session_get_profile_ordinal(const qd_tls_session_t *session);

/**
 * Fill out the given *profile with the configuration from the named sslProfile record.
 *
 * @param the name of the sslProfile
 * @param a pointer to an uninitialized qd_ssl2_profile_t instance.
 * @return a pointer to the passed in qd_ssl2_profile_t on success else 0. Use qd_tls_cleanup_ssl_profile() release
 * resources in use by *profile when done.
 */
qd_ssl2_profile_t *qd_tls_read_ssl_profile(const char *ssl_profile_name, qd_ssl2_profile_t *profile);

/**
 * Release any resources allocated by qd_tls_get_ssl_profile() and reset the profile.
 *
 * @param a pointer to an qd_ssl2_profile_t instance initialized by qd_tls_read_ssl_profile().
 *
 * Note this only releases internal resources associated with the profile, the memory pointed to by *profile is owned
 * by the caller.
 */
void qd_tls_cleanup_ssl_profile(qd_ssl2_profile_t *profile);


// Module initialization/finalization
void qd_tls_initialize(void);
void qd_tls_finalize(void);

#endif


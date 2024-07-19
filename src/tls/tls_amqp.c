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

#include "qpid/dispatch/tls_amqp.h"
#include "private.h"

#include "qpid/dispatch/error.h"

#include "proton/ssl.h"
#include "proton/transport.h"


// Allowed sslProfile uidFormat fields.
//
static const char CERT_COUNTRY_CODE = 'c';
static const char CERT_STATE = 's';
static const char CERT_CITY_LOCALITY = 'l';
static const char CERT_ORGANIZATION_NAME = 'o';
static const char CERT_ORGANIZATION_UNIT = 'u';
static const char CERT_COMMON_NAME = 'n';
static const char CERT_FINGERPRINT_SHA1 = '1';
static const char CERT_FINGERPRINT_SHA256 = '2';
static const char CERT_FINGERPRINT_SHA512 = '5';

static const char *valid_uid_fields = "csloun125";  // for validation

static const char *COMPONENT_SEPARATOR = ";";
#define UID_FORMAT_MAX_LEN 7


bool tls_private_validate_uid_format(const char *format)
{
    // valid format allows any combination of the uidFormat characters
    // "csloun" and any one of [125]

    if (strlen(format) > UID_FORMAT_MAX_LEN) {
        return false;
    }

    int fingerprints = 0;
    for (const char *ptr = format; *ptr; ++ptr) {
        if (strchr(valid_uid_fields, *ptr) == NULL) {
            return false;  // bad format specifier
        }
        if (*ptr == CERT_FINGERPRINT_SHA1 ||
            *ptr == CERT_FINGERPRINT_SHA256 ||
            *ptr == CERT_FINGERPRINT_SHA512) {
            if (++fingerprints > 1) {
                return false;  // only one fingerprint allowed
            }
        }
    }

    return true;
}


// Construct a user id from the various certificate subfields/fingerprints as specified in the uid_format string. See
// the sslProfile description in the schema file.
//
static char *_get_uid_name(pn_ssl_t *pn_session, const char *uid_format)
{
    assert(uid_format);

    const char *country_code = 0;
    const char *state = 0;
    const char *locality_city = 0;
    const char *organization = 0;
    const char *org_unit = 0;
    const char *common_name = 0;

    //
    // SHA1 is 20 octets (40 hex characters); SHA256 is 32 octets (64 hex characters).
    // SHA512 is 64 octets (128 hex characters)
    //
    char fingerprint[129] = "\0";

    // accumulate the length into uid_length on each pass so we definitively know the number of octets to malloc.
    int uid_length = 0;
    int semi_colon_count = -1;

    char *user_id = 0;

    for (const char *component = uid_format; *component; ++component) {
        if (*component == CERT_COUNTRY_CODE) {
            country_code = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_COUNTRY_NAME);
            if (country_code) {
                uid_length += strlen((const char *)country_code);
                semi_colon_count++;
            }
        } else if (*component == CERT_STATE) {
            state = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_STATE_OR_PROVINCE);
            if (state) {
                uid_length += strlen((const char *)state);
                semi_colon_count++;
            }
        } else if (*component == CERT_CITY_LOCALITY) {
            locality_city = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_CITY_OR_LOCALITY);
            if (locality_city) {
                uid_length += strlen((const char *)locality_city);
                semi_colon_count++;
            }
        } else if (*component == CERT_ORGANIZATION_NAME) {
            organization = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_ORGANIZATION_NAME);
            if (organization) {
                uid_length += strlen((const char *)organization);
                semi_colon_count++;
            }
        } else if (*component == CERT_ORGANIZATION_UNIT) {
            org_unit = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_ORGANIZATION_UNIT);
            if (org_unit) {
                uid_length += strlen((const char *)org_unit);
                semi_colon_count++;
            }
        } else if (*component == CERT_COMMON_NAME) {
            common_name = pn_ssl_get_remote_subject_subfield(pn_session, PN_SSL_CERT_SUBJECT_COMMON_NAME);
            if (common_name) {
                uid_length += strlen((const char *)common_name);
                semi_colon_count++;
            }
        } else if (*component == CERT_FINGERPRINT_SHA1 || *component == CERT_FINGERPRINT_SHA256 || *component == CERT_FINGERPRINT_SHA512) {
            // Allocate the memory for message digest
            int out = 0;

            int fingerprint_length = 0;
            if (*component == CERT_FINGERPRINT_SHA1) {
                fingerprint_length = 40;
                out = pn_ssl_get_cert_fingerprint(pn_session, fingerprint, fingerprint_length + 1, PN_SSL_SHA1);
            }
            else if (*component == CERT_FINGERPRINT_SHA256) {
                fingerprint_length = 64;
                out = pn_ssl_get_cert_fingerprint(pn_session, fingerprint, fingerprint_length + 1, PN_SSL_SHA256);
            }
            else if (*component == CERT_FINGERPRINT_SHA512) {
                fingerprint_length = 128;
                out = pn_ssl_get_cert_fingerprint(pn_session, fingerprint, fingerprint_length + 1, PN_SSL_SHA512);
            }
            (void) out;  // avoid 'out unused' compiler warnings if NDEBUG undef'ed
            assert (out == 0);

            uid_length += fingerprint_length;
            semi_colon_count++;
        } else {
            assert(false);  // should not be hit since uid_format has been validated
        }
    }

    if (uid_length > 0) {
        user_id = (char *) qd_malloc((uid_length + semi_colon_count + 1) * sizeof(char)); // the +1 is for the '\0' character
        *user_id = '\0';
        // The components in the user id string must appear in the same order as it appears in the component string. that is
        // why we have this loop

        for (const char *component = uid_format; *component; ++component) {
            if (*component == CERT_COUNTRY_CODE) {
                if (country_code) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) country_code);
                }
            } else if (*component == CERT_STATE) {
                if (state) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) state);
                }
            } else if (*component == CERT_CITY_LOCALITY) {
                if (locality_city) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) locality_city);
                }
            } else if (*component == CERT_ORGANIZATION_NAME) {
                if (organization) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) organization);
                }
            } else if (*component == CERT_ORGANIZATION_UNIT) {
                if (org_unit) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) org_unit);
                }
            } else if (*component == CERT_COMMON_NAME) {
                if (common_name) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) common_name);
                }
            } else {  // fingerprints
                if (strlen((char *) fingerprint) > 0) {
                    if (*user_id != '\0')
                        strcat(user_id, COMPONENT_SEPARATOR);
                    strcat(user_id, (char *) fingerprint);
                }
            }
        }
    }

    return user_id;
}


char *qd_tls_session_get_user_id(qd_tls_session_t *session)
{
    // only valid for Proton AMQP TLS sessions
    assert(session->pn_amqp);

    char *uid = 0;
    if (session->uid_format) {
        uid = _get_uid_name(session->pn_amqp, session->uid_format);
        if (uid) {
            // Check for display name override
            char *override = tls_private_lookup_display_name(session->ssl_profile_name, uid);
            if (override) {
                free(uid);
                uid = override;
            }
        }
    }

    return uid;
}


/**
 * Allocate a Proton AMQP TLS configuration.
 *
 * @param ssl_profile_name name of the sslProfile configuration record to use
 * @param profile the sslProfile configuration values
 * @param is_listener true if the configuration is to be used with an AMQP listener, false for a connector
 * @param verify_hostname if true verify the hostname returned in the peer's certificate (connector only).
 * @param authenticate_peer if true verify the peer's certificate (listener only).
 * @return a pointer to the new pn_ssl_domain_t on success, zero on failure. qd_error() is set on error.
 */
pn_ssl_domain_t *tls_private_allocate_amqp_config(const char *ssl_profile_name, const qd_ssl2_profile_t *profile,
                                                  bool is_listener, bool verify_hostname, bool authenticate_peer)
{
    pn_ssl_domain_t *domain = 0;

    qd_error_clear();

    do {
        domain = pn_ssl_domain(is_listener ? PN_SSL_MODE_SERVER : PN_SSL_MODE_CLIENT);
        if (!domain) {
            qd_error(QD_ERROR_CONFIG, "Failed to create TLS domain from sslProfile '%s' (TLS not available)",
                     ssl_profile_name);
            break;
        }

        //
        // Configure the CA certificate for verifying the peer:
        //
        if (profile->trusted_certificate_db) {
            if (pn_ssl_domain_set_trusted_ca_db(domain, profile->trusted_certificate_db)) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS caCertFile '%s' from sslProfile '%s'",
                         profile->trusted_certificate_db, ssl_profile_name);
                break;
            }
        }

        //
        // Configure my self-identifying cert:
        //

        if (profile->certificate_file) {
            if (pn_ssl_domain_set_credentials(domain,
                                              profile->certificate_file,
                                              profile->private_key_file,
                                              profile->password)) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS certFile '%s' from sslProfile '%s'",
                         profile->certificate_file, ssl_profile_name);
                break;
            }
        }

        //
        // Configure the peer verification mode:
        //

        int rc = 0;
        if (is_listener) {
            // do we force the peer to send a cert?
            if (authenticate_peer) {
                assert(profile->trusted_certificate_db);
                rc = pn_ssl_domain_set_peer_authentication(domain, PN_SSL_VERIFY_PEER, profile->trusted_certificate_db);
            } else {
                rc = pn_ssl_domain_set_peer_authentication(domain, PN_SSL_ANONYMOUS_PEER, 0);
            }
            if (rc) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS peer authentication for sslProfile '%s'", ssl_profile_name);
                break;
            }
        } else {  // Connector
            if (verify_hostname) {
                rc = pn_ssl_domain_set_peer_authentication(domain,
                                                           PN_SSL_VERIFY_PEER_NAME,
                                                           profile->trusted_certificate_db);
            } else {  // verify cert but ignore hostname
                rc = pn_ssl_domain_set_peer_authentication(domain,
                                                           PN_SSL_VERIFY_PEER,
                                                           profile->trusted_certificate_db);
            }
            if (rc) {
                qd_error(QD_ERROR_CONFIG, "Failed to configure TLS peer hostname verification for sslProfile '%s'", ssl_profile_name);
                break;
            }
        }

        if (profile->protocols) {
            if (pn_ssl_domain_set_protocols(domain, profile->protocols)) {
                qd_error(QD_ERROR_CONFIG,
                         "Failed to configure TLS Protocols '%s' for sslProfile '%s')",
                         profile->protocols, ssl_profile_name);
                break;
            }
        }

        if (profile->ciphers) {
            if (pn_ssl_domain_set_ciphers(domain, profile->ciphers)) {
                qd_error(QD_ERROR_CONFIG,
                         "Failed to configure TLS Ciphers '%s' for sslProfile '%s'. Use openssl ciphers -v <ciphers> to validate",
                         profile->ciphers, ssl_profile_name);
                break;
            }
        }

        return domain;

    } while (0);

    // If we get here, the configuration setup failed

    if (domain) {
        pn_ssl_domain_free(domain);
    }
    return 0;
}


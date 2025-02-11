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

/*
 * Configuration record for listener and connector entities
 */
#include "container.h"
#include "server_config.h"
#include "dispatch_private.h"
#include "entity.h"

#include <qpid/dispatch/amqp_adaptor.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/tls_common.h>

#include <proton/codec.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>


// FIXME: I cannot justify this - never call goto from a MACRO!
#define CHECK() if (qd_error_code()) goto error
#define CHECKED_STRDUP(S) ((S) ? qd_strdup(S) : NULL)


/**
 * Private function to set the values of booleans strip_inbound_annotations and strip_outbound_annotations
 * based on the corresponding values for the settings in skrouter.json
 * strip_inbound_annotations and strip_outbound_annotations are defaulted to true
 */
static void load_strip_annotations(qd_server_config_t *config, const char* stripAnnotations)
{
    if (stripAnnotations) {
    	if      (strcmp(stripAnnotations, "both") == 0) {
    		config->strip_inbound_annotations  = true;
    		config->strip_outbound_annotations = true;
    	}
    	else if (strcmp(stripAnnotations, "in") == 0) {
    		config->strip_inbound_annotations  = true;
    		config->strip_outbound_annotations = false;
    	}
    	else if (strcmp(stripAnnotations, "out") == 0) {
    		config->strip_inbound_annotations  = false;
    		config->strip_outbound_annotations = true;
    	}
    	else if (strcmp(stripAnnotations, "no") == 0) {
    		config->strip_inbound_annotations  = false;
    		config->strip_outbound_annotations = false;
    	}
    }
    else {
    	assert(stripAnnotations);
    	//This is just for safety. Default to stripInboundAnnotations and stripOutboundAnnotations to true (to "both").
		config->strip_inbound_annotations  = true;
		config->strip_outbound_annotations = true;
    }
}


/**
 * Since both the host and the addr have defaults of 127.0.0.1, we will have to use the non-default wherever it is
 * available.
 */
static void set_config_host(qd_server_config_t *config, qd_entity_t* entity)
{
    config->host = qd_entity_opt_string(entity, "host", 0);

    assert(config->host);

    int hplen = strlen(config->host) + strlen(config->port) + 2;
    config->host_port = malloc(hplen);
    snprintf(config->host_port, hplen, "%s:%s", config->host, config->port);
}


/**
 * Initialize the server_config from the listener or connector management entity instance
 */
qd_error_t qd_server_config_load(qd_server_config_t *config, qd_entity_t *entity, bool is_listener)
{
    qd_error_clear();

    bool authenticatePeer   = qd_entity_opt_bool(entity, "authenticatePeer",  false);    CHECK();
    bool verifyHostname     = qd_entity_opt_bool(entity, "verifyHostname",    true);     CHECK();
    bool requireEncryption  = qd_entity_opt_bool(entity, "requireEncryption", false);    CHECK();
    bool requireSsl         = qd_entity_opt_bool(entity, "requireSsl",        false);    CHECK();

    ZERO(config);
    config->log_message          = qd_entity_opt_string(entity, "messageLoggingComponents", 0);     CHECK();
    config->message_log_flags    = qd_message_repr_flags(config->log_message);
    config->port                 = qd_entity_get_string(entity, "port");              CHECK();
    config->name                 = qd_entity_opt_string(entity, "name", 0);           CHECK();
    config->role                 = qd_entity_get_string(entity, "role");              CHECK();
    long inter_router_cost       = qd_entity_opt_long(entity, "cost", 1);             CHECK();

    //
    // The cost field on the listener or the connector should be > 0 and <= INT32_MAX
    // The router will terminate on invalid cost values.
    //
    if (inter_router_cost <= 0 || inter_router_cost > INT32_MAX) {
        return qd_error(QD_ERROR_CONFIG,
                        "Invalid cost (%li) specified. Minimum value for cost is 1 and maximum value is %i",
                        inter_router_cost, INT32_MAX);
    }

    config->inter_router_cost = inter_router_cost;
    config->socket_address_family      = qd_entity_opt_string(entity, "socketAddressFamily", 0); CHECK();
    config->healthz              = qd_entity_opt_bool(entity, "healthz", true);       CHECK();
    config->metrics              = qd_entity_opt_bool(entity, "metrics", true);       CHECK();
    config->websockets           = qd_entity_opt_bool(entity, "websockets", true);    CHECK();
    config->http                 = qd_entity_opt_bool(entity, "http", false);         CHECK();
    config->http_root_dir        = qd_entity_opt_string(entity, "httpRootDir", 0);    CHECK();
    config->http = config->http || config->http_root_dir; /* httpRootDir implies http */
    config->idle_timeout_seconds = qd_entity_get_long(entity, "idleTimeoutSeconds");  CHECK();
    if (is_listener) {
        config->initial_handshake_timeout_seconds = qd_entity_get_long(entity, "initialHandshakeTimeoutSeconds");  CHECK();
    }
    config->sasl_username        = qd_entity_opt_string(entity, "saslUsername", 0);   CHECK();
    config->sasl_password        = qd_entity_opt_string(entity, "saslPassword", 0);   CHECK();
    config->sasl_mechanisms      = qd_entity_opt_string(entity, "saslMechanisms", 0); CHECK();
    config->ssl_profile_name     = qd_entity_opt_string(entity, "sslProfile", 0);     CHECK();
    config->link_capacity        = qd_entity_opt_long(entity, "linkCapacity", 0);     CHECK();
    config->multi_tenant         = qd_entity_opt_bool(entity, "multiTenant", false);  CHECK();
    config->policy_vhost         = qd_entity_opt_string(entity, "policyVhost", 0);    CHECK();
    config->conn_props           = qd_entity_opt_map(entity, "openProperties");       CHECK();

    set_config_host(config, entity);

    if (config->sasl_password) {
        //
        //Process the sasl password field and set the right values based on prefixes.
        //
        char *actual_pass = 0;
        bool is_file_path = 0;
        qd_server_config_process_password(&actual_pass, config->sasl_password, &is_file_path, false);
        if (actual_pass) {
            if (is_file_path) {
                   qd_set_password_from_file(actual_pass, &config->sasl_password);
                   free(actual_pass);
            }
            else {
                free(config->sasl_password);
                config->sasl_password = actual_pass;
            }
        }
    }

    //
    // Handle the defaults for various settings
    //
    if (config->link_capacity == 0)
        config->link_capacity = 250;

    // Proton does not support maxSessions > 32768
    int64_t value = (int64_t) qd_entity_get_long(entity, "maxSessions"); CHECK();
    if (value == 0) {
        value = 32768;  // default
    } else if (value < 0 || value > 32768) {
        (void) qd_error(QD_ERROR_CONFIG,
                        "Invalid maxSessions specified (%"PRId64"). Minimum value is 1 and maximum value is %i",
                        value, 32768);
        goto error;
    }
    config->max_sessions = (uint32_t) value;

    // Ensure maxFrameSize is at least the minimum value required by the standard,
    // and it does not exceed the proton APIs max of INT32_MAX
    value = (int64_t) qd_entity_get_long(entity, "maxFrameSize"); CHECK();
    if (value == 0) {
        value = 16384; // default
    } else if (value < QD_AMQP_MIN_MAX_FRAME_SIZE || value > INT32_MAX) {
        (void) qd_error(QD_ERROR_CONFIG,
                        "Invalid maxFrameSize specified (%"PRId64"). Minimum value is %d and maximum value is %"PRIi32,
                        value, QD_AMQP_MIN_MAX_FRAME_SIZE, INT32_MAX);
        goto error;
    }
    config->max_frame_size = (uint32_t) value;

    // Ensure that maxSessionFrames does not exceed the proton APIs max of INT32_MAX
    value = (int64_t) qd_entity_opt_long(entity, "maxSessionFrames", 0); CHECK();
    if (value == 0) {
        // Use a sane default. Allow router to router links more capacity than AMQP application links
        if (strcmp(config->role, "normal") == 0) {
            value = qd_session_incoming_window_normal / config->max_frame_size;
        } else {
            value = qd_session_incoming_window_router / config->max_frame_size;
        }
        // Ensure the window is at least 2 frames to allow a non-zero low water mark
        value = MAX(value, 2);
    } else if (value < 2 || value > INT32_MAX) {
        (void) qd_error(QD_ERROR_CONFIG,
                        "Invalid maxSessionFrames specified (%"PRId64"). Minimum value is 2 and maximum value is %"PRIi32,
                        value, INT32_MAX);
        goto error;
    }
    config->session_max_in_window = (uint32_t) value;

    //
    // For now we are hardwiring this attribute to true.  If there's an outcry from the
    // user community, we can revisit this later.
    //
    config->allowInsecureAuthentication = true;
    config->verify_host_name = verifyHostname;

    char *stripAnnotations  = qd_entity_opt_string(entity, "stripAnnotations", 0);
    load_strip_annotations(config, stripAnnotations);
    free(stripAnnotations);
    stripAnnotations = 0;
    CHECK();

    config->requireAuthentication = authenticatePeer;
    config->requireEncryption     = requireEncryption || requireSsl;

    if (config->ssl_profile_name) {
        config->ssl_required = requireSsl;
        config->ssl_require_peer_authentication = config->sasl_mechanisms &&
            strstr(config->sasl_mechanisms, "EXTERNAL") != 0;
    }

    return QD_ERROR_NONE;

  error:
    qd_server_config_free(config);
    return qd_error_code();
}


void qd_server_config_free(qd_server_config_t *cf)
{
    if (!cf) return;
    free(cf->host);
    free(cf->port);
    free(cf->host_port);
    free(cf->role);
    if (cf->http_root_dir)         free(cf->http_root_dir);
    if (cf->name)                  free(cf->name);
    if (cf->socket_address_family) free(cf->socket_address_family);
    if (cf->sasl_username)         free(cf->sasl_username);
    if (cf->sasl_password)         free(cf->sasl_password);
    if (cf->sasl_mechanisms)       free(cf->sasl_mechanisms);
    if (cf->ssl_profile_name)      free(cf->ssl_profile_name);
    if (cf->failover_list)         qd_failover_list_free(cf->failover_list);
    if (cf->log_message)           free(cf->log_message);
    if (cf->policy_vhost)          free(cf->policy_vhost);

    if (cf->conn_props) pn_data_free(cf->conn_props);

    memset(cf, 0, sizeof(*cf));
}


/**
 * Utility to interpret a configured password value
 */
void qd_server_config_process_password(char **actual_val, char *pw, bool *is_file, bool allow_literal_prefix)
{
    if (!pw)
        return;

    //
    // If the "password" starts with "env:" then the remaining
    // text is the environment variable that contains the password
    //
    if (strncmp(pw, "env:", 4) == 0) {
        char *env = pw + 4;
        // skip the leading whitespace if it is there
        while (*env == ' ') ++env;

        const char* passwd = getenv(env);
        if (passwd) {
            //
            // Replace the allocated directive with the looked-up password
            //
            *actual_val = strdup(passwd);
        } else {
            qd_error(QD_ERROR_NOT_FOUND, "Failed to find a password in the environment variable");
        }
    }

    //
    // If the "password" starts with "literal:" or "pass:" then
    // the remaining text is the password and the heading should be
    // stripped off
    //
    else if ( (strncmp(pw, "literal:", 8) == 0 && allow_literal_prefix) || strncmp(pw, "pass:", 5) == 0) {
        qd_log(LOG_CONN_MGR, QD_LOG_WARNING,
               "It is unsafe to provide plain text passwords in the config file");

        if (strncmp(pw, "l", 1) == 0) {
            // skip the "literal:" header
            pw += 8;
        }
        else {
            // skip the "pass:" header
            pw += 5;
        }

        //
        // Replace the password with a copy of the string after "literal: or "pass:"
        //
        char *copy = strdup(pw);
        *actual_val = copy;
    }
    //
    // If the password starts with a file: literal set the is_file to true.
    //
    else if (strncmp(pw, "file:", 5) == 0) {
        pw += 5;

        // Replace the password with a copy of the string after "file:"
        char *copy = strdup(pw);
        *actual_val = copy;
        *is_file = true;
    }
    else {
        //
        // THe password field does not have any prefixes. Use it as plain text
        //
        qd_log(LOG_CONN_MGR, QD_LOG_WARNING,
               "It is unsafe to provide plain text passwords in the config file");
    }
}


/**
 * Read the file from the password_file location on the file system and populate password_field with the
 * contents of the file.
 */
void qd_set_password_from_file(const char *password_file, char **password_field)
{
    if (password_file) {
        FILE *file = fopen(password_file, "r");

        if (file == NULL) {
            //
            // The global variable errno (found in <errno.h>) contains information about what went wrong; you can use perror() to print that information as a readable string
            //
            qd_log(LOG_CONN_MGR, QD_LOG_ERROR, "Unable to open password file %s, error: %s", password_file,
                   strerror(errno));
            return;
        }

        char buffer[200];

        int c;
        int i=0;

        while (i < 200 - 1) {
            c = fgetc(file);
            if (c == EOF || c == '\n')
                break;
            buffer[i++] = c;
        }

        if (i != 0) {
            buffer[i] = '\0';
            free(*password_field);
            *password_field = strdup(buffer);
        }
        fclose(file);
    }
}

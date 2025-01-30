#ifndef __server_config_h__
#define __server_config_h__ 1
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

#include "qpid/dispatch/error.h"
#include "qpid/dispatch/failoverlist.h"

#include <stddef.h>
#include <inttypes.h>
#include <stdbool.h>

typedef struct pn_data_t     pn_data_t;
typedef struct qd_entity_t   qd_entity_t;
typedef struct qd_dispatch_t qd_dispatch_t;


/**
 * Configuration block for a connector or a listener.
 */
typedef struct qd_server_config_t {
    /**
     * Host name or network address to bind to a listener or use in the connector.
     */
    char *host;

    /**
     * Port name or number to bind to a listener or use in the connector.
     */
    char *port;

    /**
     * Socket address family that the socket will use when binding listener or connector.
     * Possible values are IPv4 or IPv6. If not specified, the protocol family will be automatically determined from the address
     */
    char *socket_address_family;

    /**
     * Expose simple liveness check.
     */
    bool healthz;

    /**
     * Export metrics.
     */
    bool metrics;

    /**
     * Websockets enabled.
     */
    bool websockets;

    /**
     * Accept HTTP connections, allow WebSocket "amqp" protocol upgrades.
     */
    bool http;

    /**
     * Directory for HTTP content
     */
    char *http_root_dir;

    /**
     * Connection name, used as a reference from other parts of the configuration.
     */
    char *name;

    /**
     * Space-separated list of SASL mechanisms to be accepted for the connection.
     */
    char *sasl_mechanisms;

    /**
     * If appropriate for the mechanism, the username for authentication
     * (connector only)
     */
    char *sasl_username;

    /**
     * If appropriate for the mechanism, the password for authentication
     * (connector only)
     */
    char *sasl_password;

    /**
     * If appropriate for the mechanism, the minimum acceptable security strength factor
     */
    int sasl_minssf;

    /**
     * If appropriate for the mechanism, the maximum acceptable security strength factor
     */
    int sasl_maxssf;

    /**
     * Iff true, SSL/TLS must be used on the connection.
     */
    bool ssl_required;

    /**
     * Iff true, the client of the connection must authenticate with the server.
     */
    bool requireAuthentication;

    /**
     * Iff true, client authentication _may_ be insecure (i.e. PLAIN over plaintext).
     */
    bool allowInsecureAuthentication;

    /**
     * Iff true, the payload of the connection must be encrypted.
     */
    bool requireEncryption;

    /**
     * Ensures that when initiating a connection (as a client) the host name in the URL to which this connector
     * connects to matches the host name in the digital certificate that the peer sends back as part of the SSL connection
     */
    bool verify_host_name;

    /**
     * If true, strip the inbound qpid dispatch specific message annotations. This only applies to ingress and egress routers.
     * Annotations generated by inter-router messages will be untouched.
     */
    bool strip_inbound_annotations;

    /**
     * If true, strip the outbound qpid dispatch specific message annotations. This only applies to ingress and egress routers.
     * Annotations generated by inter-router messages will be untouched.
     */
    bool strip_outbound_annotations;

    /**
     * The number of deliveries that can be in-flight concurrently for each link within the connection.
     */
    int link_capacity;

    /**
     * The name of the related sslProfile configuration.
     */
    char *ssl_profile_name;

    /**
     * Iff true, require that the peer's certificate be supplied and that it be authentic
     * according to the set of trusted CAs.
     */
    bool ssl_require_peer_authentication;

    /**
     * Allow the connection to be redirected by the peer (via CLOSE->Redirect).  This is
     * meaningful for outgoing (connector) connections only.
     */
    bool allow_redirect;

    /**
     * MultiTenancy support.  If true, the vhost is used to define the address space of
     * addresses used over this connection.
     */
    bool multi_tenant;

    /**
     * Optional vhost to use for policy lookup.  If non-null, this overrides the vhost supplied
     * in the OPEN from the peer only for the purpose of identifying the policy to enforce.
     */
    char *policy_vhost;

    /**
     * The specified role of the connection.  This can be used to control the behavior and
     * capabilities of the connections.
     */
    char *role;

    /**
     * If the role is "inter-router", the cost can be set to a number greater than
     * or equal to one.  Inter-router cost is used to influence the routing algorithm
     * such that it prefers lower-cost paths.
     */
    int inter_router_cost;

    /**
     * The maximum size of an AMQP frame in octets. Frome maxFrameSize configuration attribute.
     */
    uint32_t max_frame_size;

    /**
     * The max_sessions value is the number of sessions allowed on the Connection. 
     */
    uint32_t max_sessions;

    /**
     * The session incoming window limit in frames. From maxSessionFrames configuration attribute
     *
     * The window indicates the maximum number of incoming *frames* that the session will buffer. This places a limit on
     * the amount of memory the router needs to reserve to accomodate data arriving from the peer session endpoint.
     *
     * The maximum amount of memory is computed as (max_frame_size * session_max_in_window)
     */
    uint32_t session_max_in_window;

    /**
     * The idle timeout, in seconds.  If the peer sends no data frames in this many seconds, the
     * connection will be automatically closed.
     */
    int idle_timeout_seconds;

    /**
     * The timeout, in seconds, for the initial connection handshake.  If a connection is established
     * inbound (via a listener) and the timeout expires before the OPEN frame arrives, the connection
     * shall be closed.
     */
    int initial_handshake_timeout_seconds;

    /**
     *  Holds comma separated list that indicates which components of the message should be logged.
     *  Defaults to 'none' (log nothing). If you want all properties and application properties of the message logged use 'all'.
     *  Specific components of the message can be logged by indicating the components via a comma separated list.
     *  The components are
     *  message-id
     *   user-id
     *   to
     *   subject
     *   reply-to
     *   correlation-id
     *   content-type
     *   content-encoding
     *   absolute-expiry-time
     *   creation-time
     *   group-id
     *   group-sequence
     *   reply-to-group-id
     *   app-properties.
     */
    char *log_message;

    /**
     * A bitwise representation of which log components have been enabled in the log_message field.
     */
    uint32_t message_log_flags;

    /**
     * Configured failover list
     */
    qd_failover_list_t *failover_list;

    /**
     * Extra connection properties to include in the outgoing Open frame.  Stored as a map.
     */
    pn_data_t *conn_props;

    bool  has_data_connectors;

    /**
     * @name These fields are not primary configuration, they are computed.
     * @{
     */

    /**
     * Concatenated connect/listen address "host:port"
     */
    char *host_port;

    /**
     * @}
     */
} qd_server_config_t;


qd_error_t qd_server_config_load(qd_dispatch_t *qd, qd_server_config_t *cf, qd_entity_t *entity, bool is_listener, const char *role_override);
void qd_server_config_free(qd_server_config_t *cf);
void qd_server_config_process_password(char **actual_val, char *pw, bool *is_file, bool allow_literal_prefix);
void qd_set_password_from_file(const char *password_file, char **password_field);

#endif

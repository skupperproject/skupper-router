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

#include "tcp_adaptor.h"

#include "adaptors/adaptor_buffer.h"
#include "adaptors/adaptor_listener.h"
#include "adaptors/adaptor_tls.h"
#include "delivery.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/connection_counters.h"

#include <proton/codec.h>
#include <proton/condition.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>

#include <inttypes.h>
#include <stdio.h>

// Flow Control
//
// TCP_MAX_CAPACITY is the maximum number of bytes to read from an incoming TCP connection before backpressure
// activates. Once backpressure is active this adaptor will no longer grant READ buffers to the ingress raw connection.
//
// Backpressure remains in affect until the outstanding received data is written to the egress TCP connection (the
// terminal consumer).  The egress TCP adaptor generates a PN_RECEIVED disposition to indicate data has been written out
// the TCP connection (ACKed). The total number of bytes written to the consumer is sent with the disposition update in
// the section_offset field (section number is ignored).
//
// When the PN_RECEIVED disposition arrives at the ingress adaptor it determines the total outstanding octets by
// subtracting written bytes (from the disposition section_offset) from the total number of bytes read from the raw
// connection. If the difference is less than TCP_MAX_CAPACITY backpressure is deactivated and reading resumes from the
// raw connection. Otherwise backpressure remains in effect until further PN_RECEIVED disposition updates arrive.
//
// The TCP_ACK_WINDOW is used To limit the frequency that PN_RECEIVED updates are generated. The egress adaptor keeps a
// count of the number of bytes written since the last PN_RECEIVED update was generated. A new PN_RECEIVED update is
// generated only after that count exceeds TCP_ACK_WINDOW.
//
// The value used for TCP_MAX_CAPACITY is based on the following assumptions:
//
// 1) the TCP producer is limited to GigEthernet throughput (1 Gbps).
// 2) Assume approximately 1msec latency to across a router
// 3) The message travels through a 3 hop router path to the consumer
//
// Effective GigEthernet throughput is 116MB/sec, or ~121635 bytes/msec.  For 3 hops routers with ~1msec latency gives a
// 6msec round trip time. Ideally the window would be 1/2 full at most before the ACK arrives:
//
const uint64_t TCP_MAX_CAPACITY = 121635 * 6 * 2;  // 1,459,620
const uint64_t TCP_ACK_WINDOW = (TCP_MAX_CAPACITY / 4);

static qd_duration_t half_closed_idle_timeout = 0;  // 0 = disabled

ALLOC_DEFINE(qdr_tcp_stats_t);
ALLOC_DEFINE(qd_tcp_listener_t);
ALLOC_DEFINE(qd_tcp_connector_t);
ALLOC_DEFINE(qdr_tcp_connection_ref_t);

#define WRITE_BUFFERS 64

#define LOCK   sys_mutex_lock
#define UNLOCK sys_mutex_unlock

#define TCP_NUM_ALPN_PROTOCOLS 2
// const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"h2", "http/1.1", "http/1.0"};
const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"http/1.1", "h2"};

struct qdr_tcp_connection_t {
    qd_handler_context_t  context;
    qd_tcp_connector_t   *connector;
    qd_tcp_listener_t    *listener;
    vflow_record_t        *vflow;
    char                 *reply_to;
    char                 *alpn_protocol;  // The negotiated ALPN protocol. Used only in the case of TLS connections.
    qdr_connection_t     *qdr_conn;
    uint64_t              conn_id;
    qdr_link_t           *incoming_link;
    uint64_t              incoming_link_id;
    qdr_link_t           *outgoing_link;
    uint64_t              outgoing_link_id;
    pn_raw_connection_t  *pn_raw_conn;
    sys_mutex_t           activation_lock;
    qdr_delivery_t       *in_dlv_stream;
    qdr_delivery_t       *out_dlv_stream;
    bool                  ingress;
    bool                  flow_enabled;
    bool                  is_egress_dispatcher_conn;
    bool                  connector_closed;//only used if egress_dispatcher=true
    bool                  in_list;         // This connection is in the adaptor's connections list
    bool                  raw_read_shutdown; // stream closed
    bool                  read_eos_seen;
    bool                  window_disabled;   // true: ignore unacked byte window
    sys_atomic_t          raw_closed_read;   // proton event seen
    sys_atomic_t          raw_closed_write;  // proton event seen or write_close called
    qdr_delivery_t       *initial_delivery;
    qd_timer_t           *activate_timer;
    qd_tcp_adaptor_config_t  *config;         // config
    qd_server_t          *server;
    qd_tls_t             *tls;
    char                 *remote_address;
    char                 *global_id;
    uint64_t              bytes_in;   // if this is TLS connection, the decrypted bytes read from raw conn, else just raw bytes read from the raw conn
    uint64_t              bytes_out;  // if this is TLS conn, the unencrypted bytes before writing to raw connection, else just raw bytes written to raw conn
    uint64_t              encrypted_bytes_in;  // If this is a TLS connection, the total encrypted bytes received on this connection, zero otherwise
    uint64_t              encrypted_bytes_out;  // If this is a TLS connection, the total encrypted bytes sent on this connection,  zero otherwise
    uint64_t              bytes_unacked; // bytes_unacked (read side): the count of bytes read that have yet to be acked by adaptor
    uint64_t              bytes_since_last_ack;  // bytes_since_last_ack (write side): the count of bytes since last PN_RECEIVED update was sent
    uint64_t              window_closed_count;

    uint64_t              opened_time;
    uint64_t              last_in_time;
    uint64_t              last_out_time;
    uint64_t              half_closed_bytes;    // bytes_in+_out snapshot
    sys_atomic_t          half_closed_expired;  // check for idle connection
    qd_timer_t           *half_closed_timer;

    qd_adaptor_buffer_list_t  out_buffs;           // Buffers for writing
    qd_message_stream_data_t *previous_stream_data; // previous segment (received in full)
    qd_message_stream_data_t *outgoing_stream_data; // current segment
    qd_message_stream_data_t *release_up_to;
    size_t                    outgoing_body_bytes;  // bytes received from current segment
    int                       outgoing_body_offset;  // buffer offset into current segment
    int                       outgoing_buff_count;  // number of buffers with data
    pn_raw_buffer_t           outgoing_buffs[WRITE_BUFFERS];
    int                       outgoing_buff_idx;    // first buffer with data
    bool                      require_tls;     // Is TLS required on this connection ?
    DEQ_LINKS(qdr_tcp_connection_t);
};

ALLOC_DECLARE_SAFE(qdr_tcp_connection_t);
ALLOC_DEFINE_SAFE(qdr_tcp_connection_t);
ALLOC_DEFINE(qd_tcp_adaptor_config_t);

typedef struct qdr_tcp_adaptor_t {
    qdr_core_t                    *core;
    qdr_protocol_adaptor_t        *adaptor;
    qd_tcp_listener_list_t         listeners;
    qd_tcp_connector_list_t        connectors;
    qdr_tcp_connection_ref_list_t  tcp_connections;
    sys_mutex_t                    listener_lock;  // protect listeners list
} qdr_tcp_adaptor_t;

static qdr_tcp_adaptor_t *tcp_adaptor;

static void qdr_add_tcp_connection(qdr_tcp_connection_t *conn);
static void qdr_add_tcp_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_del_tcp_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard);

static void handle_disconnected(qdr_tcp_connection_t* conn);
static void free_qdr_tcp_connection(qdr_tcp_connection_t* conn);
static void qdr_tcp_create_server_side_connection(qdr_tcp_connection_t* tc);
static void detach_links(qdr_tcp_connection_t *tc);
static void qd_tcp_listener_incref(qd_tcp_listener_t *listener);
static void qd_tcp_connector_incref(qd_tcp_connector_t *connector);
static void qd_tcp_connector_decref(qd_tcp_connector_t* c);
static void qd_tcp_listener_decref(qd_tcp_listener_t* li);
static void qdr_process_app_properties(qdr_tcp_connection_t *tc, qd_message_t *msg);
static void qdr_tcp_connection_ingress_accept(qdr_tcp_connection_t *tc);
static void handle_outgoing(qdr_tcp_connection_t *conn);
static void encrypt_outgoing_tls(qdr_tcp_connection_t *conn, qd_adaptor_buffer_t *unencrypted_buff, bool write_buffers);
static void start_half_closed_monitoring(qdr_tcp_connection_t *conn);
static void stop_half_closed_monitoring(qdr_tcp_connection_t *conn);
static bool check_half_closed_timeout(qdr_tcp_connection_t *conn);

void qdr_add_tcp_connection_ref(qdr_tcp_connection_ref_list_t *ref_list, qdr_tcp_connection_t *conn)
{
    qdr_tcp_connection_ref_t *ref = new_qdr_tcp_connection_ref_t();
    DEQ_ITEM_INIT(ref);
    ref->conn = conn;
    DEQ_INSERT_TAIL(*ref_list, ref);
}


void qdr_del_tcp_connection_ref(qdr_tcp_connection_ref_list_t *ref_list, qdr_tcp_connection_t *conn)
{
    qdr_tcp_connection_ref_t *ref = DEQ_HEAD(*ref_list);
    while (ref) {
        if (ref->conn == conn) {
            DEQ_REMOVE(*ref_list, ref);
            free_qdr_tcp_connection_ref_t(ref);
            break;
        }
        ref = DEQ_NEXT(ref);
    }
}

// is the connection in half-closed state?
//
inline static bool tcp_conn_is_half_closed(qdr_tcp_connection_t *conn)
{
    return IS_ATOMIC_FLAG_SET(&conn->raw_closed_read) != IS_ATOMIC_FLAG_SET(&conn->raw_closed_write);
}

// is the incoming byte window full
//
inline static bool read_window_full(const qdr_tcp_connection_t* conn)
{
    return !conn->window_disabled && conn->bytes_unacked >= TCP_MAX_CAPACITY;
}

/**
 * Queues up a qdr_add_tcp_connection_CT action on the core thread.
 */
static void qdr_add_tcp_connection(qdr_tcp_connection_t *conn)
{
    qdr_action_t *action           = qdr_action(qdr_add_tcp_connection_CT, "add_tcp_connection");
    action->args.general.context_1 = conn;
    qdr_action_enqueue(tcp_adaptor->core, action);
}

/**
 * Queues up a qdr_del_tcp_connection_CT action on the core thread.
 */
static void qdr_del_tcp_connection(qdr_tcp_connection_t *conn)
{
    qdr_action_t *action           = qdr_action(qdr_del_tcp_connection_CT, "delete_tcp_connection");
    action->args.general.context_1 = conn;
    qdr_action_enqueue(tcp_adaptor->core, action);
}

static qdr_tcp_stats_t *get_tcp_stats(qdr_tcp_connection_t *conn)
{
    qdr_tcp_stats_t *tcp_stats = 0;
    if (conn->connector) {
        tcp_stats = conn->connector->tcp_stats;
    }
    else {
        tcp_stats = conn->listener->tcp_stats;
    }
    return tcp_stats;
}

static inline uint64_t qdr_tcp_conn_linkid(const qdr_tcp_connection_t *conn)
{
    assert(conn);
    return conn->in_dlv_stream ? conn->incoming_link_id : conn->outgoing_link_id;
}

static inline const char * qdr_tcp_connection_role_name(const qdr_tcp_connection_t *tc)
{
    assert(tc);
    return tc->ingress ? "listener" : "connector";
}

static const char * qdr_tcp_quadrant_id(const qdr_tcp_connection_t *tc, const qdr_link_t *link)
{
    if (tc->ingress)
        return link->link_direction == QD_INCOMING ? "(listener incoming)" : "(listener outgoing)";
    else
        return link->link_direction == QD_INCOMING ? "(connector incoming)" : "(connector outgoing)";
}

static void on_activate(void *context)
{
    ASSERT_PROACTOR_MODE(SYS_THREAD_PROACTOR_MODE_TIMER);

    qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) context;

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] on_activate", conn->conn_id);
    while (qdr_connection_process(conn->qdr_conn)) {}
    if (conn->is_egress_dispatcher_conn && conn->connector_closed) {
        detach_links(conn);
        qdr_connection_set_context(conn->qdr_conn, 0);
        qdr_connection_closed(conn->qdr_conn);
        qd_connection_counter_dec(QD_PROTOCOL_TCP);
        conn->qdr_conn = 0;
        free_qdr_tcp_connection(conn);
    }
}

/**
 * Grants read buffers to the pn raw connection.
 * This is the ONLY function that should be called if we want to grant read buffers to the raw connection.
 * @see qd_raw_connection_grant_read_buffers
 */
static void grant_read_buffers(qdr_tcp_connection_t *conn, const char *msg)
{
    if (IS_ATOMIC_FLAG_SET(&conn->raw_closed_read) || read_window_full(conn))
        return;
    int granted_read_buffers = qd_raw_connection_grant_read_buffers(conn->pn_raw_conn);
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] grant_read_buffers(%s) granted %i read buffers to proton raw api", conn->conn_id, msg,
           granted_read_buffers);
}

static void qd_free_tcp_adaptor_config(qd_tcp_adaptor_config_t *config)
{
    if (!config)
        return;

    if(sys_atomic_dec(&config->ref_count) > 1)
        return;

    sys_atomic_destroy(&config->ref_count);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
           "Deleted TCP adaptor configuration '%s' for address %s, %s, siteId %s.", config->adaptor_config->name,
           config->adaptor_config->address, config->adaptor_config->host_port, config->adaptor_config->site_id);

    //
    // Free the common adaptor config.
    //
    qd_free_adaptor_config(config->adaptor_config);
    free_qd_tcp_adaptor_config_t(config);
}

/**
 * Takes in a list of qd_adaptor_buffer_t objects and copies its contents into the passed in list of qd_buffer_t
 * objects.
 */
static size_t copy_decrypted_adaptor_buffs_to_qd_buffs(qdr_tcp_connection_t     *conn,
                                                       qd_adaptor_buffer_list_t *decrypted_buffs,
                                                       qd_buffer_list_t         *qd_buffers)
{
    assert(qd_buffers);
    size_t bytes_copied = qd_adaptor_buffers_copy_to_qd_buffers(decrypted_buffs, qd_buffers);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] copy_decrypted_adaptor_buffs_to_qd_buffs() - DEQ_SIZE(qd_buffers)=%zu, bytes_copied=%zu",
           conn->conn_id, DEQ_SIZE(*qd_buffers), bytes_copied);

    return bytes_copied;
}

/**
 * Extract buffers and their bytes from raw connection
 * If connection is TLS enabled, the bytes are decrypted before the message is forwarded.
 * Adds received byte count to connection stats.
 * Returns the count of bytes in the buffers list.
 */
static int handle_incoming_raw_read(qdr_tcp_connection_t *conn, qd_buffer_list_t *buffers)
{
    CHECK_PROACTOR_RAW_CONNECTION(conn->pn_raw_conn);

    int result = 0;

    if (conn->require_tls) {
        int                      encrypted_bytes_in = 0;
        qd_adaptor_buffer_list_t decrypted_buffs;
        DEQ_INIT(decrypted_buffs);
        encrypted_bytes_in = qd_tls_decrypt(conn->tls, conn->pn_raw_conn, &decrypted_buffs);

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] handle_incoming_raw_read() output=%i",
               conn->conn_id, encrypted_bytes_in);

        if (encrypted_bytes_in == QD_TLS_ERROR) {
            pn_raw_connection_close(conn->pn_raw_conn);
            return 0;
        } else if (DEQ_SIZE(decrypted_buffs) > 0 && buffers && qd_tls_is_secure(conn->tls)) {
            result += copy_decrypted_adaptor_buffs_to_qd_buffs(conn, &decrypted_buffs, buffers);
        }
        conn->encrypted_bytes_in += encrypted_bytes_in;
    } else {
        //
        // No TLS involved, just call  qd_buffer_list_append on the incoming data and send it on its way.
        //
        pn_raw_buffer_t raw_buffers[RAW_BUFFER_BATCH];
        size_t          n;
        while ((n = pn_raw_connection_take_read_buffers(conn->pn_raw_conn, raw_buffers, RAW_BUFFER_BATCH))) {
            for (size_t i = 0; i < n && raw_buffers[i].bytes; ++i) {
                qd_adaptor_buffer_t *buf           = (qd_adaptor_buffer_t *) raw_buffers[i].context;
                uint32_t             raw_buff_size = raw_buffers[i].size;
                if (raw_buff_size > 0) {
                    result += raw_buff_size;
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "] pn_raw_connection_take_read_buffers() took buffer with %u bytes",
                           conn->conn_id, raw_buff_size);
                    if (buffers)
                        qd_buffer_list_append(buffers, (uint8_t *) (raw_buffers[i].bytes + raw_buffers[i].offset),
                                              raw_buffers[i].size);
                }
                else {
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "] pn_raw_connection_take_read_buffers() took buffer with 0 bytes",
                           conn->conn_id);
                }
                // Free the wire buffer that we got back from proton.
                qd_adaptor_buffer_free(buf);
            }
        }
    }

    if (result > 0) {
        // account for any incoming bytes just read
        const bool window_was_full = read_window_full(conn);
        conn->last_in_time = qdr_core_uptime_ticks(tcp_adaptor->core);
        conn->bytes_in += result;
        qdr_tcp_stats_t *tcp_stats = get_tcp_stats(conn);
        LOCK(&tcp_stats->stats_lock);
        tcp_stats->bytes_in += result;
        UNLOCK(&tcp_stats->stats_lock);
        conn->bytes_unacked += result;
        vflow_set_uint64(conn->vflow, VFLOW_ATTRIBUTE_OCTETS, conn->bytes_in);
        vflow_set_uint64(conn->vflow, VFLOW_ATTRIBUTE_OCTETS_UNACKED, conn->bytes_unacked);

        if (!window_was_full && read_window_full(conn)) {
            conn->window_closed_count++;
            vflow_set_uint64(conn->vflow, VFLOW_ATTRIBUTE_WINDOW_CLOSURES, conn->window_closed_count);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] TCP RX window CLOSED: bytes in=%" PRIu64 " unacked=%" PRIu64, conn->conn_id,
                   conn->bytes_in, conn->bytes_unacked);
        }
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] handle_incoming_raw_read() returning with result=%u", conn->conn_id, result);

    return result;
}

/**
 * Fetch incoming raw incoming buffers from proton and pass them to a delivery.
 * Create a new delivery if necessary.
 * Return number of bytes read from raw connection.
 */
static int handle_incoming(qdr_tcp_connection_t *conn, const char *msg)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "][L%" PRIu64 "] handle_incoming %s for %s connection. read_closed:%s, flow_enabled:%s",
           conn->conn_id, conn->incoming_link_id, msg, qdr_tcp_connection_role_name(conn),
           conn->raw_closed_read ? "T" : "F", conn->flow_enabled ? "T" : "F");

    if (conn->raw_read_shutdown) {
        // Drain all read buffers that may still be in the raw connection
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] handle_incoming %s for %s connection. drain read buffers", conn->conn_id,
               conn->incoming_link_id, msg, qdr_tcp_connection_role_name(conn));
        handle_incoming_raw_read(conn, 0);
        return 0;
    }

    // Don't initiate an ingress stream message
    // if we don't yet have a reply-to address and credit.
    if (conn->ingress && !conn->reply_to) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64
               "] Waiting for reply-to address before initiating %s ingress stream message, returning",
               conn->conn_id, conn->incoming_link_id, qdr_tcp_connection_role_name(conn));
        return 0;
    }
    if (!conn->flow_enabled) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] Waiting for credit before initiating %s ingress stream message, returning",
               conn->conn_id, conn->incoming_link_id, qdr_tcp_connection_role_name(conn));
        return 0;
    }

    // Read buffers available from proton.
    qd_buffer_list_t buffers;
    DEQ_INIT(buffers);
    int count = handle_incoming_raw_read(conn, &buffers);
    if (conn->require_tls && !qd_tls_is_secure(conn->tls)) {
        // We don't have a fully secure channel established, cannot send message yet, return.
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] handle_incoming- connection requires tls but is not secure yet, returning",
               conn->conn_id);
        return 0;
    }

    // Ensure existence of ingress stream message
    if (!conn->in_dlv_stream) {
        qd_message_t *msg = qd_message();

        qd_message_set_streaming_annotation(msg);
        qd_message_set_Q2_disabled_annotation(msg);

        qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(props);
        qd_compose_insert_null(props);                      // message-id
        qd_compose_insert_null(props);                      // user-id
        if (conn->ingress) {
            qd_compose_insert_string(props, conn->config->adaptor_config->address); // to
            qd_compose_insert_string(props, conn->global_id);      // subject
            qd_compose_insert_string(props, conn->reply_to);       // reply-to
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64
                   "] Initiating listener (ingress) stream incoming link for %s connection to: %s reply: %s",
                   conn->conn_id, conn->incoming_link_id, qdr_tcp_connection_role_name(conn),
                   conn->config->adaptor_config->address, conn->reply_to);
        } else {
            qd_compose_insert_string(props, conn->reply_to);  // to
            qd_compose_insert_string(props, conn->global_id); // subject
            qd_compose_insert_null(props);                    // reply-to
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64
                   "] Initiating connector (egress) stream incoming link for connection to: %s",
                   conn->conn_id, conn->incoming_link_id, conn->reply_to);
        }
        //qd_compose_insert_null(props);                      // correlation-id

        // ISSUE-1136 NOTE WELL: do not set the content-type field! The egress adaptor will use the fact that the content-type is
        // not present to determine that the message body is a sequence of VBIN data. Future non-compatible changes to
        // the message format will set the content-type field. Therefore this adaptor will RELEASE any AMQP messages
        // that have a content-type specified since the format is incompatible. This will happen if the endpoints of the
        // TCP stream (tcpConnector, tcpListener) are incompatible (misconfiguration).

        //qd_compose_insert_null(props);                      // content-type
        //qd_compose_insert_null(props);                      // content-encoding
        //qd_compose_insert_timestamp(props, 0);              // absolute-expiry-time
        //qd_compose_insert_timestamp(props, 0);              // creation-time
        //qd_compose_insert_null(props);                      // group-id
        //qd_compose_insert_uint(props, 0);                   // group-sequence
        //qd_compose_insert_null(props);                      // reply-to-group-id
        qd_compose_end_list(props);

        //
        // Add the application properties
        //
        props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, props);
        qd_compose_start_map(props);  // start map
        qd_compose_insert_symbol(props, QD_AP_FLOW_ID);
        vflow_serialize_identity(conn->vflow, props);
        if (conn->require_tls) {
            if (conn->alpn_protocol) {
                // propagate negotiated ALPN protocol value to server-facing side
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] Using negotiated protocol %s obtained via ALPN", conn->conn_id,
                       conn->alpn_protocol);
                qd_compose_insert_string_n(props, (const char *) QD_TLS_ALPN_KEY, QD_TLS_ALPN_KEY_LEN);
                qd_compose_insert_string_n(props, (const char *) conn->alpn_protocol, strlen(conn->alpn_protocol));
            } else {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] %s No ALPN protocol was negotiated",
                       conn->conn_id, qdr_tcp_connection_role_name(conn));
            }
        }
        qd_compose_end_map(props);  // end map

        qd_message_compose_2(msg, props, false);
        qd_compose_free(props);

        if (conn->ingress) {
            //
            // Start latency timer for this cross-van connection.
            //
            vflow_latency_start(conn->vflow);
        } else {
            //
            // End latency timer for this server-side connection.
            //
            vflow_latency_end(conn->vflow);
        }

        conn->in_dlv_stream = qdr_link_deliver(conn->incoming_link, msg, 0, false, 0, 0, 0, 0);

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "][D%" PRIu32 "] Initiating %s side empty incoming stream message",
               conn->conn_id, conn->incoming_link_id, conn->in_dlv_stream->delivery_id,
               qdr_tcp_connection_role_name(conn));
    }

    // Grant more buffers to proton for reading if read side is still open
    grant_read_buffers(conn, "handle_incoming");

    // Push the bytes just read into the streaming message
    if (count > 0) {
        bool ignore;
        qd_message_stream_data_append(qdr_delivery_message(conn->in_dlv_stream), &buffers, &ignore);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " Continuing %s message with %i bytes",
               DLV_ARGS(conn->in_dlv_stream), qdr_tcp_connection_role_name(conn), count);
        qdr_delivery_continue(tcp_adaptor->core, conn->in_dlv_stream, false);

    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] handle_incoming call to handle_incoming_raw_read returned count=%i", conn->conn_id,
               count);
        assert (DEQ_SIZE(buffers) == 0);
    }

    // Close the stream message if read side has closed
    if (IS_ATOMIC_FLAG_SET(&conn->raw_closed_read)) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               DLV_FMT " close %s in_dlv_stream delivery, setting receive_complete=true", DLV_ARGS(conn->in_dlv_stream),
               qdr_tcp_connection_role_name(conn));
        qd_message_set_receive_complete(qdr_delivery_message(conn->in_dlv_stream));
        qdr_delivery_continue(tcp_adaptor->core, conn->in_dlv_stream, true);
        conn->raw_read_shutdown = true;
    }

    if (qd_tls_has_output(conn->tls))
        encrypt_outgoing_tls(conn, 0, true);
    handle_outgoing(conn);

    return count;
}

/**
 * The adaptor connection is about to close, clean out any unsent out_buffs.
 */
static void clean_conn_out_buffs(qdr_tcp_connection_t *conn)
{
    qd_adaptor_buffer_t *buf      = DEQ_HEAD(conn->out_buffs);
    qd_adaptor_buffer_t *curr_buf = 0;
    while (buf) {
        curr_buf = buf;
        DEQ_REMOVE_HEAD(conn->out_buffs);
        buf = DEQ_HEAD(conn->out_buffs);
        qd_adaptor_buffer_free(curr_buf);
    }
}

static void free_qdr_tcp_connection(qdr_tcp_connection_t *tc)
{
    qdr_tcp_stats_t *tcp_stats = get_tcp_stats(tc);
    LOCK(&tcp_stats->stats_lock);
    tcp_stats->connections_closed += 1;
    UNLOCK(&tcp_stats->stats_lock);

    qd_tcp_connector_decref(tc->connector);
    qd_tcp_listener_decref(tc->listener);
    vflow_end_record(tc->vflow);
    free(tc->reply_to);
    free(tc->remote_address);
    free(tc->global_id);
    free(tc->alpn_protocol);
    sys_atomic_destroy(&tc->raw_closed_read);
    sys_atomic_destroy(&tc->raw_closed_write);
    sys_atomic_destroy(&tc->half_closed_expired);
    qd_timer_free(tc->activate_timer);
    qd_timer_free(tc->half_closed_timer);
    sys_mutex_free(&tc->activation_lock);

    // Free tls related stuff if need be.
    if (tc->tls) {
        qd_tls_free(tc->tls);
    }

    qd_free_tcp_adaptor_config(tc->config);
    clean_conn_out_buffs(tc);
    free_qdr_tcp_connection_t(tc);
}

static void handle_disconnected(qdr_tcp_connection_t* conn)
{
    if (conn->listener) {
        qd_tcp_listener_t *listener = conn->listener;
        sys_mutex_lock(&listener->lock);
        if (IS_ATOMIC_FLAG_SET(&listener->closing)) {
            // Wake up the next conn on the list to get it closed
            // See qd_dispatch_delete_tcp_listener() where the head connection is woken up.
            qdr_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            if (!!next_conn)
                pn_raw_connection_wake(next_conn->pn_raw_conn);
        }
        DEQ_REMOVE(listener->connections, conn);
        sys_mutex_unlock(&listener->lock);
    }
    else {
        qd_tcp_connector_t *connector = conn->connector;
        sys_mutex_lock(&connector->lock);
        if (IS_ATOMIC_FLAG_SET(&connector->closing)) {
            // Wake up the next conn on the list to get it closed
            // See qd_dispatch_delete_tcp_connector() where the head connection is woken up.
            qdr_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            if (!!next_conn)
                pn_raw_connection_wake(next_conn->pn_raw_conn);
        }
        DEQ_REMOVE(connector->connections, conn);
        sys_mutex_unlock(&connector->lock);
    }

    LOCK(&conn->activation_lock);
    if(conn->pn_raw_conn) {
        pn_raw_connection_set_context(conn->pn_raw_conn, 0);
        conn->pn_raw_conn = 0;
    }
    UNLOCK(&conn->activation_lock);

    // release all referenced message buffers since the deliveries will free
    // the message once we decref them. Note the order: outgoing_stream_data
    // comes after previous_stream_data, so previous_stream_data is
    // automagically freed when we release_up_to(outgoing_stream_data).
    if (conn->outgoing_stream_data) {
        qd_message_stream_data_release_up_to(conn->outgoing_stream_data);
        conn->outgoing_stream_data = 0;
        conn->previous_stream_data = 0;
    } else if (conn->previous_stream_data) {
        qd_message_stream_data_release_up_to(conn->previous_stream_data);
        conn->previous_stream_data = 0;
    }

    if (conn->in_dlv_stream) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] handle_disconnected - close in_dlv_stream", conn->conn_id,
               conn->incoming_link_id);
        qd_message_set_receive_complete(qdr_delivery_message(conn->in_dlv_stream));
        qdr_delivery_continue(tcp_adaptor->core, conn->in_dlv_stream, true);
        qdr_delivery_decref(tcp_adaptor->core, conn->in_dlv_stream, "tcp-adaptor.handle_disconnected - in_dlv_stream");
        conn->in_dlv_stream = 0;
    }
    if (conn->out_dlv_stream) {
        assert(!conn->initial_delivery);  // expect: moved initial_delivery to out_dlv_stream
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] handle_disconnected - close out_dlv_stream", conn->conn_id,
               conn->outgoing_link_id);

        // PN_ACCEPTED if clean eos occurs, PN_MODIFIED for error like TLS failure so the upstream remote is updated
        // correctly:
        qdr_delivery_remote_state_updated(tcp_adaptor->core, conn->out_dlv_stream,
                                          conn->read_eos_seen ? PN_ACCEPTED : PN_MODIFIED,
                                          true, // settled
                                          0, false);
        qdr_delivery_decref(tcp_adaptor->core, conn->out_dlv_stream, "tcp-adaptor.handle_disconnected - out_dlv_stream");
        conn->out_dlv_stream = 0;
    }

    detach_links(conn);

    if (conn->initial_delivery) {
        // PN_RELEASED: because if initial_delivery is set then the connection DISCONNECTED before data could be sent:
        qdr_delivery_remote_state_updated(tcp_adaptor->core, conn->initial_delivery, PN_RELEASED, true, 0, false);
        qdr_delivery_decref(tcp_adaptor->core, conn->initial_delivery, "tcp-adaptor.handle_disconnected - initial_delivery");
        conn->initial_delivery = 0;
    }

    if (conn->qdr_conn) {
        qdr_connection_set_context(conn->qdr_conn, 0);
        qdr_connection_closed(conn->qdr_conn);
        qd_connection_counter_dec(QD_PROTOCOL_TCP);
        conn->qdr_conn = 0;
    }

    // Do the actual deletion of the tcp connection instance on the core thread to prevent the core from running
    // qdr_tcp_activate_CT on a freed tcp connection pointer:
    qdr_del_tcp_connection(conn);
}

static int read_message_body(qdr_tcp_connection_t *conn, qd_message_t *msg, pn_raw_buffer_t *buffers, int count)
{
    int used = 0;

    // Advance to next stream_data vbin segment if necessary.
    // Return early if no data to process or error
    if (conn->outgoing_stream_data == 0) {
        qd_message_stream_data_result_t stream_data_result = qd_message_next_stream_data(msg, &conn->outgoing_stream_data);
        if (stream_data_result == QD_MESSAGE_STREAM_DATA_BODY_OK) {
            // a new stream_data segment has been found
            conn->outgoing_body_bytes  = 0;
            conn->outgoing_body_offset = 0;
            // continue to process this segment
        } else if (stream_data_result == QD_MESSAGE_STREAM_DATA_INCOMPLETE) {
            return 0;
        } else {
            switch (stream_data_result) {
            case QD_MESSAGE_STREAM_DATA_NO_MORE:
            case QD_MESSAGE_STREAM_DATA_ABORTED:
                // treat aborted like end-of-stream since both require closing the connection.
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] EOS", conn->conn_id);
                    conn->read_eos_seen = true;
                    break;
            case QD_MESSAGE_STREAM_DATA_INVALID:
                // Corrupted message, treat like EOS since there is no way to undo what has already been sent
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR,
                           "[C%" PRIu64 "] Invalid body data for streaming message, closing connection", conn->conn_id);
                    conn->read_eos_seen = true;
                    break;
            default:
                break;
            }
            qd_message_set_send_complete(msg);
            return -1;
        }
    }

    // A valid stream_data is in place.
    // Try to get a buffer set from it.
    used = qd_message_stream_data_buffers(conn->outgoing_stream_data, buffers, conn->outgoing_body_offset, count);
    if (used > 0) {
        // Accumulate the lengths of the returned buffers.
        for (int i=0; i<used; i++) {
            conn->outgoing_body_bytes += buffers[i].size;
        }

        // Buffers returned should never exceed the stream_data payload length
        assert(conn->outgoing_body_bytes <= conn->outgoing_stream_data->payload.length);

        if (conn->outgoing_body_bytes == conn->outgoing_stream_data->payload.length) {
            // Erase the stream_data struct from the connection so that
            // a new one gets created on the next pass.
            conn->previous_stream_data = conn->outgoing_stream_data;
            conn->outgoing_stream_data = 0;
        } else {
            // Returned buffer set did not consume the entire stream_data segment.
            // Leave existing stream_data struct in place for use on next pass.
            // Add the number of returned buffers to the offset for the next pass.
            conn->outgoing_body_offset += used;
        }
    } else {
        // No buffers returned.
        // This sender has caught up with all data available on the input stream.
    }
    return used;
}


static bool copy_outgoing_buffs(qdr_tcp_connection_t *conn)
{
    // Send the outgoing buffs to pn_raw_conn.
    size_t pn_buffs_capacity = pn_raw_connection_write_buffers_capacity(conn->pn_raw_conn);

    if (conn->outgoing_buff_count == 0) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] No outgoing buffers to copy at present, returning true", conn->conn_id);
        return true;
    } else if (DEQ_SIZE(conn->out_buffs) == pn_buffs_capacity) {
        //
        // Cannot continue reading body datas because the proton raw buffer write capacity has been reached.
        //
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] pn_raw_buffer_capacity of %zu reached, cannot copy at present, returning false",
               conn->conn_id, pn_buffs_capacity);
        return false;
    } else {
        // copy small buffers into one large adaptor buffer
        qd_adaptor_buffer_t *adaptor_buffer = qd_adaptor_buffer();
        while (conn->outgoing_buff_idx < conn->outgoing_buff_count) {
            pn_raw_buffer_t *rbuf = &conn->outgoing_buffs[conn->outgoing_buff_idx];

            // clang-format off
            if (rbuf->size > qd_adaptor_buffer_capacity(adaptor_buffer)) {
                break;
            }
            // clang-format on

            memcpy(qd_adaptor_buffer_cursor(adaptor_buffer), rbuf->bytes, rbuf->size);
            qd_adaptor_buffer_insert(adaptor_buffer, rbuf->size);
            conn->outgoing_buff_idx += 1;

            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Copying buffer %i of %i with %i bytes (total=%zu)", conn->conn_id,
                   conn->outgoing_buff_idx, conn->outgoing_buff_count, rbuf->size,
                   qd_adaptor_buffer_size(adaptor_buffer));
        }

        if (conn->require_tls) {
            // The connection requires tls, First encrypt the data before sending it out.
            encrypt_outgoing_tls(conn, adaptor_buffer, false);
        } else {
            DEQ_INSERT_TAIL(conn->out_buffs, adaptor_buffer);
        }

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Copied %d buffers, %i remain", conn->conn_id,
               conn->outgoing_buff_idx, conn->outgoing_buff_count - conn->outgoing_buff_idx);

        if (conn->outgoing_buff_idx == conn->outgoing_buff_count) {
            conn->outgoing_buff_count = 0;
            conn->outgoing_buff_idx   = 0;

            // set context only when stream data has just been consumed
            conn->release_up_to        = conn->previous_stream_data;
            conn->previous_stream_data = 0;

            return true;  // need more outgoing_buffs
        }
    }
    return false;
}

static void handle_outgoing(qdr_tcp_connection_t *conn)
{
    if (conn->out_dlv_stream) {
        if (IS_ATOMIC_FLAG_SET(&conn->raw_closed_write)) {
            // give no more buffers to raw connection
            return;
        }
        if (conn->require_tls && !qd_tls_is_secure(conn->tls)) {
            // We don't have a fully secure channel established, cannot send data yet, return.
            // The body datas have arrived but they can only be sent when the handshake is complete
            // and the channel is secure.
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] handle_outgoing - connection requires tls but is not secure yet, returning",
                   conn->conn_id);
            return;
        }
        qd_message_t *msg = qdr_delivery_message(conn->out_dlv_stream);
        bool read_more_body = true;

        if (conn->outgoing_buff_count > 0) {
            // flush outgoing buffs that hold body data waiting to go out
            read_more_body = copy_outgoing_buffs(conn);
        }
        while (read_more_body) {
            conn->outgoing_buff_idx   = 0;
            conn->outgoing_buff_count = read_message_body(conn, msg, conn->outgoing_buffs, WRITE_BUFFERS);
            if (conn->outgoing_buff_count > 0) {
                // Send the data just returned
                read_more_body = copy_outgoing_buffs(conn);
            } else {
                // The incoming stream has no new data to send
                break;
            }
        }
        assert(!IS_ATOMIC_FLAG_SET(&conn->raw_closed_write));
        int num_buffers_written = qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] handle_outgoing() num_buffers_written=%i",
               conn->conn_id, num_buffers_written);
        if (conn->read_eos_seen) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64
                   "] handle_outgoing calling pn_raw_connection_write_close(). rcv_complete:%s, send_complete:%s",
                   conn->conn_id, qd_message_receive_complete(msg) ? "T" : "F",
                   qd_message_send_complete(msg) ? "T" : "F");
            SET_ATOMIC_FLAG(&conn->raw_closed_write);
            pn_raw_connection_write_close(conn->pn_raw_conn);
        }
    }
}

static char *get_global_id(char *site_id, char *host_port)
{
    int len1 = strlen(host_port);
    int len = site_id ? len1 + strlen(site_id) + 2 : len1 + 1;
    char *result = malloc(len);
    strcpy(result, host_port);
    if (site_id) {
        result[len1] = '@';
        strcpy(result+len1+1, site_id);
    }
    return result;
}

static pn_data_t *qdr_tcp_conn_properties(void)
{
   // Return a new tcp connection properties map.
    pn_data_t *props = pn_data(0);
    pn_data_put_map(props);
    pn_data_enter(props);
    pn_data_put_symbol(props,
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_ADAPTOR_KEY),
                                       QD_CONNECTION_PROPERTY_ADAPTOR_KEY));
    pn_data_put_string(props,
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE),
                                       QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE));
    pn_data_exit(props);
    return props;
}

static void qdr_tcp_connection_ingress_accept(qdr_tcp_connection_t* tc)
{
    tc->remote_address = qd_raw_conn_get_address(tc->pn_raw_conn);
    tc->global_id = get_global_id(tc->config->adaptor_config->site_id, tc->remote_address);

    //
    // The qdr_connection_info() function makes its own copy of the passed in tcp_conn_properties.
    // So, we need to call pn_data_free(tcp_conn_properties).
    //
    pn_data_t *tcp_conn_properties = qdr_tcp_conn_properties();
    qdr_connection_info_t *info                = qdr_connection_info(tc->require_tls,      // is_encrypted,
                                                                     false,                // is_authenticated,
                                                                     true,                 // opened,
                                                                     "",                   // *sasl_mechanisms,
                                                                     QD_INCOMING,          // dir,
                                                                     tc->remote_address,   // *host,
                                                                     "",                   // *ssl_proto,
                                                                     "",                   // *ssl_cipher,
                                                                     "",                   // *user,
                                                                     "TcpAdaptor",         // *container,
                                                                     tcp_conn_properties,  // *connection_properties,
                                                                     0,                    // ssl_ssf,
                                                                     false,                // ssl,
                                                                     "",                   // peer router version,
                                                                     false,                // streaming links
                                                                     false);               // connection trunking
    pn_data_free(tcp_conn_properties);

    qdr_connection_t *conn = qdr_connection_opened(tcp_adaptor->core,
                                                   tcp_adaptor->adaptor,
                                                   true,            // incoming
                                                   QDR_ROLE_NORMAL, // role
                                                   1,               // cost
                                                   tc->conn_id,     // management_id
                                                   0,               // label
                                                   0,               // remote_container_id
                                                   false,           // strip_annotations_in
                                                   false,           // strip_annotations_out
                                                   250,             // link_capacity
                                                   0,               // policy_spec
                                                   info,            // connection_info
                                                   0,               // context_binder
                                                   0);              // bind_token
    tc->qdr_conn = conn;
    qdr_connection_set_context(conn, tc);
    qd_connection_counter_inc(QD_PROTOCOL_TCP);

    qdr_terminus_t *dynamic_source = qdr_terminus(0);
    qdr_terminus_set_dynamic(dynamic_source);
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, tc->config->adaptor_config->address);

    tc->outgoing_link = qdr_link_first_attach(conn,
                                              QD_OUTGOING,
                                              dynamic_source,     // qdr_terminus_t   *source,
                                              qdr_terminus(0),    // qdr_terminus_t   *target,
                                              "tcp.ingress.out",  // const char       *name,
                                              0,                  // const char       *terminus_addr,
                                              false,
                                              NULL,
                                              &(tc->outgoing_link_id));
    qdr_link_set_context(tc->outgoing_link, tc);
    tc->incoming_link = qdr_link_first_attach(conn,
                                              QD_INCOMING,
                                              qdr_terminus(0),   // qdr_terminus_t   *source,
                                              target,            // qdr_terminus_t   *target,
                                              "tcp.ingress.in",  // const char       *name,
                                              0,                 // const char       *terminus_addr,
                                              false,
                                              NULL,
                                              &(tc->incoming_link_id));
    tc->opened_time = qdr_core_uptime_ticks(tcp_adaptor->core);
    qdr_link_set_context(tc->incoming_link, tc);
    qdr_add_tcp_connection(tc);
}

// invoked once when tls session handshake completes successfully
static void on_tls_connection_secured(qd_tls_t *tls, void *user_context)
{
    qdr_tcp_connection_t *conn = (qdr_tcp_connection_t *) user_context;
    assert(conn);
    if (conn->qdr_conn && conn->qdr_conn->connection_info) {
        qd_tls_update_connection_info(conn->tls, conn->qdr_conn->connection_info);
    }
    if (!conn->alpn_protocol)
        qd_tls_get_alpn_protocol(conn->tls, &conn->alpn_protocol);
}

static void encrypt_outgoing_tls(qdr_tcp_connection_t *conn, qd_adaptor_buffer_t *unencrypted_buff, bool write_buffers)
{
    qd_adaptor_buffer_list_t encrypted_buffs;
    DEQ_INIT(encrypted_buffs);
    int bytes_out = qd_tls_encrypt(conn->tls, unencrypted_buff, &encrypted_buffs);
    if (bytes_out == QD_TLS_ERROR) {
        pn_raw_connection_close(conn->pn_raw_conn);
        return;
    }

    if (unencrypted_buff) {
        conn->bytes_out += bytes_out;
        conn->bytes_since_last_ack += bytes_out;
        qdr_tcp_stats_t *tcp_stats = get_tcp_stats(conn);
        LOCK(&tcp_stats->stats_lock);
        tcp_stats->bytes_out += bytes_out;
        UNLOCK(&tcp_stats->stats_lock);
    }

    if (DEQ_SIZE(encrypted_buffs) > 0) {
        DEQ_APPEND(conn->out_buffs, encrypted_buffs);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] encrypt_outgoing_tls() DEQ_SIZE(conn->out_buffs)=%zu", conn->conn_id,
               DEQ_SIZE(conn->out_buffs));
    }
    if (write_buffers && DEQ_SIZE(conn->out_buffs) > 0) {
        assert(!IS_ATOMIC_FLAG_SET(&conn->raw_closed_write));
        int num_buffers_written = qd_raw_connection_write_buffers(conn->pn_raw_conn, &conn->out_buffs);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] encrypt_outgoing_tls() num_buffers_written=%i",
               conn->conn_id, num_buffers_written);
    }
}

static void handle_connection_event(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_tcp_connection_t *conn = (qdr_tcp_connection_t *) context;
    CHECK_PROACTOR_RAW_CONNECTION(conn->pn_raw_conn);

    switch (pn_event_type(e)) {
    case PN_RAW_CONNECTION_CONNECTED: {
        qd_set_vflow_netaddr_string(conn->vflow, conn->pn_raw_conn, conn->ingress);
        if (conn->ingress) {
            qdr_tcp_connection_ingress_accept(conn);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] PN_RAW_CONNECTION_CONNECTED Listener ingress accepted to %s from %s (global_id=%s)",
                   conn->conn_id, conn->config->adaptor_config->host_port, conn->remote_address, conn->global_id);
            if (conn->require_tls) {
                assert(!conn->tls);
                conn->tls = qd_tls(conn->listener->tls_domain, conn, conn->conn_id, on_tls_connection_secured);
                if (conn->tls) {
                    // a pn_tls_session. Grant read buffers so that we can now start reading the initial TLS handshake
                    // bytes that the client is going to send us.
                    grant_read_buffers(conn, "PN_RAW_CONNECTION_CONNECTED, ingress");
                } else {
                    // There was some problem with starting up the proton tls session.
                    // Check logs for detailed INFO level output to find out more about the failure
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR,
                           "[C%" PRIu64
                           "] PN_RAW_CONNECTION_CONNECTED ingress failed to start TLS, closing raw connection",
                           conn->conn_id);
                    pn_raw_connection_close(conn->pn_raw_conn);
                }
            }
        } else {
            conn->remote_address = qd_raw_conn_get_address(conn->pn_raw_conn);
            conn->opened_time = qdr_core_uptime_ticks(tcp_adaptor->core);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] PN_RAW_CONNECTION_CONNECTED Connector egress connected to %s", conn->conn_id,
                   conn->remote_address);
            if (!!conn->initial_delivery) {
                qdr_tcp_create_server_side_connection(conn);
            }
            if (conn->require_tls && qd_tls_has_output(conn->tls)) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                       "[C%" PRIu64 "] Initiating TLS handshake on egress connection", conn->conn_id);
                encrypt_outgoing_tls(conn, 0, true);
                // Grant read buffers so we can read the response sent to us by the server
                grant_read_buffers(conn, "PN_RAW_CONNECTION_CONNECTED, egress");
            }
        }
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ: {
        SET_ATOMIC_FLAG(&conn->raw_closed_read);
        handle_incoming(conn, "PNRC_CLOSED_READ");
        if (!IS_ATOMIC_FLAG_SET(&conn->raw_closed_write)) {
            start_half_closed_monitoring(conn);
        }
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_CLOSED_READ %s",
               conn->conn_id, qdr_tcp_connection_role_name(conn));
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        SET_ATOMIC_FLAG(&conn->raw_closed_write);
        int num_drained_write_buffers = qd_raw_connection_drain_write_buffers(conn->pn_raw_conn);
        if (!IS_ATOMIC_FLAG_SET(&conn->raw_closed_read)) {
            start_half_closed_monitoring(conn);
        }
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_CLOSED_WRITE %s, drained %i write buffers", conn->conn_id,
               qdr_tcp_connection_role_name(conn), num_drained_write_buffers);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        pn_condition_t *cond = pn_raw_connection_condition(conn->pn_raw_conn);
        if (!!cond) {
            const char *cname = pn_condition_get_name(cond);
            const char *cdesc = pn_condition_get_description(cond);

            if (!!cname) {
                vflow_set_string(conn->vflow, VFLOW_ATTRIBUTE_RESULT, cname);
            }
            if (!!cdesc) {
                vflow_set_string(conn->vflow, VFLOW_ATTRIBUTE_REASON, cdesc);
            }
        }
        // If somehow the PN_RAW_CONNECTION_CLOSED_WRITE and the PN_RAW_CONNECTION_CLOSED_READ events did not come by,
        // we will drain the buffers here just as a backup.
        int drained_buffers = qd_raw_connection_drain_read_write_buffers(conn->pn_raw_conn);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_DISCONNECTED %s, drained_buffers=%i", conn->conn_id,
               qdr_tcp_connection_role_name(conn), drained_buffers);

        stop_half_closed_monitoring(conn);
        handle_disconnected(conn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_NEED_WRITE_BUFFERS %s",
               conn->conn_id, qdr_tcp_connection_role_name(conn));
        while (qdr_connection_process(conn->qdr_conn)) {}
        handle_outgoing(conn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_NEED_READ_BUFFERS %s",
               conn->conn_id, qdr_tcp_connection_role_name(conn));
        while (qdr_connection_process(conn->qdr_conn)) {}
        if (conn->in_dlv_stream) {
            grant_read_buffers(conn, "PN_RAW_CONNECTION_NEED_READ_BUFFERS");
            handle_incoming(conn, "PNRC_NEED_READ_BUFFERS");
        }
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] PN_RAW_CONNECTION_WAKE %s", conn->conn_id,
               qdr_tcp_connection_role_name(conn));
        while (qdr_connection_process(conn->qdr_conn)) {}
        if (tcp_conn_is_half_closed(conn) && check_half_closed_timeout(conn)) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_WARNING,
                   "[C%" PRIu64 "] Idle half-closed TCP connection detected, forcing close", conn->conn_id);
            pn_condition_t *cond = pn_raw_connection_condition(conn->pn_raw_conn);
            if (!!cond) {
                (void) pn_condition_set_name(cond, "connection:forced");
                (void) pn_condition_set_description(cond, "connection closed due to half-closed idle timeout");
            }
            pn_raw_connection_close(conn->pn_raw_conn);
        }

        bool closing = conn->listener ? IS_ATOMIC_FLAG_SET(&conn->listener->closing) : IS_ATOMIC_FLAG_SET(&conn->connector->closing);
        if (closing) {
            if (!!conn->qdr_conn) {
                qdr_core_close_connection(conn->qdr_conn);
            } else if (!!conn->pn_raw_conn){
                pn_raw_connection_close(conn->pn_raw_conn);
            }
        }
        break;
    }
    case PN_RAW_CONNECTION_DRAIN_BUFFERS: {
        pn_raw_connection_t *pn_raw_conn     = pn_event_raw_connection(e);
        int                  drained_buffers = qd_raw_connection_drain_read_write_buffers(pn_raw_conn);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_DRAIN_BUFFERS Drained a total of %i buffers", conn->conn_id,
               drained_buffers);
    } break;
    case PN_RAW_CONNECTION_READ: {
        int read = 0;
        read     = handle_incoming(conn, "PNRC_READ");
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] PN_RAW_CONNECTION_READ Read %i bytes. Total read %" PRIu64
               " bytes, Total encrypted bytes=%" PRIu64 "",
               conn->conn_id, read, conn->bytes_in, conn->encrypted_bytes_in);

        if (qd_tls_has_output(conn->tls)) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] PN_RAW_CONNECTION_READ qd_tls_has_output=true", conn->conn_id);
            encrypt_outgoing_tls(conn, 0, true);
        }

        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        pn_raw_buffer_t buffs[RAW_BUFFER_BATCH];
        size_t          written = 0;
        size_t          n;
        while ((n = pn_raw_connection_take_written_buffers(conn->pn_raw_conn, buffs, RAW_BUFFER_BATCH))) {
            for (size_t i = 0; i < n; ++i) {
                written += buffs[i].size;
                qd_adaptor_buffer_t *qd_adaptor_buffer = (qd_adaptor_buffer_t *) buffs[i].context;
                assert(qd_adaptor_buffer);
                qd_adaptor_buffer_free(qd_adaptor_buffer);
            }
        }

        if (written > 0) {
            if (conn->release_up_to) {
                qd_message_stream_data_release_up_to((qd_message_stream_data_t *) conn->release_up_to);
                conn->release_up_to = 0;
            }
            conn->last_out_time = qdr_core_uptime_ticks(tcp_adaptor->core);
            if (!conn->require_tls) {
                conn->bytes_out += written;
                conn->bytes_since_last_ack += written;
                qdr_tcp_stats_t *tcp_stats = get_tcp_stats(conn);
                LOCK(&tcp_stats->stats_lock);
                tcp_stats->bytes_out += written;
                UNLOCK(&tcp_stats->stats_lock);
            } else {
                // note: conn->bytes_out has already been updated when the unencrypted data is passed to TLS. See
                // encrypt_outgoing_tls
                conn->encrypted_bytes_out += written;
            }
            // Tell the upstream to open its receive window.  Note: this update
            // is sent to the upstream (ingress) TCP adaptor. Since this update
            // is internal to the router network (never sent to the client) we
            // do not need to use the section_number (no section numbers in a
            // TCP stream!) and use section_offset only.
            //
            if (conn->out_dlv_stream && (conn->bytes_since_last_ack >= TCP_ACK_WINDOW)) {
                qd_delivery_state_t *dstate = qd_delivery_state();
                dstate->section_number      = 0;
                dstate->section_offset      = conn->bytes_out;
                conn->bytes_since_last_ack  = 0;
                qdr_delivery_remote_state_updated(tcp_adaptor->core, conn->out_dlv_stream, PN_RECEIVED,
                                                  false,  // settled
                                                  dstate, false);
            }
        }
        if (conn->require_tls) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64
                   "] PN_RAW_CONNECTION_WRITTEN %s wrote %zu encrypted bytes. Total unencrypted data bytes written on "
                   "this connection so far="
                   "%" PRIu64 " , Total encrypted bytes written on this connection so far=%" PRIu64 "",
                   conn->conn_id, qdr_tcp_connection_role_name(conn), written, conn->bytes_out,
                   conn->encrypted_bytes_out);
        } else {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64
                   "] PN_RAW_CONNECTION_WRITTEN %s wrote %zu bytes. Total bytes written on this connection so far="
                   "%" PRIu64 " bytes",
                   conn->conn_id, qdr_tcp_connection_role_name(conn), written, conn->bytes_out);
        }
        handle_outgoing(conn);
        while (qdr_connection_process(conn->qdr_conn)) {}
        break;
    }
    default:
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Unexpected Event: %d", conn->conn_id,
               pn_event_type(e));
        break;
    }
}

static qdr_tcp_connection_t *qdr_tcp_connection(qd_tcp_listener_t *listener, qd_tcp_connector_t *connector,
                                                bool is_egress_dispatcher_conn, bool ingress)
{
    qdr_tcp_connection_t* tc = new_qdr_tcp_connection_t();
    ZERO(tc);
    qdr_tcp_stats_t *tcp_stats = 0;
    qd_server_t     *server    = 0;
    if (listener) {
        tc->listener = listener;
        tc->config   = listener->config;
        tcp_stats    = listener->tcp_stats;
        server       = listener->server;
        sys_mutex_lock(&listener->lock);
        DEQ_INSERT_TAIL(listener->connections, tc);
        sys_mutex_unlock(&listener->lock);
        qd_tcp_listener_incref(listener);
    } else if (connector) {
        tc->connector = connector;
        tc->config    = connector->config;
        tcp_stats     = connector->tcp_stats;
        server        = connector->server;
        sys_mutex_lock(&connector->lock);
        DEQ_INSERT_TAIL(connector->connections, tc);
        sys_mutex_unlock(&connector->lock);
        qd_tcp_connector_incref(connector);
    }
    assert(tc->config);
    assert(tcp_stats);
    assert(server);

    tc->conn_id         = qd_server_allocate_connection_id(server);
    tc->context.context = tc;
    tc->context.handler = &handle_connection_event;
    sys_atomic_init(&tc->raw_closed_read, 0);
    sys_atomic_init(&tc->raw_closed_write, 0);
    sys_atomic_init(&tc->half_closed_expired, 0);
    sys_mutex_init(&tc->activation_lock);
    tc->is_egress_dispatcher_conn = is_egress_dispatcher_conn;
    tc->ingress = ingress;
    tc->server                    = server;
    sys_atomic_inc(&tc->config->ref_count);

    LOCK(&tcp_stats->stats_lock);
    tcp_stats->connections_opened +=1;
    UNLOCK(&tcp_stats->stats_lock);
    DEQ_INIT(tc->out_buffs);
    return tc;
}


// Adaptor Listener Accept event handler callback.  This callback is invoked on
// the proactor listener thread when a client connects to the TCP listening
// socket (the PN_LISTENER_ACCEPT event).
//
static void qdr_tcp_connection_ingress(qd_adaptor_listener_t *ali,
                                       pn_listener_t *pn_listener,
                                       void *user_context)
{
    CHECK_PROACTOR_LISTENER(pn_listener);

    qd_tcp_listener_t *listener = (qd_tcp_listener_t*) user_context;
    assert(listener);

    qdr_tcp_connection_t *tc = qdr_tcp_connection(listener, 0, false, true);
    tc->require_tls = !!listener->tls_domain;

    tc->vflow = vflow_start_record(VFLOW_RECORD_FLOW, listener->vflow);
    vflow_set_uint64(tc->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(tc->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(tc->vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, TCP_MAX_CAPACITY);

    // IMPORTANT NOTE: this next call TO pn_listener_raw_accept may immediately schedule the connection on another I/O
    // thread. IF you want to access tc after this call, the activation_lock  must be held to prevent the code in PN_RAW_CONNECTION_DISCONNECTED handler from running
    // and freeing this connection(qdr_tcp_connection_t) from underneath. The PN_RAW_CONNECTION_DISCONNECTED event might occur if the
    // client that initiated the connection, closes the connection while we are trying to accept the connection.
    // For example:
    //     LOCK(tc->activation_lock);
    //     pn_listener_raw_accept(pn_listener, tc->pn_raw_conn);
    //     call_some_function(tc->conn_id);   // if the activation_lock is not held, tc might be freed
    //     UNLOCK(tc->activation_lock);
    tc->pn_raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(tc->pn_raw_conn, tc);
    pn_listener_raw_accept(pn_listener, tc->pn_raw_conn);
}


/**
 * Creates the connection objects that are necessary to communicate with the core.
 * This function does not open any socket level connections
 */
static void qdr_tcp_create_server_side_connection(qdr_tcp_connection_t* tc)
{
    const char *host = tc->is_egress_dispatcher_conn ? "egress-dispatch" : tc->config->adaptor_config->host_port;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] Opening server-side core connection %s", tc->conn_id,
           host);

    //
    // The qdr_connection_info() function makes its own copy of the passed in tcp_conn_properties.
    // So, we need to call pn_data_free(tcp_conn_properties)
    //
    pn_data_t *tcp_conn_properties = qdr_tcp_conn_properties();
    qdr_connection_info_t *info    = qdr_connection_info(tc->require_tls,  // bool             is_encrypted,
                                                         false,        // bool             is_authenticated,
                                                         true,         // bool             opened,
                                                         "",           // char            *sasl_mechanisms,
                                                         QD_OUTGOING,  // qd_direction_t   dir,
                                                         host,         // const char      *host,
                                                         "",           // const char      *ssl_proto,
                                                         "",           // const char      *ssl_cipher,
                                                         "",           // const char      *user,
                                                         "TcpAdaptor",  // const char      *container,
                                                         tcp_conn_properties,  // pn_data_t *connection_properties,
                                                         0,                    // int              ssl_ssf,
                                                         false,                // bool             ssl,
                                                         "",                   // peer router version,
                                                         false,                // streaming links
                                                         false);               // connection trunking
    pn_data_free(tcp_conn_properties);

    qdr_connection_t *conn = qdr_connection_opened(tcp_adaptor->core,
                                                   tcp_adaptor->adaptor,
                                                   false,           // incoming
                                                   QDR_ROLE_NORMAL, // role
                                                   1,               // cost
                                                   tc->conn_id,     // management_id
                                                   0,               // label
                                                   0,               // remote_container_id
                                                   false,           // strip_annotations_in
                                                   false,           // strip_annotations_out
                                                   250,             // link_capacity
                                                   0,               // policy_spec
                                                   info,            // connection_info
                                                   0,               // context_binder
                                                   0);              // bind_token
    tc->qdr_conn = conn;
    qdr_connection_set_context(conn, tc);
    qd_connection_counter_inc(QD_PROTOCOL_TCP);

    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, tc->config->adaptor_config->address);

    // This attach passes the ownership of the delivery from the core-side connection and link
    // to the adaptor-side outgoing connection and link.
    uint64_t i_conn_id = 0;
    uint64_t i_link_id = 0;
    if (!!tc->initial_delivery) {
        i_conn_id = tc->initial_delivery->conn_id;
        i_link_id = tc->initial_delivery->link_id;
    }

    tc->outgoing_link = qdr_link_first_attach(conn,
                                              QD_OUTGOING,
                                              source,            // qdr_terminus_t   *source,
                                              qdr_terminus(0),   // qdr_terminus_t   *target,
                                              "tcp.egress.out",  // const char       *name,
                                              0,                 // const char       *terminus_addr,
                                              !(tc->is_egress_dispatcher_conn),
                                              tc->initial_delivery,
                                              &(tc->outgoing_link_id));
    if (!!tc->initial_delivery) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " initial_delivery ownership passed to " DLV_FMT,
               i_conn_id, i_link_id, tc->initial_delivery->delivery_id, tc->outgoing_link->conn_id,
               tc->outgoing_link->identity, tc->initial_delivery->delivery_id);
        qdr_delivery_decref(tcp_adaptor->core, tc->initial_delivery, "tcp-adaptor - passing initial_delivery into new link");
        tc->initial_delivery = 0;
    }
    qdr_link_set_context(tc->outgoing_link, tc);
}

// Create a new TCP connection to the server (egress). Note that the actual qd_tcp_connection_t instance is NOT returned
// from this function: doing so would cause a race since the new connection is scheduled on a different proactor thread
// by this call. That thread may have deleted the new connection (if an error occurs) before this function returns.
//
// Returns true if the connection was successfully created and scheduled, otherwise false on error.
//
static bool qdr_tcp_create_egress_connection(qd_tcp_connector_t *connector, qdr_delivery_t *initial_delivery)
{
    assert(initial_delivery);

    // This is not an egress dispatcher connection.
    // Real TCP traffic flows thru this connection.
    // There is one of these connection per every client that is attaching to the router
    // network, i.e. there are N of these real connections (non-egress dispatcher connections) for N clients
    // respectively.
    //
    qdr_tcp_connection_t *tc = qdr_tcp_connection(0, connector, false, false);
    tc->require_tls = !!connector->tls_domain;
    qd_message_t *msg = qdr_delivery_message(initial_delivery);
    tc->vflow         = vflow_start_record(VFLOW_RECORD_FLOW, connector->vflow);
    qdr_process_app_properties(tc, msg);
    if (tc->require_tls) {
        // Since TLS is required on this connection, try to initialize TLS attributes
        // from the associated sslProfile.
        qd_tcp_connector_t *connector = tc->connector;
        if (tc->alpn_protocol) {
            const char *alpn_protocols[] = {tc->alpn_protocol};
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] Calling qd_tls_set_alpn_protocols on egress connection using ALPN protocol as %s",
                   tc->conn_id, tc->alpn_protocol);
            //
            // We need to call qd_tls_set_alpn_protocols *before* the call to pn_tls_start() in qd_tls() constructor can
            // be called. For the time being, we are using only a single qd_tls_domain object per connector. We are not
            // going to try to copy qd_tls_domain objects. The assumption is that users will run only one protocol
            // per connector. If you need a new protocol, use a different connector.
            //
            int res = qd_tls_set_alpn_protocols(connector->tls_domain, alpn_protocols, 1);
            if (res != 0) {
            qd_log(LOG_TCP_ADAPTOR,
                   QD_LOG_ERROR,
                   "Adaptor %s %s: failed to configure ALPN protocols (%d)",
                   qdr_tcp_connection_role_name(tc),
                   connector->config->adaptor_config->name,
                   res);
            free_qdr_tcp_connection(tc);  // this will undo the connector incref
            return false;
            }
        }

        tc->tls = qd_tls(connector->tls_domain, tc, tc->conn_id, on_tls_connection_secured);
        if (!tc->tls) {
            // There was a failure trying to setup the connector sslProfile.  Look at the logs for failure reason.
            // We cannot proceed setting up this connection, free it.
            free_qdr_tcp_connection(tc);  // this will undo the connector incref
            return false;
        }
    }

    tc->initial_delivery = initial_delivery;
    qdr_delivery_incref(initial_delivery, "qdr_tcp_connection_egress_create - held initial delivery");

    vflow_set_uint64(tc->vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(tc->vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(tc->vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, TCP_MAX_CAPACITY);
    vflow_set_trace(tc->vflow, msg);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           "[C%" PRIu64 "] qdr_tcp_connection_egress call pn_proactor_raw_connect(). Egress connecting to: %s",
           tc->conn_id, tc->config->adaptor_config->host_port);

    tc->pn_raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(tc->pn_raw_conn, tc);

    vflow_latency_start(tc->vflow);

    // IMPORTANT NOTE: this next call TO pn_proactor_raw_connect may immediately schedule the connection on another I/O
    // thread. The activation_lock  must be held if you ever want to access tc immediately after the call to
    // pn_proactor_raw_connect to prevent the code in PN_RAW_CONNECTION_DISCONNECTED handler from running and freeing
    // this qdr_tcp_connection_t from underneath.
    pn_proactor_raw_connect(qd_server_proactor(tc->server), tc->pn_raw_conn, tc->config->adaptor_config->host_port);
    return true;
}

// Create the dispatcher pseudo connection. This connection is used by the core to deliver new messages to this
// adaptor. There is no actual network connection associated with this connection.
//
static qdr_tcp_connection_t *qdr_tcp_create_dispatcher_connection(qd_tcp_connector_t      *connector,
                                                                  qd_tcp_adaptor_config_t *config,
                                                                  qd_server_t             *server)
{
    //
    // This is just an egress dispatcher connection. It is initially created so an
    // outgoing link can be created on it. When a delivery arrives on the outgoing link
    // in the egress dispatcher connection, it is moved to another connection/link
    //
    qdr_tcp_connection_t *tc = qdr_tcp_connection(0, connector, true, false);
    tc->activate_timer       = qd_timer(tcp_adaptor->core->qd, on_activate, tc);
    //
    // Create a server side dispatcher connection.
    // We don't want to create any socket level connection here.
    //
    qdr_tcp_create_server_side_connection(tc);
    return tc;
}

static qd_tcp_adaptor_config_t *qd_tcp_adaptor_config(void)
{
    qd_tcp_adaptor_config_t *tcp_config = new_qd_tcp_adaptor_config_t();
    if (!tcp_config)
        return 0;
    ZERO(tcp_config);
    qd_adaptor_config_t *adaptor_config = new_qd_adaptor_config_t();
    if (!adaptor_config) {
        free_qd_tcp_adaptor_config_t(tcp_config);
        return 0;
    }
    ZERO(adaptor_config);
    tcp_config->adaptor_config = adaptor_config;
    sys_atomic_init(&tcp_config->ref_count, 1);
    return tcp_config;
}

static void log_tcp_adaptor_config(qd_tcp_adaptor_config_t *c, const char *what)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Configured %s (legacy encap) for %s, %s:%s", what, c->adaptor_config->address,
           c->adaptor_config->host, c->adaptor_config->port);
}

static void qd_tcp_listener_free(qd_tcp_listener_t *listener)
{
    vflow_end_record(listener->vflow);
    qd_tls_domain_decref(listener->tls_domain);
    qd_free_tcp_adaptor_config(listener->config);
    free_qdr_tcp_stats_t(listener->tcp_stats);

    sys_atomic_destroy(&listener->ref_count);
    sys_mutex_free(&listener->tcp_stats->stats_lock);
    sys_mutex_free(&listener->lock);
    free_qd_tcp_listener_t(listener);
}

static void qd_tcp_listener_incref(qd_tcp_listener_t *listener)
{
    sys_atomic_inc(&listener->ref_count);
}

static void qd_tcp_connector_incref(qd_tcp_connector_t *connector)
{
    sys_atomic_inc(&connector->ref_count);
}

static void qd_tcp_listener_decref(qd_tcp_listener_t *listener)
{
    if (listener && sys_atomic_dec(&listener->ref_count) == 1) {
        qd_tcp_listener_free(listener);
    }
}

static qd_tcp_listener_t *qd_tcp_listener(qd_server_t *server)
{
    qd_tcp_listener_t *li = new_qd_tcp_listener_t();
    assert(li);
    ZERO(li);
    sys_atomic_init(&li->ref_count, 1);
    li->server = server;
    li->config = qd_tcp_adaptor_config();
    li->tcp_stats = new_qdr_tcp_stats_t();
    sys_mutex_init(&li->lock);
    sys_atomic_init(&li->closing, 0);
    DEQ_INIT(li->connections);
    ZERO(li->tcp_stats);
    sys_mutex_init(&li->tcp_stats->stats_lock);

    //
    // Create a vflow record for this listener
    //
    li->vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "tcp");

    return li;
}

static qd_error_t qd_load_tcp_adaptor_config(qdr_core_t *core, qd_tcp_adaptor_config_t *config, qd_entity_t *entity)
{
    // Make a call to the function that loads the common adaptor config.
    qd_error_t qd_error = qd_load_adaptor_config(core, config->adaptor_config, entity);
    // Add more code here if you want to load something specific to the tcp adaptor config.
    return qd_error;
}


// Note well: called from the management thread
qd_tcp_listener_t *qd_dispatch_configure_tcp_listener_legacy(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_tcp_listener_t *li = qd_tcp_listener(qd->server);
    if (qd_load_tcp_adaptor_config(qd->router->router_core, li->config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp listener: %s", qd_error_message());
        qd_tcp_listener_decref(li);
        return 0;
    }

    if (li->config->adaptor_config->ssl_profile_name) {
        // On the TCP TLS listener side, send "http/1.1", "http/1.0" and "h2" as ALPN protocols
        li->tls_domain = qd_tls_domain(li->config->adaptor_config, qd, LOG_TCP_ADAPTOR, tcp_alpn_protocols,
                                       TCP_NUM_ALPN_PROTOCOLS, true);
        if (!li->tls_domain) {
            // note qd_tls_domain logged the error
            qd_tcp_listener_decref(li);
            return 0;
        }
    }

    DEQ_ITEM_INIT(li);
    log_tcp_adaptor_config(li->config, "TcpListener");

    //
    // Report listener configuration to vflow
    //
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_NAME,             li->config->adaptor_config->name);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, li->config->adaptor_config->host);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, li->config->adaptor_config->port);
    vflow_set_string(li->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      li->config->adaptor_config->address);

    sys_mutex_lock(&tcp_adaptor->listener_lock);
    DEQ_INSERT_TAIL(tcp_adaptor->listeners, li);  // ref_count taken
    sys_mutex_unlock(&tcp_adaptor->listener_lock);

    li->adaptor_listener = qd_adaptor_listener(qd, li->config->adaptor_config, LOG_TCP_ADAPTOR);
    qd_adaptor_listener_listen(li->adaptor_listener, qdr_tcp_connection_ingress, (void*) li);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "tcpListener %s created at %s:%s for address %s",
           li->config->adaptor_config->name, li->config->adaptor_config->host, li->config->adaptor_config->port,
           li->config->adaptor_config->address);

    return li;
}


// Note: this runs on the management thread
void qd_dispatch_delete_tcp_listener_legacy(qd_dispatch_t *qd, qd_tcp_listener_t *li)
{
    if (li) {

        sys_mutex_lock(&tcp_adaptor->listener_lock);
        DEQ_REMOVE(tcp_adaptor->listeners, li);
        sys_mutex_unlock(&tcp_adaptor->listener_lock);

        qd_adaptor_listener_close(li->adaptor_listener);
        li->adaptor_listener = 0;

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Deleted TcpListener for %s, %s:%s",
               li->config->adaptor_config->address, li->config->adaptor_config->host, li->config->adaptor_config->port);

        // Initiate termination of existing connections
        //
        if (qd->terminate_tcp_conns) {
            // Note: PN_RAW_CONNECTION_CONNECTED event or PN_RAW_CONNECTION_DISCONNECTED event
            // could come upon any of the connections. We need to hold the listener->lock
            // to prevent any modification of the connections list while it is being traversed.
            sys_mutex_lock(&li->lock);
            SET_ATOMIC_FLAG(&li->closing);
            //
            // Only the head connection is woken when holding the lock.
            // This is an optimization. The next connection in the list is woken up in the handler of the PN_RAW_DISCONNECTED
            // event of this connection (See close_connection_XSIDE_IO() to see where the next connection in the list is woken up).
            // That way, we don't have to wake all the connections when holding the lock.
            //
            qdr_tcp_connection_t *conn = DEQ_HEAD(li->connections);
            if (conn)
                pn_raw_connection_wake(conn->pn_raw_conn);
            sys_mutex_unlock(&li->lock);
        }

        qd_tcp_listener_decref(li);  // drop reference held by listeners list
    }
}

qd_error_t qd_entity_refresh_tcpListener_legacy(qd_entity_t* entity, qd_tcp_listener_t *listener)
{
    LOCK(&listener->tcp_stats->stats_lock);
    uint64_t bi = listener->tcp_stats->bytes_in;
    uint64_t bo = listener->tcp_stats->bytes_out;
    uint64_t co = listener->tcp_stats->connections_opened;
    uint64_t cc = listener->tcp_stats->connections_closed;
    UNLOCK(&listener->tcp_stats->stats_lock);

    qd_listener_oper_status_t os = qd_adaptor_listener_oper_status(listener->adaptor_listener);
    if (   qd_entity_set_long(entity, "bytesIn",           bi) == 0
        && qd_entity_set_long(entity, "bytesOut",          bo) == 0
        && qd_entity_set_long(entity, "connectionsOpened", co) == 0
        && qd_entity_set_long(entity, "connectionsClosed", cc) == 0
        && qd_entity_set_string(entity, "operStatus",
                                os == QD_LISTENER_OPER_UP ? "up" : "down") == 0)
    {
        return QD_ERROR_NONE;
    }
    return qd_error_code();
}

static qd_tcp_connector_t *qd_tcp_connector(qd_server_t *server)
{
    qd_tcp_connector_t *c = new_qd_tcp_connector_t();
    assert(c);
    ZERO(c);
    sys_atomic_init(&c->ref_count, 1);
    c->server = server;
    c->config = qd_tcp_adaptor_config();
    c->tcp_stats = new_qdr_tcp_stats_t();
    sys_mutex_init(&c->lock);
    sys_atomic_init(&c->closing, 0);
    ZERO(c->tcp_stats);
    DEQ_INIT(c->connections);
    sys_mutex_init(&c->tcp_stats->stats_lock);
    //
    // Create a vflow record for this connector
    //
    c->vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(c->vflow, VFLOW_ATTRIBUTE_PROTOCOL, "tcp");

    return c;
}

static void qd_tcp_connector_free(qd_tcp_connector_t *connector)
{
    vflow_end_record(connector->vflow);
    qd_tls_domain_decref(connector->tls_domain);
    free_qdr_tcp_stats_t(connector->tcp_stats);
    qd_free_tcp_adaptor_config(connector->config);

    sys_atomic_destroy(&connector->ref_count);
    sys_mutex_free(&connector->lock);
    sys_mutex_free(&connector->tcp_stats->stats_lock);
    free_qd_tcp_connector_t(connector);
}

static void qd_tcp_connector_decref(qd_tcp_connector_t* connector)
{
    if (connector && sys_atomic_dec(&connector->ref_count) == 1) {
        qd_tcp_connector_free(connector);
    }
}


qd_tcp_connector_t *qd_dispatch_configure_tcp_connector_legacy(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_tcp_connector_t *c = qd_tcp_connector(qd->server);
    if (qd_load_tcp_adaptor_config(qd->router->router_core, c->config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        qd_tcp_connector_decref(c);
        return 0;
    }

    if (c->config->adaptor_config->ssl_profile_name) {
        c->tls_domain = qd_tls_domain(c->config->adaptor_config, qd, LOG_TCP_ADAPTOR, 0, 0, false);
        if (!c->tls_domain) {
            // note qd_tls_domain() logged the error
            qd_tcp_connector_decref(c);
            return 0;
        }
    }

    DEQ_ITEM_INIT(c);
    DEQ_INSERT_TAIL(tcp_adaptor->connectors, c);
    log_tcp_adaptor_config(c->config, "TcpConnector");

    //
    // Report connector configuration to vflow
    //
    vflow_set_string(c->vflow, VFLOW_ATTRIBUTE_NAME,             c->config->adaptor_config->name);
    vflow_set_string(c->vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, c->config->adaptor_config->host);
    vflow_set_string(c->vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, c->config->adaptor_config->port);
    vflow_set_string(c->vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      c->config->adaptor_config->address);

    c->dispatcher_conn = qdr_tcp_create_dispatcher_connection(c, c->config, c->server);
    if (!c->dispatcher_conn) {
        qd_tcp_connector_decref(c);
        return 0;
    }
    return c;
}

void qd_dispatch_delete_tcp_connector_legacy(qd_dispatch_t *qd, qd_tcp_connector_t *ct)
{
    if (ct) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Deleted TcpConnector for %s, %s:%s",
               ct->config->adaptor_config->address, ct->config->adaptor_config->host, ct->config->adaptor_config->port);

        // need to close the pseudo-connection used for dispatching
        // deliveries out to live connections:
        handle_disconnected((qdr_tcp_connection_t*) ct->dispatcher_conn);
        ct->dispatcher_conn = 0;
        DEQ_REMOVE(tcp_adaptor->connectors, ct);

        //
        // Initiate termination of existing connections
        //
        if (qd->terminate_tcp_conns) {
            // Note: PN_RAW_CONNECTION_CONNECTED event or PN_RAW_CONNECTION_DISCONNECTED event
            // could come upon any of the connections. We need to hold the connector->lock
            // to prevent any modification of the connections list while it is being traversed.
            sys_mutex_lock(&ct->lock);
            SET_ATOMIC_FLAG(&ct->closing);
            //
            // Only the head connection is woken when holding the lock.
            // This is an optimization. The next connection in the list is woken up in the handler of the PN_RAW_DISCONNECTED
            // event of this connection (See close_connection_XSIDE_IO() to see where the next connection in the list is woken up).
            // That way, we don't have to wake all the connections when holding the lock.
            //
            qdr_tcp_connection_t *conn = DEQ_HEAD(ct->connections);
            if (conn)
                pn_raw_connection_wake(conn->pn_raw_conn);
            sys_mutex_unlock(&ct->lock);
        }

        qd_tcp_connector_decref(ct);
    }
}

qd_error_t qd_entity_refresh_tcpConnector_legacy(qd_entity_t* entity, qd_tcp_connector_t *connector)
{
    LOCK(&connector->tcp_stats->stats_lock);
    uint64_t bi = connector->tcp_stats->bytes_in;
    uint64_t bo = connector->tcp_stats->bytes_out;
    uint64_t co = connector->tcp_stats->connections_opened;
    uint64_t cc = connector->tcp_stats->connections_closed;
    UNLOCK(&connector->tcp_stats->stats_lock);


    if (   qd_entity_set_long(entity, "bytesIn",           bi) == 0
        && qd_entity_set_long(entity, "bytesOut",          bo) == 0
        && qd_entity_set_long(entity, "connectionsOpened", co) == 0
        && qd_entity_set_long(entity, "connectionsClosed", cc) == 0)
    {
        return QD_ERROR_NONE;
    }
    return qd_error_code();
}

static void qdr_tcp_first_attach(void *context, qdr_connection_t *conn, qdr_link_t *link,
                                 qdr_terminus_t *source, qdr_terminus_t *target,
                                 qd_session_class_t session_class)
{
    void *tcontext = qdr_connection_get_context(conn);
    if (tcontext) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) tcontext;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_first_attach: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_first_attach: no link context");
        assert(false);
    }
}

static void qdr_tcp_connection_copy_reply_to(qdr_tcp_connection_t* tc, qd_iterator_t* reply_to)
{
    tc->reply_to = (char*)  qd_iterator_copy(reply_to);
}

static void qdr_tcp_connection_copy_global_id(qdr_tcp_connection_t* tc, qd_iterator_t* subject)
{
    int length = qd_iterator_length(subject);
    tc->global_id = malloc(length + 1);
    qd_iterator_strncpy(subject, tc->global_id, length + 1);
}

static void qdr_tcp_second_attach(void *context, qdr_link_t *link,
                                  qdr_terminus_t *source, qdr_terminus_t *target)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* tc = (qdr_tcp_connection_t*) link_context;
        if (qdr_link_direction(link) == QD_OUTGOING) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] %s qdr_tcp_second_attach",
                   tc->conn_id, tc->outgoing_link_id, qdr_tcp_quadrant_id(tc, link));
            if (tc->ingress) {
                qdr_tcp_connection_copy_reply_to(tc, qdr_terminus_get_address(source));
                // for ingress, can start reading from socket once we have
                // a reply to address, as that is when we are able to send
                // out a message
                handle_incoming(tc, "qdr_tcp_second_attach");
                if (qd_tls_has_output(tc->tls)) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] qdr_tcp_flow qd_tls_has_output=true",
                       tc->conn_id);
                encrypt_outgoing_tls(tc, 0, true);
                }
            }
            qdr_link_flow(tcp_adaptor->core, link, 10, false);
        } else if (!tc->ingress) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] %s qdr_tcp_second_attach",
                   tc->conn_id, tc->incoming_link_id, qdr_tcp_quadrant_id(tc, link));
        }
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_second_attach: no link context");
        assert(false);
    }
}


static void qdr_tcp_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_detach");
    assert(false);
}


static void qdr_tcp_flow(void *context, qdr_link_t *link, int credit)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        if (!conn->flow_enabled && credit > 0) {
            conn->flow_enabled = true;
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_flow: Flow enabled, credit=%d", conn->conn_id,
                   conn->outgoing_link_id, credit);
            handle_incoming(conn, "qdr_tcp_flow");
            if (qd_tls_has_output(conn->tls)) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] qdr_tcp_flow qd_tls_has_output=true",
                       conn->conn_id);
                encrypt_outgoing_tls(conn, 0, true);
            }
        } else {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_flow: No action. enabled:%s, credit:%d", conn->conn_id,
                   qdr_tcp_conn_linkid(conn), conn->flow_enabled ? "T" : "F", credit);
        }
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_flow: no link context");
        assert(false);
    }
}


static void qdr_tcp_offer(void *context, qdr_link_t *link, int delivery_count)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_offer: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_offer: no link context");
        assert(false);
    }

}


static void qdr_tcp_drained(void *context, qdr_link_t *link)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_drained: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_drained: no link context");
        assert(false);
    }
}


static void qdr_tcp_drain(void *context, qdr_link_t *link, bool mode)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_drain: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_drain: no link context");
        assert(false);
    }
}


static int qdr_tcp_push(void *context, qdr_link_t *link, int limit)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_push", conn->conn_id,
               qdr_tcp_conn_linkid(conn));
        return qdr_link_process_deliveries(tcp_adaptor->core, link, limit);
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_push: no link context");
        assert(false);
        return 0;
    }
}

/**
 * @brief Find the flow-id and the alpn protocol in the message's application properties,
 * If flow-id is available use it as the counterflow reference of the connection's flow record.
 * If alpn protocol is available, set it on the connection.
 *
 * @param tc Pointer to the tcp connection state
 * @param msg Pointer to the message received from the ingress (listener) side
 */
static void qdr_process_app_properties(qdr_tcp_connection_t *tc, qd_message_t *msg)
{
    assert(!!tc->vflow);
    qd_iterator_t *ap_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (!ap_iter) {
        return;
    }

    do {
        qd_parsed_field_t *ap = qd_parse(ap_iter);
        if (!ap) {
            break;
        }

        do {
            if (!qd_parse_ok(ap) || !qd_parse_is_map(ap)) {
                break;
            }

            uint32_t count = qd_parse_sub_count(ap);
            qd_parsed_field_t *id_value = 0;
            for (uint32_t i = 0; i < count; i++) {
                qd_parsed_field_t *key = qd_parse_sub_key(ap, i);
                if (key == 0) {
                    break;
                }
                qd_iterator_t *key_iter = qd_parse_raw(key);
                if (!!key_iter && qd_iterator_equal(key_iter, (const unsigned char*) QD_AP_FLOW_ID)) {
                    id_value = qd_parse_sub_value(ap, i);
                } else if (!!key_iter && tc->require_tls && qd_iterator_equal(key_iter, (const unsigned char *) QD_TLS_ALPN_KEY)) {
                    qd_parsed_field_t *alpn_field = qd_parse_sub_value(ap, i);
                    qd_iterator_t     *alpn_iter  = qd_parse_raw(alpn_field);
                    tc->alpn_protocol             = (char *) qd_iterator_copy(alpn_iter);
                }
            }

            if (!!id_value) {
                vflow_set_ref_from_parsed(tc->vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, id_value);
            }
        } while (false);
        qd_parse_free(ap);
    } while (false);
    qd_iterator_free(ap_iter);
}

static uint64_t qdr_tcp_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qd_message_t *msg = qdr_delivery_message(delivery);

    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* tc = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " qdr_tcp_deliver Delivery event", DLV_ARGS(delivery));
        if (tc->is_egress_dispatcher_conn) {
            //
            // We have received a delivery on the egress dispatcher connection.
            // Move it to a new link on a new connection. This new connection
            // will be used to communicate with the TCP server.
            //
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT
                   " tcp_adaptor delivery arrived on egress dispatcher connection, initiating actual egress connection",
                   DLV_ARGS(delivery));

            // Validate the message. Be sure at least the application-properties section is present since that is where
            // the message originator has put the flow-id for this stream
            //
            qd_message_depth_status_t depth_ok = qd_message_check_depth(msg, QD_DEPTH_APPLICATION_PROPERTIES);
            if (depth_ok == QD_MESSAGE_DEPTH_INCOMPLETE) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                       DLV_FMT " tcp_adaptor egress message incomplete, waiting for more", DLV_ARGS(delivery));
                return 0;  // retry later
            }
            if (depth_ok != QD_MESSAGE_DEPTH_OK) {  // otherwise bug? corrupted message encoding?
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_WARNING, DLV_FMT " Malformed TCP message - discarding!", DLV_ARGS(delivery));
                qd_message_set_send_complete(msg);
                return PN_REJECTED;
            }

            // ISSUE-1136: check if the message format is correct. For this adaptor the content-type field must be
            // unset. See comment in handle_incoming().
            //
            qd_iterator_t *ctype = qd_message_field_iterator(msg, QD_FIELD_CONTENT_TYPE);
            if (ctype) {
                qd_iterator_free(ctype);
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, DLV_FMT " Misconfigured tcpConnector (wrong encapsulation)",
                       DLV_ARGS(delivery));
                qd_message_set_send_complete(msg);
                return PN_RELEASED;  // allow it to be re-forwarded to a different adaptor
            }

            if (qdr_tcp_create_egress_connection(tc->connector, delivery)) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                       DLV_FMT " tcp_adaptor delivery QD_DELIVERY_MOVED_TO_NEW_LINK", DLV_ARGS(delivery));
                return QD_DELIVERY_MOVED_TO_NEW_LINK;
            } else {
                //
                // Unable to create a new egress connection. We cannot proceed. We have to release this delivery.
                //
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR,
                       DLV_FMT
                       " Failed to create a new TCP connection to %s, cannot forward this delivery - releasing it",
                       DLV_ARGS(delivery), tc->config->adaptor_config->host_port);
                qd_message_set_send_complete(msg);
                return PN_RELEASED;  // allow it to be re-forwarded to a different adaptor
            }
        } else if (!tc->out_dlv_stream) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT " tcp_adaptor delivery arrived on non-egress dispatcher connection", DLV_ARGS(delivery));

            if (tc->ingress) {
                // Egress (connector-side) outgoing messages are validated when they arrive on the dispatcher link (see
                // above). Ingress (client-side) outbound reply messages do not arrive on the dispatch link so these
                // messages need to be validated here.

                qd_message_depth_status_t depth_ok = qd_message_check_depth(msg, QD_DEPTH_APPLICATION_PROPERTIES);
                if (depth_ok == QD_MESSAGE_DEPTH_INCOMPLETE) {
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                           DLV_FMT " tcp_adaptor reply message incomplete, waiting for more", DLV_ARGS(delivery));
                    return 0;  // retry later
                }
                if (depth_ok != QD_MESSAGE_DEPTH_OK) {  // otherwise bug? corrupted message encoding?
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_WARNING, DLV_FMT " Malformed TCP message - discarding!", DLV_ARGS(delivery));
                    qd_message_set_send_complete(msg);
                    return PN_REJECTED;
                }

                // ISSUE-1136: check if the message format is correct. For this adaptor the content-type field must be
                // unset. See comment in handle_incoming().
                //
                qd_iterator_t *ctype = qd_message_field_iterator(msg, QD_FIELD_CONTENT_TYPE);
                if (ctype) {
                    qd_iterator_free(ctype);
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, DLV_FMT " Misconfigured tcpListener (wrong outgoing encapsulation)",
                           DLV_ARGS(delivery));
                    qd_message_set_send_complete(msg);

                    // What to do? This is a reply message, so it cannot be re-delivered to another service.

                    if (tc->pn_raw_conn) {
                        // set the raw connection condition info so it will appear in the vanflow logs
                        // when the connection disconnects
                        pn_condition_t *cond = pn_raw_connection_condition(tc->pn_raw_conn);
                        if (!!cond) {
                            (void) pn_condition_set_name(cond, "delivery-failed");
                            (void) pn_condition_set_description(cond, "invalid message encapsulation");
                        }
                        pn_raw_connection_close(tc->pn_raw_conn);
                    }
                    return PN_REJECTED;
                }
            }

            tc->out_dlv_stream = delivery;
            qdr_delivery_incref(delivery, "tcp_adaptor - new out_dlv_stream");
            if (tc->ingress) {
                vflow_latency_end(tc->vflow);
            } else {
                // on egress, can only set up link for the reverse
                // direction once we receive the first part of the
                // message from client to server
                qd_iterator_t *f_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
                qdr_tcp_connection_copy_global_id(tc, f_iter);
                qd_iterator_free(f_iter);
                f_iter = qd_message_field_iterator(msg, QD_FIELD_REPLY_TO);
                qdr_tcp_connection_copy_reply_to(tc, f_iter);
                qd_iterator_free(f_iter);
                qdr_terminus_t *target = qdr_terminus(0);
                qdr_terminus_set_address(target, tc->reply_to);
                tc->incoming_link = qdr_link_first_attach(tc->qdr_conn,
                                                          QD_INCOMING,
                                                          qdr_terminus(0),  // qdr_terminus_t   *source,
                                                          target,           // qdr_terminus_t   *target,
                                                          "tcp.egress.in",  // const char       *name,
                                                          0,                // const char       *terminus_addr,
                                                          false,
                                                          NULL,
                                                          &(tc->incoming_link_id));
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] %s Created link to %s",
                       tc->conn_id, tc->incoming_link->identity, qdr_tcp_quadrant_id(tc, tc->incoming_link),
                       tc->reply_to);
                qdr_link_set_context(tc->incoming_link, tc);
                //
                //add this connection to those visible through management now that we have the global_id
                //
                qdr_add_tcp_connection(tc);
                handle_incoming(tc, "qdr_tcp_deliver");
            }
        }
        handle_outgoing(tc);
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_deliver: no link context");
        assert(false);
    }
    return 0;
}


static int qdr_tcp_get_credit(void *context, qdr_link_t *link)
{
    void* link_context = qdr_link_get_context(link);
    if (link_context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_get_credit: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_get_credit: no link context");
        assert(false);
    }
    return 10;
}


static void qdr_tcp_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    void* link_context = qdr_link_get_context(qdr_delivery_link(dlv));
    if (link_context) {
        qdr_tcp_connection_t* tc = (qdr_tcp_connection_t*) link_context;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               DLV_FMT " qdr_tcp_delivery_update: disp: %" PRIu64 ", settled: %s", DLV_ARGS(dlv), disp,
               settled ? "true" : "false");

        const bool final_outcome = qd_delivery_state_is_terminal(disp);
        if (final_outcome && disp != PN_ACCEPTED) {
            // Oopsie! The message failed to be delivered due to error. Since
            // this is a streaming message do not bother waiting for settlement
            // because downstream routers will not propagate settlement until
            // after rx complete is set.  Note this is not half-closed, that
            // status is signalled by read_eos_seen and is not sufficient by
            // itself to force a connection closure.
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT " qdr_tcp_delivery_update: delivery failed with outcome=0x%" PRIx64
                           ", closing raw connection",
                   DLV_ARGS(dlv), disp);
            if (tc->pn_raw_conn) {
                // set the raw connection condition info so it will appear in the vanflow logs
                // when the connection disconnects
                pn_condition_t *cond = pn_raw_connection_condition(tc->pn_raw_conn);
                if (!!cond) {
                    (void) pn_condition_set_name(cond, "delivery-failed");
                    (void) pn_condition_set_description(cond, (disp == PN_REJECTED) ? "invalid/corrupt message" : "destination unreachable");
                }
                pn_raw_connection_close(tc->pn_raw_conn);
            }
            return;
        }

        // handle read window updates
        const bool window_was_full = read_window_full(tc);
        tc->window_disabled = tc->window_disabled || settled || final_outcome;
        if (!tc->window_disabled) {

            if (disp == PN_RECEIVED) {
                //
                // the consumer of this TCP flow has updated its tx_sequence:
                //
                uint64_t ignore;
                qd_delivery_state_t *dstate = qdr_delivery_take_local_delivery_state(dlv, &ignore);

                // resend released will generate a PN_RECEIVED with section_offset == 0, ignore it
                if (dstate && dstate->section_offset > 0) {
                    // note: the PN_RECEIVED is generated by the remote TCP
                    // adaptor, for simplicity we ignore the section_number since
                    // all we really need is a byte offset:
                    //
                    vflow_set_uint64(tc->vflow, VFLOW_ATTRIBUTE_OCTETS_UNACKED, tc->bytes_unacked);
                    tc->bytes_unacked = tc->bytes_in - dstate->section_offset;
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                           "[C%" PRIu64 "] tc->bytes_in=%" PRIu64 ",  dstate->section_offset=%" PRIu64
                           ", tc->bytes_unacked=%" PRIu64 "",
                           tc->conn_id, tc->bytes_in, dstate->section_offset, tc->bytes_unacked);
                    qd_delivery_state_free(dstate);
                }
            } else if (disp) {
                // terminal outcome: drain any pending receive data
                tc->window_disabled = true;
            }
        }

        if (window_was_full && !read_window_full(tc)) {
            // now that the window has opened (or has been disabled) resume reading
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] TCP RX window OPENED: bytes in=%" PRIu64 " unacked=%" PRIu64, tc->conn_id,
                   tc->bytes_in, tc->bytes_unacked);
            // Grant more buffers to proton for reading if read side is still open
            grant_read_buffers(tc, "TCP RX window OPENED");
        }
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_delivery_update: no link context");
        assert(false);
    }
}


static void qdr_tcp_conn_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
    void *tcontext = qdr_connection_get_context(conn);
    if (tcontext) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) tcontext;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_conn_close: closing raw connection", conn->conn_id,
               qdr_tcp_conn_linkid(conn));
        //
        // Closing the raw connection (calling pn_raw_connection_close()) will generate a PN_RAW_CONNECTION_DISCONNECTED
        // event which will call the handle_disconnected() which in turn calls qdr_connection_closed() which removes the
        // connection from the list of connections.
        //
        pn_raw_connection_close(conn->pn_raw_conn);
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_conn_close: no connection context");
        assert(false);
    }
}


static void qdr_tcp_conn_trace(void *context, qdr_connection_t *conn, bool trace)
{
    void *tcontext = qdr_connection_get_context(conn);
    if (tcontext) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) tcontext;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] qdr_tcp_conn_trace: NOOP",
               conn->conn_id, qdr_tcp_conn_linkid(conn));
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_conn_trace: no connection context");
        assert(false);
    }
}

// invoked by the Core thread to activate the TCP connection associated with 'c'
//
static void qdr_tcp_activate_CT(void *notused, qdr_connection_t *c)
{
    void *context = qdr_connection_get_context(c);
    if (context) {
        qdr_tcp_connection_t* conn = (qdr_tcp_connection_t*) context;
        LOCK(&conn->activation_lock);
        if (conn->pn_raw_conn && !(IS_ATOMIC_FLAG_SET(&conn->raw_closed_read) && IS_ATOMIC_FLAG_SET(&conn->raw_closed_write))) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] qdr_tcp_activate_CT: call pn_raw_connection_wake()", conn->conn_id);
            pn_raw_connection_wake(conn->pn_raw_conn);
            UNLOCK(&conn->activation_lock);
        } else if (conn->activate_timer) {
            UNLOCK(&conn->activation_lock);
            // On egress, the raw connection is only created once the
            // first part of the message encapsulating the
            // client->server half of the stream has been
            // received. Prior to that however a subscribing link (and
            // its associated connection must be setup), for which we
            // fake wakeup by using a timer.
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] qdr_tcp_activate_CT: schedule activate_timer", conn->conn_id);
            qd_timer_schedule(conn->activate_timer, 0);
        } else {
            UNLOCK(&conn->activation_lock);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] qdr_tcp_activate_CT: Cannot activate",
                   conn->conn_id);
        }
    } else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "qdr_tcp_activate_CT: no connection context");
        // assert(false); This is routine. TODO: Is that a problem?
    }
}

/**
 * This initialization function will be invoked when the router core is ready for the protocol
 * adaptor to be created.  This function must:
 *
 *   1) Register the protocol adaptor with the router-core.
 *   2) Prepare the protocol adaptor to be configured.
 */
static void qdr_tcp_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    qd_adaptor_common_init();
    qdr_tcp_adaptor_t *adaptor = NEW(qdr_tcp_adaptor_t);
    ZERO(adaptor);
    adaptor->core    = core;
    adaptor->adaptor = qdr_protocol_adaptor(core,
                                            "tcp",                // name
                                            adaptor,              // context
                                            qdr_tcp_activate_CT,  // runs on Core thread
                                            qdr_tcp_first_attach,
                                            qdr_tcp_second_attach,
                                            qdr_tcp_detach,
                                            qdr_tcp_flow,
                                            qdr_tcp_offer,
                                            qdr_tcp_drained,
                                            qdr_tcp_drain,
                                            qdr_tcp_push,
                                            qdr_tcp_deliver,
                                            qdr_tcp_get_credit,
                                            qdr_tcp_delivery_update,
                                            qdr_tcp_conn_close,
                                            qdr_tcp_conn_trace);
    DEQ_INIT(adaptor->listeners);
    DEQ_INIT(adaptor->connectors);
    DEQ_INIT(adaptor->tcp_connections);
    sys_mutex_init(&adaptor->listener_lock);
    *adaptor_context = adaptor;

    // ISSUE-1152: Check if half-close idle timeout enabled via the environment. If the value is a positive integer, use
    // it for the timeout value (in seconds).
    //
    char *half_close_enabled = getenv("SKUPPER_ROUTER_ENABLE_1152");
    if (half_close_enabled) {
        int timeout;
        // This default is based on 2x the conventional maximum segment lifetime (2 minutes)
        half_closed_idle_timeout = 240000;
        if (sscanf(half_close_enabled, "%d", &timeout) == 1) {
            if (timeout > 0) {
                half_closed_idle_timeout = timeout * 1000;
            }
        }
    }

    tcp_adaptor = adaptor;
}

static void qdr_tcp_adaptor_final(void *adaptor_context)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Shutting down TCP protocol adaptor");
    qdr_tcp_adaptor_t *adaptor = (qdr_tcp_adaptor_t*) adaptor_context;

    while (!DEQ_IS_EMPTY(adaptor->tcp_connections)) {
        qdr_tcp_connection_ref_t *conn_ref = DEQ_HEAD(adaptor->tcp_connections);
        DEQ_REMOVE_HEAD(adaptor->tcp_connections);
        free_qdr_tcp_connection_ref_t(conn_ref);
    }

    qd_tcp_listener_t *listener = DEQ_HEAD(adaptor->listeners);
    while (listener) {
        qd_tcp_listener_t *next = DEQ_NEXT(listener);
        //
        // Deliberately call qd_tcp_listener_incref() to make sure that freeing the connections, don't
        // free the listener. Then we call qd_tcp_listener_free() to forcefully free the listener without checking the listener->ref_count
        //
        qd_tcp_listener_incref(listener);
        qdr_tcp_connection_t *conn = DEQ_HEAD(listener->connections);
        while (conn) {
            qdr_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            free_qdr_tcp_connection(conn);
            conn = next_conn;
        }
        qd_tcp_listener_free(listener);
        listener = next;
    }

    qd_tcp_connector_t *connector = DEQ_HEAD(adaptor->connectors);
    while (connector) {
        qd_tcp_connector_t *next = DEQ_NEXT(connector);
        //
        // Deliberately call qd_tcp_connector_incref() to make sure that freeing the connections, don't
        // free the listener. Then we call qd_tcp_listener_free() to forcefully free the listener without checking the listener->ref_count
        //
        qd_tcp_connector_incref(connector);
        qdr_tcp_connection_t *conn = DEQ_HEAD(connector->connections);
        while (conn) {
            qdr_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            free_qdr_tcp_connection(conn);
            conn = next_conn;
        }
        qd_tcp_connector_free(connector);
        connector = next;
    }

    qdr_protocol_adaptor_free(adaptor->core, adaptor->adaptor);
    tcp_adaptor =  NULL;
    sys_mutex_free(&adaptor->listener_lock);
    free(adaptor);
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("tcp-adaptor", qdr_tcp_adaptor_init, qdr_tcp_adaptor_final)

#define QDR_TCP_CONNECTION_NAME                   0
#define QDR_TCP_CONNECTION_IDENTITY               1
#define QDR_TCP_CONNECTION_ADDRESS                2
#define QDR_TCP_CONNECTION_HOST                   3
#define QDR_TCP_CONNECTION_DIRECTION              4
#define QDR_TCP_CONNECTION_BYTES_IN               5
#define QDR_TCP_CONNECTION_BYTES_OUT              6
#define QDR_TCP_CONNECTION_UPTIME_SECONDS         7
#define QDR_TCP_CONNECTION_LAST_IN_SECONDS        8
#define QDR_TCP_CONNECTION_LAST_OUT_SECONDS       9


const char * const QDR_TCP_CONNECTION_DIRECTION_IN  = "in";
const char * const QDR_TCP_CONNECTION_DIRECTION_OUT = "out";

const char *qdr_tcp_connection_columns[] =
    {"name",
     "identity",
     "address",
     "host",
     "direction",
     "bytesIn",
     "bytesOut",
     "uptimeSeconds",
     "lastInSeconds",
     "lastOutSeconds",
     0};

const char *TCP_CONNECTION_TYPE = "io.skupper.router.tcpConnection";

static void insert_column(qdr_core_t *core, qdr_tcp_connection_t *conn, int col, qd_composed_field_t *body)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "Insert column %i for %p", col, (void *) conn);
    char id_str[100];

    if (!conn)
        return;

    switch(col) {
    case QDR_TCP_CONNECTION_NAME:
        qd_compose_insert_string(body, conn->global_id);
        break;

    case QDR_TCP_CONNECTION_IDENTITY: {
        snprintf(id_str, 100, "%"PRId64, conn->conn_id);
        qd_compose_insert_string(body, id_str);
        break;
    }

    case QDR_TCP_CONNECTION_ADDRESS:
        qd_compose_insert_string(body, conn->config->adaptor_config->address);
        break;

    case QDR_TCP_CONNECTION_HOST:
        qd_compose_insert_string(body, conn->remote_address);
        break;

    case QDR_TCP_CONNECTION_DIRECTION:
        if (conn->ingress)
            qd_compose_insert_string(body, QDR_TCP_CONNECTION_DIRECTION_IN);
        else
            qd_compose_insert_string(body, QDR_TCP_CONNECTION_DIRECTION_OUT);
        break;

    case QDR_TCP_CONNECTION_BYTES_IN:
        qd_compose_insert_uint(body, conn->bytes_in);
        break;

    case QDR_TCP_CONNECTION_BYTES_OUT:
        qd_compose_insert_uint(body, conn->bytes_out);
        break;

    case QDR_TCP_CONNECTION_UPTIME_SECONDS:
        qd_compose_insert_uint(body, qdr_core_uptime_ticks(core) - conn->opened_time);
        break;

    case QDR_TCP_CONNECTION_LAST_IN_SECONDS:
        if (conn->last_in_time==0)
            qd_compose_insert_null(body);
        else
            qd_compose_insert_uint(body, qdr_core_uptime_ticks(core) - conn->last_in_time);
        break;

    case QDR_TCP_CONNECTION_LAST_OUT_SECONDS:
        if (conn->last_out_time==0)
            qd_compose_insert_null(body);
        else
            qd_compose_insert_uint(body, qdr_core_uptime_ticks(core) - conn->last_out_time);
        break;

    }
}


static void write_list(qdr_core_t *core, qdr_query_t *query,  qdr_tcp_connection_t *conn)
{
    qd_composed_field_t *body = query->body;

    qd_compose_start_list(body);

    if (conn) {
        int i = 0;
        while (query->columns[i] >= 0) {
            insert_column(core, conn, query->columns[i], body);
            i++;
        }
    }
    qd_compose_end_list(body);
}

static void write_map(qdr_core_t           *core,
                      qdr_tcp_connection_t *conn,
                      qd_composed_field_t  *body,
                      const char           *qdr_connection_columns[])
{
    qd_compose_start_map(body);

    for(int i = 0; i < QDR_TCP_CONNECTION_COLUMN_COUNT; i++) {
        qd_compose_insert_string(body, qdr_connection_columns[i]);
        insert_column(core, conn, i, body);
    }

    qd_compose_end_map(body);
}

static void advance(qdr_query_t *query, qdr_tcp_connection_ref_t *conn_ref)
{
    if (conn_ref) {
        query->next_offset++;
        conn_ref = DEQ_NEXT(conn_ref);
        query->more = !!conn_ref;
    }
    else {
        query->more = false;
    }
}

static qdr_tcp_connection_t *find_by_identity(qdr_core_t *core, qd_iterator_t *identity)
{
    if (!identity)
        return 0;

    qdr_tcp_connection_ref_t *conn_ref = DEQ_HEAD(tcp_adaptor->tcp_connections);
    while (conn_ref) {
        // Convert the passed in identity to a char*
        char id[100];
        snprintf(id, 100, "%"PRId64, conn_ref->conn->conn_id);
        if (qd_iterator_equal(identity, (const unsigned char*) id))
            break;
        conn_ref = DEQ_NEXT(conn_ref);
    }

    return conn_ref->conn;

}

void qdra_tcp_connection_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "query for first tcp connection (%i)", offset);
    query->status = QD_AMQP_OK;

    if (offset >= DEQ_SIZE(tcp_adaptor->tcp_connections)) {
        query->more = false;
        qdr_agent_enqueue_response_CT(core, query);
        return;
    }

    qdr_tcp_connection_ref_t *conn_ref = DEQ_HEAD(tcp_adaptor->tcp_connections);
    for (int i = 0; i < offset && conn_ref; i++)
        conn_ref = DEQ_NEXT(conn_ref);
    assert(conn_ref);

    if (conn_ref) {
        write_list(core, query, conn_ref->conn);
        query->next_offset = offset;
        advance(query, conn_ref);
    } else {
        query->more = false;
    }

    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_tcp_connection_get_next_CT(qdr_core_t *core, qdr_query_t *query)
{
    qdr_tcp_connection_ref_t *conn_ref = 0;

    if (query->next_offset < DEQ_SIZE(tcp_adaptor->tcp_connections)) {
        conn_ref = DEQ_HEAD(tcp_adaptor->tcp_connections);
        for (int i = 0; i < query->next_offset && conn_ref; i++)
            conn_ref = DEQ_NEXT(conn_ref);
    }

    if (conn_ref) {
        write_list(core, query, conn_ref->conn);
        advance(query, conn_ref);
    } else {
        query->more = false;
    }
    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_tcp_connection_get_CT(qdr_core_t         *core,
                               qd_iterator_t       *name,
                               qd_iterator_t       *identity,
                               qdr_query_t         *query,
                               const char          *qdr_tcp_connection_columns[])
{
    qdr_tcp_connection_t *conn = 0;

    if (!identity) {
        query->status = QD_AMQP_BAD_REQUEST;
        query->status.description = "Name not supported. Identity required";
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Error performing READ of %s: %s", TCP_CONNECTION_TYPE,
               query->status.description);
    } else {
        conn = find_by_identity(core, identity);

        if (conn == 0) {
            query->status = QD_AMQP_NOT_FOUND;
        } else {
            write_map(core, conn, query->body, qdr_tcp_connection_columns);
            query->status = QD_AMQP_OK;
        }
    }
    qdr_agent_enqueue_response_CT(core, query);
}

static void qdr_add_tcp_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard) {
        qdr_tcp_connection_t *conn = (qdr_tcp_connection_t*) action->args.general.context_1;
        qdr_add_tcp_connection_ref(&tcp_adaptor->tcp_connections, conn);
        conn->in_list = true;
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "] qdr_add_tcp_connection_CT %s (%zu)",
               conn->conn_id, conn->config->adaptor_config->host_port, DEQ_SIZE(tcp_adaptor->tcp_connections));
    }
}

static void qdr_del_tcp_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard) {
        qdr_tcp_connection_t *conn = (qdr_tcp_connection_t*) action->args.general.context_1;
        if (conn->in_list) {
            qdr_del_tcp_connection_ref(&tcp_adaptor->tcp_connections, conn);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   "[C%" PRIu64 "] qdr_del_tcp_connection_CT %s deleted. bytes_in=%" PRIu64 ", bytes_out=%" PRId64
                   ", "
                   "opened_time=%" PRId64 ", last_in_time=%" PRId64 ", last_out_time=%" PRId64
                   ". Connections remaining %zu",
                   conn->conn_id, conn->config->adaptor_config->host_port, conn->bytes_in, conn->bytes_out,
                   conn->opened_time, conn->last_in_time, conn->last_out_time, DEQ_SIZE(tcp_adaptor->tcp_connections));
        }
        free_qdr_tcp_connection(conn);
    }
}


static void detach_links(qdr_tcp_connection_t *conn)
{
    if (conn->incoming_link) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] detaching incoming link",
               conn->conn_id, conn->incoming_link_id);
        qdr_link_detach(conn->incoming_link, QD_LOST, 0);
        conn->incoming_link = 0;
    }
    if (conn->outgoing_link) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] detaching outgoing link",
               conn->conn_id, conn->outgoing_link_id);
        qdr_link_detach(conn->outgoing_link, QD_LOST, 0);
        conn->outgoing_link = 0;
    }
}

// Half-closed idle timer handler. Note this callback may run in parallel with an I/O thread handling events for this
// connection. It is safe to access the raw connection since this timer will be cancelled before the raw connection is
// closed.
//
static void on_idle_timer(void *context)
{
    qdr_tcp_connection_t *conn = (qdr_tcp_connection_t *) context;
    SET_ATOMIC_FLAG(&conn->half_closed_expired);
    pn_raw_connection_wake(conn->pn_raw_conn);
}

// ISSUE-1152: the connection has entered half-closed state.
//
static void start_half_closed_monitoring(qdr_tcp_connection_t *conn)
{
    if (half_closed_idle_timeout) {
        assert(tcp_conn_is_half_closed(conn));
        assert(!conn->half_closed_timer);
        conn->half_closed_bytes = conn->bytes_in + conn->bytes_out;
        conn->half_closed_timer = qd_timer(tcp_adaptor->core->qd, on_idle_timer, conn);
        qd_timer_schedule(conn->half_closed_timer, half_closed_idle_timeout);
    }
}

static void stop_half_closed_monitoring(qdr_tcp_connection_t *conn)
{
    if (conn->half_closed_timer) {
        qd_timer_cancel(conn->half_closed_timer);
    }
}

static bool check_half_closed_timeout(qdr_tcp_connection_t *conn)
{
    bool is_idle = false;

    if (CLEAR_ATOMIC_FLAG(&conn->half_closed_expired)) {
        uint64_t bytes_now = conn->bytes_in + conn->bytes_out;
        if (conn->half_closed_bytes == bytes_now) {
            // no traffic passed during idle time
            is_idle = true;
        } else {
            // not idle, try again later
            conn->half_closed_bytes = bytes_now;
            qd_timer_schedule(conn->half_closed_timer, half_closed_idle_timeout);
        }
    }

    return is_idle;
}

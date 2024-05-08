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

#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/amqp.h>
#include <qpid/dispatch/enum.h>
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/server.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/platform.h>
#include <qpid/dispatch/connection_counters.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>
#include <proton/listener.h>

#include "tcp_adaptor.h"

#include <stdatomic.h>

//
// Function suffixes in this module:
//
//    _CSIDE_IO  - Raw-connection IO thread, connector-side
//    _LSIDE_IO  - Raw-connection IO thread, listener-side
//    _XSIDE_IO  - Raw-connection IO thread, either-side
//    _TIMER_IO  - Connectionless IO thread
//    _CSIDE     - Timer IO thread, connector-side
//    _IO        - Any IO thread
//

ALLOC_DEFINE(qd_tcp_listener_t);
ALLOC_DEFINE(qd_tcp_connector_t);
ALLOC_DEFINE_SAFE(qd_tcp_connection_t);

static const char *const state_names[] =
{
    [LSIDE_INITIAL]       = "LSIDE_INITIAL",
    [LSIDE_TLS_HANDSHAKE] = "LSIDE_TLS_HANDSHAKE",
    [LSIDE_LINK_SETUP]    = "LSIDE_LINK_SETUP",
    [LSIDE_STREAM_START]  = "LSIDE_STREAM_START",
    [LSIDE_FLOW]          = "LSIDE_FLOW",
    [LSIDE_TLS_FLOW]      = "LSIDE_TLS_FLOW",

    [CSIDE_INITIAL]       = "CSIDE_INITIAL",
    [CSIDE_LINK_SETUP]    = "CSIDE_LINK_SETUP",
    [CSIDE_FLOW]          = "CSIDE_FLOW",
    [CSIDE_TLS_FLOW]      = "CSIDE_TLS_FLOW",

    [XSIDE_CLOSING] = "XSIDE_CLOSING"
};
ENUM_DEFINE(qd_tcp_connection_state, state_names);

#define CONNECTION_CLOSE_TIME 10000
#define RAW_BUFFER_BATCH_SIZE 16

//
// Global Adaptor State
//
typedef struct {
    qdr_core_t                *core;
    qd_dispatch_t             *qd;
    qd_server_t               *server;
    qdr_protocol_adaptor_t    *pa;
    qd_tcp_listener_list_t    listeners;
    qd_tcp_connector_list_t   connectors;
    qd_tcp_connection_list_t  connections;
    sys_mutex_t                lock;
    pn_proactor_t             *proactor;
} qd_tcp_context_t;

static qd_tcp_context_t *tcp_context;

static uint64_t buffer_ceiling = 0;
static uint64_t buffer_threshold_50;
static uint64_t buffer_threshold_75;
static uint64_t buffer_threshold_85;

// Window Flow Control
//
// This adaptor uses a simple window with acknowledge algorithm to enforce backpressure on the TCP sender. The ingress
// adaptor will only permit up to TCP_MAX_CAPACITY_BYTES bytes received before backpressuring the sender by no longer
// granting empty receive buffers to the raw connection. The egress adapter will send its count of total bytes written
// out the raw connection every TCP_ACK_THRESHOLD_BYTES written bytes. The egress sends this update via a PN_RECEIVED
// frame, setting the section_offset field to the total number of bytes written out the raw connection. When the
// PN_RECEIVED update is received at the ingress adaptor it will update the window size by subtracting the
// section_offset from its total received bytes counter. If the result of the subtraction is less than TCP_MAX_CAPACITY
// then backpressure is relieved and empty receive buffers are given to the raw connection.
//
// TCP_MAX_CAPACITY_BYTES: this is set to 2x the maximum number of bytes a cut through message can buffer. This makes
// the window large enough to max out a message at each end of the TCP flow.
//
#define TCP_FULL_MSG_BYTES      (QD_BUFFER_DEFAULT_SIZE * UCT_SLOT_COUNT * UCT_SLOT_BUF_LIMIT)
#define TCP_MAX_CAPACITY_BYTES  (TCP_FULL_MSG_BYTES * UINT64_C(2))
#define TCP_ACK_THRESHOLD_BYTES TCP_FULL_MSG_BYTES

// is the incoming byte window full?
//
inline static bool window_full(const qd_tcp_connection_t *conn)
{
    return !conn->window.disabled && (conn->inbound_octets - conn->window.last_update) >= TCP_MAX_CAPACITY_BYTES;
}

//
// Forward References
//
static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context);
static void connection_run_LSIDE_IO(qd_tcp_connection_t *conn);
static void connection_run_CSIDE_IO(qd_tcp_connection_t *conn);
static void connection_run_XSIDE_IO(qd_tcp_connection_t *conn);
static uint64_t validate_outbound_message(const qdr_delivery_t *out_dlv);
static void on_accept(qd_adaptor_listener_t *listener, pn_listener_t *pn_listener, void *context);
static void on_tls_connection_secured(qd_tls_t *tls, void *user_context);
static char *get_tls_negotiated_alpn(qd_message_t *msg);  // caller must free() returned string!
static int setup_tls_session(qd_tcp_connection_t *conn, const qd_tls_domain_t *parent_domain, const char *alpn_protocol);
static void free_tcp_resource(qd_tcp_common_t *resource);

//=================================================================================
// Thread assertions
//=================================================================================
typedef enum {
    THREAD_UNKNOWN,
    THREAD_ROUTER_CORE,
    THREAD_TIMER_IO,
    THREAD_RAW_IO
} qd_tcp_thread_state_t;

__thread qd_tcp_thread_state_t tcp_thread_state;

#define SET_THREAD_UNKNOWN     tcp_thread_state = THREAD_UNKNOWN
#define SET_THREAD_ROUTER_CORE tcp_thread_state = THREAD_ROUTER_CORE
#define SET_THREAD_TIMER_IO    tcp_thread_state = THREAD_TIMER_IO
#define SET_THREAD_RAW_IO      tcp_thread_state = THREAD_RAW_IO

#define ASSERT_ROUTER_CORE assert(tcp_thread_state == THREAD_ROUTER_CORE || tcp_thread_state == THREAD_UNKNOWN)
#define ASSERT_TIMER_IO    assert(tcp_thread_state == THREAD_TIMER_IO || tcp_thread_state == THREAD_UNKNOWN)
#define ASSERT_RAW_IO      assert(tcp_thread_state == THREAD_RAW_IO || tcp_thread_state == THREAD_UNKNOWN)


//=================================================================================
// incref/decref functions.
//=================================================================================
static void qd_tcp_listener_incref(qd_tcp_listener_t *listener)
{
    sys_atomic_inc(&listener->ref_count);
}

static void qd_tcp_connector_incref(qd_tcp_connector_t *connector)
{
    sys_atomic_inc(&connector->ref_count);
}

/**
 * NOTE: Do not call this function directly. This function should only be called directly from ADAPTOR_final().
 * To free the listener, call qd_tcp_listener_decref which will check the listener's ref_count before freeing it.
 */
static void qd_tcp_listener_free(qd_tcp_listener_t *listener)
{
    sys_mutex_lock(&tcp_context->lock);
    DEQ_REMOVE(tcp_context->listeners, listener);
    sys_mutex_unlock(&tcp_context->lock);
    //
    // This call to vflow_end_record is only here to doubly make sure that any future calls to qd_tcp_listener_decref
    // will end the vflow record if it has not already ended and zeroed out.
    //
    if (listener->common.vflow) {
        vflow_end_record(listener->common.vflow);
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Deleted TcpListener for %s, %s:%s",
            listener->adaptor_config->address, listener->adaptor_config->host, listener->adaptor_config->port);

    qdpo_free(listener->protocol_observer);
    qdpo_config_free(listener->protocol_observer_config);

    qd_tls_domain_decref(listener->tls_domain);
    qd_free_adaptor_config(listener->adaptor_config);
    sys_mutex_free(&listener->lock);
    free_qd_tcp_listener_t(listener);
}

static void qd_tcp_listener_decref(qd_tcp_listener_t *listener)
{
    if (listener && sys_atomic_dec(&listener->ref_count) == 1) {
        qd_tcp_listener_free(listener);
    }
}

/**
 * NOTE: Do not call this function directly. This function should only be called directly from ADAPTOR_final().
 * To free the connector, call qd_tcp_connector_decref which will check the connector's ref_count before freeing it.
 */
static void qd_tcp_connector_free(qd_tcp_connector_t *connector)
{
    // Disable activation by the Core thread.
    sys_mutex_lock(&connector->lock);
    qd_timer_free(connector->activate_timer);
    connector->activate_timer = 0;
    sys_mutex_unlock(&connector->lock);
    // Do NOT free the connector->lock mutex since the core may be holding it.

    sys_mutex_lock(&tcp_context->lock);
    DEQ_REMOVE(tcp_context->connectors, connector);
    sys_mutex_unlock(&tcp_context->lock);

    vflow_end_record(connector->common.vflow);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Deleted TcpConnector for %s, %s:%s",
            connector->adaptor_config->address, connector->adaptor_config->host, connector->adaptor_config->port);

    qd_tls_domain_decref(connector->tls_domain);
    qd_free_adaptor_config(connector->adaptor_config);

    // Pass connector to Core for final deallocation. The Core will free the cr->lock.
    // see qdr_core_free_tcp_resource_CT()
    free_tcp_resource(&connector->common);
}

static void qd_tcp_connector_decref(qd_tcp_connector_t *connector)
{
    if (connector && sys_atomic_dec(&connector->ref_count) == 1) {
        qd_tcp_connector_free(connector);
    }
}


//=================================================================================
// Core Activation Handler
//=================================================================================
/**
 * This function in invoked in a timer thread, not associated with any IO context, in order to process core connections
 * terminated in the adaptor.  The core connections processed here are for connectors only.  Connection activation
 * happens elsewhere, in the context of a Proton raw IO connection.
 */
static void on_core_activate_TIMER_IO(void *context)
{
    SET_THREAD_TIMER_IO;
    assert(((qd_tcp_common_t*) context)->context_type == TL_CONNECTOR);
    qdr_connection_t *core_conn = ((qd_tcp_connector_t*) context)->core_conn;
    qdr_connection_process(core_conn);
}


//=================================================================================
// Helper Functions
//=================================================================================
static pn_data_t *TL_conn_properties(void)
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


static qdr_connection_t *TL_open_core_connection(uint64_t conn_id, bool incoming, const char *host)
{
    qdr_connection_t *conn;

    //
    // The qdr_connection_info() function makes its own copy of the passed in tcp_conn_properties.
    // So, we need to call pn_data_free(properties)
    //
    pn_data_t *properties       = TL_conn_properties();
    qdr_connection_info_t *info = qdr_connection_info(false,        // is_encrypted,
                                                      false,        // is_authenticated,
                                                      true,         // opened,
                                                      "",           // sasl_mechanisms,
                                                      incoming ? QD_INCOMING : QD_OUTGOING,  // dir,
                                                      host,
                                                      "",           // ssl_proto,
                                                      "",           // ssl_cipher,
                                                      "",           // user,
                                                      "TcpAdaptor", // container,
                                                      properties,   // connection_properties,
                                                      0,            // ssl_ssf,
                                                      false,        // ssl,
                                                      "",           // peer router version,
                                                      true,         // streaming links
                                                      false);       // connection trunking
    pn_data_free(properties);

    conn = qdr_connection_opened(tcp_context->core,
                                 tcp_context->pa,
                                 incoming,        // incoming
                                 QDR_ROLE_NORMAL, // role
                                 1,               // cost
                                 conn_id,         // management_id
                                 0,               // label
                                 0,               // remote_container_id
                                 false,           // strip_annotations_in
                                 false,           // strip_annotations_out
                                 5,               // link_capacity
                                 0,               // policy_spec
                                 info,            // connection_info
                                 0,               // context_binder
                                 0);              // bind_token
    qd_connection_counter_inc(QD_PROTOCOL_TCP);
    return conn;
}


static void TL_setup_listener(qd_tcp_listener_t *li)
{
    //
    // Create a vflow record for this listener
    //
    li->common.vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_PROTOCOL,         "tcp");
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_NAME,             li->adaptor_config->name);
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, li->adaptor_config->host);
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, li->adaptor_config->port);
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      li->adaptor_config->address);
    vflow_set_uint64(li->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4,    0);
    vflow_add_rate(li->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, VFLOW_ATTRIBUTE_FLOW_RATE_L4);

    //
    // Set up the protocol observer
    //
    // TODO - add configuration to the listener to influence whether and how the observer is set up.
    //
    li->protocol_observer_config = qdpo_config(0, true);
    li->protocol_observer = protocol_observer(QD_PROTOCOL_TCP, li->protocol_observer_config);

    //
    // Create an adaptor listener. This listener will automatically create a listening socket when there is at least one
    // consumer for the service address. Once the last consumer for the service address goes away the adaptor listener
    // will automatically close the listening socket. When a client connects to the listening socket the "on_accept"
    // callback will be invoked on the proactor listener thread.
    //
    li->adaptor_listener = qd_adaptor_listener(tcp_context->qd, li->adaptor_config, LOG_TCP_ADAPTOR);
    qd_adaptor_listener_listen(li->adaptor_listener, on_accept, li);
}


static void TL_setup_connector(qd_tcp_connector_t *connector)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this connector
    //
    connector->conn_id   = qd_server_allocate_connection_id(tcp_context->server);
    connector->core_conn = TL_open_core_connection(connector->conn_id, false, "egress-dispatch");
    qdr_connection_set_context(connector->core_conn, connector);
    connector->connections_opened = 1;  // for legacy compatibility: it counted the egress-dispatch conn

    //
    // Attach an out-link to represent our desire to receive connection streams for the address
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, connector->adaptor_config->address);

    //
    // Create a vflow record for this connector
    //
    connector->common.vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(connector->common.vflow, VFLOW_ATTRIBUTE_PROTOCOL,         "tcp");
    vflow_set_string(connector->common.vflow, VFLOW_ATTRIBUTE_NAME,             connector->adaptor_config->name);
    vflow_set_string(connector->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, connector->adaptor_config->host);
    vflow_set_string(connector->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, connector->adaptor_config->port);
    vflow_set_string(connector->common.vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      connector->adaptor_config->address);
    vflow_set_uint64(connector->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4,    0);
    vflow_add_rate(connector->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, VFLOW_ATTRIBUTE_FLOW_RATE_L4);

    connector->out_link = qdr_link_first_attach(connector->core_conn, QD_OUTGOING, source, 0, "tcp.connector.out", 0, false, 0, &connector->link_id);
    qdr_link_set_user_streaming(connector->out_link);
    qdr_link_set_context(connector->out_link, connector);
    qdr_link_flow(tcp_context->core, connector->out_link, 5, false);
}


static void drain_read_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn)
{
    ASSERT_RAW_IO;
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;

    while ((count = pn_raw_connection_take_read_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            qd_buffer_free(buf);
        }
    }
}


static void drain_write_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn)
{
    ASSERT_RAW_IO;
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;

    while ((count = pn_raw_connection_take_written_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            if (!!buf) {
                qd_buffer_free(buf);
            }
        }
    }
}


static void set_state_XSIDE_IO(qd_tcp_connection_t *conn, qd_tcp_connection_state_t new_state)
{
    ASSERT_RAW_IO;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] State change %s -> %s",
           conn->conn_id, qd_tcp_connection_state_name(conn->state), qd_tcp_connection_state_name(new_state));
    conn->state = new_state;
}

//
// Connector/Connection cleanup
//
// Both tcp_connector_t and tcp_connection_t allocate a core qdr_connection_t instance which is used by the Core
// thread for activation (see CORE_activate()).  During cleanup of these objects we need to ensure that both the I/O and
// Core threads do not reference them after they have been deallocated. To do this we use a two-phase approach to
// freeing these objects. In the first phase all non-activation-related resources are released by the I/O thread (see
// qd_tcp_connector_decref(), free_connection_IO). Then the object is passed to the Core thread for cleanup of the activation
// resources and freeing the base object (see free_tcp_resource(), qdr_core_free_tcp_resource_CT()).
//
// tcp_listener_t does not use a qdr_connection_t so this process does not apply to it.
//

static void qdr_core_free_tcp_resource_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    // note: need to release the resource regardless of discard flag
    qd_tcp_common_t *common = (qd_tcp_common_t *)action->args.general.context_1;
    if (common->context_type == TL_CONNECTION) {
        qd_tcp_connection_t *conn = (qd_tcp_connection_t*) common;
        sys_atomic_destroy(&conn->core_activation);
        sys_atomic_destroy(&conn->raw_opened);
        sys_mutex_free(&conn->activation_lock);
        free_qd_tcp_connection_t(conn);
    } else {
        // Core does not hold a reference to a listener so they are not freed here
        assert(common->context_type == TL_CONNECTOR);
        qd_tcp_connector_t *cr = (qd_tcp_connector_t *)common;
        sys_mutex_free(&cr->lock);
        free_qd_tcp_connector_t(cr);
    }
}

static void free_tcp_resource(qd_tcp_common_t *resource)
{
    assert(tcp_context);
    qdr_action_t *action           = qdr_action(qdr_core_free_tcp_resource_CT, "core free tcp resource");
    action->args.general.context_1 = resource;
    qdr_action_enqueue(tcp_context->core, action);
}

static void free_connection_IO(void *context)
{
    // No thread assertion here - can be RAW_IO or TIMER_IO
    qd_tcp_connection_t *conn = (qd_tcp_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Cleaning up resources", conn->conn_id);

    // Disable activation via Core thread. The lock needs to be taken to ensure the core thread is not currently
    // attempting to activate the connection: after the mutex is unlocked we're guaranteed no further activations can
    // take place.
    sys_mutex_lock(&conn->activation_lock);
    CLEAR_ATOMIC_FLAG(&conn->raw_opened);
    sys_mutex_unlock(&conn->activation_lock);
    // Do NOT free the core_activation lock since the core may be holding it

    if (conn->observer_handle) {
        qdpo_end(conn->observer_handle);
        conn->observer_handle = 0;
    }

    if (conn->common.parent) {
        if (conn->common.parent->context_type == TL_LISTENER) {
            qd_tcp_listener_t *listener = (qd_tcp_listener_t*) conn->common.parent;
            sys_mutex_lock(&listener->lock);
            DEQ_REMOVE(listener->connections, conn);
            sys_mutex_unlock(&listener->lock);
            //
            // Call listener decref when a connection associated with the listener is removed (DEQ_REMOVE(listener->connections, conn))
            //
            conn->common.parent = 0;
            qd_tcp_listener_decref(listener);
        } else {
            qd_tcp_connector_t *connector = (qd_tcp_connector_t*) conn->common.parent;
            sys_mutex_lock(&connector->lock);
            DEQ_REMOVE(connector->connections, conn);
            sys_mutex_unlock(&connector->lock);
            //
            // Call connector decref when a connection associated with the connector is removed (DEQ_REMOVE(connector->connections, conn))
            //
            conn->common.parent = 0;
            qd_tcp_connector_decref(connector);
        }
    }

    // Pass connection to Core for final deallocation. The Core will free the activation_lock and the related flags.  See
    // qdr_core_free_tcp_resource_CT()
    free_tcp_resource(&conn->common);
}

// Initate close of the raw connection.
//
// The close will be complete when the PN_RAW_CONNECTION_DISCONNECTED event is handled. At that point any associated
// connection condition information will be read from the raw conn and written to the flow log.
//
// @param conn Holds the raw connection to close
// @param condition Optional condition identifying the reason the connection was closed
// @param description Optional description assocated with condition
//
static void close_raw_connection(qd_tcp_connection_t *conn, const char *condition, const char *description)
{
    ASSERT_RAW_IO;

    assert(conn->raw_conn);
    if (condition) {
        pn_condition_t *cond = pn_raw_connection_condition(conn->raw_conn);
        if (!!cond) {
            (void) pn_condition_set_name(cond, condition);
            if (description) {
                (void) pn_condition_set_description(cond, description);
            }
        }
    }

    CLEAR_ATOMIC_FLAG(&conn->raw_opened);
    pn_raw_connection_close(conn->raw_conn);

    // Connection cleanup occurs on the PN_RAW_CONNECTION_DISCONNECTED event
}

static void close_connection_XSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    if (conn->state != XSIDE_CLOSING)
        set_state_XSIDE_IO(conn, XSIDE_CLOSING);

    if (!!conn->raw_conn) {
        CLEAR_ATOMIC_FLAG(&conn->raw_opened);
        pn_raw_connection_close(conn->raw_conn);
        drain_read_buffers_XSIDE_IO(conn->raw_conn);
        drain_write_buffers_XSIDE_IO(conn->raw_conn);

        // note: this disables the raw connection event handler. No further PN_RAW_CONNECTION_* events will occur,
        // including DISCONNECTED!
        sys_mutex_lock(&conn->activation_lock);
        pn_raw_connection_set_context(conn->raw_conn, 0);
        conn->raw_conn = 0;
        sys_mutex_unlock(&conn->activation_lock);
    }

    free(conn->reply_to);

    if (!!conn->inbound_stream) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP cancel producer activation", DLV_ARGS(conn->inbound_delivery));
        qd_message_cancel_producer_activation(conn->inbound_stream);
    }

    if (!!conn->outbound_stream) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP cancel consumer activation", DLV_ARGS(conn->outbound_delivery));
        qd_message_cancel_consumer_activation(conn->outbound_stream);
    }

    if (!!conn->inbound_delivery) {
        if (!!conn->inbound_stream) {
            qd_message_set_receive_complete(conn->inbound_stream);
            qdr_delivery_continue(tcp_context->core, conn->inbound_delivery, false);
        }

        qdr_delivery_remote_state_updated(tcp_context->core, conn->inbound_delivery, 0, true, 0, false);
        qdr_delivery_set_context(conn->inbound_delivery, 0);
        qdr_delivery_decref(tcp_context->core, conn->inbound_delivery, "close_connection_XSIDE_IO - inbound_delivery released");
    }

    if (!!conn->inbound_link) {
        qdr_link_detach(conn->inbound_link, QD_LOST, 0);
    }

    if (!!conn->outbound_delivery) {
        qdr_delivery_remote_state_updated(tcp_context->core, conn->outbound_delivery, PN_MODIFIED, true, 0, false);
        qdr_delivery_set_context(conn->outbound_delivery, 0);
        qdr_delivery_decref(tcp_context->core, conn->outbound_delivery, "close_connection_XSIDE_IO - outbound_delivery released");
    }

    if (!!conn->outbound_link) {
        qdr_link_detach(conn->outbound_link, QD_LOST, 0);
    }

    if (!!conn->core_conn) {
        qdr_connection_closed(conn->core_conn);
        conn->core_conn = 0;
        qd_connection_counter_dec(QD_PROTOCOL_TCP);
    }

    if (!!conn->common.vflow) {
        vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, conn->inbound_octets);
        vflow_end_record(conn->common.vflow);
    }

    qd_tls_free2(conn->tls);
    qd_tls_domain_decref(conn->tls_domain);
    free(conn->alpn_protocol);

    conn->reply_to          = 0;
    conn->inbound_link      = 0;
    conn->inbound_stream    = 0;
    conn->inbound_delivery  = 0;
    conn->outbound_link     = 0;
    conn->outbound_stream   = 0;
    conn->outbound_delivery = 0;
    conn->core_conn         = 0;
    conn->common.vflow      = 0;
    conn->tls               = 0;
    conn->tls_domain        = 0;

    if (conn->common.parent) {
        if (conn->common.parent->context_type == TL_LISTENER) {
            qd_tcp_listener_t *li = (qd_tcp_listener_t*) conn->common.parent;
            sys_mutex_lock(&li->lock);
            li->connections_closed++;
            sys_mutex_unlock(&li->lock);
        } else {
            qd_tcp_connector_t *cr = (qd_tcp_connector_t*) conn->common.parent;
            sys_mutex_lock(&cr->lock);
            cr->connections_closed++;
            sys_mutex_unlock(&cr->lock);
        }
    }

    free_connection_IO(conn);
}


static void grant_read_buffers_XSIDE_IO(qd_tcp_connection_t *conn, const size_t capacity)
{
    ASSERT_RAW_IO;

    //
    // Cannot grant read buffers if the connection is currently blocked due to window flow control
    //
    if (window_full(conn)) {
        return;
    }

    //
    // Define the allocation tiers.  The tier values are the number of read buffers to be granted
    // to raw connections based on the percentage of usage of the router-wide buffer ceiling.
    //
#define TIER_1 8  // [0% .. 50%)
#define TIER_2 4  // [50% .. 75%)
#define TIER_3 2  // [75% .. 85%)
#define TIER_4 1  // [85% .. 100%]

    //
    // Since we can't query Proton for the maximum read-buffer capacity, we will infer it from
    // calls to pn_raw_connection_read_buffers_capacity.
    //
    static atomic_size_t max_capacity;
    size_t current_mc = atomic_load(&max_capacity);
    while (capacity > current_mc) {
        if (atomic_compare_exchange_weak(&max_capacity, &current_mc, capacity))
            break;
    }

    //
    // Get the "held_by_threads" stats for router buffers as an approximation of how many
    // buffers are in-use.  This is an approximation since it also counts free buffers held
    // in the per-thread free-pools.  Since we will be dealing with large numbers here, the
    // number of buffers in free-pools will not be significant.
    //
    qd_alloc_stats_t  stats          = alloc_stats_qd_buffer_t();
    uint64_t          buffers_in_use = stats.held_by_threads;

    //
    // Choose the grant-allocation tier based on the number of buffers in use.
    //
    size_t desired = TIER_4;
    if (buffers_in_use < buffer_threshold_50) {
        desired = TIER_1;
    } else if (buffers_in_use < buffer_threshold_75) {
        desired = TIER_2;
    } else if (buffers_in_use < buffer_threshold_85) {
        desired = TIER_3;
    }

    //
    // Determine how many buffers are already granted.  This will always be a non-negative value.
    //
    current_mc = atomic_load(&max_capacity);
    assert(current_mc >= capacity);
    size_t already_granted = current_mc - capacity;

    //
    // If we desire to grant additional buffers, calculate the number to grant now.
    //
    const size_t granted = desired > already_granted ? desired - already_granted : 0;

    if (granted > 0) {
        //
        // Grant the buffers.
        //
        pn_raw_buffer_t raw_buffers[granted];

        for (size_t i = 0; i < granted; i++) {
            qd_buffer_t *buf = qd_buffer();
            raw_buffers[i].context  = (uintptr_t) buf;
            raw_buffers[i].bytes    = (char*) qd_buffer_base(buf);
            raw_buffers[i].capacity = qd_buffer_capacity(buf);
            raw_buffers[i].offset   = 0;
            raw_buffers[i].size     = 0;
        }

        size_t actual = pn_raw_connection_give_read_buffers(conn->raw_conn, raw_buffers, granted);
        (void) actual;
        assert(actual == granted);

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] grant_read_buffers_XSIDE_IO - %ld", conn->conn_id, granted);
    }
}


static uint64_t produce_read_buffers_XSIDE_IO(qd_tcp_connection_t *conn, qd_message_t *stream, bool *read_closed)
{
    ASSERT_RAW_IO;
    uint64_t octet_count = 0;
    *read_closed = false;

    if (qd_message_can_produce_buffers(stream)) {
        qd_buffer_list_t qd_buffers = DEQ_EMPTY;
        pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
        size_t           count;

        count = pn_raw_connection_take_read_buffers(conn->raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE);
        while (count > 0) {
            for (size_t i = 0; i < count; i++) {
                qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
                qd_buffer_insert(buf, raw_buffers[i].size);
                octet_count += raw_buffers[i].size;
                if (qd_buffer_size(buf) > 0) {
                    DEQ_INSERT_TAIL(qd_buffers, buf);
                    if (conn->listener_side && !!conn->observer_handle) {
                        qdpo_data(conn->observer_handle, true, qd_buffer_base(buf), qd_buffer_size(buf));
                    }
                } else {
                    qd_buffer_free(buf);
                }
            }
            count = pn_raw_connection_take_read_buffers(conn->raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE);
        }

        // ISSUE-1446: it is only safe to check pn_raw_connection_is_read_closed() after all read buffers are drained since
        // the connection can be marked closed while read buffers are still pending in the raw connection.
        //
        *read_closed = pn_raw_connection_is_read_closed(conn->raw_conn);

        if (!DEQ_IS_EMPTY(qd_buffers)) {
            //qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] produce_read_buffers_XSIDE_IO - Producing %ld buffers", conn->conn_id, DEQ_SIZE(qd_buffers));
            qd_message_produce_buffers(stream, &qd_buffers);
        }
    }

    return octet_count;
}


static uint64_t consume_write_buffers_XSIDE_IO(qd_tcp_connection_t *conn, qd_message_t *stream)
{
    ASSERT_RAW_IO;
    size_t   limit       = pn_raw_connection_write_buffers_capacity(conn->raw_conn);
    uint64_t octet_count = 0;

    if (limit > 0) {
        qd_buffer_list_t buffers = DEQ_EMPTY;
        size_t actual = qd_message_consume_buffers(stream, &buffers, limit);
        assert(actual == DEQ_SIZE(buffers));
        if (actual > 0) {
            pn_raw_buffer_t raw_buffers[actual];
            qd_buffer_t *buf = DEQ_HEAD(buffers);
            for (size_t i = 0; i < actual; i++) {
                if (conn->listener_side && !!conn->observer_handle) {
                    qdpo_data(conn->observer_handle, false, qd_buffer_base(buf), qd_buffer_size(buf));
                }
                raw_buffers[i].context  = (uintptr_t) buf;
                raw_buffers[i].bytes    = (char*) qd_buffer_base(buf);
                raw_buffers[i].capacity = qd_buffer_capacity(buf);
                raw_buffers[i].size     = qd_buffer_size(buf);
                raw_buffers[i].offset   = 0;
                octet_count += raw_buffers[i].size;
                buf = DEQ_NEXT(buf);
            }
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] consume_write_buffers_XSIDE_IO - Consuming %ld buffers", conn->conn_id, actual);
            pn_raw_connection_write_buffers(conn->raw_conn, raw_buffers, actual);
        }
    }

    return octet_count;
}

// Alternative to consume_message_body_XSIDE_IO() for use with TLS connections. The TLS layer takes ownership of all
// output message buffers and will free them as they are processed. Due to that we need to make a copy of these buffers
// in order to avoid freeing buffers that are part of the message (double-free).
//
static void copy_message_body_TLS_XSIDE_IO(qd_tcp_connection_t *conn, qd_message_t *stream, qd_buffer_list_t *buffers, size_t limit)
{
    size_t   offset = 0;

    assert(conn->tls);

    if (!conn->outbound_body) {
        assert(!conn->outbound_body_complete);
        qd_message_get_raw_body_data(stream, &conn->outbound_body, &offset);
    }

    while (limit-- && conn->outbound_body) {
        size_t size = qd_buffer_size(conn->outbound_body) - offset;
        if (size > 0) {
            qd_buffer_t *clone = qd_buffer();
            clone->size = size;
            memcpy(qd_buffer_base(clone), qd_buffer_base(conn->outbound_body) + offset, size);
            DEQ_INSERT_TAIL(*buffers, clone);
        }
        offset = 0;
        conn->outbound_body = DEQ_NEXT(conn->outbound_body);
    }

    if (!conn->outbound_body) {
        conn->outbound_body_complete = true;
        qd_message_release_raw_body(stream);
    }
}

static uint64_t consume_message_body_XSIDE_IO(qd_tcp_connection_t *conn, qd_message_t *stream)
{
    ASSERT_RAW_IO;
    assert(!conn->outbound_body_complete);

    uint64_t octets = 0;
    size_t   offset = 0;

    if (!conn->outbound_body) {
        // Get the pointer to the buffer containing the first octet of the body and the offset to that octet.  If there
        // are no body octets, we will be given a NULL pointer.
        //
        qd_message_get_raw_body_data(stream, &conn->outbound_body, &offset);
    }

    //
    // Process classic (non cut-though) body buffers until they are all sent onto the raw connection.
    // Note that this may take multiple runs through this function if there is any back-pressure
    // outbound on the raw connection.
    //
    // Note: There may be a non-zero offset only on the first body buffer.  It is assumed that 
    //       every subsequent buffer will have an offset of 0.
    //
    const bool observe = conn->listener_side && !!conn->observer_handle;
    while (!!conn->outbound_body && pn_raw_connection_write_buffers_capacity(conn->raw_conn) > 0) {
        unsigned char *bytes = qd_buffer_base(conn->outbound_body) + offset;
        size_t         size  = qd_buffer_size(conn->outbound_body) - offset;

        if (observe) {
            qdpo_data(conn->observer_handle, false, bytes, size);
        }
        pn_raw_buffer_t raw_buffer;
        raw_buffer.context  = 0;
        raw_buffer.bytes    = (char*) bytes;
        raw_buffer.capacity = 0;
        raw_buffer.size     = size;
        raw_buffer.offset   = 0;
        octets += raw_buffer.size;
        pn_raw_connection_write_buffers(conn->raw_conn, &raw_buffer, 1);
        conn->outbound_body = DEQ_NEXT(conn->outbound_body);
        offset = 0;
    }

    if (!conn->outbound_body) {
        conn->outbound_body_complete = true;
        qd_message_release_raw_body(stream);
    }

    return octets;
}


static void link_setup_LSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    qd_tcp_listener_t *li = (qd_tcp_listener_t*) conn->common.parent;
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_t *source = qdr_terminus(0);
    char host[64];  // for numeric remote client IP:port address

    qdr_terminus_set_address(target, li->adaptor_config->address);
    qdr_terminus_set_dynamic(source);

    qd_raw_conn_get_address_buf(conn->raw_conn, host, sizeof(host));
    conn->core_conn = TL_open_core_connection(conn->conn_id, true, host);
    qdr_connection_set_context(conn->core_conn, conn);

    conn->inbound_link = qdr_link_first_attach(conn->core_conn, QD_INCOMING, qdr_terminus(0), target, "tcp.lside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
    conn->outbound_link = qdr_link_first_attach(conn->core_conn, QD_OUTGOING, source, qdr_terminus(0), "tcp.lside.out", 0, false, 0, &conn->outbound_link_id);
    qdr_link_set_context(conn->outbound_link, conn);
    qdr_link_set_user_streaming(conn->outbound_link);
    qdr_link_flow(tcp_context->core, conn->outbound_link, 1, false);
}


static void link_setup_CSIDE_IO(qd_tcp_connection_t *conn, qdr_delivery_t *delivery)
{
    ASSERT_RAW_IO;

    assert(conn->common.parent->context_type == TL_CONNECTOR);
    const char *host = ((qd_tcp_connector_t *)conn->common.parent)->adaptor_config->host_port;
    conn->core_conn  = TL_open_core_connection(conn->conn_id, false, host);
    qdr_connection_set_context(conn->core_conn, conn);

    // use an anonymous inbound link in order to ensure credit arrives otherwise if the client has dropped the state machine will stall waiting for credit
    conn->inbound_link = qdr_link_first_attach(conn->core_conn, QD_INCOMING, qdr_terminus(0), qdr_terminus(0), "tcp.cside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
    conn->outbound_link = qdr_link_first_attach(conn->core_conn, QD_OUTGOING, qdr_terminus(0), qdr_terminus(0), "tcp.cside.out", 0, false, delivery, &conn->outbound_link_id);
    qdr_link_set_context(conn->outbound_link, conn);

    // now that the raw connection is up and able to be activated enable cutthrough activation

    assert(conn->outbound_stream);
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP enabling consumer activation", DLV_ARGS(delivery));
    qd_message_set_consumer_activation(conn->outbound_stream, &activation);
    qd_message_start_unicast_cutthrough(conn->outbound_stream);
}


static bool try_compose_and_send_client_stream_LSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    qd_tcp_listener_t  *li = (qd_tcp_listener_t*) conn->common.parent;
    qd_composed_field_t *message = 0;

    //
    // The content-type value of "application/octet-stream" is used to signal to the network that
    // the body of this stream will be a completely unstructured octet stream, without even an
    // application-data performative.  The octets directly following the application-properties
    // (or properties if there are no application-properties) section will constitute the stream
    // and will consist solely of AMQP transport frames.
    //
    if (!!conn->reply_to) {
        message = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(message);
        qd_compose_insert_null(message);                                // message-id
        qd_compose_insert_null(message);                                // user-id
        qd_compose_insert_string(message, li->adaptor_config->address); // to
        qd_compose_insert_null(message);                                // subject
        qd_compose_insert_string(message, conn->reply_to);              // reply-to
        vflow_serialize_identity(conn->common.vflow, message);          // correlation-id
        qd_compose_insert_string(message, QD_CONTENT_TYPE_APP_OCTETS);  // content-type
        //qd_compose_insert_null(message);                              // content-encoding
        //qd_compose_insert_timestamp(message, 0);                      // absolute-expiry-time
        //qd_compose_insert_timestamp(message, 0);                      // creation-time
        //qd_compose_insert_null(message);                              // group-id
        //qd_compose_insert_uint(message, 0);                           // group-sequence
        //qd_compose_insert_null(message);                              // reply-to-group-id
        qd_compose_end_list(message);

        if (conn->alpn_protocol) {
            // add the ALPN protocol as negotiated with the remote via TLS.
            message = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, message);
            qd_compose_start_map(message);
            qd_compose_insert_string_n(message, (const char *) QD_TLS_ALPN_KEY, QD_TLS_ALPN_KEY_LEN);
            qd_compose_insert_string_n(message, (const char *) conn->alpn_protocol,
                                       strlen(conn->alpn_protocol));
            qd_compose_end_map(message);
        }

        message = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, message);
        qd_compose_insert_null(message);
    }

    if (message == 0) {
        return false;
    }

    conn->inbound_stream = qd_message();
    qd_message_set_streaming_annotation(conn->inbound_stream);
    qd_message_set_Q2_disabled_annotation(conn->inbound_stream);

    qd_message_compose_2(conn->inbound_stream, message, false);
    qd_compose_free(message);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] TCP enabling producer activation", conn->conn_id, conn->inbound_link_id);
    qd_message_set_producer_activation(conn->inbound_stream, &activation);
    qd_message_start_unicast_cutthrough(conn->inbound_stream);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    conn->inbound_delivery = qdr_link_deliver(conn->inbound_link, conn->inbound_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           DLV_FMT " Initiating listener side empty client inbound stream message", DLV_ARGS(conn->inbound_delivery));
    return true;
}


static void compose_and_send_server_stream_CSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    qd_composed_field_t *message = 0;

    //
    // The lock is used here to protect access to the reply_to field.  This field is written
    // by an IO thread associated with the core connection, not this raw connection.
    //
    // The content-type value of "application/octet-stream" is used to signal to the network that
    // the body of this stream will be a completely unstructured octet stream, without even an
    // application-data performative.  The octets directly following the application-properties
    // (or properties if there are no application-properties) section will constitute the stream
    // and will consist solely of AMQP transport frames.
    //
    assert(conn->reply_to);
    message = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
    qd_compose_start_list(message);
    qd_compose_insert_null(message);                                // message-id
    qd_compose_insert_null(message);                                // user-id
    qd_compose_insert_string(message, conn->reply_to);              // to
    qd_compose_insert_null(message);                                // subject
    qd_compose_insert_null(message);                                // reply-to
    qd_compose_insert_null(message);                                // correlation-id
    qd_compose_insert_string(message, QD_CONTENT_TYPE_APP_OCTETS);  // content-type
    //qd_compose_insert_null(message);                              // content-encoding
    //qd_compose_insert_timestamp(message, 0);                      // absolute-expiry-time
    //qd_compose_insert_timestamp(message, 0);                      // creation-time
    //qd_compose_insert_null(message);                              // group-id
    //qd_compose_insert_uint(message, 0);                           // group-sequence
    //qd_compose_insert_null(message);                              // reply-to-group-id
    qd_compose_end_list(message);

    message = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, message);
    qd_compose_insert_null(message);

    conn->inbound_stream = qd_message();
    qd_message_set_streaming_annotation(conn->inbound_stream);
    qd_message_set_Q2_disabled_annotation(conn->inbound_stream);

    qd_message_compose_2(conn->inbound_stream, message, false);
    qd_compose_free(message);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] TCP enabling producer activation", conn->conn_id, conn->inbound_link_id);
    qd_message_set_producer_activation(conn->inbound_stream, &activation);
    qd_message_start_unicast_cutthrough(conn->inbound_stream);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    qd_iterator_t *iter = qd_message_field_iterator(conn->inbound_stream, QD_FIELD_TO);
    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_HASH);
    conn->inbound_delivery = qdr_link_deliver_to(conn->inbound_link, conn->inbound_stream, 0, iter, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
           DLV_FMT " Initiating connector side empty server inbound stream message", DLV_ARGS(conn->inbound_delivery));
}


static void extract_metadata_from_stream_CSIDE(qd_tcp_connection_t *conn)
{
    ASSERT_TIMER_IO;
    qd_iterator_t *rt_iter = qd_message_field_iterator(conn->outbound_stream, QD_FIELD_REPLY_TO);
    qd_iterator_t *ci_iter = qd_message_field_iterator(conn->outbound_stream, QD_FIELD_CORRELATION_ID);

    if (!!rt_iter) {
        conn->reply_to = (char*) qd_iterator_copy(rt_iter);
        qd_iterator_free(rt_iter);
    }

    if (!!ci_iter) {
        vflow_set_ref_from_iter(conn->common.vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, ci_iter);
        qd_iterator_free(ci_iter);
    }
}

// Handle delivery of outbound message to the client.
//
// @return 0 on success, otherwise a terminal outcome indicating that the message cannot be delivered.
//
static uint64_t handle_outbound_delivery_LSIDE_IO(qd_tcp_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery)
{
    ASSERT_RAW_IO;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " handle_outbound_delivery_LSIDE_IO - receive_complete=%s",
           DLV_ARGS(delivery), qd_message_receive_complete(conn->outbound_stream) ? "true" : "false");

    if (!conn->outbound_delivery) {
        // newly arrived delivery: Verify the message sections up to and including the dummy BODY_AMQP_VALUE have
        // arrived and are valid.
        //
        uint64_t dispo = validate_outbound_message(delivery);
        if (dispo != PN_RECEIVED) {
            // PN_RELEASED: since this message was delivered to this listener's unique reply-to, it cannot be
            // redelivered to another consumer. PN_RELEASED means incompatible encapsulation so this is a
            // misconfiguration. Reject the delivery.
            if (dispo == PN_RELEASED)
                dispo = PN_REJECTED;
            return dispo;
        }

        qdr_delivery_incref(delivery, "handle_outbound_delivery_LSIDE_IO");
        conn->outbound_delivery = delivery;
        conn->outbound_stream   = qdr_delivery_message(delivery);
        qdr_delivery_set_context(delivery, conn);

        //
        // Message validation ensures the start of the message body is present. Activate cutthrough on the body data.
        //
        qd_message_activation_t activation;
        activation.type     = QD_ACTIVATION_TCP;
        activation.delivery = 0;
        qd_alloc_set_safe_ptr(&activation.safeptr, conn);
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP enabling consumer activation", DLV_ARGS(delivery));
        qd_message_set_consumer_activation(conn->outbound_stream, &activation);
        qd_message_start_unicast_cutthrough(conn->outbound_stream);
    }

    connection_run_LSIDE_IO(conn);
    return 0;
}


/**
 * Handle the first indication of a new outbound delivery on CSIDE.  This is where the raw connection to the
 * external service is established.  This function executes in an IO thread not associated with a raw connection.
 *
 * @return disposition. MOVED_TO_NEW_LINK on success, 0 if more message needed, else error outcome
 */
static uint64_t handle_first_outbound_delivery_CSIDE(qd_tcp_connector_t *connector, qdr_link_t *link, qdr_delivery_t *delivery)
{
    ASSERT_TIMER_IO;
    assert(!qdr_delivery_get_context(delivery));

    // Verify the message sections up to and including the dummy BODY_AMQP_VALUE have arrived and are valid.
    //
    uint64_t dispo = validate_outbound_message(delivery);
    if (dispo != PN_RECEIVED) {
        return dispo;
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " CSIDE new outbound delivery", DLV_ARGS(delivery));

    qd_message_t          *msg = qdr_delivery_message(delivery);
    qd_tcp_connection_t *conn = new_qd_tcp_connection_t();
    ZERO(conn);

    qdr_delivery_incref(delivery, "CORE_deliver_outbound CSIDE");
    qdr_delivery_set_context(delivery, conn);

    conn->conn_id  = qd_server_allocate_connection_id(tcp_context->server);
    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (qd_tcp_common_t*) connector;
    //
    // Call connector incref when a connection references a connector via conn->common.parent
    //
    qd_tcp_connector_incref(connector);

    sys_atomic_init(&conn->core_activation, 0);
    sys_atomic_init(&conn->raw_opened, 0);

    conn->listener_side     = false;
    conn->state             = CSIDE_INITIAL;
    conn->outbound_delivery = delivery;
    conn->outbound_stream   = msg;

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, connector->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, TCP_MAX_CAPACITY_BYTES);

    // Call vflow_set_trace() only on the outbound CSIDE.
    // See https://github.com/skupperproject/skupper-router/blob/2.6.x/src/adaptors/tcp/tcp_adaptor.c#L1486
    vflow_set_trace(conn->common.vflow, msg);

    extract_metadata_from_stream_CSIDE(conn);

    conn->context.context = conn;
    conn->context.handler = on_connection_event_CSIDE_IO;

    sys_mutex_lock(&connector->lock);
    DEQ_INSERT_TAIL(connector->connections, conn);

    connector->connections_opened++;
    vflow_set_uint64(connector->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, connector->connections_opened);
    sys_mutex_unlock(&connector->lock);

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);

    //
    // The raw connection establishment must be the last thing done in this function.
    // After this call, a separate IO thread may immediately be invoked in the context
    // of the new connection to handle raw connection events.
    //
    pn_proactor_raw_connect(tcp_context->proactor, conn->raw_conn, connector->adaptor_config->host_port);

    return QD_DELIVERY_MOVED_TO_NEW_LINK;
}


/**
 * Handle subsequent pushes of the outbound delivery on CSIDE.  This is where delivery completion will be 
 * detected and raw connection write-close will occur.
 */
static void handle_outbound_delivery_CSIDE(qd_tcp_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] handle_outbound_delivery_CSIDE - receive_complete=%s",
           conn->conn_id, qd_message_receive_complete(conn->outbound_stream) ? "true" : "false");

    //
    // It is not guaranteed that this function will be called on the proper IO thread.  Wake the raw connection for
    // continued processing in the correct context.
    //
    sys_mutex_lock(&conn->activation_lock);
    if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) {
        pn_raw_connection_wake(conn->raw_conn);
    }
    sys_mutex_unlock(&conn->activation_lock);
}


/**
 * Manage the steady-state flow of a bi-directional connection from either-side point of view.
 *
 * @param conn Pointer to the TCP connection record
 * @return true if IO processing should be repeated due to state changes
 * @return false if IO processing should suspend until the next external event
 */
static bool manage_flow_XSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    //
    // Inbound stream (producer-side) processing
    //
    if (!!conn->inbound_stream && !!conn->raw_conn) {
        //
        // Produce available read buffers into the inbound stream
        //
        bool read_closed = false;
        bool was_blocked = window_full(conn);
        uint64_t octet_count = produce_read_buffers_XSIDE_IO(conn, conn->inbound_stream, &read_closed);
        conn->inbound_octets += octet_count;
        vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, conn->inbound_octets);

        if (octet_count > 0) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw read: Produced %"PRIu64" octets into stream", conn->conn_id, conn->listener_side ? 'L' : 'C', octet_count);
            if (!was_blocked && window_full(conn) && !read_closed) {
                uint64_t unacked = conn->inbound_octets - conn->window.last_update;
                conn->window.closed_count += 1;
                vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS_UNACKED, unacked);
                vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_WINDOW_CLOSURES, conn->window.closed_count);
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP RX window CLOSED: inbound_bytes=%" PRIu64 " unacked=%" PRIu64,
                       DLV_ARGS(conn->inbound_delivery), conn->inbound_octets, unacked);
            }
        }

        //
        // Manage latency measurements
        //
        if (!conn->inbound_first_octet && octet_count > 0) {
            conn->inbound_first_octet = true;
            if (conn->listener_side) {
                vflow_latency_start(conn->common.vflow);
            } else {
                vflow_latency_end(conn->common.vflow);
            }
        }

        //
        // If the raw connection is read-closed and the last produce did not block, settle and complete
        // the inbound stream/delivery and close out the inbound half of the connection.
        //

        if (read_closed) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT " Raw conn read-closed - close inbound delivery, cancel producer activation",
                   DLV_ARGS(conn->inbound_delivery));
            qd_message_set_receive_complete(conn->inbound_stream);
            qd_message_cancel_producer_activation(conn->inbound_stream);
            qdr_delivery_continue(tcp_context->core, conn->inbound_delivery, false);
            qdr_delivery_set_context(conn->inbound_delivery, 0);
            qdr_delivery_decref(tcp_context->core, conn->inbound_delivery, "FLOW_XSIDE_IO - inbound_delivery released");
            conn->inbound_delivery = 0;
            conn->inbound_stream   = 0;
            return true;
        }

        //
        // Issue read buffers when the client stream is producible and the raw connection has capacity for read buffers
        //
        size_t capacity = pn_raw_connection_read_buffers_capacity(conn->raw_conn);
        if (qd_message_can_produce_buffers(conn->inbound_stream) && capacity > 0) {
            grant_read_buffers_XSIDE_IO(conn, capacity);
        }
    }

    //
    // Outbound stream (consumer-side) processing
    //
    if (!!conn->outbound_stream && !!conn->raw_conn) {
        //
        // Drain completed write buffers from the raw connection
        //
        drain_write_buffers_XSIDE_IO(conn->raw_conn);

        //
        // If this is the beginning of an outbound stream, send any body payload that is in the
        // normal non-cut-through buffers of the message before switching to cut-through
        //
        if (!conn->outbound_body_complete) {
            uint64_t body_octets = consume_message_body_XSIDE_IO(conn, conn->outbound_stream);
            conn->outbound_octets += body_octets;
            conn->window.pending_ack += body_octets;
            if (body_octets > 0) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw write: Consumed %"PRIu64" octets from stream (body-field)", conn->conn_id, conn->listener_side ? 'L' : 'C', body_octets);
            }
        }

        //
        // Consume available write buffers from the outbound stream
        //
        if (conn->outbound_body_complete) {
            uint64_t octets = consume_write_buffers_XSIDE_IO(conn, conn->outbound_stream);
            conn->outbound_octets += octets;
            conn->window.pending_ack += octets;
            if (octets > 0) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw write: Consumed %"PRIu64" octets from stream", conn->conn_id, conn->listener_side ? 'L' : 'C', octets);
            }
        }

        //
        // Manage latency measurements
        //
        if (!conn->outbound_first_octet && conn->outbound_octets > 0) {
            conn->outbound_first_octet = true;
            if (conn->listener_side) {
                vflow_latency_end(conn->common.vflow);
            } else {
                vflow_latency_start(conn->common.vflow);
            }
        }

        //
        // Check the outbound stream for completion.  If complete, write-close the raw connection.
        // Note that this is not done if there are stream buffers yet to consume.  Wait until all of the
        // payload has been consumed and written before write-closing the connection.
        //
        if (qd_message_receive_complete(conn->outbound_stream) && !qd_message_can_consume_buffers(conn->outbound_stream)) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " Rx-complete, rings empty: Write-closing the raw connection, consumer activation cancelled",
                   DLV_ARGS(conn->outbound_delivery));
            pn_raw_connection_write_close(conn->raw_conn);
            qd_message_set_send_complete(conn->outbound_stream);
            qd_message_cancel_consumer_activation(conn->outbound_stream);
            qdr_delivery_set_context(conn->outbound_delivery, 0);
            qdr_delivery_remote_state_updated(tcp_context->core, conn->outbound_delivery, PN_ACCEPTED, true, 0, true); // accepted, settled, ref_given
            // do NOT decref outbound_delivery - ref count passed to qdr_delivery_remote_state_updated()!
            conn->outbound_delivery = 0;
            conn->outbound_stream   = 0;
        } else {
            //
            // More to send. Check if enough octets have been written to open up the window
            //
            if (conn->window.pending_ack >= TCP_ACK_THRESHOLD_BYTES) {
                qd_delivery_state_t *dstate = qd_delivery_state();
                dstate->section_number = 0;
                dstate->section_offset = conn->outbound_octets;
                qdr_delivery_remote_state_updated(tcp_context->core, conn->outbound_delivery, PN_RECEIVED,
                                                  false, dstate, false);  // received, !settled, !ref_given
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                       DLV_FMT " PN_RECEIVED sent with section_offset=%" PRIu64 " pending=%" PRIu64,
                       DLV_ARGS(conn->outbound_delivery), conn->outbound_octets, conn->window.pending_ack);
                conn->window.pending_ack = 0;
            }
        }
    }

    return false;
}

// Callback from TLS layer to get up to limit qd_buffer_t from the outgoing message for encryption and transmit out raw
// connection.
//
static int64_t tls_consume_data_buffers(void *context, qd_buffer_list_t *buffers, size_t limit)
{
    qd_tcp_connection_t *conn    = (qd_tcp_connection_t *) context;
    uint64_t             octets  = 0;

    assert(limit > 0);
    assert(DEQ_IS_EMPTY(*buffers));

    if (!conn->outbound_stream)
        return octets;

    if (!conn->outbound_body_complete) {
        copy_message_body_TLS_XSIDE_IO(conn, conn->outbound_stream, buffers, limit);
        assert(limit >= DEQ_SIZE(*buffers));
        limit -= DEQ_SIZE(*buffers);
    }

    if (conn->outbound_body_complete && limit > 0) {
        qd_buffer_list_t tmp = DEQ_EMPTY;
        qd_message_consume_buffers(conn->outbound_stream, &tmp, limit);
        assert(limit >= DEQ_SIZE(tmp));
        DEQ_APPEND(*buffers, tmp);
    }

    if (!DEQ_IS_EMPTY(*buffers)) {
        const bool observe = conn->listener_side && !!conn->observer_handle;

        qd_buffer_t *buf = DEQ_HEAD(*buffers);
        while (buf) {
            octets += qd_buffer_size(buf);
            if (observe) {
                qdpo_data(conn->observer_handle, false, qd_buffer_base(buf), qd_buffer_size(buf));
            }
            buf = DEQ_NEXT(buf);
        }

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%"PRIu64"] TLS consumed %"PRIu64" cleartext octets from stream", conn->conn_id, octets);

        conn->outbound_octets += octets;
        conn->window.pending_ack += octets;

        //
        // Manage latency measurements
        //
        if (!conn->outbound_first_octet) {
            conn->outbound_first_octet = true;
            if (conn->listener_side) {
                vflow_latency_end(conn->common.vflow);
            } else {
                vflow_latency_start(conn->common.vflow);
            }
        }

        //
        // Check if enough octets have been written to open up the window
        //
        if (conn->window.pending_ack >= TCP_ACK_THRESHOLD_BYTES) {
            qd_delivery_state_t *dstate = qd_delivery_state();
            dstate->section_number = 0;
            dstate->section_offset = conn->outbound_octets;
            qdr_delivery_remote_state_updated(tcp_context->core, conn->outbound_delivery, PN_RECEIVED,
                                              false, dstate, false);  // received, !settled, !ref_given
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                   DLV_FMT " sending PN_RECEIVED with section_offset=%" PRIu64 " pending=%" PRIu64,
                   DLV_ARGS(conn->outbound_delivery), conn->outbound_octets, conn->window.pending_ack);
            conn->window.pending_ack = 0;
        }
    } else if (qd_message_receive_complete(conn->outbound_stream) && !qd_message_can_consume_buffers(conn->outbound_stream)) {
        //
        // no more receive data so signal to TLS layer that we've hit the end of the outgoing stream
        //
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               DLV_FMT " TLS outbound stream rx complete and rings empty: signalling RX EOS", DLV_ARGS(conn->outbound_delivery));
        // @TODO(kag): need to check for message aborted and return QD_IO_EOS_ERR to prevent forwarding a TLS close notify alert
        return QD_IO_EOS;  // stream completed
    }

    return octets;
}


/**
 * Manage the steady-state flow of a bi-directional TLS connection from either-side point of view.
 *
 * @param conn Pointer to the TCP connection record
 * @return true if IO processing should be repeated due to state changes
 * @return false if IO processing should suspend until the next external event
 */
static bool manage_tls_flow_XSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    assert(conn->tls);
    assert(conn->raw_conn);

    if (qd_tls_is_error(conn->tls))
        return false;

    qd_buffer_list_t decrypted_buffers = DEQ_EMPTY;
    uint64_t          decrypted_octets = 0;
    bool                   can_produce = !!conn->inbound_stream && qd_message_can_produce_buffers(conn->inbound_stream);
    bool                     more_work = false;
    bool                window_blocked = window_full(conn);


    grant_read_buffers_XSIDE_IO(conn, pn_raw_connection_read_buffers_capacity(conn->raw_conn));

    const int tls_status = qd_tls_do_io2(conn->tls, conn->raw_conn, tls_consume_data_buffers, conn,
                                         (can_produce) ? &decrypted_buffers : 0,
                                         &decrypted_octets);

    //
    // Process inbound cleartext data.
    //

    if (decrypted_octets) {
        more_work = true;
        conn->inbound_octets += decrypted_octets;
        vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, conn->inbound_octets);

        if (conn->listener_side && !!conn->observer_handle) {
            qd_buffer_t *buf = DEQ_HEAD(decrypted_buffers);
            while (buf) {
                qdpo_data(conn->observer_handle, true, qd_buffer_base(buf), qd_buffer_size(buf));
                buf = DEQ_NEXT(buf);
            }
        }

        qd_message_produce_buffers(conn->inbound_stream, &decrypted_buffers);

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE TLS read: Produced %"PRIu64" octets into stream", conn->conn_id, conn->listener_side ? 'L' : 'C', decrypted_octets);
        if (!window_blocked && window_full(conn)) {
            uint64_t unacked = conn->inbound_octets - conn->window.last_update;
            conn->window.closed_count += 1;
            vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS_UNACKED, unacked);
            vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_WINDOW_CLOSURES, conn->window.closed_count);
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TCP RX window CLOSED: inbound_bytes=%" PRIu64 " unacked=%" PRIu64,
                   DLV_ARGS(conn->inbound_delivery), conn->inbound_octets,
                   (conn->inbound_octets - conn->window.last_update));
        }

        //
        // Manage latency measurements
        //
        if (!conn->inbound_first_octet) {
            conn->inbound_first_octet = true;
            if (conn->listener_side) {
                vflow_latency_start(conn->common.vflow);
            } else {
                vflow_latency_end(conn->common.vflow);
            }
        }
    }

    //
    // Check for end of inbound message data
    //
    bool ignore;
    if (qd_tls_is_input_drained(conn->tls, &ignore) && conn->inbound_stream) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TLS inbound stream receive complete, producer activation cancelled", DLV_ARGS(conn->inbound_delivery));

        qd_message_set_receive_complete(conn->inbound_stream);
        qd_message_cancel_producer_activation(conn->inbound_stream);

        qdr_delivery_set_context(conn->inbound_delivery, 0);
        qdr_delivery_continue(tcp_context->core, conn->inbound_delivery, false);
        qdr_delivery_decref(tcp_context->core, conn->inbound_delivery, "TLS_FLOW_XSIDE_IO - inbound_delivery released");
        conn->inbound_delivery = 0;
        conn->inbound_stream   = 0;
    }

    //
    // Check for end of outbound message data
    //
    if (qd_tls_is_output_flushed(conn->tls) && conn->outbound_stream) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " TLS outbound stream send complete, consumer activation cancelled", DLV_ARGS(conn->outbound_delivery));

        qd_message_set_send_complete(conn->outbound_stream);
        qd_message_cancel_consumer_activation(conn->outbound_stream);

        qdr_delivery_set_context(conn->outbound_delivery, 0);
        qdr_delivery_remote_state_updated(tcp_context->core, conn->outbound_delivery,
                                          tls_status < 0 ? PN_MODIFIED : PN_ACCEPTED,
                                          true, 0, true); // settled, 0, ref_given
        // do NOT decref outbound_delivery - ref count passed to qdr_delivery_remote_state_updated()!
        conn->outbound_delivery = 0;
        conn->outbound_stream   = 0;
    }

    return more_work;
}


static void connection_run_LSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    bool repeat;

    do {
        repeat = false;

        switch (conn->state) {
        case LSIDE_INITIAL:
            if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) { // raw connection is active
                qd_tcp_listener_t *li = (qd_tcp_listener_t *) conn->common.parent;
                if (li->tls_domain) {
                    if (setup_tls_session(conn, li->tls_domain, 0) != 0) {
                        // TLS setup failed: check logs for details
                        close_raw_connection(conn, "TLS-connection-failed", "Error loading credentials");
                        set_state_XSIDE_IO(conn, XSIDE_CLOSING);  // prevent further connection I/O
                    } else {
                        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] LSIDE_IO performing TLS handshake", conn->conn_id);
                        set_state_XSIDE_IO(conn, LSIDE_TLS_HANDSHAKE);
                        repeat = true;
                    }
                } else {
                    link_setup_LSIDE_IO(conn);
                    set_state_XSIDE_IO(conn, LSIDE_LINK_SETUP);
                }
            }
            break;

        case LSIDE_TLS_HANDSHAKE:
            //
            // For TLS connections wait until the negotiation has succeeded. This is done before bringing up the links
            // for two reasons: 1) don't establish router links and reply addresses if the client's creditials are bad
            // and 2) we need the negotiated ALPN value *before* we can construct the inbound message headers.
            //
            assert(conn->tls);
            if (!qd_tls_is_secure(conn->tls) && !qd_tls_is_error(conn->tls)) {
                assert(conn->raw_conn);
                grant_read_buffers_XSIDE_IO(conn, pn_raw_connection_read_buffers_capacity(conn->raw_conn));
                int rc = qd_tls_do_io2(conn->tls, conn->raw_conn, 0, 0, 0, 0);
                if (rc < 0) {
                    //
                    // TLS failed! Error logged, raw connection close initiated and error condition set. Clean up in
                    // DISCONNECTED raw connection event.
                    //
                    set_state_XSIDE_IO(conn, XSIDE_CLOSING);  // prevent further connection I/O
                } else if (qd_tls_is_secure(conn->tls)) {
                    //
                    // Handshake completed, begin the setup of the inbound and outbound links for this connection.
                    //
                    link_setup_LSIDE_IO(conn);
                    set_state_XSIDE_IO(conn, LSIDE_LINK_SETUP);
                }
            }
            break;

        case LSIDE_LINK_SETUP:
            //
            // If we have a reply-to address, compose the stream message, convert it to a
            // unicast/cut-through stream and send it.
            // Set the state to LSIDE_STREAM_START and wait for the connector side to respond.
            //
            if (try_compose_and_send_client_stream_LSIDE_IO(conn)) {
                set_state_XSIDE_IO(conn, LSIDE_STREAM_START);
                repeat = true;
            }
            break;

        case LSIDE_STREAM_START:
            //
            // If there is now an outbound stream, because the CSIDE sent a counter stream, switch to
            // LSIDE_FLOW state and let the streaming begin.
            //
            if (!!conn->outbound_stream) {
                set_state_XSIDE_IO(conn, (conn->tls) ? LSIDE_TLS_FLOW: LSIDE_FLOW);
                repeat = true;
            }
            break;

        case LSIDE_FLOW:
            //
            // Manage the ongoing bidirectional flow of the connection.
            //
            repeat = manage_flow_XSIDE_IO(conn);
            break;

        case LSIDE_TLS_FLOW:
            //
            // Manage the ongoing bidirectional flow of the TLS connection.
            //
            repeat = manage_tls_flow_XSIDE_IO(conn);
            break;

        case XSIDE_CLOSING:
            //
            // Don't do anything
            //
            break;

        default:
            assert(false);
            break;
        }
    } while (repeat);
}


static void connection_run_CSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    bool repeat;
    bool credit;

    do {
        repeat = false;

        switch (conn->state) {
        case CSIDE_INITIAL:
            if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) { // raw connection is active
                qd_tcp_connector_t *connector = (qd_tcp_connector_t *) conn->common.parent;
                if (connector->tls_domain) {
                    assert(conn->outbound_stream);
                    char *alpn = get_tls_negotiated_alpn(conn->outbound_stream);
                    int rc = setup_tls_session(conn, connector->tls_domain, alpn);
                    free(alpn);
                    if (rc != 0) {
                        // TLS setup failed: check logs for details
                        close_raw_connection(conn, "TLS-connection-failed", "Error loading credentials");
                        set_state_XSIDE_IO(conn, XSIDE_CLOSING);  // prevent further connection I/O
                        break;
                    }
                }
                link_setup_CSIDE_IO(conn, conn->outbound_delivery);
                set_state_XSIDE_IO(conn, CSIDE_LINK_SETUP);
            }
            break;

        case CSIDE_LINK_SETUP:
            credit = conn->inbound_credit;

            if (credit) {
                compose_and_send_server_stream_CSIDE_IO(conn);
                set_state_XSIDE_IO(conn, (conn->tls) ? CSIDE_TLS_FLOW : CSIDE_FLOW);
                repeat = true;
            }
            break;

        case CSIDE_FLOW:
            //
            // Manage the ongoing bidirectional flow of the connection.
            //
            repeat = manage_flow_XSIDE_IO(conn);
            break;

        case CSIDE_TLS_FLOW:
            //
            // Manage the ongoing bidirectional flow of the TLS connection.
            //
            repeat = manage_tls_flow_XSIDE_IO(conn);
            break;

        case XSIDE_CLOSING:
            //
            // Don't do anything
            //
            break;

        default:
            assert(false);
            break;
    }
    } while(repeat);
}


static void connection_run_XSIDE_IO(qd_tcp_connection_t *conn)
{
    ASSERT_RAW_IO;
    if (conn->listener_side) {
        connection_run_LSIDE_IO(conn);
    } else {
        connection_run_CSIDE_IO(conn);
    }
}

// Validate the outbound message associated with out_dlv. This verifies depth through QD_DEPTH_RAW_BODY and ensures the
// proper encapsulation.
//
// @param out_dlv Outgoing delivery holding the message
// @return a disposition value indicating the validity of the message:
// 0: message headers incomplete, wait for more data to arrive
// PN_REJECTED: corrupt headers, cannot be re-delivered
// PN_RELEASED: headers ok, incompatible body format: deliver elsewhere
// PN_RECEIVED: headers & body ok
//
static uint64_t validate_outbound_message(const qdr_delivery_t *out_dlv)
{
    qd_message_t *msg = qdr_delivery_message(out_dlv);
    qd_message_depth_status_t depth_ok = qd_message_check_depth(msg, QD_DEPTH_RAW_BODY);
    if (depth_ok == QD_MESSAGE_DEPTH_INCOMPLETE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               DLV_FMT " tcp_adaptor egress message incomplete, waiting for more", DLV_ARGS(out_dlv));
        return 0;  // retry later
    }
    if (depth_ok != QD_MESSAGE_DEPTH_OK) {  // otherwise bug? corrupted message encoding?
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_WARNING, DLV_FMT " Malformed TCP message - discarding!", DLV_ARGS(out_dlv));
        qd_message_set_send_complete(msg);
        return PN_REJECTED;
    }

    // ISSUE-1136: ensure the message body is using the proper encapsulation.
    //
    bool encaps_ok = false;
    qd_iterator_t *encaps = qd_message_field_iterator(msg, QD_FIELD_CONTENT_TYPE);
    if (encaps) {
        encaps_ok = qd_iterator_equal(encaps, (unsigned char *) QD_CONTENT_TYPE_APP_OCTETS);
        qd_iterator_free(encaps);
    }
    if (!encaps_ok) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, DLV_FMT " Misconfigured TCP adaptor (wrong encapsulation)",
               DLV_ARGS(out_dlv));
        qd_message_set_send_complete(msg);
        return PN_RELEASED;  // allow it to be re-forwarded to a different adaptor
    }
    return PN_RECEIVED;
}

// Callback from TLS I/O handler when the TLS handshake succeeds
//
static void on_tls_connection_secured(qd_tls_t *tls, void *user_context)
{
    qd_tcp_connection_t *conn = (qd_tcp_connection_t *) user_context;
    assert(conn);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] TLS handshake succeeded!", conn->conn_id);
    if (conn->core_conn && conn->core_conn->connection_info) {
        qd_tls_update_connection_info(conn->tls, conn->core_conn->connection_info);
    }

    // check if we need to propagate client ALPN to server
    if (conn->listener_side) {
        assert(!conn->alpn_protocol);
        qd_tls_get_alpn_protocol(conn->tls, &conn->alpn_protocol);
    }
}

// Check for the ALPN value negotiated on the CSIDE TLS connection (optional).
// Caller must free() returned string! Returns 0 if no ALPN present.
//
static char *get_tls_negotiated_alpn(qd_message_t *msg)
{
    qd_iterator_t *ap_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (!ap_iter) {
        return 0;
    }

    char *alpn_protocol = 0;
    qd_parsed_field_t *ap = qd_parse(ap_iter);
    if (ap && qd_parse_ok(ap) && qd_parse_is_map(ap)) {
        uint32_t count = qd_parse_sub_count(ap);
        for (uint32_t i = 0; i < count; i++) {
            qd_parsed_field_t *key = qd_parse_sub_key(ap, i);
            if (key == 0) {
                break;
            }
            qd_iterator_t *key_iter = qd_parse_raw(key);
            if (!!key_iter && qd_iterator_equal(key_iter, (const unsigned char *) QD_TLS_ALPN_KEY)) {
                qd_parsed_field_t *alpn_field = qd_parse_sub_value(ap, i);
                qd_iterator_t     *alpn_iter  = qd_parse_raw(alpn_field);
                alpn_protocol                 = (char *) qd_iterator_copy(alpn_iter);
            }
        }
    }

    qd_parse_free(ap);
    qd_iterator_free(ap_iter);
    return alpn_protocol;
}

/**
 * Create a new TLS session for the given connection.
 *
 * @param parent_domain Reference to parent connector/listener TLS domain context
 * @param alpn_protocol If CSIDE the optional alpn protocol value to pass to the remote server
 *
 * @return 0 on success, non-zero on error
 */
static int setup_tls_session(qd_tcp_connection_t *conn, const qd_tls_domain_t *parent_domain, const char *alpn_protocol)
{
    conn->tls_domain = qd_tls_domain_clone(parent_domain);
    if (!conn->tls_domain)
        return -1;  // error logged in qd_tls_domain_clone()

    if (alpn_protocol) {
        const char *alpn_protocols[] = {alpn_protocol};
        int rc = qd_tls_set_alpn_protocols(conn->tls_domain, alpn_protocols, 1);
        if (rc != 0) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR,
                   "[%" PRIu64 "] failed to configure ALPN protocol '%s' (%d)", conn->conn_id, alpn_protocol, rc);
            return -1;
        }
    }

    conn->tls = qd_tls(conn->tls_domain, conn, conn->conn_id, on_tls_connection_secured);
    if (!conn->tls)
        return -1;  // error logged in qd_tls()

    return 0;
}


//=================================================================================
// Handlers for events from the Raw Connections
//=================================================================================
static void on_connection_event_LSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    SET_THREAD_RAW_IO;
    pn_event_type_t       etype = pn_event_type(e);
    qd_tcp_connection_t *conn  = (qd_tcp_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] on_connection_event_LSIDE_IO: %s", conn->conn_id, pn_event_type_name(etype));

    if (etype == PN_RAW_CONNECTION_CONNECTED) {
        // it is safe to call pn_raw_connection_wake() now
        qd_set_vflow_netaddr_string(conn->common.vflow, conn->raw_conn, conn->listener_side);
        assert(!IS_ATOMIC_FLAG_SET(&conn->raw_opened));
        SET_ATOMIC_FLAG(&conn->raw_opened);
    } else if (etype == PN_RAW_CONNECTION_DISCONNECTED) {
        conn->error = !!conn->raw_conn ? pn_raw_connection_condition(conn->raw_conn) : 0;

        if (!!conn->error) {
            const char *cname = pn_condition_get_name(conn->error);
            const char *cdesc = pn_condition_get_description(conn->error);

            if (!!cname) {
                vflow_set_string(conn->common.vflow, VFLOW_ATTRIBUTE_RESULT, cname);
            }
            if (!!cdesc) {
                vflow_set_string(conn->common.vflow, VFLOW_ATTRIBUTE_REASON, cdesc);
            }
        }
        close_connection_XSIDE_IO(conn);
        return;
    }

    if (CLEAR_ATOMIC_FLAG(&conn->core_activation) && !!conn->core_conn) {
        qdr_connection_process(conn->core_conn);
    }

    connection_run_LSIDE_IO(conn);
}


static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    SET_THREAD_RAW_IO;
    pn_event_type_t       etype = pn_event_type(e);
    qd_tcp_connection_t *conn  = (qd_tcp_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] on_connection_event_CSIDE_IO: %s", conn->conn_id, pn_event_type_name(etype));

    if (etype == PN_RAW_CONNECTION_CONNECTED) {
        // it is safe to call pn_raw_connection_wake() now
        qd_set_vflow_netaddr_string(conn->common.vflow, conn->raw_conn, conn->listener_side);
        assert(!IS_ATOMIC_FLAG_SET(&conn->raw_opened));
        SET_ATOMIC_FLAG(&conn->raw_opened);
    } else if (etype == PN_RAW_CONNECTION_DISCONNECTED) {
        conn->error = !!conn->raw_conn ? pn_raw_connection_condition(conn->raw_conn) : 0;

        if (!!conn->error) {
            const char *cname = pn_condition_get_name(conn->error);
            const char *cdesc = pn_condition_get_description(conn->error);

            if (!!cname) {
                vflow_set_string(conn->common.vflow, VFLOW_ATTRIBUTE_RESULT, cname);
            }
            if (!!cdesc) {
                vflow_set_string(conn->common.vflow, VFLOW_ATTRIBUTE_REASON, cdesc);
            }
        }
        close_connection_XSIDE_IO(conn);
        return;
    }

    if (CLEAR_ATOMIC_FLAG(&conn->core_activation) && !!conn->core_conn) {
        qdr_connection_process(conn->core_conn);
    }

    connection_run_CSIDE_IO(conn);
}


static void on_accept(qd_adaptor_listener_t *adaptor_listener, pn_listener_t *pn_listener, void *context)
{
    qd_tcp_listener_t *listener      = (qd_tcp_listener_t*) context;
    qd_tcp_connection_t *conn  = new_qd_tcp_connection_t();

    ZERO(conn);
    conn->conn_id  = qd_server_allocate_connection_id(tcp_context->server);
    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (qd_tcp_common_t*) listener;
    //
    // Call listener incref when a connection references a listener via conn->common.parent
    //
    qd_tcp_listener_incref(listener);

    sys_mutex_init(&conn->activation_lock);
    sys_atomic_init(&conn->core_activation, 0);
    sys_atomic_init(&conn->raw_opened, 0);

    conn->listener_side = true;
    conn->state         = LSIDE_INITIAL;

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, listener->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_WINDOW_SIZE, TCP_MAX_CAPACITY_BYTES);

    conn->context.context = conn;
    conn->context.handler = on_connection_event_LSIDE_IO;

    sys_mutex_lock(&listener->lock);
    DEQ_INSERT_TAIL(listener->connections, conn);

    listener->connections_opened++;
    vflow_set_uint64(listener->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, listener->connections_opened);
    sys_mutex_unlock(&listener->lock);

    if (listener->protocol_observer) {
        conn->observer_handle = qdpo_begin(listener->protocol_observer, conn->common.vflow, conn, conn->conn_id);
    }

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);
    // Note: this will trigger the connection's event handler on another thread:
    pn_listener_raw_accept(pn_listener, conn->raw_conn);
}

//=================================================================================
// Callbacks from the Core Module
//=================================================================================
static void CORE_activate(void *context, qdr_connection_t *core_conn)
{
    SET_THREAD_ROUTER_CORE;
    qd_tcp_common_t     *common = (qd_tcp_common_t*) qdr_connection_get_context(core_conn);
    qd_tcp_connection_t *conn;

    switch (common->context_type) {
    case TL_LISTENER:
        assert(false);  // listeners are never activated, relies on adaptor_listener callback
        break;

    case TL_CONNECTOR: {
        qd_tcp_connector_t *connector = (qd_tcp_connector_t *) common;
        sys_mutex_lock(&connector->lock);
        if (connector->activate_timer)
            qd_timer_schedule(connector->activate_timer, 0);
        sys_mutex_unlock(&connector->lock);
        break;
    }

    case TL_CONNECTION:
        conn = (qd_tcp_connection_t*) common;
        sys_mutex_lock(&conn->activation_lock);
        if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) {
            SET_ATOMIC_FLAG(&conn->core_activation);
            pn_raw_connection_wake(conn->raw_conn);
        }
        sys_mutex_unlock(&conn->activation_lock);
        break;
    }
}


static void CORE_first_attach(void               *context,
                              qdr_connection_t   *conn,
                              qdr_link_t         *link,
                              qdr_terminus_t     *source,
                              qdr_terminus_t     *target,
                              qd_session_class_t  ssn_class)
{
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_connection_get_context(conn);
    qdr_link_set_context(link, common);

    qdr_terminus_t *local_source = qdr_terminus(0);
    qdr_terminus_t *local_target = qdr_terminus(0);

    qdr_terminus_set_address_iterator(local_source, qdr_terminus_get_address(target));
    qdr_terminus_set_address_iterator(local_target, qdr_terminus_get_address(source));
    qdr_link_second_attach(link, local_source, local_target);

    if (qdr_link_direction(link) == QD_OUTGOING) {
        qdr_link_flow(tcp_context->core, link, 1, false);
    }
}


static void CORE_second_attach(void           *context,
                               qdr_link_t     *link,
                               qdr_terminus_t *source,
                               qdr_terminus_t *target)
{
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_CONNECTION) {
        qd_tcp_connection_t *conn = (qd_tcp_connection_t*) common;
        if (qdr_link_direction(link) == QD_OUTGOING) {
            conn->reply_to = (char*) qd_iterator_copy(qdr_terminus_get_address(source));
            connection_run_XSIDE_IO(conn);
        }
    }
}


static void CORE_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
}


static void CORE_flow(void *context, qdr_link_t *link, int credit)
{
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_CONNECTION) {
        qd_tcp_connection_t *conn = (qd_tcp_connection_t*) common;
        if (qdr_link_direction(link) == QD_INCOMING && credit > 0) {
            conn->inbound_credit = true;
            connection_run_XSIDE_IO(conn);
        }
    }
}


static void CORE_offer(void *context, qdr_link_t *link, int delivery_count)
{
}


static void CORE_drained(void *context, qdr_link_t *link)
{
}


static void CORE_drain(void *context, qdr_link_t *link, bool mode)
{
}


static int CORE_push(void *context, qdr_link_t *link, int limit)
{
    return qdr_link_process_deliveries(tcp_context->core, link, limit);
}


static uint64_t CORE_deliver_outbound(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_delivery_get_context(delivery);
    if (!common) {
        common = (qd_tcp_common_t*) qdr_link_get_context(link);
    }

    if (common->context_type == TL_CONNECTOR) {
        return handle_first_outbound_delivery_CSIDE((qd_tcp_connector_t*) common, link, delivery);
    } else if (common->context_type == TL_CONNECTION) {
        qd_tcp_connection_t *conn = (qd_tcp_connection_t*) common;
        if (conn->listener_side) {
            return handle_outbound_delivery_LSIDE_IO(conn, link, delivery);
        } else {
            handle_outbound_delivery_CSIDE(conn, link, delivery, settled);
        }
    } else {
        assert(false);
    }

    return 0;
}


static int CORE_get_credit(void *context, qdr_link_t *link)
{
    return 1;
}


static void CORE_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    bool              need_wake = false;
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_delivery_get_context(dlv);
    if (!!common && common->context_type == TL_CONNECTION && disp != 0) {
        qd_tcp_connection_t *conn = (qd_tcp_connection_t*) common;

        if (dlv == conn->outbound_delivery) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " Outbound delivery update - disposition: %s", DLV_ARGS(dlv), pn_disposition_type_name(disp));
            conn->outbound_disposition = disp;
            need_wake = true;
        } else if (dlv == conn->inbound_delivery) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, DLV_FMT " Inbound delivery update - disposition: %s", DLV_ARGS(dlv), pn_disposition_type_name(disp));
            conn->inbound_disposition = disp;
            const bool final_outcome = qd_delivery_state_is_terminal(disp);
            if (final_outcome && disp != PN_ACCEPTED) {
                // The delivery failed - this is unrecoverable.
                if (!!conn->raw_conn) {
                    close_raw_connection(conn, "delivery-failed", "destination unreachable");
                    // clean stuff up when DISCONNECT event arrives
                }
            } else {
                //
                // handle flow control window updates
                //
                const bool window_was_full = window_full(conn);
                if (disp == PN_RECEIVED) {
                    //
                    // The egress adaptor for TCP flow has sent us its count of sent bytes
                    //
                    uint64_t ignore;
                    qd_delivery_state_t *dstate = qdr_delivery_take_local_delivery_state(dlv, &ignore);

                    // Resend released will generate a PN_RECEIVED with section_offset == 0, ignore it.  Ensure updates
                    // arrive in order, which may not happen if cut-through for disposition updates is implemented.
                    if (dstate && dstate->section_offset > 0
                        && (int64_t)(dstate->section_offset - conn->window.last_update) > 0) {

                        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                               DLV_FMT " PN_RECEIVED inbound_bytes=%" PRIu64 ", was_unacked=%" PRIu64 ", rcv_offset=%" PRIu64 " now_unacked=%" PRIu64,
                               DLV_ARGS(dlv), conn->inbound_octets,
                               (conn->inbound_octets - conn->window.last_update),
                               dstate->section_offset,
                               (conn->inbound_octets - dstate->section_offset));
                        conn->window.last_update = dstate->section_offset;
                        vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS_UNACKED, conn->inbound_octets - dstate->section_offset);

                        qd_delivery_state_free(dstate);
                    }
                }
                // the window needs to be disabled when the remote settles or sets the final outcome because once that
                // occurs the remote will no longer send PN_RECEIVED updates necessary to open the window.
                conn->window.disabled = conn->window.disabled || settled || final_outcome;
                if (window_was_full && !window_full(conn)) {
                    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
                           DLV_FMT " TCP RX window %s: inbound_bytes=%" PRIu64 " unacked=%" PRIu64,
                           DLV_ARGS(dlv),
                           conn->window.disabled ? "DISABLED" : "OPENED",
                           conn->inbound_octets, (conn->inbound_octets - conn->window.last_update));
                }
            }
            need_wake = !window_full(conn);
        }

        if (need_wake) {
            connection_run_XSIDE_IO(conn);
        }
    }
}


static void CORE_connection_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
    // hahaha
    qd_tcp_common_t *common = (qd_tcp_common_t*) qdr_connection_get_context(conn);
    qd_tcp_connection_t *tcp_conn = (qd_tcp_connection_t*) common;
    if (tcp_conn) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
               "[C%" PRIu64 "] qdr_tcp_conn_close: closing raw connection", tcp_conn->conn_id);
        //
        // Closing the raw connection (calling pn_raw_connection_close()) will generate a PN_RAW_CONNECTION_DISCONNECTED
        // event which will call the handle_disconnected() which in turn calls qdr_connection_closed() which removes the
        // connection from the list of connections.
        //
        pn_raw_connection_close(tcp_conn->raw_conn);
    }
    else {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "qdr_tcp_conn_trace: no connection context");
        assert(false);
    }




}


static void CORE_connection_trace(void *context, qdr_connection_t *conn, bool trace)
{
}


//=================================================================================
// Entrypoints for Management
//=================================================================================
#define TCP_NUM_ALPN_PROTOCOLS 2
// const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"h2", "http/1.1", "http/1.0"};
static const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"http/1.1", "h2"};

QD_EXPORT void *qd_dispatch_configure_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    SET_THREAD_UNKNOWN;
    qd_tcp_listener_t *listener = new_qd_tcp_listener_t();
    ZERO(listener);
    sys_atomic_init(&listener->ref_count, 1);

    listener->adaptor_config = new_qd_adaptor_config_t();
    ZERO(listener->adaptor_config);

    if (qd_load_adaptor_config(tcp_context->core, listener->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp listener: %s", qd_error_message());
        qd_free_adaptor_config(listener->adaptor_config);
        free_qd_tcp_listener_t(listener);
        return 0;
    }

    if (listener->adaptor_config->ssl_profile_name) {
        // On the TCP TLS listener side, send "http/1.1", "http/1.0" and "h2" as ALPN protocols
        listener->tls_domain = qd_tls_domain(listener->adaptor_config, qd, LOG_TCP_ADAPTOR, tcp_alpn_protocols,
                                       TCP_NUM_ALPN_PROTOCOLS, true);
        if (!listener->tls_domain) {
            // note qd_tls_domain logged the error
            qd_free_adaptor_config(listener->adaptor_config);
            free_qd_tcp_listener_t(listener);
            return 0;
        }

        // sanity check the configuration by creating a temporary TLS session. Is this fails
        // an error will be logged by the call to qd_tls()
        qd_tls_t *test = qd_tls(listener->tls_domain, 0, 0, 0);
        if (!test) {
            qd_free_adaptor_config(listener->adaptor_config);
            qd_tls_domain_decref(listener->tls_domain);
            free_qd_tcp_listener_t(listener);
            return 0;
        }
        qd_tls_free2(test);
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Configured TcpListener for %s, %s:%s",
            listener->adaptor_config->address, listener->adaptor_config->host, listener->adaptor_config->port);

    listener->common.context_type = TL_LISTENER;
    sys_mutex_init(&listener->lock);

    sys_mutex_lock(&tcp_context->lock);
    DEQ_INSERT_TAIL(tcp_context->listeners, listener);
    sys_mutex_unlock(&tcp_context->lock);

    TL_setup_listener(listener);

    return listener;
}


QD_EXPORT void qd_dispatch_delete_tcp_listener(qd_dispatch_t *qd, void *impl)
{

    SET_THREAD_UNKNOWN;
    qd_tcp_listener_t *listener = (qd_tcp_listener_t*) impl;
    if (listener) {
        // deactivate the listener to prevent new connections from being accepted
        // on the proactor thread
        //
        if (!!listener->adaptor_listener) {
            qd_adaptor_listener_close(listener->adaptor_listener);
            listener->adaptor_listener = 0;
            //
            // End the vanflow record here. This will make sure that the vanflow is ended
            // as soon as the listener is deleted.
            //
            vflow_end_record(listener->common.vflow);
            listener->common.vflow = 0;
        }
        //
        // If all the connections associated with this listener has been closed, this call to
        // qd_tcp_listener_decref should free the listener
        //
        qd_tcp_listener_decref(listener);
    }
}


QD_EXPORT void qd_dispatch_delete_tcp_connector(qd_dispatch_t *qd, void *impl)
{
    SET_THREAD_UNKNOWN;
    qd_tcp_connector_t *connector = (qd_tcp_connector_t*) impl;
    if (connector) {
        // Explicitly drop the out-link so that we notify any link event monitors and stop new deliveries from being
        // forwarded to this connector
        //
        if (!!connector->out_link) {
            qdr_link_set_context(connector->out_link, 0);
            qdr_link_detach(connector->out_link, QD_LOST, 0);
            connector->out_link = 0;
        }

        qdr_connection_closed(connector->core_conn);
        connector->core_conn = 0;
        qd_connection_counter_dec(QD_PROTOCOL_TCP);
        //
        // If all the connections associated with this listener has been closed, this call to
        // qd_tcp_listener_decref should free the listener
        //
        qd_tcp_connector_decref(connector);
    }
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpListener(qd_entity_t* entity, void *impl)
{
    SET_THREAD_UNKNOWN;
    uint64_t co = 0;
    uint64_t cc = 0;
    qd_listener_oper_status_t os = QD_LISTENER_OPER_DOWN;
    qd_tcp_listener_t *li = (qd_tcp_listener_t*) impl;

    if (!!li->adaptor_listener) {
        os = qd_adaptor_listener_oper_status(li->adaptor_listener);
        sys_mutex_lock(&li->lock);
        co = li->connections_opened;
        cc = li->connections_closed;
        sys_mutex_unlock(&li->lock);
    }

    if (   qd_entity_set_long(entity, "bytesIn",           0) == 0
        && qd_entity_set_long(entity, "bytesOut",          0) == 0
        && qd_entity_set_long(entity, "connectionsOpened", co) == 0
        && qd_entity_set_long(entity, "connectionsClosed", cc) == 0
        && qd_entity_set_string(entity, "operStatus", os == QD_LISTENER_OPER_UP ? "up" : "down") == 0)
    {
        return QD_ERROR_NONE;
    }

    return qd_error_code();
}


qd_tcp_connector_t *qd_dispatch_configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    SET_THREAD_UNKNOWN;
    qd_tcp_connector_t *connector = new_qd_tcp_connector_t();
    ZERO(connector);
    sys_atomic_init(&connector->ref_count, 1);

    connector->adaptor_config = new_qd_adaptor_config_t();
    ZERO(connector->adaptor_config);

    if (qd_load_adaptor_config(tcp_context->core, connector->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        qd_free_adaptor_config(connector->adaptor_config);
        free_qd_tcp_connector_t(connector);
        return 0;
    }

    if (connector->adaptor_config->ssl_profile_name) {
        connector->tls_domain = qd_tls_domain(connector->adaptor_config, qd, LOG_TCP_ADAPTOR, 0, 0, false);
        if (!connector->tls_domain) {
            // note qd_tls_domain() logged the error
            qd_free_adaptor_config(connector->adaptor_config);
            free_qd_tcp_connector_t(connector);
            return 0;
        }

        // sanity check the configuration by creating a temporary TLS session. Is this fails
        // an error will be logged by the call to qd_tls()
        qd_tls_t *test = qd_tls(connector->tls_domain, 0, 0, 0);
        if (!test) {
            qd_free_adaptor_config(connector->adaptor_config);
            qd_tls_domain_decref(connector->tls_domain);
            free_qd_tcp_connector_t(connector);
            return 0;
        }
        qd_tls_free2(test);
    }

    connector->activate_timer = qd_timer(tcp_context->qd, on_core_activate_TIMER_IO, connector);
    connector->common.context_type = TL_CONNECTOR;
    sys_mutex_init(&connector->lock);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Configured TcpConnector for %s, %s:%s",
            connector->adaptor_config->address, connector->adaptor_config->host, connector->adaptor_config->port);

    DEQ_INSERT_TAIL(tcp_context->connectors, connector);

    TL_setup_connector(connector);

    return connector;
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpConnector(qd_entity_t* entity, void *impl)
{
    SET_THREAD_UNKNOWN;
    qd_tcp_connector_t *cr = (qd_tcp_connector_t*) impl;
    sys_mutex_lock(&cr->lock);
    uint64_t co = cr->connections_opened;
    uint64_t cc = cr->connections_closed;
    sys_mutex_unlock(&cr->lock);

    if (   qd_entity_set_long(entity, "bytesIn",           0) == 0
        && qd_entity_set_long(entity, "bytesOut",          0) == 0
        && qd_entity_set_long(entity, "connectionsOpened", co) == 0
        && qd_entity_set_long(entity, "connectionsClosed", cc) == 0)
    {
        return QD_ERROR_NONE;
    }

    return qd_error_code();
}


//=================================================================================
// Interface to Protocol Adaptor registration
//=================================================================================
static void ADAPTOR_init(qdr_core_t *core, void **adaptor_context)
{
    SET_THREAD_UNKNOWN;
    tcp_context = NEW(qd_tcp_context_t);
    ZERO(tcp_context);

    tcp_context->core   = core;
    tcp_context->qd     = qdr_core_dispatch(core);
    tcp_context->server = tcp_context->qd->server;
    tcp_context->pa     = qdr_protocol_adaptor(core, "tcp", (void*) tcp_context,
                                                   CORE_activate,
                                                   CORE_first_attach,
                                                   CORE_second_attach,
                                                   CORE_detach,
                                                   CORE_flow,
                                                   CORE_offer,
                                                   CORE_drained,
                                                   CORE_drain,
                                                   CORE_push,
                                                   CORE_deliver_outbound,
                                                   CORE_get_credit,
                                                   CORE_delivery_update,
                                                   CORE_connection_close,
                                                   CORE_connection_trace);
    sys_mutex_init(&tcp_context->lock);
    tcp_context->proactor = qd_server_proactor(tcp_context->server);

    //
    // Determine the configured buffer memory ceiling.
    //
    char     *ceiling_string = getenv("SKUPPER_ROUTER_MEMORY_CEILING");
    uint64_t  memory_ceiling = (uint64_t) qd_platform_memory_size();

    //
    // Use 4Gig as a default if the platform fails to return a valid size
    //
    if (memory_ceiling == 0) {
        memory_ceiling = (uint64_t) 4 * (uint64_t) 1024 * (uint64_t) 1024 * (uint64_t) 1024;
    }

    if (!!ceiling_string) {
        long long convert = atoll(ceiling_string);
        if (convert > 0) {
            memory_ceiling = (uint64_t) convert;
        }
    }

    buffer_ceiling = MAX(memory_ceiling / QD_BUFFER_SIZE, 100);
    buffer_threshold_50 = buffer_ceiling / 2;
    buffer_threshold_75 = (buffer_ceiling / 20) * 15;
    buffer_threshold_85 = (buffer_ceiling / 20) * 17;

    const char *mc_unit;
    double mc_normalized = normalize_memory_size(memory_ceiling, &mc_unit);
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Router buffer memory ceiling: %.2f %s (%"PRIu64" buffers)", mc_normalized, mc_unit, buffer_ceiling);
}


static void ADAPTOR_final(void *adaptor_context)
{
    SET_THREAD_UNKNOWN;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO, "Shutting down TCP protocol adaptor");
    while (DEQ_HEAD(tcp_context->listeners)) {
        qd_tcp_listener_t *listener   = DEQ_HEAD(tcp_context->listeners);
        //
        // Deliberately call qd_tcp_listener_incref() to make sure that freeing the connections, don't
        // free the listener. Then we call qd_tcp_listener_free() to forcefully free the listener without checking the listener->ref_count
        //
        qd_tcp_listener_incref(listener);
        qd_tcp_connection_t *conn = DEQ_HEAD(listener->connections);
        while (conn) {
            qd_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            close_connection_XSIDE_IO(conn);
            conn = next_conn;
        }
        qd_tcp_listener_free(listener);
    }

    while (DEQ_HEAD(tcp_context->connectors)) {
        qd_tcp_connector_t *connector   = DEQ_HEAD(tcp_context->connectors);
        //
        // Deliberately call qd_tcp_connector_incref() to make sure that freeing the connections, don't
        // free the connector. Then we call qd_tcp_connector_free() to forcefully free the connector without checking the connector->ref_count
        //
        qd_tcp_connector_incref(connector);

        qd_tcp_connection_t *conn = DEQ_HEAD(connector->connections);
        while (conn) {
            qd_tcp_connection_t *next_conn = DEQ_NEXT(conn);
            close_connection_XSIDE_IO(conn);
            conn = next_conn;
        }
        qd_tcp_connector_free(connector);
    }

    qdr_protocol_adaptor_free(tcp_context->core, tcp_context->pa);
    sys_mutex_free(&tcp_context->lock);
    free(tcp_context);
    tcp_context = 0;
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("tcp", ADAPTOR_init, ADAPTOR_final)

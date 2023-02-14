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
#include <qpid/dispatch/cutthrough_utils.h>
#include <qpid/dispatch/platform.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>
#include <proton/listener.h>

#include "tcp_lite.h"

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

ALLOC_DEFINE(tcplite_listener_t);
ALLOC_DEFINE(tcplite_connector_t);
ALLOC_DEFINE_SAFE(tcplite_connection_t);

static const char *state_names[] =
{ "LSIDE_INITIAL", "LSIDE_LINK_SETUP", "LSIDE_STREAM_START", "LSIDE_FLOW",
  "CSIDE_INITIAL", "CSIDE_RAW_CONNECTION_OPENING", "CSIDE_LINK_SETUP", "CSIDE_FLOW",
  "XSIDE_CLOSING"
};
ENUM_DEFINE(tcplite_connection_state, state_names);

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
    tcplite_listener_list_t    listeners;
    tcplite_connector_list_t   connectors;
    tcplite_connection_list_t  connections;
    sys_mutex_t                lock;
    pn_proactor_t             *proactor;
    bool                       adaptor_finalizing;
} tcplite_context_t;

static tcplite_context_t *tcplite_context;

static uint64_t buffer_ceiling = 0;
static uint64_t buffer_threshold_50;
static uint64_t buffer_threshold_75;
static uint64_t buffer_threshold_85;


//
// Forward References
//
static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context);
static void connection_run_LSIDE_IO(tcplite_connection_t *conn);
static void connection_run_CSIDE_IO(tcplite_connection_t *conn);
static void connection_run_XSIDE_IO(tcplite_connection_t *conn);


//=================================================================================
// Thread assertions
//=================================================================================
typedef enum {
    THREAD_UNKNOWN,
    THREAD_ROUTER_CORE,
    THREAD_TIMER_IO,
    THREAD_RAW_IO
} tcplite_thread_state_t;

__thread tcplite_thread_state_t tcplite_thread_state;

#define SET_THREAD_UNKNOWN     tcplite_thread_state = THREAD_UNKNOWN
#define SET_THREAD_ROUTER_CORE tcplite_thread_state = THREAD_ROUTER_CORE
#define SET_THREAD_TIMER_IO    tcplite_thread_state = THREAD_TIMER_IO
#define SET_THREAD_RAW_IO      tcplite_thread_state = THREAD_RAW_IO

#define ASSERT_ROUTER_CORE assert(tcplite_thread_state == THREAD_ROUTER_CORE || tcplite_context->adaptor_finalizing)
#define ASSERT_TIMER_IO    assert(tcplite_thread_state == THREAD_TIMER_IO    || tcplite_context->adaptor_finalizing)
#define ASSERT_RAW_IO      assert(tcplite_thread_state == THREAD_RAW_IO      || tcplite_context->adaptor_finalizing)


//=================================================================================
// Core Activation Handler
//=================================================================================
/**
 * This function in invoked in a timer thread, not associated with any IO context, in order to
 * process core connections terminated in the adaptor.  The core connections processed here are
 * for listeners and connectors only.  Connection activation happens elsewhere, in the context of
 * a Proton raw IO connection.
 */
static void on_core_activate_TIMER_IO(void *context)
{
    SET_THREAD_TIMER_IO;
    qdr_connection_t *core_conn = ((tcplite_common_t*) context)->core_conn;
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


static qdr_connection_t *TL_open_core_connection(uint64_t conn_id, bool incoming)
{
    qdr_connection_t *conn;
    
    //
    // The qdr_connection_info() function makes its own copy of the passed in tcp_conn_properties.
    // So, we need to call pn_data_free(tcp_conn_properties)
    //
    pn_data_t *properties       = TL_conn_properties();
    qdr_connection_info_t *info = qdr_connection_info(false,        // is_encrypted,
                                                      false,        // is_authenticated,
                                                      true,         // opened,
                                                      "",           // sasl_mechanisms,
                                                      incoming ? QD_INCOMING : QD_OUTGOING,  // dir,
                                                      "tcplite",    // host,
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

    conn = qdr_connection_opened(tcplite_context->core,
                                 tcplite_context->pa,
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

    return conn;
}


static void TL_setup_listener(tcplite_listener_t *li)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this listener
    //
    li->common.conn_id   = qd_server_allocate_connection_id(tcplite_context->server);
    li->common.core_conn = TL_open_core_connection(li->common.conn_id, true);
    qdr_connection_set_context(li->common.core_conn, li);

    //
    // Attach an in-link to represent the desire to send connection streams to the address
    //
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, li->adaptor_config->address);

    li->in_link = qdr_link_first_attach(li->common.core_conn, QD_INCOMING, 0, target, "tcp.listener.in", 0, false, 0, &li->link_id);
    qdr_link_set_context(li->in_link, li);

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
}


static void TL_setup_connector(tcplite_connector_t *cr)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this connector
    //
    cr->common.conn_id   = qd_server_allocate_connection_id(tcplite_context->server);
    cr->common.core_conn = TL_open_core_connection(cr->common.conn_id, false);
    qdr_connection_set_context(cr->common.core_conn, cr);

    //
    // Attach an out-link to represent our desire to receive connection streams for the address
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, cr->adaptor_config->address);

    //
    // Create a vflow record for this connector
    //
    cr->common.vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_PROTOCOL,         "tcp");
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_NAME,             cr->adaptor_config->name);
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_HOST, cr->adaptor_config->host);
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_DESTINATION_PORT, cr->adaptor_config->port);
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_VAN_ADDRESS,      cr->adaptor_config->address);
    vflow_set_uint64(cr->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4,    0);
    vflow_add_rate(cr->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, VFLOW_ATTRIBUTE_FLOW_RATE_L4);

    cr->out_link = qdr_link_first_attach(cr->common.core_conn, QD_OUTGOING, source, 0, "tcp.connector.out", 0, false, 0, &cr->link_id);
    qdr_link_set_context(cr->out_link, cr);
    qdr_link_flow(tcplite_context->core, cr->out_link, 5, false);
}


static void drain_read_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn)
{
    ASSERT_RAW_IO;
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;
    size_t           drained = 0;

    while ((count = pn_raw_connection_take_read_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            qd_buffer_free(buf);
            drained++;
        }
    }
}


static void drain_write_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn)
{
    ASSERT_RAW_IO;
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;
    size_t           drained = 0;

    while ((count = pn_raw_connection_take_written_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            if (!!buf) {
                qd_buffer_free(buf);
            }
            drained++;
        }
    }
}


static void set_state_XSIDE_IO(tcplite_connection_t *conn, tcplite_connection_state_t new_state)
{
    ASSERT_RAW_IO;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] State change %s -> %s",
           conn->common.conn_id, tcplite_connection_state_name(conn->state), tcplite_connection_state_name(new_state));
    conn->state = new_state;
}


static void free_listener(tcplite_listener_t *li)
{
    sys_mutex_lock(&tcplite_context->lock);
    DEQ_REMOVE(tcplite_context->listeners, li);
    sys_mutex_unlock(&tcplite_context->lock);

    vflow_end_record(li->common.vflow);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Deleted TcpListener for %s, %s:%s",
            li->adaptor_config->address, li->adaptor_config->host, li->adaptor_config->port);

    qd_timer_free(li->activate_timer);
    qd_tls_domain_decref(li->tls_domain);
    qd_free_adaptor_config(li->adaptor_config);
    sys_mutex_free(&li->lock);
    free_tcplite_listener_t(li);
}


static void free_connector(tcplite_connector_t *cr)
{
    sys_mutex_lock(&tcplite_context->lock);
    DEQ_REMOVE(tcplite_context->connectors, cr);
    sys_mutex_unlock(&tcplite_context->lock);

    vflow_end_record(cr->common.vflow);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Deleted TcpConnector for %s, %s:%s",
            cr->adaptor_config->address, cr->adaptor_config->host, cr->adaptor_config->port);

    qd_timer_free(cr->activate_timer);
    qd_tls_domain_decref(cr->tls_domain);
    qd_free_adaptor_config(cr->adaptor_config);
    sys_mutex_free(&cr->lock);
    free_tcplite_connector_t(cr);
}


static void free_connection_IO(void *context)
{
    // No thread assertion here - can be RAW_IO or TIMER_IO
    bool free_parent = false;
    tcplite_connection_t *conn = (tcplite_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Cleaning up resources", conn->common.conn_id);

    if (!!conn->common.parent && conn->common.parent->context_type == TL_LISTENER) {
        tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
        sys_mutex_lock(&li->lock);
        DEQ_REMOVE(li->connections, conn);
        if (li->closing && DEQ_SIZE(li->connections) == 0) {
            free_parent = true;
        }
        sys_mutex_unlock(&li->lock);
        if (free_parent) {
            free_listener(li);
        }
    } else {
        tcplite_connector_t *cr = (tcplite_connector_t*) conn->common.parent;
        sys_mutex_lock(&cr->lock);
        DEQ_REMOVE(cr->connections, conn);
        if (cr->closing && DEQ_SIZE(cr->connections) == 0) {
            free_parent = true;
        }
        sys_mutex_unlock(&cr->lock);
        if (free_parent) {
            free_connector(cr);
        }
    }

    sys_atomic_destroy(&conn->core_activation);
    sys_atomic_destroy(&conn->raw_opened);
    qd_timer_free(conn->close_timer);
    free_tcplite_connection_t(conn);
}


static void close_raw_connection_XSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    if (conn->state != XSIDE_CLOSING) {
        set_state_XSIDE_IO(conn, XSIDE_CLOSING);
        if (!!conn->raw_conn) {
            CLEAR_ATOMIC_FLAG(&conn->raw_opened);
            pn_raw_connection_close(conn->raw_conn);
            drain_read_buffers_XSIDE_IO(conn->raw_conn);
            drain_write_buffers_XSIDE_IO(conn->raw_conn);
            conn->raw_conn = 0;
        }
    }
}


static void close_connection_XSIDE_IO(tcplite_connection_t *conn, bool no_delay)
{
    ASSERT_RAW_IO;
    close_raw_connection_XSIDE_IO(conn);

    free(conn->reply_to);

    if (!!conn->inbound_link) {
        qdr_link_detach(conn->inbound_link, QD_LOST, 0);
    }

    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_NONE;
    activation.delivery = 0;
    qd_nullify_safe_ptr(&activation.safeptr);

    if (!!conn->inbound_stream) {
        qd_message_set_producer_activation(conn->inbound_stream, &activation);
    }

    if (!!conn->outbound_stream) {
        qd_message_set_consumer_activation(conn->outbound_stream, &activation);
    }

    if (!!conn->inbound_delivery) {
        if (!!conn->inbound_stream) {
            qd_message_set_receive_complete(conn->inbound_stream);
            qdr_delivery_continue(tcplite_context->core, conn->inbound_delivery, false);
        }

        qdr_delivery_remote_state_updated(tcplite_context->core, conn->inbound_delivery, 0, true, 0, false);
        qdr_delivery_set_context(conn->inbound_delivery, 0);
        qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "close_connection_XSIDE_IO - inbound_delivery");
    }

    if (!!conn->outbound_link) {
        qdr_link_detach(conn->outbound_link, QD_LOST, 0);
    }

    if (!!conn->outbound_delivery) {
        qdr_delivery_remote_state_updated(tcplite_context->core, conn->outbound_delivery, PN_MODIFIED, true, 0, false);
        qdr_delivery_set_context(conn->outbound_delivery, 0);
        qdr_delivery_decref(tcplite_context->core, conn->outbound_delivery, "close_connection_XSIDE_IO - outbound_delivery");
    }

    if (!!conn->common.core_conn) {
        qdr_connection_closed(conn->common.core_conn);
    }

    if (!!conn->common.vflow) {
        vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, conn->inbound_octets);
        vflow_end_record(conn->common.vflow);
    }

    conn->reply_to          = 0;
    conn->inbound_link      = 0;
    conn->inbound_stream    = 0;
    conn->inbound_delivery  = 0;
    conn->outbound_link     = 0;
    conn->outbound_stream   = 0;
    conn->outbound_delivery = 0;
    conn->common.core_conn  = 0;
    conn->common.vflow      = 0;

    if (!!conn->common.parent && conn->common.parent->context_type == TL_LISTENER) {
        tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
        sys_mutex_lock(&li->lock);
        li->connections_closed++;
        sys_mutex_unlock(&li->lock);
    } else {
        tcplite_connector_t *cr = (tcplite_connector_t*) conn->common.parent;
        sys_mutex_lock(&cr->lock);
        cr->connections_closed++;
        sys_mutex_unlock(&cr->lock);
    }

    if (no_delay) {
        free_connection_IO(conn);
    } else {
        conn->close_timer = qd_timer(tcplite_context->qd, free_connection_IO, conn);
        qd_timer_schedule(conn->close_timer, CONNECTION_CLOSE_TIME);
    }
}


static void grant_read_buffers_XSIDE_IO(tcplite_connection_t *conn, const size_t capacity)
{
    ASSERT_RAW_IO;

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
    static size_t max_capacity = 0;
    if (capacity > max_capacity) {
        max_capacity = capacity;
    }

    //
    // Get the "held_by_threads" stats for router buffers as an approximation of how many
    // buffers are in-use.  This is an approximation since it also counts free buffers held
    // in the per-thread free-pools.  Since we will be dealing with large numbers here, the
    // number of buffers in free-pools will not be significant.
    //
    // Note that there is a thread race on the access of this value.  There's no danger associated
    // with getting a partial or corrupted value from time to time.
    //
    // Note also that the stats pointer may be NULL if no buffers have yet been allocated.
    //
    qd_alloc_stats_t *stats          = alloc_stats_qd_buffer_t();
    uint64_t          buffers_in_use = !!stats ? stats->held_by_threads : 0;  // Note: Suppressed race here

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
    size_t already_granted = max_capacity - capacity;

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

        pn_raw_connection_give_read_buffers(conn->raw_conn, raw_buffers, granted);

        qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] grant_read_buffers_XSIDE_IO - %ld", conn->common.conn_id, granted);
    }
}


static uint64_t produce_read_buffers_XSIDE_IO(tcplite_connection_t *conn, qd_message_t *stream, bool *blocked)
{
    ASSERT_RAW_IO;
    uint64_t octet_count = 0;

    if (qd_message_can_produce_buffers(stream)) {
        *blocked = false;
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
                } else {
                    qd_buffer_free(buf);
                }
            }
            count = pn_raw_connection_take_read_buffers(conn->raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE);
        }

        if (!DEQ_IS_EMPTY(qd_buffers)) {
            //qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] produce_read_buffers_XSIDE_IO - Producing %ld buffers", conn->common.conn_id, DEQ_SIZE(qd_buffers));
            qd_message_produce_buffers(stream, &qd_buffers);
            cutthrough_notify_buffers_produced_inbound(stream);
        }
    } else {
        *blocked = true;
    }

    return octet_count;
}


static uint64_t consume_write_buffers_XSIDE_IO(tcplite_connection_t *conn, qd_message_t *stream)
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
                raw_buffers[i].context  = (uintptr_t) buf;
                raw_buffers[i].bytes    = (char*) qd_buffer_base(buf);
                raw_buffers[i].capacity = qd_buffer_capacity(buf);
                raw_buffers[i].size     = qd_buffer_size(buf);
                raw_buffers[i].offset   = 0;
                octet_count += raw_buffers[i].size;
                buf = DEQ_NEXT(buf);
            }
            //qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] consume_write_buffers_XSIDE_IO - Consuming %ld buffers", conn->common.conn_id, actual);
            pn_raw_connection_write_buffers(conn->raw_conn, raw_buffers, actual);
            cutthrough_notify_buffers_consumed_outbound(stream);
        }
    }

    return octet_count;
}


static uint64_t consume_message_body_XSIDE_IO(tcplite_connection_t *conn, qd_message_t *stream)
{
    ASSERT_RAW_IO;
    assert(!conn->outbound_body_complete);

    uint64_t octets = 0;
    size_t   offset = 0;

    if (!conn->outbound_body) {
        qd_message_depth_status_t depth_status = qd_message_check_depth(stream, QD_DEPTH_RAW_BODY);
        switch (depth_status) {
        case QD_MESSAGE_DEPTH_INVALID:
            //
            // TODO - Handle corrupt message formats
            //
            break;

        case QD_MESSAGE_DEPTH_OK:
            //
            // If we have complete headers, get the pointer to the buffer containing the first
            // octet of the body and the offset to that octet.  If there are no body octets, we
            // will be given a NULL pointer.
            //
            qd_message_raw_body_and_start_cutthrough(stream, &conn->outbound_body, &offset);
            break;

        case QD_MESSAGE_DEPTH_INCOMPLETE:
            //
            // We don't have all of the header data for the stream.  Do nothing and wait for
            // more stuff to arrive.
            //
            return 0;
        }
    }

    //
    // Process classic (non cut-though) body buffers until they are all sent onto the raw connection.
    // Note that this may take multiple runs through this function if there is any back-pressure
    // outbound on the raw connection.
    //
    // Note: There may be a non-zero offset only on the first body buffer.  It is assumed that 
    //       every subsequent buffer will have an offset of 0.
    //
    while (!!conn->outbound_body && pn_raw_connection_write_buffers_capacity(conn->raw_conn) > 0) {
        pn_raw_buffer_t raw_buffer;
        raw_buffer.context  = 0;
        raw_buffer.bytes    = (char*) qd_buffer_base(conn->outbound_body);
        raw_buffer.capacity = qd_buffer_capacity(conn->outbound_body);
        raw_buffer.size     = qd_buffer_size(conn->outbound_body) - offset;
        raw_buffer.offset   = offset;
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


static void link_setup_LSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_t *source = qdr_terminus(0);

    qdr_terminus_set_address(target, li->adaptor_config->address);
    qdr_terminus_set_dynamic(source);
    
    conn->common.core_conn = TL_open_core_connection(conn->common.conn_id, true);
    qdr_connection_set_context(conn->common.core_conn, conn);

    conn->inbound_link = qdr_link_first_attach(conn->common.core_conn, QD_INCOMING, qdr_terminus(0), target, "tcp.lside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
    conn->outbound_link = qdr_link_first_attach(conn->common.core_conn, QD_OUTGOING, source, qdr_terminus(0), "tcp.lside.out", 0, false, 0, &conn->outbound_link_id);
    qdr_link_set_context(conn->outbound_link, conn);
    qdr_link_set_user_streaming(conn->outbound_link);
    qdr_link_flow(tcplite_context->core, conn->outbound_link, 1, false);
}


static void link_setup_CSIDE_IO(tcplite_connection_t *conn, qdr_delivery_t *delivery)
{
    ASSERT_RAW_IO;
    qdr_terminus_t *target = qdr_terminus(0);

    qdr_terminus_set_address(target, conn->reply_to);

    conn->common.core_conn = TL_open_core_connection(conn->common.conn_id, false);
    qdr_connection_set_context(conn->common.core_conn, conn);

    conn->inbound_link = qdr_link_first_attach(conn->common.core_conn, QD_INCOMING, qdr_terminus(0), target, "tcp.cside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
    conn->outbound_link = qdr_link_first_attach(conn->common.core_conn, QD_OUTGOING, qdr_terminus(0), qdr_terminus(0), "tcp.cside.out", 0, false, delivery, &conn->inbound_link_id);
    qdr_link_set_context(conn->outbound_link, conn);
}


static bool try_compose_and_send_client_stream_LSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    tcplite_listener_t  *li = (tcplite_listener_t*) conn->common.parent;
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

        message = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, message);
        qd_compose_insert_null(message);
    }

    if (message == 0) {
        return false;
    }

    conn->inbound_stream = qd_message();
    qd_message_set_streaming_annotation(conn->inbound_stream);

    qd_message_compose_2(conn->inbound_stream, message, false);
    qd_compose_free(message);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_start_unicast_cutthrough(conn->inbound_stream);
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_message_set_producer_activation(conn->inbound_stream, &activation);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    conn->inbound_delivery = qdr_link_deliver(conn->inbound_link, conn->inbound_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
            "[C%" PRIu64 "][L%" PRIu64 "] Initiating listener side empty client stream message",
            conn->common.conn_id, conn->inbound_link_id);

    return true;
}


static void compose_and_send_server_stream_CSIDE_IO(tcplite_connection_t *conn)
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

    qd_message_compose_2(conn->inbound_stream, message, false);
    qd_compose_free(message);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_start_unicast_cutthrough(conn->inbound_stream);
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_message_set_producer_activation(conn->inbound_stream, &activation);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    conn->inbound_delivery = qdr_link_deliver(conn->inbound_link, conn->inbound_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG,
            "[C%" PRIu64 "][L%" PRIu64 "] Initiating connector side empty server stream message",
            conn->common.conn_id, conn->inbound_link_id);
}


static void extract_metadata_from_stream_CSIDE(tcplite_connection_t *conn)
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


static void handle_outbound_delivery_LSIDE_IO(tcplite_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery)
{
    ASSERT_RAW_IO;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] handle_outbound_delivery_LSIDE_IO - receive_complete=%s",
           conn->common.conn_id, qd_message_receive_complete(conn->outbound_stream) ? "true" : "false");

    if (!conn->outbound_delivery) {
        qdr_delivery_incref(delivery, "handle_outbound_delivery_LSIDE_IO");
        conn->outbound_delivery = delivery;
        conn->outbound_stream   = qdr_delivery_message(delivery);
        qdr_delivery_set_context(delivery, conn);

        //
        // Note that we do not start_unicast_cutthrough here.  This is done in a more orderly way during
        // stream-content consumption.
        //
        qd_message_activation_t activation;
        activation.type     = QD_ACTIVATION_TCP;
        activation.delivery = 0;
        qd_alloc_set_safe_ptr(&activation.safeptr, conn);
        qd_message_set_consumer_activation(conn->outbound_stream, &activation);
    }

    connection_run_LSIDE_IO(conn);
}


/**
 * Handle the first indication of a new outbound delivery on CSIDE.  This is where the raw connection to the
 * external service is established.  This function executes in an IO thread not associated with a raw connection.
 */
static void handle_first_outbound_delivery_CSIDE(tcplite_connector_t *cr, qdr_link_t *link, qdr_delivery_t *delivery)
{
    ASSERT_TIMER_IO;
    assert(!qdr_delivery_get_context(delivery));

    tcplite_connection_t *conn = new_tcplite_connection_t();
    ZERO(conn);

    qdr_delivery_incref(delivery, "CORE_deliver_outbound CSIDE");
    qdr_delivery_set_context(delivery, conn);

    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (tcplite_common_t*) cr;

    sys_atomic_init(&conn->core_activation, 0);
    sys_atomic_init(&conn->raw_opened, 0);

    conn->listener_side     = false;
    conn->state             = CSIDE_RAW_CONNECTION_OPENING;
    conn->outbound_delivery = delivery;
    conn->outbound_link     = link;
    conn->outbound_stream   = qdr_delivery_message(delivery);

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, cr->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

    extract_metadata_from_stream_CSIDE(conn);

    conn->common.conn_id  = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_CSIDE_IO;

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] CSIDE outbound delivery", conn->common.conn_id);

    sys_mutex_lock(&cr->lock);
    DEQ_INSERT_TAIL(cr->connections, conn);
    cr->connections_opened++;
    vflow_set_uint64(cr->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, cr->connections_opened);
    sys_mutex_unlock(&cr->lock);

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);

    //
    // Note that we do not start_unicast_cutthrough here.  This is done in a more orderly way during
    // stream-content consumption.
    //
    qd_message_activation_t activation;
    activation.type     = QD_ACTIVATION_TCP;
    activation.delivery = 0;
    qd_alloc_set_safe_ptr(&activation.safeptr, conn);
    qd_message_set_consumer_activation(conn->outbound_stream, &activation);

    //
    // The raw connection establishment must be the last thing done in this function.
    // After this call, a separate IO thread may immediately be invoked in the context
    // of the new connection to handle raw connection events.
    //
    pn_proactor_raw_connect(tcplite_context->proactor, conn->raw_conn, cr->adaptor_config->host_port);
    SET_ATOMIC_FLAG(&conn->raw_opened);
}


/**
 * Handle subsequent pushes of the outbound delivery on CSIDE.  This is where delivery completion will be 
 * detected and raw connection write-close will occur.
 */
static void handle_outbound_delivery_CSIDE(tcplite_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] handle_outbound_delivery_CSIDE - receive_complete=%s",
           conn->common.conn_id, qd_message_receive_complete(conn->outbound_stream) ? "true" : "false");

    //
    // It is not guaranteed that this function will be called on the proper IO thread.  Wake the raw connection for
    // continued processing in the correct context.
    //
    if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) {
        pn_raw_connection_wake(conn->raw_conn);
    }
}


/**
 * Manage the steady-state flow of a bi-directional connection from either-side point of view.
 *
 * @param conn Pointer to the TCP connection record
 * @return true if IO processing should be repeated due to state changes
 * @return false if IO processing should suspend until the next external event
 */
static bool manage_flow_XSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    //
    // Inbound stream (producer-side) processing
    //
    if (!!conn->inbound_stream && !!conn->raw_conn) {
        //
        // Produce available read buffers into the inbound stream
        //
        bool blocked;
        uint64_t octet_count = produce_read_buffers_XSIDE_IO(conn, conn->inbound_stream, &blocked);
        conn->inbound_octets += octet_count;

        if (octet_count > 0) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw read: Produced %"PRIu64" octets into stream", conn->common.conn_id, conn->listener_side ? 'L' : 'C', octet_count);
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
        if (pn_raw_connection_is_read_closed(conn->raw_conn) && !blocked) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Read-closed - close inbound delivery", conn->common.conn_id);
            qd_message_set_receive_complete(conn->inbound_stream);
            qdr_delivery_continue(tcplite_context->core, conn->inbound_delivery, false);
            qdr_delivery_set_context(conn->inbound_delivery, 0);
            qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "TCP_LSIDE_IO - read-close");
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
            if (body_octets > 0) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw write: Consumed %"PRIu64" octets from stream (body-field)", conn->common.conn_id, conn->listener_side ? 'L' : 'C', body_octets);
            }
        }

        //
        // Consume available write buffers from the outbound stream
        //
        if (conn->outbound_body_complete) {
            uint64_t octets = consume_write_buffers_XSIDE_IO(conn, conn->outbound_stream);
            conn->outbound_octets += octets;

            if (octets > 0) {
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] %cSIDE Raw write: Consumed %"PRIu64" octets from stream", conn->common.conn_id, conn->listener_side ? 'L' : 'C', octets);
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
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Rx-complete, rings empty: Write-closing the raw connection", conn->common.conn_id);
            pn_raw_connection_write_close(conn->raw_conn);
            qdr_delivery_set_disposition(conn->outbound_delivery, PN_ACCEPTED);
            qdr_delivery_set_context(conn->outbound_delivery, 0);
            qdr_delivery_decref(tcplite_context->core, conn->outbound_delivery, "manage_flow_XSIDE_IO - release outbound");
            conn->outbound_delivery = 0;
            conn->outbound_stream   = 0;
        }
    }

    return false;
}


static void connection_run_LSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    bool repeat;

    do {
        repeat = false;

        switch (conn->state) {
        case LSIDE_INITIAL:
            //
            // Begin the setup of the inbound and outbound links for this connection.
            //
            link_setup_LSIDE_IO(conn);
            set_state_XSIDE_IO(conn, LSIDE_LINK_SETUP);
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
                set_state_XSIDE_IO(conn, LSIDE_FLOW);
                repeat = true;
            }
            break;

        case LSIDE_FLOW:
            //
            // Manage the ongoing bidirectional flow of the connection.
            //
            repeat = manage_flow_XSIDE_IO(conn);
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


static void connection_run_CSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    bool repeat;
    bool credit;

    do {
        repeat = false;

        switch (conn->state) {
        case CSIDE_RAW_CONNECTION_OPENING:
            if (!!conn->error && !!conn->outbound_delivery) {
                //
                // If there was an error during the connection-open, reject the client delivery.
                //
                qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] CSIDE connect error on initial attempt - Rejecting outbound delivery", conn->common.conn_id);
                qdr_delivery_set_context(conn->outbound_delivery, 0);
                qdr_delivery_remote_state_updated(tcplite_context->core, conn->outbound_delivery, PN_REJECTED, true, 0, true); // rejected, settled, ref_given
                conn->outbound_delivery = 0;
                conn->outbound_stream   = 0;

                close_connection_XSIDE_IO(conn, false);
            } else if (!pn_raw_connection_is_read_closed(conn->raw_conn)) {
                link_setup_CSIDE_IO(conn, conn->outbound_delivery);
                set_state_XSIDE_IO(conn, CSIDE_LINK_SETUP);
            }
            break;

        case CSIDE_LINK_SETUP:
            credit = conn->inbound_credit;

            if (credit) {
                compose_and_send_server_stream_CSIDE_IO(conn);
                set_state_XSIDE_IO(conn, CSIDE_FLOW);
                repeat = true;
            }
            break;

        case CSIDE_FLOW:
            //
            // Manage the ongoing bidirectional flow of the connection.
            //
            repeat = manage_flow_XSIDE_IO(conn);
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


static void connection_run_XSIDE_IO(tcplite_connection_t *conn)
{
    ASSERT_RAW_IO;
    //
    // If the inbound stream has a non-zero disposition, there's been an abnormal event on
    // the outbound side of that stream.  Close the raw connection and close out the inbound stuff.
    //
    if (conn->inbound_disposition != 0) {
        if (!!conn->inbound_delivery && !!conn->inbound_stream) {
            qd_message_set_send_complete(conn->inbound_stream);
            qdr_delivery_continue(tcplite_context->core, conn->inbound_delivery, true);
            qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "TCPLITE Closing inbound delivery on error");
            conn->inbound_delivery = 0;
            conn->inbound_stream   = 0;
        }
        close_raw_connection_XSIDE_IO(conn);
        return;
    }

    if (conn->listener_side) {
        connection_run_LSIDE_IO(conn);
    } else {
        connection_run_CSIDE_IO(conn);
    }
}


//=================================================================================
// Handlers for events from the Raw Connections
//=================================================================================
static void on_connection_event_LSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    SET_THREAD_RAW_IO;
    tcplite_connection_t *conn = (tcplite_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] on_connection_event_LSIDE_IO: %s", conn->common.conn_id, pn_event_type_name(pn_event_type(e)));

    if (pn_event_type(e) == PN_RAW_CONNECTION_DISCONNECTED) {
        close_connection_XSIDE_IO(conn, false);
        return;
    }

    if (CLEAR_ATOMIC_FLAG(&conn->core_activation) && !!conn->common.core_conn) {
        qdr_connection_process(conn->common.core_conn);
    }

    connection_run_LSIDE_IO(conn);
}


static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    SET_THREAD_RAW_IO;
    tcplite_connection_t *conn = (tcplite_connection_t*) context;
    qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] on_connection_event_CSIDE_IO: %s", conn->common.conn_id, pn_event_type_name(pn_event_type(e)));

    if (pn_event_type(e) == PN_RAW_CONNECTION_DISCONNECTED) {
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
        
        if (conn->state == CSIDE_FLOW || conn->state == CSIDE_LINK_SETUP) {
            close_connection_XSIDE_IO(conn, false);
            return;
        }
    }

    if (CLEAR_ATOMIC_FLAG(&conn->core_activation) && !!conn->common.core_conn) {
        qdr_connection_process(conn->common.core_conn);
    }

    connection_run_CSIDE_IO(conn);
}


void on_accept(qd_adaptor_listener_t *listener, pn_listener_t *pn_listener, void *context)
{
    tcplite_listener_t *li = (tcplite_listener_t*) context;

    tcplite_connection_t *conn = new_tcplite_connection_t();
    ZERO(conn);

    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (tcplite_common_t*) li;

    sys_atomic_init(&conn->core_activation, 0);
    sys_atomic_init(&conn->raw_opened, 1);

    conn->listener_side = true;
    conn->state         = LSIDE_INITIAL;

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, li->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

    conn->common.conn_id  = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_LSIDE_IO;

    sys_mutex_lock(&li->lock);
    DEQ_INSERT_TAIL(li->connections, conn);
    li->connections_opened++;
    vflow_set_uint64(li->common.vflow, VFLOW_ATTRIBUTE_FLOW_COUNT_L4, li->connections_opened);
    sys_mutex_unlock(&li->lock);

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);
    pn_listener_raw_accept(pn_listener, conn->raw_conn);
}

//=================================================================================
// Callbacks from the Core Module
//=================================================================================
static void CORE_activate(void *context, qdr_connection_t *core_conn)
{
    SET_THREAD_ROUTER_CORE;
    tcplite_common_t     *common = (tcplite_common_t*) qdr_connection_get_context(core_conn);
    tcplite_connection_t *conn;

    switch (common->context_type) {
    case TL_LISTENER:
        qd_timer_schedule(((tcplite_listener_t*) common)->activate_timer, 0);
        break;

    case TL_CONNECTOR:
        qd_timer_schedule(((tcplite_connector_t*) common)->activate_timer, 0);
        break;

    case TL_CONNECTION:
        conn = (tcplite_connection_t*) common;
        if (IS_ATOMIC_FLAG_SET(&conn->raw_opened)) {
            SET_ATOMIC_FLAG(&conn->core_activation);
            pn_raw_connection_wake(conn->raw_conn);
        }
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
    tcplite_common_t *common = (tcplite_common_t*) qdr_connection_get_context(conn);
    qdr_link_set_context(link, common);

    qdr_terminus_t *local_source = qdr_terminus(0);
    qdr_terminus_t *local_target = qdr_terminus(0);

    qdr_terminus_set_address_iterator(local_source, qdr_terminus_get_address(target));
    qdr_terminus_set_address_iterator(local_target, qdr_terminus_get_address(source));
    qdr_link_second_attach(link, local_source, local_target);

    if (qdr_link_direction(link) == QD_OUTGOING) {
        qdr_link_flow(tcplite_context->core, link, 1, false);
    }
}


static void CORE_second_attach(void           *context,
                               qdr_link_t     *link,
                               qdr_terminus_t *source,
                               qdr_terminus_t *target)
{
    tcplite_common_t *common = (tcplite_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
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
    tcplite_common_t *common = (tcplite_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_LISTENER) {
        tcplite_listener_t *li = (tcplite_listener_t*) common;
        if (!li->adaptor_listener) {
            //
            // There is no adaptor listener.  We need to allocate one.
            //
            li->adaptor_listener = qd_adaptor_listener(tcplite_context->qd, li->adaptor_config, LOG_TCP_ADAPTOR);

            //
            // Start listening on the socket
            //
            qd_adaptor_listener_listen(li->adaptor_listener, on_accept, li);
        }
    } else if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
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
    return qdr_link_process_deliveries(tcplite_context->core, link, limit);
}


static uint64_t CORE_deliver_outbound(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    tcplite_common_t *common = (tcplite_common_t*) qdr_delivery_get_context(delivery);
    if (!common) {
        common = (tcplite_common_t*) qdr_link_get_context(link);
    }

    if (common->context_type == TL_CONNECTOR) {
        handle_first_outbound_delivery_CSIDE((tcplite_connector_t*) common, link, delivery);
    } else if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
        if (conn->listener_side) {
            handle_outbound_delivery_LSIDE_IO(conn, link, delivery);
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
    tcplite_common_t *common = (tcplite_common_t*) qdr_delivery_get_context(dlv);
    if (!!common && common->context_type == TL_CONNECTION && disp != 0) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;

        if (dlv == conn->outbound_delivery) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Outbound delivery update - disposition: %s", conn->common.conn_id, pn_disposition_type_name(disp));
            conn->outbound_disposition = disp;
            need_wake = true;
        } else if (dlv == conn->inbound_delivery) {
            qd_log(LOG_TCP_ADAPTOR, QD_LOG_DEBUG, "[C%"PRIu64"] Inbound delivery update - disposition: %s", conn->common.conn_id, pn_disposition_type_name(disp));
            conn->inbound_disposition = disp;
            need_wake = true;
        }

        if (need_wake) {
            connection_run_XSIDE_IO(conn);
        }
    }
}


static void CORE_connection_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
}


static void CORE_connection_trace(void *context, qdr_connection_t *conn, bool trace)
{
}


//=================================================================================
// Entrypoints for Management
//=================================================================================
#define TCP_NUM_ALPN_PROTOCOLS 2
// const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"h2", "http/1.1", "http/1.0"};
const char *tcp_alpn_protocols[TCP_NUM_ALPN_PROTOCOLS] = {"http/1.1", "h2"};

QD_EXPORT void *qd_dispatch_configure_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    SET_THREAD_UNKNOWN;
    tcplite_listener_t *li = new_tcplite_listener_t();
    ZERO(li);

    li->adaptor_config = new_qd_adaptor_config_t();

    if (qd_load_adaptor_config(li->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp listener: %s", qd_error_message());
        free_tcplite_listener_t(li);
        return 0;
    }

    if (li->adaptor_config->ssl_profile_name) {
        // On the TCP TLS listener side, send "http/1.1", "http/1.0" and "h2" as ALPN protocols
        li->tls_domain = qd_tls_domain(li->adaptor_config, qd, LOG_TCP_ADAPTOR, tcp_alpn_protocols,
                                       TCP_NUM_ALPN_PROTOCOLS, true);
        if (!li->tls_domain) {
            // note qd_tls_domain logged the error
            free_tcplite_listener_t(li);
            return 0;
        }
    }

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Configured TcpListener for %s, %s:%s",
            li->adaptor_config->address, li->adaptor_config->host, li->adaptor_config->port);

    li->activate_timer = qd_timer(tcplite_context->qd, on_core_activate_TIMER_IO, li);
    li->common.context_type = TL_LISTENER;
    sys_mutex_init(&li->lock);

    sys_mutex_lock(&tcplite_context->lock);
    DEQ_INSERT_TAIL(tcplite_context->listeners, li);
    sys_mutex_unlock(&tcplite_context->lock);

    TL_setup_listener(li);

    return li;
}


QD_EXPORT void qd_dispatch_delete_tcp_listener(qd_dispatch_t *qd, void *impl)
{
    SET_THREAD_UNKNOWN;
    tcplite_listener_t *li = (tcplite_listener_t*) impl;
    if (li) {
        li->closing = true;

        if (!tcplite_context->adaptor_finalizing) {
            if (!!li->common.core_conn) {
                qdr_connection_closed(li->common.core_conn);
            }

            if (!!li->adaptor_listener) {
                qd_adaptor_listener_close(li->adaptor_listener);
            }
        } else {
            tcplite_connection_t *conn = DEQ_HEAD(li->connections);
            if (!!conn) {
                while (conn) {
                    tcplite_connection_t *next_conn = DEQ_NEXT(conn);
                    close_connection_XSIDE_IO(conn, tcplite_context->adaptor_finalizing);
                    conn = next_conn;
                }
            } else {
                free_listener(li);
            }
        }
    }
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpListener(qd_entity_t* entity, void *impl)
{
    SET_THREAD_UNKNOWN;
    tcplite_listener_t *li = (tcplite_listener_t*) impl;
    uint64_t co = 0;
    uint64_t cc = 0;
    qd_listener_oper_status_t os = QD_LISTENER_OPER_DOWN;

    if (!!li->adaptor_listener) {
        sys_mutex_lock(&li->lock);
        os = qd_adaptor_listener_oper_status(li->adaptor_listener);
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


QD_EXPORT void *qd_dispatch_configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    SET_THREAD_UNKNOWN;
    tcplite_connector_t *cr = new_tcplite_connector_t();
    ZERO(cr);

    cr->adaptor_config = new_qd_adaptor_config_t();

    if (qd_load_adaptor_config(cr->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_TCP_ADAPTOR, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        free_tcplite_connector_t(cr);
        return 0;
    }

    if (cr->adaptor_config->ssl_profile_name) {
        cr->tls_domain = qd_tls_domain(cr->adaptor_config, qd, LOG_TCP_ADAPTOR, 0, 0, false);
        if (!cr->tls_domain) {
            // note qd_tls_domain() logged the error
            free_tcplite_connector_t(cr);
            return 0;
        }
    }

    cr->activate_timer = qd_timer(tcplite_context->qd, on_core_activate_TIMER_IO, cr);
    cr->common.context_type = TL_CONNECTOR;
    sys_mutex_init(&cr->lock);

    qd_log(LOG_TCP_ADAPTOR, QD_LOG_INFO,
            "Configured TcpConnector for %s, %s:%s",
            cr->adaptor_config->address, cr->adaptor_config->host, cr->adaptor_config->port);

    DEQ_INSERT_TAIL(tcplite_context->connectors, cr);

    TL_setup_connector(cr);

    return cr;
}


QD_EXPORT void qd_dispatch_delete_tcp_connector(qd_dispatch_t *qd, void *impl)
{
    SET_THREAD_UNKNOWN;
    tcplite_connector_t *cr = (tcplite_connector_t*) impl;
    if (cr) {
        cr->closing = true;

        if (!tcplite_context->adaptor_finalizing) {
            qdr_connection_closed(cr->common.core_conn);
        } else {
            tcplite_connection_t *conn = DEQ_HEAD(cr->connections);
            if (!!conn) {
                while (conn) {
                    tcplite_connection_t *next_conn = DEQ_NEXT(conn);
                    close_connection_XSIDE_IO(conn, tcplite_context->adaptor_finalizing);
                    conn = next_conn;
                }
            } else {
                free_connector(cr);
            }
        }
    }
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpConnector(qd_entity_t* entity, void *impl)
{
    SET_THREAD_UNKNOWN;
    tcplite_connector_t *cr = (tcplite_connector_t*) impl;

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
    tcplite_context = NEW(tcplite_context_t);
    ZERO(tcplite_context);

    tcplite_context->core   = core;
    tcplite_context->qd     = qdr_core_dispatch(core);
    tcplite_context->server = tcplite_context->qd->server;
    tcplite_context->pa     = qdr_protocol_adaptor(core, "tcp_lite", (void*) tcplite_context,
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
    sys_mutex_init(&tcplite_context->lock);
    tcplite_context->proactor = qd_server_proactor(tcplite_context->server);

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
    tcplite_context->adaptor_finalizing = true;

    while (DEQ_HEAD(tcplite_context->connectors)) {
        tcplite_connector_t *cr   = DEQ_HEAD(tcplite_context->connectors);
        qd_dispatch_delete_tcp_connector(tcplite_context->qd, cr);
    }

    while (DEQ_HEAD(tcplite_context->listeners)) {
        tcplite_listener_t *li   = DEQ_HEAD(tcplite_context->listeners);
        qd_dispatch_delete_tcp_listener(tcplite_context->qd, li);
    }

    qdr_protocol_adaptor_free(tcplite_context->core, tcplite_context->pa);
    sys_mutex_free(&tcplite_context->lock);
    free(tcplite_context);
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("tcp-lite", ADAPTOR_init, ADAPTOR_final)

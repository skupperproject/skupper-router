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

#include "core_link_endpoint.h"
#include "module.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/discriminator.h"
#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/router_core.h"
#include "qpid/dispatch/protocol_adaptor.h"

#include <stdio.h>

//
// Structure to track each connected peer in the edge-mesh
//
typedef struct mesh_peer_t {
    DEQ_LINKS(struct mesh_peer_t);
    qdr_connection_t *conn;                // Connection to the peer router
    qdrc_endpoint_t  *sender;              // Sender endpoint for sending bids
    qdrc_endpoint_t  *receiver;            // Receiver endpoint for receiving bids
    int               send_credit;         // Credits available to send to the peer 
    long              negotiation_ordinal; // The peer's ordinal - used to find the winner in negotiation
    bool              needs_update;        // If true, the peer is due for a new bid
} mesh_peer_t;

ALLOC_DECLARE(mesh_peer_t);
ALLOC_DEFINE(mesh_peer_t);

DEQ_DECLARE(mesh_peer_t, mesh_peer_list_t);

//
// Global state for the module
//
static struct {
    qdr_core_t                *core;                   // Core module pointer
    qdrc_endpoint_desc_t       endpoint_desc;          // Endpoint descriptor for the module
    qdrc_event_subscription_t *event_sub;              // Subscription for core events
    qdr_connection_t          *interior_connection;    // The active connection to the interior
    qdrc_endpoint_t           *interior_sender;        // Sender endpoint for sending updates to the interior
    int                        interior_sender_credit; // Credits available to send on the interior_sender
    bool                       interior_needs_update;  // If true, the interior is due an update
    mesh_peer_list_t           peers;                  // List of active connected mesh peers
    long                       my_negotiation_ordinal;             // This router's negotiating ordinal
    char                       my_mesh_id[QD_DISCRIMINATOR_BYTES]; // This router's proposed mesh ID
    long                       winning_negotiation_ordinal;        // The winning ordinal in the negotiation (the winning id is in *core)
} state;


static mesh_peer_t *peer_for_connection(qdr_connection_t *conn)
{
    if (!!conn) {
        mesh_peer_t *peer = DEQ_HEAD(state.peers);
        while (!!peer) {
            if (peer->conn == conn) {
                return peer;
            }
            peer = DEQ_NEXT(peer);
        }
    }
    return 0;
}


static void send_mesh_id_to_interior(void)
{
    if (!!state.interior_sender && state.interior_sender_credit > 0) {
        qd_message_t *msg = qd_message();

        qd_composed_field_t *content = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
        qd_compose_start_map(content);
        qd_compose_insert_symbol(content, QD_KEY_MESH_ID_ANNOUNCE_IDENTIFIER);
        qd_compose_insert_string_n(content, state.core->edge_mesh_identifier, QD_DISCRIMINATOR_BYTES);
        qd_compose_end_map(content);

        qd_message_compose_2(msg, content, true);
        qd_compose_free(content);
        
        qdr_delivery_t *delivery = qdrc_endpoint_delivery_CT(state.core, state.interior_sender, msg);
        qdrc_endpoint_send_CT(state.core, state.interior_sender, delivery, true);

        state.interior_sender_credit--;
        state.interior_needs_update = false;

        qd_log(state.core->log, QD_LOG_INFO, "EDGE_MESH - Mesh identifier sent to Interior: %s", state.core->edge_mesh_identifier);
    } else {
        state.interior_needs_update = true;
    }
}


static void send_bid(mesh_peer_t *peer)
{
    qd_message_t *msg = qd_message();

    qd_composed_field_t *content = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(content);
    qd_compose_insert_symbol(content, QD_KEY_MESH_ID_NEGOTIATION_ORDINAL);
    qd_compose_insert_long(content, state.my_negotiation_ordinal);
    qd_compose_insert_symbol(content, QD_KEY_MESH_ID_NEGOTIATION_IDENTIFIER);
    qd_compose_insert_string_n(content, state.my_mesh_id, QD_DISCRIMINATOR_BYTES);
    qd_compose_end_map(content);

    qd_message_compose_2(msg, content, true);
    qd_compose_free(content);
    
    qdr_delivery_t *delivery = qdrc_endpoint_delivery_CT(state.core, peer->sender, msg);
    qdrc_endpoint_send_CT(state.core, peer->sender, delivery, true);

    peer->send_credit--;
    peer->needs_update = false;
}


//
// Generate a proposed mesh identifier and a negotiating ordinal.
// During negotiation, the router with the numerically highest ordinal will
// win the negotiation.  The winner's proposed identifier is used by all
// of the routers in the mesh.
//
static void generate_id(void)
{
    state.my_negotiation_ordinal = random();
    qd_generate_discriminator(state.my_mesh_id);
}


static void start_negotiation(void)
{
    //
    // Begin the process of negotiation.  Start by assuming we are the winner.
    //
    state.winning_negotiation_ordinal = state.my_negotiation_ordinal;
    memcpy(state.core->edge_mesh_identifier, state.my_mesh_id, QD_DISCRIMINATOR_BYTES);

    //
    // If possible, send our present results to the interior
    //
    send_mesh_id_to_interior();

    //
    // For every connected peer that we are able to send to, send our bid.
    //
    mesh_peer_t *peer = DEQ_HEAD(state.peers);
    while (!!peer) {
        peer->needs_update = true;
        if (!!peer->sender && peer->send_credit > 0) {
            send_bid(peer);
        }
        peer = DEQ_NEXT(peer);
    }
}


static void on_first_attach(void             *bind_context,
                            qdrc_endpoint_t  *endpoint,
                            void            **link_context,
                            qdr_terminus_t   *remote_source,
                            qdr_terminus_t   *remote_target)
{
    qd_iterator_t *iter = qdr_terminus_get_address(remote_target);
    qd_iterator_reset_view(iter, ITER_VIEW_ALL);

    qdr_connection_t *conn = qdrc_endpoint_get_connection_CT(endpoint);
    mesh_peer_t      *peer = peer_for_connection(conn);

    if (!!peer
        && qd_iterator_equal(iter, (unsigned const char*) QD_TERMINUS_MESH_ID_NEGOTIATION)
        && qdrc_endpoint_get_direction_CT(endpoint) == QD_INCOMING) {
        peer->receiver = endpoint;
        qdrc_endpoint_second_attach_CT(state.core, endpoint, remote_target, remote_source);
        qdrc_endpoint_flow_CT(state.core, peer->receiver, 3, false);
        *link_context = (void*) peer;
    } else {
        qdrc_endpoint_detach_CT(state.core, endpoint, 0);
        qdr_terminus_free(remote_source);
        qdr_terminus_free(remote_target);
    }
}


static void on_second_attach(void *link_context, qdr_terminus_t *remote_source, qdr_terminus_t *remote_target)
{
    qdr_terminus_free(remote_source);
    qdr_terminus_free(remote_target);
}



static void on_flow(void *link_context, int credit, bool drain)
{
    if (link_context == 0) {
        state.interior_sender_credit = drain ? 0 : state.interior_sender_credit + credit;
        if (state.interior_needs_update && state.interior_sender_credit > 0) {
            send_mesh_id_to_interior();
        }
    } else {
        mesh_peer_t *peer = (mesh_peer_t*) link_context;

        peer->send_credit = drain ? 0 : peer->send_credit + credit;
        if (peer->needs_update && peer->send_credit > 0) {
            send_bid(peer);
        }
    }
}


static void on_transfer(void *link_context, qdr_delivery_t *delivery, qd_message_t *message)
{
    mesh_peer_t *peer = (mesh_peer_t*) link_context;
    long ordinal = 0;
    char id[QD_DISCRIMINATOR_BYTES];
    int  values_found = 0;

    if (qd_message_check_depth(message, QD_DEPTH_APPLICATION_PROPERTIES) == QD_MESSAGE_DEPTH_OK) {
        qd_iterator_t     *iter = qd_message_field_iterator(message, QD_FIELD_APPLICATION_PROPERTIES);
        qd_parsed_field_t *ap   = qd_parse(iter);
         if (!!ap) {
            if (qd_parse_is_map(ap)) {
                for (int i = 0; i < qd_parse_sub_count(ap); i++) {
                    qd_iterator_t     *key_iter = qd_parse_raw(qd_parse_sub_key(ap, i));
                    qd_parsed_field_t *value    = qd_parse_sub_value(ap, i);

                    if        (qd_iterator_equal(key_iter, (unsigned char*) QD_KEY_MESH_ID_NEGOTIATION_ORDINAL)) {
                        ordinal = qd_parse_as_long(value);
                        values_found++;
                    } else if (qd_iterator_equal(key_iter, (unsigned char*) QD_KEY_MESH_ID_NEGOTIATION_IDENTIFIER)) {
                        qd_iterator_ncopy(qd_parse_raw(value), (unsigned char*) id, QD_DISCRIMINATOR_BYTES);
                        values_found++;
                    }
                }
            }
            qd_parse_free(ap);
            qd_iterator_free(iter);
         }
    }

    if (values_found == 2) {
        if (ordinal == state.my_negotiation_ordinal) {
            //
            // If we receive a bid with the same ordinal (should be very rare),
            // generate a new ID and ordinal and start over.
            //
            generate_id();
            start_negotiation();
        } else {
            peer->negotiation_ordinal = ordinal;
            if (ordinal > state.winning_negotiation_ordinal) {
                //
                // This peer's ordinal trumps ours.  Assume the winning identity and
                // notify the interior.
                //
                state.winning_negotiation_ordinal = ordinal;
                memcpy(state.core->edge_mesh_identifier, id, QD_DISCRIMINATOR_BYTES);
                send_mesh_id_to_interior();
            }
        }
    }

    qdrc_endpoint_settle_CT(state.core, delivery, PN_ACCEPTED);

    //
    // Replenish the credit for this delivery
    //
    qdrc_endpoint_flow_CT(state.core, peer->receiver, 1, false);
}


static void on_peer_connection_opened(qdr_connection_t *conn)
{
    mesh_peer_t *peer = new_mesh_peer_t();
    ZERO(peer);
    peer->conn         = conn;
    peer->needs_update = true;
    DEQ_INSERT_TAIL(state.peers, peer);

    //
    // Attach an outgoing link to the peer for ID negotiation
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, QD_TERMINUS_MESH_ID_NEGOTIATION);
    peer->sender = qdrc_endpoint_create_link_CT(state.core, conn, QD_OUTGOING, source, target, &state.endpoint_desc, peer);
}


static void on_peer_connection_closed(qdr_connection_t *conn)
{
    mesh_peer_t *peer = peer_for_connection(conn);

    if (!!peer) {
        //
        // Remove this peer from our list of connected routers.
        //
        DEQ_REMOVE(state.peers, peer);

        if (peer->negotiation_ordinal == state.winning_negotiation_ordinal) {
            //
            // We lost the peer with the winning identity.  We need to re-negotiate.
            //
            start_negotiation();
        }

        free_mesh_peer_t(peer);
    }
}


static void on_interior_connection_established(qdr_connection_t *conn)
{
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, QD_TERMINUS_MESH_DISCOVERY);

    state.interior_connection   = conn;
    state.interior_needs_update = true;
    state.interior_sender       = qdrc_endpoint_create_link_CT(state.core, conn, QD_OUTGOING, source, target, &state.endpoint_desc, 0);
}


static void on_interior_connection_lost(qdr_connection_t *conn)
{
    state.interior_connection    = 0;
    state.interior_sender        = 0;
    state.interior_sender_credit = 0;
    state.interior_needs_update  = false;
}


static void on_connection_event(void *context, qdrc_event_t event, qdr_connection_t *conn)
{
    switch(event) {
    case QDRC_EVENT_CONN_MESH_PEER_ESTABLISHED:
        on_peer_connection_opened(conn);
        break;

    case QDRC_EVENT_CONN_MESH_PEER_LOST:
        on_peer_connection_closed(conn);
        break;

    case QDRC_EVENT_CONN_EDGE_ESTABLISHED:
        on_interior_connection_established(conn);
        break;

    case QDRC_EVENT_CONN_EDGE_LOST:
        on_interior_connection_lost(conn);
        break;

    default:
        break;
    }
}


static bool mesh_discovery_edge_enable_CT(qdr_core_t *core)
{
    return core->router_mode == QD_ROUTER_MODE_EDGE;
}


static void mesh_discovery_edge_init_CT(qdr_core_t *core, void **module_context)
{
    *module_context = &state;
    ZERO(&state);

    state.core = core;
    state.core->edge_mesh_identifier[QD_DISCRIMINATOR_BYTES] = '\0';

    //
    // Bind to the static address QD_TERMINUS_MESH_ID_NEGOTIATION.
    //
    state.endpoint_desc.label            = "mesh_discovery_edge";
    state.endpoint_desc.on_first_attach  = on_first_attach;
    state.endpoint_desc.on_second_attach = on_second_attach;
    state.endpoint_desc.on_flow          = on_flow;
    state.endpoint_desc.on_transfer      = on_transfer;
    qdrc_endpoint_bind_mobile_address_CT(core, QD_TERMINUS_MESH_ID_NEGOTIATION, &state.endpoint_desc, &state);

    //
    // Subscribe to core events.
    //
    state.event_sub = qdrc_event_subscribe_CT(core,
                                              QDRC_EVENT_CONN_MESH_PEER_ESTABLISHED
                                              | QDRC_EVENT_CONN_MESH_PEER_LOST
                                              | QDRC_EVENT_CONN_EDGE_ESTABLISHED
                                              | QDRC_EVENT_CONN_EDGE_LOST,
                                              on_connection_event,
                                              0,
                                              0,
                                              0,
                                              &state);

    //
    // Generate an ID and ordinal for negotiation.
    //
    generate_id();
    start_negotiation();
}


static void mesh_discovery_edge_final_CT(void *module_context)
{
    // If there are any endpoint states still hanging around, clean it up.
    mesh_peer_t *peer = DEQ_HEAD(state.peers);
    while (!!peer) {
        DEQ_REMOVE_HEAD(state.peers);
        free_mesh_peer_t(peer);
        peer = DEQ_HEAD(state.peers);
    }
    qdrc_event_unsubscribe_CT(state.core, state.event_sub);
}


QDR_CORE_MODULE_DECLARE("mesh_discovery_edge", mesh_discovery_edge_enable_CT, mesh_discovery_edge_init_CT, mesh_discovery_edge_final_CT)

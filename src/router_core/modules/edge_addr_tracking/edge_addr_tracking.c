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

#include <stdio.h>

typedef struct qdr_addr_tracking_module_context_t     qdr_addr_tracking_module_context_t;
typedef struct qdr_addr_endpoint_state_t              qdr_addr_endpoint_state_t;

struct qdr_addr_endpoint_state_t {
    DEQ_LINKS(qdr_addr_endpoint_state_t);
    qdrc_endpoint_t                    *endpoint;
    qdr_connection_t                   *conn;    // The connection associated with the endpoint.
    qdr_addr_tracking_module_context_t *mc;
    int                                ref_count;
    bool                               closed; // Is the endpoint that this state belong to closed?
};

DEQ_DECLARE(qdr_addr_endpoint_state_t, qdr_addr_endpoint_state_list_t);
ALLOC_DECLARE(qdr_addr_endpoint_state_t);
ALLOC_DEFINE(qdr_addr_endpoint_state_t);

struct  qdr_addr_tracking_module_context_t {
    qdr_core_t                     *core;
    qdr_addr_endpoint_state_list_t  endpoint_state_list;
    qdrc_event_subscription_t      *event_sub;
    qdrc_endpoint_desc_t           addr_tracking_endpoint;
};


static qd_message_t *qdcm_edge_create_address_dlv(qdr_core_t *core, qdr_address_t   *addr, bool insert_addr)
{
    qd_message_t *msg = qd_message();

    //
    // Start header
    //
    qd_composed_field_t *fld   = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(fld);
    qd_compose_insert_bool(fld, 0);     // durable
    qd_compose_end_list(fld);

    qd_composed_field_t *body = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, 0);

    qd_compose_start_list(body);

    const char *addr_str = (const char*) qd_hash_key_by_handle(addr->hash_handle);

    qd_compose_insert_string(body, addr_str);
    qd_compose_insert_bool(body, insert_addr);
    qd_compose_end_list(body);

    // Finally, compose and return the message so it can be sent out.
    qd_message_compose_3(msg, fld, body, true);

    qd_compose_free(body);
    qd_compose_free(fld);

    return msg;
}

static qdr_addr_endpoint_state_t *qdrc_get_endpoint_state_for_connection(qdr_addr_endpoint_state_list_t  endpoint_state_list, qdr_connection_t *conn)
{
    qdr_addr_endpoint_state_t *endpoint_state = DEQ_HEAD(endpoint_state_list);
    while(endpoint_state) {
        if (endpoint_state->conn == conn) {
            return endpoint_state;
        }
        endpoint_state = DEQ_NEXT(endpoint_state);
    }
    return 0;
}


static void qdrc_address_endpoint_first_attach(void              *bind_context,
                                               qdrc_endpoint_t   *endpoint,
                                               void             **link_context,
                                               qdr_terminus_t   *remote_source,
                                               qdr_terminus_t   *remote_target)
{
    qdr_addr_tracking_module_context_t *bc = (qdr_addr_tracking_module_context_t*)  bind_context;

    //
    // The link to hard coded address QD_TERMINUS_EDGE_ADDRESS_TRACKING should be created only if this is a receiver link
    // and if this link is created inside the QDR_ROLE_EDGE_CONNECTION connection.
    //
    qdr_connection_role_t conn_role = qdrc_endpoint_get_connection_CT(endpoint)->role;
    if (conn_role == QDR_ROLE_EDGE_CONNECTION && qdrc_endpoint_get_direction_CT(endpoint) == QD_OUTGOING) {

        qdr_addr_endpoint_state_t *endpoint_state = new_qdr_addr_endpoint_state_t();
        ZERO(endpoint_state);
        endpoint_state->endpoint  = endpoint;
        endpoint_state->mc        = bc;
        endpoint_state->conn      = qdrc_endpoint_get_connection_CT(endpoint);
        DEQ_INSERT_TAIL(bc->endpoint_state_list, endpoint_state);
        *link_context = endpoint_state;
        qdrc_endpoint_second_attach_CT(bc->core, endpoint, remote_source, remote_target);
    }
    else {
        //
        // We simply detach any links that dont match the above condition.
        //
        qdr_error_t *error;
        if (conn_role != QDR_ROLE_EDGE_CONNECTION)
            error = qdr_error("qd:connection-role", "Connection does not support address tracking");
        else
            error = qdr_error(QD_AMQP_COND_NOT_IMPLEMENTED, "Incoming messages not allowed");
        *link_context = 0;
        qdrc_endpoint_detach_CT(bc->core, endpoint, error);
        qdr_terminus_free(remote_source);
        qdr_terminus_free(remote_target);
    }
}


static void qdrc_address_endpoint_on_first_detach(void *link_context,
                                              qdr_error_t *error)
{
    qdr_addr_endpoint_state_t *endpoint_state  = (qdr_addr_endpoint_state_t*) link_context;
    qdrc_endpoint_detach_CT(endpoint_state->mc->core, endpoint_state->endpoint, 0);
    qdr_error_free(error);
}

static void qdrc_address_endpoint_cleanup(void *link_context)
{
    qdr_addr_endpoint_state_t *endpoint_state  = (qdr_addr_endpoint_state_t*) link_context;
    if (endpoint_state) {
        qdr_addr_tracking_module_context_t *mc = endpoint_state->mc;
        assert (endpoint_state->conn);
        endpoint_state->closed = true;
        if (endpoint_state->ref_count == 0) {

            //
            // The endpoint has been closed and no other links are referencing this endpoint. Time to free it.
            // Clean out all the states held by the link_context (endpoint_state)
            //
            if (mc) {
                DEQ_REMOVE(mc->endpoint_state_list, endpoint_state);
            }

            endpoint_state->conn = 0;
            endpoint_state->endpoint = 0;
            free_qdr_addr_endpoint_state_t(endpoint_state);
        }
    }
}


/**
 * @brief Determine if deliveries to the specified address coming in on the specified connection
 * can be successfully forwarded to a valid destination.  A destination is valid if it is not
 * on the same edge-mesh as the delivery's origin.
 * 
 * @param addr 
 * @param in_conn 
 * @return true 
 * @return false 
 */
static bool qdrc_can_send_address(qdr_address_t *addr, qdr_connection_t *in_conn)
{
    if (!addr)
        return false;

    //
    // Determine if there is a mesh-loop, where all of the local or remote destinations
    // go back to the mesh from which the connection comes.
    //
    bool mesh_loop = (addr->remote_sole_destination_mesh || addr->local_sole_destination_mesh)
                     && memcmp(addr->destination_mesh_id, in_conn->edge_mesh_id, QD_DISCRIMINATOR_BYTES) == 0;

    //
    // If there is at least one remote destination and not all remote destinations go to the same mesh,
    // sending is allowed.
    //
    if (qd_bitmask_cardinality(addr->rnodes) > 0 && (!mesh_loop || !addr->remote_sole_destination_mesh)) {
        return true;
    }

    //
    // If there is at least one local destination and not all local destinations go to the same mesh,
    // sending is allowed.
    //
    if (DEQ_SIZE(addr->rlinks) > 0 && (!mesh_loop || !addr->local_sole_destination_mesh)) {
        return true;
    }

    return false;
}


static void qdrc_send_message(qdr_core_t *core, qdr_address_t *addr, qdrc_endpoint_t *endpoint, bool insert_addr)
{
    if (!addr)
        return;

    if (!endpoint)
        return;

    qd_message_t *msg = qdcm_edge_create_address_dlv(core, addr, insert_addr);
    qdr_delivery_t *dlv = qdrc_endpoint_delivery_CT(core, endpoint, msg);

    qdrc_endpoint_send_CT(core, endpoint, dlv, true);
}


static void qdrc_update_edge_peers(qdr_core_t *core, qdr_address_t *addr, bool reachable)
{
    qdr_link_ref_t *inlink = DEQ_HEAD(addr->inlinks);

    while (inlink) {
        qdr_link_t *link = inlink->link;
        if (!!link->edge_context) {
            qdr_addr_endpoint_state_t *endpoint_state = (qdr_addr_endpoint_state_t*) link->edge_context;
            qdrc_endpoint_t           *endpoint       = endpoint_state->endpoint;
            if (!!endpoint && !endpoint_state->closed) {
                if (reachable) {
                    if (!link->edge_reachable && qdrc_can_send_address(addr, endpoint_state->conn)) {
                        qdrc_send_message(core, addr, endpoint, true);
                        link->edge_reachable = true;
                    }
                } else {
                    if (link->edge_reachable && !qdrc_can_send_address(addr, endpoint_state->conn)) {
                        qdrc_send_message(core, addr, endpoint, false);
                        link->edge_reachable = false;
                    }
                }
            }
        }
        inlink = DEQ_NEXT(inlink);
    }
}


/**
 * Some address state has changed.  If the can-send-message state for any incoming link has changed,
 * notify the edge peer of the change.
 */
static void qdrc_check_edge_peers(qdr_core_t *core, qdr_address_t *addr)
{
    qdr_link_ref_t *inlink = DEQ_HEAD(addr->inlinks);

    while (inlink) {
        qdr_link_t *link = inlink->link;
        if (!!link->edge_context) {
            qdr_addr_endpoint_state_t *endpoint_state = (qdr_addr_endpoint_state_t*) link->edge_context;
            qdrc_endpoint_t           *endpoint       = endpoint_state->endpoint;
            if (!!endpoint && !endpoint_state->closed) {
                if (!link->edge_reachable) {
                    if (qdrc_can_send_address(addr, endpoint_state->conn)) {
                        qdrc_send_message(core, addr, endpoint, true);
                        link->edge_reachable = true;
                    }
                } else {
                    if (!qdrc_can_send_address(addr, endpoint_state->conn)) {
                        qdrc_send_message(core, addr, endpoint, false);
                        link->edge_reachable = false;
                    }
                }
            }
        }
        inlink = DEQ_NEXT(inlink);
    }
}


static void on_addr_event(void *context, qdrc_event_t event, qdr_address_t *addr)
{
    // We only care about mobile addresses.
    if (!qdr_address_is_mobile_CT(addr))
        return;

    qdr_addr_tracking_module_context_t *addr_tracking = (qdr_addr_tracking_module_context_t*) context;
    switch (event) {
        case QDRC_EVENT_ADDR_ADDED_LOCAL_DEST :
            //
            // If there are remote destinations or more than two local destinations, don't do anything because
            // all edge peers have already been informed about the reachability of this address.
            //
            // Otherwise, we need to update all edge peers because edges may have been excluded by being the
            // sole destination for the address.
            //
            if (qd_bitmask_cardinality(addr->rnodes) == 0 || DEQ_SIZE(addr->rlinks) - addr->proxy_rlink_count > 2) {
                qdrc_update_edge_peers(addr_tracking->core, addr, true);
            }
            break;

        case QDRC_EVENT_ADDR_ADDED_REMOTE_DEST :
            //
            //
            //
            if (qd_bitmask_cardinality(addr->rnodes) == 1) {
                qdrc_update_edge_peers(addr_tracking->core, addr, true);
            }
            break;

        case QDRC_EVENT_ADDR_REMOVED_LOCAL_DEST :
        case QDRC_EVENT_ADDR_REMOVED_REMOTE_DEST :
            //
            //
            //
            if (qd_bitmask_cardinality(addr->rnodes) == 0) {
                qdrc_update_edge_peers(addr_tracking->core, addr, false);
            }
            break;

        case QDRC_EVENT_ADDR_LOCAL_CHANGED:
        case QDRC_EVENT_ADDR_REMOTE_CHANGED:
            qdrc_check_edge_peers(addr_tracking->core, addr);
            break;

        default:
            break;
    }
}

static void on_link_event(void *context, qdrc_event_t event, qdr_link_t *link)
{
    switch (event) {
        case QDRC_EVENT_LINK_EDGE_DATA_ATTACHED :
        {
            qdr_addr_tracking_module_context_t *mc = (qdr_addr_tracking_module_context_t*) context;
            qdr_address_t *addr = link->owning_addr;
            if (addr && qdr_address_is_mobile_CT(addr) && DEQ_SIZE(addr->subscriptions) == 0 && link->link_direction == QD_INCOMING) {
                qdr_addr_endpoint_state_t *endpoint_state = qdrc_get_endpoint_state_for_connection(mc->endpoint_state_list, link->conn);
                // Fix for DISPATCH-1492. Remove the assert(endpoint_state); and add an if condition check for endpoint_state
                // We will not prevent regular endpoints from connecting to the edge listener for now.
                if (endpoint_state) {
                    assert(link->edge_context == 0);
                    link->edge_context = endpoint_state;
                    endpoint_state->ref_count++;
                    if (qdrc_can_send_address(addr, link->conn)) {
                        qdrc_send_message(mc->core, addr, endpoint_state->endpoint, true);
                        link->edge_reachable = true;
                    }
                }
            }
            break;
        }
        
        case QDRC_EVENT_LINK_EDGE_DATA_DETACHED :
        {
            if (link->edge_context) {
                qdr_addr_endpoint_state_t *endpoint_state = (qdr_addr_endpoint_state_t*) link->edge_context;
                endpoint_state->ref_count--;
                link->edge_context = 0;
                //
                // The endpoint has been closed and no other links are referencing this endpoint. Time to free it.
                //
                if (endpoint_state->ref_count == 0 && endpoint_state->closed) {
                    qdr_addr_tracking_module_context_t *mc = endpoint_state->mc;
                    if (mc) {
                        DEQ_REMOVE(mc->endpoint_state_list, endpoint_state);
                    }
                    endpoint_state->conn = 0;
                    endpoint_state->endpoint = 0;
                    free_qdr_addr_endpoint_state_t(endpoint_state);
                }
            }
            break;
        }

        default:
            break;
    }
}


static bool qdrc_edge_address_tracking_enable_CT(qdr_core_t *core)
{
    return core->router_mode == QD_ROUTER_MODE_INTERIOR;
}


static void qdrc_edge_address_tracking_init_CT(qdr_core_t *core, void **module_context)
{
    qdr_addr_tracking_module_context_t *context = NEW(qdr_addr_tracking_module_context_t);
    ZERO(context);
    context->core = core;
    *module_context = context;

    //
    // Bind to the static address QD_TERMINUS_EDGE_ADDRESS_TRACKING
    //
    context->addr_tracking_endpoint.label = "qdrc_edge_address_tracking_module_init_CT";
    context->addr_tracking_endpoint.on_first_attach  = qdrc_address_endpoint_first_attach;
    context->addr_tracking_endpoint.on_first_detach  = qdrc_address_endpoint_on_first_detach;
    context->addr_tracking_endpoint.on_cleanup  = qdrc_address_endpoint_cleanup;
    qdrc_endpoint_bind_mobile_address_CT(core, QD_TERMINUS_EDGE_ADDRESS_TRACKING, &context->addr_tracking_endpoint, context);

    //
    // Subscribe to address and link events.
    //
    context->event_sub = qdrc_event_subscribe_CT(core,
            QDRC_EVENT_ADDR_ADDED_LOCAL_DEST
            | QDRC_EVENT_ADDR_REMOVED_LOCAL_DEST
            | QDRC_EVENT_ADDR_ADDED_REMOTE_DEST
            | QDRC_EVENT_ADDR_REMOVED_REMOTE_DEST
            | QDRC_EVENT_ADDR_LOCAL_CHANGED
            | QDRC_EVENT_ADDR_REMOTE_CHANGED
            | QDRC_EVENT_LINK_EDGE_DATA_ATTACHED
            | QDRC_EVENT_LINK_EDGE_DATA_DETACHED,
            0,
            on_link_event,
            on_addr_event,
            0,
            context);
}


static void qdrc_edge_address_tracking_final_CT(void *module_context)
{
    qdr_addr_tracking_module_context_t *mc = ( qdr_addr_tracking_module_context_t*) module_context;

    // If there are any endpoint states still hanging around, clean it up.
    qdr_addr_endpoint_state_t *endpoint_state = DEQ_HEAD(mc->endpoint_state_list);
    while (endpoint_state) {
        DEQ_REMOVE_HEAD(mc->endpoint_state_list);
        free_qdr_addr_endpoint_state_t(endpoint_state);
        endpoint_state = DEQ_HEAD(mc->endpoint_state_list);
    }
    qdrc_event_unsubscribe_CT(mc->core, mc->event_sub);
    free(mc);
}


QDR_CORE_MODULE_DECLARE("edge_addr_tracking", qdrc_edge_address_tracking_enable_CT, qdrc_edge_address_tracking_init_CT, qdrc_edge_address_tracking_final_CT)

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

#include "private.h"
#include "qd_connector.h"
#include "qd_connection.h"
#include "qd_listener.h"
#include "container.h"

#include "delivery.h"
#include "dispatch_private.h"
#include "policy.h"
#include "router_private.h"

#include <qpid/dispatch.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/proton_utils.h>
#include <qpid/dispatch/protocols.h>
#include <qpid/dispatch/connection_counters.h>
#include <qpid/dispatch/amqp_adaptor.h>
#include <qpid/dispatch/tls_amqp.h>

#include <proton/sasl.h>

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//
// Protocol adaptor context
//
amqp_adaptor_t amqp_adaptor;


static char *router_role      = "inter-router";
static char *router_data_role = "inter-router-data";
static char *container_role   = "route-container";
static char *edge_role        = "edge";
static char *inter_edge_role  = "inter-edge";


static void deferred_AMQP_rx_handler(qd_connection_t *qd_conn, void *context, bool discard);
static bool parse_failover_property_list(qd_router_t *router, qd_connection_t *conn, pn_data_t *props);
static void clear_producer_activation(qdr_core_t *core, qdr_delivery_t *delivery);
static void clear_consumer_activation(qdr_core_t *core, qdr_delivery_t *delivery);

const char *QD_AMQP_COND_OVERSIZE_DESCRIPTION = "Message size exceeded";

//==============================================================================
// Functions to handle the linkage between proton deliveries and qdr deliveries
//==============================================================================

//
// qd_link.list_of_references(pn_delivery_t)
// pn_delivery.context => reference-entry
// qdr_delivery.context => pn_delivery
//

// reject a delivery, setting the apropriate condition fields so the sender can
// determine the reason the message was rejected
//
static inline void _reject_delivery(pn_delivery_t *pnd, const char *error_name, const char *description)
{
    assert(error_name && description);
    pn_condition_t *lcond = pn_disposition_condition(pn_delivery_local(pnd));
    (void) pn_condition_set_name(lcond, error_name);
    (void) pn_condition_set_description(lcond, description);
    pn_delivery_update(pnd, PN_REJECTED);
}


static void qdr_node_connect_deliveries(qd_link_t *link, qdr_delivery_t *qdlv, pn_delivery_t *pdlv)
{
    qd_link_ref_t      *ref  = new_qd_link_ref_t();
    qd_link_ref_list_t *list = qd_link_get_ref_list(link);
    ZERO(ref);
    ref->ref = qdlv;
    DEQ_INSERT_TAIL(*list, ref);

    pn_delivery_set_context(pdlv, ref);
    qdr_delivery_set_context(qdlv, pdlv);
    qdr_delivery_incref(qdlv, "referenced by a pn_delivery");
}


static void qdr_node_disconnect_deliveries(qdr_core_t *core, qd_link_t *link, qdr_delivery_t *qdlv, pn_delivery_t *pdlv)
{
    if (!link)
        return;

    qd_link_ref_t      *ref  = (qd_link_ref_t*) pn_delivery_get_context(pdlv);
    qd_link_ref_list_t *list = qd_link_get_ref_list(link);

    if (ref) {
        DEQ_REMOVE(*list, ref);
        free_qd_link_ref_t(ref);

        pn_delivery_set_context(pdlv, 0);
        qdr_delivery_set_context(qdlv, 0);

        if (qd_link_direction(link) == QD_INCOMING) {
            clear_producer_activation(core, qdlv);
        } else {
            clear_consumer_activation(core, qdlv);
        }
        qdr_delivery_decref(core, qdlv, "qdr_node_disconnect_deliveries - removed reference from pn_delivery");
    }
}


static pn_delivery_t *qdr_node_delivery_pn_from_qdr(qdr_delivery_t *dlv)
{
    return dlv ? (pn_delivery_t*) qdr_delivery_get_context(dlv) : 0;
}


static qdr_delivery_t *qdr_node_delivery_qdr_from_pn(pn_delivery_t *dlv)
{
    qd_link_ref_t *ref = (qd_link_ref_t*) pn_delivery_get_context(dlv);
    return ref ? (qdr_delivery_t*) ref->ref : 0;
}


// read the delivery-state set by the remote endpoint
//
static qd_delivery_state_t *qd_delivery_read_remote_state(pn_delivery_t *pnd)
{
    qd_delivery_state_t *dstate = 0;
    uint64_t            outcome = pn_delivery_remote_state(pnd);

    switch (outcome) {
    case 0:
        // not set - no delivery-state
        break;
    case PN_RECEIVED: {
        pn_disposition_t *disp = pn_delivery_remote(pnd);
        dstate = qd_delivery_state();
        dstate->section_number = pn_disposition_get_section_number(disp);
        dstate->section_offset = pn_disposition_get_section_offset(disp);
        break;
    }
    case PN_ACCEPTED:
    case PN_RELEASED:
        // no associated state (that we care about)
        break;
    case PN_REJECTED: {
        // See AMQP 1.0 section 3.4.4 Rejected
        pn_condition_t *cond = pn_disposition_condition(pn_delivery_remote(pnd));
        dstate = qd_delivery_state();
        dstate->error = qdr_error_from_pn(cond);
        break;
    }
    case PN_MODIFIED: {
        // See AMQP 1.0 section 3.4.5 Modified
        pn_disposition_t *disp = pn_delivery_remote(pnd);
        bool failed = pn_disposition_is_failed(disp);
        bool undeliverable = pn_disposition_is_undeliverable(disp);
        pn_data_t *anno = pn_disposition_annotations(disp);

        // avoid expensive alloc if only default values found
        const bool need_anno = (anno && pn_data_size(anno) > 0);
        if (failed || undeliverable || need_anno) {
            dstate = qd_delivery_state();
            dstate->delivery_failed = failed;
            dstate->undeliverable_here = undeliverable;
            if (need_anno) {
                dstate->annotations = pn_data(0);
                pn_data_copy(dstate->annotations, anno);
            }
        }
        break;
    }
    default: {
        // Check for custom state data. Custom outcomes and AMQP 1.0
        // Transaction defined outcomes will all be numerically >
        // PN_MODIFIED. See Part 4: Transactions and section 1.5 Descriptor
        // Values in the AMQP 1.0 spec.
        if (outcome > PN_MODIFIED) {
            pn_data_t *data = pn_disposition_data(pn_delivery_remote(pnd));
            if (data && pn_data_size(data) > 0) {
                dstate = qd_delivery_state();
                dstate->extension = pn_data(0);
                pn_data_copy(dstate->extension, data);
            }
        }
        break;
    }
    } // end switch

    return dstate;
}


// Set the delivery-state to be sent to the remote endpoint.
//
static void qd_delivery_write_local_state(pn_delivery_t *pnd, uint64_t outcome, const qd_delivery_state_t *dstate)
{
    if (pnd && dstate) {
        switch (outcome) {
        case PN_RECEIVED: {
            pn_disposition_t *ldisp = pn_delivery_local(pnd);
            pn_disposition_set_section_number(ldisp, dstate->section_number);
            pn_disposition_set_section_offset(ldisp, dstate->section_offset);
            break;
        }
        case PN_ACCEPTED:
        case PN_RELEASED:
            // no associated state (that we care about)
            break;
        case PN_REJECTED:
            if (dstate->error) {
                pn_condition_t *condition = pn_disposition_condition(pn_delivery_local(pnd));
                char *name = qdr_error_name(dstate->error);
                char *description = qdr_error_description(dstate->error);
                pn_condition_set_name(condition, (const char*) name);
                pn_condition_set_description(condition, (const char*) description);
                if (qdr_error_info(dstate->error)) {
                    pn_data_copy(pn_condition_info(condition), qdr_error_info(dstate->error));
                }
                //proton makes copies of name and description, so it is ok to free them here.
                free(name);
                free(description);
            }
            break;
        case PN_MODIFIED: {
            pn_disposition_t *ldisp = pn_delivery_local(pnd);
            pn_disposition_set_failed(ldisp, dstate->delivery_failed);
            pn_disposition_set_undeliverable(ldisp, dstate->undeliverable_here);
            if (dstate->annotations)
                pn_data_copy(pn_disposition_annotations(ldisp), dstate->annotations);
            break;
        }
        default:
            if (dstate->extension)
                pn_data_copy(pn_disposition_data(pn_delivery_local(pnd)), dstate->extension);
            break;
        }
    }
}


/**
 * Determine the role of a connection
 */
static void qd_router_connection_get_config(const qd_connection_t  *conn,
                                            qdr_connection_role_t  *role,
                                            int                    *cost,
                                            const char            **name,
                                            bool                   *strip_annotations_in,
                                            bool                   *strip_annotations_out,
                                            int                    *link_capacity)
{
    if (conn) {
        const qd_server_config_t *cf = qd_connection_config(conn);

        *strip_annotations_in  = cf ? cf->strip_inbound_annotations  : true;
        *strip_annotations_out = cf ? cf->strip_outbound_annotations : true;
        *link_capacity         = cf ? cf->link_capacity : 1;

        if (cf && (strcmp(cf->role, router_role) == 0)) {
            *strip_annotations_in  = false;
            *strip_annotations_out = false;
            *role = QDR_ROLE_INTER_ROUTER;
            *cost = cf->inter_router_cost;
        } else if (cf && (strcmp(cf->role, router_data_role) == 0)) {
            *strip_annotations_in  = false;
            *strip_annotations_out = false;
            *role = QDR_ROLE_INTER_ROUTER_DATA;
            *cost = cf->inter_router_cost;
        } else if (cf && (strcmp(cf->role, edge_role) == 0)) {
            *strip_annotations_in  = false;
            *strip_annotations_out = false;
            *role = QDR_ROLE_EDGE_CONNECTION;
            *cost = cf->inter_router_cost;
        } else if (cf && (strcmp(cf->role, inter_edge_role) == 0)) {
            *strip_annotations_in  = false;
            *strip_annotations_out = false;
            *role = QDR_ROLE_INTER_EDGE;
            *cost = cf->inter_router_cost;
        } else if (cf && (strcmp(cf->role, container_role) == 0))  // backward compat
            *role = QDR_ROLE_ROUTE_CONTAINER;
        else
            *role = QDR_ROLE_NORMAL;

        *name = cf ? cf->name : 0;
        if (*name) {
            if (strncmp("listener/", *name, 9) == 0 ||
                strncmp("connector/", *name, 10) == 0)
                *name = 0;
        }
    }
}


static void clear_consumer_activation(qdr_core_t *core, qdr_delivery_t *delivery)
{
    if (delivery->in_message_activation) {
        delivery->in_message_activation = false;
        qd_log(LOG_ROUTER, QD_LOG_DEBUG, DLV_FMT " AMQP cancel consumer activation", DLV_ARGS(delivery));
        qd_message_cancel_consumer_activation(qdr_delivery_message(delivery));
    }
}


static void clear_producer_activation(qdr_core_t *core, qdr_delivery_t *delivery)
{
    if (delivery->in_message_activation) {
        delivery->in_message_activation = false;
        qd_log(LOG_ROUTER, QD_LOG_DEBUG, DLV_FMT " AMQP cancel producer activation", DLV_ARGS(delivery));
        qd_message_cancel_producer_activation(qdr_delivery_message(delivery));
    }
}


int AMQP_conn_wake_handler(qd_router_t *router, qd_connection_t *conn, void *context)
{
    qdr_connection_t *qconn  = (qdr_connection_t*) qd_connection_get_context(conn);
    int               result = 0;

    if (CLEAR_ATOMIC_FLAG(&conn->wake_core) && !!qconn) {
        result = qdr_connection_process(qconn);
    }

    if (CLEAR_ATOMIC_FLAG(&conn->wake_cutthrough_outbound)) {
        //
        // Run outgoing cut-through processing
        //
        while (true) {
            //
            // Under the protection of the spinlock, get the next delivery in the worklist
            //
            sys_spinlock_lock(&conn->outbound_cutthrough_spinlock);
            qdr_delivery_ref_t *dref = DEQ_HEAD(conn->outbound_cutthrough_worklist);
            if (!!dref) {
                DEQ_REMOVE_HEAD(conn->outbound_cutthrough_worklist);
                dref->dlv->cutthrough_list_ref = 0;
            }
            sys_spinlock_unlock(&conn->outbound_cutthrough_spinlock);

            //
            // If the worklist was empty, exit the loop
            //
            if (!dref) {
                break;
            }

            //
            // Verify that the activation is still in force - it may have been cancelled after it was added to the
            // worklist
            //
            qdr_delivery_t *delivery = dref->dlv;
            if (delivery->in_message_activation) {
                qd_link_t      *qlink  = (qd_link_t*) qdr_link_get_context(qdr_delivery_link(delivery));
                qd_message_t   *stream = qdr_delivery_message(delivery);
                bool            q3_stalled;

                //
                // Send content from the stream outbound on the link. The message and the link must NOT be released while
                // delivery->in_message_activation is true!
                //
                assert(stream);
                assert(qlink);
                qd_message_send(stream, qlink, 0, &q3_stalled);
                if (q3_stalled) {
                    qd_link_q3_block(qlink);
                }

                //
                // If the stream is send complete, we don't need to be activated any more.  Cancel the activation on the stream.
                //
                if (qd_message_send_complete(stream)) {
                    clear_consumer_activation(router->router_core, delivery);
                }
            }

            //
            // Account for the removed reference and free the reference object
            //
            qdr_delivery_decref(router->router_core, delivery, "AMQP_conn_wake_handler - cut-through activation worklist");
            free_qdr_delivery_ref_t(dref);
        }
    }

    if (CLEAR_ATOMIC_FLAG(&conn->wake_cutthrough_inbound)) {
        //
        // Run incoming cut-through processing
        //
        while (true) {
            //
            // Under the protection of the spinlock, get the next delivery in the worklist
            //
            sys_spinlock_lock(&conn->inbound_cutthrough_spinlock);
            qdr_delivery_ref_t *dref = DEQ_HEAD(conn->inbound_cutthrough_worklist);
            if (!!dref) {
                DEQ_REMOVE_HEAD(conn->inbound_cutthrough_worklist);
                dref->dlv->cutthrough_list_ref = 0;
            }
            sys_spinlock_unlock(&conn->inbound_cutthrough_spinlock);

            //
            // If the worklist was empty, exit the loop
            //
            if (!dref) {
                break;
            }

            //
            // Verify that the activation is still in force - it may have been cancelled after it was added to the
            // worklist
            //
            qdr_delivery_t *delivery = dref->dlv;
            if (delivery->in_message_activation) {
                pn_delivery_t *pnd   = qdr_node_delivery_pn_from_qdr(delivery);
                qd_message_t *stream = qdr_delivery_message(delivery);

                //
                // Receive content from the link and produce it into the stream. The proton deliver MUST NOT be released
                // while delivery->in_message_activation is true!
                //
                assert(pnd);
                ssize_t unused;
                qd_message_receive(pnd, &unused);

                //
                // If the stream is receive complete, we don't need to be activated any more.  Cancel the activation on
                // the stream. The message (stream) MUST NOT be unlinked from the delivery while in_message_activation is true!
                //
                assert(stream);
                if (qd_message_receive_complete(stream)) {
                    clear_producer_activation(router->router_core, delivery);
                }
            }

            //
            // Account for the removed reference and free the reference object
            //
            qdr_delivery_decref(router->router_core, delivery, "AMQP_conn_wake_handler - cut-through activation worklist");
            free_qdr_delivery_ref_t(dref);
        }
    }

    return result;
}


static qd_iterator_t *process_router_annotations(qd_router_t   *router,
                                                 qd_message_t  *msg,
                                                 qd_bitmask_t **link_exclusions,
                                                 uint32_t      *distance,
                                                 int           *ingress_index)
{
    qd_iterator_t *ingress_iter = 0;
    bool           edge_mode    = router->router_mode == QD_ROUTER_MODE_EDGE;

    *link_exclusions = 0;
    *distance        = 0;
    *ingress_index   = 0;

    if (!edge_mode) { // Edge routers do not use trace or ingress meta-data
        qd_parsed_field_t *trace = qd_message_get_trace(msg);
        if (trace && qd_parse_is_list(trace)) {
            //
            // Return the distance in hops that this delivery has traveled.
            //
            *distance = qd_parse_sub_count(trace);

            //
            // Create a link-exclusion map for the items in the trace.  This map will
            // contain a one-bit for each link that leads to a neighbor router that
            // the message has already passed through.
            //
            *link_exclusions = qd_tracemask_create(router->tracemask, trace, ingress_index);
        }

        qd_parsed_field_t *ingress = qd_message_get_ingress_router(msg);
        if (ingress && qd_parse_is_scalar(ingress)) {
            ingress_iter = qd_parse_raw(ingress);
        }
    }

    //
    // Return the iterator to the ingress field _if_ it was present.
    // Otherwise this router is the ingress - return NULL.
    //
    return ingress_iter;
}

static void log_link_message(qd_connection_t *conn, pn_link_t *pn_link, qd_message_t *msg)
{
    assert(conn && pn_link && msg);

    // the message processing is expensive as this is done for every message received.
    // Do not bother if not tracing.

    if (qd_log_enabled(LOG_MESSAGE, QD_LOG_DEBUG)) {
        const qd_server_config_t *cf = qd_connection_config(conn);
        if (!cf) return;
        char buf[QD_LOG_TEXT_MAX];
        const char *msg_str = qd_message_oversize(msg) ? "oversize message" :
            qd_message_aborted(msg) ? "aborted message" :
            qd_message_repr(msg, buf, sizeof(buf), cf->message_log_flags);
        if (msg_str) {
            const char *src = pn_terminus_get_address(pn_link_source(pn_link));
            const char *tgt = pn_terminus_get_address(pn_link_target(pn_link));
            qd_log(LOG_MESSAGE, QD_LOG_DEBUG, "[C%" PRIu64 "]: %s %s on link '%s' (%s -> %s)",
                   qd_connection_connection_id(conn), pn_link_is_sender(pn_link) ? "Sent" : "Received", msg_str,
                   pn_link_name(pn_link), src ? src : "", tgt ? tgt : "");
        }
    }
}


/**
 * Inbound Delivery Handler
 *
 * @return true if we've advanced to the next delivery on this link and it is
 * ready for rx processing.  This will cause the container to immediately
 * re-call this function with the next delivery.
 */
bool AMQP_rx_handler(qd_router_t *router, qd_link_t *link)
{
    pn_link_t      *pn_link = qd_link_pn(link);

    assert(pn_link);

    if (!pn_link)
        return false;

    // ensure the current delivery is readable
    pn_delivery_t *pnd = pn_link_current(pn_link);
    if (!pnd)
        return false;

    qd_connection_t  *conn   = qd_link_connection(link);

    // DISPATCH-1628 DISPATCH-975 exit if router already closed this connection
    if (conn->closed_locally) {
        return false;
    }

    qdr_delivery_t *delivery = qdr_node_delivery_qdr_from_pn(pnd);
    bool       next_delivery = false;

    //
    // Receive the message into a local representation.
    //
    ssize_t octets_received;
    qd_message_t   *msg   = qd_message_receive(pnd, &octets_received);
    bool receive_complete = qd_message_receive_complete(msg);

    //
    // Bump LINK metrics if appropriate
    //
    if (!!conn->connector && !!conn->connector->ctor_config && !!conn->connector->ctor_config->vflow_record) {
        vflow_inc_counter(conn->connector->ctor_config->vflow_record, VFLOW_ATTRIBUTE_OCTETS_REVERSE, (uint64_t) octets_received);
    }

    // check if cut-through can be enabled or disabled
    //
    if (!!delivery) {
        if (receive_complete) {
            if (delivery->in_message_activation) {
                clear_producer_activation(router->router_core, delivery);
            }
        } else {  // !receive_complete
            if (!delivery->in_message_activation && qd_message_is_unicast_cutthrough(msg)) {
                qd_message_activation_t activation;
                activation.delivery = delivery;
                activation.type     = QD_ACTIVATION_AMQP;
                qd_alloc_set_safe_ptr(&activation.safeptr, conn);
                delivery->in_message_activation = true;
                qd_log(LOG_ROUTER, QD_LOG_DEBUG, DLV_FMT " AMQP enable producer activation", DLV_ARGS(delivery));
                qd_message_set_producer_activation(msg, &activation);
            }
        }
    }

    if (!qd_message_oversize(msg)) {
        // message not rejected as oversize
        if (receive_complete) {
            //
            // The entire message has been received and we are ready to consume the delivery by calling pn_link_advance().
            //
            pn_link_advance(pn_link);
            next_delivery = pn_link_current(pn_link) != 0;
        }

        if (qd_message_is_discard(msg)) {
            //
            // Message has been marked for discard, no further processing necessary
            //
            if (receive_complete) {
                // If this discarded delivery has already been settled by proton,
                // set the presettled flag on the delivery to true if it is not already true.
                // Since the entire message has already been received, we directly call the
                // function to set the pre-settled flag since we cannot go thru the core-thread
                // to do this since the delivery has been discarded.
                // Discarded streaming deliveries are not put thru the core thread via the continue action.
                if (pn_delivery_settled(pnd))
                    qdr_delivery_set_presettled(delivery);

                uint64_t local_disp = qdr_delivery_disposition(delivery);
                //
                // Call pn_delivery_update only if the local disposition is different than the pn_delivery's local disposition.
                // This will make sure we call pn_delivery_update only when necessary.
                //
                if (local_disp != 0 && local_disp != pn_delivery_local_state(pnd)) {
                    //
                    // DISPATCH-1626 - This enables pn_delivery_update() and pn_delivery_settle() to be called back to back in the same function call.
                    // CORE_delivery_update() will handle most of the other cases where we need to call pn_delivery_update() followed by pn_delivery_settle().
                    //
                    pn_delivery_update(pnd, local_disp);
                }

                // note: expected that the code that set discard has handled
                // setting disposition and updating flow!
                pn_delivery_settle(pnd);
                if (delivery) {
                    // if delivery already exists then the core thread discarded this
                    // delivery, it will eventually free the qdr_delivery_t and its
                    // associated message - do not free it here.
                    qdr_node_disconnect_deliveries(router->router_core, link, delivery, pnd);
                } else {
                    qd_message_free(msg);
                }
            }
            return next_delivery;
        }
    } else {
        // message is oversize
        if (receive_complete) {
            // set condition, reject, and settle the incoming delivery
            _reject_delivery(pnd, QD_AMQP_COND_MESSAGE_SIZE_EXCEEDED, QD_AMQP_COND_OVERSIZE_DESCRIPTION);
            pn_delivery_settle(pnd);
            // close the link
            pn_link_close(pn_link);
            // set condition and close the connection
            pn_connection_t * pn_conn = qd_connection_pn(conn);
            pn_condition_t * cond = pn_connection_condition(pn_conn);
            (void) pn_condition_set_name(       cond, QD_AMQP_COND_CONNECTION_FORCED);
            (void) pn_condition_set_description(cond, QD_AMQP_COND_OVERSIZE_DESCRIPTION);
            pn_connection_close(pn_conn);
            if (!delivery) {
                // this message has not been forwarded yet, so it will not be
                // cleaned up when the link is freed.
                qd_message_free(msg);
            }
            // stop all message reception on this connection
            conn->closed_locally = true;
        }
        return false;
        // oversize messages are not processed any further
    }

    //
    // If the delivery already exists we've already passed it to the core (2nd
    // frame for a multi-frame transfer). Simply continue.
    //

    if (delivery) {
        // For cutthrough the core thread only gets notified when the delivery first arrives (via a call to
        // qdr_link_deliver) and when it is complete. Do not call qdr_delivery_continue otherwise.
        if (qd_message_is_unicast_cutthrough(msg)) {
            if (receive_complete) {
                qdr_delivery_continue(router->router_core, delivery, pn_delivery_settled(pnd));
            }
        }
        else {
            qdr_delivery_continue(router->router_core, delivery, pn_delivery_settled(pnd));
        }
        return next_delivery;
    }

    //
    // No pre-existing delivery means we're starting a new delivery or
    // continuing a delivery that has not accumulated enough of the message
    // for forwarding.
    //

    qdr_link_t *rlink = (qdr_link_t*) qd_link_get_context(link);
    if (!rlink) {
        // receive link was closed or deleted - can't be forwarded
        // so no use setting disposition or adding flow
        qd_message_set_discard(msg, true);
        if (receive_complete) {
            pn_delivery_settle(pnd);
            qd_message_free(msg);
        }
        return next_delivery;
    }

    //
    // Validate the content of the delivery as an AMQP message. This is done
    // partially, only to validate that we can find the fields we need to route
    // the message.
    //
    // If per-message tracing is configured then validate the sections
    // necessary for logging (currently application properties).
    //
    // If the link is anonymous, we must validate through the message
    // properties to find the 'to' field.  If the link is not anonymous, we
    // don't need the 'to' field as we will be using the address from the link
    // target.
    //
    // Check if the user id needs to be validated (see below). If it does we
    // need to validate the message properties section.
    //
    // Otherwise simply check for the optional router annotations section
    // necessary for forwarding.
    //
    const bool anonymous_link = qdr_link_is_anonymous(rlink);
    const bool check_user     = (conn->policy_settings && !conn->policy_settings->spec.allowUserIdProxy);
    const qd_server_config_t *cf = qd_connection_config(conn);
    const qd_message_depth_t depth = (cf && cf->message_log_flags != 0) ? QD_DEPTH_APPLICATION_PROPERTIES
        : (anonymous_link || check_user) ? QD_DEPTH_PROPERTIES
        : QD_DEPTH_ROUTER_ANNOTATIONS;

    const qd_message_depth_status_t depth_valid = qd_message_check_depth(msg, depth);
    switch (depth_valid) {
    case QD_MESSAGE_DEPTH_INVALID:
            qd_log(LOG_ROUTER, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64 "] Incoming message validation failed - rejected", conn->connection_id,
                   qd_link_link_id(link));
            qd_message_set_discard(msg, true);
            pn_link_flow(pn_link, 1);
            _reject_delivery(pnd, QD_AMQP_COND_DECODE_ERROR, "invalid message format");
            pn_delivery_settle(pnd);
            qd_message_free(msg);
            return next_delivery;
    case QD_MESSAGE_DEPTH_INCOMPLETE:
        return false;  // stop rx processing
    case QD_MESSAGE_DEPTH_OK:
        break;
    }

    // Intercept deliveries destined to a core endpoint and hand them directly to the router core. Incoming links to a
    // core endpoint address are "attach routed". This means the incoming link is bound directly to the core endpoint
    // consumer when the first attach arrives. Afterwards deliveries arriving on this link are passed directly to the
    // core endpoint rather than going through the delivery forwarding logic.
    //
    if (qdr_link_is_core_endpoint(rlink)) {
        log_link_message(conn, pn_link, msg);
        delivery = qdr_link_deliver_to_core(rlink, msg,
                                            pn_delivery_settled(pnd),
                                            pn_delivery_remote_state(pnd),
                                            qd_delivery_read_remote_state(pnd));
        qd_link_set_incoming_msg(link, (qd_message_t*) 0);  // msg no longer exclusive to qd_link
        qdr_node_connect_deliveries(link, delivery, pnd);
        qdr_delivery_decref(router->router_core, delivery, "release protection of return from deliver_to_core");
        return next_delivery;
    }

    // Determine if the user of this connection is allowed to proxy the user_id
    // of messages. A message user_id is proxied when the value in the message
    // properties section differs from the authenticated user name of the
    // connection.  If the user is not allowed to proxy the user_id then the
    // message user_id must be blank or it must be equal to the connection user
    // name.
    //
    if (check_user) {
        // This connection must not allow proxied user_id
        qd_iterator_t *userid_iter  = qd_message_field_iterator(msg, QD_FIELD_USER_ID);
        if (userid_iter) {
            // The user_id property has been specified
            if (qd_iterator_remaining(userid_iter) > 0) {
                // user_id property in message is not blank
                if (!qd_iterator_equal(userid_iter, (const unsigned char *)conn->user_id)) {
                    // This message is rejected: attempted user proxy is disallowed
                    qd_log(LOG_ROUTER, QD_LOG_DEBUG,
                           "[C%" PRIu64 "][L%" PRIu64 "] Message rejected due to user_id proxy violation. User:%s",
                           conn->connection_id, qd_link_link_id(link), conn->user_id);
                    qd_message_set_discard(msg, true);
                    pn_link_flow(pn_link, 1);
                    _reject_delivery(pnd, QD_AMQP_COND_UNAUTHORIZED_ACCESS, "user_id proxy violation");
                    if (receive_complete) {
                        pn_delivery_settle(pnd);
                        qd_message_free(msg);
                    }
                    qd_iterator_free(userid_iter);
                    return next_delivery;
                }
            }
            qd_iterator_free(userid_iter);
        }
    }

    const char *ra_error = qd_message_parse_router_annotations(msg);
    if (ra_error) {
        qd_log(LOG_ROUTER, QD_LOG_WARNING,
               "[C%" PRIu64 "][L%" PRIu64 "] Message rejected - invalid router annotations section: %s",
               conn->connection_id, qd_link_link_id(link), ra_error);

        _reject_delivery(pnd, QD_AMQP_COND_INVALID_FIELD, ra_error);
        qd_message_set_discard(msg, true);
        pn_link_flow(pn_link, 1);
        if (receive_complete) {
            pn_delivery_settle(pnd);
            qd_message_free(msg);
        }
        return next_delivery;
    }

    //
    // Head of line blocking avoidance (DISPATCH-1545)
    //
    // Before we can forward a message we need to determine whether or not this
    // message is "streaming" - a large message that has the potential to block
    // other messages sharing the trunk link.  At this point we cannot for sure
    // know the actual length of the incoming message, so we employ the
    // following heuristic to determine if the message is "streaming":
    //
    // - If the message is receive-complete it is NOT a streaming message.
    // - If it is NOT receive-complete:
    //   Continue buffering incoming data until:
    //   - receive has completed => NOT a streaming message
    //   - not rx-complete AND Q2 threshold hit => a streaming message
    //
    // Once Q2 is hit we MUST forward the message regardless of rx-complete
    // since Q2 will block forever unless the incoming data is drained via
    // forwarding.
    //
    if (!receive_complete) {
        if (qd_message_is_streaming(msg) || qd_message_is_Q2_blocked(msg) || qdr_link_is_streaming_deliveries(rlink)) {
            qd_log(LOG_ROUTER, QD_LOG_DEBUG,
                   "[C%" PRIu64 "][L%" PRIu64 "] Incoming message classified as streaming. User:%s",
                   conn->connection_id, qd_link_link_id(link), conn->user_id);
        } else {
            // Continue buffering this message
            return false;
        }
    }

    uint32_t       distance = 0;
    int            ingress_index = 0; // Default to _this_ router
    qd_bitmask_t  *link_exclusions = 0;
    qd_iterator_t *ingress_iter = process_router_annotations(router, msg, &link_exclusions, &distance, &ingress_index);

    //
    // If this delivery has traveled further than the known radius of the network topology (plus 1),
    // release and settle the delivery.  This can happen in the case of "flood" multicast where the
    // deliveries follow all available paths.  This will only discard messages that will reach their
    // destinations via shorter paths.
    //
    if (distance > (router->topology_radius + 1)) {
        qd_bitmask_free(link_exclusions);
        qd_message_set_discard(msg, true);
        pn_link_flow(pn_link, 1);
        pn_delivery_update(pnd, PN_RELEASED);
        if (receive_complete) {
            pn_delivery_settle(pnd);
            qd_message_free(msg);
        }
        return next_delivery;
    }

    //
    // Before the message is delivered check if Q2 has been disabled by the upstream router.
    //
    if (qd_message_is_Q2_disabled_annotation(msg)) {
        qd_message_Q2_holdoff_disable(msg);
    }

    if (anonymous_link) {
        qd_iterator_t *addr_iter = 0;

        //
        // If the message has delivery annotations, get the to-override field from the annotations.
        //
        qd_parsed_field_t *ma_to = qd_message_get_to_override(msg);
        if (ma_to) {
            addr_iter = qd_iterator_dup(qd_parse_raw(ma_to));
        }

        //
        // Still no destination address?  Use the TO field from the message properties.
        //
        if (!addr_iter) {
            addr_iter = qd_message_field_iterator(msg, QD_FIELD_TO);
        }

        if (addr_iter) {
            if (!conn->policy_settings || qd_policy_approve_message_target(addr_iter, conn)) {
                qd_iterator_reset_view(addr_iter, ITER_VIEW_ADDRESS_HASH);

                log_link_message(conn, pn_link, msg);
                delivery = qdr_link_deliver_to(rlink, msg, ingress_iter, addr_iter, pn_delivery_settled(pnd),
                                               link_exclusions, ingress_index,
                                               pn_delivery_remote_state(pnd),
                                               qd_delivery_read_remote_state(pnd));
            } else {
                //reject
                qd_log(LOG_ROUTER, QD_LOG_DEBUG,
                       "[C%" PRIu64 "][L%" PRIu64 "] Message rejected due to policy violation on target. User:%s",
                       conn->connection_id, qd_link_link_id(link), conn->user_id);
                qd_message_set_discard(msg, true);
                pn_link_flow(pn_link, 1);
                _reject_delivery(pnd, QD_AMQP_COND_UNAUTHORIZED_ACCESS, "policy violation on target");
                if (receive_complete) {
                    pn_delivery_settle(pnd);
                    qd_message_free(msg);
                }
                qd_iterator_free(addr_iter);
                qd_bitmask_free(link_exclusions);
                return next_delivery;
            }
        }
    } else {
        //
        // This is a targeted link, not anonymous.
        //

        //
        // Look in a series of locations for the terminus address, starting
        // with the qdr_link (in case this is an auto-link with separate
        // internal and external addresses).
        //
        const char *term_addr = qdr_link_internal_address(rlink);
        if (!term_addr) {
            term_addr = pn_terminus_get_address(qd_link_remote_target(link));
            if (!term_addr)
                term_addr = pn_terminus_get_address(qd_link_source(link));
        }

        if (term_addr) {
            qd_message_set_to_override_annotation(msg, term_addr);
        }

        log_link_message(conn, pn_link, msg);
        delivery = qdr_link_deliver(rlink, msg, ingress_iter, pn_delivery_settled(pnd), link_exclusions, ingress_index,
                                    pn_delivery_remote_state(pnd),
                                    qd_delivery_read_remote_state(pnd));
    }

    //
    // End of new delivery processing
    //

    if (delivery) {
        qd_link_set_incoming_msg(link, (qd_message_t*) 0);  // msg no longer exclusive to qd_link
        qdr_node_connect_deliveries(link, delivery, pnd);
        qdr_delivery_decref(router->router_core, delivery, "release protection of return from deliver");
    } else {
        //
        // If there is no delivery, the message is now and will always be unroutable because there is no address.
        //
        qd_log(LOG_ROUTER, QD_LOG_DEBUG, "[C%" PRIu64 "][L%" PRIu64 "] Message rejected - no address present",
               conn->connection_id, qd_link_link_id(link));
        qd_bitmask_free(link_exclusions);
        qd_message_set_discard(msg, true);
        pn_link_flow(pn_link, 1);
        _reject_delivery(pnd, QD_AMQP_COND_PRECONDITION_FAILED, "Routing failure: no address present");
        if (receive_complete) {
            pn_delivery_settle(pnd);
            qd_message_free(msg);
        }
    }

    return next_delivery;
}


/**
 * Deferred callback for inbound delivery handler
 */
static void deferred_AMQP_rx_handler(qd_connection_t *qd_conn, void *context, bool discard)
{
    qd_link_t_sp *safe_qdl = (qd_link_t_sp*) context;

    if (!discard) {
        qd_link_t *qdl = safe_deref_qd_link_t(*safe_qdl);
        if (!!qdl && !!qd_link_pn(qdl)) {
            assert(qd_link_direction(qdl) == QD_INCOMING);
            while (true) {
                if (!AMQP_rx_handler(amqp_adaptor.router, qdl))
                    break;
            }
        }
    }

    free(safe_qdl);
}


/**
 * Delivery Disposition Handler
 */
void AMQP_disposition_handler(qd_router_t *router, qd_link_t *link, pn_delivery_t *pnd)
{
    qdr_delivery_t *delivery = qdr_node_delivery_qdr_from_pn(pnd);

    //
    // It's important to not do any processing without a qdr_delivery.
    if (!delivery)
        return;

    uint64_t dstate = pn_delivery_remote_state(pnd);
    bool settled = pn_delivery_settled(pnd);

    //
    // When pre-settled multi-frame deliveries arrive, it's possible for the
    // settlement to register before the whole message arrives.  Such premature
    // settlement indications must be ignored.
    //
    if (settled && !qdr_delivery_receive_complete(delivery))
        settled = false;

    if (dstate || settled) {
        //
        // Update the disposition of the delivery
        //
        qdr_delivery_remote_state_updated(router->router_core, delivery,
                                          dstate,
                                          settled,
                                          qd_delivery_read_remote_state(pnd),
                                          false);

        //
        // If settled, close out the delivery
        //
        if (settled) {
            qdr_node_disconnect_deliveries(router->router_core, link, delivery, pnd);
            pn_delivery_settle(pnd);
        }
    }
}


/**
 * New Incoming Link Handler
 */
int AMQP_incoming_link_handler(qd_router_t *router, qd_link_t *link)
{
    qd_connection_t  *conn     = qd_link_connection(link);
    uint64_t link_id;

    // The connection that this link belongs to is gone. Perhaps an AMQP close came in.
    // This link handler should not continue since there is no connection.
    if (conn == 0)
        return 0;

    qdr_connection_t *qdr_conn = (qdr_connection_t*) qd_connection_get_context(conn);

    char *terminus_addr = (char*)pn_terminus_get_address(pn_link_remote_target((pn_link_t  *)qd_link_pn(link)));

    qdr_link_t       *qdr_link = qdr_link_first_attach(qdr_conn, QD_INCOMING,
                                                       qdr_terminus(qd_link_remote_source(link)),
                                                       qdr_terminus(qd_link_remote_target(link)),
                                                       pn_link_name(qd_link_pn(link)),
                                                       terminus_addr,
                                                       false,
                                                       0,
                                                       &link_id);
    qd_link_set_link_id(link, link_id);
    qdr_link_set_context(qdr_link, link);
    qd_link_set_context(link, qdr_link);

    return 0;
}


/**
 * New Outgoing Link Handler
 */
int AMQP_outgoing_link_handler(qd_router_t *router, qd_link_t *link)
{
    qd_connection_t  *conn     = qd_link_connection(link);
    uint64_t link_id;

    // The connection that this link belongs to is gone. Perhaps an AMQP close came in.
    // This link handler should not continue since there is no connection.
    if (conn == 0)
        return 0;

    qdr_connection_t *qdr_conn = (qdr_connection_t*) qd_connection_get_context(conn);
    char *terminus_addr = (char*)pn_terminus_get_address(pn_link_remote_source((pn_link_t  *)qd_link_pn(link)));
    qdr_link_t *qdr_link = qdr_link_first_attach(qdr_conn, QD_OUTGOING,
                                                 qdr_terminus(qd_link_remote_source(link)),
                                                 qdr_terminus(qd_link_remote_target(link)),
                                                 pn_link_name(qd_link_pn(link)),
                                                 terminus_addr,
                                                 false,
                                                 0,
                                                 &link_id);
    qd_link_set_link_id(link, link_id);
    qdr_link_set_context(qdr_link, link);
    qd_link_set_context(link, qdr_link);

    return 0;
}


/**
 * Handler for remote opening of links that we initiated.
 */
int AMQP_link_attach_handler(qd_router_t *router, qd_link_t *link)
{
    qdr_link_t *qlink = (qdr_link_t*) qd_link_get_context(link);
    qdr_link_second_attach(qlink,
                           qdr_terminus(qd_link_remote_source(link)),
                           qdr_terminus(qd_link_remote_target(link)));

    return 0;
}


/**
 * Handler for flow events on links.  Flow updates include session window
 * state, which needs to be checked for unblocking Q3.
 */
int AMQP_link_flow_handler(qd_router_t *router, qd_link_t *link)
{
    pn_link_t   *pnlink  = qd_link_pn(link);
    qdr_link_t  *rlink   = (qdr_link_t*) qd_link_get_context(link);

    if (rlink) {
        qdr_link_flow(router->router_core, rlink, pn_link_remote_credit(pnlink), pn_link_get_drain(pnlink));
    }

    // check if Q3 can be unblocked
    qd_session_t *qd_ssn = qd_link_get_session(link);
    if (qd_session_is_q3_blocked(qd_ssn)) {
        // Q3 blocked - have we drained enough outgoing bytes?
        if (qd_session_get_outgoing_capacity(qd_ssn) >= qd_session_get_outgoing_capacity_low_threshold(qd_ssn)) {
            // yes.  We must now unblock all links that have been blocked by Q3

            qd_link_list_t  *blinks = qd_session_q3_blocked_links(qd_ssn);
            qd_link_t       *blink  = DEQ_HEAD(*blinks);
            qd_connection_t *conn   = qd_link_connection(blink);

            while (blink) {
                qd_link_q3_unblock(blink);  // removes from blinks list!
                pnlink = qd_link_pn(blink);
                if (blink != link) {        // already flowed this link
                    rlink = (qdr_link_t *) qd_link_get_context(blink);
                    if (rlink) {
                        // signalling flow to the core causes the link to be re-activated
                        qdr_link_flow(router->router_core, rlink, pn_link_remote_credit(pnlink), pn_link_get_drain(pnlink));
                    }
                }

                pn_delivery_t *pdlv = pn_link_current(pnlink);
                if (!!pdlv) {
                    qdr_delivery_t     *qdlv = qdr_node_delivery_qdr_from_pn(pdlv);
                    //
                    //https://github.com/skupperproject/skupper-router/issues/1221
                    // Add the delivery/delivery_ref to the outbound_cutthrough_worklist
                    // only if the delivery is a cut-through delivery.
                    // Pure all-AMQP deliveries/delivery_refs will never be cut-through and hence will never be placed
                    // on the conn->outbound_cutthrough_worklist.
                    //
                    if (qdr_delivery_is_unicast_cutthrough(qdlv)) {
                        qdr_delivery_ref_t *dref = new_qdr_delivery_ref_t();
                        bool used = false;

                        sys_spinlock_lock(&conn->outbound_cutthrough_spinlock);
                        if (!qdlv->cutthrough_list_ref) {
                            DEQ_ITEM_INIT(dref);
                            dref->dlv = qdlv;
                            qdlv->cutthrough_list_ref = dref;
                            DEQ_INSERT_TAIL(conn->outbound_cutthrough_worklist, dref);
                            qdr_delivery_incref(qdlv, "Recover from Q3 stall");
                            used = true;
                        }
                        sys_spinlock_unlock(&conn->outbound_cutthrough_spinlock);

                        if (!used) {
                            free_qdr_delivery_ref_t(dref);
                        }
                    }
                }

                blink = DEQ_HEAD(*blinks);
            }

            //
            // Wake the connection for outgoing cut-through
            //
            SET_ATOMIC_FLAG(&conn->wake_cutthrough_outbound);
            AMQP_conn_wake_handler(router, conn, 0);
        }
    }
    return 0;
}


/** Pull any pending incoming messages off the link and forward them
 *
 * (DISPATCH-1085): When Proton indicates that an incoming link has detached OR the parent connection/session of the link
 * has abruptly closed (link has not cleanly detached) there may still be incoming messages buffered on the link that
 * need to be processed. This helper function attempts to read all buffered messages and forward them.
 */
static void drain_link(qd_router_t *router, qd_link_t *qd_link)
{
    pn_link_t *pn_link = qd_link_pn(qd_link);
    if (!!pn_link && pn_link_is_receiver(pn_link)) {
        pn_delivery_t *pnd = pn_link_current(pn_link);
        if (pnd) {
            qd_message_t *msg = qd_get_message_context(pnd);
            if (msg) {
                if (!qd_message_receive_complete(msg)) {
                    qd_link_set_q2_limit_unbounded(qd_link, true);

                    // since this thread owns link we can call the
                    // rx_hander directly rather than schedule it via
                    // the unblock handler:
                    qd_message_clear_q2_unblocked_handler(msg);
                    qd_message_Q2_holdoff_disable(msg);
                    while (AMQP_rx_handler(router, qd_link))
                           ;
                }
            }
        }
    }
}


/**
 * Link Detached Handler
 */
int AMQP_link_detach_handler(qd_router_t *router, qd_link_t *link)
{
    assert(link);

    pn_link_t *pn_link = qd_link_pn(link);
    if (!pn_link)
        return 0;

    // attempt to forward any remaining pending messages
    drain_link(router, link);

    // Notify the core that a detach has been received.

    qdr_link_t *rlink = (qdr_link_t *) qd_link_get_context(link);
    if (rlink) {
        pn_condition_t *cond  = pn_link_remote_condition(pn_link);
        qdr_error_t    *error = qdr_error_from_pn(cond);
        qdr_link_detach_received(rlink, error);
    } else if ((pn_link_state(pn_link) & PN_LOCAL_CLOSED) == 0) {
        // Normally the core would be responsible for sending the response detach to close the link (via
        // CORE_link_detach) but since there is no core link that will not happen.
        pn_link_close(pn_link);
    }

    return 0;
}


/**
 * Link closed handler
 *
 * This is the last callback for the given link - the link will be freed on return from this call! Forced is true if the
 * link has not properly closed (detach handshake completed).
*/
void AMQP_link_closed_handler(qd_router_t *router, qd_link_t *qd_link, bool forced)
{
    assert(qd_link);

    // attempt to forward any remaining pending messages
    drain_link(router, qd_link);

    // Clean up all qdr_delivery/pn_delivery bindings for the link.

    qd_link_ref_list_t *list = qd_link_get_ref_list(qd_link);
    qd_link_ref_t      *ref  = DEQ_HEAD(*list);

    while (ref) {
        qdr_delivery_t *dlv = (qdr_delivery_t*) ref->ref;
        pn_delivery_t *pdlv = qdr_delivery_get_context(dlv);
        assert(pdlv && ref == (qd_link_ref_t*) pn_delivery_get_context(pdlv));

        // This will decrement the qdr_delivery_t reference count - do not access the dlv pointer after this call!
        qdr_node_disconnect_deliveries(router->router_core, qd_link, dlv, pdlv);
        ref = DEQ_HEAD(*list);  // disconnecting the delivery removes ref from the list and frees it
    }

    qdr_link_t *qdr_link = (qdr_link_t *) qd_link_get_context(qd_link);
    if (qdr_link) {
        // Notify core that this link no longer exists
        qdr_link_set_context(qdr_link, 0);
        qd_link_set_context(qd_link, 0);
        qdr_link_notify_closed(qdr_link, forced);
        // This will cause the core to free qdr_link at some point so:
        qdr_link = 0;
    }
}

static void bind_connection_context(qdr_connection_t *qdrc, void* token)
{
    qd_connection_t *conn = (qd_connection_t*) token;
    qd_connection_set_context(conn, qdrc);
    qdr_connection_set_context(qdrc, conn);
}


static void save_original_and_current_conn_info(qd_connection_t *conn)
{
    // The failover list is present but it is empty. We will wipe the old backup information from the failover list.
    // The only items we want to keep in this list is the original connection information (from the config file)
    // and the current connection information.

    if (conn->connector && DEQ_SIZE(conn->connector->conn_info_list) > 1) {
        // Here we are simply removing all other failover information except the original connection information and the one we used to make a successful connection.
        int i = 1;
        qd_failover_item_t *item = DEQ_HEAD(conn->connector->conn_info_list);
        qd_failover_item_t *next_item = 0;

        bool match_found = false;
        int dec_conn_index=0;

        while(item) {

            //The first item on this list is always the original connector, so we want to keep that item in place
            // We have to delete items in the list that were left over from the previous failover list from the previous connection
            // because the new connection might have its own failover list.
            if (i != conn->connector->conn_index) {
                if (item != DEQ_HEAD(conn->connector->conn_info_list)) {
                    next_item = DEQ_NEXT(item);
                    free(item->scheme);
                    free(item->host);
                    free(item->port);
                    free(item->hostname);
                    free(item->host_port);

                    DEQ_REMOVE(conn->connector->conn_info_list, item);

                    free(item);
                    item = next_item;

                    // We are removing an item from the list before the conn_index match was found. We need to
                    // decrement the conn_index
                    if (!match_found)
                        dec_conn_index += 1;
                }
                else {
                    item = DEQ_NEXT(item);
                }
            }
            else {
                match_found = true;
                item = DEQ_NEXT(item);
            }
            i += 1;
        }

        conn->connector->conn_index -= dec_conn_index;
    }
}

static void AMQP_opened_handler(qd_router_t *router, qd_connection_t *conn, bool inbound)
{
    qdr_connection_role_t  role = 0;
    int                    cost = 1;
    int                    link_capacity = 1;
    const char            *name = 0;
    bool                   streaming_links = false;
    bool                   connection_trunking = false;
    char                   rversion[128];
    uint64_t               connection_id = qd_connection_connection_id(conn);
    pn_connection_t       *pn_conn = qd_connection_pn(conn);
    const char            *host   = 0;
    uint64_t               group_ordinal = 0;
    const char            *container  = conn->pn_conn ? pn_connection_remote_container(conn->pn_conn) : 0;
    char                   group_correlator[QD_DISCRIMINATOR_SIZE];
    char                   host_local[255];

    rversion[0]         = 0;
    group_correlator[0] = 0;
    host_local[0]       = 0;

    conn->strip_annotations_in  = false;
    conn->strip_annotations_out = false;
    qd_router_connection_get_config(conn, &role, &cost, &name,
                                    &conn->strip_annotations_in, &conn->strip_annotations_out, &link_capacity);

    qd_connector_t *connector = qd_connection_connector(conn);
    if (connector) {
        const qd_server_config_t *config = qd_connector_get_server_config(connector);
        snprintf(host_local, 254, "%s", config->host_port);
        host = &host_local[0];

        // Use the connectors tls_ordinal value as the group ordinal because the connection with the highest tls_ordinal
        // value has the most up-to-date security credentials and should take precedence over connections with a lower
        // ordinal value.
        (void) qd_connector_get_tls_ordinal(connector, &group_ordinal);
        memcpy(group_correlator, connector->ctor_config->group_correlator, QD_DISCRIMINATOR_SIZE);
        if (connector->is_data_connector) {
            // override the configured role to identify this as a data connection
            assert(role == QDR_ROLE_INTER_ROUTER);
            role = QDR_ROLE_INTER_ROUTER_DATA;
        }
    } else {
        host = qd_connection_name(conn);
    }

    // check offered capabilities for streaming link support and connection trunking support
    //
    pn_data_t *ocaps = pn_connection_remote_offered_capabilities(pn_conn);
    if (ocaps) {
        size_t sl_len = strlen(QD_CAPABILITY_STREAMING_LINKS);
        size_t ct_len = strlen(QD_CAPABILITY_CONNECTION_TRUNKING);
        pn_data_rewind(ocaps);
        if (pn_data_next(ocaps)) {
            if (pn_data_type(ocaps) == PN_ARRAY) {
                pn_data_enter(ocaps);
                pn_data_next(ocaps);
            }
            do {
                if (pn_data_type(ocaps) == PN_SYMBOL) {
                    pn_bytes_t s = pn_data_get_symbol(ocaps);
                    if (s.size == sl_len && strncmp(s.start, QD_CAPABILITY_STREAMING_LINKS, sl_len) == 0) {
                        streaming_links = true;
                    } else if (s.size == ct_len && strncmp(s.start, QD_CAPABILITY_CONNECTION_TRUNKING, ct_len) == 0) {
                        connection_trunking = true;
                    }
                }
            } while (pn_data_next(ocaps));
        }
    }

    // if connection properties are present parse out any important data
    //
    pn_data_t *props = pn_conn ? pn_connection_remote_properties(pn_conn) : 0;
    if (props) {
        const bool is_router = (role == QDR_ROLE_INTER_ROUTER || role == QDR_ROLE_EDGE_CONNECTION);
        pn_data_rewind(props);
        if (pn_data_next(props) && pn_data_type(props) == PN_MAP) {

            const size_t num_items   = pn_data_get_map(props);
            const int    max_props   = 8;  // total possible props
            int          props_found = 0;  // once all props found exit loop

            pn_data_enter(props);
            for (int i = 0; i < num_items / 2 && props_found < max_props; ++i) {
                if (!pn_data_next(props)) break;
                if (pn_data_type(props) != PN_SYMBOL) break;  // invalid properties map
                pn_bytes_t key = pn_data_get_symbol(props);

                if (key.size == strlen(QD_CONNECTION_PROPERTY_COST_KEY) &&
                    strncmp(key.start, QD_CONNECTION_PROPERTY_COST_KEY, key.size) == 0) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    if (is_router) {
                        if (pn_data_type(props) == PN_INT) {
                            const int remote_cost = (int) pn_data_get_int(props);
                            if (remote_cost > cost)
                                cost = remote_cost;
                        }
                    }

                } else if (key.size == strlen(QD_CONNECTION_PROPERTY_ROLE_KEY) &&
                    strncmp(key.start, QD_CONNECTION_PROPERTY_ROLE_KEY, key.size) == 0) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    if (role == QDR_ROLE_INTER_ROUTER) {
                        if (pn_data_type(props) == PN_INT) {
                            const int override_role = (int) pn_data_get_int(props);
                            if (override_role == QDR_ROLE_INTER_ROUTER_DATA) {
                                role = QDR_ROLE_INTER_ROUTER_DATA;
                            }
                        }
                    }

                } else if (key.size == strlen(QD_CONNECTION_PROPERTY_GROUP_CORRELATOR_KEY) &&
                    strncmp(key.start, QD_CONNECTION_PROPERTY_GROUP_CORRELATOR_KEY, key.size) == 0) {
                    props_found += 1;
                    assert(!connector);  // expect: connector sets correlator, listener consumes it
                    if (!pn_data_next(props)) break;
                    if (role == QDR_ROLE_INTER_ROUTER || role == QDR_ROLE_INTER_ROUTER_DATA) {
                        if (pn_data_type(props) == PN_STRING) {
                            // pn_bytes is not null terminated
                            pn_bytes_t gc = pn_data_get_string(props);
                            size_t len = MIN(gc.size, QD_DISCRIMINATOR_BYTES);
                            memcpy(group_correlator, gc.start, len);
                            group_correlator[len] = '\0';
                        }
                    }

                } else if (key.size == strlen(QD_CONNECTION_PROPERTY_GROUP_ORDINAL_KEY) &&
                    strncmp(key.start, QD_CONNECTION_PROPERTY_GROUP_ORDINAL_KEY, key.size) == 0) {
                    props_found += 1;
                    assert(!connector);  // expect: connector sets ordinal, listener consumes it
                    if (!pn_data_next(props)) break;
                    if (role == QDR_ROLE_INTER_ROUTER || role == QDR_ROLE_INTER_ROUTER_DATA) {
                        if (pn_data_type(props) == PN_ULONG) {
                            group_ordinal = pn_data_get_ulong(props);
                        }
                    }

                } else if (key.size == strlen(QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY) &&
                           strncmp(key.start, QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY, key.size) == 0) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    parse_failover_property_list(router, conn, props);

                } else if (key.size == strlen(QD_CONNECTION_PROPERTY_VERSION_KEY)
                           && strncmp(key.start, QD_CONNECTION_PROPERTY_VERSION_KEY, key.size) == 0) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    if (is_router) {
                        pn_bytes_t vdata = pn_data_get_string(props);
                        size_t vlen = MIN(sizeof(rversion) - 1, vdata.size);
                        strncpy(rversion, vdata.start, vlen);
                        rversion[vlen] = 0;
                    }

                } else if ((key.size == strlen(QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY)
                           && strncmp(key.start, QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY, key.size) == 0)) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    if (is_router && pn_data_type(props) == PN_INT) {
                        const int annos_version = (int) pn_data_get_int(props);
                        qd_log(LOG_ROUTER, QD_LOG_DEBUG, "Remote router annotations version: %d",
                               annos_version);
                    }

                } else if ((key.size == strlen(QD_CONNECTION_PROPERTY_ACCESS_ID)
                           && strncmp(key.start, QD_CONNECTION_PROPERTY_ACCESS_ID, key.size) == 0)) {
                    props_found += 1;
                    if (!pn_data_next(props)) break;
                    if (!!connector && !!connector->ctor_config && !!connector->ctor_config->vflow_record && pn_data_type(props) == PN_STRING) {
                        vflow_set_ref_from_pn(connector->ctor_config->vflow_record, VFLOW_ATTRIBUTE_PEER, props);
                    }

                } else {
                    // skip this key
                    if (!pn_data_next(props)) break;
                }

                // NOTE: if adding more keys update max_props value above!
            }
        }
    }

    // Gather transport-level information

    pn_transport_t *tport  = 0;
    pn_sasl_t      *sasl   = 0;
    const char     *mech   = 0;
    const char     *user   = 0;
    char           *proto  = 0;
    char           *cipher = 0;
    int            ssl_ssf = 0;

    if (conn->pn_conn) {
        tport = pn_connection_transport(conn->pn_conn);
    }
    if (tport) {
        sasl = pn_sasl(tport);
        if(conn->user_id)
            user = conn->user_id;
        else
            user = pn_transport_get_user(tport);
    }

    if (sasl)
        mech = pn_sasl_get_mech(sasl);

    if (conn->ssl) {
        proto = qd_tls_session_get_protocol_version(conn->ssl);
        cipher = qd_tls_session_get_protocol_ciphers(conn->ssl);
        ssl_ssf = qd_tls_session_get_ssf(conn->ssl);
    }

    bool encrypted     = tport && pn_transport_is_encrypted(tport);
    bool authenticated = tport && pn_transport_is_authenticated(tport);

    qdr_connection_info_t *connection_info = qdr_connection_info(encrypted,
                                                                 authenticated,
                                                                 conn->opened,
                                                                 (char*) mech,
                                                                 connector ? QD_OUTGOING : QD_INCOMING,
                                                                 host,
                                                                 proto,
                                                                 cipher,
                                                                 (char*) user,
                                                                 container,
                                                                 props,
                                                                 qd_tls_session_get_ssl_profile_ordinal(conn->ssl),
                                                                 ssl_ssf,
                                                                 !!conn->ssl,
                                                                 rversion,
                                                                 streaming_links,
                                                                 connection_trunking);

    qdr_connection_info_set_group(connection_info, group_correlator, group_ordinal);

    qdr_connection_opened(router->router_core,
                          amqp_adaptor.adaptor,
                          inbound,
                          role,
                          cost,
                          connection_id,
                          name,
                          conn->strip_annotations_in,
                          conn->strip_annotations_out,
                          link_capacity,
                          !!conn->policy_settings ? &conn->policy_settings->spec : 0,
                          connection_info,
                          bind_connection_context,
                          conn);
    if (role == QDR_ROLE_NORMAL) {
        // These counters track the number of 'user' connections, not infrastructure connections
        qd_connection_counter_inc(QD_PROTOCOL_AMQP);
    }

    if (!!conn->listener && role != QDR_ROLE_INTER_ROUTER_DATA) {
        qd_listener_add_link(conn->listener);
    }

    if (!!connector) {
        qd_connector_add_link(connector);
        sys_mutex_lock(&connector->lock);
        qd_format_string(connector->conn_msg, QD_CTOR_CONN_MSG_BUF_SIZE,
                         "[C%"PRIu64"] Connection Opened: dir=%s host=%s encrypted=%s auth=%s user=%s container_id=%s",
                         connection_id, inbound ? "in" : "out", host, encrypted ? proto : "no",
                         authenticated ? mech : "no", (char*) user, container);
        sys_mutex_unlock(&connector->lock);
    }

    free(proto);
    free(cipher);
}


// We are attempting to find a connection property called failover-server-list which is a list of failover host names and ports..
// failover-server-list looks something like this
//      :"failover-server-list"=[{:"network-host"="some-host", :port="35000"}, {:"network-host"="localhost", :port="25000"}]
// There are three cases here -
// 1. The failover-server-list is present but the content of the list is empty in which case we scrub the failover list except we keep the original connector information and current connection information.
// 2. If the failover list contains one or more maps that contain failover connection information, that information will be appended to the list which already contains the original connection information
//    and the current connection information. Any other failover information left over from the previous connection is deleted
// 3. If the failover-server-list is not present at all in the connection properties, the failover list we maintain is untouched.
//
// props should be pointing at the value that corresponds to the QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY
// returns true if failover list properly parsed.
//
static bool parse_failover_property_list(qd_router_t *router, qd_connection_t *conn, pn_data_t *props)
{
    bool found_failover = false;

    if (pn_data_type(props) != PN_LIST)
        return false;

    size_t list_num_items = pn_data_get_list(props);

    if (list_num_items > 0) {

        save_original_and_current_conn_info(conn);

        pn_data_enter(props); // enter list

        for (int i=0; i < list_num_items; i++) {
            pn_data_next(props);// this is the first element of the list, a map.
            if (props && pn_data_type(props) == PN_MAP) {

                size_t map_num_items = pn_data_get_map(props);
                pn_data_enter(props);

                qd_failover_item_t *item = NEW(qd_failover_item_t);
                ZERO(item);

                // We have found a map with the connection information. Step thru the map contents and create qd_failover_item_t

                for (int j=0; j < map_num_items/2; j++) {

                    pn_data_next(props);
                    if (pn_data_type(props) == PN_SYMBOL) {
                        pn_bytes_t sym = pn_data_get_symbol(props);
                        if (sym.size == strlen(QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY) &&
                            strcmp(sym.start, QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY) == 0) {
                            pn_data_next(props);
                            if (pn_data_type(props) == PN_STRING) {
                                item->host = strdup(pn_data_get_string(props).start);
                            }
                        }
                        else if (sym.size == strlen(QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY) &&
                                 strcmp(sym.start, QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY) == 0) {
                            pn_data_next(props);
                            item->port = qdpn_data_as_string(props);

                        }
                        else if (sym.size == strlen(QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY) &&
                                 strcmp(sym.start, QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY) == 0) {
                            pn_data_next(props);
                            if (pn_data_type(props) == PN_STRING) {
                                item->scheme = strdup(pn_data_get_string(props).start);
                            }

                        }
                        else if (sym.size == strlen(QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY) &&
                                 strcmp(sym.start, QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY) == 0) {
                            pn_data_next(props);
                            if (pn_data_type(props) == PN_STRING) {
                                item->hostname = strdup(pn_data_get_string(props).start);
                            }
                        }
                    }
                }

                int host_length = strlen(item->host);
                //
                // We will not even bother inserting the item if there is no host available.
                //
                if (host_length != 0) {
                    if (item->scheme == 0)
                        item->scheme = strdup("amqp");
                    if (item->port == 0)
                        item->port = strdup("5672");

                    int hplen = strlen(item->host) + strlen(item->port) + 2;
                    item->host_port = malloc(hplen);
                    snprintf(item->host_port, hplen, "%s:%s", item->host, item->port);

                    //
                    // Iterate through failover list items and sets insert_tail to true
                    // when list has just original connector's host and port or when new
                    // reported host and port is not yet part of the current list.
                    //
                    bool insert_tail = false;
                    if ( DEQ_SIZE(conn->connector->conn_info_list) == 1 ) {
                        insert_tail = true;
                    } else {
                        qd_failover_item_t *conn_item = DEQ_HEAD(conn->connector->conn_info_list);
                        insert_tail = true;
                        while ( conn_item ) {
                            if ( !strcmp(conn_item->host_port, item->host_port) ) {
                                insert_tail = false;
                                break;
                            }
                            conn_item = DEQ_NEXT(conn_item);
                        }
                    }

                    // Only inserts if not yet part of failover list
                    if ( insert_tail ) {
                        DEQ_INSERT_TAIL(conn->connector->conn_info_list, item);
                        qd_log(LOG_ROUTER, QD_LOG_DEBUG, "Added %s as backup host", item->host_port);
                        found_failover = true;
                    }
                    else {
                        free(item->scheme);
                        free(item->host);
                        free(item->port);
                        free(item->hostname);
                        free(item->host_port);
                        free(item);
                    }

                }
                else {
                    free(item->scheme);
                    free(item->host);
                    free(item->port);
                    free(item->hostname);
                    free(item->host_port);
                    free(item);
                }
            }
            pn_data_exit(props);
        }
    } // list_num_items > 0
    else {
        save_original_and_current_conn_info(conn);
    }

    return found_failover;
}


int AMQP_inbound_opened_handler(qd_router_t *router, qd_connection_t *conn, void *context)
{
    AMQP_opened_handler(router, conn, true);
    return 0;
}


int AMQP_outbound_opened_handler(qd_router_t *router, qd_connection_t *conn, void *context)
{
    AMQP_opened_handler(router, conn, false);
    return 0;
}


int AMQP_closed_handler(qd_router_t *router, qd_connection_t *conn, void *context)
{
    qdr_connection_t *qdrc   = (qdr_connection_t*) qd_connection_get_context(conn);

    if (qdrc) {
        sys_mutex_lock(qd_server_get_activation_lock(router->qd->server));
        qdr_connection_set_context(qdrc, 0);
        sys_mutex_unlock(qd_server_get_activation_lock(router->qd->server));

        if (qdrc->role == QDR_ROLE_NORMAL) {
            qd_connection_counter_dec(QD_PROTOCOL_AMQP);
        }
        if (!!conn->listener && qdrc->role != QDR_ROLE_INTER_ROUTER_DATA) {
            qd_listener_remove_link(conn->listener);
        }
        qdr_connection_notify_closed(qdrc);
        qd_connection_set_context(conn, 0);
    }

    return 0;
}


static void CORE_connection_activate(void *context, qdr_connection_t *conn)
{
    qd_router_t *router = (qd_router_t*) context;

    //
    // IMPORTANT:  This is the only core callback that is invoked on the core
    //             thread itself. It must not take locks that could deadlock the core.
    //

    sys_mutex_lock(qd_server_get_activation_lock(router->qd->server));
    qd_connection_t *ctx = (qd_connection_t*) qdr_connection_get_context(conn);
    if (!!ctx && !SET_ATOMIC_FLAG(&ctx->wake_core)) {
        qd_connection_activate(ctx);
    }
    sys_mutex_unlock(qd_server_get_activation_lock(router->qd->server));
}


static void CORE_link_first_attach(void             *context,
                                   qdr_connection_t *conn,
                                   qdr_link_t       *link,
                                   qdr_terminus_t   *source,
                                   qdr_terminus_t   *target,
                                   qd_session_class_t ssn_class)
{
    qd_connection_t *qconn  = (qd_connection_t*) qdr_connection_get_context(conn);
    if (!qconn) return;        /* Connection is already closed */

    //
    // Create a new link to be attached
    //
    qd_link_t *qlink = qd_link(qconn, qdr_link_direction(link), qdr_link_name(link), ssn_class);

    //
    // Copy the source and target termini to the link
    //
    qdr_terminus_copy(source, qd_link_source(qlink));
    qdr_terminus_copy(target, qd_link_target(qlink));

    //
    // Associate the qd_link and the qdr_link to each other
    //
    qdr_link_set_context(link, qlink);
    qd_link_set_context(qlink, link);

    //
    // Open (attach) the link
    //
    pn_link_open(qd_link_pn(qlink));

    //
    // Mark the link as stalled and waiting for initial credit.
    //
    if (qdr_link_direction(link) == QD_OUTGOING)
        qdr_link_stalled_outbound(link);
}


static void CORE_link_second_attach(void *context, qdr_link_t *link, qdr_terminus_t *source, qdr_terminus_t *target)
{
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *pn_link = qd_link_pn(qlink);
    if (!pn_link)
        return;

    qdr_terminus_copy(source, qd_link_source(qlink));
    qdr_terminus_copy(target, qd_link_target(qlink));

    //
    // Open (attach) the link
    //
    pn_link_open(pn_link);

    //
    // Mark the link as stalled and waiting for initial credit.
    //
    if (qdr_link_direction(link) == QD_OUTGOING)
        qdr_link_stalled_outbound(link);
}


static void CORE_conn_trace(void *context, qdr_connection_t *qdr_conn, bool trace)
{
    qd_connection_t *qconn = (qd_connection_t*) qdr_connection_get_context(qdr_conn);

    if (!qconn)
        return;

    pn_transport_t *tport = pn_connection_transport(qconn->pn_conn);

    if (!tport)
        return;

    if (trace) {
        pn_transport_trace(tport, PN_TRACE_FRM);
        pn_transport_set_tracer(tport, qd_connection_transport_tracer);
    }
    else {
        pn_transport_trace(tport, PN_TRACE_OFF);
    }
}

static void CORE_close_connection(void *context, qdr_connection_t *qdr_conn, qdr_error_t *error)
{
    if (qdr_conn) {
        qd_connection_t *qd_conn = qdr_connection_get_context(qdr_conn);
        if (qd_conn) {
            pn_connection_t *pn_conn = qd_connection_pn(qd_conn);
            if (pn_conn) {
                //
                // Go down to the transport and close the head and tail.  This will
                // drop the socket to the peer without providing any error indication.
                // Due to issues in Proton that cause different behaviors in different
                // bindings depending on whether there is a connection:forced error,
                // this has been deemed the best way to force the peer to reconnect.
                //
                pn_transport_t *tport = pn_connection_transport(pn_conn);
                pn_transport_close_head(tport);
                pn_transport_close_tail(tport);
            }
        }
    }
}

static void CORE_link_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first)
{
    qd_link_t   *qlink  = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *pn_link = qd_link_pn(qlink);
    if (!pn_link || !!(pn_link_state(pn_link) & PN_LOCAL_CLOSED))  // already detached
        return;

    if (error) {
        pn_condition_t *cond = pn_link_condition(pn_link);
        qdr_error_copy(error, cond);
    }

    //
    // If the link is only half open, then this DETACH constitutes the rejection of
    // an incoming ATTACH.  We must nullify the source and target in order to be
    // compliant with the AMQP specification.  This is because Proton will generate
    // the missing ATTACH before the DETACH and will include spurious terminus data
    // if we don't nullify it here.
    //
    if (pn_link_state(pn_link) & PN_LOCAL_UNINIT) {
        if (pn_link_is_receiver(pn_link)) {
            pn_terminus_set_type(pn_link_target(pn_link), PN_UNSPECIFIED);
            pn_terminus_copy(pn_link_source(pn_link), pn_link_remote_source(pn_link));
        } else {
            pn_terminus_set_type(pn_link_source(pn_link), PN_UNSPECIFIED);
            pn_terminus_copy(pn_link_target(pn_link), pn_link_remote_target(pn_link));
        }
    }

    qd_link_close(qlink);
}


static void CORE_link_flow(void *context, qdr_link_t *link, int credit)
{
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *plink = qd_link_pn(qlink);

    if (plink)
        pn_link_flow(plink, credit);
}


static void CORE_link_offer(void *context, qdr_link_t *link, int delivery_count)
{
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *plink = qd_link_pn(qlink);

    if (plink)
        pn_link_offered(plink, delivery_count);
}


static void CORE_link_drained(void *context, qdr_link_t *link)
{
    qd_router_t *router = (qd_router_t*) context;
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *plink = qd_link_pn(qlink);

    if (plink) {
        pn_link_drained(plink);
        qdr_link_set_drained(router->router_core, link);
    }
}


static void CORE_link_drain(void *context, qdr_link_t *link, bool mode)
{
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return;

    pn_link_t *plink = qd_link_pn(qlink);

    if (plink) {
        if (pn_link_is_receiver(plink))
            pn_link_set_drain(plink, mode);
    }
}


static int CORE_link_push(void *context, qdr_link_t *link, int limit)
{
    qd_router_t *router = (qd_router_t*) context;
    qd_link_t   *qlink  = (qd_link_t*) qdr_link_get_context(link);
    if (!qlink)
        return 0;

    pn_link_t *plink = qd_link_pn(qlink);

    if (plink) {
        int link_credit = pn_link_credit(plink);
        if (link_credit > limit)
            link_credit = limit;

        if (link_credit > 0)
            // We will not bother calling qdr_link_process_deliveries if we have no credit.
            return qdr_link_process_deliveries(router->router_core, link, link_credit);
    }
    return 0;
}

static uint64_t CORE_link_deliver(void *context, qdr_link_t *link, qdr_delivery_t *dlv, bool settled)
{
    qd_router_t *router = (qd_router_t*) context;
    qd_link_t   *qlink  = (qd_link_t*) qdr_link_get_context(link);
    qd_connection_t *qconn = qd_link_connection(qlink);
    ssize_t octets_sent = 0;

    uint64_t update = 0;

    if (!qlink)
        return 0;

    pn_link_t *plink = qd_link_pn(qlink);
    if (!plink)
        return 0;

    //
    // If the remote send settle mode is set to 'settled' then settle the delivery on behalf of the receiver.
    //
    bool remote_snd_settled = qd_link_remote_snd_settle_mode(qlink) == PN_SND_SETTLED;
    pn_delivery_t *pdlv = 0;

    if (!qdr_delivery_tag_sent(dlv)) {
        const char *tag;
        int         tag_length;
        uint64_t    disposition = 0;

        qdr_delivery_tag(dlv, &tag, &tag_length);

        // Create a new proton delivery on link 'plink'
        pn_delivery(plink, pn_dtag(tag, tag_length));

        pdlv = pn_link_current(plink);

        // handle any delivery-state on the transfer e.g. transactional-state
        qd_delivery_state_t *dstate = qdr_delivery_take_local_delivery_state(dlv, &disposition);
        if (disposition) {
            if (dstate)
                qd_delivery_write_local_state(pdlv, disposition, dstate);
            pn_delivery_update(pdlv, disposition);
        }
        qd_delivery_state_free(dstate);

        //
        // If the remote send settle mode is set to 'settled', we should settle the delivery on behalf of the receiver.
        //
        if (qdr_delivery_get_context(dlv) == 0)
            qdr_node_connect_deliveries(qlink, dlv, pdlv);

        qdr_delivery_set_tag_sent(dlv, true);
    } else {
        pdlv = qdr_node_delivery_pn_from_qdr(dlv);
    }

    if (!pdlv)
        return 0;

    if (qdr_delivery_is_abort_outbound(dlv)) {
        pn_delivery_abort(pdlv);
        pn_link_advance(plink);
        qdr_node_disconnect_deliveries(router->router_core, qlink, dlv, pdlv);
        pn_delivery_settle(pdlv);
        return 0;
    }

    bool q3_stalled = false;

    qd_message_t *msg_out = qdr_delivery_message(dlv);

    unsigned int ra_flags = qdr_link_strip_annotations_out(link) ? QD_MESSAGE_RA_STRIP_ALL
        // edge routers do not propagate self in trace or ingress RA
        : router->router_mode == QD_ROUTER_MODE_EDGE ? (QD_MESSAGE_RA_STRIP_INGRESS | QD_MESSAGE_RA_STRIP_TRACE)
        : QD_MESSAGE_RA_STRIP_NONE;

    octets_sent = qd_message_send(msg_out, qlink, ra_flags, &q3_stalled);
    bool send_complete = qdr_delivery_send_complete(dlv);

    //
    // Bump LINK metrics if appropriate
    //
    if (!!qconn->connector && !!qconn->connector->ctor_config && !!qconn->connector->ctor_config->vflow_record) {
        vflow_inc_counter(qconn->connector->ctor_config->vflow_record, VFLOW_ATTRIBUTE_OCTETS, (uint64_t) octets_sent);
    }

    //
    // If this message content has cut-through enabled, set consumer activation in the message.
    //
    if (!send_complete && !dlv->in_message_activation && qd_message_is_unicast_cutthrough(msg_out)) {
        qd_message_activation_t activation;
        activation.delivery = dlv;
        activation.type     = QD_ACTIVATION_AMQP;
        qd_alloc_set_safe_ptr(&activation.safeptr, qconn);
        dlv->in_message_activation = true;
        qd_log(LOG_ROUTER, QD_LOG_DEBUG, DLV_FMT " AMQP enable consumer activation", DLV_ARGS(dlv));
        qd_message_set_consumer_activation(msg_out, &activation);
    }

    if (q3_stalled) {
        qd_link_q3_block(qlink);
        qdr_link_stalled_outbound(link);
    }

    if (send_complete) {
        if (qd_message_aborted(msg_out)) {
            // Aborted messages must be settled locally
            // Settling does not produce any disposition to message sender.
            if (pdlv) {
                pn_link_advance(plink);
                qdr_node_disconnect_deliveries(router->router_core, qlink, dlv, pdlv);
                pn_delivery_settle(pdlv);
            }
        } else {
            if (!settled && remote_snd_settled) {
                // The caller must tell the core that the delivery has been
                // accepted and settled, since we are settling on behalf of the
                // receiver
                update = PN_ACCEPTED;  // schedule the settle
            }

            pn_link_advance(plink);

            if (settled || remote_snd_settled) {
                if (pdlv) {
                    qdr_node_disconnect_deliveries(router->router_core, qlink, dlv, pdlv);
                    pn_delivery_settle(pdlv);
                }
            }
        }
        clear_consumer_activation(router->router_core, dlv);
        log_link_message(qconn, plink, msg_out);
    }
    return update;
}


static int CORE_link_get_credit(void *context, qdr_link_t *link)
{
    qd_link_t *qlink = (qd_link_t*) qdr_link_get_context(link);
    pn_link_t *plink = !!qlink ? qd_link_pn(qlink) : 0;

    if (!plink)
        return 0;

    return pn_link_remote_credit(plink);
}


static void CORE_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    qd_router_t   *router = (qd_router_t*) context;
    pn_delivery_t *pnd    = qdr_node_delivery_pn_from_qdr(dlv);

    if (!pnd)
        return;

    // If the delivery's link is somehow gone (maybe because of a connection drop, we don't proceed.
    if (!pn_delivery_link(pnd))
        return;

    qdr_link_t      *qlink   = qdr_delivery_link(dlv);
    qd_link_t       *link    = 0;
    qd_connection_t *qd_conn = 0;

    if (qlink) {
        link = (qd_link_t*) qdr_link_get_context(qlink);
        if (link) {
            qd_conn = qd_link_connection(link);
             if (qd_conn == 0)
                return;
        }
        else
            return;
    }
    else
        return;

    if (disp && !pn_delivery_settled(pnd)) {
        uint64_t ignore = 0;
        qd_delivery_state_t *dstate = qdr_delivery_take_local_delivery_state(dlv, &ignore);

        // update if the disposition has changed or there is new state associated with it
        if (disp != pn_delivery_local_state(pnd) || dstate) {
            // handle propagation of delivery state from qdr_delivery_t to proton:
            qd_delivery_write_local_state(pnd, disp, dstate);
            pn_delivery_update(pnd, disp);
            qd_delivery_state_free(dstate);
            if (disp == PN_MODIFIED)  // @TODO(kgiusti) why do we need this???
                pn_disposition_set_failed(pn_delivery_local(pnd), true);
        }
    }

    if (settled) {
        qd_message_t *msg = qdr_delivery_message(dlv);
        if (qd_message_receive_complete(msg)) {
            //
            // If the delivery is settled and the message has fully arrived, disconnect
            // the linkages and settle it in Proton now.
            //
            qdr_node_disconnect_deliveries(router->router_core, link, dlv, pnd);
            pn_delivery_settle(pnd);
        } else {
            if (disp == PN_RELEASED || disp == PN_MODIFIED || disp == PN_REJECTED || qdr_delivery_presettled(dlv)) {
                //
                // If the delivery is settled and it is still arriving, defer the settlement
                // until the content has fully arrived. For now set the disposition on the qdr_delivery
                // We will use this disposition later on to set the disposition on the proton delivery.
                //
                qdr_delivery_set_disposition(dlv, disp);
                //
                // We have set the message to be discarded. We will use this information
                // in AMQP_rx_handler to only update the disposition on the proton delivery if the message is discarded.
                //
                qd_message_set_discard(msg, true);
                //
                // If the disposition is RELEASED or MODIFIED, set the message to discard
                // and if it is blocked by Q2 holdoff, get the link rolling again.
                //
                qd_message_Q2_holdoff_disable(msg);
            }
        }
    }
}


// invoked by an I/O thread when enough buffers have been released deactivate
// the Q2 block.  Note that this method will likely be running on a worker
// thread that is not the same thread that "owns" the qd_link_t passed in.
//
void qd_link_q2_restart_receive(qd_alloc_safe_ptr_t context)
{
    qd_link_t *in_link = (qd_link_t*) qd_alloc_deref_safe_ptr(&context);
    if (!in_link)
        return;

    assert(qd_link_direction(in_link) == QD_INCOMING);

    qd_connection_t *in_conn = qd_link_connection(in_link);
    if (in_conn) {
        qd_link_t_sp *safe_ptr = NEW(qd_alloc_safe_ptr_t);
        *safe_ptr = context;  // use original to keep old sequence counter
        qd_connection_invoke_deferred(in_conn, deferred_AMQP_rx_handler, safe_ptr);
    }
}


// Issue a warning POLICY log message with connection and link identities
// prepended to the policy denial text string.
void qd_connection_log_policy_denial(const qd_link_t *link, const char *text)
{
    qdr_link_t *rlink = (qdr_link_t*) qd_link_get_context(link);
    uint64_t l_id = 0;
    uint64_t c_id = 0;
    if (rlink) {
        l_id = rlink->identity;
        if (rlink->conn) {
            c_id = rlink->conn->identity;
        }
    }
    qd_log(LOG_POLICY, QD_LOG_WARNING, "[C%" PRIu64 "][L%" PRIu64 "] %s", c_id, l_id, text);
}

//
// AMQP Adaptor Init/Finalize
//

static void qd_amqp_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    qd_dispatch_t *qd = qdr_core_dispatch(core);

    sys_mutex_init(&amqp_adaptor.lock);

    // not necessary but keeps thread analyizer happy:
    sys_mutex_lock(&amqp_adaptor.lock);
    DEQ_INIT(amqp_adaptor.conn_list);
    sys_mutex_unlock(&amqp_adaptor.lock);

    amqp_adaptor.core    = core;
    amqp_adaptor.dispatch = qd;
    assert(qd->router);  // ensure router has been initialized first
    amqp_adaptor.router  = qd->router;
    amqp_adaptor.container = qd_container(qd->router);
    amqp_adaptor.adaptor = qdr_protocol_adaptor(core,
                                                        "amqp",
                                                        (void*) amqp_adaptor.router,
                                                        CORE_connection_activate,
                                                        CORE_link_first_attach,
                                                        CORE_link_second_attach,
                                                        CORE_link_detach,
                                                        CORE_link_flow,
                                                        CORE_link_offer,
                                                        CORE_link_drained,
                                                        CORE_link_drain,
                                                        CORE_link_push,
                                                        CORE_link_deliver,
                                                        CORE_link_get_credit,
                                                        CORE_delivery_update,
                                                        CORE_close_connection,
                                                        CORE_conn_trace);

    *adaptor_context = (void *) &amqp_adaptor;
}

static void qd_amqp_adaptor_final(void *adaptor_context)
{
    qd_connection_list_t conn_list = DEQ_EMPTY;

    sys_mutex_lock(&amqp_adaptor.lock);
    DEQ_MOVE(amqp_adaptor.conn_list, conn_list);
    sys_mutex_unlock(&amqp_adaptor.lock);

    // KAG: todo: duplicates code in qd_connection.c (qd_connection_free). Fixme
    qd_connection_t *ctx = DEQ_HEAD(conn_list);
    while (ctx) {
        qd_log(LOG_SERVER, QD_LOG_INFO, "[C%" PRIu64 "] Closing connection on shutdown", ctx->connection_id);
        DEQ_REMOVE_HEAD(conn_list);
        if (ctx->pn_conn) {
            pn_transport_t *tport = pn_connection_transport(ctx->pn_conn);
            if (tport)
                pn_transport_set_context(tport, 0); /* for transport_tracer */
            pn_connection_set_context(ctx->pn_conn, 0);
        }
        qd_connection_invoke_deferred_calls(ctx, true);  // Discard any pending deferred calls
        free(ctx->user_id);
        sys_mutex_free(&ctx->deferred_call_lock);
        free(ctx->name);
        free(ctx->role);
        if (ctx->policy_settings)
            qd_policy_settings_free(ctx->policy_settings);
        if (ctx->connector) {
            qd_connector_remove_connection(ctx->connector, true, 0, 0);
            ctx->connector = 0;
        }
        if (ctx->listener) {
            qd_listener_remove_connection(ctx->listener, ctx);
            ctx->listener = 0;
        }

        // free up any still active sessions
        for (int i = 0; i < QD_SSN_CLASS_COUNT; ++i)
            qd_session_decref(ctx->qd_sessions[i]);
        qd_connection_release_sessions(ctx);

        qd_tls_session_free(ctx->ssl);
        sys_atomic_destroy(&ctx->wake_core);
        sys_atomic_destroy(&ctx->wake_cutthrough_inbound);
        sys_atomic_destroy(&ctx->wake_cutthrough_outbound);
        sys_spinlock_free(&ctx->inbound_cutthrough_spinlock);
        sys_spinlock_free(&ctx->outbound_cutthrough_spinlock);

        free_qd_connection_t(ctx);
        ctx = DEQ_HEAD(conn_list);
    }

    qdr_protocol_adaptor_free(amqp_adaptor.core, amqp_adaptor.adaptor);
    qd_container_free(amqp_adaptor.container);

    memset(&amqp_adaptor, 0, sizeof(amqp_adaptor));
}

QDR_CORE_ADAPTOR_DECLARE("amqp", qd_amqp_adaptor_init, qd_amqp_adaptor_final)

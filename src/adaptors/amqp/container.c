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

#include "container.h"

#include "policy.h"
#include "qd_connection.h"
#include "node_type.h"
#include "private.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/hash.h"
#include "qpid/dispatch/iterator.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/amqp_adaptor.h"

#include <proton/connection.h>
#include <proton/engine.h>
#include <proton/event.h>
#include <proton/message.h>

#include <inttypes.h>
#include <stdio.h>
#include <string.h>


/** Encapsulates a proton link for sending and receiving messages */
struct qd_link_t {
    DEQ_LINKS(qd_link_t);
    pn_session_t               *pn_sess;
    pn_link_t                  *pn_link;
    qd_direction_t              direction;
    void                       *context;
    qd_alloc_safe_ptr_t         incoming_msg;  // DISPATCH-1690: for cleanup
    pn_snd_settle_mode_t        remote_snd_settle_mode;
    qd_link_ref_list_t          ref_list;
    bool                        q2_limit_unbounded;
    bool                        q3_blocked;
    DEQ_LINKS_N(Q3, qd_link_t); ///< Q3 blocked links
    uint64_t                    link_id;
};

ALLOC_DEFINE_SAFE(qd_link_t);
ALLOC_DEFINE(qd_link_ref_t);

/** Encapsulates a proton session */
struct qd_session_t {
    DEQ_LINKS(qd_session_t);
    pn_session_t   *pn_sess;
    qd_link_list_t  q3_blocked_links;  ///< Q3 blocked if !empty
};

// Bug workaround to free links/session when we hope they are no longer used!
// Fingers crossed! :|
//
struct qd_pn_free_link_session_t {
    DEQ_LINKS(qd_pn_free_link_session_t);
    pn_session_t *pn_session;
    pn_link_t    *pn_link;
};
ALLOC_DEFINE(qd_pn_free_link_session_t);


// macros to work around PROTON-2184 (see container.h)
//
static inline void qd_session_link_pn(qd_session_t *qd_ssn, pn_session_t *pn_ssn)
{
    assert(qd_ssn && pn_ssn);
    pn_session_set_context(pn_ssn, qd_ssn);
}

static inline void qd_session_unlink_pn(qd_session_t *qd_ssn, pn_session_t *pn_ssn)
{
    assert(qd_ssn);
    pn_session_set_context(pn_ssn, 0);
}

ALLOC_DECLARE(qd_session_t);
ALLOC_DEFINE(qd_session_t);

typedef struct qdc_node_type_t {
    DEQ_LINKS(struct qdc_node_type_t);
    const qd_node_type_t *ntype;
} qdc_node_type_t;
DEQ_DECLARE(qdc_node_type_t, qdc_node_type_list_t);

struct qd_container_t {
    qd_router_t          *qd_router;
    const qd_node_type_t *ntype;
    sys_mutex_t           lock;
    // KAG: todo move to amqp_adaptor
    qd_link_list_t        links;
};


static void setup_outgoing_link(qd_container_t *container, pn_link_t *pn_link)
{
    qd_link_t *link = new_qd_link_t();
    if (!link) {
        pn_condition_t *cond = pn_link_condition(pn_link);
        pn_condition_set_name(cond, QD_AMQP_COND_INTERNAL_ERROR);
        pn_condition_set_description(cond, "Insufficient memory");
        pn_link_close(pn_link);
        return;
    }

    ZERO(link);
    sys_mutex_lock(&container->lock);
    DEQ_INSERT_TAIL(container->links, link);
    sys_mutex_unlock(&container->lock);
    link->pn_sess    = pn_link_session(pn_link);
    link->pn_link    = pn_link;
    link->direction  = QD_OUTGOING;

    link->remote_snd_settle_mode = pn_link_remote_snd_settle_mode(pn_link);

    pn_link_set_context(pn_link, link);
    container->ntype->outgoing_handler(container->qd_router, link);
}


static void setup_incoming_link(qd_container_t *container, pn_link_t *pn_link, uint64_t max_size)
{
    qd_link_t *link = new_qd_link_t();
    if (!link) {
        pn_condition_t *cond = pn_link_condition(pn_link);
        pn_condition_set_name(cond, QD_AMQP_COND_INTERNAL_ERROR);
        pn_condition_set_description(cond, "Insufficient memory");
        pn_link_close(pn_link);
        return;
    }

    ZERO(link);
    sys_mutex_lock(&container->lock);
    DEQ_INSERT_TAIL(container->links, link);
    sys_mutex_unlock(&container->lock);
    link->pn_sess    = pn_link_session(pn_link);
    link->pn_link    = pn_link;
    link->direction  = QD_INCOMING;
    link->remote_snd_settle_mode = pn_link_remote_snd_settle_mode(pn_link);

    if (max_size) {
        pn_link_set_max_message_size(pn_link, max_size);
    }
    pn_link_set_context(pn_link, link);
    container->ntype->incoming_handler(container->qd_router, link);
}


static void handle_link_open(qd_container_t *container, pn_link_t *pn_link)
{
    qd_link_t *link = (qd_link_t*) pn_link_get_context(pn_link);
    if (link == 0)
        return;
    container->ntype->link_attach_handler(container->qd_router, link);
}


static void do_receive(qd_container_t *container, pn_link_t *pn_link, pn_delivery_t *pnd)
{
    qd_link_t     *link     = (qd_link_t*) pn_link_get_context(pn_link);

    if (link) {
        while (true) {
            if (!container->ntype->rx_handler(container->qd_router, link))
                break;
        }
        return;
    }

    //
    // Reject the delivery if we couldn't find a node to handle it
    //
    pn_link_advance(pn_link);
    pn_link_flow(pn_link, 1);
    pn_delivery_update(pnd, PN_REJECTED);
    pn_delivery_settle(pnd);
}


static void do_updated(qd_container_t *container, pn_delivery_t *pnd)
{
    pn_link_t     *pn_link  = pn_delivery_link(pnd);
    qd_link_t     *link     = (qd_link_t*) pn_link_get_context(pn_link);

    if (link) {
        container->ntype->disp_handler(container->qd_router, link, pnd);
    }
}


static void notify_opened(qd_container_t *container, qd_connection_t *conn, void *context)
{
    if (qd_connection_inbound(conn)) {
        container->ntype->inbound_conn_opened_handler(container->qd_router, conn, context);
    } else {
        container->ntype->outbound_conn_opened_handler(container->qd_router, conn, context);
    }
}

void policy_notify_opened(void *container, qd_connection_t *conn, void *context)
{
    notify_opened((qd_container_t *)container, (qd_connection_t *)conn, context);
}

static void notify_closed(qd_container_t *container, qd_connection_t *conn, void *context)
{
    container->ntype->conn_closed_handler(container->qd_router, conn, context);
}

static void close_links(qd_container_t *container, pn_connection_t *conn, bool print_log)
{
    pn_link_t *pn_link = pn_link_head(conn, 0);
    while (pn_link) {
        qd_link_t *qd_link = (qd_link_t*) pn_link_get_context(pn_link);

        if (qd_link && qd_link_get_context(qd_link) == 0) {
            pn_link_set_context(pn_link, 0);
            pn_link = pn_link_next(pn_link, 0);
            qd_link_free(qd_link);
            continue;
        }

        if (qd_link) {
            if (print_log)
                qd_log(LOG_CONTAINER, QD_LOG_DEBUG, "Aborting link '%s' due to parent connection end",
                       pn_link_name(pn_link));
            container->ntype->link_detach_handler(container->qd_router, qd_link, QD_LOST);
        }
        pn_link = pn_link_next(pn_link, 0);
    }
}


/** cleanup_link
 * Prior to freeing qd_link_t - safe to call during shutdown
 */
static void cleanup_link(qd_link_t *link)
{
    if (link) {
        if (link->q3_blocked)
            qd_link_q3_unblock(link);
        if (link->pn_link) {
            pn_link_set_context(link->pn_link, NULL);
            link->pn_link = 0;
        }
        link->pn_sess = 0;

        // cleanup any inbound message that has not been forwarded
        qd_message_t *msg = qd_alloc_deref_safe_ptr(&link->incoming_msg);
        if (msg) {
            qd_message_free(msg);
        }
    }
}

static int close_handler(qd_container_t *container, pn_connection_t *conn, qd_connection_t* qd_conn)
{
    //
    // Close all links, passing QD_LOST as the reason.  These links are not
    // being properly 'detached'.  They are being orphaned.
    //
    if (qd_conn)
        qd_conn->closed = true;
    close_links(container, conn, true);
    qd_session_cleanup(qd_conn);
    if (qd_conn)
        notify_closed(container, qd_conn, qd_connection_get_context(qd_conn));
    return 0;
}


static void writable_handler(qd_container_t *container, pn_connection_t *conn, qd_connection_t* qd_conn)
{
    container->ntype->writable_handler(container->qd_router, qd_conn, 0);
}


/**
 * Returns true if the free_link already exists in free_link_list, false otherwise
 */
static bool link_exists(qd_pn_free_link_session_list_t *free_list, pn_link_t *free_link)
{
    qd_pn_free_link_session_t *free_item = DEQ_HEAD(*free_list);
    while(free_item) {
        if (free_item->pn_link == free_link)
            return true;
        free_item = DEQ_NEXT(free_item);
    }
    return false;
}

/**
 * Returns true if the free_session already exists in free_session_list, false otherwise
*/
static bool session_exists(qd_pn_free_link_session_list_t *free_list, pn_session_t *free_session)
{
    qd_pn_free_link_session_t *free_item = DEQ_HEAD(*free_list);
    while(free_item) {
        if (free_item->pn_session == free_session)
            return true;
        free_item = DEQ_NEXT(free_item);
    }
    return false;
}

static void add_session_to_free_list(qd_pn_free_link_session_list_t *free_link_session_list, pn_session_t *ssn)
{
    if (!session_exists(free_link_session_list, ssn)) {
        qd_pn_free_link_session_t *to_free = new_qd_pn_free_link_session_t();
        DEQ_ITEM_INIT(to_free);
        to_free->pn_session = ssn;
        to_free->pn_link = 0;
        DEQ_INSERT_TAIL(*free_link_session_list, to_free);
    }
}

static void add_link_to_free_list(qd_pn_free_link_session_list_t *free_link_session_list, pn_link_t *pn_link)
{
    if (!link_exists(free_link_session_list, pn_link)) {
        qd_pn_free_link_session_t *to_free = new_qd_pn_free_link_session_t();
        DEQ_ITEM_INIT(to_free);
        to_free->pn_link = pn_link;
        to_free->pn_session = 0;
        DEQ_INSERT_TAIL(*free_link_session_list, to_free);
    }

}


/*
 * The need for these lists may indicate a router bug, where the router is
 * using links/sessions after they are freed. Investigate and simplify if
 * possible.
*/
void qd_conn_event_batch_complete(qd_container_t *container, qd_connection_t *qd_conn, bool conn_closed)
{
    qd_pn_free_link_session_t *to_free = DEQ_HEAD(qd_conn->free_link_session_list);

    while(to_free) {
        if (!conn_closed) {
            if (to_free->pn_link) {
                qd_link_t *qd_link = (qd_link_t*) pn_link_get_context(to_free->pn_link);
                if (qd_link) {
                    qd_link->pn_link = 0;
                }
                pn_link_set_context(to_free->pn_link, 0);
                pn_link_free(to_free->pn_link);
            }
            if (to_free->pn_session)
                pn_session_free(to_free->pn_session);
        }
        DEQ_REMOVE_HEAD(qd_conn->free_link_session_list);
        free_qd_pn_free_link_session_t(to_free);
        to_free = DEQ_HEAD(qd_conn->free_link_session_list);

    }

    if (!conn_closed) {
        writable_handler(container, qd_conn->pn_conn, qd_conn);
    }
}


void qd_container_handle_event(qd_container_t *container, pn_event_t *event,
                               pn_connection_t *conn, qd_connection_t *qd_conn)
{
    pn_session_t  *ssn = NULL;
    pn_link_t     *pn_link = NULL;
    qd_link_t     *qd_link = NULL;
    pn_delivery_t *delivery = NULL;

    switch (pn_event_type(event)) {

    case PN_CONNECTION_REMOTE_OPEN :
        qd_connection_set_user(qd_conn);
        qd_conn->open_container = (void *)container;
        if (pn_connection_state(conn) & PN_LOCAL_UNINIT) {
            // This Open is an externally initiated connection
            // Let policy engine decide
            /* TODO aconway 2017-04-11: presently the policy test is run
             * in the current thread.
             *
             * If/when the policy test can run in another thread, the connection
             * can be stalled by saving the current pn_event_batch and passing it
             * to pn_proactor_done() when the policy check is complete. Note we
             * can't run the policy check as a deferred function on the current
             * connection since by stalling the current connection it will never be
             * run, so we need some other thread context to run it in.
             */
            qd_policy_amqp_open(qd_conn);
        } else {
            // This Open is in response to an internally initiated connection
            qd_policy_amqp_open_connector(qd_conn);
        }
        break;

    case PN_CONNECTION_REMOTE_CLOSE :
        if (qd_conn)
            qd_conn->closed = true;
        if (pn_connection_state(conn) == (PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED)) {
            close_links(container, conn, false);
            qd_session_cleanup(qd_conn);
            pn_connection_close(conn);
            qd_conn_event_batch_complete(container, qd_conn, true);
        } else if (pn_connection_state(conn) == (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED)) {
            close_links(container, conn, false);
            qd_session_cleanup(qd_conn);
            notify_closed(container, qd_conn, qd_connection_get_context(qd_conn));
            qd_conn_event_batch_complete(container, qd_conn, true);
        }
        break;

    case PN_SESSION_REMOTE_OPEN :
        if (!(pn_connection_state(conn) & PN_LOCAL_CLOSED)) {
            ssn = pn_event_session(event);
            if (pn_session_state(ssn) & PN_LOCAL_UNINIT) {
                // remote created new session
                assert(qd_session_from_pn(ssn) == 0);
                qd_session_t *qd_ssn = qd_session(ssn);
                if (!qd_ssn) {
                    pn_condition_t *cond = pn_session_condition(ssn);
                    pn_condition_set_name(cond, QD_AMQP_COND_INTERNAL_ERROR);
                    pn_condition_set_description(cond, "Insufficient memory");
                    pn_session_close(ssn);
                    break;
                }
                if (qd_conn->policy_settings) {
                    if (!qd_policy_approve_amqp_session(ssn, qd_conn)) {
                        qd_session_free(qd_ssn);
                        break;
                    }
                    qd_conn->n_sessions++;
                }
                qd_session_link_pn(qd_ssn, ssn);
                qd_policy_apply_session_settings(ssn, qd_conn);
                pn_session_open(ssn);
            }
        }
        break;

    case PN_SESSION_LOCAL_CLOSE :
        ssn = pn_event_session(event);
        for (int i = 0; i < QD_SSN_CLASS_COUNT; ++i) {
            if (ssn == qd_conn->pn_sessions[i]) {
                qd_conn->pn_sessions[i] = 0;
                break;
            }
        }
        pn_link = pn_link_head(conn, PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED);
        while (pn_link) {
            if (pn_link_session(pn_link) == ssn) {
                qd_link_t *qd_link = (qd_link_t*) pn_link_get_context(pn_link);
                if (qd_link)
                    qd_link->pn_link = 0;
            }
            pn_link = pn_link_next(pn_link, PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED);
        }

        if (pn_session_state(ssn) == (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED)) {
            qd_session_t *qd_ssn = qd_session_from_pn(ssn);
            qd_session_free(qd_ssn);
            add_session_to_free_list(&qd_conn->free_link_session_list, ssn);
        }
        break;

    case PN_SESSION_REMOTE_CLOSE :
        ssn = pn_event_session(event);
        for (int i = 0; i < QD_SSN_CLASS_COUNT; ++i) {
            if (ssn == qd_conn->pn_sessions[i]) {
                qd_conn->pn_sessions[i] = 0;
                break;
            }
        }
        if (!(pn_connection_state(conn) & PN_LOCAL_CLOSED)) {
            if (pn_session_state(ssn) == (PN_LOCAL_ACTIVE | PN_REMOTE_CLOSED)) {


                // remote has nuked our session.  Check for any links that were
                // left open and forcibly detach them, since no detaches will
                // arrive on this session.
                pn_connection_t *conn = pn_session_connection(ssn);

                //Sweep thru every pn_link in this connection and a matching session and zero out the
                // qd_link->pn_link reference. We do this in order to not miss any pn_links
                pn_link = pn_link_head(conn, 0);
                while (pn_link) {
                    if (pn_link_session(pn_link) == ssn) {
                        qd_link_t *qd_link = (qd_link_t*) pn_link_get_context(pn_link);

                        if ((pn_link_state(pn_link) == (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE))) {
                            if (qd_link) {
                                if (qd_conn->policy_settings) {
                                    if (qd_link->direction == QD_OUTGOING) {
                                        qd_conn->n_receivers--;
                                        assert(qd_conn->n_receivers >= 0);
                                    } else {
                                        qd_conn->n_senders--;
                                        assert(qd_conn->n_senders >= 0);
                                    }
                                }
                                qd_log(LOG_CONTAINER, QD_LOG_DEBUG,
                                       "Aborting link '%s' due to parent session end", pn_link_name(pn_link));
                                container->ntype->link_detach_handler(container->qd_router, qd_link, QD_LOST);
                            }
                        }

                        if (qd_link)
                            qd_link->pn_link = 0;
                    }
                    pn_link = pn_link_next(pn_link, 0);

                }
                if (qd_conn->policy_settings) {
                    qd_conn->n_sessions--;
                }

                pn_session_close(ssn);
            }
            else if (pn_session_state(ssn) == (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED)) {
                qd_session_t *qd_ssn = qd_session_from_pn(ssn);
                qd_session_free(qd_ssn);
                add_session_to_free_list(&qd_conn->free_link_session_list, ssn);
            }
        }
        break;

    case PN_LINK_REMOTE_OPEN :
        if (!(pn_connection_state(conn) & PN_LOCAL_CLOSED)) {
            pn_link = pn_event_link(event);
            if (pn_link_state(pn_link) & PN_LOCAL_UNINIT) {
                if (pn_link_is_sender(pn_link)) {
                    if (qd_conn->policy_settings) {
                        if (!qd_policy_approve_amqp_receiver_link(pn_link, qd_conn)) {
                            break;
                        }
                        qd_conn->n_receivers++;
                    }
                    setup_outgoing_link(container, pn_link);
                } else {
                    if (qd_conn->policy_settings) {
                        if (!qd_policy_approve_amqp_sender_link(pn_link, qd_conn)) {
                            break;
                        }
                        qd_conn->n_senders++;
                    }
                    setup_incoming_link(container, pn_link, qd_connection_max_message_size(qd_conn));
                }
            } else if (pn_link_state(pn_link) & PN_LOCAL_ACTIVE)
                handle_link_open(container, pn_link);
        }
        break;

    case PN_LINK_REMOTE_DETACH :
    case PN_LINK_REMOTE_CLOSE :
        if (!(pn_connection_state(conn) & PN_LOCAL_CLOSED)) {
            pn_link = pn_event_link(event);
            qd_link = (qd_link_t*) pn_link_get_context(pn_link);
            if (qd_link) {
                qd_detach_type_t dt = pn_event_type(event) == PN_LINK_REMOTE_CLOSE ? QD_CLOSED : QD_DETACHED;
                if (qd_link->pn_link == pn_link) {
                    pn_link_close(pn_link);
                }
                if (qd_conn->policy_counted && qd_conn->policy_settings) {
                    if (pn_link_is_sender(pn_link)) {
                        qd_conn->n_receivers--;
                        qd_log(LOG_CONTAINER, QD_LOG_DEBUG, "Closed receiver link %s. n_receivers: %d",
                               pn_link_name(pn_link), qd_conn->n_receivers);
                        assert (qd_conn->n_receivers >= 0);
                    } else {
                        qd_conn->n_senders--;
                        qd_log(LOG_CONTAINER, QD_LOG_DEBUG, "Closed sender link %s. n_senders: %d",
                               pn_link_name(pn_link), qd_conn->n_senders);
                        assert (qd_conn->n_senders >= 0);
                    }
                }

                if (pn_link_state(pn_link) & PN_LOCAL_CLOSED) {
                    add_link_to_free_list(&qd_conn->free_link_session_list, pn_link);
                }
                container->ntype->link_detach_handler(container->qd_router, qd_link, dt);
            } else {
                add_link_to_free_list(&qd_conn->free_link_session_list, pn_link);
            }
        }
        break;

    case PN_LINK_LOCAL_DETACH:
    case PN_LINK_LOCAL_CLOSE:
        pn_link = pn_event_link(event);
        if (pn_link_state(pn_link) == (PN_LOCAL_CLOSED | PN_REMOTE_CLOSED)) {
            add_link_to_free_list(&qd_conn->free_link_session_list, pn_link);
        }
        break;


    case PN_LINK_FLOW :
        pn_link = pn_event_link(event);
        qd_link = (qd_link_t*) pn_link_get_context(pn_link);
        if (qd_link)
            container->ntype->link_flow_handler(container->qd_router, qd_link);
        break;

    case PN_DELIVERY :
        delivery = pn_event_delivery(event);
        pn_link  = pn_event_link(event);

        if (pn_delivery_readable(delivery))
            do_receive(container, pn_link, delivery);

        if (pn_delivery_updated(delivery) || pn_delivery_settled(delivery)) {
            do_updated(container, delivery);
            pn_delivery_clear(delivery);
        }
        break;

    case PN_CONNECTION_WAKE:
        if (!qd_conn->closed)
            writable_handler(container, conn, qd_conn);
        break;

    case PN_TRANSPORT_CLOSED:
        close_handler(container, conn, qd_conn);
        break;

    default:
        break;
    }
}


qd_container_t *qd_container(qd_router_t *router, const qd_node_type_t *ntype)
{
    qd_container_t *container = NEW(qd_container_t);

    ZERO(container);

    assert(router);
    container->qd_router     = router;
    container->ntype         = ntype;
    sys_mutex_init(&container->lock);
    DEQ_INIT(container->links);

    qd_log(LOG_CONTAINER, QD_LOG_DEBUG, "Container Initialized");
    return container;
}


void qd_container_free(qd_container_t *container)
{
    if (!container) return;
    sys_mutex_lock(&container->lock);
    qd_link_t *link = DEQ_HEAD(container->links);
    while (link) {
        DEQ_REMOVE_HEAD(container->links);

        sys_mutex_unlock(&container->lock);
        cleanup_link(link);
        free_qd_link_t(link);
        sys_mutex_lock(&container->lock);

        link = DEQ_HEAD(container->links);
    }
    sys_mutex_unlock(&container->lock);

    sys_mutex_free(&container->lock);
    free(container);
}


qd_link_t *qd_link(qd_connection_t *conn, qd_direction_t dir, const char* name, qd_session_class_t ssn_class)
{
    const qd_server_config_t * cf = qd_connection_config(conn);

    pn_session_t *pn_ssn = conn->pn_sessions[ssn_class];
    if (!pn_ssn) {
        pn_ssn = pn_session(qd_connection_pn(conn));
        if (!pn_ssn) {
            return NULL;
        }
        qd_session_t *qd_ssn = qd_session(pn_ssn);
        if (!qd_ssn) {
            pn_session_free(pn_ssn);
            return NULL;  // PN_LOCAL_CLOSE event will free pn_ssn
        }

        conn->pn_sessions[ssn_class] = pn_ssn;
        pn_session_set_incoming_capacity(pn_ssn, cf->incoming_capacity);
        pn_session_open(pn_ssn);
    }

    qd_link_t *link = new_qd_link_t();
    if (!link) {
        return NULL;  // ok to keep qd_session around - will reuse & free on conn close
    }
    ZERO(link);

    sys_mutex_lock(&amqp_adaptor.container->lock);
    DEQ_INSERT_TAIL(amqp_adaptor.container->links, link);
    sys_mutex_unlock(&amqp_adaptor.container->lock);

    link->pn_sess = pn_ssn;

    if (dir == QD_OUTGOING)
        link->pn_link = pn_sender(link->pn_sess, name);
    else
        link->pn_link = pn_receiver(link->pn_sess, name);

    link->direction  = dir;
    link->remote_snd_settle_mode = pn_link_remote_snd_settle_mode(link->pn_link);

    pn_link_set_context(link->pn_link, link);

    return link;
}


void qd_link_free(qd_link_t *link)
{
    if (!link) return;

    sys_mutex_lock(&amqp_adaptor.container->lock);
    DEQ_REMOVE(amqp_adaptor.container->links, link);
    sys_mutex_unlock(&amqp_adaptor.container->lock);

    amqp_adaptor.container->ntype->link_abandoned_deliveries_handler(amqp_adaptor.container->qd_router, link);

    cleanup_link(link);
    free_qd_link_t(link);
}


qd_link_ref_list_t *qd_link_get_ref_list(qd_link_t *link)
{
    return &link->ref_list;
}


void qd_link_set_context(qd_link_t *link, void *context)
{
    link->context = context;
}


void *qd_link_get_context(const qd_link_t *link)
{
    return link->context;
}

pn_link_t *qd_link_pn(const qd_link_t *link)
{
    return link->pn_link;
}

pn_session_t *qd_link_pn_session(qd_link_t *link)
{
    return link->pn_sess;
}


bool qd_link_is_q2_limit_unbounded(const qd_link_t *link)
{
    return link->q2_limit_unbounded;
}


void qd_link_set_q2_limit_unbounded(qd_link_t *link, bool q2_limit_unbounded)
{
    link->q2_limit_unbounded = q2_limit_unbounded;
}


qd_direction_t qd_link_direction(const qd_link_t *link)
{
    return link->direction;
}

pn_snd_settle_mode_t qd_link_remote_snd_settle_mode(const qd_link_t *link)
{
    return link->remote_snd_settle_mode;
}

qd_connection_t *qd_link_connection(const qd_link_t *link)
{
    if (!link || !link->pn_link)
        return 0;

    pn_session_t *sess = pn_link_session(link->pn_link);
    if (!sess)
        return 0;

    pn_connection_t *conn = pn_session_connection(sess);
    if (!conn)
        return 0;

    qd_connection_t *ctx = pn_connection_get_context(conn);
    if (!ctx || !ctx->opened)
        return 0;

    return ctx;
}


pn_terminus_t *qd_link_source(qd_link_t *link)
{
    return pn_link_source(link->pn_link);
}


pn_terminus_t *qd_link_target(qd_link_t *link)
{
    return pn_link_target(link->pn_link);
}


pn_terminus_t *qd_link_remote_source(qd_link_t *link)
{
    return pn_link_remote_source(link->pn_link);
}


pn_terminus_t *qd_link_remote_target(qd_link_t *link)
{
    return pn_link_remote_target(link->pn_link);
}


void qd_link_close(qd_link_t *link)
{
    if (link->pn_link)
        pn_link_close(link->pn_link);

}


void qd_link_detach(qd_link_t *link)
{
    if (link->pn_link) {
        pn_link_detach(link->pn_link);
        pn_link_close(link->pn_link);
    }
}


/** sending link has entered Q3 flow control */
void qd_link_q3_block(qd_link_t *link)
{
    assert(link);
    if (!link->q3_blocked && link->pn_sess) {
        qd_session_t *qd_ssn = qd_session_from_pn(link->pn_sess);
        assert(qd_ssn);
        link->q3_blocked = true;
        DEQ_INSERT_TAIL_N(Q3, qd_ssn->q3_blocked_links, link);
    }
}


void qd_link_q3_unblock(qd_link_t *link)
{
    assert(link);
    if (link->q3_blocked) {
        qd_session_t *qd_ssn = qd_session_from_pn(link->pn_sess);
        assert(qd_ssn);
        DEQ_REMOVE_N(Q3, qd_ssn->q3_blocked_links, link);
        link->q3_blocked = false;
    }
}


uint64_t qd_link_link_id(const qd_link_t *link)
{
    return link->link_id;
}


void qd_link_set_link_id(qd_link_t *link, uint64_t link_id)
{
    link->link_id = link_id;
}


qd_session_t *qd_session(pn_session_t *pn_ssn)
{
    assert(pn_ssn);
    qd_session_t *qd_ssn = qd_session_from_pn(pn_ssn);
    if (!qd_ssn) {
        qd_ssn = new_qd_session_t();
        if (qd_ssn) {
            ZERO(qd_ssn);
            qd_ssn->pn_sess = pn_ssn;
            DEQ_INIT(qd_ssn->q3_blocked_links);
            qd_session_link_pn(qd_ssn, pn_ssn);
        }
    }
    return qd_ssn;
}


void qd_session_free(qd_session_t *qd_ssn)
{
    if (qd_ssn) {
        qd_link_t *link = DEQ_HEAD(qd_ssn->q3_blocked_links);
        while (link) {
            qd_link_q3_unblock(link);  // removes link from list
            link = DEQ_HEAD(qd_ssn->q3_blocked_links);
        }
        if (qd_ssn->pn_sess) {
            qd_session_unlink_pn(qd_ssn, qd_ssn->pn_sess);
        }
        free_qd_session_t(qd_ssn);
    }
}


qd_link_list_t *qd_session_q3_blocked_links(qd_session_t *qd_ssn)
{
    return qd_ssn ? &qd_ssn->q3_blocked_links : 0;
}


bool qd_session_is_q3_blocked(const qd_session_t *qd_ssn)
{
    return qd_ssn && !DEQ_IS_EMPTY(qd_ssn->q3_blocked_links);
}



/** release all qd_session_t instances for the connection
 * called prior to releasing the qd_connection_t
 */
void qd_session_cleanup(qd_connection_t *qd_conn)
{
    pn_connection_t *pn_conn = (qd_conn) ? qd_conn->pn_conn : 0;
    if (!pn_conn)
        return;

    pn_session_t *pn_ssn = pn_session_head(pn_conn, 0);
    while (pn_ssn) {
        qd_session_t *qd_ssn = qd_session_from_pn(pn_ssn);
        qd_session_free(qd_ssn);
        pn_ssn = pn_session_next(pn_ssn, 0);
    }
}


void qd_link_set_incoming_msg(qd_link_t *link, qd_message_t *msg)
{
    if (msg) {
        qd_alloc_set_safe_ptr(&link->incoming_msg, msg);
    } else {
        qd_nullify_safe_ptr(&link->incoming_msg);
    }
}

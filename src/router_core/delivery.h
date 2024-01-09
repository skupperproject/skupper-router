#ifndef __delivery_h__
#define __delivery_h__ 1

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

#include "router_core_private.h"

#define QDR_DELIVERY_TAG_MAX 32

typedef enum {
    QDR_DELIVERY_NOWHERE = 0,
    QDR_DELIVERY_IN_UNDELIVERED,
    QDR_DELIVERY_IN_UNSETTLED,
    QDR_DELIVERY_IN_SETTLED
} qdr_delivery_where_t;


struct qdr_delivery_t {
    DEQ_LINKS(qdr_delivery_t);
    void                   *context;
    sys_atomic_t            ref_count;
    qdr_link_t_sp           link_sp;       /// Safe pointer to the link
    qdr_link_t_sp           original_link_sp; /// Safe pointer to original link if this delivery was moved (via initial-delivery)
    qdr_delivery_t         *peer;          /// Use this peer if the delivery has one and only one peer.
    qdr_delivery_ref_t     *next_peer_ref;
    qdr_delivery_ref_t     *cutthrough_list_ref;
    qd_message_t           *msg;
    qd_iterator_t          *to_addr;
    qd_iterator_t          *origin;
    sys_mutex_t             dispo_lock;          ///< lock disposition and local_state fields
    uint64_t                disposition;         ///< local disposition, will be pushed to remote endpoint
    uint64_t                remote_disposition;  ///< disposition as set by remote endpoint
    uint64_t                mcast_disposition;   ///< temporary terminal disposition while multicast fwding
    qd_delivery_state_t    *remote_state;        ///< outcome-specific data read from remote endpoint
    qd_delivery_state_t    *local_state;         ///< outcome-specific data to send to remote endpoint
    uint32_t                ingress_time;
    qdr_delivery_where_t    where;
    uint8_t                 tag[QDR_DELIVERY_TAG_MAX];
    int                     tag_length;
    qd_bitmask_t           *link_exclusion;
    qdr_address_t          *tracking_addr;
    int                     tracking_addr_bit;
    int                     ingress_index;
    qdr_link_work_t        *link_work;         ///< Delivery work item for this delivery
    qdr_subscription_ref_list_t subscriptions;
    qdr_delivery_ref_list_t peers;                 /// Use this list if the delivery has more than one peer.
    qdr_link_t             *chosen_link;           /// Pointer to link most recently chosen for forwarding.  This pointer shall never be dereferenced, only compared.
    int                     chosen_neighbor;       /// Mask-bit of the most recently chosen neighbor router.
    qd_bitmask_t           *invalidated_neighbors; /// Bitmask of all neighbor routers that have been invalidated in the lifetime of this delivery
    qdr_link_ref_list_t     invalidated_links;     /// List of links that have been invalidated.  These link pointer shall never be dereferenced, only compared.
    uint32_t                delivery_id;           /// id for logging
    uint64_t                link_id;               /// id for logging
    uint64_t                conn_id;               /// id for logging
    bool                    settled;
    bool                    presettled; /// Proton does not have a notion of pre-settled. This flag is introduced in Dispatch and should exclusively be used only to update management counters like presettled delivery counts on links etc. This flag DOES NOT represent the remote settlement state of the delivery.
    bool                    multicast;         /// True if this delivery is targeted for a multicast address.
    bool                    via_edge;          /// True if this delivery arrived via an edge-connection.
    bool                    stuck;             /// True if this delivery was counted as stuck.
    bool                    reforwarded;       /// True if this delivery was released and re-forwarded.
    bool                    abort_outbound;    /// A re-forwarded streaming delivery needs to be aborted outbound
    bool                    in_message_activation;
};

ALLOC_DECLARE(qdr_delivery_t);

/** Delivery Id for logging - thread safe
 */
extern sys_atomic_t global_delivery_id;
static inline uint32_t next_delivery_id(void)
{
    return sys_atomic_inc(&global_delivery_id);
}

// Common log line prefix
#define DLV_FMT       "[C%"PRIu64"][L%"PRIu64"][D%"PRIu32"]"
#define DLV_ARGS(dlv) dlv->conn_id, dlv->link_id, dlv->delivery_id
#define DLV_ARGS_MAX  75

///////////////////////////////////////////////////////////////////////////////
//                               Delivery API
///////////////////////////////////////////////////////////////////////////////


bool qdr_delivery_receive_complete(const qdr_delivery_t *delivery);
bool qdr_delivery_is_unicast_cutthrough(const qdr_delivery_t *delivery);
bool qdr_delivery_send_complete(const qdr_delivery_t *delivery);
bool qdr_delivery_oversize(const qdr_delivery_t *delivery);

void qdr_delivery_set_context(qdr_delivery_t *delivery, void *context);
void *qdr_delivery_get_context(const qdr_delivery_t *delivery);

void qdr_delivery_tag(const qdr_delivery_t *delivery, const char **tag, int *length);
bool qdr_delivery_tag_sent(const qdr_delivery_t *delivery);
void qdr_delivery_set_tag_sent(const qdr_delivery_t *delivery, bool tag_sent);

// note: access to _local_ endpoint disposition (not remote endpoint)
uint64_t qdr_delivery_disposition(const qdr_delivery_t *delivery);
void qdr_delivery_set_disposition(qdr_delivery_t *delivery, uint64_t disposition);

void qdr_delivery_set_aborted(const qdr_delivery_t *delivery);
bool qdr_delivery_is_aborted(const qdr_delivery_t *delivery);

static inline bool qdr_delivery_is_abort_outbound(const qdr_delivery_t *delivery) { return delivery->abort_outbound; }

qd_message_t *qdr_delivery_message(const qdr_delivery_t *delivery);
qdr_link_t *qdr_delivery_link(const qdr_delivery_t *delivery);
bool qdr_delivery_presettled(const qdr_delivery_t *delivery);

void qdr_delivery_incref(qdr_delivery_t *delivery, const char *label);
bool qdr_delivery_move_delivery_state_CT(qdr_delivery_t *dlv, qdr_delivery_t *peer);

//
// I/O thread only functions
//

/* release dlv and possibly schedule its deletion on the core thread */
void qdr_delivery_decref(qdr_core_t *core, qdr_delivery_t *delivery, const char *label);

/** Set the presettled flag on the delivery to true if it is not already true.
 * The presettled flag can only go from false to true and not vice versa.
 * This function should only be called when the delivery has been discarded and receive_complete flag is true in which case there
 * will be no thread contention.
**/
void qdr_delivery_set_presettled(qdr_delivery_t *delivery);

/* handles delivery disposition and settlement changes from the remote end of
 * the link, and schedules Core thread */
void qdr_delivery_remote_state_updated(qdr_core_t *core, qdr_delivery_t *delivery, uint64_t disp,
                                       bool settled, qd_delivery_state_t *dstate, bool ref_given);

/* invoked when incoming message data arrives - schedule core thread */
qdr_delivery_t *qdr_delivery_continue(qdr_core_t *core, qdr_delivery_t *delivery, bool settled);


//
// CORE thread only functions
//


/* update settlement and/or disposition and schedule I/O processing */
void qdr_delivery_release_CT(qdr_core_t *core, qdr_delivery_t *delivery) TA_REQ(core_thread_capability);
void qdr_delivery_reject_CT(qdr_core_t *core, qdr_delivery_t *delivery, qdr_error_t *error) TA_REQ(core_thread_capability);
void qdr_delivery_failed_CT(qdr_core_t *core, qdr_delivery_t *delivery) TA_REQ(core_thread_capability);
bool qdr_delivery_settled_CT(qdr_core_t *core, qdr_delivery_t *delivery) TA_REQ(core_thread_capability);

/* add dlv to links list of updated deliveries and schedule I/O thread processing */
void qdr_delivery_push_CT(qdr_core_t *core, qdr_delivery_t *dlv) TA_REQ(core_thread_capability);

/* optimized decref for core thread */
void qdr_delivery_decref_CT(qdr_core_t *core, qdr_delivery_t *delivery, const char *label) TA_REQ(core_thread_capability);

/* peer delivery list management*/
void qdr_delivery_link_peers_CT(qdr_delivery_t *in_dlv, qdr_delivery_t *out_dlv) TA_REQ(core_thread_capability);
void qdr_delivery_unlink_peers_CT(qdr_core_t *core, qdr_delivery_t *dlv, qdr_delivery_t *peer) TA_REQ(core_thread_capability);

/* peer iterator - warning: not reentrant! */
qdr_delivery_t *qdr_delivery_first_peer_CT(qdr_delivery_t *dlv) TA_REQ(core_thread_capability);
qdr_delivery_t *qdr_delivery_next_peer_CT(qdr_delivery_t *dlv) TA_REQ(core_thread_capability);

/* schedules all peer deliveries with work for I/O processing */
void qdr_delivery_continue_peers_CT(qdr_core_t *core, qdr_delivery_t *in_dlv, bool more) TA_REQ(core_thread_capability);

/* update the links counters with respect to its delivery */
void qdr_delivery_increment_counters_CT(qdr_core_t *core, qdr_delivery_t *delivery) TA_REQ(core_thread_capability);

/**
 * multicast delivery state and settlement management
 */

// remote updated disposition/settlement for incoming delivery
void qdr_delivery_mcast_inbound_update_CT(qdr_core_t *core, qdr_delivery_t *in_dlv,
                                          uint64_t new_disp, bool settled) TA_REQ(core_thread_capability);
// remote update disposition/settlement for outgoing delivery
void qdr_delivery_mcast_outbound_update_CT(qdr_core_t *core, qdr_delivery_t *in_dlv,
                                           qdr_delivery_t *out_peer,
                                           uint64_t new_disp, bool settled) TA_REQ(core_thread_capability);
// number of unsettled peer (outbound) deliveries for in_dlv
int qdr_delivery_peer_count_CT(const qdr_delivery_t *in_dlv) TA_REQ(core_thread_capability);

// Return the number of outbound paths that have been invalidated due to releases
int qdr_delivery_invalidated_path_count_CT(const qdr_delivery_t *dlv) TA_REQ(core_thread_capability);


#endif // __delivery_h__

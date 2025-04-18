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

#include "connection_manager.h"

#include "core_events.h"
#include "router_core_private.h"
#include "qpid/dispatch/vanflow.h"

#include <inttypes.h>

//
// This is the Connection Manager component of the Edge Router module.
//
// The Connection Manager is responsible for keeping track of all of the
// edge connections to Interior routers and choosing one to be the active
// edge connection.  An edge router may maintain multiple "edge-connection"
// connections to different Interior routers.  Only one of those connections
// will be designated as active and carry edge traffic.  This component
// identifies the active edge connection and generates outbound core events
// to notify other interested parties:
//
//     QDRC_EVENT_CONN_EDGE_ESTABLISHED
//     QDRC_EVENT_CONN_EDGE_LOST
//

struct qcm_edge_conn_mgr_t {
    qdr_core_t                *core;
    qdrc_event_subscription_t *event_sub;
    qdr_connection_t          *active_edge_connection;
};


static qdr_edge_peer_t *qdr_find_edge_peer_CT(qdr_core_t *core, const char *container_id)
{
    qdr_edge_peer_t *edge_peer = DEQ_HEAD(core->edge_peers);

    while (!!edge_peer) {
        if (strcmp(container_id, edge_peer->identity) == 0) {
            break;
        }
        edge_peer = DEQ_NEXT(edge_peer);
    }

    return edge_peer;
}


static void qdr_inter_edge_peer_activate_CT(qdr_core_t *core, qdr_edge_peer_t *edge_peer)
{
    edge_peer->router_addr = qdr_add_local_address_CT(core, QD_ITER_HASH_PREFIX_EDGE_SUMMARY, edge_peer->identity, QD_TREATMENT_ANYCAST_CLOSEST);
    qdr_link_t *link = qdr_create_link_CT(core, edge_peer->primary_conn, QD_LINK_INTER_EDGE, QD_OUTGOING,
                                          qdr_terminus_inter_edge(), qdr_terminus_inter_edge(), QD_SSN_ENDPOINT, QDR_DEFAULT_PRIORITY);
    qdr_core_bind_address_link_CT(core, edge_peer->router_addr, link);
}


static void qdr_inter_edge_connection_setup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    qdr_edge_peer_t *edge_peer = qdr_find_edge_peer_CT(core, conn->connection_info->container);

    if (!edge_peer) {
        //
        // This is the first connection we've seen from this edge-peer.  Create a new record for it.
        //
        edge_peer = NEW(qdr_edge_peer_t);
        ZERO(edge_peer);
        edge_peer->identity = strdup(conn->connection_info->container);
        DEQ_INSERT_TAIL(core->edge_peers, edge_peer);
        qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Edge peer detected: %s", edge_peer->identity);
        qd_iterator_add_peer_edge(edge_peer->identity);
    }

    //
    // Add this connection to the list of connections associated with this edge-peer.
    //
    qdr_add_connection_ref(&edge_peer->connections, conn);
    conn->edge_peer = edge_peer;

    //
    // If there is no primary connection for this edge-peer, use this one and activate.
    //
    if (edge_peer->primary_conn == 0) {
        edge_peer->primary_conn = conn;
        qdr_inter_edge_peer_activate_CT(core, edge_peer);
        qdrc_event_conn_raise(core, QDRC_EVENT_CONN_MESH_PEER_ESTABLISHED, conn);
    }
}


static void qdr_inter_edge_connection_cleanup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    qdr_edge_peer_t *edge_peer = conn->edge_peer;

    if (!!edge_peer) {
        qdr_del_connection_ref(&edge_peer->connections, conn);
        if (DEQ_SIZE(edge_peer->connections) > 0) {
            if (edge_peer->primary_conn == conn) {
                qdrc_event_conn_raise(core, QDRC_EVENT_CONN_MESH_PEER_LOST, conn);
                edge_peer->primary_conn = DEQ_HEAD(edge_peer->connections)->conn;
                qdr_inter_edge_peer_activate_CT(core, edge_peer);
                qdrc_event_conn_raise(core, QDRC_EVENT_CONN_MESH_PEER_ESTABLISHED, edge_peer->primary_conn);
            }
        } else {
            qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Edge peer lost: %s", edge_peer->identity);
            qdrc_event_conn_raise(core, QDRC_EVENT_CONN_MESH_PEER_LOST, conn);
            qd_iterator_del_peer_edge(edge_peer->identity);
            DEQ_REMOVE(core->edge_peers, edge_peer);
            edge_peer->router_addr->ref_count--;
            qdr_check_addr_CT(core, edge_peer->router_addr);
            free(edge_peer->identity);
            free(edge_peer);
        }
    }
}


static void on_conn_event(void *context, qdrc_event_t event, qdr_connection_t *conn)
{
    qcm_edge_conn_mgr_t *cm = (qcm_edge_conn_mgr_t*) context;

    switch (event) {
    case QDRC_EVENT_CONN_OPENED :
        if (conn->role == QDR_ROLE_EDGE_CONNECTION) {
            if (cm->active_edge_connection == 0) {
                qd_log(LOG_ROUTER_CORE, QD_LOG_INFO,
                       "Edge connection (id=%" PRIu64 ") to interior established", conn->identity);
                cm->active_edge_connection       = conn;
                cm->core->active_edge_connection = conn;
                qdrc_event_conn_raise(cm->core, QDRC_EVENT_CONN_EDGE_ESTABLISHED, conn);
            } else {
                // There is already an active edge connection. Check if the new connection has a higher TLS ordinal and
                // if so it takes precedence.
                qdr_connection_t *old_conn = cm->active_edge_connection;
                if (conn->connection_info->tls
                    && conn->connection_info->tls_ordinal > old_conn->connection_info->tls_ordinal) {
                    qd_log(LOG_ROUTER_CORE, QD_LOG_INFO,
                           "Upgrading Edge connection [C%"PRIu64"] to [C%"PRIu64"] due to certificate rotation",
                           old_conn->identity, conn->identity);
                    cm->active_edge_connection       = conn;
                    cm->core->active_edge_connection = conn;
                    qdrc_event_conn_raise(cm->core, QDRC_EVENT_CONN_EDGE_ESTABLISHED, conn);
                }
            }
        }

        if (conn->role == QDR_ROLE_INTER_EDGE) {
            //
            // This is an inter-edge connection.  Maintain a list of inter-edge connections indexed by the remote
            // container-id.  Handle the case where there is more than one connection to the same peer edge.
            //
            qdr_inter_edge_connection_setup_CT(cm->core, conn);
        }
        break;

    case QDRC_EVENT_CONN_CLOSED :
        if (cm->active_edge_connection == conn) {
            qdrc_event_conn_raise(cm->core, QDRC_EVENT_CONN_EDGE_LOST, conn);

            // See if there is another edge connection that we can fail-over to. Caveat: if TLS is configured select the
            // fail-over with the largest tls_ordinal value since that connection will have the newest (longest valid)
            // TLS credentials.
            const bool        need_tls  = conn->connection_info->tls;
            qdr_connection_t *c_ptr     = DEQ_HEAD(cm->core->open_connections);
            qdr_connection_t *alternate = 0;
            while (c_ptr) {
                if (c_ptr == conn || c_ptr->role != QDR_ROLE_EDGE_CONNECTION
                    || (need_tls && !c_ptr->connection_info->tls)) {
                    c_ptr = DEQ_NEXT(c_ptr);
                    continue;
                }

                if (!need_tls) {
                    // if TLS is not used simply take first candidate
                    alternate = c_ptr;
                    break;
                }

                if (!alternate || (alternate && c_ptr->connection_info->tls_ordinal > alternate->connection_info->tls_ordinal)) {
                    alternate = c_ptr;
                }
                c_ptr = DEQ_NEXT(c_ptr);
            }

            if (alternate) {
                qd_log(LOG_ROUTER_CORE, QD_LOG_INFO,
                       "Edge connection (id=%" PRIu64 ") to interior lost, activating alternate id=%" PRIu64 "",
                       conn->identity, alternate->identity);
                cm->active_edge_connection = alternate;
                cm->core->active_edge_connection = alternate;
                qdrc_event_conn_raise(cm->core, QDRC_EVENT_CONN_EDGE_ESTABLISHED, alternate);
            } else {
                qd_log(LOG_ROUTER_CORE, QD_LOG_INFO,
                       "Edge connection (id=%" PRIu64 ") to interior lost, no alternate connection available",
                       conn->identity);
                cm->active_edge_connection = 0;
            }
        }

        //
        // If this is an inter-edge connection, clean up references to this connection in the list
        // of peer-edge routers.
        //
        if (conn->role == QDR_ROLE_INTER_EDGE) {
            qdr_inter_edge_connection_cleanup_CT(cm->core, conn);
        }
        break;

    default:
        assert(false);
        break;
    }
}


qcm_edge_conn_mgr_t *qcm_edge_conn_mgr(qdr_core_t *core)
{
    qcm_edge_conn_mgr_t *cm = NEW(qcm_edge_conn_mgr_t);

    cm->core = core;
    cm->event_sub = qdrc_event_subscribe_CT(core,
                                            QDRC_EVENT_CONN_OPENED | QDRC_EVENT_CONN_CLOSED,
                                            on_conn_event,
                                            0,
                                            0,
                                            0,
                                            cm);
    cm->active_edge_connection = 0;

    return cm;
}


void qcm_edge_conn_mgr_final(qcm_edge_conn_mgr_t *cm)
{
    qdrc_event_unsubscribe_CT(cm->core, cm->event_sub);
    free(cm);
}


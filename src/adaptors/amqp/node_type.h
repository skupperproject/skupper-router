#ifndef __amqp_node_type_h__
#define __amqp_node_type_h__ 1
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


typedef struct qd_node_type_t qd_node_type_t;
typedef struct qd_router_t    qd_router_t;

typedef bool (*qd_container_delivery_handler_t)                  (qd_router_t *, qd_link_t *link);
typedef void (*qd_container_disposition_handler_t)               (qd_router_t *, qd_link_t *link, pn_delivery_t *pnd);
typedef int  (*qd_container_link_handler_t)                      (qd_router_t *, qd_link_t *link);
typedef int  (*qd_container_link_detach_handler_t)               (qd_router_t *, qd_link_t *link);
typedef void (*qd_container_link_closed_handler_t)               (qd_router_t *, qd_link_t *link, bool forced);
typedef void (*qd_container_node_handler_t)                      (qd_router_t *);
typedef int  (*qd_container_conn_handler_t)                      (qd_router_t *, qd_connection_t *conn, void *context);

/**
 * A set  of Node handlers for deliveries, links and container events.
 */
struct qd_node_type_t {
    char *type_name;
    int   allow_dynamic_creation;

    /** @name Node-Instance Handlers
     * @{
     */

    /** Invoked when a new or existing received delivery is available for processing. */
    qd_container_delivery_handler_t rx_handler;

    /** Invoked when an existing delivery changes disposition or settlement state. */
    qd_container_disposition_handler_t disp_handler;

    /** Invoked when an attach for a new incoming link is received. */
    qd_container_link_handler_t incoming_handler;

    /** Invoked when an attach for a new outgoing link is received. */
    qd_container_link_handler_t outgoing_handler;

    /** Invoked when an activated connection is available for writing. */
    qd_container_conn_handler_t writable_handler;

    /** Invoked when link detached is received. */
    qd_container_link_detach_handler_t link_detach_handler;

    /** The last callback issued for the given qd_link_t. The adaptor must clean up all state related to the qd_link_t
     * as it will be freed on return from this call. The forced flag is set to true if the link is being forced closed
     * due to the parent connection/session closing or on shutdown.
     */
    qd_container_link_closed_handler_t link_closed_handler;

    ///@}

    /** Invoked when a link we created was opened by the peer */
    qd_container_link_handler_t link_attach_handler;

    /** Invoked when a link receives a flow event */
    qd_container_link_handler_t link_flow_handler;

    /** @name Node-Type Handlers
     * @{
     */

    /** Invoked when a new instance of the node-type is created. */
    qd_container_node_handler_t  node_created_handler;

    /** Invoked when an instance of the node type is destroyed. */
    qd_container_node_handler_t  node_destroyed_handler;

    /** Invoked when an incoming connection (via listener) is opened. */
    qd_container_conn_handler_t  inbound_conn_opened_handler;

    /** Invoked when an outgoing connection (via connector) is opened. */
    qd_container_conn_handler_t  outbound_conn_opened_handler;

    /** Invoked when a connection is closed. */
    qd_container_conn_handler_t  conn_closed_handler;
};


#endif

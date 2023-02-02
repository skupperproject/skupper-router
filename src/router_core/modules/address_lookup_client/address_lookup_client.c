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

#include "core_attach_address_lookup.h"
#include "core_client_api.h"
#include "core_events.h"
#include "module.h"
#include "router_core_private.h"

#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/discriminator.h"

#include <stdio.h>

typedef struct qcm_lookup_client_t {
    qdr_core_t *core;
} qcm_lookup_client_t;


/**
 * Generate a temporary routable address for a destination connected to this
 * router node. Caller must free() return value when done.
 */
static char *qdr_generate_temp_addr(qdr_core_t *core)
{
    static const char *edge_template     = "amqp:/_edge/%s/temp.%s";
    static const char *edge_template_van = "amqp:/_edge/%s/%s/temp.%s";
    static const char *topo_template     = "amqp:/_topo/%s/%s/temp.%s";
    static const char *topo_template_van = "amqp:/_topo/%s/%s/%s/temp.%s";
    const size_t       max_template      = 20;  // printable chars
    char discriminator[QD_DISCRIMINATOR_SIZE];

    qd_generate_discriminator(discriminator);
    size_t len = max_template + QD_DISCRIMINATOR_SIZE + 1
        + strlen(core->router_id) + strlen(core->router_area)
        + (!!core->van_id ? strlen(core->van_id) : 0);

    int rc;
    char *buffer = qd_malloc(len);
    if (core->router_mode == QD_ROUTER_MODE_EDGE) {
        if (!!core->van_id) {
            rc = snprintf(buffer, len, edge_template_van, core->router_id, core->van_id, discriminator);
        } else {
            rc = snprintf(buffer, len, edge_template, core->router_id, discriminator);
        }
    } else {
        if (!!core->van_id) {
            rc = snprintf(buffer, len, topo_template_van, core->router_area, core->router_id, core->van_id, discriminator);
        } else {
            rc = snprintf(buffer, len, topo_template, core->router_area, core->router_id, discriminator);
        }
    }
    (void)rc; assert(rc < len);
    return buffer;
}


/**
 * Generate a temporary mobile address for a producer connected to this
 * router node. Caller must free() return value when done.
 */
static char *qdr_generate_mobile_addr(qdr_core_t *core)
{
    static const char mobile_template[] = "amqp:/_$temp.%s";
    const size_t      max_template      = 13; // printable chars
    char discriminator[QD_DISCRIMINATOR_SIZE];

    qd_generate_discriminator(discriminator);
    size_t len = max_template + QD_DISCRIMINATOR_SIZE + 1;
    char *buffer = qd_malloc(len);
    int rc = snprintf(buffer, len, mobile_template, discriminator);
    (void)rc; assert(rc < len);
    return buffer;
}


/**
 * qdr_lookup_terminus_address_CT
 *
 * Lookup a terminus address in the route table and possibly create a new address
 * if no match is found.
 *
 * @param core Pointer to the core object
 * @param dir Direction of the link for the terminus
 * @param conn The connection over which the terminus was attached
 * @param terminus The terminus containing the addressing information to be looked up
 * @param create_if_not_found Iff true, return a pointer to a newly created address record
 * @param accept_dynamic Iff true, honor the dynamic flag by creating a dynamic address
 * @param [out] unavailable True iff this address is blocked as unavailable
 * @param [out] core_endpoint True iff this address is bound to a core-internal endpoint
 * @return Pointer to an address record or 0 if none is found
 */
static qdr_address_t *qdr_lookup_terminus_address_CT(qdr_core_t       *core,
                                                     qd_direction_t    dir,
                                                     qdr_connection_t *conn,
                                                     qdr_terminus_t   *terminus,
                                                     bool              create_if_not_found,
                                                     bool              accept_dynamic,
                                                     bool             *unavailable,
                                                     bool             *core_endpoint)
{
    qdr_address_t *addr = 0;

    *unavailable   = false;
    *core_endpoint = false;

    if (qdr_terminus_is_dynamic(terminus)) {
        //
        // If we are permitted to generate dynamic addresses, create a new address
        // that is local to this router and insert it into the address table with a hash index.
        //
        if (!accept_dynamic)
            return 0;

        bool generating = true;
        while (generating) {
            //
            // The address-generation process is performed in a loop in case the generated
            // address collides with a previously generated address (this should be _highly_
            // unlikely).
            //
            char *temp_addr = 0;
            if (dir == QD_OUTGOING)
                temp_addr = qdr_generate_temp_addr(core);
            else
                temp_addr = qdr_generate_mobile_addr(core);

            qd_iterator_t *temp_iter = qd_iterator_string(temp_addr, ITER_VIEW_ADDRESS_HASH);
            qd_hash_retrieve(core->addr_hash, temp_iter, (void**) &addr);
            if (!addr) {
                addr = qdr_address_CT(core, QD_TREATMENT_ANYCAST_BALANCED, 0);
                qd_hash_insert(core->addr_hash, temp_iter, addr, &addr->hash_handle);
                DEQ_INSERT_TAIL(core->addrs, addr);
                qdr_terminus_set_address(terminus, temp_addr);
                generating = false;
            }
            qd_iterator_free(temp_iter);
            free(temp_addr);
        }
        return addr;
    }

    //
    // If the terminus is anonymous, there is no address to look up.
    //
    if (qdr_terminus_is_anonymous(terminus))
        return 0;

    //
    // Look for a message-route address.
    //
    qd_iterator_t *iter = qdr_terminus_get_address(terminus);
    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_NO_HOST);
    int priority  = -1;
    qd_address_treatment_t  treat       = core->qd->default_treatment;
    qdr_address_config_t   *addr_config = qdr_config_for_address_CT(core, conn, iter);

    if (addr_config) {
        priority = addr_config->priority;
        treat    = addr_config->treatment;
    }

    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_HASH);
    qd_iterator_annotate_prefix(iter, '\0'); // Cancel previous override

    qd_hash_retrieve(core->addr_hash, iter, (void**) &addr);

    if (addr && addr->treatment == QD_TREATMENT_UNAVAILABLE)
        *unavailable = true;

    //
    // If the address is a router-class address, change treatment to closest.
    //
    qd_iterator_reset(iter);
    if (qd_iterator_octet(iter) == (unsigned char) QD_ITER_HASH_PREFIX_ROUTER) {
        treat = QD_TREATMENT_ANYCAST_CLOSEST;

        //
        // It is not valid for an outgoing link to have a router-class address.
        //
        if (dir == QD_OUTGOING)
            return 0;
    }

    if (!addr && create_if_not_found) {
        addr = qdr_address_CT(core, treat, addr_config);
        if (addr) {
            qd_iterator_reset(iter);
            qd_hash_insert(core->addr_hash, iter, addr, &addr->hash_handle);
            DEQ_INSERT_TAIL(core->addrs, addr);
        }

        if (!addr && treat == QD_TREATMENT_UNAVAILABLE)
            *unavailable = true;
    }

    if (!!addr && addr->core_endpoint != 0)
        *core_endpoint = true;

    if (addr)
        addr->priority = priority;
    return addr;
}


static void qdr_link_react_to_first_attach_CT(qdr_core_t       *core,
                                              qdr_connection_t *conn,
                                              qdr_address_t    *addr,
                                              qdr_link_t       *link,
                                              qd_direction_t    dir,
                                              qdr_terminus_t   *source,  // must free when done
                                              qdr_terminus_t   *target,  // must free when done
                                              bool              unavailable,
                                              bool              core_endpoint)
{
    if (core_endpoint) {
        qdrc_endpoint_do_bound_attach_CT(core, addr, link, source, target);
        source = target = 0;  // ownership passed to qdrc_endpoint_do_bound_attach_CT
    }
    else if (unavailable) {
        qdr_link_outbound_detach_CT(core, link, qdr_error(QD_AMQP_COND_NOT_FOUND, "Node not found"), 0, true);
    }
    else if (!addr) {
        //
        // No route to this destination, reject the link
        //
        qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_NO_ROUTE_TO_DESTINATION, true);
    } else {
        //
        // Prior to binding, check to see if this is an inter-edge connection.  If so,
        // mark the link as a proxy.
        //
        if (link->conn->role == QDR_ROLE_INTER_EDGE) {
            link->proxy = true;
        }

        //
        // Associate the link with the address.  With this association, it will be unnecessary
        // to do an address lookup for deliveries that arrive on this link.
        //
        qdr_core_bind_address_link_CT(core, addr, link);
        qdr_link_outbound_second_attach_CT(core, link, source, target);
        source = target = 0;  // ownership passed to qdr_link_outbound_second_attach_CT

        //
        // Issue the initial credit only if one of the following
        // holds:
        // - there are destinations for the address
        //
        if (dir == QD_INCOMING
            && (DEQ_SIZE(addr->subscriptions)
                || DEQ_SIZE(addr->rlinks)
                || qd_bitmask_cardinality(addr->rnodes))) {
            qdr_link_issue_credit_CT(core, link, link->capacity, false);
        }

        //
        // If this link came through an edge connection, raise a link event to
        // herald that fact.
        //
        if (link->conn->role == QDR_ROLE_EDGE_CONNECTION)
            qdrc_event_link_raise(core, QDRC_EVENT_LINK_EDGE_DATA_ATTACHED, link);
    }

    if (source)
        qdr_terminus_free(source);
    if (target)
        qdr_terminus_free(target);
}


//================================================================================
// Address Lookup Handler
//================================================================================

static void qcm_addr_lookup_CT(void             *context,
                               qdr_connection_t *conn,
                               qdr_link_t       *link,
                               qd_direction_t    dir,
                               qdr_terminus_t   *source,
                               qdr_terminus_t   *target)
{
    qcm_lookup_client_t *client = (qcm_lookup_client_t*) context;
    bool                 unavailable;
    bool                 core_endpoint;
    qdr_terminus_t      *term = dir == QD_INCOMING ? target : source;

    //
    // Always perform the built-in, synchronous address lookup
    //
    qdr_address_t *addr = qdr_lookup_terminus_address_CT(client->core, dir, conn, term, true, true,
                                                         &unavailable, &core_endpoint);
    qdr_link_react_to_first_attach_CT(client->core, conn, addr, link, dir, source, target,
                                      unavailable, core_endpoint);
}


//================================================================================
// Module Handlers
//================================================================================

static bool qcm_addr_lookup_client_enable_CT(qdr_core_t *core)
{
    return true;
}


static void qcm_addr_lookup_client_init_CT(qdr_core_t *core, void **module_context)
{
    assert(core->addr_lookup_handler == 0);
    qcm_lookup_client_t *client = NEW(qcm_lookup_client_t);
    ZERO(client);

    client->core = core;
    core->addr_lookup_handler = qcm_addr_lookup_CT;
    core->addr_lookup_context = client;
    *module_context = client;
}


static void qcm_addr_lookup_client_final_CT(void *module_context)
{
    qcm_lookup_client_t *client = (qcm_lookup_client_t*) module_context;
    client->core->addr_lookup_handler = 0;
    free(client);
}


QDR_CORE_MODULE_DECLARE("address_lookup_client", qcm_addr_lookup_client_enable_CT, qcm_addr_lookup_client_init_CT, qcm_addr_lookup_client_final_CT)

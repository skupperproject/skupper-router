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
#include "delivery.h"
#include "module.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/ctools.h"

#include <stdio.h>


static struct {
    qdr_core_t           *core;
    qdrc_endpoint_desc_t  endpoint_desc;
} state;


static void on_first_attach(void             *bind_context,
                            qdrc_endpoint_t  *endpoint,
                            void            **link_context,
                            qdr_terminus_t   *remote_source,
                            qdr_terminus_t   *remote_target)
{
    qd_iterator_t *iter = qdr_terminus_get_address(remote_target);
    qd_iterator_reset_view(iter, ITER_VIEW_ALL);

    if (qd_iterator_equal(iter, (unsigned const char*) QD_TERMINUS_MESH_DISCOVERY)
        && qdrc_endpoint_get_direction_CT(endpoint) == QD_INCOMING) {
        qdrc_endpoint_second_attach_CT(state.core, endpoint, remote_target, remote_source);
        qdrc_endpoint_flow_CT(state.core, endpoint, 3, false);
        *link_context = (void*) endpoint;
    } else {
        qdrc_endpoint_detach_CT(state.core, endpoint, 0);
        qdr_terminus_free(remote_source);
        qdr_terminus_free(remote_target);
    }
}


static void on_transfer(void *link_context, qdr_delivery_t *delivery, qd_message_t *message)
{
    qdrc_endpoint_t  *endpoint = (qdrc_endpoint_t*) link_context;
    char              id[QD_DISCRIMINATOR_BYTES];
    int               values_found = 0;
    qdr_link_t       *link         = qdr_delivery_link(delivery);
    qdr_connection_t *conn         = !!link ? link->conn : 0;

    if (!!conn) {
        if (qd_message_check_depth(message, QD_DEPTH_APPLICATION_PROPERTIES) == QD_MESSAGE_DEPTH_OK) {
            qd_iterator_t     *iter = qd_message_field_iterator(message, QD_FIELD_APPLICATION_PROPERTIES);
            qd_parsed_field_t *ap   = qd_parse(iter);
            if (!!ap) {
                if (qd_parse_is_map(ap)) {
                    for (int i = 0; i < qd_parse_sub_count(ap); i++) {
                        qd_iterator_t     *key_iter = qd_parse_raw(qd_parse_sub_key(ap, i));
                        qd_parsed_field_t *value    = qd_parse_sub_value(ap, i);

                        if (qd_iterator_equal(key_iter, (unsigned char*) QD_KEY_MESH_ID_ANNOUNCE_IDENTIFIER)) {
                            qd_iterator_ncopy(qd_parse_raw(value), (unsigned char*) id, QD_DISCRIMINATOR_BYTES);
                            values_found++;
                        }
                    }
                }
                qd_parse_free(ap);
                qd_iterator_free(iter);
            }
        }
    }

    if (values_found == 1) {
        memcpy(conn->edge_mesh_id, id, QD_DISCRIMINATOR_BYTES);
        qdr_core_edge_mesh_id_changed_CT(state.core, conn);
    }

    qdrc_endpoint_settle_CT(state.core, delivery, PN_ACCEPTED);

    //
    // Replenish the credit for this delivery
    //
    qdrc_endpoint_flow_CT(state.core, endpoint, 1, false);
}


static bool mesh_discovery_interior_enable_CT(qdr_core_t *core)
{
    return core->router_mode == QD_ROUTER_MODE_INTERIOR;
}


static void mesh_discovery_interior_init_CT(qdr_core_t *core, void **module_context)
{
    *module_context = &state;
    ZERO(&state);
    state.core = core;

    //
    // Bind to the static address QD_TERMINUS_MESH_DISCOVERY
    //
    state.endpoint_desc.label            = "mesh_discovery_interior";
    state.endpoint_desc.on_first_attach  = on_first_attach;
    state.endpoint_desc.on_transfer      = on_transfer;
    qdrc_endpoint_bind_mobile_address_CT(core, QD_TERMINUS_MESH_DISCOVERY, &state.endpoint_desc, &state);
}


static void mesh_discovery_interior_final_CT(void *module_context)
{
    // Function intentionally left blank
}


QDR_CORE_MODULE_DECLARE("mesh_discovery_interior", mesh_discovery_interior_enable_CT, mesh_discovery_interior_init_CT, mesh_discovery_interior_final_CT)

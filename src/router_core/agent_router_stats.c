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

#include "agent_router_stats.h"
#include "config.h"
#include "qpid/dispatch/protocols.h"
#include "qpid/dispatch/connection_counters.h"

#include <inttypes.h>


#define QDR_ROUTER_IDENTITY                            0
#define QDR_ROUTER_STATS_TYPE                          1
#define QDR_ROUTER_ID                                  2
#define QDR_ROUTER_ADDR_COUNT                          3
#define QDR_ROUTER_LINK_COUNT                          4
#define QDR_ROUTER_NODE_COUNT                          5
#define QDR_ROUTER_AUTO_LINK_COUNT                     6
#define QDR_ROUTER_CONNECTION_COUNT                    7
#define QDR_ROUTER_PRESETTLED_DELIVERIES               8
#define QDR_ROUTER_DROPPED_PRESETTLED_DELIVERIES       9
#define QDR_ROUTER_ACCEPTED_DELIVERIES                 10
#define QDR_ROUTER_REJECTED_DELIVERIES                 11
#define QDR_ROUTER_RELEASED_DELIVERIES                 12
#define QDR_ROUTER_MODIFIED_DELIVERIES                 13
#define QDR_ROUTER_DELAYED_1SEC                        14
#define QDR_ROUTER_DELAYED_10SEC                       15
#define QDR_ROUTER_DELIVERIES_STUCK                    16
#define QDR_ROUTER_DELIVERIES_INGRESS                  17
#define QDR_ROUTER_DELIVERIES_EGRESS                   18
#define QDR_ROUTER_DELIVERIES_TRANSIT                  19
#define QDR_ROUTER_DELIVERIES_INGRESS_ROUTE_CONTAINER  20
#define QDR_ROUTER_DELIVERIES_EGRESS_ROUTE_CONTAINER   21
#define QDR_ROUTER_DELIVERIES_REDIRECTED               22
#define QDR_ROUTER_LINKS_BLOCKED                       23
#define QDR_ROUTER_UPTIME_SECONDS                      24
#define QDR_ROUTER_MEMORY_USAGE                        25
#define QDR_ROUTER_RSS_USAGE                           26
#define QDR_ROUTER_CONNECTION_COUNTERS                 27
#define QDR_ROUTER_VERSION                             28

const char *qdr_router_columns[] =
    {"identity",
     "type",
     "id",
     "addrCount",
     "linkCount",
     "nodeCount",
     "autoLinkCount",
     "connectionCount",
     "presettledDeliveries",
     "droppedPresettledDeliveries",
     "acceptedDeliveries",
     "rejectedDeliveries",
     "releasedDeliveries",
     "modifiedDeliveries",
     "deliveriesDelayed1Sec",
     "deliveriesDelayed10Sec",
     "deliveriesStuck",
     "deliveriesIngress",
     "deliveriesEgress",
     "deliveriesTransit",
     "deliveriesIngressRouteContainer",
     "deliveriesEgressRouteContainer",
     "deliveriesRedirectedToFallback",
     "linksBlocked",
     "uptimeSeconds",
     "memoryUsage",
     "residentMemoryUsage",
     "connectionCounters",
     "version",
     0};

static void qdr_agent_write_column_CT(qd_composed_field_t *body, int col, qdr_core_t *core)
{
    switch(col) {
    case QDR_ROUTER_IDENTITY:
        // There is only one instance of router stats. Just give it an identity of 1
        qd_compose_insert_string(body, "1");
        break;

    case QDR_ROUTER_ADDR_COUNT:
        qd_compose_insert_ulong(body, DEQ_SIZE(core->addrs));
        break;

    case QDR_ROUTER_LINK_COUNT:
        qd_compose_insert_ulong(body, DEQ_SIZE(core->open_links));
        break;

    case QDR_ROUTER_NODE_COUNT:
        qd_compose_insert_ulong(body, DEQ_SIZE(core->routers));
        break;

    case QDR_ROUTER_CONNECTION_COUNT:
        qd_compose_insert_ulong(body, DEQ_SIZE(core->open_connections));
        break;

    case QDR_ROUTER_STATS_TYPE:
        qd_compose_insert_string(body, "org.apache.qpid.dispatch.routerStats");
        break;

    case QDR_ROUTER_AUTO_LINK_COUNT:
        qd_compose_insert_ulong(body, DEQ_SIZE(core->auto_links));
        break;

    case QDR_ROUTER_ID:
        if (core->router_id)
            qd_compose_insert_string(body, core->router_id);
        else
            qd_compose_insert_null(body);
        break;
    case QDR_ROUTER_VERSION:
        qd_compose_insert_string(body, QPID_DISPATCH_VERSION);
        break;

    case QDR_ROUTER_PRESETTLED_DELIVERIES:
        qd_compose_insert_ulong(body, core->presettled_deliveries);
        break;

    case QDR_ROUTER_DROPPED_PRESETTLED_DELIVERIES:
        qd_compose_insert_ulong(body, core->dropped_presettled_deliveries);
        break;

    case QDR_ROUTER_ACCEPTED_DELIVERIES:
        qd_compose_insert_ulong(body, core->accepted_deliveries);
        break;

    case QDR_ROUTER_REJECTED_DELIVERIES:
        qd_compose_insert_ulong(body, core->rejected_deliveries);
        break;

    case QDR_ROUTER_RELEASED_DELIVERIES:
        qd_compose_insert_ulong(body, core->released_deliveries);
        break;

    case QDR_ROUTER_MODIFIED_DELIVERIES:
        qd_compose_insert_ulong(body, core->modified_deliveries);
        break;

    case QDR_ROUTER_DELAYED_1SEC:
        qd_compose_insert_ulong(body, core->deliveries_delayed_1sec);
        break;

    case QDR_ROUTER_DELAYED_10SEC:
        qd_compose_insert_ulong(body, core->deliveries_delayed_10sec);
        break;

    case QDR_ROUTER_DELIVERIES_STUCK:
        qd_compose_insert_ulong(body, core->deliveries_stuck);
        break;

    case QDR_ROUTER_DELIVERIES_INGRESS:
        qd_compose_insert_ulong(body, core->deliveries_ingress);
        break;

    case QDR_ROUTER_DELIVERIES_EGRESS:
        qd_compose_insert_ulong(body, core->deliveries_egress);
        break;

    case QDR_ROUTER_DELIVERIES_TRANSIT:
        qd_compose_insert_ulong(body, core->deliveries_transit);
        break;

    case QDR_ROUTER_DELIVERIES_INGRESS_ROUTE_CONTAINER:
        qd_compose_insert_ulong(body, core->deliveries_ingress_route_container);
        break;

    case QDR_ROUTER_DELIVERIES_EGRESS_ROUTE_CONTAINER:
        qd_compose_insert_ulong(body, core->deliveries_egress_route_container);
        break;

    case QDR_ROUTER_DELIVERIES_REDIRECTED:
        qd_compose_insert_ulong(body, core->deliveries_redirected);
        break;

    case QDR_ROUTER_LINKS_BLOCKED:
        qd_compose_insert_uint(body, core->links_blocked);
        break;


    case QDR_ROUTER_UPTIME_SECONDS:
        qd_compose_insert_uint(body, qdr_core_uptime_ticks(core));
        break;

    case QDR_ROUTER_MEMORY_USAGE: {
        uint64_t size = qd_router_virtual_memory_usage();
        if (size)
            qd_compose_insert_ulong(body, size);
        else  // memory usage not available
            qd_compose_insert_null(body);
    } break;

    case QDR_ROUTER_RSS_USAGE: {
        uint64_t size = qd_router_rss_memory_usage();
        if (size)
            qd_compose_insert_ulong(body, size);
        else  // memory usage not available
            qd_compose_insert_null(body);
    } break;

    case QDR_ROUTER_CONNECTION_COUNTERS: {
        qd_compose_start_map(body);
        for (qd_protocol_t proto = 0; proto < QD_PROTOCOL_TOTAL; ++proto) {
            qd_compose_insert_string(body, qd_protocol_name(proto));
            qd_compose_insert_ulong(body, qd_connection_count(proto));
        }
        qd_compose_end_map(body);
        break;
    }

    default:
        qd_compose_insert_null(body);
        break;
    }
}



static void qdr_agent_write_router_CT(qdr_query_t *query,  qdr_core_t *core)
{
    qd_composed_field_t *body = query->body;

    qd_compose_start_list(body);
    int i = 0;
    while (query->columns[i] >= 0) {
        qdr_agent_write_column_CT(body, query->columns[i], core);
        i++;
    }
    qd_compose_end_list(body);
}

void qdra_router_stats_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset)
{
    //
    // Queries that get this far will always succeed.
    //
    query->status = QD_AMQP_OK;

    if (offset >= 1) {
        query->more = false;
        qdr_agent_enqueue_response_CT(core, query);
        return;
    }

    //
    // Write the columns of core into the response body.
    //
    qdr_agent_write_router_CT(query, core);

    //
    // Enqueue the response.
    //
    qdr_agent_enqueue_response_CT(core, query);
}

// Nothing to do here. The router stats has only one entry.
void qdra_router_stats_get_next_CT(qdr_core_t *core, qdr_query_t *query)
{

}

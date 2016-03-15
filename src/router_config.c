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

#include <qpid/dispatch/python_embedded.h>
#include <qpid/dispatch.h>
#include <qpid/dispatch/log.h>
#include "dispatch_private.h"
#include "router_private.h"
#include "lrp_private.h"
#include "entity.h"
#include "entity_cache.h"
#include "schema_enum.h"

qd_error_t qd_router_configure_fixed_address(qd_router_t *router, qd_entity_t *entity)
{
    qd_error_clear();
    int                             phase  = qd_entity_opt_long(entity, "phase", -1); QD_ERROR_RET();
    qd_schema_fixedAddress_fanout_t fanout = qd_entity_get_long(entity, "fanout");    QD_ERROR_RET();
    qd_schema_fixedAddress_bias_t   bias   = qd_entity_get_long(entity, "bias");      QD_ERROR_RET();
    char                           *prefix = qd_entity_get_string(entity, "prefix");  QD_ERROR_RET();

    if (phase != -1) {
        qd_log(router->log_source, QD_LOG_WARNING,
               "Address phases deprecated: Ignoring address configuration for '%s', phase %d", prefix, phase);
        free(prefix);
        return qd_error_code();
    }

    if (prefix[0] == '/' && prefix[1] == '\0') {
        qd_log(router->log_source, QD_LOG_WARNING, "Ignoring address configuration for '/'");
        free(prefix);
        return qd_error_code();
    }

    //
    // Convert fanout + bias to distribution
    //
    const char *distrib;

    if (fanout == QD_SCHEMA_FIXEDADDRESS_FANOUT_MULTIPLE)
        distrib = "multicast";
    else {
        if (bias == QD_SCHEMA_FIXEDADDRESS_BIAS_CLOSEST)
            distrib = "closest";
        else
            distrib = "balanced";
    }

    //
    // Formulate this configuration as a router.route and create it through the core management API.
    //
    qd_composed_field_t *body = qd_compose_subfield(0);
    qd_compose_start_map(body);
    qd_compose_insert_string(body, "prefix");
    qd_compose_insert_string(body, prefix);

    qd_compose_insert_string(body, "distribution");
    qd_compose_insert_string(body, distrib);
    qd_compose_end_map(body);

    int              length = 0;
    qd_buffer_list_t buffers;

    qd_compose_take_buffers(body, &buffers);
    qd_compose_free(body);

    qd_buffer_t *buf = DEQ_HEAD(buffers);
    while (buf) {
        length += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    qd_field_iterator_t *iter = qd_field_iterator_buffer(DEQ_HEAD(buffers), 0, length);
    qd_parsed_field_t   *in_body = qd_parse(iter);

    qdr_manage_create(router->router_core, 0, QD_ROUTER_CONFIG_ADDRESS, 0, in_body, 0);

    free(prefix);
    return qd_error_code();
}

qd_error_t qd_router_configure_waypoint(qd_router_t *router, qd_entity_t *entity)
{
    /*
    char *address = qd_entity_get_string(entity, "address"); QD_ERROR_RET();
    char *connector = qd_entity_get_string(entity, "connector"); QD_ERROR_RET();
    int   in_phase  = qd_entity_opt_long(entity, "inPhase", 0); QD_ERROR_RET();
    int   out_phase = qd_entity_opt_long(entity, "outPhase", 0);  QD_ERROR_RET();

    if (in_phase > 9 || out_phase > 9) {
        qd_error_t err = qd_error(QD_ERROR_CONFIG,
                                  "Phases for waypoint '%s' must be between 0 and 9.", address);
        free(address);
        free(connector);
        return err;
    }
    qd_waypoint_t *waypoint = NEW(qd_waypoint_t);
    memset(waypoint, 0, sizeof(qd_waypoint_t));
    DEQ_ITEM_INIT(waypoint);
    waypoint->address        = address;
    waypoint->in_phase       = in_phase >= 0  ? (char) in_phase  + '0' : '\0';
    waypoint->out_phase      = out_phase >= 0 ? (char) out_phase + '0' : '\0';
    waypoint->connector_name = connector;

    DEQ_INSERT_TAIL(router->waypoints, waypoint);

    qd_log(router->log_source, QD_LOG_INFO,
           "Configured Waypoint: address=%s in_phase=%d out_phase=%d connector=%s",
           address, in_phase, out_phase, connector);
    */
    return qd_error_code();
}


qd_error_t qd_router_configure_lrp(qd_router_t *router, qd_entity_t *entity)
{
    char *prefix    = qd_entity_get_string(entity, "prefix");    QD_ERROR_RET();
    char *connector = qd_entity_get_string(entity, "connector"); QD_ERROR_RET();
    char *direction = qd_entity_get_string(entity, "dir");       QD_ERROR_RET();

    //
    // Formulate this configuration as a router.route and create it through the core management API.
    //
    qd_composed_field_t *body = qd_compose_subfield(0);
    qd_compose_start_map(body);
    qd_compose_insert_string(body, "prefix");
    qd_compose_insert_string(body, prefix);

    qd_compose_insert_string(body, "dir");
    qd_compose_insert_string(body, direction);

    if (connector) {
        qd_compose_insert_string(body, "connectionName");
        qd_compose_insert_string(body, connector);
    }

    qd_compose_end_map(body);

    int              length = 0;
    qd_buffer_list_t buffers;

    qd_compose_take_buffers(body, &buffers);
    qd_compose_free(body);

    qd_buffer_t *buf = DEQ_HEAD(buffers);
    while (buf) {
        length += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    qd_field_iterator_t *iter    = qd_field_iterator_buffer(DEQ_HEAD(buffers), 0, length);
    qd_parsed_field_t   *in_body = qd_parse(iter);

    qdr_manage_create(router->router_core, 0, QD_ROUTER_CONFIG_LINK_ROUTE, 0, in_body, 0);

    free(prefix);
    free(connector);
    free(direction);
    return qd_error_code();
}


qd_error_t qd_router_configure_address(qd_router_t *router, qd_entity_t *entity)
{
    char *name      = qd_entity_opt_string(entity, "name", 0);         QD_ERROR_RET();
    char *prefix    = qd_entity_get_string(entity, "prefix");          QD_ERROR_RET();
    char *distrib   = qd_entity_opt_string(entity, "distribution", 0); QD_ERROR_RET();
    bool  waypoint  = qd_entity_opt_bool(entity, "waypoint", false);   QD_ERROR_RET();
    long  in_phase  = qd_entity_opt_long(entity, "ingressPhase", -1);  QD_ERROR_RET();
    long  out_phase = qd_entity_opt_long(entity, "egressPhase", -1);   QD_ERROR_RET();

    //
    // Formulate this configuration create it through the core management API.
    //
    qd_composed_field_t *body = qd_compose_subfield(0);
    qd_compose_start_map(body);

    if (name) {
        qd_compose_insert_string(body, "name");
        qd_compose_insert_string(body, name);
    }

    if (prefix) {
        qd_compose_insert_string(body, "prefix");
        qd_compose_insert_string(body, prefix);
    }

    if (distrib) {
        qd_compose_insert_string(body, "distribution");
        qd_compose_insert_string(body, distrib);
    }

    qd_compose_insert_string(body, "waypoint");
    qd_compose_insert_bool(body, waypoint);

    if (in_phase >= 0) {
        qd_compose_insert_string(body, "ingressPhase");
        qd_compose_insert_int(body, in_phase);
    }

    if (out_phase >= 0) {
        qd_compose_insert_string(body, "egressPhase");
        qd_compose_insert_int(body, out_phase);
    }

    qd_compose_end_map(body);

    int              length = 0;
    qd_buffer_list_t buffers;

    qd_compose_take_buffers(body, &buffers);
    qd_compose_free(body);

    qd_buffer_t *buf = DEQ_HEAD(buffers);
    while (buf) {
        length += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    qd_field_iterator_t *iter    = qd_field_iterator_buffer(DEQ_HEAD(buffers), 0, length);
    qd_parsed_field_t   *in_body = qd_parse(iter);

    qdr_manage_create(router->router_core, 0, QD_ROUTER_CONFIG_ADDRESS, 0, in_body, 0);

    free(name);
    free(prefix);
    free(distrib);

    return qd_error_code();
}


qd_error_t qd_router_configure_link_route(qd_router_t *router, qd_entity_t *entity)
{
    char *name      = qd_entity_opt_string(entity, "name", 0);           QD_ERROR_RET();
    char *prefix    = qd_entity_get_string(entity, "prefix");            QD_ERROR_RET();
    char *container = qd_entity_opt_string(entity, "containerId", 0);    QD_ERROR_RET();
    char *c_name    = qd_entity_opt_string(entity, "connectionName", 0); QD_ERROR_RET();
    char *distrib   = qd_entity_opt_string(entity, "distribution", 0);   QD_ERROR_RET();
    char *dir       = qd_entity_opt_string(entity, "dir", 0);            QD_ERROR_RET();

    //
    // Formulate this configuration as a route and create it through the core management API.
    //
    qd_composed_field_t *body = qd_compose_subfield(0);
    qd_compose_start_map(body);

    if (name) {
        qd_compose_insert_string(body, "name");
        qd_compose_insert_string(body, name);
    }

    if (prefix) {
        qd_compose_insert_string(body, "prefix");
        qd_compose_insert_string(body, prefix);
    }

    if (container) {
        qd_compose_insert_string(body, "containerId");
        qd_compose_insert_string(body, container);
    }

    if (c_name) {
        qd_compose_insert_string(body, "connectionName");
        qd_compose_insert_string(body, c_name);
    }

    if (distrib) {
        qd_compose_insert_string(body, "distribution");
        qd_compose_insert_string(body, distrib);
    }

    if (dir) {
        qd_compose_insert_string(body, "dir");
        qd_compose_insert_string(body, dir);
    }

    qd_compose_end_map(body);

    int              length = 0;
    qd_buffer_list_t buffers;

    qd_compose_take_buffers(body, &buffers);
    qd_compose_free(body);

    qd_buffer_t *buf = DEQ_HEAD(buffers);
    while (buf) {
        length += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    qd_field_iterator_t *iter    = qd_field_iterator_buffer(DEQ_HEAD(buffers), 0, length);
    qd_parsed_field_t   *in_body = qd_parse(iter);

    qdr_manage_create(router->router_core, 0, QD_ROUTER_CONFIG_LINK_ROUTE, 0, in_body, 0);

    free(name);
    free(prefix);
    free(container);
    free(c_name);
    free(distrib);
    free(dir);

    return qd_error_code();
}


qd_error_t qd_router_configure_auto_link(qd_router_t *router, qd_entity_t *entity)
{
    char *name      = qd_entity_opt_string(entity, "name", 0);           QD_ERROR_RET();
    char *addr      = qd_entity_get_string(entity, "addr");              QD_ERROR_RET();
    char *dir       = qd_entity_get_string(entity, "dir");               QD_ERROR_RET();
    long  phase     = qd_entity_opt_long(entity, "phase", -1);           QD_ERROR_RET();
    char *container = qd_entity_opt_string(entity, "containerId", 0);    QD_ERROR_RET();
    char *c_name    = qd_entity_opt_string(entity, "connectionName", 0); QD_ERROR_RET();

    //
    // Formulate this configuration as a route and create it through the core management API.
    //
    qd_composed_field_t *body = qd_compose_subfield(0);
    qd_compose_start_map(body);

    if (name) {
        qd_compose_insert_string(body, "name");
        qd_compose_insert_string(body, name);
    }

    if (addr) {
        qd_compose_insert_string(body, "addr");
        qd_compose_insert_string(body, addr);
    }

    if (dir) {
        qd_compose_insert_string(body, "dir");
        qd_compose_insert_string(body, dir);
    }

    if (phase >= 0) {
        qd_compose_insert_string(body, "phase");
        qd_compose_insert_int(body, phase);
    }

    if (container) {
        qd_compose_insert_string(body, "containerId");
        qd_compose_insert_string(body, container);
    }

    if (c_name) {
        qd_compose_insert_string(body, "connectionName");
        qd_compose_insert_string(body, c_name);
    }

    qd_compose_end_map(body);

    int              length = 0;
    qd_buffer_list_t buffers;

    qd_compose_take_buffers(body, &buffers);
    qd_compose_free(body);

    qd_buffer_t *buf = DEQ_HEAD(buffers);
    while (buf) {
        length += qd_buffer_size(buf);
        buf = DEQ_NEXT(buf);
    }

    qd_field_iterator_t *iter    = qd_field_iterator_buffer(DEQ_HEAD(buffers), 0, length);
    qd_parsed_field_t   *in_body = qd_parse(iter);

    qdr_manage_create(router->router_core, 0, QD_ROUTER_CONFIG_AUTO_LINK, 0, in_body, 0);

    free(name);
    free(addr);
    free(dir);
    free(container);
    free(c_name);

    return qd_error_code();
}


void qd_router_configure_free(qd_router_t *router)
{
    if (!router) return;

    //
    // All configuration to be freed is now in the router core.
    //
}


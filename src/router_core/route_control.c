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

#include "route_control.h"

#include "router_core_private.h"

#include "qpid/dispatch/iterator.h"

#include <inttypes.h>
#include <stdio.h>

ALLOC_DEFINE(qdr_auto_link_t);
ALLOC_DEFINE(qdr_conn_identifier_t);

//
// Note that these hash prefixes are in a different space than those listed in iterator.h.
// These are used in a different hash table than the address hash.
//
const char CONTAINER_PREFIX = 'C';
const char CONNECTION_PREFIX = 'L';

const int AUTO_LINK_FIRST_RETRY_INTERVAL = 2;
const int AUTO_LINK_RETRY_INTERVAL = 5;


static qdr_conn_identifier_t *qdr_route_declare_id_CT(qdr_core_t    *core,
                                                      qd_iterator_t *container,
                                                      qd_iterator_t *connection)
{
    qdr_conn_identifier_t *cid    = 0;

    if (container && connection) {
        qd_iterator_reset_view(container, ITER_VIEW_ADDRESS_HASH);
        qd_iterator_annotate_prefix(container, CONTAINER_PREFIX);
        qd_hash_retrieve(core->conn_id_hash, container, (void**) &cid);

        if (!cid) {
            qd_iterator_reset_view(connection, ITER_VIEW_ADDRESS_HASH);
            qd_iterator_annotate_prefix(connection, CONNECTION_PREFIX);
            qd_hash_retrieve(core->conn_id_hash, connection, (void**) &cid);
        }

        if (!cid) {
            cid = new_qdr_conn_identifier_t();
            ZERO(cid);
            //
            // The container and the connection will represent the same connection.
            //
            qd_hash_insert(core->conn_id_hash, container, cid, &cid->container_hash_handle);
            qd_hash_insert(core->conn_id_hash, connection, cid, &cid->connection_hash_handle);
        }
    }
    else if (container) {
        qd_iterator_reset_view(container, ITER_VIEW_ADDRESS_HASH);
        qd_iterator_annotate_prefix(container, CONTAINER_PREFIX);
        qd_hash_retrieve(core->conn_id_hash, container, (void**) &cid);
        if (!cid) {
            cid = new_qdr_conn_identifier_t();
            ZERO(cid);
            qd_hash_insert(core->conn_id_hash, container, cid, &cid->container_hash_handle);
        }
    }
    else if (connection) {
        qd_iterator_reset_view(connection, ITER_VIEW_ADDRESS_HASH);
        qd_iterator_annotate_prefix(connection, CONNECTION_PREFIX);
        qd_hash_retrieve(core->conn_id_hash, connection, (void**) &cid);
        if (!cid) {
            cid = new_qdr_conn_identifier_t();
            ZERO(cid);
            qd_hash_insert(core->conn_id_hash, connection, cid, &cid->connection_hash_handle);
        }

    }

    return cid;
}


void qdr_route_check_id_for_deletion_CT(qdr_core_t *core, qdr_conn_identifier_t *cid)
{
    //
    // If this connection identifier has no open connection and no referencing routes,
    // it can safely be deleted and removed from the hash index.
    //
    if (DEQ_IS_EMPTY(cid->connection_refs) && DEQ_IS_EMPTY(cid->auto_link_refs)) {
        qd_hash_remove_by_handle(core->conn_id_hash, cid->connection_hash_handle);
        qd_hash_remove_by_handle(core->conn_id_hash, cid->container_hash_handle);
        qd_hash_handle_free(cid->connection_hash_handle);
        qd_hash_handle_free(cid->container_hash_handle);
        free_qdr_conn_identifier_t(cid);
    }
}


static void qdr_route_log_CT(qdr_core_t *core, const char *text, const char *name, uint64_t id, qdr_connection_t *conn)
{
    const char *key = NULL;
    const char *type = "<unknown>";
    if (conn->conn_id) {
        key = (const char*) qd_hash_key_by_handle(conn->conn_id->connection_hash_handle);
        if (!key) {
            key = (const char*) qd_hash_key_by_handle(conn->conn_id->container_hash_handle);
        }
        if (key)
            type = (*key++ == 'L') ? "connection" : "container";
    }
    if (!key && conn->connection_info) {
        type = "container";
        key = conn->connection_info->container;
    }

    char  id_string[64];
    const char *log_name = name ? name : id_string;

    if (!name)
        snprintf(id_string, 64, "%"PRId64, id);

    qd_log(core->log, QD_LOG_INFO, "%s '%s' on %s %s",
           text, log_name, type, key ? key : "<unknown>");
}


static void qdr_auto_link_activate_CT(qdr_core_t *core, qdr_auto_link_t *al, qdr_connection_t *conn)
{
    const char *key;

    qdr_route_log_CT(core, "Auto Link Activated", al->name, al->identity, conn);

    if (al->addr) {
        qdr_terminus_t *source = qdr_terminus(0);
        qdr_terminus_t *target = qdr_terminus(0);
        qdr_terminus_t *term;

        if (al->dir == QD_INCOMING)
            term = source;
        else
            term = target;

        key = (const char*) qd_hash_key_by_handle(al->addr->hash_handle);
        if (key || al->external_addr) {
            if (al->external_addr) {
                qdr_terminus_set_address(term, al->external_addr);
                if (key)
                    al->internal_addr = &key[1];
            } else
                qdr_terminus_set_address(term, &key[1]); // truncate the "M" annotation
            al->link = qdr_create_link_CT(core, conn, QD_LINK_ENDPOINT, al->dir, source, target,
                                          QD_SSN_ENDPOINT, QDR_DEFAULT_PRIORITY);
            al->link->auto_link = al;
            al->state = QDR_AUTO_LINK_STATE_ATTACHING;
        }
        else {
            free_qdr_terminus_t(source);
            free_qdr_terminus_t(target);
        }
    }
}


/**
 * Attempts re-establishing auto links across the related connections/containers
 */
static void qdr_route_attempt_auto_link_CT(qdr_core_t      *core,
                                    void *context)
{
    qdr_auto_link_t *al = (qdr_auto_link_t *)context;
    qdr_connection_ref_t * cref = DEQ_HEAD(al->conn_id->connection_refs);
    while (cref) {
        qdr_auto_link_activate_CT(core, al, cref->conn);
        cref = DEQ_NEXT(cref);
    }

}


static void qdr_auto_link_deactivate_CT(qdr_core_t *core, qdr_auto_link_t *al, qdr_connection_t *conn)
{
    qdr_route_log_CT(core, "Auto Link Deactivated", al->name, al->identity, conn);

    if (al->link) {
        qdr_link_outbound_detach_CT(core, al->link, 0, QDR_CONDITION_NONE, true);
        al->link->auto_link = 0;
        al->link            = 0;
    }

    al->state = QDR_AUTO_LINK_STATE_INACTIVE;
}


void qdr_route_auto_link_detached_CT(qdr_core_t *core, qdr_link_t *link)
{
    if (!link->auto_link)
        return;

    if (!link->auto_link->retry_timer)
        link->auto_link->retry_timer = qdr_core_timer_CT(core, qdr_route_attempt_auto_link_CT, (void *)link->auto_link);

    static char *activation_failed = "Auto Link Activation Failed. ";
    int error_length = link->auto_link->last_error ? strlen(link->auto_link->last_error) : 0;
    int total_length = strlen(activation_failed) + error_length + 1;

    char *error_msg = qd_malloc(total_length);
    strcpy(error_msg, activation_failed);
    if (error_length)
        strcat(error_msg, link->auto_link->last_error);

    if (link->auto_link->retry_attempts == 0) {
        // First retry in 2 seconds
        qdr_core_timer_schedule_CT(core, link->auto_link->retry_timer, AUTO_LINK_FIRST_RETRY_INTERVAL);
        link->auto_link->retry_attempts += 1;
    }
    else {
        // Successive retries every 5 seconds
        qdr_core_timer_schedule_CT(core, link->auto_link->retry_timer, AUTO_LINK_RETRY_INTERVAL);
    }

    qdr_route_log_CT(core, error_msg, link->auto_link->name, link->auto_link->identity, link->conn);
    free(error_msg);
}


void qdr_route_auto_link_closed_CT(qdr_core_t *core, qdr_link_t *link)
{
    if (link->auto_link && link->auto_link->retry_timer)
        qdr_core_timer_cancel_CT(core, link->auto_link->retry_timer);
}


qdr_auto_link_t *qdr_route_add_auto_link_CT(qdr_core_t          *core,
                                            qd_iterator_t       *name,
                                            qd_parsed_field_t   *addr_field,
                                            qd_direction_t       dir,
                                            qd_parsed_field_t   *container_field,
                                            qd_parsed_field_t   *connection_field,
                                            qd_parsed_field_t   *external_addr)
{
    qdr_auto_link_t *al = new_qdr_auto_link_t();

    //
    // Set up the auto_link structure
    //
    ZERO(al);
    al->identity      = qdr_identifier(core);
    al->name          = name ? (char*) qd_iterator_copy(name) : 0;
    al->dir           = dir;
    al->state         = QDR_AUTO_LINK_STATE_INACTIVE;
    al->external_addr = external_addr ? (char*) qd_iterator_copy(qd_parse_raw(external_addr)) : 0;

    //
    // Find or create an address for the auto_link destination
    //
    qd_iterator_t *iter = qd_parse_raw(addr_field);
    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_HASH);

    qd_hash_retrieve(core->addr_hash, iter, (void*) &al->addr);
    if (!al->addr) {
        qdr_address_config_t   *addr_config = qdr_config_for_address_CT(core, 0, iter);
        qd_address_treatment_t  treatment   = addr_config ? addr_config->treatment : QD_TREATMENT_ANYCAST_BALANCED;

        if (treatment == QD_TREATMENT_UNAVAILABLE) {
            //if associated address is not defined, assume balanced
            treatment = QD_TREATMENT_ANYCAST_BALANCED;
        }
        al->addr = qdr_address_CT(core, treatment, addr_config);
        DEQ_INSERT_TAIL(core->addrs, al->addr);
        qd_hash_insert(core->addr_hash, iter, al->addr, &al->addr->hash_handle);
    }

    al->addr->ref_count++;

    //
    // Find or create a connection identifier structure for this auto_link
    //
    if (container_field || connection_field) {
        al->conn_id = qdr_route_declare_id_CT(core, qd_parse_raw(container_field), qd_parse_raw(connection_field));
        DEQ_INSERT_TAIL_N(REF, al->conn_id->auto_link_refs, al);

        qdr_connection_ref_t * cref = DEQ_HEAD(al->conn_id->connection_refs);
        while (cref) {
            qdr_auto_link_activate_CT(core, al, cref->conn);
            cref = DEQ_NEXT(cref);
        }
    }

    //
    // If a name was provided, use that as the key to insert the this auto link name into the hashtable so
    // we can quickly find it later.
    //
    if (name) {
        qd_iterator_view_t iter_view = qd_iterator_get_view(name);
        qd_iterator_reset_view(name, ITER_VIEW_ADDRESS_HASH);
        qd_hash_insert(core->addr_lr_al_hash, name, al, &al->hash_handle);
        qd_iterator_reset_view(name, iter_view);
    }

    //
    // Add the auto_link to the core list
    //
    DEQ_INSERT_TAIL(core->auto_links, al);

    return al;
}


void qdr_route_del_auto_link_CT(qdr_core_t *core, qdr_auto_link_t *al)
{
    //
    // Disassociate from the connection identifier.  Check to see if the identifier
    // should be removed.
    //
    qdr_conn_identifier_t *cid = al->conn_id;
    if (cid) {
        qdr_connection_ref_t * cref = DEQ_HEAD(cid->connection_refs);
        while (cref) {
            qdr_auto_link_deactivate_CT(core, al, cref->conn);
            cref = DEQ_NEXT(cref);
        }
    }

    if (al->hash_handle) {
        qd_hash_remove_by_handle(core->addr_lr_al_hash, al->hash_handle);
        qd_hash_handle_free(al->hash_handle);
        al->hash_handle = 0;
    }

    //
    // Remove the auto link from the core list.
    //
    DEQ_REMOVE(core->auto_links, al);
    qdr_core_delete_auto_link(core, al);
}

static void activate_route_connection(qdr_core_t *core, qdr_connection_t *conn, qdr_conn_identifier_t *cid)
{
    //
    // Activate all auto-links associated with this remote container.
    //
    qdr_auto_link_t *al = DEQ_HEAD(cid->auto_link_refs);
    while (al) {
        qdr_auto_link_activate_CT(core, al, conn);
        al = DEQ_NEXT_N(REF, al);
    }
}

static void deactivate_route_connection(qdr_core_t *core, qdr_connection_t *conn, qdr_conn_identifier_t *cid)
{
        //
        // Deactivate all auto-links associated with this remote container.
        //
        qdr_auto_link_t *al = DEQ_HEAD(cid->auto_link_refs);
        while (al) {
            qdr_auto_link_deactivate_CT(core, al, conn);
            al = DEQ_NEXT_N(REF, al);
        }

        //
        // Remove our own entry in the connection list
        //
        qdr_del_connection_ref(&cid->connection_refs, conn);

        qdr_route_check_id_for_deletion_CT(core, cid);
}

void qdr_route_connection_opened_CT(qdr_core_t       *core,
                                    qdr_connection_t *conn,
                                    qdr_field_t      *container_field,
                                    qdr_field_t      *connection_field)
{
    if (conn->role != QDR_ROLE_ROUTE_CONTAINER)
        return;

    if (connection_field) {
        qdr_conn_identifier_t *cid = qdr_route_declare_id_CT(core, 0, connection_field->iterator);
        qdr_add_connection_ref(&cid->connection_refs, conn);
        conn->conn_id = cid;
        activate_route_connection(core, conn, conn->conn_id);
        if (container_field) {
            cid = qdr_route_declare_id_CT(core, container_field->iterator, 0);
            if (cid != conn->conn_id) {
                //the connection and container may be indexed to different objects if
                //there are multiple distinctly named connectors which connect to the
                //same amqp container
                qdr_add_connection_ref(&cid->connection_refs, conn);
                conn->alt_conn_id = cid;
                activate_route_connection(core, conn, conn->alt_conn_id);
            }
        }
    } else {
        qdr_conn_identifier_t *cid = qdr_route_declare_id_CT(core, container_field->iterator, 0);
        qdr_add_connection_ref(&cid->connection_refs, conn);
        conn->conn_id = cid;
        activate_route_connection(core, conn, conn->conn_id);
    }
}

void qdr_route_connection_closed_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    if (conn->role != QDR_ROLE_ROUTE_CONTAINER)
        return;

    qdr_conn_identifier_t *cid = conn->conn_id;
    if (cid) {
        deactivate_route_connection(core, conn, cid);
        conn->conn_id = 0;
    }
    cid = conn->alt_conn_id;
    if (cid) {
        deactivate_route_connection(core, conn, cid);
        conn->alt_conn_id = 0;
    }
}



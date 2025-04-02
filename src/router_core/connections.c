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
#include "route_control.h"
#include "router_core_private.h"

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/discriminator.h"
#include "qpid/dispatch/router_core.h"
#include "qpid/dispatch/static_assert.h"

#include <inttypes.h>
#include <stdio.h>
#include <strings.h>

static void qdr_connection_opened_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_connection_notify_closed_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_inbound_first_attach_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_inbound_second_attach_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_inbound_detach_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_notify_closed_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_processing_complete_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_processing_complete(qdr_core_t *core, qdr_link_t *link);
static void qdr_connection_group_cleanup_CT(qdr_core_t *core, qdr_connection_t *conn);
static void qdr_connection_set_tracing_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void setup_inter_router_control_conn_CT(qdr_core_t *core, qdr_connection_t *conn);


ALLOC_DEFINE_SAFE(qdr_connection_t);
ALLOC_DEFINE(qdr_connection_work_t);

//==================================================================================
// Internal Functions
//==================================================================================

qdr_terminus_t *qdr_terminus_router_control(void)
{
    qdr_terminus_t *term = qdr_terminus(0);
    qdr_terminus_add_capability(term, QD_CAPABILITY_ROUTER_CONTROL);
    return term;
}


qdr_terminus_t *qdr_terminus_router_data(void)
{
    qdr_terminus_t *term = qdr_terminus(0);
    qdr_terminus_add_capability(term, QD_CAPABILITY_ROUTER_DATA);
    return term;
}


qdr_terminus_t *qdr_terminus_inter_edge(void)
{
    qdr_terminus_t *term = qdr_terminus(0);
    qdr_terminus_add_capability(term, QD_CAPABILITY_INTER_EDGE);
    return term;
}


//==================================================================================
// Interface Functions
//==================================================================================

qdr_connection_t *qdr_connection_opened(qdr_core_t                   *core,
                                        qdr_protocol_adaptor_t       *protocol_adaptor,
                                        bool                          incoming,
                                        qdr_connection_role_t         role,
                                        int                           cost,
                                        uint64_t                      management_id,
                                        const char                   *label,
                                        bool                          strip_annotations_in,
                                        bool                          strip_annotations_out,
                                        int                           link_capacity,
                                        const qd_policy_spec_t       *policy_spec,
                                        qdr_connection_info_t        *connection_info,
                                        qdr_connection_bind_context_t context_binder,
                                        void                         *bind_token)
{
    qdr_action_t     *action = qdr_action(qdr_connection_opened_CT, "connection_opened");
    qdr_connection_t *conn   = new_qdr_connection_t();

    ZERO(conn);
    conn->protocol_adaptor      = protocol_adaptor;
    conn->identity              = management_id;
    conn->connection_info       = connection_info;
    conn->core                  = core;
    conn->user_context          = 0;
    conn->incoming              = incoming;
    conn->role                  = role;
    conn->inter_router_cost     = cost;
    conn->strip_annotations_in  = strip_annotations_in;
    conn->strip_annotations_out = strip_annotations_out;
    conn->policy_spec           = policy_spec;
    conn->link_capacity         = link_capacity;
    conn->mask_bit              = -1;
    conn->group_parent_mask_bit = -1;
    conn->admin_status          = QD_CONN_ADMIN_ENABLED;
    conn->oper_status           = QD_CONN_OPER_UP;
    DEQ_INIT(conn->links);
    DEQ_INIT(conn->work_list);
    DEQ_INIT(conn->streaming_link_pool);
    conn->connection_info->role = conn->role;
    sys_mutex_init(&conn->work_lock);
    conn->conn_uptime = qdr_core_uptime_ticks(core);

    if (context_binder) {
        context_binder(conn, bind_token);
    }

    set_safe_ptr_qdr_connection_t(conn, &action->args.connection.conn);
    action->args.connection.connection_label = qdr_field(label);
    action->args.connection.container_id     = qdr_field(connection_info->container);
    if (qd_log_enabled(LOG_PROTOCOL, QD_LOG_DEBUG)) {
        action->args.connection.enable_protocol_trace = true;
    }
    qdr_action_enqueue(core, action);

    char   props_str[1000];
    size_t props_len = 1000;

    pn_data_format(connection_info->connection_properties, props_str, &props_len);

    // High frequency log message, but still needs to show at INFO level for non-normal(non-client) connections like
    // inter-router connections and inter-edge connections etc.
    // Normal client connections will log at DEBUG level since these are high frequency log messages.
    qd_log(LOG_ROUTER_CORE, conn->role == QDR_ROLE_NORMAL ? QD_LOG_DEBUG : QD_LOG_INFO,
           "[C%" PRIu64
           "] Connection Opened: dir=%s host=%s encrypted=%s"
           " auth=%s user=%s container_id=%s props=%s",
           management_id, incoming ? "in" : "out", connection_info->host,
           connection_info->is_encrypted ? connection_info->tls_proto : "no",
           connection_info->is_authenticated ? connection_info->sasl_mechanisms : "no", connection_info->user,
           connection_info->container, props_str);

    return conn;
}

void qdr_connection_set_tracing(qdr_connection_t *conn, bool enable_protocol_trace)
{
    qdr_action_t *action = qdr_action(qdr_connection_set_tracing_CT, "connection_tracing_on");
    set_safe_ptr_qdr_connection_t(conn, &action->args.connection.conn);
    action->args.connection.enable_protocol_trace = enable_protocol_trace;
    qdr_action_enqueue(conn->core, action);
}

void qdr_connection_notify_closed(qdr_connection_t *conn)
{
    qdr_action_t *action = qdr_action(qdr_connection_notify_closed_CT, "connection_notify_closed");
    set_safe_ptr_qdr_connection_t(conn, &action->args.connection.conn);
    qdr_action_enqueue(conn->core, action);
}

bool qdr_connection_route_container(qdr_connection_t *conn)
{
    return conn->role == QDR_ROLE_ROUTE_CONTAINER;
}


void qdr_connection_set_context(qdr_connection_t *conn, void *context)
{
    if (conn) {
        conn->user_context = context;
    }
}

qdr_connection_info_t *qdr_connection_info(bool             is_encrypted,
                                           bool             is_authenticated,
                                           bool             opened,
                                           char            *sasl_mechanisms,
                                           qd_direction_t   dir,
                                           const char      *host,
                                           const char      *tls_proto,
                                           const char      *tls_cipher,
                                           const char      *user,
                                           const char      *container,
                                           pn_data_t       *connection_properties,
                                           uint64_t         tls_ordinal,
                                           int              tls_ssf,
                                           bool             tls,
                                           const char      *version,
                                           bool             streaming_links,
                                           bool             connection_trunking)
{
    qdr_connection_info_t *connection_info = new_qdr_connection_info_t();
    ZERO(connection_info);
    connection_info->is_encrypted          = is_encrypted;
    connection_info->is_authenticated      = is_authenticated;
    connection_info->opened                = opened;
    connection_info->container             = strdup(!!container ? container : "");
    if (sasl_mechanisms)
        connection_info->sasl_mechanisms = strdup(sasl_mechanisms);
    connection_info->dir = dir;
    if (host)
        connection_info->host = strdup(host);
    if (user)
        connection_info->user = strdup(user);
    if (version)
        connection_info->version = strdup(version);

    pn_data_t *qdr_conn_properties = pn_data(0);
    if (connection_properties)
        pn_data_copy(qdr_conn_properties, connection_properties);

    connection_info->connection_properties = qdr_conn_properties;
    connection_info->streaming_links       = streaming_links;
    connection_info->connection_trunking   = connection_trunking;

    if (tls) {
        connection_info->tls         = true;
        connection_info->tls_ssf     = tls_ssf;
        connection_info->tls_ordinal = tls_ordinal;
        connection_info->tls_proto   = !!tls_proto ? strdup(tls_proto) : 0;
        connection_info->tls_cipher  = !!tls_cipher ? strdup(tls_cipher) : 0;
    }
    sys_mutex_init(&connection_info->connection_info_lock);
    return connection_info;
}


void qdr_connection_info_set_group(qdr_connection_info_t *info, const char *correlator, uint64_t ordinal)
{
    info->group_ordinal = ordinal;
    memcpy(info->group_correlator, correlator, QD_DISCRIMINATOR_SIZE);
}


void qdr_connection_info_set_tls(qdr_connection_info_t *conn_info, bool enabled, char *version, char *ciphers, int ssf)
{
    //
    // Lock using the connection_info_lock before setting the values on the
    // connection_info. This same lock is being used in the agent_connection.c's qdr_connection_insert_column_CT
    //
    sys_mutex_lock(&conn_info->connection_info_lock);
    free(conn_info->tls_cipher);
    free(conn_info->tls_proto);
    conn_info->tls = enabled;
    conn_info->is_encrypted = enabled;
    if (enabled) {
        conn_info->tls_proto  = version;
        conn_info->tls_cipher = ciphers;
        conn_info->tls_ssf    = ssf;
    } else {
        assert(!version && !ciphers);
        conn_info->tls_cipher = 0;
        conn_info->tls_proto  = 0;
        conn_info->tls_ssf    = 0;
    }
    sys_mutex_unlock(&conn_info->connection_info_lock);
}


static void qdr_connection_info_free(qdr_connection_info_t *ci)
{
    free(ci->container);
    free(ci->sasl_mechanisms);
    free(ci->host);
    free(ci->tls_proto);
    free(ci->tls_cipher);
    free(ci->user);
    free(ci->version);
    sys_mutex_free(&ci->connection_info_lock);
    pn_data_free(ci->connection_properties);
    free_qdr_connection_info_t(ci);
}


qdr_connection_role_t qdr_connection_role(const qdr_connection_t *conn)
{
    return conn->role;
}

void *qdr_connection_get_context(const qdr_connection_t *conn)
{
    return conn ? conn->user_context : NULL;
}

void qdr_record_link_credit(qdr_core_t *core, qdr_link_t *link)
{
    //
    // Get Proton's view of this link's available credit.
    //
    if (link && link->conn && link->conn->protocol_adaptor) {
        int pn_credit = link->conn->protocol_adaptor->get_credit_handler(link->conn->protocol_adaptor->user_context, link);

        if (link->credit_reported > 0 && pn_credit == 0) {
            //
            // The link has transitioned from positive credit to zero credit.
            //
            link->zero_credit_time = qdr_core_uptime_ticks(core);
        } else if (link->credit_reported == 0 && pn_credit > 0) {
            //
            // The link has transitioned from zero credit to positive credit.
            // Clear the recorded time.
            //
            link->zero_credit_time = 0;
            if (link->reported_as_blocked) {
                link->reported_as_blocked = false;
                core->links_blocked--;
            }
        }

        link->credit_reported = pn_credit;
    }
}


void qdr_close_connection_CT(qdr_core_t *core, qdr_connection_t  *conn)
{
    bool conn_already_closed = false;
    sys_mutex_lock(&conn->work_lock);
    //
    // In some cases, this function can be called on an already closed connection in which case
    // we don't want to leak a qdr_error_t object
    //
    if (conn->closed) {
        conn_already_closed = true;
    } else {
        conn->closed = true;
        if (!conn->error) {
            // Allow caller to override the default error condition
            conn->error  = qdr_error(QD_AMQP_COND_CONNECTION_FORCED, "Connection forced-closed by management request");
        }
    }

    conn->admin_status = QD_CONN_ADMIN_DELETED;
    sys_mutex_unlock(&conn->work_lock);

    if (!conn_already_closed) {
        // Activate the connection, so the I/O threads can finish the job.
        qdr_connection_activate_CT(core, conn);
    }
}


static void qdr_core_close_connection_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t  *conn = safe_deref_qdr_connection_t(action->args.connection.conn);

    if (discard || !conn)
        return;

    qdr_close_connection_CT(core, conn);
}



void qdr_core_close_connection(qdr_connection_t *conn)
{
    qdr_action_t *action = qdr_action(qdr_core_close_connection_CT, "qdr_core_close_connection");
    set_safe_ptr_qdr_connection_t(conn, &action->args.connection.conn);
    qdr_action_enqueue(conn->core, action);
}


int qdr_connection_process(qdr_connection_t *conn)
{
    if (!conn)
        return 0;
    qdr_connection_work_list_t  work_list;
    qdr_link_ref_list_t         links_with_work[QDR_N_PRIORITIES];
    qdr_core_t                 *core = conn->core;
    int                         event_count = 0;

    qdr_link_ref_t *ref;
    qdr_link_t     *link;
    bool            detach_sent;

    sys_mutex_lock(&conn->work_lock);

    if (conn->closed) {
        sys_mutex_unlock(&conn->work_lock);
        // Unlock is called before calling into the conn_close_handler.
        // The handler's pluggable and we don't control which locks it takes (or may take in the future).
        conn->protocol_adaptor->conn_close_handler(conn->protocol_adaptor->user_context, conn, conn->error);
        return 0;
    }

    DEQ_MOVE(conn->work_list, work_list);
    for (int priority = 0; priority <= QDR_MAX_PRIORITY; ++ priority) {
        DEQ_MOVE(conn->links_with_work[priority], links_with_work[priority]);

        //
        // Move the references from CLASS_WORK to CLASS_LOCAL so concurrent action in the core
        // thread doesn't assume these links are referenced from the connection's list.
        //
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            move_link_ref(ref->link, QDR_LINK_LIST_CLASS_WORK, QDR_LINK_LIST_CLASS_LOCAL);
            ref->link->processing = true;
            ref = DEQ_NEXT(ref);
        }
    }
    sys_mutex_unlock(&conn->work_lock);

    event_count += DEQ_SIZE(work_list);
    qdr_connection_work_t *work = DEQ_HEAD(work_list);
    while (work) {
        DEQ_REMOVE_HEAD(work_list);

        switch (work->work_type) {
        case QDR_CONNECTION_WORK_FIRST_ATTACH :
            conn->protocol_adaptor->first_attach_handler(conn->protocol_adaptor->user_context, conn, work->link, work->source, work->target, work->ssn_class);
            break;

        case QDR_CONNECTION_WORK_SECOND_ATTACH :
            conn->protocol_adaptor->second_attach_handler(conn->protocol_adaptor->user_context, work->link, work->source, work->target);
            break;

        case QDR_CONNECTION_WORK_TRACING_ON :
            conn->protocol_adaptor->conn_trace_handler(conn->protocol_adaptor->user_context, conn, true);
            break;

        case QDR_CONNECTION_WORK_TRACING_OFF :
            conn->protocol_adaptor->conn_trace_handler(conn->protocol_adaptor->user_context, conn, false);
            break;

        }

        qdr_connection_work_free_CT(work);
        work = DEQ_HEAD(work_list);
    }

    // Process the links_with_work array from highest to lowest priority.
    for (int priority = QDR_MAX_PRIORITY; priority >= 0; -- priority) {
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            qdr_link_work_t *link_work;
            detach_sent = false;
            link = ref->link;

            //
            // The work lock must be used to protect accesses to the link's work_list and
            // link_work->processing.
            //
            sys_mutex_lock(&conn->work_lock);
            link_work = DEQ_HEAD(link->work_list);
            if (link_work) {
                // link_work ref transferred to local link_work
                DEQ_REMOVE_HEAD(link->work_list);
                link_work->processing = true;
            }
            sys_mutex_unlock(&conn->work_lock);

            //
            // Handle disposition/settlement updates
            //
            qdr_delivery_ref_list_t updated_deliveries;
            sys_mutex_lock(&conn->work_lock);
            DEQ_MOVE(link->updated_deliveries, updated_deliveries);
            sys_mutex_unlock(&conn->work_lock);

            qdr_delivery_ref_t *dref = DEQ_HEAD(updated_deliveries);
            while (dref) {
                conn->protocol_adaptor->delivery_update_handler(conn->protocol_adaptor->user_context, dref->dlv, dref->dlv->disposition, dref->dlv->settled);
                qdr_delivery_decref(core, dref->dlv, "qdr_connection_process - remove from updated list");
                qdr_del_delivery_ref(&updated_deliveries, dref);
                dref = DEQ_HEAD(updated_deliveries);
                event_count++;
            }

            while (link_work) {
                switch (link_work->work_type) {
                case QDR_LINK_WORK_DELIVERY :
                    {
                        int count = conn->protocol_adaptor->push_handler(conn->protocol_adaptor->user_context, link, link_work->value);
                        assert(count <= link_work->value);
                        link_work->value -= count;
                        break;
                    }

                case QDR_LINK_WORK_FLOW :
                    if (link_work->value > 0)
                        conn->protocol_adaptor->flow_handler(conn->protocol_adaptor->user_context, link, link_work->value);
                    if      (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_SET)
                        conn->protocol_adaptor->drain_handler(conn->protocol_adaptor->user_context, link, true);
                    else if (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_CLEAR)
                        conn->protocol_adaptor->drain_handler(conn->protocol_adaptor->user_context, link, false);
                    else if (link_work->drain_action == QDR_LINK_WORK_DRAIN_ACTION_DRAINED)
                        conn->protocol_adaptor->drained_handler(conn->protocol_adaptor->user_context, link);
                    break;

                case QDR_LINK_WORK_FIRST_DETACH :
                case QDR_LINK_WORK_SECOND_DETACH :
                    conn->protocol_adaptor->detach_handler(conn->protocol_adaptor->user_context, link, link_work->error,
                                                           link_work->work_type == QDR_LINK_WORK_FIRST_DETACH);
                    detach_sent = true;
                    break;
                }

                sys_mutex_lock(&conn->work_lock);
                if (link_work->work_type == QDR_LINK_WORK_DELIVERY && link_work->value > 0) {
                    // link_work ref transferred from link_work to work_list
                    DEQ_INSERT_HEAD(link->work_list, link_work);
                    link_work->processing = false;
                    link_work = 0; // Halt work processing
                } else {
                    qdr_link_work_release(link_work);
                    link_work = DEQ_HEAD(link->work_list);
                    if (link_work) {
                        // link_work ref transferred to local link_work
                        DEQ_REMOVE_HEAD(link->work_list);
                        link_work->processing = true;
                    }
                }
                sys_mutex_unlock(&conn->work_lock);
                event_count++;
            }

            if (!detach_sent) {
                qdr_record_link_credit(core, link);
            }

            ref = DEQ_NEXT(ref);
        }
    }

    sys_mutex_lock(&conn->work_lock);
    for (int priority = QDR_MAX_PRIORITY; priority >= 0; -- priority) {
        ref = DEQ_HEAD(links_with_work[priority]);
        while (ref) {
            qdr_link_t *link = ref->link;

            link->processing = false;
            if (link->ready_to_free)
                qdr_link_processing_complete(core, link);

            qdr_del_link_ref(links_with_work + priority, ref->link, QDR_LINK_LIST_CLASS_LOCAL);
            ref = DEQ_HEAD(links_with_work[priority]);
        }
    }
    sys_mutex_unlock(&conn->work_lock);

    return event_count;
}


void qdr_link_set_context(qdr_link_t *link, void *context)
{
    if (link) {
        if (context == 0) {
            if (link->user_context) {
                qd_nullify_safe_ptr((qd_alloc_safe_ptr_t *)link->user_context);
                free(link->user_context);
                link->user_context = 0;
            }
        }
        else {
            if (link->user_context) {
                qd_nullify_safe_ptr((qd_alloc_safe_ptr_t *)link->user_context);
                free(link->user_context);
            }

            qd_alloc_safe_ptr_t *safe_ptr = NEW(qd_alloc_safe_ptr_t);
            qd_alloc_set_safe_ptr(safe_ptr, context);
            link->user_context = safe_ptr;
        }
    }
}


void *qdr_link_get_context(const qdr_link_t *link)
{
    if (link) {
        if (link->user_context) {
            qd_alloc_safe_ptr_t *safe_qdl = (qd_alloc_safe_ptr_t*) link->user_context;
            if (safe_qdl)
                return qd_alloc_deref_safe_ptr(safe_qdl);
        }
    }

    return 0;
}


qd_link_type_t qdr_link_type(const qdr_link_t *link)
{
    return link->link_type;
}


qd_direction_t qdr_link_direction(const qdr_link_t *link)
{
    return link->link_direction;
}


const char *qdr_link_internal_address(const qdr_link_t *link)
{
    return link && link->auto_link ? link->auto_link->internal_addr : 0;
}


bool qdr_link_is_anonymous(const qdr_link_t *link)
{
    return link->owning_addr == 0;
}


bool qdr_link_is_core_endpoint(const qdr_link_t *link)
{
    return link->core_endpoint != 0;
}


bool qdr_link_strip_annotations_in(const qdr_link_t *link)
{
    return link->strip_annotations_in;
}


bool qdr_link_strip_annotations_out(const qdr_link_t *link)
{
    return link->strip_annotations_out;
}


void qdr_link_stalled_outbound(qdr_link_t *link)
{
    link->stalled_outbound = true;
}


void qdr_link_set_user_streaming(qdr_link_t *link)
{
    link->user_streaming = true;
}


const char *qdr_link_name(const qdr_link_t *link)
{
    return link->name;
}


static void qdr_link_setup_histogram(qdr_connection_t *conn, qd_direction_t dir, qdr_link_t *link)
{
    if (dir == QD_OUTGOING && conn->role != QDR_ROLE_INTER_ROUTER) {
        link->ingress_histogram = NEW_ARRAY(uint64_t, qd_bitmask_width());
        for (int i = 0; i < qd_bitmask_width(); i++)
            link->ingress_histogram[i] = 0;
    }
}


// used by the TSAN suppression file to mask the read/write races
// caused by modifying the deliveries' conn_id and link_id while in flight
void tsan_reset_delivery_ids(qdr_delivery_t *dlv, uint64_t conn_id, uint64_t link_id)
{
    dlv->conn_id = conn_id;
    dlv->link_id = link_id;
}


qdr_link_t *qdr_link_first_attach(qdr_connection_t *conn,
                                  qd_direction_t    dir,
                                  qdr_terminus_t   *source,
                                  qdr_terminus_t   *target,
                                  const char       *name,
                                  const char       *terminus_addr,
                                  bool              no_route,
                                  qdr_delivery_t   *initial_delivery,
                                  uint64_t         *link_id)
{
    qdr_action_t   *action         = qdr_action(qdr_link_inbound_first_attach_CT, "link_first_attach");
    qdr_link_t     *link           = new_qdr_link_t();
    qdr_terminus_t *local_terminus = dir == QD_OUTGOING ? source : target;

    ZERO(link);
    link->core = conn->core;
    link->identity = qdr_identifier(conn->core);
    *link_id = link->identity;
    link->conn = conn;
    link->conn_id = conn->identity;
    link->name = (char*) malloc(strlen(name) + 1);

    if (terminus_addr) {
         char *term_addr = malloc((strlen(terminus_addr) + 2) * sizeof(char));
         term_addr[0] = '\0';
         strcat(term_addr, "M");
         strcat(term_addr, terminus_addr);
         link->terminus_addr = term_addr;
    }

    strcpy(link->name, name);
    link->link_direction = dir;
    link->state          = QDR_LINK_STATE_UNINIT;  // transition to first attach occurs on core thread
    link->capacity       = conn->link_capacity;
    link->credit_pending = conn->link_capacity;
    link->oper_status    = QDR_LINK_OPER_DOWN;
    link->core_ticks     = qdr_core_uptime_ticks(conn->core);
    link->zero_credit_time = link->core_ticks;
    SET_ATOMIC_BOOL(&link->streaming_deliveries, dir == QD_INCOMING && qdr_terminus_has_capability(target, QD_CAPABILITY_STREAMING_DELIVERIES));
    link->resend_released_deliveries           = dir == QD_INCOMING && qdr_terminus_has_capability(target, QD_CAPABILITY_RESEND_RELEASED);
    link->no_route = no_route;
    link->priority = QDR_DEFAULT_PRIORITY;

    link->strip_annotations_in  = conn->strip_annotations_in;
    link->strip_annotations_out = conn->strip_annotations_out;

    //
    // Adjust the delivery's identity
    //

    if (initial_delivery) {
        tsan_reset_delivery_ids(initial_delivery, link->conn->identity, link->identity);
    }

    if      (qdr_terminus_has_capability(local_terminus, QD_CAPABILITY_ROUTER_CONTROL)) {
        link->link_type = QD_LINK_CONTROL;
        link->priority = QDR_MAX_PRIORITY;
    } else if (qdr_terminus_has_capability(local_terminus, QD_CAPABILITY_ROUTER_DATA)) {
        link->link_type = QD_LINK_ROUTER;
    } else if (qdr_terminus_has_capability(local_terminus, QD_CAPABILITY_INTER_EDGE)) {
        link->link_type = QD_LINK_INTER_EDGE;
    } else if (qdr_terminus_has_capability(local_terminus, QD_CAPABILITY_EDGE_DOWNLINK)) {
        if (conn->core->router_mode == QD_ROUTER_MODE_INTERIOR &&
            conn->role == QDR_ROLE_EDGE_CONNECTION &&
            dir == QD_OUTGOING)
            link->link_type = QD_LINK_EDGE_DOWNLINK;
    }

    qdr_link_setup_histogram(conn, dir, link);

    set_safe_ptr_qdr_connection_t(conn, &action->args.connection.conn);
    set_safe_ptr_qdr_link_t(link, &action->args.connection.link);
    action->args.connection.dir    = dir;
    action->args.connection.source = source;
    action->args.connection.target = target;
    action->args.connection.initial_delivery = initial_delivery;
    if (!!initial_delivery)
        qdr_delivery_incref(initial_delivery, "qdr_link_first_attach - protect delivery in action list");
    qdr_action_enqueue(conn->core, action);

    return link;
}


void qdr_link_second_attach(qdr_link_t *link, qdr_terminus_t *source, qdr_terminus_t *target)
{
    qdr_action_t *action = qdr_action(qdr_link_inbound_second_attach_CT, "link_second_attach");

    set_safe_ptr_qdr_connection_t(link->conn, &action->args.connection.conn);
    set_safe_ptr_qdr_link_t(link, &action->args.connection.link);

    // ownership of source/target passed to core, core must free them when done
    action->args.connection.source = source;
    action->args.connection.target = target;
    qdr_action_enqueue(link->core, action);
}


void qdr_link_detach_received(qdr_link_t *link, qdr_error_t *error)
{
    qdr_action_t *action = qdr_action(qdr_link_inbound_detach_CT, "link_detach_received");

    set_safe_ptr_qdr_connection_t(link->conn, &action->args.connection.conn);
    set_safe_ptr_qdr_link_t(link, &action->args.connection.link);
    action->args.connection.error  = error;
    qdr_action_enqueue(link->core, action);
}


void qdr_link_notify_closed(qdr_link_t *link, bool forced)
{
    qdr_action_t *action = qdr_action(qdr_link_notify_closed_CT, "link_notify_closed");

    set_safe_ptr_qdr_link_t(link, &action->args.connection.link);
    action->args.connection.forced_close = forced;
    qdr_action_enqueue(link->core, action);
}


static void qdr_link_processing_complete(qdr_core_t *core, qdr_link_t *link)
{
    qdr_action_t *action = qdr_action(qdr_link_processing_complete_CT, "link_processing_complete");

    set_safe_ptr_qdr_link_t(link, &action->args.connection.link);
    qdr_action_enqueue(core, action);
}



//==================================================================================
// In-Thread Functions
//==================================================================================

void qdr_connection_activate_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    if (!conn->in_activate_list) {
        DEQ_INSERT_TAIL_N(ACTIVATE, core->connections_to_activate, conn);
        conn->in_activate_list = true;
    }
}


void qdr_connection_enqueue_work_CT(qdr_core_t            *core,
                                    qdr_connection_t      *conn,
                                    qdr_connection_work_t *work)
{
    sys_mutex_lock(&conn->work_lock);
    DEQ_INSERT_TAIL(conn->work_list, work);
    bool notify = DEQ_SIZE(conn->work_list) == 1;
    sys_mutex_unlock(&conn->work_lock);

    if (notify)
        qdr_connection_activate_CT(core, conn);
}


void qdr_link_enqueue_work_CT(qdr_core_t      *core,
                              qdr_link_t      *link,
                              qdr_link_work_t *work)
{
    qdr_connection_t *conn = link->conn;

    sys_mutex_lock(&conn->work_lock);
    // expect: caller transfers refcount:
    assert(sys_atomic_get(&work->ref_count) > 0);
    DEQ_INSERT_TAIL(link->work_list, work);
    qdr_add_link_ref(&conn->links_with_work[link->priority], link, QDR_LINK_LIST_CLASS_WORK);
    sys_mutex_unlock(&conn->work_lock);

    qdr_connection_activate_CT(core, conn);
}


/**
 * Generate a link name
 */
static void qdr_generate_link_name(const char *label, char *buffer, size_t length)
{
    char discriminator[QD_DISCRIMINATOR_SIZE];
    qd_generate_discriminator(discriminator);
    snprintf(buffer, length, "%s.%s", label, discriminator);
}


void qdr_link_cleanup_deliveries_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link, bool on_shutdown)
{
    //
    // Clean up the lists of deliveries on this link
    //
    qdr_delivery_ref_list_t updated_deliveries;
    qdr_delivery_list_t     undelivered;
    qdr_delivery_list_t     unsettled;
    qdr_delivery_list_t     settled;

    sys_mutex_lock(&conn->work_lock);
    DEQ_MOVE(link->updated_deliveries, updated_deliveries);

    DEQ_MOVE(link->undelivered, undelivered);
    qdr_delivery_t *d = DEQ_HEAD(undelivered);
    while (d) {
        assert(d->where == QDR_DELIVERY_IN_UNDELIVERED);
        if (d->presettled)
            core->dropped_presettled_deliveries++;
        d->where = QDR_DELIVERY_NOWHERE;
        if (on_shutdown)
            d->tracking_addr = 0;
        qdr_link_work_release(d->link_work);
        d->link_work = 0;
        d = DEQ_NEXT(d);
    }

    DEQ_MOVE(link->unsettled, unsettled);
    d = DEQ_HEAD(unsettled);
    while (d) {
        assert(d->where == QDR_DELIVERY_IN_UNSETTLED);
        d->where = QDR_DELIVERY_NOWHERE;
        qdr_link_work_release(d->link_work);
        d->link_work = 0;
        if (on_shutdown)
            d->tracking_addr = 0;
        d = DEQ_NEXT(d);
    }

    DEQ_MOVE(link->settled, settled);
    d = DEQ_HEAD(settled);
    while (d) {
        assert(d->where == QDR_DELIVERY_IN_SETTLED);
        d->where = QDR_DELIVERY_NOWHERE;
        qdr_link_work_release(d->link_work);
        d->link_work = 0;
        if (on_shutdown)
            d->tracking_addr = 0;
        d = DEQ_NEXT(d);
    }
    sys_mutex_unlock(&conn->work_lock);

    //
    // Free all the 'updated' references
    //
    qdr_delivery_ref_t *ref = DEQ_HEAD(updated_deliveries);
    while (ref) {
        //
        // Updates global and link level delivery counters like presettled_deliveries, accepted_deliveries, released_deliveries etc
        //
        qdr_delivery_increment_counters_CT(core, ref->dlv);
        qd_nullify_safe_ptr(&ref->dlv->link_sp);
        //
        // Now our reference
        //
        qdr_delivery_decref_CT(core, ref->dlv, "qdr_link_cleanup_deliveries_CT - remove from updated list");
        qdr_del_delivery_ref(&updated_deliveries, ref);
        ref = DEQ_HEAD(updated_deliveries);
    }

    //
    // Free the undelivered deliveries.  If this is an incoming link, the
    // undelivereds can simply be destroyed.  If it's an outgoing link, the
    // undelivereds' peer deliveries need to be released.
    //
    qdr_delivery_t *dlv = DEQ_HEAD(undelivered);
    qdr_delivery_t *peer;
    while (dlv) {
        DEQ_REMOVE_HEAD(undelivered);

        // expect: an inbound undelivered multicast should
        // have no peers (has not been forwarded yet)
        assert(dlv->multicast
               ? qdr_delivery_peer_count_CT(dlv) == 0
               : true);

        peer = qdr_delivery_first_peer_CT(dlv);
        while (peer) {
            if (peer->multicast) {
                //
                // dlv is outgoing mcast - tell its incoming peer that it has
                // been released and settled.  This will unlink these peers.
                //
                qdr_delivery_mcast_outbound_update_CT(core, peer, dlv, PN_RELEASED, true);
            }
            else {
                qdr_delivery_release_CT(core, peer);
                qdr_delivery_unlink_peers_CT(core, dlv, peer);
            }
            peer = qdr_delivery_next_peer_CT(dlv);
        }

        //
        // Updates global and link level delivery counters like presettled_deliveries, accepted_deliveries, released_deliveries etc
        //
        qdr_delivery_increment_counters_CT(core, dlv);
        qd_nullify_safe_ptr(&dlv->link_sp);

        //
        // Now the undelivered-list reference
        //
        qdr_delivery_decref_CT(core, dlv, "qdr_link_cleanup_deliveries_CT - remove from undelivered list");

        dlv = DEQ_HEAD(undelivered);
    }

    //
    // Free the unsettled deliveries.
    //
    dlv = DEQ_HEAD(unsettled);
    while (dlv) {
        DEQ_REMOVE_HEAD(unsettled);

        if (dlv->tracking_addr) {
            dlv->tracking_addr->outstanding_deliveries[dlv->tracking_addr_bit]--;
            dlv->tracking_addr->tracked_deliveries--;

            if (dlv->tracking_addr->tracked_deliveries == 0)
                qdr_check_addr_CT(core, dlv->tracking_addr);

            dlv->tracking_addr = 0;
        }

        if (!qdr_delivery_receive_complete(dlv)) {
            qdr_delivery_set_aborted(dlv);
            qdr_delivery_continue_peers_CT(core, dlv, false);
        }

        if (dlv->multicast) {
            //
            // forward settlement
            //
            qdr_delivery_mcast_inbound_update_CT(core, dlv,
                                                 PN_MODIFIED,
                                                 true);  // true == settled
        } else {
            peer = qdr_delivery_first_peer_CT(dlv);
            while (peer) {
                if (peer->multicast) {
                    //
                    // peer is incoming multicast and dlv is one of its corresponding
                    // outgoing deliveries.  This will unlink these peers.
                    //
                    qdr_delivery_mcast_outbound_update_CT(core, peer, dlv, PN_MODIFIED, true);
                } else {
                    if (link->link_direction == QD_OUTGOING)
                        qdr_delivery_failed_CT(core, peer);
                    qdr_delivery_unlink_peers_CT(core, dlv, peer);
                }
                peer = qdr_delivery_next_peer_CT(dlv);
            }
        }

        //
        // Updates global and link level delivery counters like presettled_deliveries, accepted_deliveries, released_deliveries etc
        //
        qdr_delivery_increment_counters_CT(core, dlv);
        qd_nullify_safe_ptr(&dlv->link_sp);

        //
        // Now the unsettled-list reference
        //
        qdr_delivery_decref_CT(core, dlv, "qdr_link_cleanup_deliveries_CT - remove from unsettled list");

        dlv = DEQ_HEAD(unsettled);
    }

    //Free/unlink/decref the settled deliveries.
    dlv = DEQ_HEAD(settled);
    while (dlv) {
        DEQ_REMOVE_HEAD(settled);

        if (!qdr_delivery_receive_complete(dlv)) {
            qdr_delivery_set_aborted(dlv);
            qdr_delivery_continue_peers_CT(core, dlv, false);
        }

        peer = qdr_delivery_first_peer_CT(dlv);
        qdr_delivery_t *next_peer = 0;
        while (peer) {
            next_peer = qdr_delivery_next_peer_CT(dlv);
            qdr_delivery_unlink_peers_CT(core, dlv, peer);
            peer = next_peer;
        }

        //
        // Updates global and link level delivery counters like presettled_deliveries, accepted_deliveries, released_deliveries etc
        //
        qdr_delivery_increment_counters_CT(core, dlv);
        qd_nullify_safe_ptr(&dlv->link_sp);

        // This decref is for the removing the delivery from the settled list
        qdr_delivery_decref_CT(core, dlv, "qdr_link_cleanup_deliveries_CT - remove from settled list");
        dlv = DEQ_HEAD(settled);
    }
}


static void qdr_link_cleanup_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link, const char *log_text)
{
    // expect: If this link is owned by an auto_link it should have been released during link detach cleanup
    assert(link->auto_link == 0);

    //
    // Remove the link from the overall list of links and possibly the streaming
    // link pool
    //
    DEQ_REMOVE(core->open_links, link);

    //
    // If the link has a core_endpoint, allow the core_endpoint module to
    // clean up its state
    //
    if (link->core_endpoint)
        qdrc_endpoint_do_cleanup_CT(core, link->core_endpoint);

    //
    // If this link is involved in inter-router communication, remove its reference
    // from the core mask-bit tables
    //
    if (qd_bitmask_valid_bit_value(conn->mask_bit)) {
        if (link->link_type == QD_LINK_CONTROL)
            conn->control_links[link->link_direction] = 0;
        if (link->link_type == QD_LINK_ROUTER) {
            if (link == conn->data_links.link[link->priority])
                conn->data_links.link[link->priority] = 0;
        }
    }

    //
    // Clean up the work list
    //
    qdr_link_work_list_t work_list;

    sys_mutex_lock(&conn->work_lock);
    DEQ_MOVE(link->work_list, work_list);
    sys_mutex_unlock(&conn->work_lock);

    //
    // Free the work list
    //
    qdr_link_work_t *link_work = DEQ_HEAD(work_list);
    while (link_work) {
        DEQ_REMOVE_HEAD(work_list);
        qdr_link_work_release(link_work);
        link_work = DEQ_HEAD(work_list);
    }

    //
    // Clean up any remaining deliveries
    //
    qdr_link_cleanup_deliveries_CT(core, conn, link, false);

    //
    // Remove all references to this link in the connection's and owning
    // address reference lists
    //
    sys_mutex_lock(&conn->work_lock);
    qdr_del_link_ref(&conn->links, link, QDR_LINK_LIST_CLASS_CONNECTION);
    qdr_del_link_ref(&conn->links_with_work[link->priority], link, QDR_LINK_LIST_CLASS_WORK);
    sys_mutex_unlock(&conn->work_lock);

    if (link->ref[QDR_LINK_LIST_CLASS_ADDRESS]) {
        assert(link->owning_addr);
        qdr_del_link_ref((link->link_direction == QD_OUTGOING)
                         ? &link->owning_addr->rlinks
                         : &link->owning_addr->inlinks,
                         link,  QDR_LINK_LIST_CLASS_ADDRESS);
    }

    if (link->in_streaming_pool) {
        DEQ_REMOVE_N(STREAMING_POOL, conn->streaming_link_pool, link);
        link->in_streaming_pool = false;
    }

    //
    // Free the link's name and terminus_addr
    //
    free(link->name);
    free(link->disambiguated_name);
    free(link->terminus_addr);
    free(link->ingress_histogram);
    free(link->insert_prefix);
    free(link->strip_prefix);

    //
    // Log the link closure.
    // These log messages flood the router log since many of these
    // links are attached and detached in an environment with high connection rates.
    // Setting this log to QD_LOG_DEBUG.
    //
    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%" PRIu64 "][L%" PRIu64 "] %s: del=%" PRIu64 " presett=%" PRIu64 " psdrop=%" PRIu64 " acc=%" PRIu64
           " rej=%" PRIu64 " rel=%" PRIu64 " mod=%" PRIu64 " delay1=%" PRIu64 " delay10=%" PRIu64 " blocked=%s",
           conn->identity, link->identity, log_text, link->total_deliveries, link->presettled_deliveries,
           link->dropped_presettled_deliveries, link->accepted_deliveries, link->rejected_deliveries,
           link->released_deliveries, link->modified_deliveries, link->deliveries_delayed_1sec,
           link->deliveries_delayed_10sec, link->reported_as_blocked ? "yes" : "no");

    if (link->reported_as_blocked)
        core->links_blocked--;
    if (link->user_context) {
        qdr_link_set_context(link, 0);
    }
    free_qdr_link_t(link);
}


static void qdr_link_cleanup_protected_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link, const char *label)
{
    bool do_cleanup = false;

    sys_mutex_lock(&conn->work_lock);
    // prevent an I/O thread from processing this link (DISPATCH-1475)
    qdr_del_link_ref(&conn->links_with_work[link->priority], link, QDR_LINK_LIST_CLASS_WORK);
    if (link->processing) {
        // Cannot cleanup link because I/O thread is currently processing it
        // Mark it so the I/O thread will notify the core when processing is complete
        link->ready_to_free = true;
    }
    else
        do_cleanup = true;
    sys_mutex_unlock(&conn->work_lock);

    if (do_cleanup)
        qdr_link_cleanup_CT(core, conn, link, label);
}


qdr_link_t *qdr_create_link_CT(qdr_core_t        *core,
                               qdr_connection_t  *conn,
                               qd_link_type_t     link_type,
                               qd_direction_t     dir,
                               qdr_terminus_t    *source,
                               qdr_terminus_t    *target,
                               qd_session_class_t ssn_class,
                               uint8_t            priority)
{
    //
    // Create a new link, initiated by the router core.  This will involve issuing a first-attach outbound.
    //
    qdr_link_t *link = new_qdr_link_t();
    ZERO(link);

    link->core           = core;
    link->identity       = qdr_identifier(core);
    link->user_context   = 0;
    link->conn           = conn;
    link->conn_id        = conn->identity;
    link->link_type      = link_type;
    link->link_direction = dir;
    link->state          = QDR_LINK_STATE_ATTACH_SENT;
    link->capacity       = conn->link_capacity;
    link->credit_pending = conn->link_capacity;
    link->name           = (char*) malloc(QD_DISCRIMINATOR_SIZE + 8);
    link->disambiguated_name = 0;
    link->terminus_addr  = 0;
    qdr_generate_link_name("qdlink", link->name, QD_DISCRIMINATOR_SIZE + 8);
    link->oper_status    = QDR_LINK_OPER_DOWN;
    link->insert_prefix  = 0;
    link->strip_prefix   = 0;
    link->core_ticks     = qdr_core_uptime_ticks(core);
    link->zero_credit_time = link->core_ticks;
    link->priority       = priority;

    link->strip_annotations_in  = conn->strip_annotations_in;
    link->strip_annotations_out = conn->strip_annotations_out;

    qdr_link_setup_histogram(conn, dir, link);

    DEQ_INSERT_TAIL(core->open_links, link);
    qdr_add_link_ref(&conn->links, link, QDR_LINK_LIST_CLASS_CONNECTION);

    qdr_connection_work_t *work = new_qdr_connection_work_t();
    ZERO(work);
    work->work_type = QDR_CONNECTION_WORK_FIRST_ATTACH;
    work->link      = link;
    work->source    = source;
    work->target    = target;
    work->ssn_class = ssn_class;

    char   source_str[1000];
    char   target_str[1000];
    size_t source_len = 1000;
    size_t target_len = 1000;

    source_str[0] = '\0';
    target_str[0] = '\0';

    if (qd_log_enabled(LOG_ROUTER_CORE, QD_LOG_DEBUG)) {
        qdr_terminus_format(source, source_str, &source_len);
        qdr_terminus_format(target, target_str, &target_len);
    }

    // This floods the router log, setting it down to QD_LOG_DEBUG.
    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%" PRIu64 "][L%" PRIu64 "] Link attached: dir=%s source=%s target=%s", conn->identity, link->identity,
           dir == QD_INCOMING ? "in" : "out", source_str, target_str);

    qdr_connection_enqueue_work_CT(core, conn, work);
    return link;
}


void qdr_link_outbound_detach_CT(qdr_core_t *core, qdr_link_t *link, qdr_error_t *error, qdr_condition_t condition)
{
    assert((link->state & QDR_LINK_STATE_DETACH_SENT) == 0);
    link->state |= QDR_LINK_STATE_DETACH_SENT;

    //
    // Ensure a pooled link is no longer available for streaming messages
    //
    if (link->streaming) {
        if (link->in_streaming_pool) {
            DEQ_REMOVE_N(STREAMING_POOL, link->conn->streaming_link_pool, link);
            link->in_streaming_pool = false;
        }
    }

    //
    // tell the I/O thread to do the detach
    //

    bool first_detach = (link->state & QDR_LINK_STATE_DETACH_RECVD) == 0;  // haven't received a detach
    qdr_link_work_t *work = qdr_link_work(first_detach ? QDR_LINK_WORK_FIRST_DETACH : QDR_LINK_WORK_SECOND_DETACH);

    if (error)
        work->error = error;
    else {
        switch (condition) {
        case QDR_CONDITION_NO_ROUTE_TO_DESTINATION:
            work->error = qdr_error("qd:no-route-to-dest", "No route to the destination node");
            break;

        case QDR_CONDITION_ROUTED_LINK_LOST:
            work->error = qdr_error("qd:routed-link-lost", "Connectivity to the peer container was lost");
            break;

        case QDR_CONDITION_FORBIDDEN:
            work->error = qdr_error("qd:forbidden", "Connectivity to the node is forbidden");
            break;

        case QDR_CONDITION_WRONG_ROLE:
            work->error = qdr_error("qd:connection-role", "Link attach forbidden on inter-router connection");
            break;

        case QDR_CONDITION_INVALID_LINK_EXPIRATION:
            work->error = qdr_error("qd:link-expiration", "Requested link expiration not allowed");
            break;

        case QDR_CONDITION_NONE:
            work->error = 0;
            break;
        }
    }

    qdr_link_enqueue_work_CT(core, link, work);
}


void qdr_link_outbound_second_attach_CT(qdr_core_t *core, qdr_link_t *link, qdr_terminus_t *source, qdr_terminus_t *target)
{
    qdr_connection_work_t *work = new_qdr_connection_work_t();
    ZERO(work);
    work->work_type = QDR_CONNECTION_WORK_SECOND_ATTACH;
    work->link      = link;
    work->source    = source;
    work->target    = target;

    link->oper_status = QDR_LINK_OPER_UP;

    qdr_connection_enqueue_work_CT(core, link->conn, work);
}


qdr_address_config_t *qdr_config_for_address_CT(qdr_core_t *core, qdr_connection_t *conn, qd_iterator_t *iter)
{
    qdr_address_config_t *addr = 0;
    qd_iterator_view_t old_view = qd_iterator_get_view(iter);

    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_NO_HOST);
    qd_parse_tree_retrieve_match(core->addr_parse_tree, iter, (void **) &addr);
    qd_iterator_annotate_prefix(iter, '\0');
    qd_iterator_reset_view(iter, old_view);

    return addr;
}


qd_address_treatment_t qdr_treatment_for_address_hash_CT(qdr_core_t *core, qd_iterator_t *iter, qdr_address_config_t **addr_config)
{
    return qdr_treatment_for_address_hash_with_default_CT(core, iter, core->qd->default_treatment, addr_config);
}

qd_address_treatment_t qdr_treatment_for_address_hash_with_default_CT(qdr_core_t              *core,
                                                                      qd_iterator_t           *iter,
                                                                      qd_address_treatment_t   default_treatment,
                                                                      qdr_address_config_t   **addr_config)
{
#define HASH_STORAGE_SIZE 1000
    char  storage[HASH_STORAGE_SIZE + 1];
    char *copy    = storage;
    bool  on_heap = false;
    int   length  = qd_iterator_length(iter);
    qd_address_treatment_t trt = default_treatment;
    qdr_address_config_t *addr = 0;

    if (length > HASH_STORAGE_SIZE) {
        copy    = (char*) malloc(length + 1);
        on_heap = true;
    }

    qd_iterator_strncpy(iter, copy, length + 1);

    if (copy[0] == QD_ITER_HASH_PREFIX_MOBILE) {
        //
        // Handle the mobile address case
        //
        qd_iterator_t *config_iter = qd_iterator_string(&copy[1], ITER_VIEW_ADDRESS_NO_HOST);
        qd_parse_tree_retrieve_match(core->addr_parse_tree, config_iter, (void **) &addr);
        if (addr)
            trt = addr->treatment;
        qd_iterator_free(config_iter);
    }

    if (on_heap)
        free(copy);

    *addr_config = addr;
    return trt;
}


/**
 * Check an address to see if it no longer has any associated destinations.
 * Depending on its policy, the address may be eligible for being closed out
 * (i.e. Logging its terminal statistics and freeing its resources).
 */
void qdr_check_addr_CT(qdr_core_t *core, qdr_address_t *addr)
{
    if (addr == 0)
        return;

    //
    // If the address has no in-process consumer or destinations, it should be
    // deleted.
    //
    if (DEQ_SIZE(addr->subscriptions) == 0
        && DEQ_SIZE(addr->rlinks) == 0
        && DEQ_SIZE(addr->inlinks) == 0
        && qd_bitmask_cardinality(addr->rnodes) == 0
        && addr->ref_count == 0
        && addr->tracked_deliveries == 0
        && addr->core_endpoint == 0) {
        qdr_core_remove_address(core, addr);
    }
}


/**
 * Process local and remote attributes for this address to see if action is needed.
 * 
 * This function should be called whenever the local_sole_destination_mesh flag changes state
 * or a remote sole_destination_mesh value is changed.
 */
void qdr_process_addr_attributes_CT(qdr_core_t *core, qdr_address_t *addr)
{
    //
    // The address has a remote sole-destination-mesh if ALL of the remote routers
    // in rnodes have the same sole-destination.
    //
    bool new_value = qd_bitmask_cardinality(addr->rnodes) > 0;

    int router_bit, c;
    if (addr->remote_sole_destination_meshes == 0) {
        new_value = false;
    } else {
        for (QD_BITMASK_EACH(addr->rnodes, router_bit, c)) {
            char *ptr = addr->remote_sole_destination_meshes + (router_bit * QD_DISCRIMINATOR_BYTES);
            if (memcmp(ptr, addr->destination_mesh_id, QD_DISCRIMINATOR_BYTES) != 0) {
                new_value = false;
                break;
            }
        }
    }

    if (new_value != addr->remote_sole_destination_mesh) {
        addr->remote_sole_destination_mesh = new_value;
        qdrc_event_addr_raise(core, QDRC_EVENT_ADDR_REMOTE_CHANGED, addr);
    }
}


/** Setup a group of inter-router connections
 *
 * An inter-router connection group consists of one inter-router control connection and zero or more inter-router data
 * connections. All these connections are generated by the same connector instance and therefore terminate at the same
 * peer router.
 *
 * Connection groups are designed to scale the amount of traffic that can be sent between peer interior routers by
 * allowing multiple connections to the peer. A message forwarded to the peer can use any connection in the group,
 * which means that N messages can be forwarded concurrently (where N == the number of connections in the group).
 *
 * A group is identified by a 2-tuple consisting of the group correlator string and an integer ordinal. All connections
 * in a group share the same (correlator, ordinal) value.  The correlator is unique to the parent connector. The ordinal
 * tracks updates to the connector's configuration (e.g. certificate refresh) with the connections with the highest
 * ordinal value taking precedence over connections with lower ordinal values. The ordinal gives the router the ability
 * to "rollover" a connection group to a new set of connections.
 *
 * This function is called when the inter-router control connection opens. It is considered the group "parent"
 * connection. All inter-router data connections belonging to the group will be associated with the parent connection,
 * and are referred to as "member" connections. The routing agent is only aware of the inter-router control connection
 * therefore the group cannot exist without it.
 */
static void qdr_connection_group_setup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    const char *correlator = conn->connection_info->group_correlator;
    uint64_t    ordinal    = conn->connection_info->group_ordinal;

    assert(conn->role == QDR_ROLE_INTER_ROUTER);

    if (!!correlator[0] && qd_bitmask_valid_bit_value(conn->mask_bit)) {

        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
               "Connection group '%s' setup parent [C%"PRIu64"] with ordinal=%"PRIu64" (host=%s)",
               correlator, conn->identity, ordinal, conn->connection_info->host);

        assert(core->group_correlator_by_maskbit[conn->mask_bit][0] == '\0');
        assert(core->rnode_conns_by_mask_bit[conn->mask_bit] == conn);

        //
        // Sanity check: Expect the same group correlator is not in use by another connection.  If so there's either a
        // bug in how we manage the groups OR the globally unique discriminator is actually not unique which breaks an
        // invariant. This should not happen.
        //
        for (int mask_bit = 0; mask_bit < qd_bitmask_width(); mask_bit++) {
            if (strcmp(core->group_correlator_by_maskbit[mask_bit], correlator) == 0) {
                qdr_connection_t *old_conn = core->rnode_conns_by_mask_bit[mask_bit];
                qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
                       "Connection group duplicate correlator detected: correlator=%s conns=[C%"PRIu64"],[C%"PRIu64"]",
                       correlator, conn->identity, old_conn ? old_conn->identity : 0);
                if (old_conn) {
                    // This is what is called a "hail mary pass" - google it - in an attempt to recover from this mess
                    qdr_connection_group_cleanup_CT(core, old_conn);
                }
                assert(false);  // Fail if this is a debug build in order to catch this in CI
            }
        }

        //
        // Record the group's correlator in the core record.
        // Check the unallocated member list for matching correlators.  Import the
        // matches into this connection's group.
        //
        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' recording correlator[mask-bit=%d]", correlator, conn->mask_bit);
        strncpy(core->group_correlator_by_maskbit[conn->mask_bit], correlator, QD_DISCRIMINATOR_SIZE);

        qdr_connection_t *member = DEQ_HEAD(core->unallocated_group_members);
        while (!!member) {
            qdr_connection_t *next = DEQ_NEXT_N(GROUP, member);
            if (member->connection_info->group_ordinal == ordinal &&
                strncmp(member->connection_info->group_correlator, correlator, QD_DISCRIMINATOR_SIZE) == 0) {
                qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' moving member [C%"PRIu64"] to parent [C%"PRIu64"]",
                       correlator, member->identity, conn->identity);
                DEQ_REMOVE_N(GROUP, core->unallocated_group_members, member);
                DEQ_INSERT_HEAD_N(GROUP, conn->connection_group, member);
                member->group_parent_mask_bit = conn->mask_bit;

                // ISSUE-1762: Check if the group member connection terminates on the wrong peer router.
                // This can happen if the group is spread across a load balancer, which is a mis-configuration.
                if (strcmp(member->connection_info->container, conn->connection_info->container) != 0) {
                    qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
                           "Connection group failure: connections split across routers: %s ([C%"PRIu64"]) and %s ([C%"PRIu64"])",
                           member->connection_info->container, member->identity,
                           conn->connection_info->container, conn->identity);
                }
            }
            member = next;
        }

        conn->group_cursor = DEQ_HEAD(conn->connection_group);
    }
}


/** Add an inter-router data connection to the connection group
 *
 * See the comment for qdr_connection_group_setup_CT()
 *
 * This is called when a new inter-router data connection comes up.
 */
static void qdr_connection_group_member_setup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER_DATA);

    const char       *correlator = conn->connection_info->group_correlator;
    uint64_t          ordinal    = conn->connection_info->group_ordinal;
    qdr_connection_t *parent     = 0;  // the inter-router control connection if present

    if (!!correlator[0]) {

        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' adding member [C%"PRIu64"] with ordinal=%"PRIu64,
               correlator, conn->identity, ordinal);

        //
        // Scan the correlators-by-maskbit table to see if this member's correlator is active (i.e. control connection is
        // up).  If so, import this into the parent's active group and reset the cursor.  If not, put this member into the
        // unallocated list and wait for the control connection to come up.
        //
        for (int maskbit = 0; maskbit < qd_bitmask_width(); maskbit++) {
            if (strncmp(core->group_correlator_by_maskbit[maskbit], correlator, QD_DISCRIMINATOR_SIZE) == 0) {
                assert(!!core->rnode_conns_by_mask_bit[maskbit]);
                if (core->rnode_conns_by_mask_bit[maskbit]->connection_info->group_ordinal == ordinal) {
                    parent = core->rnode_conns_by_mask_bit[maskbit];
                    break;
                }
            }
        }

        if (!!parent) {
            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' member [C%"PRIu64"] added to parent [C%"PRIu64"]",
                   correlator, conn->identity, parent->identity);

            assert(strncmp(parent->connection_info->group_correlator, correlator, QD_DISCRIMINATOR_SIZE) == 0);
            assert(parent->connection_info->group_ordinal == ordinal);
            DEQ_INSERT_TAIL_N(GROUP, parent->connection_group, conn);
            conn->group_parent_mask_bit = parent->mask_bit;
            parent->group_cursor = DEQ_HEAD(parent->connection_group);

            // ISSUE-1762: Check if the group member connection terminates on the wrong peer router.
            // This can happen if the group is spread across a load balancer, which is a mis-configuration.
            if (strcmp(conn->connection_info->container, parent->connection_info->container) != 0) {
                qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
                       "Connection group failure: connections split across routers: %s ([C%"PRIu64"]) and %s ([C%"PRIu64"])",
                       conn->connection_info->container, conn->identity,
                       parent->connection_info->container, parent->identity);
            }
        } else {
            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' parent not present moving [C%"PRIu64"] to unallocated",
                   correlator, conn->identity);
            DEQ_INSERT_TAIL_N(GROUP, core->unallocated_group_members, conn);
        }
    }
}


/** Teardown a group of inter-router connections
 *
 * See the comment for qdr_connection_group_setup_CT()
 *
 * This is called when the inter-router control connection is closed.
 */
static void qdr_connection_group_cleanup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER);
    //
    // Remove the correlator from the maskbit index.
    // Nullify the cursor and move all group members to the unallocated list.
    //
    const char *correlator = conn->connection_info->group_correlator;
    uint64_t    ordinal    = conn->connection_info->group_ordinal;

    if (!!correlator[0]) {

        // It is possible that this connection is not the active group parent (like on connection upgrade).  Only the
        // active parent can unregister the correlator mask bit map in the core:
        if (qd_bitmask_valid_bit_value(conn->mask_bit) && core->rnode_conns_by_mask_bit[conn->mask_bit] == conn) {
            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' clean up parent [C%"PRIu64"] with ordinal=%"PRIu64,
                   correlator, conn->identity, ordinal);
            assert(strncmp(core->group_correlator_by_maskbit[conn->mask_bit], correlator, QD_DISCRIMINATOR_SIZE) == 0);
            core->group_correlator_by_maskbit[conn->mask_bit][0] = '\0';
        }

        while (!!DEQ_HEAD(conn->connection_group)) {
            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                   "Connection group '%s' moving member [C%"PRIu64"] to unallocated",
                   correlator, DEQ_HEAD(conn->connection_group)->identity);
            qdr_connection_t *member = DEQ_HEAD(conn->connection_group);
            DEQ_REMOVE_HEAD_N(GROUP, conn->connection_group);
            DEQ_INSERT_TAIL_N(GROUP, core->unallocated_group_members, member);
            member->group_parent_mask_bit = -1;
        }
        conn->group_cursor = 0;
    }
}


/** Remove an inter-router data connection from the connection group
 *
 * See the comment for qdr_connection_group_setup_CT()
 *
 * This is called when an existing inter-router data connection goes down. It must be removed from the group parent or
 * the unallocated group list depending on where it resides.
 */
static void qdr_connection_group_member_cleanup_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER_DATA);

    const char       *correlator = conn->connection_info->group_correlator;
    uint64_t          ordinal    = conn->connection_info->group_ordinal;

    if (!!correlator[0]) {

        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' cleanup member [C%"PRIu64"] with ordinal=%"PRIu64,
               correlator, conn->identity, ordinal);

        if (qd_bitmask_valid_bit_value(conn->group_parent_mask_bit)) {
            qdr_connection_t *parent = core->rnode_conns_by_mask_bit[conn->group_parent_mask_bit];
            assert(!!parent);
            assert(strcmp(parent->connection_info->group_correlator, correlator) == 0);
            assert(parent->connection_info->group_ordinal == ordinal);

            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' removing member [C%"PRIu64"] from parent [C%"PRIu64"]",
                   correlator, conn->identity, parent->identity);
            DEQ_REMOVE_N(GROUP, parent->connection_group, conn);
            parent->group_cursor = DEQ_HEAD(parent->connection_group);
            conn->group_parent_mask_bit = -1;

        } else {
            qdr_connection_t *ptr = DEQ_HEAD(core->unallocated_group_members);
            while (!!ptr) {
                qdr_connection_t *next = DEQ_NEXT_N(GROUP, ptr);
                if (ptr == conn) {
                    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG, "Connection group '%s' removing member [C%"PRIu64"] from unallocated",
                           correlator, conn->identity);
                    DEQ_REMOVE_N(GROUP, core->unallocated_group_members, ptr);
                    break;
                }
                ptr = next;
            }
        }
    }
}


static void qdr_connection_opened_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t *conn = safe_deref_qdr_connection_t(action->args.connection.conn);

    if (!conn || discard) {
        qdr_field_free(action->args.connection.connection_label);
        qdr_field_free(action->args.connection.container_id);

        if (conn)
            qdr_connection_free(conn);
        return;
    }

    do {
        DEQ_ITEM_INIT(conn);
        conn->enable_protocol_trace = action->args.connection.enable_protocol_trace;
        DEQ_INSERT_TAIL(core->open_connections, conn);

        if (conn->role == QDR_ROLE_NORMAL) {
            //
            // No action needed for NORMAL connections
            //
            break;
        }

        if (conn->role == QDR_ROLE_INTER_ROUTER) {
            setup_inter_router_control_conn_CT(core, conn);
        }

        if (conn->role == QDR_ROLE_INTER_ROUTER_DATA) {
            //
            // Set this connection up as a member of a group.
            //
            qdr_connection_group_member_setup_CT(core, conn);
        }

        if (conn->role == QDR_ROLE_ROUTE_CONTAINER) {
            //
            // Notify the route-control module that a route-container connection has opened.
            // There may be routes that need to be activated due to the opening of this connection.
            //

            //
            // If there's a connection label, use it as the identifier.  Otherwise, use the remote
            // container id.
            //
            qdr_field_t *cid = action->args.connection.connection_label ?
                action->args.connection.connection_label : action->args.connection.container_id;
            if (cid)
                qdr_route_connection_opened_CT(core, conn, action->args.connection.container_id, action->args.connection.connection_label);
        }
    } while (false);

    qdrc_event_conn_raise(core, QDRC_EVENT_CONN_OPENED, conn);

    qdr_field_free(action->args.connection.connection_label);
    qdr_field_free(action->args.connection.container_id);
}


void qdr_connection_free(qdr_connection_t *conn)
{
    sys_mutex_free(&conn->work_lock);
    qdr_error_free(conn->error);
    qdr_connection_info_free(conn->connection_info);
    free_qdr_connection_t(conn);
}


// create a new outoing link for streaming messages
qdr_link_t *qdr_connection_new_streaming_link_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    qdr_link_t *out_link = 0;

    switch (conn->role) {
    case QDR_ROLE_INTER_ROUTER:
        out_link = qdr_create_link_CT(core, conn, QD_LINK_ROUTER, QD_OUTGOING,
                                      qdr_terminus_router_data(), qdr_terminus_router_data(),
                                      QD_SSN_LINK_STREAMING, QDR_DEFAULT_PRIORITY);
        break;
    case QDR_ROLE_EDGE_CONNECTION:
    case QDR_ROLE_INTER_ROUTER_DATA:
    case QDR_ROLE_NORMAL:
        out_link = qdr_create_link_CT(core, conn, QD_LINK_ENDPOINT, QD_OUTGOING,
                                      qdr_terminus(0), qdr_terminus(0),
                                      QD_SSN_LINK_STREAMING, QDR_DEFAULT_PRIORITY);
        break;
    default:
        assert(false);
        break;
    }

    if (out_link) {
        out_link->streaming = true;
        out_link->priority = 4;
        if (!conn->has_streaming_links) {
            qdr_add_connection_ref(&core->streaming_connections, conn);
            conn->has_streaming_links = true;
        }
        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
               "[C%" PRIu64 "][L%" PRIu64 "] Created new outgoing streaming link %s", conn->identity,
               out_link->identity, out_link->name);
    }
    return out_link;
}

static void qdr_connection_set_tracing_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t *conn = safe_deref_qdr_connection_t(action->args.connection.conn);
    if (discard || !conn)
        return;

    bool enable_protocol_trace = action->args.connection.enable_protocol_trace;
    qdr_connection_work_type_t work_type = QDR_CONNECTION_WORK_TRACING_ON;
    if (!enable_protocol_trace) {
        work_type = QDR_CONNECTION_WORK_TRACING_OFF;
    }
    qdr_connection_work_t *work = new_qdr_connection_work_t();
    ZERO(work);
    work->work_type = work_type;
    qdr_connection_enqueue_work_CT(core, conn, work);
}


static void qdr_connection_notify_closed_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t *conn = safe_deref_qdr_connection_t(action->args.connection.conn);
    if (discard || !conn)
        return;

    //
    // Deactivate routes associated with this connection
    //
    qdr_route_connection_closed_CT(core, conn);

    //
    // Do connection-group cleanup if necessary
    //
    if (conn->role == QDR_ROLE_INTER_ROUTER) {
        qdr_connection_group_cleanup_CT(core, conn);
    } else if (conn->role == QDR_ROLE_INTER_ROUTER_DATA) {
        qdr_connection_group_member_cleanup_CT(core, conn);
    }

    //
    // See if the router mask-bit can be freed.
    //
    if (conn->role == QDR_ROLE_INTER_ROUTER) {
        qdr_reset_sheaf(conn);
        if (qd_bitmask_valid_bit_value(conn->mask_bit)) {
            if (conn == core->rnode_conns_by_mask_bit[conn->mask_bit])
                core->rnode_conns_by_mask_bit[conn->mask_bit] = 0;
            if (conn == core->pending_rnode_conns_by_mask_bit[conn->mask_bit])
                core->pending_rnode_conns_by_mask_bit[conn->mask_bit] = 0;
            if (!core->rnode_conns_by_mask_bit[conn->mask_bit] && !core->pending_rnode_conns_by_mask_bit[conn->mask_bit])
                qd_bitmask_set_bit(core->neighbor_free_mask, conn->mask_bit);
            conn->mask_bit = -1;
        }
    }

    //
    // Remove the references in the links_with_work list
    //
    qdr_link_ref_t *link_ref;
    for (int priority = 0; priority < QDR_N_PRIORITIES; ++ priority) {
        link_ref = DEQ_HEAD(conn->links_with_work[priority]);
        while (link_ref) {
            qdr_del_link_ref(&conn->links_with_work[priority], link_ref->link, QDR_LINK_LIST_CLASS_WORK);
            link_ref = DEQ_HEAD(conn->links_with_work[priority]);
        }
    }

    //
    // TODO - Clean up links associated with this connection
    //        This involves the links and the dispositions of deliveries stored
    //        with the links.
    //
    link_ref = DEQ_HEAD(conn->links);
    while (link_ref) {
        qdr_link_t *link = link_ref->link;

        qdr_route_auto_link_closed_CT(core, link);

        //
        // Clean up the link and all its associated state.
        //
        qdr_link_cleanup_CT(core, conn, link, "Link closed due to connection loss"); // link_cleanup disconnects and frees the ref.
        link_ref = DEQ_HEAD(conn->links);
    }

    if (conn->has_streaming_links) {
        assert(DEQ_IS_EMPTY(conn->streaming_link_pool));  // all links have been released
        qdr_del_connection_ref(&core->streaming_connections, conn);
    }

    //
    // Discard items on the work list
    //
    qdr_connection_work_t *work = DEQ_HEAD(conn->work_list);
    while (work) {
        DEQ_REMOVE_HEAD(conn->work_list);
        qdr_connection_work_free_CT(work);
        work = DEQ_HEAD(conn->work_list);
    }

    //
    // If this connection is on the activation list, remove it from the list
    //
    if (conn->in_activate_list) {
        conn->in_activate_list = false;
        DEQ_REMOVE_N(ACTIVATE, core->connections_to_activate, conn);
    }

    qdrc_event_conn_raise(core, QDRC_EVENT_CONN_CLOSED, conn);

    // High frequency log message, but still needs to show at INFO level for non-normal (non-client) connections like
    // inter-router connections and inter-edge connections etc.
    // Normal client connections will log at DEBUG level since these are high frequency log messages.
    qd_log(LOG_ROUTER_CORE, conn->role == QDR_ROLE_NORMAL ? QD_LOG_DEBUG : QD_LOG_INFO, "[C%" PRIu64 "] Connection Closed", conn->identity);

    DEQ_REMOVE(core->open_connections, conn);
    qdr_connection_free(conn);
}


//
// Handle the attachment of an inter-router control link.
//
static void qdr_attach_link_control_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link)
{
    assert(link->link_type == QD_LINK_CONTROL);
    assert(conn->role == QDR_ROLE_INTER_ROUTER);
    assert(conn->control_links[link->link_direction] == 0);

    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%"PRIu64"] Attaching %s router control link [L%"PRIu64"]",
           conn->identity, link->link_direction == QD_OUTGOING ? "outgoing" : "incoming",
           link->identity);

    conn->control_links[link->link_direction] = link;
    if (link->link_direction == QD_OUTGOING) {
        link->owning_addr = core->hello_addr;
        qdr_add_link_ref(&core->hello_addr->rlinks, link, QDR_LINK_LIST_CLASS_ADDRESS);
    }

    //
    // Once both control links are active the control connection is ready for forwarding.
    //
    if (!!conn->control_links[QD_INCOMING] && !!conn->control_links[QD_OUTGOING]) {
        //
        // If this connection is pending upgrade we can now do the switchover.
        //
        if (qd_bitmask_valid_bit_value(conn->mask_bit) && core->pending_rnode_conns_by_mask_bit[conn->mask_bit] == conn) {
            qdr_connection_t *old_conn = core->rnode_conns_by_mask_bit[conn->mask_bit];

            if (old_conn) {
                //
                // To disable routing over the old connection we invalidate the mask_bit and teardown the
                // connection group
                //
                qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                       "[C%"PRIu64"] Upgrading to primary router control connection, downgrading [C%"PRIu64"]",
                       conn->identity, old_conn->identity);
                assert(old_conn != conn);

                qdr_connection_group_cleanup_CT(core, old_conn);
                old_conn->mask_bit = -1;

                //
                // Shut down the control link on the old connection in order to force forwarding to ignore this
                // connection. The connector-side router is responsible for creating and destroying the control links.
                //
                if (!old_conn->incoming) {
                    if (old_conn->control_links[QD_INCOMING]) {
                        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                               "[C%"PRIu64"] detaching incoming control link [L%"PRIu64"]", old_conn->identity,
                               old_conn->control_links[QD_INCOMING]->identity);
                        qdr_link_outbound_detach_CT(core, old_conn->control_links[QD_INCOMING], 0, QDR_CONDITION_NONE);
                    }
                    if (old_conn->control_links[QD_OUTGOING]) {
                        qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                               "[C%"PRIu64"] detaching outgoing control link [L%"PRIu64"]", old_conn->identity,
                               old_conn->control_links[QD_OUTGOING]->identity);
                        qdr_link_outbound_detach_CT(core, old_conn->control_links[QD_OUTGOING], 0, QDR_CONDITION_NONE);
                    }
                }
            } else {
                qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                       "[C%"PRIu64"] Upgrading to primary router control connection", conn->identity);
            }

            core->pending_rnode_conns_by_mask_bit[conn->mask_bit] = 0;
            core->rnode_conns_by_mask_bit[conn->mask_bit]         = conn;
        }

        //
        // Set up any connection group associated with this inter-router connection.
        //
        qdr_connection_group_setup_CT(core, conn);
    }
}


//
// Handle the detachment of an inter-router control link.
//
static void qdr_detach_link_control_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link)
{
    assert(link->link_type == QD_LINK_CONTROL);
    assert(conn->role == QDR_ROLE_INTER_ROUTER);

    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%"PRIu64"] Detaching %s router control link [L%"PRIu64"]",
           conn->identity, link->link_direction == QD_OUTGOING ? "outgoing" : "incoming",
           link->identity);

    conn->control_links[link->link_direction] = 0;
    if (link->link_direction == QD_OUTGOING && !!link->owning_addr) {
        assert(link->owning_addr == core->hello_addr);
        qdr_del_link_ref(&core->hello_addr->rlinks, link, QDR_LINK_LIST_CLASS_ADDRESS);
        link->owning_addr = 0;

        //
        // Optimization: immediately notify the routing agent that this path is no longer present (rather than wait for
        // heartbeat timeout). This should only be done if the connection is the active control connection and there's
        // no pending connection to take over:
        //
        if (qd_bitmask_valid_bit_value(conn->mask_bit) &&
            core->rnode_conns_by_mask_bit[conn->mask_bit] == conn &&
            core->pending_rnode_conns_by_mask_bit[conn->mask_bit] == 0) {

            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                   "[C%"PRIu64"] Notify routing agent of loss of control link [L%"PRIu64"]",
                   conn->identity, link->identity);
            qdr_post_link_lost_CT(core, conn->mask_bit);
        }
    }
}


//
// Handle the attachment and detachment of an inter-router data link
//
static void qdr_attach_link_data_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER);
    assert(link->link_type == QD_LINK_ROUTER);

    // The first 2 x QDR_N_PRIORITIES (10) QDR_LINK_ROUTER links to attach over
    // the inter-router connection are the shared priority links.  These links
    // are attached in priority order starting at zero.
    if (link->link_direction == QD_OUTGOING) {
        int next_pri = conn->data_links.count;
        if (next_pri < QDR_N_PRIORITIES) {
            link->priority = next_pri;
            conn->data_links.link[next_pri] = link;
            conn->data_links.count += 1;
        }
    } else {
        if (conn->next_pri < QDR_N_PRIORITIES) {
            link->priority = conn->next_pri++;
        }
    }
}


static void qdr_detach_link_data_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER);
    assert(link->link_type == QD_LINK_ROUTER);

    // if this link is in the priority sheaf it needs to be removed
    if (link == conn->data_links.link[link->priority]) {
        assert(link->link_direction == QD_OUTGOING);
        conn->data_links.link[link->priority] = 0;
    }
}


static void qdr_attach_link_downlink_CT(qdr_core_t *core, qdr_connection_t *conn, qdr_link_t *link, qdr_terminus_t *source)
{
    qdr_address_t *addr;
    qd_iterator_t *iter = qd_iterator_dup(qdr_terminus_get_address(source));
    qd_iterator_reset_view(iter, ITER_VIEW_ADDRESS_HASH);
    qd_iterator_annotate_prefix(iter, QD_ITER_HASH_PREFIX_EDGE_SUMMARY);

    qd_hash_retrieve(core->addr_hash, iter, (void**) &addr);
    if (!addr) {
       addr = qdr_address_CT(core, QD_TREATMENT_ANYCAST_BALANCED, 0);
        qd_hash_insert(core->addr_hash, iter, addr, &addr->hash_handle);
        DEQ_INSERT_TAIL(core->addrs, addr);
    }

    qdr_core_bind_address_link_CT(core, addr, link);

    qd_iterator_free(iter);
}


// move dlv to new link.
static void qdr_link_process_initial_delivery_CT(qdr_core_t *core, qdr_link_t *link, qdr_delivery_t *dlv)
{
    //
    // Remove the delivery from its current link if needed
    //
    qdr_link_t *old_link  = safe_deref_qdr_link_t(dlv->link_sp);
    if (!!old_link) {
        sys_mutex_lock(&old_link->conn->work_lock);
        switch (dlv->where) {
        case QDR_DELIVERY_NOWHERE:
            break;

        case QDR_DELIVERY_IN_UNDELIVERED:
            DEQ_REMOVE(old_link->undelivered, dlv);
            dlv->where = QDR_DELIVERY_NOWHERE;
            qdr_link_work_release(dlv->link_work);
            dlv->link_work = 0;
            // expect: caller holds reference to dlv (in action)
            assert(sys_atomic_get(&dlv->ref_count) > 1);
            qdr_delivery_decref_CT(core, dlv, "qdr_link_process_initial_delivery_CT - remove from undelivered list");
            break;

        case QDR_DELIVERY_IN_UNSETTLED:
            DEQ_REMOVE(old_link->unsettled, dlv);
            dlv->where = QDR_DELIVERY_NOWHERE;
            assert(sys_atomic_get(&dlv->ref_count) > 1);
            qdr_delivery_decref_CT(core, dlv, "qdr_link_process_initial_delivery_CT - remove from unsettled list");
            break;

        case QDR_DELIVERY_IN_SETTLED:
            DEQ_REMOVE(old_link->settled, dlv);
            dlv->where = QDR_DELIVERY_NOWHERE;
            assert(sys_atomic_get(&dlv->ref_count) > 1);
            qdr_delivery_decref_CT(core, dlv, "qdr_link_process_initial_delivery_CT - remove from settled list");
            break;
        }

        //
        // Account for the moved delivery in the original link's open_moved_streams counter.
        //
        set_safe_ptr_qdr_link_t(old_link, &dlv->original_link_sp);
        old_link->open_moved_streams++;

        sys_mutex_unlock(&old_link->conn->work_lock);
    }

    //
    // Enqueue the delivery onto the new link's undelivered list
    //
    set_safe_ptr_qdr_link_t(link, &dlv->link_sp);
    qdr_forward_deliver_CT(core, link, dlv);
}


static void qdr_link_inbound_first_attach_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t      *conn = safe_deref_qdr_connection_t(action->args.connection.conn);
    qdr_link_t            *link = safe_deref_qdr_link_t(action->args.connection.link);
    qdr_delivery_t *initial_dlv = action->args.connection.initial_delivery;

    qdr_terminus_t *source = action->args.connection.source;
    qdr_terminus_t *target = action->args.connection.target;

    if (discard || !conn || !link) {
        if (initial_dlv)
            qdr_delivery_decref(core, initial_dlv,
                                "qdr_link_inbound_first_attach_CT - discarding action");

        //
        // Free the terminus objects here.
        // It is ok to free the terminus objects since their ownership has been passed to us now,
        //
        qdr_terminus_free(source);
        qdr_terminus_free(target);

        //
        // https://github.com/skupperproject/skupper-router/issues/335
        // The qdr_link_t cannot be freed here since someone who called qdr_link_first_attach
        // is still holding a reference to it.
        // The right approach here would be to add this link to core->open_links
        // and hope that qdr_core_free() will free this link but currently
        // the code in qdr_core_first frees all links in core->open_links and then
        // sets all core actions to discard.
        // We have temporarily added qdr_link_t to the list of suppressed objects in alloc_pool.c
        //
        // DEQ_INSERT_TAIL(core->open_links, link);

        return;
    }

    qd_direction_t dir    = action->args.connection.dir;

    //
    // Expect this is the initial attach (remote initiated link)
    //
    assert((link->state & QDR_LINK_STATE_ATTACH_SENT) == 0);
    link->state |= QDR_LINK_STATE_ATTACH_RECVD;

    //
    // Put the link into the proper lists for tracking.
    //
    DEQ_INSERT_TAIL(core->open_links, link);
    qdr_add_link_ref(&conn->links, link, QDR_LINK_LIST_CLASS_CONNECTION);

    //
    // Mark the link as an edge link if it's inside an edge connection.
    //
    link->edge = (conn->role == QDR_ROLE_EDGE_CONNECTION);

    //
    // Reject any attaches of inter-router links that arrive on connections that are not inter-router.
    //
    if (((link->link_type == QD_LINK_CONTROL || link->link_type == QD_LINK_ROUTER) &&
         conn->role != QDR_ROLE_INTER_ROUTER)) {
        link->link_type = QD_LINK_ENDPOINT; // Demote the link type to endpoint if this is not an inter-router connection
        qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_FORBIDDEN);
        qdr_terminus_free(source);
        qdr_terminus_free(target);
        qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
               "[C%" PRIu64 "] Router attach forbidden on non-inter-router connection", conn->identity);
        return;
    }

    //
    // Reject ENDPOINT attaches if this is an inter-router connection _and_ there is no
    // CONTROL link on the connection.  This will prevent endpoints from using inter-router
    // listeners for normal traffic but will not prevent routed-links from being established.
    //
    if (conn->role == QDR_ROLE_INTER_ROUTER && link->link_type == QD_LINK_ENDPOINT &&
        conn->control_links[QD_OUTGOING] == 0) {
        qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_WRONG_ROLE);
        qdr_terminus_free(source);
        qdr_terminus_free(target);
        qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
               "[C%" PRIu64 "] Endpoint attach forbidden on inter-router connection", conn->identity);
        return;
    }

    char   source_str[1000];
    char   target_str[1000];
    size_t source_len = 1000;
    size_t target_len = 1000;

    source_str[0] = '\0';
    target_str[0] = '\0';

    //
    // Grab the formatted terminus strings before we schedule any IO-thread processing that
    // might get ahead of us and free the terminus objects before we issue the log.
    //
    if (qd_log_enabled(LOG_ROUTER_CORE, QD_LOG_DEBUG)) {
        qdr_terminus_format(source, source_str, &source_len);
        qdr_terminus_format(target, target_str, &target_len);
    }

    if (dir == QD_INCOMING) {
        //
        // Handle incoming link cases
        //
        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_INTER_EDGE: {
            if (qdr_terminus_is_anonymous(target)) {
                link->owning_addr = 0;
                qdr_link_outbound_second_attach_CT(core, link, source, target);
                qdr_link_issue_credit_CT(core, link, link->capacity, false);

            } else {
                //
                // This link has a target address
                //
                if (core->addr_lookup_handler)
                    core->addr_lookup_handler(core->addr_lookup_context, conn, link, dir, source, target);
                else {
                    qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_NO_ROUTE_TO_DESTINATION);
                    qdr_terminus_free(source);
                    qdr_terminus_free(target);
                    qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
                           "[C%" PRIu64 "] Endpoint attach failed - no address lookup handler", conn->identity);
                    return;
                }
            }
            break;
        }

        case QD_LINK_ROUTER:
            qdr_attach_link_data_CT(core, conn, link);
            qdr_link_outbound_second_attach_CT(core, link, source, target);
            qdr_link_issue_credit_CT(core, link, link->capacity, false);
            break;

        case QD_LINK_CONTROL:
            qdr_attach_link_control_CT(core, conn, link);
            qdr_link_outbound_second_attach_CT(core, link, source, target);
            qdr_link_issue_credit_CT(core, link, link->capacity, false);
            break;

        case QD_LINK_EDGE_DOWNLINK:
            break;
        }
    } else {
        //
        // Handle outgoing link cases
        //
        if (initial_dlv) {
            qdr_link_process_initial_delivery_CT(core, link, initial_dlv);
            qdr_delivery_decref(core, initial_dlv,
                                "qdr_link_inbound_first_attach_CT - dropping action reference");
            initial_dlv = 0;
        }

        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_INTER_EDGE: {
            if (core->addr_lookup_handler)
                core->addr_lookup_handler(core->addr_lookup_context, conn, link, dir, source, target);
            else {
                qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_NO_ROUTE_TO_DESTINATION);
                qdr_terminus_free(source);
                qdr_terminus_free(target);
                qd_log(LOG_ROUTER_CORE, QD_LOG_ERROR,
                       "[C%" PRIu64 "] Endpoint attach failed - no address lookup handler", conn->identity);
                return;
            }
            break;
        }

        case QD_LINK_CONTROL:
            qdr_attach_link_control_CT(core, conn, link);
            qdr_link_outbound_second_attach_CT(core, link, source, target);
            break;

        case QD_LINK_ROUTER:
            qdr_attach_link_data_CT(core, conn, link);
            qdr_link_outbound_second_attach_CT(core, link, source, target);
            break;

        case QD_LINK_EDGE_DOWNLINK:
            qdr_attach_link_downlink_CT(core, conn, link, source);
            qdr_link_outbound_second_attach_CT(core, link, source, target);
            break;
        }
    }

    // Setting this high frequency log message to QD_LOG_DEBUG.
    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%" PRIu64 "][L%" PRIu64 "] Link attached: dir=%s source=%s target=%s", conn->identity, link->identity,
           dir == QD_INCOMING ? "in" : "out", source_str, target_str);
}


static void qdr_link_inbound_second_attach_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_link_t       *link = safe_deref_qdr_link_t(action->args.connection.link);
    qdr_connection_t *conn = safe_deref_qdr_connection_t(action->args.connection.conn);
    qdr_terminus_t   *source = action->args.connection.source;
    qdr_terminus_t   *target = action->args.connection.target;

    if (discard || !link || !conn) {
        qdr_terminus_free(source);
        qdr_terminus_free(target);
        return;
    }

    // expect: called due to an attach received as a response to our sent attach
    //
    assert(!!(link->state & QDR_LINK_STATE_ATTACH_SENT));
    link->state |= QDR_LINK_STATE_ATTACH_RECVD;

    link->oper_status = QDR_LINK_OPER_UP;


    //
    // Mark the link as an edge link if it's inside an edge connection.
    //
    link->edge = (conn->role == QDR_ROLE_EDGE_CONNECTION);

    if (link->core_endpoint) {
        qdrc_endpoint_do_second_attach_CT(core, link->core_endpoint, source, target);
        return;
    }

    if (link->link_direction == QD_INCOMING) {
        //
        // Handle incoming link cases
        //
        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_INTER_EDGE:
            if (link->auto_link) {
                //
                // This second-attach is the completion of an auto-link.  If the attach
                // has a valid source, transition the auto-link to the "active" state.
                //
                if (qdr_terminus_get_address(source)) {
                    link->auto_link->state = QDR_AUTO_LINK_STATE_ACTIVE;
                    qdr_core_bind_address_link_CT(core, link->auto_link->addr, link);
                }
            }

            //
            // Issue credit if this is an anonymous link or if its address has at least one reachable destination.
            //
            qdr_address_t *addr = link->owning_addr;
            if (!addr || (DEQ_SIZE(addr->subscriptions) || DEQ_SIZE(addr->rlinks) || qd_bitmask_cardinality(addr->rnodes)))
                qdr_link_issue_credit_CT(core, link, link->capacity, false);
            break;

        case QD_LINK_ROUTER:
            qdr_attach_link_data_CT(core, conn, link);
            qdr_link_issue_credit_CT(core, link, link->capacity, false);
            break;

        case QD_LINK_CONTROL:
            qdr_attach_link_control_CT(core, conn, link);
            qdr_link_issue_credit_CT(core, link, link->capacity, false);
            break;

        case QD_LINK_EDGE_DOWNLINK:
            break;
        }
    } else {
        //
        // Handle outgoing link cases
        //
        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_INTER_EDGE:
            if (link->auto_link) {
                //
                // This second-attach is the completion of an auto-link.  If the attach
                // has a valid target, transition the auto-link to the "active" state.
                //
                if (qdr_terminus_get_address(target)) {
                    link->auto_link->state = QDR_AUTO_LINK_STATE_ACTIVE;
                    qdr_core_bind_address_link_CT(core, link->auto_link->addr, link);
                }
            }
            break;

        case QD_LINK_CONTROL:
            qdr_attach_link_control_CT(core, conn, link);
            break;

        case QD_LINK_ROUTER:
            qdr_attach_link_data_CT(core, conn, link);
            break;

        case QD_LINK_EDGE_DOWNLINK:
            break;
        }
    }

    qdr_terminus_free(source);
    qdr_terminus_free(target);
}


// Perform all detach-related link processing.
//
// error: (optional) error information that arrived in the detach performative
//
static void qdr_link_process_detach(qdr_core_t *core, qdr_link_t *link, qdr_error_t *error)
{
    qdr_connection_t *conn = link->conn;

    //
    // ensure a pooled link is no longer available for use
    //
    if (link->streaming) {
        if (link->in_streaming_pool) {
            DEQ_REMOVE_N(STREAMING_POOL, conn->streaming_link_pool, link);
            link->in_streaming_pool = false;
        }
    }

    //
    // Notify the auto-link that the link is coming down so a new link will be initiated.
    //
    if (link->auto_link) {
        qdr_route_auto_link_detached_CT(core, link, error);
    }

    qdr_address_t *addr = link->owning_addr;
    if (addr)
        addr->ref_count++;

    if (link->link_direction == QD_INCOMING) {
        qdrc_event_link_raise(core, QDRC_EVENT_LINK_IN_DETACHED, link);
        //
        // Handle incoming link cases
        //
        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_INTER_EDGE:
            if (addr) {
                //
                // Drain the undelivered list to ensure deliveries don't get dropped by a detach.
                //
                qdr_drain_inbound_undelivered_CT(core, link, addr);

                //
                // Unbind the address and the link.
                //
                qdr_core_unbind_address_link_CT(core, addr, link);

                //
                // If this is an edge data link, raise a link event to indicate its detachment.
                //
                if (link->conn->role == QDR_ROLE_EDGE_CONNECTION)
                    qdrc_event_link_raise(core, QDRC_EVENT_LINK_EDGE_DATA_DETACHED, link);
            }
            break;

        case QD_LINK_CONTROL:
            qdr_detach_link_control_CT(core, conn, link);
            break;

        case QD_LINK_ROUTER:
            break;

        case QD_LINK_EDGE_DOWNLINK:
            break;
        }
    } else {
        //
        // Handle outgoing link cases
        //
        qdrc_event_link_raise(core, QDRC_EVENT_LINK_OUT_DETACHED, link);
        switch (link->link_type) {
        case QD_LINK_ENDPOINT:
        case QD_LINK_EDGE_DOWNLINK:
        case QD_LINK_INTER_EDGE:
            if (addr) {
                qdr_core_unbind_address_link_CT(core, addr, link);
            }
            break;

        case QD_LINK_CONTROL:
            qdr_detach_link_control_CT(core, conn, link);
            break;

        case QD_LINK_ROUTER:
            qdr_detach_link_data_CT(core, conn, link);
            break;
        }
    }

    //
    // We had increased the ref_count if the link->no_route was true. Now reduce the ref_count
    //
    if (addr && link->no_route && link->no_route_bound) {
        addr->ref_count--;
    }

    link->owning_addr = 0;

    //
    // Handle the disposition of any deliveries that remain on the link
    //
    qdr_link_cleanup_deliveries_CT(core, link->conn, link, false);

    //
    // If there was an address associated with this link, check to see if any address-related
    // cleanup has to be done.
    //
    if (addr) {
        addr->ref_count--;
        qdr_check_addr_CT(core, addr);
    }
}


static void qdr_link_inbound_detach_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_connection_t *conn  = safe_deref_qdr_connection_t(action->args.connection.conn);
    qdr_link_t       *link  = safe_deref_qdr_link_t(action->args.connection.link);
    qdr_error_t      *error = action->args.connection.error;

    if (discard || !conn || !link) {
        qdr_error_free(error);
        return;
    }

    if (link->state & QDR_LINK_STATE_DETACH_RECVD)
        return;

    qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] qdr_link_inbound_detach_CT()",
           conn->identity, link->identity);

    link->state |= QDR_LINK_STATE_DETACH_RECVD;

    const bool first_detach = (link->state & QDR_LINK_STATE_DETACH_SENT) == 0;

    if (link->core_endpoint) {
        qdrc_endpoint_do_detach_CT(core, link->core_endpoint, error, first_detach);
        return;
    }

    qdr_link_process_detach(core, link, error);

    if (first_detach) {
        // Send response detach
        qdr_link_outbound_detach_CT(core, link, 0, QDR_CONDITION_NONE);
    }

    if (error)
        qdr_error_free(error);
}


static void qdr_link_notify_closed_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_link_t *link  = safe_deref_qdr_link_t(action->args.connection.link);
    bool forced_close = action->args.connection.forced_close;

    if (discard || !link)
        return;

    if (forced_close) {
        // The link has been forced closed rather than cleanly detached.
        if ((link->state & QDR_LINK_STATE_DETACH_RECVD) == 0) {
            // detach-related cleanup was not done - do it now
            if (link->core_endpoint) {
                bool first_detach = (link->state & QDR_LINK_STATE_DETACH_SENT) == 0;
                qdrc_endpoint_do_detach_CT(core, link->core_endpoint, 0, first_detach);
                return;
            }

            qd_log(LOG_ROUTER_CORE, QD_LOG_DEBUG,
                   "[C%"PRIu64"][L%"PRIu64"] qdr_link_notify_closed_CT(forced=%s) handle %s detach",
                   link->conn->identity, link->identity, forced_close ? "YES" : "NO",
                       (link->state & QDR_LINK_STATE_DETACH_SENT) == 0 ? "first" : "second");

            qdr_link_process_detach(core, link, 0);
        }
    }

    qdr_link_cleanup_protected_CT(core, link->conn, link,
                                  action->args.connection.forced_close
                                  ? "Link forced closed" : "Link closed");
}


static void qdr_link_processing_complete_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_link_t *link = safe_deref_qdr_link_t(action->args.connection.link);
    if (discard || !link)
        return;

    qdr_link_cleanup_CT(core, link->conn, link, "Link cleanup deferred after IO processing");
}


/** Set up the inter-router trunk message links.
 *
 * The connector-side of inter-router connections (i.e. outgoing connection) is responsible for setting up the
 * inter-router trunk links: Two (in and out) for control messages, 2 * QDR_N_PRIORITIES for non-streaming message
 * transfer.
 */
static void create_inter_router_message_links(qdr_core_t *core, qdr_connection_t *conn)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER);

    if (!conn->incoming) {
        (void) qdr_create_link_CT(core, conn, QD_LINK_CONTROL, QD_INCOMING,
                                  qdr_terminus_router_control(), qdr_terminus_router_control(),
                                  QD_SSN_ROUTER_CONTROL, QDR_MAX_PRIORITY);
        (void) qdr_create_link_CT(core, conn, QD_LINK_CONTROL, QD_OUTGOING,
                                  qdr_terminus_router_control(), qdr_terminus_router_control(),
                                  QD_SSN_ROUTER_CONTROL, QDR_MAX_PRIORITY);
        STATIC_ASSERT((QD_SSN_ROUTER_DATA_PRI_9 - QD_SSN_ROUTER_DATA_PRI_0 + 1) == QDR_N_PRIORITIES, PRIORITY_SESSION_NOT_SAME);

        for (int priority = 0; priority < QDR_N_PRIORITIES; ++ priority) {
            // a session is reserved for each priority link
            qd_session_class_t sc = (qd_session_class_t)(QD_SSN_ROUTER_DATA_PRI_0 + priority);
            (void) qdr_create_link_CT(core, conn, QD_LINK_ROUTER, QD_INCOMING,
                                      qdr_terminus_router_data(), qdr_terminus_router_data(),
                                      sc, priority);
            (void) qdr_create_link_CT(core, conn, QD_LINK_ROUTER,  QD_OUTGOING,
                                      qdr_terminus_router_data(), qdr_terminus_router_data(),
                                      sc, priority);
        }
    }
}


static void setup_inter_router_control_conn_CT(qdr_core_t *core, qdr_connection_t *conn)
{
    assert(conn->role == QDR_ROLE_INTER_ROUTER);

    const char *correlator        = conn->connection_info->group_correlator;
    qdr_connection_t *old_conn    = 0;
    qdr_connection_t *old_pending = 0;

    //
    // Connection group upgrade: see if there are other connections present that use the same group correlator.
    //
    if (!!correlator[0]) {
        for (int idx = 0; idx < qd_bitmask_width(); ++idx) {
            qdr_connection_t *tmp;
            tmp = core->rnode_conns_by_mask_bit[idx];
            if (!!tmp && strcmp(correlator, tmp->connection_info->group_correlator) == 0) {
                old_conn = tmp;
            }
            tmp = core->pending_rnode_conns_by_mask_bit[idx];
            if (!!tmp && strcmp(correlator, tmp->connection_info->group_correlator) == 0) {
                old_pending = tmp;
            }
        }
    }

    if (old_pending) {
        // A pending connection will have higher precedence than the old_conn (because its overriding old_conn).  Check
        // if the new conn has higher precedence than pending and either replace pending or ignore the new conn

        if (conn->connection_info->group_ordinal > old_pending->connection_info->group_ordinal) {
            // Replace old_pending
            //
            qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Overriding existing pending connection [C%"PRIu64"] with [C%"PRIu64"]",
                   old_pending->identity, conn->identity);

            // Re-use the mask bit. This is necessary to prevent the router from recalculating routes.
            conn->mask_bit = old_pending->mask_bit;
            old_pending->mask_bit = -1;
            core->pending_rnode_conns_by_mask_bit[conn->mask_bit] = conn;
            create_inter_router_message_links(core, conn);

            old_pending->error = qdr_error(QD_AMQP_COND_CONNECTION_FORCED, "New inter-router connection takes precedence");
            qdr_close_connection_CT(core, old_pending);

        } else {
            // The existing pending connection has higher precedence, keep it.
            //
            qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Existing pending connection [C%"PRIu64"] takes precedence, closing [C%"PRIu64"]",
                   old_pending->identity, conn->identity);
            conn->error = qdr_error(QD_AMQP_COND_CONNECTION_FORCED, "Existing pending connection takes precedence");
            qdr_close_connection_CT(core, conn);
        }

    } else if (old_conn) {
        // Does the new conn take precedence over the existing conn?  If so make it pending.
        //
        if (conn->connection_info->group_ordinal > old_conn->connection_info->group_ordinal) {
            // Bring up new conn to succeed old_conn
            //
            qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Upgrading connection [C%"PRIu64"] to [C%"PRIu64"]",
                   old_conn->identity, conn->identity);

            // Re-use the mask bit. This is necessary to prevent the router from recalculating routes.
            conn->mask_bit = old_conn->mask_bit;
            core->pending_rnode_conns_by_mask_bit[conn->mask_bit] = conn;
            create_inter_router_message_links(core, conn);

            // Once the new inter-router control links come up we replace the old_conn

        } else {
            qd_log(LOG_ROUTER_CORE, QD_LOG_INFO, "Existing connection [C%"PRIu64"] takes precedence, closing [C%"PRIu64"]",
                   old_conn->identity, conn->identity);
            conn->error = qdr_error(QD_AMQP_COND_CONNECTION_FORCED, "Existing connection takes precedence");
            qdr_close_connection_CT(core, conn);
        }
    } else {
        //
        // Not upgrading so simply setup the new connection. Assign a unique mask-bit to this connection as a reference
        // to be used by the router module.
        //
        if (qd_bitmask_first_set(core->neighbor_free_mask, &conn->mask_bit)) {
            qd_bitmask_clear_bit(core->neighbor_free_mask, conn->mask_bit);
            assert(core->rnode_conns_by_mask_bit[conn->mask_bit] == 0);
            core->rnode_conns_by_mask_bit[conn->mask_bit] = conn;
            create_inter_router_message_links(core, conn);

        } else {
            // Too many inter-router connections provisioned!
            //
            qd_log(LOG_ROUTER_CORE, QD_LOG_CRITICAL, "Exceeded maximum supported number of inter-router connections");
            conn->error = qdr_error(QD_AMQP_COND_CONNECTION_FORCED, "Too many inter-router connections");
            qdr_close_connection_CT(core, conn);
        }
    }
}

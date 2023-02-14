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

#include <qpid/dispatch/cutthrough_utils.h>
#include <qpid/dispatch/message.h>
#include <proton/raw_connection.h>
#include "delivery.h"
#include "qd_connection.h"
#include "adaptors/tcp_lite/tcp_lite.h"


static void activate_connection(qd_message_activation_t *activation, qd_direction_t dir)
{
    switch (activation->type) {
    case QD_ACTIVATION_NONE:
        break;

    case QD_ACTIVATION_AMQP: {
        qd_connection_t *qconn = safe_deref_qd_connection_t(activation->safeptr);

        if (!qconn) {
            return;
        }

        qdr_delivery_ref_t      *dref     = new_qdr_delivery_ref_t();
        sys_spinlock_t          *spinlock = dir == QD_INCOMING ? &qconn->inbound_cutthrough_spinlock : &qconn->outbound_cutthrough_spinlock;
        qdr_delivery_ref_list_t *worklist = dir == QD_INCOMING ? &qconn->inbound_cutthrough_worklist : &qconn->outbound_cutthrough_worklist;
        bool                     notify   = false;

        sys_spinlock_lock(spinlock);
        if (!activation->delivery->cutthrough_list_ref) {
            DEQ_ITEM_INIT(dref);
            dref->dlv = activation->delivery;
            activation->delivery->cutthrough_list_ref = dref;
            DEQ_INSERT_TAIL(*worklist, dref);
            qdr_delivery_incref(activation->delivery, "Cut-through activation worklist");
            notify = true;
        }
        sys_spinlock_unlock(spinlock);

        if (notify) {
            qd_server_activate_cutthrough(qconn, dir == QD_INCOMING);
        } else {
            free_qdr_delivery_ref_t(dref);
        }
        break;
    }

    case QD_ACTIVATION_TCP: {
        tcplite_connection_t *conn = safe_deref_tcplite_connection_t(activation->safeptr);
        if (!!conn && IS_ATOMIC_FLAG_SET(&conn->raw_opened)) {
            pn_raw_connection_wake(conn->raw_conn);
        }
        break;
    }
    }
}


void cutthrough_notify_buffers_produced_inbound(qd_message_t *msg)
{
    qd_message_activation_t activation;
    qd_message_get_consumer_activation(msg, &activation);
    activate_connection(&activation, QD_OUTGOING);
}


void cutthrough_notify_buffers_consumed_outbound(qd_message_t *msg)
{
    bool unstall = qd_message_resume_from_stalled(msg);
    if (unstall) {
        qd_message_activation_t activation;
        qd_message_get_producer_activation(msg, &activation);
        activate_connection(&activation, QD_INCOMING);
    }
}

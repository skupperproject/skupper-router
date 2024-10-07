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

#include "private.h"
#include <qpid/dispatch/alloc_pool.h>

ALLOC_DECLARE(qdpo_config_t);
ALLOC_DEFINE(qdpo_config_t);
ALLOC_DECLARE(qdpo_t);
ALLOC_DEFINE(qdpo_t);
ALLOC_DECLARE(qdpo_transport_handle_t);
ALLOC_DEFINE(qdpo_transport_handle_t);


qdpo_config_t *qdpo_config(qdpo_use_address_t use_address, qd_observer_t observer)
{
    qdpo_config_t *config = new_qdpo_config_t();
    ZERO(config);

    config->use_address = use_address;
    config->observer    = observer;
    return config;
}

void qdpo_set_observer(qdpo_t *protocol_observer, qd_observer_t observer)
{
    protocol_observer->config->observer = observer;
}


void qdpo_config_free(qdpo_config_t *config)
{
    free_qdpo_config_t(config);
}


void qdpo_config_add_exception_protocol(qdpo_config_t *config, const char *protocol)
{
    // TODO
}


void qdpo_config_add_address(qdpo_config_t *config, const char *field, const char *value, const char *address)
{
    // TODO
}


qdpo_t *protocol_observer(qd_protocol_t base, qdpo_config_t *config)
{
    qdpo_t *protocol_observer = new_qdpo_t();
    ZERO(protocol_observer);

    protocol_observer->base   = base;
    protocol_observer->config = config;

    return protocol_observer;
}


void qdpo_free(qdpo_t *observer)
{
    if(observer->config)
        qdpo_config_free(observer->config);
    free_qdpo_t(observer);
}


qdpo_transport_handle_t *qdpo_begin(qdpo_t *observer, vflow_record_t *vflow, void *transport_context, uint64_t conn_id)
{
    assert(observer);

    qdpo_transport_handle_t *th = new_qdpo_transport_handle_t();
    ZERO(th);

    th->parent            = observer;
    th->vflow             = vflow;
    th->transport_context = transport_context;
    th->protocol          = observer->base;
    th->conn_id           = conn_id;

    //
    // Directly call the corresponding init functions based on the observer
    // specified in the tcp listener.
    //
    switch (observer->config->observer) {
        case OBSERVER_AUTO:
            qdpo_tcp_init(th);
            break;
        case OBSERVER_HTTP1:
            qdpo_http1_init(th);
            break;
        case OBSERVER_HTTP2:
            qdpo_http2_init(th);
            break;
        default:
            break;
    }
    return th;
}


void qdpo_data(qdpo_transport_handle_t *th, bool from_client, const unsigned char *data, size_t length)
{
    assert(th);
    if (th->observe)
        th->observe(th, from_client, data, length);
}


void qdpo_end(qdpo_transport_handle_t *th)
{
    if (th) {
        switch (th->protocol) {
            case QD_PROTOCOL_TCP:
                qdpo_tcp_final(th);
                break;
            case QD_PROTOCOL_HTTP1:
                qdpo_http1_final(th);
                break;
            case QD_PROTOCOL_HTTP2:
                qdpo_http2_final(th);
                break;
            default:
                assert(false);  // unsupported protocol
                break;
        }

        free_qdpo_transport_handle_t(th);
    }
}


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

#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/protocol_observer.h>

struct qdpo_config_t {
    qdpo_use_address_t  use_address;
    bool                allow_all;
};

ALLOC_DECLARE(qdpo_config_t);
ALLOC_DEFINE(qdpo_config_t);

qdpo_config_t *qdpo_config(qdpo_use_address_t use_address, bool allow_all_protocols)
{
    qdpo_config_t *config = new_qdpo_config_t();
    ZERO(config);

    config->use_address = use_address;
    config->allow_all   = allow_all_protocols;

    return config;
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


struct qdpo_t {
    qdpo_config_t *config;
};

ALLOC_DECLARE(qdpo_t);
ALLOC_DEFINE(qdpo_t);


qdpo_t *protocol_observer(const char *base, qdpo_config_t *config)
{
    qdpo_t *observer = new_qdpo_t();
    ZERO(observer);

    observer->config = config;

    return observer;
}


void qdpo_free(qdpo_t *observer)
{
    free_qdpo_t(observer);
}


qdpo_transport_handle_t qdpo_first(qdpo_t *observer, vflow_record_t *vflow, void *transport_context, qd_buffer_t *buf, size_t offset)
{
    // TODO
    return 0;
}


void qdpo_data(qdpo_transport_handle_t transport_handle, bool from_client, qd_buffer_t *buf, size_t offset)
{
    // TODO
}


void qdpo_end(qdpo_transport_handle_t transport_handle)
{
    // TODO
}


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

 //
 // HOW TO USE THIS FILE:
 //
 // - Make a copy of this source file in the same directory but under a different name.
 // - Add the new file to src/CMakeLists.txt so it will be built.
 //
 // In your copy:
 //   - Change the name of the module in the module_name constant.
 //   - Fill in all of the TODO sections with your specific implementation.
 //
 // To select your plugin as the active transport:
 // Add the following line to the "router" section in etc/skrouterd.conf:
 //
 //     transportPlugin: <the name of your module>
 //

#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/enum.h>
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/adaptor_common.h>
#include <qpid/dispatch/service_transport.h>

//===========================================================================================
// Settings and Definitions
//===========================================================================================

//
// TODO: Change the name
//
static const char *module_name = "reference";

//===========================================================================================
// State Definitions
//===========================================================================================

//
// Service Listener
//
typedef struct tmod_tcp_listener_t {
    DEQ_LINKS(struct tmod_tcp_listener_t);
} tmod_tcp_listener_t;

ALLOC_DECLARE(tmod_tcp_listener_t);
ALLOC_DEFINE(tmod_tcp_listener_t);
DEQ_DECLARE(tmod_tcp_listener_t, tmod_tcp_listener_list_t);

//
// Service Connector
//
typedef struct tmod_tcp_connector_t {
    DEQ_LINKS(struct tmod_tcp_connector_t);
} tmod_tcp_connector_t;

ALLOC_DECLARE(tmod_tcp_connector_t);
ALLOC_DEFINE(tmod_tcp_connector_t);
DEQ_DECLARE(tmod_tcp_connector_t, tmod_tcp_connector_list_t);

//
// Inter-Router Listener
//

//
// Inter-Router Connector
//

//
// Inter-Router Link
//

//
// Module State
//
typedef struct {
    tmod_tcp_listener_list_t  tcpListeners;
    tmod_tcp_connector_list_t tcpConnectors;
} tmod_module_t;

//===========================================================================================
// Transport API Handlers
//===========================================================================================


//===========================================================================================
// Management API Handlers - TCP Listeners
//===========================================================================================

static void *configure_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    return 0;
}

static void *update_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity, void *impl)
{
    return 0;
}

static void delete_tcp_listener(qd_dispatch_t *qd, void *impl)
{
}

static qd_error_t refresh_tcp_listener(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}

//===========================================================================================
// Management API Handlers - TCP Connectors
//===========================================================================================

static void *configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    return 0;
}

static void delete_tcp_connector(qd_dispatch_t *qd, void *impl)
{
}

static qd_error_t refresh_tcp_connector(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}

//===========================================================================================
// Module API Handlers
//===========================================================================================
//
// This initialization function is invoked once at router startup if this module is
// the one transport module enabled in the router configuration.
//
static void TRANSPORT_init(qdr_core_t *core, void **adaptor_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Transport Module Initialized: %s", module_name);

    qd_register_tcp_management_handlers(
        configure_tcp_listener,
        configure_tcp_connector,
        update_tcp_listener,
        delete_tcp_listener,
        delete_tcp_connector,
        refresh_tcp_listener,
        refresh_tcp_connector
    );

    tmod_module_t *module = NEW(tmod_module_t);
    ZERO(module);
    *adaptor_context = module;
}

//
// This finalization function is invoked once at router shut-down only if it was earlier initialized.
//
static void TRANSPORT_final(void *module_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Transport Module Finalized: %s", module_name);
    free(module_context);
}

/**
 * Declare the module so that it will self-register on process startup.
 */
QDR_TRANSPORT_MODULE_DECLARE(module_name, TRANSPORT_init, TRANSPORT_final)

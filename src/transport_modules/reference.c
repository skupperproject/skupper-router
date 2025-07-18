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

#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/enum.h>
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/adaptor_common.h>

//===========================================================================================
// Transport API Handlers
//===========================================================================================


//===========================================================================================
// Management API Handlers
//===========================================================================================


//===========================================================================================
// Module API Handlers
//===========================================================================================
//
// This initialization function is invoked once at router startup if this module is
// the one transport module enabled in the router configuration.
//
static void TRANSPORT_init(qdr_core_t *core, void **adaptor_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Reference Transport Module Initialized");
}

//
// This finalization function is invoked once at router shut-down only if it was earlier initialized.
//
static void TRANSPORT_final(void *module_context)
{
    qd_log(LOG_ROUTER, QD_LOG_INFO, "Reference Transport Module Finalized");
}

/**
 * Declare the module so that it will self-register on process startup.
 */
QDR_TRANSPORT_MODULE_DECLARE("reference", TRANSPORT_init, TRANSPORT_final)

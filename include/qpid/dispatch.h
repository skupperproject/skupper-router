#ifndef __dispatch_h__
#define __dispatch_h__ 1
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

/**@file
 * Includes all dispatch library headers.
 *
 * @mainpage API documentation for the experimental Qpid Dispatch Library.
 *
 * @section intro Introduction
 *
 * @warning __This is not yet a stable API. It is provided for experimental use only.__
 *
 * This library allows you to send, receive, parse, construct, route and otherwise
 * manipulate AMQP messages.
 *
 * The [Qpid Dispatch Router](http://qpid.apache.org/components/dispatch-router)
 * is a flexible AMQP messaging router based on this library. It should meet
 * most general-purpose message routing needs.
 *
 * If you want to build your own custom router-like application, this library
 * provides the tools to do so.
*/

#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/bitmask.h"
#include "qpid/dispatch/buffer.h"
#include "qpid/dispatch/compose.h"
#include "qpid/dispatch/connection_manager.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/hash.h"
#include "qpid/dispatch/iterator.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/parse.h"
#include "qpid/dispatch/router.h"
#include "qpid/dispatch/router_core.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/timer.h"

#endif

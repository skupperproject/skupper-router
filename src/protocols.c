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

#include "qpid/dispatch/protocols.h"

#include <stddef.h>

// Note: names must match the order of the corresponding enum entries in qd_protocol_t, and any changes here require
// updating the connectionCounters router entity attribute in skrouter.json.
//
static const char *_names[QD_PROTOCOL_TOTAL] = {
    "tcp", "amqp", "http1", "http2"
};

// defines function qd_protocol_name(qd_protocol_t)
//
ENUM_DEFINE(qd_protocol, _names);

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

#include "helpers.hpp"

#include "../src/qd_asan_interface.h"

std::mutex QDR::startup_shutdown_lock;

// disable sanitizer, otherwise writes to memory between global variables are reported as buffer overflows
ATTRIBUTE_NO_SANITIZE_ADDRESS
void reset_static_data()
{
    static char *x;

    size_t s = BSS_END - DATA_START;

    // memset is always sanitized, so access memory as chars in a loop

    if (x == NULL) {
        x = (char *) malloc(s);
        for (size_t i = 0; i < s; i++) {
            *(x + i) = *(DATA_START + i);
        }
    } else {
        for (size_t i = 0; i < s; i++) {
            *(DATA_START + i) = *(x + i);
        }
    }
}

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

// disable sanitizer, otherwise writes to memory in between global variables
// get reported as buffer overflows
ATTRIBUTE_NO_SANITIZE_ADDRESS
void reset_static_data()
{
    static char *stored_globals;

    size_t size = BSS_END - DATA_START;

    // memcpy is always sanitized, so access memory as chars in a loop

    if (stored_globals == NULL) {
        stored_globals = (char *) malloc(size);
        for (size_t i = 0; i < size; i++) {
            *(stored_globals + i) = *(DATA_START + i);
        }
    } else {
        flush_coverage();
        for (size_t i = 0; i < size; i++) {
            *(DATA_START + i) = *(stored_globals + i);
        }
    }
}

// The __gcov_flush function writes the coverage counters to gcda files and then resets them to zero.
// It is defined at https://github.com/gcc-mirror/gcc/blob/aad93da1a579b9ae23ede6b9cf8523360f0a08b4/libgcc/libgcov-interface.c
extern "C" void __gcov_flush();

void flush_coverage() {
#if defined(QD_COVERAGE)
    __gcov_flush();
#endif
}

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

#include "cpp_stub.h"
#include "qdr_doctest.hpp"
// helpers.hpp goes after qdr_doctest.hpp
#include "helpers.hpp"

#include <stdint.h>

extern "C" {
double testonly_normalize_memory_size(const uint64_t bytes, const char **suffix);
}

static void test_normalize_memory_size(uint64_t bytes, double expected_value, const char *expected_suffix)
{
    const char *suffix = NULL;
    double value       = testonly_normalize_memory_size(bytes, &suffix);
    CHECK(value == expected_value);
    CHECK(suffix == expected_suffix);
}

TEST_CASE("normalize_memory_size")
{
    test_normalize_memory_size(0, 0.0, "B");
    test_normalize_memory_size(1023, 1023.0, "B");
    test_normalize_memory_size(1024, 1.0, "KiB");
    test_normalize_memory_size(1024 + 1024 / 2, 1.5, "KiB");
    test_normalize_memory_size(1024 * 1024, 1.0, "MiB");
    test_normalize_memory_size(1024 * 1024 * 1024, 1.0, "GiB");
    test_normalize_memory_size(1024ul * 1024 * 1024 * 1024, 1.0, "TiB");
    test_normalize_memory_size(UINT64_MAX, 16384.0, "TiB");
}

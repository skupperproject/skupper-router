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

#include "qdr_doctest.hpp"

extern "C" {
#include "qpid/dispatch/internal/symbolization.h"
}

#include <bfd.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <stdlib.h>

#include <cstdio>

namespace test_backtrace
{

const int STACK_DEPTH = 10;

struct item {
    void *backtrace[STACK_DEPTH];
    int backtrace_size;
};

extern "C" {

int probe_line = __LINE__ + 2;
void probe()
{
}

void b_stores_backtrace(item &item)
{
    item.backtrace_size = backtrace(item.backtrace, STACK_DEPTH);
}

void a_calls_b(item &item)
{
    b_stores_backtrace(item);
}
}


TEST_CASE("qd_symbolize_backtrace_line")
{
    const qd_backtrace_fileline_t &res = qd_symbolize_backtrace_line((bfd_vma) probe);
    CHECK(res.found);
    if(!res.found) {
        qd_symbolize_finalize();
        return;
    };
    CHECK(res.sourcefile == __FILE__);
    CHECK(res.funcname == "probe");
    CHECK(std::abs(probe_line - res.line) <= 3);  // require reasonable precision
    qd_symbolize_finalize();
}

TEST_CASE("qd_print_symbolized_backtrace_line")
{
    item it;
    a_calls_b(it);

    char **strings = backtrace_symbols(it.backtrace, it.backtrace_size);

    for (int i = 0; i < it.backtrace_size; i++) {
        qd_print_symbolized_backtrace_line(stdout, strings[i], i, it.backtrace[i]);
    }
    printf("\n");
    qd_symbolize_finalize();
    free(strings);
}

}  // namespace test_backtrace

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
void b_stores_backtrace(item &item)
{
    item.backtrace_size = backtrace(item.backtrace, STACK_DEPTH);
}

void a_calls_b(item &item)
{
    b_stores_backtrace(item);
}
}

const char *filename;
const char *functionname;
unsigned int line;
bool found = false;

// static void find_address_in_section(bfd *abfd, asection *section, void *data __attribute__ ((__unused__)) )
//{
//     printf("looping\n");
//
//
// }

/**
 * Run this in between ; bfd_close();
 */

void mymapaddr()
{
    const qd_backtrace_fileline_t &res = qd_symbolize_backtrace_line((bfd_vma) mymapaddr);
    printf("found: %s %s %d", res.sourcefile, res.funcname, res.line);
}

//
//    if (!found) {
//        printf("[%s] \?\?() \?\?:0\n",addr[naddr-1]);
//    } else {
//        const char *name;
//
//        name = functionname;
//        if (name == NULL || *name == '\0')
//            name = "??";
//        if (filename != NULL) {
//            char *h;
//
//            h = strrchr(filename, '/');
//            if (h != NULL)
//                filename = h + 1;
//        }
//
//        printf("\t%s:%u\t", filename ? filename : "??",
//               line);
//
//        printf("%s()\n", name);
//
//    }

void printbt(item &item)
{
    char buf[100];
    char **strings = backtrace_symbols(item.backtrace, item.backtrace_size);

    printf("Leak: %s type: %s address: %p\n", buf, "xxx", (void *) (&item));
    for (size_t i = 0; i < item.backtrace_size; i++) {
    }
    printf("\n");
    free(strings);
}

TEST_CASE("backtrace")
{
    item i;

    a_calls_b(i);

    printbt(i);

    mymapaddr();
}
}  // namespace test_backtrace

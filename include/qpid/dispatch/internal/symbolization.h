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

#ifndef QPID_DISPATCH_SYMBOLIZATION_H
#define QPID_DISPATCH_SYMBOLIZATION_H

#include <bfd.h>
#include <stdbool.h>
#include <stdio.h>

typedef struct qd_backtrace_fileline {
    bool found;
    const char *sourcefile;
    const char *funcname;
    unsigned int line;
} qd_backtrace_fileline_t;

void qd_symbolize_finalize();

void print_symbolized_backtrace_line(FILE *dump_file, const char *fallback_symbolization, int i, void *pc);
qd_backtrace_fileline_t qd_symbolize_backtrace_line(bfd_vma pc);

#endif  // QPID_DISPATCH_SYMBOLIZATION_H

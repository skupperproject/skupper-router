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

#define _GNU_SOURCE

#include "qpid/dispatch/internal/symbolization.h"

#include <bfd.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

static struct {
    bool libbfd_inited;
    bfd *abfd;      ///< abfd is the libbfd handle for open object
    asymbol *syms;  ///< syms holds symbol table from the object
} state = {0};

///
qd_backtrace_fileline_t qd_symbolize_backtrace_line(bfd_vma pc)
{
    qd_backtrace_fileline_t result;
    result.found = false;

    if (!state.libbfd_inited) {
        bfd_init();
        state.libbfd_inited = true;
    }

    unsigned int symsize;
    long symcount;

    if (state.abfd == NULL) {
        state.abfd = bfd_openr("/proc/self/exe", NULL);
        state.abfd->flags |= BFD_DECOMPRESS;  // https://sourceware.org/legacy-ml/gdb-patches/2012-10/msg00262.html
        bfd_boolean res = bfd_check_format(state.abfd, bfd_object);
        if (!res) goto finalize;
    }

    if ((bfd_get_file_flags(state.abfd) & HAS_SYMS) == 0) goto finalize;

    symcount = bfd_read_minisymbols(state.abfd, false, (void **) &state.syms, &symsize);
    if (symcount == 0) symcount = bfd_read_minisymbols(state.abfd, true, (void **) &state.syms, &symsize);
    if (symcount == 0) goto finalize;

    // loop can be more commonly written using `bfd_map_over_sections` and a callback
    for (struct bfd_section *section = state.abfd->sections; section != NULL; section = section->next) {
        // use preprocessor to handle pre binutils-2.34 API (on CentOS 8)
#ifdef bfd_get_section_flags
        flagword flags = bfd_get_section_flags(state.abfd, section);
#else
        flagword flags = bfd_section_flags(section);
#endif
        if ((flags & SEC_ALLOC) == 0) continue;

#ifdef bfd_get_section_vma
        bfd_vma vma = bfd_get_section_vma(state.abfd, section);
#else
        bfd_vma vma = bfd_section_vma(section);
#endif
        if (pc < vma) continue;

#ifdef bfd_get_section_size
        bfd_size_type size = bfd_get_section_size(section);
#else
        bfd_size_type size = bfd_section_size(section);
#endif
        if (pc >= vma + size) continue;

        result.found = bfd_find_nearest_line(state.abfd, section, &state.syms, pc - vma, &result.sourcefile,
                                             NULL, &result.line);

        if (result.found) break;
    }

finalize:
    if (result.found) {
        Dl_info info;
        if (dladdr((void*)pc, &info) != 0) {
            result.funcname = info.dli_sname;
        }
    }
    return result;
}

void print_symbolized_backtrace_line(FILE *dump_file, const char *fallback_symbolization, int i, void *pc)
{
    // attempt to symbolize the address

    qd_backtrace_fileline_t res = qd_symbolize_backtrace_line((bfd_vma) pc);
    if (res.found) {
        fprintf(dump_file, "#%d %s %s:%d\n", i,
                res.funcname ? res.funcname : "(?""?)", res.sourcefile, res.line);
        return;
    }
    // symbolization did not succeed, print the uninspiring backtrace_symbols output as fallback
    fprintf(dump_file, "   %s\n", fallback_symbolization);
}

void qd_symbolize_finalize()
{
    if (state.syms) {
        free(state.syms);
    }
    state.syms = NULL;

    if (state.abfd) {
        bfd_close(state.abfd);
    }
    state.abfd = NULL;

    state.libbfd_inited = false;
}

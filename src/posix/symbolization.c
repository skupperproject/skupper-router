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

#include <elfutils/libdw.h>
#include <elfutils/libdwfl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static struct {
    bool           libdwlf_inited;
    Dwfl          *dwfl;            ///< libdwfl handle
    Dwfl_Callbacks dwfl_callbacks;  ///< struct holding libdfl callbacks
} state = {0};

bool ensure_libdwfl_intialized()
{
    if (!state.libdwlf_inited) {
        state.dwfl_callbacks.find_elf       = &dwfl_linux_proc_find_elf;
        state.dwfl_callbacks.find_debuginfo = &dwfl_standard_find_debuginfo;
        state.dwfl_callbacks.debuginfo_path = NULL;
        state.dwfl                          = dwfl_begin(&(state.dwfl_callbacks));

        // note all objects in the address space of current process
        dwfl_report_begin(state.dwfl);
        int r = dwfl_linux_proc_report(state.dwfl, getpid());
        dwfl_report_end(state.dwfl, NULL, NULL);
        if (r < 0) {
            return false;
        }

        state.libdwlf_inited = true;
    }

    return true;
}

qd_backtrace_fileline_t qd_symbolize_backtrace_line(void *target_address)
{
    qd_backtrace_fileline_t result = {0};

    if (!ensure_libdwfl_intialized()) {
        return result;
    }

    Dwarf_Addr addr = (Dwarf_Addr) target_address;

    // find the object that contains queried address
    Dwfl_Module *module = dwfl_addrmodule(state.dwfl, addr);
    if (module) {
        const char *module_name = dwfl_module_info(module, 0, 0, 0, 0, 0, 0, 0);
        if (module_name) {
            result.object_filename = module_name;
        }
        // returns mangled function name, which does not matter if function is written in C
        const char *sym_name = dwfl_module_addrname(module, addr);
        if (sym_name) {
            result.object_function = sym_name;
        }
    }

    Dwarf_Addr module_bias = 0;
    Dwarf_Die *cudie       = dwfl_module_addrdie(module, addr, &module_bias);

    if (cudie == NULL) {
        // this should not happen if we have debug info,
        // but if it does fail, then look how bombela/backward-cpp hacks this
        return result;
    }

    Dwarf_Line *srcloc = dwarf_getsrc_die(cudie, addr - module_bias);

    if (srcloc) {
        const char *srcfile = dwarf_linesrc(srcloc, 0, 0);
        if (srcfile) {
            result.source_filename = srcfile;
        }
        int line = 0;
        int col  = 0;
        dwarf_lineno(srcloc, &line);
        dwarf_linecol(srcloc, &col);
        result.source_line = line;
        result.source_col  = col;
    }

    result.found = true;
    return result;
}

void qd_print_symbolized_backtrace_line(FILE *dump_file, const char *fallback_symbolization, int i, void *pc)
{
    // attempt to symbolize the address
    qd_backtrace_fileline_t res = qd_symbolize_backtrace_line(pc);
    if (res.found) {
        fprintf(dump_file,
                "#%d %s %s:%d\n\tin %s\n",
                i,
                res.object_function ? res.object_function : "(? ?)",
                res.source_filename ? res.source_filename : "(null)",
                res.source_line,
                res.object_filename ? res.object_filename : "(null)");
        return;
    }

    // symbolization did not succeed, print the uninspiring backtrace_symbols output as fallback
    fprintf(dump_file, "   %s\n", fallback_symbolization);
}

void qd_symbolize_finalize()
{
    if (state.libdwlf_inited) {
        dwfl_end(state.dwfl);
    }
    state.libdwlf_inited = false;
}

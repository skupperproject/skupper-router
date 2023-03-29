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

#include <dwarf.h>
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

bool ensure_libdwfl_intialized(void)
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

// helper functions to recover cudie, taken from bombela/backward-cpp
static Dwarf_Die *find_fundie_by_pc(Dwarf_Die *parent_die, Dwarf_Addr pc, Dwarf_Die *result);
static bool       die_has_pc(Dwarf_Die *die, Dwarf_Addr pc);

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

    // This is a trick from bombela/backward-cpp to recover source lines when using clang
    // We could instead compile with clang using -gdwarf-aranges, but that does not work with when LTO is enabled
    //  see https://sourceware.org/bugzilla/show_bug.cgi?id=22288 (backtrace line printing with libdw and clang)
    if (!cudie) {
        // Sadly clang does not generate the section .debug_aranges, thus
        // dwfl_module_addrdie will fail early. Clang doesn't either set
        // the lowpc/highpc/range info for every compilation unit.
        //
        // So in order to save the world:
        // for every compilation unit, we will iterate over every single
        // DIEs. Normally functions should have a lowpc/highpc/range, which
        // we will use to infer the compilation unit.

        // note that this is probably badly inefficient.
        while ((cudie = dwfl_module_nextcu(module, cudie, &module_bias))) {
            Dwarf_Die  die_mem;
            Dwarf_Die *fundie = find_fundie_by_pc(cudie, addr - module_bias, &die_mem);
            if (fundie) {
                break;
            }
        }
    }

    if (cudie == NULL) {
        // we failed, not even bombela/backward-cpp trick saved us
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

static bool die_has_pc(Dwarf_Die *die, Dwarf_Addr pc)
{
    Dwarf_Addr low, high;

    // continuous range
    if (dwarf_hasattr(die, DW_AT_low_pc) && dwarf_hasattr(die, DW_AT_high_pc)) {
        if (dwarf_lowpc(die, &low) != 0) {
            return false;
        }
        if (dwarf_highpc(die, &high) != 0) {
            Dwarf_Attribute  attr_mem;
            Dwarf_Attribute *attr = dwarf_attr(die, DW_AT_high_pc, &attr_mem);
            Dwarf_Word       value;
            if (dwarf_formudata(attr, &value) != 0) {
                return false;
            }
            high = low + value;
        }
        return pc >= low && pc < high;
    }

    // non-continuous range.
    Dwarf_Addr base;
    ptrdiff_t  offset = 0;
    while ((offset = dwarf_ranges(die, offset, &base, &low, &high)) > 0) {
        if (pc >= low && pc < high) {
            return true;
        }
    }
    return false;
}

static Dwarf_Die *find_fundie_by_pc(Dwarf_Die *parent_die, Dwarf_Addr pc, Dwarf_Die *result)
{
    if (dwarf_child(parent_die, result) != 0) {
        return 0;
    }

    Dwarf_Die *die = result;
    do {
        switch (dwarf_tag(die)) {
            case DW_TAG_subprogram:
            case DW_TAG_inlined_subroutine:
                if (die_has_pc(die, pc)) {
                    return result;
                }
        };
        bool            declaration = false;
        Dwarf_Attribute attr_mem;
        dwarf_formflag(dwarf_attr(die, DW_AT_declaration, &attr_mem), &declaration);
        if (!declaration) {
            // let's be curious and look deeper in the tree,
            // function are not necessarily at the first level, but
            // might be nested inside a namespace, structure etc.
            Dwarf_Die  die_mem;
            Dwarf_Die *indie = find_fundie_by_pc(die, pc, &die_mem);
            if (indie) {
                *result = die_mem;
                return result;
            }
        }
    } while (dwarf_siblingof(die, result) == 0);
    return 0;
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

void qd_symbolize_finalize(void)
{
    if (state.libdwlf_inited) {
        dwfl_end(state.dwfl);
    }
    state.libdwlf_inited = false;
}

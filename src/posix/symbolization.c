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

#include <assert.h>
#include <bfd.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
//#include <demangle.h>

/* These global variables are used to pass information between
   translate_addresses and find_address_in_section.  */

static bfd_vma pc;
static const char *filename;
static const char *functionname;
static unsigned int line;
static unsigned int discriminator;
static bool found;

#define DEBUG printf
//static bfd *abfd      = NULL;
static asymbol **syms = NULL;

const char * program_name = "program name";

static void
find_address_in_section (bfd *abfd, asection *section,
                        void *data ATTRIBUTE_UNUSED)
{
    bfd_vma vma;
    bfd_size_type size;

    if (found)
        return;

    if ((bfd_section_flags (section) & SEC_ALLOC) == 0)
        return;

    vma = bfd_section_vma (section);
    if (pc < vma)
        return;

    size = bfd_section_size (section);
    if (pc >= vma + size)
        return;

    found = bfd_find_nearest_line_discriminator (abfd, section, syms, pc - vma,
                                                &filename, &functionname,
                                                &line, &discriminator);
}


bool unwind_inlines = false;
static bool base_names = false;		/* -s, strip directory names.  */
static bool pretty_print = true;
static bool with_addresses = true;
static bool with_functions = true;
static bool do_demangle = true;

static asymbol **syms;		/* Symbol table.  */

///* Flags passed to the name demangler.  */
// static int demangle_flags = DMGL_PARAMS | DMGL_ANSI;

/* Look for an offset in a section.  This is directly called.  */

static void
find_offset_in_section (bfd *abfd, asection *section)
{
    bfd_size_type size;

    if (found)
        return;

    if ((bfd_section_flags (section) & SEC_ALLOC) == 0)
        return;

    size = bfd_section_size (section);
    if (pc >= size)
        return;

    found = bfd_find_nearest_line_discriminator (abfd, section, syms, pc,
                                                &filename, &functionname,
                                                &line, &discriminator);
}

/* Read hexadecimal addresses from stdin, translate into
   file_name:line_number and optionally function name.  */

static void
translate_addresses (bfd *abfd, asection *section)
{
            pc = (bfd_vma) &translate_addresses;

        if (bfd_get_flavour (abfd) == bfd_target_elf_flavour)
        {
            //            const struct elf_backend_data *bed = get_elf_backend_data (abfd);
            //            bfd_vma sign = (bfd_vma) 1 << (bed->s->arch_size - 1);
            //
            //            pc &= (sign << 1) - 1;
            //            if (bed->sign_extend_vma)
            //                pc = (pc ^ sign) - sign;
        }

        if (with_addresses)
        {
            printf ("0x");
            bfd_printf_vma (abfd, pc);

            if (pretty_print)
                printf (": ");
            else
                printf ("\n");
        }

        found = false;
        if (section)
            find_offset_in_section (abfd, section);
        else
            bfd_map_over_sections (abfd, find_address_in_section, NULL);

        if (! found)
        {
            if (with_functions)
            {
                if (pretty_print)
                    printf ("?? ");
                else
                    printf ("??\n");
            }
            printf ("??:0\n");
        }
        else
        {
            while (1)
            {
                if (with_functions)
                {
                    const char *name;
                    char *alloc = NULL;

                    name = functionname;
                    if (name == NULL || *name == '\0')
                        name = "??";
                    else if (do_demangle)
                    {
                        //                        alloc = bfd_demangle (abfd, name, demangle_flags);
                        //                        if (alloc != NULL)
                        //                            name = alloc;
                    }

                    printf ("%s", name);
                    if (pretty_print)
                        /* Note for translators:  This printf is used to join the
                           function name just printed above to the line number/
                           file name pair that is about to be printed below.  Eg:

                             foo at 123:bar.c  */
                        printf (" at ");
                    else
                        printf ("\n");

                    free (alloc);
                }

                if (base_names && filename != NULL)
                {
                    char *h;

                    h = strrchr (filename, '/');
                    if (h != NULL)
                        filename = h + 1;
                }

                printf ("%s:", filename ? filename : "??");
                if (line != 0)
                {
                    if (discriminator != 0)
                        printf ("%u (discriminator %u)\n", line, discriminator);
                    else
                        printf ("%u\n", line);
                }
                else
                    printf ("?\n");
                if (!unwind_inlines)
                    found = false;
                else
                    found = bfd_find_inliner_info (abfd, &filename, &functionname,
                                                  &line);
                if (! found)
                    break;
                if (pretty_print)
                    /* Note for translators: This printf is used to join the
                       line number/file name pair that has just been printed with
                       the line number/file name pair that is going to be printed
                       by the next iteration of the while loop.  Eg:

                         123:bar.c (inlined by) 456:main.c  */
                    printf (" (inlined by) ");
            }
        }

        /* fflush() is essential for using this command as a server
           child process that reads addresses from a pipe and responds
           with line number information, processing one address at a
           time.  */
        fflush (stdout);

}

/* After a FALSE return from bfd_check_format_matches with
   bfd_get_error () == bfd_error_file_ambiguously_recognized, print
   the possible matching targets.  */

void
list_matching_formats (char **p)
{
    fflush (stdout);
    fprintf (stderr, "%s: Matching formats:", program_name);
    while (*p)
        fprintf (stderr, " %s", *p++);
    fputc ('\n', stderr);
}

void
bfd_nonfatal (const char *string)
{
    const char *errmsg;
    enum bfd_error err = bfd_get_error ();

    if (err == bfd_error_no_error)
        errmsg = "cause of error unknown";
    else
        errmsg = bfd_errmsg (err);
    fflush (stdout);
    if (string)
        fprintf (stderr, "%s: %s: %s\n", program_name, string, errmsg);
    else
        fprintf (stderr, "%s: %s\n", program_name, errmsg);
}

void
bfd_fatal (const char *string)
{
    bfd_nonfatal (string);
    exit (1);
}

/* Read in the symbol table.  */

static void
slurp_symtab (bfd *abfd)
{
    long storage;
    long symcount;
    bool dynamic = false;

    if ((bfd_get_file_flags (abfd) & HAS_SYMS) == 0)
        return;

    storage = bfd_get_symtab_upper_bound (abfd);
    if (storage == 0)
    {
        storage = bfd_get_dynamic_symtab_upper_bound (abfd);
        dynamic = true;
    }
    if (storage < 0)
        bfd_fatal (bfd_get_filename (abfd));

    syms = (asymbol **) malloc (storage);
    if (dynamic)
        symcount = bfd_canonicalize_dynamic_symtab (abfd, syms);
    else
        symcount = bfd_canonicalize_symtab (abfd, syms);
    if (symcount < 0)
        bfd_fatal (bfd_get_filename (abfd));

    /* If there are no symbols left after canonicalization and
       we have not tried the dynamic symbols then give them a go.  */
    if (symcount == 0
        && ! dynamic
        && (storage = bfd_get_dynamic_symtab_upper_bound (abfd)) > 0)
    {
        free (syms);
        syms = xmalloc (storage);
        symcount = bfd_canonicalize_dynamic_symtab (abfd, syms);
    }

    /* PR 17512: file: 2a1d3b5b.
       Do not pretend that we have some symbols when we don't.  */
    if (symcount <= 0)
    {
        free (syms);
        syms = NULL;
    }
}

const char *section_name;

int main2() {
    //    const qd_backtrace_fileline_t res = qd_symbolize_backtrace_line((bfd_vma) main);
    //    printf("%s:%d\n", res.funcname, res.line);
    //    free(res.funcname);
    //    qd_symbolize_finalize();

    //    char * string;
    //    debug_sigInit();
    //    debug_translateAddress(string, (bfd_vma) (bfd_hostptr_t) main);
    //    free(string);
    //    debug_sigClose();

    bfd *abfd;
    asection *section;
    char **matching;
    char *file_name = "/proc/self/exe";

    //    if (get_file_size (file_name) < 1)
    //        return 1;

    abfd = bfd_openr (file_name, NULL);
    if (abfd == NULL)
        //        bfd_fatal (file_name);
        ;

    /* Decompress sections.  */
    abfd->flags |= BFD_DECOMPRESS;

    if (bfd_check_format (abfd, bfd_archive))
        printf ("%s: cannot get addresses from archive", file_name);

    if (! bfd_check_format_matches (abfd, bfd_object, &matching))
    {
        bfd_nonfatal (bfd_get_filename (abfd));
        if (bfd_get_error () == bfd_error_file_ambiguously_recognized)
        {
            list_matching_formats (matching);
            free (matching);
        }
        xexit (1);
    }

//    if (section_name != NULL)
//    {
//        section = bfd_get_section_by_name (abfd, section_name);
//        if (section == NULL)
//            printf ("%s: cannot find section %s", file_name, section_name);
//    }
//    else
        section = NULL;

    slurp_symtab (abfd);

    translate_addresses (abfd, section);

    free (syms);
    syms = NULL;

    bfd_close (abfd);

    return 0;
}


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
        if (state.abfd == NULL) goto finalize;
        state.abfd->flags |= BFD_DECOMPRESS;  // https://sourceware.org/legacy-ml/gdb-patches/2012-10/msg00262.html
        bfd_boolean res = bfd_check_format(state.abfd, bfd_object);
        if (!res) goto finalize;
        char **matching;
        if (!bfd_check_format_matches(state.abfd, bfd_object, &matching)) {
            free(matching);
            goto finalize;
        }
    }

    if ((bfd_get_file_flags(state.abfd) & HAS_SYMS) == 0) goto finalize;

    symcount = bfd_read_minisymbols(state.abfd, false, (void **) &state.syms, &symsize);
//    if (symcount == 0) symcount = bfd_read_minisymbols(state.abfd, true, (void **) &state.syms, &symsize);
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

        const char * funcname; // throwaway, but bfd_find_nearest_line does not accept NULL
        if (result.found) continue;
        result.found = bfd_find_nearest_line(state.abfd, section, &state.syms, pc - vma, &result.sourcefile,
                                             &funcname, (unsigned int *)&result.line);

        if (result.found) break;
    }

finalize:
    if (result.found) {
        Dl_info info;
        if (dladdr((void*)pc, &info) != 0) {
            result.funcname = info.dli_sname;
        }
    } else {
        qd_symbolize_finalize();
    }
    return result;
}

void qd_print_symbolized_backtrace_line(FILE *dump_file, const char *fallback_symbolization, int i, void *pc)
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

//    bfd_cache_close_all();

//    state.libbfd_inited = false;
}

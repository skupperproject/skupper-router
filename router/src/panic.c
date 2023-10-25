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

//
// Code for unwinding the stack and printing debug information when a non-recoverable signal occurs.
//

#define _GNU_SOURCE  // to get gettid()

#include "config.h"

#include "qpid/dispatch/threading.h"

#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <link.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef HAVE_LIBUNWIND
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif

static void panic_signal_handler(int signum, siginfo_t *siginfo, void *ucontext);

// expected to be defined by the linker
//
extern char *program_invocation_name;

// Define those signals which will be handled.
//
typedef struct {
    int         signal;
    const char *name;
} panic_signal_info_t;

// clang-format off
static const panic_signal_info_t panic_signals[] = {
    { .signal = SIGABRT, .name = "SIGABRT" },
    { .signal = SIGBUS,  .name = "SIGBUS" },
    { .signal = SIGFPE,  .name = "SIGFPE" },
    { .signal = SIGILL,  .name = "SIGILL" },
    { .signal = SIGSEGV, .name="SIGSEGV" },
    {0}
};
// clang-format on

// Memory map including mapped file path
//
typedef struct {
    uintptr_t base_address;
    char      path[PATH_MAX];
} mem_map_t;

#define MAX_MAP_ENTRIES 64

static mem_map_t mem_map[MAX_MAP_ENTRIES];
static int       mem_map_count = 0;

// for sorting the map
//
static int map_compare(const void *arg1, const void *arg2)
{
    mem_map_t *entry1 = (mem_map_t *) arg1;
    mem_map_t *entry2 = (mem_map_t *) arg2;
    assert(entry1->base_address != entry2->base_address);
    return entry1->base_address > entry2->base_address ? 1 : -1;
}

static void lib_map_init(void)
{
    struct link_map *map = 0;

    void *handle = dlopen(NULL, RTLD_NOW);
    if (handle) {
        int rc = dlinfo(handle, RTLD_DI_LINKMAP, &map);
        if (rc == 0) {
            while (map) {
                assert(mem_map_count < MAX_MAP_ENTRIES);  // update MAX_MAP_ENTRIES, too small!

                mem_map_t *entry = &mem_map[mem_map_count++];

                entry->base_address = (uintptr_t) map->l_addr;

                // KAG: I'm not particularly sure what's going on here, but if map->l_name has strlen() == 0 apparently
                // it means this executable
                if (map->l_name) {
                    if (strlen(map->l_name) == 0) {
                        snprintf(entry->path, PATH_MAX, "%s", program_invocation_name);
                    } else {
                        snprintf(entry->path, PATH_MAX, "%s", map->l_name);
                    }
                } else {
                    snprintf(entry->path, PATH_MAX, "?? UNKNOWN ??");
                }
                map = map->l_next;
            }
        }
        dlclose(handle);
    }

    qsort(mem_map, mem_map_count, sizeof(mem_map[0]), map_compare);
}

/**
 * Install the panic signal handler. This is done early in the router initialization so do not attempt to log or use the
 * alloc pool, etc. This is not called during a signal handler so there is no need to avoid async signal unsafe calls.
 */
void panic_handler_init(void)
{
    if (getenv("SKUPPER_ROUTER_DISABLE_PANIC_HANDLER") == 0) {
        lib_map_init();
        struct sigaction sa = {
            .sa_flags     = SA_SIGINFO | SA_RESETHAND,
            .sa_sigaction = panic_signal_handler,
        };

        sigemptyset(&sa.sa_mask);

        for (int i = 0; panic_signals[i].signal != 0; ++i) {
            sigaction(panic_signals[i].signal, &sa, 0);
        }
    }
}

//
// The remaining routines are invoked by a signal handler. They must not invoke any function that is not Async Signal
// Safe. See man signal(7) and man signal-safety(7). Ignore this sage advice at your on peril..
//

#define BACKTRACE_LIMIT 64
#define BUFFER_SIZE     256
static unsigned char buffer[BUFFER_SIZE];

// async signal safe
//
static void print(const char *str)
{
    size_t ignore = write(STDERR_FILENO, str, strlen((char *) str));
    (void) ignore;
    fsync(STDERR_FILENO);
}

// Convert an unsigned value into a hex string and print it. Async signal safe. If trim then discard leading zeros.
//
static void print_hex_trim(uintptr_t value, bool trim)
{
    int            i   = sizeof(value) * 2;
    unsigned char *ptr = &buffer[BUFFER_SIZE - 1];

    *ptr = 0;
    while (i--) {
        uint8_t nybble = value & 0x0F;

        *--ptr = nybble > 9 ? (nybble - 10) + 'a' : nybble + '0';
        value >>= 4;
        if (value == 0 && trim)
            break;
    }
    print((char *) ptr);
}

// Print a register in hex. No leading '0x' added.
//
static void print_reg(uintptr_t reg)
{
    print_hex_trim(reg, false);
}

// print a base-ten unsigned integer
// async signal safe
//
static void print_uint(uintptr_t num)
{
    if (num == 0)
        print("0");
    else {
        unsigned char *ptr = &buffer[BUFFER_SIZE - 1];

        *ptr = 0;
        while (num > 0) {
            *--ptr = (num % 10) + '0';
            num /= 10;
        }
        print((char *) ptr);
    }
}

#ifdef HAVE_LIBUNWIND

// Print an address offset, stripping leading zeros
//
static void print_offset(uintptr_t offset)
{
    print_hex_trim(offset, true);
}

// given an address find it's mapping and offset
// async signal safe
//
static bool get_offset(uintptr_t address, uintptr_t *offset, const char **path)
{
    *offset = 0;
    *path   = 0;

    // map is sorted lowest address first
    int index = mem_map_count;
    while (index-- > 0) {
        mem_map_t *entry = &mem_map[index];
        if (address >= entry->base_address) {
            *offset = address - entry->base_address;
            *path   = entry->path;
            return true;
        }
    }

    return false;
}

static void print_libunwind_error(int err)
{
    print("ERROR: libunwind failed: ");
    print(unw_strerror(err));
    print("\n");
}

static void print_registers(unw_cursor_t *cursor)
{
#ifdef UNW_TARGET_X86_64
    unw_word_t rax, rbx, rcx, rdx, rdi, rsi, rbp, rsp, r8, r9, r10, r11, r12, r13, r14, r15;

    unw_get_reg(cursor, UNW_X86_64_RAX, &rax);
    unw_get_reg(cursor, UNW_X86_64_RBX, &rbx);
    unw_get_reg(cursor, UNW_X86_64_RCX, &rcx);
    unw_get_reg(cursor, UNW_X86_64_RDX, &rdx);
    unw_get_reg(cursor, UNW_X86_64_RDI, &rdi);
    unw_get_reg(cursor, UNW_X86_64_RSI, &rsi);
    unw_get_reg(cursor, UNW_X86_64_RBP, &rbp);
    unw_get_reg(cursor, UNW_X86_64_RSP, &rsp);
    unw_get_reg(cursor, UNW_X86_64_R8, &r8);
    unw_get_reg(cursor, UNW_X86_64_R9, &r9);
    unw_get_reg(cursor, UNW_X86_64_R10, &r10);
    unw_get_reg(cursor, UNW_X86_64_R11, &r11);
    unw_get_reg(cursor, UNW_X86_64_R12, &r12);
    unw_get_reg(cursor, UNW_X86_64_R13, &r13);
    unw_get_reg(cursor, UNW_X86_64_R14, &r14);
    unw_get_reg(cursor, UNW_X86_64_R15, &r15);

    print("    Registers:\n");
    print("      RAX: 0x");
    print_reg(rax);
    print(" RDI: 0x");
    print_reg(rdi);
    print(" R11: 0x");
    print_reg(r11);
    print("\n");

    print("      RBX: 0x");
    print_reg(rbx);
    print(" RBP: 0x");
    print_reg(rbp);
    print(" R12: 0x");
    print_reg(r12);
    print("\n");

    print("      RCX: 0x");
    print_reg(rcx);
    print(" R8:  0x");
    print_reg(r8);
    print(" R13: 0x");
    print_reg(r13);
    print("\n");

    print("      RDX: 0x");
    print_reg(rdx);
    print(" R9:  0x");
    print_reg(r9);
    print(" R14: 0x");
    print_reg(r14);
    print("\n");

    print("      RSI: 0x");
    print_reg(rsi);
    print(" R10: 0x");
    print_reg(r10);
    print(" R15: 0x");
    print_reg(r15);
    print("\n");
#endif  // UNW_TARGET_X86_64

    unw_word_t sp = {0};
    unw_get_reg(cursor, UNW_REG_SP, &sp);

    print("      SP:  0x");
    print_reg(sp);
    print("\n\n");
}

static void print_stack_frame(int index, unw_cursor_t *cursor)
{
    unw_word_t ip = {0};

    unw_get_reg(cursor, UNW_REG_IP, &ip);

    uintptr_t   offset = 0;
    const char *path   = 0;

    print("[");
    print_uint((uintptr_t) index);
    print("] IP: 0x");
    print_reg(ip);

    // Cannot reliably invoke unw_get_proc_name() from a signal handler due to a bug:
    // https://github.com/libunwind/libunwind/issues/123
    // v1.8.0 hasn't been released yet but I'm assuming it will contain the fix
    //
    if (UNW_VERSION_MAJOR > 1 || (UNW_VERSION_MAJOR == 1 && UNW_VERSION_MINOR >= 8)) {
        unw_word_t off = 0;
        if (unw_get_proc_name(cursor, (char *) buffer, BUFFER_SIZE, &off) == 0) {
            print(":");
            print((char *) buffer);
            print("+0x");
            print_offset((uintptr_t) off);
        }
    }

    if (get_offset((uintptr_t) ip, &offset, &path)) {
        print(" (");
        print(path);
        print("+0x");
        print_offset(offset);
        print(")");
    }
    print("\n");
    print_registers(cursor);
}

static void print_backtrace(unw_context_t *context)
{
    unw_cursor_t cursor;

    int err = unw_init_local(&cursor, context);

    if (err) {
        print_libunwind_error(err);
        return;
    }

    print("\nBacktrace:\n");

    for (int i = 0; i < BACKTRACE_LIMIT; i++) {
        int ret = unw_step(&cursor);

        if (ret < 0) {
            print_libunwind_error(ret);
            break;
        }

        if (ret == 0) {
            break;
        }

        print_stack_frame(i, &cursor);
    }
}

#endif  // HAVE_LIBUNWIND

// signal handler
//
static void panic_signal_handler(int signum, siginfo_t *siginfo, void *ucontext)
{
    (void) siginfo;
    (void) ucontext;

    print("\n*** SKUPPER-ROUTER FATAL ERROR ***\n");  // or "guru meditation error" (google it)
    print("Version: ");
    print(QPID_DISPATCH_VERSION);
    print("\n");

    print("Signal: ");
    print_uint(signum);
    for (int i = 0; panic_signals[i].name; ++i) {
        if (panic_signals[i].signal == signum) {
            print(" ");
            print(panic_signals[i].name);
            break;
        }
    }
    print("\n");

    // Process Info

    print("Process ID: ");
    print_uint((uintptr_t) getpid());
    print(" (");
    print(program_invocation_name);
    print(")\n");

#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ > 29)
    // Thread (note: is gettid() async-safe?)

    print("Thread ID: ");
    print_uint((uintptr_t) gettid());
    print(" (");
    print(sys_thread_name(0));
    print(")\n");
#endif

    // Text segment mapping

#ifndef HAVE_LIBUNWIND
    print("Memory Map:\n");
    for (int i = 0; i < mem_map_count; ++i) {
        print_reg(mem_map[i].base_address);
        print(": ");
        print(mem_map[i].path);
        print("\n");
    }

    print("!!! libunwind not present: backtrace unavailable !!!\n");

#else  // HAVE_LIBUNWIND

    unw_context_t context;

    int err = unw_getcontext(&context);
    if (err) {
        print_libunwind_error(err);
        return;
    }

    print_backtrace(&context);
#endif
    print("*** END ***\n");
}

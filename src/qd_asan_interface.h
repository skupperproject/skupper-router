#ifndef __qd_asan_interface_h__
#define __qd_asan_interface_h__ 1
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

/**
 * Defines the ASAN interface file if available. If not, defines stub macros.
 *
 * See #include <sanitizer/asan_interface.h> in Clang for the source.
 */

#include <stddef.h>

#if defined(__clang__)
# define QD_HAS_ADDRESS_SANITIZER __has_feature(address_sanitizer)
# define QD_HAS_THREAD_SANITIZER __has_feature(thread_sanitizer)
#elif defined (__GNUC__)
# if defined(__SANITIZE_ADDRESS__)
#  define QD_HAS_ADDRESS_SANITIZER __SANITIZE_ADDRESS__
# else
#  define QD_HAS_ADDRESS_SANITIZER 0
# endif
# if defined(__SANITIZE_THREAD__)
#  define QD_HAS_THREAD_SANITIZER __SANITIZE_THREAD__
# else
#  define QD_HAS_THREAD_SANITIZER 0
# endif
#else
# define QD_HAS_ADDRESS_SANITIZER 0
# define QD_HAS_THREAD_SANITIZER 0
#endif

#if QD_HAS_ADDRESS_SANITIZER

#ifdef __cplusplus
extern "C" {
#endif

void __asan_poison_memory_region(void const volatile *addr, size_t size);
void __asan_unpoison_memory_region(void const volatile *addr, size_t size);
void __lsan_do_leak_check(void);
int  __lsan_do_recoverable_leak_check(void);

/// Marks a memory region as unaddressable.
///
/// \note Macro provided for convenience; defined as a no-op if ASan is not
/// enabled.
///
/// \param addr Start of memory region.
/// \param size Size of memory region.
#define ASAN_POISON_MEMORY_REGION(addr, size) __asan_poison_memory_region((addr), (size))

/// Marks a memory region as addressable.
///
/// \note Macro provided for convenience; defined as a no-op if ASan is not
/// enabled.
///
/// \param addr Start of memory region.
/// \param size Size of memory region.
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) __asan_unpoison_memory_region((addr), (size))

// Check for leaks now. This function behaves identically to the default
// end-of-process leak check. In particular, it will terminate the process if
// leaks are found and the exitcode runtime flag is non-zero.
// Subsequent calls to this function will have no effect and end-of-process
// leak check will not run. Effectively, end-of-process leak check is moved to
// the time of first invocation of this function.
// By calling this function early during process shutdown, you can instruct
// LSan to ignore shutdown-only leaks which happen later on.
#define LSAN_DO_LEAK_CHECK() __lsan_do_leak_check()

// Check for leaks now. Returns zero if no leaks have been found or if leak
// detection is disabled, non-zero otherwise.
// This function may be called repeatedly, e.g. to periodically check a
// long-running process. It prints a leak report if appropriate, but does not
// terminate the process. It does not affect the behavior of
// __lsan_do_leak_check() or the end-of-process leak check, and is not
// affected by them.
#define LSAN_DO_RECOVERABLE_LEAK_CHECK() __lsan_do_recoverable_leak_check()

#ifdef __cplusplus
}  // extern "C"
#endif

#else  // QD_HAS_ADDRESS_SANITIZER

#define ASAN_POISON_MEMORY_REGION(addr, size) \
  ((void)(addr), (void)(size))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
  ((void)(addr), (void)(size))

#define LSAN_DO_LEAK_CHECK() /**/
#define LSAN_DO_RECOVERABLE_LEAK_CHECK() 0

#endif  // QD_HAS_ADDRESS_SANITIZER

// https://github.com/google/sanitizers/wiki/AddressSanitizer#turning-off-instrumentation
#if QD_HAS_ADDRESS_SANITIZER || QD_HAS_THREAD_SANITIZER
// note: tsan can also detect some invalid memory accesses and that will crash the binary
# define ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address)) __attribute__((no_sanitize("address")))
#else
# define ATTRIBUTE_NO_SANITIZE_ADDRESS
#endif  // QD_HAS_ADDRESS_SANITIZER

#if QD_HAS_THREAD_SANITIZER
# define ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread)) __attribute__((no_sanitize("thread")))
#else
# define ATTRIBUTE_NO_SANITIZE_THREAD
#endif  // QD_HAS_THREAD_SANITIZER

#endif  // __qd_asan_interface_h__

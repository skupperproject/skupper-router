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

// Copyright 2017 The Fuchsia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

// This header defines thread annotation macros to be used everywhere in Zircon
// outside of publicly exposed headers. See system/public/zircon/compiler.h for
// the publicly exported macros.

// The thread safety analysis system is documented at
// http://clang.llvm.org/docs/ThreadSafetyAnalysis.html and its use in Zircon is documented at
// docs/thread_annotations.md.  The macros we use are:
//
// TA_CAP(x)                    |x| is the capability this type represents, e.g. "mutex".
// TA_GUARDED(x)                the annotated variable is guarded by the capability (e.g. lock) |x|
// TA_ACQ(x)                    function acquires the mutex |x|
// TA_ACQ_SHARED(x)             function acquires the mutex |x| for shared reading
// TA_ACQ_BEFORE(x)             Indicates that if both this mutex and muxex |x| are to be acquired,
//                              that this mutex must be acquired before mutex |x|.
// TA_ACQ_AFTER(x)              Indicates that if both this mutex and muxex |x| are to be acquired,
//                              that this mutex must be acquired after mutex |x|.
// TA_TRY_ACQ(bool, x)          function acquires the mutex |x| if the function returns |bool|
// TA_TRY_ACQ_SHARED(bool, x)   function acquires the mutex |x| for shared reading if the function
//                              returns |bool|
// TA_REL(x)                    function releases the mutex |x|
// TA_REL_SHARED(x)             function releases the shared for reading mutex |x|
// TA_ASSERT(x)                 function asserts that |x| is held
// TA_ASSERT_SHARED(x)          function asserts that |x| is held for shared reading
// TA_REQ(x)                    function requires that the caller hold the mutex |x|
// TA_REQ_SHARED(x)             function requires that the caller hold the mutex |x| for shared
// reading TA_EXCL(x)                   function requires that the caller not be holding the mutex
// |x| TA_RET_CAP(x)                function returns a reference to the mutex |x| TA_SCOPED_CAP type
// represents a scoped or RAII-style wrapper around a capability TA_NO_THREAD_SAFETY_ANALYSIS
// function is excluded entirely from thread safety analysis

#ifdef __clang__
#define TA_SUPPRESS _Pragma("clang diagnostic ignored \"-Wthread-safety-analysis\"")
#else
#define TA_SUPPRESS
#endif

#ifdef __clang__
#define THREAD_ANNOTATION(x) __attribute__((x))
#else
#define THREAD_ANNOTATION(x)
#endif

#define TA_CAP(x)                    THREAD_ANNOTATION(capability(x))
#define TA_GUARDED(x)                THREAD_ANNOTATION(guarded_by(x))
#define TA_ACQ(...)                  THREAD_ANNOTATION(acquire_capability(__VA_ARGS__))
#define TA_ACQ_SHARED(...)           THREAD_ANNOTATION(acquire_shared_capability(__VA_ARGS__))
#define TA_ACQ_BEFORE(...)           THREAD_ANNOTATION(acquired_before(__VA_ARGS__))
#define TA_ACQ_AFTER(...)            THREAD_ANNOTATION(acquired_after(__VA_ARGS__))
#define TA_TRY_ACQ(...)              THREAD_ANNOTATION(try_acquire_capability(__VA_ARGS__))
#define TA_TRY_ACQ_SHARED(...)       THREAD_ANNOTATION(try_acquire_shared_capability(__VA_ARGS__))
#define TA_REL(...)                  THREAD_ANNOTATION(release_capability(__VA_ARGS__))
#define TA_REL_SHARED(...)           THREAD_ANNOTATION(release_shared_capability(__VA_ARGS__))
#define TA_REL_GENERIC(...)          THREAD_ANNOTATION(release_generic_capability(__VA_ARGS__))
#define TA_ASSERT(...)               THREAD_ANNOTATION(assert_capability(__VA_ARGS__))
#define TA_ASSERT_SHARED(...)        THREAD_ANNOTATION(assert_shared_capability(__VA_ARGS__))
#define TA_REQ(...)                  THREAD_ANNOTATION(requires_capability(__VA_ARGS__))
#define TA_REQ_SHARED(...)           THREAD_ANNOTATION(requires_shared_capability(__VA_ARGS__))
#define TA_EXCL(...)                 THREAD_ANNOTATION(locks_excluded(__VA_ARGS__))
#define TA_RET_CAP(x)                THREAD_ANNOTATION(lock_returned(x))
#define TA_SCOPED_CAP                THREAD_ANNOTATION(scoped_lockable)
#define TA_NO_THREAD_SAFETY_ANALYSIS THREAD_ANNOTATION(no_thread_safety_analysis)

typedef void const * const core_thread_capability_t TA_CAP("core_thread");

extern core_thread_capability_t core_thread_capability;

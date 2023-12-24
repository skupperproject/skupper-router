#ifndef QPID_DISPATCH_ANNOTATIONS_H
#define QPID_DISPATCH_ANNOTATIONS_H 1

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

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>

// no return
#if defined(__GNUC__)
#define QD_NORETURN __attribute__((noreturn))
#else
#define QD_NORETURN [[noreturn]]
#endif

// always inline
#ifdef _MSC_VER
#define QD_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__)
#define QD_ALWAYS_INLINE inline __attribute__((__always_inline__))
#else
#define QD_ALWAYS_INLINE inline
#endif

// helpers
QD_NORETURN QD_ALWAYS_INLINE void qd_compiler_may_unsafely_assume_unreachable(void);

/**
 * assume_unreachable() informs the compiler that the statement is not reachable
 * at runtime. It is undefined behavior if the statement is actually reached.
 *
 * Common use cases are to suppress a warning when the compiler cannot prove
 * that the end of a non-void function is not reachable, or to optimize the
 * evaluation of switch/case statements when all the possible values are
 * provably enumerated.
 */
QD_NORETURN QD_ALWAYS_INLINE void qd_assume_unreachable(void)
{
    qd_compiler_may_unsafely_assume_unreachable();
}


QD_NORETURN QD_ALWAYS_INLINE void qd_compiler_may_unsafely_assume_unreachable(void)
{
    assert(false); // "compiler-hint unreachability reached at runtime");
#if defined(__GNUC__)
    __builtin_unreachable();
#elif defined(_MSC_VER)
    __assume(0);
#else
    while (!0)
        ;
#endif
}

#endif // QPID_DISPATCH_ANNOTATIONS_H

#ifndef __sys_atomic_h__
#define __sys_atomic_h__ 1
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

/**@file
 * Portable atomic operations on uint32_t.
 */

#include <stdint.h>

#ifdef __cplusplus
// Compatibility maneuver required due to c_unittests written in C++.
// The stdatomic.h header is proposed only for C++23, which is still far out.
extern "C++" {
#include <atomic>
}
using std::atomic_uint;
#else
#include <stdatomic.h>
#endif

typedef atomic_uint sys_atomic_t;

static inline void sys_atomic_init(sys_atomic_t *ref, uint32_t value)
{
    atomic_init(ref, value);
}

static inline uint32_t sys_atomic_add(sys_atomic_t *ref, uint32_t value)
{
    return atomic_fetch_add(ref, value);
}

static inline uint32_t sys_atomic_sub(sys_atomic_t *ref, uint32_t value)
{
    return atomic_fetch_sub(ref, value);
}

static inline uint32_t sys_atomic_get(sys_atomic_t *ref)
{
    return atomic_load(ref);
}

static inline uint32_t sys_atomic_set(sys_atomic_t *ref, uint32_t value)
{
    return atomic_exchange(ref, value);
}

static inline void sys_atomic_destroy(sys_atomic_t *ref) {}

#define    SET_ATOMIC_FLAG(flag)        sys_atomic_set((flag), 1)
#define  CLEAR_ATOMIC_FLAG(flag)        sys_atomic_set((flag), 0)
#define    SET_ATOMIC_BOOL(flag, value) sys_atomic_set((flag), ((value) ? 1 : 0))

#define IS_ATOMIC_FLAG_SET(flag)       (sys_atomic_get(flag) == 1)

/** Atomic increase: NOTE returns value *before* increase, like i++ */
static inline uint32_t sys_atomic_inc(sys_atomic_t *ref) { return sys_atomic_add((ref), 1); }

/** Atomic decrease: NOTE returns value *before* decrease, like i-- */
static inline uint32_t sys_atomic_dec(sys_atomic_t *ref) { return sys_atomic_sub((ref), 1); }

#endif

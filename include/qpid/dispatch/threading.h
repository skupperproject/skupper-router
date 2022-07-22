#ifndef __sys_threading_h__
#define __sys_threading_h__ 1
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
 * Threading and locking API.
 */

#include <pthread.h>

typedef struct sys_mutex_t sys_mutex_t;
struct sys_mutex_t {
    pthread_mutex_t mutex;
};

void sys_mutex_init(sys_mutex_t *mutex);
void sys_mutex_free(sys_mutex_t *mutex);
void sys_mutex_lock(sys_mutex_t *mutex);
void sys_mutex_unlock(sys_mutex_t *mutex);

typedef struct sys_cond_t sys_cond_t;
struct sys_cond_t {
    pthread_cond_t cond;
};

void sys_cond_init(sys_cond_t *cond);
void sys_cond_free(sys_cond_t *cond);
void sys_cond_wait(sys_cond_t *cond, sys_mutex_t *held_mutex);
void sys_cond_signal(sys_cond_t *cond);
void sys_cond_signal_all(sys_cond_t *cond);


typedef struct sys_rwlock_t sys_rwlock_t;
struct sys_rwlock_t {
    pthread_rwlock_t lock;
};

void sys_rwlock_init(sys_rwlock_t *lock);
void sys_rwlock_free(sys_rwlock_t *lock);
void sys_rwlock_wrlock(sys_rwlock_t *lock);
void sys_rwlock_rdlock(sys_rwlock_t *lock);
void sys_rwlock_unlock(sys_rwlock_t *lock);


typedef struct sys_thread_t sys_thread_t;

sys_thread_t *sys_thread(const char *thread_name, void *(*run_function) (void *), void *arg);
void          sys_thread_free(sys_thread_t *thread);
void          sys_thread_join(sys_thread_t *thread);
sys_thread_t *sys_thread_self(void);

#endif

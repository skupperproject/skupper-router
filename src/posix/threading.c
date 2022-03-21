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

// obtain access to pthread_setname_np
#define _GNU_SOURCE

//
// Enable debug for asserts in this module regardless of what the project-wide
// setting is.
//
#undef NDEBUG

#include "qpid/dispatch/threading.h"

#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/internal/thread_annotations.h"

#include <assert.h>
#include <pthread.h>

void sys_mutex_init(sys_mutex_t *mutex)
{
    int result = pthread_mutex_init(&(mutex->mutex), 0);
    (void) result; assert(result == 0);
}


void sys_mutex_free(sys_mutex_t *mutex)
{
    int result = pthread_mutex_destroy(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_mutex_lock(sys_mutex_t *mutex) TA_ACQ(*mutex) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_mutex_lock(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_mutex_unlock(sys_mutex_t *mutex) TA_REL(*mutex) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_mutex_unlock(&(mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_cond_init(sys_cond_t *cond)
{
    int result = pthread_cond_init(&(cond->cond), 0);
    (void) result; assert(result == 0);
}


void sys_cond_free(sys_cond_t *cond)
{
    int result = pthread_cond_destroy(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_cond_wait(sys_cond_t *cond, sys_mutex_t *held_mutex) TA_REQ(*held_mutex)
{
    int result = pthread_cond_wait(&(cond->cond), &(held_mutex->mutex));
    (void) result; assert(result == 0);
}


void sys_cond_signal(sys_cond_t *cond)
{
    int result = pthread_cond_signal(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_cond_signal_all(sys_cond_t *cond)
{
    int result = pthread_cond_broadcast(&(cond->cond));
    (void) result; assert(result == 0);
}


void sys_rwlock_init(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_init(&(lock->lock), 0);
    (void) result; assert(result == 0);
}


void sys_rwlock_free(sys_rwlock_t *lock)
{
    int result = pthread_rwlock_destroy(&(lock->lock));
    (void) result; assert(result == 0);
}


void sys_rwlock_wrlock(sys_rwlock_t *lock) TA_ACQ(*lock) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_rwlock_wrlock(&(lock->lock));
    assert(result == 0);
}


void sys_rwlock_rdlock(sys_rwlock_t *lock) TA_ACQ_SHARED(*lock) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_rwlock_rdlock(&(lock->lock));
    assert(result == 0);
}


void sys_rwlock_unlock(sys_rwlock_t *lock) TA_REL_GENERIC(*lock) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_rwlock_unlock(&(lock->lock));
    assert(result == 0);
}

#define SYS_THREAD_NAME_MAX 15  // thread name may be at most 15 bytes

static const char thread_names[SYS_THREAD_ROLE_COUNT][SYS_THREAD_NAME_MAX + 1] = {
    // Indexed by sys_thread_role_t. pthread names must be < 15 chars.
    "main",          // SYS_THREAD_MAIN - root thread (not pthread!)
    "core_thread",   // SYS_THREAD_CORE
    "wrkr_",         // SYS_THREAD_PROACTOR (multiple)
    "vflow_thread",  // SYS_THREAD_VFLOW
    "lws_thread"     // SYS_THREAD_LWS_HTTP
};

static sys_atomic_t proactor_thread_count = 0;

void sys_spinlock_init(sys_spinlock_t *lock)
{
    int result;
    result = pthread_mutexattr_init(&lock->attr);
    assert(result == 0);
    result = pthread_mutexattr_settype(&lock->attr, PTHREAD_MUTEX_ADAPTIVE_NP);
    assert(result == 0);
    result = pthread_mutex_init(&lock->lock, &lock->attr);
    assert(result == 0);
}


void sys_spinlock_free(sys_spinlock_t *lock)
{
    int result = pthread_mutex_destroy(&lock->lock);
    assert(result == 0);
    result = pthread_mutexattr_destroy(&lock->attr);
    assert(result == 0);
}


void sys_spinlock_lock(sys_spinlock_t *lock) TA_ACQ(*lock) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_mutex_lock(&(lock->lock));
    assert(result == 0);
}


void sys_spinlock_unlock(sys_spinlock_t *lock) TA_REL(*lock) TA_NO_THREAD_SAFETY_ANALYSIS
{
    int result = pthread_mutex_unlock(&(lock->lock));
    assert(result == 0);
}



struct sys_thread_t {
    pthread_t thread;
    void *(*f)(void *);
    void *arg;
    void                      *proactor_context;
    sys_thread_proactor_mode_t proactor_mode;
    sys_thread_role_t          role;
    char                       name[SYS_THREAD_NAME_MAX + 1];
};

// initialize the per-thread _self to a non-zero value.  This dummy value will
// be returned when sys_thread_self() is called from the process's main thread
// of execution (which is not a pthread).  Using a non-zero value provides a
// way to distinguish a thread id from a zero (unset) value.
//
static sys_thread_t           _main_thread = {.role = SYS_THREAD_MAIN, .name = "main"};
static __thread sys_thread_t *_self        = &_main_thread;

// bootstrap _self before calling thread's main function
//
static void *_thread_init(void *arg)
{
    _self = (sys_thread_t*) arg;
    return _self->f(_self->arg);
}

sys_thread_t *sys_thread(sys_thread_role_t role, void *(*run_function)(void *), void *arg)
{
    assert(role < SYS_THREAD_ROLE_COUNT);

    sys_thread_t *thread = NEW(sys_thread_t);
    ZERO(thread);

    thread->f    = run_function;
    thread->arg  = arg;
    thread->role = role;

    if (role == SYS_THREAD_PROACTOR) {
        int rc = snprintf(thread->name, sizeof(thread->name), "%s%d", thread_names[role],
                          (int) sys_atomic_inc(&proactor_thread_count));
        (void) rc;
        assert(rc > 0 && rc < 16);
    } else {
        strcpy(thread->name, thread_names[role]);
    }
    int rc = pthread_create(&(thread->thread), 0, _thread_init, (void *) thread);
    (void) rc;
    assert(rc == 0);
    pthread_setname_np(thread->thread, thread->name);

    return thread;
}

// note: called by signal handler, do not invoke async signal unsafe code in this function!  See man signal(7)
sys_thread_t *sys_thread_self(void)
{
    return _self;
}

// note: called by signal handler, do not invoke async signal unsafe code in this function!  See man signal(7)
const char *sys_thread_name(const sys_thread_t *thread)
{
    if (thread == 0)
        thread = _self;
    return thread->name;
}

void sys_thread_free(sys_thread_t *thread)
{
    assert(thread != &_main_thread);
    free(thread);
}


void sys_thread_join(sys_thread_t *thread)
{
    assert(thread != &_main_thread);
    pthread_join(thread->thread, 0);
}

sys_thread_role_t sys_thread_role(const sys_thread_t *thread)
{
    if (!thread)
        thread = sys_thread_self();
    return thread->role;
}

sys_thread_proactor_mode_t sys_thread_proactor_mode(void)
{
    return _self->proactor_mode;
}

sys_thread_proactor_mode_t sys_thread_proactor_set_mode(sys_thread_proactor_mode_t new_mode, void *context)
{
    assert(_self->role == SYS_THREAD_PROACTOR);
    sys_thread_proactor_mode_t old_mode = _self->proactor_mode;
    _self->proactor_mode                = new_mode;
    _self->proactor_context             = context;
    return old_mode;
}

void *sys_thread_proactor_context(void)
{
    assert(_self->role == SYS_THREAD_PROACTOR);
    return _self->proactor_context;
}

// Avoid calling this function directly: use CHECK_PROACTOR_CONNECTION() instead
//
void _sys_thread_proactor_check_connection(const struct pn_connection_t *conn, const char *file, int line)
{
    if ((sys_thread_proactor_mode() & SYS_THREAD_PROACTOR_MODE_CONNECTION) == 0
        || conn != (const struct pn_connection_t *) sys_thread_proactor_context()) {
        fprintf(stderr, "Invalid proactor AMQP connection @ %s:%d\n", file, line);
        abort();
    }
}

// Avoid calling this function directly: use CHECK_PROACTOR_RAW_CONNECTION() instead
//
void _sys_thread_proactor_check_raw_connection(const struct pn_raw_connection_t *raw_conn, const char *file, int line)
{
    if ((sys_thread_proactor_mode() & SYS_THREAD_PROACTOR_MODE_RAW_CONNECTION) == 0
        || raw_conn != (const struct pn_raw_connection_t *) sys_thread_proactor_context()) {
        fprintf(stderr, "Invalid proactor raw connection @ %s:%d\n", file, line);
        abort();
    }
}

// Avoid calling this function directly: use CHECK_PROACTOR_LISTENER() instead
//
void _sys_thread_proactor_check_listener(const struct pn_listener_t *listener, const char *file, int line)
{
    if ((sys_thread_proactor_mode() & SYS_THREAD_PROACTOR_MODE_LISTENER) == 0
        || listener != (const struct pn_listener_t *) sys_thread_proactor_context()) {
        fprintf(stderr, "Invalid proactor listener @ %s:%d\n", file, line);
        abort();
    }
}

// DEBUG/TEST only. Simple thread function that exits after taking a lock
//
static void *test_thread(void *arg)
{
    sys_mutex_t *lock = (sys_mutex_t *) arg;

    sys_mutex_lock(lock);
    sys_mutex_unlock(lock);

    return 0;
}

// DEBUG/TEST only (see test/thread_test.c)
char *test_threading_roles_names(void *context)
{
    sys_mutex_t lock;
    const char *name_ptr;

    sys_mutex_init(&lock);

    // check main thread is correct
    if (sys_thread_role(0) != SYS_THREAD_MAIN) {
        return "FAILED: expected SYS_THREAD_MAIN";
    }

    name_ptr = sys_thread_name(0);
    if (strcmp(name_ptr, thread_names[SYS_THREAD_MAIN]) != 0)
        return "FAILED: main thread name mismatch";

    // verify proactor threads get unique names

    sys_atomic_set(&proactor_thread_count, 0);

    for (int i = 0; i < 5; i++) {
        char p_name[16];

        sys_mutex_lock(&lock);
        sys_thread_t *t = sys_thread(SYS_THREAD_PROACTOR, test_thread, &lock);
        if (sys_thread_role(t) != SYS_THREAD_PROACTOR) {
            sys_mutex_unlock(&lock);
            sys_thread_join(t);
            sys_thread_free(t);
            return "FAILED: expected thread role SYS_THREAD_PROACTOR";
        }
        name_ptr = sys_thread_name(t);
        snprintf(p_name, sizeof(p_name), "%s%d", thread_names[SYS_THREAD_PROACTOR], i);
        if (strcmp(p_name, name_ptr) != 0) {
            fprintf(stdout, "THREAD NAME=%s PNAME=%s\n", name_ptr, p_name);
            sys_mutex_unlock(&lock);
            sys_thread_join(t);
            sys_thread_free(t);
            return "FAILED: proactor thread name mismatch";
        }
        sys_mutex_unlock(&lock);
        sys_thread_join(t);
        sys_thread_free(t);
    }

    // check non-proactor thread roles and names

    sys_thread_role_t roles[3] = {
        SYS_THREAD_CORE,
        SYS_THREAD_VFLOW,
        SYS_THREAD_LWS_HTTP,
    };

    for (int i = 0; i < 3; i++) {
        sys_mutex_lock(&lock);

        sys_thread_t *t = sys_thread(roles[i], test_thread, &lock);
        if (sys_thread_role(t) != roles[i]) {
            sys_mutex_unlock(&lock);
            sys_thread_join(t);
            sys_thread_free(t);
            return "FAILED: expected thread role mismatch";
        }
        name_ptr = sys_thread_name(t);
        if (strcmp(name_ptr, thread_names[roles[i]]) != 0) {
            sys_mutex_unlock(&lock);
            sys_thread_join(t);
            sys_thread_free(t);
            return "FAILED: thread name mismatch";
        }
        sys_mutex_unlock(&lock);
        sys_thread_join(t);
        sys_thread_free(t);
    }

    return 0;
}

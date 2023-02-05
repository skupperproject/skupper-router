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
#include "test_case.h"

#include "qpid/dispatch/threading.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define thread_count 10
static sys_thread_t *threads[thread_count] = {0};

static sys_mutex_t   mutex;
static sys_cond_t    cond;

static char         *result;

// for test_thread_id. Note well: never set result to zero here or you'll lose the test failures!
//
void *thread_id_thread(void *arg)
{
    intptr_t index = (intptr_t) arg;
    assert(index < thread_count);

    char expected_name[16];
    snprintf(expected_name, 16, "wrkr_%hu", (unsigned short) index);

    sys_mutex_lock(&mutex);

    // check if self corresponds to my index in threads[]
    if (!sys_thread_self()) {
        result = "sys_thread_self returned zero!";
    } else if (threads[index] != sys_thread_self()) {
        result = "sys_thread_self mismatch";
    } else if (strcmp(sys_thread_name(0), expected_name) != 0) {
        result = "sys_thread_name mismatch";
    }

    sys_mutex_unlock(&mutex);

    return 0;
}


// ensure sys_thread_self is correct
//
static char *test_thread_id(void *context)
{
    sys_mutex_init(&mutex);
    sys_mutex_lock(&mutex);

    // start threads and retain their addresses
    //
    result = 0;
    memset(threads, 0, sizeof(threads));
    for (intptr_t i = 0; i < thread_count; ++i) {
        threads[i] = sys_thread(SYS_THREAD_PROACTOR, thread_id_thread, (void *) i);
    }

    sys_mutex_unlock(&mutex);

    for (int i = 0; i < thread_count; ++i) {
        sys_thread_join(threads[i]);
        sys_thread_free(threads[i]);
    }

    if (result)
        return result;

    //
    // test calling sys_thread_self() from the main context.  This context
    // was not created by sys_thread(), however a dummy non-zero value is returned.
    //
    sys_thread_t *main_id = sys_thread_self();
    if (!main_id) {
        result = "sys_thread_self() returned 0 for main thread";
    } else {
        for (int i = 0; i < thread_count; ++i) {
            if (threads[i] == main_id) {   // must be unique!
                result = "main thread sys_thread_self() not unique!";
                break;
            }
        }
    }

    sys_mutex_free(&mutex);
    return result;
}



static int cond_count;


// run by test_condition
//
void *test_condition_thread(void *arg)
{
    int *test = (int *)arg;

    sys_mutex_lock(&mutex);
    while (*test == 0) {
        // it is expected that cond_count will never be > 1 since the condition
        // is triggered only once
        cond_count += 1;
        sys_cond_wait(&cond, &mutex);
    }
    if (*test != 1) {
        result = "error expected *test to be 1";
    } else {
        *test += 1;
    }
    sys_mutex_unlock(&mutex);

    return 0;
}


static char *test_condition(void *context)
{
    sys_mutex_init(&mutex);
    sys_cond_init(&cond);

    sys_mutex_lock(&mutex);

    int test = 0;
    cond_count = 0;
    result = 0;
    sys_thread_t *thread = sys_thread(SYS_THREAD_PROACTOR, test_condition_thread, &test);

    sys_mutex_unlock(&mutex);

    // let thread run and block on condition
    sleep(1);

    sys_mutex_lock(&mutex);

    if (cond_count != 1) {
        result = "expected thread to wait on condition";
    }

    test = 1;

    sys_cond_signal(&cond);
    sys_mutex_unlock(&mutex);

    sys_thread_join(thread);
    sys_thread_free(thread);

    if (!result && test != 2) {
        result = "expected thread to increment test variable";
    }

    sys_cond_free(&cond);
    sys_mutex_free(&mutex);
    return result;
}

static sys_mutex_t _mode_test_mutex;

// note: do not clear *reason - doing so will mask previous test failures!
static void *_mode_test_thread(void *arg)
{
    char **reason = (char **) arg;

    // wait until test is ready to run
    sys_mutex_lock(&_mode_test_mutex);
    sys_mutex_unlock(&_mode_test_mutex);

    // haven't assigned a mode yet:
    if (sys_thread_proactor_mode() != SYS_THREAD_PROACTOR_MODE_NONE) {
        *reason = "Subthread expected default mode, failed!";
        return 0;
    }

    if (sys_thread_proactor_context() != (void *) 0) {
        *reason = "Subthread expected default proactor context to be zero, failed!";
        return 0;
    }

    sys_thread_proactor_mode_t old_mode = sys_thread_proactor_set_mode(SYS_THREAD_PROACTOR_MODE_LISTENER, (void *) 123);
    if (old_mode != SYS_THREAD_PROACTOR_MODE_NONE) {
        *reason = "Subthread expected old mode to be default, failed!";
        return 0;
    }

    if (sys_thread_proactor_mode() != SYS_THREAD_PROACTOR_MODE_LISTENER) {
        *reason = "Subthread expected new mode LISTENER, failed!";
        return 0;
    }

    if (sys_thread_proactor_context() != (void *) 123) {
        *reason = "Subthread expected new LISTENER context, failed!";
        return 0;
    }

    return 0;
}

char *test_threading_mode(void *context)
{
    if (sys_thread_proactor_mode() != SYS_THREAD_PROACTOR_MODE_NONE) {
        return "Main thread expected default mode, failed!";
    }

    char *reason = 0;
    sys_mutex_init(&_mode_test_mutex);
    sys_mutex_lock(&_mode_test_mutex);
    sys_thread_t *t = sys_thread(SYS_THREAD_PROACTOR, _mode_test_thread, &reason);
    // allow _mode_test_thread to run:
    sys_mutex_unlock(&_mode_test_mutex);

    sys_thread_join(t);
    sys_thread_free(t);

    sys_mutex_free(&_mode_test_mutex);

    if (reason)
        return reason;

    if (sys_thread_proactor_mode() != SYS_THREAD_PROACTOR_MODE_NONE) {
        return "Main thread mode changed unexpectantly, failed!";
    }

    return 0;
}

int thread_tests(void)
{
    int result = 0;
    char *test_group = "thread_tests";

    TEST_CASE(test_thread_id, 0);
    TEST_CASE(test_condition, 0);
    TEST_CASE(test_threading_roles_names, 0);
    TEST_CASE(test_threading_mode, 0);

    return result;
}


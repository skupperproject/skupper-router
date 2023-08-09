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

#include "qpid/dispatch/alloc.h"

#include "test_case.h"

#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/threading.h"

#include <inttypes.h>
#include <stdio.h>

typedef struct {
    int A;
    int B;
} object_t;

#define TEST_TRANSFER_BATCH_SIZE  3
#define TEST_LOCAL_FREE_LIST_MAX  7
#define TEST_GLOBAL_FREE_LIST_MAX 10
const qd_alloc_config_t config = {.transfer_batch_size  = TEST_TRANSFER_BATCH_SIZE,
                                  .local_free_list_max  = TEST_LOCAL_FREE_LIST_MAX,
                                  .global_free_list_max = TEST_GLOBAL_FREE_LIST_MAX};

ALLOC_DECLARE_SAFE(object_t);
ALLOC_DEFINE_CONFIG_SAFE(object_t, sizeof(object_t), 0, &config);

static char *check_stats(qd_alloc_stats_t stats, uint64_t ah, uint64_t fh, uint64_t ht, uint64_t rt, uint64_t rg)
{
    if (stats.total_alloc_from_heap != ah)
        return "Incorrect alloc-from-heap";
    if (stats.total_free_to_heap != fh)
        return "Incorrect free-to-heap";
    if (stats.held_by_threads != ht)
        return "Incorrect held-by-threads";
    if (stats.batches_rebalanced_to_threads != rt)
        return "Incorrect rebalance-to-threads";
    if (stats.batches_rebalanced_to_global != rg)
        return "Incorrect rebalance-to-global";
    return 0;
}


static char* test_alloc_basic(void *context)
{
    object_t         *obj[50];
    int               idx;
    char             *error = 0;

    for (idx = 0; idx < 20; idx++)
        obj[idx] = new_object_t();
    error = check_stats(alloc_stats_object_t(), 21, 0, 21, 0, 0);
    for (idx = 0; idx < 20; idx++)
        free_object_t(obj[idx]);
    if (error) return error;

    error = check_stats(alloc_stats_object_t(), 21, 5, 6, 0, 5);
    if (error) return error;

    for (idx = 0; idx < 20; idx++)
        obj[idx] = new_object_t();
    error = check_stats(alloc_stats_object_t(), 27, 5, 21, 3, 5);
    for (idx = 0; idx < 20; idx++)
        free_object_t(obj[idx]);
    if (error) return error;

    return 0;
}


static char *test_safe_references(void *context)
{
    object_t    *obj = new_object_t();
    object_t_sp  safe_obj;

    set_safe_ptr_object_t(obj, &safe_obj);
    object_t *alias = safe_deref_object_t(safe_obj);

    if (obj != alias)
        return "Safe alias was not equal to the original pointer";

    free_object_t(obj);
    alias = safe_deref_object_t(safe_obj);

    if (alias != 0)
        return "Safe dereference of a freed object was not null";

    return 0;
}

//
// Multi-threading test
//

#define THREAD_COUNT      10
#define PER_THREAD_ALLOC  (TEST_GLOBAL_FREE_LIST_MAX * 10)
#define THREAD_LOOP_COUNT 100

static sys_atomic_t test_failed;

static void *alloc_test_thread(void *arg)
{
    object_t *objects[PER_THREAD_ALLOC] = {0};

    // never set this atomic flag to true in this function!
    // It must only be set true if the test fails!
    sys_atomic_t *test_failed = (sys_atomic_t *) arg;

    for (int i = 0; i < THREAD_LOOP_COUNT; ++i) {
        for (int j = 0; j < PER_THREAD_ALLOC; ++j) {
            objects[j] = new_object_t();
            if (!objects[j]) {
                fprintf(stderr, "ERROR: Failed to allocate a new object_t!");
                SET_ATOMIC_FLAG(test_failed);
                goto exit;
            }
        }
        qd_alloc_stats_t stats     = alloc_stats_object_t();
        uint64_t         allocated = stats.total_alloc_from_heap - stats.total_free_to_heap;
        if (allocated < PER_THREAD_ALLOC) {
            fprintf(stderr, "ERROR: total_alloc_from_heap too low! Expected >= %d, got %" PRIu64 "\n", PER_THREAD_ALLOC,
                    allocated);
            SET_ATOMIC_FLAG(test_failed);
            goto exit;
        }
        if (stats.held_by_threads < PER_THREAD_ALLOC) {
            fprintf(stderr, "ERROR: held_by_threads too low! Expected >= %d, got %" PRIu64 "\n", PER_THREAD_ALLOC,
                    stats.held_by_threads);
            SET_ATOMIC_FLAG(test_failed);
            goto exit;
        }
        for (int j = 0; j < PER_THREAD_ALLOC; ++j) {
            free_object_t(objects[j]);
            objects[j] = 0;
        }
    }

exit:
    for (int j = 0; j < PER_THREAD_ALLOC; ++j) {
        free_object_t(objects[j]);
    }

    return (void *) 0;
}

static char *test_threaded_alloc(void *context)
{
    sys_thread_t *threads[THREAD_COUNT] = {0};
    char         *result                = 0;

    sys_atomic_init(&test_failed, 0);

    for (int i = 0; i < THREAD_COUNT; ++i) {
        threads[i] = sys_thread(SYS_THREAD_PROACTOR, alloc_test_thread, (void *) &test_failed);
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        sys_thread_join(threads[i]);
        sys_thread_free(threads[i]);
    }

    if (IS_ATOMIC_FLAG_SET(&test_failed)) {
        result = "Threaded alloc_pool test failed!";
    } else {
        // check statistics: include main in thread count
        //
        qd_alloc_stats_t stats         = alloc_stats_object_t();
        uint64_t         allocated     = stats.total_alloc_from_heap - stats.total_free_to_heap;
        uint64_t         max_allocated = TEST_GLOBAL_FREE_LIST_MAX + ((THREAD_COUNT + 1) * TEST_LOCAL_FREE_LIST_MAX);

        if (allocated > max_allocated) {
            fprintf(stderr, "ERROR: too many allocated objects remain, expected %" PRIu64 " got %" PRIu64 "\n",
                    max_allocated, allocated);
            result = "Too many allocated objects remain";
        }
    }

    sys_atomic_destroy(&test_failed);

    return result;
}

int alloc_tests(void)
{
    int result = 0;
    char *test_group = "alloc_tests";

    TEST_CASE(test_alloc_basic, 0);  // must be first: expects counters to be zeroed
    TEST_CASE(test_safe_references, 0);
    TEST_CASE(test_threaded_alloc, 0);

    return result;
}


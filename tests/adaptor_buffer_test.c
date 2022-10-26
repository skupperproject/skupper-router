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
#include "adaptors/adaptor_buffer.h"

#include "test_case.h"

void qd_log_initialize(void);
void qd_log_finalize(void);
void qd_error_initialize();
void qd_router_id_finalize(void);

static char *test_qd_adaptor_buffer(void *context)
{
    qd_adaptor_buffer_t *adaptor_buffer = qd_adaptor_buffer();
    if (qd_adaptor_buffer_size(adaptor_buffer) != 0) {
        return "adaptor_buffer size is not zero";
    }
    if (DEQ_NEXT(adaptor_buffer) != 0) {
        return "adaptor_buffer next is not zero";
    }
    if (DEQ_PREV(adaptor_buffer) != 0) {
        return "adaptor_buffer next is not zero";
    }
    qd_adaptor_buffer_free(adaptor_buffer);
    return 0;
}

static char *test_qd_adaptor_buffer_list_free_buffers(void *context)
{
    size_t                   max_buffers = 1000;
    qd_adaptor_buffer_list_t adaptor_buffer_list;
    DEQ_INIT(adaptor_buffer_list);
    for (size_t i = 0; i < max_buffers; i++) {
        qd_adaptor_buffer_t *adaptor_buffer = qd_adaptor_buffer();
        DEQ_INSERT_TAIL(adaptor_buffer_list, adaptor_buffer);
    }

    // The adaptor_buffer_list now contains 1000 adaptor_buffer objects.
    // qd_adaptor_buffer_list_free_buffers will free all the buffers
    // If some buffers were not freed, the alloc pool leak detector will
    // throw an error.
    qd_adaptor_buffer_list_free_buffers(&adaptor_buffer_list);
    if (DEQ_SIZE(adaptor_buffer_list) != 0) {
        return "adaptor_buffer list length is not zero";
    }

    // Pass in an empty adaptor buffer list and make sure it is not an issue
    qd_adaptor_buffer_list_t adaptor_buffer_list1;
    DEQ_INIT(adaptor_buffer_list1);
    qd_adaptor_buffer_list_free_buffers(&adaptor_buffer_list1);

    return 0;
}

static char *test_qd_adaptor_buffer_pn_raw_buffer(void *context)
{
    qd_adaptor_buffer_t *adaptor_buffer     = qd_adaptor_buffer();
    char                *hello_world        = "Hello World";
    size_t               hello_world_length = strlen(hello_world);
    memcpy(qd_adaptor_buffer_cursor(adaptor_buffer), hello_world, hello_world_length);
    qd_adaptor_buffer_insert(adaptor_buffer, hello_world_length);

    if (qd_adaptor_buffer_size(adaptor_buffer) != hello_world_length) {
        return "adaptor buffer size did not match expected size";
    }

    pn_raw_buffer_t pn_raw_buff;
    qd_adaptor_buffer_pn_raw_buffer(&pn_raw_buff, adaptor_buffer);
    if (qd_adaptor_buffer_size(adaptor_buffer) != pn_raw_buff.size) {
        return "string length did not match buffer length";
    }

    qd_adaptor_buffer_t *ad_buff = (qd_adaptor_buffer_t *) pn_raw_buff.context;
    if (ad_buff != adaptor_buffer) {
        return "Raw buffer context did not match adaptor buffer";
    }

    if (qd_adaptor_buffer_capacity(adaptor_buffer) != pn_raw_buff.capacity) {
        return "Raw buffer capacity did not match adaptor buffer";
    }

    if (pn_raw_buff.offset != 0) {
        return "Raw buffer offset is not zero";
    }

    size_t total = pn_raw_buff.capacity + pn_raw_buff.size;
    if (total != QD_ADAPTOR_MAX_BUFFER_SIZE) {
        return "pn_raw buffer size did not match QD_ADAPTOR_MAX_BUFFER_SIZE";
    }

    qd_adaptor_buffer_free(adaptor_buffer);

    return 0;
}

static char *test_qd_adaptor_buffer_list_append(void *context)
{
    const int DATA_SIZE = 40000;
    char      data_array[DATA_SIZE];
    for (int i = 0; i < DATA_SIZE; i++) {
        data_array[i] = 'a';
    }

    qd_adaptor_buffer_list_t adaptor_buffs;
    // Send in an empty adaptor_buffs
    DEQ_INIT(adaptor_buffs);
    qd_adaptor_buffer_list_append(&adaptor_buffs, (uint8_t *) data_array, DATA_SIZE);

    int left_over = DATA_SIZE % QD_ADAPTOR_MAX_BUFFER_SIZE;
    int num_buffs = DATA_SIZE / QD_ADAPTOR_MAX_BUFFER_SIZE;
    if (left_over != 0) {
        num_buffs += 1;
    }
    if (DEQ_SIZE(adaptor_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "Expected number of buffers not there in adaptor_buffs";
    }

    qd_adaptor_buffer_t *tail_buff = DEQ_TAIL(adaptor_buffs);
    if (qd_adaptor_buffer_size(tail_buff) != left_over) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "tail adaptor buffer size does not match expected size";
    }

    qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);

    if (DEQ_SIZE(adaptor_buffs) != 0) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "Expected adaptor_buffs size to be 0 but it is not";
    }

    //
    // Send in a adaptor_buffs which already has a single buffer (of size 11).
    // qd_adaptor_buffer_list_append will pack the tail buffer with data
    // before adding new buffers to the qd_adaptor buffer list.
    //
    qd_adaptor_buffer_t *adaptor_buffer     = qd_adaptor_buffer();
    char                *hello_world        = "Hello World";
    size_t               hello_world_length = strlen(hello_world);
    memcpy(qd_adaptor_buffer_cursor(adaptor_buffer), hello_world, hello_world_length);
    qd_adaptor_buffer_insert(adaptor_buffer, hello_world_length);
    DEQ_INIT(adaptor_buffs);
    DEQ_INSERT_TAIL(adaptor_buffs, adaptor_buffer);

    qd_adaptor_buffer_list_append(&adaptor_buffs, (uint8_t *) data_array, DATA_SIZE);
    int left_over_world = (DATA_SIZE + hello_world_length) % QD_ADAPTOR_MAX_BUFFER_SIZE;
    num_buffs           = (DATA_SIZE + hello_world_length) / QD_ADAPTOR_MAX_BUFFER_SIZE;
    if (left_over_world != 0) {
        num_buffs += 1;
    }

    if (DEQ_SIZE(adaptor_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "Expected number of buffers not there in adaptor_buffs";
    }

    tail_buff = DEQ_TAIL(adaptor_buffs);
    if (qd_adaptor_buffer_size(tail_buff) != left_over_world) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "tail adaptor buffer size did not match expected size";
    }
    qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);

    if (DEQ_SIZE(adaptor_buffs) != 0) {
        return "adaptor_buffer list length is not zero";
    }

    return 0;
}

static char *test_qd_adaptor_buffers_copy_to_qd_buffers(void *context)
{
    const int DATA_SIZE = 50000;

    // Create a char array of size DATA_SIZE and fill it up with data.
    char data_array[DATA_SIZE];
    for (int i = 0; i < DATA_SIZE; i++) {
        data_array[i] = 'a';
    }

    // Copy the contents of the data_array into an adaptor buffer list.
    qd_adaptor_buffer_list_t adaptor_buffs;
    DEQ_INIT(adaptor_buffs);
    qd_adaptor_buffer_list_append(&adaptor_buffs, (uint8_t *) data_array, DATA_SIZE);

    qd_buffer_list_t qd_buffs;
    DEQ_INIT(qd_buffs);

    // This functions frees the adaptor buffs in the adaptor_buffs list, no need to explicitly free them.
    qd_adaptor_buffers_copy_to_qd_buffers(&adaptor_buffs, &qd_buffs);

    int left_over = DATA_SIZE % QD_BUFFER_DEFAULT_SIZE;
    int num_buffs = DATA_SIZE / QD_BUFFER_DEFAULT_SIZE;
    if (left_over > 0) {
        num_buffs += 1;
    }

    // Check if the number of qd_buffers created by qd_adaptor_buffers_copy_to_qd_buffers match the computed
    // num_buffs.
    if (DEQ_SIZE(qd_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        qd_buffer_list_free_buffers(&qd_buffs);
        return "Number of buffers in qd_buffs list does not match the expected number of buffers";
    }

    // Create a new adaptor buffer list using the contents of data_array
    // and pass in the same list of qd_buffs that was used in the previous call
    // to qd_adaptor_buffers_copy_to_qd_buffers. The qd_buffs must have additional buffers (all fully packed) appended
    // to the end of the qd_buffer list.
    DEQ_INIT(adaptor_buffs);
    qd_adaptor_buffer_list_append(&adaptor_buffs, (uint8_t *) data_array, DATA_SIZE);

    qd_adaptor_buffers_copy_to_qd_buffers(&adaptor_buffs, &qd_buffs);

    left_over = (DATA_SIZE * 2) % QD_BUFFER_DEFAULT_SIZE;
    num_buffs = (DATA_SIZE * 2) / QD_BUFFER_DEFAULT_SIZE;
    if (left_over > 0) {
        num_buffs += 1;
    }

    // Check if the number of qd_buffers created by qd_adaptor_buffers_copy_to_qd_buffers match the computed
    // num_buffs.
    if (DEQ_SIZE(qd_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        qd_buffer_list_free_buffers(&qd_buffs);
        return "Number of buffers in qd_buffs list does not match the expected number of buffers";
    }

    if (DEQ_SIZE(adaptor_buffs) != 0) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        qd_buffer_list_free_buffers(&qd_buffs);
        return "Number of buffers in adaptor_buffs was not zero";
    }

    qd_buffer_t *tail_buff = DEQ_TAIL(qd_buffs);
    if (qd_buffer_size(tail_buff) != left_over) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        qd_buffer_list_free_buffers(&qd_buffs);
        return "tail qd_buffer size does not match expected size";
    }

    // Free the qd_buffer list before finishing the test in order to avoid leaks.
    qd_buffer_list_free_buffers(&qd_buffs);

    return 0;
}

static char *test_qd_adaptor_copy_qd_buffers_to_adaptor_buffers(void *context)
{
    const int DATA_SIZE = 50000;

    // Create a char array of size DATA_SIZE and fill it up with data.
    char data_array[DATA_SIZE];
    for (int i = 0; i < DATA_SIZE; i++) {
        data_array[i] = 'a';
    }

    // Copy the contents of the data_array into an qd_buffer list.
    qd_buffer_list_t qd_buffs;
    DEQ_INIT(qd_buffs);
    qd_buffer_list_append(&qd_buffs, (uint8_t *) data_array, DATA_SIZE);

    qd_adaptor_buffer_list_t adaptor_buffs;
    DEQ_INIT(adaptor_buffs);

    // This functions frees all the qd_buffs in the qd_buffs list, no need to explicitly free them.
    // qd_adaptor_copy_qd_buffers_to_adaptor_buffers() packs the adaptor buffers
    qd_adaptor_copy_qd_buffers_to_adaptor_buffers(&qd_buffs, &adaptor_buffs);

    int left_over = DATA_SIZE % QD_ADAPTOR_MAX_BUFFER_SIZE;
    int num_buffs = DATA_SIZE / QD_ADAPTOR_MAX_BUFFER_SIZE;
    if (left_over > 0) {
        num_buffs += 1;
    }

    if (DEQ_SIZE(adaptor_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "adaptor buffer list size did not match the number of expected buffs";
    }

    // Copy the contents of the data_array into an qd_buffer list once again.
    qd_buffer_list_append(&qd_buffs, (uint8_t *) data_array, DATA_SIZE);

    // The adaptor_buffs list already has some buffs in it. Copy the contents of the
    // qd_buffers into the adaptor_buffs and make sure the contents of the adaptor_buffs is
    // fully packed.
    qd_adaptor_copy_qd_buffers_to_adaptor_buffers(&qd_buffs, &adaptor_buffs);

    left_over = (DATA_SIZE * 2) % QD_ADAPTOR_MAX_BUFFER_SIZE;
    num_buffs = (DATA_SIZE * 2) / QD_ADAPTOR_MAX_BUFFER_SIZE;
    if (left_over > 0) {
        num_buffs += 1;
    }

    if (DEQ_SIZE(adaptor_buffs) != num_buffs) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "adaptor buffer list size did not match the number of expected buffs";
    }

    qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);

    // Send in empty qd_buffs and adaptor_buffs and make sure nothing happened.
    qd_adaptor_copy_qd_buffers_to_adaptor_buffers(&qd_buffs, &adaptor_buffs);

    if (DEQ_SIZE(adaptor_buffs) != 0) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "adaptor buffer list size is not zero";
    }
    if (DEQ_SIZE(qd_buffs) != 0) {
        qd_adaptor_buffer_list_free_buffers(&adaptor_buffs);
        return "qd buffer list size is not zero";
    }
    return 0;
}

int adaptor_buffer_tests()
{
    int   result     = 0;
    char *test_group = "adaptor_buffer_tests";

    TEST_CASE(test_qd_adaptor_buffer, 0);
    TEST_CASE(test_qd_adaptor_buffer_list_free_buffers, 0);
    TEST_CASE(test_qd_adaptor_buffer_pn_raw_buffer, 0);
    TEST_CASE(test_qd_adaptor_buffer_list_append, 0);
    TEST_CASE(test_qd_adaptor_buffers_copy_to_qd_buffers, 0);
    TEST_CASE(test_qd_adaptor_copy_qd_buffers_to_adaptor_buffers, 0);

    return result;
}

int main(int argc, char **argv)
{
    qd_alloc_initialize();
    qd_log_initialize();
    qd_error_initialize();
    int result = adaptor_buffer_tests();
    qd_log_finalize();
    qd_alloc_finalize();
    qd_router_id_finalize();

    return result;
}

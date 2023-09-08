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
 * Unit test for platform utilities
 */

#include "qpid/dispatch/platform.h"

#include "test_case.h"

#include <stdio.h>

// simple sanity check that qd_platform_memory_size() returns a meaningful value
//
static char *test_memory_size(void *context)
{
    if (qd_platform_memory_size() == 0) {
        return "ERROR: qd_platform_memory_size() cannot detect memory size";
    }
    return 0;
}


int platform_tests(void)
{
    int result = 0;
    char *test_group = "platform_tests";

    TEST_CASE(test_memory_size, 0);

    return result;
}


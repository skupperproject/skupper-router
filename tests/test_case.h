#ifndef _nexus_test_case_h_
#define _nexus_test_case_h_ 1
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

typedef char* (*testcase_t)(void *context);

#define TEST_CASE(T,C) do {                        \
    char *r = T(C);                                \
    printf("Test Case %s.%s: ", test_group, #T); \
    if (r) {                                       \
        printf("FAIL: %s\n", r);                   \
        result++;                                  \
    } else                                         \
        printf("PASS\n");                          \
} while(0);

#endif

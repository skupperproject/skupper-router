#ifndef STATIC_ASSERT_H
#define STATIC_ASSERT_H

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

#include <assert.h>

/** @file
 * STATIC_ASSERT allows you to do compile time assertions at file scope or in a function.
 * @param expr: a boolean expression that is valid at compile time.
 * @param msg: a "message" that must also be a valid identifier, i.e. message_with_underscores
 */


#define STATIC_ASSERT(expr, msg) static_assert(expr, #msg)

#define STATIC_ASSERT_ARRAY_LEN(array, len) \
    STATIC_ASSERT(sizeof(array)/sizeof(array[0]) == len, array##_wrong_size);

#endif // STATIC_ASSERT_H

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

#include "qpid/dispatch/static_assert.h"

static unsigned long ul;
static long          l;
static int           i;

STATIC_ASSERT(IS_SAME(unsigned long, ul), ul is unsigned long);
STATIC_ASSERT(!IS_SAME(unsigned long, l), l is not unsigned long);

STATIC_ASSERT(!IS_SAME(int, l), l is not int);
STATIC_ASSERT(!IS_SAME(long, i), i is not long);

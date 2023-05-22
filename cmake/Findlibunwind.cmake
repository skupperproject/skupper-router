#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Sets LIBUNWIND_LIBRARY to libunwind if it exists
# Sets LIBUNWIND_INCLUDE_DIRS to directory containing libunwind.h header

find_library(LIBUNWIND_LIBRARY unwind DOC "libunwind is used to dump the stack on crash")

find_path(LIBUNWIND_INCLUDE_DIRS libunwind.h
  HINTS "${CMAKE_INSTALL_PREFIX}/include"
  PATHS "/usr/include")

if (NOT (LIBUNWIND_INCLUDE_DIRS AND LIBUNWIND_LIBRARY))
  message(STATUS "libunwind library not found: stack dump on crash disabled")
endif()

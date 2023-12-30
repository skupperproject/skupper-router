##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##

# -fno-inline: just to be extra sure, no particular reason so far
# -fno-builtin: GCC would optimize e.g. abs() and we would not be able to stub
# -Wno-literal-suffix allows compiling string literals such as "[C%"PRIu64"]" as C++
set(CXX_WARNING_GNU "-Wno-literal-suffix")
set(CXX_WARNING_Clang "-Wno-error=reserved-user-defined-literal") # https://sourcecodeartisan.com/2021/03/06/invalid-suffix-on-literal.html
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_STANDARD_FLAGS} -fno-inline -fno-builtin -fno-stack-protector ${CXX_WARNING_${CMAKE_C_COMPILER_ID}}")

if(NOT dw_lib)
    add_compile_definitions("QD_SKIP_LIBDW_TESTS")
endif()

add_executable(cpp_unit
        ../helpers/qdr_stubbing_probe.cpp
        ../helpers/c_unittests_main.cpp
        ../doctest/doctest.h
        ../helpers/qdr_doctest.hpp
        ../helpers/helpers.cpp
        ../helpers/helpers.hpp

        test_alloc_pool.cpp
        test_amqp.cpp
        test_server.cpp
        test_terminus.cpp
)
target_link_libraries(cpp_unit cpp-stub pthread skupper-router ${bfd_lib})
# http_common.h includes "delivery.h"
target_include_directories(cpp_unit PRIVATE ${CMAKE_SOURCE_DIR}/src/router_core)

add_test(
        NAME cpp_unit
        COMMAND ${TEST_WRAP} $<TARGET_FILE:cpp_unit>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)


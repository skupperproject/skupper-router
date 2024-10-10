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
add_definitions(${C_STANDARD_FLAGS} ${COMPILE_WARNING_FLAGS})

option(FUZZ_REGRESSION_TESTS "Run fuzz tests with regression test driver" ON)
option(FUZZ_LONG_TESTS "Run fuzz tests that take a long time" OFF)
set(FUZZER AFL CACHE STRING "Fuzzing engine to use") # Set AFL as the default fuzzer
set(FUZZING_LIB_LibFuzzer FuzzingEngine)
set(FUZZING_LIB_AFL -fsanitize=fuzzer)

add_library(StandaloneFuzzTargetMain STATIC StandaloneFuzzTargetMain.c StandaloneFuzzTargetInit.c)

if (FUZZ_REGRESSION_TESTS)
  message(STATUS "FUZZ_REGRESSION_TESTS")
  set(FUZZING_LIBRARY StandaloneFuzzTargetMain)
else ()
  message(STATUS "NO FUZZ_REGRESSION_TESTS")
  set(FUZZING_LIBRARY ${FUZZING_LIB_${FUZZER}})
endif ()

macro(add_fuzz_test test)
  add_executable (${test} ${ARGN})
  target_link_libraries (${test} ${FUZZING_LIBRARY} skupper-router)
  set_target_properties(fuzz_http2_decoder PROPERTIES LINKER_LANGUAGE CXX)
  
  if(FUZZ_REGRESSION_TESTS)
    file(GLOB_RECURSE files ${CMAKE_CURRENT_SOURCE_DIR}/${test}/*)
    unset(file_lines)
    foreach(f IN LISTS files)
      set(file_lines "${file_lines}${f}\n")
    endforeach()
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${test}-files" "${file_lines}")
    add_test(${test} ${TEST_WRAP} ${test} "@${CMAKE_CURRENT_BINARY_DIR}/${test}-files")
  endif(FUZZ_REGRESSION_TESTS)
endmacro(add_fuzz_test test)

add_fuzz_test(fuzz_http2_decoder fuzz_http2_decoder.c)
#add_fuzz_test(fuzz_http1_decoder fuzz_http1_decoder.c)

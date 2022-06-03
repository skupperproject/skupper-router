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

# Configuration for code analysis tools.
#
# The RUNTIME_CHECK variable enables run-time checking when running
# the CTest test suite. The following tools are supported
#
# -DRUNTIME_CHECK=tsan      # turns on thread sanitizer
# -DRUNTIME_CHECK=asan      # address and undefined behavior sanitizer
# -DRUNTIME_CHECK=hwasan    # hardware-supported asan for aarch64
# -DRUNTIME_CHECK=memcheck  # valgrind memcheck (in progress)
# -DRUNTIME_CHECK=helgrind  # valgrind helgrind (in progress)
#
# This file updates the QDROUTERD_RUNNER and CMAKE_C_FLAGS
# appropriately for use when running the ctest suite.

# Disabling memory pool turns use-after-poison asan warnings into use-after-free warnings. The latter come with
# a freeing stacktrace that makes them easier to triage.
#
# Safe pointers require memory pool to work (memory may never be relinquished to the OS, otherwise safe pointers may
# break). Therefore, only disable memory pool for special debugging purposes. Sanitizers minimize memory reuse and
# make it significantly less likely that a safe pointer breaks.
set(QD_DISABLE_MEMORY_POOL OFF CACHE STRING "Disables memory pool. Should be only used with asan or msan RUNTIME_CHECK")
if (QD_DISABLE_MEMORY_POOL)
  add_definitions(-DQD_DISABLE_MEMORY_POOL)
endif()

# Valgrind configuration
#
find_program(VALGRIND_EXECUTABLE valgrind DOC "Location of the valgrind program")
set(VALGRIND_SUPPRESSIONS "${CMAKE_SOURCE_DIR}/tests/valgrind.supp" CACHE STRING "Suppressions file for valgrind")
set(VALGRIND_COMMON_ARGS "--error-exitcode=42 --xml=yes --xml-file=valgrind-%p.xml --quiet --suppressions=${VALGRIND_SUPPRESSIONS}")
mark_as_advanced(VALGRIND_EXECUTABLE VALGRIND_SUPPRESSIONS VALGRIND_COMMON_ARGS)
macro(assert_has_valgrind)
  if(NOT VALGRIND_EXECUTABLE)
    message(FATAL_ERROR "valgrind is not available")
  endif()
endmacro()

# Check for compiler's support of sanitizers.
# Currently have tested back to gcc 5.4.0 and clang 6.0.0, older
# versions may require more work
#
if((CMAKE_C_COMPILER_ID MATCHES "GNU"
      AND (CMAKE_C_COMPILER_VERSION VERSION_GREATER 5.4
        OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 5.4))
    OR (CMAKE_C_COMPILER_ID MATCHES "Clang"
      AND (CMAKE_C_COMPILER_VERSION VERSION_GREATER 6.0
        OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 6.0)))
  set(HAS_SANITIZERS TRUE)
endif()
macro(assert_has_sanitizers)
  if(NOT HAS_SANITIZERS)
    message(FATAL_ERROR "compiler sanitizers are not available")
  endif()
endmacro()

# Valid options for RUNTIME_CHECK
#
set(runtime_checks OFF tsan asan hwasan memcheck helgrind)

# Set RUNTIME_CHECK value and deal with the older cmake flags for
# valgrind and TSAN
#
set(RUNTIME_CHECK_DEFAULT OFF)
macro(deprecated_enable_check old new doc)
  if (${old})
    message("WARNING: option ${old} is deprecated, use -DRUNTIME_CHECK=${new} instead")
    set(RUNTIME_CHECK_DEFAULT ${new})
  endif()
  unset(${old} CACHE)
endmacro()
option(VALGRIND_XML "Write valgrind output as XML (DEPRECATED)" OFF)
deprecated_enable_check(USE_VALGRIND memcheck "Use valgrind to detect run-time problems")
deprecated_enable_check(USE_TSAN tsan "Compile with thread sanitizer (tsan)")

set(RUNTIME_CHECK ${RUNTIME_CHECK_DEFAULT} CACHE STRING "Enable runtime checks. Valid values: ${runtime_checks}")
if(CMAKE_BUILD_TYPE MATCHES "Coverage" AND RUNTIME_CHECK)
  message(FATAL_ERROR "Cannot set RUNTIME_CHECK with CMAKE_BUILD_TYPE=Coverage")
endif()
if(QD_DISABLE_MEMORY_POOL AND NOT RUNTIME_CHECK)
  message(FATAL_ERROR "Do not set QD_DISABLE_MEMORY_POOL without enabling RUNTIME_CHECK at the same time")
endif()

# set -Wp,-U_FORTIFY_SOURCE to avoid bad interaction with fortify flags, https://developers.redhat.com/blog/2021/05/05/memory-error-checking-in-c-and-c-comparing-sanitizers-and-valgrind#fortifysource
set(common_sanitizer_flags "-g -fno-omit-frame-pointer -Wp,-U_FORTIFY_SOURCE")

if(RUNTIME_CHECK STREQUAL "memcheck")
  assert_has_valgrind()
  message(STATUS "Runtime memory checker: valgrind memcheck")
  set(QDROUTERD_RUNNER "${VALGRIND_EXECUTABLE} --tool=memcheck --leak-check=full --show-leak-kinds=definite --errors-for-leak-kinds=definite ${VALGRIND_COMMON_ARGS}")

elseif(RUNTIME_CHECK STREQUAL "helgrind")
  assert_has_valgrind()
  message(STATUS "Runtime race checker: valgrind helgrind")
  set(QDROUTERD_RUNNER "${VALGRIND_EXECUTABLE} --tool=helgrind ${VALGRIND_COMMON_ARGS}")

elseif(RUNTIME_CHECK STREQUAL "asan" OR RUNTIME_CHECK STREQUAL "hwasan")
  assert_has_sanitizers()
  find_library(ASAN_LIBRARY NAME asan libasan)
  if(ASAN_LIBRARY-NOTFOUND)
    message(FATAL_ERROR "libasan not installed - address sanitizer not available")
  endif(ASAN_LIBRARY-NOTFOUND)
  find_library(UBSAN_LIBRARY NAME ubsan libubsan)
  if(UBSAN_LIBRARY-NOTFOUND)
    message(FATAL_ERROR "libubsan not installed - address sanitizer not available")
  endif(UBSAN_LIBRARY-NOTFOUND)
  if(RUNTIME_CHECK STREQUAL "asan")
    set(ASAN_VARIANTS "address,undefined")
  elseif(RUNTIME_CHECK STREQUAL "hwasan")
    set(ASAN_VARIANTS "hwaddress,undefined")
    # hwasan currently needs lld, otherwise binaries crash on invalid instruction
    #  https://github.com/google/sanitizers/issues/1241
    add_link_options("-fuse-ld=lld")
  endif()
  message(STATUS "Runtime memory checker: gcc/clang address sanitizers: ${ASAN_VARIANTS}")
  option(SANITIZE_PYTHON "Detect leaks in 3rd party libpython library used by Router while running tests" ON)
  if (SANITIZE_PYTHON)
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/tests/lsan.supp
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/tests/lsan.supp ${CMAKE_BINARY_DIR}/tests/lsan.supp
        DEPENDS ${CMAKE_SOURCE_DIR}/tests/lsan.supp
        VERBATIM)
  else (SANITIZE_PYTHON)
    # Append wholesale library suppressions
    #  this is necessary if target system has older version of Python which still exhibits leaks
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/tests/lsan.supp
        COMMAND bash -c 'cat ${CMAKE_SOURCE_DIR}/tests/lsan.supp > ${CMAKE_BINARY_DIR}/tests/lsan.supp'
        COMMAND bash -c 'echo "leak:/libpython3.*.so" >> ${CMAKE_BINARY_DIR}/tests/lsan.supp'
        DEPENDS ${CMAKE_SOURCE_DIR}/tests/lsan.supp)
  endif ()
  add_custom_target(generate_lsan.supp ALL
        DEPENDS ${CMAKE_BINARY_DIR}/tests/lsan.supp)
  # force QD_MEMORY_DEBUG else lsan will catch alloc_pool suppressed leaks (ok to remove this once leaks are fixed)
  set(SANITIZE_FLAGS "${common_sanitizer_flags} -fsanitize=${ASAN_VARIANTS} -DQD_MEMORY_DEBUG=1")
  # `detect_leaks=1` is set by default where it is available; better not to set it conditionally ourselves
  # https://github.com/openSUSE/systemd/blob/1270e56526cd5a3f485ae2aba975345c38860d37/docs/TESTING_WITH_SANITIZERS.md
  set(RUNTIME_ASAN_ENV_OPTIONS "disable_coredump=0 strict_string_checks=1 detect_stack_use_after_return=1 check_initialization_order=1 strict_init_order=1 detect_invalid_pointer_pairs=2 suppressions=${CMAKE_SOURCE_DIR}/tests/asan.supp")
  set(RUNTIME_LSAN_ENV_OPTIONS "disable_coredump=0 suppressions=${CMAKE_BINARY_DIR}/tests/lsan.supp")
  set(RUNTIME_UBSAN_ENV_OPTIONS "disable_coredump=0 print_stacktrace=1 print_summary=1")

elseif(RUNTIME_CHECK STREQUAL "tsan")
  assert_has_sanitizers()
  find_library(TSAN_LIBRARY NAME tsan libtsan)
  if(TSAN_LIBRARY-NOTFOUND)
    message(FATAL_ERROR "libtsan not installed - thread sanitizer not available")
  endif(TSAN_LIBRARY-NOTFOUND)
  message(STATUS "Runtime race checker: gcc/clang thread sanitizer")
  set(SANITIZE_FLAGS "${common_sanitizer_flags} -fsanitize=thread")
  set(RUNTIME_TSAN_ENV_OPTIONS "disable_coredump=0 history_size=4 second_deadlock_stack=1 suppressions=${CMAKE_SOURCE_DIR}/tests/tsan.supp")

elseif(RUNTIME_CHECK)
  message(FATAL_ERROR "'RUNTIME_CHECK=${RUNTIME_CHECK}' is invalid, valid values: ${runtime_checks}")
endif()

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

cmake_minimum_required(VERSION 3.20)
project(skupper-router C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON) # gnu11

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_ENABLE_EXPORTS TRUE)


# Set warning compile flags
set(C_WARNING_GNU -Wall -Wextra -Werror -Wpedantic -pedantic-errors -Wimplicit-fallthrough=3 -Wno-empty-body -Wno-unused-parameter  -Wno-missing-field-initializers -Wno-sign-compare -Wno-type-limits)
set(C_WARNING_Clang -Wall -Wextra -Werror -Wpedantic -Wno-unused-parameter -Wno-missing-field-initializers -Wno-sign-compare -Wno-language-extension-token)

set(C_WARNING_FLAGS ${C_WARNING_${CMAKE_C_COMPILER_ID}})
add_compile_options(${C_WARNING_FLAGS})

# Set default build type. Must use FORCE because project() sets default to ""
if (NOT CMAKE_BUILD_TYPE)
  set (CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
    "Build type: Debug, Release, RelWithDebInfo, MinSizeRel or Coverage (default RelWithDebInfo)" FORCE)
endif(NOT CMAKE_BUILD_TYPE)
if (CMAKE_BUILD_TYPE MATCHES "Deb|Cover")
  set (has_debug_symbols " (has debug symbols)")
endif (CMAKE_BUILD_TYPE MATCHES "Deb|Cover")
message(STATUS "Build type is \"${CMAKE_BUILD_TYPE}\"${has_debug_symbols}")
# This is commonly needed so define it before we include anything else.
string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)

if(uppercase_CMAKE_BUILD_TYPE STREQUAL "DEBUG" )
    option(QD_ENABLE_ASSERTIONS "Enable assertions" ON)
else()
    option(QD_ENABLE_ASSERTIONS "Enable assertions" OFF)
endif()

if(NOT DEFINED VERSION)
   # Version was not provided. First check to see if git is available
   find_package(Git)
   if(Git_FOUND)
      execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --dirty=-modified --always
                      OUTPUT_VARIABLE DEFAULT_VERSION
                      RESULT_VARIABLE GIT_RESULT
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
                      
                
      # You might sometimes get a fatal error when running the above command (when this is not a git repo).
      # GIT_RESULT will contain the result of last child process. It will be zero if successful.
      if (GIT_RESULT EQUAL 0)
          # Git succeeded, we will use the DEFAULT_VERSION as the QPID_DISPATCH_VERSION
          set(QPID_DISPATCH_VERSION ${DEFAULT_VERSION})
       else()
          # The git command failed, set QPID_DISPATCH_VERSION to "UNKNOWN" 
          set(QPID_DISPATCH_VERSION "UNKNOWN")
       endif(GIT_RESULT EQUAL 0)
   else(Git_FOUND)
       # Git executable was not available, we will not be able to determine the version, just set it to "UNKNOWN"
       set(QPID_DISPATCH_VERSION "UNKNOWN")
   endif(Git_FOUND)
   
else(NOT DEFINED VERSION)

   # What if VERSION is defined but someone passed in an empty value for VERSION? Deal with that case here.
   if (VERSION STREQUAL "")
      set(QPID_DISPATCH_VERSION "UNKNOWN")
   else()
      string(STRIP ${VERSION} VERSION)
      if (VERSION STREQUAL "")
          set(QPID_DISPATCH_VERSION "UNKNOWN")
      else()
          set(QPID_DISPATCH_VERSION ${VERSION})
      endif()
   endif(VERSION STREQUAL "")  
endif(NOT DEFINED VERSION)

message(STATUS "Setting skupper-router version to ${QPID_DISPATCH_VERSION}")

include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckFunctionExists)
include(CheckIncludeFiles)
include(CMakeDependentOption)

##
## Find dependencies
##
find_library(rt_lib rt)

find_package(Python 3.6.8 REQUIRED COMPONENTS Interpreter Development)
find_package(Threads REQUIRED)
find_package(Proton 0.37.0 REQUIRED COMPONENTS Core Proactor Tls)
message(STATUS "Found Proton: ${Proton_LIBRARIES} (found version \"${Proton_VERSION}\")" )
##
## Optional dependencies
##

# google benchmark tests are disabled by default
OPTION(BUILD_BENCHMARKS "Enable building and running benchmarks with Google Benchmark" OFF)

# http2 support is optional
find_package(libnghttp2 1.33.0)
CMAKE_DEPENDENT_OPTION(USE_LIBNGHTTP2 "Use libnghttp2 for HTTP/2 support" ON
                         "libnghttp2_FOUND" OFF)

# Web Sockets
find_package(LibWebSockets 3.0.1)
CMAKE_DEPENDENT_OPTION(USE_LIBWEBSOCKETS "Use libwebsockets for WebSocket support" ON
                         "LIBWEBSOCKETS_FOUND" OFF)

if (NOT DEFINED DISPATCH_TEST_TIMEOUT)
    set(DISPATCH_TEST_TIMEOUT "600")
endif (NOT DEFINED DISPATCH_TEST_TIMEOUT)
SET(DART_TESTING_TIMEOUT ${DISPATCH_TEST_TIMEOUT} CACHE STRING "Default CTest timeout in seconds")
include (CTest)

set(PYTHON_TEST_COMMAND "-m" "unittest" "-v" "\${py_test_module}"
    CACHE STRING "Command used to run tests implemented in Python unittest; \${py_test_module} will be replaced with test module name.")

set (SO_VERSION_MAJOR 2)
set (SO_VERSION_MINOR 0)
set (SO_VERSION "${SO_VERSION_MAJOR}.${SO_VERSION_MINOR}")

if (NOT DEFINED LIB_SUFFIX)
    get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
    if ("${LIB64}" STREQUAL "TRUE" AND ${CMAKE_SIZEOF_VOID_P} STREQUAL "8")
        set(LIB_SUFFIX 64)
    else()
        set(LIB_SUFFIX "")
    endif()
endif()

set(INCLUDE_INSTALL_DIR include CACHE PATH "Include file directory")
set(QPID_DISPATCH_HOME "lib/skupper-router" CACHE PATH "Private Skupper library directory")
set(LIB_INSTALL_DIR "lib${LIB_SUFFIX}" CACHE PATH "Library object file directory")
set(SHARE_INSTALL_DIR share CACHE PATH "Shared read only data directory")
set(DOC_INSTALL_DIR ${SHARE_INSTALL_DIR}/doc CACHE PATH "Documentation directory")
set(QD_DOC_INSTALL_DIR ${SHARE_INSTALL_DIR}/doc/skupper-router CACHE PATH "Skupper Router documentation directory")
set(MAN_INSTALL_DIR share/man CACHE PATH "Manpage directory")
set(QPID_DISPATCH_HOME_INSTALLED ${CMAKE_INSTALL_PREFIX}/${QPID_DISPATCH_HOME})
set(QPID_DISPATCH_HTML_DIR ${CMAKE_INSTALL_PREFIX}/share/skupper-router/html CACHE PATH "Skupper Router HTML directory")

set(RUN ${Python_EXECUTABLE} ${CMAKE_BINARY_DIR}/run.py)

# define the configuration directory based on whether or not the install prefix is defined
if(NOT DEFINED SYSCONF_INSTALL_DIR)
    if(CMAKE_INSTALL_PREFIX STREQUAL "/usr")
        set(SYSCONF_INSTALL_DIR "/etc")
    else()
        set(SYSCONF_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/etc")
    endif()
endif()

set(QPID_DISPATCH_CONFDIR ${SYSCONF_INSTALL_DIR}/skupper-router)

# Set up runtime checks (valgrind, sanitizers etc.)
include(cmake/RuntimeChecks.cmake)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SANITIZE_FLAGS}")

##
## Include directories used by all sub-directories.
##
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${Proton_Proactor_INCLUDE_DIRS}
    ${Proton_Core_INCLUDE_DIRS}
    ${Python_INCLUDE_DIRS}
    )

# Originally from the LLVM project, https://opensource.apple.com/source/llvmCore/llvmCore-2358.3/CMakeLists.txt.auto.html
if(QD_ENABLE_ASSERTIONS)
    # MSVC doesn't like _DEBUG on release builds.
    if(NOT MSVC)
        add_definitions(-D_DEBUG)
    endif()
    # On non-Debug builds cmake automatically defines NDEBUG, so we explicitly undefine it:
    if(NOT uppercase_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
        # NOTE: use `add_compile_options` rather than `add_definitions` since
        # `add_definitions` does not support generator expressions.
        add_compile_options($<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:-UNDEBUG>)


        # Also remove /D NDEBUG to avoid MSVC warnings about conflicting defines.
        foreach (flags_var_to_scrub
                CMAKE_CXX_FLAGS_RELEASE
                CMAKE_CXX_FLAGS_RELWITHDEBINFO
                CMAKE_CXX_FLAGS_MINSIZEREL
                CMAKE_C_FLAGS_RELEASE
                CMAKE_C_FLAGS_RELWITHDEBINFO
                CMAKE_C_FLAGS_MINSIZEREL)
            string (REGEX REPLACE "(^| )[/-]D *NDEBUG($| )" " "
                    "${flags_var_to_scrub}" "${${flags_var_to_scrub}}")
        endforeach()
    endif()
endif()

# Set up extra coverage analysis options for gcc and clang
if (CMAKE_BUILD_TYPE MATCHES "Coverage")
 set (CMAKE_C_FLAGS_COVERAGE "-g -O0 --coverage")
 set (CMAKE_EXE_LINKER_FLAGS_COVERAGE "--coverage")
 set (CMAKE_MODULE_LINKER_FLAGS_COVERAGE "--coverage")
 set (CMAKE_SHARED_LINKER_FLAGS_COVERAGE "--coverage")
 mark_as_advanced(
   CMAKE_C_FLAGS_COVERAGE
   CMAKE_EXE_LINKER_FLAGS_COVERAGE
   CMAKE_MODULE_LINKER_FLAGS_COVERAGE
   CMAKE_SHARED_LINKER_FLAGS_COVERAGE)
 make_directory(${CMAKE_BINARY_DIR}/coverage_results)
 add_custom_target(coverage
   WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/coverage_results
   COMMAND ${CMAKE_SOURCE_DIR}/bin/record-coverage.sh ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
endif(CMAKE_BUILD_TYPE MATCHES "Coverage")

##
## configuration/html file installation
##
install(FILES etc/skrouterd.conf DESTINATION ${SYSCONF_INSTALL_DIR}/skupper-router)
install(FILES etc/sasl2/skrouterd.conf DESTINATION ${SYSCONF_INSTALL_DIR}/sasl2)
install(FILES share/index.html DESTINATION ${QPID_DISPATCH_HTML_DIR})


# Tools
install(PROGRAMS
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/skstat
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/skmanage
    DESTINATION bin)

# Doc files
install(FILES
  LICENSE
  README.adoc
  DESTINATION ${QD_DOC_INSTALL_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run.py.in ${CMAKE_CURRENT_BINARY_DIR}/run.py)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run.py.in ${CMAKE_CURRENT_BINARY_DIR}/tests/run.py)
execute_process(COMMAND ${RUN} --sh OUTPUT_FILE config.sh)

add_subdirectory(src) # Build src first so other subdirs can use library

if (BUILD_TESTING)
  file(GLOB SCRAPER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tools/scraper/*.py)
  file(COPY ${SCRAPER_SRC} DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/tests/scraper/)
  add_subdirectory(tests)
endif(BUILD_TESTING)
add_subdirectory(python)
add_subdirectory(router)
add_subdirectory(docs)

# reconfigure.in is a workaround to force cmake re-configuration. For example,
# we use GLOB to collect .h files for install and apidoc, so if you _remove_ a
# .h file it won't trigger automatic re-configure and everybody's builds will
# fail till they run cmake manually.
#
# If you do check in such a change, increase the number in this file by 1.
# That will force automatic re-configure and everybody will be happy.
#
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/reconfigure.in ${CMAKE_CURRENT_BINARY_DIR}/reconfigure)

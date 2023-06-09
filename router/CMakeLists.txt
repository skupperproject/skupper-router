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


set(DEFAULT_CONFIG_PATH "${QPID_DISPATCH_CONFDIR}/skrouterd.conf" CACHE STRING "Default config file path")
set(DEFAULT_DISPATCH_PYTHON_DIR ${QPID_DISPATCH_PYTHON_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

##
## Build the router application
##
set(router_SOURCES
    src/main.c
    src/panic.c
    )

add_executable(skrouterd ${router_SOURCES})
target_link_libraries(skrouterd skupper-router)
target_link_options(skrouterd PUBLIC LINKER:-Map=skrouterd.map)

if (ENABLE_PROFILE_GUIDED_OPTIMIZATION)
    if("GNU" STREQUAL ${CMAKE_C_COMPILER_ID})
        add_custom_command(TARGET skrouterd
            POST_BUILD
            # Run the tests with the profiled skrouterd binary
            # Running the tests produce the files with the .gcda extension in the build/profile-dir folder
            # For now, we are only running the adaptor based tests to keep it quick.
            # Even if one of the adaptor tests fail, we will go ahead and create the final executable.
            COMMAND QPID_SYSTEM_TEST_TIMEOUT=300 ${CMAKE_CTEST_COMMAND} -j4 -V -R tcp || (exit 0)
            COMMAND QPID_SYSTEM_TEST_TIMEOUT=300 ${CMAKE_CTEST_COMMAND} -j4 -V -R http || (exit 0)
            # Set the ENABLE_PROFILE_GUIDED_OPTIMIZATION=OFF so that we don't once again create profile data.
            COMMAND ${CMAKE_COMMAND} ${CMAKE_BINARY_DIR} -DENABLE_PROFILE_GUIDED_OPTIMIZATION=OFF -DCMAKE_EXE_LINKER_FLAGS=-fprofile-use=${CMAKE_BINARY_DIR}/profile-data

            # Feed the profile data we created from the above test runs using the -fprofile-use compiler option.
            # Use the -fprofile-correction option to smoothen the counter updates when using multi-threaded applications.
            COMMAND ${CMAKE_COMMAND} ${CMAKE_BINARY_DIR} -DC_EXTRA_GNU=-fprofile-use=${CMAKE_BINARY_DIR}/profile-data\\\;-fprofile-correction\\\;-fprofile-partial-training
            COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target skrouterd

            # Set back the ENABLE_PROFILE_GUIDED_OPTIMIZATION=ON, so can leave it same as when we started the build.
            COMMAND ${CMAKE_COMMAND} ${CMAKE_BINARY_DIR} -DENABLE_PROFILE_GUIDED_OPTIMIZATION=ON

            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        )
    endif("GNU" STREQUAL ${CMAKE_C_COMPILER_ID})
endif (ENABLE_PROFILE_GUIDED_OPTIMIZATION)

install(TARGETS skrouterd RUNTIME DESTINATION sbin)

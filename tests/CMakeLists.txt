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

include_directories(
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_BINARY_DIR}/src
  )

##
## Build test applications
##
set(unit_test_SOURCES
    compose_test.c
    policy_test.c
    run_unit_tests.c
    tool_test.c
    failoverlist_test.c
    http2_decoder_tests.c
    timer_test.c
    core_timer_test.c
    parse_tree_tests.c
    proton_utils_tests.c
    alloc_test.c
    hash_test.c
    thread_test.c
    platform_test.c
    static_assert_test.c
    )

add_executable(unit_tests ${unit_test_SOURCES})
target_link_libraries(unit_tests skupper-router)

set(unit_test_size_SOURCES
    field_test.c
    parse_test.c
    message_test.c
    buffer_test.c
    run_unit_tests_size.c
    )

add_executable(unit_tests_size ${unit_test_size_SOURCES})
target_link_libraries(unit_tests_size skupper-router)

add_executable(test-sender test-sender.c)
target_link_libraries(test-sender Proton::qpid-proton)

add_executable(test-receiver test-receiver.c)
target_link_libraries(test-receiver Proton::qpid-proton)

add_executable(clogger clogger.c)
target_link_libraries(clogger Proton::qpid-proton)

add_executable(threaded_timer_test threaded_timer_test.c)
target_link_libraries(threaded_timer_test skupper-router)

add_executable(http1-relay http1_relay.c)
target_link_libraries(http1-relay skupper-router)

set(TEST_WRAP ${Python_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/run.py)

add_test(unit_tests_size_10000 ${TEST_WRAP} unit_tests_size 10000)
add_test(unit_tests_size_512   ${TEST_WRAP} unit_tests_size 512)
add_test(unit_tests_size_10    ${TEST_WRAP} unit_tests_size 10)
add_test(unit_tests_size_7     ${TEST_WRAP} unit_tests_size 7)
add_test(unit_tests_size_5     ${TEST_WRAP} unit_tests_size 5)
add_test(unit_tests_size_3     ${TEST_WRAP} unit_tests_size 3)
add_test(unit_tests_size_2     ${TEST_WRAP} unit_tests_size 2)
add_test(unit_tests_size_1     ${TEST_WRAP} unit_tests_size 1)
add_test(unit_tests            ${TEST_WRAP} unit_tests ${CMAKE_CURRENT_SOURCE_DIR}/threads4.conf)

# stand alone tests
add_test(threaded_timer_test   ${TEST_WRAP} threaded_timer_test ${CMAKE_CURRENT_SOURCE_DIR}/dummy.conf)

# Unit test python modules
foreach(py_test_module
    router_engine_test
    management_test
    router_policy_test
    test_command
    )

  string(CONFIGURE "${PYTHON_TEST_COMMAND}" CONFIGURED_PYTHON_TEST_COMMAND)

  add_test(${py_test_module} ${TEST_WRAP} ${CONFIGURED_PYTHON_TEST_COMMAND})
endforeach()

set(SYSTEM_TESTS_HTTP system_tests_http system_tests_websockets)


if(DEFINED ENV{SKUPPER_SYSTEM_TEST_SKIP_POLICY})
  #nothing to do
else()
  set(SYSTEM_TESTS_POLICY system_tests_policy system_tests_policy_oversize_basic system_tests_policy_oversize_compound)
endif()

if(DEFINED ENV{SKUPPER_SYSTEM_TEST_SKIP_DELIVERY_ABORT})
  #nothing to do
else()
  set(SYSTEM_TESTS_DELIVERY_ABORT system_tests_delivery_abort)
endif()

# System test python modules
foreach(py_test_module
#   system_tests_broker
    system_tests_autolinks
    system_tests_drain
    system_tests_management
    system_tests_one_router
    system_tests_handle_failover
    system_tests_default_distribution
    ${SYSTEM_TESTS_POLICY}
    system_tests_socket_address_family
    system_tests_protocol_settings
    system_tests_skmanage
    system_tests_skstat
    system_tests_sasl_plain
    system_tests_user_id
    system_tests_user_id_proxy
    system_tests_two_routers
    system_tests_interior_sync_up
    system_tests_distribution
    system_tests_dynamic_terminus
    system_tests_log_message_components
    system_tests_failover_list
    ${SYSTEM_TESTS_DELIVERY_ABORT}
    system_tests_topology
    system_tests_topology_disposition
    system_tests_topology_addition
    system_tests_delivery_counts
    system_tests_cmdline_parsing
    system_tests_bad_configuration
    system_tests_ssl
    system_tests_edge_router
    system_tests_edge_router1
#    system_tests_edge_mesh
    system_tests_connector_status
    system_tests_core_endpoint
    ${SYSTEM_TESTS_HTTP}
    system_tests_priority
    system_tests_core_client
    system_tests_multicast
    system_tests_router_mesh
    system_tests_stuck_deliveries
    system_tests_streaming_deliveries
    system_tests_log_level_update
    system_tests_routing_protocol
    system_tests_open_properties
    system_tests_grpc
    system_tests_http1_over_tcp
    system_tests_http_observer
    system_tests_tcp_adaptor
    system_tests_tcp_adaptor_tls
    system_tests_tcp_conns_terminate
    system_tests_address_watch
    system_tests_router_annotations
    system_tests_vflow
    system_tests_expandvars
    system_tests_panic_handler
    system_tests_resend_released
    system_tests_http1_decoder
    )

  string(CONFIGURE "${PYTHON_TEST_COMMAND}" CONFIGURED_PYTHON_TEST_COMMAND)

  add_test(${py_test_module} ${TEST_WRAP} ${CONFIGURED_PYTHON_TEST_COMMAND})
  set_tests_properties(${py_test_module} PROPERTIES TIMEOUT 1200)
  list(APPEND SYSTEM_TEST_FILES ${CMAKE_CURRENT_SOURCE_DIR}/${py_test_module}.py)
endforeach()

# Place .coveragerc where coverage.py will find it when running system_tests
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/.coveragerc ${CMAKE_CURRENT_BINARY_DIR}/.coveragerc COPYONLY)

# Location of setup.cfg determines the testroot directory for Pytest
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/setup.cfg.in ${CMAKE_CURRENT_BINARY_DIR}/setup.cfg)

#This python-checker shell script uses three tools - mypy, flake8 and pylint to validate python code
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/python-checker.sh.in ${CMAKE_CURRENT_BINARY_DIR}/python-checker.sh)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pylintrc ${CMAKE_CURRENT_BINARY_DIR}/pylintrc COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pytest.ini ${CMAKE_CURRENT_BINARY_DIR}/pytest.ini COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/mypy.ini.in ${CMAKE_CURRENT_BINARY_DIR}/mypy.ini)

find_program (BASH_PROGRAM bash)
add_test (NAME python-checker 
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
          COMMAND ${BASH_PROGRAM} ${CMAKE_CURRENT_BINARY_DIR}/python-checker.sh)
          
# Additional files to install for running system tests.
# NOTE: Don't install run.py. A system test of a dispatch installation should pick everything
# up from standard install locations.
#
list(APPEND SYSTEM_TEST_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/run_system_tests.py
  ${CMAKE_CURRENT_SOURCE_DIR}/system_test.py)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config-2/A-ssl.conf.in ${CMAKE_CURRENT_BINARY_DIR}/config-2/A-ssl.conf)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config-2/B-ssl.conf.in ${CMAKE_CURRENT_BINARY_DIR}/config-2/B-ssl.conf)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/policy-1/test-policy-conf-includes-folder.conf.in ${CMAKE_CURRENT_BINARY_DIR}/policy-1/test-policy-conf-includes-folder.conf)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/policy-2/policy-photoserver-sasl.conf.in ${CMAKE_CURRENT_BINARY_DIR}/policy-2/policy-photoserver-sasl.conf)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/policy-2/test-router-with-policy.json.in ${CMAKE_CURRENT_BINARY_DIR}/policy-2/test-router-with-policy.json)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/ssl_certs DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/displayname_files DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/policy-1/management-access.json  DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/policy-1/)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/policy-2/policy-photoserver-sasl.sasldb  DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/policy-2)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/policy-3/test-sender-receiver-limits.json DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/policy-3)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/policy-4/management-access.json  DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/policy-4/)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/TCP_echo_server.py  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/TCP_echo_client.py  DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# following install() functions will be called only if you do a make "install"
install(FILES ${SYSTEM_TEST_FILES}
        DESTINATION ${QPID_DISPATCH_HOME_INSTALLED}/tests
        )

install(DIRECTORY config-1 config-2
        DESTINATION ${QPID_DISPATCH_HOME_INSTALLED}/tests
        PATTERN *.in EXCLUDE
        )

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/config-2
        DESTINATION ${QPID_DISPATCH_HOME_INSTALLED}/tests
        FILES_MATCHING PATTERN *.conf
        )

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ssl_certs
        DESTINATION ${QPID_DISPATCH_HOME_INSTALLED}/tests)

add_subdirectory(cpp)

if(BUILD_BENCHMARKS)
  add_subdirectory(c_benchmarks)
endif()

if (ENABLE_FUZZ_TESTING)
  add_subdirectory(fuzz)
endif (ENABLE_FUZZ_TESTING)

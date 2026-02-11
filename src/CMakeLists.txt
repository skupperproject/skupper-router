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

# Generate code from the management schema.
set(GENERATED_SOURCES
  schema_enum.h
  schema_enum.c
  )

set(GENERATOR_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/schema_c.py
  ${CMAKE_SOURCE_DIR}/python/skupper_router/management/skrouter.json)
file (GLOB GENERATOR_SRC ${CMAKE_SOURCE_DIR}/python/skupper_router_internal/management/*.py)
set(GENERATOR_DEPENDS ${GENERATOR_DEPENDS} ${GENERATOR_SRC})
file (GLOB GENERATOR_SRC ${CMAKE_SOURCE_DIR}/python/skupper_router/management/*.py)
set(GENERATOR_DEPENDS ${GENERATOR_DEPENDS} ${GENERATOR_SRC})

add_custom_command (
  OUTPUT ${GENERATED_SOURCES}
  COMMAND ${Python_EXECUTABLE} ${CMAKE_BINARY_DIR}/tests/run.py -s ${CMAKE_CURRENT_SOURCE_DIR}/schema_c.py
  DEPENDS ${GENERATOR_DEPENDS})

# Build the skupper-router library.
set(qpid_dispatch_SOURCES
  adaptors/adaptor_common.c
  adaptors/tcp/tcp_adaptor.c
  adaptors/test_adaptor.c
  adaptors/adaptor_listener.c
  adaptors/amqp/amqp_adaptor.c
  adaptors/amqp/container.c
  adaptors/amqp/connection_manager.c
  adaptors/amqp/qd_listener.c
  adaptors/amqp/qd_connection.c
  adaptors/amqp/qd_connector.c
  adaptors/amqp/server_config.c
  adaptors/amqp/proxy.c
  observers/protocol_observer.c
  observers/tcp_observer.c
  observers/http1/http1_observer.c
  observers/http2/http2_observer.c
  decoders/http1/http1_decoder.c
  decoders/http2/http2_decoder.c
  alloc.c
  alloc_pool.c
  aprintf.c
  amqp.c
  atomic.c
  bitmask.c
  buffer.c
  enum.c
  error.c
  compose.c
  ctools.c
  cutthrough_utils.c
  delivery_state.c
  discriminator.c
  dispatch.c
  entity.c
  entity_cache.c
  failoverlist.c
  hash.c
  http-libwebsockets.c
  iterator.c
  log.c
  message.c
  parse.c
  parse_tree.c
  policy.c
  policy_spec.c
  protocol_adaptor.c
  proton_utils.c
  posix/threading.c
  python_embedded.c
  router_agent.c
  router_config.c
  platform.c
  vanflow.c
  router.c
  router_core.c
  router_core/address_watch.c
  router_core/agent.c
  router_core/agent_address.c
  router_core/agent_config_address.c
  router_core/agent_config_auto_link.c
  router_core/agent_connection.c
  router_core/agent_link.c
  router_core/agent_router_metrics.c
  router_core/connections.c
  router_core/core_attach_address_lookup.c
  router_core/core_events.c
  router_core/core_link_endpoint.c
  router_core/core_client_api.c
  router_core/delivery.c
  router_core/error.c
  router_core/forwarder.c
  router_core/route_control.c
  router_core/router_core.c
  router_core/router_core_thread.c
  router_core/route_tables.c
  router_core/management_agent.c
  router_core/terminus.c
  router_core/transfer.c
  router_core/core_timer.c
  router_core/module.c
  router_core/modules/edge_router/module.c
  router_core/modules/edge_router/addr_proxy.c
  router_core/modules/edge_router/connection_manager.c
  router_core/modules/test_hooks/core_test_hooks.c
  router_core/modules/edge_addr_tracking/edge_addr_tracking.c
  router_core/modules/mesh_discovery/mesh_discovery_edge.c
  router_core/modules/mesh_discovery/mesh_discovery_interior.c
  router_core/modules/address_lookup_client/address_lookup_client.c
  router_core/modules/stuck_delivery_detection/delivery_tracker.c
  router_core/modules/mobile_sync/mobile.c
  router_core/modules/streaming_link_scrubber/streaming_link_scrubber.c
  tls/tls.c
  tls/tls_raw.c
  tls/tls_amqp.c
  tls/display_name.c
  router_pynode.c
  schema_enum.c
  static_assert.c
  server.c
  timer.c
  trace_mask.c
  python_utils.c
  qd_asan_interface.c
  protocols.c
  connection_counters.c
  )

set(qpid_dispatch_INCLUDES
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/router_core
  )

set(qpid_dispatch_LIBRARIES
  ${CMAKE_DL_LIBS}
  ${CMAKE_THREAD_LIBS_INIT}
  Python::Python
  PkgConfig::libwebsockets
  PkgConfig::libnghttp2
  Proton::core
  Proton::proactor
  Proton::tls
  OpenSSL::SSL
  ${rt_lib}
  )

if (dw_lib)
  set(qpid_dispatch_SOURCES ${qpid_dispatch_SOURCES} posix/symbolization.c)
  set(qpid_dispatch_LIBRARIES ${qpid_dispatch_LIBRARIES} ${dw_lib})
else()
  set(qpid_dispatch_SOURCES ${qpid_dispatch_SOURCES} posix/symbolization_none.c)
endif()

if ((DEFINED ASAN_LIBRARY) OR (DEFINED UBSAN_LIBRARY) OR (DEFINED TSAN_LIBRARY))
  set(USING_SANITIZERS TRUE)
endif()

add_library(skupper-router OBJECT ${qpid_dispatch_SOURCES})
# PUBLIC linking makes the includes and libraries available to tests
target_include_directories(skupper-router PUBLIC ${qpid_dispatch_INCLUDES})

if (libunwind_FOUND)
  set(qpid_dispatch_LIBRARIES ${qpid_dispatch_LIBRARIES} PkgConfig::libunwind)
  target_compile_definitions(skupper-router PUBLIC "HAVE_LIBUNWIND")
endif()

target_link_libraries(skupper-router PUBLIC ${qpid_dispatch_LIBRARIES})

# check for various function availability
check_symbol_exists(getrlimit sys/resource.h QD_HAVE_GETRLIMIT)
check_symbol_exists(getrandom sys/random.h QD_HAVE_GETRANDOM)

# https://stackoverflow.com/questions/54771452/expanding-a-variable-cmakedefine-and-generator-expression-in-template-file
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/config.h.in" CONFIG_H_IN)
string(CONFIGURE "${CONFIG_H_IN}" CONFIG_H_TMP)
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/config.h" CONTENT "${CONFIG_H_TMP}")

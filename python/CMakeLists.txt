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

configure_file(${CMAKE_SOURCE_DIR}/python/skupper_router/_skupper_router_site.py.in
               ${CMAKE_CURRENT_BINARY_DIR}/_skupper_router_site.py)

add_custom_target(NAME ALL
                  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/python/skupper_router ${CMAKE_CURRENT_BINARY_DIR}/skupper_router
                  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/_skupper_router_site.py ${CMAKE_CURRENT_BINARY_DIR}/skupper_router)

install(DIRECTORY skupper_router_internal DESTINATION ${QPID_DISPATCH_HOME}/python)
install(DIRECTORY skupper_router DESTINATION ${QPID_DISPATCH_HOME}/python)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/_skupper_router_site.py
        DESTINATION ${QPID_DISPATCH_HOME}/python/skupper_router/)

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/skupper_router/management/skrouter.json
  ${CMAKE_CURRENT_SOURCE_DIR}/skupper_router/management/skrouter.json.readme.txt
  DESTINATION ${QD_DOC_INSTALL_DIR}
  )

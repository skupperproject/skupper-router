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

# Private python modules, not in standard python site dir.
install(DIRECTORY skupper_router_internal DESTINATION ${QPID_DISPATCH_HOME}/python)

# Copy the public python modules into a build/python/src folder.
# We do this because, pip install, by default, will look into a src folder
# and automatically figure out python packaging and installs the files
# in the appropriate location.

add_custom_target(NAME ALL
                  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/python/skupper_router ${CMAKE_CURRENT_BINARY_DIR}/src/skupper_router
                  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/_skupper_router_site.py ${CMAKE_CURRENT_BINARY_DIR}/src/skupper_router)

configure_file(${CMAKE_SOURCE_DIR}/python/skupper_router/_skupper_router_site.py.in
               ${CMAKE_CURRENT_BINARY_DIR}/_skupper_router_site.py)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pyproject.toml.in
               ${CMAKE_CURRENT_BINARY_DIR}/pyproject.toml)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/packages/ DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/packages)

# Use pip install to install the public python modules.
# --compile option compiles Python source files to bytecode
install(CODE
        "execute_process( \
        COMMAND ${Python_EXECUTABLE} -m pip install ${CMAKE_CURRENT_BINARY_DIR} --no-deps --no-index --disable-pip-version-check --find-links ${CMAKE_CURRENT_BINARY_DIR}/packages --verbose --compile --prefix=\$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX} \
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} \
        COMMAND_ECHO STDOUT \
        COMMAND_ERROR_IS_FATAL ANY)")
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/skupper_router/management/skrouter.json
  ${CMAKE_CURRENT_SOURCE_DIR}/skupper_router/management/skrouter.json.readme.txt
  DESTINATION ${QD_DOC_INSTALL_DIR}
  )

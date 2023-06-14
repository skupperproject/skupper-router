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

# Find libwebsockets include dirs and libraries.
#
# Sets the following variables:
#
#   libwebsockets_FOUND            - True if headers and requested libraries were found
#   libwebsockets_VERSION          - The library version number

include(FindPackageHandleStandardArgs)
find_package(PkgConfig REQUIRED)

pkg_check_modules(libwebsockets REQUIRED IMPORTED_TARGET libwebsockets)

# strip trailing version elaboration, e.g. #define LWS_LIBRARY_VERSION "4.1.6-git..."
if(DEFINED libwebsockets_VERSION)
  string(REGEX REPLACE
      "^([0-9.]+).*$"
      "\\1"
      libwebsockets_VERSION
      "${libwebsockets_VERSION}")
endif()

find_package_handle_standard_args(libwebsockets
    REQUIRED_VARS libwebsockets_FOUND
    VERSION_VAR libwebsockets_VERSION
    HANDLE_COMPONENTS)

set_package_properties(libwebsockets PROPERTIES
    TYPE REQUIRED
    PURPOSE "Enables the /healthz endpoint and AMQP over websocket connections"
    DESCRIPTION "flexible, lightweight pure C library for implementing modern network protocols"
    URL "https://libwebsockets.org")

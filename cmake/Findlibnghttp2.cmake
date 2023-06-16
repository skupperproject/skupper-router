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

include(FindPackageHandleStandardArgs)
find_package(PkgConfig REQUIRED)

pkg_check_modules(libnghttp2 REQUIRED IMPORTED_TARGET libnghttp2)

find_package_handle_standard_args(libnghttp2
    REQUIRED_VARS libnghttp2_FOUND
    VERSION_VAR libnghttp2_VERSION
    HANDLE_COMPONENTS)

set_package_properties(libnghttp2 PROPERTIES
    TYPE REQUIRED
    PURPOSE "Enables the HTTP/2 router adaptor"
    DESCRIPTION "HTTP/2 C Library and tools"
    URL "https://nghttp2.org")

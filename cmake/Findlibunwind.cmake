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

pkg_check_modules(libunwind IMPORTED_TARGET libunwind)

find_package_handle_standard_args(libunwind
    REQUIRED_VARS libunwind_FOUND
    VERSION_VAR libunwind_VERSION
    HANDLE_COMPONENTS)

set_package_properties(libunwind PROPERTIES
    TYPE RECOMMENDED
    PURPOSE "Used to dump the stack on crash"
    DESCRIPTION "defines a portable and efficient C programming interface (API) to determine the call-chain of a program"
    URL "https://www.nongnu.org/libunwind")

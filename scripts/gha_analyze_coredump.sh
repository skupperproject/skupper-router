#!/usr/bin/env bash
set -Exeuo pipefail

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

# args
corefile=$(realpath "${1}")

# EXECFN seems to be most reliable way to find excutable, other ways may give truncated path
executable=$(file "${corefile}" | python3 -c "import re; print( re.search(\"execfn: '([^']+)'\", input())[1] )")

# new gdb can automatically download debuginfo as needed
#  see https://www.redhat.com/en/blog/how-debuginfod-project-evolved-2021
export DEBUGINFOD_URLS=https://debuginfod.elfutils.org

# despite debuginfod, it still makes sense to install python3-debuginfo
#  because it provides py-bt command, see https://fedoraproject.org/wiki/Features/EasierPythonDebugging

# executable path stored in corefile might be relative; change directory to handle that
pushd "$(dirname "${corefile}")"
gdb "${executable}" \
  --core "${corefile}" \
  -iex 'set pagination off' -iex 'set debuginfod enabled on' \
  -ex 'thread apply all bt' -ex 'thread apply all py-bt' \
  --batch
popd

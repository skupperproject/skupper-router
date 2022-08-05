#!/usr/bin/env bash
set -Eeuo pipefail

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

# This is the continuous delivery build script executed after a git
# extract by the Jenkins build process located at the following URL:
# https://builds.apache.org/view/M-R/view/Qpid/job/Qpid-proton-c/
#

# https://gist.github.com/alexeagle/c8ed91b14a407342d9a8e112b5ac7dab

readonly help=$(cat <<- EOF
  This script should be run through the pre-commit git hook, which
   ensures that all dependencies are already present on PATH.
EOF
)

filedir=$(readlink -f "$(dirname "$0")")

readonly out=$(PATH=$PATH:${filedir} git clang-format --style=file -v)

if [[ "$out" == *"no modified files to format"* ]]; then exit 0; fi
if [[ "$out" == *"clang-format did not modify any files"* ]]; then exit 0; fi

echo "ERROR: you need to run git clang-format on your commit"
exit 1

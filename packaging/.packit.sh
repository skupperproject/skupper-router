#!/usr/bin/env bash

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

# This script handles any custom processing of the spec file generated using the `post-upstream-clone`
# action and gets used by the fix-spec-file action in .packit.yaml.

# Inspiration taken from https://github.com/containers/skopeo/blob/main/.packit.sh

set -eo pipefail

PROJECT=skupper-router

# RPM Spec modifications

# Version should be accepted by python3 -c 'import packaging.version; print(packaging.version.parse("0.0.0.3-git"))'
sed -i "s/^Version:.*/Version: $PACKIT_PROJECT_VERSION/" ${PROJECT}.spec

# Update Release in spec with Packit's release envvar
sed -i "s/^Release:.*/Release: $PACKIT_RPMSPEC_RELEASE%{?dist}/" ${PROJECT}.spec

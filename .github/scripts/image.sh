#!/bin/bash
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

set -exo pipefail

CONTAINER=docker
PROJECT_NAME=skupper-router
CONTAINER_REGISTRY=quay.io
CONTAINER_ORG=skupper
TARGET_PLATFORMS="linux/amd64,linux/arm64,linux/s390x,linux/ppc64le"
export BUILDAH_FORMAT=docker

# If PROJECT_TAG is not defined set PROJECT_TAG to main
if [ -z "${PROJECT_TAG:-}" ]; then
  PROJECT_TAG=main
fi

# Defining tags to be pushed
TAGS=(
    "-t ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}"
)
if [[ -n "${BUILD_NUMBER:-}" ]]; then
    TAGS+=("-t ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}-${BUILD_NUMBER}")
fi

if [[ -n "${PUSH_LATEST:-}" ]]; then
    TAGS+=("-t ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:latest")
fi

# Building the skupper-router image
# Pass the VERSION as a build argument so Containerfile can use it when calling compile.sh
# This version is passed in as a -DVERSION build parameter when building skupper-router.
# Pushing only when credentials available
if [[ -n "${CONTAINER_USER}" && -n "${CONTAINER_PASSWORD}" ]]; then
    # Login to the quay.io container repo.
    ${CONTAINER} login -u "${CONTAINER_USER}"      \
                       -p "${CONTAINER_PASSWORD}"  \
                          "${CONTAINER_REGISTRY}"
    PROVENANCE_FLAG=""
    if [ -n "$(docker buildx build --help | grep provenance)" ]; then
        PROVENANCE_FLAG="--provenance=false"
    fi
    ${CONTAINER} buildx build ${PROVENANCE_FLAG}                        \
        --platform "${TARGET_PLATFORMS}"                                \
        --build-arg "PLATFORM=$PLATFORM" --build-arg "VERSION=$VERSION" \
        --push ${TAGS[@]}                                               \
        -f ./Containerfile .
fi

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

CONTAINER=podman
PROJECT_NAME=skupper-router
CONTAINER_REGISTRY=quay.io
CONTAINER_ORG=skupper
export BUILDAH_FORMAT=docker
# If PROJECT_TAG is not defined set PROJECT_TAG to main
if [ -z "$PROJECT_TAG" ]; then
  PROJECT_TAG=main
fi

# Building the skupper-router image
# Pass the VERSION as a build argument so Containerfile can use it when calling compile.sh
# This version is passed in as a -DVERSION build parameter when building skupper-router.
${CONTAINER} build --build-arg VERSION=$VERSION -t ${PROJECT_NAME}:${PROJECT_TAG}  -f ./Containerfile .

# Pushing only when credentials available
if [[ -n "${CONTAINER_USER}" && -n "${CONTAINER_PASSWORD}" ]]; then
    ${CONTAINER} login -u ${CONTAINER_USER} -p ${CONTAINER_PASSWORD} ${CONTAINER_REGISTRY}
    ${CONTAINER} tag ${PROJECT_NAME}:${PROJECT_TAG} ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}
    ${CONTAINER} push ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}

    # PUSH_LATEST environment variable is exported only in release.yml
    # Only when an actual release tag (for e.g. 2.1.0) is pushed, we push the :latest.
    # :latest represents the latest released version of the software.
    # We do not push :latest when main or other non-release tags are pushed.
    if [ -z "$PUSH_LATEST" ]; then
         echo 'NOT Pushing :latest tag'
    else
        echo 'Pushing :latest tag'
        ${CONTAINER} tag ${PROJECT_NAME}:${PROJECT_TAG} ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:latest
        ${CONTAINER} push ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:latest
    fi
fi

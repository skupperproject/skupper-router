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

DOCKER=docker
PROJECT_NAME=skupper-router
DOCKER_REGISTRY=quay.io
DOCKER_ORG=skupper

# If PROJECT_TAG is not defined set PROJECT_TAG to main
PUSH_LATEST=true
if [ -z "$PROJECT_TAG" ]
then
  PROJECT_TAG=main
fi


# If PROJECT_TAG is not main (it is possibly a release), don't push the :latest tag
if [[ ${PROJECT_TAG} =~ rc || ${PROJECT_TAG} =~ x || ${PROJECT_TAG} =~ freeze || "${PROJECT_TAG}" != main ]]; then
    PUSH_LATEST=false
fi

# Building the skupper-router image
# Pass the VERSION as a build argument so Containerfile can use it when calling compile.sh
# This version is passed in as a -DVERSION build parameter when building skupper-router.
${DOCKER} build --build-arg VERSION=$VERSION -t ${PROJECT_NAME}:${PROJECT_TAG}  -f ./Containerfile .

# Pushing only when credentials available
if [[ -n "${DOCKER_USER}" && -n "${DOCKER_PASSWORD}" ]]; then
    ${DOCKER} login -u ${DOCKER_USER} -p ${DOCKER_PASSWORD} ${DOCKER_REGISTRY}
    ${DOCKER} tag ${PROJECT_NAME}:${PROJECT_TAG} ${DOCKER_REGISTRY}/${DOCKER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}
    ${DOCKER} push ${DOCKER_REGISTRY}/${DOCKER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}
    if ${PUSH_LATEST}; then
        ${DOCKER} tag ${PROJECT_NAME}:${PROJECT_TAG} ${DOCKER_REGISTRY}/${DOCKER_ORG}/${PROJECT_NAME}:latest
        ${DOCKER} push ${DOCKER_REGISTRY}/${DOCKER_ORG}/${PROJECT_NAME}:latest
    fi
fi

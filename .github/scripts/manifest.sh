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

# We currently do two platforms
PLATFORM_ARM64=linux-arm64
PLATFORM_AMD64=linux-amd64

# If PROJECT_TAG is not defined set PROJECT_TAG to main
if [ -z "$PROJECT_TAG" ]; then
  PROJECT_TAG=main
fi

PROJECT_TAG_ARM64=${PROJECT_TAG}-${PLATFORM_ARM64}
PROJECT_TAG_AMD64=${PROJECT_TAG}-${PLATFORM_AMD64}

# Manifest can be pushed only when credentials are available
if [[ -n "${CONTAINER_USER}" && -n "${CONTAINER_PASSWORD}" ]]; then
  # First, pull the arm64 and amd64 images
  ${CONTAINER} login -u ${CONTAINER_USER} -p ${CONTAINER_PASSWORD} ${CONTAINER_REGISTRY}

  # Pull the arm64 and amd64 images from the container registry
  ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_ARM64}
  ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_AMD64}

  # Create manifest
  ${CONTAINER} manifest create ${PROJECT_NAME}:${PROJECT_TAG} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_AMD64}
  ${CONTAINER} manifest create ${PROJECT_NAME}:${PROJECT_TAG} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_ARM64}
  # Push manifest to container registry
  ${CONTAINER} manifest push ${PROJECT_NAME}:${PROJECT_TAG} ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}

  # Only publish build number tag if $BUILD_NUMBER provided
  if [[ -n "${BUILD_NUMBER}" ]]; then
      # Pull the arm64 and amd64 images from the container registry
      ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_ARM64}-${BUILD_NUMBER}
      ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_AMD64}-${BUILD_NUMBER}

      # Create manifest using build number
      ${CONTAINER} manifest create ${PROJECT_NAME}:${PROJECT_TAG}-${BUILD_NUMBER} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_AMD64}-${BUILD_NUMBER} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_ARM64}-${BUILD_NUMBER}
      # Push manifest to container registry
      ${CONTAINER} manifest push ${PROJECT_NAME}:${PROJECT_TAG}-${BUILD_NUMBER} ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG}-${BUILD_NUMBER}
  fi

  if [ -z "$PUSH_LATEST" ]; then
    echo 'NOT Pushing :latest tag (manifest.sh)'
  else
    echo 'Pulling :latest-amd64 tag and :latest-arm64 (manifest.sh)'
    PROJECT_TAG_LATEST_ARM64=latest-linux-arm64
    PROJECT_TAG_LATEST_AMD64=latest-linux-amd64
    PROJECT_TAG_LATEST=latest
    ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_LATEST_ARM64}
    ${CONTAINER} pull ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_LATEST_AMD64}
    # Create manifest for latest tag
    ${CONTAINER} manifest create ${PROJECT_NAME}:${PROJECT_TAG_LATEST} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_LATEST_AMD64} --amend ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_LATEST_ARM64}
    # Push manifest to container registry
    ${CONTAINER} manifest push ${PROJECT_NAME}:${PROJECT_TAG_LATEST} ${CONTAINER_REGISTRY}/${CONTAINER_ORG}/${PROJECT_NAME}:${PROJECT_TAG_LATEST}
  fi
fi

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

# This is the continuous delivery build script executed after a git
# extract by the Jenkins build process located at the following URL:
# https://builds.apache.org/view/M-R/view/Qpid/job/Qpid-proton-c/
#

PREFIX=$PWD/install
DISPATCH_CMAKE_ARGS="-DRUNTIME_CHECK=${RUNTIME_CHECK} -DQD_ENABLE_ASSERTIONS=${QD_ENABLE_ASSERTIONS} -DDISPATCH_TEST_TIMEOUT=500 -DSANITIZE_PYTHON=OFF"
DISPATCH_CTEST_EXTRA='-E ^python-checker$'
export NPROC=3
export QPID_SYSTEM_TEST_TIMEOUT=300
export QPID_SYSTEM_TEST_SKIP_FALLBACK_SWITCHOVER_TEST=True
export QPID_SYSTEM_TEST_SKIP_HTTP2_LARGE_IMAGE_UPLOAD_TEST=True

echo '==='
echo 'Install additional prerequisites'
echo '==='

if [[ ${TRAVIS_CPU_ARCH} == "arm64" ]]; then
  sudo apt-get install -y clang-12 llvm-12-dev
  export CC=clang-12 CXX=clang++-12
fi

# Update pip, it may prevent issues later
python3 -m pip install --user --upgrade pip
# Install grpcio and protobuf to run the grpc tests.
#  Installation on s390x currently broken https://github.com/grpc/grpc/pull/25363
#  Binary wheel is not available in PyPI for s390x and source install requires fetching git submodules first
if [[ ${TRAVIS_CPU_ARCH} == "aarch64" ]]; then
  python3 -m pip install --user grpcio protobuf
else
  python3 -m pip install --user protobuf
  sudo apt install python3-grpcio
fi
python3 -m pip install --user -r requirements-dev.txt

echo '==='
echo 'Build and install qpid-proton from source'
echo '==='

git clone --depth=1 --branch="$PROTON_VERSION" https://github.com/apache/qpid-proton.git
echo "Current proton commit: $(git --git-dir=qpid-proton rev-parse HEAD) (${PROTON_VERSION})"

mkdir qpid-proton/build
pushd qpid-proton/build
  cmake .. -DCMAKE_INSTALL_PREFIX="${PREFIX}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DBUILD_BINDINGS=python -DBUILD_TLS=ON
  cmake --build . --target install -- -j $NPROC
popd

source qpid-proton/build/config.sh

echo '==='
echo "Build skupper-router and run tests"
echo '==='

# CMake on Ubuntu Focal is 3.16; patch the requirement in skupper-router
sed -i -e 's/cmake_minimum_required(VERSION 3.20)/cmake_minimum_required(VERSION 3.16)/' CMakeLists.txt

mkdir build
pushd build
  cmake .. -DCMAKE_INSTALL_PREFIX="${PREFIX}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" ${DISPATCH_CMAKE_ARGS}
  make -j $NPROC
  ctest -j $NPROC -V ${DISPATCH_CTEST_EXTRA}
  if [[ "${BUILD_TYPE}" == "Coverage" ]]; then
    cmake --build . --target coverage
  fi
popd

echo '==='
echo "Report coverage"
echo '==='

if [[ "$BUILD_TYPE" = "Coverage" ]]; then
  pushd "${TRAVIS_BUILD_DIR}/build"
    bash <(curl -s https://codecov.io/bash);
  popd
fi

echo '==='
echo "Script completed"
echo '==='

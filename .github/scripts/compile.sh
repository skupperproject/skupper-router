#!/usr/bin/bash

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

# https://sipb.mit.edu/doc/safe-shell
set -Eefuxo pipefail

BUILD_FLAGS="$(rpmbuild --undefine _annotated_build --eval '%set_build_flags')"
eval "${BUILD_FLAGS}"

if [ -z "${REMOTE_SOURCES_DIR:-}" ]; then
  # If no REMOTE_SOURCES_DIR present in env, we use $(pwd) as working dir
  WORKING_DIR="$(pwd)"
  SKUPPER_DIR="$WORKING_DIR"
  PROTON_DIR="$WORKING_DIR/proton/app"
  LWS_DIR="$WORKING_DIR/libwebsockets/app"
  # $REMOTE_SOURCES_DIR was not provided, we will have to download the libwebsockets source from ${LWS_SOURCE_URL}
  # Get the libwebsockets source into a libwebsockets.tar.gz file
  # and untar it into the libwebsockets folder
  wget "${LWS_SOURCE_URL}" -O libwebsockets.tar.gz
  tar -zxf libwebsockets.tar.gz --one-top-level="${LWS_DIR}" --strip-components 1

  # No $REMOTE_SOURCES_DIR was provided, we will have to download the proton source tar.gz from ${PROTON_SOURCE_URL}
  wget "${PROTON_SOURCE_URL}" -O qpid-proton.tar.gz
  tar -zxf qpid-proton.tar.gz --one-top-level="${PROTON_DIR}" --strip-components 1
else
  # If the env contains REMOTE_SOURCES_DIR, we will use that as the working dir
  # If REMOTE_SOURCES_DIR is provided, this scripts expects the following -
  # 1. proton sources to be in $REMOTE_SOURCES_DIR/app/proton
  # 2. libwebsockets sources to be in $REMOTE_SOURCES_DIR/app/libwebsockets
  # 3. skupper-router sources to be in $REMOTE_SOURCES_DIR/app/skupper-router
  WORKING_DIR="${REMOTE_SOURCES_DIR}"
  SKUPPER_DIR="${WORKING_DIR}/skupper-router/app"
  PROTON_DIR="${WORKING_DIR}/proton/app"
  LWS_DIR="${WORKING_DIR}/libwebsockets/app"
  cd "${WORKING_DIR}"
fi

LWS_BUILD_DIR="${LWS_DIR}/build"
LWS_INSTALL_DIR="${LWS_DIR}/install"

PROTON_INSTALL_DIR="${PROTON_DIR}/proton_install"
PROTON_BUILD_DIR="${PROTON_DIR}/build"
SKUPPER_BUILD_DIR="${SKUPPER_DIR}/build"


#region libwebsockets
# Build libwebsockets library.
# Source folder (cmake -S) is $LWS_DIR
# Build dir (cmake -B) is $LWS_BUILD_DIR
cmake -S "${LWS_DIR}" -B "${LWS_BUILD_DIR}" \
  -DLWS_LINK_TESTAPPS_DYNAMIC=ON \
  -DLWS_WITH_LIBUV=OFF \
  -DLWS_WITHOUT_BUILTIN_GETIFADDRS=ON \
  -DLWS_USE_BUNDLED_ZLIB=OFF \
  -DLWS_WITHOUT_BUILTIN_SHA1=ON \
  -DLWS_WITH_STATIC=OFF \
  -DLWS_IPV6=ON \
  -DLWS_WITH_HTTP2=OFF \
  -DLWS_WITHOUT_CLIENT=OFF \
  -DLWS_WITHOUT_SERVER=OFF \
  -DLWS_WITHOUT_TESTAPPS=ON \
  -DLWS_WITHOUT_TEST_SERVER=ON \
  -DLWS_WITHOUT_TEST_SERVER_EXTPOLL=ON \
  -DLWS_WITHOUT_TEST_PING=ON \
  -DLWS_WITHOUT_TEST_CLIENT=ON
cmake --build "${LWS_BUILD_DIR}" --parallel "$(nproc)" --verbose
cmake --install "${LWS_BUILD_DIR}"

DESTDIR="${LWS_INSTALL_DIR}" cmake --install "${LWS_BUILD_DIR}"
tar -z -C "${LWS_INSTALL_DIR}" -cf /libwebsockets-image.tar.gz usr
#endregion libwebsockets

do_patch () {
    PATCH_DIR=$1
    PATCH_SRC=$2
    if [ -d "${PATCH_DIR}" ]
    then
        for patch in $(find "${PATCH_DIR}" -type f -name "*.patch"); do
            echo Applying patch "${patch}"
            patch -f -d "${PATCH_SRC}" -p1 < "$patch"
        done;
    fi
}

do_patch "patches/proton" "${PROTON_DIR}"

do_build () {
  local suffix=${1}
  local runtime_check=${2}

  cmake -S "${PROTON_DIR}" -B "${PROTON_BUILD_DIR}${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DENABLE_LINKTIME_OPTIMIZATION=ON \
    -DCMAKE_POLICY_DEFAULT_CMP0069=NEW -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DBUILD_TLS=ON -DSSL_IMPL=openssl -DBUILD_STATIC_LIBS=ON -DBUILD_BINDINGS=python -DSYSINSTALL_PYTHON=ON \
    -DBUILD_EXAMPLES=OFF -DBUILD_TESTING=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr
  cmake --build "${PROTON_BUILD_DIR}${suffix}" --verbose

  DESTDIR="$PROTON_INSTALL_DIR${suffix}" cmake --install "$PROTON_BUILD_DIR${suffix}"

  cmake -S "${SKUPPER_DIR}" -B "${SKUPPER_BUILD_DIR}${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DSANITIZE_PYTHON=OFF \
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DProton_USE_STATIC_LIBS=ON \
    -DProton_DIR="${PROTON_INSTALL_DIR}${suffix}/usr/lib64/cmake/Proton" \
    -DBUILD_TESTING=OFF \
    -DVERSION="${VERSION}" \
    -DCMAKE_INSTALL_PREFIX=/usr
  cmake --build "${SKUPPER_BUILD_DIR}${suffix}" --verbose
}

# Do a regular build without asan or tsan.
do_build "" OFF

# talking to annobin is not straightforward, https://bugzilla.redhat.com/show_bug.cgi?id=1536569
common_sanitizer_flags="-Wp,-U_FORTIFY_SOURCE -fplugin=annobin -fplugin-arg-annobin-no-active-checks"
export CFLAGS="${CFLAGS} ${common_sanitizer_flags}"
export CXXFLAGS="${CXXFLAGS} ${common_sanitizer_flags}"
do_build "_asan" asan
do_build "_tsan" tsan

tar -z -C "${PROTON_INSTALL_DIR}" -cf /qpid-proton-image.tar.gz usr

DESTDIR="${SKUPPER_DIR}/staging/" cmake --install "${SKUPPER_BUILD_DIR}"
cp "${SKUPPER_BUILD_DIR}_asan/router/skrouterd" "${SKUPPER_DIR}/staging/usr/sbin/skrouterd_asan"
cp "${SKUPPER_BUILD_DIR}_tsan/router/skrouterd" "${SKUPPER_DIR}/staging/usr/sbin/skrouterd_tsan"
cp --target-directory="${SKUPPER_DIR}/staging/" "${SKUPPER_DIR}/tests/tsan.supp" "${SKUPPER_BUILD_DIR}_asan/tests/lsan.supp"
tar -z -C "${SKUPPER_DIR}/staging/" -cf /skupper-router-image.tar.gz usr etc lsan.supp tsan.supp
#endregion qpid-proton and skupper-router

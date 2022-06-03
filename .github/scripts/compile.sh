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

set -euxo pipefail

WORKING=$(pwd)
eval "$(rpmbuild --eval '%set_build_flags')"

#region libwebsockets
wget ${LWS_SOURCE_URL} -O libwebsockets.tar.gz
tar -zxf libwebsockets.tar.gz --one-top-level=lws-src --strip-components 1

cmake -S "$WORKING/lws-src" -B "$WORKING/lws_build" \
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
cmake --build "$WORKING/lws_build" --parallel "$(nproc)" --verbose
cmake --install "$WORKING/lws_build"

DESTDIR=$WORKING/lws_install cmake --install "$WORKING/lws_build"
tar -z -C $WORKING/lws_install -cf /libwebsockets-image.tar.gz usr
#endregion

#region qpid-proton and skupper-router
wget ${PROTON_SOURCE_URL} -O qpid-proton.tar.gz
tar -zxf qpid-proton.tar.gz --one-top-level=qpid-proton-src --strip-components 1

do_patch () {
    PATCH_DIR=$1
    PATCH_SRC=$2
    if [ -d "${PATCH_DIR}" ]
    then
        for patch in $(find ${PATCH_DIR} -type f -name "*.patch"); do
            echo Applying patch ${patch}
            patch -f -d "${PATCH_SRC}" -p1 < $patch
        done;
    fi
}

do_patch "patches/proton" qpid-proton-src

do_build () {
  local suffix=${1}
  local runtime_check=${2}

  cmake -S "$WORKING/qpid-proton-src" -B "$WORKING/proton_build${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DENABLE_LINKTIME_OPTIMIZATION=ON \
    -DCMAKE_POLICY_DEFAULT_CMP0069=NEW -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DBUILD_TLS=ON -DSSL_IMPL=openssl -DBUILD_STATIC_LIBS=ON -DBUILD_BINDINGS=python -DSYSINSTALL_PYTHON=ON \
    -DBUILD_EXAMPLES=OFF -DBUILD_TESTING=OFF \
    -DCMAKE_INSTALL_PREFIX=/usr
  cmake --build "proton_build${suffix}" --parallel "$(nproc)" --verbose
  DESTDIR="$WORKING/proton_install${suffix}" cmake --install "proton_build${suffix}"

  cmake -S "$WORKING/" -B "$WORKING/build${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DSANITIZE_PYTHON=OFF \
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DProton_USE_STATIC_LIBS=ON \
    -DProton_DIR="$WORKING/proton_install${suffix}/usr/lib64/cmake/Proton" \
    -DBUILD_TESTING=OFF \
    -DVERSION="${VERSION}" \
    -DCMAKE_INSTALL_PREFIX=/usr
  cmake --build "$WORKING/build${suffix}" --parallel "$(nproc)" --verbose
}

do_build "" OFF

# talking to annobin is not straightforward, https://bugzilla.redhat.com/show_bug.cgi?id=1536569
common_sanitizer_flags="-Wp,-U_FORTIFY_SOURCE -fplugin=annobin -fplugin-arg-annobin-no-active-checks"
export CFLAGS="${CFLAGS} ${common_sanitizer_flags}"
export CXXFLAGS="${CXXFLAGS} ${common_sanitizer_flags}"
do_build "_asan" asan
do_build "_tsan" tsan

tar -z -C $WORKING/proton_install -cf /qpid-proton-image.tar.gz usr

DESTDIR=$WORKING/staging/ cmake --install $WORKING/build
cp "$WORKING/build_asan/router/skrouterd" "$WORKING/staging/usr/sbin/skrouterd_asan"
cp "$WORKING/build_tsan/router/skrouterd" "$WORKING/staging/usr/sbin/skrouterd_tsan"
cp --target-directory="$WORKING/staging/" "$WORKING/tests/tsan.supp" "$WORKING/build_asan/tests/lsan.supp"
tar -z -C $WORKING/staging/ -cf /skupper-router-image.tar.gz usr etc lsan.supp tsan.supp
#endregion

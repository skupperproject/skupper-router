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

WORKING=`pwd`
wget ${PROTON_SOURCE_URL} -O qpid-proton.tar.gz
tar -zxf qpid-proton.tar.gz --one-top-level=qpid-proton-src --strip-components 1

mkdir -p qpid-proton-src build staging proton_build proton_install

do_patch "patches/proton" qpid-proton-src

cmake -S $WORKING/qpid-proton-src -B proton_build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DRUNTIME_CHECK=OFF \
  -DENABLE_LINKTIME_OPTIMIZATION=ON \
  -DCMAKE_POLICY_DEFAULT_CMP0069=NEW -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
  -DBUILD_TLS=ON -DSSL_IMPL=openssl -DBUILD_STATIC_LIBS=ON -DBUILD_BINDINGS=python -DSYSINSTALL_PYTHON=ON \
  -DBUILD_EXAMPLES=OFF -DBUILD_TESTING=OFF \
  -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build proton_build --verbose \
    && DESTDIR=$WORKING/proton_install cmake --install proton_build \
    && tar -z -C $WORKING/proton_install -cf /qpid-proton-image.tar.gz usr \
    && cmake --install proton_build

cmake -S $WORKING/ -B $WORKING/build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DRUNTIME_CHECK=OFF \
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
  -DProton_USE_STATIC_LIBS=ON -DUSE_LIBWEBSOCKETS=ON -DUSE_LIBNGHTTP2=ON \
  -DBUILD_TESTING=OFF \
  -DVERSION=${VERSION} \
  -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build $WORKING/build --verbose \
    && VERBOSE=1 DESTDIR=$WORKING/staging/ cmake --install $WORKING/build \
    && tar -z -C $WORKING/staging/ -cf /skupper-router-image.tar.gz usr etc

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

mkdir -p qpid-proton-src build staging proton_build proton_install
tar -zxf qpid-proton.tar.gz -C qpid-proton-src --strip-components 1

do_patch "patches/proton" qpid-proton-src

cd proton_build
cmake -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_BINDINGS=python -DCMAKE_INSTALL_PREFIX=/usr -DSYSINSTALL_PYTHON=ON -DSSL_IMPL=openssl $WORKING/qpid-proton-src/ \
    && ninja \
    && DESTDIR=$WORKING/proton_install ninja install \
    && tar -z -C $WORKING/proton_install -cf /qpid-proton-image.tar.gz usr \
    && ninja install
cd $WORKING/build
cmake -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DUSE_LIBWEBSOCKETS=ON -DCMAKE_INSTALL_PREFIX=/usr $WORKING/ \
    && ninja  \
    && DESTDIR=$WORKING/staging/ ninja install \
    && tar -z -C $WORKING/staging/ -cf /skupper-router-image.tar.gz usr etc

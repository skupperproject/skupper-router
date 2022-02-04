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

FROM fedora:34 as builder

RUN dnf -y install gcc gcc-c++ make cmake cyrus-sasl-devel cyrus-sasl-plain cyrus-sasl-gssapi cyrus-sasl-md5 openssl-devel libuuid-devel swig wget patch findutils git valgrind libwebsockets-devel python3-devel libnghttp2-devel && dnf clean all -y
WORKDIR /build
COPY . .
ENV PROTON_VERSION=0.36.0
ENV PROTON_SOURCE_URL=${PROTON_SOURCE_URL:-http://archive.apache.org/dist/qpid/proton/${PROTON_VERSION}/qpid-proton-${PROTON_VERSION}.tar.gz}
RUN .github/scripts/compile.sh


FROM fedora:34

# gdb and valgrind are part of final image as they can be used as debug options for Skupper
RUN dnf -y install glibc cyrus-sasl-lib libuuid openssl gettext hostname iputils libwebsockets-devel libnghttp2 gdb valgrind && dnf -y update && dnf clean all

WORKDIR /
COPY --from=builder /qpid-proton-image.tar.gz /skupper-router-image.tar.gz /
RUN tar zxpf qpid-proton-image.tar.gz && tar zxpf skupper-router-image.tar.gz && rm -f /qpid-proton-image.tar.gz /skupper-router-image.tar.gz

WORKDIR /home/qdrouterd/etc
WORKDIR /home/qdrouterd/bin
COPY ./scripts/* /home/qdrouterd/bin/

ARG version=latest
ENV VERSION=${version}
ENV QDROUTERD_HOME=/home/qdrouterd

EXPOSE 5672 55672 5671
CMD ["/home/qdrouterd/bin/launch.sh"]

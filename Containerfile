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

FROM registry.access.redhat.com/ubi8/ubi:latest as builder

RUN dnf -y --setopt=tsflags=nodocs install \
      http://mirror.centos.org/centos/8-stream/BaseOS/x86_64/os/Packages/centos-gpg-keys-8-4.el8.noarch.rpm \
      http://mirror.centos.org/centos/8-stream/BaseOS/x86_64/os/Packages/centos-stream-repos-8-4.el8.noarch.rpm \
 && dnf -y --setopt=tsflags=nodocs install epel-release \
 && dnf config-manager --set-enabled powertools \
 && dnf clean all

RUN dnf -y --setopt=tsflags=nodocs install \
    gcc gcc-c++ make cmake \
    cyrus-sasl-devel openssl-devel libuuid-devel \
    python3-devel swig \
    libwebsockets-devel libnghttp2-devel \
    wget patch findutils git valgrind \
 && dnf clean all -y
WORKDIR /build
COPY . .
ENV PROTON_VERSION=0.37.0
ENV PROTON_SOURCE_URL=${PROTON_SOURCE_URL:-http://archive.apache.org/dist/qpid/proton/${PROTON_VERSION}/qpid-proton-${PROTON_VERSION}.tar.gz}

ARG VERSION=UNKNOWN
ENV VERSION=$VERSION
RUN .github/scripts/compile.sh

FROM registry.access.redhat.com/ubi8/ubi:latest

RUN dnf -y --setopt=tsflags=nodocs update \
 && dnf -y --setopt=tsflags=nodocs install \
      http://mirror.centos.org/centos/8-stream/BaseOS/x86_64/os/Packages/centos-gpg-keys-8-4.el8.noarch.rpm \
      http://mirror.centos.org/centos/8-stream/BaseOS/x86_64/os/Packages/centos-stream-repos-8-4.el8.noarch.rpm \
 && dnf -y --setopt=tsflags=nodocs install epel-release \
 && dnf config-manager --set-enabled powertools \
 && dnf clean all

# gdb and valgrind are part of final image as they can be used as debug options for Skupper
RUN dnf -y --setopt=tsflags=nodocs install \
    glibc \
    cyrus-sasl-lib cyrus-sasl-plain cyrus-sasl-gssapi libuuid openssl \
    python3 \
    libwebsockets libnghttp2 \
    gdb valgrind \
    gettext hostname iputils \
 && dnf clean all

WORKDIR /
COPY --from=builder /qpid-proton-image.tar.gz /skupper-router-image.tar.gz /
RUN tar zxpf qpid-proton-image.tar.gz && tar zxpf skupper-router-image.tar.gz && rm -f /qpid-proton-image.tar.gz /skupper-router-image.tar.gz

WORKDIR /home/skrouterd/etc
WORKDIR /home/skrouterd/bin
COPY ./scripts/* /home/skrouterd/bin/

ARG version=latest
ENV VERSION=${version}
ENV QDROUTERD_HOME=/home/skrouterd

EXPOSE 5672 55672 5671
CMD ["/home/skrouterd/bin/launch.sh"]

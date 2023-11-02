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

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest as builder

RUN microdnf -y --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install \
    rpm-build \
    gcc gcc-c++ make cmake pkgconfig \
    cyrus-sasl-devel openssl-devel libuuid-devel \
    python3-devel python3-pip \
    libnghttp2-devel \
    wget tar patch findutils git \
    libtool \
 && microdnf clean all -y

WORKDIR /build
COPY . .
ENV PROTON_VERSION=0.39.0
ENV PROTON_SOURCE_URL=${PROTON_SOURCE_URL:-https://github.com/apache/qpid-proton/archive/${PROTON_VERSION}.tar.gz}
ENV LWS_VERSION=v4.3.2
ENV LIBUNWIND_VERSION=v1.6.2
ENV LWS_SOURCE_URL=${LWS_SOURCE_URL:-https://github.com/warmcat/libwebsockets/archive/refs/tags/${LWS_VERSION}.tar.gz}
ENV LIBUNWIND_SOURCE_URL=${LIBUNWIND_SOURCE_URL:-https://github.com/libunwind/libunwind/archive/refs/tags/${LIBUNWIND_VERSION}.tar.gz}
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

ARG VERSION=0.0.0
ENV VERSION=$VERSION
RUN .github/scripts/compile.sh
RUN tar zxpf /qpid-proton-image.tar.gz --one-top-level=/image && tar zxpf /skupper-router-image.tar.gz --one-top-level=/image && tar zxpf /libwebsockets-image.tar.gz --one-top-level=/image && tar zxpf /libunwind-image.tar.gz --one-top-level=/image

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

RUN microdnf -y --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install \
    glibc \
    cyrus-sasl-lib cyrus-sasl-plain cyrus-sasl-gssapi openssl \
    python3 \
    libnghttp2 \
    gettext hostname iputils \
    shadow-utils \
 && microdnf clean all

RUN useradd --uid 10000 runner
USER 10000

WORKDIR /
COPY --from=builder /image /

WORKDIR /home/skrouterd/etc
WORKDIR /home/skrouterd/bin
COPY ./scripts/* /home/skrouterd/bin/

ARG version=latest
ENV VERSION=${version}
ENV QDROUTERD_HOME=/home/skrouterd

EXPOSE 5672 55672 5671
CMD ["/home/skrouterd/bin/launch.sh"]

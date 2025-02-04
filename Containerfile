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

# upgrade first to avoid fixable vulnerabilities
# do this in builder as well as in buildee, so builder does not have different pkg versions from buildee image
RUN microdnf -y upgrade --refresh --best --nodocs --noplugins --setopt=install_weak_deps=0 --setopt=keepcache=0 \
 && microdnf clean all -y

RUN microdnf -y --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install \
    rpm-build \
    gcc gcc-c++ make cmake pkgconfig \
    cyrus-sasl-devel openssl-devel libuuid-devel \
    python3-devel python3-pip python3-wheel \
    libnghttp2-devel \
    wget tar patch findutils git \
    libtool \
 && microdnf clean all -y

WORKDIR /build
COPY . .
ENV PROTON_VERSION=main
ENV PROTON_SOURCE_URL=${PROTON_SOURCE_URL:-https://github.com/apache/qpid-proton/archive/${PROTON_VERSION}.tar.gz}
ENV LWS_VERSION=v4.3.3
ENV LIBUNWIND_VERSION=v1.8.1
ENV LWS_SOURCE_URL=${LWS_SOURCE_URL:-https://github.com/warmcat/libwebsockets/archive/refs/tags/${LWS_VERSION}.tar.gz}
ENV LIBUNWIND_SOURCE_URL=${LIBUNWIND_SOURCE_URL:-https://github.com/libunwind/libunwind/archive/refs/tags/${LIBUNWIND_VERSION}.tar.gz}
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

ARG VERSION=0.0.0
ENV VERSION=$VERSION
ARG PLATFORM=amd64
ENV PLATFORM=$PLATFORM
RUN .github/scripts/compile.sh
RUN if [ "$PLATFORM" = "amd64" ]; then tar zxpf /qpid-proton-image.tar.gz --one-top-level=/image && tar zxpf /skupper-router-image.tar.gz --one-top-level=/image && tar zxpf /libwebsockets-image.tar.gz --one-top-level=/image && tar zxpf /libunwind-image.tar.gz --one-top-level=/image; fi
RUN if [ "$PLATFORM" = "arm64" ]; then tar zxpf /qpid-proton-image.tar.gz --one-top-level=/image && tar zxpf /skupper-router-image.tar.gz --one-top-level=/image && tar zxpf /libwebsockets-image.tar.gz --one-top-level=/image; fi
RUN mkdir /image/licenses && cp ./LICENSE /image/licenses

FROM registry.access.redhat.com/ubi9/ubi:latest AS packager

RUN dnf -y --setopt=install_weak_deps=0 --nodocs \
    --installroot /output install \
    coreutils-single \
    cyrus-sasl-lib cyrus-sasl-plain openssl \
    python3 \
    libnghttp2 \
    hostname iputils \
    shadow-utils \
 && chroot /output useradd --uid 10000 runner \
 && dnf -y --installroot /output remove shadow-utils \
 && dnf clean all --installroot /output

FROM scratch

COPY --from=packager /output /
COPY --from=packager /etc/yum.repos.d /etc/yum.repos.d
COPY --from=packager /root/buildinfo /root/buildinfo

USER 10000

COPY --from=builder /image /

WORKDIR /home/skrouterd/bin
COPY ./scripts/* /home/skrouterd/bin/

ARG version=latest
ENV VERSION=${version}
ENV QDROUTERD_HOME=/home/skrouterd

EXPOSE 5672 55672 5671
CMD ["/home/skrouterd/bin/launch.sh"]

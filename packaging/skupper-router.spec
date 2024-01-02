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

# Tutorial for .spec files is available at https://rpm-packaging-guide.github.io

# This .spec file uses `packit` (https://packit.dev/docs/cli/srpm/) to provide
# pleasant user experience to developers.
#
#  Command                           Description
# `packit srpm`                      Creates a *.src.rpm file in the local directory (exact path is printed)
# `packit build locally`             Builds a *.rpm for your system in the local directory (exact path is printed)
#
# See `man packit` for more commands. See https://packit.dev/docs/actions/#fix-spec-file for explanation of "actions".

# not undefine, that would break COPR, https://pagure.io/rpkg-util/issue/44
%define _disable_source_fetch 0
# https://bugzilla.redhat.com/show_bug.cgi?id=1668512#c19
%undefine __cmake_in_source_build

# This package builds and statically links against its own proton-c library
# so we can use newest Proton features even before it is packaged for our distro
%global proton_vendored_version 0.39.0
%define proton_install_prefix %{_builddir}/qpid-proton-%{proton_vendored_version}/install

%global python_minimum_version 3.9.0
%global proton_minimum_version 0.37.0
%global libwebsockets_minimum_version 3.0.1
%global libnghttp2_minimum_version 1.33.0
%global libunwind_minimum_version 1.3.1

Name:          skupper-router
Version:       0.0.0
Release:       1%{?dist}
Summary:       The skrouterd router daemon for Skupper.io
License:       ASL 2.0
URL:           https://skupper.io

Requires: python3
Requires: skupper-router-common == %{version}
Requires: libwebsockets >= %{libwebsockets_minimum_version}
Requires: libnghttp2 >= %{libnghttp2_minimum_version}
Requires: cyrus-sasl-plain
Requires: cyrus-sasl-gssapi
%ifnarch aarch64
Requires: libunwind >= %{libunwind_minimum_version}
%endif

BuildRequires: gcc
BuildRequires: gcc-c++
BuildRequires: cmake
BuildRequires: gdb

# skupper-router requirements
BuildRequires: python3-devel >= %{python_minimum_version}
BuildRequires: python3-setuptools
BuildRequires: python3-wheel
BuildRequires: python3-pip
BuildRequires: python3-rpm-macros
BuildRequires: libwebsockets-devel >= %{libwebsockets_minimum_version}
BuildRequires: libnghttp2-devel >= %{libnghttp2_minimum_version}
# man pages --help
BuildRequires: asciidoc
BuildRequires: python3-qpid-proton >= %{proton_minimum_version}
# check ctest
BuildRequires: cyrus-sasl-plain
BuildRequires: openssl
# check python linters
BuildRequires: python3-flake8
BuildRequires: pylint
BuildRequires: python3-mypy

# proton-c requirements
BuildRequires: openssl-devel
BuildRequires: cyrus-sasl-devel

# skupper-router sources
Source0: packit-placeholder-value.tar.gz
# vendored qpid-proton
Source1: https://www.apache.org/dist/qpid/proton/%{proton_vendored_version}/qpid-proton-%{proton_vendored_version}.tar.gz
Source2: https://github.com/libunwind/libunwind/releases/download/v1.8.0-rc1/libunwind-1.8.0-rc1.tar.gz
#Source2: https://github.com/libunwind/libunwind/releases/download/v1.7.2/libunwind-1.7.2.tar.gz

%description
A lightweight message router, written in C and built on Qpid Proton, that provides flexible and scalable interconnect backend for Skupper.io Level 7 Virtual Application Network.

%prep
%setup -T -b 0 -q -n skupper-router
%setup -q -D -b 1 -n qpid-proton-%{proton_vendored_version}
%setup -q -D -b 2 -n libunwind-1.8.0-rc1

%build
%set_build_flags

cd %{_builddir}/libunwind-1.8.0-rc1
./configure --prefix=%{_builddir}/libunwind-install "CFLAGS='-latomic -O2'" LDFLAGS="-latomic"
make install

cd %{_builddir}/qpid-proton-%{proton_vendored_version}
%__cmake . -B "%{__cmake_builddir}" \
    -DBUILD_TOOLS=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_BINDINGS=OFF \
    -DPython_EXECUTABLE=%{python3} \
    -DBUILD_TLS=ON -DSSL_IMPL=openssl \
    -DBUILD_STATIC_LIBS=ON \
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DCMAKE_INSTALL_PREFIX=%{proton_install_prefix}
%__cmake --build "%{__cmake_builddir}" %{?_smp_mflags} --verbose
%__cmake --install "%{__cmake_builddir}"

cd %{_builddir}/skupper-router-%{version}
%cmake \
    -DVERSION="%{version}" \
    -DPython_EXECUTABLE=%{python3} \
    -DProton_USE_STATIC_LIBS=ON \
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DProton_DIR=%{proton_install_prefix}/lib64/cmake/Proton
%cmake_build --target all --target man

%install
cd %{_builddir}/skupper-router-%{version}
%cmake_install

# https://docs.fedoraproject.org/en-US/packaging-guidelines/Python/#py3_shebang_fix
%py3_shebang_fix %{buildroot}/%{_bindir}/skmanage %{buildroot}/%{_bindir}/skstat

%check
cd %{_builddir}/skupper-router-%{version}
# Python 3.12 considers emtpy test suite a failure, and the following suites skip all tests due to missing reqs:
#  test_stopping_broker_while_websocket_is_connected_does_not_crash (system_tests_websockets.WebsocketsConsoleTest.test_stopping_broker_while_websocket_is_connected_does_not_crash) ... skipped 'python test requirement package `websockets` is missing'
#  test_grpc_01_unary (system_tests_grpc.GrpcServiceMethodsTest.test_grpc_01_unary) ... skipped 'grpcio is needed to run grpc tests'
# -R cpp_unit
#gdb -quiet -iex 'set pagination off' -iex 'set debuginfod enabled on' -ex run -ex 'thread apply all bt' -ex 'quit $_exitcode' --batch --args %{__cmake_builddir}/tests/cpp/cpp_unit/cpp_unit
LD_PRELOAD=%{_builddir}/libunwind-install/lib/libunwind.so %{__cmake_builddir}/tests/cpp/cpp_unit/cpp_unit

%files
/usr/sbin/skrouterd
/usr/lib/skupper-router/python/skupper_router/

%config /etc/skupper-router/skrouterd.conf
%config /etc/sasl2/skrouterd.conf

/usr/share/man/man5/skrouterd.conf.5.gz
/usr/share/man/man8/skrouterd.8.gz

/usr/share/skupper-router/html/index.html

%package common
Summary:  Internal code shared between the router daemon and the tools
BuildArch: noarch
Requires: python3
Requires: python3-qpid-proton >= %{proton_minimum_version}

%description common
%{summary}.

%files common
/usr/lib/skupper-router/python/skupper_router_internal/

%package tools
Summary:  The skstat and skmanage tools for skrouterd
BuildArch: noarch
Requires: python3
Requires: python3-qpid-proton >= %{proton_minimum_version}
Requires: skupper-router-common == %{version}
Requires: cyrus-sasl-plain
Requires: cyrus-sasl-gssapi

%description tools
%{summary}.

%files tools
/usr/bin/skmanage
/usr/bin/skstat

/usr/share/man/man8/skstat.8.gz
/usr/share/man/man8/skmanage.8.gz

%package tests
Summary:  Tests for the skupper router and the tools
Requires: python3
Requires: python3-qpid-proton >= %{proton_minimum_version}
Requires: skupper-router == %{version}
Requires: skupper-router-tools == %{version}
Requires: cyrus-sasl-plain

%description tests
%{summary}.

%files tests
/usr/lib/skupper-router/tests/

%package docs
Summary:  Documentation for the skupper router
BuildArch: noarch

%description docs
%{summary}.

%files docs
/usr/share/doc/skupper-router/README.adoc
/usr/share/doc/skupper-router/skrouter.json
/usr/share/doc/skupper-router/skrouter.json.readme.txt
%license /usr/share/doc/skupper-router/LICENSE

%changelog
%autochangelog

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

# This .spec file uses `rpkg` (https://pagure.io/rpkg-util) to provide
# pleasant user experience to developers.
#
#  Command                  Description
# `rpkg srpm`              Creates a *.src.rpm file in /tmp/rpkg (exact path is printed)
# `rpkg local --nocheck`   Builds a *.rpm for your system in /tmp/rpkg (exact path is printed)
#
# See `man rpkg` for more commands.

%global proton_minimum_version 0.34.0
%global libwebsockets_minimum_version 3.0.1
%global libnghttp2_minimum_version 1.33.0

Name:          skupper
Version:       {{{ git_dir_version }}}
Release:       2%{?dist}
Summary:       Router for Skupper.io
License:       ASL 2.0
URL:           https://skupper.io
VCS:           {{{ git_dir_vcs }}}

BuildRequires: cmake
BuildRequires: python3-devel
BuildRequires: openssl-devel
BuildRequires: qpid-proton-c-devel >= %{proton_minimum_version}
BuildRequires: python3-qpid-proton >= %{proton_minimum_version}
BuildRequires: libwebsockets-devel >= %{libwebsockets_minimum_version}
BuildRequires: libnghttp2-devel >= %{libnghttp2_minimum_version}

Source: {{{ git_dir_pack }}}

%description
A lightweight message router, written in C and built on Qpid Proton, that provides flexible and scalable interconnect backend for Skupper.io Level 7 Virtual Application Network.

%package router
Summary:  The skrouterd Skupper router deamon
Requires: python3
Requires: qpid-proton-c >= %{proton_minimum_version}
Requires: python3-qpid-proton >= %{proton_minimum_version}
Requires: libwebsockets >= %{libwebsockets_minimum_version}
Requires: libnghttp2 >= %{libnghttp2_minimum_version}

%description router
%{summary}.

%prep
{{{ git_dir_setup_macro }}}

%build
%cmake -DPython_EXECUTABLE=%python3
%cmake_build

%install
%cmake_install

%check
%ctest

%files router
/etc/qpid-dispatch/qdrouterd.conf
/etc/sasl2/qdrouterd.conf
/usr/bin/qdmanage
/usr/bin/qdstat
/usr/sbin/qdrouterd

/usr/include/qpid/dispatch.h
/usr/include/qpid/dispatch/

%{python3_sitelib}/qpid_dispatch/
%{python3_sitelib}/qpid_dispatch_site.py
%{python3_sitelib}/__pycache__/qpid_dispatch_site.*.pyc
%{python3_sitelib}/qpid_dispatch-*.egg-info
/usr/lib/qpid-dispatch/python/qpid_dispatch_internal/

/usr/lib/qpid-dispatch/tests/
/usr/share/doc/qpid-dispatch/
/usr/share/qpid-dispatch/

%changelog
{{{ git_dir_changelog }}}

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
# under the License
#

"""
Interface between Python and C router code.

This module contains python ctypes definitions to directly call from Python
router functions implemented in C.

The C library also injects the following C extension types to this Python module:
- LogAdapter: Logs to the C logging system.
- IoAdapter: Receives messages from the router into python.
(You will see empty stubs in this file but for full implementations and extension injection code see python_embedded.c.
The logging constants e.g LOG_TRACE, LOG_DEBUG etc. are registered in `qd_python_setup`)


This module also prevents the proton python module from being accidentally loaded.
For Python unit-testing, this module is replaced by tests/mock/dispatch.py to break
the dependency on the C.
"""
import builtins
import os
import sys
from ctypes import c_char_p, c_long, py_object, c_void_p, c_bool, PyDLL
from types import ModuleType

from typing import List, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .router.message import Message


FORBIDDEN: List[str]

LOG_TRACE: int
LOG_DEBUG: int
LOG_INFO: int
LOG_NOTICE: int
LOG_WARNING: int
LOG_ERROR: int
LOG_CRITICAL: int
LOG_STACK_LIMIT: int

TREATMENT_MULTICAST_FLOOD: int
TREATMENT_MULTICAST_ONCE: int
TREATMENT_ANYCAST_CLOSEST: int
TREATMENT_ANYCAST_BALANCED: int
TREATMENT_LINK_BALANCED: int


class LogAdapter:
    def __init__(self, mod_name):
        ...

    def log(self, level, text, *args):
        ...


class IoAdapter:
    def __init__(self, handler: Callable, address: str, aclass: str, treatment: int) -> None:
        ...

    def send(self, message: 'Message', no_echo: bool = True, control: bool = False) -> None:
        ...


class CError(Exception):
    """Exception raised if there is an error in a C call"""
    pass


class QdDll(PyDLL):
    """
    Load the library, set up function prototypes.

    NOTE: We use the python calling convention because the C library
    internally makes python calls.
    """

    def __init__(self) -> None:
        # `dlopen(NULL, ...)` opens the current executable; the router used to dlopen libqpid-dispatch.so before
        super().__init__(name=None, mode=os.RTLD_LAZY | os.RTLD_NOLOAD)

        # Types
        self.qd_dispatch_p = c_void_p

        # No check on qd_error_* functions, it would be recursive
        self._prototype(self.qd_error_code, c_long, [], check=False)
        self._prototype(self.qd_error_message, c_char_p, [], check=False)
        self._prototype(self.qd_log_entity, c_long, [py_object])
        self._prototype(self.qd_dispatch_configure_router, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_prepare, None, [self.qd_dispatch_p])
        self._prototype(self.qd_dispatch_configure_listener, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_connector, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_ssl_profile, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_tcp_listener, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_tcp_connector, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_http_listener, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_http_connector, c_void_p, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_delete_tcp_listener, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_dispatch_delete_tcp_connector, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_dispatch_delete_http_listener, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_dispatch_delete_http_connector, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_connection_manager_delete_listener, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_connection_manager_delete_connector, None, [self.qd_dispatch_p, c_void_p])
        self._prototype(self.qd_connection_manager_delete_ssl_profile, c_bool, [self.qd_dispatch_p, c_void_p])

        self._prototype(self.qd_dispatch_configure_address, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_auto_link, None, [self.qd_dispatch_p, py_object])

        self._prototype(self.qd_dispatch_configure_policy, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_register_policy_manager, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_policy_c_counts_alloc, py_object, [], check=False)
        self._prototype(self.qd_dispatch_policy_c_counts_refresh, None, [py_object, py_object])
        self._prototype(self.qd_dispatch_policy_host_pattern_add, c_bool, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_policy_host_pattern_remove, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_policy_host_pattern_lookup, c_char_p, [self.qd_dispatch_p, py_object])

        self._prototype(self.qd_dispatch_register_display_name_service, None, [self.qd_dispatch_p, py_object])

        self._prototype(self.qd_dispatch_set_agent, None, [self.qd_dispatch_p, py_object])

        self._prototype(self.qd_router_setup_late, None, [self.qd_dispatch_p])

        self._prototype(self.qd_dispatch_router_lock, None, [self.qd_dispatch_p])
        self._prototype(self.qd_dispatch_router_unlock, None, [self.qd_dispatch_p])

        self._prototype(self.qd_connection_manager_start, None, [self.qd_dispatch_p])
        self._prototype(self.qd_entity_refresh_begin, c_long, [py_object])
        self._prototype(self.qd_entity_refresh_end, None, [])

        self._prototype(self.qd_log_recent_py, py_object, [c_long])

    def _prototype(self, f, restype, argtypes, check=True):
        """Set up the return and argument types and the error checker for a
        ctypes function"""

        def _do_check(result, func, args):
            if check and self.qd_error_code():
                raise CError(self.qd_error_message())
            if restype is c_char_p and result:
                # in python3 c_char_p returns a byte type for the error
                # message. We need to convert that to a string
                result = result.decode('utf-8')
            return result

        f.restype = restype
        f.argtypes = argtypes
        f.errcheck = _do_check
        return f

    def function(self, fname, restype, argtypes, check=True):
        return self._prototype(getattr(self, fname), restype, argtypes, check)


# Prevent accidental loading of the proton python module inside dispatch.
# The proton-C library is linked with the dispatch C library, loading the proton
# python module loads a second copy of the library and mayhem ensues.
#
# Note the FORBIDDEN list is over-written to disable this tests in mock python
# testing code.
FORBIDDEN = ["proton"]


def check_forbidden() -> None:
    bad = set(FORBIDDEN) & set(sys.modules)
    if bad:
        raise ImportError("Forbidden modules loaded: '%s'." % "', '".join(bad))


def import_check(name: str, *args, **kw) -> ModuleType:
    if name in FORBIDDEN:
        raise ImportError("Python code running inside a dispatch router cannot import '%s', use the 'dispatch' module for internal messaging" % name)
    return builtin_import(name, *args, **kw)


check_forbidden()

builtin_import = builtins.__import__
builtins.__import__ = import_check

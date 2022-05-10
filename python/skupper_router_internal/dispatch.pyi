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

"""Type stubs for objects implemented in the C extension module.

ISSUE #452:
Given the way C and Python is interleaved, it is best to put all type
annotations into a separate .pyi file, even though Python 3 allows
type-annotating the Python part of the code directly.
"""

import ctypes
from typing import List, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .router.message import Message


class QdDll(ctypes.PyDLL):
    def __init__(self) -> None:
        ...

    def _prototype(self, f, restype, argtypes, check: bool = True):
        ...

    def function(self, fname: str, restype, argtypes, check: bool = True):
        ...


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
    def __init__(self, mod_name: str) -> None:
        ...

    def log(self, level: int, text: str, *args) -> None:
        ...


class IoAdapter:
    def __init__(self, handler: Callable, address: str, aclass: str, treatment: int) -> None:
        ...

    def send(self, message: 'Message', no_echo: int, control: int) -> None:
        ...

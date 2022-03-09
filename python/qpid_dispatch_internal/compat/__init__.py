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

"""Compatibility hacks for older versions of python"""

__all__ = [
    "dictify",
    "UNICODE",
    "BINARY"
]

from collections import OrderedDict
from typing import Any, Dict, Union


def dictify(od: Union[OrderedDict, Any]) -> Dict[Any, Any]:
    """Recursively replace OrderedDict with dict"""
    if isinstance(od, OrderedDict):
        return dict((k, dictify(v)) for k, v in od.items())
    else:
        return od


def BINARY(s: Union[str, bytes]) -> bytes:
    if isinstance(s, str):
        return s.encode("utf-8")
    elif isinstance(s, bytes):
        return s
    else:
        raise TypeError("%s cannot be converted to binary" % type(s))


def UNICODE(s: Union[str, bytes]) -> str:
    if isinstance(s, bytes):
        return s.decode("utf-8")
    elif isinstance(s, str):
        return s
    else:
        return str(s)

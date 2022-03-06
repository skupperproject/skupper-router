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
import io
import sys
import unittest
from typing import List, Tuple, TextIO, Iterable, Union

__all__ = [
    'parse_last_tests_failed_log',
]


def parse_last_tests_failed_log(lines: Union[TextIO, Iterable[str]]) -> List[Tuple[str, str]]:
    """CTest creates a `Testing/LastTestsFailed.log` containing a list of failed tests."""
    tests = []
    for line in lines:
        fields = line.rstrip().split(':', maxsplit=1)
        if len(fields) != 2:
            break
        number, name = fields
        tests.append((number, name))
    return tests


class GHAToolsSelfTests(unittest.TestCase):
    def test_parse_last_tests_failed_log(self):
        with self.subTest("empty file"):
            file_content = self._fake_file("\n")
            self.assertSequenceEqual(parse_last_tests_failed_log(file_content), [])

        with self.subTest("one test"):
            file_content = self._fake_file("58:system_tests_http2\n")
            self.assertSequenceEqual(parse_last_tests_failed_log(file_content), [('58', 'system_tests_http2')])

        with self.subTest("two tests"):
            file_content = self._fake_file("58:system_tests_http2\n60:system_tests_http1_adaptor\n")
            self.assertSequenceEqual(parse_last_tests_failed_log(file_content),
                                     [('58', 'system_tests_http2'), ('60', 'system_tests_http1_adaptor')])

    def _fake_file(self, content: str) -> Iterable[str]:
        f = io.StringIO()
        f.write(content)
        f.seek(0)
        return f


if __name__ == '__main__':
    sys.exit(unittest.main())

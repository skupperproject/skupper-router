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

# Test the router panic handler

import os
import re

from system_test import TestCase, unittest, main_module, Process
from system_test import retry, Qdrouterd, QdManager, HTTP_LISTENER_TYPE


@unittest.skipIf(os.environ.get("QPID_RUNTIME_CHECK", None) != "OFF",
                 "Skipping panic test: RUNTIME_CHECK enabled")
class PanicHandlerTest(TestCase):
    """
    Force the router to crash and verify the panic handler ran
    """

    @classmethod
    def setUpClass(cls):
        super(PanicHandlerTest, cls).setUpClass()

        config = [
            ('router', {'mode': 'interior', 'id': "PanicRouter"}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        # turn off python debug for this test to prevent it from dumping stuff
        # to output
        os.environ.pop("PYTHONMALLOC", None)
        os.environ.pop("PYTHONDEVMODE", None)
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd("PanicRouter", config, wait=True,
                                          cl_args=["-T"], expect=Process.EXIT_FAIL)

    def test_01_panic_handler(self):
        """
        Crash the router and verify the panic handler writes the crash to
        stderr
        """
        self.router.wait_startup_message()

        mgmt = QdManager(address=self.router.addresses[0], timeout=1)
        # this call will crash the router
        try:
            mgmt.create(HTTP_LISTENER_TYPE,
                        {'address': 'closest/panic',
                         'port': self.tester.get_port(),
                         'protocolVersion': 'HTTP1',
                         'host': '$FORCE$CRASH$SEGV$'})
            self.assertTrue(False, 'mgmt.create() should have crashed')
        except Exception as exc:
            pass  # expected - the router just crashed

        self.assertTrue(retry(lambda: self.router.poll() is not None))

        # do some simple validation on the generated panic output:

        crash_dump = ""
        with open(self.router.outfile_path, 'r') as fd:
            crash_dump = fd.read()

        regex = re.compile(r"^\*\*\* SKUPPER-ROUTER FATAL ERROR \*\*\*", re.MULTILINE)
        match = regex.search(crash_dump)
        self.assertIsNotNone(match, "failed to match banner")

        regex = re.compile(r"^Version: (\S+)$", re.MULTILINE)
        match = regex.search(crash_dump)
        self.assertIsNotNone(match, "failed to find Version:")

        regex = re.compile(r"^Signal: (\d+) (\S+)$", re.MULTILINE)
        match = regex.search(crash_dump)
        self.assertIsNotNone(match, "failed to find Signal:")

        regex = re.compile(r"^Process ID: (\d+) (\S+)$", re.MULTILINE)
        match = regex.search(crash_dump)
        self.assertIsNotNone(match, "failed to find Process ID:")

        regex = re.compile(r"^!!! libunwind not present: backtrace unavailable !!!$",
                           re.MULTILINE)
        if regex.search(crash_dump) is None:
            # libunwind present, expect at least one stack frame:
            regex = re.compile(r"^\[(\d+)\] IP: ", re.MULTILINE)
            self.assertRegex(crash_dump, regex, "No stack frames found!!!")

        regex = re.compile(r"^\*\*\* END \*\*\*$", re.MULTILINE)
        match = regex.search(crash_dump)
        self.assertIsNotNone(match, "failed to dump end line")


if __name__ == '__main__':
    unittest.main(main_module())

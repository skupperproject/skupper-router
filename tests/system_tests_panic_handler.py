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
from system_test import retry, Qdrouterd, AsyncTestSender

from proton import Message


@unittest.skipIf(os.environ.get("QPID_RUNTIME_CHECK", None) != "OFF",
                 "Skipping panic test: RUNTIME_CHECK enabled")
class PanicHandlerTest(TestCase):
    """
    Force the router to crash and verify the panic handler produces a dump to
    stdout.
    """
    @classmethod
    def setUpClass(cls):
        super(PanicHandlerTest, cls).setUpClass()

    def _start_router(self, name):
        # create and start a router
        config = [
            ('router', {'mode': 'standalone', 'id': name}),
            ('listener', {'role': 'normal',
                          'port': self.tester.get_port()}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        # turn off python and heap debug for this router so we can crash it
        os.environ.pop("PYTHONMALLOC", None)
        os.environ.pop("PYTHONDEVMODE", None)
        os.environ.pop("MALLOC_CHECK_", None)

        config = Qdrouterd.Config(config)
        router = self.tester.qdrouterd(name, config, wait=True,
                                       cl_args=["-T"], expect=Process.EXIT_FAIL)
        router.wait_startup_message()
        return router

    def _validate_panic(self, router):
        # do some simple validation on the generated panic output:
        crash_dump = ""
        with open(router.outfile_path, 'r') as fd:
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

    def test_01_abort_handling(self):
        """
        Crash the router via having it call abort() and verify the panic
        handler output
        """
        router = self._start_router("AbortRouter")
        ts = AsyncTestSender(router.addresses[0],
                             "io.skupper.router.router/test/crash",
                             message=Message(subject="ABORT"),
                             presettle=True)
        ts.wait()

        self.assertTrue(retry(lambda: router.poll() is not None))
        self._validate_panic(router)

    def test_02_segv_handling(self):
        """
        Crash the router via having it attempt to write to invalid memory and
        verify the panic handler output
        """
        router = self._start_router("HeapRouter")
        ts = AsyncTestSender(router.addresses[0],
                             "io.skupper.router.router/test/crash",
                             message=Message(subject="SEGV"),
                             presettle=True)
        ts.wait()

        self.assertTrue(retry(lambda: router.poll() is not None))
        self._validate_panic(router)

    def test_03_heap_handling(self):
        """
        Crash the router via having it attempt to overwrite heap memory and
        verify the panic handler output
        """
        router = self._start_router("SegvRouter")
        ts = AsyncTestSender(router.addresses[0],
                             "io.skupper.router.router/test/crash",
                             message=Message(subject="HEAP"),
                             presettle=True)
        ts.wait()

        self.assertTrue(retry(lambda: router.poll() is not None))
        self._validate_panic(router)


if __name__ == '__main__':
    unittest.main(main_module())

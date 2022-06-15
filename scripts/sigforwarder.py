#!/usr/bin/env python3

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

"""
Sigforwarder catches signals and forwards them to its grandchildren processes

To use sigforwarder, configure CMake with

    -DQDROUTERD_RUNNER="/absolute/path/to/sigforwarder.py rr record --print-trace-dir=1"

The parameter to rr causes it to print the trace name for each given router to ctest log.
Same output can be found in files in the build directory, such as `build/tests/system_test.dir/system_tests_autolinks/AutoLinkRetryTest/setUpClass/A-2.out`

```
$ ctest -I 15,15 -VV
[...]
15: Router A output file:
15: >>>>
15: /home/jdanek/.local/share/rr/skrouterd-22
15:
15: <<<<
```

## Motivation

The router tests offer the `-DQDROUTERD_RUNNER` CMake option.
This allows interposing any program of our choice between the Python test process and the skrouterd instances running under test.
For example, we may use the rr (record-reply debugger from Mozilla) as runner and record router execution this way.

The tests send SIGTERM to the immediate child when it is time to stop the router.
This is problematic with rr, because in response to SIGTERM, rr quits itself and does not record shutdown of its child (skrouterd).

Therefore, add sigforwarder on top, so the process tree looks like this

```
      Python (test process)
         |
    sigforwarder (this program)
         |
      rr record
         |
     skrouterd (the Software Under Test)
```

Now, sigforwarder intercepts the SIGTERM and delivers it to skrouterd (actually, to all grandchildren processes we might have).

Note that rr option `--continue-through-signal` does something different than sigforwarder and it does not help here.
"""

import ctypes
import ctypes.util
import logging
import os
import signal
import subprocess
import sys
import unittest
from typing import List, Optional

import psutil

# logging.basicConfig(level=logging.DEBUG)  # pytest with `-p no:logging -s`
libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

"""Only one child process; we are single-threaded, no need to protect it"""
P: Optional[subprocess.Popen] = None


def pre_exec() -> int:
    """This will be run in the child before doing exec syscall; to be extra sure
    we don't leave any zombies around. Because Linux has this feature."""

    PR_SET_PDEATHSIG = 1
    ret = libc.prctl(PR_SET_PDEATHSIG, signal.SIGHUP)
    if ret != 0:
        raise OSError("Failed to run prctl: " + os.strerror(ctypes.get_errno()))
    return ret


def handle_signal(sig_number: int, stackframe):
    """Forward the signal we got to the grandchildren"""
    del stackframe  # unused
    logging.debug(f"Sigforwarder got signal: {sig_number}")

    if not P:
        return
    logging.debug("We have a child, forwarding signal to all our grandchildren")

    for prc in psutil.process_iter(attrs=('pid', 'ppid')):
        if prc.ppid() == P.pid:
            logging.debug(f"Grandchild pid: {prc.pid}, sending signal: {sig_number} to it")
            os.kill(prc.pid, sig_number)


def sigforwarder(program: str, program_args: List[str]) -> int:
    global P

    P = subprocess.Popen(
        args=(program, *program_args),
        preexec_fn=pre_exec
    )

    # first start the child, then install signal handler, so that we cannot
    # be asked to handle a signal when child is not yet present
    for s in (signal.SIGHUP, signal.SIGQUIT, signal.SIGTERM, signal.SIGINT):
        signal.signal(s, handle_signal)

    # rr will propagate exit code from skrouterd, so we only need to propagate from rr
    logging.debug(f"Sigforwarder running {program}, waiting for status")
    return P.wait()


class SigforwarderTests(unittest.TestCase):
    def test_preexec_fn(self):
        res = pre_exec()
        self.assertEqual(res, 0)

    def test_run_child_process_propagate_exit_code(self):
        res = sigforwarder("/bin/sh", ["-c", "exit 42"])
        self.assertEqual(res, 42)

    def test_run_child_process_parent_kill_is_ignored(self):
        res = sigforwarder("/usr/bin/bash", ["-c", r"""
        kill -SIGTERM $PPID
        """])
        self.assertEqual(res, 0)

    def test_run_child_process_grandchild_gets_the_kill(self):
        # be careful to not leave `sleep infinity` behind,
        # http://mywiki.wooledge.org/SignalTrap#When_is_the_signal_handled.3F
        res = sigforwarder("/usr/bin/bash", ["-c", r"""
            trap "echo 'WRONG: child has trapped'; exit 11" TERM

            bash -c $'pid=baf; trap \'[[ -v pid ]] && kill $pid; exit 22\' TERM; sleep infinity & pid=$!; wait $pid' &
            grandchild=$!

            kill -SIGTERM $PPID
            wait $grandchild
            exit $?
            """])
        self.assertEqual(res, 22)


def main():
    if len(sys.argv) < 2:
        RuntimeError("At least one argument is required")

    argv0, program, *program_args = sys.argv

    code = sigforwarder(program, program_args)
    sys.exit(code)


if __name__ == '__main__':
    main()

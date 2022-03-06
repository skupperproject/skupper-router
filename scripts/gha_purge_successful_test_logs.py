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
import argparse
import logging
import pathlib
import shutil
import sys

import gha_tools

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Deletes system_tests.dir/* directories of passed tests.')
parser.add_argument(
    '--no-dry-run', dest='dry_run', action='store_false', default=True, help='actually delete the directories')
parser.add_argument('--build-dir', type=str, help='path to the CMake build directory')

args = parser.parse_args()


def main() -> int:
    build_dir = pathlib.Path(args.build_dir)
    if not build_dir.exists() or not build_dir.is_dir():
        logging.error(f"Build dir does not exist or is not a directory")
        return 1

    with pathlib.Path(build_dir, 'Testing', 'Temporary', 'LastTestsFailed.log').open('rt') as f:
        failed_tests = gha_tools.parse_last_tests_failed_log(f)

    failed_test_names = {test[1] for test in failed_tests}
    logging.debug("Failed tests: %s", failed_test_names)

    system_tests_dir = pathlib.Path(build_dir, 'tests', 'system_test.dir')
    if not system_tests_dir.exists():
        logging.info("system_test.dir does not exits, nothing to do")
        return 0

    for test_dir in system_tests_dir.iterdir():
        if test_dir.name not in failed_test_names:
            remove_directory(test_dir)

    return 0


def remove_directory(test_dir: pathlib.Path):
    if args.dry_run:
        logging.info("(dry run) deleting %s", test_dir)
        return

    shutil.rmtree(test_dir)
    logging.info("deleted %s", test_dir)


if __name__ == '__main__':
    sys.exit(main())

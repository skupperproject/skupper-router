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

# Uses the python expandvars to replace environment variables with its corresponding values in the file provided.
# Malformed variable names and references to non-existing environment variables are left unchanged.

from __future__ import print_function
import sys
import os


def expand_file(input_file, output_file):
    """
    Calls os.path.expandvars on each line in the input file and writes
    the expanded output to the output_file
    If no output_file is provided, the input file will be overwritten with the
    expanded output.
    :param input_file:
    :param output_file:
    """
    out_list = []
    with open(input_file) as f:
        for line in f:
            if line.startswith("#") or '$' not in line:
                out_list.append(line)
            else:
                out_list.append(os.path.expandvars(line))

    output_file = output_file if output_file else input_file
    with open(output_file, 'w') as f:
        for out in out_list:
            f.write(out)


if __name__ == '__main__':
    try:
        input_filename = sys.argv[1]
        output_filename = None
        if len(sys.argv) > 2:
            output_filename = sys.argv[2]
        is_file = os.path.isfile(input_filename)
        if not is_file:
            raise Exception()
    except Exception as e:
        print("Usage: python3 expandvars.py <absolute_input_file_path> <absolute_output_file_path>. "
              "Example - python3 expandvars.py /tmp/skrouterd-in.conf "
              "/tmp/skrouterd.conf; <absolute_output_file_path> is optional, if not provided, output file will be same"
              "as input file")
        # Unix programs generally use 2 for command line syntax errors
        sys.exit(2)
    expand_file(input_filename, output_filename)

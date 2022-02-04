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

try:
    filename = sys.argv[1]
    is_file = os.path.isfile(filename)
    if not is_file:
        raise Exception()
except Exception as e:
    print("Usage: python3 expandvars.py <absolute_file_path>. Example - python3 expandvars.py /tmp/qdrouterd.conf")
    # Unix programs generally use 2 for command line syntax errors
    sys.exit(2)

out_list = []
with open(filename) as f:
    for line in f:
        if line.startswith("#") or '$' not in line:
            out_list.append(line)
        else:
            out_list.append(os.path.expandvars(line))

with open(filename, 'w') as f:
    for out in out_list:
        f.write(out)

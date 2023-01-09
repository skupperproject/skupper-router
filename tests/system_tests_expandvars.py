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
import os
import sys
import subprocess
from subprocess import STDOUT, CalledProcessError
import shutil
from system_test import TestCase

env_config = {
    'PORT': '12345',
    'NAME': 'YoYoMa'
}

current_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
path_parent = os.path.dirname(os.path.join(os.path.dirname(os.path.abspath(__file__))))
expandvars_script_folder = os.path.join(path_parent, 'scripts')
script_file = "expandvars.py"
env = dict(os.environ)
env.update(env_config)


class ExpandvarsTest(TestCase):
    def test_expandvars_input_file_output_file(self):
        """
        Provide an input file and an output file to the expandvars.py script
        and make sure the output file is populated with the expanded vars.
        """
        nginx_conf_dir = os.path.join(current_dir, 'nginx-configs')
        input_file = os.path.join(nginx_conf_dir, 'expandvars.conf')
        output_file = os.path.join(self.directory, 'expandvars.conf')
        args = [sys.executable, os.path.join(expandvars_script_folder, script_file), input_file, output_file]
        subprocess.run(args, stderr=STDOUT, check=True, env=env)
        port_expanded = False
        name_expanded = False
        with open(output_file) as ofile:
            for test_line in ofile:
                test_line = test_line.strip()
                if "PORT=12345" == test_line:
                    port_expanded = True
                if "NAME=YoYoMa" == test_line:
                    name_expanded = True
        self.assertTrue(port_expanded and name_expanded)

    def test_expandvars_input_file_without_output_file(self):
        """
        Provide an input file and NO output file to the expandvars.py script
        and make sure the input file is overwritten/populated with the expanded vars.
        """
        nginx_conf_dir = os.path.join(current_dir, 'nginx-configs')
        orig_file = os.path.join(nginx_conf_dir, 'expandvars.conf')

        # Make a copy of tests/nginx-configs/expandvars.conf, we don't want to modify that file.
        input_file = os.path.join(self.directory, 'expandvars.in.conf')
        shutil.copyfile(orig_file, input_file)
        args = [sys.executable, os.path.join(expandvars_script_folder, script_file), input_file]
        subprocess.run(args, stderr=STDOUT, check=True, env=env)
        port_expanded = False
        name_expanded = False
        with open(input_file) as ofile:
            for test_line in ofile:
                test_line = test_line.strip()
                if "PORT=12345" == test_line:
                    port_expanded = True
                if "NAME=YoYoMa" == test_line:
                    name_expanded = True
        self.assertTrue(port_expanded and name_expanded)

    def test_expandvars_no_input_file_no_output_file(self):
        """
        Does not pass any arguments to the expandvars.py
        and makes sure that the script fails.
        """
        args = [sys.executable, os.path.join(expandvars_script_folder, script_file)]
        with self.assertRaises(CalledProcessError) as cm:
            subprocess.run(args, stderr=STDOUT, check=True, env=env)
        self.assertIn("returned non-zero exit status 2", str(cm.exception))

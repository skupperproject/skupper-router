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

import argparse
import json
import posixpath


def main():
    parser = argparse.ArgumentParser(
        prog='gha_sarif_masher',
        description='Processes SARIF files from GCC so that GitHub Actions ingest them.',
        epilog='This complements @microsoft/sarif-multitool with additional transformations.')
    parser.add_argument('filename', type=argparse.FileType('rt'))
    parser.add_argument('--output', required=True, type=argparse.FileType('wt'))
    parser.add_argument('--basedir', required=True)

    args = parser.parse_args()
    filename = args.filename
    output = args.output
    basedir = args.basedir

    sarif = json.load(filename)

    relativize_urls(sarif, basedir=basedir)
    fill_in_missing_pysicalLocations(sarif)

    json.dump(sarif, fp=output)


def iterate_subtrees(o):
    """Walks a json object and yields each subtree of it in turn."""
    yield o
    if isinstance(o, dict):
        for _, v in o.items():
            yield from iterate_subtrees(v)
    elif isinstance(o, list):
        for i in o:
            yield from iterate_subtrees(i)


def relativize_urls(sarif, basedir):
    """GitHub SARIF importer expects file urls to be relative to the root of the repository."""
    for item in iterate_subtrees(sarif):
        if not isinstance(item, dict):
            continue
        if 'uri' in item:
            item['uri'] = posixpath.relpath(item['uri'], start=basedir)



def fill_in_missing_pysicalLocations(sarif):
    """GitHub SARIF importer requires physicalLocation to be present, otherwise
    the following error is raised.

    `Code Scanning could not process the submitted SARIF file: buildCodeFlows: expected physical location`
    """
    for item in iterate_subtrees(sarif):
        if not isinstance(item, dict):
            continue
        if 'location' in item and 'physicalLocation' not in item['location']:
            item['location']['physicalLocation'] = {
                'artifactLocation': {
                    'uri': 'gcc/associated/no/file'
                }
            }


if __name__ == '__main__':
    main()

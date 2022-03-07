#!/bin/bash

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

set -Eeuo pipefail

export POD_ID=${HOSTNAME##*-}
HOME_DIR=$1
OUTFILE=$2

function printConfig() {
    echo "---------------------------------------" && cat $OUTFILE && echo "---------------------------------------"
}

if [[ ${QDROUTERD_CONF:-} =~ .*\{.*\}.* ]]; then
    # env var contains inline config
    echo "$QDROUTERD_CONF" > $OUTFILE
elif [[ -n ${QDROUTERD_CONF:-} ]]; then
    # treat as path(s)
    IFS=':,' read -r -a array <<< "$QDROUTERD_CONF"
    > $OUTFILE
    for i in "${array[@]}"; do
        if [[ -d $i ]]; then
            # if directory, concatenate to output all .conf files
            # within it
            for f in $i/*.conf; do
                cat "$f" >> $OUTFILE
            done
        elif [[ -f $i ]]; then
            # if file concatenate that to the output
            cat "$i" >> $OUTFILE
        else
            echo "No such file or directory: $i"
        fi
    done
fi

if [ -f $OUTFILE ]; then
    python3 $HOME_DIR/bin/expandvars.py $OUTFILE
fi

if [ -n "${QDROUTERD_AUTO_MESH_DISCOVERY:-}" ]; then
    python3 $HOME_DIR/bin/auto_mesh.py $OUTFILE || printConfig
fi

if [ -n "${QDROUTERD_AUTO_CREATE_SASLDB_SOURCE:-}" ]; then
    $HOME_DIR/bin/create_sasldb.sh ${QDROUTERD_AUTO_CREATE_SASLDB_PATH:-$HOME_DIR/etc/qdrouterd.sasldb} $QDROUTERD_AUTO_CREATE_SASLDB_SOURCE "${APPLICATION_NAME:-qdrouterd}"
fi

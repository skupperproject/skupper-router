#!/bin/sh

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

export HOSTNAME_IP_ADDRESS=$(hostname -i)

EXT=${QDROUTERD_CONF_TYPE:-conf}
CONFIG_FILE=/tmp/qdrouterd.${EXT}

${QDROUTERD_HOME}/bin/configure.sh ${QDROUTERD_HOME} $CONFIG_FILE

if [ -f $CONFIG_FILE ]; then
    ARGS="-c $CONFIG_FILE"
fi

if [[ $QDROUTERD_DEBUG = "gdb" ]]; then
    exec gdb -batch -ex "run" -ex "bt" --args qdrouterd $ARGS
elif [[ $QDROUTERD_DEBUG = "valgrind" ]]; then
    exec valgrind qdrouterd $ARGS
else
    exec qdrouterd $ARGS
fi

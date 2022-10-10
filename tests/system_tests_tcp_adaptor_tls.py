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
from system_tests_tcp_adaptor import TcpAdaptorBase, CommonTcpTests, ncat_available


class TcpTlsAdaptor(TcpAdaptorBase, CommonTcpTests):
    @classmethod
    def setUpClass(cls):
        super(TcpTlsAdaptor, cls).setUpClass(test_ssl=True)

    def test_authenticate_peer(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        name = "test_authenticate_peer"
        self.logger.log("TCP_TEST TLS Start %s" % name)
        # Now, run ncat with a client cert and this time it should pass.
        self.ncat_runner(name, client="INTA",
                         server="INTA",
                         logger=self.logger,
                         ncat_port=self.authenticate_peer_port,
                         use_ssl=True,
                         use_client_cert=True)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

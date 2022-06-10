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
from proton import VERSION
from system_tests_tcp_adaptor import TcpAdaptor, ncat_available


def check_proton_38():
    if VERSION >= (0, 38, 0):
        return True
    return False


class TcpTlsAdaptor(TcpAdaptor):
    @classmethod
    def setUpClass(cls):
        super(TcpTlsAdaptor, cls).setUpClass(test_ssl=True)

    def test_authenticate_peer(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        if not check_proton_38():
            self.skipTest("Proton version > 0.37.0 needed to run authenticate peer tests, see PROTON-2535")
        name = "test_authenticate_peer"
        self.logger.log("TCP_TEST TLS Start %s" % name)

        # Run ncat without client cert. This should fail because the router expects a client cert (authenticatePeer:yes)
        error_log = "peer did not return a certificate"
        try:
            self.ncat_runner(name, client="INTA",
                             server="INTA",
                             logger=self.logger,
                             ncat_port=self.authenticate_peer_port,
                             use_ssl=True,
                             use_client_cert=False)
        except Exception as e:
            pass

        self.INTA.wait_log_message(error_log)

        # Now, run ncat with a client cert and this time it should pass.
        self.ncat_runner(name, client="INTA",
                         server="INTA",
                         logger=self.logger,
                         ncat_port=self.authenticate_peer_port,
                         use_ssl=True,
                         use_client_cert=True)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def test_wrong_file_on_ssl_profile(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")

        name = "test_wrong_path_on_ssl_profile"

        self.logger.log("TCP_TEST TLS Start %s" % name)

        # Run ncat on a port which is associated with an sslProfile that has a bad cert filename.
        # The router should try to start TLS and then fail because of the bad certfile and immediately close
        # the client connection.
        error_log = "unable to set tls trusted certificates"
        try:
            self.ncat_runner(name, client="INTA",
                             server="INTA",
                             logger=self.logger,
                             ncat_port=self.wrong_path_in_ssl_profile_port,
                             use_ssl=True,
                             use_client_cert=False)
        except Exception as e:
            print(e)
            pass

        self.INTA.wait_log_message(error_log)

        self.logger.log("TCP_TEST TLS Stop %s SUCCESS" % name)

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
from system_test import TestCase, Qdrouterd
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
        # Now, run ncat with a client cert and it should pass.
        self.ncat_runner(name,
                         client="INTA",
                         server="INTA",
                         logger=self.logger,
                         ncat_port=self.authenticate_peer_port,
                         use_ssl=True,
                         use_client_cert=True)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def test_authenticate_peer_fail(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        name = "test_authenticate_peer_fail"
        self.logger.log("TCP_TEST TLS Start %s" % name)
        # Now, run ncat without a client cert and it should fail.
        self.ncat_runner(name,
                         client="INTA",
                         server="INTA",
                         logger=self.logger,
                         ncat_port=self.authenticate_peer_port,
                         use_ssl=True,
                         use_client_cert=False,
                         error_ok=True)
        # Look for a log line that proves the peer did not return a certificate.
        self.INTA.wait_log_message("peer did not return a certificate")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def test_connector_bad_server_cert(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        name = "test_connector_bad_server_cert"
        self.logger.log("TCP_TEST TLS Start %s" % name)

        # The router presents a cert to the echo server but the echo server is
        # responding back with a different ca cert that the router cannot verify.
        # Hence the router side TLS fails certificate verification.
        self.ncat_runner(name,
                         client="INTA",
                         server="INTA",
                         logger=self.logger,
                         ncat_port=self.bad_server_port,
                         use_ssl=True,
                         use_client_cert=False,
                         error_ok=True)
        # Look for a log line that proves the certificate verification failure.
        self.INTA.wait_log_message("certificate verify failed")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def test_xxx_present_bad_cert_to_good_listener_good_connector(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        name = "test_xxx_present_bad_cert_to_good_listener_good_connector"
        self.logger.log("TCP_TEST TLS Start %s" % name)

        # Run the ncat command using a bad ca certificate to a tcpListener that is good
        # and a tcpConnector that is good. The router must reject the connection made by
        # ncat since it presents an unknown certificate.
        self.ncat_runner(name,
                         client="INTA",
                         server="INTC",
                         logger=self.logger,
                         ncat_port=self.good_server_port,
                         use_ssl=True,
                         use_client_cert=False,
                         use_bad_ca_cert=True,
                         error_ok=True)
        # Look for a log line that says "certificate unknown"
        self.INTA.wait_log_message("certificate unknown")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def test_yyy_present_bad_cert_to_good_listener_bad_connector(self):
        if not ncat_available():
            self.skipTest("Ncat utility is not available")
        name = "test_yyy_present_bad_cert_to_good_listener_bad_connector"
        self.logger.log("TCP_TEST TLS Start %s" % name)

        # Run the ncat command using a bad ca certificate to a tcpListener that is good
        # and a tcpConnector that is bad. This test is mostly similar to
        # test_xxx_present_bad_cert_to_good_listener_good_connector except that this test has a tcpConnector that has
        # a bad ca cert. The router must reject the connection made by
        # ncat since it presents an unknown certificate.
        self.ncat_runner(name,
                         client="INTA",
                         server="INTC",
                         logger=self.logger,
                         ncat_port=self.bad_server_port,
                         use_ssl=True,
                         use_client_cert=False,
                         use_bad_ca_cert=True,
                         error_ok=True)
        # Look for a log line that says "certificate unknown"
        self.INTA.wait_log_message("certificate unknown", num_occurrence=2)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)


class TcpTlsBadConfigTests(TestCase):
    """
    Negative test for invalid TCP connector and listener configurations
    """
    @classmethod
    def setUpClass(cls):
        super(TcpTlsBadConfigTests, cls).setUpClass()

        config = [
            ('router', {'mode': 'interior',
                        'id': 'BadTcpConfigRouter'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.router = cls.tester.qdrouterd('BadTcpConfigRouter',
                                          Qdrouterd.Config(config), wait=True)

    def test_connector_mgmt_missing_ssl_profile(self):
        """Attempt to create a connector with a bad sslProfile"""
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        self.assertRaises(Exception, mgmt.create, "tcpConnector",
                          {'address': 'foo',
                           'host': '127.0.0.1',
                           'port': port,
                           'sslProfile': "NotFound"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid tcpConnector configuration", mgmt.stdout)

    def test_connector_mgmt_missing_ca_file(self):
        """Attempt to create a connector with an invalid CA file"""
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        mgmt.create("sslProfile",
                    {'name': 'BadCAFile',
                     'caCertFile': '/bad/path/CA.pem'})
        self.assertRaises(Exception, mgmt.create, "tcpConnector",
                          {'address': 'foo',
                           'host': '127.0.0.1',
                           'port': port,
                           'sslProfile': "BadCAFile"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid tcpConnector configuration", mgmt.stdout)
        mgmt.delete("sslProfile", name='BadCAFile')

    def test_listener_mgmt_missing_ssl_profile(self):
        """Attempt to create a listener with a bad sslProfile"""
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        self.assertRaises(Exception, mgmt.create, "tcpListener",
                          {'address': 'foo',
                           'host': '0.0.0.0',
                           'port': port,
                           'sslProfile': "NotFound"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid tcpListener configuration", mgmt.stdout)

    def test_listener_mgmt_missing_ca_file(self):
        """Attempt to create a listener with an invalid CA file"""
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        mgmt.create("sslProfile",
                    {'name': 'BadCAFile',
                     'caCertFile': '/bad/path/CA.pem'})
        self.assertRaises(Exception, mgmt.create, "tcpListener",
                          {'address': 'foo',
                           'host': '0.0.0.0',
                           'port': port,
                           'sslProfile': "BadCAFile"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid tcpListener configuration", mgmt.stdout)
        mgmt.delete("sslProfile", name='BadCAFile')

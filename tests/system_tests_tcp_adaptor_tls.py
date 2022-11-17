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
from system_test import unittest, TestCase, Qdrouterd, NcatException, Logger, Process
from system_tests_tcp_adaptor import TcpAdaptorBase, CommonTcpTests, ncat_available, \
    CA_CERT, CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, \
    SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY_NO_PASS, BAD_CA_CERT
from TCP_echo_server import TcpEchoServer


class TcpTlsAdaptor(TcpAdaptorBase, CommonTcpTests):
    @classmethod
    def setUpClass(cls):
        super(TcpTlsAdaptor, cls).setUpClass(test_ssl=True)

    @unittest.skipIf(not ncat_available(), "Ncat utility is not available")
    def test_authenticate_peer(self):
        name = "test_authenticate_peer"
        self.logger.log("TCP_TEST TLS Start %s" % name)
        # Run ncat with a client cert against an authenticatePeer: yes port and it should pass.
        data = b'abcd'
        ssl_info = dict()
        ssl_info['CA_CERT'] = self.client_ssl_info.get('CA_CERT')
        ssl_info['CLIENT_CERTIFICATE'] = self.client_ssl_info.get('CLIENT_CERTIFICATE')
        ssl_info['CLIENT_PRIVATE_KEY'] = self.client_ssl_info.get('CLIENT_PRIVATE_KEY_NO_PASS')
        out, _ = self._ncat_runner(name=name,
                                   client="INTA",
                                   server="INTA",
                                   data=data,
                                   port=self.authenticate_peer_port,
                                   ssl_info=ssl_info)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available(), "Ncat utility is not available")
    def test_authenticate_peer_fail(self):
        name = "test_authenticate_peer_fail"
        self.logger.log("TCP_TEST TLS Start %s" % name)

        ssl_info = dict()
        ssl_info['CA_CERT'] = self.client_ssl_info.get('CA_CERT')
        # Now, run ncat without a client cert and it should fail.
        try:
            out, err = self._ncat_runner(name=name,
                                         client="INTA",
                                         server="INTA",
                                         port=self.authenticate_peer_port,
                                         ssl_info=ssl_info)
        except NcatException as e:
            # In some CI test runs, the NcatException is raised.
            # In other cases no exception is raised
            # So, assertRaises cannot be used.
            expected_error = "Ncat: Input/output error"
            actual_error = str(e)
            self.assertIn(expected_error, actual_error, f"Expected error message not found. "
                                                        f"Expected {expected_error} but got {actual_error}")

        # Look for a log line that proves the peer did not return a certificate.
        self.INTA.wait_log_message("peer did not return a certificate")
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


class TcpTlsGoodListenerBadClient(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TcpTlsGoodListenerBadClient, cls).setUpClass()
        cls.good_listener_port = cls.tester.get_port()
        cls.bad_server_port = cls.tester.get_port()

        cls.server_logger = Logger(title="TcpTlsGoodListenerBadClient",
                                   print_to_console=True,
                                   save_for_dump=False,
                                   ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                          "setUpClass/TcpAdaptor_echo_server_INTA.log"))
        server_prefix = "ECHO_SERVER_TcpTlsGoodListenerBadClient_INTA"
        cls.ssl_info = {'SERVER_CERTIFICATE': SERVER_CERTIFICATE,
                        'SERVER_PRIVATE_KEY': SERVER_PRIVATE_KEY_NO_PASS,
                        'CA_CERT': CA_CERT}
        cls.echo_server = TcpEchoServer(prefix=server_prefix,
                                        port=0,
                                        logger=cls.server_logger,
                                        ssl_info=cls.ssl_info)
        assert cls.echo_server.is_running

        config = [
            ('router', {'mode': 'interior', 'id': 'INTA'}),
            # Listener for handling router management requests.
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('tcpListener',
             {'name': "good-listener",
              'host': "localhost",
              'port': cls.good_listener_port,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'ES_GOOD_CONNECTOR_CERT_INTA',
              'siteId': "mySite"}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': CLIENT_PRIVATE_KEY_PASSWORD}),
            ('tcpConnector',
             {'name': "good-connector",
              'host': "localhost",
              'port': cls.echo_server.port,
              'address': 'ES_GOOD_CONNECTOR_CERT_INTA',
              'sslProfile': 'tcp-connector-ssl-profile',
              'siteId': "mySite"}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),

            # The following listener connector pair are associated
            # by address ES_BAD_CONNECTOR_CERT_INTA.
            ('tcpListener',
             {'name': "bad-cert-test",
              'host': "localhost",
              'port': cls.bad_server_port,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'ES_BAD_CONNECTOR_CERT_INTA',
              'siteId': "mySite"}),
            ('sslProfile', {'name': 'bad-server-cert-connector-ssl-profile',
                            'caCertFile': BAD_CA_CERT}),
            ('tcpConnector',
             {'name': "bad-connector",
              'host': "localhost",
              'port': cls.echo_server.port,
              'address': 'ES_BAD_CONNECTOR_CERT_INTA',
              'sslProfile': 'bad-server-cert-connector-ssl-profile',
              'siteId': "mySite"})
        ]

        cls.router = cls.tester.qdrouterd('BadTcpConfigRouter',
                                          Qdrouterd.Config(config), wait=True)
        cls.logger = Logger(title="TcpTlsGoodListenerBadClient-testClass",
                            print_to_console=False,
                            save_for_dump=False,
                            ofilename=os.path.join(os.path.dirname(os.getcwd()), "setUpClass/TcpAdaptor.log"))

    @unittest.skipIf(not ncat_available(), "Ncat utility is not available")
    def test_present_bad_cert_to_good_listener_good_connector(self):
        name = "test_present_bad_cert_to_good_listener_good_connector"
        full_name = "%s_%s_%s" % (name, "INTA", "INTA")
        self.logger.log("TCP_TEST TLS Start %s" % name)
        ssl_info = dict()
        ssl_info['CA_CERT'] = BAD_CA_CERT

        try:
            # Run the ncat command using a bad ca certificate to a tcpListener that is good
            # and a tcpConnector that is good. The router must reject the connection made by
            # ncat since it presents an unknown certificate/unknown ca.
            self.ncat(port=self.good_listener_port,
                      logger=self.logger,
                      name=full_name,
                      expect=Process.EXIT_FAIL,
                      ssl_info=ssl_info)
        except NcatException as e:
            # In some CI test runs, the NcatException is raised.
            # In other cases no exception is raised
            # This error happens in F36
            expected_error1 = "ncat failed: stdout='b''' stderr='b'Ncat: Input/output error"
            # This error happens in other operating sytems.
            expected_error2 = "ncat failed: stdout='b''' stderr='b'Ncat: Connection refused"
            actual_error = str(e)
            error_found = False
            if expected_error1 in actual_error or expected_error2 in actual_error:
                error_found = True
            self.assertTrue(error_found, f"Expected error message not found. "
                                         f"Expected {expected_error1} or  {expected_error2} but got {actual_error}")

        # Look for a log line that says "certificate unknown"
        self.router.wait_log_message("certificate unknown|unknown ca")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available(), "Ncat utility is not available")
    def test_connector_bad_server_cert(self):
        name = "test_connector_bad_server_cert"
        full_name = "%s_%s_%s" % (name, "INTA", "INTA")
        self.logger.log("TCP_TEST TLS Start %s" % name)
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        # The ncat client presents a good cert to the good listener.
        # When ncat makes a connection on the tcpListener, the corresponding
        # tcpConnector attempts to make a connection to the echo server
        # by presenting a bad cert to the echo server but the echo server is
        # responding back with a different ca cert that the router cannot verify.
        # Hence the router connector side TLS fails certificate verification.
        try:
            out, err = self.ncat(port=self.bad_server_port,
                                 logger=self.logger,
                                 name=full_name,
                                 ssl_info=ssl_info)
            self.assertEqual(len(out), 0)
        except NcatException as e:
            # In some CI test runs, the NcatException is raised.
            # In other cases no exception is raised
            # This error happens in F36
            expected_error1 = "ncat failed: stdout='b''' stderr='b'Ncat: Input/output error"
            # This error happens in other operating sytems.
            expected_error2 = "ncat failed: stdout='b''' stderr='b'Ncat: Connection refused"
            actual_error = str(e)
            error_found = False
            if expected_error1 in actual_error or expected_error2 in actual_error:
                error_found = True
            self.assertTrue(error_found, f"Expected error message not found. "
                                         f"Expected {expected_error1} or  {expected_error2} but got {actual_error}")

        self.router.wait_log_message("certificate verify failed")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

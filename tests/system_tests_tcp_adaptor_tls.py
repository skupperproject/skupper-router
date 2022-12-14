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
from system_test import unittest, TestCase, Qdrouterd, NcatException, Logger, Process, OpenSSLServer
from system_tests_tcp_adaptor import TcpAdaptorBase, CommonTcpTests, ncat_available, \
    CA_CERT, CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, \
    SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY_NO_PASS, BAD_CA_CERT
from http1_tests import wait_tcp_listeners_up
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


class TcpAdaptorOpenSSLTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorOpenSSLTests, cls).setUpClass()
        cls.openssl_server_listening_port_http11 = cls.tester.get_port()
        cls.openssl_server_listening_port_http2 = cls.tester.get_port()
        cls.openssl_server_listening_port_auth_peer = cls.tester.get_port()
        cls.openssl_server_listening_port_tlsv1_2 = cls.tester.get_port()
        cls.router_listener_port_http11 = cls.tester.get_port()
        cls.router_listener_port_http2 = cls.tester.get_port()
        cls.router_listener_port_auth_peer = cls.tester.get_port()
        cls.router_listener_port_tlsv12 = cls.tester.get_port()
        cls.router_listener_port_server_auth_peer = cls.tester.get_port()
        cls.router_listener_port_server_auth_peer_1 = cls.tester.get_port()
        cls.ssl_info = dict()
        cls.ssl_info['CA_CERT'] = CA_CERT
        cls.ssl_info['SERVER_CERTIFICATE'] = SERVER_CERTIFICATE
        cls.ssl_info['SERVER_PRIVATE_KEY'] = SERVER_PRIVATE_KEY
        cls.ssl_info['SERVER_PRIVATE_KEY_PASSWORD'] = SERVER_PRIVATE_KEY_PASSWORD

        # This openssl server accepts ALPN protocol http/1.1
        cls.openssl_server_alpn_http11 = OpenSSLServer(listening_port=cls.openssl_server_listening_port_http11,
                                                       ssl_info=cls.ssl_info,
                                                       name="OpenSSLServerhttp11",
                                                       cl_args=['-alpn', 'http/1.1'])

        # This openssl server accepts ALPN protocol http2 (h2)
        cls.openssl_server_alpn_http2 = OpenSSLServer(listening_port=cls.openssl_server_listening_port_http2,
                                                      ssl_info=cls.ssl_info,
                                                      name="OpenSSLServerhttp2",
                                                      cl_args=['-alpn', 'h2'])

        # This openssl server does TLSv1.2. The openssl client connecting to the router will also have to do
        # TLSv1.2.
        cls.openssl_server_tlsv1_2 = OpenSSLServer(listening_port=cls.openssl_server_listening_port_tlsv1_2,
                                                   ssl_info=cls.ssl_info,
                                                   name="OpenSSLServertlsv1_2",
                                                   cl_args=['-tls1_2'])

        # This openssl server requires that any client connecting to it show its client certificate.
        # If the client does not present a client cert, the openssl server will terminate the connection
        # with error -
        # 80FB44718D7F0000:error:0A0000C7:SSL routines:tls_process_client_certificate:peer did not return a certificate
        cls.openssl_server_auth_peer = OpenSSLServer(listening_port=cls.openssl_server_listening_port_auth_peer,
                                                     ssl_info=cls.ssl_info,
                                                     name="OpenSSLServerAuthPeer",
                                                     cl_args=['-Verify', '1'])

        inter_router_port = cls.tester.get_port()
        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            # Listener for handling router management requests.
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('tcpListener',
             {'name': "listener-for-opensslclient-http1",
              'host': "localhost",
              'port': cls.router_listener_port_http11,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'http11',
              'siteId': "mySite"
              }
             ),
            ('tcpListener',
             {'name': "listener-for-opensslclient-http2",
              'host': "localhost",
              'port': cls.router_listener_port_http2,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'http2',
              'siteId': "mySite"
              }
             ),
            ('tcpListener',
             {'name': "peer-authentication-required",
              'host': "localhost",
              'port': cls.router_listener_port_auth_peer,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'http2',
              'authenticatePeer': 'yes',
              'siteId': "mySite"
              }
             ),
            ('tcpListener',
             {'name': "tlsv12",
              'host': "localhost",
              'port': cls.router_listener_port_tlsv12,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'tlsv12',
              'siteId': "mySite"
              }
             ),
            ('tcpListener',
             {'name': "server-requires-client-cert",
              'host': "localhost",
              'port': cls.router_listener_port_server_auth_peer,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'server-requires-client-cert',
              'siteId': "mySite"
              }
             ),
            ('tcpListener',
             {'name': "server-requires-client-cert-1",
              'host': "localhost",
              'port': cls.router_listener_port_server_auth_peer_1,
              'sslProfile': 'tcp-listener-ssl-profile',
              'address': 'server-requires-client-cert-1',
              'siteId': "mySite"
              }
             ),
            ('listener', {'role': 'inter-router', 'port': inter_router_port})
        ])

        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            # Listener for handling router management requests.
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('connector', {'name': 'connectorToA', 'role': 'inter-router',
                           'port': inter_router_port,
                           'verifyHostname': 'no'}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': CLIENT_PRIVATE_KEY_PASSWORD}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile-no-client-cert',
                            'caCertFile': CA_CERT}),
            ('tcpConnector', {'port': cls.openssl_server_listening_port_http11,
                              'address': 'http11',
                              'host': 'localhost',
                              'name': 'http11-connector',
                              # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
                              'verifyHostname': 'yes',
                              'sslProfile': 'tcp-connector-ssl-profile'
                              }),
            ('tcpConnector', {'port': cls.openssl_server_listening_port_http2,
                              'address': 'http2',
                              'host': 'localhost',
                              'name': 'http2-connector',
                              # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
                              'verifyHostname': 'yes',
                              'sslProfile': 'tcp-connector-ssl-profile'
                              }),
            ('tcpConnector', {'port': cls.openssl_server_listening_port_tlsv1_2,
                              'address': 'tlsv12',
                              'host': 'localhost',
                              'name': 'tlsv12-connector',
                              # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
                              'verifyHostname': 'yes',
                              'sslProfile': 'tcp-connector-ssl-profile'
                              }),
            # This connector is connecting to an openssl server which requires a client auth but this connector
            # is not presenting a client cert, so there should be an error on the *openssl out file* which
            # resembles the following -
            # SSL routines:tls_process_client_certificate:peer did not return a certificate
            ('tcpConnector', {'port': cls.openssl_server_listening_port_auth_peer,
                              'address': 'server-requires-client-cert',
                              'host': 'localhost',
                              'name': 'server-requires-client-cert',
                              # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
                              'verifyHostname': 'yes',
                              'sslProfile': 'tcp-connector-ssl-profile-no-client-cert'
                              }),
            # This connector is connecting to an openssl server which requires a client auth and this connector
            # is presenting a client cert, so we should be all good.
            ('tcpConnector', {'port': cls.openssl_server_listening_port_auth_peer,
                              'address': 'server-requires-client-cert-1',
                              'host': 'localhost',
                              'name': 'server-requires-client-cert-1',
                              # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
                              'verifyHostname': 'yes',
                              'sslProfile': 'tcp-connector-ssl-profile'
                              }),
        ])
        cls.router_qdra = cls.tester.qdrouterd("tcp-two-router-tls-A", config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("tcp-two-router-tls-B", config_qdrb)
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        # Wait for every TCP listener to be active before we start running the battery
        # of openssl tests.
        wait_tcp_listeners_up(cls.router_qdra.addresses[0])

    def test_alpn_http11(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        # Openssl client is connecting the the router listening port and sending ALPN protocol as http/1.1.
        # The router should forward this ALPN protocol all the way to the openssl server via the router
        out, error = self.opensslclient(port=self.router_listener_port_http11,
                                        ssl_info=ssl_info,
                                        data=b"test_alpn_http11",
                                        cl_args=['-alpn', 'http/1.1'])
        self.assertTrue(b"Verification: OK" in out and b"Verify return code: 0 (ok)")
        # The openssl client has negotiated http/1.1 as the ALPN protocol with the router QDR.A.
        self.assertIn(b"ALPN protocol: http/1.1", out)
        # The openssl client is sending b"test_alpn_http11" to the QDR.A. This will travel from QDR.A to
        # QDR.B and then to the openssl server. We are checking the openssl server out file to make sure
        # that it is printing the data we sent it.
        # The data transfer from openssl client thru the router network to the openssl server will not
        # happen if there was an ALPN failure. This test will fail if you, say, set  cl_args=['-alpn', 'h2']
        self.openssl_server_alpn_http11.wait_out_message("test_alpn_http11")
        # The openssl server out file confirms that the ALPN protocol selected is indeed http/1.1
        self.openssl_server_alpn_http11.wait_out_message("ALPN protocols selected: http/1.1")

    def test_alpn_http2(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        out, error = self.opensslclient(port=self.router_listener_port_http2,
                                        ssl_info=ssl_info,
                                        data=b"test_alpn_http2",
                                        cl_args=['-alpn', 'h2'])
        self.assertTrue(b"Verification: OK" in out and b"Verify return code: 0 (ok)")
        self.assertIn(b"ALPN protocol: h2", out)
        # Check the outfile of the openssl server to make sure the data we sent the router reached it.
        self.openssl_server_alpn_http2.wait_out_message("test_alpn_http2")
        # The openssl server out file confirms that the ALPN protocol selected is indeed h2
        self.openssl_server_alpn_http2.wait_out_message("ALPN protocols selected: h2")

    def test_no_alpn(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        out, error = self.opensslclient(port=self.router_listener_port_http2,
                                        ssl_info=ssl_info,
                                        data=b"test_no_alpn")

        self.assertTrue(b"Verification: OK" in out and b"Verify return code: 0 (ok)")
        # We did not negotiate any ALPN, so the check the client output to see if it says so.
        self.assertIn(b"No ALPN negotiated", out)
        # Without ALPN, the data the client sent, still reached the server, which is good.
        self.openssl_server_alpn_http2.wait_out_message("test_no_alpn")

    #def test_auth_peer_fail(self):
    #    ssl_info = dict()
    #    ssl_info['CA_CERT'] = CA_CERT
    #    # Openssl client connects to a router port without presenting a client certificate.
    #    out, error = self.opensslclient(port=self.router_listener_port_auth_peer,
    #                                    ssl_info=ssl_info)
    #    print("out=", out)
    #    print("error=", error)
    #    # Currently, the router simply closes the connection and writes a log message to the router log
    #    # Ideally, the response received by the client from the router should contain this error in the
    #    # "error" string. This feature is being worked on now.
    #
    #    # Check the log file of router QDR.A to see if there is a log messages indicating that the
    #    # peer did not return a certificate.
    #    self.router_qdra.wait_log_message("peer did not return a certificate")

    def test_auth_peer_pass(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        ssl_info['CLIENT_CERTIFICATE'] = CLIENT_CERTIFICATE
        ssl_info['CLIENT_PRIVATE_KEY'] = CLIENT_PRIVATE_KEY
        ssl_info['CLIENT_PRIVATE_KEY_PASSWORD'] = CLIENT_PRIVATE_KEY_PASSWORD
        # Openssl client presents client cert to a router listener that is set to require client cert
        # and all is good.
        out, error = self.opensslclient(port=self.router_listener_port_auth_peer,
                                        ssl_info=ssl_info,
                                        data=b"test_auth_peer_pass",
                                        cl_args=['-alpn', 'h2'])
        self.assertTrue(b"Verification: OK" in out and b"Verify return code: 0 (ok)")
        self.assertIn(b"ALPN protocol: h2", out)
        self.openssl_server_alpn_http2.wait_out_message("test_auth_peer_pass")

    def test_tlsv12(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        out, error = self.opensslclient(port=self.router_listener_port_tlsv12,
                                        ssl_info=ssl_info,
                                        data=b"test_tlsv12",
                                        cl_args=['-tls1_2'])
        self.assertTrue(b"Verification: OK" in out and b"Verify return code: 0 (ok)")
        # The data that the openssl client sent to the router is seen in the openssl server logs which
        # means that TLSv1.2 worked.
        self.openssl_server_tlsv1_2.wait_out_message("test_tlsv12")

    def test_connector_requires_client_auth_fail(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        # The router QDR.B is *not* presenting a client certificate to the openssl server which is not
        # going to be happy.
        _, _ = self.opensslclient(port=self.router_listener_port_server_auth_peer,
                                  ssl_info=ssl_info,
                                  data=b"test_connector_requires_client_auth")
        # There will be an message on the openssl server out file that the peer did not return a certificate since
        # the corresponding connector sslProfile did not have a client cert set on it.
        self.openssl_server_auth_peer.wait_out_message("SSL routines:tls_process_client_certificate:peer "
                                                       "did not return a certificate")

    def test_connector_requires_client_auth_pass(self):
        ssl_info = dict()
        ssl_info['CA_CERT'] = CA_CERT
        # The router QDR.B is now presenting a client certificate to the openssl server
        # which is going to be happy.
        _, _ = self.opensslclient(port=self.router_listener_port_server_auth_peer_1,
                                  ssl_info=ssl_info,
                                  data=b"test_connector_requires_client_auth_pass")
        # The router QDR.B is presenting a client certificate to the openssl server.
        # The data the client is sending to the router should ultimately show up in
        # the openssl server out file.
        self.openssl_server_auth_peer.wait_out_message("test_connector_requires_client_auth_pass")


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

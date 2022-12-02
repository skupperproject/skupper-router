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
import unittest

from http1_tests import wait_http_listeners_up, HttpTlsBadConfigTestsBase, wait_tcp_listeners_up
from system_test import Qdrouterd, DIR
from system_tests_ssl import RouterTestSslBase

from proton import VERSION, SASL
from system_tests_sasl_plain import RouterTestPlainSaslCommon
from system_tests_http2 import skip_test, skip_h2_test, image_file
from system_tests_http2 import Http2TestTwoRouter, Http2TestBase, CommonHttp2Tests


def check_proton_38():
    if VERSION >= (0, 38, 0):
        return True
    return False


class Http2TestTlsStandaloneRouter(Http2TestBase, CommonHttp2Tests, RouterTestSslBase):
    """
    This test has one standalone router QDR. It has one httpListener with an associated SSL Profile and has one
    unencrypted httpConnector.
    Does not authenticate the curl client connecting to the httpListener i.e. the curl client does not present a
    client certificate.
    """
    @classmethod
    def setUpClass(cls, tls_v12=False):
        super(Http2TestTlsStandaloneRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-tls-standalone-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        cls.listener_ssl_profile = {'name': 'http-listener-ssl-profile',
                                    'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                    'certFile': cls.ssl_file('server-certificate.pem'),
                                    'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                                    'password': 'server-password'}
        if cls.tls_v12:
            cls.listener_ssl_profile['protocols'] = 'TLSv1.2'
        else:
            cls.listener_ssl_profile['protocols'] = 'TLSv1.3'

        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            # Only the listener side has the SSL Profile, the connector side does not.
            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': 'localhost', 'protocolVersion': 'HTTP2',
                              'sslProfile': 'http-listener-ssl-profile'}),
            ('httpConnector', cls.connector_props),
            ('sslProfile', cls.listener_ssl_profile)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

        if cls.tls_v12:
            cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM', '--tlsv1.2']
        else:
            cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM', '--tlsv1.3']


class Http2ATlsV12TestStandaloneRouter(Http2TestTlsStandaloneRouter, RouterTestSslBase):
    """
    This test has one standalone router QDR. It has one httpListener with an associated SSL Profile and has one
    unencrypted httpConnector.
    Does not authenticate the curl client connecting to the httpListener i.e. the curl client does not present a
    client certificate.
    Tests to make sure TLS 1.2 works.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2ATlsV12TestStandaloneRouter, cls).setUpClass(tls_v12=True)


class Http2TestTlsTwoRouter(Http2TestTwoRouter, RouterTestSslBase):
    """
    In this two router test, the listener is on Router QDR.A and the connector is on router
    QDR.B. Both the listener and the connector are encrypted. The server that QDR.B connects to
    is also encrypted.
    Client authentication is required for curl to talk to the router QDR.A listener http port.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestBase, cls).setUpClass()
        if skip_test():
            return

        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server_tls"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  wait=False,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'SERVER_TLS': "yes",
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port),
                                                      'SERVER_CERTIFICATE': cls.ssl_file('server-certificate.pem'),
                                                      'SERVER_PRIVATE_KEY': cls.ssl_file('server-private-key-no-pass.pem'),
                                                      'SERVER_CA_CERT': cls.ssl_file('ca-certificate.pem')
                                                  })
        inter_router_port = cls.tester.get_port()
        cls.listener_name = 'listenerToBeDeleted'
        cls.http_listener_props = {'port': cls.tester.get_port(),
                                   'address': 'examples',
                                   'host': 'localhost',
                                   'name': cls.listener_name,
                                   'protocolVersion': 'HTTP2',
                                   'authenticatePeer': 'yes',
                                   'sslProfile': 'http-listener-ssl-profile'}
        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            # curl will connect to this httpListener and run the tests.
            ('httpListener', cls.http_listener_props),
            ('sslProfile', {'name': 'http-listener-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'server-password'}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port})
        ])

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': 'localhost',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name,
            # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
            'verifyHostname': 'yes',
            'sslProfile': 'http-connector-ssl-profile'
        }
        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector', cls.connector_props),
            ('connector', {'name': 'connectorToA', 'role': 'inter-router',
                           'port': inter_router_port,
                           'verifyHostname': 'no'}),
            ('sslProfile', {'name': 'http-connector-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('client-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'client-password'}),
        ])

        cls.router_qdra = cls.tester.qdrouterd("http2-two-router-tls-A", config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("http2-two-router-tls-B", config_qdrb)
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        cls.router_qdrb.wait_http_server_connected(is_tls=True)
        wait_http_listeners_up(cls.router_qdra.addresses[0])

        # curl will use these additional args to connect to the router.
        cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM',
                         '--cert', cls.ssl_file('client-certificate.pem') + ":client-password",
                         '--key', cls.ssl_file('client-private-key.pem')]

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    # Tests the HTTP2 head request without http2-prior-knowledge
    def test_head_request_no_http2_prior_knowledge(self):
        # Run curl 127.0.0.1:port --head
        # In this test, we do not use curl's --http2-prior-knowledge flag. This means that curl client will first offer
        # http1 and http2 (h2) as the protocol list in ClientHello ALPN (since the curl client by default does
        # ALPN over TLS). The router will respond back with just h2 in its
        # ALPN response. curl will then know that the server (router) speaks only http2 and hence when the TLS handshake
        # between curl and the server (router) is successful, curl starts speaking http2 to the server (router).
        # If this test works, it is proof that ALPN over TLS is working.
        address = self.router_qdra.http_addresses[0]
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['--head']), http2_prior_knowledge=False)
        self.assertIn('HTTP/2 200', out)
        self.assertIn('server: hypercorn-h2', out)
        self.assertIn('content-type: text/html; charset=utf-8', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    # Tests the HTTP2 head request without APLN and without http2-prior-knowledge
    def test_head_request_no_alpn_no_http2_prior_knowledge(self):
        # Run curl 127.0.0.1:port --head --no-alpn
        # In this test, we do not use curl's --http2-prior-knowledge flag but instead use the --no-alpn flag.
        # This means that curl client will not offer an ALPN protocol at all. The router (server) sends back 'h2'
        # in its ALPN response which is ignored by the curl client.
        # The TLS handshake is successful and curl receives a http2 settings frame from the router which it does
        # not understand. Hence it complains with the error message
        # 'Received HTTP/0.9 when not allowed'
        address = self.router_qdra.http_addresses[0]
        _, out, err = self.run_curl(address,
                                    args=self.get_all_curl_args(['--head']),
                                    http2_prior_knowledge=False,
                                    no_alpn=True, assert_status=False)
        self.assertIn('Received HTTP/0.9 when not allowed', err)


class Http2TlsQ2TwoRouterTest(RouterTestPlainSaslCommon, Http2TestBase, RouterTestSslBase):
    """
    Tests Q2 over TLS
    """
    @staticmethod
    def sasl_file(name):
        return os.path.join(DIR, 'sasl_files', name)

    @classmethod
    def setUpClass(cls):
        super(Http2TlsQ2TwoRouterTest, cls).setUpClass()
        if skip_h2_test():
            return
        if not SASL.extended():
            return

        a_listener_port = cls.tester.get_port()
        b_listener_port = cls.tester.get_port()

        super(Http2TlsQ2TwoRouterTest, cls).createSaslFiles()

        # Start the HTTP2 Server
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_slow_q2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  wait=False,
                                                  server_file="http2_slow_q2_server.py",
                                                  env_config={
                                                      'SERVER_TLS': "yes",
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port),
                                                      'SERVER_CERTIFICATE': cls.ssl_file('server-certificate.pem'),
                                                      'SERVER_PRIVATE_KEY': cls.ssl_file('server-private-key-no-pass.pem'),
                                                      'SERVER_CA_CERT': cls.ssl_file('ca-certificate.pem')
                                                  })

        config_qdra = Qdrouterd.Config([
            ('router', {'id': 'QDR.A', 'mode': 'interior'}),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port(),
                          'authenticatePeer': 'no'}),
            # curl will connect to this httpListener and run the tests.
            ('httpListener', {'port': cls.tester.get_port(),
                              'address': 'examples',
                              'host': 'localhost',
                              'protocolVersion': 'HTTP2',
                              'authenticatePeer': 'yes',
                              'sslProfile': 'http-listener-ssl-profile'}),
            ('sslProfile', {'name': 'http-listener-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'server-password'}),
            ('sslProfile', {'name': 'inter-router-client-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem')}),
            ('connector', {'host': 'localhost', 'role': 'inter-router', 'port': b_listener_port,
                           'sslProfile': 'inter-router-client-ssl-profile',
                           # Provide a sasl user name and password to connect to QDR.B
                           'saslMechanisms': 'PLAIN',
                           'saslUsername': 'test@domain.com',
                           'saslPassword': 'file:' + cls.sasl_file('password.txt')}),
        ])

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': 'localhost',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name,
            'verifyHostname': 'no',
            'sslProfile': 'http-connector-ssl-profile'
        }

        config_qdrb = Qdrouterd.Config([
            # This router will act like a client. First an SSL connection will be established and then
            # we will have SASL plain authentication over SSL on the inter-router connection.
            ('router', {'mode': 'interior', 'id': 'QDR.B', 'saslConfigName': 'tests-mech-PLAIN',
                        'saslConfigDir': os.getcwd()}),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port(),
                          'authenticatePeer': 'no'}),
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': b_listener_port,
                          'sslProfile': 'inter-router-server-ssl-profile', 'maxSessionFrames': '10',
                          'saslMechanisms': 'PLAIN', 'authenticatePeer': 'yes'}),
            ('httpConnector', cls.connector_props),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port()}),
            ('sslProfile', {'name': 'http-connector-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('client-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'client-password'}),
            ('sslProfile', {'name': 'inter-router-server-ssl-profile',
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                            'protocols': 'TLSv1.1 TLSv1.2',
                            'password': 'server-password'})
        ])

        cls.router_qdra = cls.tester.qdrouterd("http2-two-router-tls-q2-A", config_qdra, wait=False)
        cls.router_qdrb = cls.tester.qdrouterd("http2-two-router-tls-q2-B", config_qdrb, wait=False)
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        cls.router_qdrb.wait_http_server_connected(is_tls=True)
        wait_http_listeners_up(cls.router_qdra.addresses[0])

        # curl will use these additional args to connect to the router.
        cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM',
                         '--cert', cls.ssl_file('client-certificate.pem') + ":client-password",
                         '--key', cls.ssl_file('client-private-key.pem')]

    @unittest.skipIf(skip_h2_test(),
                     "Python 3.7 or greater, hyper-h2 and curl needed to run hyperhttp2 tests")
    def test_q2_block_unblock(self):
        # curl  -X POST -H "Content-Type: multipart/form-data"  -F "data=@/home/gmurthy/opensource/test.jpg"
        # http://127.0.0.1:<port?>/upload --http2-prior-knowledge
        address = self.router_qdra.http_addresses[0] + "/upload"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-X', 'POST',
                                                                        '-H', 'Content-Type: multipart/form-data',
                                                                        '-F', 'data=@' + image_file('test.jpg')]))
        self.assertIn('Success', out)
        num_blocked = 0
        num_unblocked = 0
        blocked = "q2 is blocked"
        unblocked = "q2 is unblocked"
        with open(self.router_qdra.logfile_path, 'r') as router_log:
            for line_no, log_line in enumerate(router_log, start=1):
                if unblocked in log_line:
                    num_unblocked += 1
                    unblock_line = line_no
                elif blocked in log_line:
                    block_line = line_no
                    num_blocked += 1
        self.assertGreater(num_blocked, 0)
        self.assertGreater(num_unblocked, 0)
        self.assertGreaterEqual(num_unblocked, num_blocked)
        self.assertGreater(unblock_line, block_line)


class Http2TwoRouterTlsOverSASLExternal(RouterTestPlainSaslCommon,
                                        Http2TestBase,
                                        CommonHttp2Tests):

    @staticmethod
    def ssl_file(name):
        return os.path.join(DIR, 'ssl_certs', name)

    @staticmethod
    def sasl_file(name):
        return os.path.join(DIR, 'sasl_files', name)

    @classmethod
    def setUpClass(cls):
        """
        This test has two routers QDR.A and QDR.B, they talk to each other over TLS and are SASL authenticated.
        QDR.A has a httpListener (with an sslProfile) which accepts curl connections. curl talks to the httpListener
        over TLS. Client authentication is required for curl to talk to the httpListener.
        QDR.B has a httpConnector to an http2 Server and they talk to each other over an encrypted connection.
        """
        super(Http2TwoRouterTlsOverSASLExternal, cls).setUpClass()
        if skip_test():
            return

        if not SASL.extended():
            return

        super(Http2TwoRouterTlsOverSASLExternal, cls).createSaslFiles()

        cls.routers = []

        a_listener_port = cls.tester.get_port()
        b_listener_port = cls.tester.get_port()

        # Start the HTTP2 Server
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  wait=False,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'SERVER_TLS': "yes",
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port),
                                                      'SERVER_CERTIFICATE': cls.ssl_file('server-certificate.pem'),
                                                      'SERVER_PRIVATE_KEY': cls.ssl_file('server-private-key-no-pass.pem'),
                                                      'SERVER_CA_CERT': cls.ssl_file('ca-certificate.pem')
                                                  })
        config_qdra = Qdrouterd.Config([
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port(),
                          'authenticatePeer': 'no'}),
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': a_listener_port,
                          'sslProfile': 'inter-router-server-ssl-profile',
                          'saslMechanisms': 'PLAIN', 'authenticatePeer': 'yes'}),
            # curl will connect to this httpListener and run the tests.
            ('httpListener', {'port': cls.tester.get_port(),
                              'address': 'examples',
                              'host': 'localhost',
                              'protocolVersion': 'HTTP2',
                              'authenticatePeer': 'yes',
                              'sslProfile': 'http-listener-ssl-profile'}),
            ('sslProfile', {'name': 'http-listener-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'server-password'}),
            ('sslProfile', {'name': 'inter-router-server-ssl-profile',
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                            'protocols': 'TLSv1.1 TLSv1.2',
                            'password': 'server-password'}),
            ('router', {'id': 'QDR.A',
                        'mode': 'interior',
                        'saslConfigName': 'tests-mech-PLAIN',
                        'saslConfigDir': os.getcwd()}),
        ])

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': 'localhost',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name,
            'verifyHostname': 'no',
            'sslProfile': 'http-connector-ssl-profile'
        }

        config_qdrb = Qdrouterd.Config([
            # This router will act like a client. First an SSL connection will be established and then
            # we will have SASL plain authentication over SSL on the inter-router connection.
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            ('httpConnector', cls.connector_props),
            ('connector', {'host': 'localhost', 'role': 'inter-router', 'port': a_listener_port,
                           'sslProfile': 'inter-router-client-ssl-profile',
                           # Provide a sasl user name and password to connect to QDR.X
                           'saslMechanisms': 'PLAIN',
                           'saslUsername': 'test@domain.com',
                           'saslPassword': 'file:' + cls.sasl_file('password.txt')}),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': b_listener_port}),
            ('sslProfile', {'name': 'http-connector-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('client-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                            'protocols': 'TLSv1.3',
                            'password': 'client-password'}),
            ('sslProfile', {'name': 'inter-router-client-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem')}),
        ])
        cls.router_qdra = cls.tester.qdrouterd("http2-two-router-tls-A", config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("http2-two-router-tls-B", config_qdrb)
        cls.router_qdrb.wait_router_connected('QDR.A')
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_http_server_connected(is_tls=True)
        wait_http_listeners_up(cls.router_qdra.addresses[0])

        # curl will use these additional args to connect to the router.
        cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM',
                         '--cert', cls.ssl_file('client-certificate.pem') + ":client-password",
                         '--key', cls.ssl_file('client-private-key.pem')]


class Http2TlsAuthenticatePeerOneRouter(Http2TestBase, RouterTestSslBase):
    """
    This test has one standalone router QDR. It has one httpListener with an associated SSL Profile and has one
    unencrypted httpConnector.
    The curl client does not present a client certificate.
    Tests to make sure client cannot connect without client cert since authenticatePeer is set to 'yes'.
    """

    @classmethod
    def setUpClass(cls):
        super(Http2TestBase, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_TLS': "no",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-tls-auth-peer-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }

        cls.listener_ssl_profile = {
            'name': 'http-listener-ssl-profile',
            'caCertFile': cls.ssl_file('ca-certificate.pem'),
            'certFile': cls.ssl_file('server-certificate.pem'),
            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
            'password': 'server-password',
            'protocols': 'TLSv1.3'
        }

        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            # Only the listener side has the SSL Profile, the connector side does not.
            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': 'localhost',
                              # Requires peer to be authenticated.
                              'authenticatePeer': 'yes',
                              'protocolVersion': 'HTTP2',
                              'sslProfile': 'http-listener-ssl-profile'}),
            ('httpConnector', cls.connector_props),
            ('sslProfile', cls.listener_ssl_profile)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

        # Note that the curl client does not present client cert. It only presents the ca-cert
        cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM', '--tlsv1.3']

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_head_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        # This test should fail because the curl client is not presenting a client cert but the router has
        # authenticatePeer set to true.
        if not check_proton_38():
            self.skipTest("Proton version > 0.37.0 needed to run authenticate peer tests, see PROTON-2535")
        address = self.router_qdra.http_addresses[0]
        rc, out, err = self.run_curl(address,
                                     args=self.get_all_curl_args(['--head']),
                                     assert_status=False,
                                     timeout=5)
        self.assertNotEqual(0, rc, f"Expected curl to fail {out} {err}")
        error_log = ":peer did not return a certificate"
        self.router_qdra.wait_log_message(error_log)


class Http2TlsBadConfigTests(HttpTlsBadConfigTestsBase):
    PROTOCOL_VERSION = "HTTP2"

    @classmethod
    def setUpClass(cls):
        super(Http2TlsBadConfigTests, cls).setUpClass()

    def test_connector_mgmt_missing_ca_file(self):
        self._test_connector_mgmt_missing_ca_file()

    def test_connector_mgmt_missing_ssl_profile(self):
        self._test_connector_mgmt_missing_ssl_profile()

    def test_listener_mgmt_missing_ssl_profile(self):
        self._test_listener_mgmt_missing_ssl_profile()

    def test_listener_mgmt_missing_ca_file(self):
        self._test_listener_mgmt_missing_ca_file()


class Http2TestTlsOverTcpTwoRouter(Http2TestBase, CommonHttp2Tests, RouterTestSslBase):
    """
    In this two router test, the tcpListener is on Router QDR.A and the tcpConnector is on router
    QDR.B. Both the listener and the connector are encrypted. The server that QDR.B connects to
    is also encrypted.
    Client authentication is required for curl to talk to the router QDR.A listener http port.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestBase, cls).setUpClass()
        if skip_test():
            return

        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server_tls"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  wait=False,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'SERVER_TLS': "yes",
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port),
                                                      'SERVER_CERTIFICATE': cls.ssl_file('server-certificate.pem'),
                                                      'SERVER_PRIVATE_KEY': cls.ssl_file('server-private-key-no-pass.pem'),
                                                      'SERVER_CA_CERT': cls.ssl_file('ca-certificate.pem')
                                                  })
        inter_router_port = cls.tester.get_port()
        cls.listener_name = 'listenerToBeDeleted'
        cls.tcp_listener_props = {'port': cls.tester.get_port(),
                                  'address': 'examples',
                                  'host': 'localhost',
                                  'name': cls.listener_name,
                                  'authenticatePeer': 'yes',
                                  'sslProfile': 'tcp-listener-ssl-profile'}
        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            # curl will connect to this tcpListener and run the tests.
            ('tcpListener', cls.tcp_listener_props),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'password': 'server-password'}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port})
        ])

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': 'localhost',
            'name': cls.connector_name,
            # Verifies host name. The host name in the certificate sent by the server must match 'localhost'
            'verifyHostname': 'yes',
            'sslProfile': 'tcp-connector-ssl-profile'
        }
        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('tcpConnector', cls.connector_props),
            ('connector', {'name': 'connectorToA', 'role': 'inter-router',
                           'port': inter_router_port,
                           'verifyHostname': 'no'}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('client-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                            'password': 'client-password'}),
        ])

        cls.router_qdra = cls.tester.qdrouterd("http2-two-router-tls-A", config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("http2-two-router-tls-B", config_qdrb)
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        wait_tcp_listeners_up(cls.router_qdra.addresses[0])

        # curl will use these additional args to connect to the router.
        cls.curl_args = ['--cacert', cls.ssl_file('ca-certificate.pem'), '--cert-type', 'PEM',
                         '--cert', cls.ssl_file('client-certificate.pem') + ":client-password",
                         '--key', cls.ssl_file('client-private-key.pem')]

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    # Tests the HTTP2 head request
    def test_head_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        _, out, _ = self.run_curl(self.get_address(), args=self.get_all_curl_args(['--head']))
        # Since in the TCP Adaptor we have the ALPN protocols array set to {"http/1.1", "h2"}
        # it is ok if the response comes back as HTTP/1.1 200
        # For pure http2 adaptor the response comes back as HTTP/2 200
        # Since this test is shared between the TCP and the HTTP2 adaptors, it is ok if we see either
        # HTTP/1.1 200 or HTTP/2 200 in the response.
        success_response = False
        if 'HTTP/2 200' in out or 'HTTP/1.1 200' in out:
            success_response = True
        self.assertTrue(success_response, f"Expected HTTP/2 200 or HTTP/1.1 200 in the response but found {out}")
        self.assertIn('content-type: text/html; charset=utf-8', out)

        # Make sure the ALPN log statements are in the log.
        self.router_qdra.wait_log_message("Using negotiated protocol (h2|http\\/1\\.1) obtained via ALPN")
        self.router_qdrb.wait_log_message("Calling qd_tls_set_alpn_protocols on egress connection using ALPN protocol as (h2|http\\/1\\.1)")

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_yyy_http_listener_delete(self):
        self.check_listener_delete(client_addr=self.router_qdra.tcp_addresses[0],
                                   server_addr=self.router_qdra.addresses[0],
                                   tcp_listener=True)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.tcp_addresses[0],
                                    server_addr=self.router_qdrb.addresses[0],
                                    server_port=self.server_port,
                                    listener_addr=self.router_qdra.addresses[0],
                                    tcp_connector=True)

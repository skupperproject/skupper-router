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

#
# Test the HTTP/1.x Adaptor
#

import errno
import json
import os
import re
import select
import socket
from http.client import HTTPSConnection
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, PROTOCOL_TLS_SERVER
from ssl import CERT_REQUIRED, SSLSocket
from time import sleep, time
from email.parser import BytesParser

from proton import Message
from system_test import TestCase, unittest, main_module, Qdrouterd, QdManager
from system_test import TIMEOUT, AsyncTestSender, AsyncTestReceiver
from system_test import retry_exception, curl_available, run_curl, retry
from system_test import nginx_available, get_digest, NginxServer
from http1_tests import http1_ping, TestServer, RequestHandler10
from http1_tests import RequestMsg, ResponseMsg, ResponseValidator
from http1_tests import ThreadedTestClient, Http1OneRouterTestBase
from http1_tests import CommonHttp1OneRouterTest
from http1_tests import CommonHttp1Edge2EdgeTest
from http1_tests import Http1Edge2EdgeTestBase
from http1_tests import Http1ClientCloseTestsMixIn
from http1_tests import Http1CurlTestsMixIn
from http1_tests import wait_http_listeners_up, wait_http_listeners_down
from http1_tests import HttpAdaptorListenerConnectTestBase
from http1_tests import HttpTlsBadConfigTestsBase
from http1_tests import http1_simple_request
from system_tests_ssl import RouterTestSslBase

CA_CERT = RouterTestSslBase.ssl_file('ca-certificate.pem')
SERVER_CERTIFICATE = RouterTestSslBase.ssl_file('server-certificate.pem')
SERVER_PRIVATE_KEY = RouterTestSslBase.ssl_file('server-private-key.pem')
CLIENT_CERTIFICATE = RouterTestSslBase.ssl_file('client-certificate.pem')
CLIENT_PRIVATE_KEY = RouterTestSslBase.ssl_file('client-private-key.pem')
SERVER_KEY_NO_PASS = RouterTestSslBase.ssl_file('server-private-key-no-pass.pem')
CHAINED_PEM = RouterTestSslBase.ssl_file('chained.pem')


def _read_socket(sock, length, timeout=TIMEOUT):
    """
    Read data from socket until either length octets are read or the socket
    closes.  Return all data read. Note: may return > length if more data is
    present on the socket. Raises a timeout error if < length data arrives.
    """
    old_timeout = sock.gettimeout()
    sock.settimeout(timeout)
    data = b''

    try:
        while len(data) < length:
            chunk = sock.recv(length - len(data))
            if not chunk:  # socket closed
                break
            data += chunk
    finally:
        sock.settimeout(old_timeout)
    return data


class Http1AdaptorListenerConnectTest(HttpAdaptorListenerConnectTestBase):
    """
    Instantiate the Adaptor Listener test for the HTTP1 adaptor
    """
    def test_02_listener_interior(self):
        """
        Test tcpListener socket lifecycle interior to interior
        """
        self._test_listener_socket_lifecycle(self.INTA, self.INTB, "test_02_listener_interior")

    def test_03_listener_edge_interior(self):
        """
        Test tcpListener socket lifecycle edge to interior
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.INTB, "test_03_listener_edge_interior")

    def test_04_listener_interior_edge(self):
        """
        Test tcpListener socket lifecycle interior to edge
        """
        self._test_listener_socket_lifecycle(self.INTA, self.EdgeB, "test_04_listener_interior_edge")

    def test_05_listener_edge_edge(self):
        """
        Test tcpListener socket lifecycle edge to edge
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.EdgeB, "test_05_listener_edge_edge")


class Http1AdaptorManagementTest(TestCase):
    """
    Test Creation and deletion of HTTP1 management entities.
    """
    @classmethod
    def setUpClass(cls):
        super(Http1AdaptorManagementTest, cls).setUpClass()

        cls.LISTENER_TYPE = 'io.skupper.router.httpListener'
        cls.CONNECTOR_TYPE = 'io.skupper.router.httpConnector'
        cls.CONNECTION_TYPE = 'io.skupper.router.connection'

        cls.interior_edge_port = cls.tester.get_port()
        cls.interior_mgmt_port = cls.tester.get_port()
        cls.edge_mgmt_port = cls.tester.get_port()

        cls.http_server_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()

        i_config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTP1MgmtTestInterior'}),
            ('listener', {'role': 'normal',
                          'port': cls.interior_mgmt_port}),
            ('listener', {'role': 'edge', 'port': cls.interior_edge_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(i_config)
        cls.i_router = cls.tester.qdrouterd('HTTP1MgmtTestInterior', config, wait=False)

        e_config = [
            ('router', {'mode': 'edge',
                        'id': 'HTTP1MgmtTestEdge'}),
            ('listener', {'role': 'normal',
                          'port': cls.edge_mgmt_port}),
            ('connector', {'name': 'edge', 'role': 'edge',
                           'port': cls.interior_edge_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(e_config)
        cls.e_router = cls.tester.qdrouterd('HTTP1MgmtTestEdge', config,
                                            wait=False)

        cls.i_router.wait_ready()
        cls.e_router.wait_ready()

    def test_01_create_delete(self):
        """ Create and delete HTTP1 connectors and listeners.  The
        connectors/listeners are created on the edge router.  Verify that the
        adaptor properly notifies the interior of the subscribers/producers.
        """
        e_mgmt = self.e_router.management
        self.assertEqual(0, len(e_mgmt.query(type=self.LISTENER_TYPE).results))
        self.assertEqual(0, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        e_mgmt.create(type=self.CONNECTOR_TYPE,
                      name="ServerConnector",
                      attributes={'address': 'closest/http1Service',
                                  'port': self.http_server_port,
                                  'protocolVersion': 'HTTP1'})

        e_mgmt.create(type=self.LISTENER_TYPE,
                      name="ClientListener",
                      attributes={'address': 'closest/http1Service',
                                  'port': self.http_listener_port,
                                  'protocolVersion': 'HTTP1'})

        # verify the entities have been created and http traffic works

        self.assertEqual(1, len(e_mgmt.query(type=self.LISTENER_TYPE).results))
        self.assertEqual(1, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        http1_ping(sport=self.http_server_port, cport=self.http_listener_port)

        # now check the interior router for the closest/http1Service address
        self.i_router.wait_address("closest/http1Service", subscribers=1)

        #
        # delete the connector and listener; wait for the associated connection
        # to be removed
        #
        e_mgmt.delete(type=self.CONNECTOR_TYPE, name="ServerConnector")
        self.assertEqual(0, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))
        e_mgmt.delete(type=self.LISTENER_TYPE, name="ClientListener")
        self.assertEqual(0, len(e_mgmt.query(type=self.LISTENER_TYPE).results))

        # will hit test timeout on failure:
        while True:
            hconns = 0
            obj = e_mgmt.query(type=self.CONNECTION_TYPE,
                               attribute_names=["protocol"])
            for item in obj.get_dicts():
                if "http/1.x" in item["protocol"]:
                    hconns += 1
            if hconns == 0:
                break
            sleep(0.25)

        # When a connector is configured the router will periodically attempt
        # to connect to the server address. To prove that the connector has
        # been completely removed listen for connection attempts on the server
        # port. The router is expected to stop connecting to the server
        # and calling accept() should time out
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", self.http_server_port))
            s.setblocking(True)
            s.settimeout(3)  # reconnect attempts every 2.5 seconds
            s.listen(1)
            while True:
                try:
                    conn, addr = s.accept()
                    conn.close()  # oop! still connecting...
                    sleep(0.25)
                except socket.timeout:
                    break
        finally:
            s.close()

        # Verify that the address is no longer bound on the interior
        self.i_router.wait_address_unsubscribed("closest/http1Service")

        #
        # re-create the connector and listener; verify it works
        #
        e_mgmt.create(type=self.CONNECTOR_TYPE,
                      name="ServerConnector",
                      attributes={'address': 'closest/http1Service',
                                  'port': self.http_server_port,
                                  'protocolVersion': 'HTTP1'})

        e_mgmt.create(type=self.LISTENER_TYPE,
                      name="ClientListener",
                      attributes={'address': 'closest/http1Service',
                                  'port': self.http_listener_port,
                                  'protocolVersion': 'HTTP1'})

        self.assertEqual(1, len(e_mgmt.query(type=self.LISTENER_TYPE).results))
        self.assertEqual(1, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        http1_ping(sport=self.http_server_port, cport=self.http_listener_port)

        self.i_router.wait_address("closest/http1Service", subscribers=1)

        e_mgmt.delete(type=self.CONNECTOR_TYPE, name="ServerConnector")
        self.assertEqual(0, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))
        e_mgmt.delete(type=self.LISTENER_TYPE, name="ClientListener")
        self.assertEqual(0, len(e_mgmt.query(type=self.LISTENER_TYPE).results))

    def test_01_delete_active_connector(self):
        """Delete an HTTP1 connector that is currently connected to a server.
        Verify the connection is dropped.
        """
        e_mgmt = self.e_router.management
        self.assertEqual(0, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        e_mgmt.create(type=self.CONNECTOR_TYPE,
                      name="ServerConnector",
                      attributes={'address': 'closest/http1Service',
                                  'port': self.http_server_port,
                                  'protocolVersion': 'HTTP1'})

        # verify the connector has been created and attach a dummy server
        self.assertEqual(1, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("", self.http_server_port))
            server.setblocking(True)
            server.settimeout(5)
            server.listen(1)
            conn, _ = server.accept()
        finally:
            server.close()

        # now check the interior router for the closest/http1Service address
        self.i_router.wait_address("closest/http1Service", subscribers=1)

        # delete the connector
        e_mgmt.delete(type=self.CONNECTOR_TYPE, name="ServerConnector")
        self.assertEqual(0, len(e_mgmt.query(type=self.CONNECTOR_TYPE).results))

        # expect socket to close
        while True:
            try:
                rd, _, _ = select.select([conn], [], [])
            except select.error as serror:
                if serror[0] == errno.EINTR:
                    print("ignoring interrupt from select(): %s" % str(serror))
                    continue
                raise  # assuming fatal...
            if len(conn.recv(10)) == 0:
                break

        conn.close()

        # Verify that the address is no longer bound on the interior
        self.i_router.wait_address_unsubscribed("closest/http1Service")


class Http1AdaptorOneRouterTest(Http1OneRouterTestBase,
                                CommonHttp1OneRouterTest):
    """
    Test HTTP servers and clients attached to a standalone router
    """

    # these can be overridden to provide sslProfile configuration (dicts) for
    # the routers connectors and listeners
    connector_ssl_profile = None
    listener_ssl_profile = None

    # these can be set to SSLContext instances to enable TLS for the test
    # clients and servers.
    client_ssl_context = None
    server_ssl_context = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorOneRouterTest, cls).setUpClass()

        # configuration:
        #  One interior router, two servers (one running as HTTP/1.0)
        #
        #  +----------------+
        #  |     INT.A      |
        #  +----------------+
        #      ^         ^
        #      |         |
        #      V         V
        #  <clients>  <servers>

        connector11_config = {'port': cls.server11_port,
                              'host': cls.server11_host,
                              'protocolVersion': 'HTTP1',
                              'address': 'testServer11'}
        connector10_config = {'port': cls.server10_port,
                              'host': cls.server10_host,
                              'protocolVersion': 'HTTP1',
                              'address': 'testServer10'}
        listener11_config = {'port': cls.listener11_port,
                             'host': cls.listener11_host,
                             'protocolVersion': 'HTTP1',
                             'address': 'testServer11'}
        listener10_config = {'port': cls.listener10_port,
                             'host': cls.listener10_host,
                             'protocolVersion': 'HTTP1',
                             'address': 'testServer10'}

        extra_config = []
        if cls.connector_ssl_profile is not None:
            connector11_config.update({'sslProfile': cls.connector_ssl_profile['name'],
                                       'verifyHostname': True})
            connector10_config.update({'sslProfile': cls.connector_ssl_profile['name'],
                                       'verifyHostname': True})
            extra_config.extend([('sslProfile', cls.connector_ssl_profile)])

        if cls.listener_ssl_profile is not None:
            listener11_config.update({'sslProfile': cls.listener_ssl_profile['name'],
                                      'authenticatePeer': True})
            listener10_config.update({'sslProfile': cls.listener_ssl_profile['name'],
                                      'authenticatePeer': True})
            extra_config.extend([('sslProfile', cls.listener_ssl_profile)])

        super(Http1AdaptorOneRouterTest, cls).\
            router(name='INT.A', mode='standalone',
                   extra=[('httpConnector', connector11_config),
                          ('httpConnector', connector10_config),
                          ('httpListener', listener11_config),
                          ('httpListener', listener10_config)
                          ] + extra_config)

        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        cls.http11_server = cls.create_server(server_port=cls.server11_port,
                                              client_port=cls.listener11_port,
                                              tests=cls.TESTS_11)
        cls.http10_server = cls.create_server(server_port=cls.server10_port,
                                              client_port=cls.listener10_port,
                                              tests=cls.TESTS_10,
                                              handler_cls=RequestHandler10)

        cls.INT_A.wait_connectors()
        wait_http_listeners_up(cls.INT_A.addresses[0])

    @classmethod
    def tearDownClass(cls):
        cls.http10_server.wait(TIMEOUT)
        cls.http11_server.wait(TIMEOUT)
        super().tearDownClass()

    @classmethod
    def create_server(cls, server_port, client_port, tests, handler_cls=None):
        """Creates basic HTTP TestServer. May be overridden for SSL support"""
        return TestServer.new_server(server_port=server_port,
                                     client_port=client_port,
                                     tests=tests,
                                     handler_cls=handler_cls)

    def test_005_get_10(self):
        client = self.create_client(self.listener10_host_port)
        self._do_request(client, self.TESTS_10["GET"])
        client.close()

    def test_000_stats(self):
        client = self.create_client(self.listener11_host_port)
        self._do_request(client, self.TESTS_11["GET"])
        self._do_request(client, self.TESTS_11["POST"])
        client.close()

        qd_manager = QdManager(address=self.INT_A.listener)
        stats = qd_manager.query('io.skupper.router.httpRequestInfo')
        self.assertEqual(len(stats), 2)
        for s in stats:
            self.assertEqual(s.get('requests'), 10)
            self.assertEqual(s.get('details').get('GET:400'), 1)
            self.assertEqual(s.get('details').get('GET:200'), 6)
            self.assertEqual(s.get('details').get('GET:204'), 1)
            self.assertEqual(s.get('details').get('POST:200'), 2)

        def assert_approximately_equal(a, b):
            self.assertTrue((abs(a - b) / a) < 0.1)
        if stats[0].get('direction') == 'out':
            self.assertEqual(stats[1].get('direction'), 'in')
            assert_approximately_equal(stats[0].get('bytesOut'), 1059)
            assert_approximately_equal(stats[0].get('bytesIn'), 8849)
            assert_approximately_equal(stats[1].get('bytesOut'), 8830)
            assert_approximately_equal(stats[1].get('bytesIn'), 1059)
        else:
            self.assertEqual(stats[0].get('direction'), 'in')
            self.assertEqual(stats[1].get('direction'), 'out')
            assert_approximately_equal(stats[0].get('bytesOut'), 8849)
            assert_approximately_equal(stats[0].get('bytesIn'), 1059)
            assert_approximately_equal(stats[1].get('bytesOut'), 1059)
            assert_approximately_equal(stats[1].get('bytesIn'), 8830)


# @unittest.skip("Avoid TLS tests due to flakey Python HTTP server")
class Http1AdaptorOneRouterTLSTest(Http1AdaptorOneRouterTest):
    """
    Run the One Router tests using a TLS enabled client
    """
    @classmethod
    def setUpClass(cls):
        # note: connector uses client SSL mode
        cls.connector_ssl_profile = {
            'name': 'Http1AdaptorOneRouterConnectorSSL',
            'caCertFile': CA_CERT,
            'certFile': CLIENT_CERTIFICATE,
            'privateKeyFile': CLIENT_PRIVATE_KEY,
            'password': "client-password",
        }

        # TLS configuration for use by the test servers
        ctxt = SSLContext(protocol=PROTOCOL_TLS_SERVER)
        ctxt.load_verify_locations(cafile=CA_CERT)
        ctxt.load_cert_chain(SERVER_CERTIFICATE,
                             SERVER_PRIVATE_KEY,
                             "server-password")
        ctxt.verify_mode = CERT_REQUIRED
        ctxt.check_hostname = False
        cls.server_ssl_context = ctxt

        cls.listener_ssl_profile = {
            'name': 'Http1AdaptorOneRouterListenerSSL',
            'caCertFile': CA_CERT,
            'certFile': SERVER_CERTIFICATE,
            'privateKeyFile': SERVER_PRIVATE_KEY,
            'password': "server-password"
        }

        # TLS configuration for use by the test clients
        ctxt = SSLContext(protocol=PROTOCOL_TLS_CLIENT)
        ctxt.load_verify_locations(cafile=CA_CERT)
        ctxt.load_cert_chain(CLIENT_CERTIFICATE,
                             CLIENT_PRIVATE_KEY,
                             "client-password")
        ctxt.verify_mode = CERT_REQUIRED
        ctxt.check_hostname = True
        cls.client_ssl_context = ctxt

        super(Http1AdaptorOneRouterTLSTest, cls).setUpClass()

    @classmethod
    def create_client(cls, host_port, timeout=TIMEOUT):
        # overrides base class method to use TLS
        return HTTPSConnection(host_port, timeout=timeout, context=cls.client_ssl_context)

    @classmethod
    def create_server(cls, server_port, client_port, tests, handler_cls=None):
        # overrides base class method to use TLS
        return TestServer.new_server(server_port=server_port,
                                     client_port=client_port,
                                     tests=tests,
                                     handler_cls=handler_cls,
                                     server_ssl_context=cls.server_ssl_context,
                                     client_ssl_context=cls.client_ssl_context)


class Http1AdaptorEdge2EdgeTest(Http1Edge2EdgeTestBase,
                                CommonHttp1Edge2EdgeTest,
                                Http1ClientCloseTestsMixIn,
                                Http1CurlTestsMixIn):
    """
    Test an HTTP servers and clients attached to edge routers separated by an
    interior router
    """
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorEdge2EdgeTest, cls).setUpClass()

        # configuration:
        # one edge, one interior
        #
        #  +-------+    +---------+    +-------+
        #  |  EA1  |<==>|  INT.A  |<==>|  EA2  |
        #  +-------+    +---------+    +-------+
        #      ^                           ^
        #      |                           |
        #      V                           V
        #  <clients>                   <servers>

        super(Http1AdaptorEdge2EdgeTest, cls).\
            router('INT.A', 'interior', [('listener', {'role': 'edge', 'port': cls.INTA_edge1_port}),
                                         ('listener', {'role': 'edge', 'port': cls.INTA_edge2_port}),
                                         ])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        super(Http1AdaptorEdge2EdgeTest, cls).\
            router('EA1', 'edge',
                   [('connector', {'name': 'uplink', 'role': 'edge',
                                   'port': cls.INTA_edge1_port}),
                    ('httpListener', {'name': 'L_testServer11',
                                      'port': cls.listener11_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer11'}),
                    ('httpListener', {'name': 'L_testServer10',
                                      'port': cls.listener10_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer10'})
                    ])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        super(Http1AdaptorEdge2EdgeTest, cls).\
            router('EA2', 'edge',
                   [('connector', {'name': 'uplink', 'role': 'edge',
                                   'port': cls.INTA_edge2_port}),
                    ('httpConnector', {'name': 'C_testServer11',
                                       'port': cls.server11_port,
                                       'host': cls.server11_host,
                                       'protocolVersion': 'HTTP1',
                                       'address': 'testServer11'}),
                    ('httpConnector', {'name': 'C_testServer10',
                                       'port': cls.server10_port,
                                       'host': cls.server10_host,
                                       'protocolVersion': 'HTTP1',
                                       'address': 'testServer10'})
                    ])
        cls.EA2 = cls.routers[-1]
        cls.EA2.listener = cls.EA2.addresses[0]

        cls.INT_A.wait_address('EA1')
        cls.INT_A.wait_address('EA2')

    def test_1001_client_request_close(self):
        """
        Simulate an HTTP client drop while sending a very large PUT
        """
        self.client_request_close_test(self.server11_port,
                                       self.listener11_port,
                                       self.EA2.management)

    def test_1002_client_response_close(self):
        """
        Simulate an HTTP client drop while server sends very large response
        """
        self.client_response_close_test(self.server11_port,
                                        self.listener11_port)

    def test_2000_curl_get(self):
        """
        Perform a get via curl
        """
        self.curl_get_test("127.0.0.1", self.listener11_port,
                           self.server11_port)

    def test_2001_curl_put(self):
        """
        Perform a put via curl
        """
        self.curl_put_test("127.0.0.1", self.listener11_port,
                           self.server11_port)

    def test_2002_curl_post(self):
        """
        Perform a post via curl
        """
        self.curl_post_test("127.0.0.1", self.listener11_port,
                            self.server11_port)

    @staticmethod
    def _server_get_undelivered_out(mgmt, service_address):
        # Return the total count of outgoing undelivered deliveries to
        # service_address
        count = 0
        links = mgmt.query('io.skupper.router.router.link')
        for link in filter(lambda link:
                           link['linkName'] == 'http1.server.out' and
                           link['owningAddr'].endswith(service_address), links):
            count += link['undeliveredCount']
        return count

    @staticmethod
    def _server_get_unsettled_out(mgmt, service_address):
        # Return the total count of outgoing unsettled deliveries to
        # service_address
        count = 0
        links = mgmt.query('io.skupper.router.router.link')
        for link in filter(lambda link:
                           link['linkName'] == 'http1.server.out' and
                           link['owningAddr'].endswith(service_address), links):
            count += link['unsettledCount']
        return count

    @staticmethod
    def _client_in_link_count(mgmt, service_address):
        # get the total number of active HTTP1 client in-links for the given
        # service address
        links = mgmt.query('io.skupper.router.router.link')
        count = len(list(filter(lambda link:
                                link['linkName'] == 'http1.client.in' and
                                link['owningAddr'].endswith(service_address),
                                links)))
        return count

    def test_3000_N_client_pipeline_cancel(self):
        """
        Create N clients. Have several clients send short GET requests. Have
        two clients send incomplete GET requests. Close the incomplete clients
        with their requests outstanding. Verify that the router services the
        remaining requests properly
        """

        # note: this test assumes server-side is synchronous. It will need to
        # be re-written if server-facing pipelining is implemented!

        CLIENT_COUNT = 10
        KILL_INDEX = [4, 5]  # clients to force close

        request = b'GET /client_pipeline/3000 HTTP/1.1\r\n' \
            + b'index: %d\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'\r\n'
        request_len = 67
        request_re = re.compile(r"^(index:).(\d+)", re.MULTILINE)

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 7\r\n' \
            + b'\r\n' \
            + b'index=%d'
        response_len = 45
        response_re = re.compile(r"(index=)(\d+)$", re.MULTILINE)

        truncated_req = b'GET /client_pipeline_truncated HTTP/1.1\r\n' \
            + b'Content-Length: 10000\r\n' \
            + b'\r\n' \
            + b'I like cereal...'

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.server11_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            clients = []
            for index in range(CLIENT_COUNT):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                retry_exception(lambda cs=client:
                                cs.connect(("localhost",
                                            self.listener11_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.settimeout(TIMEOUT)
                clients.append(client)

                if index in KILL_INDEX:
                    # Pause before sending the incomplete request until the
                    # previous deliveries arrive at the server. This prevents
                    # the incomplete requests from arriving ahead of other
                    # requests and having the server start reading the
                    # incomplete request first (before the abort signal arrives
                    # to cancel the request).
                    self.assertTrue(retry(lambda mg=EA2_mgmt, dc=index:
                                          self._server_get_undelivered_out(mg, "testServer11")
                                          + self._server_get_unsettled_out(mg, "testServer11")
                                          == dc))

                    # Send an incomplete request. When the client closes this
                    # should cause the client-facing router to abort the
                    # in-flight delivery.
                    client.sendall(truncated_req)
                else:
                    client.sendall(request % index)

            # Wait for the client deliveries to arrive on the outgoing link to
            # the server. The arrival order is not guaranteed! The test will
            # validate that responses are sent to the proper client

            self.assertTrue(retry(lambda mg=EA2_mgmt, dc=CLIENT_COUNT:
                                  self._server_get_undelivered_out(mg, "testServer11")
                                  + self._server_get_unsettled_out(mg, "testServer11")
                                  == dc))

            # Now destroy the incomplete clients. Wait until the socket has
            # actually closed at the ingress router:

            for index in KILL_INDEX:
                clients[index].shutdown(socket.SHUT_RDWR)
                clients[index].close()
                clients[index] = None

            # wait for the killed client connections to the router drop
            self.assertTrue(retry(lambda mg=EA1_mgmt:
                                  self._client_in_link_count(mg, 'testServer11') == CLIENT_COUNT - len(KILL_INDEX)))
            sleep(1.0)  # hack: ensure the abort signal has propagated to the server

            # Since the cancelled request has yet to be written to the server
            # connection by the adaptor, the adaptor should be smart enough to
            # dispose of the cancelled request without writing anything to the
            # server (or dropping the server connection)

            for _ in range(CLIENT_COUNT - len(KILL_INDEX)):
                data = _read_socket(server, request_len)
                self.assertEqual(request_len, len(data))
                index_match = request_re.search(data.decode())
                self.assertIsNotNone(index_match,
                                     f"request not matched >{data.decode()}<")
                self.assertEqual(2, len(index_match.groups()))
                server.sendall(response % int(index_match.group(2)))

            # expect no more data from the router
            server.settimeout(1.0)
            self.assertRaises(TimeoutError, server.recv, 4096)
            server.close()

            # expect remaining clients get responses, and the correct responses
            # are returned
            for index in range(CLIENT_COUNT):
                if clients[index] is not None:
                    data = _read_socket(clients[index], response_len)
                    self.assertEqual(response_len, len(data))
                    index_match = response_re.search(data.decode())
                    self.assertIsNotNone(index_match,
                                         f"response not matched >{data.decode()}<")
                    self.assertEqual(2, len(index_match.groups()))
                    self.assertEqual(index, int(index_match.group(2)))
                    clients[index].close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

    def test_3001_N_client_pipeline_recover(self):
        """
        Similar to test_3000, but the request from the dropped client is in the
        process of being read by the server. This should cause the router to
        drop the connection to the server. When the connection re-establishes
        the remaining requests must arrive without error.
        """

        # note: this test assumes server-side is synchronous. It will need to
        # be re-written if server-facing pipelining is implemented!

        CLIENT_COUNT = 10

        request = b'GET /client_pipeline/3001 HTTP/1.1\r\n' \
            + b'index: %d\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'\r\n'
        request_len = 67
        request_re = re.compile(r"^(index:).(\d+)", re.MULTILINE)

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 7\r\n' \
            + b'\r\n' \
            + b'index=%d'
        response_len = 45
        response_re = re.compile(r"(index=)(\d+)$", re.MULTILINE)

        truncated_req = b'GET /client_pipeline_truncated HTTP/1.1\r\n' \
            + b'Content-Length: 10000\r\n' \
            + b'\r\n' \
            + b'I like potatoes...'

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.server11_host, self.server11_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            clients = []
            for index in range(CLIENT_COUNT):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                retry_exception(lambda cs=client:
                                cs.connect((self.listener11_host,
                                            self.listener11_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.settimeout(TIMEOUT)
                clients.append(client)

                if index == 0:
                    # Have the first client send an incomplete request. When
                    # the client closes this should cause the client-facing
                    # router to abort the in-flight delivery. Wait for the
                    # request to arrive at the server in order to prevent the
                    # following requests from arriving first
                    client.sendall(truncated_req)
                    self.assertTrue(retry(lambda mg=EA2_mgmt, dc=1:
                                          self._server_get_undelivered_out(mg, "testServer11")
                                          + self._server_get_unsettled_out(mg, "testServer11")
                                          == dc))
                else:
                    client.sendall(request % index)

            # Wait for the client deliveries to arrive on the outgoing link to
            # the server. The arrival order is not guaranteed! The test will
            # validate that responses are sent to the proper client

            self.assertTrue(retry(lambda mg=EA2_mgmt, dc=CLIENT_COUNT:
                                  self._server_get_undelivered_out(mg, "testServer11")
                                  + self._server_get_unsettled_out(mg, "testServer11")
                                  == dc))

            # Have the server start processing the incomplete request

            data = _read_socket(server, length=len(truncated_req))
            self.assertEqual(len(truncated_req), len(data))
            self.assertIn(b"potatoes...", data)

            # Now destroy the client. Wait until the socket has
            # actually closed at the ingress router:

            clients[0].shutdown(socket.SHUT_RDWR)
            clients[0].close()
            clients[0] = None
            self.assertTrue(retry(lambda mg=EA1_mgmt:
                                  self._client_in_link_count(mg, 'testServer11') == CLIENT_COUNT - 1))

            # attempting to read the rest of the request should result in the
            # server socket closing

            data = _read_socket(server, 4096, timeout=TIMEOUT)
            self.assertEqual(b'', data, "Server did not disconnect as expected")
            server.shutdown(socket.SHUT_RDWR)
            server.close()

            # expect the router to reconnect

            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            # expect the remaining requests to complete successfully

            for index in range(1, CLIENT_COUNT):
                data = _read_socket(server, length=request_len)
                self.assertEqual(request_len, len(data))
                index_match = request_re.search(data.decode())
                self.assertIsNotNone(index_match,
                                     f"request not matched >{data.decode()}<")
                self.assertEqual(2, len(index_match.groups()))
                server.sendall(response % int(index_match.group(2)))

            # expect no more data from the router
            server.settimeout(1.0)
            self.assertRaises(TimeoutError, server.recv, 4096)
            server.close()

            # expect remaining clients get responses, and the correct responses
            # are returned
            for index in range(CLIENT_COUNT):
                if clients[index] is not None:
                    data = _read_socket(clients[index], response_len)
                    self.assertEqual(response_len, len(data))
                    index_match = response_re.search(data.decode())
                    self.assertIsNotNone(index_match,
                                         f"response not matched >{data.decode()}<")
                    self.assertEqual(2, len(index_match.groups()))
                    self.assertEqual(index, int(index_match.group(2)))
                    clients[index].close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

    def test_4000_client_half_close(self):
        """
        Verify that a client can close the write side of its socket and still
        receive a response
        """

        # note: this test assumes server-side is synchronous. It will need to
        # be re-written if server-facing pipelining is implemented!

        CLIENT_COUNT = 9  # single digit or content length will be bad!

        request_template = 'GET /client%d HTTP/1.1\r\n' \
            + 'Content-Length: 0\r\n' \
            + '\r\n'
        request_length = len(request_template) - 1

        response_template = 'HTTP/1.1 200 OK\r\n' \
            + 'content-length: 7\r\n' \
            + '\r\n' \
            + 'client%d'
        response_length = len(response_template) - 1

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.server11_host, self.server11_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            clients = []
            for index in range(CLIENT_COUNT):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                retry_exception(lambda cs=client:
                                cs.connect((self.listener11_host,
                                            self.listener11_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.settimeout(TIMEOUT)
                clients.append(client)
                request = request_template % index
                client.sendall(request.encode())

                # Ensure that the delivery arrives at the server before sending
                # the next request. Otherwise if all requests are sent at once
                # they can arrive out of order. That will cause the test to
                # fail since it services each client in order of transmission
                self.assertTrue(retry(lambda mg=EA2_mgmt, dc=index + 1:
                                      self._server_get_undelivered_out(mg, "testServer11")
                                      + self._server_get_unsettled_out(mg, "testServer11")
                                      == dc))

                # Now close the write side of the client socket:
                client.shutdown(socket.SHUT_WR)

            # expect the requests to complete successfully

            for index in range(CLIENT_COUNT):
                data = _read_socket(server, length=request_length)
                self.assertEqual(request_length, len(data))
                self.assertIn(b"GET /client%d" % index, data)

                response = response_template % index
                server.sendall(response.encode())

                data = _read_socket(clients[index], length=response_length)
                self.assertEqual(response_length, len(data))
                self.assertIn(b"client%d" % index, data)
                clients[index].close()

            server.shutdown(socket.SHUT_RDWR)
            server.close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

    def test_4001_server_half_close(self):
        """
        Verify that a server can close the side of its socket after receiving a
        request and the response will be received by the client
        """

        # note: this test assumes server-side is synchronous. It will need to
        # be re-written if server-facing pipelining is implemented!

        request = b'GET /client1 HTTP/1.1\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'\r\n'

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 7\r\n' \
            + b'\r\n' \
            + b'client1'

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.server11_host, self.server11_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            retry_exception(lambda cs=client:
                            cs.connect((self.listener11_host,
                                        self.listener11_port)),
                            delay=0.25,
                            exception=ConnectionRefusedError)
            client.settimeout(TIMEOUT)
            client.sendall(request)

            data = _read_socket(server, length=len(request))
            self.assertEqual(len(request), len(data))
            self.assertIn(b"GET /client1", data)

            server.shutdown(socket.SHUT_RD)
            server.sendall(response)

            data = _read_socket(client, length=len(response))
            self.assertEqual(len(response), len(data))
            self.assertIn(b"client1", data)
            client.close()

            server.shutdown(socket.SHUT_RDWR)
            server.close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

    def test_4002_server_early_reply(self):
        """
        Verify that a server can send a response before the entire request
        message has been received.
        """

        request_part1 = b'GET /early_reply HTTP/1.1\r\n' \
            + b'Content-Length: 50\r\n' \
            + b'\r\n' \
            + b'0123456789'

        request_part2 = b'0123456789012345678901234567890123456789'

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 14\r\n' \
            + b'\r\n' \
            + b'Early Response'

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.server11_host, self.server11_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            retry_exception(lambda cs=client:
                            cs.connect((self.listener11_host,
                                        self.listener11_port)),
                            delay=0.25,
                            exception=ConnectionRefusedError)
            client.settimeout(TIMEOUT)
            client.sendall(request_part1)

            # server read part 1
            data = _read_socket(server, length=len(request_part1))
            self.assertEqual(len(request_part1), len(data),
                             f"Unexpected request: {data}")

            # fire off the response
            server.sendall(response)

            # Consume response
            data = _read_socket(client, length=len(response))
            self.assertEqual(len(response), len(data),
                             f"Unexpected response: {data}")

            # finish request
            client.sendall(request_part2)
            client.close()

            # consume the remaining request
            data = _read_socket(server, length=len(request_part2))
            self.assertEqual(len(request_part2), len(data),
                             f"Unexpected request: {data}")
            server.shutdown(socket.SHUT_RDWR)
            server.close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})


class Http1AdaptorEdge2EdgeTLSTest(Http1Edge2EdgeTestBase,
                                   CommonHttp1Edge2EdgeTest):
    """
    Run the Edge2Edge tests using TLS transport
    """

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorEdge2EdgeTLSTest, cls).setUpClass()

        # configuration:
        # one edge, one interior
        #
        #  +-------+    +---------+    +-------+
        #  |  EA1  |<==>|  INT.A  |<==>|  EA2  |
        #  +-------+    +---------+    +-------+
        #      ^                           ^
        #      |                           |
        #      V                           V
        #  <clients>                   <servers>

        # note: connector uses client SSL mode
        cls.connector_ssl_profile = {
            'name': 'Http1AdaptorEdge2EdgeConnectorSSL',
            'caCertFile': CA_CERT,
            'certFile': CLIENT_CERTIFICATE,
            'privateKeyFile': CLIENT_PRIVATE_KEY,
            'password': "client-password",
        }

        # TLS configuration for use by the test servers
        ctxt = SSLContext(protocol=PROTOCOL_TLS_SERVER)
        ctxt.load_verify_locations(cafile=CA_CERT)
        ctxt.load_cert_chain(SERVER_CERTIFICATE,
                             SERVER_PRIVATE_KEY,
                             "server-password")
        ctxt.verify_mode = CERT_REQUIRED
        ctxt.check_hostname = False
        cls.server_ssl_context = ctxt

        cls.listener_ssl_profile = {
            'name': 'Http1AdaptorEdge2EdgeListenerSSL',
            'caCertFile': CA_CERT,
            'certFile': SERVER_CERTIFICATE,
            'privateKeyFile': SERVER_PRIVATE_KEY,
            'password': "server-password"
        }

        ctxt = SSLContext(protocol=PROTOCOL_TLS_CLIENT)
        ctxt.load_verify_locations(cafile=CA_CERT)
        ctxt.load_cert_chain(CLIENT_CERTIFICATE,
                             CLIENT_PRIVATE_KEY,
                             "client-password")
        ctxt.verify_mode = CERT_REQUIRED
        ctxt.check_hostname = True
        cls.client_ssl_context = ctxt

        super(Http1AdaptorEdge2EdgeTLSTest, cls).\
            router('INT.A', 'interior', [('listener', {'role': 'edge', 'port': cls.INTA_edge1_port}),
                                         ('listener', {'role': 'edge', 'port': cls.INTA_edge2_port}),
                                         ])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        super(Http1AdaptorEdge2EdgeTLSTest, cls).\
            router('EA1', 'edge',
                   [('sslProfile', cls.listener_ssl_profile),
                    ('connector', {'name': 'uplink', 'role': 'edge',
                                   'port': cls.INTA_edge1_port}),
                    ('httpListener', {'name': 'L_testServer11',
                                      'port': cls.listener11_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer11',
                                      'sslProfile': cls.listener_ssl_profile['name'],
                                      'authenticatePeer': True}),

                    ('httpListener', {'name': 'L_testServer10',
                                      'port': cls.listener10_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer10',
                                      'sslProfile': cls.listener_ssl_profile['name'],
                                      'authenticatePeer': True}),
                    ])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        super(Http1AdaptorEdge2EdgeTLSTest, cls).\
            router('EA2', 'edge',
                   [('sslProfile', cls.connector_ssl_profile),
                    ('connector', {'name': 'uplink', 'role': 'edge',
                                   'port': cls.INTA_edge2_port}),
                    ('httpConnector', {'name': 'C_testServer11',
                                       'port': cls.server11_port,
                                       'host': cls.server11_host,
                                       'protocolVersion': 'HTTP1',
                                       'sslProfile': cls.connector_ssl_profile['name'],
                                       'verifyHostname': True,
                                       'address': 'testServer11'}),

                    ('httpConnector', {'name': 'C_testServer10',
                                       'port': cls.server10_port,
                                       'host': cls.server10_host,
                                       'protocolVersion': 'HTTP1',
                                       'sslProfile': cls.connector_ssl_profile['name'],
                                       'verifyHostname': True,
                                       'address': 'testServer10'})
                    ],
                   wait=False)
        cls.EA2 = cls.routers[-1]
        cls.EA2.listener = cls.EA2.addresses[0]

        cls.INT_A.wait_address('EA1')
        cls.INT_A.wait_address('EA2')

    @classmethod
    def create_threaded_client(cls, *args, **kwargs):
        """
        Override base class to provide TLS configuration
        """
        return ThreadedTestClient(*args, **kwargs,
                                  ssl_context=cls.client_ssl_context)

    @classmethod
    def create_server(cls, *args, **kwargs):
        """
        Override base class to provide TLS configuration
        """
        return TestServer.new_server(*args, **kwargs,
                                     server_ssl_context=cls.server_ssl_context,
                                     client_ssl_context=cls.client_ssl_context)

    def test_4000_server_no_notify(self):
        """
        Force server close without sending proper close-notify TLS
        handshake, but since the response has an explicit length no error
        should occur
        """
        request = b'GET /client HTTP/1.1\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'\r\n'

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 6\r\n' \
            + b'\r\n' \
            + b'client'

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as raw_listener:
            raw_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            raw_listener.settimeout(TIMEOUT)
            with self.server_ssl_context.wrap_socket(raw_listener,
                                                     server_side=True) as listener:
                listener.bind((self.server11_host, self.server11_port))
                listener.listen(1)
                server, addr = listener.accept()
                wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

                raw_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                raw_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                raw_client.settimeout(TIMEOUT)
                client = self.client_ssl_context.wrap_socket(raw_client,
                                                             server_hostname=self.listener11_host)
                retry_exception(lambda cs=client:
                                cs.connect((self.listener11_host,
                                            self.listener11_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)

                client.sendall(request)

                data = _read_socket(server, length=len(request))
                self.assertEqual(len(request), len(data))
                self.assertIn(b"GET /client", data)
                server.sendall(response)
                server.shutdown(socket.SHUT_RDWR)
                server.close()

                data = _read_socket(client, length=len(response))
                self.assertEqual(len(response), len(data))
                self.assertIn(b"200 OK", data)
                client.close()
        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

    def test_4001_server_no_notify_truncated(self):
        """
        Similar to 4000, but cause an error by not sending an explictly
        terminated message.
        """
        request = b'GET /client HTTP/1.1\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'\r\n'

        response = b'HTTP/1.1 200 OK\r\n' \
            + b'\r\n' \
            + b'unterminated response'

        EA1_mgmt = self.EA1.qd_manager
        EA2_mgmt = self.EA2.qd_manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as raw_listener:
            raw_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            raw_listener.settimeout(TIMEOUT)
            with self.server_ssl_context.wrap_socket(raw_listener,
                                                     server_side=True) as listener:
                listener.bind((self.server11_host, self.server11_port))
                listener.listen(1)
                server, addr = listener.accept()
                wait_http_listeners_up(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})

                raw_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                raw_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                raw_client.settimeout(TIMEOUT)
                client = self.client_ssl_context.wrap_socket(raw_client,
                                                             server_hostname=self.listener11_host)
                retry_exception(lambda cs=client:
                                cs.connect((self.listener11_host,
                                            self.listener11_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)

                client.sendall(request)

                data = _read_socket(server, length=len(request))
                self.assertEqual(len(request), len(data))
                self.assertIn(b"GET /client", data)

                server.sendall(response)

                # force close without close-notify
                server.shutdown(socket.SHUT_RDWR)
                server.close()

                # What happens on the client-facing side is timing dependent:
                # it may get a partial message, or even an error response from
                # the server. In any case the router should force close the
                # client connection. Attempt to drain the socket. If this times
                # out the router did not force close the connection properly
                #
                while True:
                    data = _read_socket(client, 4096, timeout=TIMEOUT)
                    if data == b'':
                        # yay! socket closed!
                        break
                client.close()

        wait_http_listeners_down(self.EA1.addresses[0], l_filter={'name': 'L_testServer11'})


class FakeHttpServerBase:
    """
    A very base socket server to simulate HTTP server behaviors
    """

    def __init__(self, host='', port=80, bufsize=1024):
        super(FakeHttpServerBase, self).__init__()
        self.host = host
        self.port = port
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.settimeout(TIMEOUT)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, port))
        self.listener.listen(1)
        self.conn, self.addr = self.listener.accept()
        self.do_connect()
        while True:
            data = self.conn.recv(bufsize)
            if not data:
                break
            self.do_data(data)
        self.do_close()

    def do_connect(self):
        pass

    def do_data(self, data):
        pass

    def do_close(self):
        self.listener.shutdown(socket.SHUT_RDWR)
        self.listener.close()
        del self.listener
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        del self.conn
        sleep(0.5)  # fudge factor allow socket close to complete


class Http1AdaptorBadEndpointsTest(TestCase,
                                   Http1ClientCloseTestsMixIn):
    """
    Subject the router to mis-behaving HTTP endpoints.
    """
    @classmethod
    def setUpClass(cls):
        """
        Single router configuration with one HTTPListener and one
        HTTPConnector.
        """
        super(Http1AdaptorBadEndpointsTest, cls).setUpClass()
        cls.http_server_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.http_fake_port = cls.tester.get_port()

        config = [
            ('router', {'mode': 'standalone',
                        'id': 'TestBadEndpoints'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('httpConnector', {'port': cls.http_server_port,
                               'protocolVersion': 'HTTP1',
                               'address': 'testServer'}),
            ('httpListener', {'name': 'L_testServer',
                              'port': cls.http_listener_port,
                              'protocolVersion': 'HTTP1',
                              'address': 'testServer'}),
            ('httpListener', {'port': cls.http_fake_port,
                              'protocolVersion': 'HTTP1',
                              'address': 'fakeServer'}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.INT_A = cls.tester.qdrouterd("TestBadEndpoints", config, wait=True)
        cls.INT_A.listener = cls.INT_A.addresses[0]

    def test_01_unsolicited_response(self):
        """
        Create a server that sends an immediate Request Timeout response
        without first waiting for a request to arrive.
        """
        class UnsolicitedResponse(FakeHttpServerBase):
            def __init__(self, host, port):
                self.request_sent = False
                super(UnsolicitedResponse, self).__init__(host, port)

            def do_connect(self):
                self.conn.sendall(b'HTTP/1.1 408 Request Timeout\r\n'
                                  + b'Content-Length: 10\r\n'
                                  + b'\r\n'
                                  + b'Bad Server')
                self.request_sent = True

        http1_ping(self.http_server_port, self.http_listener_port)

        server = UnsolicitedResponse('127.0.0.1', self.http_server_port)
        self.assertTrue(server.request_sent)

        http1_ping(self.http_server_port, self.http_listener_port)

    def test_02_bad_AMQP_request_message(self):
        """
        Test various improperly constructed AMQP request messages. Note this
        test deals with improperly encoded inter-router AMQP messages: no HTTP
        clients are used.
        """
        with TestServer.new_server(server_port=self.http_server_port,
                                   client_port=self.http_listener_port,
                                   tests={}) as server:

            body_filler = "?" * 1024 * 300  # Q2

            msg = Message(body="NOMSGID " + body_filler)
            ts = AsyncTestSender(address=self.INT_A.listener,
                                 target="testServer",
                                 message=msg)
            ts.wait()
            self.assertEqual(1, ts.rejected)

            msg = Message(body="NO REPLY TO " + body_filler)
            msg.id = 1
            ts = AsyncTestSender(address=self.INT_A.listener,
                                 target="testServer",
                                 message=msg)
            ts.wait()
            self.assertEqual(1, ts.rejected)

            msg = Message(body="NO SUBJECT " + body_filler)
            msg.id = 1
            msg.reply_to = "amqp://fake/reply_to"
            ts = AsyncTestSender(address=self.INT_A.listener,
                                 target="testServer",
                                 message=msg)
            ts.wait()
            self.assertEqual(1, ts.rejected)

            msg = Message(body="NO APP PROPERTIES " + body_filler)
            msg.id = 1
            msg.reply_to = "amqp://fake/reply_to"
            msg.subject = "GET"
            ts = AsyncTestSender(address=self.INT_A.listener,
                                 target="testServer",
                                 message=msg)
            ts.wait()
            self.assertEqual(1, ts.rejected)

            # TODO: fix body parsing (returns NEED_MORE)
            # msg = Message(body="INVALID BODY " + body_filler)
            # msg.id = 1
            # msg.reply_to = "amqp://fake/reply_to"
            # msg.subject = "GET"
            # msg.properties = {"http:target": "/Some/target"}
            # ts = AsyncTestSender(address=self.INT_A.listener,
            #                      target="testServer",
            #                      message=msg)
            # ts.wait()
            # self.assertEqual(1, ts.rejected);

        # verify router is still sane:
        http1_ping(self.http_server_port, self.http_listener_port)

    def test_03_bad_AMQP_response_message(self):
        """
        Test various improperly constructed AMQP response messages. Note this
        test deals with improperly encoded inter-router AMQP messages: no HTTP
        server is used.
        """
        DUMMY_TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_03_bad_response_message",
                            headers={"Content-Length": "000"}),
                 None,
                 None,
                 ),
            ]
        }

        body_filler = "?" * 1024 * 300  # Q2

        # fake server - just to create a sink for the "fakeServer" address so
        # credit will be granted.
        rx = AsyncTestReceiver(self.INT_A.listener,
                               source="fakeServer")

        # no correlation id:
        client = ThreadedTestClient(DUMMY_TESTS,
                                    self.http_fake_port)
        req = rx.queue.get(timeout=TIMEOUT)
        resp = Message(body="NO CORRELATION ID " + body_filler)
        resp.to = req.reply_to
        ts = AsyncTestSender(address=self.INT_A.listener,
                             target=req.reply_to,
                             message=resp)
        ts.wait()
        self.assertEqual(1, ts.rejected)
        client.wait(dump_on_error=False)
        self.assertIsNotNone(client.error)

        # missing application properties
        client = ThreadedTestClient(DUMMY_TESTS,
                                    self.http_fake_port)
        req = rx.queue.get(timeout=TIMEOUT)

        resp = Message(body="NO APPLICATION PROPS " + body_filler)
        resp.to = req.reply_to
        resp.correlation_id = req.id
        ts = AsyncTestSender(address=self.INT_A.listener,
                             target=req.reply_to,
                             message=resp)
        ts.wait()
        self.assertEqual(1, ts.rejected)
        client.wait(dump_on_error=False)
        self.assertIsNotNone(client.error)

        # no status application property
        client = ThreadedTestClient(DUMMY_TESTS,
                                    self.http_fake_port)
        req = rx.queue.get(timeout=TIMEOUT)
        resp = Message(body="MISSING STATUS HEADER " + body_filler)
        resp.to = req.reply_to
        resp.correlation_id = req.id
        resp.properties = {"stuff": "value"}
        ts = AsyncTestSender(address=self.INT_A.listener,
                             target=req.reply_to,
                             message=resp)
        ts.wait()
        self.assertEqual(1, ts.rejected)
        client.wait(dump_on_error=False)
        self.assertIsNotNone(client.error)

        # TODO: fix body parsing (returns NEED_MORE)
        # # invalid body format
        # client = ThreadedTestClient(DUMMY_TESTS,
        #                             self.http_fake_port)
        # req = rx.queue.get(timeout=TIMEOUT)
        # resp = Message(body="INVALID BODY FORMAT " + body_filler)
        # resp.to = req.reply_to
        # resp.correlation_id = req.id
        # resp.properties = {"http:status": 200}
        # ts = AsyncTestSender(address=self.INT_A.listener,
        #                      target=req.reply_to,
        #                      message=resp)
        # ts.wait()
        # self.assertEqual(1, ts.rejected);
        # client.wait()
        # self.assertIsNotNone(client.error)

        rx.stop()
        sleep(0.5)  # fudge factor allow socket close to complete

        # verify router is still sane:
        http1_ping(self.http_server_port, self.http_listener_port)

    def test_04_client_request_close(self):
        """
        Simulate an HTTP client drop while sending a very large PUT
        """
        self.client_request_close_test(self.http_server_port,
                                       self.http_listener_port,
                                       self.INT_A.management)

    def test_05_client_response_close(self):
        """
        Simulate an HTTP client drop while server sends very large response
        """
        self.client_response_close_test(self.http_server_port,
                                        self.http_listener_port)

    def test_06_bad_request_headers(self):
        """
        Construct request messages with various header violations
        """

        # no need for a full server - expect that no data will arrive at server
        # since all parse errors will be detected and handling on the
        # client-facing router

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.http_server_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)
            server, addr = listener.accept()
            wait_http_listeners_up(self.INT_A.listener, l_filter={'name': 'L_testServer'})

            bad_requests = [
                # malformed request line
                b'GET \r\n',
                # invalid version field
                b'GET /badversion HTTP/1.bannana\r\n',
                # unsupported version
                b'GET /wrongversion HTTP/0.9\r\n',
                # invalid header format
                b'GET /badheader HTTP/1.1\r\n' \
                + b'novalue\r\n\r\n',
                # invalid header format 2
                b'GET /badheader1 HTTP/1.1\r\n' \
                + b'novalue :\r\n\r\n',
                # invalid transfer encoding value
                b'PUT /bad/encoding HTTP/1.1\r\n' \
                + b'Transfer-Encoding: bannana\r\n\r\nBLAH',
                # invalid content length format
                b'PUT /bad/len1 HTTP/1.1\r\n' \
                + b'Content-Length:\r\n\r\nFOO',
                # invalid content length
                b'PUT /bad/len2 HTTP/1.1\r\n' \
                + b'Content-Length: spaghetti\r\n\r\nFOO',
                # duplicate conflicting content length fields
                b'PUT /dup/len3 HTTP/1.1\r\n' \
                + b'Content-length: 1\r\n' \
                + b'Content-length: 2\r\n\r\nHA',
            ]

            for req in bad_requests:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(TIMEOUT)
                retry_exception(lambda cs=client:
                                cs.connect(("127.0.0.1", self.http_listener_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.sendall(req)

                # expect the router to close the connection. Otherwise this
                # raises a time out error:
                _ = _read_socket(client, length=4096)
                self.assertEqual(0, len(_))
                client.close()

            # ensure none of theses bad requests were forwarded to the server
            server.settimeout(0.25)
            self.assertRaises(TimeoutError, server.recv, 4096)
            server.shutdown(socket.SHUT_RDWR)
            server.close()
        wait_http_listeners_down(self.INT_A.listener, l_filter={'name': 'L_testServer'})

    def test_07_bad_response_line(self):
        """
        Construct response messages with various violations
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.http_server_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)

            bad_responses = [
                # malformed response line
                b'bad-response\r\n',
                # invalid version field
                b'HTTP/1.bannana 999\r\n',
                # unsupported version
                b'HTTP/0.9 200 OK\r\n',
                # missing status code
                b'HTTP/1.1 \r\n',
                # invalid status code 1
                b'HTTP/1.1 skupper\r\n',
                # invalid status code 2
                b'HTTP/1.1 2\r\n',
            ]

            # unterminated request to check client cleanup
            request = b'GET / HTTP/1.1\r\nContent-Length: 100\r\n\r\nX'
            for response in bad_responses:
                server, addr = listener.accept()
                wait_http_listeners_up(self.INT_A.listener, l_filter={'name': 'L_testServer'})

                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(TIMEOUT)
                retry_exception(lambda cs=client:
                                cs.connect(("127.0.0.1", self.http_listener_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.sendall(request)

                _ = _read_socket(server, length=len(request))
                server.sendall(response)

                # expect the router to close the connection to the server due
                # to the error. Otherwise this raises a time out error:
                _ = _read_socket(server, length=4096)
                self.assertEqual(0, len(_))

                # This should cause the router to send an error response and
                # close the connection to the client. Otherwise this raises a
                # time out error:
                err = _read_socket(client, length=4096)
                self.assertIn(b'HTTP/1.1 503', err)
                client.close()

                server.shutdown(socket.SHUT_RDWR)
                server.close()
        wait_http_listeners_down(self.INT_A.listener, l_filter={'name': 'L_testServer'})

    def test_08_bad_chunked_body(self):
        """
        Construct a messages with invalid chunk header
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.http_server_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)

            server, addr = listener.accept()
            wait_http_listeners_up(self.INT_A.listener, l_filter={'name': 'L_testServer'})

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.settimeout(TIMEOUT)
            retry_exception(lambda cs=client:
                            cs.connect(("127.0.0.1", self.http_listener_port)),
                            delay=0.25,
                            exception=ConnectionRefusedError)

            # send a valid start so server gets a delivery
            request = b'PUSH / HTTP/1.1\r\n' \
                + b'Transfer-Encoding: chunked\r\n' \
                + b'\r\n' \
                + b'10\r\n' \
                + b'ABCDEFGHIJKLMNOP\r\n'

            client.sendall(request)

            _ = _read_socket(server, length=len(request))
            self.assertEqual(len(request), len(_))

            # now send a bad chunk header
            client.sendall(b'GERBIL\r\n')

            # the error should be detected on the client facing router and the
            # connection should be dropped (timeout if not dropped)
            _ = _read_socket(client, length=4096)

            # since the message was in-flight at the server the connection
            # should drop there as well
            _ = _read_socket(server, length=4096)

            client.close()
            server.shutdown(socket.SHUT_RDWR)
            server.close()

            # repeat the test, but have the server response attempt to send an
            # invalid chunk

            server, addr = listener.accept()
            wait_http_listeners_up(self.INT_A.listener, l_filter={'name': 'L_testServer'})

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.settimeout(TIMEOUT)
            retry_exception(lambda cs=client:
                            cs.connect(("127.0.0.1", self.http_listener_port)),
                            delay=0.25,
                            exception=ConnectionRefusedError)

            request = b'GET / HTTP/1.1\r\n' \
                + b'Content-length: 0\r\n' \
                + b'\r\n'

            client.sendall(request)

            _ = _read_socket(server, length=len(request))
            self.assertEqual(len(request), len(_))

            response = b'HTTP/1.1 200 OK\r\n' \
                + b'Transfer-Encoding: chunked\r\n' \
                + b'\r\n' \
                + b'10\r\n' \
                + b'ABCDEFGHIJKLMNOP\r\n'

            server.sendall(response)

            _ = _read_socket(client, length=len(response))
            self.assertEqual(len(response), len(_))

            server.sendall(b'HAMSTER\r\n')

            # the error should be detected on the server facing router and the
            # connection should be dropped (timeout if not dropped)
            _ = _read_socket(server, length=4096)

            # and the client connection should drop as well
            _ = _read_socket(client, length=4096)

            client.close()
            server.shutdown(socket.SHUT_RDWR)
            server.close()

        wait_http_listeners_down(self.INT_A.listener, l_filter={'name': 'L_testServer'})


class Http1AdaptorQ2Standalone(TestCase):
    """
    Force Q2 blocking/recovery on both client and server endpoints. This test
    uses a single router to ensure both client facing and server facing
    Q2 components of the HTTP/1.x adaptor are triggered.
    """

    def create_router(self, name):
        """
        Single router configuration with one HTTPListener and one
        HTTPConnector.
        """
        http_server_port = self.tester.get_port()
        http_listener_port = self.tester.get_port()

        config = [
            ('router', {'mode': 'standalone',
                        'id': name}),
            ('listener', {'role': 'normal',
                          'port': self.tester.get_port()}),
            ('httpListener', {'port': http_listener_port,
                              'protocolVersion': 'HTTP1',
                              'address': 'testServer'}),
            ('httpConnector', {'port': http_server_port,
                               'host': '127.0.0.1',
                               'protocolVersion': 'HTTP1',
                               'address': 'testServer'}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        router = self.tester.qdrouterd(name, config, wait=True)
        router.listener = router.addresses[0]
        return (router, http_listener_port, http_server_port)

    def _write_until_full(self, sock, data, timeout):
        """
        Write data to socket until either all data written or timeout.
        Return the number of bytes written, which will == len(data) if timeout
        not hit
        """
        sock.setblocking(0)
        sent = 0

        while sent < len(data):
            try:
                _, rw, _ = select.select([], [sock], [], timeout)
            except select.error as serror:
                if serror[0] == errno.EINTR:
                    print("ignoring interrupt from select(): %s" % str(serror))
                    continue
                raise  # assuming fatal...
            if rw:
                sent += sock.send(data[sent:])
            else:
                break  # timeout
        return sent

    def _read_until_empty(self, sock, timeout):
        """
        Read data from socket until timeout occurs.  Return read data.
        """

        # Trying to use SSLSocket with select is an exercise in pain. Just
        # don't do it.
        assert not isinstance(sock, SSLSocket)

        sock.setblocking(0)
        data = b''

        while True:
            try:
                rd, _, _ = select.select([sock], [], [], timeout)
            except select.error as serror:
                if serror[0] == errno.EINTR:
                    print("ignoring interrupt from select(): %s" % str(serror))
                    continue
                raise  # assuming fatal...
            if rd:
                data += sock.recv(4096)
            else:
                break  # timeout
        return data

    def check_logs(self, prefix, log_file):
        """Check router log for proper block/unblock activity"""
        block_ct = 0
        unblock_ct = 0
        block_line = 0
        unblock_line = 0
        with open(log_file) as f:
            for line_no, line in enumerate(f, start=1):
                if '%s link blocked on Q2 limit' % prefix in line:
                    block_ct += 1
                    block_line = line_no
                if '%s link unblocked from Q2 limit' % prefix in line:
                    unblock_ct += 1
                    unblock_line = line_no
        self.assertGreater(block_ct, 0)
        self.assertGreaterEqual(unblock_ct, block_ct)
        self.assertGreater(unblock_line, block_line)

    def test_01_backpressure_client(self):
        """
        Trigger Q2 backpressure against the HTTP client.
        """
        # Q2 is disabled on egress http1 messages to prevent an
        # irrecoverable stall - see Issue #754
        self.skipTest("ISSUE #754 workaround causes this test to fail")

        router, listener_port, server_port = self.create_router("Q2Router1")

        # create a listener socket to act as the server service
        server_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_listener.settimeout(TIMEOUT)
        server_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_listener.bind(('', server_port))
        server_listener.listen(1)

        # block until router connects
        server_sock, host_port = server_listener.accept()
        server_sock.settimeout(0.5)
        server_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        wait_http_listeners_up(router.addresses[0])

        # create a client connection to the router
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        retry_exception(lambda: client_sock.connect((host_port[0],
                                                     listener_port)),
                        delay=0.25,
                        exception=ConnectionRefusedError)
        client_sock.settimeout(0.5)

        # send a Very Large PUSH request, expecting it to block at some point

        push_req_hdr = b'PUSH / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n'
        count = self._write_until_full(client_sock, push_req_hdr, 1.0)
        self.assertEqual(len(push_req_hdr), count)

        chunk = b'8000\r\n' + b'X' * 0x8000 + b'\r\n'
        last_chunk = b'0 \r\n\r\n'
        count = 0
        deadline = time() + TIMEOUT
        while deadline >= time():
            count = self._write_until_full(client_sock, chunk, 5.0)
            if count < len(chunk):
                break
        self.assertFalse(time() > deadline,
                         "Client never blocked as expected!")

        # client should now be in Q2 block. Drain the server to unblock Q2
        _ = self._read_until_empty(server_sock, 2.0)

        # finish the PUSH
        if count:
            remainder = self._write_until_full(client_sock, chunk[count:], 1.0)
            self.assertEqual(len(chunk), count + remainder)

        count = self._write_until_full(client_sock, last_chunk, 1.0)
        self.assertEqual(len(last_chunk), count)

        # receive the request and reply
        _ = self._read_until_empty(server_sock, 2.0)

        response = b'HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n'
        count = self._write_until_full(server_sock, response, 1.0)
        self.assertEqual(len(response), count)

        # complete the response read
        _ = self._read_until_empty(client_sock, 2.0)
        self.assertEqual(len(response), len(_))

        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()

        server_sock.shutdown(socket.SHUT_RDWR)
        server_sock.close()

        server_listener.shutdown(socket.SHUT_RDWR)
        server_listener.close()

        router.teardown()
        self.check_logs("client", router.logfile_path)

    def test_02_backpressure_server(self):
        """
        Trigger Q2 backpressure against the HTTP server.
        """
        # Q2 is disabled on egress http1 messages to prevent an
        # irrecoverable stall - see Issue #754
        self.skipTest("ISSUE #754 workaround causes this test to fail")

        router, listener_port, server_port = self.create_router("Q2Router2")

        small_get_req = b'GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n'

        # create a listener socket to act as the server service
        server_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_listener.settimeout(TIMEOUT)
        server_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_listener.bind(('', server_port))
        server_listener.listen(1)

        # block until router connects
        server_sock, host_port = server_listener.accept()
        server_sock.settimeout(0.5)
        server_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        wait_http_listeners_up(router.addresses[0])

        # create a client connection to the router
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        retry_exception(lambda: client_sock.connect((host_port[0],
                                                     listener_port)),
                        delay=0.25,
                        exception=ConnectionRefusedError)
        client_sock.settimeout(0.5)

        # send GET request - expect this to be successful
        count = self._write_until_full(client_sock, small_get_req, 1.0)
        self.assertEqual(len(small_get_req), count)

        request = self._read_until_empty(server_sock, 5.0)
        self.assertEqual(len(small_get_req), len(request))

        # send a Very Long response, expecting it to block at some point
        chunk = b'8000\r\n' + b'X' * 0x8000 + b'\r\n'
        last_chunk = b'0 \r\n\r\n'
        response = b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'

        count = self._write_until_full(server_sock, response, 1.0)
        self.assertEqual(len(response), count)

        count = 0
        deadline = time() + TIMEOUT
        while deadline >= time():
            count = self._write_until_full(server_sock, chunk, 5.0)
            if count < len(chunk):
                break
        self.assertFalse(time() > deadline,
                         "Server never blocked as expected!")

        # server should now be in Q2 block. Drain the client to unblock Q2
        _ = self._read_until_empty(client_sock, 2.0)

        # finish the response
        if count:
            remainder = self._write_until_full(server_sock, chunk[count:], 1.0)
            self.assertEqual(len(chunk), count + remainder)

        count = self._write_until_full(server_sock, last_chunk, 1.0)
        self.assertEqual(len(last_chunk), count)
        server_sock.shutdown(socket.SHUT_RDWR)
        server_sock.close()

        _ = self._read_until_empty(client_sock, 1.0)
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()

        server_listener.shutdown(socket.SHUT_RDWR)
        server_listener.close()

        router.teardown()
        self.check_logs("server", router.logfile_path)


class Http1AdaptorAggregationTest(TestCase):
    """
    """
    @classmethod
    def setUpClass(cls):
        super(Http1AdaptorAggregationTest, cls).setUpClass()

        # ports for testing JSON and MULTIPART aggregation
        cls.json_server1_port = cls.tester.get_port()
        cls.json_server2_port = cls.tester.get_port()
        cls.json_listener_port = cls.tester.get_port()

        cls.mpart_server1_port = cls.tester.get_port()
        cls.mpart_server2_port = cls.tester.get_port()
        cls.mpart_listener_port = cls.tester.get_port()

        config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTP1Aggregator'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),

            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),

            ('httpConnector', {
                'name': 'httpConnectorJsonServer1',
                'port': cls.json_server1_port,
                'host': '127.0.0.1',
                'protocolVersion': 'HTTP1',
                'aggregation': 'json',
                'address': 'multicast/json'}),
            ('httpConnector', {
                'name': 'httpConnectorJsonServer2',
                'port': cls.json_server2_port,
                'host': '127.0.0.1',
                'protocolVersion': 'HTTP1',
                'aggregation': 'json',
                'address': 'multicast/json'}),
            ('httpListener', {
                'name': 'httpListenerJson',
                'port': cls.json_listener_port,
                'host': '0.0.0.0',
                'protocolVersion': 'HTTP1',
                'aggregation': 'json',
                'address': 'multicast/json'}),

            ('httpConnector', {
                'name': 'httpConnectorMPartServer1',
                'port': cls.mpart_server1_port,
                'host': '127.0.0.1',
                'protocolVersion': 'HTTP1',
                'aggregation': 'multipart',
                'address': 'multicast/multipart'}),
            ('httpConnector', {
                'name': 'httpConnectorMPartServer2',
                'port': cls.mpart_server2_port,
                'host': '127.0.0.1',
                'protocolVersion': 'HTTP1',
                'aggregation': 'multipart',
                'address': 'multicast/multipart'}),
            ('httpListener', {
                'name': 'httpListenerMPart',
                'port': cls.mpart_listener_port,
                'host': '0.0.0.0',
                'protocolVersion': 'HTTP1',
                'aggregation': 'multipart',
                'address': 'multicast/multipart'}),
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('HTTP1Aggregator', config, wait=False)
        # do not wait for connectors - servers haven't been created yet
        cls.router.wait_ports()
        cls.router.wait_startup_message()

    @unittest.skipIf(not curl_available(), "test required 'curl' command not found")
    def test_json_aggregation(self):
        """
        """
        SERVER1_TEST = {
            "GET": [
                (RequestMsg("GET", "/GET/json_aggregation",
                            headers={"Content-Length": 0}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": 24,
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'server1 json aggregation'),
                 None)
            ]
        }

        SERVER2_TEST = {
            "GET": [
                (RequestMsg("GET", "/GET/json_aggregation",
                            headers={"Content-Length": 0}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": 24,
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'server2 json aggregation'),
                 None)
            ]
        }

        # Cannot use a context for the servers since this is multicast and
        # __exit__ing one will shutdown both causing the second call to
        # __exit__ to fail

        try:
            server1 = TestServer.new_server(self.json_server1_port,
                                            self.json_listener_port,
                                            SERVER1_TEST)
        except Exception as exc:
            print(f"Failed to start server1: {exc}", flush=True)
            raise

        try:
            server2 = TestServer.new_server(self.json_server2_port,
                                            self.json_listener_port,
                                            SERVER2_TEST)
        except Exception as exc:
            print(f"Failed to start server2: {exc}", flush=True)
            server1.wait()
            raise

        try:
            self.router.wait_address('multicast/json', subscribers=2)
            wait_http_listeners_up(self.router.addresses[0],
                                   l_filter={'name': 'httpListenerJson'})

            url = f"http://127.0.0.1:{self.json_listener_port}/GET/json_aggregation"
            args = ['--http1.1', '-G', url, '--connect-timeout', str(TIMEOUT)]
            returncode, out, err = run_curl(args)
            self.assertEqual(0, returncode, "curl returned a failure code")
            response = json.loads(out)  # throws if response is invalid json
            self.assertEqual(2, len(response), "expected two json parts!")
            for part in response:
                self.assertIn(part['body'],
                              ['server1 json aggregation', 'server2 json aggregation'],
                              "Unexpected json body!")

        finally:
            server1.wait()  # this will shutdown both servers
            self.router.wait_address_unsubscribed('multicast/json')

    @unittest.skipIf(not curl_available(), "test required 'curl' command not found")
    def test_multipart_aggregation(self):
        """
        """
        SERVER1_TEST = {
            "GET": [
                (RequestMsg("GET", "/GET/multipart_aggregation",
                            headers={"Content-Length": 0}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": 29,
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'server1 multipart aggregation'),
                 None)
            ]
        }

        SERVER2_TEST = {
            "GET": [
                (RequestMsg("GET", "/GET/multipart_aggregation",
                            headers={"Content-Length": 0}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": 29,
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'server2 multipart aggregation'),
                 None)
            ]
        }

        # Cannot use a context for the servers since this is multicast and
        # __exit__ing one will shutdown both causing the second call to
        # __exit__ to fail

        try:
            server1 = TestServer.new_server(self.mpart_server1_port,
                                            self.mpart_listener_port,
                                            SERVER1_TEST)
        except Exception as exc:
            print(f"Failed to start server1: {exc}", flush=True)
            raise

        try:
            server2 = TestServer.new_server(self.mpart_server2_port,
                                            self.mpart_listener_port,
                                            SERVER2_TEST)
        except Exception as exc:
            print(f"Failed to start server2: {exc}", flush=True)
            server1.wait()
            raise

        try:
            self.router.wait_address('multicast/multipart', subscribers=2)
            wait_http_listeners_up(self.router.addresses[0],
                                   l_filter={'name': 'httpListenerMPart'})

            # normally I'd use curl, but for the life of me I cannot get it to
            # return the full raw http response message, which is necessary to
            # validate the multipart body...

            request = b'GET /GET/multipart_aggregation HTTP/1.1\r\n' \
                + b'Content-Length: 0\r\n' \
                + b'Connection: close\r\n' \
                + b'\r\n'

            reply = http1_simple_request(request, port=self.mpart_listener_port)

            # mime parser chokes on HTTP response line, remove it:
            prefix = b'HTTP/1.1 200\r\n'
            self.assertTrue(reply.startswith(prefix))
            reply = reply[len(prefix):]

            bp = BytesParser()
            mimetype = bp.parsebytes(reply)
            self.assertTrue(mimetype.is_multipart())
            count = 0
            for part in mimetype.walk():
                if "text/plain" in part.get_content_type():
                    self.assertIn(part.get_payload(),
                                  ["server1 multipart aggregation",
                                   "server2 multipart aggregation"],
                                  f"unexpected multipart body! {mimetype.as_string()}")
                    count += 1

            self.assertEqual(2, count, f"expected 2 mimetype parts {mimetype.as_string()}")

        finally:
            server1.wait()  # this will shutdown both servers
            self.router.wait_address_unsubscribed('multicast/multipart')


class Http1AdaptorEventChannelTest(TestCase):
    """Verify the event-channel functionality."""
    @classmethod
    def setUpClass(cls):
        super(Http1AdaptorEventChannelTest, cls).setUpClass()

        cls.server_port = cls.tester.get_port()
        cls.listener_port = cls.tester.get_port()

        config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTPEventChannel'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),

            ('httpConnector', {
                'name': 'httpConnector1',
                'port': cls.server_port,
                'host': '127.0.0.1',
                'protocolVersion': 'HTTP1',
                'eventChannel': 'true',
                'address': 'closest/EventChannel'}),
            ('httpListener', {
                'name': 'httpListener1',
                'port': cls.listener_port,
                'host': '0.0.0.0',
                'protocolVersion': 'HTTP1',
                'eventChannel': 'true',
                'address': 'closest/EventChannel'}),

            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('HTTP1EventChannel', config, wait=False)
        # do not wait for connectors - servers haven't been created yet
        cls.router.wait_ports()
        cls.router.wait_startup_message()

    @unittest.skipIf(not curl_available(), "test required 'curl' command not found")
    def test_event_channel_curl(self):
        """Verify event channel post via curl"""
        TESTS = {
            "POST": [
                (RequestMsg("POST", "/POST/EventChannel/1",
                            headers={"Content-Length": 33},
                            body=b'This is not used dummy test entry'),
                 ResponseMsg(204, reason="No Content",
                             headers={"Content-Length": 0,
                                      "Dummy-header": "OOGA BOOGA!"}),
                 None),
            ]
        }

        # Cannot use a context for the servers since this is POST event channel
        # which does not return a response. Exiting the context will cause
        # a POST /SHUTDOWN to occur, but the test will fail waiting for a
        # response to the shutdown command

        try:
            server = TestServer.new_server(self.server_port,
                                           self.listener_port,
                                           TESTS)
        except Exception as exc:
            print(f"Failed to start test server: {exc}", flush=True)
            raise

        try:
            self.router.wait_address('closest/EventChannel', subscribers=1)
            wait_http_listeners_up(self.router.addresses[0])

            # try a simple POST via curl:

            url = f"http://127.0.0.1:{self.listener_port}/POST/EventChannel/1"
            args = ['--http1.1', '-d', '@-', '--connect-timeout', str(TIMEOUT),
                    '--show-error', '--silent', url]
            try:
                returncode, out, err = run_curl(args, input='X' * 1000)
                self.assertEqual(0, returncode, "curl returned a failure code")
                self.assertEqual(0, len(out), f"Unexpected content returned: '{out}'")
                self.assertEqual(0, len(err), f"Unexpected error returned: '{err}'")
            except Exception as exc:
                print(f"CURL FAILED: '{exc}'", flush=True)
                raise

        finally:
            server.wait(check_reply=False)
            self.router.wait_address_unsubscribed('closest/EventChannel')

    def test_event_channel_raw(self):
        """Test event channel post and error requests"""
        TESTS = {
            "POST": [
                (RequestMsg("POST", "/POST/EventChannel/1",
                            headers={"Content-Length": 33},
                            body=b'This is not used dummy test entry'),
                 ResponseMsg(204, reason="No Content",
                             headers={"Content-Length": 0,
                                      "Dummy-header": "OOGA BOOGA!"}),
                 None),
            ],
            "GET": [
                (RequestMsg("GET", "/POST/EventChannel/1",
                            headers={"Content-Length": 33},
                            body=b'This is not used dummy test entry'),
                 ResponseMsg(204, reason="No Content",
                             headers={"Content-Length": 0,
                                      "Dummy-header": "OOGA BOOGA!"}),
                 None),
            ]
        }

        # Cannot use a context for the servers since this is POST event channel
        # which does not return a response. Exiting the context will cause
        # a POST /SHUTDOWN to occur, but the test will fail waiting for a
        # response to the shutdown command

        try:
            server = TestServer.new_server(self.server_port,
                                           self.listener_port,
                                           TESTS)
        except Exception as exc:
            print(f"Failed to start test server: {exc}", flush=True)
            raise

        try:
            self.router.wait_address('closest/EventChannel', subscribers=1)
            wait_http_listeners_up(self.router.addresses[0])

            # try a simple POST:

            request = b'POST /POST/EventChannel/1 HTTP/1.1\r\n' \
                + b'Content-Length: 19\r\n' \
                + b'Connection: close\r\n' \
                + b'\r\n' \
                + b'This is a POST body'
            expect = b'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n'

            reply = http1_simple_request(request, port=self.listener_port)
            self.assertEqual(expect, reply, "Expected response not received")

            # error case - non-POST command:

            request = b'GET /GET/EventChannel/1 HTTP/1.1\r\n' \
                + b'Content-Length: 18\r\n' \
                + b'\r\n' \
                + b'This is a GET body'
            expect = b'HTTP/1.1 405 Method Not Allowed\r\n' \
                + b'Content-Length: 68\r\n' \
                + b'Content-Type: text/plain\r\n\r\n' \
                + b'Invalid method for event channel httpListener, only POST is allowed.'

            reply = http1_simple_request(request, port=self.listener_port)
            self.assertEqual(expect, reply, "Expected error response not received")

            # non-error case - pipelined POST commands:

            request = b'POST /POST/EventChannel/1 HTTP/1.1\r\n' \
                + b'Content-Length: 27\r\n' \
                + b'\r\n' \
                + b'This is the first POST body'
            request += b'POST /POST/EventChannel/2 HTTP/1.1\r\n' \
                + b'Content-Length: 28\r\n' \
                + b'Connection: close\r\n' \
                + b'\r\n' \
                + b'This is the second POST body'
            expect = b'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n' \
                + b'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n'

            reply = http1_simple_request(request, port=self.listener_port)
            self.assertEqual(expect, reply, "Expected pipelined response not received")

        finally:
            server.wait(check_reply=False)
            self.router.wait_address_unsubscribed('closest/EventChannel')


class Http1TlsBadConfigTests(HttpTlsBadConfigTestsBase):
    PROTOCOL_VERSION = "HTTP1"

    @classmethod
    def setUpClass(cls):
        super(Http1TlsBadConfigTests, cls).setUpClass()

    def test_connector_mgmt_missing_ssl_profile(self):
        self._test_connector_mgmt_missing_ssl_profile()

    def test_connector_mgmt_missing_ca_file(self):
        self._test_connector_mgmt_missing_ca_file()

    def test_listener_mgmt_missing_ssl_profile(self):
        self._test_listener_mgmt_missing_ssl_profile()

    def test_listener_mgmt_missing_ca_file(self):
        self._test_listener_mgmt_missing_ca_file()


class Http1TLSConnectorErrorTests(TestCase):
    """Test server-facing connector error handling"""
    @classmethod
    def setUpClass(cls):
        super(Http1TLSConnectorErrorTests, cls).setUpClass()

    def test_001_cleartext_reject(self):
        """Attempt to connect to a server that does not use TLS"""

        mgmt_port = self.tester.get_port()
        r_config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTP1ConnectorErrorTests001'}),
            ('listener', {'role': 'normal',
                          'port': mgmt_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        r_config = Qdrouterd.Config(r_config)
        with self.tester.qdrouterd('HTTP1ConnectorErrorTests001', r_config,
                                   wait=True) as router:
            mgmt = router.qd_manager

            server_port = self.tester.get_port()
            mgmt.create("sslProfile",
                        {'name': 'SP_test_001_cleartext_reject',
                         'caCertFile': CA_CERT})
            mgmt.create("httpConnector",
                        {"name": "C_test_001_cleartext_reject",
                         "address": "test_001_cleartext_reject",
                         'host': 'localhost',
                         'port': server_port,
                         'protocolVersion': 'HTTP1',
                         'sslProfile': 'SP_test_001_cleartext_reject'})

            # test server:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.settimeout(TIMEOUT)
                server.bind(("localhost", server_port))
                server.listen(1)
                conn, addr = server.accept()

                def _io(self):
                    while True:
                        conn.send(b'This is not TLS')
                        ignore = conn.recv(4096)

                self.assertRaises(OSError, _io, self)
                conn.close()
            router.wait_log_message(pattern=r"TLS connection failed")

            # Ensure router can still connect given the proper TLS config.
            # If the handshake fails an error will be raised by server.accept()

            ctxt = SSLContext(protocol=PROTOCOL_TLS_SERVER)
            ctxt.load_verify_locations(cafile=CA_CERT)
            ctxt.load_cert_chain(SERVER_CERTIFICATE,
                                 SERVER_PRIVATE_KEY,
                                 "server-password")
            # ctxt.verify_mode = CERT_REQUIRED
            # ctxt.check_hostname = False

            with ctxt.wrap_socket(socket.socket(socket.AF_INET,
                                                socket.SOCK_STREAM),
                                  server_side=True) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.settimeout(TIMEOUT)
                server.bind(("localhost", server_port))
                server.listen(1)
                conn, addr = server.accept()
                conn.close()

    def test_002_client_cert_fail(self):
        """Handle a handshake failure initiated by the server due to a missing
        client certificate
        """
        mgmt_port = self.tester.get_port()
        r_config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTP1ConnectorErrorTests002'}),
            ('listener', {'role': 'normal',
                          'port': mgmt_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        r_config = Qdrouterd.Config(r_config)
        with self.tester.qdrouterd('HTTP1ConnectorErrorTests002', r_config,
                                   wait=True) as router:
            mgmt = router.qd_manager
            server_port = self.tester.get_port()

            # this profile does not provide a client cert
            mgmt.create("sslProfile",
                        {'name': 'SP_test_002_client_cert_fail',
                         'caCertFile': CA_CERT})
            mgmt.create("httpConnector",
                        {"name": 'C_test_002_client_cert_fail',
                         "address": 'test_002_client_cert_fail',
                         'host': 'localhost',
                         'port': server_port,
                         'protocolVersion': 'HTTP1',
                         'sslProfile': 'SP_test_002_client_cert_fail'})

            # This test server expects a client cert from the router. Expect
            # the handshake to fail
            ctxt = SSLContext(protocol=PROTOCOL_TLS_SERVER)
            ctxt.load_verify_locations(cafile=CA_CERT)
            ctxt.load_cert_chain(SERVER_CERTIFICATE,
                                 SERVER_PRIVATE_KEY,
                                 "server-password")
            ctxt.verify_mode = CERT_REQUIRED

            with ctxt.wrap_socket(socket.socket(socket.AF_INET,
                                                socket.SOCK_STREAM),
                                  server_side=True) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.settimeout(TIMEOUT)
                server.bind(("localhost", server_port))
                server.listen(1)
                self.assertRaises(OSError, server.accept)

            router.wait_log_message(pattern=r"TLS connection failed")

            # Ensure router can still connect given the proper TLS config.

            ctxt = SSLContext(protocol=PROTOCOL_TLS_SERVER)
            ctxt.load_verify_locations(cafile=CA_CERT)
            ctxt.load_cert_chain(SERVER_CERTIFICATE,
                                 SERVER_PRIVATE_KEY,
                                 "server-password")
            with ctxt.wrap_socket(socket.socket(socket.AF_INET,
                                                socket.SOCK_STREAM),
                                  server_side=True) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.settimeout(TIMEOUT)
                server.bind(("localhost", server_port))
                server.listen(1)
                conn, addr = server.accept()
                conn.close()


class Http1TLSListenerErrorTests(TestCase):
    """Test client-facing listener error handling"""
    @classmethod
    def setUpClass(cls):
        super(Http1TLSListenerErrorTests, cls).setUpClass()
        cls.connector_port = cls.tester.get_port()
        cls.listener_port = cls.tester.get_port()
        r_config = [
            ('router', {'mode': 'interior',
                        'id': 'HTTP1TLSListenerErrorTests'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ('httpConnector', {'name': 'C_HTTP1TLSListenerErrorTests',
                               'address': 'HTTP1TLSListenerErrorTests',
                               'host': 'localhost',
                               'port': cls.connector_port,
                               'protocolVersion': 'HTTP1'}),
            ('sslProfile', {'name': 'SP_HTTP1TLSListenerErrorTests',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': "server-password"}),
            ('httpListener', {'name': 'L_HTTP1TLSListenerErrorTests',
                              'address': 'HTTP1TLSListenerErrorTests',
                              'host': 'localhost',
                              'port': cls.listener_port,
                              'protocolVersion': 'HTTP1',
                              'sslProfile': 'SP_HTTP1TLSListenerErrorTests',
                              'authenticatePeer': True})
        ]
        r_config = Qdrouterd.Config(r_config)
        cls.router = cls.tester.qdrouterd('HTTP1TLSListenerErrorTests', r_config, wait=False)

    @unittest.skipIf(not curl_available(), "test required 'curl' command not found")
    def test_001_client_failures(self):
        """Test various client errors"""
        TEST = {
            "GET": [
                (RequestMsg("GET", "/GET/ping",
                            headers={"Content-Length": 0}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": 4,
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'pong'),
                 ResponseValidator(expect_body=b'pong'))
            ]
        }

        ctxt = SSLContext(protocol=PROTOCOL_TLS_CLIENT)
        ctxt.load_verify_locations(cafile=CA_CERT)
        ctxt.load_cert_chain(CLIENT_CERTIFICATE,
                             CLIENT_PRIVATE_KEY,
                             "client-password")
        ctxt.verify_mode = CERT_REQUIRED
        ctxt.check_hostname = True

        with TestServer.new_server(self.connector_port, self.listener_port,
                                   TEST, client_ssl_context=ctxt) as server:
            self.router.wait_ready()
            wait_http_listeners_up(self.router.addresses[0])

            # verify a good client can connect

            client = ThreadedTestClient(TEST,
                                        f"localhost:{self.listener_port}",
                                        ssl_context=ctxt)
            client.wait()
            client.check_count(1)

            # now attempt to attach without TLS - should fail

            url = f"http://localhost:{self.listener_port}/GET/ping"
            args = ['--http1.1', '--show-error', '--silent', '-G', url]
            rc, out, err = run_curl(args)
            self.assertNotEqual(0, rc, f"Expected curl to fail: out={out}")

            # connect without a self identifying certificate should also fail

            url = f"https://localhost:{self.listener_port}/GET/ping"
            args = ['--http1.1',
                    '--cacert', CA_CERT,
                    '--show-error', '--silent', '-G', url]
            rc, out, err = run_curl(args)
            self.assertNotEqual(0, rc, f"Expected curl to fail: out={out}")

            # test properly configured client - should connect ok

            url = f"https://localhost:{self.listener_port}/GET/ping"
            args = ['--http1.1',
                    '--cacert', CA_CERT,
                    '--cert', f"{CLIENT_CERTIFICATE}:client-password",
                    '--key', CLIENT_PRIVATE_KEY,
                    '--show-error', '--silent', '-G', url]
            rc, out, err = run_curl(args)
            self.assertEqual(0, rc, f"Expected curl fail: rc={rc} err={err}")
            self.assertEqual('pong', out, f"Expected 'pong', got {out}")


@unittest.skipIf(not nginx_available() or not curl_available(), "both nginx and curl needed")
class Http1AdaptorTwoRouterNginxTLS(TestCase):
    """
    Verify curl requests across two routers to an nginx server
    """
    @classmethod
    def setUpClass(cls):
        super(Http1AdaptorTwoRouterNginxTLS, cls).setUpClass()

        # configuration:
        # two interiors
        #
        #  +------+    +---------+
        #  | INTA |<==>|  INT.B  |
        #  +------+    +---------+
        #     ^             ^
        #     |             |
        #     V             V
        #   <curl>       <nginx>

        cls.interior_port = cls.tester.get_port()
        cls.http_server_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()

        # INTA
        config = [
            ('router', {'mode': 'interior',
                        'id': 'INTA'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('connector', {'name': 'backbone',
                           'role': 'inter-router',
                           'host': '127.0.0.1',
                           'port': cls.interior_port}),
            ('sslProfile', {'name': 'ListenerSSLProfile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': "server-password"}),
            ('httpListener', {'name': 'L_curl',
                              'port': cls.http_listener_port,
                              'protocolVersion': 'HTTP1',
                              'address': 'closest/nginx',
                              'sslProfile': 'ListenerSSLProfile',
                              'authenticatePeer': True}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.INTA = cls.tester.qdrouterd('INTA', config, wait=False)

        # INTB
        config = [
            ('router', {'mode': 'interior',
                        'id': 'INTB'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('listener', {'name': 'backbone',
                          'role': 'inter-router',
                          'port': cls.interior_port}),
            ('sslProfile', {'name': 'ConnectorSSLProfile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': "client-password"}),
            ('httpConnector', {'name': 'C_nginx',
                               'host': 'localhost',
                               'port': cls.http_server_port,
                               'protocolVersion': 'HTTP1',
                               'address': 'closest/nginx',
                               'sslProfile': 'ConnectorSSLProfile',
                               'verifyHostname': True}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.INTB = cls.tester.qdrouterd('INTB', config, wait=False)

        env = dict()
        env['nginx-base-folder'] = NginxServer.BASE_FOLDER
        env['setupclass-folder'] = cls.tester.directory
        env['nginx-configs-folder'] = NginxServer.CONFIGS_FOLDER
        env['listening-port'] = str(cls.http_server_port)
        env['http2'] = ''  # disable HTTP/2
        env['ssl'] = 'ssl'
        env['tls-enabled'] = ''  # Will enable TLS lines

        # TLS stuff
        env['chained-pem'] = CHAINED_PEM
        env['server-private-key-no-pass-pem'] = SERVER_KEY_NO_PASS
        env['ssl-verify-client'] = 'on'
        env['ca-certificate'] = CA_CERT
        cls.nginx_server = cls.tester.nginxserver(config_path=NginxServer.CONFIG_FILE,
                                                  env=env)

        # wait for everything to connect and settle
        cls.INTA.wait_ready()
        cls.INTB.wait_ready()
        wait_http_listeners_up(cls.INTA.addresses[0])

        # curl will use these additional args to connect to the router.
        cls.curl_args = ['--http1.1',
                         '--cacert', CA_CERT,
                         '--cert-type', 'PEM',
                         '--cert', f"{CLIENT_CERTIFICATE}:client-password",
                         '--key', CLIENT_PRIVATE_KEY]

    def test_get_image_jpg(self):
        images = ['test.jpg',
                  'pug.png',
                  'skupper.png',
                  'skupper-logo-vertical.svg']

        for image in images:
            in_file = os.path.join(NginxServer.IMAGES_FOLDER, image)
            out_file = os.path.join(self.INTA.outdir, 'curl-images', image)
            url = f"https://localhost:{self.http_listener_port}/images/{image}"
            (rc, _, err) = run_curl(args=self.curl_args +
                                    ['--verbose',
                                     '--create-dirs',
                                     '--output', out_file,
                                     '-G', url])
            self.assertEqual(0, rc, f"curl failed: {err}")
            self.assertEqual(get_digest(in_file), get_digest(out_file),
                             f"Error: {out_file} corrupted by HTTP/1 transfer!")

    def test_get_html(self):
        pages = ['index.html', 't100K.html', 't10K.html', 't1K.html']
        for page in pages:
            in_file = os.path.join(NginxServer.HTML_FOLDER, page)
            out_file = os.path.join(self.INTA.outdir, 'curl-html', page)
            url = f"https://localhost:{self.http_listener_port}/{page}"
            (rc, _, err) = run_curl(args=self.curl_args +
                                    ['--verbose',
                                     '--create-dirs',
                                     '--output', out_file,
                                     '-G', url])
            self.assertEqual(0, rc, f"curl failed: {err}")
            self.assertEqual(get_digest(in_file), get_digest(out_file),
                             f"Error: {out_file} corrupted by HTTP/1 transfer!")


if __name__ == '__main__':
    unittest.main(main_module())

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
import select
import socket
from time import sleep, time
from typing import Optional
from email.parser import BytesParser
from http.client import HTTPConnection

from proton import Message
from system_test import TestCase, unittest, main_module, Qdrouterd, QdManager
from system_test import TIMEOUT, AsyncTestSender, AsyncTestReceiver
from system_test import retry_exception, curl_available, run_curl
from http1_tests import http1_ping, TestServer, RequestHandler10
from http1_tests import RequestMsg, ResponseMsg
from http1_tests import ThreadedTestClient, Http1OneRouterTestBase
from http1_tests import CommonHttp1OneRouterTest
from http1_tests import CommonHttp1Edge2EdgeTest
from http1_tests import Http1Edge2EdgeTestBase
from http1_tests import Http1ClientCloseTestsMixIn
from http1_tests import Http1CurlTestsMixIn
from http1_tests import wait_http_listeners_up
from http1_tests import HttpAdaptorListenerConnectTestBase


class SimpleRequestTimeout(Exception):
    """Thrown by http1_simple_request when timeout occurs during socket
    read. Contains any reply data read from the server prior to the timeout
    """
    pass


def http1_simple_request(raw_request: bytes,
                         port: int,
                         host: Optional[str] = '127.0.0.1',
                         timeout: Optional[float] = TIMEOUT):
    """Perform a simple HTTP/1 request and return the response read from the
    server. raw_request is a complete HTTP/1 request message. The client socket
    will be read from until either it is closed or the TIMEOUT occurs.

    It is expected that raw_request will include a "Connection: close" header
    to prevent the TIMEOUT exception from being thrown.
    """
    reply = b''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.settimeout(timeout)

        # allow time for the listening socket to initialize
        retry_exception(lambda: client.connect((host, port)),
                        delay=0.25, exception=ConnectionRefusedError)
        client.sendall(raw_request, socket.MSG_WAITALL)
        try:
            while True:
                rc = client.recv(4096)
                if not rc:
                    # socket closed (response complete)
                    break
                reply += rc
        except TimeoutError:
            raise SimpleRequestTimeout(reply)
    return reply


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

        cls.routers = []
        super(Http1AdaptorOneRouterTest, cls).\
            router('INT.A', 'standalone',
                   [('httpConnector', {'port': cls.http_server11_port,
                                       'host': '127.0.0.1',
                                       'protocolVersion': 'HTTP1',
                                       'address': 'testServer11'}),
                    ('httpConnector', {'port': cls.http_server10_port,
                                       'host': '127.0.0.1',
                                       'protocolVersion': 'HTTP1',
                                       'address': 'testServer10'}),
                    ('httpListener', {'port': cls.http_listener11_port,
                                      'protocolVersion': 'HTTP1',
                                      'host': '127.0.0.1',
                                      'address': 'testServer11'}),
                    ('httpListener', {'port': cls.http_listener10_port,
                                      'protocolVersion': 'HTTP1',
                                      'host': '127.0.0.1',
                                      'address': 'testServer10'})
                    ])

        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        cls.http11_server = TestServer.new_server(server_port=cls.http_server11_port,
                                                  client_port=cls.http_listener11_port,
                                                  tests=cls.TESTS_11)
        cls.http10_server = TestServer.new_server(server_port=cls.http_server10_port,
                                                  client_port=cls.http_listener10_port,
                                                  tests=cls.TESTS_10,
                                                  handler_cls=RequestHandler10)
        cls.INT_A.wait_connectors()
        wait_http_listeners_up(cls.INT_A.addresses[0])

    @classmethod
    def tearDownClass(cls):
        cls.http10_server.wait(TIMEOUT)
        cls.http11_server.wait(TIMEOUT)
        super().tearDownClass()

    def test_005_get_10(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener10_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_10["GET"])
        client.close()

    def test_000_stats(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
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
                    ('httpListener', {'port': cls.http_listener11_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer11'}),
                    ('httpListener', {'port': cls.http_listener10_port,
                                      'protocolVersion': 'HTTP1',
                                      'address': 'testServer10'})
                    ])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        super(Http1AdaptorEdge2EdgeTest, cls).\
            router('EA2', 'edge',
                   [('connector', {'name': 'uplink', 'role': 'edge',
                                   'port': cls.INTA_edge2_port}),
                    ('httpConnector', {'port': cls.http_server11_port,
                                       'protocolVersion': 'HTTP1',
                                       'address': 'testServer11'}),
                    ('httpConnector', {'port': cls.http_server10_port,
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
        self.client_request_close_test(self.http_server11_port,
                                       self.http_listener11_port,
                                       self.EA2.management)

    def test_1002_client_response_close(self):
        """
        Simulate an HTTP client drop while server sends very large response
        """
        self.client_response_close_test(self.http_server11_port,
                                        self.http_listener11_port)

    def test_2000_curl_get(self):
        """
        Perform a get via curl
        """
        self.curl_get_test("127.0.0.1", self.http_listener11_port,
                           self.http_server11_port)

    def test_2001_curl_put(self):
        """
        Perform a put via curl
        """
        self.curl_put_test("127.0.0.1", self.http_listener11_port,
                           self.http_server11_port)

    def test_2002_curl_post(self):
        """
        Perform a post via curl
        """
        self.curl_post_test("127.0.0.1", self.http_listener11_port,
                            self.http_server11_port)


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
            ('httpListener', {'port': cls.http_listener_port,
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

    def test_02_bad_request_message(self):
        """
        Test various improperly constructed request messages
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

    def test_03_bad_response_message(self):
        """
        Test various improperly constructed response messages
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


if __name__ == '__main__':
    unittest.main(main_module())

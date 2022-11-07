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

from http1_tests import CommonHttp1OneRouterTest, Http1OneRouterTestBase
from http1_tests import TestServer, RequestHandler10
from http1_tests import Http1Edge2EdgeTestBase
from http1_tests import CommonHttp1Edge2EdgeTest
from http1_tests import wait_tcp_listeners_up


class Http1OverTcpOneRouterTest(Http1OneRouterTestBase,
                                CommonHttp1OneRouterTest):
    """
    Test HTTP servers and clients attached to a standalone router
    """
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1OverTcpOneRouterTest, cls).setUpClass()

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

        super(Http1OverTcpOneRouterTest, cls).router('INT.A', 'standalone',
                                                     [('tcpConnector', {'port': cls.server11_port,
                                                                        'host': cls.server11_host,
                                                                        'address': 'testServer11'}),
                                                      ('tcpConnector', {'port': cls.server10_port,
                                                                        'host': cls.server10_host,
                                                                        'address': 'testServer10'}),
                                                      ('tcpListener', {'port': cls.listener11_port,
                                                                       'host': cls.listener11_host,
                                                                       'address': 'testServer11'}),
                                                      ('tcpListener', {'port': cls.listener10_port,
                                                                       'host': cls.listener10_host,
                                                                       'address': 'testServer10'})
                                                      ])

        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        cls.http11_server = TestServer.new_server(server_port=cls.server11_port, client_port=cls.listener11_port, tests=cls.TESTS_11)
        cls.http10_server = TestServer.new_server(server_port=cls.server10_port, client_port=cls.listener10_port, tests=cls.TESTS_10,
                                                  handler_cls=RequestHandler10)
        cls.INT_A.wait_connectors()
        wait_tcp_listeners_up(cls.INT_A.listener)

    @classmethod
    def tearDownClass(cls):
        cls.http10_server.wait()
        cls.http11_server.wait()
        super().tearDownClass()


class Http1OverTcpEdge2EdgeTest(Http1Edge2EdgeTestBase, CommonHttp1Edge2EdgeTest):
    """
    Test an HTTP servers and clients attached to edge routers separated by an
    interior router
    """
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1OverTcpEdge2EdgeTest, cls).setUpClass()

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

        # Http1 will auto-generate a response, TCP will not:
        cls.skip = {'test_04_server_pining_for_the_fjords': True,
                    'test_03_server_reconnect': True}

        super(Http1OverTcpEdge2EdgeTest, cls).\
            router('INT.A', 'interior', [('listener', {'role': 'edge', 'port': cls.INTA_edge1_port}),
                                         ('listener', {'role': 'edge', 'port': cls.INTA_edge2_port}),
                                         ])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        super(Http1OverTcpEdge2EdgeTest, cls).\
            router('EA1', 'edge', [('connector', {'name': 'uplink', 'role': 'edge',
                                                  'port': cls.INTA_edge1_port}),
                                   ('tcpListener', {'port': cls.listener11_port,
                                                    'address': 'testServer11'}),
                                   ('tcpListener', {'port': cls.listener10_port,
                                                    'address': 'testServer10'})
                                   ])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        super(Http1OverTcpEdge2EdgeTest, cls).\
            router('EA2', 'edge', [('connector', {'name': 'uplink', 'role': 'edge',
                                                  'port': cls.INTA_edge2_port}),
                                   ('tcpConnector', {'port': cls.server11_port,
                                                     'host': cls.server11_host,
                                                     'address': 'testServer11'}),
                                   ('tcpConnector', {'port': cls.server10_port,
                                                     'host': cls.server10_host,
                                                     'address': 'testServer10'})
                                   ])
        cls.EA2 = cls.routers[-1]
        cls.EA2.listener = cls.EA2.addresses[0]

        cls.INT_A.wait_address('EA1')
        cls.INT_A.wait_address('EA2')
        wait_tcp_listeners_up(cls.EA1.listener)

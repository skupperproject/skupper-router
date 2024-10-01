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
import unittest

from http1_tests import wait_tcp_listeners_up
from system_test import TestCase, Qdrouterd, TIMEOUT, CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, \
    CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY_PASSWORD
try:
    import grpc
    import friendship_server as fs
    from friendship_pb2_grpc import FriendshipStub
    from friendship_pb2 import Person, FriendshipRequest, PersonEmail
    _GRPC_UNAVAILABLE = False
except ImportError:
    _GRPC_UNAVAILABLE = True

from system_tests_ssl import RouterTestSslBase


def skip_test():
    """If grpc cannot be imported, test must be skipped"""
    return _GRPC_UNAVAILABLE


class GrpcServiceMethodsTest(TestCase):

    """
    Data for the grpc service
    """
    NAME_EMAIL = {
        "One": "one@apache.org",
        "Two": "two@apache.org",
        "Three": "three@apache.org",
        "Four": "four@apache.org",
        "Five": "five@apache.org",
    }

    """
    List of how friendships will be defined at the grpc service
    1 = 2; 3; 4
    2 = 1; 3; 5
    3 = 1; 2; 4
    4 = 1; 3; 5
    5 = 2; 4
    """
    FRIENDS = {
        "one@apache.org": ["two@apache.org", "three@apache.org", "four@apache.org"],
        "two@apache.org": ["three@apache.org", "five@apache.org"],
        "three@apache.org": ["four@apache.org"],
        "four@apache.org": ["five@apache.org"],
    }
    EXP_FRIENDS = {
        "one@apache.org": ["two@apache.org", "three@apache.org", "four@apache.org"],
        "two@apache.org": ["one@apache.org", "three@apache.org", "five@apache.org"],
        "three@apache.org": ["one@apache.org", "two@apache.org", "four@apache.org"],
        "four@apache.org": ["one@apache.org", "three@apache.org", "five@apache.org"],
        "five@apache.org": ["two@apache.org", "four@apache.org"]
    }

    """
    List of common friends to request for and the expected
    result to be returned by the service
    """
    COMMON_FRIENDS_ITER = [
        ["one@apache.org", "five@apache.org"],
        ["two@apache.org", "four@apache.org"],
    ]
    COMMON_FRIENDS_EXP = [
        ["two@apache.org", "four@apache.org"],
        ["one@apache.org", "three@apache.org", "five@apache.org"],
    ]

    @classmethod
    def setUpClass(cls):
        super(GrpcServiceMethodsTest, cls).setUpClass()
        if skip_test():
            return

        # Define a random port for  the gRPC server to bind
        cls.grpc_server_port = str(cls.tester.get_port())

        # Run the gRPC server (see friendship.proto for more info)
        cls.grpc_server = fs.serve(cls.grpc_server_port)

        # Prepare router to communicate with the gRPC server
        cls.connector_props = {
            'port': cls.grpc_server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'name': 'grpc-server'
        }
        cls.router_listener_port = cls.tester.get_port()
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('tcpListener', {'port': cls.router_listener_port, 'address': 'examples',
                             'host': '127.0.0.1'}),
            ('tcpConnector', cls.connector_props)
        ])
        cls.router_qdr = cls.tester.qdrouterd("grpc-test-router", config,
                                              wait=True)
        wait_tcp_listeners_up(cls.router_qdr.addresses[0])

        # If you wanna try it without the router, set the grpc_channel
        # directly to the grpc_server_port
        cls.grpc_channel = grpc.insecure_channel('127.0.0.1:%s' %
                                                 cls.router_listener_port)
        cls.grpc_stub = FriendshipStub(cls.grpc_channel)

    @classmethod
    def tearDownClass(cls):
        super(GrpcServiceMethodsTest, cls).tearDownClass()
        if skip_test():
            return
        cls.grpc_server.stop(TIMEOUT)

    @classmethod
    def create_person(cls, name, email):
        p = Person()
        p.name = name
        p.email = email
        res = cls.grpc_stub.Create(p)
        assert res.success
        assert res.message == ""
        return p

    @classmethod
    def friendship_generator(cls):
        for key in cls.FRIENDS:
            for friend in cls.FRIENDS[key]:
                fr = FriendshipRequest()
                fr.email1 = key
                fr.email2 = friend
                yield fr

    @classmethod
    def common_friends_list(cls, friends):
        for friend in friends:
            pe = PersonEmail()
            pe.email = friend
            yield pe

    @unittest.skipIf(skip_test(), "grpcio is needed to run grpc tests")
    def test_grpc_01_unary(self):
        """
        Validates unary request and response message
        :return:
        """
        for key in self.NAME_EMAIL:
            name = key
            mail = self.NAME_EMAIL[key]
            p = self.create_person(name, mail)
            assert p is not None

    @unittest.skipIf(skip_test(), "grpcio is needed to run grpc tests")
    def test_grpc_02_bidirectional_stream(self):
        """
        Validates bidirectional streaming request and response messages
        :return:
        """
        for res in self.grpc_stub.MakeFriends(self.friendship_generator()):
            assert res.friend1 is not None
            assert res.friend2 is not None
            assert not res.error

    @unittest.skipIf(skip_test(), "grpcio is needed to run grpc tests")
    def test_grpc_03_server_stream(self):
        """
        Validates server streaming response messages
        :return:
        """
        for key in self.EXP_FRIENDS:
            pe = PersonEmail()
            pe.email = key
            friends = []
            for friend in self.grpc_stub.ListFriends(pe):
                friends.append(friend.email)
            assert all(f in self.EXP_FRIENDS[key] for f in friends)
            assert all(f in friends for f in self.EXP_FRIENDS[key])

    @unittest.skipIf(skip_test(), "grpcio is needed to run grpc tests")
    def test_grpc_04_client_stream(self):
        """
        Validates client streaming request messages
        :return:
        """
        for i in range(len(self.COMMON_FRIENDS_ITER)):
            friends = self.COMMON_FRIENDS_ITER[i]
            exp_friends = self.COMMON_FRIENDS_EXP[i]
            res = self.grpc_stub.CommonFriendsCount(self.common_friends_list(friends))
            assert res.count == len(exp_friends)
            assert all(f in res.friends for f in exp_friends)
            assert all(f in exp_friends for f in res.friends)


# This following test (running GRPC over TCP Adaptor TLS) will fail
# without the fix for https://github.com/skupperproject/skupper-router/issues/845
# GRPC client requires that the server it is connecting to (in this case, the router)
# support "h2"(http2) as the protocol and this protocol is decided only using ALPN.
# Same with the GRPC server, it too expects the client to present h2 in ALPN
# as part of the router initiated TLS handshake.
# The TCP Adaptor now accepts h2 as the protocol when acting as the server
# and sends h2 as the protocol when it is connecting as a client to the GRPC server.
class GrpcServiceMethodsTestOverTcpTls(GrpcServiceMethodsTest, RouterTestSslBase):
    @classmethod
    def setUpClass(cls):
        super(GrpcServiceMethodsTestOverTcpTls, cls).setUpClass()
        if skip_test():
            return

        # Define a random port for  the gRPC server to bind
        cls.grpc_server_port = str(cls.tester.get_port())

        # Run the gRPC server which will listen on a secure port (see friendship_server.py for more info)
        cls.grpc_server = fs.serve_secure(cls.grpc_server_port)
        cls.router_tcp_port = cls.tester.get_port()

        # Prepare router to communicate with the gRPC server
        cls.connector_props = {
            # This grpc_server_port is a secure TLS enabled port and the router connector will
            # connect to this port.
            'port': cls.grpc_server_port,
            'address': 'examples',
            'host': 'localhost',
            'name': 'grpc-server',
            'sslProfile': 'tcp-connector-ssl-profile'
        }
        cls.listener_props = {
            'port': cls.router_tcp_port,
            'address': 'examples',
            'host': 'localhost',
            'authenticatePeer': 'no',
            'sslProfile': 'tcp-listener-ssl-profile'
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('tcpListener', cls.listener_props),
            ('tcpConnector', cls.connector_props),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': CLIENT_PRIVATE_KEY_PASSWORD})
        ])
        cls.router_qdr = cls.tester.qdrouterd("grpc-test-router", config, wait=True)
        wait_tcp_listeners_up(cls.router_qdr.addresses[0])

        ca_certificate = RouterTestSslBase.get_byte_string(CA_CERT)
        credentials = grpc.ssl_channel_credentials(root_certificates=ca_certificate)

        # The GRPC client is opening a secure channel to the TLS enabled router listener tcp port which has its
        # sslProfile as tcp-listener-ssl-profile
        cls.secure_grpc_channel = grpc.secure_channel('localhost:%s' % cls.router_tcp_port, credentials=credentials)
        cls.grpc_stub = FriendshipStub(cls.secure_grpc_channel)

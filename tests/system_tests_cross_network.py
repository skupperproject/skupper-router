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

from proton import Message, symbol
from proton.handlers import MessagingHandler
from proton.reactor import Container, LinkOption

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, TestTimeout
from system_test import unittest


class CrossNetworkOption(LinkOption):
    def apply(self, link):
        if link.is_receiver:
            link.source.capabilities.put_object(symbol('qd.cross-network'))


class CrossNetworkPreconfiguredTest(TestCase):
    """System tests involving a single router"""
    @classmethod
    def setUpClass(cls):
        """Start a router and a messenger"""
        super(CrossNetworkPreconfiguredTest, cls).setUpClass()

        def router(name, mode, netid, connection1, connection2=None, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('managedRouter', {'networkId': netid}),
                ('listener', {'port': cls.tester.get_port()}),
                ('address', {'prefix': 'cl', 'distribution': 'closest'}),
                connection1
            ]

            if connection2:
                config.append(connection2)
            if extra:
                if extra.__class__ == list:
                    for e in extra:
                        config.append(e)
                else:
                    config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        inter_router_port_X = cls.tester.get_port()
        inter_router_port_Y = cls.tester.get_port()
        cross_network_port  = cls.tester.get_port()
        edge_port_X         = cls.tester.get_port()
        edge_port_Y         = cls.tester.get_port()

        router('NET.X.INT.A', 'interior', 'net-x',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_X}),
               ('listener', {'name': 'cross-net', 'role': 'route-container', 'port': cross_network_port}),
               ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'externalAddress': '_xnet/net-x'}))
        router('NET.X.INT.B', 'interior', 'net-x',
               ('connector', {'role': 'inter-router', 'port': inter_router_port_X}),
               ('listener', {'role': 'edge', 'port': edge_port_X}))
        router('EX', 'edge', 'net-x', ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_X}))

        router('NET.Y.INT.A', 'interior', 'net-y',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_Y}),
               ('connector', {'name': 'cross-net', 'role': 'route-container', 'port': cross_network_port}),
               ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service'}))
        router('NET.Y.INT.B', 'interior', 'net-y',
               ('connector', {'role': 'inter-router', 'port': inter_router_port_Y}),
               ('listener', {'role': 'edge', 'port': edge_port_Y}))
        router('EY', 'edge', 'net-y', ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_Y}))

        cls.routers[0].wait_router_connected('NET.X.INT.B')
        cls.routers[1].wait_router_connected('NET.X.INT.A')

        cls.routers[3].wait_router_connected('NET.Y.INT.B')
        cls.routers[4].wait_router_connected('NET.Y.INT.A')

        cls.routers[1].is_edge_routers_connected(num_edges=1)
        cls.routers[4].is_edge_routers_connected(num_edges=1)

        cls.routers[1].wait_address('service', 0, 1, 0)

        cls.xinta = cls.routers[0].addresses[0]
        cls.xintb = cls.routers[1].addresses[0]
        cls.ex    = cls.routers[2].addresses[0]
        cls.yinta = cls.routers[3].addresses[0]
        cls.yintb = cls.routers[4].addresses[0]
        cls.ey    = cls.routers[5].addresses[0]

    def test_01_local_to_local(self):
        test = ClientServerTest(self.xinta, self.yinta, self.yinta, 'service')
        test.run()
        self.assertIsNone(test.error)

    def test_02_distant_to_local(self):
        test = ClientServerTest(self.xintb, self.yinta, self.yinta, 'service')
        test.run()
        self.assertIsNone(test.error)

    def test_03_local_to_distant(self):
        test = ClientServerTest(self.xinta, self.yintb, self.yinta, 'service')
        test.run()
        self.assertIsNone(test.error)

    def test_04_distant_to_distant(self):
        test = ClientServerTest(self.xintb, self.yintb, self.yinta, 'service')
        test.run()
        self.assertIsNone(test.error)


class ClientServerTest(MessagingHandler):
    def __init__(self, client_address, server_address, probe_address, addr):
        super(ClientServerTest, self).__init__()
        self.client_address    = client_address
        self.server_address    = server_address
        self.probe_address     = probe_address
        self.addr              = addr
        self.error             = None
        self.client_sender     = None
        self.client_receiver   = None
        self.server_sender     = None
        self.server_receiver   = None
        self.probe_sender      = None
        self.timer             = None
        self.client_conn       = None
        self.server_conn       = None
        self.probe_conn        = None
        self.reply_to          = None
        self.n_client_sent     = 0
        self.n_client_rcvd     = 0
        self.n_server_sent     = 0
        self.n_server_rcvd     = 0
        self.n_attached        = 0
        self.n_client_released = 0
        self.n_server_released = 0
        self.count             = 400

    def timeout(self):
        self.error = "Timeout Expired - client_sent=%d, server_rcvd=%d, server_sent=%d, client_rcvd=%d, attached=%d, client_released=%d, server_released=%d, reply_to=%s" % \
            (self.n_client_sent, self.n_server_rcvd, self.n_server_sent, self.n_client_rcvd, self.n_attached, self.n_client_released, self.n_server_released, self.reply_to)
        self.client_conn.close()
        self.server_conn.close()
        self.probe_conn.close()

    def fail(self, cause):
        self.error = cause
        self.client_conn.close()
        self.server_conn.close()
        self.probe_conn.close()
        self.timer.cancel()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.client_conn = event.container.connect(self.client_address)
        self.server_conn = event.container.connect(self.server_address)
        self.probe_conn  = event.container.connect(self.probe_address)
        self.client_receiver = event.container.create_receiver(self.client_conn, None, dynamic=True, options=CrossNetworkOption())
        self.server_sender   = event.container.create_sender(self.server_conn, None)
        self.server_receiver = event.container.create_receiver(self.server_conn, self.addr)

    def on_link_opened(self, event):
        self.n_attached += 1
        if event.receiver == self.client_receiver:
            self.reply_to = event.receiver.remote_source.address
        if self.n_attached == 3:
            self.probe_sender = event.container.create_sender(self.probe_conn, self.addr)

    def on_sendable(self, event):
        if event.sender == self.probe_sender:
            self.client_sender = event.container.create_sender(self.client_conn, self.addr)
        if event.sender == self.client_sender:
            while self.n_client_sent < self.count and self.client_sender.credit > 0:
                self.client_sender.send(Message(address=self.addr, reply_to=self.reply_to, body={'sequence': self.n_client_sent}))
                self.n_client_sent += 1

    def on_message(self, event):
        if event.receiver == self.server_receiver:
            self.n_server_rcvd += 1
            msg = event.message
            self.server_sender.send(Message(address=msg.reply_to, body=msg.body))
            self.n_server_sent += 1
        elif event.receiver == self.client_receiver:
            self.n_client_rcvd += 1
            if self.n_client_rcvd == self.count:
                self.fail(None)

    def on_released(self, event):
        if event.sender == self.client_sender:
            self.n_client_released += 1
        elif event.sender == self.server_sender:
            self.n_server_released += 1

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

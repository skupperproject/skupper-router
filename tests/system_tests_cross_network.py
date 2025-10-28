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

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, TestTimeout
from system_test import unittest


class CrossNetworkPreconfiguredTest(TestCase):
    """System tests involving a single router"""
    @classmethod
    def setUpClass(cls):
        """Start a router and a messenger"""
        super(CrossNetworkPreconfiguredTest, cls).setUpClass()

        def router(name, mode, netid, connection1, connection2=None, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('network', {'networkId': netid}),
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
               ('listener', {'role': 'inter-router', 'port': inter_router_port_X, 'linkCapacity': '10'}),
               ('listener', {'name': 'cross-net', 'role': 'inter-network', 'port': cross_network_port, 'linkCapacity': '10'}),
               ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'externalAddress': '_topo/net-x'}))
        router('NET.X.INT.B', 'interior', 'net-x',
               ('connector', {'role': 'inter-router', 'port': inter_router_port_X, 'linkCapacity': '10'}),
               ('listener', {'role': 'edge', 'port': edge_port_X, 'linkCapacity': '10'}))
        router('EX', 'edge', 'net-x', ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_X, 'linkCapacity': '10'}))

        router('NET.Y.INT.A', 'interior', 'net-y',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_Y, 'linkCapacity': '10'}),
               ('connector', {'name': 'cross-net', 'role': 'inter-network', 'port': cross_network_port, 'linkCapacity': '10'}),
               [('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service01'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service02'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service03'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service04'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service05'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service06'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service07'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service08'}),
                ('autoLink', {'connection': 'cross-net', 'direction': 'in', 'address': 'service09'})])
        router('NET.Y.INT.B', 'interior', 'net-y',
               ('connector', {'role': 'inter-router', 'port': inter_router_port_Y, 'linkCapacity': '10'}),
               ('listener', {'role': 'edge', 'port': edge_port_Y, 'linkCapacity': '10'}))
        router('EY', 'edge', 'net-y', ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_Y, 'linkCapacity': '10'}))

        cls.routers[0].wait_router_connected('NET.X.INT.B')
        cls.routers[1].wait_router_connected('NET.X.INT.A')

        cls.routers[3].wait_router_connected('NET.Y.INT.B')
        cls.routers[4].wait_router_connected('NET.Y.INT.A')

        cls.routers[1].is_edge_routers_connected(num_edges=1)
        cls.routers[4].is_edge_routers_connected(num_edges=1)

        cls.routers[0].wait_address('EX', 0, 1, 1)
        cls.routers[4].wait_address('net-x', 0, 1, 1)

        cls.xinta = cls.routers[0].addresses[0]
        cls.xintb = cls.routers[1].addresses[0]
        cls.ex    = cls.routers[2].addresses[0]
        cls.yinta = cls.routers[3].addresses[0]
        cls.yintb = cls.routers[4].addresses[0]
        cls.ey    = cls.routers[5].addresses[0]

        #print("export XINTA=%s" % cls.xinta)
        #print("export XINTB=%s" % cls.xintb)
        #print("export YINTA=%s" % cls.yinta)
        #print("export YINTB=%s" % cls.yintb)
        #print("export EX=%s" % cls.ex)
        #print("export EY=%s" % cls.ey)

    def _run(self, test):
        test.run()
        self.assertIsNone(test.error)

    def test_01_local_to_local(self):
        self._run(ClientServerTest(self.xinta, self.yinta, self.yinta, 'service01'))

    def test_02_distant_to_local(self):
        self._run(ClientServerTest(self.xintb, self.yinta, self.yinta, 'service02'))

    def test_03_local_to_distant(self):
        self._run(ClientServerTest(self.xinta, self.yintb, self.yinta, 'service03'))

    def test_04_distant_to_distant(self):
        self._run(ClientServerTest(self.xintb, self.yintb, self.yinta, 'service04'))

    def test_05_local_to_edge(self):
        self._run(ClientServerTest(self.xinta, self.ey, self.yinta, 'service05'))

    def test_06_distant_to_edge(self):
        self._run(ClientServerTest(self.xintb, self.ey, self.yinta, 'service06'))

    def test_07_edge_to_local(self):
        self._run(ClientServerTest(self.ex, self.yinta, self.yinta, 'service07'))

    def test_08_edge_to_distant(self):
        self._run(ClientServerTest(self.ex, self.yintb, self.yinta, 'service08'))

    def test_09_edge_to_edge(self):
        self._run(ClientServerTest(self.ex, self.ey, self.yinta, 'service09'))


class ClientServerTest(MessagingHandler):
    def __init__(self, client_address, server_address, probe_address, addr):
        super(ClientServerTest, self).__init__()
        self.client_address     = client_address
        self.server_address     = server_address
        self.probe_address      = probe_address
        self.addr               = addr
        self.error              = None
        self.client_sender      = None
        self.client_receiver    = None
        self.server_sender      = None
        self.server_receiver    = None
        self.probe_sender       = None
        self.timer              = None
        self.client_conn        = None
        self.server_conn        = None
        self.probe_conn         = None
        self.reply_to           = None
        self.sequence           = 0
        self.n_client_sent      = 0
        self.n_client_rcvd      = 0
        self.n_server_sent      = 0
        self.n_server_rcvd      = 0
        self.n_attached         = 0
        self.n_client_released  = 0
        self.n_server_released  = 0
        self.first_rel_sequence = None
        self.last_rel_sequence  = None
        self.count              = 25

    def timeout(self):
        self.error = "Timeout Expired - client_sent=%d, server_rcvd=%d, server_sent=%d, client_rcvd=%d, attached=%d, client_released=%d, server_released=%d, first_rel=%r, last_rel=%r, reply_to=%s" % \
            (self.n_client_sent, self.n_server_rcvd, self.n_server_sent, self.n_client_rcvd, self.n_attached, self.n_client_released, self.n_server_released, self.first_rel_sequence, self.last_rel_sequence, self.reply_to)
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
        self.client_receiver = event.container.create_receiver(self.client_conn, None, dynamic=True)
        self.server_sender = event.container.create_sender(self.server_conn, None)
        self.server_receiver = event.container.create_receiver(self.server_conn, self.addr)

    def on_link_opened(self, event):
        self.n_attached += 1
        if event.receiver == self.client_receiver:
            self.reply_to = event.receiver.remote_source.address
        if self.n_attached == 3:
            self.probe_sender = event.container.create_sender(self.probe_conn, self.addr)

    def on_sendable(self, event):
        if event.sender == self.probe_sender and not self.client_sender:
            self.client_sender = event.container.create_sender(self.client_conn, self.addr)
        elif event.sender == self.client_sender:
            while self.n_client_sent < self.count and self.client_sender.credit > 0:
                delivery = self.client_sender.send(Message(address=self.addr, reply_to=self.reply_to, body={'sequence': self.sequence}))
                delivery._sequence = self.sequence
                self.n_client_sent += 1
                self.sequence += 1

    def on_message(self, event):
        if event.receiver == self.server_receiver:
            self.n_server_rcvd += 1
            msg = event.message
            delivery = self.server_sender.send(Message(address=msg.reply_to, body=msg.body))
            delivery._sequence = msg.body['sequence']
            self.n_server_sent += 1
        elif event.receiver == self.client_receiver:
            self.n_client_rcvd += 1
            if self.n_client_rcvd == self.count:
                self.fail(None)

    def on_released(self, event):
        if event.sender == self.client_sender:
            self.n_client_released += 1
            # Client releases are ok because they are caused by normal propagation delays of the mobile service address
            self.n_client_sent -= 1
            if not self.first_rel_sequence:
                self.first_rel_sequence = event.delivery._sequence
            self.last_rel_sequence = event.delivery._sequence
        elif event.sender == self.server_sender:
            self.n_server_released += 1

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

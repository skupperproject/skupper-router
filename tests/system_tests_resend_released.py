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
from proton._events import Event
from proton.handlers import MessagingHandler
from proton.reactor import Container

from system_test import unittest
from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, MgmtMsgProxy, TestTimeout, PollTimeout


class AddrTimer:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.check_address()


class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, mode, connection1, connection2=None, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                connection1
            ]

            if connection2:
                config.append(connection2)
            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        inter_router_port_A = cls.tester.get_port()
        inter_router_port_B = cls.tester.get_port()
        edge_port_A         = cls.tester.get_port()
        edge_port_B         = cls.tester.get_port()
        edge_port_C         = cls.tester.get_port()

        router('INT.A', 'interior',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_A}),
               ('listener', {'role': 'edge', 'port': edge_port_A}))
        router('INT.B', 'interior',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_B}),
               ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port_A}),
               ('listener', {'role': 'edge', 'port': edge_port_B}))
        router('INT.C', 'interior',
               ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port_A}),
               ('connector', {'name': 'connectorToB', 'role': 'inter-router', 'port': inter_router_port_B}),
               ('listener', {'role': 'edge', 'port': edge_port_C}))
        router('EA1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EA2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EB1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))
        router('EB2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))
        router('EC1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_C}))
        router('EC2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_C}))

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[0].wait_router_connected('INT.C')
        cls.routers[1].wait_router_connected('INT.A')
        cls.routers[1].wait_router_connected('INT.C')
        cls.routers[2].wait_router_connected('INT.A')
        cls.routers[2].wait_router_connected('INT.B')

        cls.inta = cls.routers[0].addresses[0]
        cls.intb = cls.routers[1].addresses[0]
        cls.intc = cls.routers[2].addresses[0]
        cls.ea1  = cls.routers[3].addresses[0]
        cls.ea2  = cls.routers[4].addresses[0]
        cls.eb1  = cls.routers[5].addresses[0]
        cls.eb2  = cls.routers[6].addresses[0]
        cls.ec1  = cls.routers[7].addresses[0]
        cls.ec2  = cls.routers[8].addresses[0]

    def test_01_baseline_released(self):
        test = ResendReleasedTest(self.inta, [self.inta], [], 'resrel.01', 1)
        test.run()
        self.assertIsNone(test.error)

    def test_02_baseline_accepted(self):
        test = ResendReleasedTest(self.inta, [], [self.inta], 'resrel.02', 0)
        test.run()
        self.assertIsNone(test.error)

    def test_03_all_released_same_router(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.inta], [], 'resrel.03', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_04_all_released_remote_edge(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea1], [], 'resrel.04', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_05_all_released_remote_interior(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb], [], 'resrel.05', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_06_all_released_remote_interiors(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb, self.intc, self.intc], [], 'resrel.06', 5, 0, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_07_all_released_interior_to_local_edges(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea2, self.ea2, self.ea2], [], 'resrel.07', 5, 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_08_all_released_interior_to_remote_edges(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.eb2, self.ec1, self.ec2], [], 'resrel.08', 5, 1, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_09_all_released_edge_to_local_interior(self):
        test = ResendReleasedTest(self.ea1, [self.inta, self.inta, self.inta], [], 'resrel.09', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_10_all_released_edge_to_remote_interior(self):
        test = ResendReleasedTest(self.ec1, [self.inta, self.inta, self.inta], [], 'resrel.10', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_11_all_released_edge_to_remote_edge(self):
        test = ResendReleasedTest(self.ec1, [self.ea1, self.ea1, self.ea1], [], 'resrel.11', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_12_all_released_many(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.intb, self.intc, self.intc, self.ea1, self.ea2], [], 'resrel.12', 8, 4, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_13_accept_same_interior(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.inta], 'resrel.13')
        test.run()
        self.assertIsNone(test.error)

    def test_14_accept_remote_interior(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.intb], 'resrel.14', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_15_accept_local_edge(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ea1], 'resrel.15', None, 3, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_16_accept_remote_edge(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ec1], 'resrel.16', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)


class ResendReleasedTest(MessagingHandler):
    def __init__(self, sender_host, release_hosts, accept_hosts, addr, expected_releases=None, wait_local=0, wait_remote=0):
        super(ResendReleasedTest, self).__init__(auto_accept=False)
        self.sender_host       = sender_host
        self.release_hosts     = release_hosts
        self.accept_hosts      = accept_hosts
        self.addr              = addr
        self.wait_local        = wait_local
        self.wait_remote       = wait_remote
        self.expected_releases = expected_releases

        self.sender_conn       = None
        self.sender            = None
        self.release_conns     = []
        self.accept_conns      = []
        self.release_receivers = []
        self.accept_receivers  = []
        self.error             = None
        self.poll_timer        = None
        self.n_receivers       = 0
        self.n_released        = 0
        self.n_accepted        = 0
        self.n_sent            = 0

    def timeout(self):
        self.error = "Timeout Expired - n_receivers=%d, n_released=%d, n_accepted=%d, n_sent=%d" % (self.n_receivers, self.n_released, self.n_accepted, self.n_sent)
        self.sender_conn.close()
        for conn in self.release_conns:
            conn.close()
        for conn in self.accept_conns:
            conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def fail(self, error):
        self.error = error
        self.sender_conn.close()
        for conn in self.release_conns:
            conn.close()
        for conn in self.accept_conns:
            conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()
        self.timer.cancel()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.proxy = None
        self.sender_conn = event.container.connect(self.sender_host)

        for host in self.release_hosts:
            self.release_conns.append(event.container.connect(host))
        for host in self.accept_hosts:
            self.accept_conns.append(event.container.connect(host))

        self.query_sender = event.container.create_sender(self.sender_conn, "$management")
        self.reply_receiver = event.container.create_receiver(self.sender_conn, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)
            for conn in self.release_conns:
                self.release_receivers.append(event.container.create_receiver(conn, self.addr))
            for conn in self.accept_conns:
                self.accept_receivers.append(event.container.create_receiver(conn, self.addr))
        elif event.receiver != None:
            self.n_receivers += 1
            if self.n_receivers == len(self.release_receivers) + len(self.accept_receivers) and self.sender == None:
                if self.wait_local > 0 or self.wait_remote > 0:
                    self.query_stats()
                else:
                    self.setup_sender(event)

    def on_sendable(self, event):
        if event.sender == self.sender and self.n_sent == 0:
            self.sender.send(Message("Message %d" % self.n_sent))
            self.n_sent += 1

    def find_stats(self, response):
        for record in response.results:
            if record.name == 'M' + self.addr:
                return (record.subscriberCount, record.remoteCount)
        return (0,0)
    
    def setup_sender(self, event):
        self.sender = event.container.create_sender(self.sender_conn, self.addr)
        self.sender.target.capabilities.put_object(symbol("qd.resend-released"))

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            self.accept(event.delivery)
            local, remote = self.find_stats(response)
            if local == self.wait_local and remote == self.wait_remote:
                self.setup_sender(event)
            else:
                #print("local=%d remote=%d" % (local, remote))
                self.poll_timer = event.reactor.schedule(0.5, PollTimeout(self))
        elif event.receiver in self.release_receivers:
            self.release(event.delivery, False)
            self.n_released += 1
            if self.n_released > len(self.release_receivers):
                self.fail("released %d deliveries but there are only %d releasing receivers" % (self.n_released, len(self.release_receivers)))
        elif event.receiver in self.accept_receivers:
            self.accept(event.delivery)
            self.n_accepted += 1

    def on_released(self, event):
        if event.sender == self.query_sender:
            return
        if self.n_accepted > 0:
            self.fail("delivery released after %d acceptances" % self.n_accepted)
        elif len(self.accept_receivers) > 0:
            self.fail("delivery released when there were available acceptors")
        elif len(self.release_receivers) != self.n_released:
            self.fail("delivery released after %d releases, had %d releasors" % (self.n_released, len(self.release_receivers)))
        elif self.expected_releases != None and self.n_released != self.expected_releases:
            self.fail("delivery released: expected %d released, but got %d" % (self.expected_releases, self.n_released))
        else:
            self.fail(None)

    def on_accepted(self, event):
        if event.sender == self.query_sender:
            return
        if self.n_accepted != 1:
            self.fail("delivery accepted after %d acceptances.  Expected 1" % self.n_accepted)
        elif self.expected_releases != None and self.n_released != self.expected_releases:
            self.fail("delivery accepted: expected %d released, but got %d" % (self.expected_releases, self.n_released))
        else:
            self.fail(None)

    def on_modified(self, event):
        self.fail("delivery modified: not expected")

    def query_stats(self):
        msg = self.proxy.query_addresses()
        self.query_sender.send(msg)

    def poll_timeout(self):
        self.query_stats()

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

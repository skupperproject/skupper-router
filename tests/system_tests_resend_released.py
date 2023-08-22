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

from proton import Message, symbol, Data
from proton._events import Event
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Delivery

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
                ('listener', {'port': cls.tester.get_port()}),
                ('address', {'prefix': 'cl', 'distribution': 'closest'}),
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
        router('INT.D', 'interior',
               ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port_A, 'cost' : 10}))
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

        cls.routers[0].wait_router_connected('INT.D')
        cls.routers[3].wait_router_connected('INT.A')

        cls.inta = cls.routers[0].addresses[0]
        cls.intb = cls.routers[1].addresses[0]
        cls.intc = cls.routers[2].addresses[0]
        cls.intd = cls.routers[3].addresses[0]
        cls.ea1  = cls.routers[4].addresses[0]
        cls.ea2  = cls.routers[5].addresses[0]
        cls.eb1  = cls.routers[6].addresses[0]
        cls.eb2  = cls.routers[7].addresses[0]
        cls.ec1  = cls.routers[8].addresses[0]
        cls.ec2  = cls.routers[9].addresses[0]

    def test_010_baseline_released_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta], [], 'resrel.010', 1)
        test.run()
        self.assertIsNone(test.error)

    def test_020_baseline_accepted_balanced(self):
        test = ResendReleasedTest(self.inta, [], [self.inta], 'resrel.020', 0)
        test.run()
        self.assertIsNone(test.error)

    def test_030_all_released_same_router_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.inta], [], 'resrel.030', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_040_all_released_remote_edge_balanced(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea1], [], 'resrel.040', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_050_all_released_remote_interior_balanced(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb], [], 'resrel.050', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_060_all_released_remote_interiors_balanced(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb, self.intc, self.intc], [], 'resrel.060', 5, 0, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_070_all_released_interior_to_local_edges_balanced(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea2, self.ea2, self.ea2], [], 'resrel.070', 5, 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_080_all_released_interior_to_remote_edges_balanced(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.eb2, self.ec1, self.ec2], [], 'resrel.080', 5, 1, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_090_all_released_edge_to_local_interior_balanced(self):
        test = ResendReleasedTest(self.ea1, [self.inta, self.inta, self.inta], [], 'resrel.090', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_100_all_released_edge_to_remote_interior_balanced(self):
        test = ResendReleasedTest(self.ec1, [self.inta, self.inta, self.inta], [], 'resrel.100', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_110_all_released_edge_to_remote_edge_balanced(self):
        test = ResendReleasedTest(self.ec1, [self.ea1, self.ea1, self.ea1], [], 'resrel.110', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_120_all_released_many_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.intb, self.intc, self.intc, self.ea1, self.ea2], [], 'resrel.120', 8, 4, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_130_accept_same_interior_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.inta], 'resrel.130')
        test.run()
        self.assertIsNone(test.error)

    def test_140_accept_remote_interior_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.intb], 'resrel.140', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_150_accept_local_edge_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ea1], 'resrel.150', None, 3, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_160_accept_remote_edge_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ec1], 'resrel.160', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_161_accept_many_high_cost_balanced(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.ea1, self.ea2, self.eb2, self.eb2], [self.intd], 'resrel.161', 7, 4, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_170_baseline_released_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta], [], 'cl.resrel.170', 1)
        test.run()
        self.assertIsNone(test.error)

    def test_180_baseline_accepted_closest(self):
        test = ResendReleasedTest(self.inta, [], [self.inta], 'cl.resrel.180', 0)
        test.run()
        self.assertIsNone(test.error)

    def test_190_all_released_same_router_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.inta], [], 'cl.resrel.190', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_200_all_released_remote_edge_closest(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea1], [], 'cl.resrel.200', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_210_all_released_remote_interior_closest(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb], [], 'cl.resrel.210', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_220_all_released_remote_interiors_closest(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb, self.intc, self.intc], [], 'cl.resrel.220', 5, 0, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_221_all_released_multiple_costs_closest(self):
        test = ResendReleasedTest(self.inta, [self.intb, self.intb, self.intb, self.intc, self.intd, self.intd], [], 'cl.resrel.221', 6, 0, 3)
        test.run()
        self.assertIsNone(test.error)

    def test_230_all_released_interior_to_local_edges_closest(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.ea2, self.ea2, self.ea2], [], 'cl.resrel.230', 5, 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_240_all_released_interior_to_remote_edges_closest(self):
        test = ResendReleasedTest(self.inta, [self.ea1, self.ea1, self.eb2, self.ec1, self.ec2], [], 'cl.resrel.240', 5, 1, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_250_all_released_edge_to_local_interior_closest(self):
        test = ResendReleasedTest(self.ea1, [self.inta, self.inta, self.inta], [], 'cl.resrel.250', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_260_all_released_edge_to_remote_interior_closest(self):
        test = ResendReleasedTest(self.ec1, [self.inta, self.inta, self.inta], [], 'cl.resrel.260', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_270_all_released_edge_to_remote_edge_closest(self):
        test = ResendReleasedTest(self.ec1, [self.ea1, self.ea1, self.ea1], [], 'cl.resrel.270', 3)
        test.run()
        self.assertIsNone(test.error)

    def test_280_all_released_many_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.intb, self.intc, self.intc, self.ea1, self.ea2], [], 'cl.resrel.280', 8, 4, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_290_accept_same_interior_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.inta], 'cl.resrel.290')
        test.run()
        self.assertIsNone(test.error)

    def test_300_accept_remote_interior_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.intb], 'cl.resrel.300', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_310_accept_local_edge_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ea1], 'cl.resrel.310', None, 3, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_320_accept_remote_edge_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta], [self.ec1], 'cl.resrel.320', 2, 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_321_accept_many_high_cost_closest(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.ea1, self.ea2, self.eb2, self.eb2], [self.intd], 'cl.resrel.321', 7, 4, 2)
        test.run()
        self.assertIsNone(test.error)

    def test_400_baseline_released_balanced_stream(self):
        test = ResendReleasedTest(self.inta, [self.inta], [], 'resrel.400', 1, 0, 0, True)
        test.run()
        self.assertIsNone(test.error)

    def test_410_baseline_accepted_balanced_stream(self):
        test = ResendReleasedTest(self.inta, [], [self.inta], 'resrel.410', 0, 0, 0, True)
        test.run()
        self.assertIsNone(test.error)

    def test_420_accept_many_high_cost_balanced_stream(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.ea1, self.ea2, self.eb2, self.eb2], [self.intd], 'resrel.420', 7, 4, 2, True)
        test.run()
        self.assertIsNone(test.error)

    def test_421_accept_many_high_cost_balanced_stream_from_edge(self):
        test = ResendReleasedTest(self.ea1, [self.inta, self.inta, self.intb, self.ea2, self.ea2, self.eb2, self.eb2], [self.intd], 'resrel.421', 7, 3, 2, True, query_host=self.inta)
        test.run()
        self.assertIsNone(test.error)

    def test_430_accept_many_high_cost_closest_stream(self):
        test = ResendReleasedTest(self.inta, [self.inta, self.inta, self.intb, self.ea1, self.ea2, self.eb2, self.eb2], [self.intd], 'cl.resrel.430', 7, 4, 2, True)
        test.run()
        self.assertIsNone(test.error)

    def test_431_accept_many_high_cost_closest_stream_from_edge(self):
        test = ResendReleasedTest(self.ea1, [self.inta, self.inta, self.intb, self.ea2, self.ea2, self.eb2, self.eb2], [self.intd], 'cl.resrel.431', 7, 3, 2, True, query_host=self.inta)
        test.run()
        self.assertIsNone(test.error)


class ResendReleasedTest(MessagingHandler):
    def __init__(self, sender_host, release_hosts, accept_hosts, addr, expected_releases=None, wait_local=0, wait_remote=0, streaming=False, query_host=None):
        super(ResendReleasedTest, self).__init__(auto_accept=False)
        self.sender_host        = sender_host
        self.query_host         = query_host or sender_host
        self.release_hosts      = release_hosts
        self.accept_hosts       = accept_hosts
        self.addr               = addr
        self.wait_local         = wait_local
        self.wait_remote        = wait_remote
        self.expected_releases  = expected_releases
        self.streaming          = streaming
        self.fragments_sent     = 0
        self.fragments_received = 0
        self.fragment_count     = 10  # 10 big_payloads exceeds Q2
        if self.streaming:
            self.big_payload = ""
            for i in range(1500):
                self.big_payload += "0123456789"

        self.query_conn        = None
        self.sender_conn       = None
        self.sender            = None
        self.release_conns     = []
        self.accept_conns      = []
        self.release_receivers = []
        self.accept_receivers  = []
        self.stream_receivers  = []
        self.error             = None
        self.poll_timer        = None
        self.n_receivers       = 0
        self.n_released        = 0
        self.n_accepted        = 0
        self.n_sent            = 0

    def timeout(self):
        self.error = "Timeout Expired - n_receivers=%d, n_released=%d, n_accepted=%d, n_sent=%d" % (self.n_receivers, self.n_released, self.n_accepted, self.n_sent)
        self.query_conn.close()
        self.sender_conn.close()
        for conn in self.release_conns:
            conn.close()
        for conn in self.accept_conns:
            conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def fail(self, error):
        self.error = error
        self.query_conn.close()
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
        self.query_conn  = event.container.connect(self.query_host)
        self.sender_conn = event.container.connect(self.sender_host)

        for host in self.release_hosts:
            self.release_conns.append(event.container.connect(host, offered_capabilities=symbol("qd.streaming-links")))
        for host in self.accept_hosts:
            self.accept_conns.append(event.container.connect(host, offered_capabilities=symbol("qd.streaming-links")))

        self.query_sender = event.container.create_sender(self.query_conn, "$management")
        self.reply_receiver = event.container.create_receiver(self.query_conn, None, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)
            for conn in self.release_conns:
                self.release_receivers.append(event.container.create_receiver(conn, self.addr))
            for conn in self.accept_conns:
                self.accept_receivers.append(event.container.create_receiver(conn, self.addr))
        elif self.sender == None and event.receiver != None:
            self.n_receivers += 1
            if self.n_receivers == len(self.release_receivers) + len(self.accept_receivers):
                if self.wait_local > 0 or self.wait_remote > 0:
                    self.query_stats()
                else:
                    self.setup_sender(event)
        elif self.sender != None and event.receiver != None:
            self.stream_receivers.append(event.receiver)

    def on_sendable(self, event):
        if event.sender == self.sender and self.n_sent == 0:
            if self.streaming:
                msg = Message(body=None)
                self.delivery = self.sender.delivery(self.sender.delivery_tag())
                encoded = msg.encode()
                self.sender.stream(encoded)
            else:
                self.sender.send(Message("Message %d" % self.n_sent))
            self.n_sent += 1

    def find_stats(self, response):
        for record in response.results:
            if record.name == 'M' + self.addr:
                return (record.subscriberCount, record.remoteCount)
        return (0,0)
    
    def setup_sender(self, event):
        self.sender = event.container.create_sender(self.sender_conn, self.addr)
        self.sender.target.capabilities.put_array(False, Data.SYMBOL)
        self.sender.target.capabilities.enter()
        self.sender.target.capabilities.put_symbol(symbol("qd.resend-released"))
        self.sender.target.capabilities.put_symbol(symbol("qd.streaming-deliveries"))
        self.sender.target.capabilities.exit()

    def on_delivery(self, event):
        if self.streaming:
            if event.receiver in self.stream_receivers:
                if event.connection in self.release_conns:
                    self.release(event.delivery, False)
                    self.n_released += 1
                    if self.n_released > len(self.release_receivers):
                        self.fail("released %d deliveries but there are only %d releasing receivers" % (self.n_released, len(self.release_receivers)))
                else:
                    event.delivery.update(Delivery.RECEIVED)
                    stuff = event.link.recv(20000)
                    self.fragments_received += 1
                    if self.fragments_sent < self.fragment_count:
                        self.sender.stream(bytes(self.big_payload, "ascii"))
                        self.fragments_sent += 1
                        if self.fragments_sent == self.fragment_count:
                            self.sender.advance()
                    if not event.delivery.partial:
                        self.accept(event.delivery)
                        event.link.advance()
                        self.n_accepted += 1
            elif event.sender == self.sender:
                if event.delivery.remote_state == Delivery.RECEIVED:
                    if self.fragments_sent == 0:
                        self.sender.stream(bytes(self.big_payload, "ascii"))
                        self.fragments_sent = 1

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
        elif not self.streaming and event.receiver in self.release_receivers:
            self.release(event.delivery, False)
            self.n_released += 1
            if self.n_released > len(self.release_receivers):
                self.fail("released %d deliveries but there are only %d releasing receivers" % (self.n_released, len(self.release_receivers)))
        elif not self.streaming and event.receiver in self.accept_receivers:
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

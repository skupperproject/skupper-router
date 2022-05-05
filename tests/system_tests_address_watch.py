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

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, unittest, TestTimeout, retry_exception
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Message


class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, connection, connection2=None, mode='interior'):

            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port()}),
                connection
            ]

            if connection2:
                config.append(connection2)

            config = Qdrouterd.Config(config)

            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=["-T"]))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port         = cls.tester.get_port()

        router('A', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
                    ('listener', {'role': 'edge', 'port': edge_port}))
        router('B', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}))
        router('C', ('connector', {'name': 'connectorToA', 'role': 'edge', 'port': edge_port}), mode='edge')
        router('D', ('connector', {'name': 'connectorToA', 'role': 'edge', 'port': edge_port}), mode='edge')

        cls.routers[0].wait_router_connected('B')
        cls.routers[1].wait_router_connected('A')
        cls.routers[0].is_edge_routers_connected()

    def test_01_interior_interior(self):
        test = AddressWatchTest(self.routers[0], self.routers[1], 0)
        test.run()
        self.assertIsNone(test.error)

    def test_02_edge_interior(self):
        test = AddressWatchTest(self.routers[2], self.routers[0], 1)
        test.run()
        self.assertIsNone(test.error)

    def test_03_interior_edge(self):
        test = AddressWatchTest(self.routers[0], self.routers[2], 2)
        test.run()
        self.assertIsNone(test.error)

    def test_04_edge_far_interior(self):
        test = AddressWatchTest(self.routers[2], self.routers[1], 3)
        test.run()
        self.assertIsNone(test.error)

    def test_05_far_interior_edge(self):
        test = AddressWatchTest(self.routers[1], self.routers[2], 4)
        test.run()
        self.assertIsNone(test.error)

    def test_06_dynamic_same_interior(self):
        test = DynamicAddressWatchTest('test_06', self.routers[0], [self.routers[0]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_07_dynamic_interior_interior(self):
        test = DynamicAddressWatchTest('test_07', self.routers[0], [self.routers[1]], 0, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_08_dynamic_two_interiors(self):
        test = DynamicAddressWatchTest('test_08', self.routers[0], [self.routers[0], self.routers[1]], 1, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_09_dynamic_two_interiors_multiple_local(self):
        test = DynamicAddressWatchTest('test_09', self.routers[0], [self.routers[0], self.routers[1], self.routers[0]], 2, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_10_dynamic_same_edge(self):
        test = DynamicAddressWatchTest('test_10', self.routers[2], [self.routers[2]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_11_dynamic_edge_interior(self):
        test = DynamicAddressWatchTest('test_11', self.routers[2], [self.routers[0]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_12_dynamic_edge_interior_both(self):
        test = DynamicAddressWatchTest('test_12', self.routers[2], [self.routers[0], self.routers[2]], 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_13_dynamic_edge_far_interior(self):
        test = DynamicAddressWatchTest('test_13', self.routers[2], [self.routers[1]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_14_dynamic_edge_all_interior(self):
        test = DynamicAddressWatchTest('test_14', self.routers[2], [self.routers[1], self.routers[0]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_15_dynamic_edge_all_interior_multiple_local(self):
        test = DynamicAddressWatchTest('test_15', self.routers[2], [self.routers[1], self.routers[0], self.routers[2]], 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_16_dynamic_interior_edge(self):
        test = DynamicAddressWatchTest('test_16', self.routers[0], [self.routers[2]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_17_dynamic_interior_edge_both(self):
        test = DynamicAddressWatchTest('test_17', self.routers[0], [self.routers[2], self.routers[0]], 2, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_18_dynamic_far_interior_edge(self):
        test = DynamicAddressWatchTest('test_18', self.routers[1], [self.routers[2]], 0, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_19_dynamic_far_interior_edge_both(self):
        test = DynamicAddressWatchTest('test_19', self.routers[1], [self.routers[2], self.routers[1]], 1, 1)
        test.run()
        self.assertIsNone(test.error)

    def test_20_dynamic_edge_edge(self):
        test = DynamicAddressWatchTest('test_20', self.routers[2], [self.routers[3]], 1, 0)
        test.run()
        self.assertIsNone(test.error)

    def test_21_dynamic_edge_edge_local(self):
        test = DynamicAddressWatchTest('test_21', self.routers[2], [self.routers[3], self.routers[2]], 2, 0)
        test.run()
        self.assertIsNone(test.error)


class AddressWatchTest(MessagingHandler):
    def __init__(self, host_a, host_b, index):
        super(AddressWatchTest, self).__init__()
        self.host_a = host_a
        self.host_b = host_b
        self.index  = index
        self.addr     = 'addr_watch/test_address/%d' % index
        self.conn_a   = None
        self.conn_b   = None
        self.error    = None
        self.sender   = None
        self.receiver = None
        self.n_closed = 0

    def timeout(self):
        self.error = "Timeout Expired"
        if self.conn_a:
            self.conn_a.close()
        if self.conn_b:
            self.conn_b.close()

    def on_start(self, event):
        self.timer    = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.conn_a   = event.container.connect(self.host_a.addresses[0])
        self.conn_b   = event.container.connect(self.host_b.addresses[0])
        self.receiver = event.container.create_receiver(self.conn_a, self.addr)
        self.sender   = event.container.create_sender(self.conn_b, self.addr)

    def on_sendable(self, event):
        if event.sender == self.sender:
            self.conn_a.close()
            self.conn_b.close()

    def on_connection_closed(self, event):
        self.n_closed += 1
        if self.n_closed == 2:
            def check_log_lines():
                with open(self.host_a.logfile_path, 'r') as router_log:
                    log_lines = router_log.read().split("\n")
                    search_lines = [s for s in log_lines if "ADDRESS_WATCH" in s and "on_watch(%d)" % self.index in s]
                    matches = [s for s in search_lines if "loc: 1 rem: 0" in s]
                    if len(matches) == 0:
                        raise Exception("Didn't see local consumer on router 1")
                with open(self.host_b.logfile_path, 'r') as router_log:
                    log_lines = router_log.read().split("\n")
                    search_lines = [s for s in log_lines if "ADDRESS_WATCH" in s and "on_watch(%d)" % self.index in s]
                    matches = [s for s in search_lines if ("loc: 0 rem: 1" in s) or ("loc: 1 rem: 0" in s)]
                    if len(matches) == 0:
                        raise Exception("Didn't see remote consumer and local producer on router 2")

            # Sometimes the CI is so fast that there is not enough time for the router to write to the log file.
            # Try repeatedly until TIMEOUT seconds.
            retry_exception(check_log_lines, delay=2)
            self.timer.cancel()

    def run(self):
        Container(self).run()


class DynamicAddressWatchTest(MessagingHandler):
    def __init__(self, address, watch_host, dest_hosts, expected_local, expected_remote):
        super(DynamicAddressWatchTest, self).__init__()
        self.address = address
        self.watch_host = watch_host
        self.dest_hosts = dest_hosts
        self.expected_local  = expected_local
        self.expected_remote = expected_remote

        self.conn_watch = None
        self.conn_dests = []
        self.error      = None
        self.sender     = None
        self.receiver   = None
        self.dest_receivers = []
        self.phase = "START"

    def fail(self, error=None):
        self.error = error
        if self.conn_watch:
            self.conn_watch.close()
        for conn in self.conn_dests:
            conn.close()
        self.timer.cancel()

    def timeout(self):
        self.fail("Timeout Expired - Phase: %s" % self.phase)

    def setup_dests(self):
        for conn in self.conn_dests:
            self.dest_receivers.append(self.container.create_receiver(conn, self.address))

    def close_dests(self):
        for rx in self.dest_receivers:
            rx.close()
        self.dest_receivers = []

    def on_start(self, event):
        self.container  = event.container
        self.timer      = event.reactor.schedule(10.0, TestTimeout(self))
        self.conn_watch = event.container.connect(self.watch_host.addresses[0])
        self.receiver   = event.container.create_receiver(self.conn_watch, "_local/_testhook/watch_event")
        for host in self.dest_hosts:
            self.conn_dests.append(self.container.connect(host.addresses[0]))

    def on_link_opened(self, event):
        if event.receiver == self.receiver:
            self.sender = event.container.create_sender(self.conn_watch, "_local/_testhook/address_watch")

    def on_sendable(self, event):
        if self.phase == "START":
            self.phase = "SET-WATCH"
            msg = Message(subject='watch', properties={'opcode': 'watch-on', 'address': self.address})
            self.sender.send(msg)

    def on_accepted(self, event):
        if self.phase == "UNWATCH":
            self.fail(None)

    def on_message(self, event):
        msg = event.message
        ap = msg.properties
        local  = ap['local_consumers']
        remote = ap['remote_consumers']
        addr   = ap['address']

        if addr != self.address:
            self.fail("Received a watch for an unexpected address:  Expected %s, got %s" % (self.address, addr))

        ##print("phase=%s, local=%d, remote=%d" % (self.phase, local, remote))

        if self.phase == "SET-WATCH":
            if local == 0 and remote == 0:
                self.phase = "WATCHING"
                self.setup_dests()
            else:
                self.fail("Expected 0 consumers, got local=%d, remote=%d" % (local, remote))

        elif self.phase == "WATCHING":
            if local == self.expected_local and remote == self.expected_remote:
                self.phase = "CLEAR-WATCH"
                self.close_dests()

        elif self.phase == "CLEAR-WATCH":
            if local == 0 and remote == 0:
                self.phase = "UNWATCH"
                msg = Message(subject='watch', properties={'opcode': 'watch-off', 'address': self.address})
                self.sender.send(msg)
            elif local > self.expected_local or remote > self.expected_remote:
                self.fail("Exceeded expected counts: expected l=%d, r=%d; got l=%d, r=%d" % (self.expected_local, self.expected_remote, local, remote))

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

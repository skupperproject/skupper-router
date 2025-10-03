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

from proton import Message, Delivery
from proton.handlers import MessagingHandler
from proton.reactor import Container

from system_test import unittest, Logger
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

        def router(name, mode, connection, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                connection
            ]

            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port_A       = cls.tester.get_port()
        edge_port_B       = cls.tester.get_port()

        router('INT.A', 'interior', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_A}), ["-T"])
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_B}), ["-T"])
        router('EA1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}), None, ["-T"])
        router('EA2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}), None, ["-T"])
        router('EB1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}), None, ["-T"])
        router('EB2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}), None, ["-T"])

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')

    def test_01_delayed_settlement_same_interior(self):
        test = DelayedSettlementTest(self.routers[0].addresses[0],
                                     self.routers[0].addresses[0],
                                     self.routers[0].addresses[0],
                                     'dest.01', 10, [2], False,
                                     test_name="test_01_delayed_settlement_same_interior",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_02_delayed_settlement_different_edges_check_sender(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[2].addresses[0],
                                     'dest.02', 10, [2, 3, 8], False,
                                     test_name="test_02_delayed_settlement_different_edges_check_sender",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_03_delayed_settlement_different_edges_check_receiver(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[5].addresses[0],
                                     'dest.03', 10, [2, 4, 9], False,
                                     test_name="test_03_delayed_settlement_different_edges_check_receiver",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_04_delayed_settlement_different_edges_check_interior(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[0].addresses[0],
                                     'dest.04', 10, [0, 2, 3, 8], False,
                                     test_name="test_04_delayed_settlement_different_edges_check_interior",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_05_no_settlement_same_interior(self):
        test = DelayedSettlementTest(self.routers[0].addresses[0],
                                     self.routers[0].addresses[0],
                                     self.routers[0].addresses[0],
                                     'dest.05', 10, [0, 2, 4, 9], True,
                                     test_name="test_05_no_settlement_same_interior",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_06_no_settlement_different_edges_check_sender(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[2].addresses[0],
                                     'dest.06', 10, [9], True,
                                     test_name="test_06_no_settlement_different_edges_check_sender",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_07_no_settlement_different_edges_check_receiver(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[5].addresses[0],
                                     'dest.07', 10, [0, 9], True,
                                     test_name="test_07_no_settlement_different_edges_check_receiver",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_08_no_settlement_different_edges_check_interior(self):
        test = DelayedSettlementTest(self.routers[2].addresses[0],
                                     self.routers[5].addresses[0],
                                     self.routers[0].addresses[0],
                                     'dest.08', 10, [1, 2, 3, 4, 5, 6, 7, 8], True,
                                     test_name="test_08_no_settlement_different_edges_check_interior",
                                     print_debug=True)
        test.run()
        self.assertIsNone(test.error)

    def test_09_receiver_link_credit_test(self):
        test = RxLinkCreditTest(self.routers[0].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_10_sender_link_credit_test(self):
        test = TxLinkCreditTest(self.routers[0].addresses[0])
        test.run()
        self.assertIsNone(test.error)


class DelayedSettlementTest(MessagingHandler):
    def __init__(self, sender_host, receiver_host, query_host, addr, dlv_count, stuck_list, close_link, test_name,
                 print_debug=False):
        super(DelayedSettlementTest, self).__init__(auto_accept=False)
        self.sender_host   = sender_host
        self.receiver_host = receiver_host
        self.query_host    = query_host
        self.addr          = addr
        self.dlv_count     = dlv_count
        self.stuck_list    = stuck_list
        self.close_link    = close_link
        self.stuck_dlvs    = []

        self.sender_conn      = None
        self.receiver_conn    = None
        self.query_conn       = None
        self.error            = None
        self.timer            = None
        self.poll_timer       = None
        self.receiver         = None
        self.reply_receiver   = None
        self.proxy            = None
        self.query_sender     = None
        self.reply_addr       = None
        self.sender           = None
        self.link_closed      = False
        self.n_tx             = 0
        self.n_rx             = 0
        self.expected_stuck   = 0
        self.deliveries_stuck = 0
        self.logger           =  Logger(title=test_name, print_to_console=print_debug)

    def timeout(self):
        self.error = "Timeout Expired - n_tx=%d, n_rx=%d, expected_stuck=%d deliveries_stuck=%d" %\
            (self.n_tx, self.n_rx, self.expected_stuck, self.deliveries_stuck)
        self.sender_conn.close()
        self.receiver_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def stop_test(self, error):
        self.error = error
        self.sender_conn.close()
        self.receiver_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()
        self.timer.cancel()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.query_conn     = event.container.connect(self.query_host)
        self.query_sender = event.container.create_sender(self.query_conn, "$management")
        self.reply_receiver = event.container.create_receiver(self.query_conn, None, dynamic=True)
        self.receiver_conn  = event.container.connect(self.receiver_host)
        self.receiver       = event.container.create_receiver(self.receiver_conn, self.addr)

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)

            # Create the sender only after the self.proxy is populated.
            # If the sender was created in the on_start, in some cases, the on_sendable
            # is called before the on_link_opened and the self.proxy remains empty.
            # see DISPATCH-1675.
            # The second error in test_04_delayed_settlement_different_edges_check_interior
            # is caused due to the first test failure, so this fix will
            # fix the second failure
            self.sender_conn = event.container.connect(self.sender_host)
            self.sender = event.container.create_sender(self.sender_conn, self.addr)

    def on_sendable(self, event):
        if event.sender == self.sender:
            while self.sender.credit > 0 and self.n_tx < self.dlv_count:
                self.sender.send(Message("Message %d" % self.n_tx))
                self.n_tx += 1

    def on_message(self, event):
        if event.receiver == self.receiver:
            if self.n_rx not in self.stuck_list:
                self.accept(event.delivery)
            else:
                self.stuck_dlvs.append(event.delivery)
            self.n_rx += 1
            if self.n_rx == self.dlv_count:
                self.query_stats(len(self.stuck_list) * 2)
        elif event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            self.accept(event.delivery)
            self.deliveries_stuck = response.results[0].deliveriesStuck
            if self.deliveries_stuck == self.expected_stuck:
                self.logger.log(
                    f"self.deliveries_stuck={self.deliveries_stuck}, self.expected_stuck={self.expected_stuck}")
                # The deliveries stuck queried via management equals what we expected.
                # The test has passed.
                if self.expected_stuck != 0:
                    if self.close_link:
                        self.receiver.close()
                    else:
                        for dlv in self.stuck_dlvs:
                            self.accept(dlv)
                # Once the deliveries have been accepted, we want to make sure that the number of stuck deliveries
                # is back down to zero before we quit this test, so the next test can start
                if self.expected_stuck > 0:
                    self.logger.log(f"self.expected_stuck={self.expected_stuck}")
                    self.query_stats(0)
                else:
                    self.logger.log(f"self.expected_stuck={self.expected_stuck}, stopping test")
                    self.stop_test(None)
            else:
                # The deliveries stuck queried via management is not what we expected.
                # We will keep querying until TIMEOUT seconds to see if our condition comes True.
                self.logger.log(f"self.deliveries_stuck={self.deliveries_stuck}, self.expected_stuck={self.expected_stuck}, starting poll timer")
                self.poll_timer = event.reactor.schedule(0.5, PollTimeout(self))

    def query_stats(self, expected_stuck):
        self.expected_stuck = expected_stuck
        msg = self.proxy.query_router()
        self.query_sender.send(msg)

    def poll_timeout(self):
        self.query_stats(self.expected_stuck)

    def run(self):
        Container(self).run()


class RxLinkCreditTest(MessagingHandler):
    def __init__(self, host):
        super(RxLinkCreditTest, self).__init__(prefetch=0)
        self.host = host

        self.receiver_conn = None
        self.query_conn    = None
        self.addr          = "rx/link/credit/test"
        self.credit_issued = 0
        self.error         = None
        self.get_baseline  = True

        self.stages = ['Setup', 'LinkBlocked', 'LinkUnblocked', '10Credits', '20Credits']
        self.stage  = 0

    def timeout(self):
        self.error = "Timeout Expired - stage: %s" % self.stages[self.stage]
        self.receiver_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def fail(self, error):
        self.error = error
        self.receiver_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()
        self.timer.cancel()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.poll_timer     = None
        self.receiver_conn  = event.container.connect(self.host)
        self.query_conn     = event.container.connect(self.host)
        self.reply_receiver = event.container.create_receiver(self.query_conn, None, dynamic=True)
        self.query_sender   = event.container.create_sender(self.query_conn, "$management")
        self.receiver       = None

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)
            self.receiver   = event.container.create_receiver(self.receiver_conn, self.addr)
            self.reply_receiver.flow(1)
        elif event.receiver == self.receiver:
            self.stage = 1
            self.process()

    def process(self):
        if self.stage == 1:
            #
            # LinkBlocked
            #
            msg = self.proxy.query_router()
            self.query_sender.send(msg)

        elif self.stage == 2:
            #
            # LinkUnblocked
            #
            msg = self.proxy.query_router()
            self.query_sender.send(msg)

        elif self.stage == 3:
            #
            # 10Credits
            #
            msg = self.proxy.query_links()
            self.query_sender.send(msg)

        elif self.stage == 4:
            #
            # 20Credits
            #
            msg = self.proxy.query_links()
            self.query_sender.send(msg)

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            self.reply_receiver.flow(1)
            if self.stage == 1:
                #
                # LinkBlocked
                #
                if self.get_baseline:
                    self.get_baseline = False
                    self.baseline_blocked = response.results[0].linksBlocked
                if response.results[0].linksBlocked == self.baseline_blocked + 1:
                    self.receiver.flow(10)
                    self.stage = 2
                    self.process()
                    return

            elif self.stage == 2:
                #
                # LinkUnblocked
                #
                if response.results[0].linksBlocked == self.baseline_blocked:
                    self.stage = 3
                    self.process()
                    return

            elif self.stage == 3:
                #
                # 10Credits
                #
                for link in response.results:
                    if 'M' + self.addr == link.owningAddr:
                        if link.creditAvailable == 10:
                            self.receiver.flow(10)
                            self.stage = 4
                            self.process()
                            return

            elif self.stage == 4:
                #
                # 20Credits
                #
                for link in response.results:
                    if 'M' + self.addr == link.owningAddr:
                        if link.creditAvailable == 20:
                            self.fail(None)
                            return

            self.poll_timer = event.reactor.schedule(0.5, PollTimeout(self))

    def poll_timeout(self):
        self.process()

    def run(self):
        Container(self).run()


class TxLinkCreditTest(MessagingHandler):
    def __init__(self, host):
        super(TxLinkCreditTest, self).__init__()
        self.host = host

        self.sender_conn   = None
        self.query_conn    = None
        self.addr          = "rx/link/credit/test"
        self.credit_issued = 0
        self.error         = None
        self.get_baseline  = True

        self.stages = ['Setup', 'LinkBlocked', 'LinkUnblocked', '250Credits']
        self.stage  = 0

    def timeout(self):
        self.error = "Timeout Expired - stage: %s" % self.stages[self.stage]
        self.sender_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def fail(self, error):
        self.error = error
        self.sender_conn.close()
        self.query_conn.close()
        if self.poll_timer:
            self.poll_timer.cancel()
        self.timer.cancel()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.poll_timer     = None
        self.sender_conn    = event.container.connect(self.host)
        self.query_conn     = event.container.connect(self.host)
        self.reply_receiver = event.container.create_receiver(self.query_conn, None, dynamic=True)
        self.query_sender   = event.container.create_sender(self.query_conn, "$management")
        self.sender         = None
        self.receiver       = None

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)
            self.sender     = event.container.create_sender(self.sender_conn, self.addr)
        elif event.sender == self.sender:
            self.stage = 1
            self.process()

    def process(self):
        if self.stage == 1:
            #
            # LinkBlocked
            #
            msg = self.proxy.query_router()
            self.query_sender.send(msg)

        elif self.stage == 2:
            #
            # LinkUnblocked
            #
            msg = self.proxy.query_router()
            self.query_sender.send(msg)

        elif self.stage == 3:
            #
            # 250Credits
            #
            msg = self.proxy.query_links()
            self.query_sender.send(msg)

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            if self.stage == 1:
                #
                # LinkBlocked
                #
                if self.get_baseline:
                    self.get_baseline = False
                    self.baseline_blocked = response.results[0].linksBlocked
                if response.results[0].linksBlocked == self.baseline_blocked + 1:
                    self.receiver = event.container.create_receiver(self.sender_conn, self.addr)
                    self.stage = 2
                    self.process()
                    return

            elif self.stage == 2:
                #
                # LinkUnblocked
                #
                if response.results[0].linksBlocked == self.baseline_blocked:
                    self.stage = 3
                    self.process()
                    return

            elif self.stage == 3:
                #
                # 250Credits
                #
                for link in response.results:
                    if 'M' + self.addr == link.owningAddr:
                        if link.creditAvailable == 250:
                            self.fail(None)
                            return

            self.poll_timer = event.reactor.schedule(0.5, PollTimeout(self))

    def poll_timeout(self):
        self.process()

    def run(self):
        Container(self).run()


class RouterTestAutoLink(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTestAutoLink, cls).setUpClass()

        def router(name, mode, connection, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                connection
            ]

            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        inter_router_port = cls.tester.get_port()

        router('INT.A', 'interior', ('listener', {'role': 'normal', 'port': inter_router_port}))
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'route-container', 'port': inter_router_port}),
               ('autoLink', {'address': 'test-address', 'connection': 'connectorToA', 'direction': 'in' }))

    def test_101_recovery_of_2ack_delivery(self):
        test = TwoAckRecoveryTest(self.routers[0].addresses[0],
                                  self.routers[1].addresses[0],
                                  self.routers[1].addresses[0],
                                  'test-address')
        test.run()
        self.assertIsNone(test.error)


class TwoAckRecoveryTest(MessagingHandler):
    """
    Run 2-ack deliveries between two routers using a connection and auto-link.

        sender          receiver
        ======          ========
           send ------------>
           <---------- accept
           settle ---------->

    N messages are sent (multiple of 3)
    2/3 N deliveries are accepted
    1/3 N (half of accepted) are settled.
    Then delete the connection and collect settled/modified updates.
    There should be (2/3)N at both the sender and the receiver (those not already settled).
    """
    def __init__(self, sender_host, receiver_host, query_host, addr):
        super(TwoAckRecoveryTest, self).__init__(auto_accept=False, auto_settle=False)
        self.sender_host   = sender_host
        self.receiver_host = receiver_host
        self.query_host    = query_host
        self.addr          = addr

        self.dlv_count         = 30
        self.sender_conn       = None
        self.receiver_conn     = None
        self.query_conn        = None
        self.error             = None
        self.timer             = None
        self.receiver          = None
        self.sender            = None
        self.query_sender      = None
        self.reply_receiver    = None
        self.proxy             = None
        self.reply_addr        = None
        self.n_sent            = 0
        self.n_accepted        = 0
        self.n_settled_prefail = 0
        self.n_mod_at_sender   = 0
        self.n_mod_at_receiver = 0
        self.link_failed       = False

    def timeout(self):
        self.error = "Timeout Expired - sent=%d, accepted=%d, settled_prefail=%d mod_sender=%d mod_receiver=%d" %\
            (self.n_sent, self.n_accepted, self.n_settled_prefail, self.n_mod_at_sender, self.n_mod_at_receiver)
        self.sender_conn.close()
        self.receiver_conn.close()
        self.query_conn.close()

    def stop_test(self, error):
        self.error = error
        self.sender_conn.close()
        self.receiver_conn.close()
        self.query_conn.close()
        self.timer.cancel()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver_conn  = event.container.connect(self.receiver_host)
        self.sender_conn    = event.container.connect(self.sender_host)
        self.query_conn     = event.container.connect(self.query_host)
        self.query_sender   = event.container.create_sender(self.query_conn, "$management")
        self.receiver       = event.container.create_receiver(self.receiver_conn, self.addr)
        self.reply_receiver = event.container.create_receiver(self.query_conn, dynamic=True)

    def on_sendable(self, event):
        if event.sender == self.sender:
            while self.sender.credit > 0 and self.n_sent < self.dlv_count:
                delivery = self.sender.send(Message(address=self.addr, body={'ordinal': self.n_sent}))
                delivery._ordinal = self.n_sent
                self.n_sent += 1

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr = event.receiver.remote_source.address
            self.proxy      = MgmtMsgProxy(self.reply_addr)
            self.sender     = event.container.create_sender(self.sender_conn, self.addr)

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            if response.status_code != 204:
                self.stop_test("Error from connector deletion: %d (%s)" % (response.status_code, response.status_description))
            self.accept(event.delivery)
        elif event.receiver == self.receiver:
            ordinal = event.message.body['ordinal']
            event.delivery._ordinal = ordinal
            if ordinal % 3 < 2:   # 2/3rds are accepted
                event.delivery.update(Delivery.ACCEPTED)
 
    def on_accepted(self, event):
        if event.sender == self.sender:
            ordinal = event.delivery._ordinal
            if ordinal % 3 == 2:
                self.stop_test("Delivery with unexpected ordinal was accepted, ordinal=%d" % ordinal)
                return
            self.n_accepted += 1
            if ordinal % 3 == 1: # half of the 2/3rds are settled
                self.settle(event.delivery)

    def on_settled(self, event):
        if event.sender == self.sender:
            ordinal = event.delivery._ordinal
            if not self.link_failed:
                self.stop_test("Delivery settled on sender prior to link failure, ordinal=%d" % ordinal)
            else:
                self.n_mod_at_sender += 1
        elif event.receiver == self.receiver:
            ordinal = event.delivery._ordinal
            if not self.link_failed:
                self.n_settled_prefail += 1
                if ordinal % 3 != 1:
                    self.stop_test("Delivery settled on receiver prior to link fail with unexpected ordinal: %d" % ordinal)
                if self.n_settled_prefail == self.dlv_count / 3:
                    self.link_failed = True
                    msg = self.proxy.delete_connector('connectorToA')
                    self.query_sender.send(msg)
            else:
                self.n_mod_at_receiver += 1
        expected_mod = (self.dlv_count * 2) / 3
        if self.n_mod_at_receiver == expected_mod and self.n_mod_at_sender == expected_mod:
            self.stop_test(None)

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

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

from time import sleep

from proton import Message, Delivery
from proton.handlers import MessagingHandler
from proton.reactor import Container, LinkOption

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, TestTimeout
from system_test import unittest, Logger, ROUTER_ADDRESS_TYPE


# ------------------------------------------------
# Helper classes for all tests.
# ------------------------------------------------


class AddressCheckResponse:
    """
    Convenience class for the responses returned by an AddressChecker.
    """

    def __init__(self, status_code, status_description, attrs):
        self.status_code        = status_code
        self.status_description = status_description
        self.attrs              = attrs

    def __getattr__(self, key):
        return self.attrs[key]

    def __str__(self):
        return "Address Check Response: status=%s desc=%s attrs=%s" % \
            (self.status_code, self.status_description, self.attrs)


class AddressChecker:
    """
    Format address-query messages and parse the responses.
    """

    def __init__(self, reply_addr):
        self.reply_addr = reply_addr

    def parse_address_query_response(self, msg):
        ap = msg.properties
        return AddressCheckResponse(ap['statusCode'], ap['statusDescription'], msg.body)

    def make_address_query(self, name):
        ap = {'operation': 'READ', 'type': ROUTER_ADDRESS_TYPE, 'name': name}
        return Message(properties=ap, reply_to=self.reply_addr)

    def make_addresses_query(self):
        ap = {'operation': 'QUERY', 'type': ROUTER_ADDRESS_TYPE}
        return Message(properties=ap, reply_to=self.reply_addr)


class AddressCheckerTimeout:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.address_check_timeout()


class DistributionSkipMapper:
    # 1 means skip that test.
    skip = {'test_01': 0,
            'test_02': 0,
            'test_03': 0,
            'test_04': 0,
            'test_05': 0,
            'test_06': 0,
            'test_07': 0,
            'test_08': 0,
            'test_09': 0,
            'test_10': 0,
            'test_11': 0,
            'test_12': 0,
            'test_13': 0,
            'test_14': 0,
            'test_15': 0,
            'test_16': 1,
            'test_17': 1,
            'test_18': 1,
            'test_19': 1,
            'test_20': 1,
            'test_21': 1,
            'test_22': 1,
            'test_23': 0,
            'test_24': 0,
            'test_25': 0
            }


# ------------------------------------------------
# END Helper classes for all tests.
# ------------------------------------------------


# ================================================================
#     Setup
# ================================================================

class DistributionTests(TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Create a router topology that is a superset of the topologies we will
        need for various tests.  So far, we have only two types of tests:
        3-router linear, and 3-router triangular.  The various tests simply
        attach their senders and receivers appropriately to 'see' their
        desired topology.
        """
        super(DistributionTests, cls).setUpClass()

        # -----------------------------------------------------
        # Here are some chunks of configuration that will be
        # the same on all routers.
        # -----------------------------------------------------

        def router(name, more_config):

            config = [('router',  {'mode': 'interior', 'id': name}),
                      ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                      ('address', {'prefix': 'balanced',  'distribution': 'balanced'}),
                      ('address', {'prefix': 'multicast', 'distribution': 'multicast'})
                      ]                                 \
                + more_config

            config = Qdrouterd.Config(config)

            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        #
        #     Connection picture
        #
        #           1           1
        #         A <-------- B <------ C
        #          ^ 2       ^ 2
        #           \       /
        #            \     /
        #             \   /
        #              \ /
        #               D
        #
        #  Note: in the above picture, an arrow from, i.e., B to A
        #        means that B initiates the connection from itself to A.
        #        So if you see "B ----> A" in the picture, you should also
        #        see a connector block in the configuration of B that
        #        connects to an inter-router port on A.
        #

        A_client_port          = cls.tester.get_port()
        B_client_port          = cls.tester.get_port()
        C_client_port          = cls.tester.get_port()
        D_client_port          = cls.tester.get_port()

        A_inter_router_port_1  = cls.tester.get_port()
        A_inter_router_port_2  = cls.tester.get_port()
        B_inter_router_port_1  = cls.tester.get_port()
        B_inter_router_port_2  = cls.tester.get_port()

        # "Route-container port" does not mean that the port
        # contains a route.  It means that any client that
        # connectsd to the port is considered to be a route-
        # container.
        A_route_container_port = cls.tester.get_port()
        B_route_container_port = cls.tester.get_port()
        C_route_container_port = cls.tester.get_port()
        D_route_container_port = cls.tester.get_port()

        # Costs for balanced tests. The 'balanced' distribution
        # takes these costs into account in its algorithm.
        # Costs are associated not with routers, but with the
        # connections between routers.  In the config, they may
        # be attached to the inter-router listener, or the connector,
        # or both.  If both the inter-router listener and the
        # connector have associated costs, the higher of the two
        # will be used.
        cls.A_B_cost =   10
        cls.B_C_cost =   20
        cls.A_D_cost =   50
        cls.B_D_cost =  100

        router('A',
               [
                   ('listener',
                    {'port': A_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': A_inter_router_port_1
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': A_inter_router_port_2
                     }
                    ),
                   ('listener',
                    {'port': A_route_container_port,  # route-container is listener number 3
                     'stripAnnotations': 'no',
                     'role': 'route-container'
                     }
                    )
               ]
               )

        router('B',
               [
                   ('listener',
                    {'port': B_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': B_inter_router_port_1
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': B_inter_router_port_2
                     }
                    ),
                   ('listener',
                    {'port': B_route_container_port,  # route-container is number 3
                     'stripAnnotations': 'no',
                     'role': 'route-container'
                     }
                    ),
                   ('connector',
                    {'name': 'connectorToA',
                     'role': 'inter-router',
                     'port': A_inter_router_port_1,
                     'cost': cls.A_B_cost
                     }
                    )
               ]
               )

        router('C',
               [
                   ('listener',
                    {'port': C_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'port': C_route_container_port,  # route-container is number 1
                     'stripAnnotations': 'no',
                     'role': 'route-container'
                     }
                    ),
                   ('connector',
                    {'name': 'connectorToB',
                     'role': 'inter-router',
                     'port': B_inter_router_port_1,
                     'cost' : cls.B_C_cost
                     }
                    )
               ]
               )

        router('D',
               [
                   ('listener',
                    {'port': D_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'port': D_route_container_port,  # route-container is number 1
                     'stripAnnotations': 'no',
                     'role': 'route-container'
                     }
                    ),
                   ('connector',
                    {'name': 'connectorToA',
                     'role': 'inter-router',
                     'port': A_inter_router_port_2,
                     'cost' : cls.A_D_cost
                     }
                    ),
                   ('connector',
                    {'name': 'connectorToB',
                     'role': 'inter-router',
                     'port': B_inter_router_port_2,
                     'cost' : cls.B_D_cost
                     }
                    )
               ]
               )

        router_A = cls.routers[0]
        router_B = cls.routers[1]
        router_C = cls.routers[2]
        router_D = cls.routers[3]

        cls.A_route_container_addr = router_A.addresses[3]
        cls.B_route_container_addr = router_B.addresses[3]
        cls.C_route_container_addr = router_C.addresses[1]
        cls.D_route_container_addr = router_D.addresses[1]

        router_A.wait_router_connected('B')
        router_A.wait_router_connected('C')
        router_A.wait_router_connected('D')

        cls.A_addr = router_A.addresses[0]
        cls.B_addr = router_B.addresses[0]
        cls.C_addr = router_C.addresses[0]
        cls.D_addr = router_D.addresses[0]

    @unittest.skipIf(DistributionSkipMapper.skip['test_01'], 'Test skipped during development.')
    def test_01_targeted_sender_AC(self):
        name = 'test_01'
        test = TargetedSenderTest(name, self.A_addr, self.C_addr, "closest/01")
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_02'], 'Test skipped during development.')
    def test_02_targeted_sender_DC(self):
        name = 'test_02'
        test = TargetedSenderTest(name, self.D_addr, self.C_addr, "closest/02")
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_03'], 'Test skipped during development.')
    def test_03_anonymous_sender_AC(self):
        name = 'test_03'
        test = AnonymousSenderTest(name, self.A_addr, self.C_addr)
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_04'], 'Test skipped during development.')
    def test_04_anonymous_sender_DC(self):
        name = 'test_04'
        test = AnonymousSenderTest(name, self.D_addr, self.C_addr)
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_05'], 'Test skipped during development.')
    def test_05_dynamic_reply_to_AC(self):
        name = 'test_05'
        test = DynamicReplyTo(name, self.A_addr, self.C_addr)
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_06'], 'Test skipped during development.')
    def test_06_dynamic_reply_to_DC(self):
        name = 'test_06'
        test = DynamicReplyTo(name, self.D_addr, self.C_addr)
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_09'], 'Test skipped during development.')
    def test_09_closest_linear(self):
        name = 'test_09'
        test = ClosestTest(name,
                           self.A_addr,
                           self.B_addr,
                           self.C_addr,
                           "addr_09",
                           print_debug=False
                           )
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_10'], 'Test skipped during development.')
    def test_10_closest_mesh(self):
        name = 'test_10'
        test = ClosestTest(name,
                           self.A_addr,
                           self.B_addr,
                           self.D_addr,
                           "addr_10"
                           )
        test.run()
        self.assertIsNone(test.error)

        #
        #     Cost picture for balanced distribution tests.
        #
        #              10          20
        #         A <-------- B <------ C
        #          ^         ^
        #           \       /
        #       50   \     /  100
        #             \   /
        #              \ /
        #               D
        #
        #
        #
        #  Here is how the message balancing should work for
        #  various total number of messages, up to 100:
        #
        #  NOTE: remember these messages are all unsettled.
        #        And will stay that way.  This is not a realistic
        #        usage scenario, but it the best way to test the
        #        balanced distribution algorithm.
        #
        #  1. Messages start flowing in at A.  They will all
        #     be used by A (sent to its receiver) until the
        #     total == cost ( A, B ).
        #
        #  2. At that point, A will start sharing with B,
        #     one-for-me-one-for-you. (So A will go to 11 before
        #     B gets its first message.)
        #
        #  3. A and B will count up until B reaches
        #     cost ( B, C )
        #     B will then start sharings its messages with C,
        #     one-for-me-one-for-you.  (So B will go to 21 before
        #      C gets its first message.)
        #
        #  4. However note: it is NOT round-robin at this point.
        #     A is still taking every other message, B is only getting
        #     A's overflow, and now B is sharing half of that with C.
        #     So at this point B will start falling farther behind A.
        #
        #  5. The totals here are completely deterministic, so we pass
        #     to the test a 'slop' amount of 0.
        #
        #    total   near --10--> mid ---20--> far
        #
        #     1        1            0            0
        #     10      10            0            0
        #     11      11            0            0
        #     12      11            1            0
        #     13      12            1            0
        #     14      12            2            0
        #     ...
        #     50      30           20            0
        #     51      31           20            0
        #     52      31           21            0
        #     53      32           21            0
        #     54      32           21            1
        #     55      33           21            1
        #     56      33           22            1
        #     57      34           22            1
        #     58      34           22            2
        #     59      35           22            2
        #     60      35           23            2
        #     ...
        #     100     55           33           12
        #

    @unittest.skipIf(DistributionSkipMapper.skip['test_11'], 'Test skipped during development.')
    def test_11_balanced_linear(self):
        name = 'test_11'
        # slop is how much the second two values may diverge from
        # the expected.  But they still must sum to total - A.
        total      = 100
        expected_A = 55
        expected_B = 33
        expected_C = 12
        # FIXME - or investigate -- I believe this slop
        # should not be necessary -- the distribution
        # algorithm should be perfectly deterministic.
        # But -- without it, I am getting 0.3% failure rate on this test.
        slop = 1
        omit_middle_receiver = False

        test = BalancedTest(name,
                            self.A_addr,
                            self.B_addr,
                            self.C_addr,
                            "addr_11",
                            total,
                            expected_A,
                            expected_B,
                            expected_C,
                            slop,
                            omit_middle_receiver
                            )
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_12'], 'Test skipped during development.')
    def test_12_balanced_linear_omit_middle_receiver(self):
        name = 'test_12'
        # If we omit the middle receiver, then router A will count
        # up to cost ( A, B ) and the keep counting up a further
        # cost ( B, C ) before it starts to spill over.
        # That is, it will count up to
        #    cost ( A, B ) + cost ( B, C ) == 30
        # After that it will start sharing downstream (router C)
        # one-for-me-one-for-you.  So when the number of total messages
        # is odd, A will be 31 ahead of C.  When total message count is
        # even, A will be 30 ahead.
        # As in the other linear scenario, there is no 'slop' here.
        total      = 100
        expected_A = 65
        expected_B = 0
        expected_C = 35
        # FIXME - or investigate -- I believe this slop
        # should not be necessary -- the distribution
        # algorithm should be perfectly deterministic.
        # But -- without it, I am getting 0.2% failure rate on this test.
        slop = 1
        omit_middle_receiver = True

        test = BalancedTest(name,
                            self.A_addr,
                            self.B_addr,
                            self.C_addr,
                            "addr_12",
                            total,
                            expected_A,
                            expected_B,
                            expected_C,
                            slop,
                            omit_middle_receiver
                            )
        test.run()
        self.assertIsNone(test.error)

        #     Reasoning for the triangular balanced case:
        #
        #     Cost picture
        #
        #              10          20
        #         A <-------- B <------ C
        #          ^         ^
        #           \       /
        #       50   \     /  100
        #             \   /
        #              \ /
        #               D
        #
        # We are doing  ( A, B, D ), with the sender attached at A.
        # All these messages are unsettled, which is what allows us to
        # see how the balanced distribution algorithm works.
        #
        #  1. total unsettled msgs at A cannot be more than B_cost + 1,
        #     and also cannot be more than D_cost + 1
        #
        #  2. A will always keep the message for itself (for its own receiver)
        #     if it can do so without violating rule (1).
        #
        #  3. So, A will count up to 11, and then it will start alternating
        #     with B.
        #
        #  4. When A counts up to 51, it must also start sharing with D.
        #     It will alternate between B and D.
        #
        #  5. As long as B does not yet have 100 messages, it will not
        #     share with D.
        #
        #  6. So! at 100 messages total, A must be above both of its
        #     neighbors by that neighbor's cost, or 1 more -- and the total
        #     of all 3 must sum to 100.
        #
        #     A = B + 10      B = A - 10
        #     A = D + 50      D = A - 50
        #     A + B + D == 100
        #     -->
        #     A + (A - 10) + (A - 50) == 100
        #     3A - 60 == 100
        #     A == 53.333...
        #     A == 54
        #
        #     so B + D == 46
        #     A is 10 or 11 > B --> B == 44 or 43
        #     A is 50 or 51 > D --> D ==  4 or  3
        #     B == 43 and D == 3
        #
        #     So pass these values in to the test: (54, 43, 3)
        #     and test that:
        #       1. A is exactly that value.
        #       2. B and D sum to 100 - A
        #       3. B and D are both with 1 of their expected values.
        #

    @unittest.skipIf(DistributionSkipMapper.skip['test_13'], 'Test skipped during development.')
    def test_13_balanced_mesh(self):
        name = 'test_13'
        total      = 100
        expected_A = 54
        expected_B = 43
        expected_D = 3
        slop       = 1
        omit_middle_receiver = False
        test = BalancedTest(name,
                            self.A_addr,
                            self.B_addr,
                            self.D_addr,
                            "addr_13",
                            total,
                            expected_A,
                            expected_B,
                            expected_D,
                            slop,
                            omit_middle_receiver
                            )
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_14'], 'Test skipped during development.')
    def test_14_multicast_linear(self):
        name = 'test_14'
        test = MulticastTest(name,
                             self.A_addr,
                             self.B_addr,
                             self.C_addr,
                             "addr_14")
        test.run()
        self.assertIsNone(test.error)

    @unittest.skipIf(DistributionSkipMapper.skip['test_15'], 'Test skipped during development.')
    def test_15_multicast_mesh(self):
        name = 'test_15'
        test = MulticastTest(name,
                             self.A_addr,
                             self.B_addr,
                             self.D_addr,
                             "addr_15"
                             )
        test.run()
        self.assertIsNone(test.error)


# ================================================================
#     Tests
# ================================================================
next_link_sequence = 1


def link_name(suffix=None):
    global next_link_sequence
    suffix = suffix or "name"
    name = "link-%s.%d" % (suffix, next_link_sequence)
    next_link_sequence += 1
    return name


class TargetedSenderTest (MessagingHandler):
    """
    A 'targeted' sender is one in which we tell the router what
    address we want to send to. (As opposed to letting the router
    pass back an address to us.)
    """

    def __init__(self, test_name, send_addr, recv_addr, destination):
        super(TargetedSenderTest, self).__init__(prefetch=0)
        self.send_addr  = send_addr
        self.recv_addr  = recv_addr
        self.dest       = destination
        self.error      = None
        self.sender     = None
        self.receiver   = None
        self.n_expected = 10
        self.n_sent     = 0
        self.n_received = 0
        self.n_accepted = 0
        self.test_name = test_name

    def timeout(self):
        self.error = "Timeout Expired: n_sent=%d n_received=%d n_accepted=%d" % \
                     (self.n_sent, self.n_received, self.n_accepted)
        self.send_conn.close()
        self.recv_conn.close()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_conn = event.container.connect(self.send_addr)
        self.recv_conn = event.container.connect(self.recv_addr)
        self.sender   = event.container.create_sender(self.send_conn, self.dest)
        self.receiver = event.container.create_receiver(self.recv_conn, self.dest)
        self.receiver.flow(self.n_expected)

    def send(self):
        while self.sender.credit > 0 and self.n_sent < self.n_expected:
            msg = Message(body=self.n_sent)
            self.sender.send(msg)
            self.n_sent += 1

    def on_sendable(self, event):
        if self.n_sent < self.n_expected:
            self.send()

    def on_accepted(self, event):
        self.n_accepted += 1

    def on_message(self, event):
        self.n_received += 1
        if self.n_received == self.n_expected:
            self.receiver.close()
            self.send_conn.close()
            self.recv_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class DynamicTarget(LinkOption):

    def apply(self, link):
        link.target.dynamic = True
        link.target.address = None


class AnonymousSenderTest (MessagingHandler):
    """
    An 'anonymous' sender is one in which we let the router tell
    us what address the sender should use.  It will pass back this
    information to us when we get the on_link_opened event.
    """

    def __init__(self, test_name, send_addr, recv_addr):
        super(AnonymousSenderTest, self).__init__()
        self.send_addr = send_addr
        self.recv_addr = recv_addr

        self.error     = None
        self.recv_conn = None
        self.send_conn = None
        self.sender    = None
        self.receiver  = None
        self.address   = None

        self.expected   = 10
        self.n_sent     = 0
        self.n_received = 0
        self.n_accepted = 0
        self.test_name  = test_name

    def timeout(self):
        self.error = "Timeout Expired: n_sent=%d n_received=%d n_accepted=%d" % \
                     (self.n_sent, self.n_received, self.n_accepted)
        self.send_conn.close()
        self.recv_conn.close()

    def on_start(self, event):
        self.timer     = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_conn = event.container.connect(self.send_addr)
        self.recv_conn = event.container.connect(self.recv_addr)
        self.sender    = event.container.create_sender(self.send_conn, options=DynamicTarget())

    def send(self):
        while self.sender.credit > 0 and self.n_sent < self.expected:
            self.n_sent += 1
            m = Message(address=self.address, body="Message %d of %d" % (self.n_sent, self.expected))
            self.sender.send(m)

    def on_link_opened(self, event):
        if event.sender == self.sender:
            # Here we are told the address that we will use for the sender.
            self.address = self.sender.remote_target.address
            self.receiver = event.container.create_receiver(self.recv_conn, self.address)

    def on_sendable(self, event):
        self.send()

    def on_message(self, event):
        if event.receiver == self.receiver:
            self.n_received += 1

    def on_accepted(self, event):
        self.n_accepted += 1
        if self.n_accepted == self.expected:
            self.send_conn.close()
            self.recv_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


# =======================================================================
# =======================================================================
class DynamicReplyTo(MessagingHandler):
    """
    In this test we have a separate 'client' and 'server' with separate
    connections.  The client sends requests to the server, and embeds in
    them its desired reply-to address.  The server uses that address to
    send back messages.  The tests ends with success if the client receives
    the expected number of replies, or with failure if we time out before
    that happens.
    """

    def __init__(self, test_name, client_addr, server_addr):
        super(DynamicReplyTo, self).__init__(prefetch=10)
        self.client_addr        = client_addr
        self.server_addr        = server_addr
        self.dest               = "closest.dynamicRequestResponse"
        self.error              = None
        self.server_receiver    = None
        self.client_receiver    = None
        self.client_sender      = None
        self.server_sender      = None
        self.n_expected         = 10
        self.n_sent             = 0
        self.received_by_server = 0
        self.received_by_client = 0
        self.test_name          = test_name
        self.server_receiver_ready = False
        self.client_receiver_ready = False
        self.reply_to_addr = None
        self.senders_created = False
        self.addr_check_timer = None
        self.addr_check_sender = None
        self.container = None
        self.num_attempts = 0
        self.addr_check_receiver = None

    def timeout(self):
        self.error = "Timeout Expired: n_sent=%d received_by_server=%d received_by_client=%d" % \
                     (self.n_sent, self.received_by_server, self.received_by_client)
        self.client_connection.close()
        self.server_connection.close()

    def address_check_timeout(self):
        self.addr_check_sender.send(self.addr_checker.make_address_query("M" + self.dest))

    def bail(self):
        self.timer.cancel()
        self.server_receiver.close()
        self.client_receiver.close()
        self.addr_check_sender.close()
        self.addr_check_receiver.close()
        self.server_sender.close()
        self.client_sender.close()
        self.client_connection.close()
        self.server_connection.close()

    def on_start(self, event):
        self.timer             = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        # separate connections to simulate client and server.
        self.client_connection = event.container.connect(self.client_addr)
        self.server_connection = event.container.connect(self.server_addr)
        self.server_receiver   = event.container.create_receiver(self.server_connection, self.dest)
        self.client_receiver   = event.container.create_receiver(self.client_connection, None, dynamic=True)
        self.addr_check_sender = event.container.create_sender(self.client_connection, "$management")
        self.container         = event.container
        self.addr_check_receiver = event.container.create_receiver(self.client_connection, dynamic=True)

    def create_senders(self):
        if not self.senders_created:
            self.senders_created = True
            self.client_sender = self.container.create_sender(self.client_connection, self.dest)
            self.server_sender = self.container.create_sender(self.server_connection, None)

    def on_link_opened(self, event):
        if event.receiver == self.addr_check_receiver:
            self.addr_checker = AddressChecker(self.addr_check_receiver.remote_source.address)
        if not self.server_receiver_ready and event.receiver == self.server_receiver:
            self.server_receiver_ready = True
        if not self.client_receiver_ready and event.receiver == self.client_receiver:
            self.client_receiver_ready = True
        if self.server_receiver_ready and self.client_receiver_ready:
            if self.num_attempts == 0:
                self.reply_to_addr = self.client_receiver.remote_source.address
                self.num_attempts += 1
                self.addr_check_timer = event.reactor.schedule(3, AddressCheckerTimeout(self))

    def on_sendable(self, event):
        if self.reply_to_addr is None:
            return

        if event.sender == self.client_sender:
            while event.sender.credit > 0 and self.n_sent < self.n_expected:
                # We send to server, and tell it how to reply to the client.
                request = Message(body=self.n_sent,
                                  address=self.dest,
                                  reply_to=self.reply_to_addr)
                event.sender.send(request)
                self.n_sent += 1

    def on_message(self, event):
        if event.receiver == self.addr_check_receiver:
            response = self.addr_checker.parse_address_query_response(event.message)
            # Create the senders if the address has propagated.
            if response.status_code == 200 and response.remoteCount == 1:
                self.create_senders()
            else:
                if self.num_attempts < 2:
                    self.num_attempts += 1
                    self.addr_check_timer = event.reactor.schedule(3, AddressCheckerTimeout(self))
                else:
                    self.error = "Address %s did not propagate to the router to which the sender is attached" % self.dest
                    self.bail()
                    return

        # Server gets a request and responds to
        # the address that is embedded in the message.
        if event.receiver == self.server_receiver :
            self.server_sender.send(Message(address=event.message.reply_to,
                                            body="Reply hazy, try again later."))
            self.received_by_server += 1

        # Client gets a response and counts it.
        elif event.receiver == self.client_receiver :
            self.received_by_client += 1
            if self.received_by_client == self.n_expected:
                self.bail()

    def run(self):
        Container(self).run()


class ClosestTest (MessagingHandler):
    """
    Test whether distance-based message routing works in a 3-router
    network. The network may be linear or mesh, depending on which routers the
    caller gives us.

    (Illustration is a linear network.)

    sender -----> Router_1 -----> Router_2 -----> Router_3
                     |              |                |
                     v              v                v
                  rcvr_1         rcvr_2           rcvr_3

    With a linear network of 3 routers, set up a sender on router_1, and then 1
    receiver each on all 3 routers.  Requirement: router 2 is closer than
    router 3 by one hop.

    Once the closest pair of receivers has received the required amount of
    messages they are closed. Neither of the other receivers should have
    received any messages, as they were not the closest receivers.

    Repeat until all three routers have received messages.

    The test is set up in phases to ensure there are no races between fast/slow
    clients and routers:

    Phase 1: bring up all connections and create receivers, wait until all
    receivers can finished link setup.

    Phase 2: poll routers until the subscriber count shows all receivers are
    ready.

    Phase 3: start the sender, wait until on_sendable triggers

    Phase 4: send test messages, verify distribution.

    Once a batch of messages has completely arrived at the current closest
    receiver, close that receiver. Note that there can be a few seconds before
    that loss of link propagates to all three routers.  During that time any
    sent messages may fail with outcome RELEASED - that is expected.
    """

    def __init__(self, test_name, router_1, router_2, router_3, addr_suffix,
                 print_debug=False):
        super(ClosestTest, self).__init__(prefetch=0)
        self.test_name   = test_name
        self.error       = None
        self.router_1    = router_1
        self.router_2    = router_2
        self.router_3    = router_3
        self.addr_suffix = addr_suffix
        self.dest        = "closest/" + addr_suffix

        # after send_batch sent messages are accepted, verify the closest
        # receivers have consumed the batch.
        self.send_batch = 4
        self.n_sent = 0
        self.n_received = 0
        self.sender = None

        self.rx_opened  = 0
        self.rx_count_1 = 0
        self.rx_count_2 = 0
        self.rx_count_3 = 0
        self.closest_rx = None

        # for checking the number of subscribers to the destination address
        self.addr_checker        = None
        self.addr_check_receiver = None
        self.addr_check_sender   = None

        self._logger = Logger(title=test_name, print_to_console=print_debug)

    def _new_message(self):
        """Add expected rx for log tracing
        """
        return Message(body="%s: Hello, %s." % (self.test_name, self.closest_rx.name),
                       address=self.dest)

    def timeout(self):
        self.bail("Timeout Expired")

    def bail(self, error_text=None):
        self.timer.cancel()
        self.error = error_text
        self.send_cnx.close()
        self.cnx_1.close()
        self.cnx_2.close()
        self.cnx_3.close()
        if error_text:
            self._logger.log("Test failed: %s" % error_text)
            self._logger.dump()

    def on_start(self, event):
        self.timer    = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_cnx = event.container.connect(self.router_1)
        self.cnx_1    = event.container.connect(self.router_1)
        self.cnx_2    = event.container.connect(self.router_2)
        self.cnx_3    = event.container.connect(self.router_3)

        self.recv_1  = event.container.create_receiver(self.cnx_1, self.dest,
                                                       name=link_name("rx1"))
        self.recv_2  = event.container.create_receiver(self.cnx_2, self.dest,
                                                       name=link_name("rx2"))
        self.recv_3  = event.container.create_receiver(self.cnx_3, self.dest,
                                                       name=link_name("rx3"))

        # grant way more flow than necessary so we can consume any mis-routed
        # message (that is a bug)
        self.recv_1.flow(100)
        self.recv_2.flow(100)
        self.recv_3.flow(100)

        self.closest_rx = self.recv_1

    def on_link_opened(self, event):
        self._logger.log("Link opened: %s" % event.link.name)
        if event.link.is_receiver:
            if event.receiver == self.addr_check_receiver:
                # Phase 2: address checker address available:
                self._logger.log("Address check receiver link opened")
                assert self.addr_checker is None
                self.addr_checker = AddressChecker(self.addr_check_receiver.remote_source.address)
                self.addr_check_sender = event.container.create_sender(self.cnx_1,
                                                                       "$management",
                                                                       name=link_name("check_tx"))
            elif self.rx_opened != 3:
                # Phase 1: wait for receivers to come up
                assert event.receiver in [self.recv_1, self.recv_2, self.recv_3]
                self._logger.log("Test receiver link opened: %s" % event.link.name)
                self.rx_opened += 1
                if self.rx_opened == 3:
                    # All test receivers links open, start Phase 2:
                    self._logger.log("Opening address check receiver")
                    self.addr_check_receiver = event.container.create_receiver(self.cnx_1,
                                                                               dynamic=True,
                                                                               name=link_name("check_rx"))
                    self.addr_check_receiver.flow(100)

        elif event.sender == self.addr_check_sender:
            # fire off the first address query
            self.addr_check()

    def on_message(self, event):
        self._logger.log("on_message %s" % event.receiver.name)
        if event.receiver == self.addr_check_receiver:
            # This is a response to one of my address-readiness checking messages.
            response = self.addr_checker.parse_address_query_response(event.message)
            if response.status_code == 200 and response.subscriberCount == 1 and response.remoteCount == 2:
                # now we know that we have two subscribers on attached router, and two remote
                # routers that know about the address. The network is ready.
                self._logger.log("Network ready")
                assert self.sender is None
                self.sender = event.container.create_sender(self.send_cnx,
                                                            self.dest,
                                                            name=link_name("sender"))
            else:
                # Not ready yet - poll again. This will continue until either
                # the routers have updated or the test times out
                self._logger.log("Network not ready yet: %s" % response)
                self.addr_check_receiver.flow(1)
                sleep(0.25)
                self.addr_check()
        else:
            # This is a payload message.
            self.n_received += 1

            # Count the messages that have come in for
            # each receiver.
            if event.receiver == self.recv_1:
                self.rx_count_1 += 1
                self._logger.log("RX 1 got message, total=%s" % self.rx_count_1)
            elif event.receiver == self.recv_2:
                self.rx_count_2 += 1
                self._logger.log("RX 2 got message, total=%s" % self.rx_count_2)
            elif event.receiver == self.recv_3:
                self.rx_count_3 += 1
                self._logger.log("RX 3 got message, total=%s" % self.rx_count_3)
            else:
                self.bail("Unexpected receiver?")

    def on_sendable(self, event):
        self._logger.log("on_sendable %s" % event.sender.name)
        if event.sender == self.sender:
            if self.n_sent == 0:
                # only have one message outstanding
                self._logger.log("sending (sent=%s)" % self.n_sent)
                self.sender.send(self._new_message())
                self.n_sent += 1

    def on_settled(self, event):
        self._logger.log("On settled, link: %s" % event.link.name)
        if event.link == self.sender:
            dlv = event.delivery
            if dlv.remote_state == Delivery.ACCEPTED:
                if self.closest_rx == self.recv_1:
                    if self.rx_count_2 or self.rx_count_3:
                        self.bail("Error: non-closest client got message!")
                    else:
                        self.rx_count_1 += 1
                        if self.rx_count_1 == self.send_batch:
                            self._logger.log("RX 1 complete, closing")
                            self.recv_1.close()
                            self.closest_rx = self.recv_2
                            # now wait for close to complete before sending more
                        else:
                            self.sender.send(self._new_message())
                            self.n_sent += 1
                elif self.closest_rx == self.recv_2:
                    if self.rx_count_1 != self.send_batch or self.rx_count_3:
                        self.bail("Error: non-closest client got message!")
                    else:
                        self.rx_count_2 += 1
                        if self.rx_count_2 == self.send_batch:
                            self._logger.log("RX 2 complete, closing")
                            self.recv_2.close()
                            self.closest_rx = self.recv_3
                            # now wait for close to complete before sending more
                        else:
                            self.sender.send(self._new_message())
                            self.n_sent += 1
                elif self.closest_rx == self.recv_3:
                    if (self.rx_count_1 != self.send_batch or self.rx_count_2 != self.send_batch):
                        self.bail("Error: non-closest client got message!")
                    else:
                        self.rx_count_3 += 1
                        if self.rx_count_3 == self.send_batch:
                            self._logger.log("RX 3 complete, closing, Test Done!")
                            self.recv_3.close()
                            self.closest_rx = None
                            self.bail()
                        else:
                            self.sender.send(self._new_message())
                            self.n_sent += 1
                else:
                    self.bail("Error: self.closest_rx no match!")
            else:
                self._logger.log("Delivery Not Accepted: %s" % dlv.remote_state)
                if dlv.remote_state == Delivery.RELEASED:
                    # This occurs when the loss-of-rx-link event has not been
                    # propagated to all routers. When that happens the message
                    # may be forwarded back to the router with the last known
                    # location of the dropped link.
                    # This is expected, just try again
                    sleep(0.25)
                    self.sender.send(self._new_message())
                    self.n_sent += 1
                else:
                    self.bail("Error: unexpected delivery failure: %s"
                              % dlv.remote_state)

    def on_link_closed(self, event):
        self._logger.log("Link closed %s" % event.link.name)
        if event.link == self.recv_1 and self.closest_rx == self.recv_2:
            self._logger.log("Next send for RX 2")
            sleep(2.0)  # give time for the link loss to propagate
            self.sender.send(self._new_message())
            self.n_sent += 1

        elif event.link == self.recv_2 and self.closest_rx == self.recv_3:
            self._logger.log("Next send for RX 3")
            sleep(2.0)  # give time for the link loss to propagate
            self.sender.send(self._new_message())
            self.n_sent += 1

    def addr_check(self):
        # Send the message that will query the management code to discover
        # information about our destination address. We cannot make our payload
        # sender until the network is ready.
        #
        # BUGALERT: We have to prepend the 'M' to this address prefix
        # because that's what the router does internally.  Someday this
        # may change.
        self._logger.log("Query addresses...")
        self.addr_check_sender.send(self.addr_checker.make_address_query("M" + self.dest))

    def run(self):
        container = Container(self)
        container.run()


class BalancedTest (MessagingHandler):
    """
    This test is topology-agnostic. This code thinks of its nodes as 1, 2, 3.
    The caller knows if they are linear or triangular, or a tree.  It calculates
    the expected results for nodes 1, 2, and 3, and also tells me if there can be
    a little 'slop' in the results.
    ( Slop can happen in some topologies when you can't tell whether spillover
    will happen first to node 2, or to node 3.
    """

    def __init__(self,
                 test_name,
                 router_1,
                 router_2,
                 router_3,
                 addr_suffix,
                 total_messages,
                 expected_1,
                 expected_2,
                 expected_3,
                 slop,
                 omit_middle_receiver
                 ):
        super(BalancedTest, self).__init__(prefetch=0, auto_accept=False)
        self.error       = None
        self.router_3    = router_3
        self.router_2    = router_2
        self.router_1    = router_1
        self.addr_suffix = addr_suffix
        self.dest        = "balanced/" + addr_suffix

        self.total_messages  = total_messages
        self.n_sent          = 0
        self.n_received      = 0

        self.recv_1 = None
        self.recv_2 = None
        self.recv_3 = None

        self.count_3 = 0
        self.count_2 = 0
        self.count_1 = 0

        self.expected_1 = expected_1
        self.expected_2 = expected_2
        self.expected_3 = expected_3
        self.slop       = slop
        self.omit_middle_receiver = omit_middle_receiver

        self.address_check_timer    = None
        self.address_check_receiver = None
        self.address_check_sender   = None

        self.payload_sender = None
        self.test_name      = test_name

    def timeout(self):
        self.bail("Timeout Expired ")

    def address_check_timeout(self):
        self.address_check()

    def bail(self, text):
        self.timer.cancel()
        self.error = text
        self.cnx_3.close()
        self.cnx_2.close()
        self.cnx_1.close()
        if self.address_check_timer:
            self.address_check_timer.cancel()

    def on_start(self, event):
        self.timer    = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.cnx_3    = event.container.connect(self.router_3)
        self.cnx_2    = event.container.connect(self.router_2)
        self.cnx_1    = event.container.connect(self.router_1)

        self.recv_3  = event.container.create_receiver(self.cnx_3,  self.dest)
        if self.omit_middle_receiver is False :
            self.recv_2 = event.container.create_receiver(self.cnx_2,  self.dest)
        self.recv_1  = event.container.create_receiver(self.cnx_1,  self.dest)

        self.recv_3.flow(self.total_messages)
        if self.omit_middle_receiver is False :
            self.recv_2.flow(self.total_messages)
        self.recv_1.flow(self.total_messages)

        self.address_check_receiver = event.container.create_receiver(self.cnx_1, dynamic=True)
        self.address_check_sender   = event.container.create_sender(self.cnx_1, "$management")

    def on_link_opened(self, event):
        if event.receiver:
            event.receiver.flow(self.total_messages)
        if event.receiver == self.address_check_receiver:
            # My address check-link has opened: make the address_checker
            self.address_checker = AddressChecker(self.address_check_receiver.remote_source.address)
            self.address_check()

    def on_message(self, event):

        if self.n_received >= self.total_messages:
            return   # Sometimes you can get a message or two even after you have called bail().

        if event.receiver == self.address_check_receiver:
            # This is one of my route-readiness checking messages.
            response = self.address_checker.parse_address_query_response(event.message)
            if self.omit_middle_receiver is True :
                expected_remotes = 1
            else :
                expected_remotes = 2

            if response.status_code == 200 and response.subscriberCount == 1 and response.remoteCount == expected_remotes:
                # Got confirmation of dest addr fully propagated through network.
                # Since I have 3 nodes, I want to see 1 subscriber (which is on the local router) and
                # 2 remote routers that know about my destination address.
                # Now we can safely make the payload sender without getting a 'No Path To Destination' error.
                self.payload_sender = event.container.create_sender(self.cnx_1, self.dest)
                # And we can quit checking.
                if self.address_check_timer:
                    self.address_check_timer.cancel()
                    self.address_check_timer = None
            else:
                # If the latest check did not find the link-attack route ready,
                # schedule another check a little while from now.
                self.address_check_timer = event.reactor.schedule(0.50, AddressCheckerTimeout(self))

        else:
            self.n_received += 1

            if event.receiver == self.recv_1:
                self.count_1 += 1
            elif event.receiver == self.recv_2:
                self.count_2 += 1
            elif event.receiver == self.recv_3:
                self.count_3 += 1

            # I do not check for count_1 + count_2 + count_3 == total,
            # because it always will be due to how the code counts things.
            if self.n_received == self.total_messages:
                if abs(self.count_1 - self.expected_1) > self.slop or \
                   abs(self.count_2 - self.expected_2) > self.slop or \
                   abs(self.count_3 - self.expected_3) > self.slop  :
                    self.bail("expected: ( %d, %d, %d )  got: ( %d, %d, %d )" % (self.expected_1, self.expected_2, self.expected_3, self.count_1, self.count_2, self.count_3))
                else:
                    self.bail(None)  # All is well.

    def on_sendable(self, event):
        if self.n_sent < self.total_messages and event.sender == self.payload_sender :
            msg = Message(body="Hello, balanced.",
                          address=self.dest
                          )
            self.payload_sender.send(msg)
            self.n_sent += 1

    def address_check(self):
        # Send the message that will query the management code to discover
        # information about our destination address. We cannot make our payload
        # sender until the network is ready.
        #
        # BUGALERT: We have to prepend the 'M' to this address prefix
        # because that's what the router does internally.  Someday this
        # may change.
        self.address_check_sender.send(self.address_checker.make_address_query("M" + self.dest))

    def run(self):
        container = Container(self)
        container.run()


class MulticastTest (MessagingHandler):
    """
    Using multicast, we should see all receivers get everything,
    whether the topology is linear or mesh.
    """

    def __init__(self,
                 test_name,
                 router_1,
                 router_2,
                 router_3,
                 addr_suffix):
        super(MulticastTest, self).__init__(prefetch=0)
        self.error       = None
        self.router_1    = router_1
        self.router_2    = router_2
        self.router_3    = router_3
        self.addr_suffix = addr_suffix
        self.dest        = "multicast/" + addr_suffix

        self.n_to_send = 50
        self.n_sent    = 0

        self.n_received = 0

        self.count_1_a = 0
        self.count_1_b = 0
        self.count_2_a = 0
        self.count_2_b = 0
        self.count_3_a = 0
        self.count_3_b = 0

        self.timer    = None
        self.send_cnx = None
        self.cnx_1    = None
        self.cnx_2    = None
        self.cnx_3    = None
        self.recv_1_a = None
        self.recv_1_b = None
        self.recv_2_a = None
        self.recv_2_b = None
        self.recv_3_a = None
        self.recv_3_b = None

        self.addr_check_timer    = None
        self.addr_check_receiver = None
        self.addr_check_sender   = None
        self.sender              = None
        self.bailed              = False
        self.test_name           = test_name

    def timeout(self):
        self.bail("Timeout Expired ")

    def address_check_timeout(self):
        self.addr_check()

    def bail(self, text):
        self.timer.cancel()
        self.error = text
        self.send_cnx.close()
        self.cnx_1.close()
        self.cnx_2.close()
        self.cnx_3.close()
        if self.addr_check_timer:
            self.addr_check_timer.cancel()

    def on_start(self, event):
        self.timer    = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_cnx = event.container.connect(self.router_1)
        self.cnx_1    = event.container.connect(self.router_1)
        self.cnx_2    = event.container.connect(self.router_2)
        self.cnx_3    = event.container.connect(self.router_3)

        # Warning!
        # The two receiver-links on each router must be given
        # explicit distinct names, or we will in fact only get
        # one link.  And then wonder why receiver 2 on each
        # router isn't getting any messages.
        self.recv_1_a  = event.container.create_receiver(self.cnx_1, self.dest, name=link_name())
        self.recv_1_b  = event.container.create_receiver(self.cnx_1, self.dest, name=link_name())

        self.recv_2_a  = event.container.create_receiver(self.cnx_2,  self.dest, name=link_name())
        self.recv_2_b  = event.container.create_receiver(self.cnx_2,  self.dest, name=link_name())

        self.recv_3_a  = event.container.create_receiver(self.cnx_3,  self.dest, name=link_name())
        self.recv_3_b  = event.container.create_receiver(self.cnx_3,  self.dest, name=link_name())

        self.recv_1_a.flow(self.n_to_send)
        self.recv_2_a.flow(self.n_to_send)
        self.recv_3_a.flow(self.n_to_send)

        self.recv_1_b.flow(self.n_to_send)
        self.recv_2_b.flow(self.n_to_send)
        self.recv_3_b.flow(self.n_to_send)

        self.addr_check_receiver = event.container.create_receiver(self.cnx_1, dynamic=True)
        self.addr_check_sender   = event.container.create_sender(self.cnx_1, "$management")

    def on_link_opened(self, event):
        if event.receiver:
            event.receiver.flow(self.n_to_send)
        if event.receiver == self.addr_check_receiver:
            # my addr-check link has opened: make the addr_checker with the given address.
            self.addr_checker = AddressChecker(self.addr_check_receiver.remote_source.address)
            self.addr_check()

    def on_sendable(self, event):
        if self.sender and self.n_sent < self.n_to_send :
            msg = Message(body="Hello, closest.",
                          address=self.dest)
            dlv = self.sender.send(msg)
            self.n_sent += 1
            dlv.settle()

    def on_message(self, event):
        if event.receiver == self.addr_check_receiver:
            # This is a response to one of my address-readiness checking messages.
            response = self.addr_checker.parse_address_query_response(event.message)
            if response.status_code == 200 and response.subscriberCount == 2 and response.remoteCount == 2:
                # now we know that we have two subscribers on attached router, and two remote
                # routers that know about the address. The network is ready.
                # Now we can make the sender without getting a
                # "No Path To Destination" error.
                self.sender = event.container.create_sender(self.send_cnx, self.dest)

                # And we can quit checking.
                if self.addr_check_timer:
                    self.addr_check_timer.cancel()
                    self.addr_check_timer = None
            else:
                # If the latest check did not find the link-attack route ready,
                # schedule another check a little while from now.
                self.addr_check_timer = event.reactor.schedule(1, AddressCheckerTimeout(self))
        else :
            # This is a payload message.
            self.n_received += 1

            # Count the messages that have come in for
            # each receiver.
            if event.receiver == self.recv_1_a:
                self.count_1_a += 1
            elif event.receiver == self.recv_1_b:
                self.count_1_b += 1
            elif event.receiver == self.recv_2_a:
                self.count_2_a += 1
            elif event.receiver == self.recv_2_b:
                self.count_2_b += 1
            elif event.receiver == self.recv_3_a:
                self.count_3_a += 1
            elif event.receiver == self.recv_3_b:
                self.count_3_b += 1

            if self.n_received >= 6 * self.n_to_send :
                # In multicast, everybody gets everything.
                # Our reception count should be 6x our send-count,
                # and all receiver-counts should be equal.
                if self.count_1_a == self.count_1_b and self.count_1_b == self.count_2_a and self.count_2_a == self.count_2_b and self.count_2_b == self.count_3_a and self.count_3_a == self.count_3_b :
                    self.bail(None)
                    self.bailed = True
                else:
                    self.bail("receivers not equal: %d %d %d %d %d %d" % (self.count_1_a, self.count_1_b, self.count_2_a, self.count_2_b, self.count_3_a, self.count_3_b))
                    self.bailed = True

    def addr_check(self):
        # Send the message that will query the management code to discover
        # information about our destination address. We cannot make our payload
        # sender until the network is ready.
        #
        # BUGALERT: We have to prepend the 'M' to this address prefix
        # because that's what the router does internally.  Someday this
        # may change.
        self.addr_check_sender.send(self.addr_checker.make_address_query("M" + self.dest))

    def run(self):
        container = Container(self)
        container.run()


if __name__ == '__main__':
    unittest.main(main_module())

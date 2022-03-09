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

import os
import re
from subprocess import PIPE
from time import sleep
from threading import Timer

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton.utils import BlockingConnection

from qpid_dispatch.management.client import Node

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, MgmtMsgProxy, TestTimeout
from system_test import Logger
from system_test import QdManager
from system_test import unittest
from system_test import Process
from test_broker import FakeBroker


class AddrTimer:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.check_address()


class EdgeRouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(EdgeRouterTest, cls).setUpClass()

        def router(name, mode, connection, extra=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no', 'role': 'route-container'}),
                ('address', {'prefix': 'closest', 'distribution': 'closest'}),
                ('address', {'prefix': 'spread', 'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
                connection
            ]

            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port_A = cls.tester.get_port()
        edge_port_B = cls.tester.get_port()

        router('INT.A', 'interior', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_A}))
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_B}))
        router('EA1', 'edge', ('connector', {'name': 'edge', 'role': 'edge',
                                             'port': edge_port_A}
                               ),
               ('connector', {'name': 'edge.1', 'role': 'edge',
                              'port': edge_port_B}
                )
               )

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')

        # 1 means skip that test.
        cls.skip = {'test_01' : 0
                    }

    def setUp(self):
        super().setUp()
        self.success = False
        self.timer_delay = 2
        self.max_attempts = 3
        self.attempts = 0

    def run_qdstat(self, args, regexp=None, address=None):
        p = self.popen(
            ['qdstat', '--bus', str(address or self.router.addresses[0]),
             '--timeout', str(TIMEOUT)] + args,
            name='qdstat-' + self.id(), stdout=PIPE, expect=None,
            universal_newlines=True)

        out = p.communicate()[0]
        assert p.returncode == 0, \
            "qdstat exit status %s, output:\n%s" % (p.returncode, out)
        if regexp:
            assert re.search(regexp, out,
                             re.I), "Can't find '%s' in '%s'" % (
                regexp, out)
        return out

    def can_terminate(self):
        if self.attempts == self.max_attempts:
            return True

        if self.success:
            return True

        return False

    def run_int_b_edge_qdstat(self):
        outs = self.run_qdstat(['--edge'],
                               address=self.routers[2].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "INT.B" in line and "yes" in line:
                self.success = True

    def run_int_a_edge_qdstat(self):
        outs = self.run_qdstat(['--edge'],
                               address=self.routers[2].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "INT.A" in line and "yes" in line:
                self.success = True

    def schedule_int_a_qdstat_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.run_int_a_edge_qdstat).start()
                self.attempts += 1

    def schedule_int_b_qdstat_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.run_int_b_edge_qdstat).start()
                self.attempts += 1

    def test_01_active_flag(self):
        """
        In this test, we have one edge router connected to two interior
        routers. One connection is to INT.A and another connection is to
        INT.B . But only one of these connections is active. We use qdstat
        to make sure that only one of these connections is active.
        Then we kill the router with the active connection and make sure
        that the other connection is now the active one
        """
        if self.skip['test_01'] :
            self.skipTest("Test skipped during development.")

        success = False
        outs = self.run_qdstat(['--edge'],
                               address=self.routers[0].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "EA1" in line and "yes" in line:
                success = True
        if not success:
            self.fail("Active edge connection not found not found for "
                      "interior router")

        outs = self.run_qdstat(['--edge'],
                               address=self.routers[2].addresses[0])
        conn_map_edge = dict()
        #
        # We dont know which interior router the edge will connect to.
        #
        conn_map_edge["INT.A"] = False
        conn_map_edge["INT.B"] = False
        lines = outs.split("\n")
        for line in lines:
            if "INT.A" in line and "yes" in line:
                conn_map_edge["INT.A"] = True
            if "INT.B" in line and "yes" in line:
                conn_map_edge["INT.B"] = True

        if conn_map_edge["INT.A"] and conn_map_edge["INT.B"]:
            self.fail("Edhe router has two active connections to interior "
                      "routers. Should have only one")

        if not conn_map_edge["INT.A"] and not conn_map_edge["INT.B"]:
            self.fail("There are no active aconnections to interior routers")

        if conn_map_edge["INT.A"]:
            #
            # INT.A has the active connection. Let's kill INT.A and see
            # if the other connection becomes active
            #
            EdgeRouterTest.routers[0].teardown()
            self.schedule_int_b_qdstat_test()

            while not self.can_terminate():
                pass

            self.assertTrue(self.success)

        elif conn_map_edge["INT.B"]:
            #
            # INT.B has the active connection. Let's kill INT.B and see
            # if the other connection becomes active
            #
            EdgeRouterTest.routers[1].teardown()
            self.schedule_int_a_qdstat_test()

            while not self.can_terminate():
                pass

            self.assertTrue(self.success)


class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, mode, connection, extra=None):
            config = [
                ('router', {'mode': mode, 'id': name, "helloMaxAgeSeconds": '10'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no', 'role': 'route-container'}),
                ('address', {'prefix': 'closest', 'distribution': 'closest'}),
                ('address', {'prefix': 'spread', 'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
                connection
            ]

            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port_A       = cls.tester.get_port()
        edge_port_B       = cls.tester.get_port()

        router('INT.A', 'interior', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_A}))
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_B}))
        router('EA1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EA2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EB1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))
        router('EB2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')

        cls.skip = {'test_01' : 0,
                    'test_02' : 0,
                    'test_03' : 0,
                    'test_04' : 0,
                    'test_05' : 0,
                    'test_06' : 0,
                    'test_07' : 0,
                    'test_08' : 0,
                    'test_09' : 0,
                    'test_10' : 0,
                    'test_11' : 0,
                    'test_12' : 0,
                    'test_13' : 0,
                    'test_14' : 0,
                    'test_15' : 0,
                    'test_16' : 0,
                    'test_17' : 0,
                    'test_18' : 0,
                    'test_19' : 0,
                    'test_20' : 0,
                    'test_21' : 0,
                    'test_22' : 0,
                    'test_23' : 0,
                    'test_24' : 0,
                    'test_25' : 0,
                    'test_26' : 0,
                    'test_27' : 0,
                    'test_28' : 0,
                    'test_29' : 0,
                    'test_30' : 0,
                    'test_31' : 0,
                    'test_32' : 0,
                    'test_33' : 0,
                    'test_34' : 0,
                    'test_35' : 0,
                    'test_36' : 0,
                    'test_37' : 0,
                    'test_38' : 0,
                    'test_39' : 0,
                    'test_40' : 0,
                    'test_41' : 0,
                    'test_42' : 0,
                    'test_43':  0,
                    'test_44':  0,
                    'test_45':  0,
                    'test_46':  0,
                    'test_47':  0,
                    'test_48':  0,
                    'test_49':  0,
                    'test_50':  0,
                    'test_51':  0,
                    'test_52':  0,
                    'test_53':  0,
                    'test_54':  0,
                    'test_55':  0,
                    'test_56':  0,
                    'test_57':  0,
                    'test_58':  0,
                    'test_59':  0,
                    'test_60':  0,
                    'test_61':  0,
                    'test_62':  0,
                    'test_63':  0,
                    'test_64':  0,
                    'test_65':  0,
                    'test_66':  0,
                    'test_67':  0,
                    'test_68':  0,
                    'test_69':  0,
                    'test_70':  0,
                    'test_71':  0,
                    'test_72':  0,
                    'test_73':  0
                    }

    def test_01_connectivity_INTA_EA1(self):
        if self.skip['test_01'] :
            self.skipTest("Test skipped during development.")

        test = ConnectivityTest(self.routers[0].addresses[0],
                                self.routers[2].addresses[0],
                                'EA1')
        test.run()
        self.assertIsNone(test.error)

    def test_02_connectivity_INTA_EA2(self):
        if self.skip['test_02'] :
            self.skipTest("Test skipped during development.")

        test = ConnectivityTest(self.routers[0].addresses[0],
                                self.routers[3].addresses[0],
                                'EA2')
        test.run()
        self.assertIsNone(test.error)

    def test_03_connectivity_INTB_EB1(self):
        if self.skip['test_03'] :
            self.skipTest("Test skipped during development.")

        test = ConnectivityTest(self.routers[1].addresses[0],
                                self.routers[4].addresses[0],
                                'EB1')
        test.run()
        self.assertIsNone(test.error)

    def test_04_connectivity_INTB_EB2(self):
        if self.skip['test_04'] :
            self.skipTest("Test skipped during development.")

        test = ConnectivityTest(self.routers[1].addresses[0],
                                self.routers[5].addresses[0],
                                'EB2')
        test.run()
        self.assertIsNone(test.error)

    def test_05_dynamic_address_same_edge(self):
        if self.skip['test_05'] :
            self.skipTest("Test skipped during development.")

        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[2].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_06_dynamic_address_interior_to_edge(self):
        if self.skip['test_06'] :
            self.skipTest("Test skipped during development.")

        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[0].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_07_dynamic_address_edge_to_interior(self):
        if self.skip['test_07'] :
            self.skipTest("Test skipped during development.")

        test = DynamicAddressTest(self.routers[0].addresses[0],
                                  self.routers[2].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_08_dynamic_address_edge_to_edge_one_interior(self):
        if self.skip['test_08'] :
            self.skipTest("Test skipped during development.")

        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[3].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_09_dynamic_address_edge_to_edge_two_interior(self):
        if self.skip['test_09'] :
            self.skipTest("Test skipped during development.")

        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[4].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_10_mobile_address_same_edge(self):
        if self.skip['test_10'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressTest(self.routers[2].addresses[0],
                                 self.routers[2].addresses[0],
                                 "test_10")
        test.run()
        self.assertIsNone(test.error)

    def test_11_mobile_address_interior_to_edge(self):
        if self.skip['test_11'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressTest(self.routers[2].addresses[0],
                                 self.routers[0].addresses[0],
                                 "test_11")
        test.run()
        self.assertIsNone(test.error)

    def test_12_mobile_address_edge_to_interior(self):
        if self.skip['test_12'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressTest(self.routers[0].addresses[0],
                                 self.routers[2].addresses[0],
                                 "test_12")
        test.run()
        if test.error is not None:
            test.logger.dump()
        self.assertIsNone(test.error)

    def test_13_mobile_address_edge_to_edge_one_interior(self):
        if self.skip['test_13'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressTest(self.routers[2].addresses[0],
                                 self.routers[3].addresses[0],
                                 "test_13")
        test.run()
        self.assertIsNone(test.error)

    def test_14_mobile_address_edge_to_edge_two_interior(self):
        if self.skip['test_14'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressTest(self.routers[2].addresses[0],
                                 self.routers[4].addresses[0],
                                 "test_14")
        test.run()
        self.assertIsNone(test.error)

    # One sender two receiver tests.
    # One sender and two receivers on the same edge
    def test_15_mobile_address_same_edge(self):
        if self.skip['test_15'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "test_15")
        test.run()
        self.assertIsNone(test.error)

    # One sender and two receivers on the different edges. The edges are
    #  hanging off the  same interior router.
    def test_16_mobile_address_edge_to_another_edge_same_interior(self):
        if self.skip['test_16'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      "test_16")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on the interior and sender on the edge
    def test_17_mobile_address_edge_to_interior(self):
        if self.skip['test_17'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[0].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "test_17")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on the edge and the sender on the interior
    def test_18_mobile_address_interior_to_edge(self):
        if self.skip['test_18'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      "test_18")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on the edge and the sender on the 'other' interior
    def test_19_mobile_address_other_interior_to_edge(self):
        if self.skip['test_19'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      "test_19")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on the edge and the sender on the edge of
    # the 'other' interior
    def test_20_mobile_address_edge_to_edge_two_interiors(self):
        if self.skip['test_20'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      "test_20")
        test.run()
        self.assertIsNone(test.error)

    # One receiver in an edge, another one in interior and the sender
    # is on the edge of another interior
    def test_21_mobile_address_edge_interior_receivers(self):
        if self.skip['test_21'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[4].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "test_21")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers one on each interior router and and an edge sender
    # connectoed to the first interior
    def test_22_mobile_address_edge_sender_two_interior_receivers(self):
        if self.skip['test_22'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[0].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      "test_22")
        test.run()
        self.assertIsNone(test.error)

    def test_23_mobile_address_edge_sender_two_edge_receivers(self):
        if self.skip['test_23'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressOneSenderTwoReceiversTest(self.routers[4].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "test_23")
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender and 3 receivers all on the same edge
    def test_24_multicast_mobile_address_same_edge(self):
        if self.skip['test_24'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.24")
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender and receiver on one edge and 2 receivers on another edge
    # all in the same  interior
    def test_25_multicast_mobile_address_different_edges_same_interior(self):
        if self.skip['test_25'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[3].addresses[0],
                                          "multicast.25")
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on each edge, one receiver on interior and sender
    # on the edge
    def test_26_multicast_mobile_address_edge_to_interior(self):
        if self.skip['test_26'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.26")
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on the interior
    def test_27_multicast_mobile_address_interior_to_edge(self):
        if self.skip['test_27'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.27")
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on an interior that is not connected
    # to the edges.
    def test_28_multicast_mobile_address_other_interior_to_edge(self):
        if self.skip['test_28'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[1].addresses[0],
                                          "multicast.28")
        test.run()
        self.assertIsNone(test.error)

    # Sender on an interior and 3 receivers connected to three different edges
    def test_29_multicast_mobile_address_edge_to_edge_two_interiors(self):
        if self.skip['test_29'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.29")
        test.run()
        self.assertIsNone(test.error)

    def test_30_multicast_mobile_address_all_edges(self):
        if self.skip['test_30'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[5].addresses[0],
                                          "multicast.30")
        test.run()
        self.assertIsNone(test.error)

    ######### Multicast Large message tests ######################

    # 1 Sender and 3 receivers all on the same edge

    def test_31_multicast_mobile_address_same_edge(self):
        if self.skip['test_31'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.31",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender on one edge and 3 receivers on another edge all in the same
    # interior
    def test_32_multicast_mobile_address_different_edges_same_interior(self):
        if self.skip['test_32'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[3].addresses[0],
                                          "multicast.32",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on each edge, one receiver on interior and sender
    # on the edge
    def test_33_multicast_mobile_address_edge_to_interior(self):
        if self.skip['test_33'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.33",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on the interior
    def test_34_multicast_mobile_address_interior_to_edge(self):
        if self.skip['test_34'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.34",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on an interior that is not connected
    # to the edges.
    def test_35_multicast_mobile_address_other_interior_to_edge(self):
        if self.skip['test_35'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[1].addresses[0],
                                          "multicast.35",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    # Sender on an interior and 3 receivers connected to three different edges
    def test_36_multicast_mobile_address_edge_to_edge_two_interiors(self):
        if self.skip['test_36'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.36",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    def test_37_multicast_mobile_address_all_edges(self):
        if self.skip['test_37'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[5].addresses[0],
                                          "multicast.37",
                                          large_msg=True)
        test.run()
        self.assertIsNone(test.error)

    def test_38_mobile_addr_event_three_receivers_same_interior(self):
        if self.skip['test_38'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressEventTest(self.routers[2].addresses[0],
                                      self.routers[3].addresses[0],
                                      self.routers[3].addresses[0],
                                      self.routers[2].addresses[0],
                                      self.routers[0].addresses[0],
                                      "test_38", check_remote=False, subscriber_count=2)

        test.run()
        self.assertIsNone(test.error)

    def test_39_mobile_addr_event_three_receivers_diff_interior(self):
        if self.skip['test_39'] :
            self.skipTest("Test skipped during development.")

        # This will test the QDRC_EVENT_ADDR_TWO_DEST event
        test = MobileAddressEventTest(self.routers[2].addresses[0],
                                      self.routers[4].addresses[0],
                                      self.routers[5].addresses[0],
                                      self.routers[2].addresses[0],
                                      self.routers[0].addresses[0],
                                      "test_39", check_remote=True, subscriber_count=1)

        test.run()
        self.assertIsNone(test.error)

    def test_40_drop_rx_client_multicast_large_message(self):
        if self.skip['test_40'] :
            self.skipTest("Test skipped during development.")

        # test what happens if some multicast receivers close in the middle of
        # a multiframe transfer
        test = MobileAddrMcastDroppedRxTest(self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            "multicast.40")
        test.run()
        self.assertIsNone(test.error)

    def test_41_drop_rx_client_multicast_small_message(self):
        if self.skip['test_41'] :
            self.skipTest("Test skipped during development.")

        # test what happens if some multicast receivers close in the middle of
        # a multiframe transfer
        test = MobileAddrMcastDroppedRxTest(self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            self.routers[2].addresses[0],
                                            "multicast.40", large_msg=False)
        test.run()
        self.assertIsNone(test.error)

    def test_42_anon_sender_mobile_address_same_edge(self):
        if self.skip['test_42'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "test_42")
        test.run()
        self.assertIsNone(test.error)

    def test_43_anon_sender_mobile_address_interior_to_edge(self):
        if self.skip['test_43'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[0].addresses[0],
                                          "test_43")
        test.run()
        self.assertIsNone(test.error)

    def test_44_anon_sender_mobile_address_edge_to_interior(self):
        if self.skip['test_44'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "test_44")
        test.run()
        self.assertIsNone(test.error)

    def test_45_anon_sender_mobile_address_edge_to_edge_one_interior(self):
        if self.skip['test_45'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          "test_45")
        test.run()
        self.assertIsNone(test.error)

    def test_46_anon_sender_mobile_address_edge_to_edge_two_interior(self):
        if self.skip['test_46'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[4].addresses[0],
                                          "test_46")
        test.run()
        self.assertIsNone(test.error)

    def test_47_anon_sender_mobile_address_large_msg_same_edge(self):
        if self.skip['test_47'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "test_47", True)
        test.run()
        self.assertIsNone(test.error)

    def test_48_anon_sender_mobile_address_large_msg_interior_to_edge(self):
        if self.skip['test_48'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[0].addresses[0],
                                          "test_48", True)
        test.run()
        self.assertIsNone(test.error)

    def test_49_anon_sender_mobile_address_large_msg_edge_to_interior(self):
        if self.skip['test_49'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "test_49", True)
        test.run()
        self.assertIsNone(test.error)

    def test_50_anon_sender_mobile_address_large_msg_edge_to_edge_one_interior(self):
        if self.skip['test_50'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          "test_50", True)
        test.run()
        self.assertIsNone(test.error)

    def test_51_anon_sender_mobile_address_large_msg_edge_to_edge_two_interior(self):
        if self.skip['test_51'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressAnonymousTest(self.routers[2].addresses[0],
                                          self.routers[4].addresses[0],
                                          "test_51", True)
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender and 3 receivers all on the same edge
    def test_52_anon_sender_multicast_mobile_address_same_edge(self):
        if self.skip['test_52'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.52",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender and receiver on one edge and 2 receivers on another edge
    # all in the same  interior
    def test_53_anon_sender_multicast_mobile_address_different_edges_same_interior(self):
        if self.skip['test_53'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[3].addresses[0],
                                          "multicast.53",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on each edge, one receiver on interior and sender
    # on the edge
    def test_54_anon_sender_multicast_mobile_address_edge_to_interior(self):
        if self.skip['test_54'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.54",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on the interior
    def test_55_anon_sender_multicast_mobile_address_interior_to_edge(self):
        if self.skip['test_55'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.55",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on an interior that is not connected
    # to the edges.
    def test_56_anon_sender_multicast_mobile_address_other_interior_to_edge(self):
        if self.skip['test_56'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[1].addresses[0],
                                          "multicast.56",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Sender on an interior and 3 receivers connected to three different edges
    def test_57_anon_sender_multicast_mobile_address_edge_to_edge_two_interiors(self):
        if self.skip['test_57'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.57",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_58_anon_sender_multicast_mobile_address_all_edges(self):
        if self.skip['test_58'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[5].addresses[0],
                                          "multicast.58",
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    ######### Multicast Large message anon sender tests ####################

    # 1 Sender and 3 receivers all on the same edge

    def test_59_anon_sender_multicast_mobile_address_same_edge(self):
        if self.skip['test_59'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.59",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # 1 Sender on one edge and 3 receivers on another edge all in the same
    # interior
    def test_60_anon_sender_multicast_mobile_address_different_edges_same_interior(self):
        if self.skip['test_60'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[3].addresses[0],
                                          "multicast.60",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Two receivers on each edge, one receiver on interior and sender
    # on the edge
    def test_61_anon_sender_multicast_mobile_address_edge_to_interior(self):
        if self.skip['test_61'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          self.routers[2].addresses[0],
                                          "multicast.61",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on the interior
    def test_62_anon_sender_multicast_mobile_address_interior_to_edge(self):
        if self.skip['test_62'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.62",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Receivers on the edge and sender on an interior that is not connected
    # to the edges.
    def test_63_anon_sender_multicast_mobile_address_other_interior_to_edge(self):
        if self.skip['test_63'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[1].addresses[0],
                                          "multicast.63",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    # Sender on an interior and 3 receivers connected to three different edges
    def test_64_anon_sender_multicast_mobile_address_edge_to_edge_two_interiors(self):
        if self.skip['test_64'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[0].addresses[0],
                                          "multicast.64",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_65_anon_sender_multicast_mobile_address_all_edges(self):
        if self.skip['test_65'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[5].addresses[0],
                                          "multicast.65",
                                          large_msg=True,
                                          anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_66_anon_sender_drop_rx_client_multicast_large_message(self):
        # test what happens if some multicast receivers close in the middle of
        # a multiframe transfer. The sender is an anonymous sender.
        if self.skip['test_66'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddrMcastAnonSenderDroppedRxTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "multicast.66")
        test.run()
        self.assertIsNone(test.error)

    def test_67_drop_rx_client_multicast_small_message(self):
        # test what happens if some multicast receivers close in the middle of
        # a multiframe transfer. The sender is an anonymous sender.
        if self.skip['test_67'] :
            self.skipTest("Test skipped during development.")

        test = MobileAddrMcastAnonSenderDroppedRxTest(self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      "multicast.67",
                                                      large_msg=False)
        test.run()
        self.assertIsNone(test.error)

    def run_qdstat(self, args, regexp=None, address=None):
        if args:
            popen_arg = ['qdstat', '--bus', str(address or self.router.addresses[0]),
                         '--timeout', str(TIMEOUT)] + args
        else:
            popen_arg = ['qdstat', '--bus',
                         str(address or self.router.addresses[0]),
                         '--timeout', str(TIMEOUT)]

        p = self.popen(popen_arg,
                       name='qdstat-' + self.id(), stdout=PIPE, expect=None,
                       universal_newlines=True)

        out = p.communicate()[0]
        assert p.returncode == 0, \
            "qdstat exit status %s, output:\n%s" % (p.returncode, out)
        if regexp:
            assert re.search(regexp, out,
                             re.I), "Can't find '%s' in '%s'" % (
                regexp, out)
        return out

    def test_68_edge_qdstat_all_routers(self):
        # Connects to an edge router and runs "qdstat --all-routers"
        # "qdstat --all-routers" is same as "qdstat --all-routers --g"
        # Connecting to an edge router and running "qdstat --all-routers""will only yield the
        # summary statistics of the edge router. It will not show statistics of the interior routers.
        outs = self.run_qdstat(['--all-routers'],
                               address=self.routers[2].addresses[0])
        self.assertIn("Router Id                        EA1", outs)

        outs = self.run_qdstat(['--all-routers', '--all-entities'],
                               address=self.routers[2].addresses[0])
        # Check if each entity  section is showing
        self.assertIn("Router Links", outs)
        self.assertIn("Router Addresses", outs)
        self.assertIn("Connections", outs)
        self.assertIn("AutoLinks", outs)
        self.assertIn("Auto Links", outs)
        self.assertIn("Router Statistics", outs)
        self.assertIn("Router Id                        EA1", outs)

        self.assertIn("Memory Pools", outs)

        outs = self.run_qdstat(['-c', '--all-routers'],
                               address=self.routers[2].addresses[0])

        # Verify that the the edhe uplink connection is showing
        self.assertIn("INT.A", outs)
        self.assertNotIn("inter-router", outs)

        outs = self.run_qdstat(['--all-entities'],
                               address=self.routers[2].addresses[0])
        # Check if each entity  section is showing
        self.assertIn("Router Links", outs)
        self.assertIn("Router Addresses", outs)
        self.assertIn("Connections", outs)
        self.assertIn("AutoLinks", outs)
        self.assertIn("Auto Links", outs)
        self.assertIn("Router Statistics", outs)
        self.assertIn("Router Id                        EA1", outs)

        self.assertIn("Memory Pools", outs)

    def test_69_interior_qdstat_all_routers(self):
        # Connects to an interior router and runs "qdstat --all-routers"
        # "qdstat --all-routers" is same as "qdstat --all-routers --all-entities"
        # Connecting to an interior router and running "qdstat --all-routers""will yield the
        # summary statistics of all the interior routers.
        outs = self.run_qdstat(['--all-routers'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Statistics"), 2)

        outs = self.run_qdstat(['--all-routers', '-nv'],
                               address=self.routers[0].addresses[0])
        # 5 occurences including section headers
        self.assertEqual(outs.count("INT.A"), 5)
        self.assertEqual(outs.count("INT.B"), 5)

        outs = self.run_qdstat(['--all-routers', '--all-entities'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Links"), 2)
        self.assertEqual(outs.count("Router Addresses"), 2)
        self.assertEqual(outs.count("Connections"), 12)
        self.assertEqual(outs.count("Router Statistics"), 2)
        self.assertEqual(outs.count("Memory Pools"), 2)

        outs = self.run_qdstat(['--all-routers', '-nv'],
                               address=self.routers[0].addresses[0])
        # 5 occurrences including section headers
        self.assertEqual(outs.count("INT.A"), 5)
        self.assertEqual(outs.count("INT.B"), 5)

        outs = self.run_qdstat(['-c', '--all-routers'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("INT.A"), 2)
        self.assertEqual(outs.count("INT.B"), 2)

        outs = self.run_qdstat(['-l', '--all-routers'],
                               address=self.routers[0].addresses[0])

        # Two edge-downlinks from each interior to the two edges, 4 in total.
        self.assertEqual(outs.count("edge-downlink"), 4)

        # Gets all entity information of the interior router
        outs = self.run_qdstat(['--all-entities'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Links"), 1)
        self.assertEqual(outs.count("Router Addresses"), 1)
        self.assertEqual(outs.count("Router Statistics"), 1)

        has_error = False
        try:
            # You cannot combine --all-entities  with -c
            outs = self.run_qdstat(['-c', '--all-entities'],
                                   address=self.routers[0].addresses[0])
        except Exception as e:
            if "error: argument --all-entities: not allowed with argument -c/--connections" in str(e):
                has_error = True

        self.assertTrue(has_error)

        has_error = False
        try:
            outs = self.run_qdstat(['-r', 'INT.A', '--all-routers'],
                                   address=self.routers[0].addresses[0])
        except Exception as e:
            if "error: argument --all-routers: not allowed with argument -r/--router" in str(e):
                has_error = True

        self.assertTrue(has_error)

    def test_70_qdstat_edge_router_option(self):
        # Tests the --edge-router (-d) option of qdstat
        # The goal of this test is to connect to any router in the
        # network (interior or edge) and ask for details about a specific edge router
        # You could not do that before DISPATCH-1580

        # Makes a connection to an interior router INT.A and runs qdstat
        # asking for all connections of an edge router EA1
        outs = self.run_qdstat(['-d', 'EA1', '-c'],
                               address=self.routers[0].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

        # Makes a connection to an edge router and runs qdstat
        # asking for all connections of an edge router EA1
        outs = self.run_qdstat(['-d', 'EA1', '-c'],
                               address=self.routers[2].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

        # Makes a connection to an interior router INT.B and runs qdstat
        # asking for all connections of an edge router EA1. The interior
        # router INT.B is connected to edge router EA1 indirectly via
        # interior router INT.A
        outs = self.run_qdstat(['--edge-router', 'EA1', '-c'],
                               address=self.routers[1].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

    def test_71_qdmanage_edge_router_option(self):
        # Makes a connection to an interior router INT.A and runs qdstat
        # asking for all connections of an edge router EA1
        mgmt = QdManager(self, address=self.routers[0].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query('org.apache.qpid.dispatch.connection')
        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

        # Makes a connection to an edge router and runs qdstat
        # asking for all connections of an edge router EA1
        mgmt = QdManager(self, address=self.routers[2].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query('org.apache.qpid.dispatch.connection')

        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

        # Makes a connection to an interior router INT.B and runs qdstat
        # asking for all connections of an edge router EA1. The interior
        # router INT.B is connected to edge router EA1 indirectly via
        # interior router INT.A
        mgmt = QdManager(self, address=self.routers[1].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query('org.apache.qpid.dispatch.connection')

        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

    def test_72_qdstat_query_interior_from_edge(self):

        # Connect to Edge Router EA1 and query the connections on
        # Interior Router INT.A
        outs = self.run_qdstat(['-r', 'INT.A', '-c'],
                               address=self.routers[2].addresses[0])

        # The Interior Router INT.A is connected to two edge routers
        # EA1 and EA2 and is also connected to another interior router INT.B
        # We will connect to edge router EA1 (which has an edge
        # uplink to INT.A) and query for connections on INT.A
        ea1_conn_found = False
        ea2_conn_found = False
        int_b_inter_router_conn_found = False
        parts = outs.split("\n")
        for part in parts:
            if "INT.B" in part and "inter-router" in part and "in" in part:
                int_b_inter_router_conn_found = True
            if "EA1" in part and "edge" in part and "in" in part:
                ea1_conn_found = True
            if "EA2" in part and "edge" in part and "in" in part:
                ea2_conn_found = True

        self.assertTrue(ea1_conn_found and ea2_conn_found and int_b_inter_router_conn_found)

        # The Interior Router INT.B is connected  indirectly to edge router
        # EA1 via INT.A
        # We will connect to edge router EA1 (which has an edge
        # uplink to INT.A) and query for connections on INT.B
        outs = self.run_qdstat(['-r', 'INT.B', '-c'],
                               address=self.routers[2].addresses[0])

        eb1_conn_found = False
        eb2_conn_found = False
        int_a_inter_router_conn_found = False
        parts = outs.split("\n")
        for part in parts:
            if "INT.A" in part and "inter-router" in part and "out" in part:
                int_a_inter_router_conn_found = True
            if "EB1" in part and "edge" in part and "in" in part:
                eb1_conn_found = True
            if "EB2" in part and "edge" in part and "in" in part:
                eb2_conn_found = True

        self.assertTrue(eb1_conn_found and eb2_conn_found and int_a_inter_router_conn_found)

    def test_73_qdmanage_query_interior_from_edge(self):
        # The Interior Router INT.A is connected to two edge routers
        # EA1 and EA2 and is also connected to another interior router INT.B
        # We will connect to edge router EA1 (which has an edge
        # uplink to INT.A) and query for connections on INT.A
        mgmt = QdManager(self, address=self.routers[2].addresses[0],
                         router_id='INT.A')
        outs = mgmt.query('org.apache.qpid.dispatch.connection')
        ea1_conn_found = False
        ea2_conn_found = False
        int_b_inter_router_conn_found = False
        for out in outs:
            if out['container'] == "INT.B" and out['role'] == "inter-router" and out['dir'] == "in":
                int_b_inter_router_conn_found = True
            if out['container'] == "EA1" and out['role'] == "edge" and out['dir'] == "in":
                ea1_conn_found = True
            if out['container'] == "EA2" and out['role'] == "edge" and out['dir'] == "in":
                ea2_conn_found = True

        self.assertTrue(ea1_conn_found and ea2_conn_found and int_b_inter_router_conn_found)

        # The Interior Router INT.B is connected  indirectly to edge router
        # EA1 via INT.A
        # We will connect to edge router EA1 (which has an edge
        # uplink to INT.A) and query for connections on INT.B
        mgmt = QdManager(self, address=self.routers[2].addresses[0],
                         router_id='INT.B')
        outs = mgmt.query('org.apache.qpid.dispatch.connection')
        eb1_conn_found = False
        eb2_conn_found = False
        int_a_inter_router_conn_found = False
        for out in outs:
            if out['container'] == "INT.A" and out['role'] == "inter-router" and out['dir'] == "out":
                int_a_inter_router_conn_found = True
            if out['container'] == "EB1" and out['role'] == "edge" and out['dir'] == "in":
                eb1_conn_found = True
            if out['container'] == "EB2" and out['role'] == "edge" and out['dir'] == "in":
                eb2_conn_found = True

        self.assertTrue(int_a_inter_router_conn_found and eb1_conn_found and eb2_conn_found)


class ConnectivityTest(MessagingHandler):
    def __init__(self, interior_host, edge_host, edge_id):
        super(ConnectivityTest, self).__init__()
        self.interior_host = interior_host
        self.edge_host     = edge_host
        self.edge_id       = edge_id

        self.interior_conn = None
        self.edge_conn     = None
        self.error         = None
        self.proxy         = None
        self.query_sent    = False

    def timeout(self):
        self.error = "Timeout Expired"
        self.interior_conn.close()
        self.edge_conn.close()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.interior_conn  = event.container.connect(self.interior_host)
        self.edge_conn      = event.container.connect(self.edge_host)
        self.reply_receiver = event.container.create_receiver(self.interior_conn, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.proxy        = MgmtMsgProxy(self.reply_receiver.remote_source.address)
            self.agent_sender = event.container.create_sender(self.interior_conn, "$management")

    def on_sendable(self, event):
        if not self.query_sent:
            self.query_sent = True
            self.agent_sender.send(self.proxy.query_connections())

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            if response.status_code != 200:
                self.error = "Unexpected error code from agent: %d - %s" % (response.status_code, response.status_description)
            connections = response.results
            count = 0
            for conn in connections:
                if conn.role == 'edge' and conn.container == self.edge_id:
                    count += 1
            if count != 1:
                self.error = "Incorrect edge count for container-id.  Expected 1, got %d" % count
            self.interior_conn.close()
            self.edge_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class DynamicAddressTest(MessagingHandler):
    def __init__(self, receiver_host, sender_host):
        super(DynamicAddressTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host

        self.receiver_conn = None
        self.sender_conn   = None
        self.receiver      = None
        self.address       = None
        self.count         = 300
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.error         = None

    def timeout(self):
        self.error = "Timeout Expired - n_sent=%d n_rcvd=%d addr=%s" % (self.n_sent, self.n_rcvd, self.address)
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer         = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.receiver:
            self.address = self.receiver.remote_source.address
            self.sender  = event.container.create_sender(self.sender_conn, self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            self.sender.send(Message(body="Message %d" % self.n_sent))
            self.n_sent += 1

    def on_message(self, event):
        self.n_rcvd += 1
        if self.n_rcvd == self.count:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class CustomTimeout:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        message = Message(body="Test Message")
        message.address = self.parent.address
        self.parent.sender.send(message)
        self.parent.cancel_custom()


class MobileAddressAnonymousTest(MessagingHandler):
    """
    Attach a receiver to the interior and an anonymous sender to the edge router
    In a non-anonymous sender scenario, the sender will never be given credit
    to send until a receiver on the same address shows up . Since this
    is an anonymous sender, credit is given instatnly and the sender starts
    sending immediately.

    This test will first send 3 messages with a one second interval to make
    sure receiver is available. Then it will fire off 300 messages
    After dispositions are received for the 300 messages, it will close the
    receiver and send 50 more messages. These 50 messages should be released
    or modified.
    """

    def __init__(self, receiver_host, sender_host, address, large_msg=False):
        super(MobileAddressAnonymousTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host = sender_host
        self.receiver_conn = None
        self.sender_conn = None
        self.receiver = None
        self.sender = None
        self.error = None
        self.n_sent = 0
        self.n_rcvd = 0
        self.address = address
        self.ready = False
        self.custom_timer = None
        self.num_msgs = 100
        self.extra_msgs = 50
        self.n_accepted = 0
        self.n_modified = 0
        self.n_released = 0
        self.error = None
        self.max_attempts = 10
        self.num_attempts = 0
        self.test_started = False
        self.large_msg = large_msg
        if self.large_msg:
            self.body = "0123456789101112131415" * 5000
            self.properties = {'big field': 'X' * 3200}

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, self.address)
        # This is an anonymous sender.
        self.sender        = event.container.create_sender(self.sender_conn)

    def cancel_custom(self):
        self.custom_timer.cancel()

    def timeout(self):
        if self.ready:
            self.error = "Timeout Expired - n_sent=%d n_accepted=%d n_modified=%d n_released=%d" % (
                self.n_sent,  self.n_accepted, self.n_modified, self.n_released)
        else:
            self.error = "Did not get a settlement from the receiver. The test cannot be started until " \
                         "a settlement to a test message is received"
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_sendable(self, event):
        if not self.test_started and event.sender == self.sender:
            message = Message(body="Test Message")
            message.address = self.address
            self.sender.send(message)
            self.num_attempts += 1
            self.test_started = True

    def on_message(self, event):
        if event.receiver == self.receiver:
            if self.ready:
                self.n_rcvd += 1

    def on_link_closed(self, event):
        # The receiver has closed. We will send messages again and
        # make sure they are released.
        if event.receiver == self.receiver:
            for i in range(self.extra_msgs):
                if self.large_msg:
                    message = Message(body=self.body, properties=self.properties)
                else:
                    message = Message(body="Message %d" % self.n_sent)
                message.address = self.address
                self.sender.send(message)
                self.n_sent += 1

    def on_settled(self, event):
        rdisp = str(event.delivery.remote_state)
        if rdisp == "RELEASED" and not self.ready:
            if self.num_attempts < self.max_attempts:
                self.custom_timer = event.reactor.schedule(1, CustomTimeout(self))
                self.num_attempts += 1
        elif rdisp == "ACCEPTED" and not self.ready:
            self.ready = True
            for i in range(self.num_msgs):
                if self.large_msg:
                    message = Message(body=self.body, properties=self.properties)
                else:
                    message = Message(body="Message %d" % self.n_sent)
                message.address = self.address
                self.sender.send(message)
                self.n_sent += 1
        elif rdisp == "ACCEPTED" and self.ready:
            self.n_accepted += 1
            if self.n_accepted == self.num_msgs:
                # Close the receiver after sending 300 messages
                self.receiver.close()
        elif rdisp == "RELEASED" and self.ready:
            self.n_released += 1
        elif rdisp == "MODIFIED" and self.ready:
            self.n_modified += 1

        if self.num_msgs == self.n_accepted and self.extra_msgs == self.n_released + self.n_modified:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileAddressTest(MessagingHandler):
    """
    From a single container create a sender and a receiver connection.
    Send a batch of normal messages that should be accepted by the receiver.
    Close the receiver but not the receiver connection and then
      send an extra batch of messages that should be released or modified.
    Success is when message disposition counts add up correctly.
    """

    def __init__(self, receiver_host, sender_host, address):
        super(MobileAddressTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host
        self.address       = address

        self.receiver_conn = None
        self.sender_conn   = None

        self.receiver      = None
        self.sender        = None

        self.logger        = Logger()

        self.normal_count  = 300
        self.extra_count   = 50
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.n_accepted    = 0
        self.n_rel_or_mod  = 0
        self.error         = None
        self.warning       = False

    def fail_exit(self, title):
        self.error = title
        self.logger.log("MobileAddressTest result:ERROR: %s" % title)
        self.logger.log("address %s     " % self.address)
        self.logger.log("n_sent       = %d. Expected total:%d normal=%d, extra=%d" %
                        (self.n_sent, (self.normal_count + self.extra_count), self.normal_count, self.extra_count))
        self.logger.log("n_rcvd       = %d. Expected %d" % (self.n_rcvd,       self.normal_count))
        self.logger.log("n_accepted   = %d. Expected %d" % (self.n_accepted,   self.normal_count))
        self.logger.log("n_rel_or_mod = %d. Expected %d" % (self.n_rel_or_mod, self.extra_count))
        self.timer.cancel()
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_timer_task(self, event):
        self.fail_exit("Timeout Expired")

    def on_start(self, event):
        self.logger.log("on_start address=%s" % self.address)
        self.timer         = event.reactor.schedule(TIMEOUT, self)
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, self.address)
        self.sender        = event.container.create_sender(self.sender_conn, self.address)

    def on_sendable(self, event):
        self.logger.log("on_sendable")
        if event.sender == self.sender:
            self.logger.log("on_sendable sender")
            while self.n_sent < self.normal_count:
                # send the normal messages
                message = Message(body="Message %d" % self.n_sent)
                self.sender.send(message)
                self.logger.log("on_sendable sender: send message %d: %s" % (self.n_sent, message))
                self.n_sent += 1
        elif event.receiver == self.receiver:
            self.logger.log("on_sendable receiver: WARNING unexpected callback for receiver")
            self.warning = True
        else:
            self.fail_exit("on_sendable not for sender nor for receiver")

    def on_message(self, event):
        self.logger.log("on_message")
        if event.receiver == self.receiver:
            self.n_rcvd += 1
            self.logger.log("on_message receiver: receiver message %d" % (self.n_rcvd))
        else:
            self.logger.log("on_message: WARNING callback not for test receiver.")

    def on_settled(self, event):
        # Expect all settlement events at sender as remote state
        self.logger.log("on_settled")
        rdisp = str(event.delivery.remote_state)
        ldisp = str(event.delivery.local_state)
        if event.sender == self.sender:
            if rdisp is None:
                self.logger.log("on_settled: WARNING: sender remote delivery state is None. Local state = %s." % ldisp)
            elif rdisp == "ACCEPTED":
                self.n_accepted += 1
                self.logger.log("on_settled sender: ACCEPTED %d (of %d)" %
                                (self.n_accepted, self.normal_count))
            elif rdisp in ('RELEASED', 'MODIFIED'):
                self.n_rel_or_mod += 1
                self.logger.log("on_settled sender: %s %d (of %d)" %
                                (rdisp, self.n_rel_or_mod, self.extra_count))
            else:
                self.logger.log("on_settled sender: WARNING unexpected settlement: %s, n_accepted: %d, n_rel_or_mod: %d" %
                                (rdisp, self.n_accepted, self.n_rel_or_mod))
                self.warning = True

            if self.n_sent == self.normal_count and self.n_accepted == self.normal_count:
                # All normal messages are accounted.
                # Close receiver and launch extra messages into the router network.
                self.logger.log("on_settled sender: normal messages all accounted. receiver.close() then send extra messages")
                self.receiver.close()
                for i in range(self.extra_count):
                    message = Message(body="Message %d" % self.n_sent)
                    self.sender.send(message)
                    # Messages must be blasted to get them into the network before news
                    # of the receiver closure is propagated back to EA1.
                    # self.logger.log("on_settled sender: send extra message %d: %s" % (self.n_sent, message))
                    self.n_sent += 1

            if self.n_accepted > self.normal_count:
                self.fail_exit("Too many messages were accepted")
            if self.n_rel_or_mod > self.extra_count:
                self.fail_exit("Too many messages were released or modified")

            if self.n_rel_or_mod == self.extra_count:
                # All extra messages are accounted. Exit with success.
                result = "SUCCESS" if not self.warning else "WARNING"
                self.logger.log("MobileAddressTest result:%s" % result)
                self.timer.cancel()
                self.receiver_conn.close()
                self.sender_conn.close()

        elif event.receiver == self.receiver:
            self.logger.log("on_settled receiver: WARNING unexpected on_settled. remote: %s, local: %s" % (rdisp, ldisp))
            self.warning = True

    def run(self):
        Container(self).run()


class MobileAddressOneSenderTwoReceiversTest(MessagingHandler):
    def __init__(self, receiver1_host, receiver2_host, sender_host, address):
        super(MobileAddressOneSenderTwoReceiversTest, self).__init__()
        self.receiver1_host = receiver1_host
        self.receiver2_host = receiver2_host
        self.sender_host = sender_host
        self.address = address

        # One sender connection and two receiver connections
        self.receiver1_conn = None
        self.receiver2_conn = None
        self.sender_conn   = None

        self.receiver1 = None
        self.receiver2 = None
        self.sender = None

        self.count = 300
        self.rel_count = 50
        self.n_rcvd1 = 0
        self.n_rcvd2 = 0
        self.n_sent = 0
        self.n_settled = 0
        self.n_released = 0
        self.error = None
        self.timer = None
        self.all_msgs_received = False
        self.recvd_msg_bodies = dict()
        self.dup_msg = None

    def timeout(self):
        if self.dup_msg:
            self.error = "Duplicate message %s received " % self.dup_msg
        else:
            self.error = "Timeout Expired - n_sent=%d n_rcvd=%d n_settled=%d n_released=%d addr=%s" % \
                         (self.n_sent, (self.n_rcvd1 + self.n_rcvd2), self.n_settled, self.n_released, self.address)

        self.receiver1_conn.close()
        self.receiver2_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))

        # Create two receivers
        self.receiver1_conn = event.container.connect(self.receiver1_host)
        self.receiver2_conn = event.container.connect(self.receiver2_host)
        self.receiver1 = event.container.create_receiver(self.receiver1_conn,
                                                         self.address)
        self.receiver2 = event.container.create_receiver(self.receiver2_conn,
                                                         self.address)

        # Create one sender
        self.sender_conn = event.container.connect(self.sender_host)
        self.sender = event.container.create_sender(self.sender_conn,
                                                    self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            self.sender.send(Message(body="Message %d" % self.n_sent))
            self.n_sent += 1

    def on_message(self, event):
        if self.recvd_msg_bodies.get(event.message.body):
            self.dup_msg = event.message.body
            self.timeout()
        else:
            self.recvd_msg_bodies[event.message.body] = event.message.body

        if event.receiver == self.receiver1:
            self.n_rcvd1 += 1
        if event.receiver == self.receiver2:
            self.n_rcvd2 += 1

        if self.n_sent == self.n_rcvd1 + self.n_rcvd2:
            self.all_msgs_received = True

    def on_settled(self, event):
        self.n_settled += 1
        if self.n_settled == self.count:
            self.receiver1.close()
            self.receiver2.close()
            for i in range(self.rel_count):
                self.sender.send(Message(body="Message %d" % self.n_sent))
                self.n_sent += 1

    def on_released(self, event):
        self.n_released += 1
        if self.n_released == self.rel_count and self.all_msgs_received:
            self.receiver1_conn.close()
            self.receiver2_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileAddressMulticastTest(MessagingHandler):
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=False,
                 anon_sender=False):
        super(MobileAddressMulticastTest, self).__init__()
        self.receiver1_host = receiver1_host
        self.receiver2_host = receiver2_host
        self.receiver3_host = receiver3_host
        self.sender_host = sender_host
        self.address = address
        self.anon_sender = anon_sender

        # One sender connection and two receiver connections
        self.receiver1_conn = None
        self.receiver2_conn = None
        self.receiver3_conn = None
        self.sender_conn = None

        self.receiver1 = None
        self.receiver2 = None
        self.receiver3 = None
        self.sender = None

        self.count = 100
        self.n_rcvd1 = 0
        self.n_rcvd2 = 0
        self.n_rcvd3 = 0
        self.n_sent = 0
        self.n_settled = 0
        self.n_released = 0
        self.error = None
        self.timer = None
        self.all_msgs_received = False
        self.recvd1_msgs = dict()
        self.recvd2_msgs = dict()
        self.recvd3_msgs = dict()
        self.dup_msg_rcvd = False
        self.dup_msg = None
        self.receiver_name = None
        self.large_msg = large_msg
        self.body = ""
        self.r_attaches = 0
        self.reactor = None
        self.addr_timer = None
        self.n_released = 0
        # The maximum number of times we are going to try to check if the
        # address  has propagated.
        self.max_attempts = 5
        self.num_attempts = 0
        self.container = None
        self.test_msg_received_r1 = False
        self.test_msg_received_r2 = False
        self.test_msg_received_r3 = False
        self.initial_msg_sent = False
        self.n_accepted = 0

        if self.large_msg:
            self.body = "0123456789101112131415" * 5000
            self.properties = {'big field': 'X' * 3200}

    def on_released(self, event):
        self.n_released += 1
        self.send_test_message()

    def timeout(self):
        if self.dup_msg:
            self.error = "%s received  duplicate message %s" % \
                         (self.receiver_name, self.dup_msg)
        else:
            if not self.error:
                self.error = "Timeout Expired - n_sent=%d n_rcvd1=%d, " \
                             "n_rcvd2=%d, n_rcvd3=%d, n_released=%d, addr=%s" % \
                             (self.n_sent, self.n_rcvd1, self.n_rcvd2,
                              self.n_rcvd3, self.n_released, self.address)
        self.receiver1_conn.close()
        self.receiver2_conn.close()
        self.receiver3_conn.close()
        if self.sender_conn:
            self.sender_conn.close()

    def on_start(self, event):
        self.reactor = event.reactor
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver1_conn = event.container.connect(self.receiver1_host)
        self.receiver2_conn = event.container.connect(self.receiver2_host)
        self.receiver3_conn = event.container.connect(self.receiver3_host)

        # Create receivers and sender all in one shot, no need to check for any address table
        # before creating sender
        self.receiver1 = event.container.create_receiver(self.receiver1_conn,
                                                         self.address)
        self.receiver2 = event.container.create_receiver(self.receiver2_conn,
                                                         self.address)
        self.receiver3 = event.container.create_receiver(self.receiver3_conn,
                                                         self.address)
        self.sender_conn = event.container.connect(self.sender_host)
        if self.anon_sender:
            self.sender = event.container.create_sender(self.sender_conn)
        else:
            self.sender = event.container.create_sender(self.sender_conn,
                                                        self.address)

    def send_test_message(self):
        msg = Message(body="Test Message")
        if self.anon_sender:
            msg.address = self.address
        self.sender.send(msg)

    def send(self):
        if self.large_msg:
            msg = Message(body=self.body, properties=self.properties)
        else:
            msg = Message(body="Message %d" % self.n_sent)
        if self.anon_sender:
            msg.address = self.address
        msg.correlation_id = self.n_sent
        self.sender.send(msg)

    def on_accepted(self, event):
        if self.test_msg_received_r1 and self.test_msg_received_r2 and self.test_msg_received_r3:
            # All receivers have received the test message.
            # Now fire off 100 messages to see if the message was multicasted to all
            # receivers.
            self.n_accepted += 1
            while self.n_sent < self.count:
                self.send()
                self.n_sent += 1
        else:
            self.send_test_message()

    def on_sendable(self, event):
        if not self.initial_msg_sent:
            # First send a single test message. This message
            # could be accepted or released based on if
            # some receiver is already online to receive the message
            self.send_test_message()
            self.initial_msg_sent = True

    def on_message(self, event):
        if event.receiver == self.receiver1:
            if event.message.body == "Test Message":
                self.test_msg_received_r1 = True
            else:
                if self.recvd1_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 1"
                    self.timeout()
                self.n_rcvd1 += 1
                self.recvd1_msgs[event.message.correlation_id] = event.message.correlation_id
        if event.receiver == self.receiver2:
            if event.message.body == "Test Message":
                self.test_msg_received_r2 = True
            else:
                if self.recvd2_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 2"
                    self.timeout()
                self.n_rcvd2 += 1
                self.recvd2_msgs[event.message.correlation_id] = event.message.correlation_id
        if event.receiver == self.receiver3:
            if event.message.body == "Test Message":
                self.test_msg_received_r3 = True
            else:
                if self.recvd3_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 3"
                    self.timeout()
                self.n_rcvd3 += 1
                self.recvd3_msgs[event.message.correlation_id] = event.message.correlation_id

        if self.n_rcvd1 == self.count and self.n_rcvd2 == self.count and \
                self.n_rcvd3 == self.count:
            self.timer.cancel()
            self.receiver1_conn.close()
            self.receiver2_conn.close()
            self.receiver3_conn.close()
            self.sender_conn.close()

    def run(self):
        Container(self).run()


class MobileAddrMcastDroppedRxTest(MobileAddressMulticastTest):
    # failure scenario - cause some receiving clients to close while a large
    # message is in transit
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=True, anon_sender=False):
        super(MobileAddrMcastDroppedRxTest, self).__init__(receiver1_host,
                                                           receiver2_host,
                                                           receiver3_host,
                                                           sender_host,
                                                           address,
                                                           large_msg=large_msg,
                                                           anon_sender=anon_sender)

        self.n_released = 0
        self.recv1_closed = False
        self.recv2_closed = False

    def _check_done(self):
        if self.n_accepted + self.n_released == self.count:
            self.receiver3_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def on_message(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_message(event)

        # start closing receivers
        if self.n_rcvd1 == 50:
            if not self.recv1_closed:
                self.receiver1_conn.close()
                self.recv1_closed = True
        if self.n_rcvd2 == 75:
            if not self.recv2_closed:
                self.recv2_closed = True
                self.receiver2_conn.close()

    def on_accepted(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_accepted(event)
        self._check_done()

    def on_released(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_released(event)
        self.n_released += 1
        self._check_done()


class MobileAddrMcastAnonSenderDroppedRxTest(MobileAddrMcastDroppedRxTest):
    # failure scenario - cause some receiving clients to close while a large
    # message is in transit
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=True, anon_sender=True):
        super(MobileAddrMcastAnonSenderDroppedRxTest, self).__init__(receiver1_host,
                                                                     receiver2_host,
                                                                     receiver3_host,
                                                                     sender_host,
                                                                     address,
                                                                     large_msg=large_msg,
                                                                     anon_sender=anon_sender)
        self.n_released = 0
        self.recv1_closed = False
        self.recv2_closed = False


class MobileAddressEventTest(MessagingHandler):
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, interior_host, address, check_remote=False, subscriber_count=0):
        super(MobileAddressEventTest, self).__init__(auto_accept=False)
        self.receiver1_host = receiver1_host
        self.receiver2_host = receiver2_host
        self.receiver3_host = receiver3_host
        self.sender_host = sender_host
        self.address = address
        self.subscriber_count = subscriber_count
        self.receiver1_conn = None
        self.receiver2_conn = None
        self.receiver3_conn = None
        self.sender_conn = None
        self.recvd1_msgs = dict()
        self.recvd2_msgs = dict()
        self.recvd3_msgs = dict()
        self.n_rcvd1 = 0
        self.n_rcvd2 = 0
        self.n_rcvd3 = 0
        self.timer = None
        self.receiver1 = None
        self.receiver2 = None
        self.receiver3 = None
        self.sender = None
        self.interior_host = interior_host
        self.container = None
        self.count = 600
        self.dup_msg = None
        self.receiver_name = None
        self.n_sent = 0
        self.error = None
        self.r_attaches = 0
        self.n_released = 0
        self.n_settled = 0
        self.addr_timer = None
        self.container = None
        self.max_attempts = 5
        self.num_attempts = 0
        self.check_remote = check_remote
        self.reactor = None

    def timeout(self):
        if self.dup_msg:
            self.error = "%s received  duplicate message %s" % \
                         (self.receiver_name, self.dup_msg)
        else:
            if not self.error:
                self.error = "Timeout Expired - n_sent=%d, n_rcvd1=%d, " \
                             "n_rcvd2=%d, n_rcvd3=%d, n_released=%d, addr=%s" % \
                             (self.n_sent, self.n_rcvd1, self.n_rcvd2,
                              self.n_rcvd3, self.n_released, self.address)
        self.receiver1_conn.close()
        self.receiver2_conn.close()
        self.receiver3_conn.close()
        if self.sender_conn:
            self.sender_conn.close()

    def on_link_opened(self, event):
        if self.r_attaches == 3:
            return
        if event.receiver in (self.receiver1, self.receiver2, self.receiver3):
            self.r_attaches += 1

        if self.r_attaches == 3:
            self.addr_timer = event.reactor.schedule(1.0, AddrTimer(self))

    def check_address(self):
        local_node = Node.connect(self.interior_host, timeout=TIMEOUT)
        outs = local_node.query(type='org.apache.qpid.dispatch.router.address')
        remote_count = outs.attribute_names.index("remoteCount")
        subs_count = outs.attribute_names.index("subscriberCount")
        found = False
        self.num_attempts += 1
        remote_count_good = False
        subscriber_count_good = False

        for result in outs.results:
            if self.address in result[0]:
                if self.check_remote:
                    if result[remote_count] > 0:
                        remote_count_good = True
                else:
                    remote_count_good = True
                if self.subscriber_count > 0:
                    if self.subscriber_count == result[subs_count]:
                        subscriber_count_good = True
                else:
                    subscriber_count_good = True

                if remote_count_good and subscriber_count_good:
                    found = True

                if found:
                    self.sender_conn = self.container.connect(self.sender_host)
                    self.sender = self.container.create_sender(self.sender_conn, self.address)
                    local_node.close()
                    break

        if not found:
            if self.num_attempts < self.max_attempts:
                local_node.close()
                self.addr_timer = self.reactor.schedule(1.0, AddrTimer(self))
            else:
                self.error = "Unable to create sender because of " \
                             "absence of address in the address table"
                self.addr_timer.cancel()
                local_node.close()
                self.timeout()

    def on_start(self, event):
        self.reactor = event.reactor
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))

        # Create two receivers
        self.receiver1_conn = event.container.connect(self.receiver1_host)
        self.receiver2_conn = event.container.connect(self.receiver2_host)
        self.receiver3_conn = event.container.connect(self.receiver3_host)

        # Create all 3 receivers first.
        self.receiver1 = event.container.create_receiver(self.receiver1_conn, self.address)
        self.receiver2 = event.container.create_receiver(self.receiver2_conn, self.address)
        self.receiver3 = event.container.create_receiver(self.receiver3_conn, self.address)
        self.container = event.container

    def on_sendable(self, event):
        if self.n_sent < self.count:
            msg = Message(body="Message %d" % self.n_sent)
            msg.correlation_id = self.n_sent
            self.sender.send(msg)
            self.n_sent += 1

    def on_message(self, event):
        if event.receiver == self.receiver1:
            if self.recvd1_msgs.get(event.message.correlation_id):
                self.dup_msg = event.message.correlation_id
                self.receiver_name = "Receiver 1"
                self.timeout()
            self.n_rcvd1 += 1
            self.recvd1_msgs[
                event.message.correlation_id] = event.message.correlation_id

            event.delivery.settle()

        if event.receiver == self.receiver2:
            if self.recvd2_msgs.get(event.message.correlation_id):
                self.dup_msg = event.message.correlation_id
                self.receiver_name = "Receiver 2"
                self.timeout()
            self.n_rcvd2 += 1
            self.recvd2_msgs[
                event.message.correlation_id] = event.message.correlation_id

            event.delivery.settle()

        if event.receiver == self.receiver3:
            if self.recvd3_msgs.get(event.message.correlation_id):
                self.dup_msg = event.message.correlation_id
                self.receiver_name = "Receiver 3"
                self.timeout()
            self.n_rcvd3 += 1
            self.recvd3_msgs[
                event.message.correlation_id] = event.message.correlation_id

            event.delivery.settle()

    def on_settled(self, event):
        if self.n_rcvd1 + self.n_rcvd2 + self.n_rcvd3 == self.count and \
                self.n_rcvd2 != 0 and self.n_rcvd3 != 0:
            self.timer.cancel()
            self.receiver1_conn.close()
            self.receiver2_conn.close()
            self.receiver3_conn.close()
            self.sender_conn.close()

    def on_released(self, event):
        self.n_released += 1

    def run(self):
        Container(self).run()


class EdgeListenerSender(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        super(EdgeListenerSender, cls).setUpClass()

        def router(name, mode, connection, extra=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('address',
                 {'prefix': 'multicast', 'distribution': 'multicast'}),
                connection
            ]

            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        edge_port_A = cls.tester.get_port()
        router('INT.A', 'interior',  ('listener', {'role': 'edge', 'port': edge_port_A}))
        cls.routers[0].wait_ports()

    # Without the fix for DISPATCH-1492, this test will fail because
    # of the router crash.
    def test_edge_listener_sender_crash_DISPATCH_1492(self):
        addr = self.routers[0].addresses[0]
        blocking_connection = BlockingConnection(addr)
        blocking_sender = blocking_connection.create_sender(address="multicast")
        self.assertTrue(blocking_sender is not None)


class StreamingMessageTest(TestCase):
    """
    Test streaming message flows across edge and interior routers
    """

    SIG_TERM = -15  # Process.terminate() sets this exit value
    BODY_MAX = 4294967295  # AMQP 1.0 allows types of length 2^32-1

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(StreamingMessageTest, cls).setUpClass()

        def router(name, mode, extra):
            config = [
                ('router', {'mode': mode, 'id': name, "helloMaxAgeSeconds": '10'}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port(),
                              'maxFrameSize': 65535}),

                ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                ('address', {'prefix': 'balanced',  'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'})
            ]

            if extra:
                config.extend(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=False))
            return cls.routers[-1]

        # configuration:
        # two edge routers connected via 2 interior routers.
        # fake broker (route-container) on EB1
        #
        #  +-------+    +---------+    +---------+    +-------+
        #  |  EA1  |<==>|  INT.A  |<==>|  INT.B  |<==>|  EB1  |<-- Fake Broker
        #  +-------+    +---------+    +---------+    +-------+
        #

        cls.routers = []

        interrouter_port = cls.tester.get_port()
        cls.INTA_edge_port   = cls.tester.get_port()
        cls.INTB_edge_port   = cls.tester.get_port()

        router('INT.A', 'interior',
               [('listener', {'role': 'inter-router', 'port': interrouter_port}),
                ('listener', {'role': 'edge', 'port': cls.INTA_edge_port})])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        router('INT.B', 'interior',
               [('connector', {'name': 'connectorToA', 'role': 'inter-router',
                               'port': interrouter_port}),
                ('listener', {'role': 'edge', 'port': cls.INTB_edge_port})])
        cls.INT_B = cls.routers[1]
        cls.INT_B.listener = cls.INT_B.addresses[0]

        router('EA1', 'edge',
               [('listener', {'name': 'rc', 'role': 'route-container',
                              'port': cls.tester.get_port()}),
                ('connector', {'name': 'uplink', 'role': 'edge',
                               'port': cls.INTA_edge_port})
                ])
        cls.EA1 = cls.routers[2]
        cls.EA1.listener = cls.EA1.addresses[0]

        router('EB1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge',
                               'port': cls.INTB_edge_port}),
                # to connect to the fake broker
                ('connector', {'name': 'broker',
                               'role': 'route-container',
                               'host': '127.0.0.1',
                               'port': cls.tester.get_port(),
                               'saslMechanisms': 'ANONYMOUS'})
                ])
        cls.EB1 = cls.routers[3]
        cls.EB1.listener = cls.EB1.addresses[0]
        cls.EB1.route_container = cls.EB1.connector_addresses[1]

        cls.INT_A.wait_router_connected('INT.B')
        cls.INT_B.wait_router_connected('INT.A')
        cls.EA1.wait_connectors()

        cls._container_index = 0

        cls.skip = {'test_01' : 0,
                    'test_02' : 0,
                    'test_03' : 0,
                    'test_50' : 0,
                    'test_51' : 0,
                    'test_52' : 0
                    }

    def _get_address(self, router, address):
        """Lookup address in route table"""
        a_type = 'org.apache.qpid.dispatch.router.address'
        addrs = router.management.query(a_type).get_dicts()
        return [a for a in addrs if address in a['name']]

    def _wait_address_gone(self, router, address):
        """Block until address is removed from the route table"""
        while self._get_address(router, address):
            sleep(0.1)

    def _start_broker_EB1(self):
        # start a new broker on EB1
        fake_broker = FakeBroker(self.EB1.route_container)
        return fake_broker

    def spawn_receiver(self, router, count, address, expect=None):
        if expect is None:
            expect = Process.EXIT_OK
        cmd = ["test-receiver",
               "-i", "TestReceiver-%d" % self._container_index,
               "-a", router.listener,
               "-c", str(count),
               "-s", address,
               "-d"]
        self._container_index += 1
        env = dict(os.environ, PN_TRACE_FRM="1")
        return self.popen(cmd, expect=expect, env=env)

    def spawn_sender(self, router, count, address, expect=None, size=None):
        if expect is None:
            expect = Process.EXIT_OK
        if size is None:
            size = "-sm"
        cmd = ["test-sender",
               "-i", "TestSender-%d" % self._container_index,
               "-a", router.listener,
               "-c", str(count),
               "-t", address,
               size,
               "-d"]
        self._container_index += 1
        env = dict(os.environ, PN_TRACE_FRM="1")
        return self.popen(cmd, expect=expect, env=env)

    def spawn_clogger(self, router, count, address,
                      size, pause_ms, expect=None):
        if expect is None:
            expect = Process.EXIT_OK
        cmd = ["clogger",
               "-a", router.listener,
               "-c", str(count),
               "-t", address,
               "-s", str(size),
               "-D",
               "-P", str(pause_ms)]
        env = dict(os.environ, PN_TRACE_FRM="1")
        return self.popen(cmd, expect=expect, env=env)

    def _streaming_test(self, address):

        # send a streaming message to address across the routers
        rx = self.spawn_receiver(self.EB1,
                                 count=1,
                                 address=address)
        self.INT_A.wait_address(address)

        tx = self.spawn_sender(self.EA1,
                               count=1,
                               address=address,
                               expect=Process.EXIT_OK,
                               size="-sx")
        out_text, out_error = tx.communicate(timeout=TIMEOUT)
        if tx.returncode:
            raise Exception("Sender failed: %s %s" % (out_text, out_error))

        out_text, out_error = rx.communicate(timeout=TIMEOUT)
        if rx.returncode:
            raise Exception("receiver failed: %s %s" % (out_text, out_error))

        self._wait_address_gone(self.INT_B, address)
        self._wait_address_gone(self.INT_A, address)
        self._wait_address_gone(self.EA1, address)
        self._wait_address_gone(self.EB1, address)

    def test_02_streaming_closest(self):
        """
        Verify that a streaming message with closest treatment is forwarded
        correctly.
        """
        self._streaming_test("closest/test-address")

    def test_03_streaming_multicast(self):
        """
        Verify a streaming multicast message is forwarded correctly
        """

        routers = [self.EB1, self.INT_B, self.INT_A]
        streaming_rx = [self.spawn_receiver(router,
                                            count=1,
                                            address="multicast/test-address")
                        for router in routers]
        self.EB1.wait_address("multicast/test-address", subscribers=1)
        self.INT_B.wait_address("multicast/test-address", subscribers=2, remotes=1)
        self.INT_A.wait_address("multicast/test-address", subscribers=1, remotes=1)

        # This sender will end up multicasting the message to ALL receivers.
        tx = self.spawn_sender(self.EA1,
                               count=1,
                               address="multicast/test-address",
                               expect=Process.EXIT_OK,
                               size="-sx")

        out_text, out_error = tx.communicate(timeout=TIMEOUT)
        if tx.returncode:
            raise Exception("sender failed: %s %s" % (out_text, out_error))

        for rx in streaming_rx:
            out_text, out_error = rx.communicate(timeout=TIMEOUT)
            if rx.returncode:
                raise Exception("receiver failed: %s %s" % (out_text, out_error))

        self._wait_address_gone(self.EA1, "multicast/test_address")
        self._wait_address_gone(self.EB1, "multicast/test_address")
        self._wait_address_gone(self.INT_A, "multicast/test_address")
        self._wait_address_gone(self.INT_B, "multicast/test_address")

    def test_04_streaming_balanced(self):
        """
        Verify streaming balanced messages are forwarded correctly.
        """
        balanced_rx = [self.spawn_receiver(self.EB1,
                                           count=1,
                                           address="balanced/test-address")
                       for _ in range(2)]
        self.EB1.wait_address("balanced/test-address", subscribers=2)

        tx = self.spawn_sender(self.EA1,
                               count=2,
                               address="balanced/test-address")
        out_text, out_error = tx.communicate(timeout=TIMEOUT)
        if tx.returncode:
            raise Exception("sender failed: %s %s" % (out_text, out_error))

        for rx in balanced_rx:
            out_text, out_error = rx.communicate(timeout=TIMEOUT)
            if rx.returncode:
                raise Exception("receiver failed: %s %s" % (out_text, out_error))

        self._wait_address_gone(self.EA1, "balanced/test-address")
        self._wait_address_gone(self.EB1,  "balanced/test-address")
        self._wait_address_gone(self.INT_A,  "balanced/test-address")
        self._wait_address_gone(self.INT_B,  "balanced/test-address")

    def test_11_streaming_closest_parallel(self):
        """
        Ensure that a streaming message of closest treatment does not block
        other non-streaming messages.
        """

        # this receiver should get the streaming message
        rx1 = self.spawn_receiver(self.EB1,
                                  count=1,
                                  address="closest/test-address",
                                  expect=self.SIG_TERM)

        self.INT_A.wait_address("closest/test-address")

        clogger = self.spawn_clogger(self.EA1,
                                     count=1,
                                     address="closest/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=self.SIG_TERM)
        sleep(0.5)

        # this receiver has less cost than rx1 since it is 1 less hop from the
        # sender
        rx2 = self.spawn_receiver(self.INT_A,
                                  count=1,
                                  address="closest/test-address")

        # wait for rx2 to set up links to INT_A:
        self.INT_A.wait_address("closest/test-address", subscribers=1, remotes=1)

        # start a sender in parallel. Expect the message to arrive at rx1
        tx = self.spawn_sender(self.EA1, count=1, address="closest/test-address")
        out_text, out_error = tx.communicate(timeout=TIMEOUT)
        if tx.returncode:
            raise Exception("Sender failed: %s %s" % (out_text, out_error))

        out_text, out_error = rx2.communicate(timeout=TIMEOUT)
        if rx2.returncode:
            raise Exception("receiver failed: %s %s" % (out_text, out_error))

        rx1.terminate()
        rx1.wait()

        clogger.terminate()
        clogger.wait()

        self._wait_address_gone(self.EA1, "closest/test-address")
        self._wait_address_gone(self.EB1,  "closest/test-address")
        self._wait_address_gone(self.INT_A,  "closest/test-address")
        self._wait_address_gone(self.INT_B,  "closest/test-address")

    def test_12_streaming_multicast_parallel(self):
        """
        Verify a streaming multicast message does not block other non-streaming
        multicast messages

        Start a group of receivers to consume the streaming message.  Then
        start a separate group to consume the non-streaming message.  Ensure
        that the second group properly receives the non-streaming message.
        """

        routers = [self.EB1, self.INT_A, self.INT_B]
        streaming_rx = [self.spawn_receiver(router,
                                            count=1,
                                            address="multicast/test-address",
                                            expect=self.SIG_TERM)
                        for router in routers]

        self.EB1.wait_address("multicast/test-address", subscribers=1)
        self.INT_B.wait_address("multicast/test-address", subscribers=2, remotes=1)
        self.INT_A.wait_address("multicast/test-address", subscribers=1, remotes=1)

        # this will block all of the above receivers with a streaming message

        clogger = self.spawn_clogger(self.EA1,
                                     count=1,
                                     address="multicast/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=self.SIG_TERM)
        sleep(0.5)

        # this second set of receivers should be able to receive multicast
        # messages sent _after_ the clogger's streaming message

        blocking_rx = [self.spawn_receiver(router,
                                           count=1,
                                           address="multicast/test-address")
                       for router in routers]

        self.EB1.wait_address("multicast/test-address", subscribers=2)
        self.INT_B.wait_address("multicast/test-address", subscribers=3, remotes=1)
        self.INT_A.wait_address("multicast/test-address", subscribers=2, remotes=1)

        # This sender will end up multicasting the message to ALL receivers.
        # Expect it to block since the first set of receivers will never get
        # around to acking the message
        tx = self.spawn_sender(self.EA1,
                               count=1,
                               address="multicast/test-address",
                               expect=self.SIG_TERM)

        # however the second set of receivers _should_ end up getting the
        # message, acking it and exit (count=1)
        for rx in blocking_rx:
            out_text, out_error = rx.communicate(timeout=TIMEOUT)
            if rx.returncode:
                raise Exception("receiver failed: %s %s" % (out_text, out_error))

        tx.terminate()
        tx.wait()

        for rx in streaming_rx:
            rx.terminate()
            rx.wait()

        clogger.terminate()
        clogger.wait()

        self._wait_address_gone(self.EA1, "multicast/test-address")
        self._wait_address_gone(self.EB1,  "multicast/test-address")
        self._wait_address_gone(self.INT_A,  "multicast/test-address")
        self._wait_address_gone(self.INT_B,  "multicast/test-address")

    def test_13_streaming_balanced_parallel(self):
        """
        Verify streaming does not block other balanced traffic.
        """

        # create 2 consumers on the balanced address. Since our Process class
        # requires the exit code to be known when the process is spawned and we
        # cannot predict which receiver will get the streaming message use
        # count=2 to force the receivers to run until we force termination
        balanced_rx = [self.spawn_receiver(self.EB1,
                                           count=2,
                                           address="balanced/test-address",
                                           expect=self.SIG_TERM)
                       for _ in range(2)]
        self.EB1.wait_address("balanced/test-address", subscribers=2)

        # this will block one of the above receivers with a streaming message

        clogger = self.spawn_clogger(self.EA1,
                                     count=1,
                                     address="balanced/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=self.SIG_TERM)
        sleep(0.5)

        # This sender should get its message through to the other receiver
        tx = self.spawn_sender(self.EA1,
                               count=1,
                               address="balanced/test-address")
        out_text, out_error = tx.communicate(timeout=TIMEOUT)
        if tx.returncode:
            raise Exception("sender failed: %s %s" % (out_text, out_error))

        for rx in balanced_rx:
            rx.terminate()
            rx.wait()

        clogger.terminate()
        clogger.wait()

        self._wait_address_gone(self.EA1, "balanced/test-address")
        self._wait_address_gone(self.EB1,  "balanced/test-address")
        self._wait_address_gone(self.INT_A,  "balanced/test-address")
        self._wait_address_gone(self.INT_B,  "balanced/test-address")


if __name__ == '__main__':
    unittest.main(main_module())

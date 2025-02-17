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

from system_test import TestCase, Qdrouterd, TIMEOUT
from system_test import Process
from system_test import SkManager, unittest, main_module
from system_test import CONNECTION_TYPE, ROUTER_ADDRESS_TYPE

from test_broker import FakeBroker
from system_tests_edge_router import EdgeBase, MobileAddressEventTest

from message_tests import MobileAddressAnonymousTest
from message_tests import MobileAddressMulticastTest
from message_tests import MobileAddrMcastDroppedRxTest, MobileAddrMcastAnonSenderDroppedRxTest


class EdgeRouterTest1(EdgeBase):
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

    def run_skstat(self, args, regexp=None, address=None, expect=Process.EXIT_OK):
        if args:
            popen_arg = ['skstat', '--bus', str(address or self.router.addresses[0]),
                         '--timeout', str(TIMEOUT)] + args
        else:
            popen_arg = ['skstat', '--bus',
                         str(address or self.router.addresses[0]),
                         '--timeout', str(TIMEOUT)]

        p = self.popen(popen_arg,
                       name='skstat-' + self.id(), stdout=PIPE, expect=expect,
                       universal_newlines=True)

        out = p.communicate()[0]
        assert p.returncode == 0, \
            "skstat exit status %s, output:\n%s" % (p.returncode, out)
        if regexp:
            assert re.search(regexp, out,
                             re.I), "Can't find '%s' in '%s'" % (
                regexp, out)
        return out

    def test_68_edge_skstat_all_routers(self):
        # Connects to an edge router and runs "skstat --all-routers"
        # "skstat --all-routers" is same as "skstat --all-routers --g"
        # Connecting to an edge router and running "skstat --all-routers""will only yield the
        # summary statistics of the edge router. It will not show statistics of the interior routers.
        outs = self.run_skstat(['--all-routers'],
                               address=self.routers[2].addresses[0])
        self.assertIn("Router Id                        EA1", outs)

        outs = self.run_skstat(['--all-routers', '--all-entities'],
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

        outs = self.run_skstat(['-c', '--all-routers'],
                               address=self.routers[2].addresses[0])

        # Verify that the the edhe uplink connection is showing
        self.assertIn("INT.A", outs)
        self.assertNotIn("inter-router", outs)

        outs = self.run_skstat(['--all-entities'],
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

    def test_69_interior_skstat_all_routers(self):
        # Connects to an interior router and runs "skstat --all-routers"
        # "skstat --all-routers" is same as "skstat --all-routers --all-entities"
        # Connecting to an interior router and running "skstat --all-routers""will yield the
        # summary statistics of all the interior routers.
        outs = self.run_skstat(['--all-routers'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Statistics"), 2)

        outs = self.run_skstat(['--all-routers', '-nv'],
                               address=self.routers[0].addresses[0])
        # 5 occurrences including section headers
        self.assertEqual(outs.count("INT.A"), 5)
        self.assertEqual(outs.count("INT.B"), 5)

        outs = self.run_skstat(['--all-routers', '--all-entities'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Links"), 2)
        self.assertEqual(outs.count("Router Addresses"), 2)
        self.assertEqual(outs.count("Total Connections"), 2)
        self.assertEqual(outs.count("Router Statistics"), 2)
        self.assertEqual(outs.count("Memory Pools"), 2)

        outs = self.run_skstat(['--all-routers', '-nv'],
                               address=self.routers[0].addresses[0])
        # 5 occurrences including section headers
        self.assertEqual(outs.count("INT.A"), 5)
        self.assertEqual(outs.count("INT.B"), 5)

        outs = self.run_skstat(['-c', '--all-routers'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("INT.A"), 4)
        self.assertEqual(outs.count("INT.B"), 4)

        outs = self.run_skstat(['-l', '--all-routers'],
                               address=self.routers[0].addresses[0])

        # Two edge-downlinks from each interior to the two edges, 4 in total.
        self.assertEqual(outs.count("edge-downlink"), 4)

        # Gets all entity information of the interior router
        outs = self.run_skstat(['--all-entities'],
                               address=self.routers[0].addresses[0])
        self.assertEqual(outs.count("Router Links"), 1)
        self.assertEqual(outs.count("Router Addresses"), 1)
        self.assertEqual(outs.count("Router Statistics"), 1)

        has_error = False
        try:
            # You cannot combine --all-entities  with -c
            self.run_skstat(['-c', '--all-entities'],
                            address=self.routers[0].addresses[0], expect=Process.EXIT_FAIL)
        except Exception as e:
            if "error: argument --all-entities: not allowed with argument -c/--connections" in str(e):
                has_error = True

        self.assertTrue(has_error)

        has_error = False
        try:
            outs = self.run_skstat(['-r', 'INT.A', '--all-routers'],
                                   address=self.routers[0].addresses[0], expect=Process.EXIT_FAIL)
        except Exception as e:
            if "error: argument --all-routers: not allowed with argument -r/--router" in str(e):
                has_error = True

        #self.assertTrue(has_error)

    def test_70_skstat_edge_router_option(self):
        # Tests the --edge-router (-d) option of skstat
        # The goal of this test is to connect to any router in the
        # network (interior or edge) and ask for details about a specific edge router
        # You could not do that before DISPATCH-1580

        # Makes a connection to an interior router INT.A and runs skstat
        # asking for all connections of an edge router EA1
        outs = self.run_skstat(['-d', 'EA1', '-c'],
                               address=self.routers[0].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

        # Makes a connection to an edge router and runs skstat
        # asking for all connections of an edge router EA1
        outs = self.run_skstat(['-d', 'EA1', '-c'],
                               address=self.routers[2].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

        # Makes a connection to an interior router INT.B and runs skstat
        # asking for all connections of an edge router EA1. The interior
        # router INT.B is connected to edge router EA1 indirectly via
        # interior router INT.A
        outs = self.run_skstat(['--edge-router', 'EA1', '-c'],
                               address=self.routers[1].addresses[0])
        parts = outs.split("\n")
        conn_found = False
        for part in parts:
            if "INT.A" in part and "edge" in part and "out" in part:
                conn_found = True
                break

        self.assertTrue(conn_found)

    def test_71_skmanage_edge_router_option(self):
        # Makes a connection to an interior router INT.A and runs skstat
        # asking for all connections of an edge router EA1
        mgmt = SkManager(address=self.routers[0].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query(CONNECTION_TYPE)
        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

        # Makes a connection to an edge router and runs skstat
        # asking for all connections of an edge router EA1
        mgmt = SkManager(address=self.routers[2].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query(CONNECTION_TYPE)

        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

        # Makes a connection to an interior router INT.B and runs skstat
        # asking for all connections of an edge router EA1. The interior
        # router INT.B is connected to edge router EA1 indirectly via
        # interior router INT.A
        mgmt = SkManager(address=self.routers[1].addresses[0],
                         edge_router_id='EA1')
        conn_found = False
        outs = mgmt.query(CONNECTION_TYPE)

        for out in outs:
            if out['container'] == 'INT.A' and out['dir'] == "out" and out['role'] == "edge":
                conn_found = True
                break
        self.assertTrue(conn_found)

    def test_72_skstat_query_interior_from_edge(self):

        # Connect to Edge Router EA1 and query the connections on
        # Interior Router INT.A
        outs = self.run_skstat(['-r', 'INT.A', '-c'],
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
        outs = self.run_skstat(['-r', 'INT.B', '-c'],
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

    def test_73_skmanage_query_interior_from_edge(self):
        # The Interior Router INT.A is connected to two edge routers
        # EA1 and EA2 and is also connected to another interior router INT.B
        # We will connect to edge router EA1 (which has an edge
        # uplink to INT.A) and query for connections on INT.A
        mgmt = SkManager(address=self.routers[2].addresses[0],
                         router_id='INT.A')
        outs = mgmt.query(CONNECTION_TYPE)
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
        mgmt = SkManager(address=self.routers[2].addresses[0],
                         router_id='INT.B')
        outs = mgmt.query(CONNECTION_TYPE)
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

    def run_skstat(self, args, regexp=None, address=None, expect=Process.EXIT_OK):
        p = self.popen(
            ['skstat', '--bus', str(address or self.router.addresses[0]),
             '--timeout', str(TIMEOUT)] + args,
            name='skstat-' + self.id(), stdout=PIPE, expect=expect,
            universal_newlines=True)

        out = p.communicate()[0]
        assert p.returncode == 0, \
            "skstat exit status %s, output:\n%s" % (p.returncode, out)
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

    def run_int_b_edge_skstat(self):
        outs = self.run_skstat(['--edge'],
                               address=self.routers[2].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "INT.B" in line and "yes" in line:
                self.success = True

    def run_int_a_edge_skstat(self):
        outs = self.run_skstat(['--edge'],
                               address=self.routers[2].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "INT.A" in line and "yes" in line:
                self.success = True

    def schedule_int_a_skstat_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.run_int_a_edge_skstat).start()
                self.attempts += 1

    def schedule_int_b_skstat_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.run_int_b_edge_skstat).start()
                self.attempts += 1

    def test_01_active_flag(self):
        """
        In this test, we have one edge router connected to two interior
        routers. One connection is to INT.A and another connection is to
        INT.B . But only one of these connections is active. We use skstat
        to make sure that only one of these connections is active.
        Then we kill the router with the active connection and make sure
        that the other connection is now the active one
        """
        if self.skip['test_01'] :
            self.skipTest("Test skipped during development.")

        success = False
        outs = self.run_skstat(['--edge'],
                               address=self.routers[0].addresses[0])
        lines = outs.split("\n")
        for line in lines:
            if "EA1" in line and "yes" in line:
                success = True
        if not success:
            self.fail("Active edge connection not found not found for "
                      "interior router")

        outs = self.run_skstat(['--edge'],
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
            self.schedule_int_b_skstat_test()

            while not self.can_terminate():
                pass

            self.assertTrue(self.success)

        elif conn_map_edge["INT.B"]:
            #
            # INT.B has the active connection. Let's kill INT.B and see
            # if the other connection becomes active
            #
            EdgeRouterTest.routers[1].teardown()
            self.schedule_int_a_skstat_test()

            while not self.can_terminate():
                pass

            self.assertTrue(self.success)


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
        addrs = router.management.query(ROUTER_ADDRESS_TYPE).get_dicts()
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
        return self.popen(cmd, expect=expect, env=env, abort=True)

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
        return self.popen(cmd, expect=expect, env=env, abort=True)

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
        return self.popen(cmd, expect=expect, env=env, abort=True)

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
                                  count=0,
                                  address="closest/test-address",
                                  expect=Process.RUNNING)

        self.INT_A.wait_address("closest/test-address")

        clogger = self.spawn_clogger(self.EA1,
                                     count=0,
                                     address="closest/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=Process.RUNNING)
        # Wait for clogger to start sending data
        # TODO(kgiusti): best to use mgmt to determine this! sleep() is a hack
        sleep(1.0)

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

        clogger.teardown()
        rx1.teardown()

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
                                            expect=Process.RUNNING)
                        for router in routers]

        self.EB1.wait_address("multicast/test-address", subscribers=1)
        self.INT_B.wait_address("multicast/test-address", subscribers=2, remotes=1)
        self.INT_A.wait_address("multicast/test-address", subscribers=1, remotes=1)

        # this will block all of the above receivers with a streaming message

        clogger = self.spawn_clogger(self.EA1,
                                     count=0,
                                     address="multicast/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=Process.RUNNING)
        # Wait for clogger to start sending data
        # TODO(kgiusti): best to use mgmt to determine this! sleep() is a hack
        sleep(1.0)

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
                               expect=Process.RUNNING)

        # however the second set of receivers _should_ end up getting the
        # message, acking it and exit (rx count=1)
        for rx in blocking_rx:
            rx.wait(timeout=TIMEOUT)
            rx.teardown()  # will raise error if rx fails to receive the message

        # any of the following teardowns will error if the client is not
        # blocked:

        tx.teardown()
        for rx in streaming_rx:
            rx.teardown()
        clogger.teardown()

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
                                           expect=Process.RUNNING)
                       for _ in range(2)]
        self.EB1.wait_address("balanced/test-address", subscribers=2)

        # this will block one of the above receivers with a streaming message
        clogger = self.spawn_clogger(self.EA1,
                                     count=0,
                                     address="balanced/test-address",
                                     size=self.BODY_MAX,
                                     pause_ms=100,
                                     expect=Process.RUNNING)
        # Wait for clogger to start sending data
        # TODO(kgiusti): best to use mgmt to determine this! sleep() is a hack
        sleep(1.0)

        # This sender should get its message through to the other receiver.
        # when it does it will exit successfully
        tx = self.spawn_sender(self.EA1,
                               count=1,
                               address="balanced/test-address")
        # these will raise an error if tx does not succeed
        tx.wait(timeout=TIMEOUT)
        tx.teardown()

        for rx in balanced_rx:
            rx.teardown()
        clogger.teardown()


if __name__ == '__main__':
    unittest.main(main_module())

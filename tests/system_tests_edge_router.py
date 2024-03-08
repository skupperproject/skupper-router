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
from proton.utils import BlockingConnection

from skupper_router.management.client import Node
from skupper_router.management.error import ForbiddenStatus

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, MgmtMsgProxy, TestTimeout
from system_test import unittest
from system_test import CONNECTION_TYPE, ROUTER_ADDRESS_TYPE

from message_tests import DynamicAddressTest, MobileAddressTest
from message_tests import MobileAddressOneSenderTwoReceiversTest, MobileAddressMulticastTest


class AddrTimer:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.check_address()


class EdgeBase(TestCase):
    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(EdgeBase, cls).setUpClass()

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


class EdgeRouterTest(EdgeBase):
    def test_01_1_connectivity_INTA_EA1(self):
        if self.skip['test_01'] :
            self.skipTest("Test skipped during development.")

        test = ConnectivityTest(self.routers[0].addresses[0],
                                self.routers[2].addresses[0],
                                'EA1')
        test.run()
        self.assertIsNone(test.error)

    def test_01_2_ensure_no_admin_delete(self):
        """
        This test tries to delete edge uplink connections but that operation
        is forbidden. This test runs immediately after the INTA/EA1
        connectivity test to ensure both routers are operational. Futher tests
        of the INTA/EA1 path follow to ensure the delete attempt did not change
        the routers state.
        """
        if self.skip['test_01'] :
            self.skipTest("Test skipped during development.")

        mgmt_a = self.routers[0].management
        mgmt_ea1 = self.routers[2].management

        for mgmt in (mgmt_a, mgmt_ea1):
            conns = [d for d in
                     mgmt.query(type=CONNECTION_TYPE).get_dicts() if
                     d['role'] == 'edge']
            self.assertNotEqual(0, len(conns), f"Expected at least one connection: {conns}")
            for conn in conns:
                with self.assertRaises(ForbiddenStatus):
                    mgmt.update(attributes={'adminStatus': 'deleted'},
                                type=CONNECTION_TYPE,
                                identity=conn['identity'])

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
        outs = local_node.query(type=ROUTER_ADDRESS_TYPE)
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


if __name__ == '__main__':
    unittest.main(main_module())

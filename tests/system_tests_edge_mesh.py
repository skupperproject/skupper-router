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

from system_test import TestCase, Qdrouterd, main_module, unittest
from message_tests import DynamicAddressTest, MobileAddressAnonymousTest, MobileAddressTest
from message_tests import MobileAddressOneSenderTwoReceiversTest, MobileAddressMulticastTest


class StandaloneMeshTest(TestCase):
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(StandaloneMeshTest, cls).setUpClass()

        def router(name, connections=None, mode='interior', my_edge_port=None, edge_ports=None):

            config = [
                ('router', {'mode': mode, 'id': name}),
                ('address', {'prefix': 'mc', 'distribution': 'multicast'}),
                ('listener', {'port': cls.tester.get_port()})
            ]

            for connection in connections or []:
                config.append(connection)

            if my_edge_port:
                config.append(('listener', {'role': 'inter-edge', 'port': my_edge_port}))

            for ep in edge_ports or []:
                config.append(('connector', {'role': 'inter-edge', 'port': ep}))

            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=False, cl_args=["-T"]))

        cls.routers = []

        edge_a_port = cls.tester.get_port()
        edge_b_port = cls.tester.get_port()
        edge_c_port = cls.tester.get_port()

        router('EA', mode='edge', my_edge_port=edge_a_port, edge_ports=[edge_b_port, edge_c_port])
        router('EB', mode='edge', my_edge_port=edge_b_port, edge_ports=[edge_a_port, edge_c_port])
        router('EC', mode='edge', my_edge_port=edge_c_port, edge_ports=[edge_a_port, edge_b_port])

        cls.routers[0].is_edge_routers_connected(num_edges=2, role='inter-edge')
        cls.routers[1].is_edge_routers_connected(num_edges=2, role='inter-edge')
        cls.routers[2].is_edge_routers_connected(num_edges=2, role='inter-edge')

    def test_01_dynamic_address_same_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[0].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_02_dynamic_address_different_edges_0_1(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[1].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_03_dynamic_address_different_edges_1_2(self):
        test = DynamicAddressTest(self.routers[1].addresses[0], self.routers[2].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_04_dynamic_address_different_edges_2_0(self):
        test = DynamicAddressTest(self.routers[2].addresses[0], self.routers[0].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_05_dynamic_address_anon_same_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[0].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_06_dynamic_address_anon_different_edges_0_1(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[1].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_07_dynamic_address_anon_different_edges_1_2(self):
        test = DynamicAddressTest(self.routers[1].addresses[0], self.routers[2].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_08_dynamic_address_anon_different_edges_2_0(self):
        test = DynamicAddressTest(self.routers[2].addresses[0], self.routers[0].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_09_mobile_address_anon_same_edge(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[0].addresses[0], 'test_09')
        test.run()
        self.assertIsNone(test.error)

    def test_10_mobile_address_anon_different_edge(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[1].addresses[0], 'test_10')
        test.run()
        self.assertIsNone(test.error)

    def test_11_mobile_address_same_edge(self):
        test = MobileAddressTest(self.routers[1].addresses[0], self.routers[1].addresses[0], 'test_11')
        test.run()
        self.assertIsNone(test.error)

    def test_12_mobile_address_different_edge(self):
        test = MobileAddressTest(self.routers[1].addresses[0], self.routers[2].addresses[0], 'test_12')
        test.run()
        self.assertIsNone(test.error)

    def test_13_mobile_address_two_receivers_1(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      'test_13')
        test.run()
        self.assertIsNone(test.error)

    def test_14_mobile_address_two_receivers_2(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      'test_14')
        test.run()
        self.assertIsNone(test.error)

    def test_15_mobile_address_two_receivers_3(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[2].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      'test_15')
        test.run()
        self.assertIsNone(test.error)

    def test_16_mobile_address_multicast_1(self):
        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          'mc.test_16')
        test.run()
        self.assertIsNone(test.error)

    def test_17_mobile_address_multicast_2(self):
        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[0].addresses[0],
                                          'mc.test_17')
        test.run()
        self.assertIsNone(test.error)

    def test_18_mobile_address_multicast_3(self):
        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[0].addresses[0],
                                          'mc.test_18')
        test.run()
        self.assertIsNone(test.error)

    def test_19_mobile_address_multicast_4(self):
        test = MobileAddressMulticastTest(self.routers[2].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[0].addresses[0],
                                          self.routers[0].addresses[0],
                                          'mc.test_19')
        test.run()
        self.assertIsNone(test.error)


class ConnectedMeshTest(TestCase):
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(ConnectedMeshTest, cls).setUpClass()

        def router(name, connections=None, mode='interior', my_edge_port=None, edge_ports=None):

            config = [
                ('router', {'mode': mode, 'id': name}),
                ('address', {'prefix': 'mc', 'distribution': 'multicast'}),
                ('listener', {'port': cls.tester.get_port()})
            ]

            for connection in connections or []:
                config.append(connection)

            if my_edge_port:
                config.append(('listener', {'role': 'inter-edge', 'port': my_edge_port}))

            for ep in edge_ports or []:
                config.append(('connector', {'role': 'inter-edge', 'port': ep}))

            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=False, cl_args=["-T"]))

        cls.routers = []

        edge_a_port       = cls.tester.get_port()
        edge_b_port       = cls.tester.get_port()
        edge_c_port       = cls.tester.get_port()
        inter_router_port = cls.tester.get_port()
        edge_port_ix      = cls.tester.get_port()
        edge_port_iy      = cls.tester.get_port()

        router('EA', mode='edge', my_edge_port=edge_a_port, edge_ports=[edge_b_port, edge_c_port],
               connections=[('connector', {'role': 'edge', 'port': edge_port_ix})])
        router('EB', mode='edge', my_edge_port=edge_b_port, edge_ports=[edge_a_port, edge_c_port],
               connections=[('connector', {'role': 'edge', 'port': edge_port_ix})])
        router('EC', mode='edge', my_edge_port=edge_c_port, edge_ports=[edge_a_port, edge_b_port],
               connections=[('connector', {'role': 'edge', 'port': edge_port_iy})])

        router('IX', connections=[('listener', {'role': 'inter-router', 'port': inter_router_port}),
                                  ('listener', {'role': 'edge', 'port': edge_port_ix})])
        router('IY', connections=[('connector', {'role': 'inter-router', 'port': inter_router_port}),
                                  ('listener', {'role': 'edge', 'port': edge_port_iy})])

        router('EZ', mode='edge', connections=[('connector', {'role': 'edge', 'port': edge_port_iy})])

        cls.routers[0].is_edge_routers_connected(num_edges=2, role='inter-edge')
        cls.routers[1].is_edge_routers_connected(num_edges=2, role='inter-edge')
        cls.routers[2].is_edge_routers_connected(num_edges=2, role='inter-edge')

        cls.routers[3].wait_router_connected('IY')
        cls.routers[4].wait_router_connected('IX')

        cls.routers[3].is_edge_routers_connected(num_edges=2)
        cls.routers[4].is_edge_routers_connected(num_edges=2)

    def test_01_dynamic_address_same_edge(self):
        test = DynamicAddressTest(self.routers[1].addresses[0], self.routers[1].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_02_dynamic_address_different_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[1].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_03_dynamic_address_int_to_edge(self):
        test = DynamicAddressTest(self.routers[3].addresses[0], self.routers[1].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_04_dynamic_address_edge_to_int(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[3].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_05_dynamic_address_far_int_to_edge(self):
        test = DynamicAddressTest(self.routers[4].addresses[0], self.routers[1].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_06_dynamic_address_edge_to_far_int(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[4].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_07_dynamic_address_edge_to_far_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[5].addresses[0])
        test.run()
        self.assertIsNone(test.error)

    def test_08_dynamic_address_anon_same_edge(self):
        test = DynamicAddressTest(self.routers[1].addresses[0], self.routers[1].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_09_dynamic_address_anon_different_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[1].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_10_dynamic_address_anon_int_to_edge(self):
        test = DynamicAddressTest(self.routers[3].addresses[0], self.routers[1].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_11_dynamic_address_anon_edge_to_int(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[3].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_12_dynamic_address_anon_far_int_to_edge(self):
        test = DynamicAddressTest(self.routers[4].addresses[0], self.routers[1].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_13_dynamic_address_anon_edge_to_far_int(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[4].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_14_dynamic_address_anon_edge_to_far_edge(self):
        test = DynamicAddressTest(self.routers[0].addresses[0], self.routers[5].addresses[0], anon_sender=True)
        test.run()
        self.assertIsNone(test.error)

    def test_15_mobile_address_anon_same_edge(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[0].addresses[0], 'test_15')
        test.run()
        self.assertIsNone(test.error)

    def test_16_mobile_address_anon_different_edge(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[1].addresses[0], 'test_16')
        test.run()
        self.assertIsNone(test.error)

    def test_17_mobile_address_anon_int_to_edge(self):
        test = MobileAddressAnonymousTest(self.routers[3].addresses[0], self.routers[1].addresses[0], 'test_17')
        test.run()
        self.assertIsNone(test.error)

    def test_18_mobile_address_anon_edge_to_int(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[3].addresses[0], 'test_18')
        test.run()
        self.assertIsNone(test.error)

    def test_19_mobile_address_anon_far_int_to_edge(self):
        test = MobileAddressAnonymousTest(self.routers[4].addresses[0], self.routers[1].addresses[0], 'test_19')
        test.run()
        self.assertIsNone(test.error)

    def test_20_mobile_address_anon_edge_to_far_int(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[4].addresses[0], 'test_20')
        test.run()
        self.assertIsNone(test.error)

    def test_21_mobile_address_anon_edge_to_far_edge(self):
        test = MobileAddressAnonymousTest(self.routers[0].addresses[0], self.routers[5].addresses[0], 'test_21')
        test.run()
        self.assertIsNone(test.error)

    def test_22_mobile_address_same_edge(self):
        test = MobileAddressTest(self.routers[0].addresses[0], self.routers[0].addresses[0], 'test_22')
        test.run()
        self.assertIsNone(test.error)

    def test_23_mobile_address_different_edge(self):
        test = MobileAddressTest(self.routers[0].addresses[0], self.routers[1].addresses[0], 'test_23')
        test.run()
        if test.error is not None:
            test.logger.dump()
        self.assertIsNone(test.error)

    def test_24_mobile_address_int_to_edge(self):
        test = MobileAddressTest(self.routers[3].addresses[0], self.routers[1].addresses[0], 'test_24')
        test.run()
        self.assertIsNone(test.error)

    def test_25_mobile_address_edge_to_int(self):
        test = MobileAddressTest(self.routers[0].addresses[0], self.routers[3].addresses[0], 'test_25')
        test.run()
        self.assertIsNone(test.error)

    def test_26_mobile_address_far_int_to_edge(self):
        test = MobileAddressTest(self.routers[4].addresses[0], self.routers[1].addresses[0], 'test_26')
        test.run()
        self.assertIsNone(test.error)

    def test_27_mobile_address_edge_to_far_int(self):
        test = MobileAddressTest(self.routers[0].addresses[0], self.routers[4].addresses[0], 'test_27')
        test.run()
        self.assertIsNone(test.error)

    def test_28_mobile_address_edge_to_far_edge(self):
        test = MobileAddressTest(self.routers[0].addresses[0], self.routers[5].addresses[0], 'test_28')
        test.run()
        self.assertIsNone(test.error)

    def test_29_mobile_address_two_receivers_same_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[1].addresses[0],
                                                      self.routers[0].addresses[0],
                                                      'test_29')
        test.run()
        self.assertIsNone(test.error)

    def test_30_mobile_address_two_receivers_int_to_same_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[2].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      'test_30')
        test.run()
        self.assertIsNone(test.error)

    def test_31_mobile_address_two_receivers_int_to_self_and_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      'test_31')
        test.run()
        self.assertIsNone(test.error)

    def test_32_mobile_address_two_receivers_far_int_to_int_and_far_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      self.routers[4].addresses[0],
                                                      'test_32')
        test.run()
        self.assertIsNone(test.error)

    def test_33_mobile_address_two_receivers_far_mesh_to_int_and_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[3].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      'test_33')
        test.run()
        self.assertIsNone(test.error)

    def test_34_mobile_address_two_receivers_int_to_near_and_far_meshes(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      self.routers[4].addresses[0],
                                                      'test_34')
        test.run()
        self.assertIsNone(test.error)

    def test_35_mobile_address_two_receivers_same_mesh_and_far_mesh(self):
        test = MobileAddressOneSenderTwoReceiversTest(self.routers[1].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      self.routers[5].addresses[0],
                                                      'test_35')
        test.run()
        self.assertIsNone(test.error)

    def test_36_mobile_address_multicast_same_mesh(self):
        test = MobileAddressMulticastTest(self.routers[0].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[2].addresses[0],
                                          'mc.test_36')
        test.run()
        self.assertIsNone(test.error)

    def test_37_mobile_address_multicast_int(self):
        test = MobileAddressMulticastTest(self.routers[3].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[4].addresses[0],
                                          self.routers[3].addresses[0],
                                          'mc.test_37')
        test.run()
        self.assertIsNone(test.error)

    def test_38_mobile_address_multicast_same_mesh_from_int(self):
        test = MobileAddressMulticastTest(self.routers[0].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[2].addresses[0],
                                          self.routers[3].addresses[0],
                                          'mc.test_38')
        test.run()
        self.assertIsNone(test.error)

    def test_39_mobile_address_multicast_different_meshes_from_int(self):
        test = MobileAddressMulticastTest(self.routers[0].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[5].addresses[0],
                                          self.routers[3].addresses[0],
                                          'mc.test_39')
        test.run()
        self.assertIsNone(test.error)

    def test_40_mobile_address_multicast_different_meshes_from_edge(self):
        test = MobileAddressMulticastTest(self.routers[0].addresses[0],
                                          self.routers[1].addresses[0],
                                          self.routers[5].addresses[0],
                                          self.routers[5].addresses[0],
                                          'mc.test_40')
        test.run()
        self.assertIsNone(test.error)


if __name__ == '__main__':
    unittest.main(main_module())

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

"""
Tests the routers TLS Certificate Rotation feature.
"""

import time
from http1_tests import wait_tcp_listeners_up
from system_test import TestCase, main_module, Qdrouterd, unittest, retry
from system_test import CA_CERT, SSL_PROFILE_TYPE, ROUTER_LINK_TYPE
from system_test import CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD
from system_test import SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD
from system_test import CA2_CERT
from system_test import CLIENT2_CERTIFICATE, CLIENT2_PRIVATE_KEY, CLIENT2_PRIVATE_KEY_PASSWORD
from system_test import SERVER2_CERTIFICATE, SERVER2_PRIVATE_KEY, SERVER2_PRIVATE_KEY_PASSWORD
from tcp_streamer import TcpStreamerThread
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE


class InterRouterCertRotationTest(TestCase):
    """
    Validate the Certificate Rotation feature against interior inter-router connections.
    """
    @classmethod
    def setUpClass(cls):
        super(InterRouterCertRotationTest, cls).setUpClass()

    def router(self, name, test_config, data_connection_count, **kwargs):
        config = [
            ('router', {'mode': 'interior',
                        'id': name,
                        'dataConnectionCount': f"{data_connection_count}"}),
            ('listener', {'port': self.tester.get_port(), 'role': 'normal'}),
        ]
        config.extend(test_config)
        return self.tester.qdrouterd(name, Qdrouterd.Config(config), **kwargs)

    def wait_inter_router_conns(self, router, count):
        # Wait until the number of inter-router connections equals count
        ok = retry(lambda rtr=router, ct=count:
                   len(rtr.get_inter_router_conns()) == ct)
        self.assertTrue(ok, f"Failed to get {count} i.r. conns: {router.get_inter_router_conns()}")

    def wait_control_links(self, router, group_ordinal):
        # This function is used after the oldestValidOrdinal is advanced in
        # order to block until the proper inter-router control links are
        # active.
        # NOTE restriction: only works for a router with a single inter-router
        # connector or listener. In other words it blocks until there are only
        # 2 inter-router control links present with the given group_ordinal.
        def _control_link_test(router, ordinal):
            clinks = router.get_active_inter_router_control_links()
            if len(clinks) != 2:
                return False
            cid = clinks[0]['connectionId']
            if cid != clinks[1]['connectionId']:
                return False  # not the same parent conn
            cconns = router.get_inter_router_control_conns()
            for conn in cconns:
                if conn['identity'] == cid and conn['groupOrdinal'] == ordinal:
                    return True
            return False
        return retry(lambda rtr=router, ordinal=group_ordinal:
                     _control_link_test(rtr, ordinal))

    def test_01_ordinal_updates(self):
        """
        Verify that ordinal updates create new inter-router connections. Verify
        that updating oldestValidOrdinal results in the closing of connections
        using expired ordinals
        """
        data_conn_count = 4
        inter_router_port = self.tester.get_port()
        router_L = self.router("RouterL",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'})],
                               data_conn_count, wait=False)
        router_C = self.router("RouterC",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # get the number of active inter-router conns, verify count and tlsOrdinal are 0
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        irc = router_C.get_inter_router_conns()
        zero_ordinals = [c for c in irc if c['tlsOrdinal'] == 0]
        self.assertEqual(data_conn_count + 1, len(zero_ordinals), f"Missing conns: {zero_ordinals}")

        snooper_thread = VFlowSnooperThread(router_C.addresses[0])

        expected = {
            'RouterL': [('ROUTER_ACCESS', {'LINK_COUNT': 1,
                                           'ROLE': 'inter-router',
                                           'IDENTITY': ANY_VALUE})],
            'RouterC': [('LINK', {'PEER': ANY_VALUE,
                                  'OPER_STATUS': 'up',
                                  'ACTIVE_TLS_ORDINAL': 0,
                                  'ROLE': 'inter-router'})]
        }
        success = retry(lambda: snooper_thread.match_records(expected), delay=2)
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Before incrementing the ordinal on the sslProfile, get the vflow LINK record
        # and capture its identity and start time. After we update the ordinal, we will
        # make sure that the LINK record we obtain then has the same ordinal and start time
        # as the current LINK record. This will prove that the LINK records were not recreated
        # across ordinal updates.
        link_vflow_recs = snooper_thread.get_router_records("RouterC", record_type='LINK')
        link_vflow_dict = link_vflow_recs[0]
        link_identity = link_vflow_dict['IDENTITY']
        link_start_time = link_vflow_dict['START_TIME']

        # update tlsOrdinal to 3 and wait for new conns to appear
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, 2 * (data_conn_count + 1))
        self.wait_inter_router_conns(router_L, 2 * (data_conn_count + 1))

        # The ordinal has been updated to 3, check to see if the LINK has the correct
        # ordinal value of 3
        expected = {
            'RouterC': [('LINK', {'PEER': ANY_VALUE,
                                  'OPER_STATUS': 'up',
                                  'ACTIVE_TLS_ORDINAL': 3,
                                  'ROLE': 'inter-router'})]
        }
        success = retry(lambda: snooper_thread.match_records(expected), delay=2)
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Check to see if there is still the same link
        # record as from before the ordinal was updated.
        link_vflow_recs = snooper_thread.get_router_records("RouterC", record_type='LINK')
        link_vflow_dict = link_vflow_recs[0]
        self.assertEqual(link_identity, link_vflow_dict['IDENTITY'])
        self.assertEqual(link_start_time, link_vflow_dict['START_TIME'])

        # Update oldestValidOrdinal to 3. Expect the older connections with an
        # ordinal value of 0 to be deleted
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        self.wait_inter_router_conns(router_C, data_conn_count + 1)

        # The oldestValidOrdinal has been updated to 3, we will check to see if there is
        # still the same link record as from before the ordinal was updated.
        link_vflow_recs = snooper_thread.get_router_records("RouterC", record_type='LINK')
        link_vflow_dict = link_vflow_recs[0]
        self.assertEqual(link_identity, link_vflow_dict['IDENTITY'])
        self.assertEqual(link_start_time, link_vflow_dict['START_TIME'])

        # Verify all group Ordinals are 3 (same as connector tlsOrdinal)
        irc = router_C.get_inter_router_conns()
        irc.extend(router_L.get_inter_router_conns())
        self.assertEqual(2 * (data_conn_count + 1),
                         len([c for c in irc if c['groupOrdinal'] == 3]),
                         f"Unexpected conns: {irc}")
        router_L.teardown()

        # Router L has now been torn down, check to see if the RouterC's OPER_STATUS on the LINK record is "down"
        expected = {
            'RouterC': [('LINK', {'PEER': ANY_VALUE,
                                  'OPER_STATUS': 'down',
                                  "PROTOCOL": "amqp",
                                  'ROLE': 'inter-router'})]
        }

        success = retry(lambda: snooper_thread.match_records(expected), delay=2)
        router_C.teardown()

    def test_02_drop_old(self):
        """
        Verify that connections that use older TLS ordinals are not
        restored when the inter-router connection drops.
        """
        data_conn_count = 4
        inter_router_port = self.tester.get_port()
        router_L = self.router("RouterL",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'})],
                               data_conn_count, wait=False)
        router_C = self.router("RouterC",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # wait for all the inter-router connections to come up
        self.wait_inter_router_conns(router_C, data_conn_count + 1)

        # update tlsOrdinal to 3 and wait for new conns to appear
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, 2 * (data_conn_count + 1))
        self.wait_inter_router_conns(router_L, 2 * (data_conn_count + 1))

        # Destroy router_L - this will cause all connections to drop
        router_L.teardown()
        self.wait_inter_router_conns(router_C, 0)

        # Re-instantiate router_L:
        router_L = self.router("RouterL2",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL2")
        router_L.wait_router_connected("RouterC")

        # expect only those connectors with ordinal == 3 are restored
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        time.sleep(1.0)  # ensure no extra conns come up
        irc = router_C.get_inter_router_conns()
        irc.extend(router_L.get_inter_router_conns())
        self.assertEqual(2 * (data_conn_count + 1),
                         len([c for c in irc if c['groupOrdinal'] == 3]),
                         f"Unexpected conns: {irc}")

        router_L.teardown()
        router_C.teardown()

    def test_03_connector_tcp_streams(self):
        """
        Verify that existing TCP streams are not interrupted when new
        inter-router connections are established.

        This test sets up several TCP streaming connections through two
        routers. It then does a connector-side certificate rotation and
        verifies that the streams have not failed.

        It then creates another set of TCP streaming connections. It verifies
        that these streams are sent over the upgraded connections.

        Lastly it expires the original certificates and verifies that the first
        set of streaming TCP sessions have been dropped. It also verifies that
        the second set of streaming TCP sessions are still active.
        """
        data_conn_count = 4
        inter_router_port = self.tester.get_port()
        tcp_listener_port_1 = self.tester.get_port()
        tcp_listener_port_2 = self.tester.get_port()
        tcp_connector_port_1 = self.tester.get_port()
        tcp_connector_port_2 = self.tester.get_port()

        router_L = self.router("RouterL",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'}),
                                ('tcpListener', {'name': 'tcpListener01',
                                                 'address': 'tcp/streaming/1',
                                                 'port': tcp_listener_port_1}),
                                ('tcpListener', {'name': 'tcpListener02',
                                                 'address': 'tcp/streaming/2',
                                                 'port': tcp_listener_port_2})],
                               data_conn_count, wait=False)
        router_C = self.router("RouterC",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'}),
                                ('tcpConnector', {'name': 'tcpConnector01',
                                                  'address': 'tcp/streaming/1',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_1}),
                                ('tcpConnector', {'name': 'tcpConnector02',
                                                  'address': 'tcp/streaming/2',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_2})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # wait for all the inter-router data connections and the TCP listener
        # ports to come up
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        wait_tcp_listeners_up(router_L.addresses[0])

        # Verify all inter-router conns on Router_C are based on the same
        # tlsOrdinal, which is zero.
        ir_conns = router_C.get_inter_router_conns()
        for ir_conn in ir_conns:
            self.assertEqual(0, ir_conn['tlsOrdinal'])

        # This test allows the certificate rotation to complete before expiring
        # the old inter-router connections. Therefore we expect that the
        # router's topology does not change during this test. Let the topology
        # settle before continuting the test. Using the default flux_interval
        # which should be "long enough" (fingers crossed)
        flux_interval = 4.1  # wait a bit longer than the interval to prevent races
        last_topo_C = router_C.get_last_topology_change()
        last_topo_L = router_L.get_last_topology_change()
        deadline = time.time() + flux_interval
        while deadline > time.time():  # test will timeout on failure
            time.sleep(0.1)
            topo_C = router_C.get_last_topology_change()
            topo_L = router_L.get_last_topology_change()
            if topo_C != last_topo_C or topo_L != last_topo_L:
                last_topo_C = topo_C
                last_topo_L = topo_L
                deadline = time.time() + flux_interval

        # start TCP streaming connections across the routers
        tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_1),
                                         server_addr=('0.0.0.0', tcp_connector_port_1),
                                         client_count=10, poll_timeout=0.2)

        # Now wait until the streaming client have connected and traffic is
        # being sent
        ok = retry(lambda: tcp_streamer.active_clients == 10)
        self.assertTrue(ok, f"Streaming clients failed {tcp_streamer.active_clients}")
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")

        # Expect 2 streaming links per TCP flow (links are uni-directional)
        self.assertEqual(20, len(router_L.get_active_inter_router_data_links()),
                         f"Failed to get 20 links: {router_L.get_active_inter_router_data_links()}")

        # Now rotate the certs: update tlsOrdinal to 3
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')

        # wait until the new control links are active and all the data
        # connections have established
        ok = self.wait_control_links(router_C, 3)
        self.assertTrue(ok, f"Bad control links: {router_C.get_active_inter_router_control_links()}")
        ok = self.wait_control_links(router_L, 3)
        self.assertTrue(ok, f"Bad control links: {router_L.get_active_inter_router_control_links()}")
        self.wait_inter_router_conns(router_L, 2 * (data_conn_count + 1))

        # verify that the streamer is still running and the streams are still passing traffic
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")
        self.assertTrue(tcp_streamer.is_alive, "Streamer has failed!")

        # Now create a new streamer. Its TCP flows should use the new
        # inter-router-data links
        new_tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_2),
                                             server_addr=('0.0.0.0', tcp_connector_port_2),
                                             client_count=4, poll_timeout=0.2)
        ok = retry(lambda: new_tcp_streamer.active_clients == 4)
        self.assertTrue(ok, f"Streaming clients failed {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {new_tcp_streamer.bytes_received}")

        # Expect an additional 2 streaming links per TCP flow (links are uni-directional)
        self.assertEqual(28, len(router_L.get_active_inter_router_data_links()),
                         f"Failed to get 28 links: {router_L.get_active_inter_router_data_links()}")

        # Now expire the old inter-router connections by setting the
        # oldestValidOrdinal to 3. Expect the connections that carry the
        # old streaming data to close.
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        ok = retry(lambda: tcp_streamer.is_alive is False)
        self.assertTrue(ok, "Failed to terminate the streamer")
        tcp_streamer.join()

        # Verify that the new TCP flows are still actively passing data
        self.assertEqual(4, new_tcp_streamer.active_clients,
                         f"New flows failed: {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Streaming data failed {new_tcp_streamer.bytes_received}")
        new_tcp_streamer.join()

        # Verify that the remaining inter-router conns (both data and control)
        # share the group ordinal value (currently the same as the
        # connector-side tlsOrdinal - may change in the future)
        ir_conns = router_C.get_inter_router_conns()
        ir_conns.extend(router_L.get_inter_router_conns())
        for ir_conn in ir_conns:
            self.assertEqual(3, ir_conn['groupOrdinal'], f"Wrong ordinal {ir_conn}")

        # Lastly check that neither router has seen a topology change:
        self.assertEqual(last_topo_C, router_C.get_last_topology_change(),
                         "Unexpected topology change for RouterC")
        self.assertEqual(last_topo_L, router_L.get_last_topology_change(),
                         "Unexpected topology change for RouterL")

        router_L.teardown()
        router_C.teardown()

    def test_04_rotate_storm(self):
        """
        Similar to test_03_tcp_streams but stresses the router by requesting
        back to back rotations while creating new TCP streams. This test does
        not wait for inter-router connections to settle before expiring them.
        """
        data_conn_count = 4
        inter_router_port = self.tester.get_port()
        tcp_listener_port_1 = self.tester.get_port()
        tcp_listener_port_2 = self.tester.get_port()
        tcp_connector_port_1 = self.tester.get_port()
        tcp_connector_port_2 = self.tester.get_port()

        router_L = self.router("RouterL",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'}),
                                ('tcpListener', {'name': 'tcpListener01',
                                                 'address': 'tcp/streaming/1',
                                                 'port': tcp_listener_port_1}),
                                ('tcpListener', {'name': 'tcpListener02',
                                                 'address': 'tcp/streaming/2',
                                                 'port': tcp_listener_port_2})],
                               data_conn_count, wait=False)
        router_C = self.router("RouterC",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'}),
                                ('tcpConnector', {'name': 'tcpConnector01',
                                                  'address': 'tcp/streaming/1',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_1}),
                                ('tcpConnector', {'name': 'tcpConnector02',
                                                  'address': 'tcp/streaming/2',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_2})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # wait for all the inter-router connections and the TCP listener ports
        # to come up
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        wait_tcp_listeners_up(router_L.addresses[0])

        # start TCP streaming connections across the routers
        tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_1),
                                         server_addr=('0.0.0.0', tcp_connector_port_1),
                                         client_count=20, poll_timeout=0.2)

        # do several back to back rotations while the connections are coming
        # up. Do not wait for anything to stabilize between updates

        max_ordinal = 20
        for tls_ordinal in range(1, max_ordinal + 1):
            router_C.management.update(type=SSL_PROFILE_TYPE,
                                       attributes={'ordinal': tls_ordinal},
                                       name='ConnectorSslProfile')

        # Immediately teardown all new connections but the last one (max_ordinal)
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': max_ordinal},
                                   name='ConnectorSslProfile')

        # Wait for the carnage to subside by waiting until the control links
        # have all closed with the exception of two links for max_ordinal
        ok = self.wait_control_links(router_C, max_ordinal)
        self.assertTrue(ok, f"Bad control links: {router_C.get_active_inter_router_control_links()}")
        ok = self.wait_control_links(router_L, max_ordinal)
        self.assertTrue(ok, f"Bad control links: {router_L.get_active_inter_router_control_links()}")

        # wait for all data conns to come up and verify all inter-router conns
        # have the same group ordinal
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        ir_conns = router_C.get_inter_router_conns()
        ir_conns.extend(router_L.get_inter_router_conns())
        for ir_conn in ir_conns:
            self.assertEqual(max_ordinal, ir_conn['groupOrdinal'], f"Wrong ordinal {ir_conn}")

        # This test aggressively tears down inter-router connections without
        # waiting for them to complete connection to the peer. Therefore it is
        # likely the routing path was momentarily lost. Ensure the routers are
        # visible to each other before starting new flows:
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # Test the inter-router path by firing up more TCP client flows
        new_tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_2),
                                             server_addr=('0.0.0.0', tcp_connector_port_2),
                                             client_count=4, poll_timeout=0.2)
        ok = retry(lambda: new_tcp_streamer.active_clients == 4)
        self.assertTrue(ok, f"Streaming clients failed {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {new_tcp_streamer.bytes_received}")

        tcp_streamer.join()
        new_tcp_streamer.join()

        router_L.teardown()
        router_C.teardown()

    def test_05_listener_tcp_streams(self):
        """
        Similar to test_03_connector_tcp_streams but in this case the
        connections are dropped due to advancing the oldestValidOrdinal on the
        listener-side.

        In this test the listener-side sslProfile will be rotated to a new
        CA/certificate that are incompatible with the connector side. The
        connector side will then be rotated to a compatible
        CA/certificate. Then the older certificates will be expired on the
        listener side.
        """
        data_conn_count = 4
        inter_router_port = self.tester.get_port()
        tcp_listener_port_1 = self.tester.get_port()
        tcp_listener_port_2 = self.tester.get_port()
        tcp_connector_port_1 = self.tester.get_port()
        tcp_connector_port_2 = self.tester.get_port()

        router_L = self.router("RouterL",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'inter-router',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'}),
                                ('tcpListener', {'name': 'tcpListener01',
                                                 'address': 'tcp/streaming/1',
                                                 'port': tcp_listener_port_1}),
                                ('tcpListener', {'name': 'tcpListener02',
                                                 'address': 'tcp/streaming/2',
                                                 'port': tcp_listener_port_2})],
                               data_conn_count, wait=False)
        router_C = self.router("RouterC",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'}),
                                ('tcpConnector', {'name': 'tcpConnector01',
                                                  'address': 'tcp/streaming/1',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_1}),
                                ('tcpConnector', {'name': 'tcpConnector02',
                                                  'address': 'tcp/streaming/2',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_2})],
                               data_conn_count, wait=True)
        router_C.wait_router_connected("RouterL")
        router_L.wait_router_connected("RouterC")

        # wait for all the inter-router data connections and the TCP listener
        # ports to come up
        self.wait_inter_router_conns(router_L, data_conn_count + 1)
        wait_tcp_listeners_up(router_L.addresses[0])

        # Verify all inter-router conns on Router_C are based on the same
        # tlsOrdinal, which is zero.
        ir_conns = router_C.get_inter_router_conns()
        for ir_conn in ir_conns:
            self.assertEqual(0, ir_conn['tlsOrdinal'])

        # This test allows the certificate rotation to complete before expiring
        # the old inter-router connections. Therefore we expect that the
        # router's topology does not change during this test. Let the topology
        # settle before continuting the test. Using the default flux_interval
        # which should be "long enough" (fingers crossed)
        flux_interval = 4.1  # wait a bit longer than the interval to prevent races
        last_topo_C = router_C.get_last_topology_change()
        last_topo_L = router_L.get_last_topology_change()
        deadline = time.time() + flux_interval
        while deadline > time.time():  # test will timeout on failure
            time.sleep(0.1)
            topo_C = router_C.get_last_topology_change()
            topo_L = router_L.get_last_topology_change()
            if topo_C != last_topo_C or topo_L != last_topo_L:
                last_topo_C = topo_C
                last_topo_L = topo_L
                deadline = time.time() + flux_interval

        # start TCP streaming connections across the routers
        tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_1),
                                         server_addr=('0.0.0.0', tcp_connector_port_1),
                                         client_count=10, poll_timeout=0.2)

        # Now wait until the streaming client have connected and traffic is
        # being sent
        ok = retry(lambda: tcp_streamer.active_clients == 10)
        self.assertTrue(ok, f"Streaming clients failed {tcp_streamer.active_clients}")
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")

        # Expect 2 streaming links per TCP flow (links are uni-directional)
        self.assertEqual(20, len(router_L.get_active_inter_router_data_links()),
                         f"Failed to get 20 links: {router_L.get_active_inter_router_data_links()}")

        # Store the connection identifiers of all inter-router connections on
        # the Listener side. This will be used as a filter to identify the new
        # connections that have established due to certificate rotation.
        old_conns = [conn["identity"] for conn in router_L.get_inter_router_conns()]
        self.assertEqual(data_conn_count + 1, len(old_conns))

        # Now rotate the certs: Start on the listener side
        router_L.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 10,
                                               'caCertFile': CA2_CERT,
                                               'certFile': SERVER2_CERTIFICATE,
                                               'privateKeyFile': SERVER2_PRIVATE_KEY,
                                               'password': SERVER2_PRIVATE_KEY_PASSWORD},
                                   name='ListenerSslProfile')

        # And now the connector. This will result in a new set of inter-router
        # connections that will replace the existing ones.
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 11,
                                               'caCertFile': CA2_CERT,
                                               'certFile': CLIENT2_CERTIFICATE,
                                               'privateKeyFile': CLIENT2_PRIVATE_KEY,
                                               'password': CLIENT2_PRIVATE_KEY_PASSWORD},
                                   name='ConnectorSslProfile')

        # wait until the new control links are active and all the data
        # connections have established
        self.wait_inter_router_conns(router_L, 2 * (data_conn_count + 1))
        ok = self.wait_control_links(router_C, 11)
        self.assertTrue(ok, f"Bad control links: {router_C.get_active_inter_router_control_links()}")
        ok = self.wait_control_links(router_L, 11)
        self.assertTrue(ok, f"Bad control links: {router_L.get_active_inter_router_control_links()}")

        # verify that the streamer is still running and the streams are still passing traffic
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")
        self.assertTrue(tcp_streamer.is_alive, "Streamer has failed!")

        # Now create a new streamer. Its TCP flows should use the new
        # inter-router-data links
        new_tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_2),
                                             server_addr=('0.0.0.0', tcp_connector_port_2),
                                             client_count=4, poll_timeout=0.2)
        ok = retry(lambda: new_tcp_streamer.active_clients == 4)
        self.assertTrue(ok, f"Streaming clients failed {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {new_tcp_streamer.bytes_received}")

        # Expect an additional 2 streaming links per TCP flow (links are uni-directional)
        self.assertEqual(28, len(router_L.get_active_inter_router_data_links()),
                         f"Failed to get 28 links: {router_L.get_active_inter_router_data_links()}")

        # Verify that new Listener-side connections are using the latest
        # tlsOrdinal value for the Listener's sslProfile (10).
        new_conns = [conn for conn in router_L.get_inter_router_conns() if conn['identity'] not in old_conns]
        self.assertEqual(data_conn_count + 1, len(new_conns))
        for conn in new_conns:
            self.assertEqual(10, conn["tlsOrdinal"], f"Wrong tlsOrdinal {conn}")

        # Now expire the old inter-router connections by setting the listeners
        # oldestValidOrdinal to 10. Expect the connections that carry the old
        # streaming data to close.
        router_L.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': 10},
                                   name='ListenerSslProfile')
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        ok = retry(lambda: tcp_streamer.is_alive is False)
        self.assertTrue(ok, "Failed to terminate the streamer")
        tcp_streamer.join()

        # verify that all remaining connections are using the proper tlsOrdinal
        for conn in router_L.get_inter_router_conns():
            self.assertEqual(10, conn["tlsOrdinal"], f"Wrong Listener tlsOrdinal {conn}")
        for conn in router_C.get_inter_router_conns():
            self.assertEqual(11, conn["tlsOrdinal"], f"Wrong Connector tlsOrdinal {conn}")

        # Verify that the new TCP flows are still actively passing data
        self.assertEqual(4, new_tcp_streamer.active_clients,
                         f"New flows failed: {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Streaming data failed {new_tcp_streamer.bytes_received}")
        new_tcp_streamer.join()

        # Verify that the remaining inter-router conns (both data and control)
        # share the group ordinal value (currently the same as the
        # connector-side tlsOrdinal - may change in the future)
        ir_conns = router_C.get_inter_router_conns()
        ir_conns.extend(router_L.get_inter_router_conns())
        for ir_conn in ir_conns:
            self.assertEqual(11, ir_conn['groupOrdinal'], f"Wrong ordinal {ir_conn}")

        # Lastly check that neither router has seen a topology change:
        self.assertEqual(last_topo_C, router_C.get_last_topology_change(),
                         "Unexpected topology change for RouterC")
        self.assertEqual(last_topo_L, router_L.get_last_topology_change(),
                         "Unexpected topology change for RouterL")

        router_L.teardown()
        router_C.teardown()


class InteriorEdgeCertRotationTest(TestCase):
    """
    Validate the Certificate Rotation feature on edge-to-interior connections
    """
    @classmethod
    def setUpClass(cls):
        super(InteriorEdgeCertRotationTest, cls).setUpClass()

    def router(self, name, mode, test_config, **kwargs):
        config = [
            ('router', {'mode': mode,
                        'id': name}),
            ('listener', {'port': self.tester.get_port(), 'role': 'normal'}),
        ]
        config.extend(test_config)
        return self.tester.qdrouterd(name, Qdrouterd.Config(config), **kwargs)

    def _get_edge_downlinks(self, router):
        mgmt = router.management
        links = mgmt.query(type=ROUTER_LINK_TYPE).get_dicts()
        return [link for link in links if link['linkType'] == "edge-downlink"]

    def _get_addr_tracking_links(self, router):
        mgmt = router.management
        links = mgmt.query(type=ROUTER_LINK_TYPE).get_dicts()
        return [link for link in links if link['linkType'] == "endpoint"
                and link['owningAddr'] == "M_$qd.edge_addr_tracking"]

    def _get_anonymous_links_by_conn(self, router, conn_id):
        mgmt = router.management
        links = mgmt.query(type=ROUTER_LINK_TYPE).get_dicts()
        return [link for link in links if link["connectionId"] == conn_id
                and link["linkType"] == "endpoint"
                and link["owningAddr"] is None]

    def test_01_tcp_streams(self):
        """
        Verify that existing TCP streams are not interrupted when new
        inter-router connections are established.

        This test sets up several TCP streaming connections through an edge
        router into an interior router. It then does a certificate rotation and
        verifies that the streams have not failed.

        It then creates another set of TCP streaming connections. It verifies
        that these streams are sent over the upgraded connections.
        """
        inter_router_port = self.tester.get_port()
        tcp_listener_port_1 = self.tester.get_port()
        tcp_listener_port_2 = self.tester.get_port()
        tcp_connector_port_1 = self.tester.get_port()
        tcp_connector_port_2 = self.tester.get_port()

        router_I = self.router("RouterI", "interior",
                               [('sslProfile', {'name': 'ListenerSslProfile',
                                                'caCertFile': CA_CERT,
                                                'certFile': SERVER_CERTIFICATE,
                                                'privateKeyFile': SERVER_PRIVATE_KEY,
                                                'password': SERVER_PRIVATE_KEY_PASSWORD}),
                                ('listener', {'name': 'Listener01',
                                              'role': 'edge',
                                              'host': '0.0.0.0',
                                              'port': inter_router_port,
                                              'requireSsl': 'yes',
                                              'sslProfile': 'ListenerSslProfile'}),
                                ('tcpConnector', {'name': 'tcpConnector01',
                                                  'address': 'tcp/streaming/1',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_1}),
                                ('tcpConnector', {'name': 'tcpConnector02',
                                                  'address': 'tcp/streaming/2',
                                                  'host': 'localhost',
                                                  'port': tcp_connector_port_2})],
                               wait=False)
        router_E = self.router("RouterE", "edge",
                               [('sslProfile', {'name': "ConnectorSslProfile",
                                                'ordinal': 0,
                                                'oldestValidOrdinal': 0,
                                                'caCertFile': CA_CERT,
                                                'certFile': CLIENT_CERTIFICATE,
                                                'privateKeyFile': CLIENT_PRIVATE_KEY,
                                                'password': CLIENT_PRIVATE_KEY_PASSWORD}),
                                ('connector', {'role': 'edge',
                                               'host': 'localhost',
                                               'port': inter_router_port,
                                               'verifyHostname': 'yes',
                                               'sslProfile': 'ConnectorSslProfile'}),
                                ('tcpListener', {'name': 'tcpListener01',
                                                 'address': 'tcp/streaming/1',
                                                 'port': tcp_listener_port_1}),
                                ('tcpListener', {'name': 'tcpListener02',
                                                 'address': 'tcp/streaming/2',
                                                 'port': tcp_listener_port_2})],
                               wait=False)
        router_I.is_edge_routers_connected(1)

        # wait for all the TCP listeners to come up
        wait_tcp_listeners_up(router_E.addresses[0])

        # Wait for the edge downlink and tracking link to come up. Record the
        # connection id. The test will time out if these loops do not exit
        edge_conn_1 = None
        while True:
            downlinks = self._get_edge_downlinks(router_I)
            etlinks = self._get_addr_tracking_links(router_I)
            if len(downlinks) == 1 and len(etlinks) == 1:
                self.assertEqual(downlinks[0]["connectionId"],
                                 etlinks[0]["connectionId"],
                                 "Incorrect edge router links")
                edge_conn_1 = downlinks[0]["connectionId"]
                break

        # now start TCP streaming connections across the routers
        tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_1),
                                         server_addr=('0.0.0.0', tcp_connector_port_1),
                                         client_count=10, poll_timeout=0.2)

        # Now wait until the streaming client have connected and traffic is
        # being sent
        ok = retry(lambda: tcp_streamer.active_clients == 10)
        self.assertTrue(ok, f"Streaming clients failed {tcp_streamer.active_clients}")
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")

        # Expect 2 anonymous links are created over the edge connection, 2 for
        # each TCP client
        alinks = self._get_anonymous_links_by_conn(router_I, edge_conn_1)
        self.assertGreaterEqual(len(alinks), 20,
                                f"Expected at least 20 anonymous links: {alinks}")

        # Now rotate the certificate on the edge. This should create a new
        # inter-edge connection
        router_E.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')

        # wait until both the edge downlink and tracking links have moved
        def _wait_edge_rotate(router, old_conn):
            downlinks = self._get_edge_downlinks(router)
            if len(downlinks) != 1:
                return False
            etlinks = self._get_addr_tracking_links(router)
            if len(etlinks) != 1:
                return False
            if downlinks[0]["connectionId"] != etlinks[0]["connectionId"]:
                return False
            if downlinks[0]["connectionId"] == old_conn:
                return False
            return downlinks[0]["connectionId"]
        edge_conn_2 = retry(lambda router=router_I, old_conn=edge_conn_1:
                            _wait_edge_rotate(router, old_conn))
        self.assertTrue(edge_conn_2 is not False,
                        "Second edge conn did not activate")

        # Now create a new streamer. Its TCP flows should use the new
        # edge connection
        new_tcp_streamer = TcpStreamerThread(client_addr=('localhost', tcp_listener_port_2),
                                             server_addr=('0.0.0.0', tcp_connector_port_2),
                                             client_count=10, poll_timeout=0.2)
        ok = retry(lambda: new_tcp_streamer.active_clients == 10)
        self.assertTrue(ok, f"Streaming clients failed {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {new_tcp_streamer.bytes_received}")

        # verify that the old streamer is still running and the streams are still passing traffic
        begin_recv = tcp_streamer.bytes_received
        ok = retry(lambda: tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {tcp_streamer.bytes_received}")
        self.assertTrue(tcp_streamer.is_alive, "Streamer has failed!")

        # Expect 2 anonymous links are created over the edge connection, 2 for
        # each TCP client. Verify this is the case for the new and old edge
        # connections:
        alinks = self._get_anonymous_links_by_conn(router_I, edge_conn_1)
        self.assertGreaterEqual(len(alinks), 20,
                                f"Expected at least 20 anonymous links on 1: {alinks}")
        alinks = self._get_anonymous_links_by_conn(router_I, edge_conn_2)
        self.assertGreaterEqual(len(alinks), 20,
                                f"Expected at least 20 anonymous links on 2: {alinks}")

        # Now expire the certificate on the original connection, this should
        # cause the first TCP streamer to exit due to connection drop
        router_E.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': 3},
                                   name='ConnectorSslProfile')
        ok = retry(lambda: tcp_streamer.is_alive is False)
        self.assertTrue(ok, "Failed to terminate the streamer")
        tcp_streamer.join()

        # Verify there is only one edge connection
        while True:
            edge_conns = router_I.get_edge_router_conns()
            if len(edge_conns) == 1:
                break

        # And the streamer is still passing data:
        ok = retry(lambda: new_tcp_streamer.active_clients == 10)
        self.assertTrue(ok, f"Streaming clients failed {new_tcp_streamer.active_clients}")
        begin_recv = new_tcp_streamer.bytes_received
        ok = retry(lambda: new_tcp_streamer.bytes_received > begin_recv)
        self.assertTrue(ok, f"Failed to stream data {new_tcp_streamer.bytes_received}")

        new_tcp_streamer.join()
        router_I.teardown()
        router_E.teardown()


if __name__ == '__main__':
    unittest.main(main_module())

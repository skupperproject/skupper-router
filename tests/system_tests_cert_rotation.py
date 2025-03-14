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
from system_test import TestCase, main_module, Qdrouterd, unittest, retry
from system_test import CA_CERT, SSL_PROFILE_TYPE
from system_test import CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD
from system_test import SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD


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
        ok = retry(lambda rtr=router, ct=count:
                   len(rtr.get_inter_router_conns()) == ct)
        self.assertTrue(ok, f"Failed to get {count} i.r. conns: {router.get_inter_router_conns()}")

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

        # get the number of active inter-router conns, verify count and tlsOrdinal are 0
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        irc = router_C.get_inter_router_conns()
        zero_ordinals = [c for c in irc if c['tlsOrdinal'] == 0]
        self.assertEqual(data_conn_count + 1, len(zero_ordinals), f"Missing conns: {zero_ordinals}")

        # update tlsOrdinal to 3 and wait for new conns to appear
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, 2 * (data_conn_count + 1))

        # Update oldestValidOrdinal to 3. Expect the older connections with an
        # ordinal value of 0 to be deleted
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'oldestValidOrdinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, data_conn_count + 1)

        # Verify all tlsOrdinals are 3
        irc = router_C.get_inter_router_conns()
        self.assertEqual(data_conn_count + 1,
                         len([c for c in irc if c['tlsOrdinal'] == 3]),
                         f"Unexpected conns: {irc}")
        router_L.teardown()
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

        # wait for the inter-router connections to come up
        self.wait_inter_router_conns(router_C, data_conn_count + 1)

        # update tlsOrdinal to 3 and wait for new conns to appear
        router_C.management.update(type=SSL_PROFILE_TYPE,
                                   attributes={'ordinal': 3},
                                   name='ConnectorSslProfile')
        self.wait_inter_router_conns(router_C, 2 * (data_conn_count + 1))

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

        # expect only those connectors with ordinal == 3 are restored
        self.wait_inter_router_conns(router_C, data_conn_count + 1)
        time.sleep(1.0)  # ensure no extra conns come up
        irc = router_C.get_inter_router_conns()
        self.assertEqual(data_conn_count + 1, len(irc), f"Wrong conns: {irc}")
        self.assertEqual(0, len([c for c in irc if c['tlsOrdinal'] != 3]),
                         f"tlsOrdinals !=3: {irc}")
        router_L.teardown()
        router_C.teardown()


if __name__ == '__main__':
    unittest.main(main_module())

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
# under the License
#

import json
import os

from system_test import TestCase, Qdrouterd, retry, retry_assertion
from system_test import Logger, TCP_LISTENER_TYPE, TCP_CONNECTOR_TYPE
from system_test import SERVER_CERTIFICATE, SERVER_PRIVATE_KEY_NO_PASS, CA_CERT
from system_test import CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY
from system_test import SERVER_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD
from system_test import SERVER_PRIVATE_KEY_PASSWORD
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE
from TCP_echo_client import TcpEchoClient
from TCP_echo_server import TcpEchoServer


class TerminateTcpConnectionsTest(TestCase):
    """
    Test the router config flag 'dropTcpConnections'. All  corresponding active
    tcp flows are expected to terminate when a tcpListener or tcpConnector is
    deleted if the config flag is set. Otherwise, the flows should stay alive.
    """

    @classmethod
    def setUpClass(cls):
        """
        Start two routers: R1 and R2. Both have two tcpConnetors with two TCP echo
        server attached. Both router also  has four tcpListeners. One of the
        tcpConnectors and two of the tcpListeners are using SSL at both routers.
        The new config flag is tuned on for R1 but not for R2.
        Each tcpConnector has a unique VAN address which is also used by one tcpListener
        at each router. The four distinct VAN addresses are used to test deletion of a
        tcpListeners and tcpConnectors with ot without using SSL.
        """
        super(TerminateTcpConnectionsTest, cls).setUpClass()

        cls.test_name = 'TerminateTcpConnectionsTest'

        router_1_id = 'R1'
        router_2_id = 'R2'

        # SSL info
        cls.ssl_info = {'SERVER_CERTIFICATE': SERVER_CERTIFICATE,
                        'SERVER_PRIVATE_KEY': SERVER_PRIVATE_KEY_NO_PASS,
                        'CA_CERT': CA_CERT}
        cls.client_ssl_info = {'CLIENT_CERTIFICATE': CLIENT_CERTIFICATE,
                               'CLIENT_PRIVATE_KEY': CLIENT_PRIVATE_KEY,
                               'CLIENT_PRIVATE_KEY_PASSWORD': CLIENT_PRIVATE_KEY_PASSWORD,
                               'CA_CERT': CA_CERT}
        tcp_listener_ssl_profile_name = 'tcp-listener-ssl-profile'

        # VAN addresses to use without SSL config
        cls.address_no_ssl = [cls.test_name + '_no_ssl_1', cls.test_name + '_no_ssl_2']

        # VAN addresses to use with SSL config
        cls.address_ssl = [cls.test_name + '_ssl_1', cls.test_name + '_ssl_2']

        # Launch TCP echo servers
        server_logger = Logger(title=cls.test_name,
                               print_to_console=True,
                               save_for_dump=False,
                               ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                      f"{cls.test_name}_echo_server.log"))
        echo_servers = {}
        server_prefix = f"{cls.test_name} ECHO_SERVER_1_no_ssl"
        echo_servers[cls.address_no_ssl[0]] = TcpEchoServer(prefix=server_prefix,
                                                            port=0,
                                                            logger=server_logger)
        assert echo_servers[cls.address_no_ssl[0]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_2_no_ssl"
        echo_servers[cls.address_no_ssl[1]] = TcpEchoServer(prefix=server_prefix,
                                                            port=0,
                                                            logger=server_logger)
        assert echo_servers[cls.address_no_ssl[1]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_1_ssl"
        echo_servers[cls.address_ssl[0]] = TcpEchoServer(prefix=server_prefix,
                                                         port=0,
                                                         ssl_info=cls.ssl_info,
                                                         logger=server_logger)
        assert echo_servers[cls.address_ssl[0]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_2_ssl"
        echo_servers[cls.address_ssl[1]] = TcpEchoServer(prefix=server_prefix,
                                                         port=0,
                                                         ssl_info=cls.ssl_info,
                                                         logger=server_logger)
        assert echo_servers[cls.address_ssl[1]].is_running
        cls.echo_servers = echo_servers

        # Create listener ports
        cls.listener_ports = {router_1_id: {}, router_2_id: {}}
        for rtr in [router_1_id, router_2_id]:
            for addr in cls.address_no_ssl + cls.address_ssl:
                cls.listener_ports[rtr][addr] = cls.tester.get_port()

        # Launch routers: router_1 has the TCP connections termination flag turned on (by default),
        # router_2 has the flag turned off explicitly
        inter_router_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_1_id, 'dropTcpConnections': True}),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': CLIENT_PRIVATE_KEY_PASSWORD}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port}),
            ('tcpListener', {'host': "0.0.0.0",
                             'address': cls.address_no_ssl[0],
                             'port': cls.listener_ports[router_1_id][cls.address_no_ssl[0]]}),
            ('tcpListener', {'host': "0.0.0.0",
                             'address': cls.address_no_ssl[1],
                             'port': cls.listener_ports[router_1_id][cls.address_no_ssl[1]]}),
            ('tcpConnector', {'host': "localhost",
                              'address': cls.address_no_ssl[0],
                              'port': echo_servers[cls.address_no_ssl[0]].port}),
            ('tcpListener', {'host': "0.0.0.0",
                             'sslProfile': tcp_listener_ssl_profile_name,
                             'address': cls.address_ssl[0],
                             'port': cls.listener_ports[router_1_id][cls.address_ssl[0]]}),
            ('tcpListener', {'host': "0.0.0.0",
                             'sslProfile': tcp_listener_ssl_profile_name,
                             'address': cls.address_ssl[1],
                             'port': cls.listener_ports[router_1_id][cls.address_ssl[1]]}),
            ('tcpConnector', {'host': "localhost",
                              'sslProfile': 'tcp-connector-ssl-profile',
                              'address': cls.address_ssl[0],
                              'port': echo_servers[cls.address_ssl[0]].port})
        ])
        config_2 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_2_id, 'dropTcpConnections': False}),
            ('sslProfile', {'name': 'tcp-listener-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('sslProfile', {'name': 'tcp-connector-ssl-profile',
                            'caCertFile': CA_CERT,
                            'certFile': CLIENT_CERTIFICATE,
                            'privateKeyFile': CLIENT_PRIVATE_KEY,
                            'password': CLIENT_PRIVATE_KEY_PASSWORD}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port}),
            ('tcpListener', {'host': "0.0.0.0",
                             'address': cls.address_no_ssl[0],
                             'port': cls.listener_ports[router_2_id][cls.address_no_ssl[0]]}),
            ('tcpListener', {'host': "0.0.0.0",
                             'address': cls.address_no_ssl[1],
                             'port': cls.listener_ports[router_2_id][cls.address_no_ssl[1]]}),
            ('tcpConnector', {'host': "localhost",
                              'address': cls.address_no_ssl[1],
                              'port': echo_servers[cls.address_no_ssl[1]].port}),
            ('tcpListener', {'host': "0.0.0.0",
                             'sslProfile': tcp_listener_ssl_profile_name,
                             'address': cls.address_ssl[0],
                             'port': cls.listener_ports[router_2_id][cls.address_ssl[0]]}),
            ('tcpListener', {'host': "0.0.0.0",
                             'sslProfile': tcp_listener_ssl_profile_name,
                             'address': cls.address_ssl[1],
                             'port': cls.listener_ports[router_2_id][cls.address_ssl[1]]}),
            ('tcpConnector', {'host': "localhost",
                              'sslProfile': 'tcp-connector-ssl-profile',
                              'address': cls.address_ssl[1],
                              'port': echo_servers[cls.address_ssl[1]].port})
        ])

        cls.router_2 = cls.tester.qdrouterd('test_router_2', config_2)
        cls.router_1 = cls.tester.qdrouterd('test_router_1', config_1)

        cls.router_1.wait_router_connected('R2')
        cls.router_2.wait_router_connected('R1')

        cls.snooper_thread = VFlowSnooperThread(cls.router_1.addresses[0])

        # wait for the TCP listeners and connectors
        expected = {
            router_1_id : [
                ('LISTENER', {'VAN_ADDRESS': cls.address_no_ssl[0]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_no_ssl[1]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_ssl[0]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_ssl[1]}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_no_ssl[0]}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_ssl[0]})
            ],
            router_2_id : [
                ('LISTENER', {'VAN_ADDRESS': cls.address_no_ssl[0]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_no_ssl[1]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_ssl[0]}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_ssl[1]}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_no_ssl[1]}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_ssl[1]})
            ]
        }
        success = retry(lambda: cls.snooper_thread.match_records(expected))
        result = cls.snooper_thread.get_results()
        cls.assertTrue(success, f"Failed to match records {result}")

        # vflow ids are necessary to relate flows to tcp listeners
        cls.router_1_vflow_id = None
        cls.router_2_vflow_id = None
        for router_key, router_attrs in result.items():
            for rec in router_attrs:
                if rec['RECORD_TYPE'] == 'ROUTER':
                    if rec['NAME'].endswith(f"/{router_1_id}"):
                        cls.router_1_vflow_id = rec['IDENTITY']
                    elif rec['NAME'].endswith(f"/{router_2_id}"):
                        cls.router_2_vflow_id = rec['IDENTITY']

        # retry() parameters for VanFlowSnooper.match_record() tests where we
        # expect failure. E.g. we try to match the 'END_TIME' attribute in the
        # 'FLOW' record - and expect failure - in order to check that a flow
        # is still acive.
        cls.timeout = 5
        cls.delay = 0.5

    @classmethod
    def tearDownClass(cls):
        # stop echo servers
        for _, server in cls.echo_servers.items():
            server.wait()
        super(TerminateTcpConnectionsTest, cls).tearDownClass()

    def get_tcp_entity_vflow_id(self, router_vflow_id, record_type, address):
        res = self.snooper_thread.get_results()
        try:
            for rec in res[router_vflow_id]:
                if rec['RECORD_TYPE'] == record_type and rec['PROTOCOL'] == 'tcp' and rec['VAN_ADDRESS'] == address:
                    return rec['IDENTITY']
        except KeyError:
            pass
        return None

    def delete_tcp_entity(self, address, entity_type, router):
        def find_entity(address, entity_type, router, expected=True):
            query_cmd = f'QUERY --type={entity_type}'
            output = json.loads(router.qd_manager(query_cmd))
            res = [e for e in output if e['address'] == address]
            if expected:
                self.assertTrue(len(res) == 1)
                return res[0]
            else:
                self.assertTrue(len(res) == 0)
            return None

        e = find_entity(address, entity_type, router)
        name = e['name']
        delete_cmd = 'DELETE --type=' + entity_type + ' --name=' + name
        router.qd_manager(delete_cmd)
        # check that rthe entity has been deleted
        retry_assertion(lambda: find_entity(address, entity_type, router, expected=False),
                        timeout=2, delay=1)

    def create_echo_clients(self, client_prefix, client_port, ssl=False):
        # We use the delay_close flag to keep the connections open
        echo_clients = []
        client_logger = Logger(title=client_prefix,
                               print_to_console=True)

        ssl_info = self.client_ssl_info if ssl else None

        for i in [1, 2]:
            echo_clients.append(TcpEchoClient(client_prefix + '_' + str(i),
                                              host='localhost',
                                              port=client_port,
                                              size=1,
                                              count=1,
                                              logger=client_logger,
                                              ssl_info=ssl_info,
                                              delay_close=True))
        return echo_clients

    def clean_up_echo_clients(self, echo_clients):
        for e in echo_clients:
            e.wait()

    def setup_flows(self, address, ssl):
        """
        Setup tcp flows via the tcpListeners and the tcpConnector which have
        the particular VAN address. Two flows are created for each tcpListener.
        """
        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        # vflow Ids are used to associate flows with tcpListeners and tcpConnectors
        vflow_ids = {}
        self.assertIsNotNone(self.router_1_vflow_id)
        self.assertIsNotNone(self.router_2_vflow_id)
        vflow_ids['listener_1'] = self.get_tcp_entity_vflow_id(self.router_1_vflow_id,
                                                               'LISTENER', address)
        vflow_ids['listener_2'] = self.get_tcp_entity_vflow_id(self.router_2_vflow_id,
                                                               'LISTENER', address)
        self.assertIsNotNone(vflow_ids['listener_1'])
        self.assertIsNotNone(vflow_ids['listener_2'])

        # Create two flows for tcpListener at router_1
        client_prefix = self.test_name + " ECHO_CLIENT_1_" + address
        client_port = self.listener_ports[router_1_id][address]
        echo_clients = self.create_echo_clients(client_prefix, client_port, ssl)

        # Create another two flows for tcpListener at router_2
        client_prefix = self.test_name + " ECHO_CLIENT_2_" + address
        client_port = self.listener_ports[router_2_id][address]
        echo_clients.extend(self.create_echo_clients(client_prefix, client_port, ssl))

        # Check if all  vflows are created
        expected = {
            router_1_id : [
                ('FLOW', {'PARENT': vflow_ids['listener_1']}),
                ('FLOW', {'PARENT': vflow_ids['listener_1']}),
            ],
            router_2_id : [
                ('FLOW', {'PARENT': vflow_ids['listener_2']}),
                ('FLOW', {'PARENT': vflow_ids['listener_2']}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected), delay=1)
        self.assertTrue(success, f"Failed to match records {self.snooper_thread.get_results()}")

        return vflow_ids, echo_clients

    def check_vflows_active(self, router_id, parent_vflow_id, timeout=0):
        """
        Check if flows with a specific parent listener are active (i.e. no END_TIME
        attributes are present)
        """
        expected = {
            router_id: [
                ('FLOW', {'PARENT': parent_vflow_id, 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=timeout, delay=self.delay)
        self.assertFalse(success,
                         f"ParentId {parent_vflow_id} Matched records  {self.snooper_thread.get_results()}")

    def check_vflows_terminated(self, router_id, parent_vflow_id):
        """
        Check if flows with a specific parent listener have terminated
        """
        expected = {
            router_id: [
                ('FLOW', {'PARENT': parent_vflow_id, 'END_TIME': ANY_VALUE}),
                ('FLOW', {'PARENT': parent_vflow_id, 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected))
        self.assertTrue(success,
                        f"ParentId {parent_vflow_id} Matched records {self.snooper_thread.get_results()}")

    def check_all_vflows_active(self, vflow_ids, timeout=0):
        """
        Check if all flows are still active on both routers
        """
        self.check_vflows_active(self.router_1.config.router_id, vflow_ids['listener_1'], timeout=timeout)
        self.check_vflows_active(self.router_2.config.router_id, vflow_ids['listener_2'], timeout=timeout)

    def delete_tcp_entities_conns_terminate(self, address, ssl=False):
        # router_1 has the "dropTcpConnections" config flag turned on
        # This test deletes tcpListener and tcpconnector at router_1
        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        # Create flows from tcpListener:router_1 to tcpConnector:router_1 and
        # from tcpListener:router_2 to tcpConnector:router_1
        vflow_ids, echo_clients = self.setup_flows(address, ssl)
        self.assertTrue(len(echo_clients) == 4)

        # Delete tcpListener_1:router_1
        self.delete_tcp_entity(address, TCP_LISTENER_TYPE, self.router_1)
        # Flows of deleted listener are expected to terminate
        self.check_vflows_terminated(router_1_id, vflow_ids['listener_1'])
        # Flows of tcpListener:router_2 to tcpConnector:router_1 should stay active
        self.check_vflows_active(router_2_id, vflow_ids['listener_2'], timeout=self.timeout)

        # Delete tcpConnector:router_1
        self.delete_tcp_entity(address, TCP_CONNECTOR_TYPE, self.router_1)
        # Flows of tcpListener:router_2 to tcpConnector:router_1 should terminate
        self.check_vflows_terminated(router_2_id, vflow_ids['listener_2'])

        self.clean_up_echo_clients(echo_clients)

    def delete_tcp_entities_conns_active(self, address, ssl=False):
        # router_2 does not have the "dropTcpConnections" config flag turned on
        # This test we deletes tcpListener and tcpconnector at router_2

        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        vflow_ids, echo_clients = self.setup_flows(address, ssl)
        self.assertTrue(len(echo_clients) == 4)
        self.check_all_vflows_active(vflow_ids)

        # Delete tcpListener:router_2
        self.delete_tcp_entity(address, TCP_LISTENER_TYPE, self.router_2)
        # All flows should stay active
        self.check_all_vflows_active(vflow_ids, timeout=self.timeout)

        # Delete tcpConnector:router_2
        self.delete_tcp_entity(address, TCP_CONNECTOR_TYPE, self.router_2)
        # All flows should stay active
        self.check_all_vflows_active(vflow_ids, timeout=self.timeout)

        self.clean_up_echo_clients(echo_clients)

    def test_delete_tcp_entities_conns_terminate(self):
        self.delete_tcp_entities_conns_terminate(self.address_no_ssl[0])

    def test_delete_tcp_entities_conns_active(self):
        self.delete_tcp_entities_conns_active(self.address_no_ssl[1])

    def test_delete_tcp_entities_conns_terminate_ssl(self):
        self.delete_tcp_entities_conns_terminate(self.address_ssl[0], ssl=True)

    def test_delete_tcp_entities_conns_active_ssl(self):
        self.delete_tcp_entities_conns_active(self.address_ssl[1], ssl=True)

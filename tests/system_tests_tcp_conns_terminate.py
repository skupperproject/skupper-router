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
from subprocess import PIPE, STDOUT

from system_test import TestCase, Qdrouterd, retry, retry_assertion
from system_test import Logger, TIMEOUT, TCP_LISTENER_TYPE, TCP_CONNECTOR_TYPE
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE
from TCP_echo_client import TcpEchoClient
from TCP_echo_server import TcpEchoServer


class TerminateTcpConnectionsTest(TestCase):
    """
    Test the config flag 'closeConnectionsOnDelete' for tcpListeners and
    tcpConnectors. The corresponding active tcp flows are expected to terminate
    when a tcpListener or tcpConnector is deleted if the config flag is set.
    Otherwise, the flows should stay alive.
    """

    @classmethod
    def setUpClass(cls):
        """
        Start two routers: R1 and R2. R2 has two tcpConnetors with two TCP echo
        server attached. Both router also  has two tcpListeners. One of the
        tcpListener is created with the config flag  'closeConnectionsOnDelete'
        is set for each router. The two tcpConnectors  follow the same
        configuration: the config flag is set for one while it is unset for the
        other. The tcpListeners and the tcpConnector with the config flag set
        share the same VAN address. Similarly, the ones which does
        not have the flag set also share the same VAN address. A VanFlowSnooper
        thread is created to check the state of flows during the tests.
        """
        super(TerminateTcpConnectionsTest, cls).setUpClass()

        cls.test_name = 'TerminateTcpConnectionsTest'

        # Launch TCP echo server
        server_logger = Logger(title=cls.test_name,
                               print_to_console=True,
                               save_for_dump=False,
                               ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                      f"{cls.test_name}_echo_server.log"))
        server_prefix = f"{cls.test_name} ECHO_SERVER_1"
        echo_server_1 = TcpEchoServer(prefix=server_prefix,
                                      port=0,
                                      logger=server_logger)
        assert echo_server_1.is_running

        server_prefix = f"{cls.test_name} ECHO_SERVER_2"
        echo_server_2 = TcpEchoServer(prefix=server_prefix,
                                      port=0,
                                      logger=server_logger)
        assert echo_server_2.is_running

        router_1_id = 'R1'
        router_2_id = 'R2'

        # VAN address to test config flag 'closeConnectionsOnDelete' is set
        cls.address_terminate = cls.test_name + '_terminate'
        # VAN address to test config flag 'closeConnectionsOnDelete' is unset (the default behavour)
        cls.address_default = cls.test_name + '_default'

        # Create listener ports, 1 port for each VAN address for each router
        cls.listener_ports = dict()
        for r in [router_1_id, router_2_id]:
            cls.listener_ports[r] = {
                cls.address_default: cls.tester.get_port(),
                cls.address_terminate: cls.tester.get_port(),
            }

        # Launch routers
        inter_router_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_1_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_ports[router_1_id][cls.address_default],
                             'address': cls.address_default}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_ports[router_1_id][cls.address_terminate],
                             'address': cls.address_terminate,
                             "closeConnectionsOnDelete": True}),

        ])
        config_2 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_2_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port}),
            ('tcpConnector', {'host': "localhost",
                              'port': echo_server_1.port,
                              'address': cls.address_default}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_ports[router_2_id][cls.address_default],
                             'address': cls.address_default}),
            ('tcpConnector', {'host': "localhost",
                              'port': echo_server_2.port,
                              'address': cls.address_terminate,
                              "closeConnectionsOnDelete": True}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_ports[router_2_id][cls.address_terminate],
                             'address': cls.address_terminate,
                             "closeConnectionsOnDelete": True}),
        ])

        cls.router_2 = cls.tester.qdrouterd('test_router_2', config_2, wait=True)
        cls.router_1 = cls.tester.qdrouterd('test_router_1', config_1, wait=True)

        cls.router_1.wait_router_connected('R2')
        cls.router_2.wait_router_connected('R1')

        cls.snooper_thread = VFlowSnooperThread(cls.router_1.addresses[0])

        # wait for the TCP listeners and connectors
        expected = {
            router_1_id : [
                ('LISTENER', {'VAN_ADDRESS': cls.address_terminate}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_default})
            ],
            router_2_id : [
                ('LISTENER', {'VAN_ADDRESS': cls.address_terminate}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_terminate}),
                ('LISTENER', {'VAN_ADDRESS': cls.address_default}),
                ('CONNECTOR', {'VAN_ADDRESS': cls.address_default})
            ]
        }
        success = retry(lambda: cls.snooper_thread.match_records(expected))
        result = cls.snooper_thread.get_results()
        cls.assertTrue(success, f"Failed to match records {result}")

        # vflow ids are necessary to relate flows to tcp listeners and connectors
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
        cls.timeout = 1
        cls.delay = 0.5

    def get_tcp_entity_vflow_id(self, router_vflow_id, record_type, address):
        res = self.snooper_thread.get_results()
        try:
            for rec in res[router_vflow_id]:
                if rec['RECORD_TYPE'] == record_type and rec['PROTOCOL'] == 'tcp' and rec['VAN_ADDRESS'] == address:
                    return rec['IDENTITY']
        except KeyError:
            pass
        return None

    def run_skmanage(self, cmd, router):
        p = self.popen(
            ['skmanage'] + cmd.split(' ') + ['--bus', router.addresses[0],
                                             '--indent=-1', '--timeout', str(TIMEOUT)],
            stdin=PIPE, stdout=PIPE, stderr=STDOUT, universal_newlines=True)
        out = p.communicate()[0]
        try:
            p.teardown()
        except Exception as e:
            raise Exception(out if out else str(e))
        return out

    def delete_tcp_entity(self, address, entity_type, router):
        def find_entity(address, entity_type, router, expected=True):
            query_cmd = f'QUERY --type={entity_type}'
            output = json.loads(self.run_skmanage(query_cmd, router))
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
        self.run_skmanage(delete_cmd, router)
        # check that rthe entity has been deleted
        retry_assertion(lambda: find_entity(address, entity_type, router, expected=False),
                        timeout=2, delay=1)

    def create_echo_clients(self, client_prefix, client_port):
        # We use the delay_close flag to keep the connections open
        echo_clients = []
        client_logger = Logger(title=client_prefix,
                               print_to_console=True)
        for i in [1, 2]:
            echo_clients.append(TcpEchoClient(client_prefix + '_' + str(i),
                                              host='localhost',
                                              port=client_port,
                                              size=1,
                                              count=1,
                                              logger=client_logger,
                                              delay_close=True))
        return echo_clients

    def clean_up_echo_clients(self, echo_clients):
        for e in echo_clients:
            e.wait()

    def setup_flows(self, address):
        """
        Setup tcp flows via the tcpListeners and the tcpConnector which have
        the particular VAN address. Two flows are created for each tcpListener.
        """
        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        # vflow Ids are used to associate flows with tcpListeners and tcpConnectors
        vflow_ids = {}
        self.assertTrue(self.router_1_vflow_id is not None)
        self.assertTrue(self.router_2_vflow_id is not None)
        vflow_ids['listener_1'] = self.get_tcp_entity_vflow_id(self.router_1_vflow_id,
                                                               'LISTENER', address)
        vflow_ids['listener_2'] = self.get_tcp_entity_vflow_id(self.router_2_vflow_id,
                                                               'LISTENER', address)
        self.assertTrue(vflow_ids['listener_1'] is not None)
        self.assertTrue(vflow_ids['listener_2'] is not None)

        # Create two flows from router_1:tcpListener to router_2:tcpConnector
        client_prefix = self.test_name + " ECHO_CLIENT"
        client_port = self.listener_ports[router_1_id][address]
        echo_clients = self.create_echo_clients(client_prefix, client_port)

        # Create another two flows from router_2:tcpListener to router_2:tcpConnector
        client_port = self.listener_ports[router_2_id][address]
        echo_clients.extend(self.create_echo_clients(client_prefix, client_port))

        # Check if all  vflows are created
        expected = {
            router_1_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_1']}),
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_1']}),
            ],
            router_2_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2']}),
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2']}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected), delay=1)
        self.assertTrue(success, f"Failed to match records {self.snooper_thread.get_results()}")

        return vflow_ids, echo_clients

    def check_all_vflows_active(self, vflow_ids, timeout=0):
        """
        Check if all flows are still active on both routers (i.e. no END_TIME
        attributes are present)
        """
        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        expected = {
            router_1_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_1'], 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=timeout, delay=self.delay)
        self.assertFalse(success, f"Matched records  {self.snooper_thread.get_results()}")
        expected = {
            router_2_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2'], 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=timeout, delay=self.delay)
        self.assertFalse(success, f"Matched records  {self.snooper_thread.get_results()}")

    def check_listener_1_vflows_terminated(self, vflow_ids):
        """
        Check if flows from router_1:tcpListener to router_2:tcpConnector are terminated
        """

        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        expected = {
            router_1_id: [
                ('BIFLOW_TPORT', {'END_TIME': ANY_VALUE, 'PARENT': vflow_ids['listener_1']}),
                ('BIFLOW_TPORT', {'END_TIME': ANY_VALUE, 'PARENT': vflow_ids['listener_1']})
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=self.timeout, delay=self.delay)
        self.assertTrue(success, f"Failed to match records {self.snooper_thread.get_results()}")

    def check_listener_2_vflows_active(self, vflow_ids):
        """
        Check if flows from router_2:tcpListener to router_2:tcpConnector are active
        """
        router_2_id = self.router_2.config.router_id

        expected = {
            router_2_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2'], 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=self.timeout, delay=self.delay)
        self.assertFalse(success, f"Matched records  {self.snooper_thread.get_results()}")

    def check_all_vflows_terminated(self, vflow_ids):
        """
        Check if all four flows are terminated
        """
        router_1_id = self.router_2.config.router_id
        router_2_id = self.router_2.config.router_id

        expected = {
            router_1_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_1'], 'END_TIME': ANY_VALUE}),
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_1'], 'END_TIME': ANY_VALUE}),
            ],
            router_2_id : [
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2'], 'END_TIME': ANY_VALUE}),
                ('BIFLOW_TPORT', {'PARENT': vflow_ids['listener_2'], 'END_TIME': ANY_VALUE}),
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected),
                        timeout=self.timeout, delay=self.delay)
        self.assertTrue(success, f"Failed to match records {self.snooper_thread.get_results()}")

    def test_delete_tcp_entities_without_terminate_conns(self):
        vflow_ids, echo_clients = self.setup_flows(self.address_default)
        self.assertTrue(len(echo_clients) == 4)
        self.check_all_vflows_active(vflow_ids)

        # Delete router_1:tcpListener.
        self.delete_tcp_entity(self.address_default, TCP_LISTENER_TYPE, self.router_1)
        # All flows should be still actve (i.e. default behaviour)
        self.check_all_vflows_active(vflow_ids, timeout=self.timeout)

        # Delete tcpConnector
        self.delete_tcp_entity(self.address_default, TCP_CONNECTOR_TYPE, self.router_2)
        # All flows should be still actve (i.e. default behaviour)
        self.check_all_vflows_active(vflow_ids, timeout=self.timeout)

        self.clean_up_echo_clients(echo_clients)

    def test_delete_tcp_entities_with_terminate_conns(self):
        vflow_ids, echo_clients = self.setup_flows(self.address_terminate)
        self.assertTrue(len(echo_clients) == 4)
        self.check_all_vflows_active(vflow_ids)

        # Delete router_1:tcpListener.
        self.delete_tcp_entity(self.address_terminate, TCP_LISTENER_TYPE, self.router_1)
        # flows from router_1:tcpListener to router_2:tcpConnector must be terminated
        self.check_listener_1_vflows_terminated(vflow_ids)

        # flows from router_2:tcpListener to router_2:tcpConector must be still active
        self.check_listener_2_vflows_active(vflow_ids)

        # Delete tcpConnector
        self.delete_tcp_entity(self.address_terminate, TCP_CONNECTOR_TYPE, self.router_2)
        # all flows must be terminated
        self.check_all_vflows_terminated(vflow_ids)

        self.clean_up_echo_clients(echo_clients)

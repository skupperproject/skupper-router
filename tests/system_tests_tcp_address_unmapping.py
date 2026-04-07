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
from system_test import Logger
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE
from TCP_echo_client import TcpEchoClient
from TCP_echo_server import TcpEchoServer


class MultiAddressListenerTest(TestCase):
    """
    Test the unmapping of mobile addresses when a node gets unreachable. Three
    routers are connected in a mesh. One router has a multi-key listener with
    three addresses. There is a TCP connector for each address. Each router
    has a tcp connector with one of the addresses. The multi-key listener uses
    priority strategy. The test removes inter-router links one after the other
    and checks if the multi-key listener assign new tcp connections to a still
    reachable address with the highest priority.
    """

    @classmethod
    def findNewFlowId(cls, entity_type, entity_name, van_address=None, sources=None):
        def findId(sources):
            flow_id = None
            for _, records in sources.items():
                for rec in records:
                    if (rec['RECORD_TYPE'] == entity_type and rec['NAME'] == entity_name and rec['VAN_ADDRESS'] == van_address and rec['FLOW_COUNT_L4'] == 0):
                        return rec['IDENTITY']
            return None

        def findIdRetry():
            sources = cls.snooper_thread.get_results()
            return findId(sources)

        if sources is not None:
            return findId(sources)
        else:
            return retry(findIdRetry)

    @classmethod
    def setUpClass(cls):
        super(MultiAddressListenerTest, cls).setUpClass()

        cls.test_name = 'MultiAddressListenerTest'

        router_a_id = 'RouterA'
        router_b_id = 'RouterB'
        router_c_id = 'RouterC'

        # VAN addresses
        cls.van_address = [cls.test_name + '_addr_1',
                           cls.test_name + '_addr_2',
                           cls.test_name + '_addr_3']
        # listener addresses
        cls.listener_address_name =    ['addr1', 'addr2', 'addr3']
        cls.listener_address_value =   ['1',     '2',     '3']
        # Listener
        cls.listener_name = 'listener_multi_1'
        cls.listener_port = cls.tester.get_port()
        # Connectors
        cls.connector_name = ['connector_1', 'connector_2', 'connector_3']

        # Launch TCP echo servers
        server_logger = Logger(title=cls.test_name,
                               print_to_console=True,
                               save_for_dump=False,
                               ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                      f"{cls.test_name}_echo_server.log"))
        echo_servers = {}
        for i in range(3):
            server_prefix = f"{cls.test_name} ECHO_SERVER_addr_{i + 1}"
            echo_servers[cls.van_address[i]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger)
            assert echo_servers[cls.van_address[i]].is_running

        cls.echo_servers = echo_servers

        # listener config
        cls.listener_config = ('tcpListener', {'host': "0.0.0.0",
                                               'port': cls.listener_port,
                                               'multiAddressStrategy': "priority",
                                               'name': cls.listener_name})

        # listener address and tcp connector configs
        cls.listener_address_config = []
        cls.connector_config = []
        for i in range(3):
            cls.listener_address_config.append(
                ('listenerAddress', {'name': cls.listener_address_name[i],
                                     'value': cls.listener_address_value[i],
                                     'address': cls.van_address[i],
                                     'listener': cls.listener_name}))
            cls.connector_config.append(
                ('tcpConnector', {'host': "localhost",
                                  'address': cls.van_address[i],
                                  'port': echo_servers[cls.van_address[i]].port,
                                  'name': cls.connector_name[i]}))

        # Names of inter-router connectors
        cls.inter_router_conn_name_a_b = "inter-conn-a-b"
        cls.inter_router_conn_name_a_c = "inter-conn-a-c"
        cls.inter_router_conn_name_b_c = "inter-conn-b-c"

        # Link state max age
        cls.remoteLsMaxAgeSeconds = 60

        # Launch routers
        inter_router_port_b = cls.tester.get_port()
        inter_router_port_c = cls.tester.get_port()
        config_a = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_a_id, 'remoteLsMaxAgeSeconds': cls.remoteLsMaxAgeSeconds}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port_b, 'name': cls.inter_router_conn_name_a_b}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port_c, 'name': cls.inter_router_conn_name_a_c}),
            cls.listener_config,
            cls.listener_address_config[0],
            cls.listener_address_config[1],
            cls.listener_address_config[2],
            cls.connector_config[0]
        ])
        config_b = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_b_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port_c, 'name': cls.inter_router_conn_name_b_c}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port_b}),
            cls.connector_config[1]
        ])
        config_c = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_c_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port_c}),
            cls.connector_config[2]
        ])

        cls.router_c = cls.tester.qdrouterd('test_router_b', config_c)
        cls.router_b = cls.tester.qdrouterd('test_router_b', config_b)
        cls.router_a = cls.tester.qdrouterd('test_router_a', config_a)

        cls.router_b.wait_router_connected(router_c_id)
        cls.router_b.wait_router_connected(router_a_id)
        cls.router_c.wait_router_connected(router_a_id)

        cls.snooper_thread = VFlowSnooperThread(cls.router_a.addresses[0])

        # Wait for the TCP connectors and listeners
        expected = {
            router_a_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[0], 'VAN_ADDRESS': cls.van_address[0]},
                 'LISTENER', {'NAME': cls.listener_name, 'VAN_ADDRESS': cls.van_address[0]},
                 'LISTENER', {'NAME': cls.listener_name, 'VAN_ADDRESS': cls.van_address[1]},
                 'LISTENER', {'NAME': cls.listener_name, 'VAN_ADDRESS': cls.van_address[2]}
                 )
            ],
            router_b_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[1], 'VAN_ADDRESS': cls.van_address[1]})
            ],
            router_c_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[2], 'VAN_ADDRESS': cls.van_address[2]})
            ]
        }

        # Save the status of this check to avoid calling assert() in the ctor
        cls.tcp_entities_ready = retry(lambda: cls.snooper_thread.match_records(expected))
        vflow_records = cls.snooper_thread.get_results()

        if cls.tcp_entities_ready:
            # vflow ids are necessary to relate flows to tcp listeners and connectors
            # Note that a multi-address listener create as many vflow records of type LISTENER as many address it has.
            # All those LISTENER records share the same name.
            cls.connector_vflow_id = {}
            cls.listener_vflow_id = {}
            for i, aname in enumerate(cls.listener_address_name):
                cls.listener_vflow_id[aname] = cls.findNewFlowId('LISTENER', cls.listener_name, cls.van_address[i], vflow_records)
            for i, cname in enumerate(cls.connector_name):
                cls.connector_vflow_id[cname] = cls.findNewFlowId('CONNECTOR', cname, cls.van_address[i], vflow_records)
        cls.setup_vflow_records = vflow_records

        # BIFLOW_TPORT records match history. It is needed to match new instances of flow records referencing the same
        # listener and connector vflow ids.
        cls.matched_biflow_tport_records = {}

        # Retry attempt delay for the EchoClient
        cls.echo_client_retry_delay = 0.5

        # Generic info messages
        cls.logger = Logger(title=cls.test_name, print_to_console=True)

    @classmethod
    def tearDownClass(cls):
        # stop echo servers
        for _, server in cls.echo_servers.items():
            server.wait()
        super(MultiAddressListenerTest, cls).tearDownClass()

    def check_setup_status(self):
        self.assertTrue(self.tcp_entities_ready, f"Match failed: {self.setup_vflow_records}")
        for i, aname in enumerate(self.listener_address_name):
            self.assertIsNotNone(self.listener_vflow_id[aname])
        for cname in self.connector_name:
            self.assertIsNotNone(self.connector_vflow_id[cname])

    def find_entity(self, entity_type, entity_name, router, expected=True):
        query_cmd = f'QUERY --type={entity_type}'
        output = json.loads(router.sk_manager(query_cmd))
        res = [e for e in output if e['name'] == entity_name]
        if expected:
            self.assertTrue(len(res) == 1)
            return res[0]
        else:
            self.assertTrue(len(res) == 0)
        return None

    def delete_entity(self, entity_type, entity_name, router):
        delete_cmd = 'DELETE --type=' + entity_type + ' --name=' + entity_name
        router.sk_manager(delete_cmd)
        # check that the entity has been deleted
        retry_assertion(lambda: self.find_entity(entity_type, entity_name, router, expected=False), delay=0.5)

    def create_echo_client(self, client_prefix, client_port, num_clients=1, verbose_log=True):
        # There may be a delay before address watch marks a listener address
        # unreachable. We use the retry() function to re-launch echo client
        # when the client gets stuck trying to send via the tcpConnector
        # already unreachable. This manifests as a "server closed" exception
        # when we call the client wait() function.
        client_logger = Logger(title=client_prefix,
                               print_to_console=verbose_log)
        num_failures = 0

        def echo_client():
            nonlocal num_failures
            try:
                client_logger = Logger(title=client_prefix + '_attempt_' + str(num_failures),
                                       print_to_console=verbose_log)
                client = TcpEchoClient(client_prefix,
                                       host='localhost',
                                       port=client_port,
                                       size=1,
                                       count=1,
                                       logger=client_logger)
                client.wait()
            except Exception as e:
                if "server closed" in str(e):
                    num_failures += 1
                    return False
                raise
            return True

        for i in range(num_clients):
            retry(echo_client, delay=self.echo_client_retry_delay)

        return num_failures

    def check_biflow_tport_vflow_record(self, router_id, listener_vflow_id, connector_vflow_id):
        """
        Check if BIFLOW_TPORT vflow record exists for the specific listener and connector
        """

        # Add the new expected record description to the ones that have been matched already (in previous calls
        # to this function). This is needed to match the exact number of BIFLOW_TPORT records referencing the same
        # listener and connector vflow ids.
        expected = self.matched_biflow_tport_records
        if expected.get(router_id) is None:
            expected[router_id] = []

        expected[router_id].append(('BIFLOW_TPORT', {'PARENT': listener_vflow_id, 'END_TIME': ANY_VALUE, 'CONNECTOR': connector_vflow_id}))
        success = retry(lambda: self.snooper_thread.match_records(expected))
        self.assertTrue(success,
                        f"Listener vflowId {listener_vflow_id} Matched records {self.snooper_thread.get_results()}")

    def check_flow(self, flow_index, echo_client_name, max_client_retries=0):
        client_prefix = self.test_name + echo_client_name +  self.van_address[flow_index]
        num_failures = self.create_echo_client(client_prefix, self.listener_port)
        connector_vflow_id = self.connector_vflow_id[self.connector_name[flow_index]]
        listener_vflow_id = self.listener_vflow_id[self.listener_address_name[flow_index]]
        router_a_id = self.router_a.config.router_id
        self.check_biflow_tport_vflow_record(router_a_id, listener_vflow_id, connector_vflow_id)
        self.assertLessEqual(num_failures, max_client_retries, "Too many echo client failures")

    def test_address_unmap(self):
        '''
         B --> A <-- C
         ^___________|

        - Router A has a multi-key listener configured with three addresses
        - Router C has a tcp connector for addr3 with priority 3
        - Router B has a tcp connector for addr2 with priority 2
        - Router A has a tcp connector for addr1 with priority 1

        The test checks if new TCP connections targets a still reachable
        connector with the highest priority while inter-router links gets deleted
        one by one.
        Note that mobile address unmapping used to happen before only when the
        the corresponding node got deleted due to link state expiration (~60s
        by default). This test checks that address unmapping happens right away
        when the node gets unreachable.
        '''

        self.check_setup_status()  # throw here if something failed in setUp()

        router_a_id = self.router_a.config.router_id
        router_b_id = self.router_b.config.router_id
        router_c_id = self.router_c.config.router_id

        # Max number of echo client retry attempts when checking if tcp flows
        # targets a new address, after the current one got unreachable. We want
        # to check that the address unmapping happens before the link state
        # expires. As a rough estimate, we check if it happens sooner than half
        # of the link state expiration period.
        max_client_retries =   (self.remoteLsMaxAgeSeconds  // self.echo_client_retry_delay) // 2

        # addr3:(prio=3, reachable:True)
        # addr2:(prio=2, reachable:True)
        # addr1:(prio=1, reachable:True)
        # check flow: listener addr3 -> connector_3 (flow index 2)
        flow_index = 2
        client_name = "ECHO_CLIENT_1_"
        self.check_flow(flow_index, client_name)

        # Delete link C -> A. addr3 is still reachable via B
        self.delete_entity('connector', self.inter_router_conn_name_a_c, self.router_a)
        # # addr3:(prio=3, reachable:True)
        # addr2:(prio=2, reachable:True)
        # addr1:(prio=1, reachable:True)
        # check flow: listener addr3 -> connector_3 (flow index 2)
        flow_index = 2
        client_name = "ECHO_CLIENT_2_"
        self.check_flow(flow_index, client_name, max_client_retries)

        # Delete link C -> B. addr3 is no longer reachable
        self.delete_entity('connector', self.inter_router_conn_name_b_c, self.router_b)
        # # addr3:(prio=3, reachable:False)
        # addr2:(prio=2, reachable:True)
        # addr1:(prio=1, reachable:True)
        # check flow: listener addr2 -> connector_2 (flow index 1)
        flow_index = 1
        client_name = "ECHO_CLIENT_3_"
        self.check_flow(flow_index, client_name, max_client_retries)

        # Delete link B -> A. addr2 and addr3 is no longer reachable
        self.delete_entity('connector', self.inter_router_conn_name_a_b, self.router_a)
        # # addr3:(prio=3, reachable:False)
        # addr2:(prio=2, reachable:False)
        # addr1:(prio=1, reachable:True)
        # check flow: listener addr1 -> connector_1 (flow index 0)
        flow_index = 0
        client_name = "ECHO_CLIENT_4_"
        self.check_flow(flow_index, client_name, max_client_retries)

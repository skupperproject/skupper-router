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
import shutil
import subprocess

from system_test import TestCase, Qdrouterd, retry, retry_assertion
from system_test import Logger
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE
from TCP_echo_client import TcpEchoClient
from TCP_echo_server import TcpEchoServer


class MultiAddressListenerTest(TestCase):
    """
    Test for the multi address listener. Two routers are run. The first one has a multi address listener
    configured at startup. Four distinct service addresses are created for the listener. Each address targets a
    separate tcpConnector which in turn connects to a separate echo server. The test uses a vanflow snooper thread
    to check if the correct connection flow got exercised under the specific configuration. Possible flows are
    as follows.
    addr1 -> connector_1
    addr2 -> connector_2
    addr3 -? connector_3
    addr4 -? connector_4
    The test create and delete listener addresses and tcpConnecters. It launches an echo client after each
    configuration change to check if new tcp connections target the expected tcpConnecter.
    """

    @classmethod
    def findNewFlowId(cls, entity_type, entity_name, van_address=None, sources=None):
        def findIdOnce(sources):
            flow_id = None
            for _, records in sources.items():
                for rec in records:
                    if (rec['RECORD_TYPE'] == entity_type and rec['NAME'] == entity_name and rec['VAN_ADDRESS'] == van_address and rec['FLOW_COUNT_L4'] == 0):
                        return rec['IDENTITY']
            return None

        def findIdRepeat():
            sources = cls.snooper_thread.get_results()
            return findIdOnce(sources)

        if sources is not None:
            return findIdOnce(sources)
        else:
            return retry(findIdRepeat)

    @classmethod
    def setUpClass(cls):
        """
        Start two routers: R1 and R2. Both have two tcpConnectors with two TCP echo
        server attached. Both router also  has four tcpListeners. One of the
        tcpConnectors and two of the tcpListeners are using SSL at both routers.
        The new config flag is tuned on for R1 but not for R2.
        Each tcpConnector has a unique VAN address which is also used by one tcpListener
        at each router. The four distinct VAN addresses are used to test deletion of a
        tcpListeners and tcpConnectors with ot without using SSL.
        """
        super(MultiAddressListenerTest, cls).setUpClass()

        cls.test_name = 'MultiAddressListenerTest'

        router_1_id = 'R1'
        router_2_id = 'R2'

        # VAN addresses
        cls.van_address = [cls.test_name + '_addr_1',
                           cls.test_name + '_addr_2',
                           cls.test_name + '_addr_3',
                           cls.test_name + '_addr_4']
        # listener addresses
        cls.listener_address_name =     ['addr1', 'addr2', 'addr3', 'addr4']
        cls.listener_address_priority = ['10',    '20',    '5',     '15']
        cls.listener_address_startup =  [True,    True,    False,   False]
        # Listener
        cls.listener_name = 'listener_multi_1'
        cls.listener_port = cls.tester.get_port()
        # Connectors
        cls.connector_name = ['connector_1', 'connector_2', 'connector_3', 'connector_4']

        # Launch TCP echo servers
        server_logger = Logger(title=cls.test_name,
                               print_to_console=True,
                               save_for_dump=False,
                               ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                      f"{cls.test_name}_echo_server.log"))
        echo_servers = {}
        server_prefix = f"{cls.test_name} ECHO_SERVER_addr_1"
        echo_servers[cls.van_address[0]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger)
        assert echo_servers[cls.van_address[0]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_addr_2"
        echo_servers[cls.van_address[1]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger)
        assert echo_servers[cls.van_address[1]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_addr_3"
        echo_servers[cls.van_address[2]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger)
        assert echo_servers[cls.van_address[2]].is_running
        server_prefix = f"{cls.test_name} ECHO_SERVER_addr_4"
        echo_servers[cls.van_address[3]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger)
        assert echo_servers[cls.van_address[3]].is_running

        cls.echo_servers = echo_servers

        # listener config
        cls.listener_config = [
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_port,
                             'multiAddressStrategy': "priority",
                             'name': cls.listener_name})]

        # listener address config
        cls.listener_address_config = [
            ('listenerAddress', {'name': cls.listener_address_name[0],
                                 'value': cls.listener_address_priority[0],
                                 'address': cls.van_address[0],
                                 'listener': cls.listener_name}),
            ('listenerAddress', {'name': cls.listener_address_name[1],
                                 'value': cls.listener_address_priority[1],
                                 'address': cls.van_address[1],
                                 'listener': cls.listener_name}),
            ('listenerAddress', {'name': cls.listener_address_name[2],
                                 'value': cls.listener_address_priority[2],
                                 'address': cls.van_address[2],
                                 'listener': cls.listener_name}),
            ('listenerAddress', {'name': cls.listener_address_name[3],
                                 'value': cls.listener_address_priority[3],
                                 'address': cls.van_address[3],
                                 'listener': cls.listener_name})]

        # tcp connector configs
        cls.connector_config = [
            ('tcpConnector', {'host': "localhost",
                              'address': cls.van_address[0],
                              'port': echo_servers[cls.van_address[0]].port,
                              'name': cls.connector_name[0]}),
            ('tcpConnector', {'host': "localhost",
                              'address': cls.van_address[1],
                              'port': echo_servers[cls.van_address[1]].port,
                              'name': cls.connector_name[1]}),
            ('tcpConnector', {'host': "localhost",
                              'address': cls.van_address[2],
                              'port': echo_servers[cls.van_address[2]].port,
                              'name': cls.connector_name[2]}),
            ('tcpConnector', {'host': "localhost",
                              'address': cls.van_address[3],
                              'port': echo_servers[cls.van_address[3]].port,
                              'name': cls.connector_name[3]})
        ]

        # Launch routers
        inter_router_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_1_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port}),
            cls.listener_config[0],
            cls.listener_address_config[0],
            cls.listener_address_config[1],
            cls.connector_config[0]
        ])
        config_2 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_2_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port}),
            cls.connector_config[1],
            cls.connector_config[2],
            cls.connector_config[3]
        ])

        cls.router_2 = cls.tester.qdrouterd('test_router_2', config_2)
        cls.router_1 = cls.tester.qdrouterd('test_router_1', config_1)

        cls.router_1.wait_router_connected('R2')
        cls.router_2.wait_router_connected('R1')

        cls.snooper_thread = VFlowSnooperThread(cls.router_1.addresses[0])

        # Wait for the TCP connectors and listeners
        expected = {
            router_1_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[0], 'VAN_ADDRESS': cls.van_address[0]},
                 'LISTENER', {'NAME': cls.listener_name, 'VAN_ADDRESS': cls.van_address[0]},
                 'LISTENER', {'NAME': cls.listener_name, 'VAN_ADDRESS': cls.van_address[1]})
            ],
            router_2_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[1], 'VAN_ADDRESS': cls.van_address[1]}),
                ('CONNECTOR', {'NAME': cls.connector_name[2], 'VAN_ADDRESS': cls.van_address[2]}),
                ('CONNECTOR', {'NAME': cls.connector_name[3], 'VAN_ADDRESS': cls.van_address[3]})
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
                if cls.listener_address_startup[i]:
                    cls.listener_vflow_id[aname] = cls.findNewFlowId('LISTENER', cls.listener_name, cls.van_address[i], vflow_records)
            for i, cname in enumerate(cls.connector_name):
                cls.connector_vflow_id[cname] = cls.findNewFlowId('CONNECTOR', cname, cls.van_address[i], vflow_records)
        cls.setup_vflow_records = vflow_records

        # L4 flow counts for listener and connector vflow records
        cls.flow_count_l4 = {}
        # BIFLOW_TPORT records match history. It is needed to match new instances of flow records referencing the same
        # listener and connector vflow ids.
        cls.matched_biflow_tport_records = {}

        # Generix info messages
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
            if self.listener_address_startup[i]:
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

    def create_entity(self, entity_name, entity_config, router):
        entity_type = entity_config[0]
        create_cmd = f"CREATE --type={entity_type}"
        for k, v in entity_config[1].items():
            create_cmd += f" {k}={v}"
        router.sk_manager(create_cmd)
        # check that the entity has been created
        retry_assertion(lambda: self.find_entity(entity_type, entity_name, router))

    def is_listener_port_closed(self):
        cmd = shutil.which("lsof")
        if cmd is None:
            self.logger.log("(INFO) Skipping listening port check because 'lsof' command is not available")
            return None

        def check_port():
            try:
                res = subprocess.run([cmd, "-nP", f"-iTCP:{self.listener_port}", "-sTCP:LISTEN"],
                                     stdout=subprocess.DEVNULL, check=True)
            except subprocess.CalledProcessError:
                return True
            return res.returncode != 0

        return retry(check_port, delay=0.1)

    def create_echo_client(self, client_prefix, client_port):
        # There may be a delay before address watch marks a listener address unreachable
        # when the corresponding tcpConnector is deleted. We use the retry() function to re-launch
        # echo client when the client gets stuck trying to send via the tcpConnector already
        # closed. This manifest as a "server closed" exception when we call the client wait()
        # function.
        client_logger = Logger(title=client_prefix,
                               print_to_console=True)

        def echo_client():
            try:
                client = TcpEchoClient(client_prefix,
                                       host='localhost',
                                       port=client_port,
                                       size=1,
                                       count=1,
                                       logger=client_logger,
                                       delay_close=True)
                client.wait()
            except Exception as e:
                if "server closed" in str(e):
                    return False
                raise
            return True

        retry(echo_client, delay=0.2)

    def check_biflow_tport_vflow_record(self, router_id, listener_vflow_id, connector_vflow_id):
        """
        Check if BIFLOW_TPORT vflow record for the specific listener and connector
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
        # increase L4 flow counts
        for vflow_id in [listener_vflow_id, connector_vflow_id]:
            if self.flow_count_l4.get(vflow_id) is not None:
                self.flow_count_l4[vflow_id] += 1
            else:
                self.flow_count_l4[vflow_id] = 1

    def check_entity_vflow_record_end(self, router_id, record_type, entity_vflow_id, flow_count_l4):
        """
        Check if the end vflow record exists for the specific entity
        """
        expected = {
            router_id: [
                (record_type, {'IDENTITY': entity_vflow_id, 'END_TIME': ANY_VALUE, 'FLOW_COUNT_L4': flow_count_l4})
            ]
        }
        success = retry(lambda: self.snooper_thread.match_records(expected))
        self.assertTrue(success,
                        f"Router vflowId {router_id} entity vflowId {entity_vflow_id} RECORDS: {self.snooper_thread.get_results()}")

    def check_flow(self, flow_index, echo_client_name):
        client_prefix = self.test_name + echo_client_name +  self.van_address[flow_index]
        self.create_echo_client(client_prefix, self.listener_port)
        connector_vflow_id = self.connector_vflow_id[self.connector_name[flow_index]]
        listener_vflow_id = self.listener_vflow_id[self.listener_address_name[flow_index]]
        router_1_id = self.router_1.config.router_id
        self.check_biflow_tport_vflow_record(router_1_id, listener_vflow_id, connector_vflow_id)

    def test_multi_address_listener(self):
        self.check_setup_status()  # throw here if something failed in setUp()

        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        # addr2:(prio=20, reachable:True)
        # addr1:(prio=10, reachable:True)
        # check flow: listener addr2 -> connector_2 (flow index 1)
        flow_index = 1
        client_name = "ECHO_CLIENT_1_"
        self.check_flow(flow_index, client_name)

        # delete addr2
        self.delete_entity('listenerAddress', self.listener_address_name[1], self.router_1)
        # addr1:(prio=10, reachable:True)
        # check flow: listener addr1 -> connector_1 (flow index 0)
        flow_index = 0
        client_name = "ECHO_CLIENT_2_"
        self.check_flow(flow_index, client_name)

        # delete addr1
        self.delete_entity('listenerAddress', self.listener_address_name[0], self.router_1)
        # no listener address
        # check that listener socket is closed
        ret = self.is_listener_port_closed()
        if ret is not None:
            self.assertTrue(ret)

        # re-create addr1
        self.create_entity(self.listener_address_name[0], self.listener_address_config[0], self.router_1)
        self.listener_vflow_id[self.listener_address_name[0]] = self.findNewFlowId('LISTENER',
                                                                                   self.listener_name,
                                                                                   self.van_address[0])
        self.assertIsNotNone(self.listener_vflow_id[self.listener_address_name[0]])
        # addr1:(prio=10, reachable:True)
        # check flow: listener addr1 -> connector_1 (flow index 0)
        flow_index = 0
        client_name = "ECHO_CLIENT_3_"
        self.check_flow(flow_index, client_name)

        # re-create addr2
        self.create_entity(self.listener_address_name[1], self.listener_address_config[1], self.router_1)
        self.listener_vflow_id[self.listener_address_name[1]] = self.findNewFlowId('LISTENER',
                                                                                   self.listener_name,
                                                                                   self.van_address[1])
        self.assertIsNotNone(self.listener_vflow_id[self.listener_address_name[1]])
        # addr2:(prio=20, reachable:True)
        # addr1:(prio=10, reachable:True)
        # check flow: listener addr2 -> connector_2 (flow index 1)
        flow_index = 1
        client_name = "ECHO_CLIENT_4_"
        self.check_flow(flow_index, client_name)

        # create addr3
        self.create_entity(self.listener_address_name[2], self.listener_address_config[2], self.router_1)
        self.listener_vflow_id[self.listener_address_name[2]] = self.findNewFlowId('LISTENER',
                                                                                   self.listener_name,
                                                                                   self.van_address[2])
        self.assertIsNotNone(self.listener_vflow_id[self.listener_address_name[2]])
        # addr2:(prio=20, reachable:True)
        # addr1:(prio=10, reachable:True)
        # addr3:(prio=5,  reachable:True)
        # check flow: listener addr2 -> connector_2 (flow index 1)
        flow_index = 1
        client_name = "ECHO_CLIENT_5_"
        self.check_flow(flow_index, client_name)

        # create addr4
        self.create_entity(self.listener_address_name[3], self.listener_address_config[3], self.router_1)
        self.listener_vflow_id[self.listener_address_name[3]] = self.findNewFlowId('LISTENER',
                                                                                   self.listener_name,
                                                                                   self.van_address[3])
        self.assertIsNotNone(self.listener_vflow_id[self.listener_address_name[3]])
        # addr2:(prio=20, reachable:True)
        # addr4:(prio=15, reachable:True)
        # addr1:(prio=10, reachable:True)
        # addr3:(prio=5,  reachable:True)
        # check flow: listener addr2 -> connector_2 (flow index 1)
        flow_index = 1
        client_name = "ECHO_CLIENT_6_"
        self.check_flow(flow_index, client_name)

        # delete connector_2
        self.delete_entity('tcpConnector', self.connector_name[1], self.router_2)
        # wait until connector END vflow record gets propagated to the vanflow snooper at router_1
        connector_vflow_id = self.connector_vflow_id[self.connector_name[1]]
        flow_count_l4 = self.flow_count_l4[connector_vflow_id] + 1
        self.check_entity_vflow_record_end(router_2_id, 'CONNECTOR', connector_vflow_id, flow_count_l4)
        # addr2:(prio=20, reachable:False)
        # addr4:(prio=15, reachable:True)
        # addr1:(prio=10, reachable:True)
        # addr3:(prio=5,  reachable:True)
        # check flow: listener addr4 -> connector_4 (flow index 3)
        flow_index = 3
        client_name = "ECHO_CLIENT_7_"
        self.check_flow(flow_index, client_name)

        # delete connector_1
        self.delete_entity('tcpConnector', self.connector_name[0], self.router_1)
        # wait until connector END vflow record gets propagated to the vanflow snooper at router_1
        connector_vflow_id = self.connector_vflow_id[self.connector_name[0]]
        flow_count_l4 = self.flow_count_l4[connector_vflow_id] + 1
        self.check_entity_vflow_record_end(router_1_id, 'CONNECTOR', connector_vflow_id, flow_count_l4)
        # addr2:(prio=20, reachable:False)
        # addr4:(prio=15, reachable:True)
        # addr1:(prio=10, reachable:False)
        # addr3:(prio=5,  reachable:True)
        # check flow: listener addr4 -> connector_4 (flow index 3)
        flow_index = 3
        client_name = "ECHO_CLIENT_8_"
        self.check_flow(flow_index, client_name)

        # delete connector_4
        self.delete_entity('tcpConnector', self.connector_name[3], self.router_2)
        # wait until connector END vflow record gets propagated to the vanflow snooper at router_1
        connector_vflow_id = self.connector_vflow_id[self.connector_name[3]]
        flow_count_l4 = self.flow_count_l4[connector_vflow_id] + 1
        self.check_entity_vflow_record_end(router_2_id, 'CONNECTOR', connector_vflow_id, flow_count_l4)
        # addr2:(prio=20, reachable:False)
        # addr4:(prio=15, reachable:False)
        # addr1:(prio=10, reachable:False)
        # addr3:(prio=5,  reachable:True)
        # check flow: listener addr3 -> connector_3 (flow index 2)
        flow_index = 2
        client_name = "ECHO_CLIENT_9_"
        self.check_flow(flow_index, client_name)

        # delete connector_3
        self.delete_entity('tcpConnector', self.connector_name[2], self.router_2)
        # wait until connector END vflow record gets propagated to the vanflow snooper at router_1
        connector_vflow_id = self.connector_vflow_id[self.connector_name[2]]
        flow_count_l4 = self.flow_count_l4[connector_vflow_id] + 1
        self.check_entity_vflow_record_end(router_2_id, 'CONNECTOR', connector_vflow_id, flow_count_l4)
        # addr2:(prio=20, reachable:False)
        # addr4:(prio=15, reachable:False)
        # addr1:(prio=10, reachable:False)
        # addr3:(prio=5,  reachable:False)
        # check that listening socket got closed
        ret = self.is_listener_port_closed()
        if ret is not None:
            self.assertTrue(ret)

        # re-create connector_1 at router_2
        self.create_entity(self.connector_name[0], self.connector_config[0], self.router_2)
        self.connector_vflow_id[self.connector_name[0]] = self.findNewFlowId('CONNECTOR',
                                                                             self.connector_name[0],
                                                                             self.van_address[0])
        # addr2:(prio=20, reachable:False)
        # addr4:(prio=15, reachable:False)
        # addr1:(prio=10, reachable:True)
        # addr3:(prio=5,  reachable:False)
        # check flow: listener addr1 -> connector_1 (flow index 0)
        flow_index = 0
        client_name = "ECHO_CLIENT_10_"
        self.check_flow(flow_index, client_name)

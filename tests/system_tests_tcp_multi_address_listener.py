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
import resource

from system_test import TestCase, Qdrouterd, retry, retry_assertion
from system_test import Logger
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE
from TCP_echo_client import TcpEchoClient
from TCP_echo_server import TcpEchoServer


class MultiAddressListenerTest(TestCase):
    """
    Test for the multi address listener. Two routers are run. The first one has two multi address listener
    configured at startup. Four and two distinct service addresses are created for the listeners, respectively.
    Each address targets a separate tcpConnector which in turn connects to a separate echo server.
    """

    @classmethod
    def set_nofile_limit(cls):
        (cls.soft_nofile_limit, cls.hard_nofile_limit) = resource.getrlimit(resource.RLIMIT_NOFILE)
        if cls.soft_nofile_limit < 2048:
            resource.setrlimit(resource.RLIMIT_NOFILE, (2048, cls.hard_nofile_limit))
            cls.nofile_limit_changed = True
        else:
            cls.nofile_limit_changed = False

    @classmethod
    def restore_nofile_limit(cls):
        if cls.nofile_limit_changed:
            resource.setrlimit(resource.RLIMIT_NOFILE, (cls.soft_nofile_limit, cls.hard_nofile_limit))

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

        router_1_id = 'R1'
        router_2_id = 'R2'

        # VAN addresses
        cls.van_address = [cls.test_name + '_addr_1',
                           cls.test_name + '_addr_2',
                           cls.test_name + '_addr_3',
                           cls.test_name + '_addr_4',
                           cls.test_name + '_addr_5',
                           cls.test_name + '_addr_6']
        # listener addresses
        cls.listener_address_name =    ['addr1', 'addr2', 'addr3', 'addr4', 'addr5', 'addr6']
        cls.listener_address_value =   ['10',    '20',    '5',     '15',     '3',    '7']
        cls.listener_address_startup = [True,    True,    False,   False,    True,   True]
        # Listener
        cls.listener_name = ['listener_multi_1', 'listener_multi_2']
        cls.listener_port = [cls.tester.get_port(), cls.tester.get_port()]
        # Connectors
        cls.connector_name = ['connector_1', 'connector_2', 'connector_3', 'connector_4', 'connector_5', 'connector_6']

        # Launch TCP echo servers
        server_logger = [Logger(title=cls.test_name,
                                print_to_console=True,
                                save_for_dump=False,
                                ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                       f"{cls.test_name}_echo_server.log")),
                         Logger(title=cls.test_name + "_weighted",
                                save_for_dump=False,
                                ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                       f"{cls.test_name}_echo_server_weighted.log"))]
        echo_servers = {}
        for i in range(6):
            server_prefix = f"{cls.test_name} ECHO_SERVER_addr_{i + 1}"
            echo_servers[cls.van_address[i]] = TcpEchoServer(prefix=server_prefix, port=0, logger=server_logger[i // 4])
            assert echo_servers[cls.van_address[i]].is_running

        cls.echo_servers = echo_servers

        # listener config
        cls.listener_config = [
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_port[0],
                             'multiAddressStrategy': "priority",
                             'name': cls.listener_name[0]}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_port[1],
                             'multiAddressStrategy': "weighted",
                             'name': cls.listener_name[1]})]

        # listener address amd tcp connector configs
        cls.listener_address_config = []
        cls.connector_config = []
        for i in range(6):
            cls.listener_address_config.append(
                ('listenerAddress', {'name': cls.listener_address_name[i],
                                     'value': cls.listener_address_value[i],
                                     'address': cls.van_address[i],
                                     'listener': cls.listener_name[i // 4]}))
            cls.connector_config.append(
                ('tcpConnector', {'host': "localhost",
                                  'address': cls.van_address[i],
                                  'port': echo_servers[cls.van_address[i]].port,
                                  'name': cls.connector_name[i]}))

        # Launch routers
        inter_router_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_1_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('connector', {'role': 'inter-router', 'port': inter_router_port}),
            cls.listener_config[0],
            cls.listener_config[1],
            cls.listener_address_config[0],
            cls.listener_address_config[1],
            cls.listener_address_config[4],
            cls.listener_address_config[5],
            cls.connector_config[0]
        ])
        config_2 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': router_2_id}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': inter_router_port}),
            cls.connector_config[1],
            cls.connector_config[2],
            cls.connector_config[3],
            cls.connector_config[4],
            cls.connector_config[5]
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
                 'LISTENER', {'NAME': cls.listener_name[0], 'VAN_ADDRESS': cls.van_address[0]},
                 'LISTENER', {'NAME': cls.listener_name[0], 'VAN_ADDRESS': cls.van_address[1]},
                 'LISTENER', {'NAME': cls.listener_name[1], 'VAN_ADDRESS': cls.van_address[4]},
                 'LISTENER', {'NAME': cls.listener_name[1], 'VAN_ADDRESS': cls.van_address[5]}
                 )
            ],
            router_2_id : [
                ('CONNECTOR', {'NAME': cls.connector_name[1], 'VAN_ADDRESS': cls.van_address[1]}),
                ('CONNECTOR', {'NAME': cls.connector_name[2], 'VAN_ADDRESS': cls.van_address[2]}),
                ('CONNECTOR', {'NAME': cls.connector_name[3], 'VAN_ADDRESS': cls.van_address[3]}),
                ('CONNECTOR', {'NAME': cls.connector_name[4], 'VAN_ADDRESS': cls.van_address[4]}),
                ('CONNECTOR', {'NAME': cls.connector_name[5], 'VAN_ADDRESS': cls.van_address[5]})
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
                    cls.listener_vflow_id[aname] = cls.findNewFlowId('LISTENER', cls.listener_name[i // 4], cls.van_address[i], vflow_records)
            for i, cname in enumerate(cls.connector_name):
                cls.connector_vflow_id[cname] = cls.findNewFlowId('CONNECTOR', cname, cls.van_address[i], vflow_records)
        cls.setup_vflow_records = vflow_records

        # L4 flow counts for listener and connector vflow records
        cls.flow_count_l4 = {}
        # BIFLOW_TPORT records match history. It is needed to match new instances of flow records referencing the same
        # listener and connector vflow ids.
        cls.matched_biflow_tport_records = {}

        # Increase number of open files limit if necessary for the random weighted distribution tests
        cls.set_nofile_limit()

        # Generic info messages
        cls.logger = Logger(title=cls.test_name, print_to_console=True)

    @classmethod
    def tearDownClass(cls):
        # stop echo servers
        for _, server in cls.echo_servers.items():
            server.wait()
        cls.restore_nofile_limit()
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

    def read_entity_attribute(self, attr_name, entity_type, entity_name, router):
        cmd = 'READ --type=' + entity_type + ' --name=' + entity_name
        out = router.sk_manager(cmd)
        try:
            e = json.loads(out)
            res = e[attr_name]
        except Exception:
            return None
        return res

    def is_listener_port_closed(self, port):
        cmd = shutil.which("lsof")
        if cmd is None:
            self.logger.log("(INFO) Skipping listening port check because 'lsof' command is not available")
            return None

        def check_port():
            try:
                res = subprocess.run([cmd, "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
                                     stdout=subprocess.DEVNULL, check=True)
            except subprocess.CalledProcessError:
                return True
            return res.returncode != 0

        return retry(check_port, delay=0.1)

    def create_echo_client(self, client_prefix, client_port, num_clients=1, verbose_log=True):
        # There may be a delay before address watch marks a listener address unreachable
        # when the corresponding tcpConnector is deleted. We use the retry() function to re-launch
        # echo client when the client gets stuck trying to send via the tcpConnector already
        # closed. This manifests as a "server closed" exception when we call the client wait()
        # function.
        client_logger = Logger(title=client_prefix,
                               print_to_console=verbose_log)
        num_failures = 0

        def echo_client():
            nonlocal num_failures
            try:
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
            retry(echo_client, delay=0.2)

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
        self.create_echo_client(client_prefix, self.listener_port[0])
        connector_vflow_id = self.connector_vflow_id[self.connector_name[flow_index]]
        listener_vflow_id = self.listener_vflow_id[self.listener_address_name[flow_index]]
        router_1_id = self.router_1.config.router_id
        self.check_biflow_tport_vflow_record(router_1_id, listener_vflow_id, connector_vflow_id)

    def test_multi_address_listener_priority(self):
        '''
        Testing `priority`address selection strategy. The test uses a vanflow snooper thread to check if the correct
        connection flow got exercised under the specific configuration. Possible flows are as follows.
        addr1 -> connector_1
        addr2 -> connector_2
        addr3 -? connector_3
        addr4 -? connector_4
        The test create and delete listener addresses and tcpConnecters. It launches an echo client after each
        configuration change to check if new tcp connections target the expected tcpConnecter.
        '''
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
        ret = self.is_listener_port_closed(self.listener_port[0])
        if ret is not None:
            self.assertTrue(ret)

        # re-create addr1
        self.create_entity(self.listener_address_name[0], self.listener_address_config[0], self.router_1)
        self.listener_vflow_id[self.listener_address_name[0]] = self.findNewFlowId('LISTENER',
                                                                                   self.listener_name[0],
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
                                                                                   self.listener_name[0],
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
                                                                                   self.listener_name[0],
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
                                                                                   self.listener_name[0],
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
        ret = self.is_listener_port_closed(self.listener_port[0])
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

    def test_multi_address_listener_weighted(self):
        '''
        Testing ´weighted´ random address selection strategy. The test uses READ management actions to check
        if the ratio of the opened connections counts is close enough to that of the weight values of the
        corresponding addresses.
        '''
        self.check_setup_status()  # throw here if something failed in setUp()

        router_1_id = self.router_1.config.router_id
        router_2_id = self.router_2.config.router_id

        num_conns_1 = 2000
        num_conns_2 = 100
        max_diff    = 0.3

        # addr5: (weight:3, reachable:True)
        # addr6: (weight:7, reachable:True)
        # 1000 new connections, addr5 and addr6 get addr5_co and addr6_co, respectively (addr5_co+addr6_co=1000)
        # We expect that addr5_co:addr6_co is close enough to 3:7 (the ratio of the weights)
        client_prefix = "Echo_client_WEIGHTED"
        num_failures = self.create_echo_client(client_prefix, self.listener_port[1], num_clients=num_conns_1, verbose_log=False)
        self.assertEqual(num_failures, 0)
        addr5_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr5', self.router_1)
        self.assertIsNotNone(addr5_co)
        addr6_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr6', self.router_1)
        self.assertIsNotNone(addr6_co)
        self.assertEqual(addr5_co + addr6_co, num_conns_1)
        # check that difference from expected value is less than 30% for addr5
        addr5_weight = int(self.listener_address_value[4])
        addr6_weight = int(self.listener_address_value[5])
        total_weight = addr5_weight + addr6_weight
        addr5_expected = (addr5_weight / total_weight) * num_conns_1
        diff = abs(addr5_co - addr5_expected) / addr5_expected
        self.assertLess(diff, max_diff, f"addr5_co:{addr5_co} addr5_co:{addr6_co} num_conns:{num_conns_1}")
        self.logger.log(f"connections opened addr5:{addr5_co} addr6:{addr6_co}")

        # Delete connector_6 to make addr6 unreachable
        self.delete_entity('tcpConnector', self.connector_name[5], self.router_2)
        connector_vflow_id = self.connector_vflow_id[self.connector_name[5]]
        flow_count_l4 = addr6_co + 1  # +1: connector flow to the workload
        self.check_entity_vflow_record_end(router_2_id, 'CONNECTOR', connector_vflow_id, flow_count_l4)
        # addr5: (weight:3, reachable:True)
        # addr6: (weight:7, reachable:False)
        num_failures = self.create_echo_client(client_prefix, self.listener_port[1], num_clients=num_conns_2, verbose_log=False)
        # check that all connections were assigned to addr5
        new_addr6_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr6', self.router_1)
        self.assertEqual(new_addr6_co, addr6_co + num_failures)
        new_addr5_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr5', self.router_1)
        self.assertIsNotNone(addr5_co)
        self.assertEqual(new_addr5_co - addr5_co, num_conns_2)

        # Re-create connector_6 to make addr6 reachable again
        self.create_entity(self.connector_name[5], self.connector_config[5], self.router_2)
        self.connector_vflow_id[self.connector_name[5]] = self.findNewFlowId('CONNECTOR',
                                                                             self.connector_name[5],
                                                                             self.van_address[5])
        # check connections distributions again
        # addr5: (weight:3, reachable:True)
        # addr6: (weight:7, reachable:True)
        num_failures = self.create_echo_client(client_prefix, self.listener_port[1], num_clients=num_conns_1, verbose_log=False)
        self.assertEqual(num_failures, 0)
        addr5_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr5', self.router_1)
        self.assertIsNotNone(addr5_co)
        addr6_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr6', self.router_1)
        self.assertIsNotNone(addr6_co)
        addr5_co_recent = addr5_co - new_addr5_co
        addr6_co_recent = addr6_co - new_addr6_co
        self.assertEqual(addr5_co_recent + addr6_co_recent, num_conns_1)
        # check that difference from expected value is less than 30% for addr5
        diff = abs(addr5_co_recent - addr5_expected) / addr5_expected
        self.assertLess(diff, max_diff, f"addr5_co_recent:{addr5_co_recent} addr6_co_recent:{addr6_co_recent} num_conns:{num_conns_1}")
        self.logger.log(f"recent connections opened addr5:{addr5_co_recent} addr6:{addr6_co_recent}")

        # Delete addr6
        self.delete_entity('listenerAddress', self.listener_address_name[5], self.router_1)
        listener_vflow_id = self.listener_vflow_id[self.listener_address_name[5]]
        flow_count_l4 = addr6_co
        self.check_entity_vflow_record_end(router_1_id, 'LISTENER', listener_vflow_id, flow_count_l4)
        # check that all connections were assigned to addr5
        num_failures = self.create_echo_client(client_prefix, self.listener_port[1], num_clients=num_conns_2, verbose_log=False)
        new_addr5_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr5', self.router_1)
        self.assertIsNotNone(addr5_co)
        self.assertEqual(new_addr5_co - addr5_co, num_conns_2)

        # Re-create addr6
        self.create_entity(self.listener_address_name[5], self.listener_address_config[5], self.router_1)
        # check connections distributions again
        # addr5: (weight:3, reachable:True)
        # addr6: (weight:7, reachable:True)
        num_failures = self.create_echo_client(client_prefix, self.listener_port[1], num_clients=num_conns_1, verbose_log=False)
        self.assertEqual(num_failures, 0)
        addr5_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr5', self.router_1)
        self.assertIsNotNone(addr5_co)
        addr6_co = self.read_entity_attribute('connectionsOpened', 'listenerAddress', 'addr6', self.router_1)
        self.assertIsNotNone(addr6_co)
        addr5_co_recent = addr5_co - new_addr5_co
        addr6_co_recent = addr6_co  # new address entity
        self.assertEqual(addr5_co_recent + addr6_co_recent, num_conns_1)
        # check that difference from expected value is less than 30% for addr5
        diff = abs(addr5_co_recent - addr5_expected) / addr5_expected
        self.assertLess(diff, max_diff, f"addr5_co_recent:{addr5_co_recent} addr6_co_recent:{addr6_co_recent} num_conns:{num_conns_1}")
        self.logger.log(f"recent connections opened addr5:{addr5_co_recent} addr6:{addr6_co_recent}")

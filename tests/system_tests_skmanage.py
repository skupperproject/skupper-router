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
import sys
from subprocess import PIPE, STDOUT
from time import sleep

from proton.utils import BlockingConnection

from skupper_router_internal.compat import dictify
from skupper_router_internal.management.qdrouter import QdSchema

from system_test import unittest, retry_assertion
from system_test import Logger, TestCase, Process, Qdrouterd, main_module, TIMEOUT
from system_test import SkManager, DUMMY_TYPE, SSL_PROFILE_TYPE, LOG_STATS_TYPE
from system_test import CONFIG_ADDRESS_TYPE, ROUTER_ADDRESS_TYPE
from system_test import AMQP_LISTENER_TYPE, LOG_TYPE, ROUTER_TYPE, ROUTER_METRICS_TYPE
from system_test import CONFIG_ENTITY_TYPE, ENTITY_TYPE, CONFIG_AUTOLINK_TYPE
from system_test import AMQP_CONNECTOR_TYPE, ROUTER_LINK_TYPE, CONNECTION_TYPE
from system_test import CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD, \
    CLIENT_CERTIFICATE, CLIENT_PASSWORD_FILE, CLIENT_PRIVATE_KEY
CONNECTION_PROPERTIES_UNICODE_STRING = {'connection': 'properties', 'int_property': 6451}

TOTAL_ENTITIES = 28   # for tests that check the total # of entities


class SkmanageTest(TestCase):
    """Test skmanage tool output"""

    @classmethod
    def setUpClass(cls):
        super(SkmanageTest, cls).setUpClass()
        cls.inter_router_port = cls.tester.get_port()
        cls.secure_port = cls.tester.get_port()
        cls.secure_user_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'R1'}),
            ('sslProfile', {'name': 'server-ssl',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),
            ('listener', {'port': cls.tester.get_port()}),

            ('connector', {'role': 'inter-router', 'port': cls.inter_router_port}),
            ('address', {'name': 'test-address', 'prefix': 'abcd', 'distribution': 'multicast'}),
            ('autoLink', {'name': 'test-auto-link', 'address': 'mnop', 'direction': 'out'}),
            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl'}),
            ('address', {'name': 'pattern-address', 'pattern': 'a/*/b/#/c', 'distribution': 'closest'}),

            # for testing SSL
            ('listener', {'host': 'localhost', 'port': cls.secure_port,
                          'sslProfile': 'server-ssl', 'requireSsl': 'yes'}),
            ('listener', {'host': 'localhost', 'port': cls.secure_user_port,
                          'sslProfile': 'server-ssl', 'requireSsl': 'yes',
                          'authenticatePeer': 'yes',
                          'saslMechanisms': 'EXTERNAL'})
        ])

        config_2 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'R2'}),
            ('listener', {'port': cls.tester.get_port()}),
            ('listener', {'role': 'inter-router', 'port': cls.inter_router_port}),
        ])
        cls.router_2 = cls.tester.qdrouterd('test_router_2', config_2, wait=True)
        cls.router_1 = cls.tester.qdrouterd('test_router_1', config_1, wait=True)
        cls.router_1.wait_router_connected('R2')
        cls.router_2.wait_router_connected('R1')

    def address(self):
        return self.router_1.addresses[0]

    def run_skmanage(self, cmd, input=None, expect=Process.EXIT_OK, address=None):
        p = self.popen(
            ['skmanage'] + cmd.split(' ') + ['--bus', address or self.address(), '--indent=-1', '--timeout', str(TIMEOUT)],
            stdin=PIPE, stdout=PIPE, stderr=STDOUT, expect=expect,
            universal_newlines=True)
        out = p.communicate(input)[0]
        try:
            p.teardown()
        except Exception as e:
            raise sys.exc_info()[0](out if out else str(e))
        return out

    def assert_entity_equal(self, expect, actual, copy=None):
        """Copy keys in copy from actual to expect, then assert maps equal."""
        if copy:
            for k in copy:
                expect[k] = actual[k]
        self.assertEqual(expect, actual)

    def assert_entities_equal(self, expect, actual, copy=None):
        """Do assert_entities_equal on a list of maps."""
        for e, a in zip(expect, actual):
            self.assert_entity_equal(e, a, copy)

    def test_crud(self):

        def check(cmd, expect, copy=None, **kwargs):
            actual = json.loads(self.run_skmanage(cmd))
            self.assert_entity_equal(expect, actual, copy=copy)

        expect = {'arg1': 'foo', 'type': DUMMY_TYPE, 'name': 'mydummy2'}
        # create with type, name in attributes
        check('create arg1=foo type=dummy name=mydummy2', expect, copy=['identity'], attributes=json.dumps(expect))
        # create with type, name as arguments
        expect['name'] = 'mydummy'
        check('create name=mydummy type=dummy arg1=foo', expect, copy=['identity'])
        check('read --name mydummy', expect)
        check('read --identity %s' % expect['identity'], expect)
        expect.update([], arg1='bar', num1=555)
        check('update name=mydummy arg1=bar num1=555', expect)
        check('read --name=mydummy', expect)
        expect.update([], arg1='xxx', num1=888)
        # name outside attributes
        check('update name=mydummy arg1=xxx num1=888', expect)
        check('read --name=mydummy', expect)
        self.run_skmanage('delete --name mydummy')
        self.run_skmanage('read --name=mydummy', expect=Process.EXIT_FAIL)

    def test_stdin(self):
        """Test piping from stdin"""
        def check(cmd, expect, input, copy=None):
            actual = json.loads(self.run_skmanage(cmd + " --stdin", input=input))
            self.assert_entity_equal(expect, actual, copy=copy)

        def check_list(cmd, expect_list, input, copy=None):
            actual = json.loads(self.run_skmanage(cmd + " --stdin", input=input))
            self.assert_entities_equal(expect_list, actual, copy=copy)

        expect = {'type': DUMMY_TYPE, 'name': 'mydummyx', 'arg1': 'foo'}
        check('create', expect, json.dumps(expect), copy=['identity'])

        expect_list = [{'type': DUMMY_TYPE, 'name': 'mydummyx%s' % i} for i in range(3)]
        check_list('create', expect_list, json.dumps(expect_list), copy=['identity'])

        expect['arg1'] = 'bar'
        expect['num1'] = 42
        check('update', expect, json.dumps(expect))

        for i in range(3):
            expect_list[i]['arg1'] = 'bar'
            expect_list[i]['num1'] = i
        check_list('update', expect_list, json.dumps(expect_list))

    def test_query(self):

        def long_type(name):
            long_names = {'listener': AMQP_LISTENER_TYPE,
                          'log': LOG_TYPE,
                          'router': ROUTER_TYPE,
                          'router.link': ROUTER_LINK_TYPE,
                          'connection': CONNECTION_TYPE,
                          'router.address': ROUTER_ADDRESS_TYPE}
            return long_names[name]

        types = ['listener', 'log', 'router']
        long_types = [long_type(name) for name in types]

        qall = json.loads(self.run_skmanage('query'))
        qall_types = {e['type'] for e in qall}
        for t in long_types:
            self.assertIn(t, qall_types)

        qlistener = json.loads(self.run_skmanage(f'query --type={AMQP_LISTENER_TYPE}'))
        self.assertEqual([long_type('listener')] * 4, [e['type'] for e in qlistener])
        self.assertEqual(self.router_1.ports[0], int(qlistener[0]['port']))

        def name_type(entities):
            ignore_types = [long_type(t) for t in ['router.link', 'connection', 'router.address']]
            return set((e['name'], e['type']) for e in entities
                       if e['type'] not in ignore_types)

        def query_type_name():
            qattr = json.loads(self.run_skmanage('query type name'))
            for e in qattr:
                self.assertEqual(2, len(e))
            self.assertEqual(name_type(qall), name_type(qattr))

        retry_assertion(query_type_name)

    def test_get_schema(self):
        schema = dictify(QdSchema().dump())
        actual = self.run_skmanage("get-json-schema")
        self.assertEqual(schema, dictify(json.loads(actual)))
        actual = self.run_skmanage("get-schema")
        self.assertEqual(schema, dictify(json.loads(actual)))

    def test_get_annotations(self):
        """
        The skmanage GET-ANNOTATIONS call must return an empty dict since we don't support annotations at the moment.
        """
        out = json.loads(self.run_skmanage("get-annotations"))
        self.assertEqual(len(out), 0)

    def test_get_types(self):
        out = json.loads(self.run_skmanage("get-types"))
        self.assertEqual(len(out), TOTAL_ENTITIES)

    def test_get_attributes(self):
        out = json.loads(self.run_skmanage("get-attributes"))
        self.assertEqual(len(out), TOTAL_ENTITIES)

    def test_get_operations(self):
        out = json.loads(self.run_skmanage("get-operations"))
        self.assertEqual(len(out), TOTAL_ENTITIES)
        self.assertEqual(out[SSL_PROFILE_TYPE], ['CREATE', 'UPDATE', 'DELETE', 'READ'])

    def test_get_types_with_ssl_profile_type(self):
        out = json.loads(self.run_skmanage(f"get-types --type={SSL_PROFILE_TYPE}"))
        self.assertEqual(out[SSL_PROFILE_TYPE], [CONFIG_ENTITY_TYPE, ENTITY_TYPE])

    def test_get_ssl_profile_type_attributes(self):
        out = json.loads(self.run_skmanage(f'get-attributes --type={SSL_PROFILE_TYPE}'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[SSL_PROFILE_TYPE]), 13)

    def test_get_ssl_profile_attributes(self):
        out = json.loads(self.run_skmanage(f'get-attributes {SSL_PROFILE_TYPE}'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[SSL_PROFILE_TYPE]), 13)

    def test_get_ssl_profile_type_operations(self):
        out = json.loads(self.run_skmanage(f'get-operations --type={SSL_PROFILE_TYPE}'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[SSL_PROFILE_TYPE]), 4)

    def test_get_ssl_profile_operations(self):
        out = json.loads(self.run_skmanage(f'get-operations {SSL_PROFILE_TYPE}'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[SSL_PROFILE_TYPE]), 4)

    def test_create_update_delete_ssl_profile(self):
        cmd = f'CREATE --type={SSL_PROFILE_TYPE} --name=testSslProfile'
        cmd += f' caCertFile={CA_CERT}'
        cmd += f' certFile={SERVER_CERTIFICATE}'
        cmd += f' privateKeyFile={SERVER_PRIVATE_KEY}'
        cmd += f' password={SERVER_PRIVATE_KEY_PASSWORD}'
        out = json.loads(self.run_skmanage(cmd))
        self.assertEqual(CA_CERT, out['caCertFile'])
        self.assertEqual(SERVER_CERTIFICATE, out['certFile'])
        self.assertEqual(SERVER_PRIVATE_KEY, out['privateKeyFile'])
        self.assertEqual(SERVER_PRIVATE_KEY_PASSWORD, out['password'])
        identity = out['identity']

        cmd = f'UPDATE --type={SSL_PROFILE_TYPE} --identity={identity}'
        cmd += f' certFile={CLIENT_CERTIFICATE}'
        cmd += f' privateKeyFile={CLIENT_PRIVATE_KEY}'
        cmd += f' password={CLIENT_PASSWORD_FILE}'
        out = json.loads(self.run_skmanage(cmd))
        self.assertEqual(CA_CERT, out['caCertFile'])
        self.assertEqual(CLIENT_CERTIFICATE, out['certFile'])
        self.assertEqual(CLIENT_PRIVATE_KEY, out['privateKeyFile'])
        self.assertEqual(CLIENT_PASSWORD_FILE, out['password'])

        cmd = f'DELETE --type={SSL_PROFILE_TYPE} --identity={identity}'
        self.run_skmanage(cmd)

        out = json.loads(self.run_skmanage(f'QUERY --type={SSL_PROFILE_TYPE}'))
        self.assertEqual(1, len(out))
        self.assertNotEqual(out[0]['name'], 'testSslProfile')

    def test_get_log(self):
        logs = json.loads(self.run_skmanage("get-log limit=20"))
        found = False
        for log in logs:
            if 'get-log' in log[2] and ['AGENT', 'debug'] == log[0:2]:
                found = True
        self.assertTrue(found)

    def test_get_logstats(self):
        query_command = f'QUERY --type={LOG_STATS_TYPE}'
        logs = json.loads(self.run_skmanage(query_command))
        # Each value returned by the above query should be
        # a log, and each log should contain an entry for each
        # log level.
        log_levels = ['criticalCount',
                      'debugCount',
                      'errorCount',
                      'infoCount',
                      'warningCount'
                      ]
        n_log_levels = len(log_levels)

        good_logs = 0

        for log_dict in logs:
            log_levels_present = 0
            log_levels_missing = 0
            for log_level in log_levels:
                if log_level in log_dict:
                    log_levels_present += 1
                else:
                    log_levels_missing += 1
            if log_levels_present == n_log_levels:
                good_logs += 1

        self.assertEqual(good_logs, len(logs))

    def test_update(self):
        exception = False
        try:
            # Try to not set 'output'
            json.loads(self.run_skmanage(f"UPDATE --type {LOG_TYPE} --name log/DEFAULT outputFile="))
        except Exception as e:
            exception = True
            self.assertIn("InternalServerErrorStatus: CError: Configuration: Failed to open log file ''", str(e))
        self.assertTrue(exception)

        # Set a valid 'output'
        output = json.loads(self.run_skmanage(f"UPDATE --type {LOG_TYPE} --name log/DEFAULT "
                                              "enable=debug+ outputFile=A.log"))
        self.assertEqual("A.log", output['outputFile'])
        self.assertEqual("debug+", output['enable'])

    def create(self, type, name, port, role=None):
        create_command = 'CREATE --type=' + type + ' --name=' + name + ' host=0.0.0.0 port=' + port
        if role:
            create_command = create_command + ' role=' + role
        ret_entity = json.loads(self.run_skmanage(create_command))
        return ret_entity

    def test_check_address_name(self):
        query_command = 'QUERY --type=' + CONFIG_ADDRESS_TYPE
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['name'], "test-address")
        self.assertEqual(output[0]['distribution'], "multicast")
        self.assertEqual(output[0]['prefix'], "abcd")
        self.assertNotIn('pattern', output[0])
        self.assertEqual(output[1]['name'], "pattern-address")
        self.assertEqual(output[1]['distribution'], "closest")
        self.assertEqual(output[1]['pattern'], "a/*/b/#/c")
        self.assertNotIn('prefix', output[1])

    def test_create_address(self):
        create_command = f'CREATE --type={CONFIG_ADDRESS_TYPE} pattern="a.b.#"'
        output = json.loads(self.run_skmanage(create_command))
        self.assertEqual(output['pattern'], '"a.b.#"')

    def test_check_auto_link_name(self):
        query_command = f'QUERY --type={CONFIG_AUTOLINK_TYPE}'
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(output[0]['name'], "test-auto-link")
        self.assertEqual(output[0]['direction'], "out")
        self.assertEqual(output[0]['addr'], "mnop")

    def test_specify_container_id_connection_auto_link(self):
        create_command = f'CREATE --type={CONFIG_AUTOLINK_TYPE} address=abc containerId=id1 connection=conn1 direction=out'
        output = self.run_skmanage(create_command, expect=Process.EXIT_FAIL)
        self.assertIn("Both connection and containerId cannot be specified", output)

    def test_yyy_delete_create_connector(self):
        """
        This test tries to delete a connector and makes sure that there are no inter-router connections
        It then adds back the connector and make sure that there is at least one inter-router connection.
        The test name starts with a yyy so that it runs towards the end.
        """
        query_command = f'QUERY --type={AMQP_CONNECTOR_TYPE}'
        output = json.loads(self.run_skmanage(query_command))
        name = output[0]['name']

        # Delete an existing connector
        delete_command = 'DELETE --type=' + AMQP_CONNECTOR_TYPE + ' --name=' + name
        self.run_skmanage(delete_command)
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(output, [])

        def query_inter_router_connector(check_inter_router_present=False):
            outs = json.loads(self.run_skmanage(f'query --type={CONNECTION_TYPE}'))
            inter_router_present = False
            for out in outs:
                if check_inter_router_present:
                    if out['role'] == "inter-router":
                        inter_router_present = True
                        break
                else:
                    self.assertNotEqual(out['role'], "inter-router")
            if check_inter_router_present:
                self.assertTrue(inter_router_present)

        # The connector has been deleted, check to make sure that there are no connections of 'inter-router' role
        retry_assertion(query_inter_router_connector, delay=2)

        # Create back the connector with role="inter-router"
        self.create(AMQP_CONNECTOR_TYPE, name, str(SkmanageTest.inter_router_port), role="inter-router")
        outputs = json.loads(self.run_skmanage(query_command))
        created = False
        for output in outputs:
            conn_name = 'connector/127.0.0.1:%s' % SkmanageTest.inter_router_port
            conn_name_1 = 'connector/0.0.0.0:%s' % SkmanageTest.inter_router_port
            if conn_name == output['name'] or conn_name_1 == output['name']:
                created = True
                break

        self.assertTrue(created)

        # The connector has been created, check to make sure that there is at least one
        # connection of role 'inter-router'
        retry_assertion(lambda: query_inter_router_connector(check_inter_router_present=True), delay=2)

    def test_zzz_add_connector(self):
        port = self.get_port()
        # dont provide role and make sure that role is defaulted to 'normal'
        command = f"CREATE --type={AMQP_CONNECTOR_TYPE} --name=eaconn1 port=" + str(port) + " host=0.0.0.0"
        output = json.loads(self.run_skmanage(command))
        self.assertEqual("normal", output['role'])
        # provide the same connector name (eaconn1), expect duplicate value failure
        self.assertRaises(Exception, self.run_skmanage,
                          f"CREATE --type={AMQP_CONNECTOR_TYPE} --name=eaconn1 port=12345 host=0.0.0.0")
        port = self.get_port()
        # provide role as 'normal' and make sure that it is preserved
        command = f"CREATE --type={AMQP_CONNECTOR_TYPE} --name=eaconn2 port=" + str(port) + " host=0.0.0.0 role=normal"
        output = json.loads(self.run_skmanage(command))
        self.assertEqual("normal", output['role'])

    def test_zzz_create_delete_listener(self):
        name = 'ealistener'

        listener_port = self.get_port()

        listener = self.create(AMQP_LISTENER_TYPE, name, str(listener_port))
        self.assertEqual(listener['type'], AMQP_LISTENER_TYPE)
        self.assertEqual(listener['name'], name)

        exception_occurred = False

        delete_command = 'DELETE --type=' + AMQP_LISTENER_TYPE + ' --name=' + name
        self.run_skmanage(delete_command)

        exception_occurred = False
        try:
            # Try deleting an already deleted connector, this should raise an exception
            self.run_skmanage(delete_command)
        except Exception as e:
            exception_occurred = True
            self.assertIn(("NotFoundStatus: No entity with name=%s" % name), str(e))

        self.assertTrue(exception_occurred)

    def test_create_delete_ssl_profile(self):
        ssl_profile_name = 'ssl-profile-test'
        ssl_create_command = f'CREATE --type={SSL_PROFILE_TYPE} certFile=' + SERVER_CERTIFICATE + \
            ' privateKeyFile=' + SERVER_PRIVATE_KEY + ' password=' + SERVER_PRIVATE_KEY_PASSWORD + \
            ' name=' + ssl_profile_name + ' caCertFile=' + CA_CERT
        output = json.loads(self.run_skmanage(ssl_create_command))
        self.assertEqual(output['name'], ssl_profile_name)
        self.run_skmanage(f'DELETE --type={SSL_PROFILE_TYPE} --name=' +
                          ssl_profile_name)

    def test_delete_connection(self):
        """
        This test creates a blocking connection and tries to delete that connection using skmanage DELETE operation.
        Make sure we are Forbidden from deleting a connection because skmanage DELETEs are not allowed on a connection
        Only skmanage UPDATEs are allowed..
        :return:
        """
        connection = BlockingConnection(self.address(), properties=CONNECTION_PROPERTIES_UNICODE_STRING)
        query_command = f'QUERY --type={CONNECTION_TYPE}'
        outputs = json.loads(self.run_skmanage(query_command))
        identity = None
        passed = False
        for output in outputs:
            if output.get('properties'):
                conn_properties = output['properties']
                if conn_properties.get('int_property'):
                    identity = output.get("identity")
                    if identity:
                        delete_command = 'DELETE --type=connection --id=' + identity
                        try:
                            outs = json.loads(self.run_skmanage(delete_command))
                        except Exception as e:
                            if "Forbidden" in str(e):
                                passed = True

        # The test has passed since we were forbidden from deleting a connection
        # due to lack of policy permissions.
        self.assertTrue(passed)

    def test_create_delete_address_pattern(self):
        config = [('mercury.*.earth.#', 'closest'),
                  ('*/mars/*/#', 'multicast'),
                  ('*.mercury', 'closest'),
                  ('*/#/pluto', 'multicast')]

        # add patterns:
        pcount = 0
        for p in config:
            query_command = 'CREATE --type=' + CONFIG_ADDRESS_TYPE + \
                ' pattern=' + p[0] + \
                ' distribution=' + p[1] + \
                ' name=Pattern' + str(pcount)
            self.run_skmanage(query_command)
            pcount += 1

        # verify correctly added:
        query_command = 'QUERY --type=' + CONFIG_ADDRESS_TYPE
        output = json.loads(self.run_skmanage(query_command))
        total = len(output)

        pcount = 0
        for o in output:
            pattern = o.get('pattern')
            if pattern is not None:
                for p in config:
                    if p[0] == pattern:
                        pcount += 1
                        self.assertEqual(p[1], o.get('distribution'))
        self.assertEqual(pcount, len(config))

        # delete
        pcount = 0
        for p in config:
            query_command = 'DELETE --type=' + CONFIG_ADDRESS_TYPE + \
                ' --name=Pattern' + str(pcount)
            self.run_skmanage(query_command)
            pcount += 1

        # verify deleted:
        query_command = 'QUERY --type=' + CONFIG_ADDRESS_TYPE
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(len(output), total - len(config))
        for o in output:
            pattern = o.get('pattern')
            if pattern is not None:
                for p in config:
                    self.assertNotEqual(p[0], pattern)

    def test_yy_query_many_links(self):
        # This test will fail without the fix for DISPATCH-974
        c = BlockingConnection(self.address())
        self.logger = Logger(title="test_yy_query_many_links")
        count = 0
        COUNT = 3000

        ADDRESS_SENDER = "examples-sender"
        ADDRESS_RECEIVER = "examples-receiver"

        # This loop creates 5000 consumer and 5000 producer links with
        # different addresses
        while count < COUNT:
            r = c.create_receiver(ADDRESS_RECEIVER + str(count))
            s = c.create_sender(ADDRESS_SENDER + str(count))
            count += 1

        # Try fetching all 10,000 addresses
        # This skmanage query command would fail without the fix
        # for DISPATCH-974
        query_command = f'QUERY --type={ROUTER_ADDRESS_TYPE}'
        for i in range(3):
            sender_addresses = 0
            receiver_addresses = 0
            outs = json.loads(self.run_skmanage(query_command))
            for out in outs:
                if ADDRESS_SENDER in out['name']:
                    sender_addresses += 1
                if ADDRESS_RECEIVER in out['name']:
                    receiver_addresses += 1
            if sender_addresses < COUNT or receiver_addresses < COUNT:
                sleep(2)
            else:
                break

        self.assertEqual(sender_addresses, COUNT)
        self.assertEqual(receiver_addresses, COUNT)

        query_command = 'QUERY --type=link'
        success = False
        for i in range(3):
            out_links = 0
            in_links = 0
            outs = json.loads(self.run_skmanage(query_command))
            for out in outs:
                if out.get('owningAddr'):
                    if ADDRESS_SENDER in out['owningAddr']:
                        in_links += 1
                    if ADDRESS_RECEIVER in out['owningAddr']:
                        out_links += 1

            # If the link count is less than COUNT, try again in 2 seconds
            # Try after 2 more seconds for a total of 6 seconds.
            # If the link count is still less than expected count, there
            # is something wrong, the test has failed.
            if out_links < COUNT or in_links < COUNT:
                self.logger.log("out_links=%s, in_links=%s" % (str(out_links), str(in_links)))
                sleep(2)
            else:
                self.logger.log("Test success!")
                success = True
                break

        if not success:
            self.logger.dump()

        self.assertEqual(out_links, COUNT)
        self.assertEqual(in_links, COUNT)

    def test_worker_threads(self):
        sk_manager = SkManager(address=self.address())
        output = sk_manager.query(ROUTER_TYPE)
        self.assertEqual(output[0]['workerThreads'], 4)

    def test_check_memory_usage(self):
        """
        Verify that the process memory usage is present. Non-Linux platforms
        may return zero, so accept that as a valid value.
        """
        query_command = f'QUERY --type={ROUTER_METRICS_TYPE}'
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(len(output), 1)
        vmsize = output[0].get('memoryUsage')
        rss = output[0].get('residentMemoryUsage')

        if sys.platform.lower().startswith('linux'):
            # @TODO(kgiusti) - linux only for now
            self.assertIsNotNone(vmsize)
            self.assertIsNotNone(rss)
            self.assertGreaterEqual(vmsize, 0)
            self.assertGreaterEqual(rss, 0)
        else:
            # @TODO(kgiusti) - update test to handle other platforms as support
            # is added
            self.assertIsNone(vmsize)
            self.assertIsNone(rss)

    def test_ssl_connection(self):
        """Verify skmanage can securely connect via SSL"""
        ssl_address = "amqps://localhost:%s" % self.secure_port
        ssl_user_address = "amqps://localhost:%s" % self.secure_user_port
        query = f'QUERY --type {ROUTER_TYPE}'

        # this should fail: no trustfile
        with self.assertRaises(RuntimeError,
                               msg="failure expected: no trustfile") as exc:
            self.run_skmanage(query, address=ssl_address)
        self.assertIn("certificate verify failed", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass:
        self.run_skmanage(query + " --ssl-trustfile " +
                          CA_CERT,
                          address=ssl_address)

        # this should fail: wrong hostname
        with self.assertRaises(RuntimeError,
                               msg="failure expected: wrong hostname") as exc:
            self.run_skmanage(query + " --ssl-trustfile " +
                              CA_CERT,
                              address="amqps://127.0.0.1:%s" % self.secure_port)
        self.assertIn("certificate verify failed", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass: disable hostname check:
        self.run_skmanage(query + " --ssl-trustfile " +
                          CA_CERT +
                          " --ssl-disable-peer-name-verify",
                          address="amqps://127.0.0.1:%s" % self.secure_port)

        # this should fail: router requires client to authenticate
        with self.assertRaises(RuntimeError,
                               msg="client authentication should fail") as exc:
            self.run_skmanage(query + " --ssl-trustfile " +
                              CA_CERT,
                              address=ssl_user_address)
        self.assertIn("SSL Failure", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass: skmanage provides credentials
        self.run_skmanage(query + " --ssl-trustfile " +
                          CA_CERT +
                          " --ssl-certificate " +
                          CLIENT_CERTIFICATE +
                          " --ssl-password-file " +
                          CLIENT_PASSWORD_FILE +
                          " --ssl-key " +
                          CLIENT_PRIVATE_KEY,
                          address=ssl_user_address)

    def test_listener_connector_cost(self):
        # Try creating a connector and listener with negative cost
        create_command = 'CREATE --type=' + AMQP_CONNECTOR_TYPE + ' port=' +  \
                         str(self.get_port()) + ' cost=-1 role=normal'
        error_string = "Configuration: Invalid cost (-1) specified. Minimum value for cost is " \
                       "1 and maximum value is 2147483647"
        passed = False
        try:
            json.loads(self.run_skmanage(create_command))
        except RuntimeError as e:
            if error_string in str(e):
                passed = True
        self.assertTrue(passed)

        create_command = 'CREATE --type=' + AMQP_LISTENER_TYPE + ' port=' +  \
                         str(self.get_port()) + ' cost=-1 role=normal'
        error_string = "Configuration: Invalid cost (-1) specified. Minimum value for cost is " \
                       "1 and maximum value is 2147483647"
        passed = False
        try:
            json.loads(self.run_skmanage(create_command))
        except RuntimeError as e:
            if error_string in str(e):
                passed = True
        self.assertTrue(passed)

        # Try creating a connector and listener with cost past the int range.
        create_command = 'CREATE --type=' + AMQP_CONNECTOR_TYPE + ' port=' +  \
                         str(self.get_port()) + ' cost=2147483648 role=normal'
        error_string = "Configuration: Invalid cost (2147483648) specified. Minimum value for cost is " \
                       "1 and maximum value is 2147483647"
        passed = False
        try:
            json.loads(self.run_skmanage(create_command))
        except RuntimeError as e:
            if error_string in str(e):
                passed = True
        self.assertTrue(passed)

        create_command = 'CREATE --type=' + AMQP_LISTENER_TYPE + ' port=' +  \
                         str(self.get_port()) + ' cost=2147483648 role=normal'
        passed = False
        try:
            json.loads(self.run_skmanage(create_command))
        except RuntimeError as e:
            if error_string in str(e):
                passed = True
        self.assertTrue(passed)


if __name__ == '__main__':
    unittest.main(main_module())

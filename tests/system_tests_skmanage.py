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
import sys
from subprocess import PIPE, STDOUT
from time import sleep

from proton.utils import BlockingConnection

from skupper_router_internal.compat import dictify
from skupper_router_internal.management.qdrouter import QdSchema

from system_test import unittest
from system_test import Logger, TestCase, Process, Qdrouterd, main_module, TIMEOUT, DIR
from system_test import QdManager

DUMMY = "io.skupper.router.dummy"

CONNECTION_PROPERTIES_UNICODE_STRING = {'connection': 'properties', 'int_property': 6451}

TOTAL_ENTITIES = 29   # for tests that check the total # of entities


class SkmanageTest(TestCase):
    """Test skmanage tool output"""

    @staticmethod
    def ssl_file(name):
        return os.path.join(DIR, 'ssl_certs', name)

    @classmethod
    def setUpClass(cls):
        super(SkmanageTest, cls).setUpClass()
        cls.inter_router_port = cls.tester.get_port()
        cls.secure_port = cls.tester.get_port()
        cls.secure_user_port = cls.tester.get_port()
        config_1 = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'R1'}),
            ('sslProfile', {'name': 'server-ssl',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'password': 'server-password'}),
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

        expect = {'arg1': 'foo', 'type': DUMMY, 'name': 'mydummy2'}
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

        expect = {'type': DUMMY, 'name': 'mydummyx', 'arg1': 'foo'}
        check('create', expect, json.dumps(expect), copy=['identity'])

        expect_list = [{'type': DUMMY, 'name': 'mydummyx%s' % i} for i in range(3)]
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
            return 'io.skupper.router.' + name

        types = ['listener', 'log', 'router']
        long_types = [long_type(name) for name in types]

        qall = json.loads(self.run_skmanage('query'))
        qall_types = {e['type'] for e in qall}
        for t in long_types:
            self.assertIn(t, qall_types)

        qlistener = json.loads(self.run_skmanage('query --type=listener'))
        self.assertEqual([long_type('listener')] * 4, [e['type'] for e in qlistener])
        self.assertEqual(self.router_1.ports[0], int(qlistener[0]['port']))

        qattr = json.loads(self.run_skmanage('query type name'))
        for e in qattr:
            self.assertEqual(2, len(e))

        def name_type(entities):
            ignore_types = [long_type(t) for t in ['router.link', 'connection', 'router.address']]
            return set((e['name'], e['type']) for e in entities
                       if e['type'] not in ignore_types)
        self.assertEqual(name_type(qall), name_type(qattr))

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
        self.assertTrue(len(out) == 0)

    def test_get_types(self):
        out = json.loads(self.run_skmanage("get-types"))
        self.assertEqual(len(out), TOTAL_ENTITIES)

    def test_get_attributes(self):
        out = json.loads(self.run_skmanage("get-attributes"))
        self.assertEqual(len(out), TOTAL_ENTITIES)

    def test_get_operations(self):
        out = json.loads(self.run_skmanage("get-operations"))
        self.assertEqual(len(out), TOTAL_ENTITIES)
        self.assertEqual(out['io.skupper.router.sslProfile'], ['CREATE', 'DELETE', 'READ'])

    def test_get_types_with_ssl_profile_type(self):
        out = json.loads(self.run_skmanage("get-types --type=io.skupper.router.sslProfile"))
        self.assertEqual(out['io.skupper.router.sslProfile'], ['io.skupper.router.configurationEntity', 'io.skupper.router.entity'])

    def test_get_ssl_profile_type_attributes(self):
        out = json.loads(self.run_skmanage('get-attributes --type=io.skupper.router.sslProfile'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out['io.skupper.router.sslProfile']), 11)

    def test_get_ssl_profile_attributes(self):
        out = json.loads(self.run_skmanage('get-attributes io.skupper.router.sslProfile'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out['io.skupper.router.sslProfile']), 11)

    def test_get_ssl_profile_type_operations(self):
        out = json.loads(self.run_skmanage('get-operations --type=io.skupper.router.sslProfile'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out['io.skupper.router.sslProfile']), 3)

    def test_get_ssl_profile_operations(self):
        out = json.loads(self.run_skmanage('get-operations io.skupper.router.sslProfile'))
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out['io.skupper.router.sslProfile']), 3)

    def test_get_log(self):
        logs = json.loads(self.run_skmanage("get-log limit=20"))
        found = False
        for log in logs:
            if 'get-log' in log[2] and ['AGENT', 'debug'] == log[0:2]:
                found = True
        self.assertTrue(found)

    def test_get_logstats(self):
        query_command = 'QUERY --type=logStats'
        logs = json.loads(self.run_skmanage(query_command))
        # Each value returned by the above query should be
        # a log, and each log should contain an entry for each
        # log level.
        log_levels = ['criticalCount',
                      'debugCount',
                      'errorCount',
                      'infoCount',
                      'noticeCount',
                      'traceCount',
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
            json.loads(self.run_skmanage("UPDATE --type io.skupper.router.log --name log/DEFAULT outputFile="))
        except Exception as e:
            exception = True
            self.assertTrue("InternalServerErrorStatus: CError: Configuration: Failed to open log file ''" in str(e))
        self.assertTrue(exception)

        # Set a valid 'output'
        output = json.loads(self.run_skmanage("UPDATE --type io.skupper.router.log --name log/DEFAULT "
                                              "enable=trace+ outputFile=A.log"))
        self.assertEqual("A.log", output['outputFile'])
        self.assertEqual("trace+", output['enable'])

    def create(self, type, name, port):
        create_command = 'CREATE --type=' + type + ' --name=' + name + ' host=0.0.0.0 port=' + port
        connector = json.loads(self.run_skmanage(create_command))
        return connector

    def test_check_address_name(self):
        long_type = 'io.skupper.router.router.config.address'
        query_command = 'QUERY --type=' + long_type
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
        long_type = 'io.skupper.router.router.config.address'
        create_command = 'CREATE --type=' + long_type + ' pattern="a.b.#"'
        output = json.loads(self.run_skmanage(create_command))
        self.assertEqual(output['pattern'], '"a.b.#"')

    def test_check_auto_link_name(self):
        long_type = 'io.skupper.router.router.config.autoLink'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(output[0]['name'], "test-auto-link")
        self.assertEqual(output[0]['direction'], "out")
        self.assertEqual(output[0]['addr'], "mnop")

    def test_specify_container_id_connection_auto_link(self):
        long_type = 'io.skupper.router.router.config.autoLink'
        create_command = 'CREATE --type=' + long_type + ' address=abc containerId=id1 connection=conn1 direction=out'
        output = self.run_skmanage(create_command, expect=Process.EXIT_FAIL)
        self.assertIn("Both connection and containerId cannot be specified", output)

    def test_create_delete_connector(self):
        long_type = 'io.skupper.router.connector'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_skmanage(query_command))
        name = output[0]['name']

        # Delete an existing connector
        delete_command = 'DELETE --type=' + long_type + ' --name=' + name
        self.run_skmanage(delete_command)
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(output, [])

        # Re-create the connector and then try wait_connectors
        self.create(long_type, name, str(SkmanageTest.inter_router_port))

        outputs = json.loads(self.run_skmanage(query_command))
        created = False
        for output in outputs:
            conn_name = 'connector/127.0.0.1:%s' % SkmanageTest.inter_router_port
            conn_name_1 = 'connector/0.0.0.0:%s' % SkmanageTest.inter_router_port
            if conn_name == output['name'] or conn_name_1 == output['name']:
                created = True
                break

        self.assertTrue(created)

    def test_zzz_add_connector(self):
        port = self.get_port()
        # dont provide role and make sure that role is defaulted to 'normal'
        command = "CREATE --type=connector --name=eaconn1 port=" + str(port) + " host=0.0.0.0"
        output = json.loads(self.run_skmanage(command))
        self.assertEqual("normal", output['role'])
        # provide the same connector name (eaconn1), expect duplicate value failure
        self.assertRaises(Exception, self.run_skmanage,
                          "CREATE --type=connector --name=eaconn1 port=12345 host=0.0.0.0")
        port = self.get_port()
        # provide role as 'normal' and make sure that it is preserved
        command = "CREATE --type=connector --name=eaconn2 port=" + str(port) + " host=0.0.0.0 role=normal"
        output = json.loads(self.run_skmanage(command))
        self.assertEqual("normal", output['role'])

    def test_zzz_create_delete_listener(self):
        long_type = 'io.skupper.router.listener'
        name = 'ealistener'

        listener_port = self.get_port()

        listener = self.create(long_type, name, str(listener_port))
        self.assertEqual(listener['type'], long_type)
        self.assertEqual(listener['name'], name)

        exception_occurred = False

        delete_command = 'DELETE --type=' + long_type + ' --name=' + name
        self.run_skmanage(delete_command)

        exception_occurred = False
        try:
            # Try deleting an already deleted connector, this should raise an exception
            self.run_skmanage(delete_command)
        except Exception as e:
            exception_occurred = True
            self.assertTrue(("NotFoundStatus: No entity with name=%s" % name) in str(e))

        self.assertTrue(exception_occurred)

    def test_create_delete_ssl_profile(self):
        ssl_profile_name = 'ssl-profile-test'
        ssl_create_command = 'CREATE --type=sslProfile certFile=' + self.ssl_file('server-certificate.pem') + \
            ' privateKeyFile=' + self.ssl_file('server-private-key.pem') + ' password=server-password' + \
            ' name=' + ssl_profile_name + ' caCertFile=' + self.ssl_file('ca-certificate.pem')
        output = json.loads(self.run_skmanage(ssl_create_command))
        self.assertEqual(output['name'], ssl_profile_name)
        self.run_skmanage('DELETE --type=sslProfile --name=' +
                          ssl_profile_name)

    def test_delete_connection(self):
        """
        This test creates a blocking connection and tries to delete that connection using skmanage DELETE operation.
        Make sure we are Forbidden from deleting a connection because skmanage DELETEs are not allowed on a connection
        Only skmanage UPDATEs are allowed..
        :return:
        """
        connection = BlockingConnection(self.address(), properties=CONNECTION_PROPERTIES_UNICODE_STRING)
        query_command = 'QUERY --type=connection'
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
        long_type = 'io.skupper.router.router.config.address'

        # add patterns:
        pcount = 0
        for p in config:
            query_command = 'CREATE --type=' + long_type + \
                ' pattern=' + p[0] + \
                ' distribution=' + p[1] + \
                ' name=Pattern' + str(pcount)
            self.run_skmanage(query_command)
            pcount += 1

        # verify correctly added:
        query_command = 'QUERY --type=' + long_type
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
            query_command = 'DELETE --type=' + long_type + \
                ' --name=Pattern' + str(pcount)
            self.run_skmanage(query_command)
            pcount += 1

        # verify deleted:
        query_command = 'QUERY --type=' + long_type
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
        COUNT = 5000

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
        query_command = 'QUERY --type=io.skupper.router.router.address'
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
        long_type = 'io.skupper.router.router'
        qd_manager = QdManager(address=self.address())
        output = qd_manager.query('io.skupper.router.router')
        self.assertEqual(output[0]['workerThreads'], 4)

    def test_check_memory_usage(self):
        """
        Verify that the process memory usage is present. Non-Linux platforms
        may return zero, so accept that as a valid value.
        """
        long_type = 'io.skupper.router.router'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_skmanage(query_command))
        self.assertEqual(len(output), 1)
        mem = output[0].get('memoryUsage')

        if sys.platform.lower().startswith('linux'):
            # @TODO(kgiusti) - linux only for now
            self.assertTrue(mem is not None)
            self.assertTrue(mem >= 0)
        else:
            # @TODO(kgiusti) - update test to handle other platforms as support
            # is added
            self.assertTrue(mem is None)

    def test_ssl_connection(self):
        """Verify skmanage can securely connect via SSL"""
        ssl_address = "amqps://localhost:%s" % self.secure_port
        ssl_user_address = "amqps://localhost:%s" % self.secure_user_port
        query = 'QUERY --type io.skupper.router.router'

        # this should fail: no trustfile
        with self.assertRaises(RuntimeError,
                               msg="failure expected: no trustfile") as exc:
            self.run_skmanage(query, address=ssl_address)
        self.assertIn("certificate verify failed", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass:
        self.run_skmanage(query + " --ssl-trustfile " +
                          self.ssl_file('ca-certificate.pem'),
                          address=ssl_address)

        # this should fail: wrong hostname
        with self.assertRaises(RuntimeError,
                               msg="failure expected: wrong hostname") as exc:
            self.run_skmanage(query + " --ssl-trustfile " +
                              self.ssl_file('ca-certificate.pem'),
                              address="amqps://127.0.0.1:%s" % self.secure_port)
        self.assertIn("certificate verify failed", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass: disable hostname check:
        self.run_skmanage(query + " --ssl-trustfile " +
                          self.ssl_file('ca-certificate.pem') +
                          " --ssl-disable-peer-name-verify",
                          address="amqps://127.0.0.1:%s" % self.secure_port)

        # this should fail: router requires client to authenticate
        with self.assertRaises(RuntimeError,
                               msg="client authentication should fail") as exc:
            self.run_skmanage(query + " --ssl-trustfile " +
                              self.ssl_file('ca-certificate.pem'),
                              address=ssl_user_address)
        self.assertIn("SSL Failure", str(exc.exception),
                      "unexpected exception: %s" % str(exc.exception))

        # this should pass: skmanage provides credentials
        self.run_skmanage(query + " --ssl-trustfile " +
                          self.ssl_file('ca-certificate.pem') +
                          " --ssl-certificate " +
                          self.ssl_file('client-certificate.pem') +
                          " --ssl-password-file " +
                          self.ssl_file('client-password-file.txt') +
                          " --ssl-key " +
                          self.ssl_file('client-private-key.pem'),
                          address=ssl_user_address)


if __name__ == '__main__':
    unittest.main(main_module())

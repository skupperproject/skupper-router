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

import json
import os
import re
import sys
from subprocess import PIPE, STDOUT
from threading import Timer

from system_test import TestCase, Qdrouterd, Process, TIMEOUT
from system_test import main_module
from system_test import unittest


class FailoverTest(TestCase):
    inter_router_port = None

    @classmethod
    def router(cls, name, config):
        config = Qdrouterd.Config(config)

        cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

    @classmethod
    def setUpClass(cls):
        super(FailoverTest, cls).setUpClass()

        cls.routers = []

        cls.inter_router_port = cls.tester.get_port()
        cls.inter_router_port_1 = cls.tester.get_port()
        cls.backup_port = cls.tester.get_port()
        cls.backup_url = 'amqp://0.0.0.0:' + str(cls.backup_port)
        cls.my_server_port = cls.tester.get_port()

        cls.failover_list = 'amqp://third-host:5671, ' + cls.backup_url

        #
        # Router A tries to connect to Router B via its connectorToB. Router B responds with an open frame which will
        # have the failover-server-list as one of its connection properties like the following -
        # [0x13024d0]:0 <- @open(16) [container-id="Router.A", max-frame-size=16384, channel-max=32767,
        # idle-time-out=8000, offered-capabilities=:"ANONYMOUS-RELAY",
        # properties={:product="qpid-dispatch-router", :version="1.0.0",
        #  :"failover-server-list"=[{:"network-host"="some-host", :port="35000"},
        #  {:"network-host"="0.0.0.0", :port="25000"}]}]
        #
        # The suite of tests determine if the router receiving this open frame stores it properly and if the
        # original connection goes down, check that the router is trying to make connections to the failover urls.
        #
        FailoverTest.router('B', [
            ('router', {'mode': 'interior', 'id': 'B'}),
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': cls.inter_router_port,
                          'failoverUrls': cls.failover_list}),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port()}),
        ]
        )

        FailoverTest.router('A',
                            [
                                ('router', {'mode': 'interior', 'id': 'A'}),
                                ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port()}),
                                ('connector', {'name': 'connectorToB', 'role': 'inter-router',
                                               'port': cls.inter_router_port}),
                            ]
                            )

        FailoverTest.router('C', [
                            ('router', {'mode': 'interior', 'id': 'C'}),
                            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': cls.backup_port}),
                            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.tester.get_port()}),
                            ]
                            )

        cls.routers[1].wait_router_connected('B')

    def setUp(self):
        super().setUp()
        self.success = False
        self.timer_delay = 2
        self.max_attempts = 10
        self.attempts = 0

    def address(self):
        return self.routers[1].addresses[0]

    def run_qdmanage(self, cmd, input=None, expect=Process.EXIT_OK, address=None):
        p = self.popen(
            ['qdmanage'] + cmd.split(' ') + ['--bus', address or self.address(), '--indent=-1', '--timeout', str(TIMEOUT)],
            stdin=PIPE, stdout=PIPE, stderr=STDOUT, expect=expect,
            universal_newlines=True)
        out = p.communicate(input)[0]
        try:
            p.teardown()
        except Exception as e:
            raise Exception("%s\n%s" % (e, out))
        return out

    def run_qdstat(self, args, regexp=None, address=None):
        p = self.popen(
            ['qdstat', '--bus', str(address or self.router.addresses[0]), '--timeout', str(TIMEOUT)] + args,
            name='qdstat-' + self.id(), stdout=PIPE, expect=None,
            universal_newlines=True)

        out = p.communicate()[0]
        assert p.returncode == 0, \
            "qdstat exit status %s, output:\n%s" % (p.returncode, out)
        if regexp:
            assert re.search(regexp, out, re.I), "Can't find '%s' in '%s'" % (regexp, out)
        return out

    def test_1_connector_has_failover_list(self):
        """
        This is the most simple and straightforward case. Router A connects to Router B. Router B sends
        failover information to Router A.
        We make a qdmanage connector query to Router A which checks if Router A is storing the failover information
        received from  Router B.The failover list must consist of the original connection info (from the connector)
        followed by the two items sent by the Router B (stored in cls.failover_list)
        The 'failoverUrls' is comma separated.
        """
        long_type = 'org.apache.qpid.dispatch.connector'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_qdmanage(query_command))
        expected = "amqp://127.0.0.1:" + str(FailoverTest.inter_router_port) + ", " + FailoverTest.failover_list

        self.assertEqual(expected, output[0]['failoverUrls'])

    def schedule_B_to_C_failover_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.check_C_connector).start()
                self.attempts += 1

    def check_C_connector(self):
        long_type = 'org.apache.qpid.dispatch.connector'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_qdmanage(query_command, address=self.routers[1].addresses[0]))

        expected = FailoverTest.backup_url  + ", " + "amqp://127.0.0.1:" + str(FailoverTest.inter_router_port) \
            + ", " + "amqp://third-host:5671"

        if output[0].get('failoverUrls') == expected:
            self.success = True
        else:
            self.schedule_B_to_C_failover_test()

    def can_terminate(self):
        if self.attempts == self.max_attempts:
            return True

        if self.success:
            return True

        return False

    def test_2_remove_router_B(self):
        """
        In this test, we are killing Router B. As a result, Router A should try to connect to Router C.
        Router C does NOT have a failover list, so the open frame that Router C sends to Router A will not contain
        the failover-server-list property..Hence the failoverUrls list will remain unchanged except that the order of
        the URLs would be different.
        """

        # First make sure there are no inter-router connections on router C
        outs = self.run_qdstat(['--connections'], address=self.routers[2].addresses[1])

        inter_router = 'inter-router' in outs
        self.assertFalse(inter_router)

        # Kill the router B
        FailoverTest.routers[0].teardown()

        # Schedule a test to make sure that the failover url is available
        # and Router C has an inter-router connection
        self.schedule_B_to_C_failover_test()

        while not self.can_terminate():
            pass

        self.assertTrue(self.success)

    def schedule_C_to_B_failover_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.check_B_connector).start()
                self.attempts += 1

    def check_B_connector(self):
        # Router A should now try to connect to Router B again since we killed Router C.
        long_type = 'org.apache.qpid.dispatch.connector'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_qdmanage(query_command, address=self.routers[1].addresses[0]))

        # The order that the URLs appear in the failoverUrls is important. This is the order in which the router
        # will attempt to make connections in case the existing connection goes down.

        expected = "amqp://127.0.0.1:" + str(FailoverTest.inter_router_port) + ", " + \
                   FailoverTest.failover_list + \
                   ', amqp://127.0.0.1:%d' % FailoverTest.my_server_port

        if output[0].get('failoverUrls') == expected:
            self.success = True
        else:
            self.schedule_C_to_B_failover_test()

    def test_3_reinstate_router_B(self):
        """
        In this test, we are restarting Router B and killing Router C. Router A should now try to connect back to
        Router B since it maintains the original connection info to Router B from the connector config information.
        Before starting Router B back again, we
        have a small config change to Router B  wherein we are adding a new failover url to the original list.
        This new failover url
        points to our own server which will accept connections. This server will actually be used in the next test
        but this test maskes sure that the new server url also shows up in the failoverUrls list.
        """
        FailoverTest.router('B', [
            ('router', {'mode': 'interior', 'id': 'B'}),
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': FailoverTest.inter_router_port,
                          'failoverUrls': FailoverTest.failover_list +  ', amqp://127.0.0.1:%d' % FailoverTest.my_server_port}),
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': FailoverTest.tester.get_port()}),
        ])

        FailoverTest.routers[3].wait_ready()

        # Kill the router C.
        # Now since Router B is up and running, router A should try to re-connect to Router B.
        # This will prove that the router A is preserving the original connector information specified in its config.
        FailoverTest.routers[2].teardown()

        self.success = False
        self.attempts = 0

        # Schedule a test to make sure that the failover url is available
        self.schedule_C_to_B_failover_test()

        while not self.can_terminate():
            pass

        self.assertTrue(self.success)

    def check_A_connector(self):
        # Router A should now try to connect to Router B again since we killed Router C.
        long_type = 'org.apache.qpid.dispatch.connector'
        query_command = 'QUERY --type=' + long_type
        output = json.loads(self.run_qdmanage(query_command, address=self.routers[1].addresses[0]))

        # The order that the URLs appear in the failoverUrls is important. This is the order in which the router
        # will attempt to make connections in case the existing connection goes down.
        expected = 'amqp://127.0.0.1:%d' % FailoverTest.my_server_port + ", " + "amqp://127.0.0.1:" + str(FailoverTest.inter_router_port)

        if output[0].get('failoverUrls') == expected:
            self.success = True
        else:
            self.schedule_B_to_my_server_failover_test()

    def schedule_B_to_my_server_failover_test(self):
        if self.attempts < self.max_attempts:
            if not self.success:
                Timer(self.timer_delay, self.check_A_connector).start()
                self.attempts += 1

    def test_4_remove_router_B_connect_to_my_server(self):
        """
        This test kills Router B again and makes sure that Router A now connects to our custom server that
        accepts connections. This custom server intentionally sends an empty list for failover-server-list
        Router A must look at this empty list and wipe out all failover information except the original connector information
        and the current connection info.
        """

        # Start MyServer
        proc = FailoverTest.tester.popen(
            [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'failoverserver.py'), '-a',
             'amqp://127.0.0.1:%d' % FailoverTest.my_server_port], expect=Process.RUNNING)

        # Kill the router B again
        FailoverTest.routers[3].teardown()

        self.success = False
        self.attempts = 0

        self.schedule_B_to_my_server_failover_test()

        while not self.can_terminate():
            pass

        self.assertTrue(self.success)


if __name__ == '__main__':
    unittest.main(main_module())

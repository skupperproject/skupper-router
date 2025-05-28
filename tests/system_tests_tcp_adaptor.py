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

import io
import json
import os
import re
import socket
import subprocess
import time
import traceback
from subprocess import PIPE
from subprocess import STDOUT
from typing import List, Optional, Mapping, Tuple

from proton import Message, Disposition
from proton.handlers import MessagingHandler
from proton.reactor import Container

from skupper_router.management.error import ForbiddenStatus

from system_test import Logger
from system_test import Process
from system_test import Qdrouterd
from system_test import QdManager
from system_test import TIMEOUT
from system_test import TestCase
from system_test import TestTimeout
from system_test import main_module
from system_test import unittest
from system_test import retry
from system_test import CONNECTION_TYPE, TCP_CONNECTOR_TYPE, TCP_LISTENER_TYPE
from system_test import ROUTER_LINK_TYPE, ROUTER_TYPE
from system_test import retry_assertion
from system_test import retry_exception
from system_test import CA_CERT, CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, \
    CLIENT_PRIVATE_KEY_NO_PASS, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PRIVATE_KEY_PASSWORD, \
    SERVER_PRIVATE_KEY_NO_PASS, BAD_CA_CERT
from http1_tests import wait_tcp_listeners_up

# Tests in this file are organized by classes that inherit TestCase.
# The first instance is TcpAdaptor(TestCase).
# The tests emit files that are named starting with 'TcpAdaptor'. This includes
# logs and shell scripts.
# Subsequent TestCase subclasses must follow this pattern and emit files named
# with the test class name at the beginning of the emitted files.

try:
    from TCP_echo_client import TcpEchoClient
    from TCP_echo_server import TcpEchoServer
except ImportError:
    class TCP_echo_client:
        pass

    class TCP_echo_server:
        pass


DISABLE_SELECTOR_TESTS = False
DISABLE_SELECTOR_REASON = ''
try:
    import selectors  # noqa F401: imported but unused (echo server and echo client import this, they run as subprocesses)  # pylint: disable=unused-import
except ImportError:
    DISABLE_SELECTOR_TESTS = True
    DISABLE_SELECTOR_REASON = "Python selectors module is not available on this platform."

# This code takes a wild guess how long an echo server must stall receiving
# input data before it fills the adaptor's flow control window in the host
# router on all the various CI systems out there.
CONN_STALL_DELAY_SECONDS = 1.0


def check_runners(dup_runners):
    finished_runners = []
    for dup_runner in dup_runners:
        # Gather only the clients that have already finished running.
        # Once the echo client receives all bytes from the echo server, keep_running is set
        # to False by the echo client.
        if not dup_runner.e_client.keep_running:
            finished_runners.append(dup_runner)
    print("len(finished_runners)=", len(finished_runners))
    for finished_runner in finished_runners:
        dup_runners.remove(finished_runner)
    return len(dup_runners) == 0


def ncat_available():
    try:
        return 0 == subprocess.check_call(['ncat', '--version'],
                                          stdout=subprocess.DEVNULL,
                                          stderr=subprocess.DEVNULL)
    except OSError:
        return False


#
# Test concurrent clients
#
class EchoClientRunner:
    """
    Launch an echo client upon construction.
    Provide poll interface for checking done/error.
    Provide wait/join to shut down.
    """

    def __init__(self, test_name, client_n, logger, client, server, size,
                 count,
                 print_client_logs=True,
                 timeout=TIMEOUT,
                 port_override=None,
                 test_ssl=False,
                 delay_close=False):
        """
        Launch an echo client upon construction.

        :param test_name: Unique name for log file prefix
        :param client_n: Client number for differentiating otherwise identical clients
        :param logger: parent logger for logging test activity vs. client activity
        :param client: router name to which the client connects
        :param server: name whose address the client is targeting
        :param size: length of messages in bytes
        :param count: number of messages to be sent/verified
        :param print_client_logs: verbosity switch
        :return Null if success else string describing error
        """
        self.test_name = test_name
        self.client_n = str(client_n)
        self.logger = logger
        self.client = client
        self.server = server
        self.size = size
        self.count = count
        self.timeout = timeout
        self.print_client_logs = print_client_logs
        self.client_final = False

        # Each router has a listener for the echo server attached to every router
        self.listener_port = TcpAdaptor.tcp_client_listener_ports[self.client][self.server] if port_override is None else port_override

        self.name = "%s_%s_%s_%s" % \
                    (self.test_name, self.client_n, self.size, self.count)
        self.client_prefix = "ECHO_CLIENT %s" % self.name

        parent_path = os.path.dirname(os.getcwd())
        self.client_logger = Logger(title=self.client_prefix,
                                    print_to_console=self.print_client_logs,
                                    save_for_dump=False,
                                    ofilename=os.path.join(parent_path, "setUpClass/TcpAdaptor_echo_client_%s.log" % self.name))
        ssl_info = None
        if test_ssl:
            ssl_info = {'CLIENT_CERTIFICATE': CLIENT_CERTIFICATE,
                        'CLIENT_PRIVATE_KEY': CLIENT_PRIVATE_KEY,
                        'CLIENT_PRIVATE_KEY_PASSWORD': CLIENT_PRIVATE_KEY_PASSWORD,
                        'CA_CERT': CA_CERT}

        try:
            self.e_client = TcpEchoClient(prefix=self.client_prefix,
                                          host='localhost',
                                          port=self.listener_port,
                                          size=self.size,
                                          count=self.count,
                                          timeout=self.timeout,
                                          logger=self.client_logger,
                                          ssl_info=ssl_info,
                                          delay_close=delay_close)

        except Exception as exc:
            self.e_client.error = "TCP_TEST TcpAdaptor_runner_%s failed. Exception: %s" % \
                                  (self.name, traceback.format_exc())
            self.logger.log(self.e_client.error)
            raise Exception(self.e_client.error)

    def client_error(self):
        return self.e_client.error

    def client_exit_status(self):
        return self.e_client.exit_status

    def client_running(self):
        return self.e_client.is_running

    def client_waiting_to_close(self):
        return self.e_client.is_waiting_to_close

    def wait(self):
        # wait for client to exit
        # Return None if successful wait/join/exit/close else error message
        result = None
        try:
            self.e_client.wait()

        except Exception as exc:
            self.e_client.error = "TCP_TEST EchoClient %s failed. Exception: %s" % \
                                  (self.name, traceback.format_exc())
            self.logger.log(self.e_client.error)
            result = self.e_client.error
        return result


class TcpAdaptorBase(TestCase):
    """
    6 edge routers connected via 3 interior routers.
    9 echo servers are connected via tcpConnector, one to each router.
    Each router has 10 listeners, one for each server and
    another for which there is no server.
    """
    #  +-------+    +---------+    +---------+    +---------+    +-------+
    #  |  EA1  |<-->|  INTA   |<==>|  INTB   |<==>|  INTC   |<-->|  EC1  |
    #  +-------+    |         |    |         |    |         |    +-------+
    #  +-------+    |         |    |         |    |         |    +-------+
    #  |  EA2  |<-->|         |    |         |    |         |<-->|  EC2  |
    #  +-------+    +---------+    +---------+    +---------+    +-------+
    #                                ^     ^
    #                                |     |
    #                          +-------+ +-------+
    #                          |  EB1  | |  EB2  |
    #                          +-------+ +-------+
    #
    # Each router tcp-connects to a like-named echo server.
    # Each router has tcp-listeners. The echo clients connect to these tcp-listeners
    # to talk to the respective echo server via the router network. The echo server echoes back
    # the data sent to it by the echo client.
    #
    #      +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+
    #   +--|tcp |-|tcp |-|tcp |-|tcp |-|tcp |-|tcp |-|tcp |-|tcp |-|tcp |--+
    #   |  |lsnr| |lsnr| |lsnr| |lsnr| |lsnr| |lsnr| |lsnr| |lsnr| |lsnr|  |
    #   |  |EA1 | |EA2 | |INTA| |EB1 | |EB2 | |INTB| |EC1 | |EC2 | |INTC|  |
    #   |  +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+  |
    #   |                                                               +---------+  +------+
    #   |          Router                                               | tcp     |  | echo |
    #   |          EA1                                                  |connector|->|server|
    #   |                                                               +---------+  | EA1  |
    #   |                                                                  |         +------+
    #   +------------------------------------------------------------------+
    #
    # Router EC2 has naughty, misbehaving echo servers:
    #  * conn_stall - delays before reading socket to force triggering
    #    backpressure via the adaptor's flow control window
    #
    # Routers EC2 has a TCP listener for conn_stall echo server.
    #  * Sending "large" messages through this listener should trigger
    #    the flow control window on router EC1.
    #  * A similar listener on INTA does *not* trigger flow control on EA1.

    # Allocate routers in this order
    router_order = ['INTA', 'INTB', 'INTC', 'EA1', 'EA2', 'EB1', 'EB2', 'EC1', 'EC2']

    # List indexed in router_order
    # First listener in each router is normal AMQP for test setup and mgmt.
    amqp_listener_ports = {}

    # Each router listens for TCP where the tcp-address is the router name.
    # Each router has N listeners, one for the echo server connected to each router.
    tcp_client_listener_ports = {}

    # Each router connects to an echo server
    tcp_server_listener_ports = {}

    # Each router has a TCP listener that has no associated server
    nodest_listener_ports = {}

    # Each router has a TCP listener that is associated with the ES_ALL address
    balanced_listener_ports = {}

    # Each router has a console listener
    # http_listener_ports = {}

    # TCP siteId for listeners and connectors
    site = "mySite"

    # Each router has an echo server to which it connects
    echo_servers = {}

    # Special echo servers
    echo_server_NS_CONN_STALL = None

    @classmethod
    def setUpClass(cls, test_ssl=False, encap="legacy"):
        """Start a router"""
        super(TcpAdaptorBase, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

        def router(name, mode, connection, extra=None, ssl=False, encapsulation="legacy"):
            """
            Launch a router through the system_test framework.
            For each router:
             * normal listener first
             #* http listener for console connections
             * tcp listener for 'nodest', which will never exist
             * tcp connector to echo server whose address is the same as this router's name
             * six tcp listeners, one for each server on each router on the network
            :param name: router name
            :param mode: router mode: interior or edge
            :param connection: list of router-level connection/listener tuples
            :param encapsulation: do we use the tcp legacy or the tcp_lite adaptor.
            :param extra: yet more configuation tuples. unused for now
            :param ssl: use an sslProfile on the connectors and listeners.

            :return:
            """
            tcp_listener_ssl_profile_name = 'tcp-listener-ssl-profile'
            if ssl:
                listener_props = {'host': "localhost",
                                  'port': cls.nodest_listener_ports[name],
                                  'address': 'nodest',
                                  'sslProfile': tcp_listener_ssl_profile_name,
                                  'encapsulation': encapsulation,
                                  'siteId': cls.site}
                listener_props_balanced_1 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][0],
                                             'address': 'ES_ALL_1',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_2 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][1],
                                             'address': 'ES_ALL_2',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_3 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][2],
                                             'address': 'ES_ALL_3',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_4 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][3],
                                             'address': 'ES_ALL_4',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}

                connector_props_balanced_1 = {'name': "balanced-ES_ALL_1",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_1',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_2 = {'name': "balanced-ES_ALL_2",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_2',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_3 = {'name': "balanced-ES_ALL_3",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_3',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_4 = {'name': "balanced-ES_ALL_4",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_4',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}

                connector_props_direct = {'host': "localhost",
                                          'port': cls.tcp_server_listener_ports[name],
                                          'address': 'ES_' + name,
                                          'sslProfile': 'tcp-connector-ssl-profile',
                                          'encapsulation': encapsulation,
                                          'siteId': cls.site}

            else:
                listener_props = {'host': "0.0.0.0",
                                  'port': cls.nodest_listener_ports[name],
                                  'address': 'nodest',
                                  'encapsulation': encapsulation,
                                  'siteId': cls.site}

                listener_props_balanced_1 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][0],
                                             'address': 'ES_ALL_1',
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_2 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][1],
                                             'address': 'ES_ALL_2',
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_3 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][2],
                                             'address': 'ES_ALL_3',
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}
                listener_props_balanced_4 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][3],
                                             'address': 'ES_ALL_4',
                                             'encapsulation': encapsulation,
                                             'siteId': cls.site}

                connector_props_balanced_1 = {'name': "balanced-ES_ALL_1",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_1',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_2 = {'name': "balancedES_ALL_2",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_2',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_3 = {'name': "balanced-ES_ALL_3",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_3',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}
                connector_props_balanced_4 = {'name': "balanced-ES_ALL_4",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_4',
                                              'encapsulation': encapsulation,
                                              'siteId': cls.site}

                connector_props_direct = {'host': "127.0.0.1",
                                          'port': cls.tcp_server_listener_ports[name],
                                          'address': 'ES_' + name,
                                          'encapsulation': encapsulation,
                                          'siteId': cls.site}
            if mode == "interior":
                router_dict = {'mode': mode, 'id': name, 'dataConnectionCount': '4', "helloMaxAgeSeconds": '10'}
            else:
                router_dict = {'mode': mode, 'id': name}
            config = [
                ('router', router_dict),
                ('listener', {'port': cls.amqp_listener_ports[name]}),
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
                ('tcpListener', listener_props),

                ('tcpListener', listener_props_balanced_1),
                ('tcpListener', listener_props_balanced_2),
                ('tcpListener', listener_props_balanced_3),
                ('tcpListener', listener_props_balanced_4),

                ('tcpConnector', connector_props_balanced_1),
                ('tcpConnector', connector_props_balanced_2),
                ('tcpConnector', connector_props_balanced_3),
                ('tcpConnector', connector_props_balanced_4),

                ('tcpConnector', connector_props_direct)
            ]

            if connection:
                config.extend(connection)

            listeners = []
            for rtr in cls.router_order:
                listener = {'host': "localhost",
                            'port': cls.tcp_client_listener_ports[name][rtr],
                            'address': 'ES_' + rtr,
                            'encapsulation': encapsulation,
                            'siteId': cls.site}
                if ssl:
                    listener['sslProfile'] = tcp_listener_ssl_profile_name
                tup = [(('tcpListener', listener))]
                listeners.extend(tup)
            config.extend(listeners)

            if extra:
                config.extend(extra)

            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        # monitor router memory usage:
        os.environ["SKUPPER_ROUTER_ALLOC_MONITOR_SECS"] = "10"

        cls.routers = []
        cls.test_ssl = test_ssl
        cls.encapsulation = encap
        # define logging levels. Set the following two flags to True
        # if you want to see copious logging from the echo client and echo server
        ###################################################################
        cls.print_logs_server = False
        cls.print_logs_client = False
        ###################################################################
        parent_path = os.path.dirname(os.getcwd())
        cls.logger = Logger(title="TcpAdaptor-testClass",
                            print_to_console=True,
                            save_for_dump=False,
                            ofilename=os.path.join(parent_path, "setUpClass/TcpAdaptor.log"))
        # Write a dummy log line for scraper.
        cls.logger.log("SERVER (info) Container Name: TCP_TEST")
        cls.ssl_info = None
        cls.client_ssl_info = {}
        if cls.test_ssl:
            cls.ssl_info = {'SERVER_CERTIFICATE': SERVER_CERTIFICATE,
                            'SERVER_PRIVATE_KEY': SERVER_PRIVATE_KEY_NO_PASS,
                            'CA_CERT': CA_CERT}
            cls.client_ssl_info = {'CLIENT_CERTIFICATE': CLIENT_CERTIFICATE,
                                   'CLIENT_PRIVATE_KEY': CLIENT_PRIVATE_KEY,
                                   'CLIENT_PRIVATE_KEY_PASSWORD': CLIENT_PRIVATE_KEY_PASSWORD,
                                   'BAD_CA_CERT': BAD_CA_CERT,
                                   'CLIENT_PRIVATE_KEY_NO_PASS': CLIENT_PRIVATE_KEY_NO_PASS,
                                   'CA_CERT': CA_CERT}

        # Start echo servers first, store their listening port numbers
        parent_path = os.path.dirname(os.getcwd())
        for rtr in cls.router_order:
            test_name = "TcpAdaptor"
            server_prefix = "ECHO_SERVER %s ES_%s" % (test_name, rtr)
            server_logger = Logger(title=test_name,
                                   print_to_console=cls.print_logs_server,
                                   save_for_dump=False,
                                   ofilename=os.path.join(parent_path, "setUpClass/TcpAdaptor_echo_server_%s.log" % rtr))
            cls.logger.log("TCP_TEST Launching echo server '%s'" % server_prefix)
            server = TcpEchoServer(prefix=server_prefix,
                                   port=0,
                                   logger=server_logger,
                                   ssl_info=cls.ssl_info)
            assert server.is_running
            cls.tcp_server_listener_ports[rtr] = server.port
            cls.echo_servers[rtr] = server

        # start special naughty servers that misbehave on purpose
        server_prefix = "ECHO_SERVER TcpAdaptor NS_EC2_CONN_STALL"
        server_logger = Logger(title="TcpAdaptor",
                               print_to_console=cls.print_logs_server,
                               save_for_dump=False,
                               ofilename=os.path.join(parent_path, "setUpClass/TcpAdaptor_echo_server_NS_CONN_STALL.log"))
        cls.logger.log("TCP_TEST Launching echo server '%s'" % server_prefix)
        server = TcpEchoServer(prefix=server_prefix,
                               port=0,
                               logger=server_logger,
                               conn_stall=CONN_STALL_DELAY_SECONDS,
                               ssl_info=cls.ssl_info)
        assert server.is_running
        cls.EC2_conn_stall_connector_port = server.port
        cls.echo_server_NS_CONN_STALL = server

        # Allocate a sea of router ports
        for rtr in cls.router_order:
            cls.amqp_listener_ports[rtr] = cls.tester.get_port()
            tl_ports = {}
            for tcp_listener in cls.router_order:
                tl_ports[tcp_listener] = cls.tester.get_port()
            cls.tcp_client_listener_ports[rtr] = tl_ports
            cls.nodest_listener_ports[rtr] = cls.tester.get_port()

            cls.balanced_ports = []
            for i in range(4):
                cls.balanced_ports.append(cls.tester.get_port())
            cls.balanced_listener_ports[rtr] = cls.balanced_ports

        inter_router_port_AB = cls.tester.get_port()
        cls.authenticate_peer_port = cls.tester.get_port()
        cls.wrong_path_in_ssl_profile_port = cls.tester.get_port()
        cls.INTA_edge_port = cls.tester.get_port()
        cls.INTA_conn_stall_listener_port = cls.tester.get_port()

        int_a_config = [('listener', {'role': 'inter-router', 'port': inter_router_port_AB}),
                        ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.INTA_edge_port}),
                        ('tcpListener', {'host': "0.0.0.0", 'port': cls.INTA_conn_stall_listener_port,
                                         'address': 'NS_EC2_CONN_STALL', 'encapsulation': cls.encapsulation, 'siteId': cls.site})]

        if cls.test_ssl:
            # This listener will be used to test the authenticatePeer functionality.
            int_a_config.append(('tcpListener',
                                 {'host': "localhost",
                                  'port': cls.authenticate_peer_port,
                                  'sslProfile': 'tcp-listener-ssl-profile',
                                  'authenticatePeer': 'yes',
                                  'address': 'ES_INTA',
                                  'encapsulation': cls.encapsulation,
                                  'siteId': cls.site}))

        # Launch the routers using the sea of router ports
        cls.logger.log("TCP_TEST Launching interior routers")
        router('INTA', 'interior', int_a_config, ssl=cls.test_ssl, encapsulation=cls.encapsulation)
        inter_router_port_BC = cls.tester.get_port()
        cls.INTB_edge_port = cls.tester.get_port()
        router('INTB', 'interior',
               [('connector', {'role': 'inter-router', 'port': inter_router_port_AB}),
                ('listener', {'role': 'inter-router', 'port': inter_router_port_BC}),
                ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)

        cls.INTC_edge_port = cls.tester.get_port()
        router('INTC', 'interior',
               [('connector', {'role': 'inter-router', 'port': inter_router_port_BC}),
                ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)

        cls.logger.log("TCP_TEST Launching edge routers")
        router('EA1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTA_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)
        router('EA2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTA_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)
        router('EB1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)
        router('EB2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)
        router('EC1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port})], ssl=cls.test_ssl,
               encapsulation=cls.encapsulation)
        cls.EC2_conn_stall_listener_port = cls.tester.get_port()
        router('EC2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port}),
                ('tcpConnector', {'host': "127.0.0.1", 'port': cls.EC2_conn_stall_connector_port,
                                  'address': 'NS_EC2_CONN_STALL', 'encapsulation': cls.encapsulation,
                                  'siteId': cls.site}),
                ('tcpListener', {'host': "0.0.0.0", 'port': cls.EC2_conn_stall_listener_port,
                                 'address': 'NS_EC2_CONN_STALL', 'encapsulation': cls.encapsulation,
                                 'siteId': cls.site})],
               ssl=cls.test_ssl, encapsulation=cls.encapsulation)

        cls.INTA = cls.routers[0]
        cls.INTB = cls.routers[1]
        cls.INTC = cls.routers[2]
        cls.EA1 = cls.routers[3]
        cls.EA2 = cls.routers[4]
        cls.EB1 = cls.routers[5]
        cls.EB2 = cls.routers[6]
        cls.EC1 = cls.routers[7]
        cls.EC2 = cls.routers[8]

        cls.router_dict = {}
        cls.router_dict['INTA'] = cls.INTA
        cls.router_dict['INTB'] = cls.INTB
        cls.router_dict['INTC'] = cls.INTC
        cls.router_dict['EA1'] = cls.EA1
        cls.router_dict['EA2'] = cls.EA2
        cls.router_dict['EB1'] = cls.EB1
        cls.router_dict['EB2'] = cls.EB2
        cls.router_dict['EC1'] = cls.EC1
        cls.router_dict['EC2'] = cls.EC2

        cls.logger.log("TCP_TEST INTA waiting for connection to INTB")
        cls.INTA.wait_router_connected('INTB')
        cls.logger.log("TCP_TEST INTB waiting for connection to INTA")
        cls.INTB.wait_router_connected('INTA')
        cls.logger.log("TCP_TEST INTB waiting for connection to INTC")
        cls.INTB.wait_router_connected('INTC')
        cls.logger.log("TCP_TEST INTC waiting for connection to INTB")
        cls.INTC.wait_router_connected('INTB')

        # Check to make sure that the routers are connected to their two
        # respective edges.
        cls.INTA.is_edge_routers_connected(num_edges=2)
        cls.INTB.is_edge_routers_connected(num_edges=2)
        cls.INTC.is_edge_routers_connected(num_edges=2)

        # Create a scoreboard for the ports
        p_out = []
        for rtr in cls.router_order:
            p_out.append("%s_amqp=%d" %
                         (rtr, cls.amqp_listener_ports[rtr]))
            p_out.append("%s_echo_server=%d" %
                         (rtr, cls.tcp_server_listener_ports[rtr]))
            for tcp_listener in cls.router_order:
                p_out.append("%s_echo_listener_for_%s=%d" %
                             (rtr, tcp_listener, cls.tcp_client_listener_ports[rtr][tcp_listener]))
            p_out.append("%s_nodest_listener=%d" %
                         (rtr, cls.nodest_listener_ports[rtr]))
            # p_out.append("%s_http_listener=%d" %
            #             (rtr, cls.http_listener_ports[rtr]))
        p_out.append("inter_router_port_AB=%d" % inter_router_port_AB)
        p_out.append("inter_router_port_BC=%d" % inter_router_port_BC)
        p_out.append("INTA_edge_port=%d" % cls.INTA_edge_port)
        p_out.append("INTB_edge_port=%d" % cls.INTB_edge_port)
        p_out.append("INTC_edge_port=%d" % cls.INTC_edge_port)
        p_out.append("EC2_conn_stall_connector_port%d" % cls.EC2_conn_stall_connector_port)
        p_out.append("INTA_conn_stall_listener_port%d" % cls.INTA_conn_stall_listener_port)
        p_out.append("EC2_conn_stall_listener_port%d" % cls.EC2_conn_stall_listener_port)

        # write to log
        for line in p_out:
            cls.logger.log("TCP_TEST %s" % line)

        # write to shell script
        parent_path = os.path.dirname(os.getcwd())
        file_name = os.path.join(parent_path, "setUpClass/TcpAdaptor-ports.sh")
        with open(file_name, 'w') as o_file:
            for line in p_out:
                o_file.write("set %s\n" % line)

        # Write a script to run scraper on this test's log files
        scraper_abspath = os.path.join(os.environ.get('BUILD_DIR'), 'tests', 'scraper', 'scraper.py')
        logs_dir     = os.path.join(parent_path, "setUpClass")
        main_log     = "TcpAdaptor.log"
        echo_logs    = "TcpAdaptor_echo*"
        big_test_log = "TcpAdaptor_all.log"
        int_logs     = "I*.log"
        edge_logs    = "E*.log"
        log_modules_spec = "--log-modules TCP_ADAPTOR,TCP_TEST,ECHO_SERVER,ECHO_CLIENT"
        html_output  = "TcpAdaptor.html"
        with open(os.path.join(parent_path, "setUpClass/TcpAdaptor-run-scraper.sh"), 'w') as o_file:
            o_file.write("#!/bin/bash\n\n")
            o_file.write("# Script to run scraper on test class TcpAdaptor test result\n")
            o_file.write("# cd into logs directory\n")
            o_file.write("cd %s\n\n" % logs_dir)
            o_file.write("# Concatenate test class logs into single file\n")
            o_file.write("cat %s %s > %s\n\n" % (main_log, echo_logs, big_test_log))
            o_file.write("# run scraper\n")
            o_file.write("python %s %s -f %s %s %s > %s\n\n" %
                         (scraper_abspath, log_modules_spec, int_logs, edge_logs, big_test_log, html_output))
            o_file.write("echo View the results by opening the html file\n")
            o_file.write("echo     firefox %s" % (os.path.join(logs_dir, html_output)))

        # wait for server addresses (mobile ES_<rtr>) to propagate to all interior routers
        interior_rtrs = [rtr for rtr in cls.router_order if rtr.startswith('I')]
        poll_loops = 100
        poll_loop_delay = 0.5  # seconds
        found_all = False
        while not found_all:
            found_all = True
            cls.logger.log("TCP_TEST Poll wait for echo server addresses to propagate")
            for rtr in interior_rtrs:
                # query each interior for addresses
                p = Process(
                    ['skstat', '-b', str(cls.router_dict[rtr].addresses[0]), '-a'],
                    name='skstat-snap1', stdout=PIPE, expect=Process.EXIT_OK,
                    universal_newlines=True)
                out = p.communicate()[0]
                # examine what this router can see; signal poll loop to continue or not
                lines = out.split("\n")
                server_lines = [line for line in lines if "mobile" in line and "ES_" in line]
                if len(server_lines) < len(cls.router_order):
                    found_all = False
                    seen = []
                    for line in server_lines:
                        flds = line.split()
                        seen.extend([fld for fld in flds if fld.startswith("ES_")])
                    unseen = [srv for srv in cls.router_order if "ES_" + srv not in seen]
                    cls.logger.log("TCP_TEST Router %s sees only %d of %d addresses. Waiting for %s" %
                                   (rtr, len(server_lines), len(cls.router_order), unseen))
                if poll_loops == 1:
                    # last poll loop
                    for line in lines:
                        cls.logger.log("TCP_TEST Router %s : %s" % (rtr, line))
            poll_loops -= 1
            if poll_loops == 0:
                assert False, "TCP_TEST TCP_Adaptor test setup failed. Echo tests never executed."
            else:
                time.sleep(poll_loop_delay)
        cls.logger.log("TCP_TEST Done poll wait")

        cls.logger.log("TCP_TEST waiting for all tcpListeners to activate...")

        for rtr in cls.routers:
            mgmt = rtr.management
            listeners_ready = False
            while not listeners_ready:
                listeners = mgmt.query(type=TCP_LISTENER_TYPE,
                                       attribute_names=["operStatus", "name",
                                                        "address"]).get_dicts()
                listeners_ready = True
                for listener in listeners:
                    if listener['address'] != 'nodest':
                        if listener['operStatus'] != 'up':
                            listeners_ready = False
                            cls.logger.log("Listener %s for %s is not active, retrying..." %
                                           (listener['name'],
                                            listener['address']))
                            time.sleep(0.25)
                            break

        cls.logger.log("^^^^^^^^^^^^^^^TCP_TEST All tcpListeners are active^^^^^^^^^^^^^")

    @classmethod
    def tearDownClass(cls):
        # stop echo servers
        for rtr in cls.router_order:
            server = cls.echo_servers.get(rtr)
            if server is not None:
                if cls.test_ssl:
                    msg = "TCP_TEST Stopping TLS enabled echo server "
                else:
                    msg = "TCP_TEST Stopping echo server "
                cls.logger.log(msg +  "ES_%s" % rtr)
                server.wait()
        if cls.echo_server_NS_CONN_STALL is not None:
            cls.logger.log("TCP_TEST Stopping echo server NS_EC2_CONN_STALL ")
            cls.echo_server_NS_CONN_STALL.wait()
        super(TcpAdaptorBase, cls).tearDownClass()

    def run_skmanage(self, cmd, input=None, expect=Process.EXIT_OK, address=None, router='INTA'):
        p = self.popen(
            ['skmanage'] + cmd.split(' ') + ['--bus', address or str(self.router_dict[router].addresses[0]),
                                             '--indent=-1', '--timeout', str(TIMEOUT)],
            stdin=PIPE, stdout=PIPE, stderr=STDOUT, expect=expect,
            universal_newlines=True)
        out = p.communicate(input)[0]
        try:
            p.teardown()
        except Exception as e:
            raise Exception(out if out else str(e))
        return out


class CommonTcpTests:
    class EchoPair():
        """
        For the concurrent tcp tests this class describes one of the client-
        server echo pairs and the traffic pattern between them.
        """

        def __init__(self, client_rtr, server_rtr, sizes=None, counts=None):
            self.client_rtr = client_rtr
            self.server_rtr = server_rtr
            self.sizes = [1] if sizes is None else sizes
            self.counts = [1] if counts is None else counts

        def __repr__(self):
            return 'EchoPair(client router=%s, server router=%s)' % (self.client_rtr.name, self.server_rtr.name)

    def do_tcp_echo_n_routers(self, test_name, echo_pair_list, test_ssl=False):
        """
        Launch all the echo pairs defined in the list
        Wait for completion.
        :param test_name test name
        :param echo_pair_list list of EchoPair objects describing the test
        :param test_ssl should this function use ssl ?
        :return: None if success else error message for ctest
        """
        self.logger.log("TCP_TEST %s Start do_tcp_echo_n_routers" % (test_name))
        result = None
        runners = []
        dup_runners = []
        client_num = 0

        try:
            # Launch the runners
            for echo_pair in echo_pair_list:
                client = echo_pair.client_rtr.name
                server = echo_pair.server_rtr.name
                for size in echo_pair.sizes:
                    for count in echo_pair.counts:
                        over_tls = ' over TLS' if test_ssl else ''
                        log_msg = "TCP_TEST" +  over_tls + " %s Running pair %d %s->%s size=%d count=%d" % \
                                  (test_name, client_num, client, server, size, count)
                        self.logger.log(log_msg)
                        runner = EchoClientRunner(test_name, client_num,
                                                  self.logger,
                                                  client, server, size, count,
                                                  self.print_logs_client,
                                                  test_ssl=test_ssl)
                        dup_runners.append(runner)
                        runners.append(runner)
                        client_num += 1

            # check if every echo runner has completed and allow TIMEOUT
            # seconds for the operation to complete.
            retry(lambda: check_runners(dup_runners))

            # Loop until timeout, error, or completion
            while result is None:
                # Make sure servers are still up
                for rtr in TcpAdaptor.router_order:
                    es = TcpAdaptor.echo_servers[rtr]
                    if es.error is not None:
                        self.logger.log("TCP_TEST %s Server %s stopped with error: %s" %
                                        (test_name, es.prefix, es.error))
                        result = es.error
                        break
                if result is not None:
                    break

                # Check for completion or runner error
                complete = True
                for runner in runners:
                    if not runner.client_final:
                        error = runner.client_error()
                        if error is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with error: %s" %
                                            (test_name, runner.name, error))
                            result = error
                            runner.client_final = True
                            break
                        status = runner.client_exit_status()
                        if status is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with status: %s" %
                                            (test_name, runner.name, status))
                            result = status
                            runner.client_final = True
                            break
                        running = runner.client_running()
                        if running:
                            complete = False
                        else:
                            self.logger.log("TCP_TEST %s Client %s exited normally" %
                                            (test_name, runner.name))
                            runner.client_final = True
                if complete and result is None:
                    break

            # Wait/join all the runners
            for runner in runners:
                runner.wait()

            if result is not None:
                self.logger.log("TCP_TEST %s failed: %s" % (test_name, result))
            else:
                self.logger.log(("TCP_TEST %s SUCCESS " + over_tls) % test_name)

        except Exception as exc:
            result = "TCP_TEST %s failed. Exception: %s" % \
                (test_name, traceback.format_exc())

        return result

    def do_tcp_echo_singleton(self, test_name, client, server, size, count, echo_port):
        """
        Launch a single echo client to the echo_port
        Wait for completion.
        Note that client and server do not define the port that the echo client
        must connect to. That is overridden by echo_port. Still client and server
        are passed to the EchoClientRunner
        :param test_name test name
        :param client router to which echo client attaches
        :param server router that has the connector to the echo server
        :param size size of message to be echoed
        :param count number of messages to be echoed
        :param echo_port the router network listener port
        :return: None if success else error message for ctest
        """
        self.logger.log("TCP_TEST %s Start do_tcp_echo_singleton" % test_name)
        result = None
        runners = []
        dup_runners = []
        client_num = 0
        start_time = time.time()

        try:
            # Launch the runner
            log_msg = "TCP_TEST %s Running singleton %d %s->%s port %d, size=%d count=%d" % \
                      (test_name, client_num, client.name, server.name, echo_port, size, count)
            self.logger.log(log_msg)
            runner = EchoClientRunner(test_name, client_num, self.logger,
                                      client.name, server.name, size, count,
                                      self.print_logs_client,
                                      port_override=echo_port)
            dup_runners.append(runner)
            runners.append(runner)
            client_num += 1

            retry(lambda: check_runners(dup_runners))

            # Loop until timeout, error, or completion
            while result is None:
                # Make sure servers are still up
                for rtr in TcpAdaptor.router_order:
                    es = TcpAdaptor.echo_servers[rtr]
                    if es.error is not None:
                        self.logger.log("TCP_TEST %s Server %s stopped with error: %s" %
                                        (test_name, es.prefix, es.error))
                        result = es.error
                        break
                if result is not None:
                    break

                # Check for completion or runner error
                complete = True
                for runner in runners:
                    if not runner.client_final:
                        error = runner.client_error()
                        if error is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with error: %s" %
                                            (test_name, runner.name, error))
                            result = error
                            runner.client_final = True
                            break
                        status = runner.client_exit_status()
                        if status is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with status: %s" %
                                            (test_name, runner.name, status))
                            result = status
                            runner.client_final = True
                            break
                        running = runner.client_running()
                        if running:
                            complete = False
                        else:
                            self.logger.log("TCP_TEST %s Client %s exited normally" %
                                            (test_name, runner.name))
                            runner.client_final = True
                if complete and result is None:
                    self.logger.log("TCP_TEST %s SUCCESS" %
                                    test_name)
                    break

            # Wait/join all the runners
            for runner in runners:
                runner.wait()

            if result is not None:
                self.logger.log("TCP_TEST %s failed: %s" % (test_name, result))

        except Exception as exc:
            result = "TCP_TEST %s failed. Exception: %s" % \
                (test_name, traceback.format_exc())

        return result

    def all_balanced_connector_stats(self, iter_num):
        """
        Query each router for the connection counts for the balanced connector.
        Return an array of integers with the router stats.
        """
        counts = []
        query_command = 'QUERY --type=tcpConnector'
        for router in self.router_dict.keys():
            outputs = json.loads(self.run_skmanage(query_command, router=router))
            for output in outputs:
                if output['address'] == "ES_ALL_" + str(iter_num + 1):
                    counts.append(output["connectionsOpened"])
        return counts

    def do_tcp_balance_test(self, test_name, client, count, iter_num, test_ssl=False):
        """
        Launch 'count' connections from a single location.
        Wait for stability (all connections established)
        Test that every instance of the echo server has at least one connection open.
        Close the connections
        :param test_name test name
        :param client router to which echo clients attach
        :param count number of connections to be established
        :return: None if success else error message for ctest
        """
        self.logger.log("TCP_TEST %s do_tcp_balance_test (ingress: %s connections: %d)" % (test_name, client, count))
        result = None
        runners = []
        dup_runners = []

        # Collect a baseline of the connection counts for the balanced connectors
        baseline = self.all_balanced_connector_stats(iter_num)

        try:
            # Launch the runners
            listener_port = self.balanced_listener_ports[client][iter_num]
            for client_num in range(count):
                over_tls = ' over TLS' if test_ssl else ''
                log_msg = "TCP_TEST " +  over_tls + " %s Running client %d connecting to router %s, port=%d" % \
                          (test_name, client_num + 1, client, listener_port)
                self.logger.log(log_msg)
                runner = EchoClientRunner(test_name, client_num + 1,
                                          self.logger,
                                          None,
                                          None,
                                          3,
                                          1,
                                          print_client_logs=False,
                                          port_override=listener_port,
                                          test_ssl=test_ssl,
                                          delay_close=True)
                dup_runners.append(runner)
                runners.append(runner)

            retry(lambda: check_runners(dup_runners))

            # Loop until timeout, error, or completion
            while result is None:
                # Make sure servers are still up
                for rtr in TcpAdaptor.router_order:
                    es = TcpAdaptor.echo_servers[rtr]
                    if es.error is not None:
                        self.logger.log("TCP_TEST %s Server %s stopped with error: %s" %
                                        (test_name, es.prefix, es.error))
                        result = es.error
                        break
                if result is not None:
                    break

                # Check for completion or runner error
                complete = True
                for runner in runners:
                    if not runner.client_final:
                        error = runner.client_error()
                        if error is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with error: %s" %
                                            (test_name, runner.name, error))
                            result = error
                            runner.client_final = True
                            break
                        status = runner.client_exit_status()
                        if status is not None:
                            self.logger.log("TCP_TEST %s Client %s stopped with status: %s" %
                                            (test_name, runner.name, status))
                            result = status
                            runner.client_final = True
                            break
                        running = runner.client_running() and not runner.client_waiting_to_close()
                        if running:
                            complete = False
                        else:
                            self.logger.log("TCP_TEST %s Client %s exited normally" %
                                            (test_name, runner.name))
                            runner.client_final = True
                if complete and result is None:
                    break

            # Wait/join all the runners
            for runner in runners:
                runner.wait()

            # Verify that all balanced connectors saw at least one new connection
            if result is None:
                metrics = self.all_balanced_connector_stats(iter_num)
                diffs = {}
                fail = False
                for i in range(len(metrics)):
                    diff = metrics[i] - baseline[i]
                    diffs[self.router_order[i]] = diff
                    if diff == 0:
                        fail = True
                if fail:
                    result = "At least one server did not receive a connection: origin=%s: counts=%s" % (client, diffs)
                else:
                    self.logger.log("TCP_TEST origin=%s: counts=%s" % (client, diffs))

            if result is not None:
                self.logger.log("TCP_TEST %s failed: %s" % (test_name, result))
            else:
                self.logger.log(("TCP_TEST %s SUCCESS " + over_tls) % test_name)

        except Exception as exc:
            result = "TCP_TEST %s failed. Exception: %s" % \
                (test_name, traceback.format_exc())

        return result

    #
    # Tests run by ctest
    #
    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_01_tcp_basic_connectivity(self):
        """
        Echo a series of 1-byte messages, one at a time, to prove general connectivity.
        Every listener is tried. Proves every router can forward to servers on
        every other router.
        """
        for l_rtr in self.router_order:
            for s_rtr in self.router_order:
                name = "test_01_tcp_%s_%s" % (l_rtr, s_rtr)
                self.logger.log("TCP_TEST test_01_tcp_basic_connectivity Start %s" % name)
                pairs = [self.EchoPair(self.router_dict[l_rtr], self.router_dict[s_rtr])]
                result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
                if result is not None:
                    print(result)
                assert result is None, "TCP_TEST test_01_tcp_basic_connectivity Stop %s FAIL: %s" % (name, result)
                self.logger.log("TCP_TEST test_01_tcp_basic_connectivity Stop %s SUCCESS" % name)

    # larger messages
    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_10_tcp_INTA_INTA_100(self):
        name = "test_10_tcp_INTA_INTA_100"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[100])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_11_tcp_INTA_INTA_1000(self):
        name = "test_11_tcp_INTA_INTA_1000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[1000])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_12_tcp_INTA_INTA_500000(self):
        name = "test_12_tcp_INTA_INTA_500000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[500000])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_13_tcp_EA1_EC2_500000(self):
        name = "test_13_tcp_EA1_EC2_500000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA1, self.EC2, sizes=[500000])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    # This test sends and receives 2 million bytes from edge router to edge router
    # which will make the code go in and out of the TCP window.
    def test_14_tcp_EA2_EC1_2000000(self):
        name = "test_14_tcp_EA2_EC1_2000000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA2, self.EC1, sizes=[2000000])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_20_tcp_connect_disconnect(self):
        name = "test_20_tcp_connect_disconnect"
        # This test starts an echo pair on Router INTA. The echo client opens a connection
        # to INTA and immediately closes the connection without sending any data (sizes=[0])
        # The router *might* open a connection to the echo server
        # in some cases, and in other cases, the router might sense that the client connection has
        # already been closed by the echo client without sending any data and hence the router will
        # not bother to open a connection on the server side. This really depends on how fast the router
        # gets the disconnect event from the proton raw connection API.
        # The connectionsOpened and connectionsClosed counters might not match depending on if the
        # server side connection was created or not.
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[0])]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        # TODO: This test passes but in passing router INTA crashes undetected.
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    # concurrent messages
    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_50_concurrent_interior(self):
        name = "test_50_concurrent_interior"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA),
                 self.EchoPair(self.INTB, self.INTB)]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_50_concurrent_edges(self):
        name = "test_50_concurrent_edges"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA1, self.EA2),
                 self.EchoPair(self.EA2, self.EC2)]
        result = self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        if result is not None:
            print(result)
        assert result is None, "TCP_TEST Stop %s FAIL: %s" % (name, result)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    def _ncat_runner(self, name, client, server, data=b'abcd', port=None, ssl_info=None):
        def get_full_test_name(test_name, client_name, server_name):
            full_test_name = "%s_%s_%s" % (test_name, client_name, server_name)
            return full_test_name

        if not ssl_info:
            ssl_info = {}
            if self.client_ssl_info.get('CA_CERT'):
                ssl_info['CA_CERT'] = self.client_ssl_info.get('CA_CERT')

        out, err = self.ncat(port=port if port else TcpAdaptor.tcp_client_listener_ports[client][server],
                             logger=self.logger,
                             name=get_full_test_name(name, client, server),
                             expect=Process.EXIT_OK,
                             timeout=TIMEOUT,
                             data=data,
                             ssl_info=ssl_info)
        return out, err

    # half-closed handling
    # Each ncat test will run in its own test.
    # This makes it easier to identify which test failed.
    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_70_half_closed_INTA_INTA(self):
        name = "test_70_half_closed_INTA_INTA"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "INTA", "INTA", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_71_half_closed_INTA_INTB(self):
        name = "test_71_half_closed_INTA_INTB"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "INTA", "INTB", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_72_half_closed_INTA_INTC(self):
        name = "test_72_half_closed_INTA_INTC"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "INTA", "INTC", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_73_half_closed_EA1_EA1(self):
        name = "test_73_half_closed_EA1_EA1"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "EA1", "EA1", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_74_half_closed_EA1_EB1(self):
        name = "test_74_half_closed_EA1_EB1"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "EA1", "EB1", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_75_half_closed_EA1_EC2(self):
        name = "test_75_half_closed_EA1_EC2"
        self.logger.log("TCP_TEST Start %s" % name)
        data = b'abcd'
        out, _ = self._ncat_runner(name, "EA1", "EC2", data)
        self.assertEqual(data, out, f"ncat command returned invalid data, expected {data} but got {out}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_76_half_closed_EA1_EC2_large_message(self):
        name = "test_76_half_closed_EA1_EC2_large_message"
        self.logger.log("TCP_TEST Start %s" % name)
        large_msg = b'G' * 40000 + b'END OF TRANSMISSION'
        out, _ = self._ncat_runner(name, "EA1", "EC2", large_msg)
        self.assertEqual(large_msg, out, f"ncat command returned invalid data, expected {len(large_msg)} but got {len(out)}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(not ncat_available() or DISABLE_SELECTOR_TESTS, "Ncat utility is not available or selector tests disabled")
    def test_77_half_closed_INTA_INTC_large_message(self):
        name = "test_77_half_closed_INTA_INTC_large_message"
        self.logger.log("TCP_TEST Start %s" % name)
        large_msg = b'T' * 40000 + b'END OF TRANSMISSION'
        out, _ = self._ncat_runner(name, "INTA", "INTC", large_msg)
        self.assertEqual(large_msg, out, f"ncat command returned invalid data, expected {len(large_msg)} but got {len(out)}")
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    # connection balancing
    def test_80_balancing(self):
        tname = "test_80_balancing connections"
        self.logger.log(tname + " START")
        iterations = [('EA1', 94), ('INTA', 63), ('EB2', 28), ('INTB', 18)]
        iter_num = 0
        for p in iterations:
            result = self.do_tcp_balance_test(tname, p[0], p[1], iter_num, test_ssl=self.test_ssl)
            iter_num = iter_num + 1
            self.assertIsNone(result)
        self.logger.log(tname + " SUCCESS")

    # connector/listener stats
    def test_90_stats(self):
        tname = "test_90 check stats in skmanage"
        self.logger.log(tname + " START")
        # Verify listener stats

        query_command = 'QUERY --type=tcpListener'
        outputs = json.loads(self.run_skmanage(query_command))
        es_inta_connections_opened = 0
        for output in outputs:
            if output['address'].startswith("ES"):
                if output['address'] == 'ES_INTA':
                    es_inta_connections_opened += output["connectionsOpened"]
                # Check only echo server listeners
                assert "connectionsOpened" in output
                # There is a listener with authenticatePeer:yes and we have not run any tests on it yet, so
                # it is allowed to have zero connectionsOpened
                if output['address'] != 'ES_INTA' and output['address'] != 'ES_BAD_CONNECTOR_CERT_INTA' \
                        and output['address'] != 'ES_GOOD_CONNECTOR_CERT_INTA' \
                        and 'ES_ALL' not in output['address']:
                    assert output["connectionsOpened"] > 0
                assert output["bytesIn"] == output["bytesOut"]

        # The connections opened count is 7 only if the ncat tests were run.
        # The ncat tests will only be run if ncat_available() is True.
        # https://github.com/skupperproject/skupper-router/issues/1019
        if ncat_available():
            self.assertEqual(es_inta_connections_opened, 7)

        # Verify connector stats
        query_command = 'QUERY --type=tcpConnector'
        outputs = json.loads(self.run_skmanage(query_command))
        for output in outputs:
            assert output['address'].startswith("ES")
            assert "connectionsOpened" in output
            assert output["connectionsOpened"] > 0

            # egress_dispatcher connection opens and should never close
            if ncat_available():
                # See the comments in test_20_tcp_connect_disconnect()
                self.assertIn(output["connectionsOpened"], (output["connectionsClosed"],
                                                            output["connectionsClosed"] + 1,
                                                            output["connectionsClosed"] + 2))
            assert output["bytesIn"] == output["bytesOut"]
        self.logger.log(tname + " SUCCESS")

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_9999_memory_metrics(self):
        """
        Take advantage of the long running TCP test to verify that alloc_pool
        metrics have been correctly written to the logs
        """
        def _poll_logs(router, regex_mem, regex_action):
            last_mem_match = None  # match the start of the alloc log line
            last_action_match = None  # match the qdr_action_t entry in the log line
            with open(router.logfile_path, 'rt') as log_file:
                for line in log_file:
                    m = mem_re.search(line)
                    if m:
                        last_mem_match = m
                    m = action_re.search(line)
                    if m:
                        last_action_match = m
            if last_mem_match is None:
                print("failed to find alloc_pool output, retrying...", flush=True)
                return False
            if last_action_match is None:
                print("failed to find qdr_action_t entry, retrying...", flush=True)
                return False

            # Sanity check that metrics are present:

            # match = 'ram:62.49GiB vm:20.00TiB rss:58.88MiB pool:3.70KiB'
            mems = last_mem_match.group().strip().split()
            for mem in mems:
                name, value = mem.split(':')
                if name not in ["ram", "vm", "rss", "pool"]:
                    print(f"failed to find {name} metric, retrying...", flush=True)
                    return False
                value = int(value.split('.')[0])
                if value <= 0:
                    print(f"Expected nonzero {name} counter, got {value}, retrying...",
                          flush=True)
                    return False
            # match = ' qdr_action_t:192:0'
            name, in_use, in_free = last_action_match.group().strip().split(':')
            if name != "qdr_action_t":
                print(f"Name mismatch: {name} is not 'qdr_action_t, retrying...",
                      flush=True)
                return False
            if int(in_use) + int(in_free) <= 0:
                print(f"zero qdr_action_ts alloced? in_use={in_use} in_free={in_free}")
                return False
            return True

        mem_re = re.compile(r' ram:[0-9]+\.[0-9]+[BKMGTi]+ vm:[0-9]+\.[0-9]+[BKMGTi]+ rss:[0-9]+\.[0-9]+[BKMGTi]+ pool:[0-9]+\.[0-9]+[BKMGTi]+')
        action_re = re.compile(r' qdr_action_t:[0-9]+:[0-9]+')
        for router in self.routers:
            retry(lambda rtr=router, mre=mem_re, are=action_re: _poll_logs(rtr, mre, are),
                  delay=0.5, max_delay=5.0)


class TcpAdaptor(TcpAdaptorBase, CommonTcpTests):
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptor, cls).setUpClass(test_ssl=False)


class TcpAdaptorLite(TcpAdaptorBase, CommonTcpTests):
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorLite, cls).setUpClass(test_ssl=False, encap="lite")


class TcpAdaptorStuckDeliveryTest(TestCase):
    """
    Verify that the routers stuck delivery detection is not applied to TCP
    deliveries.  See Dispatch-2036.
    """
    @classmethod
    def setUpClass(cls, encap='legacy', test_name='TCPStuckDeliveryTest'):
        super(TcpAdaptorStuckDeliveryTest, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

        cls.encapsulation = encap
        cls.test_name = test_name

        # Topology: linear.
        # tcp-client->edge1->interior1->tcp-server

        cls.edge_tcp_listener_port = cls.tester.get_port()
        cls.interior_edge_listener_port = cls.tester.get_port()
        cls.interior_tcp_connector_port = cls.tester.get_port()

        def router(cls, name, mode, config):
            base_config = [
                ('router', {'mode': mode,
                            'id': name}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port()}),
                ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ]
            config = base_config + config
            config = Qdrouterd.Config(config)
            return cls.tester.qdrouterd(name, config, wait=False,
                                        # enable stuck delivery short timer
                                        # (3 seconds)
                                        cl_args=['-T'])

        cls.i_router = router(cls,
                              "%s_I" % cls.test_name,
                              "interior",
                              [('listener', {'role': 'edge',
                                             'host': '0.0.0.0',
                                             'port':
                                             cls.interior_edge_listener_port}),
                               ('tcpConnector', {'host': "127.0.0.1",
                                                 'port':
                                                 cls.interior_tcp_connector_port,
                                                 'address': 'nostuck',
                                                 'encapsulation': cls.encapsulation})])
        cls.i_router.wait_ready()
        cls.e_router = router(cls,
                              "%s_E" % cls.test_name,
                              "edge",
                              [('tcpListener', {'host': "0.0.0.0",
                                                'port':
                                                cls.edge_tcp_listener_port,
                                                'address': 'nostuck',
                                                'encapsulation': cls.encapsulation}),
                               ('connector', {'role': 'edge',
                                              'port':
                                              cls.interior_edge_listener_port})])
        cls.e_router.wait_ready()

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_01_ignore_stuck_deliveries(self):
        """
        Create a TCP delivery, wait for it to be classified as stuck and ensure
        that no stuck delivery logs are posted
        """

        # step 1: create a TCP listener for interior to connect to
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.settimeout(TIMEOUT)
            server.bind(("", self.interior_tcp_connector_port))
            server.listen(10)
        except Exception as exc:
            server.close()
            print("%s: failed creating tcp server: %s" % (self.test_name, exc),
                  flush=True)
            raise

        # step 2: initiate client connection to edge1. Since the listening
        # socket may not be immediately available allow for connection refused
        # error until it is ready. Should the listening socket fail to appear
        # the test will fail on timeout.

        while True:
            client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_conn.settimeout(TIMEOUT)
            try:
                client_conn.connect(('127.0.0.1', self.edge_tcp_listener_port))
                break
            except ConnectionRefusedError:
                print("%s: client connection refused, retrying..." % self.test_name,
                      flush=True)
                client_conn.close()
                time.sleep(1.0)
                continue

        # step 3: accept the connection request from interior:
        try:
            server_conn, addr = server.accept()
        except Exception as exc:
            print("%s: failed to accept at server: %s" % (self.test_name,
                                                          exc),
                  flush=True)
            server.close()
            raise
        server.close()

        # step 4: now wait long enough for the stuck delivery detection cycle
        # to run on each router (approx 5 seconds)
        client_conn.send(b'Hi There')
        time.sleep(12)
        client_conn.close()
        server_conn.close()

        # step 5: expect no log messages for stuck delivery were issued
        for router in [self.e_router, self.i_router]:
            logfile = router.logfile_path
            router.teardown()
            with io.open(logfile) as f:
                for line in f:
                    self.assertNotIn("Stuck delivery: At least one delivery",
                                     line,
                                     "Stuck deliveries should not be logged!")


class TcpAdaptorStuckDeliveryLiteTest(TcpAdaptorStuckDeliveryTest):
    """
    Verify that the routers stuck delivery detection is not applied to TCP
    deliveries.  See Dispatch-2036.
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorStuckDeliveryLiteTest, cls).setUpClass(encap='lite', test_name='TCPStuckDeliveryLiteTest')


class TcpAdaptorManagementTest(TestCase):
    """
    Test Creation and deletion of TCP management entities
    """
    @classmethod
    def setUpClass(cls, encap='legacy', test_name='TCPMgmtTest'):
        super(TcpAdaptorManagementTest, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

        cls.test_name = test_name
        cls.encapsulation = encap

        # create edge and interior routers.  The listener/connector will be on
        # the edge router.  It is expected that the edge will create proxy
        # links to the interior and remove them when the test is done.

        cls.interior_edge_port = cls.tester.get_port()
        cls.interior_mgmt_port = cls.tester.get_port()
        cls.edge_mgmt_port = cls.tester.get_port()

        cls.tcp_server_port = cls.tester.get_port()
        cls.tcp_listener_port = cls.tester.get_port()

        i_config = [
            ('router', {'mode': 'interior',
                        'id': 'TCPMgmtTestInterior'}),
            ('listener', {'role': 'normal',
                          'port': cls.interior_mgmt_port}),
            ('listener', {'role': 'edge', 'port': cls.interior_edge_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(i_config)
        cls.i_router = cls.tester.qdrouterd('TCPMgmtTestInterior', config, wait=False)

        e_config = [
            ('router', {'mode': 'edge',
                        'id': 'TCPMgmtTestEdge'}),
            ('listener', {'role': 'normal',
                          'port': cls.edge_mgmt_port}),
            ('connector', {'name': 'edge', 'role': 'edge',
                           'port': cls.interior_edge_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(e_config)
        cls.e_router = cls.tester.qdrouterd('TCPMgmtTestEdge', config,
                                            wait=False)

        cls.i_router.wait_ready()
        cls.e_router.wait_ready()

    def _query_links_by_addr(self, router_mgmt, owning_addr):
        attrs = ['owningAddr', 'linkDir']

        links = []
        rc = router_mgmt.query(type=ROUTER_LINK_TYPE, attribute_names=attrs).results
        for link in rc:
            if link[0] is not None and link[0].endswith(owning_addr):
                links.append(link)
        return links

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_01_mgmt(self):
        """
        Create and delete TCP connectors and listeners. Ensure that the service
        address is properly removed on the interior router.
        """
        mgmt = self.e_router.management
        van_address = self.test_name + "/test_01_mgmt"

        # When starting out, there should be no tcpListeners or tcpConnectors.
        self.assertEqual(0, len(mgmt.query(type=TCP_LISTENER_TYPE).results))
        self.assertEqual(0, len(mgmt.query(type=TCP_CONNECTOR_TYPE).results))

        connector_name = "ServerConnector"
        listener_name = "ClientListener"

        mgmt.create(type=TCP_LISTENER_TYPE,
                    name=listener_name,
                    attributes={'address': van_address,
                                'port': self.tcp_listener_port,
                                'host': '127.0.0.1',
                                'encapsulation': self.encapsulation})
        mgmt.create(type=TCP_CONNECTOR_TYPE,
                    name=connector_name,
                    attributes={'address': van_address,
                                'port': self.tcp_server_port,
                                'host': '127.0.0.1',
                                'encapsulation': self.encapsulation})

        # verify the entities have been created and tcp traffic works
        self.assertEqual(1, len(mgmt.query(type=TCP_LISTENER_TYPE).results))
        self.assertEqual(1, len(mgmt.query(type=TCP_CONNECTOR_TYPE).results))

        # now verify that the interior router sees the service address
        # and two proxy links are created: one outgoing for the connector and
        # one incoming for the listeners address watcher
        self.i_router.wait_address(van_address, subscribers=1)
        while True:
            links = self._query_links_by_addr(self.i_router.management,
                                              van_address)
            if links and len(links) == 2:
                self.assertTrue((links[0][1] == "out" and links[1][1] == "in") or
                                (links[1][1] == "out" and links[0][1] == "in"))
                break
            time.sleep(0.25)

        # Delete the connector and listener
        out = mgmt.delete(type=TCP_CONNECTOR_TYPE, name=connector_name)  # pylint: disable=assignment-from-no-return
        self.assertIsNone(out)
        self.assertEqual(0, len(mgmt.query(type=TCP_CONNECTOR_TYPE).results))
        out = mgmt.delete(type=TCP_LISTENER_TYPE, name=listener_name)  # pylint: disable=assignment-from-no-return
        self.assertIsNone(out)
        self.assertEqual(0, len(mgmt.query(type=TCP_LISTENER_TYPE).results))

        # verify the service address and proxy links are no longer active on
        # the interior router
        self.i_router.wait_address_unsubscribed(van_address)
        while True:
            links = self._query_links_by_addr(self.i_router.management,
                                              van_address)
            if len(links) == 0:
                break
            time.sleep(0.25)

        # verify that clients can no longer connect to the listener
        client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_conn.setblocking(True)
        client_conn.settimeout(5)
        with self.assertRaises(ConnectionRefusedError):
            client_conn.connect(('127.0.0.1', self.tcp_listener_port))
        client_conn.close()

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_02_mgmt_recreate(self):
        """
        Verify that deleting then re-creating listeners and connectors works
        """
        mgmt = self.e_router.management
        van_address = self.test_name + "/test_02_mgmt_recreate"

        # When starting out, there should be no tcpListeners or tcpConnectors.
        self.assertEqual(0, len(mgmt.query(type=TCP_LISTENER_TYPE).results))
        self.assertEqual(0, len(mgmt.query(type=TCP_CONNECTOR_TYPE).results))

        connector_name = "ServerConnector"
        listener_name = "ClientListener"

        for i in range(2):

            mgmt.create(type=TCP_LISTENER_TYPE,
                        name=listener_name,
                        attributes={'address': van_address,
                                    'port': self.tcp_listener_port,
                                    'host': '127.0.0.1',
                                    'encapsulation': self.encapsulation})
            mgmt.create(type=TCP_CONNECTOR_TYPE,
                        name=connector_name,
                        attributes={'address': van_address,
                                    'port': self.tcp_server_port,
                                    'host': '127.0.0.1',
                                    'encapsulation': self.encapsulation})

            # wait until the listener has initialized

            def _wait_for_listener_up():
                li = mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
                if li['operStatus'] == 'up':
                    return True
                return False
            self.assertTrue(retry(_wait_for_listener_up))

            # verify initial statistics

            l_stats = mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
            self.assertEqual(0, l_stats['bytesIn'])
            self.assertEqual(0, l_stats['bytesOut'])
            self.assertEqual(0, l_stats['connectionsOpened'])
            self.assertEqual(0, l_stats['connectionsClosed'])

            c_stats = mgmt.read(type=TCP_CONNECTOR_TYPE, name=connector_name)
            self.assertEqual(0, c_stats['bytesIn'])
            self.assertEqual(0, c_stats['bytesOut'])
            self.assertEqual(1, c_stats['connectionsOpened'])  # dispatcher
            self.assertEqual(0, c_stats['connectionsClosed'])

            # test everything works

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.settimeout(TIMEOUT)
                server.bind(("", self.tcp_server_port))
                server.listen(1)

                self.assertTrue(retry(lambda:
                                      mgmt.read(type=TCP_LISTENER_TYPE,
                                                name=listener_name)['operStatus']
                                      == 'up'))

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    client.settimeout(TIMEOUT)
                    client.connect(('127.0.0.1', self.tcp_listener_port))

                    ssock, _ = server.accept()

                    try:
                        client.sendall(b'0123456789')

                        data = b''
                        while data != b'0123456789':
                            data += ssock.recv(1024)

                        ssock.sendall(b'ABCD')

                        data = b''
                        while data != b'ABCD':
                            data += client.recv(1024)
                    finally:
                        ssock.shutdown(socket.SHUT_RDWR)
                        ssock.close()

            # Wait until the test clients have closed

            def _wait_for_close():
                if 0 == mgmt.read(type=TCP_LISTENER_TYPE,
                                  name=listener_name)['connectionsClosed']:
                    return False
                if 0 == mgmt.read(type=TCP_CONNECTOR_TYPE,
                                  name=connector_name)['connectionsClosed']:
                    return False
                return True
            self.assertTrue(retry(_wait_for_close, delay=0.25))

            # Verify updated statistics.

            l_stats = mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
            if self.encapsulation == 'legacy':
                # deprecated for tcp-lite
                self.assertEqual(10, l_stats['bytesIn'])
                self.assertEqual(4, l_stats['bytesOut'])
            self.assertEqual(1, l_stats['connectionsOpened'])
            self.assertEqual(1, l_stats['connectionsClosed'])

            c_stats = mgmt.read(type=TCP_CONNECTOR_TYPE, name=connector_name)
            if self.encapsulation == 'legacy':
                # deprecated for tcp-lite
                self.assertEqual(4, c_stats['bytesIn'])
                self.assertEqual(10, c_stats['bytesOut'])
            self.assertEqual(2, c_stats['connectionsOpened'])
            self.assertEqual(1, c_stats['connectionsClosed'])

            # Attempt to delete the special tcp-dispatcher pseudo connection.
            # Doing so is forbidden so expect this to fail

            conns = [d for d in mgmt.query(type=CONNECTION_TYPE).get_dicts()
                     if d['host'] == 'egress-dispatch']
            for conn in conns:
                with self.assertRaises(ForbiddenStatus):
                    mgmt.update(attributes={'adminStatus': 'deleted'},
                                type=CONNECTION_TYPE,
                                identity=conn['identity'])

            # splendid!  Not delete all the things

            mgmt.delete(type=TCP_LISTENER_TYPE, name=listener_name)
            self.assertEqual(0, len(mgmt.query(type=TCP_LISTENER_TYPE).results))

            mgmt.delete(type=TCP_CONNECTOR_TYPE, name=connector_name)
            self.assertEqual(0, len(mgmt.query(type=TCP_CONNECTOR_TYPE).results))

            # attempting to connect should fail once the listener socket has
            # been closed

            def _retry_until_fail():
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.setblocking(True)
                    client.settimeout(TIMEOUT)
                    client.connect(('127.0.0.1', self.tcp_listener_port))
                    return False
            with self.assertRaises(ConnectionRefusedError):
                retry(_retry_until_fail, delay=0.25)


class TcpAdaptorManagementLiteTest(TcpAdaptorManagementTest):
    """
    Test Creation and deletion of TCP management entities
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorManagementLiteTest, cls).setUpClass(encap='lite',
                                                            test_name='TCPMgmtLiteTest')


class TcpAdaptorListenerConnectTest(TestCase):
    """
    Test client connecting to TcpListeners in various scenarios
    """
    @classmethod
    def setUpClass(cls, encap='legacy', test_name='TCPListenConnTest'):
        super(TcpAdaptorListenerConnectTest, cls).setUpClass()

        cls.encapsulation = encap
        cls.test_name = 'TCPListenConnTest'

        # 4 router linear deployment:
        #  EdgeA - InteriorA - InteriorB - EdgeB

        cls.inter_router_port = cls.tester.get_port()
        cls.INTA_edge_port = cls.tester.get_port()
        cls.INTB_edge_port = cls.tester.get_port()

        def router(name: str, mode: str,
                   extra: Optional[List] = None) -> Qdrouterd:
            config = [
                ('router', {'mode': mode,
                            'id': name}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port()}),
                ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ]

            if extra:
                config.extend(extra)

            config = Qdrouterd.Config(config)
            return cls.tester.qdrouterd(name, config, wait=True)

        cls.INTA = router('INTA', 'interior',
                          [
                              ('listener', {'role': 'inter-router', 'port':
                                            cls.inter_router_port}),
                              ('listener', {'role': 'edge', 'port':
                                            cls.INTA_edge_port}),
                          ])
        cls.INTB = router('INTB', 'interior',
                          [
                              ('connector', {'role': 'inter-router',
                                             'host': '127.0.0.1',
                                             'port': cls.inter_router_port}),
                              ('listener', {'role': 'edge', 'port':
                                            cls.INTB_edge_port}),
                          ])
        cls.EdgeA = router('EdgeA', 'edge',
                           [
                               ('connector', {'role': 'edge',
                                              'host': '127.0.0.1',
                                              'port': cls.INTA_edge_port}),
                           ])
        cls.EdgeB = router('EdgeB', 'edge',
                           [
                               ('connector', {'role': 'edge',
                                              'host': '127.0.0.1',
                                              'port': cls.INTB_edge_port}),
                           ])

    def test_01_no_service(self):
        """
        This is a test for the fix to ISSUE #263.  Connect to the listener port
        without having a server present at the connector. we expect the client
        connection to close since there is no service.
        """
        van_address = "closest/test_01_no_service"
        listener_port = self.tester.get_port()
        connector_port = self.tester.get_port()

        a_mgmt = self.INTA.management
        a_mgmt.create(type=TCP_LISTENER_TYPE,
                      name="ClientListener01",
                      attributes={'address': van_address,
                                  'port': listener_port,
                                  'encapsulation': self.encapsulation})

        b_mgmt = self.INTB.management
        b_mgmt.create(type=TCP_CONNECTOR_TYPE,
                      name="ServerConnector01",
                      attributes={'address': van_address,
                                  'host': '127.0.0.1',
                                  'port': connector_port,
                                  'encapsulation': self.encapsulation})

        self.INTA.wait_address(van_address, remotes=1)

        # Note: the listeners operational state is expected to be "up" even
        # though there is no active server present. In a real deployment
        # skupper will only provision the connector when there is something to
        # connect to.

        while True:
            listener = a_mgmt.read(type=TCP_LISTENER_TYPE,
                                   name='ClientListener01')
            if listener['operStatus'] != 'up':
                time.sleep(0.1)
                continue

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
                client_conn.setblocking(True)
                client_conn.settimeout(TIMEOUT)

                # There may be a delay between the operStatus going up and the
                # actual listener socket availability, so allow that:

                try:
                    client_conn.connect(('127.0.0.1', listener_port))
                except ConnectionRefusedError:
                    time.sleep(0.1)
                    continue

                # It is expected that data sent to the connector end will
                # result in a non-ACCEPTED disposition arriving at the listener
                # side, which will close the socket.

                try:
                    # note: if the sendall does not eventually fail then this test
                    # will fail on timeout
                    while True:
                        client_conn.sendall(b'123')
                        time.sleep(0.1)
                except (BrokenPipeError, ConnectionResetError):
                    # Yay we did not hang!  Test passed
                    break

        a_mgmt.delete(type=TCP_LISTENER_TYPE, name="ClientListener01")
        b_mgmt.delete(type=TCP_CONNECTOR_TYPE, name="ServerConnector01")

        self.INTA.wait_address_unsubscribed(van_address)

    def _test_listener_socket_lifecycle(self,
                                        l_router: Qdrouterd,  # listener router
                                        c_router: Qdrouterd,  # connector router
                                        test_id: str):
        """
        Verify that the listener socket is active only when a connector for the
        VAN address has been provisioned
        """
        listener_name = "Listener_%s" % test_id
        connector_name = "Connector_%s" % test_id
        van_address = "closest/%s" % test_id
        listener_port = self.tester.get_port()
        connector_port = self.tester.get_port()

        l_mgmt = l_router.management
        l_mgmt.create(type=TCP_LISTENER_TYPE,
                      name=listener_name,
                      attributes={'address': van_address,
                                  'port': listener_port,
                                  'encapsulation': self.encapsulation})

        # since there is no connector present, the operational state must be
        # down and connection attempts must be refused

        listener = l_mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
        self.assertEqual('down', listener['operStatus'])

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
            client_conn.setblocking(True)
            client_conn.settimeout(TIMEOUT)
            self.assertRaises(ConnectionRefusedError,
                              client_conn.connect, ('127.0.0.1', listener_port))

        # create a connector and a TCP server for the connector to connect to

        c_mgmt = c_router.management
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.settimeout(TIMEOUT)
            server.bind(("", connector_port))
            server.listen(1)

            c_mgmt.create(type=TCP_CONNECTOR_TYPE,
                          name=connector_name,
                          attributes={'address': van_address,
                                      'host': '127.0.0.1',
                                      'port': connector_port,
                                      'encapsulation': self.encapsulation})

            # let the connectors address propagate to listener router

            if l_router.config.router_mode == 'interior':
                l_router.wait_address(van_address, remotes=1)
            else:
                l_router.wait_address(van_address, subscribers=1)

            # expect the listener socket to come up

            while True:
                listener = l_mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
                if listener['operStatus'] != 'up':
                    time.sleep(0.1)
                    continue

                # ensure clients can connect successfully.

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
                    client_conn.settimeout(TIMEOUT)
                    try:
                        client_conn.connect(('127.0.0.1', listener_port))
                    except ConnectionRefusedError:
                        # There may be a delay between the operStatus going up and
                        # the actual listener socket availability, so allow that:
                        time.sleep(0.1)
                        continue

                    server_conn, _ = server.accept()
                    client_conn.sendall(b' test ')
                    client_conn.close()
                    server_conn.close()
                    break

        # Teardown the connector, expect listener admin state to go down
        # and connections be refused

        c_mgmt.delete(type=TCP_CONNECTOR_TYPE, name=connector_name)
        l_router.wait_address_unsubscribed(van_address)

        while True:
            listener = l_mgmt.read(type=TCP_LISTENER_TYPE, name=listener_name)
            if listener['operStatus'] != 'down':
                time.sleep(0.1)
                continue

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
                client_conn.setblocking(True)
                client_conn.settimeout(TIMEOUT)
                try:
                    client_conn.connect(('127.0.0.1', listener_port))
                    # There may be a delay between the operStatus going down and
                    # the actual listener socket closing, so allow that:
                    continue
                except ConnectionRefusedError:
                    # Test successful
                    break

        l_mgmt.delete(type=TCP_LISTENER_TYPE, name=listener_name)

    def test_02_listener_interior(self):
        """
        Test tcpListener socket lifecycle interior to interior
        """
        self._test_listener_socket_lifecycle(self.INTA, self.INTB, "test_02_listener_interior")

    def test_03_listener_edge_interior(self):
        """
        Test tcpListener socket lifecycle edge to interior
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.INTB, "test_03_listener_edge_interior")

    def test_04_listener_interior_edge(self):
        """
        Test tcpListener socket lifecycle interior to edge
        """
        self._test_listener_socket_lifecycle(self.INTA, self.EdgeB, "test_04_listener_interior_edge")

    def test_05_listener_edge_edge(self):
        """
        Test tcpListener socket lifecycle edge to edge
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.EdgeB, "test_05_listener_edge_edge")


class TcpAdaptorListenerConnectLiteTest(TestCase):
    """
    Test client connecting to TcpListeners in various scenarios
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorListenerConnectLiteTest, cls).setUpClass(encap='lite', test_name='TCPListenConnLiteTest')


class TcpDeleteConnectionTest(TestCase):
    @classmethod
    def setUpClass(cls, encap='legacy', test_name="TcpDeleteConnectionTest"):
        super(TcpDeleteConnectionTest, cls).setUpClass()
        cls.good_listener_port = cls.tester.get_port()
        cls.server_logger = Logger(title=test_name,
                                   print_to_console=True,
                                   save_for_dump=False,
                                   ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                          "setUpClass/TcpAdaptor_echo_server_INTA.log"))
        server_prefix = f"ECHO_SERVER_{test_name}_INTA"
        cls.echo_server = TcpEchoServer(prefix=server_prefix,
                                        port=0,
                                        logger=cls.server_logger)
        assert cls.echo_server.is_running
        cls.echo_server_port = cls.echo_server.port

        config = [
            ('router', {'mode': 'interior', 'id': 'INTA'}),
            # Listener for handling router management requests.
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpListener',
             {'name': "good-listener",
              'host': "localhost",
              'port': cls.good_listener_port,
              'address': 'ES_GOOD_CONNECTOR_CERT_INTA',
              'siteId': "mySite",
              'encapsulation': encap}),

            ('tcpConnector',
             {'name': "good-connector",
              'host': "localhost",
              'port': cls.echo_server.port,
              'address': 'ES_GOOD_CONNECTOR_CERT_INTA',
              'siteId': "mySite",
              'encapsulation': encap}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.router = cls.tester.qdrouterd(test_name, Qdrouterd.Config(config), wait=True)
        cls.address = cls.router.addresses[0]
        wait_tcp_listeners_up(cls.router.addresses[0])

    def test_delete_tcp_connection(self):
        client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_conn.settimeout(TIMEOUT)
        client_conn.connect(('127.0.0.1', self.good_listener_port))
        qd_manager = QdManager(self.address)
        conn_id = None
        results = qd_manager.query(CONNECTION_TYPE)
        for result in results:
            conn_direction = result['dir']
            # Find the id of the tcp connection we want to delete.
            if conn_direction == 'out' and result['host'] != 'egress-dispatch':
                # Delete the connection by updating the adminStatus to deleted.
                qd_manager.update(CONNECTION_TYPE, {"adminStatus": "deleted"}, identity=result['identity'])
                conn_id = result['identity']
                break
        self.assertIsNotNone(conn_id, "Expected connection id to be not None")

        def check_connection_deleted():
            outs = qd_manager.query(CONNECTION_TYPE)
            is_conn_present = False
            for out in outs:
                if out['identity'] == conn_id:
                    is_conn_present = True
                    break
            self.assertFalse(is_conn_present)

        # Keep retrying until the connection is gone from the connection table.
        retry_assertion(check_connection_deleted, delay=2)
        client_conn.close()


class TcpDeleteConnectionLiteTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TcpDeleteConnectionLiteTest, cls).setUpClass(encap='lite', test_name="TcpDeleteConnectionLiteTest")


class TcpLegacyInvalidEncodingTest(TestCase):
    """
    Ensure that the TCP adaptor can recover from receiving an improperly
    formatted AMQP encoded stream message.
    """
    @classmethod
    def setUpClass(cls):
        super(TcpLegacyInvalidEncodingTest, cls).setUpClass()

        config = [
            ('router', {'mode': 'interior', 'id': 'TcpInvalidEncoding'}),
            # Listener for handling router management requests.
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpConnector', {'host': "127.0.0.1",
                              'port': cls.tester.get_port(),
                              'address': 'tcp-connector',
                              'encapsulation': 'legacy',
                              'siteId': "mySite"}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.tester.get_port(),
                             'address': 'tcp-listener',
                             'encapsulation': 'legacy',
                             'siteId': "mySite"}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.router = cls.tester.qdrouterd('TcpInvalidEncoding',
                                          Qdrouterd.Config(config), wait=True)

        cls.amqp_address = cls.router.addresses[0]
        cls.listener_address = cls.router.tcp_addresses[0]

    def test_invalid_egress_client_request_encaps(self):
        """
        Simulate an message with an incompatible ecapsulation sent by a client
        that arrives at the egress connector. Verify that the egress connector
        RELEASED the message and an error has been logged. The message should
        be released so it can be delivered to another (compatible) connector.
        """

        # send a request message with an incompatible encapsulation

        msg = Message()
        msg.to = "tcp-connector"
        msg.subject = "stuff"
        msg.reply_to = "invalid/reply/to"
        msg.content_type = "This-is-wrong"
        test = InvalidClientSendRequest(msg, self.amqp_address, 'tcp-connector')
        test.run()
        self.assertIsNone(test.error)
        self.router.wait_log_message(pattern=r"Misconfigured tcpConnector \(wrong encapsulation\)")

    def test_invalid_ingress_server_reply_encaps(self):
        """
        Simulate an invalid reply message arriving at the ingress
        listener. Verify the message is REJECTED and an error has been
        logged. The message is rejected because the link is a reply-to stream
        with a unique address for the client - it cannot be redelivered to
        another client.
        """

        # send a reply message with an incompatible encapsulation

        msg = Message()
        msg.subject = "Subject"
        msg.annotations = {":flowid": "whatever"}
        msg.content_type = "This-is-wrong"
        test = InvalidServerSendReply(msg, self.amqp_address,
                                      self.listener_address, 'tcp-listener',
                                      Disposition.REJECTED)
        test.run()
        self.assertIsNone(test.error)
        self.router.wait_log_message(pattern=r"Misconfigured tcpListener \(wrong outgoing encapsulation\)")

    def test_invalid_ingress_server_reply_body(self):
        """
        Simulate a reply message arriving at the ingress listener that has an
        invalid body structure. The client should close the connection and set
        the outcome to ACCEPTED. The reason REJECTED is not used is because
        data may already have been sent to the TCP client and it is too late to
        reject the message.
        """

        # send a reply message with an incompatible body format

        msg = Message()
        msg.subject = "Subject"
        msg.annotations = {":flowid": "whatever"}
        msg.body = "This is a STRING, NOT VBIN!"
        test = InvalidServerSendReply(msg, self.amqp_address,
                                      self.listener_address, 'tcp-listener',
                                      Disposition.ACCEPTED)
        test.run()
        self.assertIsNone(test.error)
        self.router.wait_log_message(pattern=r"Invalid body data for streaming message")


class InvalidClientSendRequest(MessagingHandler):
    """
    Builds a legacy TCP adaptor client request message with an incompatible
    encapsulation format. Expect the TCP adaptor connector to release the
    message so it can be re-delivered to another (compatible) connector.
    """
    def __init__(self, msg, address, destination):
        super(InvalidClientSendRequest, self).__init__(auto_settle=False)
        self.msg = msg
        self.address = address
        self.destination = destination
        self.error = None
        self.sent = False
        self.timer = None
        self.conn = None
        self.sender = None

    def done(self, error=None):
        self.error = error
        self.timer.cancel()
        self.conn.close()

    def timeout(self):
        self.error = f"Timeout Expired: sent={self.sent}"
        self.conn.close()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.conn = event.container.connect(self.address)
        self.sender = event.container.create_sender(self.conn, self.destination)

    def on_sendable(self, event):
        if not self.sent:
            event.sender.send(self.msg)
            self.sent = True

    def on_released(self, event):
        self.done()

    def on_accepted(self, event):
        self.done("Test Failed: message ACCEPTED (expected RELEASED)")

    def on_rejected(self, event):
        self.done("Test Failed: message REJECTED (expected RELEASED)")

    def run(self):
        Container(self).run()


class InvalidServerSendReply(MessagingHandler):
    """
    Simulate via AMQP a TCP client/server flow and have the fake server send
    "msg" as the TCP adaptor reply message to the client. Expect the client to
    set the reply messages disposition to "dispo".
    """
    def __init__(self, msg, server_address, listener_address, service_address, dispo):
        super(InvalidServerSendReply, self).__init__(auto_settle=False)
        self.msg = msg
        self.service_address = service_address
        self.error = None
        self.timer = None
        self.expected_dispo = dispo

        # fake server connection, receive link for request, send link for reply-to
        self.server_address = server_address
        self.server_conn = None
        self.server_sender = None
        self.server_receiver = None
        self.server_sent = False

        # The request message that arrives at the "server" is streaming. Proton
        # does not give us an "on_message" callback since it never
        # completes. Wait long enough for the headers to arrive so we can
        # extract the reply-to
        self.request_dlv = None
        self.dlv_drain_timer = None

        # fake tcp client, just opens an AMQP connection to the TCP
        # listener. This initiates the ingress streaming request message.
        self.listener_address = listener_address
        self.client_conn = None

    def done(self, error=None):
        self.error = error
        if self.timer:
            self.timer.cancel()
        self.server_conn.close()
        if self.client_conn is not None:
            self.client_conn.close()
        if self.dlv_drain_timer:
            self.dlv_drain_timer.cancel()

    def timeout(self):
        self.timer = None
        self.done(error=f"Timeout Expired: server_sent={self.server_sent}")

    def on_start(self, event):
        # Create an AMQP receiver for the service address. This will simulate a
        # fake server and activate the TCP adaptor listener port
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.server_conn = event.container.connect(self.server_address)
        self.server_receiver = event.container.create_receiver(self.server_conn,
                                                               self.service_address)

    def on_timer_task(self, event):
        # At this point we expect the reply-to header to have arrived. Use the
        # reply-to address to send the 'msg' parameter to the client as the
        # response streaming message.
        try:
            data = self.server_receiver.recv(self.request_dlv.pending)
            #print(f"len={len(xxx)}\nBODY=[{xxx}]", flush=True)
            msg = Message()
            msg.decode(data)
            self.server_sender = event.container.create_sender(self.server_conn,
                                                               msg.reply_to)
        except Exception as exc:
            self.bail(error=f"Incomplete request msg headers {data}")

        self.request_dlv.settle()

    def on_delivery(self, event):
        # We've received the start of the client request message. In order to
        # set up a reply-to link wait until the message's reply-to field has
        # arrived.
        if event.receiver == self.server_receiver:
            if self.request_dlv is None and event.delivery.readable:
                # sleep a bit to allow all the header data to arrive on the
                # delivery
                self.request_dlv = event.delivery
                self.dlv_drain_timer = event.reactor.schedule(1.0, self)

    def on_link_opened(self, event):
        if event.receiver == self.server_receiver:
            # "server" ready to take requests, fire up the "client". All we
            # need is to connect since that will activate the tcp adaptor
            # client-side due to the AMQP @open handshake.
            self.client_conn = event.container.connect(self.listener_address)

    def on_sendable(self, event):
        # Have the fake server send 'msg' to the reply-to of the fake client
        if event.sender == self.server_sender:
            if not self.server_sent:
                # send the invalid reply
                self.server_sender.send(self.msg)
                self.server_sent = True

    def on_released(self, event):
        self.done(None if self.expected_dispo == Disposition.RELEASED else "Unexpected PN_RELEASED")

    def on_accepted(self, event):
        self.done(None if self.expected_dispo == Disposition.ACCEPTED else "Unexpected PN_ACCEPTED")

    def on_rejected(self, event):
        self.done(None if self.expected_dispo == Disposition.REJECTED else "Unexpected PN_REJECTED")

    def run(self):
        Container(self).run()


class TcpAdaptorConnCounter(TestCase):
    """
    Validate the TCP service connection counter
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorConnCounter, cls).setUpClass()

        config = [
            ('router', {'mode': 'interior',
                        'id': 'TCPConnCounter'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('TCPConnCounter', config)

    def _get_conn_counters(self) -> Mapping[str, int]:
        attributes = ["connectionCounters"]
        rc = self.router.management.query(type=ROUTER_TYPE,
                                          attribute_names=attributes)
        self.assertIsNotNone(rc, "unexpected query failure")
        self.assertEqual(1, len(rc.get_dicts()), "expected one attribute!")
        counters = rc.get_dicts()[0].get("connectionCounters")
        self.assertIsNotNone(counters, "expected a counter map to be returned")
        return counters

    def _run_test(self, encaps, idle_ct, active_ct):
        # idle_ct: expected connection count when tcp configured, but prior to
        # connections active. This includes the count of special dispatcher
        # connections.
        # activ_ct: expected connection count when 1 client and 1 server
        # connected (also include dispatcher conns)
        mgmt = self.router.management

        # verify the counters start at zero (not including amqp)

        counters = self._get_conn_counters()
        for proto in ["tcp", "http1", "http2"]:
            counter = counters.get(proto)
            self.assertIsNotNone(counter, f"Missing expected protocol counter {proto}!")
            self.assertEqual(0, counter, "counters must be zero on startup")

        # Bring up a server and client, check the counter

        connector_port = self.get_port()
        listener_port = self.get_port()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", connector_port))
            listener.setblocking(True)
            listener.listen(10)

            mgmt.create(type=TCP_LISTENER_TYPE,
                        name="ClientListener",
                        attributes={'address': 'closest/tcpService',
                                    'port': listener_port,
                                    'encapsulation': encaps})
            mgmt.create(type=TCP_CONNECTOR_TYPE,
                        name="ServerConnector",
                        attributes={'address': 'closest/tcpService',
                                    'host': "localhost",
                                    'port': connector_port,
                                    'encapsulation': encaps})

            wait_tcp_listeners_up(self.router.addresses[0],
                                  l_filter={'name': 'ClientListener'})

            # expect that simply configuring the tcp listener/connector will
            # instantiate the "dispatcher" connection:

            self.assertTrue(retry(lambda:
                                  self._get_conn_counters().get("tcp") == idle_ct),
                            f"Expected {idle_ct} got {self._get_conn_counters()}!")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                retry_exception(lambda cs=client, lp=listener_port:
                                cs.connect(("localhost", lp)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                server, _ = listener.accept()
                try:
                    self.assertTrue(retry(lambda:
                                          self._get_conn_counters().get("tcp") ==
                                          active_ct),
                                    f"Expected {active_ct} got {self._get_conn_counters()}!")
                finally:
                    server.shutdown(socket.SHUT_RDWR)
                    server.close()
                client.shutdown(socket.SHUT_RDWR)  # context manager calls close

        mgmt.delete(type=TCP_CONNECTOR_TYPE, name="ServerConnector")
        mgmt.delete(type=TCP_LISTENER_TYPE, name="ClientListener")

        # expect tcp counter to return to zero once config is cleaned up

        self.assertTrue(retry(lambda:
                              self._get_conn_counters().get("tcp") == 0),
                        f"Expected 0 tcp conns, got {self._get_conn_counters()}")

    def test_01_check_counter(self):
        """ Create and destroy TCP network connections, verify the connection
        counter is correct.
        """
        for encaps in ['legacy', 'lite']:
            self._run_test(encaps, idle_ct=1, active_ct=3)


class TcpAdaptorNoDelayedDelivery(TestCase):
    """
    Ensure long lived TCP sessions are not counted as delayed deliveries
    """
    @classmethod
    def setUpClass(cls, encap="legacy"):
        super(TcpAdaptorNoDelayedDelivery, cls).setUpClass()

        cls.encapsulation = encap
        cls.listener_port = cls.tester.get_port()
        cls.connector_port = cls.tester.get_port()
        config = [
            ('router', {'mode': 'interior',
                        'id': 'TCPNoDelay'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_port,
                             'address': "no/delay",
                             'encapsulation': cls.encapsulation}),
            ('tcpConnector', {'host': "127.0.0.1",
                              'port': cls.connector_port,
                              'address': "no/delay",
                              'encapsulation': cls.encapsulation}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('TCPNoDelay', config)

    def _get_delayed_counters(self) -> Tuple[int, int]:
        attributes = ["deliveriesDelayed1Sec", "deliveriesDelayed10Sec"]
        rc = self.router.management.query(type=ROUTER_TYPE,
                                          attribute_names=attributes)
        self.assertIsNotNone(rc, "unexpected query failure")
        result = rc.get_dicts()[0]
        counters = (result.get("deliveriesDelayed1Sec"),
                    result.get("deliveriesDelayed10Sec"))
        for ctr in counters:
            self.assertIsNotNone(ctr, "expected a counter to be returned")
        return counters

    def test_01_check_delayed_deliveries(self):
        # idle_ct: expected connection count when tcp configured, but prior to
        # connections active
        # activ_ct: expected connection count when 1 client and 1 server
        # connected
        mgmt = self.router.management

        # Get a baseline for the counters
        baseline = self._get_delayed_counters()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.connector_port))
            listener.setblocking(True)
            listener.listen(10)

            wait_tcp_listeners_up(self.router.addresses[0])

            # create a long-lived TCP stream:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                retry_exception(lambda cs=client, lp=self.listener_port:
                                cs.connect(("localhost", lp)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                server, _ = listener.accept()
                try:
                    client.sendall(b'123')
                    data = b''
                    while data != b'123':
                        data += server.recv(1024)
                    # now sleep for enough time to trigger the 1 second delayed
                    # delivery counter:
                    time.sleep(2)
                finally:
                    server.shutdown(socket.SHUT_RDWR)
                    server.close()

                # Wait for the client connection to drop. The delivery will be
                # released and all delivery counters will be updated
                while True:
                    if client.recv(1024) == b'':
                        break

        counters = self._get_delayed_counters()
        self.assertEqual(0, counters[0] - baseline[0],
                         f"Expected delay counter to be zero, got {counters}")
        self.assertEqual(0, counters[1] - baseline[1],
                         f"Expected delay counter to be zero, got {counters}")


class TcpAdaptorNoDelayedDeliveryLite(TcpAdaptorNoDelayedDelivery):
    """
    Ensure long lived TCP sessions are not counted as delayed deliveries
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorNoDelayedDeliveryLite, cls).setUpClass(encap='lite')


class TcpMisconfiguredLegacyLiteEncapsTest(TestCase):
    """
    Ensure that the TCP adaptor can detect misconfiguration of the
    encapsulation setting by creating a tcpListener and tcpConnector pair that
    use different encaps.
    """
    @classmethod
    def setUpClass(cls):
        super(TcpMisconfiguredLegacyLiteEncapsTest, cls).setUpClass()

        config = [
            ('router', {'mode': 'interior', 'id': 'TcpMisconfiguredEncaps'}),
            # Listener for handling router management requests.
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.router = cls.tester.qdrouterd('TcpInvalidEncoding',
                                          Qdrouterd.Config(config), wait=True)
        cls.address = cls.router.addresses[0]

    def _test(self, ingress_encaps, egress_encaps):
        ingress_port = self.tester.get_port()
        egress_port = self.tester.get_port()
        mgmt = self.router.qd_manager

        mgmt.create(TCP_CONNECTOR_TYPE,
                    {"name": "EncapsConnector",
                     "address": "EncapsTest",
                     "host": "localhost",
                     "port": egress_port,
                     "encapsulation": egress_encaps})
        mgmt.create(TCP_LISTENER_TYPE,
                    {"name": "EncapsListener",
                     "address": "EncapsTest",
                     "port": ingress_port,
                     "encapsulation": ingress_encaps})
        wait_tcp_listeners_up(self.address)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.settimeout(TIMEOUT)
            server.bind(("", egress_port))
            server.listen(1)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
                client_conn.settimeout(TIMEOUT)
                while True:
                    try:
                        client_conn.connect(('127.0.0.1', ingress_port))
                        break
                    except ConnectionRefusedError:
                        # There may be a delay between the operStatus going up and
                        # the actual listener socket availability, so allow that:
                        time.sleep(0.1)
                        continue

                # send data to kick off the flow, but expect connection to fail
                # in recv (either close or reset)

                client_conn.sendall(b' test ')
                try:
                    data = client_conn.recv(4096)
                except ConnectionResetError:
                    data = b''

                self.assertEqual(b'', data, "expected recv to fail")

        mgmt.delete(TCP_CONNECTOR_TYPE, name="EncapsConnector")
        mgmt.delete(TCP_LISTENER_TYPE, name="EncapsListener")

    def test_01_encaps_mismatch(self):
        """
        Attempt to configure incompatible TCP encapsulation on each side of the
        TCP path
        """
        self._test(ingress_encaps="legacy", egress_encaps="lite")
        self._test(ingress_encaps="lite", egress_encaps="legacy")


class TcpLiteCutthroughAbortTest(TestCase):
    """
    Verify that the TCP adaptor can properly handle an AMQP Aborted delivery
    """
    @classmethod
    def setUpClass(cls):
        super(TcpLiteCutthroughAbortTest, cls).setUpClass()
        cls.van_address = 'closest/cutthroughabort'
        cls.edge_port = cls.tester.get_port()
        cls.logger = Logger(title="TcpLiteCutthroughAbortTest",
                            print_to_console=False,
                            ofilename=os.path.join(os.path.dirname(os.getcwd()),
                                                   "setUpClass/TcpCutthroughAbort_server.log"))
        cls.echo_server = TcpEchoServer(prefix="TcpCutthroughAbortServer",
                                        port=0,
                                        logger=cls.logger)
        assert cls.echo_server.is_running

        e_config = [
            ('router', {'mode': 'edge', 'id': 'TcpCutthroughAbortEdge'}),
            ('listener', {'role': 'normal', 'port': cls.tester.get_port()}),
            ('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.edge_port}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        i_config = [
            ('router', {'mode': 'interior', 'id': 'TcpCutthroughAbortInt'}),
            ('listener', {'role': 'normal', 'port': cls.tester.get_port()}),
            ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.edge_port}),
            ('tcpConnector', {'host': "localhost",
                              'port': cls.echo_server.port,
                              'address': cls.van_address,
                              'encapsulation': 'lite'}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.i_router = cls.tester.qdrouterd('TcpCutthroughAbortInt',
                                            Qdrouterd.Config(i_config), wait=True)
        cls.e_router = cls.tester.qdrouterd('TcpCutthroughAbortEdge',
                                            Qdrouterd.Config(e_config), wait=True)
        cls.e_address = cls.e_router.addresses[0]

    def test_aborted_streaming_link(self):
        test = FakeClientStreamAborter(self.e_address, self.van_address, self.logger)
        test.run()
        self.assertIsNone(test.error)


class FakeClientStreamAborter(MessagingHandler):
    """
    Simulate a tcpListener by creating an inbound AMQP link to the service
    address and a reply-to. Initiate a streaming message to the service. Once
    data transfer is in progress abort the inbound AMQP delivery
    """
    def __init__(self, listener_address, van_address, logger):
        super(FakeClientStreamAborter, self).__init__(auto_settle=False)

        self.listener_address = listener_address
        self.van_address = van_address
        self.logger = logger

        self.error = None
        self.timer = None
        self.amqp_conn = None
        self.amqp_receiver = None
        self.amqp_sender = None
        self.inbound_dlv = None
        self.reply_to = None
        self.octets_rx = 0

    def done(self, error=None):
        self.logger.log("done")
        self.error = error
        if self.timer:
            self.timer.cancel()
        self.amqp_conn.close()
        if error is not None:
            self.logger.dump()

    def timeout(self):
        self.logger.log("Timeout Expired")
        self.timer = None
        self.done(error="Timeout Expired")

    def on_start(self, event):
        self.logger.log("on_start")
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.amqp_conn = event.container.connect(self.listener_address)
        self.amqp_receiver = event.container.create_receiver(self.amqp_conn,
                                                             dynamic=True)

    def on_link_opened(self, event):
        self.logger.log("on_link_opened")
        if event.receiver == self.amqp_receiver:
            self.reply_to = self.amqp_receiver.remote_source.address
            self.logger.log(f"dynamic reply-to {self.reply_to}, attaching sender...")
            self.amqp_sender = event.container.create_sender(self.amqp_conn,
                                                             self.van_address)

    def on_sendable(self, event):
        self.logger.log("on_sendable")
        if event.sender == self.amqp_sender:
            if self.inbound_dlv is None:
                self.logger.log("generating inbound message")
                assert self.reply_to is not None
                self.inbound_dlv = self.amqp_sender.delivery("INBOUND-DELIVERY")
                msg = Message()
                msg.address = self.van_address
                msg.reply_to = self.reply_to
                msg.correlation_id = "A0Z_P:1"
                msg.content_type = "application/octet-stream"
                encoded = msg.encode()
                # add body value null terminator
                encoded += b'\x00\x53\x77\x40'
                # and enough content to classify this message as streaming...
                encoded += b'ABC' * 100000
                self.amqp_sender.stream(encoded)

    def on_delivery(self, event):
        self.logger.log("on_delivery")
        dlv = event.delivery
        if dlv.readable:
            data = self.amqp_receiver.recv(dlv.pending)
            if data:
                #print(f"RECV-->'{data}'", flush=True)
                self.octets_rx += len(data)
                # fake update to TCP window:
                dlv.local.section_number = 0
                dlv.local.section_offset = self.octets_rx
                dlv.update(dlv.RECEIVED)
            if not self.inbound_dlv.aborted:
                # Continue streaming but mark end of stream
                self.logger.log("Sending payload")
                self.amqp_sender.stream(b'?' * 65535)
                self.amqp_sender.stream(b'END-OF-STREAM')

                if b'END-OF-STREAM' in data:
                    # end-of-stream echoed back, abort inbound delivery
                    self.logger.log(f"END-OF-STREAM received, aborting {self.inbound_dlv.tag}...")
                    self.inbound_dlv.abort()

            if dlv.partial is False:
                self.logger.log("End of echo stream, test complete")
                dlv.update(dlv.ACCEPTED)
                dlv.settle()
                self.done()

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

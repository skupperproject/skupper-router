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

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from skupper_router.management.error import ForbiddenStatus

from system_test import Logger
from system_test import Process
from system_test import Qdrouterd
from system_test import SkManager
from system_test import TIMEOUT
from system_test import TestCase
from system_test import TestTimeout
from system_test import main_module
from system_test import unittest
from system_test import retry
from system_test import CONNECTION_TYPE, TCP_CONNECTOR_TYPE, TCP_LISTENER_TYPE
from system_test import ROUTER_LINK_TYPE, ROUTER_METRICS_TYPE
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
    Wrapper for a TcpEchoClient. Provides automatic port assignment and client
    TLS configuration.
    """

    def __init__(self, test_name, client_n, logger, client, server, size,
                 count,
                 print_client_logs=True,
                 timeout=TIMEOUT,
                 port_override=None,
                 test_ssl=False,
                 delay_close=False):
        """
        Launch an echo client thread.  Raises an exception if the client fails.

        :param test_name: Unique name for log file prefix
        :param client_n: Client number for differentiating otherwise identical clients
        :param logger: parent logger for logging test activity vs. client activity
        :param client: router name to which the client connects
        :param server: name whose address the client is targeting
        :param size: length of messages in bytes
        :param count: number of messages to be sent/verified
        :param print_client_logs: verbosity switch
        :param port_override: TCP port to use otherwise use the test client ports
        :param test_ssl: if True use TLS on the connection
        :param delay_close: if True keep connection open after all data transfered
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

    def wait(self):
        """
        Block until the client completes. An exception is raised if the client
        failed.
        """
        self.e_client.wait()


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
    def setUpClass(cls, test_ssl=False):
        """Start a router"""
        super(TcpAdaptorBase, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

        def router(name, mode, connection, extra=None, ssl=False):
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
                                  'siteId': cls.site}
                listener_props_balanced_1 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][0],
                                             'address': 'ES_ALL_1',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'siteId': cls.site}
                listener_props_balanced_2 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][1],
                                             'address': 'ES_ALL_2',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'siteId': cls.site}
                listener_props_balanced_3 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][2],
                                             'address': 'ES_ALL_3',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'siteId': cls.site}
                listener_props_balanced_4 = {'host': "localhost",
                                             'port': cls.balanced_listener_ports[name][3],
                                             'address': 'ES_ALL_4',
                                             'sslProfile': tcp_listener_ssl_profile_name,
                                             'siteId': cls.site}

                connector_props_balanced_1 = {'name': "balanced-ES_ALL_1",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_1',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'siteId': cls.site}
                connector_props_balanced_2 = {'name': "balanced-ES_ALL_2",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_2',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'siteId': cls.site}
                connector_props_balanced_3 = {'name': "balanced-ES_ALL_3",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_3',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'siteId': cls.site}
                connector_props_balanced_4 = {'name': "balanced-ES_ALL_4",
                                              'host': "localhost",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_4',
                                              'sslProfile': 'tcp-connector-ssl-profile',
                                              'siteId': cls.site}

                connector_props_direct = {'host': "localhost",
                                          'port': cls.tcp_server_listener_ports[name],
                                          'address': 'ES_' + name,
                                          'sslProfile': 'tcp-connector-ssl-profile',
                                          'siteId': cls.site}

            else:
                listener_props = {'host': "0.0.0.0",
                                  'port': cls.nodest_listener_ports[name],
                                  'address': 'nodest',
                                  'siteId': cls.site}

                listener_props_balanced_1 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][0],
                                             'address': 'ES_ALL_1',
                                             'siteId': cls.site}
                listener_props_balanced_2 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][1],
                                             'address': 'ES_ALL_2',
                                             'siteId': cls.site}
                listener_props_balanced_3 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][2],
                                             'address': 'ES_ALL_3',
                                             'siteId': cls.site}
                listener_props_balanced_4 = {'host': "0.0.0.0",
                                             'port': cls.balanced_listener_ports[name][3],
                                             'address': 'ES_ALL_4',
                                             'siteId': cls.site}

                connector_props_balanced_1 = {'name': "balanced-ES_ALL_1",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_1',
                                              'siteId': cls.site}
                connector_props_balanced_2 = {'name': "balancedES_ALL_2",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_2',
                                              'siteId': cls.site}
                connector_props_balanced_3 = {'name': "balanced-ES_ALL_3",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_3',
                                              'siteId': cls.site}
                connector_props_balanced_4 = {'name': "balanced-ES_ALL_4",
                                              'host': "127.0.0.1",
                                              'port': cls.tcp_server_listener_ports[name],
                                              'address': 'ES_ALL_4',
                                              'siteId': cls.site}

                connector_props_direct = {'host': "127.0.0.1",
                                          'port': cls.tcp_server_listener_ports[name],
                                          'address': 'ES_' + name,
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
                                         'address': 'NS_EC2_CONN_STALL', 'siteId': cls.site})]

        if cls.test_ssl:
            # This listener will be used to test the authenticatePeer functionality.
            int_a_config.append(('tcpListener',
                                 {'host': "localhost",
                                  'port': cls.authenticate_peer_port,
                                  'sslProfile': 'tcp-listener-ssl-profile',
                                  'authenticatePeer': 'yes',
                                  'address': 'ES_INTA',
                                  'siteId': cls.site}))

        # Launch the routers using the sea of router ports
        cls.logger.log("TCP_TEST Launching interior routers")
        router('INTA', 'interior', int_a_config, ssl=cls.test_ssl)
        inter_router_port_BC = cls.tester.get_port()
        cls.INTB_edge_port = cls.tester.get_port()
        router('INTB', 'interior',
               [('connector', {'role': 'inter-router', 'port': inter_router_port_AB}),
                ('listener', {'role': 'inter-router', 'port': inter_router_port_BC}),
                ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl)

        cls.INTC_edge_port = cls.tester.get_port()
        router('INTC', 'interior',
               [('connector', {'role': 'inter-router', 'port': inter_router_port_BC}),
                ('listener', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port})], ssl=cls.test_ssl)

        cls.logger.log("TCP_TEST Launching edge routers")
        router('EA1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTA_edge_port})], ssl=cls.test_ssl)
        router('EA2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTA_edge_port})], ssl=cls.test_ssl)
        router('EB1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl)
        router('EB2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTB_edge_port})], ssl=cls.test_ssl)
        router('EC1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port})], ssl=cls.test_ssl)
        cls.EC2_conn_stall_listener_port = cls.tester.get_port()
        router('EC2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge', 'port': cls.INTC_edge_port}),
                ('tcpConnector', {'host': "127.0.0.1", 'port': cls.EC2_conn_stall_connector_port,
                                  'address': 'NS_EC2_CONN_STALL',
                                  'siteId': cls.site}),
                ('tcpListener', {'host': "0.0.0.0", 'port': cls.EC2_conn_stall_listener_port,
                                 'address': 'NS_EC2_CONN_STALL',
                                 'siteId': cls.site})],
               ssl=cls.test_ssl)

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
        Raise an Exception on failure

        :param test_name test name
        :param echo_pair_list list of EchoPair objects describing the test
        :param test_ssl should this function use ssl ?
        """
        self.logger.log("TCP_TEST %s Start do_tcp_echo_n_routers" % (test_name))
        runners = []
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
                        runners.append(runner)
                        client_num += 1

            # Wait for the clients to finish transferring data.
            # This will raise an exception (fail the test) if any client fails

            for runner in runners:
                runner.wait()

            # Make sure servers are still up and did not fail
            for rtr in TcpAdaptor.router_order:
                es = TcpAdaptor.echo_servers[rtr]
                if es.error is not None:
                    error = f"TCP_TEST {test_name} Server {es.prefix} stopped with error: {es.error}"
                    self.logger.log(error)
                    raise Exception(error)

        except Exception as exc:
            self.logger.log("TCP_TEST %s failed. Exception: %s" %
                            (test_name, traceback.format_exc()))
            raise

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
        Launch 'count' echo client connected to single Router.
        Wait for data to be passed.
        Test that every instance of the echo server has seen least one connection opened.
        Raise an Exception on failure

        :param test_name test name
        :param client router to which echo clients attach
        :param count number of connections to be established
        :return: None if success else error message for ctest
        """
        self.logger.log("TCP_TEST %s do_tcp_balance_test (ingress: %s connections: %d)" % (test_name, client, count))
        runners = []

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
                runners.append(runner)

            # Wait for the runners to complete. An Exception is raised should a
            # runner fail
            for runner in runners:
                runner.wait()

            # Check for errors on the echo servers
            for rtr in TcpAdaptor.router_order:
                es = TcpAdaptor.echo_servers[rtr]
                if es.error is not None:
                    error = f"TCP_TEST {test_name} Server {es.prefix} stopped with error: {es.error}"
                    self.logger.log(error)
                    raise Exception(error)

            # Verify that all balanced connectors saw at least one new connection

            metrics = self.all_balanced_connector_stats(iter_num)
            diffs = {}
            fail = False
            for i in range(len(metrics)):
                diff = metrics[i] - baseline[i]
                diffs[self.router_order[i]] = diff
                if diff == 0:
                    fail = True
            if fail:
                error = "At least one server did not receive a connection: origin=%s: counts=%s" % (client, diffs)
                self.logger.log(error)
                raise Exception(error)
            self.logger.log(f"TCP_TEST {test_name} SUCCESS: origin={client} counts={diffs} " + over_tls)

        except Exception as exc:
            self.logger.log("TCP_TEST %s failed. Exception: %s" %
                            (test_name, traceback.format_exc()))
            raise

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
                self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
                self.logger.log("TCP_TEST test_01_tcp_basic_connectivity Stop %s SUCCESS" % name)

    # larger messages
    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_10_tcp_INTA_INTA_100(self):
        name = "test_10_tcp_INTA_INTA_100"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[100])]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_11_tcp_INTA_INTA_1000(self):
        name = "test_11_tcp_INTA_INTA_1000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[1000])]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_12_tcp_INTA_INTA_500000(self):
        name = "test_12_tcp_INTA_INTA_500000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA, sizes=[500000])]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_13_tcp_EA1_EC2_500000(self):
        name = "test_13_tcp_EA1_EC2_500000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA1, self.EC2, sizes=[500000])]
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    # This test sends and receives 2 million bytes from edge router to edge router
    # which will make the code go in and out of the TCP window.
    def test_14_tcp_EA2_EC1_2000000(self):
        name = "test_14_tcp_EA2_EC1_2000000"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA2, self.EC1, sizes=[2000000])]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

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
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        # TODO: This test passes but in passing router INTA crashes undetected.
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    # concurrent messages
    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_50_concurrent_interior(self):
        name = "test_50_concurrent_interior"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.INTA, self.INTA),
                 self.EchoPair(self.INTB, self.INTB)]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
        self.logger.log("TCP_TEST Stop %s SUCCESS" % name)

    @unittest.skipIf(DISABLE_SELECTOR_TESTS, DISABLE_SELECTOR_REASON)
    def test_50_concurrent_edges(self):
        name = "test_50_concurrent_edges"
        self.logger.log("TCP_TEST Start %s" % name)
        pairs = [self.EchoPair(self.EA1, self.EA2),
                 self.EchoPair(self.EA2, self.EC2)]
        self.do_tcp_echo_n_routers(name, pairs, test_ssl=self.test_ssl)
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
            self.do_tcp_balance_test(tname, p[0], p[1], iter_num, test_ssl=self.test_ssl)
            iter_num = iter_num + 1
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


class TcpAdaptorStuckDeliveryTest(TestCase):
    """
    Verify that the routers stuck delivery detection is not applied to TCP
    deliveries.  See Dispatch-2036.
    """
    @classmethod
    def setUpClass(cls, test_name='TCPStuckDeliveryTest'):
        super(TcpAdaptorStuckDeliveryTest, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

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
                                                 'address': 'nostuck'})])
        cls.i_router.wait_ready()
        cls.e_router = router(cls,
                              "%s_E" % cls.test_name,
                              "edge",
                              [('tcpListener', {'host': "0.0.0.0",
                                                'port':
                                                cls.edge_tcp_listener_port,
                                                'address': 'nostuck'}),
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


class TcpAdaptorManagementTest(TestCase):
    """
    Test Creation and deletion of TCP management entities
    """
    def _query_links_by_addr(self, router_mgmt, owning_addr):
        attrs = ['owningAddr', 'linkDir']

        links = []
        rc = router_mgmt.query(type=ROUTER_LINK_TYPE, attribute_names=attrs).results
        for link in rc:
            if link[0] is not None and link[0].endswith(owning_addr):
                links.append(link)
        return links

    @classmethod
    def setUpClass(cls, test_name='TCPMgmtTest'):
        super(TcpAdaptorManagementTest, cls).setUpClass()

        if DISABLE_SELECTOR_TESTS:
            return

        cls.test_name = test_name

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
                                'host': '127.0.0.1'})
        mgmt.create(type=TCP_CONNECTOR_TYPE,
                    name=connector_name,
                    attributes={'address': van_address,
                                'port': self.tcp_server_port,
                                'host': '127.0.0.1'})

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
                                    'host': '127.0.0.1'})
            mgmt.create(type=TCP_CONNECTOR_TYPE,
                        name=connector_name,
                        attributes={'address': van_address,
                                    'port': self.tcp_server_port,
                                    'host': '127.0.0.1'})

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
            self.assertEqual(1, l_stats['connectionsOpened'])
            self.assertEqual(1, l_stats['connectionsClosed'])

            c_stats = mgmt.read(type=TCP_CONNECTOR_TYPE, name=connector_name)
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
        super(TcpAdaptorManagementLiteTest, cls).setUpClass(test_name='TCPMgmtLiteTest')


class TcpAdaptorListenerConnectTest(TestCase):
    """
    Test client connecting to TcpListeners in various scenarios
    """
    @classmethod
    def setUpClass(cls, test_name='TCPListenConnTest'):
        super(TcpAdaptorListenerConnectTest, cls).setUpClass()

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
                                  'port': listener_port})

        b_mgmt = self.INTB.management
        b_mgmt.create(type=TCP_CONNECTOR_TYPE,
                      name="ServerConnector01",
                      attributes={'address': van_address,
                                  'host': '127.0.0.1',
                                  'port': connector_port})

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
                                  'port': listener_port})

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
                                      'port': connector_port})

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


class TcpDeleteConnectionTest(TestCase):
    @classmethod
    def setUpClass(cls, test_name="TcpDeleteConnectionTest"):
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
              'siteId': "mySite"}),

            ('tcpConnector',
             {'name': "good-connector",
              'host': "localhost",
              'port': cls.echo_server.port,
              'address': 'ES_GOOD_CONNECTOR_CERT_INTA',
              'siteId': "mySite"}),
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
        sk_manager = SkManager(self.address)
        conn_id = None
        results = sk_manager.query(CONNECTION_TYPE)
        for result in results:
            conn_direction = result['dir']
            # Find the id of the tcp connection we want to delete.
            if conn_direction == 'out' and result['host'] != 'egress-dispatch':
                # Delete the connection by updating the adminStatus to deleted.
                sk_manager.update(CONNECTION_TYPE, {"adminStatus": "deleted"}, identity=result['identity'])
                conn_id = result['identity']
                break
        self.assertIsNotNone(conn_id, "Expected connection id to be not None")

        def check_connection_deleted():
            outs = sk_manager.query(CONNECTION_TYPE)
            is_conn_present = False
            for out in outs:
                if out['identity'] == conn_id:
                    is_conn_present = True
                    break
            self.assertFalse(is_conn_present)

        # Keep retrying until the connection is gone from the connection table.
        retry_assertion(check_connection_deleted, delay=2)
        client_conn.close()


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
        rc = self.router.management.query(type=ROUTER_METRICS_TYPE,
                                          attribute_names=attributes)
        self.assertIsNotNone(rc, "unexpected query failure")
        self.assertEqual(1, len(rc.get_dicts()), "expected one attribute!")
        counters = rc.get_dicts()[0].get("connectionCounters")
        self.assertIsNotNone(counters, "expected a counter map to be returned")
        return counters

    def _run_test(self, idle_ct, active_ct):
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
                                    'port': listener_port})
            mgmt.create(type=TCP_CONNECTOR_TYPE,
                        name="ServerConnector",
                        attributes={'address': 'closest/tcpService',
                                    'host': "localhost",
                                    'port': connector_port})

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
        self._run_test(idle_ct=1, active_ct=3)


class TcpAdaptorNoDelayedDelivery(TestCase):
    """
    Ensure long lived TCP sessions are not counted as delayed deliveries
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorNoDelayedDelivery, cls).setUpClass()

        cls.listener_port = cls.tester.get_port()
        cls.connector_port = cls.tester.get_port()
        config = [
            ('router', {'mode': 'interior',
                        'id': 'TCPNoDelay'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpListener', {'host': "0.0.0.0",
                             'port': cls.listener_port,
                             'address': "no/delay"}),
            ('tcpConnector', {'host': "127.0.0.1",
                              'port': cls.connector_port,
                              'address': "no/delay"}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('TCPNoDelay', config)

    def _get_delayed_counters(self) -> Tuple[int, int]:
        attributes = ["deliveriesDelayed1Sec", "deliveriesDelayed10Sec"]
        rc = self.router.management.query(type=ROUTER_METRICS_TYPE,
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


class TcpCutthroughAbortTest(TestCase):
    """
    Verify that the TCP adaptor can properly handle an AMQP Aborted delivery
    """
    @classmethod
    def setUpClass(cls):
        super(TcpCutthroughAbortTest, cls).setUpClass()
        cls.van_address = 'closest/cutthroughabort'
        cls.edge_port = cls.tester.get_port()
        cls.logger = Logger(title="TcpCutthroughAbortTest",
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
                              'address': cls.van_address}),
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
        self.payload_tx = 0
        self.payload_rx = 0

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
                msg.properties = {"V": 1}  # version
                encoded = msg.encode()
                # add body value null terminator
                encoded += b'\x00\x53\x77\x40'
                # and enough content to classify this message as streaming...
                self.payload_tx = 100000
                encoded += b'ABC' * self.payload_tx
                self.amqp_sender.stream(encoded)

    def on_delivery(self, event):
        self.logger.log("on_delivery")
        dlv = event.delivery
        if dlv.readable:
            # Reply data is available. This indicates that the return path
            # has been established. Now we can abort the inbound message.
            data = self.amqp_receiver.recv(dlv.pending)
            if data:
                self.payload_rx += len(data)
                self.logger.log(f"client recv--> ({len(data)}) '{data.decode(errors='replace')[:100]} ...'")
                # Fake update to TCP window, do not acknowledge msg header octets
                # which makes the total number of octets received greater than
                # the total number of octets sent (ISSUE #1452)
                dlv.local.section_number = 0
                dlv.local.section_offset = min(self.payload_rx, self.payload_tx)
                dlv.update(dlv.RECEIVED)

                if not self.inbound_dlv.aborted:
                    self.logger.log(f"Reply received, aborting {self.inbound_dlv.tag}...")
                    self.inbound_dlv.abort()

            if dlv.partial is False:
                self.logger.log("End of echo stream, test complete")
                dlv.update(dlv.ACCEPTED)
                dlv.settle()
                self.done()

    def run(self):
        Container(self).run()


class TcpAdaptorVersionTest(TestCase):
    """
    Ensure the correct protocol mapping version is advertised in the
    AMQP message properties
    """
    @classmethod
    def setUpClass(cls):
        super(TcpAdaptorVersionTest, cls).setUpClass()

        cls.listener_port = cls.tester.get_port()
        cls.connector_port = cls.tester.get_port()

        config = [
            ('router', {'mode': 'interior',
                        'id': 'TCPVersionTest'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),

            ('tcpListener', {'host': "0.0.0.0", 'port': cls.listener_port,
                             'address': 'Listener/Addr'}),

            ('tcpConnector', {'host': "127.0.0.1", 'port': cls.connector_port,
                              'address': 'Connector/Addr'})
        ]
        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('TCPVersionTest', config)

    def test_01_check_client_version(self):
        """
        Verify the proper version is added by the listener-facing TCP Adaptor
        """
        tester = ServerVersionTest(server_address=self.router.addresses[0],
                                   listener_port=self.listener_port)
        tester.run()
        self.assertIsNone(tester.error, f"Error: {tester.error}")

    def test_01_check_server_version(self):
        """
        Verify the proper version is added by the connector-facing TCP Adaptor
        """
        logger = Logger(title="VersionChecker")
        echo_server = TcpEchoServer(port=self.connector_port,
                                    logger=logger)
        assert echo_server.is_running
        tester = ClientVersionTest(router_address=self.router.addresses[0])
        tester.run()
        echo_server.wait()
        self.assertIsNone(tester.error, f"Error: {tester.error}")


class ServerVersionTest(MessagingHandler):
    """
    Verify that the Listener-facing TCP adaptor adds the correct version to the
    internal application properties field in the streaming message

    This creates an AMQP receiver for "Listener/Addr" and then attempts to
    connect and AMQP client to the router's tcpListener. When the streaming
    message arrives as the AMQP receiver it checks the version in the
    application properties header field.
    """
    def __init__(self, server_address, listener_port):
        super(ServerVersionTest, self).__init__()
        self.service_address = "Listener/Addr"
        self.error = None
        self.timer = None

        # fake server connection, receive link for Listener adaptor message,
        # send link for reply-to
        self.server_address = server_address
        self.server_conn = None
        self.server_sender = None
        self.server_receiver = None
        self.server_sent = False

        # The "client" message that arrives at the "server" is
        # streaming. Proton does not give us an "on_message" callback since it
        # never completes. Wait long enough for the headers to arrive so we can
        # extract the application properties
        self.request_dlv = None
        self.dlv_drain_timer = None

        # tcp client, just opens a socket to the TCP listener. This initiates
        # the ingress streaming request message.
        self.listener_port = listener_port
        self.client_socket = None

    def done(self, error=None):
        # print(f"DONE error={error}", flush=True)
        self.error = error
        if self.timer:
            self.timer.cancel()
        self.server_conn.close()
        if self.client_socket is not None:
            self.client_socket.close()
        if self.dlv_drain_timer:
            self.dlv_drain_timer.cancel()

    def timeout(self):
        self.timer = None
        self.done(error=f"Timeout Expired: server_sent={self.server_sent}")

    def connect_client(self):
        while True:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(TIMEOUT)
            try:
                client.connect(('127.0.0.1', self.listener_port))
                return client
            except ConnectionRefusedError:
                # keep retrying until the router listener is opened
                client.close()
                print("client connection refused, retrying...", flush=True)
                time.sleep(0.5)
                continue

    def on_start(self, event):
        # Create an AMQP receiver for the service address. This will simulate a
        # fake server and activate the TCP adaptor listener port
        # print("ON START", flush=True)
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.server_conn = event.container.connect(self.server_address)
        self.server_receiver = event.container.create_receiver(self.server_conn,
                                                               self.service_address)

    def on_timer_task(self, event):
        # At this point we expect the reply-to header to have arrived. Use the
        # reply-to address to send the 'msg' parameter to the client as the
        # response streaming message.
        # print("ON TIMER", flush=True)
        data = self.server_receiver.recv(self.request_dlv.pending)
        msg = Message()
        try:
            msg.decode(data)
        except Exception as exc:
            self.done(error=f"Incomplete request msg headers {data}")
            return

        if "V" not in msg.properties:
            self.done(error=f"No 'V' key: {msg.properties}")
            return

        version = msg.properties['V']
        if version != 1:
            self.done(error=f"Expected version of 1: {msg.properties}")
            return

        self.request_dlv.settle()
        self.done(error=None)

    def on_delivery(self, event):
        # We've received the start of the client request message. In order to
        # ensure the properties field has arrive wait a bit
        # print("ON DLV", flush=True)
        if event.receiver == self.server_receiver:
            if self.request_dlv is None and event.delivery.readable:
                # sleep a bit to allow all the header data to arrive on the
                # delivery
                self.request_dlv = event.delivery
                self.dlv_drain_timer = event.reactor.schedule(1.0, self)

    def on_link_opened(self, event):
        # print("ON link opened", flush=True)
        if event.receiver == self.server_receiver:
            # "server" ready to take requests, fire up the "client". All we
            # need is to connect since that will activate the tcp adaptor
            # client-side due to the AMQP @open handshake.
            self.client_socket = self.connect_client()

    def run(self):
        Container(self).run()


class ClientVersionTest(MessagingHandler):
    """
    Simulate a tcpListener by creating an inbound AMQP link to the service
    address and a reply-to. Initiate a streaming message to the service. Once
    the reply message arrives from the connector side TCP Adaptor verify the
    version.
    """
    def __init__(self, router_address):
        super(ClientVersionTest, self).__init__()
        self.service_address = "Connector/Addr"
        self.router_address = router_address
        self.error = None
        self.timer = None

        self.client_conn = None
        self.client_receiver = None
        self.client_sender = None
        self.reply_to = None
        self.message_sent = False

        # Reply from server. Since it is streaming Proton will not give us an
        # "on_message" callback. Wait long enough for all message headers to
        # arrive before attempting to extract the application properties
        self.reply_dlv = None
        self.dlv_drain_timer = None

    def done(self, error=None):
        # print(f"DONE error={error}", flush=True)
        self.error = error
        if self.timer:
            self.timer.cancel()
        self.client_conn.close()
        if self.dlv_drain_timer is not None:
            self.dlv_drain_timer.cancel()

    def timeout(self):
        self.timer = None
        self.done(error="Timeout Expired")

    def on_start(self, event):
        # print("ONE START", flush=True)
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.client_conn = event.container.connect(self.router_address)
        self.client_receiver = event.container.create_receiver(self.client_conn,
                                                               dynamic=True)

    def on_timer_task(self, event):
        # At this point we expect the reply message header is completely
        # received and we can extract the properties
        # print("ON TIMER", flush=True)
        data = self.client_receiver.recv(self.reply_dlv.pending)
        msg = Message()
        try:
            msg.decode(data)
        except Exception as exc:
            self.done(error=f"Incomplete request msg headers {data}")
            return

        if "V" not in msg.properties:
            self.done(error=f"No 'V' key: {msg.properties}")
            return

        version = msg.properties['V']
        if version != 1:
            self.done(error=f"Expected version of 1: {msg.properties}")
            return

        self.reply_dlv.settle()
        self.done(error=None)

    def on_link_opened(self, event):
        # print("on_link_opened", flush=True)
        if event.receiver == self.client_receiver:
            self.reply_to = self.client_receiver.remote_source.address
            # print(f"dynamic reply-to {self.reply_to}, attaching sender...", flush=True)
            self.client_sender = event.container.create_sender(self.client_conn,
                                                               self.service_address)

    def on_sendable(self, event):
        # print("on_sendable", flush=True)
        if event.sender == self.client_sender:
            if self.message_sent is False:
                # print("generating inbound message", flush=True)
                self.message_sent = True
                assert self.reply_to is not None
                dlv = self.client_sender.delivery("INBOUND-DELIVERY")
                msg = Message()
                msg.address = self.service_address
                msg.reply_to = self.reply_to
                msg.correlation_id = "A0Z_P:1"
                msg.content_type = "application/octet-stream"
                msg.properties = {"V": 1}  # version
                encoded = msg.encode()
                # add body value null terminator
                encoded += b'\x00\x53\x77\x40'
                # and enough content to classify this message as streaming...
                self.payload_tx = 100000
                encoded += b'ABC' * self.payload_tx
                self.client_sender.stream(encoded)

    def on_delivery(self, event):
        # Reply returned from connector TCP adaptor
        # print("on_delivery", flush=True)
        if event.receiver == self.client_receiver:
            if self.reply_dlv is None and event.delivery.readable:
                self.reply_dlv = event.delivery
                self.dlv_drain_timer = event.reactor.schedule(1.0, self)

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

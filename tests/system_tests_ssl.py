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

"""
Provides tests related with allowed TLS protocol version restrictions.
"""
import os
import time
import unittest
from subprocess import Popen, PIPE

import cproton
from proton import SASL, Url, SSLDomain, SSLUnavailable, ConnectionException
from proton.utils import BlockingConnection

from skupper_router.management.client import Node
from system_test import TIMEOUT, TestCase, main_module, Qdrouterd, DIR
from system_test import unittest, retry


def protocol_name(proto):
    # depending on the version of openssl the protocol name for TLSv1 may be
    # either "TLSv1" or "TLSv1.0. Convert to "TLSv1" when needed.
    if proto.endswith(".0"):
        proto = proto[:-2]
    return proto


class RouterTestSslBase(TestCase):
    """
    Base class to help with SSL related testing.
    """
    # If unable to determine which protocol versions are allowed system wide
    DISABLE_SSL_TESTING = False
    DISABLE_REASON = "Unable to determine MinProtocol"

    @classmethod
    def setUpClass(cls):
        super(RouterTestSslBase, cls).setUpClass()

        cls.PROTON_VERSIONS = []

        # Determine those TLS versions that Proton recognizes. Restrict the
        # tested protocol versions to those that are no longer considered
        # insecure since support for older versions is going away.
        all_versions = ['TLSv1.2',
                        'TLSv1.3']
        for version in all_versions:
            try:
                dummydomain = SSLDomain(SSLDomain.MODE_CLIENT)
                rc = cproton.pn_ssl_domain_set_protocols(dummydomain._domain,
                                                         version)
                if rc == cproton.PN_OK:
                    cls.PROTON_VERSIONS.append(version)
            except SSLUnavailable:
                cls.DISABLE_SSL_TESTING = True
                cls.DISABLE_REASON = "Proton SSL Unavailable"
                return

        cls.assertTrue(len(cls.PROTON_VERSIONS) > 0,
                       "Failed to find any supported protocol versions!")
        cls.ALL_VERSIONS = ' '.join(cls.PROTON_VERSIONS)

    @staticmethod
    def ssl_file(name):
        """
        Returns fully qualified ssl certificate file name
        :param name:
        :return:
        """
        return os.path.join(DIR, 'ssl_certs', name)

    @classmethod
    def get_byte_string(cls, file_path):
        with open(file_path, 'rb') as f:
            credentials = f.read()
        return credentials

    @classmethod
    def create_sasl_files(cls):
        """
        Creates the SASL DB
        :return:
        """
        # Create a sasl database.
        pipe = Popen(['saslpasswd2', '-c', '-p', '-f', 'qdrouterd.sasldb',
                      '-u', 'domain.com', 'test'],
                     stdin=PIPE, stdout=PIPE, stderr=PIPE,
                     universal_newlines=True)
        result = pipe.communicate('password')
        assert pipe.returncode == 0, \
            "saslpasswd2 exit status %s, output:\n%s" % (pipe.returncode, result)

        # Create a SASL configuration file.
        with open('tests-mech-PLAIN.conf', 'w') as sasl_conf:
            sasl_conf.write("""
pwcheck_method: auxprop
auxprop_plugin: sasldb
sasldb_path: qdrouterd.sasldb
mech_list: ANONYMOUS DIGEST-MD5 EXTERNAL PLAIN
# The following line stops spurious 'sql_select option missing' errors when cyrus-sql-sasl plugin is installed
sql_select: dummy select
""")


class RouterTestSslClient(RouterTestSslBase):
    """
    Starts a router with multiple listeners, all of them using an sslProfile.
    Then it runs multiple tests to validate that only the allowed protocol versions
    are being accepted through the related listener.
    """
    @classmethod
    def setUpClass(cls):
        """
        Prepares a single router with multiple listeners, each one associated with a particular
        sslProfile and each sslProfile has its own specific set of allowed protocols.
        """
        super(RouterTestSslClient, cls).setUpClass()

        if SASL.extended():
            conf = [('router', {'id': 'QDR.A',
                                'mode': 'interior',
                                'saslConfigName': 'tests-mech-PLAIN',
                                'saslConfigDir': os.getcwd()})]

            # Generate authentication DB
            super(RouterTestSslClient, cls).create_sasl_files()
        else:
            conf = [('router', {'id': 'QDR.A',
                                'mode': 'interior'})]

        conf += [
            # for management access:
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                          cls.tester.get_port()})
        ]

        # generate listeners for each protocol version supported by Proton

        cls.TLS_PORT_VERSION_MAP = {}
        for version in cls.PROTON_VERSIONS:
            cls.TLS_PORT_VERSION_MAP[version] = cls.tester.get_port()
            conf += [
                ('sslProfile', {'name': f"ssl-profile-{version}",
                                'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                'certFile': cls.ssl_file('server-certificate.pem'),
                                'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                                'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:'
                                'DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                                'protocols': version,
                                'password': 'server-password'}),
                ('listener', {'host': '0.0.0.0', 'role': 'normal',
                              'port': cls.TLS_PORT_VERSION_MAP[version],
                              'authenticatePeer': 'no',
                              'requireSsl': 'yes',
                              'sslProfile': f"ssl-profile-{version}"})
            ]

        # Adding SASL listener only when SASL is available
        if SASL.extended():
            cls.PORT_TLS_SASL = cls.tester.get_port()
            conf += [
                # TLS SASL PLAIN authentication for proton client validation
                ('sslProfile', {'name': 'ssl-profile-tls-all',
                                'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                'certFile': cls.ssl_file('server-certificate.pem'),
                                'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                                'password': 'server-password'}),
                ('listener', {'host': '0.0.0.0', 'role': 'normal',
                              'port': cls.PORT_TLS_SASL,
                              'authenticatePeer': 'yes', 'saslMechanisms': 'PLAIN',
                              'requireSsl': 'yes',
                              'requireEncryption': 'yes',
                              'sslProfile': 'ssl-profile-tls-all'}),
            ]

        config = Qdrouterd.Config(conf)
        cls.router = cls.tester.qdrouterd("A", config, wait=False)
        cls.router.wait_ports()

    def check_tls_protocol(self, mgmt, listener_port, tls_protocol,
                           sasl_enabled=False, sasl_user=None,
                           sasl_mechs=None, client_password=None):
        """ Uses a Proton client to connect to the router via the provided TCP
        port using the specified TLS protocol version(s). If the connection
        succeeds the resulting connection's encryption and sasl state returned
        to the caller for verification. If the connection fails None is
        returned.

        tls_protocol is a space separated string of TLS protocol version names,
        example: 'TLSv1.2 TLSv13.'. If tls_protocol is None, then there is no
        restriction of the protocol to use and the router will pick the best
        available protocol.

        If sasl_enabled is true use client authentication via SASL.
        """
        ATTR_NAMES = ['ssl', 'sslProto', 'sasl', 'isAuthenticated',
                      'isEncrypted', 'user']

        CONN_TYPE = 'io.skupper.router.connection'

        # Management address to connect using the given TLS protocol
        url = Url("amqps://0.0.0.0:%d/$management" % listener_port)

        # Preparing SSLDomain (client cert) and SASL authentication info
        domain = SSLDomain(SSLDomain.MODE_CLIENT)
        if sasl_enabled:
            domain.set_credentials(self.ssl_file('client-certificate.pem'),
                                   self.ssl_file('client-private-key.pem'),
                                   'client-password')
        domain.set_trusted_ca_db(self.ssl_file('ca-certificate.pem'))
        domain.set_peer_authentication(SSLDomain.VERIFY_PEER)

        # Restrict client to using only the given TLS protocol version.
        if tls_protocol:
            ok = cproton.pn_ssl_domain_set_protocols(domain._domain,
                                                     tls_protocol)
            self.assertEqual(ok, cproton.PN_OK,
                             f"Test error: {tls_protocol} not supported")

        # Try opening the secure and authenticated connection
        try:
            connection = BlockingConnection(url, sasl_enabled=sasl_enabled,
                                            ssl_domain=domain, timeout=TIMEOUT,
                                            allowed_mechs=sasl_mechs,
                                            user=sasl_user,
                                            password=client_password)
        except Exception as exc:
            # Connection failed
            return None

        # get the TLS/SASL state for the new connection. This check assumes
        # that the new connection is the only tls connection present on the
        # router!

        def _get_tls_conn():
            conns = mgmt.query(type=CONN_TYPE,
                               attribute_names=ATTR_NAMES).get_entities()
            ssl_conns = [c for c in conns if c['ssl']]
            if ssl_conns:
                self.assertEqual(1, len(ssl_conns),
                                 f"Test expects 1 TLS conn: {ssl_conns}")
                return ssl_conns[0]
            return None
        ssl_conn = retry(_get_tls_conn)
        self.assertIsNotNone(ssl_conn, "Failed to find new SSL connection")

        connection.close()

        # Cleanup: wait until the SSL connection is cleaned up on the router so it will
        # not interfere with other tests

        def _wait_conn_gone():
            conns = mgmt.query(type=CONN_TYPE,
                               attribute_names=ATTR_NAMES).get_entities()
            if len([c for c in conns if c['ssl']]) == 0:
                return True
            return False
        gone = retry(_wait_conn_gone)
        self.assertTrue(gone, "Failed to clean up test SSL connection")
        return ssl_conn

    def test_tls_protocol_versions_client(self):
        """
        Test all available protocols via client connections to the
        router. Ensure that the configured TLS protocol versions are used.
        """
        if self.DISABLE_SSL_TESTING:
            self.skipTest(self.DISABLE_REASON)

        mgmt = self.router.management

        # for every listener ensure that the router will allow clients
        # to connect using only the allowed version

        for version, port in self.TLS_PORT_VERSION_MAP.items():
            result = self.check_tls_protocol(mgmt, port, self.ALL_VERSIONS)
            self.assertIsNotNone(result,
                                 f"Failed to connect with version {version}")
            self.assertTrue(result['isEncrypted'],
                            f"Connection not encrypted {result}")
            self.assertEqual(version, protocol_name(result['sslProto']),
                             f"Unexpected sslProto value: {result}")

            # attempt to connect a non-secured client, expect failure:

            url = Url("amqps://0.0.0.0:%d/$management" % port)
            with self.assertRaises(ConnectionException, msg="Expected connection failure"):
                connection = BlockingConnection(url, ssl_domain=None, timeout=TIMEOUT)

    def test_tls_ssl_sasl_client(self):
        """
        Attempts connecting a Proton client using a valid SASL authentication info
        and forcing the TLS protocol version, which should be accepted by the listener.
        :return:
        """
        if self.DISABLE_SSL_TESTING:
            self.skipTest(self.DISABLE_REASON)

        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        mgmt = self.router.management

        # Verify that SASL succeeds for all supported TLS versions

        for version in self.PROTON_VERSIONS:
            result = self.check_tls_protocol(mgmt,
                                             self.PORT_TLS_SASL,
                                             version,
                                             sasl_enabled=True,
                                             sasl_user='test@domain.com',
                                             sasl_mechs='PLAIN',
                                             client_password='password')
            self.assertIsNotNone(result, f"Failed to connect with {version}")
            self.assertTrue(result['isEncrypted'],
                            f"Connection not encrypted {result}")
            self.assertEqual(version, protocol_name(result['sslProto']),
                             f"Unexpected sslProto value: {result}")
            self.assertEqual("PLAIN", result['sasl'],
                             f"Wrong SASL mechanism: {result['sasl']}")
            self.assertTrue(result['isAuthenticated'], "SASL not authenticated properly")
            self.assertEqual('test@domain.com', result['user'],
                             "Unexpected SASL user")

        # ensure that the connection fails if client attempts non-SSL connection

        url = Url("amqps://0.0.0.0:%d/$management" % self.PORT_TLS_SASL)
        with self.assertRaises(ConnectionException, msg="Expected connection failure"):
            connection = BlockingConnection(url,
                                            ssl_domain=None,
                                            sasl_enabled=True,
                                            user='test@domain.com',
                                            allowed_mechs='PLAIN',
                                            password='password',
                                            timeout=TIMEOUT)


class RouterTestSslInterRouter(RouterTestSslBase):
    """
    Verifies that the SSL/TLS configurations on inter-router connections are
    correctly implemented.
    """

    @classmethod
    def setUpClass(cls):
        """
        """
        super(RouterTestSslInterRouter, cls).setUpClass()

        if not SASL.extended() or cls.DISABLE_SSL_TESTING:
            return

        os.environ["ENV_SASL_PASSWORD"] = "password"

        # expect 3 connections per connector: 1 inter-router, 2 inter-router-data
        cls.inter_router_conn_count = 3

        # Generate authentication DB
        super(RouterTestSslInterRouter, cls).create_sasl_files()

        cls.PORT_TLS_ALL = cls.tester.get_port()

        conf = [
            ('router', {'id': 'QDR.A',
                        'mode': 'interior',
                        'saslConfigName': 'tests-mech-PLAIN',
                        'saslConfigDir': os.getcwd()}),

            # For management access:
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                          cls.tester.get_port()}),

            # Listener allowing all TLS supported versions
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router',
                          'port': cls.PORT_TLS_ALL,
                          'authenticatePeer': 'yes', 'saslMechanisms': 'PLAIN',
                          'requireEncryption': 'yes', 'requireSsl': 'yes',
                          'sslProfile': 'ssl-profile-tls-all'}),
            ('sslProfile', {'name': 'ssl-profile-tls-all',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'password': 'server-password'}),
        ]

        # create inter-router listeners that restrict the allowed TLS version

        cls.TLS_PORT_VERSION_MAP = {}
        for version in cls.PROTON_VERSIONS:
            cls.TLS_PORT_VERSION_MAP[version] = cls.tester.get_port()
            conf += [
                ('sslProfile', {'name': f"ssl-profile-{version}",
                                'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                'certFile': cls.ssl_file('server-certificate.pem'),
                                'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                                'protocols': version,
                                'password': 'server-password'}),
                ('listener', {'host': '0.0.0.0', 'role': 'inter-router',
                              'port': cls.TLS_PORT_VERSION_MAP[version],
                              'authenticatePeer': 'yes', 'saslMechanisms': 'PLAIN',
                              'requireEncryption': 'yes', 'requireSsl': 'yes',
                              'sslProfile': f"ssl-profile-{version}"})
            ]

        conf = Qdrouterd.Config(conf)
        cls.router_a = cls.tester.qdrouterd("A", conf, wait=True)

        # create a router that will connect to the unrestricted TLS listener:

        conf = [
            ('router', {'id': 'UNRESTRICTED', 'mode': 'interior'}),

            # For management access:
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                          cls.tester.get_port()}),

            ('sslProfile', {'name': "ssl-profile-tls-all",
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('client-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                            'password': 'client-password'}),

            ('connector', {'host': '0.0.0.0', 'role': 'inter-router',
                           'port': cls.PORT_TLS_ALL,
                           'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                           'saslPassword': 'env:ENV_SASL_PASSWORD',
                           'saslUsername': 'test@domain.com',
                           'sslProfile': 'ssl-profile-tls-all',
                           'dataConnectionCount': '2'})
        ]
        conf = Qdrouterd.Config(conf)
        cls.router_unrestricted = cls.tester.qdrouterd("UNRESTRICTED", conf, wait=False)
        cls.router_unrestricted.wait_ports()

        cls.routers_any = {}
        cls.routers_only = {}
        for version in cls.PROTON_VERSIONS:

            # allow any, connection to 'version'
            conf = [
                ('router', {'id': f'ANY-{version}', 'mode': 'interior'}),

                # For management access:
                ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                              cls.tester.get_port()}),

                ('sslProfile', {'name': "ssl-profile-all",
                                'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                'certFile': cls.ssl_file('client-certificate.pem'),
                                'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                                'password': 'client-password'}),

                ('connector', {'host': '0.0.0.0', 'role': 'inter-router',
                               'port': cls.TLS_PORT_VERSION_MAP[version],
                               'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                               'saslUsername': 'test@domain.com', 'saslPassword': 'pass:password',
                               'sslProfile': 'ssl-profile-all',
                               'dataConnectionCount': '2'})
            ]
            conf = Qdrouterd.Config(conf)
            cls.routers_any[version] = cls.tester.qdrouterd(f"ANY-{version}",
                                                            conf, wait=False)
            cls.routers_any[version].wait_ports()

            # allow only 'version'
            conf = [
                ('router', {'id': f'ONLY-{version}', 'mode': 'interior'}),

                # For management access:
                ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                              cls.tester.get_port()}),

                ('sslProfile', {'name': f"ssl-profile-{version}",
                                'caCertFile': cls.ssl_file('ca-certificate.pem'),
                                'certFile': cls.ssl_file('client-certificate.pem'),
                                'privateKeyFile': cls.ssl_file('client-private-key.pem'),
                                'protocols': version,
                                'password': 'client-password'}),

                ('connector', {'host': '0.0.0.0', 'role': 'inter-router',
                               'port': cls.TLS_PORT_VERSION_MAP[version],
                               'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                               'saslUsername': 'test@domain.com', 'saslPassword': 'pass:password',
                               'sslProfile': f'ssl-profile-{version}',
                               'dataConnectionCount': '2'})
            ]
            conf = Qdrouterd.Config(conf)
            cls.routers_only[version] = cls.tester.qdrouterd(f"ONLY-{version}",
                                                             conf, wait=False)
            cls.routers_only[version].wait_ports()

        # finally, create a router that does not use TLS

        conf = [
            ('router', {'id': 'BAD-ROUTER', 'mode': 'interior'}),

            # For management access:
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port':
                          cls.tester.get_port()}),

            ('connector', {'host': '0.0.0.0', 'role': 'inter-router',
                           'port': cls.PORT_TLS_ALL,
                           'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                           'saslPassword': 'env:ENV_SASL_PASSWORD',
                           'saslUsername': 'test@domain.com',
                           'dataConnectionCount': '2'})
        ]

        conf = Qdrouterd.Config(conf)
        cls.bad_router = cls.tester.qdrouterd("BAD-ROUTER", conf, wait=False)
        cls.bad_router.wait_ports()

    def get_router_nodes(self):
        """
        Retrieves connected router nodes.
        :return:
        """
        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        url = Url("amqp://0.0.0.0:%d/$management" % self.PORT_NO_SSL)
        node = Node.connect(url)
        response = node.query(type="io.skupper.router.router.node", attribute_names=["id"])
        router_nodes = []
        for resp in response.get_dicts():
            router_nodes.append(resp['id'])
        node.close()
        return router_nodes

    def test_connected_tls_sasl_routers(self):
        """
        Validates if all expected routers are connected in the network with the
        proper TLS/SASL settings on the inter-router connections
        """
        if self.DISABLE_SSL_TESTING:
            self.skipTest(self.DISABLE_REASON)

        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        def _get_ssl_conns(mgmt):
            # query all inter-router connections, wait until all expected
            # connections have come up
            conns = mgmt.query(type='io.skupper.router.connection',
                               attribute_names=['role',
                                                'ssl',
                                                'sslProto',
                                                'sasl',
                                                'isAuthenticated',
                                                'isEncrypted',
                                                'user']).get_dicts()
            conns = [c for c in conns
                     if 'inter-router' in c['role']]
            return conns if len(conns) == self.inter_router_conn_count else None

        # wait for the routers that should connect successfully, and verify
        # the resulting connection's TLS/SASL config

        self.router_unrestricted.wait_router_connected("QDR.A")
        conns = retry(lambda mgmt=self.router_unrestricted.management: _get_ssl_conns(mgmt))
        self.assertIsNotNone(conns)
        for c in conns:
            self.assertTrue(c['isEncrypted'], f"Not encrypted {c}")
            self.assertTrue(c['isAuthenticated'], f"Not authed {c}")
            self.assertEqual('PLAIN', c['sasl'], f"bad mech {c}")
            self.assertEqual('test@domain.com', c['user'], f"bad user {c}")

        for version, router in self.routers_any.items():
            router.wait_router_connected("QDR.A")
            conns = retry(lambda mgmt=router.management: _get_ssl_conns(mgmt))
            self.assertIsNotNone(conns)
            for c in conns:
                self.assertTrue(c['isEncrypted'], f"Not encrypted {c}")
                self.assertTrue(c['isAuthenticated'], f"Not authed {c}")
                self.assertEqual('PLAIN', c['sasl'], f"bad mech {c}")
                self.assertEqual('test@domain.com', c['user'], f"bad user {c}")
                self.assertEqual(version, c['sslProto'], f"wrong proto {c}")

        for version, router in self.routers_only.items():
            router.wait_router_connected("QDR.A")
            conns = retry(lambda mgmt=router.management: _get_ssl_conns(mgmt))
            self.assertIsNotNone(conns)
            for c in conns:
                self.assertTrue(c['isEncrypted'], f"Not encrypted {c}")
                self.assertTrue(c['isAuthenticated'], f"Not authed {c}")
                self.assertEqual('PLAIN', c['sasl'], f"bad mech {c}")
                self.assertEqual('test@domain.com', c['user'], f"bad user {c}")
                self.assertEqual(version, c['sslProto'], f"wrong proto {c}")

        # wait for the bad router to log that the connection to QDR.A has
        # failed

        self.bad_router.wait_log_message(f"Connection to 0.0.0.0:{self.PORT_TLS_ALL} failed")
        self.router_a.wait_log_message("Connection from .* failed: amqp:connection:policy-error Client connection unencrypted")


class RouterTestSslInterRouterWithInvalidPathToCA(RouterTestSslBase):
    """
    DISPATCH-1762
    Starts 2 routers:
       Router A two listeners serve a normal, good certificate
       Router B two connectors configured with an invalid CA file path in its profile
          - one sets verifyHostname true, the other false.
    Test proves:
       Router B must not connect to A with mis-configured CA file path regardless of
       verifyHostname setting.
    """
    # Listener ports for each TLS protocol definition
    PORT_NO_SSL  = 0
    PORT_TLS_ALL = 0

    @classmethod
    def setUpClass(cls):
        """
        Prepares 2 routers to form a network.
        """
        super(RouterTestSslInterRouterWithInvalidPathToCA, cls).setUpClass()

        if not SASL.extended():
            return

        os.environ["ENV_SASL_PASSWORD"] = "password"

        # Generate authentication DB
        super(RouterTestSslInterRouterWithInvalidPathToCA, cls).create_sasl_files()

        # Router expected to be connected
        cls.connected_tls_sasl_routers = []

        # Generated router list
        cls.routers = []

        # Saving listener ports for each TLS definition
        cls.PORT_NO_SSL = cls.tester.get_port()
        cls.PORT_TLS_ALL_1 = cls.tester.get_port()
        cls.PORT_TLS_ALL_2 = cls.tester.get_port()

        # Configured connector host
        cls.CONNECTOR_HOST = "localhost"

        config_a = Qdrouterd.Config([
            ('router', {'id': 'QDR.A',
                        'mode': 'interior',
                        'saslConfigName': 'tests-mech-PLAIN',
                        'saslConfigDir': os.getcwd()}),
            # No auth and no SSL for management access
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.PORT_NO_SSL}),
            # All TLS versions and normal, good sslProfile config
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': cls.PORT_TLS_ALL_1,
                          'saslMechanisms': 'PLAIN',
                          'requireEncryption': 'yes', 'requireSsl': 'yes',
                          'sslProfile': 'ssl-profile-tls-all'}),
            # All TLS versions and normal, good sslProfile config
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': cls.PORT_TLS_ALL_2,
                          'saslMechanisms': 'PLAIN',
                          'requireEncryption': 'yes', 'requireSsl': 'yes',
                          'sslProfile': 'ssl-profile-tls-all'}),
            # SSL Profile for all TLS versions (protocols element not defined)
            ('sslProfile', {'name': 'ssl-profile-tls-all',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:' \
                                       'DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                            'password': 'server-password'})
        ])

        # Router B has a connector to listener that allows all protocols but will not verify hostname.
        # The sslProfile has a bad caCertFile name and this router should not connect.
        config_b = Qdrouterd.Config([
            ('router', {'id': 'QDR.B',
                        'mode': 'interior'}),
            # Connector to All TLS versions allowed listener
            ('connector', {'name': 'connector1',
                           'host': cls.CONNECTOR_HOST, 'role': 'inter-router',
                           'port': cls.PORT_TLS_ALL_1,
                           'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                           'saslUsername': 'test@domain.com', 'saslPassword': 'pass:password',
                           'sslProfile': 'ssl-profile-tls-all',
                           'dataConnectionCount': '2'}),
            # Connector to All TLS versions allowed listener
            ('connector', {'name': 'connector2',
                           'host': cls.CONNECTOR_HOST, 'role': 'inter-router',
                           'port': cls.PORT_TLS_ALL_2,
                           'verifyHostname': 'yes', 'saslMechanisms': 'PLAIN',
                           'saslUsername': 'test@domain.com', 'saslPassword': 'pass:password',
                           'sslProfile': 'ssl-profile-tls-all',
                           'dataConnectionCount': '2'}),

            # SSL Profile with an invalid caCertFile file path. The correct file path here would allow this
            # router to connect. The object is to trigger a specific failure in the ssl
            # setup chain of calls to pn_ssl_domain_* functions.
            ('sslProfile', {'name': 'ssl-profile-tls-all',
                            'caCertFile': cls.ssl_file('ca-certificate-INVALID-FILENAME.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:' \
                                       'DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS'})
        ])

        cls.routers.append(cls.tester.qdrouterd("A", config_a, wait=False))
        cls.routers.append(cls.tester.qdrouterd("B", config_b, wait=False))

        # Wait until A is running
        cls.routers[0].wait_ports()

        # Can't wait until B is connected because it's not supposed to connect.

    def get_router_nodes(self):
        """
        Retrieves connected router nodes from QDR.A
        :return: list of connected router id's
        """
        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        url = Url("amqp://0.0.0.0:%d/$management" % self.PORT_NO_SSL)
        node = Node.connect(url)
        response = node.query(type="io.skupper.router.router.node", attribute_names=["id"])
        router_nodes = []
        for resp in response.get_dicts():
            router_nodes.append(resp['id'])
        node.close()
        return router_nodes

    def test_invalid_ca_path(self):
        """
        Prove sslProfile with invalid path to CA prevents the router from joining the network
        """
        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        # Poll for a while until the connector error shows up in router B's log
        pattern = " SERVER (error) SSL CA configuration failed"
        host_port_1 = self.CONNECTOR_HOST + ":" + str(self.PORT_TLS_ALL_1)
        host_port_2 = self.CONNECTOR_HOST + ":" + str(self.PORT_TLS_ALL_2)
        sleep_time = 0.1  # seconds
        poll_duration = 60.0  # seconds
        verified = False
        for tries in range(int(poll_duration / sleep_time)):
            logfile = os.path.join(self.routers[1].outdir, self.routers[1].logfile)
            if os.path.exists(logfile):
                with open(logfile, 'r') as router_log:
                    log_lines = router_log.read().split("\n")
                e1_lines = [s for s in log_lines if pattern in s and host_port_1 in s]
                e2_lines = [s for s in log_lines if pattern in s and host_port_2 in s]
                verified = len(e1_lines) > 0 and len(e2_lines) > 0
                if verified:
                    break
            time.sleep(sleep_time)
        self.assertTrue(verified, "Log line containing '%s' not seen for both connectors in QDR.B log" % pattern)

        verified = False
        pattern1 = "Connection to %s failed:" % host_port_1
        pattern2 = "Connection to %s failed:" % host_port_2
        for tries in range(int(poll_duration / sleep_time)):
            logfile = os.path.join(self.routers[1].outdir, self.routers[1].logfile)
            if os.path.exists(logfile):
                with open(logfile, 'r') as router_log:
                    log_lines = router_log.read().split("\n")
                e1_lines = [s for s in log_lines if pattern1 in s]
                e2_lines = [s for s in log_lines if pattern2 in s]
                verified = len(e1_lines) > 0 and len(e2_lines) > 0
                if verified:
                    break
            time.sleep(sleep_time)
        self.assertTrue(verified, "Log line containing '%s' or '%s' not seen in QDR.B log" % (pattern1, pattern2))

        # Show that router A does not have router B in its network
        router_nodes = self.get_router_nodes()
        self.assertTrue(router_nodes)
        node = "QDR.B"
        self.assertNotIn(node, router_nodes, msg=("%s should not be connected" % node))


class RouterTestSslInterRouterWithoutHostnameVerificationAndMismatchedCA(RouterTestSslBase):
    """
    DISPATCH-1762
    Starts 2 routers:
       Router A listener serves a normal, good certificate.
       Router B connector is configured with a CA cert that did not sign the server cert, and verifyHostname is false.
    Test proves:
       Router B must not connect to A.
    """
    # Listener ports for each TLS protocol definition
    PORT_NO_SSL  = 0
    PORT_TLS_ALL = 0

    @classmethod
    def setUpClass(cls):
        """
        Prepares 2 routers to form a network.
        """
        super(RouterTestSslInterRouterWithoutHostnameVerificationAndMismatchedCA, cls).setUpClass()

        if not SASL.extended():
            return

        os.environ["ENV_SASL_PASSWORD"] = "password"

        # Generate authentication DB
        super(RouterTestSslInterRouterWithoutHostnameVerificationAndMismatchedCA, cls).create_sasl_files()

        # Router expected to be connected
        cls.connected_tls_sasl_routers = []

        # Generated router list
        cls.routers = []

        # Saving listener ports for each TLS definition
        cls.PORT_NO_SSL = cls.tester.get_port()
        cls.PORT_TLS_ALL = cls.tester.get_port()

        config_a = Qdrouterd.Config([
            ('router', {'id': 'QDR.A',
                        'mode': 'interior',
                        'saslConfigName': 'tests-mech-PLAIN',
                        'saslConfigDir': os.getcwd()}),
            # No auth and no SSL for management access
            ('listener', {'host': '0.0.0.0', 'role': 'normal', 'port': cls.PORT_NO_SSL}),
            # All TLS versions and normal, good sslProfile config
            ('listener', {'host': '0.0.0.0', 'role': 'inter-router', 'port': cls.PORT_TLS_ALL,
                          'saslMechanisms': 'PLAIN',
                          'requireEncryption': 'yes', 'requireSsl': 'yes',
                          'sslProfile': 'ssl-profile-tls-all'}),
            # SSL Profile for all TLS versions (protocols element not defined)
            ('sslProfile', {'name': 'ssl-profile-tls-all',
                            'caCertFile': cls.ssl_file('ca-certificate.pem'),
                            'certFile': cls.ssl_file('server-certificate.pem'),
                            'privateKeyFile': cls.ssl_file('server-private-key.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:' \
                                       'DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                            'password': 'server-password'})
        ])

        # Router B has a connector to listener that allows all protocols but will not verify hostname.
        # The sslProfile has a caCertFile that does not sign the server cert, so this router should not connect.
        config_b = Qdrouterd.Config([
            ('router', {'id': 'QDR.B',
                        'mode': 'interior'}),
            # Connector to All TLS versions allowed listener
            ('connector', {'host': 'localhost', 'role': 'inter-router', 'port': cls.PORT_TLS_ALL,
                           'verifyHostname': 'no', 'saslMechanisms': 'PLAIN',
                           'saslUsername': 'test@domain.com', 'saslPassword': 'pass:password',
                           'sslProfile': 'ssl-profile-tls-all',
                           'dataConnectionCount': '2'}),
            # SSL Profile with caCertFile to cert that does not sign the server cert. The correct path here would allow this
            # router to connect. The object is to trigger a certificate verification failure while hostname verification is off.
            ('sslProfile', {'name': 'ssl-profile-tls-all',
                            'caCertFile': cls.ssl_file('bad-ca-certificate.pem'),
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:' \
                                       'DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS'})
        ])

        cls.routers.append(cls.tester.qdrouterd("A", config_a, wait=False))
        cls.routers.append(cls.tester.qdrouterd("B", config_b, wait=False))

        # Wait until A is running
        cls.routers[0].wait_ports()

        # Can't wait until B is connected because it's not supposed to connect.

    def get_router_nodes(self):
        """
        Retrieves connected router nodes from QDR.A
        :return: list of connected router id's
        """
        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        url = Url("amqp://0.0.0.0:%d/$management" % self.PORT_NO_SSL)
        node = Node.connect(url)
        response = node.query(type="io.skupper.router.router.node", attribute_names=["id"])
        router_nodes = []
        for resp in response.get_dicts():
            router_nodes.append(resp['id'])
        node.close()
        return router_nodes

    def test_mismatched_ca_and_no_hostname_verification(self):
        """
        Prove that improperly configured ssl-enabled connector prevents the router
        from joining the network
        """
        if not SASL.extended():
            self.skipTest("Cyrus library not available. skipping test")

        # Poll for a while until the connector error shows up in router B's log
        pattern = "Connection to localhost:%s failed:" % self.PORT_TLS_ALL
        sleep_time = 0.1  # seconds
        poll_duration = 60.0  # seconds
        verified = False
        for tries in range(int(poll_duration / sleep_time)):
            logfile = os.path.join(self.routers[1].outdir, self.routers[1].logfile)
            if os.path.exists(logfile):
                with open(logfile, 'r') as router_log:
                    log_lines = router_log.read().split("\n")
                e_lines = [s for s in log_lines if pattern in s]
                verified = len(e_lines) > 0
                if verified:
                    break
            time.sleep(sleep_time)
        self.assertTrue(verified, "Log line containing '%s' not seen in QDR.B log" % pattern)

        # Show that router A does not have router B in its network
        router_nodes = self.get_router_nodes()
        self.assertTrue(router_nodes)
        node = "QDR.B"
        self.assertNotIn(node, router_nodes, msg=("%s should not be connected" % node))


if __name__ == '__main__':
    unittest.main(main_module())

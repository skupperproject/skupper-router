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

import os
from system_test import TestCase, Qdrouterd, DIR, main_module
from system_test import unittest
from system_test import CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, \
    CLIENT_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY_PASSWORD
import proton
from proton import SSLDomain, Delivery
from proton.utils import BlockingConnection
from skupper_router_internal.compat import BINARY


class QdSSLUseridTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super(QdSSLUseridTest, cls).setUpClass()

        ssl_profile1_json = os.path.join(DIR, 'displayname_files', 'profile_names1.json')
        ssl_profile2_json = os.path.join(DIR, 'displayname_files', 'profile_names2.json')
        policy_config_path = os.path.join(DIR, 'policy-4')

        config = Qdrouterd.Config([
            ('router', {'id': 'QDR', 'workerThreads': 1}),

            ('policy', {'maxConnections': 20, 'policyDir': policy_config_path, 'enableVhostPolicy': 'true'}),

            # sha1
            ('sslProfile', {'name': 'server-ssl1',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # sha256
            ('sslProfile', {'name': 'server-ssl2',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '2',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # sha512
            ('sslProfile', {'name': 'server-ssl3',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '5',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # sha256 combination
            ('sslProfile', {'name': 'server-ssl4',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '2noucs',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # sha1 combination
            ('sslProfile', {'name': 'server-ssl5',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1cs',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # sha512 combination
            ('sslProfile', {'name': 'server-ssl6',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': 'cs5',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # no fingerprint field
            ('sslProfile', {'name': 'server-ssl7',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': 'nsuco',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # no fingerprint field variation
            ('sslProfile', {'name': 'server-ssl8',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': 'scounl',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # no uidFormat
            ('sslProfile', {'name': 'server-ssl9',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # one component of uidFormat is invalid (x), the unrecognized component will be ignored,
            # this will be treated like 'uidFormat': '1'
            ('sslProfile', {'name': 'server-ssl10',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1x',
                            'uidNameMappingFile': ssl_profile2_json,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # All components in the uidFormat are unrecognized, pn_get_transport_user will be returned
            ('sslProfile', {'name': 'server-ssl11',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': 'abxd',
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            ('sslProfile', {'name': 'server-ssl12',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1',
                            'uidNameMappingFile': ssl_profile1_json,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            # should translate a display name
            ('sslProfile', {'name': 'server-ssl13',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '2',
                            'uidNameMappingFile': ssl_profile2_json,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            ('sslProfile', {'name': 'server-ssl14',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1',
                            'uidNameMappingFile': ssl_profile1_json,
                            'password': SERVER_PRIVATE_KEY_PASSWORD}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl1', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl2', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl3', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl4', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl5', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl6', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl7', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl8', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl9', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl10', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl11', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            # peer is not being authenticated here. the user must "anonymous" which is what pn_transport_get_user
            # returns
            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl12', 'authenticatePeer': 'no',
                          'requireSsl': 'yes', 'saslMechanisms': 'ANONYMOUS'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl13', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'sslProfile': 'server-ssl14', 'authenticatePeer': 'yes',
                          'requireSsl': 'yes', 'saslMechanisms': 'EXTERNAL'}),

            ('listener', {'port': cls.tester.get_port(), 'authenticatePeer': 'no'})

        ])

        cls.router = cls.tester.qdrouterd('ssl-test-router', config, wait=True)

    def address(self, index):
        return self.router.addresses[index]

    def create_ssl_domain(self, ssl_options_dict, mode=SSLDomain.MODE_CLIENT):
        """Return proton.SSLDomain from command line options or None if no SSL options specified.
            @param opts: Parsed optoins including connection_options()
        """
        certificate, key, trustfile, password = ssl_options_dict.get('ssl-certificate'), \
            ssl_options_dict.get('ssl-key'), \
            ssl_options_dict.get('ssl-trustfile'), \
            ssl_options_dict.get('ssl-password')

        if not (certificate or trustfile):
            return None
        domain = SSLDomain(mode)
        if trustfile:
            domain.set_trusted_ca_db(str(trustfile))
            domain.set_peer_authentication(SSLDomain.VERIFY_PEER, str(trustfile))
        if certificate:
            domain.set_credentials(str(certificate), str(key), str(password))

        return domain


class QdSSLUseridProxy(QdSSLUseridTest):

    def test_message_user_id_proxy_bad_name_disallowed(self):
        ssl_opts = dict()
        ssl_opts['ssl-trustfile'] = CA_CERT
        ssl_opts['ssl-certificate'] = CLIENT_CERTIFICATE
        ssl_opts['ssl-key'] = CLIENT_PRIVATE_KEY
        ssl_opts['ssl-password'] = CLIENT_PRIVATE_KEY_PASSWORD

        # create the SSL domain object
        domain = self.create_ssl_domain(ssl_opts)

        # Send a message with bad user_id. This message should be rejected.
        # Connection has user_id 'user13'.
        addr = self.address(13).replace("amqp", "amqps")
        blocking_connection = BlockingConnection(addr, ssl_domain=domain)
        blocking_sender = blocking_connection.create_sender("$management")

        request = proton.Message()
        request.user_id = BINARY("bad-user-id")

        result = Delivery.ACCEPTED
        try:
            delivery = blocking_sender.send(request, timeout=10)
            result = delivery.remote_state
        except proton.utils.SendException as e:
            result = e.state

        self.assertTrue(result == Delivery.REJECTED,
                        "Router accepted a message with user_id that did not match connection user_id")

    def test_message_user_id_proxy_zzz_credit_handled(self):
        # Test for DISPATCH-519. Make sure the REJECTED messages result
        # in the client receiving credit.
        credit_limit = 250   # router issues 250 credits
        ssl_opts = dict()
        ssl_opts['ssl-trustfile'] = CA_CERT
        ssl_opts['ssl-certificate'] = CLIENT_CERTIFICATE
        ssl_opts['ssl-key'] = CLIENT_PRIVATE_KEY
        ssl_opts['ssl-password'] = CLIENT_PRIVATE_KEY_PASSWORD

        # create the SSL domain object
        domain = self.create_ssl_domain(ssl_opts)

        # Send a message with bad user_id. This message should be rejected.
        # Connection has user_id 'user13'.
        addr = self.address(13).replace("amqp", "amqps")
        blocking_connection = BlockingConnection(addr, ssl_domain=domain)
        blocking_sender = blocking_connection.create_sender("$management")

        request = proton.Message()
        request.user_id = BINARY("bad-user-id")

        for i in range(0, credit_limit + 1):
            result = Delivery.ACCEPTED
            try:
                delivery = blocking_sender.send(request, timeout=10)
                result = delivery.remote_state
            except proton.utils.SendException as e:
                result = e.state
            except proton.utils.Timeout as e:
                self.fail("Timed out waiting for send credit")

            self.assertTrue(result == Delivery.REJECTED,
                            "Router accepted a message with user_id that did not match connection user_id")


if __name__ == '__main__':
    unittest.main(main_module())

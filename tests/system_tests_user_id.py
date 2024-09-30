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
from system_test import TestCase, Qdrouterd, DIR, main_module, TIMEOUT
from system_test import unittest, CONNECTION_TYPE
from system_test import CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, SERVER_PASSWORD_FILE, CLIENT_CERTIFICATE, \
    CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY_PASSWORD
from skupper_router.management.client import Node
from proton import SSLDomain


class QdSSLUseridTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super(QdSSLUseridTest, cls).setUpClass()

        os.environ["TLS_SERVER_PASSWORD"] = SERVER_PRIVATE_KEY_PASSWORD

        ssl_profile1_json = os.path.join(DIR, 'displayname_files', 'profile_names1.json')
        ssl_profile2_json = os.path.join(DIR, 'displayname_files', 'profile_names2.json')

        config = Qdrouterd.Config([
            ('router', {'id': 'QDR', 'workerThreads': 1}),

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
                            # Use the env: prefix TLS_SERVER_PASSWORD. The TLS_SERVER_PASSWORD
                            # is set to SERVER_PRIVATE_KEY_PASSWORD
                            'password': 'env:TLS_SERVER_PASSWORD'}),

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
                            # Use the prefix 'file:' for the password. This should read the file and
                            # use the password from the file.
                            'password': 'file:' + SERVER_PASSWORD_FILE}),

            # one component of uidFormat is invalid (x), this will result in an error in the fingerprint calculation.
            # The user_id will fall back to proton's pn_transport_get_user
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
                            # Use the prefix 'literal:'. This makes sure we maintain
                            # backward compatability
                            'password': 'literal:server-password'}),

            ('sslProfile', {'name': 'server-ssl12',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'uidFormat': '1',
                            'uidNameMappingFile': ssl_profile1_json,
                            # Use the pass: followed by the actual password
                            'password': 'pass:server-password'}),

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
                            'password': 'file:' + SERVER_PASSWORD_FILE}),

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

            # peer is not being authenticated here. the user must be "anonymous" which is what pn_transport_get_user
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
        domain.set_peer_authentication(SSLDomain.VERIFY_PEER, None)
        if certificate:
            domain.set_credentials(str(certificate), str(key), str(password))

        return domain

    def test_ssl_user_id(self):
        ssl_opts = dict()
        ssl_opts['ssl-trustfile'] = CA_CERT
        ssl_opts['ssl-certificate'] = CLIENT_CERTIFICATE
        ssl_opts['ssl-key'] = CLIENT_PRIVATE_KEY
        ssl_opts['ssl-password'] = CLIENT_PRIVATE_KEY_PASSWORD

        # from pn_transport_get_user() if no uidFormat given
        DEFAULT_UID = 'CN=127.0.0.1,O=Client,OU=Dev,L=San Francisco,ST=CA,C=US'

        # create the SSL domain object
        domain = self.create_ssl_domain(ssl_opts)

        # SHA1 fingerprint of client-certificate.pem:
        addr = self.address(0).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        user_id = node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[0][0]
        self.assertEqual("789d81777d7426286656747bb21310d5cb54e91e", user_id)

        # sha256 fingerprint of client-certificate.pem:
        addr = self.address(1).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("6cea7256e7d65c16a5c9d34036ede844e45e77a3271bcc7ec43badeb0550fd46",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[1][0])

        # sha512 fingerprint of client-certificate.pem:
        addr = self.address(2).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        fp = "9d7be15953e46b55b9c132156bc4d1dac9463382600f12002ad62f5b71207c4445cf907421d8ac8aba00a9316496d5b3b902655ba6ec0cdc0410ffa4c8de89fe"
        self.assertEqual(fp, node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[2][0])

        # uidFormat 2noucs
        addr = self.address(3).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("6cea7256e7d65c16a5c9d34036ede844e45e77a3271bcc7ec43badeb0550fd46;127.0.0.1;Client;Dev;US;CA",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[3][0])

        addr = self.address(4).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("789d81777d7426286656747bb21310d5cb54e91e;US;CA",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[4][0])

        addr = self.address(5).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("US;CA;9d7be15953e46b55b9c132156bc4d1dac9463382600f12002ad62f5b71207c4445cf907421d8ac8aba00a9316496d5b3b902655ba6ec0cdc0410ffa4c8de89fe",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[5][0])

        addr = self.address(6).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("127.0.0.1;CA;Dev;US;Client",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[6][0])

        addr = self.address(7).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual("CA;US;Client;Dev;127.0.0.1;San Francisco",
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[7][0])

        addr = self.address(8).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual(DEFAULT_UID,
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[8][0])

        addr = self.address(9).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        self.assertEqual(DEFAULT_UID,
                         node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[9][0])

        addr = self.address(10).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        user = node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[10][0]
        self.assertEqual(DEFAULT_UID, str(user))

        # authenticatePeer is set to 'no' in this listener, the user should anonymous on the connection.
        addr = self.address(11).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        user = node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[11][0]
        self.assertEqual("anonymous", user)

        addr = self.address(12).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        user = node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[12][0]
        self.assertEqual("user12", str(user))

        addr = self.address(13).replace("amqp", "amqps")
        node = Node.connect(addr, timeout=TIMEOUT, ssl_domain=domain)
        user_id = node.query(type=CONNECTION_TYPE, attribute_names=['user']).results[13][0]
        self.assertEqual("user13", user_id)

        node.close()

        # verify that the two invalid uidFormat fields were detected:

        m1 = r"Invalid format for uidFormat field in sslProfile 'server-ssl10': 1x"
        m2 = r"Invalid format for uidFormat field in sslProfile 'server-ssl11': abxd"
        self.router.wait_log_message(m1)
        self.router.wait_log_message(m2)


if __name__ == '__main__':
    unittest.main(main_module())

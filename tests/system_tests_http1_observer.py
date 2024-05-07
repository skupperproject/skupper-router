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

#
# Test the HTTP/1.x Protocol observer
#

import os

from system_test import TestCase, unittest, main_module, Qdrouterd
from system_test import curl_available, run_curl
from system_test import nginx_available, NginxServer
from system_test import Http1Server, retry, TIMEOUT
from vanflow_snooper import VFlowSnooperThread


def spawn_nginx(port, tester):
    """
    Spawn the Nginx server listening on port
    """
    env = dict()
    env['nginx-base-folder'] = NginxServer.BASE_FOLDER
    env['setupclass-folder'] = tester.directory
    env['nginx-configs-folder'] = NginxServer.CONFIGS_FOLDER
    env['listening-port'] = str(port)
    env['http2'] = ''  # disable HTTP/2
    # disable ssl/tls for now
    env['ssl'] = ''
    env['tls-enabled'] = '#'  # Will comment out TLS configuration lines

    # TBD: TLS stuff
    # env['ssl'] = 'ssl'
    # env['tls-enabled'] = ''  # Will enable TLS lines
    # env['chained-pem'] = CHAINED_CERT
    # env['server-private-key-no-pass-pem'] = SERVER_PRIVATE_KEY_NO_PASS
    # env['ssl-verify-client'] = 'on'
    # env['ca-certificate'] = CA_CERT

    return tester.nginxserver(config_path=NginxServer.CONFIG_FILE, env=env)


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class Http1ObserverTest(TestCase):
    """
    Verify the HTTP/1.x observer produces the expected VanFlow records
    """
    @classmethod
    def router(cls, name, listener_port, server_port, extra_config=None):
        """
        Create a router with a tcpConnector and a tcpListener. Set the logging
        config to only enable Van Flow record logs.
        """
        config = [
            ('router', {'mode': 'interior',
                        'id': name}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),

            ('tcpListener', {'host': "0.0.0.0",
                             'port': listener_port,
                             'address': 'Http1ObserverTest'}),
            ('tcpConnector', {'host': "localhost",
                              'port': server_port,
                              'address': 'Http1ObserverTest'}),

            ('log', {'module': 'DEFAULT',
                     'enable': 'warning+',
                     'includeTimestamp': 'false',
                     'includeSource': 'false',
                     'outputFile': os.path.abspath(f"{name}-flow.log")}),
            ('log', {'module': 'HTTP_OBSERVER', 'enable': 'debug+'}),
            ('log', {'module': 'FLOW_LOG', 'enable': 'debug+'})
        ]

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        router = cls.tester.qdrouterd(name, config, wait=False, cl_args=["-T"])
        router.wait_ports()
        router.wait_address('Http1ObserverTest', subscribers=1)
        return router

    @classmethod
    def setUpClass(cls):
        """
        Start the HTTP1 servers
        """
        super(Http1ObserverTest, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.nginx_server = spawn_nginx(cls.nginx_port, cls.tester)
        cls.http1_port = cls.tester.get_port()
        cls.http1_server = cls.tester.cleanup(Http1Server(cls.http1_port))

    def test_01_get(self):
        """
        Simple pipelined GET request.
        """
        l_port = self.tester.get_port()
        router = self.router("test_01", l_port, self.nginx_port)

        snooper_thread = VFlowSnooperThread(router.addresses[0])
        retry(lambda: snooper_thread.sources_ready == 1, delay=0.25)
        self.assertEqual(1, snooper_thread.sources_ready, "timed out waiting for router beacon")

        curl_args = [
            '--http1.1',
            '--output', "/dev/null",
            '-G'
        ]

        pages = ['index.html', 't100K.html', 't10K.html', 't1K.html']
        for page in pages:
            curl_args.append(f"http://localhost:{l_port}/{page}")
        (rc, _, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl failed: {err}")

        # Expect at least 8 records:
        # 1 - Listener
        # 1 - Connector
        # 1 - TCP Flow
        # 1 - TCP Counter-flow
        # 4 - HTTP requests

        retry(lambda: snooper_thread.total_records > 7, delay=0.25)
        self.assertLess(7, snooper_thread.total_records, f"{snooper_thread.total_records}")

        router.teardown()
        snooper_thread.join(timeout=TIMEOUT)
        results = snooper_thread.get_results()

        #
        # Expect 4 'GET' requests with 200 (OK) status
        #

        matches = 0
        self.assertEqual(1, len(results), f"Expected one router entry: {results}")
        records = results.popitem()[1]
        for record in records:
            if 'METHOD' in record:
                self.assertEqual('GET', record['METHOD'])
                self.assertIn('RESULT', record)
                self.assertEqual('200', record['RESULT'])
                self.assertIn('REASON', record)
                self.assertEqual('OK', record['REASON'])
                matches += 1

        self.assertEqual(len(pages), matches, f"unexpected results {results}")

    def test_02_post(self):
        """
        Simple POST request (chunked). Uses the Http server from the Python
        standard library.

        Note: no pipelining POST requests! It is illegal and not supported by
        the server.
        """
        l_port = self.tester.get_port()
        router = self.router("test_02", l_port, self.http1_port)

        snooper_thread = VFlowSnooperThread(router.addresses[0])
        retry(lambda: snooper_thread.sources_ready == 1, delay=0.25)
        self.assertEqual(1, snooper_thread.sources_ready, "timed out waiting for router beacon")

        curl_args = [
            '--http1.1',
            '--output', "/dev/null",
            '-H', "Transfer-Encoding: chunked",
            '--data-ascii', "Start",
            '--data-ascii', "End",
            f"http://localhost:{l_port}/cgi-bin/script.py"
        ]

        (rc, _, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl post failed: {err}")

        # this will pipeline 3 get requests due to the globbing parameter
        # 'ignore':
        curl_args = [
            '--http1.1',
            '--output', "/dev/null",
            '-G',
            f"http://localhost:{l_port}/index.html?ignore=[1-3]"
        ]

        (rc, _, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl get failed: {err}")

        # Expect at least 10 records: listener, connector, two tcp flows, two
        # counter flows, and 4 HTTP requests:
        retry(lambda: snooper_thread.total_records > 9, delay=0.25)
        self.assertLess(9, snooper_thread.total_records, f"{snooper_thread.total_records}")

        router.teardown()
        snooper_thread.join(timeout=TIMEOUT)
        results = snooper_thread.get_results()

        get_count = 0
        post_count = 0
        self.assertEqual(1, len(results), f"Expected one router entry: {results}")
        records = results.popitem()[1]
        for record in records:
            if 'METHOD' in record:
                if record['METHOD'] == 'GET':
                    self.assertIn('RESULT', record)
                    self.assertEqual('200', record['RESULT'])
                    self.assertIn('REASON', record)
                    self.assertEqual('OK', record['REASON'])
                    get_count += 1
                else:
                    self.assertEqual('POST', record['METHOD'])
                    self.assertIn('RESULT', record)
                    self.assertEqual('200', record['RESULT'])
                    self.assertIn('REASON', record)
                    # reason is hardcoded by Python Http server:
                    self.assertEqual('Script output follows', record['REASON'])
                    post_count += 1

        self.assertEqual(3, get_count, f"{results}")
        self.assertEqual(1, post_count, f"{results}")


if __name__ == '__main__':
    unittest.main(main_module())

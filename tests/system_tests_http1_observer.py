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

import json
import os
import subprocess
import sys

from system_test import TestCase, unittest, main_module, Qdrouterd
from system_test import curl_available, run_curl, Process
from system_test import nginx_available, NginxServer, current_dir
from system_test import Http1Server, retry, TIMEOUT
from system_test import CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY
from system_test import SERVER_PRIVATE_KEY_PASSWORD
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE


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


class VFlowSnooperProcess(Process):
    """
    For testing the vanflow_snooper tool. Not intended for general testing -
    use the VFlowSnooperThread instead (see above imports).  The
    VFlowSnooperThread has an API to help synchronize it with the testcase
    which helps avoid racy tests.
    """
    def __init__(self, router_address, name=None, expect=Process.EXIT_OK, **kwargs):
        name = name or "vanflow_snooper_process"
        kwargs.setdefault('stdout', subprocess.PIPE)
        kwargs.setdefault('stderr', subprocess.PIPE)
        if 'idle_timeout' in kwargs:
            kwargs['idle_timeout'] = str(kwargs['idle_timeout'])
        else:
            kwargs.setdefault('idle_timeout', "0")
        kwargs.setdefault('debug', False)

        args = [sys.executable, os.path.join(current_dir, "vanflow_snooper.py"),
                "-a", router_address,
                "--idle-timeout", kwargs['idle_timeout']]
        if kwargs['debug'] is True:
            args.append("-d")

        # remove keywords not used by super class
        kwargs.pop('idle_timeout')
        kwargs.pop('debug')
        super(VFlowSnooperProcess, self).__init__(args, name=name, expect=expect, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.teardown()


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
                              'address': 'Http1ObserverTest'})
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

    @unittest.skipUnless(sys.version_info >= (3, 11), "Requires HTTP/1.1 support")
    def test_01_get(self):
        """
        Simple pipelined GET request.
        """
        l_port = self.tester.get_port()
        router = self.router("test_01", l_port, self.nginx_port)

        snooper_thread = VFlowSnooperThread(router.addresses[0])

        # wait for the TCP Listener/Connector
        expected = {
            "test_01": [('LISTENER', {'VAN_ADDRESS': 'Http1ObserverTest'}),
                        ('CONNECTOR', {'VAN_ADDRESS': 'Http1ObserverTest'})]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        curl_args = [
            '--http1.1',
            '-G'
        ]

        pages = ['index.html', 't100K.html', 't10K.html', 't1K.html']
        for page in pages:
            curl_args.append(f"http://localhost:{l_port}/{page}")
        (rc, out, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl failed: {rc}, {err}, {out}")

        #
        # Expect a Flow/Counter-flow and 4 HTTP Requests
        #
        expected = {
            "test_01": [
                ('FLOW', {"METHOD": "GET",
                          "RESULT": "200",
                          "REASON": "OK",
                          "PROTOCOL": "HTTP/1.1",
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {"METHOD": "GET",
                          "RESULT": "200",
                          "REASON": "OK",
                          "PROTOCOL": "HTTP/1.1",
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {"METHOD": "GET",
                          "RESULT": "200",
                          "REASON": "OK",
                          "PROTOCOL": "HTTP/1.1",
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {'METHOD': "GET",
                          'RESULT': "200",
                          'REASON': "OK",
                          'PROTOCOL': 'HTTP/1.1',
                          'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected), delay=1)
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")
        router.teardown()
        snooper_thread.join(timeout=TIMEOUT)

    @unittest.skipUnless(sys.version_info >= (3, 11), "Requires HTTP/1.1 support")
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

        # wait for the TCP Listener/Connector
        expected = {
            "test_02": [('LISTENER', {'VAN_ADDRESS': 'Http1ObserverTest'}),
                        ('CONNECTOR', {'VAN_ADDRESS': 'Http1ObserverTest'})]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        curl_args = [
            '--http1.1',
            '-H', "Transfer-Encoding: chunked",
            '--data-ascii', "Start",
            '--data-ascii', "End",
            f"http://localhost:{l_port}/cgi-bin/script.py"
        ]

        (rc, out, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl post failed: {rc}, {err}, {out}")

        # this will pipeline 3 get requests due to the globbing parameter
        # 'ignore':
        curl_args = [
            '--http1.1',
            '-G',
            f"http://localhost:{l_port}/index.html?ignore=[1-3]"
        ]

        (rc, out, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl get failed: {rc}, {err}, {out}")

        #
        # Expect 2 TCP flows, 2 Counter-flows, and 4 HTTP requests (3 GET, 1
        # POST):
        #
        expected = {
            "test_02": [
                ('FLOW', {'PROTOCOL': 'HTTP/1.1',
                          'METHOD': 'GET',
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {'PROTOCOL': 'HTTP/1.1',
                          'METHOD': 'GET',
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {'PROTOCOL': 'HTTP/1.1',
                          'METHOD': 'GET',
                          'END_TIME': ANY_VALUE}),
                ('FLOW', {'PROTOCOL': 'HTTP/1.1',
                          'METHOD': 'POST',
                          'REASON': ANY_VALUE,
                          'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected), delay=1)
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        router.teardown()
        snooper_thread.join(timeout=TIMEOUT)

    @unittest.skipUnless(sys.version_info >= (3, 11), "Requires HTTP/1.1 support")
    def test_03_encrypted(self):
        """
        Verify that the observer simply ignores encrypted HTTP data.
        """
        s_port = self.tester.get_port()
        l_port = self.tester.get_port()

        server_ssl_cfg = dict()
        server_ssl_cfg['CA_CERT'] = CA_CERT
        server_ssl_cfg['SERVER_CERTIFICATE'] = SERVER_CERTIFICATE
        server_ssl_cfg['SERVER_PRIVATE_KEY'] = SERVER_PRIVATE_KEY
        server_ssl_cfg['SERVER_PRIVATE_KEY_PASSWORD'] = SERVER_PRIVATE_KEY_PASSWORD

        # Start SSL Server
        server_func = self.tester.openssl_server
        openssl_server_alpn_http11 = server_func(listening_port=s_port,
                                                 ssl_info=server_ssl_cfg,
                                                 name="OpenSSLServerhttp11",
                                                 cl_args=['-alpn', 'http/1.1'])

        # configure the router to use pass-through TCP (i.e. do not use
        # re-encrypt!)
        router = self.router("test_03", l_port, s_port)

        snooper_thread = VFlowSnooperThread(router.addresses[0])

        # wait for the TCP Listener/Connector
        expected = {
            "test_03": [('LISTENER', {'VAN_ADDRESS': 'Http1ObserverTest'}),
                        ('CONNECTOR', {'VAN_ADDRESS': 'Http1ObserverTest'})]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Transfer encrypted data across the router

        client_ssl_cfg = dict()
        client_ssl_cfg['CA_CERT'] = CA_CERT
        out, error = self.opensslclient(port=l_port, ssl_info=client_ssl_cfg,
                                        data=b"test_encrypted_http11",
                                        cl_args=['-alpn', 'http/1.1'])
        self.assertIn(b"Verification: OK", out)
        self.assertIn(b"Verify return code: 0 (ok)", out)

        # Wait until the BIFLOW records show up and are closed

        expected = {
            "test_03": [
                ('BIFLOW', {'SOURCE_HOST': ANY_VALUE,
                            'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Verify there are NO HTTP flows since the data is encrypted, only the
        # two flows checked above

        flow_recs = snooper_thread.get_router_records("test_03", record_type='FLOW')
        self.assertEqual(2, len(flow_recs), f"Too many flows: {flow_recs}")

        router.teardown()
        snooper_thread.join(timeout=TIMEOUT)

    @unittest.skipUnless(sys.version_info >= (3, 11), "Requires HTTP/1.1 support")
    def test_999_vanflow_snooper(self):
        """
        Validate that the vanflow_snooper tool correctly reports records.
        """
        l_port = self.tester.get_port()
        router = self.router("test_999", l_port, self.nginx_port)

        snooper_process = self.tester.cleanup(VFlowSnooperProcess(router.addresses[0],
                                                                  idle_timeout=3))
        out, err = snooper_process.communicate(timeout=TIMEOUT)
        rc = snooper_process.poll()
        self.assertEqual(0, rc, f"snooper failed: {rc}, {out}, {err}")

        # Expect at least 3 records:
        # 1 - Router
        # 1 - Listener
        # 1 - Connector

        results = json.loads(out)
        self.assertEqual(1, len(results), f"Expected one router entry: {results}")
        records = results.popitem()[1]

        matches = 0
        for record in records:
            if 'RECORD_TYPE' in record:
                if record['RECORD_TYPE'] == 'ROUTER':
                    matches += 1
                elif record['RECORD_TYPE'] == 'CONNECTOR':
                    matches += 1
                elif record['RECORD_TYPE'] == 'LISTENER':
                    matches += 1
        self.assertLessEqual(3, matches, f"Unexpected results: {results}")


if __name__ == '__main__':
    unittest.main(main_module())

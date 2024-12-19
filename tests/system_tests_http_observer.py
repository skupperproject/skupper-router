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
import system_test
from system_test import TestCase, unittest, main_module, Qdrouterd, SkManager
from system_test import curl_available, run_curl, Process, get_digest
from system_test import nginx_available, NginxServer, current_dir
from system_test import Http1Server, retry, TIMEOUT
from system_test import CA_CERT, SERVER_CERTIFICATE, SERVER_PRIVATE_KEY, TCP_LISTENER_TYPE
from system_test import SERVER_PRIVATE_KEY_PASSWORD
from vanflow_snooper import VFlowSnooperThread, ANY_VALUE


def spawn_http_nginx(port, tester, http2=''):
    """
    Spawn the Nginx server listening on port
    """
    env = dict()
    env['nginx-base-folder'] = NginxServer.BASE_FOLDER
    env['setupclass-folder'] = tester.directory
    env['nginx-configs-folder'] = NginxServer.CONFIGS_FOLDER
    env['listening-port'] = str(port)
    env['http2'] = http2  # disable HTTP/2
    # disable ssl/tls for now
    env['ssl'] = ''
    env['tls-enabled'] = '#'  # Will comment out TLS configuration lines
    return tester.nginxserver(config_path=NginxServer.CONFIG_FILE, env=env)


def run_local_curl(address, args=None, input=None, timeout=TIMEOUT,
                   http2_prior_knowledge=True,
                   no_alpn=False,
                   assert_status=True):
    """
    Run the curl command using the HTTP/2 protocol.  Returns (return code,
    stdout, stderr)
    """
    local_args = [str(address)]
    if http2_prior_knowledge:
        local_args += ["--http2-prior-knowledge"]
    if no_alpn:
        local_args += ["--no-alpn"]

    if args:
        local_args += args

    status, out, err = system_test.run_curl(local_args, input=input, timeout=timeout)
    if assert_status:
        assert status == 0, f"CURL ERROR {status}: {out} {err}"
    return status, out, err


def get_address(router):
    tcp_address = router.tcp_addresses
    if tcp_address:
        return router.tcp_addresses[0]
    return None


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
class Http1AutoObserverTest(TestCase):
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
                             #'observer' is defaulted to auto which will detect http1
                             'address': 'Http1AutoObserverTest'}),
            ('tcpConnector', {'host': "localhost",
                              'port': server_port,
                              'address': 'Http1AutoObserverTest'})
        ]

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        router = cls.tester.qdrouterd(name, config, wait=False, cl_args=["-T"])
        router.wait_ports()
        router.wait_address('Http1AutoObserverTest', subscribers=1)
        return router

    @classmethod
    def setUpClass(cls):
        """
        Start the HTTP1 servers
        """
        super(Http1AutoObserverTest, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester)
        cls.http1_port = cls.tester.get_port()
        cls.http1_server = cls.tester.cleanup(Http1Server(cls.http1_port))
        cls.address = 'Http1AutoObserverTest'

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
            "test_01": [('LISTENER', {'VAN_ADDRESS': self.address}),
                        ('CONNECTOR', {'VAN_ADDRESS': self.address})]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        curl_args = [
            '--http1.1',
            '-G'
        ]

        # Note: the lengths of these files are hardcoded in the expected map below:
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
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "PROTOCOL": "HTTP/1.1",
                                "OCTETS": 0,
                                "OCTETS_REVERSE": 45,  # index.html length
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "PROTOCOL": "HTTP/1.1",
                                "OCTETS": 0,
                                "OCTETS_REVERSE": 108803,  # t100K.html length
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "OCTETS": 0,
                                "OCTETS_REVERSE": 10972,  # t10K.html length
                                "PROTOCOL": "HTTP/1.1",
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {'METHOD': "GET",
                                'RESULT': "200",
                                'REASON': "OK",
                                'PROTOCOL': 'HTTP/1.1',
                                "OCTETS": 0,
                                "OCTETS_REVERSE": 1188,  # t1K.html length
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
            "test_02": [('LISTENER', {'VAN_ADDRESS': self.address}),
                        ('CONNECTOR', {'VAN_ADDRESS': self.address})]
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
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/1.1',
                                'METHOD': 'GET',
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/1.1',
                                'METHOD': 'GET',
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/1.1',
                                'METHOD': 'GET',
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/1.1',
                                'METHOD': 'POST',
                                'REASON': ANY_VALUE,
                                'END_TIME': ANY_VALUE,
                                # curl sends 'Start&End' as the POST request
                                # message body:
                                'OCTETS': 9,
                                # The python HTTP test server replies with
                                # '<html><h1>Dummy CGI output!</h1></html>'
                                'OCTETS_REVERSE': 39})
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
            "test_03": [('LISTENER', {'VAN_ADDRESS': self.address}),
                        ('CONNECTOR', {'VAN_ADDRESS': self.address})]
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
                ('BIFLOW_TPORT', {'SOURCE_HOST': ANY_VALUE, 'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Verify there are NO HTTP flows since the data is encrypted, only the
        # one flow checked above

        flow_recs = snooper_thread.get_router_records("test_03", record_type='BIFLOW_TPORT')
        self.assertEqual(1, len(flow_recs), f"Too many flows: {flow_recs}")

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


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class Http1ObserverTest(Http1AutoObserverTest):
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
                             'observer': "http1",  # observer set to http1
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
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester)
        cls.http1_port = cls.tester.get_port()
        cls.http1_server = cls.tester.cleanup(Http1Server(cls.http1_port))
        cls.address = 'Http1ObserverTest'


def image_file(name):
    return os.path.join(system_test.DIR, 'images', name)


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class Http2TestAutoRouterNginx(TestCase):

    @classmethod
    def router(cls, name, listener_port, server_port, extra_config=None):
        """
        Create a router with a tcpConnector and a tcpListener.
        """
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(),
                          'role': 'normal',
                          'host': '0.0.0.0'}),
            ('tcpListener', {'port': listener_port,
                             'address': 'examples',
                             # defaulting observer to auto which will auto detect http2
                             'host': '127.0.0.1'}),
            ('tcpConnector', {'port': server_port,
                              'address': 'examples',
                              'host': 'localhost'})
        ])

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_ports()
        cls.router_qdra.wait_address('examples', subscribers=1)

    @classmethod
    def setUpClass(cls):
        super(Http2TestAutoRouterNginx, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester, http2='http2')
        cls.router("test_01", cls.http_listener_port, cls.nginx_port)

    def test_head_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        snooper_thread = VFlowSnooperThread(self.router_qdra.addresses[0], verbose=False)

        # wait for the TCP Listener/Connector records
        expected = {
            "QDR": [
                ('LISTENER', {'VAN_ADDRESS': 'examples'}),
                ('CONNECTOR', {'VAN_ADDRESS': 'examples'})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic with one X-Forwarded-For header:
        _, out, _ = run_local_curl(get_address(self.router_qdra), args=['--head', '--header', 'X-Forwarded-For: 192.168.0.2'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)

        # Expect a TCP flow/counter-flow and one HTTP/2 flow
        expected = {
            "QDR": [
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/2',
                                'METHOD': 'HEAD',
                                'RESULT': '200',
                                'SOURCE_HOST': '192.168.0.2',
                                'STREAM_ID': ANY_VALUE,
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic with comma separated X-Forwarded-For header:
        _, out, _ = run_local_curl(get_address(self.router_qdra), args=['--head', '--header', 'X-Forwarded-For: 192.168.0.1, 203.168.2.2'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)

        # Expect a TCP flow/counter-flow and one HTTP/2 flow
        expected = {
            "QDR": [
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/2',
                                'METHOD': 'HEAD',
                                'RESULT': '200',
                                'SOURCE_HOST': '192.168.0.1',
                                'STREAM_ID': ANY_VALUE,
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic with many comma separated X-Forwarded-For headers:
        _, out, _ = run_local_curl(get_address(self.router_qdra),
                                   args=['--head', '--header',
                                         'X-Forwarded-For: 192.168.1.7, 203.168.2.2',
                                         '--header',
                                         'X-Forwarded-For: 2001:db8:85a3:8d3:1319:8a2e:370:7348, 207.168.2.2',
                                         '--header',
                                         'X-Forwarded-For: 2003:db9:85a3:8d5:1318:8a2e:370:6322, 207.168.2.1'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)

        # Expect a TCP flow/counter-flow and one HTTP/2 flow
        expected = {
            "QDR": [
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/2',
                                'METHOD': 'HEAD',
                                'RESULT': '200',
                                'SOURCE_HOST': '192.168.1.7',
                                'STREAM_ID': ANY_VALUE,
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")


    def test_get_image_jpg(self):
        # Run curl 127.0.0.1:port --output images/test.jpg --http2-prior-knowledge
        snooper_thread = VFlowSnooperThread(self.router_qdra.addresses[0],
                                            verbose=False)
        # wait for the TCP Listener/Connector records
        expected = {
            "QDR": [
                ('LISTENER', {'VAN_ADDRESS': 'examples'}),
                ('CONNECTOR', {'VAN_ADDRESS': 'examples'})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic

        image_file_name = '/test.jpg'
        address = get_address(self.router_qdra) + "/images" + image_file_name
        run_local_curl(address, args=['--output', self.router_qdra.outdir + image_file_name])
        digest_of_server_file = get_digest(image_file(image_file(image_file_name[1:])))
        digest_of_response_file = get_digest(self.router_qdra.outdir + image_file_name)
        self.assertEqual(digest_of_server_file, digest_of_response_file)

        # Expect a TCP flow/counter-flow and one HTTP/2 flow
        expected = {
            "QDR": [
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/2',
                                'METHOD': 'GET',
                                'RESULT': '200',
                                'STREAM_ID': ANY_VALUE,
                                # 9634665 is the size of the image test.jpg.
                                'OCTETS_REVERSE': 9634665,
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class Http2TestOneRouterNginx(Http2TestAutoRouterNginx):

    @classmethod
    def router(cls, name, listener_port, server_port, extra_config=None):
        """
        Create a router with a tcpConnector and a tcpListener.
        """
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(),
                          'role': 'normal',
                          'host': '0.0.0.0'}),
            ('tcpListener', {'port': listener_port,
                             'address': 'examples',
                             'observer': 'http2',
                             # setting observer to http2
                             'host': '127.0.0.1'}),
            ('tcpConnector', {'port': server_port,
                              'address': 'examples',
                              'host': 'localhost'})
        ])

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_ports()
        cls.router_qdra.wait_address('examples', subscribers=1)

    @classmethod
    def setUpClass(cls):
        super(Http2TestOneRouterNginx, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester, http2='http2')
        cls.router("test_02", cls.http_listener_port, cls.nginx_port)


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class Http2TestObserverNoneNginx(Http2TestAutoRouterNginx):

    @classmethod
    def router(cls, name, listener_port, server_port, extra_config=None):
        """
        Create a router with a tcpConnector and a tcpListener.
        """
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(),
                          'role': 'normal',
                          'host': '0.0.0.0'}),
            ('tcpListener', {'port': listener_port,
                             'address': 'examples',
                             'observer': 'none',
                             # setting observer to 'none'. There will be no observer used
                             # on any of the connections created via this listener.
                             'host': '127.0.0.1'}),
            ('tcpConnector', {'port': server_port,
                              'address': 'examples',
                              'host': 'localhost'})
        ])

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_ports()
        cls.router_qdra.wait_address('examples', subscribers=1)

    @classmethod
    def setUpClass(cls):
        super(Http2TestObserverNoneNginx, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester, http2='http2')
        cls.router("test_02", cls.http_listener_port, cls.nginx_port)

    def test_head_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        snooper_thread = VFlowSnooperThread(self.router_qdra.addresses[0], verbose=False)

        # wait for the TCP Listener/Connector records
        expected = {
            "QDR": [
                ('LISTENER', {'VAN_ADDRESS': 'examples'}),
                ('CONNECTOR', {'VAN_ADDRESS': 'examples'})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic:
        _, out, _ = run_local_curl(get_address(self.router_qdra), args=['--head'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)

        # Since we have set the observer field to 'none', we are looking for this log message in the
        # router log which confirms that no protocol observer was setup in the connection.
        self.router_qdra.wait_log_message("no protocol observer setup for this connection", timeout=1.0)

    def test_get_image_jpg(self):
        pass


@unittest.skipUnless(nginx_available() and curl_available(),
                     "Requires both nginx and curl tools")
class HttpUpdateObserver(TestCase):
    @classmethod
    def router(cls, name, listener_port, server_port, extra_config=None):
        """
        Create a router with a tcpConnector and a tcpListener.
        """
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(),
                          'role': 'normal',
                          'host': '0.0.0.0'}),
            ('tcpListener', {'name': 'tcpListener1',
                             'port': listener_port,
                             'address': 'examples',
                             'observer': 'none',
                             'host': '127.0.0.1'}),
            ('tcpConnector', {'port': server_port,
                              'address': 'examples',
                              'host': 'localhost'})
        ])

        if extra_config is not None:
            config.extend(extra_config)
        config = Qdrouterd.Config(config)
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_ports()
        cls.router_qdra.wait_address('examples', subscribers=1)

    @classmethod
    def setUpClass(cls):
        super(HttpUpdateObserver, cls).setUpClass()
        cls.nginx_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.nginx_server = spawn_http_nginx(cls.nginx_port, cls.tester, http2='http2')
        cls.router("test_03", cls.http_listener_port, cls.nginx_port)

    def test_observer_update(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        snooper_thread = VFlowSnooperThread(self.router_qdra.addresses[0], verbose=False)
        # wait for the TCP Listener/Connector records
        expected = {
            "QDR": [
                ('LISTENER', {'VAN_ADDRESS': 'examples'}),
                ('CONNECTOR', {'VAN_ADDRESS': 'examples'})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Pass traffic:
        _, out, _ = run_local_curl(get_address(self.router_qdra), args=['--head'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)
        # Since we have set the observer field to 'none', we are looking for this log message in the
        # router log which confirms that no protocol observer was setup in the connection.
        self.router_qdra.wait_log_message("no protocol observer setup for this connection", timeout=1.0)

        sk_manager = SkManager(address=self.router_qdra.addresses[0])
        sk_manager.update(TCP_LISTENER_TYPE, {"observer": "http2"}, name='tcpListener1')
        # Pass traffic:
        _, out, _ = run_local_curl(get_address(self.router_qdra), args=['--head'])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('content-type: text/html', out)

        # Expect a TCP flow/counter-flow and one HTTP/2 flow
        expected = {
            "QDR": [
                ('BIFLOW_APP', {'PROTOCOL': 'HTTP/2',
                                'METHOD': 'HEAD',
                                'RESULT': '200',
                                'STREAM_ID': ANY_VALUE,
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected))
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        # Terminate the nginx server which was serving http2 requests
        self.nginx_server.terminate()

        sk_manager.update(TCP_LISTENER_TYPE, {"observer": "http1"}, name='tcpListener1')

        # Start an nginx server that responds to http1 requests
        self.nginx_server = spawn_http_nginx(self.nginx_port, self.tester, http2='')
        curl_args = [
            '--http1.1',
            '-G'
        ]
        pages = ['index.html', 't100K.html', 't10K.html', 't1K.html']
        for page in pages:
            curl_args.append(f"{get_address(self.router_qdra)}/{page}")
        (rc, out, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl failed: {rc}, {err}, {out}")

        #
        # Expect a Flow/Counter-flow and 4 HTTP Requests
        #
        expected = {
            "QDR": [
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "PROTOCOL": "HTTP/1.1",
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "PROTOCOL": "HTTP/1.1",
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {"METHOD": "GET",
                                "RESULT": "200",
                                "REASON": "OK",
                                "PROTOCOL": "HTTP/1.1",
                                'END_TIME': ANY_VALUE}),
                ('BIFLOW_APP', {'METHOD': "GET",
                                'RESULT': "200",
                                'REASON': "OK",
                                'PROTOCOL': 'HTTP/1.1',
                                'END_TIME': ANY_VALUE})
            ]
        }
        success = retry(lambda: snooper_thread.match_records(expected), delay=1)
        self.assertTrue(success, f"Failed to match records {snooper_thread.get_results()}")

        sk_manager.update(TCP_LISTENER_TYPE, {"observer": "none"}, name='tcpListener1')

        curl_args = [
            '--http1.1',
            '-G'
        ]
        pages = ['index.html', 't100K.html', 't10K.html', 't1K.html']
        for page in pages:
            curl_args.append(f"{get_address(self.router_qdra)}/{page}")
        (rc, out, err) = run_curl(args=curl_args)
        self.assertEqual(0, rc, f"curl failed: {rc}, {err}, {out}")


if __name__ == '__main__':
    unittest.main(main_module())

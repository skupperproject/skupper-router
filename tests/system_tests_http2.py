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

import os
import sys
import hashlib
import unittest
from subprocess import PIPE
from time import sleep

import system_test
from http1_tests import wait_http_listeners_up, HttpAdaptorListenerConnectTestBase, wait_tcp_listeners_up
from system_test import TestCase, Qdrouterd, QdManager, Process, retry_assertion
from system_test import curl_available, TIMEOUT, skip_test_in_ci, Http2Server

h2hyper_installed = True
try:
    import h2.connection # noqa F401: imported but unused  # pylint: disable=unused-import
except ImportError:
    h2hyper_installed = False


def python_37_available():
    if sys.version_info >= (3, 7):
        return True


def quart_available():
    """
    Checks if quart version is greater than 0.13
    """
    popen_args = ['quart', '--version']
    try:
        process = Process(popen_args,
                          name='quart_check',
                          stdout=PIPE,
                          expect=Process.EXIT_OK,
                          universal_newlines=True)
        out = process.communicate()[0]
        parts = out.split(".")
        major_version = parts[0]
        if int(major_version[-1]) > 0 or int(parts[1]) >= 13:
            return True
        return False
    except Exception as e:
        print(e)
        print("quart_not_available")
        return False


def skip_test():
    if python_37_available() and quart_available() and curl_available():
        return False
    return True


def skip_h2_test():
    if python_37_available() and h2hyper_installed and curl_available():
        return False
    return True


def get_digest(file_path):
    h = hashlib.sha256()

    with open(file_path, 'rb') as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def image_file(name):
    return os.path.join(system_test.DIR, 'images', name)


class Http2TestBase(TestCase):
    @classmethod
    def setUpClass(cls, tls_v12=False):
        super(Http2TestBase, cls).setUpClass()
        cls.curl_args = None
        cls.tls_v12 = tls_v12

    def get_all_curl_args(self, args=None):
        if self.curl_args:
            if args:
                return self.curl_args + args
            return self.curl_args
        return args

    def run_curl(self, address, args=None, input=None, timeout=TIMEOUT,
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


class CommonHttp2Tests:
    """
    Common Base class containing all tests. These tests are run by all
    topologies of routers.
    """
    def get_address(self):
        http_address = self.router_qdra.http_addresses
        address = None
        if http_address:
            address = self.router_qdra.http_addresses[0]
        else:
            tcp_address = self.router_qdra.tcp_addresses
            if tcp_address:
                address = self.router_qdra.tcp_addresses[0]
        return address

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    # Tests the HTTP2 head request
    def test_head_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head
        _, out, _ = self.run_curl(self.get_address(), args=self.get_all_curl_args(['--head']))
        self.assertIn('HTTP/2 200', out)
        self.assertIn('server: hypercorn-h2', out)
        self.assertIn('content-type: text/html; charset=utf-8', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_get_request(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge
        _, out, _ = self.run_curl(self.get_address(), args=self.get_all_curl_args())
        i = 0
        ret_string = ""
        while i < 1000:
            ret_string += str(i) + ","
            i += 1
        self.assertIn(ret_string, out)

    # @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    # def test_large_get_request(self):
        # Tests a large get request. Response is more than 50k which means it
        # will span many qd_http2_buffer_t objects.
        # Run curl 127.0.0.1:port/largeget --http2-prior-knowledge
    #    address = self.router_qdra.http_addresses[0] + "/largeget"
    #    _, out, _ = self.run_curl(address)
    #    self.assertIn("49996,49997,49998,49999", out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_post_request(self):
        # curl -d "fname=John&lname=Doe" -X POST 127.0.0.1:9000/myinfo --http2-prior-knowledge
        address = self.get_address() + "/myinfo"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-d', 'fname=John&lname=Doe', '-X', 'POST']))
        self.assertIn('Success! Your first name is John, last name is Doe', out)

    skip_reason = 'Test skipped on certain Travis environments'

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    @unittest.skipIf(skip_test_in_ci('QPID_SYSTEM_TEST_SKIP_HTTP2_LARGE_IMAGE_UPLOAD_TEST'), skip_reason)
    def test_post_upload_large_image_jpg(self):
        # curl  -X POST -H "Content-Type: multipart/form-data"  -F "data=@/home/gmurthy/opensource/test.jpg"
        # http://127.0.0.1:9000/upload --http2-prior-knowledge
        address = self.get_address() + "/upload"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-X', 'POST', '-H',
                                                                        'Content-Type: multipart/form-data',
                                                                        '-F', 'data=@' + image_file('test.jpg')]))
        self.assertIn('Success', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_delete_request(self):
        #curl -X DELETE "http://127.0.0.1:9000/myinfo/delete/22122" -H
        # "accept: application/json" --http2-prior-knowledge
        address = self.get_address() + "/myinfo/delete/22122"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-X', 'DELETE']))
        self.assertIn('{"fname": "John", "lname": "Doe", "id": "22122"}', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_put_request(self):
        # curl -d "fname=John&lname=Doe" -X PUT 127.0.0.1:9000/myinfo --http2-prior-knowledge
        address = self.get_address() + "/myinfo"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-d', 'fname=John&lname=Doe', '-X', 'PUT']))
        self.assertIn('Success! Your first name is John, last name is Doe', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_patch_request(self):
        # curl -d "fname=John&lname=Doe" -X PATCH 127.0.0.1:9000/myinfo --http2-prior-knowledge
        address = self.get_address() + "/patch"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['--data',
                                                                        '{\"op\":\"add\",\"path\":\"/user\",\"value\":\"jane\"}',
                                                                        '-X', 'PATCH']))
        self.assertIn('"op":"add"', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_404(self):
        # Run curl 127.0.0.1:port/unavailable --http2-prior-knowledge
        address = self.get_address() + "/unavailable"
        _, out, _ = self.run_curl(address=address, args=self.get_all_curl_args())
        self.assertIn('404 Not Found', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_500(self):
        # Run curl 127.0.0.1:port/test/500 --http2-prior-knowledge
        address = self.get_address() + "/test/500"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args())
        self.assertIn('500 Internal Server Error', out)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_get_image_png(self):
        # Run curl 127.0.0.1:port --output images/balanced-routing.png --http2-prior-knowledge
        image_file_name = '/balanced-routing.png'
        address = self.get_address() + "/images" + image_file_name
        self.run_curl(address, args=self.get_all_curl_args(['--output', self.router_qdra.outdir + image_file_name]))
        digest_of_server_file = get_digest(image_file(image_file_name[1:]))
        digest_of_response_file = get_digest(self.router_qdra.outdir + image_file_name)
        self.assertEqual(digest_of_server_file, digest_of_response_file)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_get_image_jpg(self):
        # Run curl 127.0.0.1:port --output images/apache.jpg --http2-prior-knowledge
        image_file_name = '/apache.jpg'
        address = self.get_address() + "/images" + image_file_name
        self.run_curl(address, args=self.get_all_curl_args(['--output', self.router_qdra.outdir + image_file_name]))
        digest_of_server_file = get_digest(image_file(image_file(image_file_name[1:])))
        digest_of_response_file = get_digest(self.router_qdra.outdir + image_file_name)
        self.assertEqual(digest_of_server_file, digest_of_response_file)

    def check_listener_delete(self, client_addr, server_addr, tcp_listener=False):
        # Run curl 127.0.0.1:port --http2-prior-knowledge
        # We are first making sure that the http request goes thru successfully.
        _, out, _ = self.run_curl(client_addr, args=self.get_all_curl_args())
        ret_string = ""
        i = 0
        while i < 1000:
            ret_string += str(i) + ","
            i += 1
        self.assertIn(ret_string, out)

        qd_manager = QdManager(address=server_addr)
        if tcp_listener:
            entity_name = 'io.skupper.router.tcpListener'
        else:
            entity_name = 'io.skupper.router.httpListener'
        listeners = qd_manager.query(entity_name)
        self.assertEqual(len(listeners), 1)

        # Run a skmanage DELETE on the httpListener
        qd_manager.delete(entity_name, name=self.listener_name)

        # Make sure the listener is gone
        listeners  = qd_manager.query(entity_name)
        self.assertEqual(len(listeners), 0)

        # Try running a curl command against the listener to make sure it times out
        request_timed_out = False
        try:
            self.run_curl(client_addr, args=self.get_all_curl_args(), timeout=3)
        except Exception as e:
            request_timed_out = True
        self.assertTrue(request_timed_out)

        # Add back the listener and run a curl command to make sure that the newly added listener is
        # back up and running.
        create_result = qd_manager.create(entity_name,
                                          self.tcp_listener_props if tcp_listener else self.http_listener_props)
        if tcp_listener:
            wait_tcp_listeners_up(server_addr)
        else:
            wait_http_listeners_up(server_addr)
        _, out, _ = self.run_curl(client_addr, args=self.get_all_curl_args())
        self.assertIn(ret_string, out)

    def check_connector_delete(self, client_addr, server_addr, server_port,
                               listener_addr=None, tcp_connector=False):
        # listener_addr: management address of router with httpListener
        # configured. Must be supplied if httpListener is on a different router
        # than the httpConnector
        listener_addr = listener_addr or server_addr

        # Run curl 127.0.0.1:port --http2-prior-knowledge
        # We are first making sure that the http request goes thru successfully.
        _, out, _ = self.run_curl(client_addr, args=self.get_all_curl_args())

        # Run a skmanage query on connections to see how many qdr_connections are
        # there on the egress router
        qd_manager = QdManager(address=server_addr)
        connections = qd_manager.query('io.skupper.router.connection')
        self.assertGreaterEqual(len(connections), 2)

        if not tcp_connector:
            server_conn_found = False
            for conn in connections:
                print(conn['name'])
                print("server_port", server_port)
                if str(server_port) in conn['name']:
                    server_conn_found = True
                    break
            self.assertTrue(server_conn_found)

        # Run a skmanage DELETE on the httpConnector
        if tcp_connector:
            connector_type = 'io.skupper.router.tcpConnector'
        else:
            connector_type = 'io.skupper.router.httpConnector'
        connectors  = qd_manager.query(connector_type)
        self.assertEqual(len(connectors), 1)

        # Delete the httpConnector
        qd_manager.delete(connector_type, name=self.connector_name)

        # Make sure the connector is gone
        connectors  = qd_manager.query(connector_type)
        self.assertEqual(len(connectors), 0)

        # Deleting the connector must have taken out the connection to the server.
        connections = qd_manager.query('io.skupper.router.connection')
        server_conn_found = False
        for conn in connections:
            if str(server_port) in conn['name']:
                server_conn_found = True
                break
        self.assertFalse(server_conn_found)

        sleep(2)

        # Now, run a curl client GET request with a timeout
        request_timed_out = False
        try:
            _, out, _ = self.run_curl(client_addr, args=self.get_all_curl_args(), timeout=5)
            print(out)
        except Exception as e:
            request_timed_out = True

        self.assertTrue(request_timed_out)

        # Add back the httpConnector
        # skmanage CREATE type=httpConnector address=examples.com host=127.0.0.1 port=80 protocolVersion=HTTP2
        qd_manager.create(connector_type, self.connector_props)
        num_tries = 2
        tries = 0
        conn_present = False
        while tries < num_tries:
            connections = qd_manager.query('io.skupper.router.connection')
            tries += 1
            if len(connections) < 2:
                sleep(2)
            else:
                conn_present = True
        self.assertTrue(conn_present)

        if tcp_connector:
            # wait for the tcpListener to become available again
            wait_tcp_listeners_up(listener_addr)
        else:
            # wait for the httpListener to become available again
            wait_http_listeners_up(listener_addr)

        _, out, _ = self.run_curl(client_addr, args=self.get_all_curl_args())
        ret_string = ""
        i = 0
        while i < 1000:
            ret_string += str(i) + ","
            i += 1
        self.assertIn(ret_string, out)


class Http2TestOneStandaloneRouter(Http2TestBase, CommonHttp2Tests):
    @classmethod
    def setUpClass(cls):
        super(Http2TestOneStandaloneRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': 'http2server:app',
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-test-standalone-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('httpConnector', cls.connector_props)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.http_addresses[0],
                                    server_addr=self.router_qdra.addresses[0],
                                    server_port=self.server_port)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_000_stats(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge
        address = self.router_qdra.http_addresses[0]
        qd_manager = QdManager(address=self.router_qdra.addresses[0])

        # First request
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args())

        i = 0
        ret_string = ""
        while i < 1000:
            ret_string += str(i) + ","
            i += 1
        self.assertIn(ret_string, out)

        # Second request
        address = self.router_qdra.http_addresses[0] + "/myinfo"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-d', 'fname=Mickey&lname=Mouse', '-X', 'POST']))
        self.assertIn('Success! Your first name is Mickey, last name is Mouse', out)

        stats = qd_manager.query('io.skupper.router.httpRequestInfo')
        self.assertEqual(len(stats), 2)

        def check_num_requests():
            statistics = qd_manager.query('io.skupper.router.httpRequestInfo')
            for stat in statistics:
                self.assertEqual(stat.get('requests'), 2)

        # This test intermittently fail s with the following error -
        # self.assertEqual(s.get('requests'), 2) - AssertionError: 1 != 2
        # The logs do not show any request failures.
        # Since this test is failing intermittently and not always, I am going to give more
        # time for this test to execute. If this test still fails, I will look at the http2 code
        # in more detail.
        retry_assertion(check_num_requests)

        stats = qd_manager.query('io.skupper.router.httpRequestInfo')

        for s in stats:
            self.assertEqual(s.get('requests'), 2)
            self.assertEqual(s.get('details').get('GET:200'), 1)
            self.assertEqual(s.get('details').get('POST:200'), 1)
        if stats[0].get('direction') == 'out':
            self.assertEqual(stats[1].get('direction'), 'in')
            self.assertEqual(stats[0].get('bytesOut'), 24)
            self.assertEqual(stats[0].get('bytesIn'), 3944)
            self.assertEqual(stats[1].get('bytesOut'), 3944)
            self.assertEqual(stats[1].get('bytesIn'), 24)
        else:
            self.assertEqual(stats[0].get('direction'), 'in')
            self.assertEqual(stats[1].get('direction'), 'out')
            self.assertEqual(stats[0].get('bytesOut'), 3944)
            self.assertEqual(stats[0].get('bytesIn'), 24)
            self.assertEqual(stats[1].get('bytesOut'), 24)
            self.assertEqual(stats[1].get('bytesIn'), 3944)


class Http2TestOneEdgeRouter(Http2TestBase, CommonHttp2Tests):
    @classmethod
    def setUpClass(cls):
        super(Http2TestOneEdgeRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-test-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('httpConnector', cls.connector_props)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.http_addresses[0],
                                    server_addr=self.router_qdra.addresses[0],
                                    server_port=self.server_port)


class Http2TestOneInteriorRouter(Http2TestBase, CommonHttp2Tests):
    @classmethod
    def setUpClass(cls):
        super(Http2TestOneInteriorRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-test-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('httpConnector', cls.connector_props)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.http_addresses[0],
                                    server_addr=self.router_qdra.addresses[0],
                                    server_port=self.server_port)


class Http2TestTwoRouter(Http2TestBase, CommonHttp2Tests):
    @classmethod
    def setUpClass(cls):
        super(Http2TestTwoRouter, cls).setUpClass()
        if skip_test():
            return
        cls.http2_server_name = "http2_server"
        cls.server_port = cls.tester.get_port()
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name_a = "http2-test-router-a"
        inter_router_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()
        cls.listener_name = 'listenerToBeDeleted'
        cls.http_listener_props = {
            'port': cls.http_listener_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.listener_name
        }
        name_b = "http2-test-router-b"
        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpListener', cls.http_listener_props),
            ('listener', {'role': 'inter-router', 'port': inter_router_port})
        ])

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector', cls.connector_props),
            ('connector', {'name': 'connectorToA', 'role': 'inter-router',
                           'port': inter_router_port})

        ])

        cls.router_qdra = cls.tester.qdrouterd(name_a, config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd(name_b, config_qdrb, wait=True)

        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_000_stats(self):
        # Run curl 127.0.0.1:port --http2-prior-knowledge
        address = self.router_qdra.http_addresses[0]
        qd_manager_a = QdManager(address=self.router_qdra.addresses[0])
        qd_manager_b = QdManager(address=self.router_qdrb.addresses[0])
        stats_a = qd_manager_a.query('io.skupper.router.httpRequestInfo')

        # First request
        self.run_curl(address, args=self.get_all_curl_args())
        address = self.router_qdra.http_addresses[0] + "/myinfo"

        # Second request
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-d', 'fname=Mickey&lname=Mouse', '-X', 'POST']))
        self.assertIn('Success! Your first name is Mickey, last name is Mouse', out)

        def check_num_requests():
            stats = qd_manager_a.query('io.skupper.router.httpRequestInfo')
            self.assertEqual(stats[0].get('requests'), 2)
            stats = qd_manager_b.query('io.skupper.router.httpRequestInfo')
            self.assertEqual(stats[0].get('requests'), 2)

        # This test intermittently fails with the following error -
        # self.assertEqual(s.get('requests'), 2) - AssertionError: 1 != 2
        # The logs do not show any request failures.
        # Since this test is failing intermittently and not always, I am going to give more
        # time for this test to execute. If this test still fails, I will look at the http2 code
        # in more detail.
        retry_assertion(check_num_requests)

        stats_a = qd_manager_a.query('io.skupper.router.httpRequestInfo')

        self.assertEqual(len(stats_a), 1)
        self.assertEqual(stats_a[0].get('requests'), 2)
        self.assertEqual(stats_a[0].get('direction'), 'in')
        self.assertEqual(stats_a[0].get('bytesOut'), 3944)
        self.assertEqual(stats_a[0].get('bytesIn'), 24)

        stats_b = qd_manager_b.query('io.skupper.router.httpRequestInfo')
        self.assertEqual(len(stats_b), 1)

        self.assertEqual(stats_b[0].get('requests'), 2)
        self.assertEqual(stats_b[0].get('direction'), 'out')
        self.assertEqual(stats_b[0].get('bytesOut'), 24)
        self.assertEqual(stats_b[0].get('bytesIn'), 3944)

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_yyy_http_listener_delete(self):
        self.check_listener_delete(client_addr=self.router_qdra.http_addresses[0],
                                   server_addr=self.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.http_addresses[0],
                                    server_addr=self.router_qdrb.addresses[0],
                                    server_port=self.server_port,
                                    listener_addr=self.router_qdra.addresses[0])


class Http2TestEdgeInteriorRouter(Http2TestBase, CommonHttp2Tests):
    """
    The interior router connects to the HTTP2 server and the curl client
    connects to the edge router.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestEdgeInteriorRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        inter_router_port = cls.tester.get_port()
        config_edgea = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('connector', {'name': 'connectorToA', 'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('listener', {'role': 'edge', 'port': inter_router_port}),
            ('httpConnector',
             {'port': cls.server_port, 'address': 'examples',
              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'})
        ])

        cls.router_qdrb = cls.tester.qdrouterd("interior-router", config_qdrb, wait=True)
        cls.router_qdra = cls.tester.qdrouterd("edge-router", config_edgea)
        cls.router_qdrb.is_edge_routers_connected(num_edges=1)
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])


class Http2TestInteriorEdgeRouter(Http2TestBase, CommonHttp2Tests):
    """
    The edge router connects to the HTTP2 server and the curl client
    connects to the interior router.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestInteriorEdgeRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        inter_router_port = cls.tester.get_port()
        config_edge = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector',
             {'port': cls.server_port, 'address': 'examples',
              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('connector', {'name': 'connectorToA', 'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('listener', {'role': 'edge', 'port': inter_router_port}),
            ('httpListener',
             {'port': cls.tester.get_port(), 'address': 'examples',
              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
        ])

        cls.router_qdra = cls.tester.qdrouterd("interior-router", config_qdra, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("edge-router", config_edge)
        cls.router_qdra.is_edge_routers_connected(num_edges=1)
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])


class Http2TestDoubleEdgeInteriorRouter(Http2TestBase):
    """
    There are two edge routers connecting to the same interior router. The two edge routers
    connect to a HTTP2 server. The curl client connects to the interior router and makes
    requests. Since the edge routers are each connected to the http2 server, one of the
    edge router will receive the curl request. We then take down the connector of one of the
    edge routers and make sure that a request is still routed via the the other edge router.
    We will then take down the connector on the other edge router and make sure that
    a curl request times out since the it has nowhere to go.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestDoubleEdgeInteriorRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        inter_router_port = cls.tester.get_port()
        cls.edge_a_connector_name = 'connectorFromEdgeAToIntA'
        cls.edge_a_http_connector_name = 'httpConnectorFromEdgeAToHttpServer'
        config_edgea = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector',
             {'port': cls.server_port,
              'address': 'examples',
              'name': cls.edge_a_http_connector_name,
              'host': '127.0.0.1',
              'protocolVersion': 'HTTP2'}),
            ('connector', {'name': cls.edge_a_connector_name,
                           'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        cls.edge_b_connector_name = 'connectorFromEdgeBToIntA'
        cls.edge_b_http_connector_name = 'httpConnectorFromEdgeBToHttpServer'
        config_edgeb = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector',
             {'port': cls.server_port,
              'address': 'examples',
              'name': cls.edge_b_http_connector_name,
              'host': '127.0.0.1',
              'protocolVersion': 'HTTP2'}),
            ('connector', {'name': cls.edge_b_connector_name,
                           'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        config_qdrc = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('listener', {'role': 'edge', 'port': inter_router_port}),
            ('httpListener', {'port': cls.tester.get_port(),
                              'address': 'examples',
                              'host': '127.0.0.1',
                              'protocolVersion': 'HTTP2'}),
        ])

        cls.router_qdrc = cls.tester.qdrouterd("interior-router", config_qdrc, wait=True)
        cls.router_qdra = cls.tester.qdrouterd("edge-router-a", config_edgea, wait=True)
        cls.router_qdrb = cls.tester.qdrouterd("edge-router-b", config_edgeb, wait=True)
        cls.router_qdrc.is_edge_routers_connected(num_edges=2)
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdrc.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_check_connector_delete(self):
        # We are first making sure that the http request goes thru successfully.
        # We are making the http request on the interior router.
        # The interior router will route this request to the http server via one of the edge routers.
        # Run curl 127.0.0.1:port --http2-prior-knowledge --head

        # There should be one proxy link for each edge router.
        self.router_qdrc.wait_address("examples", subscribers=2)

        address = self.router_qdrc.http_addresses[0]
        _, out, _ = self.run_curl(address, args=["--head"])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('server: hypercorn-h2', out)
        self.assertIn('content-type: text/html; charset=utf-8', out)

        # Now delete the httpConnector on the edge router config_edgea
        qd_manager = QdManager(address=self.router_qdra.addresses[0])
        qd_manager.delete("io.skupper.router.httpConnector", name=self.edge_a_http_connector_name)
        sleep(2)

        # now check the interior router for the examples address. Since the httpConnector on one of the
        # edge routers was deleted, the proxy link on that edge router must be gone leaving us with just one proxy
        # link on the other edge router.
        self.router_qdrc.wait_address("examples", subscribers=1)

        # Run the curl command again to make sure that the request completes again. The request is now routed thru
        # edge router B since the connector on  edge router A is gone
        address = self.router_qdrc.http_addresses[0]
        _, out, _ = self.run_curl(address, args=["--head"])
        self.assertIn('HTTP/2 200', out)
        self.assertIn('server: hypercorn-h2', out)
        self.assertIn('content-type: text/html; charset=utf-8', out)

        # Now delete the httpConnector on the edge router config_edgeb
        qd_manager = QdManager(address=self.router_qdrb.addresses[0])
        qd_manager.delete("io.skupper.router.httpConnector", name=self.edge_b_http_connector_name)
        sleep(2)

        # Now, run a curl client GET request with a timeout.
        # Since both connectors on both edge routers are gone, the curl client will time out
        # The curl client times out instead of getting a 503 because the credit is not given on the interior
        # router to create an AMQP message because there is no destination for the router address.
        request_timed_out = False
        try:
            _, out, _ = self.run_curl(address, args=["--head"], timeout=3)
            print(out)
        except Exception as e:
            request_timed_out = True
        self.assertTrue(request_timed_out)


class Http2TestEdgeToEdgeViaInteriorRouter(Http2TestBase, CommonHttp2Tests):
    """
    The edge router connects to the HTTP2 server and the curl client
    connects to another edge router. The two edge routers are connected
    via an interior router.
    """
    @classmethod
    def setUpClass(cls):
        super(Http2TestEdgeToEdgeViaInteriorRouter, cls).setUpClass()
        if skip_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_server.py",
                                                  env_config={
                                                      'QUART_APP': "http2server:app",
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })

        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        inter_router_port = cls.tester.get_port()
        config_edge_b = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector', cls.connector_props),
            ('connector', {'name': 'connectorToA', 'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('listener', {'role': 'edge', 'port': inter_router_port}),
        ])

        config_edge_a = Qdrouterd.Config([
            ('router', {'mode': 'edge', 'id': 'EDGE.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal',
                          'host': '0.0.0.0'}),
            ('httpListener',
             {'port': cls.tester.get_port(), 'address': 'examples',
              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('connector', {'name': 'connectorToA', 'role': 'edge',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        cls.interior_qdr = cls.tester.qdrouterd("interior-router", config_qdra,
                                                wait=True)
        cls.router_qdra = cls.tester.qdrouterd("edge-router-a", config_edge_a)
        cls.router_qdrb = cls.tester.qdrouterd("edge-router-b", config_edge_b)
        cls.interior_qdr.is_edge_routers_connected(num_edges=2)
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_zzz_http_connector_delete(self):
        self.check_connector_delete(client_addr=self.router_qdra.http_addresses[0],
                                    server_addr=self.router_qdrb.addresses[0],
                                    server_port=self.server_port)


class Http2TestGoAway(Http2TestBase):
    @classmethod
    def setUpClass(cls):
        super(Http2TestGoAway, cls).setUpClass()
        if skip_h2_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "hyperh2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="hyperh2_server.py",
                                                  env_config={
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })

        name = "http2-test-router"
        cls.connector_name = 'connectorToBeDeleted'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('httpConnector', cls.connector_props)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_h2_test(),
                     "Python 3.7 or greater, hyper-h2 and curl needed to run hyperhttp2 tests")
    def test_goaway(self):
        # Executes a request against the router at the /goaway_test_1 URL
        # The router in turn forwards the request to the http2 server which
        # responds with a GOAWAY frame. The router propagates this
        # GOAWAY frame to the client and issues a HTTP 503 to the client
        address = self.router_qdra.http_addresses[0] + "/goaway_test_1"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(["-i"]))
        self.assertIn("HTTP/2 503", out)


class Http2Q2OneRouterTest(Http2TestBase):
    @classmethod
    def setUpClass(cls):
        super(Http2Q2OneRouterTest, cls).setUpClass()
        if skip_h2_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_slow_q2_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_slow_q2_server.py",
                                                  env_config={
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        name = "http2-test-router"
        cls.connector_name = 'connectorToServer'
        cls.connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'QDR'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),

            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('httpConnector', cls.connector_props)
        ])
        cls.router_qdra = cls.tester.qdrouterd(name, config, wait=True)
        cls.router_qdra.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_h2_test(),
                     "Python 3.7 or greater, hyper-h2 and curl needed to run hyperhttp2 tests")
    def test_q2_block_unblock(self):
        # curl  -X POST -H "Content-Type: multipart/form-data"  -F "data=@/home/gmurthy/opensource/test.jpg"
        # http://127.0.0.1:9000/upload --http2-prior-knowledge
        address = self.router_qdra.http_addresses[0] + "/upload"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-X', 'POST',
                                                                        '-H', 'Content-Type: multipart/form-data',
                                                                        '-F', 'data=@' + image_file('test.jpg')]))
        self.assertIn('Success', out)
        num_blocked = 0
        num_unblocked = 0
        blocked = "q2 is blocked"
        unblocked = "q2 is unblocked"
        with open(self.router_qdra.logfile_path, 'r') as router_log:
            for line_no, log_line in enumerate(router_log, start=1):
                if unblocked in log_line:
                    num_unblocked += 1
                    unblock_line = line_no
                elif blocked in log_line:
                    block_line = line_no
                    num_blocked += 1
        self.assertGreater(num_blocked, 0)
        self.assertGreater(num_unblocked, 0)
        self.assertGreaterEqual(num_unblocked, num_blocked)
        self.assertGreater(unblock_line, block_line)


class Http2Q2TwoRouterTest(Http2TestBase):
    @classmethod
    def setUpClass(cls):
        super(Http2Q2TwoRouterTest, cls).setUpClass()
        if skip_h2_test():
            return
        cls.server_port = cls.tester.get_port()
        cls.http2_server_name = "http2_slow_qd_server"
        cls.http2_server = cls.tester.http2server(name=cls.http2_server_name,
                                                  listen_port=cls.server_port,
                                                  server_file="http2_slow_q2_server.py",
                                                  env_config={
                                                      'SERVER_LISTEN_PORT': str(cls.server_port)
                                                  })
        qdr_a = "QDR.A"
        inter_router_port = cls.tester.get_port()
        config_qdra = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.A'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpListener', {'port': cls.tester.get_port(), 'address': 'examples',
                              'host': '127.0.0.1', 'protocolVersion': 'HTTP2'}),
            ('connector', {'name': 'connectorToB', 'role': 'inter-router',
                           'port': inter_router_port,
                           'verifyHostname': 'no'})
        ])

        qdr_b = "QDR.B"
        cls.connector_name = 'serverConnector'
        cls.http_connector_props = {
            'port': cls.server_port,
            'address': 'examples',
            'host': '127.0.0.1',
            'protocolVersion': 'HTTP2',
            'name': cls.connector_name
        }
        config_qdrb = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': 'QDR.B'}),
            ('listener', {'port': cls.tester.get_port(), 'role': 'normal', 'host': '0.0.0.0'}),
            ('httpConnector', cls.http_connector_props),
            ('listener', {'role': 'inter-router', 'maxSessionFrames': '10', 'port': inter_router_port})
        ])
        cls.router_qdrb = cls.tester.qdrouterd(qdr_b, config_qdrb, wait=True)
        cls.router_qdra = cls.tester.qdrouterd(qdr_a, config_qdra, wait=True)
        cls.router_qdra.wait_router_connected('QDR.B')
        cls.router_qdrb.wait_router_connected('QDR.A')
        cls.router_qdrb.wait_http_server_connected()
        wait_http_listeners_up(cls.router_qdra.addresses[0])

    @unittest.skipIf(skip_h2_test(),
                     "Python 3.7 or greater, hyper-h2 and curl needed to run hyperhttp2 tests")
    def test_q2_block_unblock(self):
        # curl  -X POST -H "Content-Type: multipart/form-data"  -F "data=@/home/gmurthy/opensource/test.jpg"
        # http://127.0.0.1:9000/upload --http2-prior-knowledge
        address = self.router_qdra.http_addresses[0] + "/upload"
        _, out, _ = self.run_curl(address, args=self.get_all_curl_args(['-X', 'POST',
                                                                        '-H', 'Content-Type: multipart/form-data',
                                                                        '-F', 'data=@' + image_file('test.jpg')]))
        self.assertIn('Success', out)
        num_blocked = 0
        num_unblocked = 0
        blocked = "q2 is blocked"
        unblocked = "q2 is unblocked"
        with open(self.router_qdra.logfile_path, 'r') as router_log:
            for line_no, log_line in enumerate(router_log, start=1):
                if unblocked in log_line:
                    num_unblocked += 1
                    unblock_line = line_no
                elif blocked in log_line:
                    block_line = line_no
                    num_blocked += 1

        self.assertGreater(num_blocked, 0)
        self.assertGreater(num_unblocked, 0)
        self.assertGreaterEqual(num_unblocked, num_blocked)
        self.assertGreater(unblock_line, block_line)


class Http2AdaptorListenerConnectTest(HttpAdaptorListenerConnectTestBase):
    """
    Test client connecting to adaptor listeners in various scenarios
    """
    PROTOCOL_VERSION = "HTTP2"

    def start_server(self, connector_port):

        class Http2ServerContext(Http2Server):
            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                self.teardown()

        if skip_test():
            return

        server = Http2ServerContext(name="AdaptorListenerServer",
                                    listen_port=connector_port,
                                    server_file="http2_server.py",
                                    env_config={
                                        'QUART_APP': 'http2server:app',
                                        'SERVER_LISTEN_PORT': str(connector_port)
                                    })
        return server

    def client_connect(self, listener_port):
        """
        Returns True if connection succeeds, else raises an error
        """
        local_args = [f"127.0.0.1:{listener_port}", "--http2-prior-knowledge"]
        status, out, err = system_test.run_curl(local_args, timeout=TIMEOUT)
        if status == 0:
            return True
        if "Connection refused" in err:
            raise ConnectionRefusedError(err)
        raise Exception(f"CURL ERROR {status}: {out} {err}")

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_02_listener_interior(self):
        """
        Test tcpListener socket lifecycle interior to interior
        """
        self._test_listener_socket_lifecycle(self.INTA, self.INTB, "test_02_listener_interior")

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_03_listener_edge_interior(self):
        """
        Test tcpListener socket lifecycle edge to interior
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.INTB, "test_03_listener_edge_interior")

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_04_listener_interior_edge(self):
        """
        Test tcpListener socket lifecycle interior to edge
        """
        self._test_listener_socket_lifecycle(self.INTA, self.EdgeB, "test_04_listener_interior_edge")

    @unittest.skipIf(skip_test(), "Python 3.7 or greater, Quart 0.13.0 or greater and curl needed to run http2 tests")
    def test_05_listener_edge_edge(self):
        """
        Test tcpListener socket lifecycle edge to edge
        """
        self._test_listener_socket_lifecycle(self.EdgeA, self.EdgeB, "test_05_listener_edge_edge")

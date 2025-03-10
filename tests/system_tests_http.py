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
import errno
import os
import re
import threading
import ssl

from urllib.request import urlopen, build_opener, HTTPSHandler
from urllib.error import HTTPError, URLError
from skupper_router._skupper_router_site import SKIP_DELETE_HTTP_LISTENER
from system_test import Process, SkManager, retry
from system_test import TestCase, Qdrouterd, main_module
from system_test import unittest, AMQP_LISTENER_TYPE, ALLOCATOR_TYPE
from system_test import CA_CERT, CLIENT_CERTIFICATE, CLIENT_PRIVATE_KEY, CLIENT_PRIVATE_KEY_PASSWORD, \
    SERVER_CERTIFICATE, SERVER_PRIVATE_KEY_PASSWORD, SERVER_PRIVATE_KEY
#
# Note: these tests exercise the management interface accessed via HTTP. These
# tests have nothing to do with the HTTP adaptors!
#


class RouterTestHttp(TestCase):

    @classmethod
    def setUpClass(cls):
        super(RouterTestHttp, cls).setUpClass()
        # DISPATCH-1513, DISPATCH-2299: Http listener delete was broken in LWS v4 until v4.2.0
        cls.skip_delete_http_listener_test = SKIP_DELETE_HTTP_LISTENER

    @classmethod
    def get(cls, url, use_ca=True):
        if use_ca:
            sctxt = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
            sctxt.load_verify_locations(cafile=CA_CERT)
            http_data = urlopen(url, context=sctxt)
        else:
            http_data = urlopen(url)
        return http_data.read().decode('utf-8')

    @classmethod
    def get_cert(cls, url):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_cert_chain(CLIENT_CERTIFICATE,
                                CLIENT_PRIVATE_KEY,
                                CLIENT_PRIVATE_KEY_PASSWORD)
        context.load_verify_locations(CA_CERT)
        opener = build_opener(HTTPSHandler(context=context))
        return opener.open(url).read().decode('utf-8')

    def assert_get(self, url):
        self.assertEqual('HTTP test\n', self.get("%s/system_tests_http.txt" % url))

    def assert_get_cert(self, url):
        self.assertEqual('HTTP test\n', self.get_cert("%s/system_tests_http.txt" % url))

    def test_listen_error(self):
        """Make sure a router exits if an initial HTTP listener fails, doesn't hang"""
        listen_port = self.get_port()
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'bad'}),
            ('listener', {'port': listen_port, 'maxFrameSize': '2048', 'stripAnnotations': 'no'}),
            ('listener', {'port': listen_port, 'http': True})])
        with Qdrouterd(name="expect_fail", config=config, wait=False, expect=Process.EXIT_FAIL) as r:
            self.assertEqual(1, r.wait())

    def is_get_request_failing(self, url, use_ca=True, use_get_cert=False):
        try:
            if use_get_cert:
                self.get_cert(url)
            else:
                self.get(url, use_ca=use_ca)
            return False
        except OSError as e:
            expected = (errno.ECONNREFUSED, errno.ECONNRESET)
            if e.errno in expected or e.reason.errno in expected:
                return True
            raise e

    def test_http_listener_delete(self):
        name = 'delete_listener'
        name_1 = 'delete_listener_1'
        normal_listen_port = self.get_port()
        #
        # Open listeners on two HTTP enabled ports. Delete one of the
        # HTTP listeners and make sure that it is really gone and also
        # make sure that the other HTTP listener is still working.
        #
        http_delete_listen_port_1 = self.get_port()
        http_delete_listen_port_2 = self.get_port()
        config = Qdrouterd.Config([
            ('router', {'mode': 'standalone', 'id': 'A'}),
            ('listener', {'port': normal_listen_port}),
            ('listener', {'httpRootDir': os.path.dirname(__file__), 'name': name, 'port': http_delete_listen_port_1, 'http': True}),
            ('listener', {'httpRootDir': os.path.dirname(__file__), 'name': name_1, 'port': http_delete_listen_port_2, 'http': True})])
        router = self.qdrouterd(name="expect_fail_1", config=config, wait=True)

        def address():
            return router.addresses[0]

        # Perform a GET request on the http_delete_listen_port_1 just to make
        # sure that it is up and running.
        url_1 = "%s/system_tests_http.txt" % "http://localhost:%d" % http_delete_listen_port_1
        out = self.get(url_1, use_ca=False)

        # Perform a GET request on the http_delete_listen_port_2 just to make
        # sure that it is up and running.
        url_2 = "%s/system_tests_http.txt" % "http://localhost:%d" % http_delete_listen_port_2
        out = self.get(url_2, use_ca=False)

        # Now both http_delete_listen_port_1 and http_delete_listen_port_2
        # are working.

        # Delete the listener on port http_delete_listen_port_1
        mgmt = SkManager(address=address())

        if self.skip_delete_http_listener_test:
            # You are not allowed to delete a http:yes listener
            # Try deleting it and make sure you get an exception.
            exception_raised = False
            try:
                mgmt.delete(AMQP_LISTENER_TYPE, name=name)
            except Exception as e:
                if "BadRequestStatus: HTTP listeners cannot be deleted" in str(e):
                    exception_raised = True
            self.assertTrue(exception_raised)
        else:
            mgmt.delete(AMQP_LISTENER_TYPE, name=name)

            # Once again try to perform a GET request. Now since the listener
            # is gone, the GET will fail.
            ret_val = retry(lambda: self.is_get_request_failing(url_1, use_ca=False), timeout=10, delay=2)
            self.assertTrue(ret_val)

            # HTTP listener on port http_delete_listen_port_1 has been
            # deleted successfully. Make sure that the listener on port
            # http_delete_listen_port_2 is still working.
            out = self.get(url_2, use_ca=False)

    def test_http_get(self):
        config = Qdrouterd.Config([
            ('router', {'id': 'QDR.HTTP'}),
            ('listener', {'port': self.get_port(), 'httpRootDir': os.path.dirname(__file__)}),
            ('listener', {'port': self.get_port(), 'httpRootDir': os.path.dirname(__file__)}),
        ])
        r = self.qdrouterd('http-test-router', config)

        def test(port):
            self.assert_get("http://localhost:%d" % port)
            self.assertRaises(HTTPError, urlopen, "http://localhost:%d/nosuch" % port)

        # Sequential calls on multiple ports
        for port in r.ports:
            test(port)

        # Concurrent calls on multiple ports
        class TestThread(threading.Thread):
            def __init__(self, port):
                threading.Thread.__init__(self)
                self.port, self.ex = port, None
                self.start()

            def run(self):
                try:
                    test(self.port)
                except Exception as e:
                    self.ex = e
        threads = [TestThread(p) for p in r.ports + r.ports]
        for t in threads:
            t.join()
        for t in threads:
            if t.ex:
                raise t.ex

        # https not configured
        self.assertRaises(URLError, urlopen, "https://localhost:%d/nosuch" % r.ports[0])

    @unittest.skipIf(os.environ.get("SKIP_HTTP_METRICS_TEST", None) is not None, "Skipping metrics test on arm64 if asan turned on")
    def test_http_metrics(self):
        """ Verify the prometheus metrics provided by the router """
        metrics_ports = [self.get_port(), self.get_port()]
        config = Qdrouterd.Config([
            ('router', {'id': 'QDR.METRICS'}),
            ('listener', {'role': 'normal', 'port': self.get_port()}),
            ('listener', {'port': metrics_ports[0], 'http': 'yes'}),
            ('listener', {'port': metrics_ports[1], 'httpRootDir': os.path.dirname(__file__)}),
        ])
        r = self.qdrouterd('metrics-test-router', config)
        r.wait_ready()

        # generate a list of all metric names expected to be provided via HTTP:

        stat_names = ["qdr_connections_total", "qdr_links_total",
                      "qdr_addresses_total", "qdr_routers_total",
                      "qdr_presettled_deliveries_total",
                      "qdr_dropped_presettled_deliveries_total",
                      "qdr_accepted_deliveries_total",
                      "qdr_released_deliveries_total",
                      "qdr_rejected_deliveries_total",
                      "qdr_modified_deliveries_total",
                      "qdr_deliveries_ingress_total",
                      "qdr_deliveries_egress_total",
                      "qdr_deliveries_transit_total",
                      "qdr_deliveries_delayed_1sec_total",
                      "qdr_deliveries_delayed_10sec_total",
                      "qdr_deliveries_stuck_total",
                      "qdr_links_blocked_total",
                      "qdr_tcp_service_connections",
                      "qdr_amqp_service_connections",
                      "qdr_http1_service_connections",
                      "qdr_http2_service_connections"]
        for stat in r.management.query(type=ALLOCATOR_TYPE).get_dicts():
            stat_names.append(stat['typeName'])

        def _test_metrics(stat_names, port):
            # sanity check that all expected stats are reported
            sctxt = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
            sctxt.load_verify_locations(cafile=CA_CERT)
            resp = urlopen(f"http://localhost:{port}/metrics", context=sctxt)
            self.assertEqual(200, resp.getcode())
            http_response = b""
            response_chunk = resp.read(1024)
            while response_chunk:
                http_response += response_chunk
                response_chunk = resp.read(1024)
            resp_lines = http_response.decode('utf-8').splitlines()
            metrics = [x for x in resp_lines if not x.startswith("#")]

            # Verify that all metric names are valid prometheus names that
            # must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
            for metric in metrics:
                # remove trailing counter
                mname = metric.strip().split()[0]
                match = re.fullmatch(r'([a-zA-Z_:])([a-zA-Z0-9_:])*', mname)
                self.assertIsNotNone(match, f"Metric {mname} has invalid name syntax")

            # Verify that all expected stats are reported by the metrics URL
            for name in stat_names:
                found = False
                for metric in metrics:
                    # remove the counter and strip the allocator name suffix
                    # (if present)
                    mname = metric.strip().split()[0].split(':')[0]
                    if mname == name:
                        found = True
                        break
                self.assertTrue(found, f"Did not find {name} in returned metrics!")

        # Sequential calls on multiple ports
        for port in metrics_ports:
            _test_metrics(stat_names, port)

        # Concurrent calls on multiple ports
        class TestThread(threading.Thread):
            def __init__(self, port):
                threading.Thread.__init__(self)
                self.port, self.ex = port, None
                self.start()

            def run(self):
                try:
                    _test_metrics(stat_names, self.port)
                except Exception as e:
                    self.ex = e

        threads = [TestThread(p) for p in metrics_ports * 4]
        for t in threads:
            t.join()
        for t in threads:
            if t.ex:
                raise t.ex

    def test_http_healthz(self):
        config = Qdrouterd.Config([
            ('router', {'id': 'QDR.HEALTHZ'}),
            ('listener', {'port': self.get_port(), 'http': 'yes'}),
            ('listener', {'port': self.get_port(), 'httpRootDir': os.path.dirname(__file__)}),
        ])
        r = self.qdrouterd('metrics-test-router', config)

        def test(port):
            sctxt = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
            sctxt.load_verify_locations(cafile=CA_CERT)
            result = urlopen("http://localhost:%d/healthz" % port, context=sctxt)
            self.assertEqual(200, result.getcode())

        # Sequential calls on multiple ports
        for port in r.ports:
            test(port)

        # Concurrent calls on multiple ports
        class TestThread(threading.Thread):
            def __init__(self, port):
                threading.Thread.__init__(self)
                self.port, self.ex = port, None
                self.start()

            def run(self):
                try:
                    test(self.port)
                except Exception as e:
                    self.ex = e
        threads = [TestThread(p) for p in r.ports + r.ports]
        for t in threads:
            t.join()
        for t in threads:
            if t.ex:
                raise t.ex

    def test_https_get(self):
        def http_listener(**kwargs):
            args = dict(kwargs)
            args.update({'port': self.get_port(), 'http': 'yes', 'httpRootDir': os.path.dirname(__file__)})
            return ('listener', args)

        def listener(**kwargs):
            args = dict(kwargs)
            args.update({'port': self.get_port()})
            return ('listener', args)

        name = 'delete-me'
        config = Qdrouterd.Config([
            ('router', {'id': 'QDR.HTTPS'}),
            ('sslProfile', {'name': 'simple-ssl',
                            'caCertFile': CA_CERT,
                            'certFile': SERVER_CERTIFICATE,
                            'privateKeyFile': SERVER_PRIVATE_KEY,
                            'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:RSA+AESGCM:RSA+AES:!aNULL:!MD5:!DSS',
                            'password': SERVER_PRIVATE_KEY_PASSWORD
                            }),
            http_listener(sslProfile='simple-ssl', requireSsl=False, authenticatePeer=False),
            http_listener(sslProfile='simple-ssl', requireSsl=True, authenticatePeer=False),
            http_listener(sslProfile='simple-ssl', requireSsl=True, authenticatePeer=True),
            http_listener(name=name, sslProfile='simple-ssl', requireSsl=True, authenticatePeer=True),
            listener(name='mgmt_listener', authenticatePeer=False)])
        # saslMechanisms='EXTERNAL'

        r = self.qdrouterd('https-test-router', config)
        r.wait_ready()

        def address():
            return r.addresses[4]

        self.assert_get("https://localhost:%s" % r.ports[0])
        # requireSsl=false Allows simple-ssl HTTP

        # DISPATCH-1513: libwebsockets version 3.2.0 introduced a new flag called
        # LWS_SERVER_OPTION_ALLOW_HTTP_ON_HTTPS_LISTENER
        # The new flag allows (as the flag says) HTTP over HTTPS listeners.
        self.assert_get("http://localhost:%s" % r.ports[0])

        self.assert_get("https://localhost:%s" % r.ports[1])
        # requireSsl=True does not allow simple-ssl HTTP
        self.assertRaises(Exception, self.assert_get, "http://localhost:%s" % r.ports[1])

        # authenticatePeer=True requires a client cert
        self.assertRaises((URLError, ssl.SSLError), self.assert_get, "https://localhost:%s" % r.ports[2])

        # Provide client cert
        self.assert_get_cert("https://localhost:%d" % r.ports[2])

        # Try a get on the HTTP listener we are going to delete
        self.assert_get_cert("https://localhost:%d" % r.ports[3])

        if not self.skip_delete_http_listener_test:
            # Delete the listener with name 'delete-me'
            mgmt = SkManager(address=address())
            mgmt.delete(AMQP_LISTENER_TYPE, name=name)

            # Make sure that the listener got deleted.
            ret_val = retry(lambda: self.is_get_request_failing("https://localhost:%s/system_tests_http.txt" % r.ports[3], use_get_cert=True), timeout=10, delay=2)
            self.assertTrue(ret_val)

            # Make sure other ports are working normally after the above delete.
            self.assert_get_cert("https://localhost:%d" % r.ports[2])


if __name__ == '__main__':
    unittest.main(main_module())

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

import logging
import os
import socket
import uuid
import weakref
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.client import HTTPConnection, HTTPSConnection
from ssl import SSLContext, SSLSocket
from threading import Thread, Event
from time import sleep, time

from typing import List, Any, Tuple, Mapping, Optional

from skupper_router.management.client import Node
from system_test import TestCase, TIMEOUT, Logger, Qdrouterd, unittest
from system_test import curl_available, run_curl, retry
from system_test import retry_exception


CURL_VERSION = (7, 47, 0)   # minimum required


class SimpleRequestTimeout(Exception):
    """Thrown by http1_simple_request when timeout occurs during socket
    read. Contains any reply data read from the server prior to the timeout
    """
    pass


def _curl_ok():
    """
    Returns True if curl is installed and is the proper version for
    running http1.1
    """
    installed = curl_available()
    return installed and installed >= CURL_VERSION


def http1_simple_request(raw_request: bytes,
                         port: int,
                         host: Optional[str] = '127.0.0.1',
                         timeout: Optional[float] = TIMEOUT,
                         ssl_context: Optional[SSLContext] = None):
    """Perform a simple HTTP/1 request and return the response read from the
    server. raw_request is a complete HTTP/1 request message. The client socket
    will be read from until either it is closed or the TIMEOUT occurs.

    It is expected that raw_request will include a "Connection: close" header
    to prevent the TIMEOUT exception from being thrown.
    """
    reply = b''
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if ssl_context is not None:
        client = ssl_context.wrap_socket(client, server_side=False,
                                         server_hostname=host)
    with client:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.settimeout(timeout)

        # allow time for the listening socket to initialize
        retry_exception(lambda: client.connect((host, port)),
                        delay=0.25, exception=ConnectionRefusedError)
        if ssl_context is None:
            client.sendall(raw_request, socket.MSG_WAITALL)
        else:
            # because SSLSocket barfs on non-zero flags:
            while raw_request:
                sent = client.send(raw_request)
                raw_request = raw_request[sent:]
        try:
            while True:
                rc = client.recv(4096)
                if not rc:
                    # socket closed (response complete)
                    break
                reply += rc
        except TimeoutError:
            raise SimpleRequestTimeout(reply)
    return reply


class RequestHandler(BaseHTTPRequestHandler):
    """
    Dispatches requests received by the HTTPServer based on the method
    """
    protocol_version = 'HTTP/1.1'

    def _execute_request(self, method):
        self.server.logger.log(f"Executing {method} request")
        tests = self.server.system_tests.get(method)
        if tests is None:
            self.server.logger.log(f"request failed: method '{method}' not supported")
            self.send_error(405, "Method Not Allowed")
            return

        for req, resp, val in tests:
            if req.target == self.path:
                self.server.logger.log(f"target={req.target}")
                req.rx_count += 1
                xhdrs = None
                if "test-echo" in self.headers:
                    xhdrs = {"test-echo":
                             self.headers["test-echo"]}
                self._consume_body()
                if not isinstance(resp, list):
                    resp = [resp]
                for r in resp:
                    octets = len(r.body) if r.body else 0
                    self.server.logger.log(f"sending response: {r.status}: {r.reason} {octets} octets")
                    r.send_response(self, extra_headers=xhdrs)
                self.server.logger.log(f"target={req.target} complete!")
                self.server.request_count += 1
                return
        self.server.logger.log(f"request failed: {self.path} not found")
        self.send_error(404, "Not Found")

    def do_GET(self):
        self._execute_request("GET")

    def do_HEAD(self):
        self._execute_request("HEAD")

    def do_POST(self):
        if self.path == "/SHUTDOWN":
            self.server.logger.log("SHUTDOWN request received!")
            self.send_response(200, "OK")
            self.send_header("Content-Length", "13")
            self.end_headers()
            self.wfile.write(b'Server Closed')
            self.wfile.flush()
            self.close_connection = True  # server will close connection from router
            self.server.server_killed = True  # server will not accept a reconnect
            return
        self._execute_request("POST")

    def do_PUT(self):
        self._execute_request("PUT")

    # these overrides just quiet the test output
    # comment them out to help debug:
    def log_request(self, code=None, size=None):
        pass

    def log_message(self, *args, format=None):
        pass

    def _consume_body(self):
        """
        Read the entire body off the rfile.  This must be done to allow
        multiple requests on the same socket
        """
        if self.command == 'HEAD':
            return b''

        for key, value in self.headers.items():
            if key.lower() == 'content-length':
                self.server.logger.log(f"reading {value} body octets")
                return self.rfile.read(int(value))

            if key.lower() == 'transfer-encoding'  \
               and 'chunked' in value.lower():
                body = b''
                while True:
                    header = self.rfile.readline().strip().split(b';')[0]
                    hlen = int(header, base=16)
                    self.server.logger.log(f"reading {hlen} chunk octets")
                    if hlen > 0:
                        data = self.rfile.read(hlen + 2)  # 2 = \r\n
                        body += data[:-2]
                    else:
                        self.rfile.readline()  # discard last \r\n
                        break
                self.server.logger.log(f"body read: {len(body)} octets")
                return body
        return b''


class RequestHandler10(RequestHandler):
    """
    RequestHandler that forces the server to use HTTP version 1.0 semantics
    """
    protocol_version = 'HTTP/1.0'


class MyHTTPServer(HTTPServer):
    """Adds a switch to the HTTPServer to allow it to exit gracefully"""

    def __init__(self, addr, handler_cls, testcases, logger,
                 ssl_context: Optional[SSLContext] = None):
        assert logger
        self._client_sockets: List[socket.socket] = []

        logger.log(f"Creating HTTPServer for {addr}")

        self.allow_reuse_address = True
        self.timeout = TIMEOUT
        self.server_killed = False
        self.system_tests = testcases
        self.request_count = 0
        self.logger = logger
        self.my_ssl_context = ssl_context
        super().__init__(addr, handler_cls, bind_and_activate=False)
        if ssl_context is not None:
            logger.log("Wrapping listener socket in SSLContext")
            try:
                self.socket = ssl_context.wrap_socket(self.socket,
                                                      server_side=True)
            except Exception as exc:
                logger.log(f"Failed to wrap listener socket: {exc}")
                raise
        try:
            self.server_bind()
            self.server_activate()
        except Exception as exc:
            logger.log(f"Failed bind/activate listening socket: {exc}")
            self.server_close()
            raise

    def get_request(self) -> Tuple[socket.socket, Any]:
        """Remember the client socket"""
        self.logger.log("Accepting new client connection...")
        sock, addr = super().get_request()
        self.logger.log(f"Accepted client socket {addr}")
        self._client_sockets.append(sock)
        return sock, addr

    def server_close(self):
        """Close the server listening socket"""
        # hack alert: sometimes the listener socket remains active after the
        # server has exited. This causes the router to hang (outstanding
        # connect request). Try to force-close the socket to prevent this.
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except Exception as exc:
            self.logger.log(f"Failed to shutdown listener socket: {exc}")
        super().server_close()
        self.logger.log("Listener socket shutdown")

    def shutdown_request(self, request) -> None:
        """Forget the client socket"""
        assert isinstance(request, socket.socket)

        self._client_sockets.remove(request)

        if isinstance(request, SSLSocket):
            try:
                self.logger.log("Start TLS close handshake")
                request = request.unwrap()
                self.logger.log("TLS close handshake complete")
            except Exception as exc:
                self.logger.log(f"TLS close handshake error: {exc}")
                print(f"TLS socket shutdown failed: {exc}", flush=True)
                raise
        super().shutdown_request(request)  # type: ignore[misc]  # error: "shutdown_request" undefined in superclass
        self.logger.log("Request socket shutdown")

    def force_disconnect_clients(self) -> None:
        """Shutdown the remembered sockets

        https://stackoverflow.com/questions/70916335/how-do-i-forcibly-disconnect-all-currently-connected-clients-to-my-tcp-or-http-s
        """
        for client in self._client_sockets:
            client.shutdown(socket.SHUT_RDWR)


class ThreadedTestClient:
    """
    An HTTP client running in a separate thread
    """

    def __init__(self, tests, host_port, repeat=1, wait=True, timeout=TIMEOUT,
                 ssl_context: Optional[SSLContext] = None):
        self.error = None
        self._id = uuid.uuid4().hex
        host_port = str(host_port)
        self._conn_addr = host_port if ':' in host_port else f"127.0.0.1:{host_port}"
        self._tests = tests
        self._repeat = repeat
        self._count = 0
        self._ssl_context = ssl_context
        self._logger = Logger(title="TestClient: %s" % self._id,
                              print_to_console=False)
        self._thread = Thread(target=self._run, args=(wait, timeout), daemon=True)
        self._thread.start()

    def _run(self, wait, timeout):
        self._logger.log("TestClient connecting on %s" % self._conn_addr)
        if self._ssl_context is not None:
            client = HTTPSConnection(self._conn_addr, timeout=timeout, context=self._ssl_context)
        else:
            client = HTTPConnection(self._conn_addr, timeout=timeout)
        deadline = timeout + time()
        while True:
            # wait for listener socket to initialize
            try:
                client.connect()
                break
            except ConnectionRefusedError:
                if wait is False or time() >= deadline:
                    self._logger.log("TestClient: connection to %s refused"
                                     % self._conn_addr)
                    self._logger.dump()
                    return
                sleep(0.25)
        try:
            self._logger.log("TestClient connected")
            for loop in range(self._repeat):
                self._logger.log("TestClient start request %d" % loop)
                for op, tests in self._tests.items():
                    for req, _, val in tests:
                        self._logger.log("TestClient sending %s %s request" % (op, req.target))
                        req.send_request(client,
                                         {"test-echo": "%s-%s-%s-%s" % (self._id,
                                                                        loop,
                                                                        op,
                                                                        req.target)})
                        self._logger.log("TestClient getting %s response" % op)
                        try:
                            rsp = client.getresponse()
                        except Exception as exc:
                            self._logger.log("TestClient response failed: %s" % exc)
                            self.error = str(exc)
                            return
                        self._logger.log("TestClient response %s received" % op)
                        if val:
                            try:
                                body = val.check_response(rsp)
                            except Exception as exc:
                                self._logger.log("TestClient response invalid: %s"
                                                 % str(exc))
                                self.error = "client failed: %s" % str(exc)
                                return

                            if req.method == "BODY" and body != b'':
                                self._logger.log("TestClient response invalid: %s"
                                                 % "body present!")
                                self.error = "error: body present!"
                                return
                        self._count += 1
                        self._logger.log("TestClient request %s %s completed!" %
                                         (op, req.target))
        finally:
            client.close()
            self._logger.log("TestClient to %s closed" % self._conn_addr)

    def wait(self, timeout=TIMEOUT, dump_on_error=True):
        self._thread.join(timeout=timeout)
        self._logger.log("TestClient %s shut down" % self._conn_addr)
        if dump_on_error and self.error:
            self.dump_log()

    def dump_log(self):
        self._logger.dump()

    def check_count(self, expected_count):
        if expected_count != self._count:
            msg = f"Expected {expected_count} requests, got {self._count}"
            self._logger.log(msg)
            self.dump_log()
            raise AssertionError(msg)


class TestServer:
    """A HTTPServer running in a separate thread"""
    __test__ = False
    #_active_servers: Mapping[int, 'TestServer'] = weakref.WeakValueDictionary()
    _active_servers = weakref.WeakValueDictionary()  # type: ignore

    def __init__(self, server_port: int, client_port: int, tests, handler_cls=None,
                 server_ssl_context: Optional[SSLContext] = None,
                 client_ssl_context: Optional[SSLContext] = None):
        self._request_count = 0
        log_file = os.path.join(os.getcwd(), f"TestServer_{server_port}.log")
        self._logger = Logger(title=f"TestServer:{server_port}",
                              print_to_console=False,
                              ofilename=log_file)
        self._client_port = client_port
        self._client_ssl_context = client_ssl_context
        self._server_addr = ("localhost", server_port)
        self._server_error = None
        self._server_ssl_context = server_ssl_context
        self._is_ready = Event()
        self._thread = Thread(target=self._run, args=(tests,
                                                      handler_cls or
                                                      RequestHandler),
                              daemon=True)
        self._thread.start()

        # Block the caller until the MyHTTPServer.__init__() call completes in
        # the background thread. This guarantees that the server socket is
        # present and listening before the caller starts connecting clients.
        self._is_ready.wait(TIMEOUT)

    @classmethod
    def new_server(cls, server_port: int, client_port: int, tests, handler_cls=None,
                   server_ssl_context: Optional[SSLContext] = None,
                   client_ssl_context: Optional[SSLContext] = None):
        """Repeatedly tries to create a new server.

        Always use this factory method, otherwise the mechanism
        to kill leaked servers will not work.
        """
        exc = None
        for _ in range(2):
            try:
                server = TestServer(server_port=server_port,
                                    client_port=client_port,
                                    tests=tests,
                                    handler_cls=handler_cls,
                                    server_ssl_context=server_ssl_context,
                                    client_ssl_context=client_ssl_context)
                cls._active_servers[server_port] = server
                return server
            except OSError as e:
                previous_server = cls._active_servers.get(server_port)
                if previous_server:
                    logging.log(logging.WARNING, "TestServer on port %d was not wait()'ed before", server_port)
                    previous_server.wait(timeout=1)  # die quickly, we don't have a whole day
                exc = e  # remember the exception
        else:
            raise RuntimeError(f"TestServer failed to start due to port {server_port} already in use issue") from exc

    def _run(self, tests, handler_cls):
        self._logger.log("TestServer listening on %s:%s" % self._server_addr)

        with MyHTTPServer(self._server_addr, handler_cls, tests, self._logger,
                          ssl_context=self._server_ssl_context) as server:
            # listening socket is now open and clients can connect
            self._is_ready.set()
            while not server.server_killed:
                try:
                    self._logger.log("calling handle_request()")
                    server.handle_request()
                    self._logger.log("handle_request() complete")
                except Exception as exc:
                    self._logger.log("TestServer %s crash: %s" %
                                     (self._server_addr, exc))
                    # do not bother raising here since this is an internal
                    # thread context:
                    self._server_error = exc
                    server.server_killed = True
            self._request_count = server.request_count
        self._logger.log("TestServer %s:%s thread exited" % self._server_addr)

    def wait(self, timeout=TIMEOUT, check_reply=True):
        self._logger.log("TestServer %s:%s requesting shut down" % self._server_addr)
        self._send_shutdown_request(check_reply)
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            # should not happen unless the shutdown request failed due to
            # a router error
            self._logger.log("TestServer Error: Failed to shutdown HTTP/1 TestServer")
            self._logger.dump()
            raise RuntimeError("HTTP/1 TestServer failed to shut down")
        if self._server_error:
            self._logger.log(f"TestSever Error: server error on shutdown: '{self._server_error}'")
            self._logger.dump()
            raise RuntimeError("HTTP/1 TestServer fatal exception") from self._server_error
        self._logger.log("TestServer %s:%s shut down!" % self._server_addr)

    def dump_log(self):
        self._logger.dump()

    def check_count(self, expected_count):
        if expected_count != self._request_count:
            msg = f"Expected {expected_count} requests, got {self._request_count}"
            self._logger.log(msg)
            self.dump_log()
            raise AssertionError(msg)

    def _send_shutdown_request(self, check_reply=True):
        """Sends a POST request instructing the test HTTPServer to shut down.

        This is necessary because the test HTTPServer cannot be interrupted while there is an open
        connection to it. The incoming connection in question is from a qdrouterd that we keep up
        for the duration of the entire testclass. The only place for server to graciously die is
        after processing an incoming request and closing the connection, but
        before accepting a new one.

        TODO: deprecate this test server and just use nginx
        """
        shutdown_request = b'POST /SHUTDOWN HTTP/1.1\r\n' \
            + b'Content-Length: 0\r\n' \
            + b'Connection: close\r\n' \
            + b'\r\n'

        reply = http1_simple_request(shutdown_request, self._client_port,
                                     host="localhost",
                                     ssl_context=self._client_ssl_context)
        if check_reply and b'Server Closed' not in reply:
            if self._server_error is None:  # do not overwrite first error
                self._server_error = RuntimeError(f"bad shutdown response {reply}")

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._thread.is_alive():
            self.wait()


def wait_http_listeners_up(mgmt_address: str,
                           l_filter: Optional[Mapping[str, Any]] = None,
                           timeout: float = TIMEOUT):
    """
    Wait until the configured HTTP listener sockets have come up. Optionally
    filter the set of configured listeners using attribute names and values
    """
    LISTENER_TYPE = 'io.skupper.router.httpListener'
    return _wait_adaptor_listeners_oper_status(LISTENER_TYPE, mgmt_address,
                                               'up', l_filter, timeout)


def wait_http_listeners_down(mgmt_address: str,
                             l_filter: Optional[Mapping[str, Any]] = None,
                             timeout: float = TIMEOUT):
    """
    Wait until the configured HTTP listener sockets have
    deactivated. Optionally filter the set of configured listeners using
    attribute names and values
    """
    LISTENER_TYPE = 'io.skupper.router.httpListener'
    return _wait_adaptor_listeners_oper_status(LISTENER_TYPE, mgmt_address,
                                               'down', l_filter, timeout)


def wait_tcp_listeners_up(mgmt_address: str,
                          l_filter: Optional[Mapping[str, Any]] = None,
                          timeout: float = TIMEOUT):
    """
    Wait until the configured TCP listener sockets have come up. Optionally
    filter the set of configured listeners using attribute names and values
    """
    LISTENER_TYPE = 'io.skupper.router.tcpListener'
    return _wait_adaptor_listeners_oper_status(LISTENER_TYPE, mgmt_address,
                                               'up', l_filter, timeout)


def _wait_adaptor_listeners_oper_status(listener_type,
                                        mgmt_address: str,
                                        oper_status: str,
                                        l_filter: Optional[Mapping[str, Any]] = None,
                                        timeout: float = TIMEOUT):
    """
    Wait until the selected listener socket operStatus has reached 'oper_status'
    """
    mgmt = Node.connect(mgmt_address, timeout=timeout)
    l_filter = l_filter or {}
    attributes = set(l_filter.keys())
    attributes.add("name")  # required for query() to work
    attributes.add("operStatus")

    def _filter_listener(listener):
        for key, value in l_filter.items():
            if listener[key] != value:
                return False
        return True

    def _check():
        listeners = mgmt.query(type=listener_type,
                               attribute_names=list(attributes)).get_dicts()
        listeners = filter(_filter_listener, listeners)
        assert listeners, "Filter error: no listeners matched"
        for listener in listeners:
            if listener['operStatus'] != oper_status:
                return False
        return True
    assert retry(_check, timeout=timeout, delay=0.25), \
        f"Timed out waiting for {listener_type} operStatus {oper_status}"


def http1_ping(sport, cport):
    """
    Test the HTTP path by doing a simple GET request
    """
    TEST = {
        "GET": [
            (RequestMsg("GET", "/GET/ping",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 4,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'pong'),
             ResponseValidator(expect_body=b'pong'))
        ]
    }

    with TestServer.new_server(sport, cport, TEST) as server:
        client = ThreadedTestClient(tests=TEST, host_port=cport)
        client.wait()
        client.check_count(1)


class ResponseMsg:
    """
    A 'hardcoded' HTTP response message.  This class writes its response
    message when called by the HTTPServer via the BaseHTTPRequestHandler
    """

    def __init__(self, status, version=None, reason=None,
                 headers=None, body=None, error=False):
        self.status = status
        self.version = version or "HTTP/1.1"
        self.reason = reason
        self.headers = headers or {}
        self.body = body
        self.error = error

    def send_response(self, handler, extra_headers=None):
        extra_headers = extra_headers or {}
        if self.error:
            handler.send_error(self.status,
                               message=self.reason)
            return

        handler.send_response(self.status, self.reason)
        for key, value in self.headers.items():
            handler.send_header(key, value)
        for key, value in extra_headers.items():
            handler.send_header(key, value)
        handler.end_headers()

        if self.body:
            handler.wfile.write(self.body)
        handler.wfile.flush()


class RequestMsg:
    """
    A 'hardcoded' HTTP request message.  This class writes its request
    message to the HTTPConnection.
    """

    def __init__(self, method, target, headers=None, body=None):
        self.method = method
        self.target = target
        self.headers = headers or {}
        self.body = body
        self.rx_count = 0  # +1 when server gets the request

    def send_request(self, conn: HTTPConnection, extra_headers=None):
        extra_headers = extra_headers or {}
        conn.putrequest(self.method, self.target)
        for key, value in self.headers.items():
            conn.putheader(key, value)
        for key, value in extra_headers.items():
            conn.putheader(key, value)
        conn.endheaders()
        if self.body:
            conn.send(self.body)


class ResponseValidator:
    """
    Validate a response as received by the HTTP client
    """

    def __init__(self, status=200, expect_headers=None, expect_body=None):
        if expect_headers is None:
            expect_headers = {}
        self.status = status
        self.expect_headers = expect_headers
        self.expect_body = expect_body

    def check_response(self, rsp):
        # always fully read the response first
        body = rsp.read()

        if self.status and rsp.status != self.status:
            raise Exception("Bad response code, expected %s got %s"
                            % (self.status, rsp.status))
        for key, value in self.expect_headers.items():
            if rsp.getheader(key) != value:
                raise Exception("Missing/bad header (%s), expected %s got %s"
                                % (key, value, rsp.getheader(key)))

        if self.expect_body and self.expect_body != body:
            raise Exception("Bad response body expected %s got %s"
                            % (self.expect_body, body))
        return body


class CommonHttp1Edge2EdgeTest:
    """
    Defines the Edge2Edge test cases. Parent classes should be derived from
    this class and Http1Edge2EdgeTestBase (as peers).
    """
    def test_01_concurrent_requests(self):
        """
        Test multiple concurrent clients sending streaming messages. This class
        expects to be derived with Http1Edge2EdgeTestBase as a peer.
        """

        REQ_CT = 3  # 3 requests per TEST_*
        TESTS_11 = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/test_01_concurrent_requests_11",
                            headers={
                                "Transfer-encoding": "chunked",
                                "Content-Type": "text/plain;charset=utf-8"
                            },
                            # ~384K to trigger Q2
                            body=b'20000\r\n' + b'1' * 0x20000 + b'\r\n'
                            + b'20000\r\n' + b'2' * 0x20000 + b'\r\n'
                            + b'20000\r\n' + b'3' * 0x20000 + b'\r\n'
                            + b'13\r\nEND OF TRANSMISSION\r\n'
                            + b'0\r\n\r\n'),
                 ResponseMsg(201, reason="Created",
                             headers={"Test-Header": "/PUT/test_01_concurrent_requests_11",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201)
                 )],

            "GET": [
                (RequestMsg("GET", "/GET/test_01_concurrent_requests_11_small",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "Content-Length": "19",
                                 "Content-Type": "text/plain;charset=utf-8",
                                 "Test-Header": "/GET/test_01_concurrent_requests_11_small"
                             },
                             body=b'END OF TRANSMISSION'),
                 ResponseValidator(status=200)),

                (RequestMsg("GET", "/GET/test_01_concurrent_requests_11",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "transfer-Encoding": "chunked",
                                 "Content-Type": "text/plain;charset=utf-8",
                                 "Test-Header": "/GET/test_01_concurrent_requests_11"
                             },
                             # ~384K to trigger Q2
                             body=b'20000\r\n' + b'1' * 0x20000 + b'\r\n'
                             + b'20000\r\n' + b'2' * 0x20000 + b'\r\n'
                             + b'20000\r\n' + b'3' * 0x20000 + b'\r\n'
                             + b'13\r\nEND OF TRANSMISSION\r\n'
                             + b'0\r\n\r\n'),
                 ResponseValidator(status=200)
                 )],
        }

        TESTS_10 = {
            "POST": [
                (RequestMsg("POST", "/POST/test_01_concurrent_requests_10",
                            headers={"Content-Type": "text/plain;charset=utf-8",
                                     "Content-Length": "393216"},
                            body=b'P' * 393197
                            + b'END OF TRANSMISSION'),
                 ResponseMsg(201, reason="Created",
                             headers={"Test-Header": "/POST/test_01_concurrent_requests_10",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201)
                 )],

            "GET": [
                (RequestMsg("GET", "/GET/test_01_concurrent_requests_10_small",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             # no content-length, server must close conn when done
                             headers={"Test-Header": "/GET/test_01_concurrent_requests_10_small",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'END OF TRANSMISSION'),
                 ResponseValidator(status=200)),

                (RequestMsg("GET", "/GET/test_01_concurrent_requests_10",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Test-Header": "/GET/test_01_concurrent_requests_10",
                                      "Content-Length": "393215",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'G' * 393196
                             + b'END OF TRANSMISSION'),
                 ResponseValidator(status=200)
                 )],
        }

        with self.create_server(self.server11_port, self.listener11_port, TESTS_11) as server11, \
             self.create_server(self.server10_port, self.listener10_port, TESTS_10,
                                handler_cls=RequestHandler10) as server10:

            self.EA2.wait_connectors()

            repeat_ct = 10
            client_ct = 4  # per version
            clients = []
            for _ in range(client_ct):
                clients.append(self.create_threaded_client(TESTS_11,
                                                           self.listener11_host_port,
                                                           repeat=repeat_ct))
                clients.append(self.create_threaded_client(TESTS_10,
                                                           self.listener10_host_port,
                                                           repeat=repeat_ct))
                for client in clients:
                    client.wait()
                    client.check_count(repeat_ct * REQ_CT)

            server11.wait()
            server11.check_count(client_ct * repeat_ct * REQ_CT)
            server10.wait()
            server10.check_count(client_ct * repeat_ct * REQ_CT)

    def test_02_credit_replenish(self):
        """
        Verify credit is replenished by sending > the default credit window
        requests across the routers.  The default credit window is 250
        """

        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_02_credit_replenish",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "24",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_02_credit_replenish'),
                 ResponseValidator(status=200),
                 ),
            ]
        }

        with self.create_server(self.server11_port, self.listener11_port, TESTS) as server11:
            self.EA2.wait_connectors()

            client = self.create_threaded_client(TESTS,
                                                 self.listener11_host_port,
                                                 repeat=300)
            client.wait()
            client.check_count(300)

    def test_03_server_reconnect(self):
        """
        Verify server reconnect logic.
        """
        if self.skip.get("test_03_server_reconnect"):
            self.skipTest("Not supported")

        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_03_server_reconnect",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "24",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_03_server_reconnect'),
                 ResponseValidator(status=200),
                 ),
            ]
        }

        # bring up the server and send some requests. This will cause the
        # router to grant credit for clients
        with self.create_server(self.server11_port, self.listener11_port, TESTS) as server:
            self.EA2.wait_connectors()

            client = self.create_threaded_client(TESTS,
                                                 self.listener11_host_port,
                                                 repeat=2)
            client.wait()
            client.check_count(2)

        # server has been closed due to exiting the with block (simulate server
        # loss).  Fire up a client which should be granted credit since the
        # adaptor does not immediately teardown the server links.  This will
        # cause the adaptor to run qdr_connection_process without a raw
        # connection available to wake the I/O thread..
        client = self.create_threaded_client(TESTS,
                                             self.listener11_host_port,
                                             repeat=2)
        # the adaptor will detach the links to the server if the connection
        # cannot be reestablished after 2.5 seconds.  Restart the server before
        # that occurrs to prevent client messages from being released with 503
        # status.
        with self.create_server(self.server11_port,
                                self.listener11_port, TESTS) as server:

            client.wait()
            client.check_count(2)

    def test_04_server_pining_for_the_fjords(self):
        """
        Test permanent loss of server
        """
        if self.skip.get("test_04_server_pining_for_the_fjords"):
            self.skipTest("Not supported")

        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_04_fjord_pining",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "20",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_04_fjord_pining'),
                 ResponseValidator(status=200),
                 ),
            ]
        }

        # bring up the server and send some requests. This will cause the
        # router to grant credit for clients
        with self.create_server(self.server11_port, self.listener11_port, TESTS) as server:
            self.EA2.wait_connectors()

            client = self.create_threaded_client(TESTS, self.listener11_host_port)
            client.wait()
            client.check_count(1)

        # After the server has exited issue client requests. These requests
        # will be held on the server's outgoing links until they expire (2.5
        # seconds).  At that point the client will receive a 503 response.

        TESTS_FAIL = {
            "GET": [
                (RequestMsg("GET", "/GET/test_04_fjord_pining",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "20",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_04_fjord_pining'),
                 ResponseValidator(status=503),
                 ),
            ]
        }

        client = self.create_threaded_client(TESTS_FAIL, self.listener11_host_port)
        client.wait()
        client.check_count(1)

        # ensure links recover once the server re-appears
        with self.create_server(self.server11_port, self.listener11_port, TESTS) as server:
            self.EA2.wait_connectors()

            client = self.create_threaded_client(TESTS, self.listener11_host_port)
            client.wait()
            client.check_count(1)

    def test_05_large_streaming_msg(self):
        """
        Verify large streaming message transfer
        """
        TESTS_11 = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/streaming_test_11",
                            headers={
                                "Transfer-encoding": "chunked",
                                "Content-Type": "text/plain;charset=utf-8"
                            },
                            # 4 chunks each ~= 600K
                            body=b'927C1\r\n' + b'0' * 0x927C0 + b'X\r\n'
                            + b'927C0\r\n' + b'1' * 0x927C0 + b'\r\n'
                            + b'927C1\r\n' + b'2' * 0x927C0 + b'X\r\n'
                            + b'927C0\r\n' + b'3' * 0x927C0 + b'\r\n'
                            + b'0\r\n\r\n'),

                 ResponseMsg(201, reason="Created",
                             headers={"Response-Header": "data",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201))
            ],

            "GET": [
                (RequestMsg("GET", "/GET/streaming_test_11",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "transfer-Encoding": "chunked",
                                 "Content-Type": "text/plain;charset=utf-8"
                             },
                             # two 1.2MB chunk
                             body=b'124f80\r\n' + b'4' * 0x124F80 + b'\r\n'
                             + b'124f80\r\n' + b'5' * 0x124F80 + b'\r\n'
                             + b'0\r\n\r\n'),
                 ResponseValidator(status=200))
            ],
        }

        TESTS_10 = {
            "POST": [
                (RequestMsg("POST", "/POST/streaming_test_10",
                            headers={"Header-1": "H" * 2048,
                                     "Content-Length": "2097155",
                                     "Content-Type": "text/plain;charset=utf-8"},
                            body=b'P' * 2097155),
                 ResponseMsg(201, reason="Created",
                             headers={"Response-Header": "data",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201))
            ],

            "GET": [
                (RequestMsg("GET", "/GET/streaming_test_10",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "1999999",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'G' * 1999999),
                 ResponseValidator(status=200))
            ],
        }

        with self.create_server(self.server11_port, self.listener11_port, TESTS_11) as server11, \
             self.create_server(self.server10_port, self.listener10_port, TESTS_10,
                                handler_cls=RequestHandler10) as server10:
            self.EA2.wait_connectors()
            client11 = self.create_threaded_client(TESTS_11,
                                                   self.listener11_host_port,
                                                   repeat=2)
            client11.wait()
            client11.check_count(4)

            client10 = self.create_threaded_client(TESTS_10,
                                                   self.listener10_host_port,
                                                   repeat=2)
            client10.wait()
            client10.check_count(4)


class CommonHttp1OneRouterTest:
    # Base tests for HTTP/1 One Router tests. Use Http1OneRouterTestBase to
    # derive One Router test classes
    TESTS_11 = {
        #
        # GET
        #
        "GET": [
            (RequestMsg("GET", "/GET/error",
                        headers={"Content-Length": 0}),
             ResponseMsg(400, reason="Bad breath", error=True),
             ResponseValidator(status=400)),

            (RequestMsg("GET", "/GET/no_content",
                        headers={"Content-Length": 0}),
             ResponseMsg(204, reason="No Content"),
             ResponseValidator(status=204)),

            (RequestMsg("GET", "/GET/content_len",
                        headers={"Content-Length": "00"}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 1,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'?'),
             ResponseValidator(expect_headers={'Content-Length': '1'},
                               expect_body=b'?')),

            (RequestMsg("GET", "/GET/content_len_511",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 511,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 511),
             ResponseValidator(expect_headers={'Content-Length': '511'},
                               expect_body=b'X' * 511)),

            (RequestMsg("GET", "/GET/content_len_4096",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 4096,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 4096),
             ResponseValidator(expect_headers={'Content-Length': '4096'},
                               expect_body=b'X' * 4096)),

            (RequestMsg("GET", "/GET/chunked",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"transfer-encoding": "chunked",
                                  "Content-Type": "text/plain;charset=utf-8"},
                         # note: the chunk length does not count the trailing CRLF
                         body=b'16\r\n'
                         + b'Mary had a little pug \r\n'
                         + b'1b\r\n'
                         + b'Its name was "Skupper-Jack"\r\n'
                         + b'0\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_headers={'transfer-encoding': 'chunked'},
                               expect_body=b'Mary had a little pug Its name was "Skupper-Jack"')),

            (RequestMsg("GET", "/GET/chunked_large",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"transfer-encoding": "chunked",
                                  "Content-Type": "text/plain;charset=utf-8"},
                         # note: the chunk length does not count the trailing CRLF
                         body=b'1\r\n'
                         + b'?\r\n'
                         + b'800\r\n'
                         + b'X' * 0x800 + b'\r\n'
                         + b'13\r\n'
                         + b'Y' * 0x13  + b'\r\n'
                         + b'0\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_headers={'transfer-encoding': 'chunked'},
                               expect_body=b'?' + b'X' * 0x800 + b'Y' * 0x13)),

            (RequestMsg("GET", "/GET/info_content_len",
                        headers={"Content-Length": 0}),
             [ResponseMsg(100, reason="Continue",
                          headers={"Blab": 1, "Blob": "?"}),
              ResponseMsg(200, reason="OK",
                          headers={"Content-Length": 1,
                                   "Content-Type": "text/plain;charset=utf-8"},
                          body=b'?')],
             ResponseValidator(expect_headers={'Content-Type': "text/plain;charset=utf-8"},
                               expect_body=b'?')),

            # (RequestMsg("GET", "/GET/no_length",
            #             headers={"Content-Length": "0"}),
            #  ResponseMsg(200, reason="OK",
            #              headers={"Content-Type": "text/plain;charset=utf-8",
            #                       "connection": "close"
            #              },
            #              body=b'Hi! ' * 1024 + b'X'),
            #  ResponseValidator(expect_body=b'Hi! ' * 1024 + b'X')),
        ],
        #
        # HEAD
        #
        "HEAD": [
            (RequestMsg("HEAD", "/HEAD/test_01",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Content-Length": "10",
                                       "App-Header-2": "Value 02"},
                         body=None),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Content-Length": "10",
                                               "App-Header-2": "Value 02"})
             ),
            (RequestMsg("HEAD", "/HEAD/test_02",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Transfer-Encoding": "chunked",
                                       "App-Header-2": "Value 02"}),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Transfer-Encoding": "chunked",
                                               "App-Header-2": "Value 02"})),

            (RequestMsg("HEAD", "/HEAD/test_03",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-3": "Value 03"}),
             ResponseValidator(expect_headers={"App-Header-3": "Value 03"})),
        ],
        #
        # POST
        #
        "POST": [
            (RequestMsg("POST", "/POST/test_01",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "19",
                                 "Content-Type": "application/x-www-form-urlencoded"},
                        body=b'one=1&two=2&three=3'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Transfer-Encoding": "chunked"},
                         body=b'8\r\n'
                         + b'12345678\r\n'
                         + b'f\r\n'
                         + b'abcdefghijklmno\r\n'
                         + b'000\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_body=b'12345678abcdefghijklmno')
             ),
            (RequestMsg("POST", "/POST/test_02",
                        headers={"App-Header-1": "Value 01",
                                 "Transfer-Encoding": "chunked"},
                        body=b'01\r\n'
                        + b'!\r\n'
                        + b'0\r\n\r\n'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Content-Length": "9"},
                         body=b'Hi There!'),
             ResponseValidator(expect_body=b'Hi There!')
             ),
        ],
        #
        # PUT
        #
        "PUT": [
            (RequestMsg("PUT", "/PUT/test_01",
                        headers={"Put-Header-1": "Value 01",
                                 "Transfer-Encoding": "chunked",
                                 "Content-Type": "text/plain;charset=utf-8"},
                        body=b'80\r\n'
                        + b'$' * 0x80 + b'\r\n'
                        + b'0\r\n\r\n'),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Content-length": "3"},
                         body=b'ABC'),
             ResponseValidator(status=201, expect_body=b'ABC')
             ),

            (RequestMsg("PUT", "/PUT/test_02",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-length": "0",
                                 "Content-Type": "text/plain;charset=utf-8"}),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Transfer-Encoding": "chunked"},
                         body=b'1\r\n$\r\n0\r\n\r\n'),
             ResponseValidator(status=201, expect_body=b'$')
             ),
        ]
    }

    # HTTP/1.0 compliant test cases (no chunked, response length unspecified)
    TESTS_10 = {
        #
        # GET
        #
        "GET": [
            (RequestMsg("GET", "/GET/error",
                        headers={"Content-Length": 0}),
             ResponseMsg(400, reason="Bad breath", error=True),
             ResponseValidator(status=400)),

            (RequestMsg("GET", "/GET/no_content",
                        headers={"Content-Length": 0}),
             ResponseMsg(204, reason="No Content"),
             ResponseValidator(status=204)),

            (RequestMsg("GET", "/GET/content_len_511",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 511,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 511),
             ResponseValidator(expect_headers={'Content-Length': '511'},
                               expect_body=b'X' * 511)),

            (RequestMsg("GET", "/GET/content_len_4096",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 4096),
             ResponseValidator(expect_headers={"Content-Type": "text/plain;charset=utf-8"},
                               expect_body=b'X' * 4096)),

            (RequestMsg("GET", "/GET/info_content_len",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8"},
                         body=b'?'),
             ResponseValidator(expect_headers={'Content-Type': "text/plain;charset=utf-8"},
                               expect_body=b'?')),

            # test support for "folded headers"

            (RequestMsg("GET", "/GET/folded_header_01",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8",
                                  "Content-Length": 1,
                                  "folded-header": "One\r\n \r\n\tTwo"},
                         body=b'X'),
             ResponseValidator(expect_headers={"Content-Type":
                                               "text/plain;charset=utf-8",
                                               "folded-header":
                                               "One     \tTwo"},
                               expect_body=b'X')),

            (RequestMsg("GET", "/GET/folded_header_02",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8",
                                  "Content-Length": 1,
                                  "folded-header": "\r\n \r\n\tTwo",
                                  "another-header": "three"},
                         body=b'X'),
             ResponseValidator(expect_headers={"Content-Type":
                                               "text/plain;charset=utf-8",
                                               # trim leading and
                                               # trailing ws:
                                               "folded-header":
                                               "Two",
                                               "another-header":
                                               "three"},
                               expect_body=b'X')),
        ],
        #
        # HEAD
        #
        "HEAD": [
            (RequestMsg("HEAD", "/HEAD/test_01",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Content-Length": "10",
                                       "App-Header-2": "Value 02"},
                         body=None),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Content-Length": "10",
                                               "App-Header-2": "Value 02"})
             ),

            (RequestMsg("HEAD", "/HEAD/test_03",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-3": "Value 03"}),
             ResponseValidator(expect_headers={"App-Header-3": "Value 03"})),
        ],
        #
        # POST
        #
        "POST": [
            (RequestMsg("POST", "/POST/test_01",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "19",
                                 "Content-Type": "application/x-www-form-urlencoded"},
                        body=b'one=1&two=2&three=3'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever"},
                         body=b'12345678abcdefghijklmno'),
             ResponseValidator(expect_body=b'12345678abcdefghijklmno')
             ),
            (RequestMsg("POST", "/POST/test_02",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "5"},
                        body=b'01234'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Content-Length": "9"},
                         body=b'Hi There!'),
             ResponseValidator(expect_body=b'Hi There!')
             ),
        ],
        #
        # PUT
        #
        "PUT": [
            (RequestMsg("PUT", "/PUT/test_01",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-Length": "513",
                                 "Content-Type": "text/plain;charset=utf-8"},
                        body=b'$' * 513),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Content-length": "3"},
                         body=b'ABC'),
             ResponseValidator(status=201, expect_body=b'ABC')
             ),

            (RequestMsg("PUT", "/PUT/test_02",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-length": "0",
                                 "Content-Type": "text/plain;charset=utf-8"}),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever"},
                         body=b'No Content Length'),
             ResponseValidator(status=201, expect_body=b'No Content Length')
             ),
        ]
    }

    @classmethod
    def create_client(cls, host_port, timeout=TIMEOUT):
        """Creates basic HTTPConnection. May be overridden for SSL support"""
        return HTTPConnection(host=host_port, timeout=timeout)

    def _do_request(self, client, tests):
        for req, _, val in tests:
            req.send_request(client)
            rsp = client.getresponse()
            try:
                body = val.check_response(rsp)
            except Exception as exc:
                self.fail("request failed:  %s" % str(exc))

            if req.method == "BODY":
                self.assertEqual(b'', body)

    def test_001_get(self):
        client = self.create_client(self.listener11_host_port)
        self._do_request(client, self.TESTS_11["GET"])
        client.close()

    def test_002_head(self):
        client = self.create_client(self.listener11_host_port)
        self._do_request(client, self.TESTS_11["HEAD"])
        client.close()

    def test_003_post(self):
        client = self.create_client(self.listener11_host_port)
        self._do_request(client, self.TESTS_11["POST"])
        client.close()

    def test_004_put(self):
        client = self.create_client(self.listener11_host_port)
        self._do_request(client, self.TESTS_11["PUT"])
        client.close()

    def test_006_head_10(self):
        client = self.create_client(self.listener10_host_port)
        self._do_request(client, self.TESTS_10["HEAD"])
        client.close()

    def test_007_post_10(self):
        client = self.create_client(self.listener10_host_port)
        self._do_request(client, self.TESTS_10["POST"])
        client.close()

    def test_008_put_10(self):
        client = self.create_client(self.listener10_host_port)
        self._do_request(client, self.TESTS_10["PUT"])
        client.close()


class Http1OneRouterTestBase(TestCase):
    # HTTP/1.1 One Router test setup

    @classmethod
    def router(cls, name, mode, extra):
        config = [
            ('router', {'mode': mode, 'id': name}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address',
             {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        if extra:
            config.extend(extra)
        config = Qdrouterd.Config(config)
        cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))
        return cls.routers[-1]

    @classmethod
    def setUpClass(cls):
        """Configure server and listener addresses and ports"""
        super(Http1OneRouterTestBase, cls).setUpClass()

        cls.routers = []

        cls.server11_host = 'localhost'
        cls.server11_port = cls.tester.get_port()
        cls.server11_host_port = f"localhost:{cls.server11_port}"

        cls.listener11_host = 'localhost'
        cls.listener11_port = cls.tester.get_port()
        cls.listener11_host_port = f"localhost:{cls.listener11_port}"

        cls.server10_host = 'localhost'
        cls.server10_port = cls.tester.get_port()
        cls.server10_host_port = f"localhost:{cls.server10_port}"

        cls.listener10_host = 'localhost'
        cls.listener10_port = cls.tester.get_port()
        cls.listener10_host_port = f"localhost:{cls.listener10_port}"


class Http1Edge2EdgeTestBase(TestCase):
    """
    Common base class for Edge2Edge tests. Provides common utility functions
    and host/port configuration.
    """
    @classmethod
    def router(cls, name, mode, extra, wait=True):
        config = [
            ('router', {'mode': mode, 'id': name}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        if extra:
            config.extend(extra)
        config = Qdrouterd.Config(config)
        cls.routers.append(cls.tester.qdrouterd(name, config, wait=wait))
        return cls.routers[-1]

    @classmethod
    def create_threaded_client(cls, *args, **kwargs):
        """
        Wrapper for creating ThreadedTestClient instances. May be overridden
        for SSL support
        """
        return ThreadedTestClient(*args, **kwargs)

    @classmethod
    def create_server(cls, *args, **kwargs):
        """
        Wrapper for creating TestServer instances. May be overridden for SSL
        support
        """
        return TestServer.new_server(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        """Configure server and listener addresses and ports"""
        super(Http1Edge2EdgeTestBase, cls).setUpClass()
        cls.skip = {}
        cls.routers = []
        cls.INTA_edge1_port   = cls.tester.get_port()
        cls.INTA_edge2_port   = cls.tester.get_port()

        cls.server11_host = 'localhost'
        cls.server11_port = cls.tester.get_port()
        cls.server11_host_port = f"localhost:{cls.server11_port}"

        cls.listener11_host = 'localhost'
        cls.listener11_port = cls.tester.get_port()
        cls.listener11_host_port = f"localhost:{cls.listener11_port}"

        cls.server10_host = 'localhost'
        cls.server10_port = cls.tester.get_port()
        cls.server10_host_port = f"localhost:{cls.server10_port}"

        cls.listener10_host = 'localhost'
        cls.listener10_port = cls.tester.get_port()
        cls.listener10_host_port = f"localhost:{cls.listener10_port}"


class Http1ClientCloseTestsMixIn:
    """
    Generic test functions for simulating HTTP/1.x client connection drops.
    """
    def client_request_close_test(self, server_port, client_port, server_mgmt):
        """
        Simulate an HTTP client drop while sending a very large PUT request
        """
        PING = {
            "GET": [
                (RequestMsg("GET", "/GET/test_04_client_request_close/ping",
                            headers={"Content-Length": "0"}),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "Content-Length": "19",
                                 "Content-Type": "text/plain;charset=utf-8",
                             },
                             body=b'END OF TRANSMISSION'),
                 ResponseValidator(status=200)
                 )]
        }

        TESTS = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/test_04_client_request_close",
                            headers={
                                "Content-Length": "500000",
                                "Content-Type": "text/plain;charset=utf-8"
                            },
                            body=b'4' * (500000 - 19) + b'END OF TRANSMISSION'),
                 ResponseMsg(201, reason="Created",
                             headers={"Test-Header": "/PUT/test_04_client_request_close",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201)
                 )]
        }
        TESTS.update(PING)

        with TestServer.new_server(server_port=server_port,
                                   client_port=client_port,
                                   tests=TESTS) as server:
            #
            # ensure the server has fully connected
            #
            client = ThreadedTestClient(PING, client_port)
            client.wait()

            #
            # Simulate an HTTP client that dies during the sending of the PUT
            # request
            #

            fake_request = b'PUT /PUT/test_04_client_request_close HTTP/1.1\r\n' \
                + b'Content-Length: 500000\r\n' \
                + b'Content-Type: text/plain;charset=utf-8\r\n' \
                + b'\r\n' \
                + b'?' * 50000
            fake_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            fake_client.settimeout(TIMEOUT)
            fake_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            fake_client.connect(("127.0.0.1", client_port))
            fake_client.sendall(fake_request, socket.MSG_WAITALL)

            # Wait until the test server receives the partial request
            # by checking the rx_count field in the RequestMsg for the
            # test PUT.  Then simulate a loss of client by closing the
            # socket.

            while TESTS["PUT"][0][0].rx_count == 0:
                sleep(1.0)

            fake_client.close()

            # now ensure the connection between the router and the HTTP server
            # still functions:
            client = ThreadedTestClient(PING, client_port)
            client.wait()

    def client_response_close_test(self, server_port, client_port):
        """
        Simulate an HTTP client drop while the server is sending a very large
        response message.
        """
        PING = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/test_05_client_response_close/ping",
                            headers={"Content-Length": "1",
                                     "content-type":
                                     "text/plain;charset=utf-8"},
                            body=b'X'),
                 ResponseMsg(201, reason="Created",
                             headers={"Content-Length": "0"}),
                 ResponseValidator(status=201)
                 )]
        }

        big_headers = dict([('Huge%s' % i, chr(ord(b'0') + i) * 8000)
                            for i in range(10)])

        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_05_client_response_close",
                            headers={
                                "Content-Length": "0",
                                "Content-Type": "text/plain;charset=utf-8"
                            }),
                 [ResponseMsg(100, reason="Continue", headers=big_headers),
                  ResponseMsg(100, reason="Continue", headers=big_headers),
                  ResponseMsg(100, reason="Continue", headers=big_headers),
                  ResponseMsg(100, reason="Continue", headers=big_headers),
                  ResponseMsg(200,
                              reason="OK",
                              headers={"Content-Length": 1000000,
                                       "Content-Type": "text/plain;charset=utf-8"},
                              body=b'?' * 1000000)],
                 ResponseValidator(status=200)
                 )]
        }
        TESTS.update(PING)

        with TestServer.new_server(server_port=server_port,
                                   client_port=client_port,
                                   tests=TESTS) as server:
            #
            # ensure the server has fully connected
            #
            client = ThreadedTestClient(PING, client_port)
            client.wait()

            #
            # Simulate an HTTP client that dies during the receipt of the
            # response
            #

            fake_request = b'GET /GET/test_05_client_response_close HTTP/1.1\r\n' \
                + b'Content-Length: 0\r\n' \
                + b'Content-Type: text/plain;charset=utf-8\r\n' \
                + b'\r\n'
            fake_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            fake_client.settimeout(TIMEOUT)
            fake_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            fake_client.connect(("127.0.0.1", client_port))
            fake_client.sendall(fake_request, socket.MSG_WAITALL)
            fake_client.recv(1)
            fake_client.close()

            #
            # Verify the server is still reachable
            #
            client = ThreadedTestClient(PING, client_port)
            client.wait()


class Http1CurlTestsMixIn:
    """
    Test cases using curl as the command line client
    """

    @unittest.skipIf(not _curl_ok(),
                     "required curl version >= %s" % str(CURL_VERSION))
    def curl_get_test(self, host, port, server_port):
        """
        Use curl to get a resource
        """
        CURL_TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/curl_get"),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "Content-Length": "19",
                                 "Content-Type": "text/plain;charset=utf-8",
                                 "Test-Header": "/GET/curl_get"
                             },
                             body=b'END OF TRANSMISSION'),
                 ResponseValidator())
            ],

            "HEAD": [
                (RequestMsg("HEAD", "/HEAD/curl_head",
                            headers={"Content-Length": "0"}),
                 ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                           "Content-Length": "10",
                                           "App-Header-2": "Value 02"},
                             body=None),
                 ResponseValidator())
            ]
        }

        with TestServer.new_server(server_port, port, CURL_TESTS) as server:

            get_url = "http://%s:%s/GET/curl_get" % (host, port)
            head_url = "http://%s:%s/HEAD/curl_head" % (host, port)

            status, out, err = run_curl(["--http1.1", "-G", get_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("END OF TRANSMISSION", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.1", "-I", head_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("App-Header-2", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.0", "-G", get_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("END OF TRANSMISSION", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.1", "-G", get_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("END OF TRANSMISSION", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

    @unittest.skipIf(not _curl_ok(),
                     "required curl version >= %s" % str(CURL_VERSION))
    def curl_put_test(self, host, port, server_port):
        """
        Use curl to PUT a resource
        """

        CURL_TESTS = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/curl_put"),
                 ResponseMsg(201, reason="Created",
                             headers={
                                 "Test-Header": "/PUT/curl_put",
                                 "content-length": "0"
                             }),
                 ResponseValidator())
            ],

            "HEAD": [
                (RequestMsg("HEAD", "/HEAD/curl_head",
                            headers={"Content-Length": "0"}),
                 ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                           "Content-Length": "10",
                                           "App-Header-2": "Value 02"},
                             body=None),
                 ResponseValidator())
            ]
        }

        with TestServer.new_server(server_port, port, CURL_TESTS) as server:

            put_url = "http://%s:%s/PUT/curl_put" % (host, port)
            head_url = "http://%s:%s/HEAD/curl_head" % (host, port)

            status, out, err = run_curl(["--http1.1", "-T", ".", put_url],
                                        input="Mary had a little pug."
                                        "\nIts fleece was brown as dirt."
                                        "\nIts color made Mary shrug."
                                        "\nShe should dress it in a shirt.")
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))

            status, out, err = run_curl(["--http1.1", "-I", head_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("App-Header-2", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.1", "-T", ".", put_url],
                                        input="Ph'nglui mglw'nafh Cthulhu"
                                        "\nR'lyeh wgah'nagl fhtagn")
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))

    @unittest.skipIf(not _curl_ok(),
                     "required curl version >= %s" % str(CURL_VERSION))
    def curl_post_test(self, host, port, server_port):
        """
        Use curl to post to a resource
        """

        CURL_TESTS = {
            "POST": [
                (RequestMsg("POST", "/POST/curl_post"),
                 ResponseMsg(201, reason="Created",
                             headers={
                                 "Test-Header": "/POST/curl_put",
                                 "content-length": "19",
                                 "Content-Type": "text/plain;charset=utf-8",
                             },
                             body=b'END OF TRANSMISSION'),
                 ResponseValidator())
            ],

            "GET": [
                (RequestMsg("GET", "/GET/curl_get",
                            headers={"Content-Length": "0"}),
                 ResponseMsg(200, reason="OK",
                             headers={"App-Header-1": "Value 01",
                                      "Content-Length": "10",
                                      "App-Header-2": "Value 02"},
                             body=b'0123456789'),
                 ResponseValidator())
            ]
        }

        with TestServer.new_server(server_port, port, CURL_TESTS) as server:

            post_url = "http://%s:%s/POST/curl_post" % (host, port)
            get_url = "http://%s:%s/GET/curl_get" % (host, port)

            status, out, err = run_curl(["--http1.1", "-F", "name=Skupper",
                                         "-F", "breed=Pug", post_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("END OF TRANSMISSION", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.1", "-G", get_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("0123456789", out, "Unexpected out=%s (err=%s)"
                          % (out, err))

            status, out, err = run_curl(["--http1.1", "-F", "name=Coco",
                                         "-F", "breed=French Bulldog",
                                         post_url])
            self.assertEqual(0, status, "curl error '%s' '%s'" % (out, err))
            self.assertIn("END OF TRANSMISSION", out, "Unexpected out=%s (err=%s)"
                          % (out, err))


class HttpAdaptorListenerConnectTestBase(TestCase):
    """
    Test client connecting to adaptor listeners in various scenarios
    """
    LISTENER_TYPE = 'io.skupper.router.httpListener'
    CONNECTOR_TYPE = 'io.skupper.router.httpConnector'
    PROTOCOL_VERSION = "HTTP1"

    @classmethod
    def setUpClass(cls):
        super(HttpAdaptorListenerConnectTestBase, cls).setUpClass()

        cls.test_name = 'HTTPListenConnTest'

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

    def start_server(self, connector_port):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(TIMEOUT)
        server.bind(("", connector_port))
        server.listen(1)
        return server

    def client_connect(self, listener_port):
        """
        Returns True if connection succeeds, else raises an error
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_conn:
            client_conn.setblocking(True)
            client_conn.settimeout(TIMEOUT)
            client_conn.connect(('127.0.0.1', listener_port))
            return True

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
        c_mgmt = c_router.management

        # instantiate a listener

        attributes = {'address': van_address,
                      'port': listener_port,
                      'protocolVersion': self.PROTOCOL_VERSION}
        l_mgmt.create(type=self.LISTENER_TYPE,
                      name=listener_name,
                      attributes=attributes)

        # since there is no connector present, the operational state must be
        # down and connection attempts must be refused

        listener = l_mgmt.read(type=self.LISTENER_TYPE, name=listener_name)
        self.assertEqual('down', listener['operStatus'])

        self.assertRaises(ConnectionRefusedError, self.client_connect, listener_port)

        # create a connector and a server for the connector to connect to

        with self.start_server(connector_port) as server:

            attributes = {'address': van_address,
                          'host': '127.0.0.1',
                          'port': connector_port,
                          'protocolVersion': self.PROTOCOL_VERSION}
            c_mgmt.create(type=self.CONNECTOR_TYPE,
                          name=connector_name,
                          attributes=attributes)

            # let the connectors address propagate to listener router

            if l_router.config.router_mode == 'interior':
                l_router.wait_address(van_address, remotes=1)
            else:
                l_router.wait_address(van_address, subscribers=1)

            # expect the listener socket to come up

            self.assertTrue(retry(lambda: l_mgmt.read(type=self.LISTENER_TYPE,
                                                      name=listener_name)['operStatus'] == 'up'))

            # ensure clients can connect successfully. There may be a delay
            # between the operStatus going up and the actual listener socket
            # availability, so tolerate ConnectionRefusedErrors until the
            # connection succeeds

            retry_exception(lambda: self.client_connect(listener_port),
                            exception=ConnectionRefusedError)

        # Teardown the connector, expect listener admin state to go down

        c_mgmt.delete(type=self.CONNECTOR_TYPE, name=connector_name)
        l_router.wait_address_unsubscribed(van_address)

        self.assertTrue(retry(lambda: l_mgmt.read(type=self.LISTENER_TYPE,
                                                  name=listener_name)['operStatus']
                              == 'down'))

        # attempt reconnecting until ConnectionRefused occurs
        def _func():
            return self.client_connect(listener_port) != True
        self.assertRaises(ConnectionRefusedError, retry, _func)

        l_mgmt.delete(type=self.LISTENER_TYPE, name=listener_name)


class HttpTlsBadConfigTestsBase(TestCase):
    """
    Negative test for invalid HTTP connector and listener configurations
    """

    # TestCase subclasses must provide the correct value for PROTOCOL_VERSION
    PROTOCOL_VERSION = None

    @classmethod
    def setUpClass(cls):
        super(HttpTlsBadConfigTestsBase, cls).setUpClass()
        config = [
            ('router', {'mode': 'interior',
                        'id': 'BadConfigRouter'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        cls.router = cls.tester.qdrouterd('BadConfigRouter',
                                          Qdrouterd.Config(config), wait=True)

    def _test_connector_mgmt_missing_ssl_profile(self):
        """Attempt to create a connector with a bad sslProfile"""
        assert self.PROTOCOL_VERSION is not None
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        self.assertRaises(Exception, mgmt.create, "httpConnector",
                          {'address': 'foo',
                           'host': '127.0.0.1',
                           'port': port,
                           'protocolVersion': self.PROTOCOL_VERSION,
                           'sslProfile': "NotFound"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid httpConnector configuration", mgmt.stdout)

    def _test_connector_mgmt_missing_ca_file(self):
        """Attempt to create a connector with an invalid CA file"""
        assert self.PROTOCOL_VERSION is not None
        port = self.tester.get_port()
        mgmt = self.router.qd_manager

        mgmt.create("sslProfile",
                    {'name': 'BadCAFile',
                     'caCertFile': '/bad/path/CA.pem'})
        self.assertRaises(Exception, mgmt.create, "httpConnector",
                          {'address': 'foo',
                           'host': '127.0.0.1',
                           'port': port,
                           'protocolVersion': self.PROTOCOL_VERSION,
                           'sslProfile': "BadCAFile"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid httpConnector configuration", mgmt.stdout)
        mgmt.delete("sslProfile", name='BadCAFile')

    def _test_listener_mgmt_missing_ssl_profile(self):
        """Attempt to create a listener with a bad sslProfile"""
        assert self.PROTOCOL_VERSION is not None
        port = self.tester.get_port()
        mgmt = self.router.qd_manager
        self.assertRaises(Exception, mgmt.create, "httpListener",
                          {'address': 'foo',
                           'host': '0.0.0.0',
                           'port': port,
                           'protocolVersion': self.PROTOCOL_VERSION,
                           'sslProfile': "NotFound"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid httpListener configuration", mgmt.stdout)

    def _test_listener_mgmt_missing_ca_file(self):
        """Attempt to create a listener with an invalid CA file"""
        assert self.PROTOCOL_VERSION is not None
        port = self.tester.get_port()
        mgmt = self.router.qd_manager

        mgmt.create("sslProfile",
                    {'name': 'BadCAFile',
                     'caCertFile': '/bad/path/CA.pem'})
        self.assertRaises(Exception, mgmt.create, "httpListener",
                          {'address': 'foo',
                           'host': '0.0.0.0',
                           'port': port,
                           'protocolVersion': self.PROTOCOL_VERSION,
                           'sslProfile': "BadCAFile"})
        self.assertEqual(1, mgmt.returncode, "Unexpected returncode from skmanage")
        self.assertIn("Invalid httpListener configuration", mgmt.stdout)
        mgmt.delete("sslProfile", name='BadCAFile')

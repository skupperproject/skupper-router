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
# Test the HTTP/1.x Protocol Decoder
#

import socket
import subprocess
from time import sleep

from system_test import TestCase, unittest, main_module
from system_test import Process, TIMEOUT, retry_exception


# commands for the log file parser
#
MATCH = 1
CLIENT_BODY = 2
SERVER_BODY = 3


def _drain_socket(sock):
    """
    Read data from socket until the socket is closed
    """
    data = b''
    while True:
        chunk = sock.recv(4096)
        if not chunk:  # socket closed
            break
        data += chunk
    return data


class Http1TestRelay(Process):
    """
    Run the HTTP/1.x test relay program (http1-relay) as a subprocess
    """
    def __init__(self, listener_address, server_address, name="Http1TestRelay",
                 **kwargs):
        self.listener_address = listener_address
        self.server_address = server_address

        kwargs.setdefault('stdin', subprocess.DEVNULL)  # else file descriptor leak
        kwargs.setdefault('bufsize', 1)  # line-buffer output
        kwargs.setdefault('universal_newlines', True)  # text output
        args = ['http1-relay',
                '-l', f"{listener_address[0]}:{listener_address[1]}",
                '-s', f"{server_address[0]}:{server_address[1]}",
                # '-v'
                ]
        super(Http1TestRelay, self).__init__(args, name=name,
                                             expect=Process.EXIT_OK, **kwargs)

    def read_log(self):
        log = []
        with open(self.outfile_path, 'r') as out:
            line = out.readline()
            while line:
                log.append(line)
                line = out.readline()
        return log

    def shutdown(self):
        if self.poll() is None:
            self.terminate()
            self.wait(TIMEOUT)


class Http1DecoderTest(TestCase):
    """
    Simulate various client and server flows across the HTTP/1.x test proxy.
    Verify expected behavior of the decoder
    """
    @classmethod
    def setUpClass(cls):
        super(Http1DecoderTest, cls).setUpClass()

    def _spawn_relay(self, name, listener_address, server_address):
        return Http1TestRelay(listener_address=listener_address, server_address=server_address, name=name)

    def _run_io(self, proxy, client_stream, server_stream):
        """
        Pass client_stream and server_stream through the proxy.
        Validate that the inputs are passed through completely.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.settimeout(TIMEOUT)
            listener.bind(proxy.server_address)
            listener.listen(10)

            sleep(0.1)  # paranoia: ensure listener is active

            # attach a client to the proxy

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(TIMEOUT)
                retry_exception(lambda cs=client:
                                cs.connect(proxy.listener_address),
                                delay=0.25,
                                exception=(ConnectionRefusedError,
                                           ConnectionAbortedError))

                # wait for the server connection from the proxy then push one
                # byte of data at a time to simulate worst-case tcp stream
                # fragmentation

                server, _ = listener.accept()
                try:
                    while client_stream:
                        client.sendall(client_stream[:1])
                        client_stream = client_stream[1:]
                    client.shutdown(socket.SHUT_WR)

                    server_output = _drain_socket(server)

                    while server_stream:
                        server.sendall(server_stream[:1])
                        server_stream = server_stream[1:]
                    server.shutdown(socket.SHUT_WR)

                    client_output = _drain_socket(client)
                    return (client_output, server_output)

                finally:
                    server.close()

    def _test_runner(self, name, client_stream, server_stream):
        """
        Simple test runner that passes client and server stream through the
        HTTP/1.x decoder proxy. Returns the log output generated by the proxy.
        """
        listener_address = ("127.0.0.1", self.get_port())
        server_address = ("127.0.0.1", self.get_port())

        proxy = self._spawn_relay(name, listener_address, server_address)
        try:
            cout, sout = self._run_io(proxy, client_stream, server_stream)
        finally:
            proxy.shutdown()

        return proxy.read_log()

    def _compare_log(self, expected, log):
        """
        Compare the output log of the relay against the expected output.
        Do some special hacky fu to verify the body length count
        """
        try:
            index = 0
            for opcode in expected:
                if opcode[0] == MATCH:
                    # opcode = (MATCH, "<text to match log entry>")
                    while log[index].split()[0] == 'DEBUG:':
                        index += 1
                    self.assertEqual(opcode[1], log[index])
                    index += 1
                elif opcode[0] in [CLIENT_BODY, SERVER_BODY]:
                    # opcode = (SERVER/CLIENT_BODY, "<stream-id>", <total-length>"
                    # log = "[Cx:Rx] SERVER/CLIENT_BODY LENGTH=<length>\n"
                    stream_id = opcode[1]
                    body_length = opcode[2]
                    while True:
                        token = log[index].split()
                        index += 1
                        if token[0] != 'DEBUG:':
                            self.assertEqual(stream_id, token[0], "stream id mismatch")
                            if opcode[0] == CLIENT_BODY:
                                self.assertEqual(token[1], 'CLIENT-BODY', "expected body identifier")
                            else:
                                self.assertEqual(token[1], 'SERVER-BODY', "expected body identifier")
                            chunk_len = int(token[2].split("=")[1])
                            self.assertLessEqual(chunk_len, body_length, "body overflow")
                            body_length -= chunk_len
                            if body_length <= 0:
                                break
                    self.assertEqual(0, body_length, f"Expected body_length={opcode[2]}")
        except IndexError:
            self.fail(f"log failed to match (too short)\n{log}")

    def test_01_simple_get(self):
        """
        Simple GET request with zero content reply
        """
        client_stream = b'GET / HTTP/1.1\r\n\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n'
        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/ VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n')
        ]
        log = self._test_runner("test_01", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_02_get_content_len_pipeline(self):
        """
        Pipelined GET requests with content-length replies
        """
        client_stream = b'GET /one HTTP/1.1\r\n\r\n' \
            + b'GET /two HTTP/1.1\r\n\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\ncontent-length: 51\r\n' \
            + b'\r\n' + b'2' * 51 \
            + b'HTTP/1.1 200 OK\r\ncontent-length: 1\r\n' \
            + b'\r\n' + b'3'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=GET TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=51\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 51),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=1\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 1),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]

        log = self._test_runner("test_02", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_03_post_content_len_pipeline(self):
        """
        Pipelined POST requests with content-length request and replies
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'CoNteNT-lenGth:  12\r\n' \
            + b'Hi: There\r\n' \
            + b'\r\n' \
            + b'ABCDEFGHIJKL' \
            + b'POST /two HTTP/1.1\r\n' \
            + b'content-length: +4\r\n' \
            + b'\r\n' \
            + b'1234'
        server_stream = b'HTTP/1.0 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 1\r\n' \
            + b'\r\n' \
            + b'X'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=CoNteNT-lenGth VALUE=12\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=Hi VALUE=There\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R1]', 12),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=POST TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=+4\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=0 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=1\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 1),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]

        log = self._test_runner("test_03", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_04_post_content_len_big_msg(self):
        """
        Pipelined POST requests with multi-buffer request and reply bodies
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'CoNteNT-lenGth:  32777\r\n' \
            + b'Hi: There\r\n' \
            + b'\r\n' \
            + b'?' * 32777 \
            + b'POST /two HTTP/1.1\r\n' \
            + b'content-length: +4\r\n' \
            + b'\r\n' \
            + b'1234'
        server_stream = b'HTTP/1.0 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 52831\r\n' \
            + b'\r\n' \
            + b'!' * 52831 \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 1\r\n' \
            + b'\r\n' \
            + b'X'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=CoNteNT-lenGth VALUE=32777\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=Hi VALUE=There\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R1]', 32777),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=POST TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=+4\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=0 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=52831\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 52831),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=1\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 1),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n'),
        ]

        log = self._test_runner("test_04", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_05_chunked_get(self):
        """
        GET with chunked body response
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'CoNteNT-lenGth:  0 \r\n' \
            + b'\r\n' \
            + b'POST /two HTTP/1.1\r\n' \
            + b'content-length: +4\r\n' \
            + b'\r\n' \
            + b'1234'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'TransfeR-enCoding: biz,baz,chunked\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'0A\r\n' \
            + b'1234567890\r\n' \
            + b'1\r\n' \
            + b'1\r\n' \
            + b'00\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 1\r\n' \
            + b'\r\n' \
            + b'X'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=CoNteNT-lenGth VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=POST TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=+4\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=TransfeR-enCoding VALUE=biz,baz,chunked\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 10),
            (SERVER_BODY, '[C1:R1]', 1),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=1\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 1),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]

        log = self._test_runner("test_05", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_06_chunked_extensions(self):
        """
        GET with chunked body response with chunk extensions
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'CoNteNT-lenGth:  0 \r\n' \
            + b'\r\n' \
            + b'POST /two HTTP/1.1\r\n' \
            + b'content-length: +4\r\n' \
            + b'\r\n' \
            + b'1234'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'TransfeR-enCoding: biz ,, baz, chunked,\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'0A;ex1=hi\r\n' \
            + b'1234567890\r\n' \
            + b'1 ; ex2 = bad\r\n' \
            + b'1\r\n' \
            + b'FF ; ex3 = bad\r\n' \
            + b'F' * 0xFF \
            + b'\r\n' \
            + b'00;boo= bar; zim=zam\r\n' \
            + b'trailer=1\r\n' \
            + b't=trailer,two\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'content-length: 1\r\n' \
            + b'\r\n' \
            + b'X'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=CoNteNT-lenGth VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=POST TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=+4\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (CLIENT_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=TransfeR-enCoding VALUE=biz ,, baz, chunked,\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 10),
            (SERVER_BODY, '[C1:R1]', 1),
            (SERVER_BODY, '[C1:R1]', 255),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=1\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 1),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n'),
        ]

        log = self._test_runner("test_06", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_07_http1_0(self):
        """
        HTTP/1.0 implied length
        """
        client_stream = b'GET /one HTTP/1.0\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.0 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'CONTENT'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),
            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=0 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 7)
            # note: connection drop indicates end of message (no output logged)
        ]

        log = self._test_runner("test_07", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_08_invalid_server_version(self):
        """
        Unknown server version
        """
        client_stream = b'GET /one HTTP/1.0\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/banana 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'CONTENT'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Unsupported HTTP/1.x version\n')
        ]

        log = self._test_runner("test_08", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_09_invalid_request_version(self):
        """
        Unknown request version
        """
        client_stream = b'GET /one HTTP/0.9\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n'

        expected = [
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Unsupported HTTP/1.x version\n'),
        ]
        log = self._test_runner("test_09", client_stream, server_stream)
        #self._compare_log(expected, log)

    def test_10_invalid_request_line(self):
        """
        Invalid request line (illegal chars)
        """
        client_stream = b'GET\x80 /one HTTP/1.0\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.0 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n'

        expected = [
            (MATCH, '[C1] PROTOCOL-ERROR REASON=protocol error: non USASCII data\n'),
        ]
        log = self._test_runner("test_10", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_11_invalid_request_line(self):
        """
        Invalid request line (wrong format)
        """
        client_stream = b'GET/oneHTTP/1.0\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.0 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n'

        expected = [
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Malformed request line\n'),
        ]
        log = self._test_runner("test_11", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_12_invalid_response_line(self):
        """
        Invalid response line (illegal chars)
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n' \
            + b'GET /two HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 \x00200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 0\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=GET TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1] PROTOCOL-ERROR REASON=protocol error: non USASCII data\n')
        ]
        log = self._test_runner("test_12", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_13_HEAD_request(self):
        """
        No body allowed
        """
        client_stream = b'HEAD /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n' \
            + b'GET /two HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 900\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=HEAD TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=GET TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=900\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]
        log = self._test_runner("test_13", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_14_continue_response(self):
        """
        Handle continue response
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n' \
            + b'GET /two HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 100 Continue\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'4321' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=GET TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=100 VMAJOR=1 VMINOR=1 REASON=Continue\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 4),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]
        log = self._test_runner("test_14", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_15_no_content_response(self):
        """
        Handle No Content response
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n' \
            + b'GET /two HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 204 No Content\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R2] RX-REQ METHOD=GET TARGET=/two VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R2] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R2] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=204 VMAJOR=1 VMINOR=1 REASON=No Content\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n'),

            (MATCH, '[C1:R2] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R2] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R2] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R2]', 4),
            (MATCH, '[C1:R2] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R2] TRANSACTION-COMPLETE\n')
        ]
        log = self._test_runner("test_15", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_16_header_line_length_error(self):
        """
        Handle header lines that exceed our internal maximum (64k)
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'header: bar\r\n' \
            + b'toobig: ' + b'X' * 128000 \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=protocol error: received line too long\n')
        ]
        log = self._test_runner("test_16", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_17_xfer_encoding_unchunked(self):
        """
        Handle a server-side transfer encoding that does not use chunked.
        Decoder should allow assuming response ends on connection close.
        """
        client_stream = b'GET /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'transfer-encoding: yeah,right\r\n' \
            + b'header: bar\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=GET TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=transfer-encoding VALUE=yeah,right\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=bar\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 4)
        ]
        log = self._test_runner("test_17", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_18_invalid_xfer_encoding(self):
        """
        Detect client using invalid transfer-encoding
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'transfer-encoding: biff,bang,bong\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=transfer-encoding VALUE=biff,bang,bong\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Unrecognized Transfer-Encoding\n')
        ]
        log = self._test_runner("test_18", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_19_invalid_response_code(self):
        """
        Detect server invalid response code
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 9999999999 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1] PROTOCOL-ERROR REASON=Bad response code\n')
        ]
        log = self._test_runner("test_19", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_20_missing_response_code(self):
        """
        Detect server missing response code
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 \r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1] PROTOCOL-ERROR REASON=Malformed response line\n')
        ]
        log = self._test_runner("test_20", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_21_invalid_content_len(self):
        """
        Detect invalid content-length header
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: BAGELS!\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=BAGELS!\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Malformed Content-Length value\n')
        ]
        log = self._test_runner("test_21", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_22_content_smuggling(self):
        """
        Detect content smuggling attach via duplicate length headers
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'content-length: 13\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=13\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Invalid duplicate Content-Length header\n')
        ]
        log = self._test_runner("test_22", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_23_invalid_header_format(self):
        """
        Ignore invalid HTTP headers
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'content-length: 4\r\n' \
            + b'bogus-header:\r\n' \
            + b':this is an invalid header field\r\n' \
            + b'\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=content-length VALUE=4\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 4),
            (MATCH, '[C1:R1] SERVER-MSG-DONE\n'),
            (MATCH, '[C1:R1] TRANSACTION-COMPLETE\n')
        ]
        log = self._test_runner("test_23", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_24_invalid_chunk_len(self):
        """
        Detect invalid chunk length encoding
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'transfer-encoding: chunked\r\n' \
            + b'\r\n' \
            + b'WOOF\r\n' \
            + b'1234'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=transfer-encoding VALUE=chunked\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Invalid chunk length\n')
        ]
        log = self._test_runner("test_24", client_stream, server_stream)
        self._compare_log(expected, log)

    def test_25_invalid_end_chunk(self):
        """
        Detect invalid terminal chunk
        """
        client_stream = b'POST /one HTTP/1.1\r\n' \
            + b'IAmAHeader: Header value\r\n' \
            + b'content-length: 0\r\n' \
            + b'\r\n'
        server_stream = b'HTTP/1.1 200 OK\r\n' \
            + b'header: foo\r\n' \
            + b'transfer-encoding: chunked\r\n' \
            + b'\r\n' \
            + b'4\r\n' \
            + b'1234X\r\n'

        expected = [
            (MATCH, '[C1:R1] RX-REQ METHOD=POST TARGET=/one VMAJOR=1 VMINOR=1\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=IAmAHeader VALUE=Header value\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER KEY=content-length VALUE=0\n'),
            (MATCH, '[C1:R1] CLIENT-HEADER-DONE\n'),
            (MATCH, '[C1:R1] CLIENT-MSG-DONE\n'),

            (MATCH, '[C1:R1] RX-RESP STATUS=200 VMAJOR=1 VMINOR=1 REASON=OK\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=header VALUE=foo\n'),
            (MATCH, '[C1:R1] SERVER-HEADER KEY=transfer-encoding VALUE=chunked\n'),
            (MATCH, '[C1:R1] SERVER-HEADER-DONE\n'),
            (SERVER_BODY, '[C1:R1]', 4),
            (MATCH, '[C1] PROTOCOL-ERROR REASON=Unexpected chunk body end\n')
        ]
        log = self._test_runner("test_25", client_stream, server_stream)
        self._compare_log(expected, log)


if __name__ == '__main__':
    unittest.main(main_module())

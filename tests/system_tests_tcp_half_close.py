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
import socket
import time

from system_test import Qdrouterd, TIMEOUT, TestCase, unittest
from system_test import main_module, retry_exception, retry
from system_test import CONNECTION_TYPE


class TcpAdaptorIdleHalfClosedTest(TestCase):
    """
    Test the TCP adaptor half-closed idle connection timeout functionality
    """
    @classmethod
    def setUpClass(cls):
        # Create a single router with a tcpConnector and tcpListener.
        super(TcpAdaptorIdleHalfClosedTest, cls).setUpClass()

        cls.idle_timeout = 3
        cls.service_addr = "idle/timeout"
        cls.listener_port = cls.tester.get_port()
        cls.connector_port = cls.tester.get_port()
        config = [
            ('router', {'mode': 'interior', 'id': 'TcpIdleTimeout'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpListener', {'port': cls.listener_port,
                             'address': cls.service_addr}),
            ('tcpConnector', {'host': '127.0.0.1',
                              'port': cls.connector_port,
                              'address': cls.service_addr}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        config = Qdrouterd.Config(config)

        os.environ["SKUPPER_ROUTER_ENABLE_1152"] = str(cls.idle_timeout)
        cls.router = cls.tester.qdrouterd('TcpIdleTimeout',
                                          Qdrouterd.Config(config),
                                          wait=False)
        cls.router.wait_startup_message()
        os.environ.pop("SKUPPER_ROUTER_ENABLE_1152")

    def _get_tcp_conn_count(self):
        """
        Return the number of currently active TCP connections
        """
        mgmt = self.router.management
        conns = mgmt.query(type=CONNECTION_TYPE, attribute_names=['protocol',
                                                                  'container',
                                                                  'host']).get_dicts()
        results = [c for c in filter(lambda c:
                                     c['protocol'] == 'tcp' and
                                     c['container'] == 'TcpAdaptor' and
                                     c['host'] != 'egress-dispatch', conns)]
        return len(results)

    def _is_socket_closed(self, sock):
        sock.settimeout(TIMEOUT)
        try:
            data = sock.recv(4096)
            if data == b'':
                return True
        except Exception as exc:
            print(f"Socket Recv Failed! Error={exc}", flush=True)
        return False

    def test_01_detect_idle_conn(self):
        """
        Connect a client and server that perform a half-close. Wait for the
        idle timeout to expire and verify the TCP connections have been
        force-closed by the router.
        """

        # create the server listening socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.connector_port))
            listener.settimeout(TIMEOUT)
            listener.listen(1)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                retry_exception(lambda cs=client:
                                cs.connect(("localhost",
                                            self.listener_port)),
                                delay=0.25,
                                exception=ConnectionRefusedError)
                client.settimeout(TIMEOUT)

                # accept the client connection
                server, _ = listener.accept()
                try:
                    # Ensure both conns are established
                    self.assertTrue(retry(lambda: self._get_tcp_conn_count() == 2))

                    # now half-close the client
                    client.shutdown(socket.SHUT_WR)

                    # send a continuous stream of data from the server to the
                    # client for longer than the timeout. Since there is
                    # active traffic the connection must not drop

                    deadline = time.time() + self.idle_timeout + 1
                    while time.time() < deadline:
                        time.sleep(0.25)
                        server.sendall(b'ping')
                        data = client.recv(4096)
                        self.assertNotEqual(b'', data)

                    # verify the connections are still present
                    self.assertEqual(2, self._get_tcp_conn_count())

                    # Now wait until both conns are torn down
                    self.assertTrue(retry(lambda: self._get_tcp_conn_count() == 0))

                    # verify that the client and server sockets are closed
                    self.assertTrue(retry(lambda sock=client:
                                          self._is_socket_closed(sock)))
                    self.assertTrue(retry(lambda sock=server:
                                          self._is_socket_closed(sock)))

                finally:
                    server.close()

        # verify the vanflow event was generated
        self.router.wait_log_message("result=connection:forced reason=connection closed due to half-closed idle timeout")


if __name__ == '__main__':
    unittest.main(main_module())

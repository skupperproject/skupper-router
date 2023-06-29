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

import socket

from system_test import Qdrouterd, TIMEOUT, TestCase, unittest
from system_test import main_module, retry_exception, retry


class TcpAdaptorIdleTimeoutTest(TestCase):
    """
    Test the TCP adaptor idle connection timeout functionality
    """
    @classmethod
    def setUpClass(cls):
        # Create a single router with a tcpConnector and tcpListener.
        # Configure a short idle timeout on the tcpListener
        super(TcpAdaptorIdleTimeoutTest, cls).setUpClass()

        cls.idle_timeout = 5
        cls.service_addr = "idle/timeout"
        cls.listener_port = cls.tester.get_port()
        cls.connector_port = cls.tester.get_port()
        config = [
            ('router', {'mode': 'interior', 'id': 'TcpIdleTimeout'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('tcpListener', {'port': cls.listener_port,
                             'address': cls.service_addr,
                             'idleTimeoutSeconds': cls.idle_timeout}),
            ('tcpConnector', {'host': '127.0.0.1',
                              'port': cls.connector_port,
                              'address': cls.service_addr}),
            ('address', {'prefix': 'closest', 'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
        ]

        config = Qdrouterd.Config(config)

        cls.router = cls.tester.qdrouterd('TcpIdleTimeout',
                                          Qdrouterd.Config(config),
                                          wait=False)
        cls.router.wait_startup_message()

    def _get_tcp_conn_count(self):
        """
        Return the number of currently active TCP connections
        """
        CONNECTION_TYPE = 'io.skupper.router.connection'
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
        Connect a client and server that do not transfer any data. Wait for the
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

                    # Now wait until both conns are torn down
                    self.assertTrue(retry(lambda: self._get_tcp_conn_count() == 0))

                    # verify that the client and server sockets are closed
                    self.assertTrue(retry(lambda sock=client:
                                          self._is_socket_closed(sock)))
                    self.assertTrue(retry(lambda sock=server:
                                          self._is_socket_closed(sock)))
                finally:
                    server.close()


if __name__ == '__main__':
    unittest.main(main_module())

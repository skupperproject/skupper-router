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

"""
Verifies that the router detects when the inter-router connection group is
split across routers
"""

import os
import selectors
import socket
from threading import Event, Thread
from system_test import TestCase, main_module, Qdrouterd, unittest
from system_test import TIMEOUT, Logger


class MyLoadBalancer:
    """
    A mock connection load balancer. Used by the tests to spread inter-router
    connections across two routers. Runs as a background thread.

    @param server_addr: the TCP address to listen on. Should be the destination
    for a routers Connector.
    @param routerN_addr: the TCP address of routers that incoming connections
    will be spread across. Should be the routers Listener addresses.
    """
    class _BufferedSocket:
        """
        A socket with a buffer of pending input.
        """
        def __init__(self, sock):
            self.sock = sock
            self.input_buf = bytearray()

        def fileno(self):
            return self.sock.fileno()

    def __init__(self, server_addr, router1_addr, router2_addr):
        self.server_addr = server_addr
        self.router_addrs = (router1_addr, router2_addr)
        self.router_index = 0  # index into router_addrs
        self.selector = selectors.DefaultSelector()
        self._socks = []
        self._shutdown = False
        self._logger = Logger(title="MyLoadBalancer", print_to_console=False,
                              ofilename=os.path.join(os.getcwd(), "MyLoadBalancer.out"))
        self._ready = Event()
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()
        running = self._ready.wait(timeout=TIMEOUT)
        if running is False:
            raise Exception("MyLoadBalancer failed to start")
        msg = "MyLoadBalancer created"
        msg += f" server={self.server_addr[0]}:{self.server_addr[1]}"
        msg += f" router1={self.router_addrs[0][0]}:{self.router_addrs[0][1]}"
        msg += f" router2={self.router_addrs[1][0]}:{self.router_addrs[1][1]}"
        self._logger.log(msg)

    def _run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.setblocking(True)
            server.bind(self.server_addr)
            server.listen()
            self.selector.register(server, selectors.EVENT_READ, data=None)
            self._ready.set()

            self._logger.log("MyLoadBalancer begin")
            while not self._shutdown:
                events = self.selector.select(timeout=0.5)
                if events:
                    for key, mask in events:
                        if key.data is None:
                            # server socket
                            self.on_accept(key.fileobj)
                        else:
                            # socket I/O
                            self.on_io_event(key.fileobj, mask, key.data)
            self._logger.log("MyLoadBalancer end")

            for sock in self._socks:
                if sock.sock is not None:
                    self.selector.unregister(sock)
                    sock.sock.shutdown(socket.SHUT_RDWR)
                    sock.sock.close()
            self.selector.unregister(server)
        self.selector.close()

    def on_accept(self, server):
        # Accept a connection from the Connector
        conn, addr = server.accept()
        csock = self._BufferedSocket(conn)

        # Create an outgoing socket to the Listener
        laddr = self.router_addrs[self.router_index]
        self.router_index = 0 if self.router_index == 1 else 1

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.setblocking(True)
        while True:
            try:
                conn.connect(laddr)
                break
            except (ConnectionRefusedError, ConnectionAbortedError):
                pass

        lsock = self._BufferedSocket(conn)
        self.selector.register(lsock, selectors.EVENT_READ, csock)
        self.selector.register(csock, selectors.EVENT_READ, lsock)
        self._socks.append(lsock)
        self._socks.append(csock)
        self._logger.log(f"MyLoadBalancer forwarding between {lsock.sock.getpeername()} and {csock.sock.getpeername()}")

    def on_io_event(self, bsock, mask, peer_bsock):
        # If writable send any buffered data
        if mask & selectors.EVENT_WRITE == selectors.EVENT_WRITE:
            sent = bsock.sock.send(bsock.input_buf)
            self._logger.log(f"MyLoadBalancer sending {sent} bytes to {bsock.sock.getpeername()}")
            del bsock.input_buf[:sent]
            if not bsock.input_buf:
                # No more to write
                self.selector.modify(bsock, selectors.EVENT_READ, peer_bsock)

        # If readable fill the input buffer of the peer
        if mask & selectors.EVENT_READ == selectors.EVENT_READ:
            data = bsock.sock.recv(4096)
            if data:
                self._logger.log(f"MyLoadBalancer receiving {len(data)} bytes from {bsock.sock.getpeername()}")
                peer_bsock.input_buf += data
                self.selector.modify(peer_bsock,
                                     selectors.EVENT_READ | selectors.EVENT_WRITE,
                                     bsock)
            else:
                self._logger.log(f"MyLoadBalancer closing conns {bsock.sock.getpeername()} {peer_bsock.sock.getpeername()}")
                self.selector.unregister(bsock)
                bsock.sock.shutdown(socket.SHUT_RDWR)
                bsock.sock.close()
                self._socks.remove(bsock)

                self.selector.unregister(peer_bsock)
                peer_bsock.sock.shutdown(socket.SHUT_RDWR)
                peer_bsock.sock.close()
                self._socks.remove(peer_bsock)

    def teardown(self):
        self._shutdown = True
        self._thread.join(TIMEOUT)
        if self._thread.is_alive():
            raise Exception("MyLoadBalancer failed to join!")


class InterRouterSplitPathTest(TestCase):
    """
    Verify the router logs errors when it detects that connections from the
    same connection group arrive a different routers
    """
    @classmethod
    def setUpClass(cls):
        super(InterRouterSplitPathTest, cls).setUpClass()

    def router(self, name, test_config, data_connection_count, **kwargs):
        config = [
            ('router', {'mode': 'interior',
                        'id': name,
                        'dataConnectionCount': f"{data_connection_count}"}),
            ('listener', {'port': self.tester.get_port(), 'role': 'normal'}),
        ]
        config.extend(test_config)
        return self.tester.qdrouterd(name, Qdrouterd.Config(config), **kwargs)

    def test_01(self):
        """
        Simulate splitting connections to the wrong routers.  Creates a single
        connector-based router and two listener-based routers. A mock load
        balancer sits between the connector router and the listeners. The mock
        load balancer splits the connection group connections between the two
        listener routers
        """
        data_conn_count = 2
        inter_router_port_c = self.tester.get_port()
        inter_router_port_l1 = self.tester.get_port()
        inter_router_port_l2 = self.tester.get_port()

        load_balancer = MyLoadBalancer(('', inter_router_port_c),
                                       ('localhost', inter_router_port_l1),
                                       ('localhost', inter_router_port_l2))

        # Start the two listener routers
        router_L1 = self.router("RouterL1",
                                [('listener', {'role': 'inter-router',
                                               'host': '0.0.0.0',
                                               'port': inter_router_port_l1})],
                                data_conn_count, wait=True)
        router_L2 = self.router("RouterL2",
                                [('listener', {'role': 'inter-router',
                                               'host': '0.0.0.0',
                                               'port': inter_router_port_l2})],
                                data_conn_count, wait=True)

        # Start the connector router
        router_C = self.router("RouterC",
                               [('connector', {'role': 'inter-router',
                                               'host': 'localhost',
                                               'port': inter_router_port_c})],
                               data_conn_count, wait=True)

        # Expect to see error logs in the connector-side router
        router_C.wait_log_message("Connection group failure: connections split across routers")

        load_balancer.teardown()
        router_L1.teardown()
        router_L2.teardown()
        router_C.teardown()


if __name__ == '__main__':
    unittest.main(main_module())

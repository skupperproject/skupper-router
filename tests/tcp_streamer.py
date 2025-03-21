#!/usr/bin/env python3
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

import argparse
import logging
import selectors
import socket
import sys
from threading import Thread
from system_test import TIMEOUT


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class TcpStreamer:
    """
    A test tool that establishes N TCP connections across the router. Data is
    continually passed across these links at a very slow rate. The intent of
    this tool is to verify that TCP streams are preserved while router
    operations are performed.
    """
    def __init__(self, client_addr, server_addr, client_count, poll_timeout=1.0):
        self.client_addr = client_addr
        self.server_addr = server_addr
        self.client_count = client_count  # two sockets will be created per client
        self.selector = selectors.DefaultSelector()
        self.poll_timeout = poll_timeout
        self.clients = []
        self._bytes_received = 0
        self._shutdown = False
        logger.debug("TcpStreamer created host=%s listen=%s",
                     f"{self.client_addr[0]}:{self.client_addr[1]}",
                     f"{self.server_addr[0]}:{self.server_addr[1]}")

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.setblocking(True)
            server.bind(self.server_addr)
            server.listen()
            self.selector.register(server, selectors.EVENT_READ, self.on_accept)

            for index in range(self.client_count):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.setblocking(True)
                while True:
                    try:
                        client.connect(self.client_addr)
                        break
                    except (ConnectionRefusedError, ConnectionAbortedError):
                        pass
                self.selector.register(client, selectors.EVENT_WRITE, self.on_event)
                self.clients.append(client)

            logger.debug("TcpStreamer begin sending traffic")
            while self.clients and not self._shutdown:
                events = self.selector.select(timeout=self.poll_timeout)
                if events:
                    for key, mask in events:
                        cb = key.data
                        cb(key.fileobj, mask)
                else:
                    for client in self.clients:
                        client.send(b'!')
            logger.debug("TcpStreamer end sending traffic")

            for client in self.clients:
                self.selector.unregister(client)
                client.shutdown(socket.SHUT_RDWR)
                client.close()
            self.selector.unregister(server)
        self.selector.close()

    def on_accept(self, server, mask):
        conn, addr = server.accept()
        logger.debug("TcpStreamer accepting %s", str(addr))
        self.selector.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, self.on_event)
        self.clients.append(conn)

    def on_event(self, conn, mask):
        if mask & selectors.EVENT_WRITE == selectors.EVENT_WRITE:
            conn.send(b'?')
            self.selector.modify(conn, selectors.EVENT_READ, self.on_event)

        if mask & selectors.EVENT_READ == selectors.EVENT_READ:
            data = conn.recv(10)
            if data:
                self._bytes_received += len(data)
            else:
                logger.debug("TcpStreamer closing %s", str(conn.getsockname()))
                self.selector.unregister(conn)
                self.clients.remove(conn)
                conn.close()

    @property
    def bytes_received(self):
        return self._bytes_received

    @property
    def active_clients(self):
        return len(self.clients) / 2  # two sockets per client

    def shutdown(self):
        self._shutdown = True


class TcpStreamerThread:
    """
    A wrapper for running the TCP streamer in a thread.
    """
    def __init__(self, **kwargs):
        self._streamer = TcpStreamer(**kwargs)
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        self._streamer.run()

    def join(self, timeout=TIMEOUT):
        self._streamer.shutdown()
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise Exception("TcpStreamer failed to join!")

    @property
    def bytes_received(self):
        return self._streamer.bytes_received

    @property
    def active_clients(self):
        if self.is_alive is False:
            return 0
        return self._streamer.active_clients

    @property
    def is_alive(self):
        return self._thread.is_alive()


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument('--host', help="Host to connect to [127.0.0.1:10000]", default='127.0.0.1:10000')
    p.add_argument('--listen', help="Address to listen on [0.0.0.0:20000]", default='0.0.0.0:20000')
    p.add_argument('--count', help="Total clients to create", default=1, type=int)

    del argv[0]
    args = p.parse_args(argv)

    if args.host is None:
        raise Exception("NO HOST")
    if args.listen is None:
        raise Exception("NO LISTEN ADDRESS")

    host_address = args.host.split(':')
    listen_address = args.listen.split(':')
    streamer = TcpStreamer((host_address[0], int(host_address[1])),
                           (listen_address[0], int(listen_address[1])),
                           args.count)
    try:
        streamer.run()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

#!/usr/bin/env python

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
# A simple TCP echo server that supports TLS connections
#

import argparse
import asyncio
import signal
import sys
import ssl
import time
import traceback
from threading import Condition, Thread
from typing import Union

from system_test import Logger
from system_test import TIMEOUT


class GracefulExitSignaler:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


def split_chunk_for_display(raw_bytes):
    """
    Given some raw bytes, return a display string
    Only show the beginning and end of largish (2x CONTENT_CHUNK_SIZE) arrays.
    :param raw_bytes:
    :return: display string
    """
    CONTENT_CHUNK_SIZE = 50  # Content repeats after chunks this big - used by echo client, too
    if len(raw_bytes) > 2 * CONTENT_CHUNK_SIZE:
        result = repr(raw_bytes[:CONTENT_CHUNK_SIZE]) + " ... " + repr(raw_bytes[-CONTENT_CHUNK_SIZE:])
    else:
        result = repr(raw_bytes)
    return result


class TcpEchoServerHandler(asyncio.Protocol):
    def __init__(self, echo_server):
        self.peername = "UNKNOWN"
        self.echo_server = echo_server
        self.logger = echo_server.logger
        self.name = echo_server.prefix
        super(TcpEchoServerHandler, self).__init__()

    def connection_made(self, transport):
        self.peername = "%s:%s" % transport.get_extra_info('peername')
        self.transport = transport
        self.logger.log(f" {self.name}: Accepted connection from {self.peername}")

        if self.echo_server.conn_stall > 0.0:
            self.logger.log(f' {self.name}: Connection from {self.peername} stall start')
            time.sleep(self.echo_server.conn_stall)
            self.logger.log(f' {self.name}: Connection from {self.peername} stall end')
        if self.echo_server.close_on_conn:
            self.logger.log(f' {self.name}: Connection from {self.peername} closing due to close_on_conn')
            self.transport.close()
            return

    def connection_lost(self, exc):
        self.logger.log(f' {self.name}: Connection to {self.peername} lost, exception={exc}')

    def eof_received(self):
        self.logger.log(f' {self.name}: EOF received from peer {self.peername}')
        return False

    def data_received(self, data):
        self.logger.log(f' {self.name}: peer {self.peername} received: {len(data)} bytes {split_chunk_for_display(data)}')
        self.echo_server.total_bytes_received += len(data)

        if self.echo_server.close_on_data:
            self.logger.log(f' {self.name}: closing peer {self.peername} due to close_on_data')
            self.transport.close()
            return

        self.transport.write(data)
        self.logger.log(f' {self.name}: peer {self.peername} reply written')


class TcpEchoServer:
    def __init__(self, prefix="ECHO_SERVER",
                 host='127.0.0.1',
                 port: Union[str, int] = "0",
                 logger=None,
                 conn_stall=0.0,
                 close_on_conn=False,
                 close_on_data=False,
                 ssl_info=None) -> None:
        """
        Start echo server in separate thread

        :param prefix: log prefix
        :param port: port to listen on
        :param echo_count: exit after echoing this many bytes
        :param timeout: exit after this many seconds
        :param logger: Logger() object
        """
        self.prefix = prefix
        self.port = int(port)
        self.logger = logger
        self.conn_stall = conn_stall
        self.close_on_conn = close_on_conn
        self.close_on_data = close_on_data
        self.HOST = host
        self._cv = Condition()
        self._is_running = None
        self.error = None
        self.ssl_info = ssl_info
        self.total_bytes_received = 0

        self.loop = None
        self.server = None

        self._thread = Thread(target=self.run)
        self._thread.daemon = True
        # Note: do not add any code after the following thread.start()
        # The run() method starts running immediately after call to start().
        self._thread.start()
        _ = self.is_running  # pause until server is bound and listening

    @property
    def is_running(self):
        with self._cv:
            self._cv.wait_for(lambda: self._is_running is not None, timeout=10)
            return self._is_running

    @is_running.setter
    def is_running(self, value):
        with self._cv:
            self._is_running = value
            self._cv.notify_all()

    def get_listening_port(self) -> int:
        if self.is_running:
            return self.port
        return 0

    def run(self):
        """
        Run server in daemon thread.
        A single thread runs multiple sockets through selectors.
        Note that timeouts and such are done in line and processing stops for
        all sockets when one socket is timing out. For the intended one-at-a-time
        test cases this works but it is not general solution for all cases.
        :return:
        """
        try:
            ssl_context = None
            if self.ssl_info:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.load_verify_locations(cafile=self.ssl_info['CA_CERT'])
                ssl_context.load_cert_chain(certfile=self.ssl_info['SERVER_CERTIFICATE'],
                                            keyfile=self.ssl_info['SERVER_PRIVATE_KEY'])
        except Exception:
            self.error = ('%s configuring ssl_context for %s:%s exception: %s' %
                          (self.prefix, self.HOST, self.port, traceback.format_exc()))
            self.logger.log(self.error)
            return 1

        # run the client socket event loop until it is cancelled
        try:
            asyncio.run(self.event_loop(ssl_context, self.conn_stall,
                                        self.close_on_conn,
                                        self.close_on_data))
        except asyncio.CancelledError:
            pass

        self.is_running = False

    async def event_loop(self, ssl_context, conn_stall, close_on_conn, close_on_data):
        try:
            self.loop = asyncio.get_running_loop()

            self.server = await self.loop.create_server(
                lambda: TcpEchoServerHandler(self),
                host=self.HOST,
                port=self.port,
                ssl=ssl_context,
                reuse_address=True,
                start_serving=True)

            if self.port == 0:
                self.port = self.server.sockets[0].getsockname()[1]

            # notify whoever is waiting on the condition variable for this
            self.is_running = True

            self.logger.log(' %s Listening on host:%s, TLS enabled port:%s' % (self.prefix, self.HOST, self.port))

            async with self.server:
                await self.server.serve_forever()

        except Exception:
            self.error = "ERROR: exception : '%s'" % traceback.format_exc()

    def wait(self, timeout=TIMEOUT):
        self.logger.log(" %s Server is shutting down" % self.prefix)
        if self.loop:
            def _cancel_server():
                loop = asyncio.get_running_loop()
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()

            self.loop.call_soon_threadsafe(_cancel_server)
        self._thread.join(timeout)
        self.logger.log(" %s Server shutdown completed" % self.prefix)


def main(argv):
    retval = 0
    logger = None
    # parse args
    p = argparse.ArgumentParser()
    p.add_argument('--port', '-p',
                   help='Required listening port number')
    p.add_argument('--host',
                   default='127.0.0.1',
                   help='Address to bind to')
    p.add_argument('--name',
                   help='Optional logger prefix')
    p.add_argument('--echo', '-e', type=int, default=0, const=1, nargs="?",
                   help='Exit after echoing this many bytes. Default value "0" disables exiting on byte count.')
    p.add_argument('--timeout', '-t', type=float, default=0.0, const=1, nargs="?",
                   help='Timeout in seconds. Default value "0.0" disables timeouts')
    p.add_argument('--log', '-l',
                   action='store_true',
                   help='Write activity log to console')
    # Add controlled server misbehavior for testing conditions seen in the field
    # Stall required to trigger adaptor windowed flow control
    p.add_argument('--connect-stall', type=float, default=0.0, const=1, nargs="?",
                   help='Accept connections but wait this many seconds before reading from socket. Default value "0.0" disables stall')
    # Close on connect - exercises control paths scrutinized under DISPATCH-1968
    p.add_argument('--close-on-connect',
                   action='store_true',
                   help='Close client connection without reading from socket when listener connects. If stall is specified then stall before closing.')
    # Close on data - exercises control paths scrutinized under DISPATCH-1968
    p.add_argument('--close-on-data',
                   action='store_true',
                   help='Close client connection as soon as data arrives.')

    # TLS configuration
    p.add_argument('--ca-file',
                   help='Path to the file containing the CA certificate (PEM)')
    p.add_argument('--cert-file',
                   help='Path to the server self-identifying certificate file (PEM)')
    p.add_argument('--key-file',
                   help='Path to server key file (PEM)')

    del argv[0]
    args = p.parse_args(argv)

    # port
    if args.port is None:
        raise Exception("User must specify a port number")
    port = args.port

    # name / prefix
    prefix = args.name if args.name is not None else "ECHO_SERVER (%s)" % (str(port))

    # echo
    if args.echo < 0:
        raise Exception("Echo count must be greater than zero")

    # timeout
    if args.timeout < 0.0:
        raise Exception("Timeout must be greater than or equal to zero")

    # timeout
    if args.connect_stall < 0.0:
        raise Exception("Connect-stall must be greater than or equal to zero")

    ssl_info = {}
    if args.ca_file is not None:
        ssl_info['CA_CERT'] = args.ca_file
    if args.cert_file is not None:
        ssl_info['SERVER_CERTIFICATE'] = args.cert_file
    if args.key_file is not None:
        ssl_info['SERVER_PRIVATE_KEY'] = args.key_file

    signaller = GracefulExitSignaler()
    server = None

    try:
        # logging
        logger = Logger(title="%s port %s" % (prefix, port),
                        print_to_console=args.log,
                        save_for_dump=False)

        server = TcpEchoServer(prefix, args.host, port, logger,
                               args.connect_stall, args.close_on_connect,
                               args.close_on_data, ssl_info=ssl_info)

        keep_running = True
        while keep_running:
            time.sleep(0.1)
            if server.error is not None:
                logger.log("ECHO SERVER %s stopped with error: %s" % (prefix, server.error))
                keep_running = False
                retval = 1
            if signaller.kill_now:
                logger.log("ECHO SERVER  %s Process killed with signal" % prefix)
                keep_running = False
            if keep_running and not server.is_running:
                logger.log("ECHO SERVER  %s stopped with no error or status" % prefix)
                keep_running = False

    except Exception:
        if logger is not None:
            logger.log("ECHO SERVER  %s Exception: %s" % (prefix, traceback.format_exc()))
        retval = 1

    return retval


if __name__ == "__main__":
    sys.exit(main(sys.argv))

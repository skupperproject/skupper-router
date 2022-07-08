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

# This HTTP/2 server can be configured via the following environment variables:
# - SERVER_LISTEN_PORT: the port the server will listen on
# - SERVER_TLS: if 'yes' use TLS
# - SERVER_CA_CERT: TLS configuration
# - SERVER_CERTIFICATE: TLS configuration
# - SERVER_PRIVATE_KEY: TLS configuration

import socket
import ssl
import signal
import sys
import os
import h2.connection
import h2.events
import h2.config
import h2.errors

BYTES = 16384


def receive_signal(signalNumber, frame):
    print('Received:', signalNumber)
    sys.exit(0)


def send_response(event, conn):
    """
    conn.close_connection() sends a goaway frame to the client
    and closes the connection.
    """
    stream_id = event.stream_id
    conn.send_headers(stream_id=stream_id,
                      headers=[(':status', '200'), ('server', 'h2_slow_q2_server/0.1.0')])
    conn.send_data(stream_id=stream_id,
                   data=b'Success!',
                   end_stream=True)


def handle_events(conn, events):
    for event in events:
        if isinstance(event, h2.events.DataReceived):
            # When the server receives a DATA frame from the router, we send back a WINDOW_UPDATE frame
            # with a window size increment of only 1k (1024 bytes)
            # This pushes the router into q2 since it is able to only send two qd_buffers at a time.
            conn.increment_flow_control_window(1024, None)
            conn.increment_flow_control_window(1024, event.stream_id)
        elif isinstance(event, h2.events.StreamEnded):
            send_response(event, conn)


def handle(sock):
    print("Connection accepted, socket fileno=%s!" % sock.fileno())
    config = h2.config.H2Configuration(client_side=False)

    # The default initial window per HTTP2 spec is 64K.
    # That means that the router is allowed to send only 64k before it needs more WINDOW_UPDATE frames
    # providing more credit for the router to send more data.
    conn = h2.connection.H2Connection(config=config)
    conn.initiate_connection()
    sock.sendall(conn.data_to_send())

    while True:
        data = None
        try:
            data = sock.recv(BYTES)
        except:
            pass
        if not data:
            break
        try:
            events = conn.receive_data(data)
        except Exception as e:
            print(e)
            break
        handle_events(conn, events)
        data_to_send = conn.data_to_send()
        if data_to_send:
            sock.sendall(data_to_send)
    print("Closing client connection, socket fileno=%s" % sock.fileno())
    sock.close()


def main():
    signal.signal(signal.SIGHUP, receive_signal)
    signal.signal(signal.SIGINT, receive_signal)
    signal.signal(signal.SIGQUIT, receive_signal)
    signal.signal(signal.SIGILL, receive_signal)
    signal.signal(signal.SIGTERM, receive_signal)

    port = os.getenv('SERVER_LISTEN_PORT')
    if port is None:
        raise RuntimeError("Environment variable `SERVER_LISTEN_PORT` is not set.")

    if os.getenv('SERVER_TLS') == "yes":
        # TLS enabled
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_verify_locations(cafile=os.getenv('SERVER_CA_CERT'))
        context.load_cert_chain(certfile=os.getenv('SERVER_CERTIFICATE'), keyfile=os.getenv('SERVER_PRIVATE_KEY'))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0', int(port)))
            sock.listen(5)
            print(f"HTTP/2 test server listening for TLS connections on port {port}")
            with context.wrap_socket(sock, server_side=True) as ssl_sock:
                while True:
                    # The accept method blocks until someone attempts to connect to our TCP
                    # port: when they do, it returns a tuple: the first element is a new
                    # socket object, the second element is a tuple of the address the new
                    # connection is from
                    handle(ssl_sock.accept()[0])

    else:
        # non-TLS socket.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0', int(port)))
            sock.listen(5)
            print(f"HTTP/2 test server listening for connections on port {port}")

            while True:
                # The accept method blocks until someone attempts to connect to our TCP
                # port: when they do, it returns a tuple: the first element is a new
                # socket object, the second element is a tuple of the address the new
                # connection is from
                handle(sock.accept()[0])


if __name__ == '__main__':
    main()

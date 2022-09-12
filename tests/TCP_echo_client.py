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

import argparse
import selectors
import signal
import socket
import ssl
import sys
from threading import Thread
import time
import traceback
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
    Only show the beginning and end of largish (2xMAGIC_SIZE) arrays.
    :param raw_bytes:
    :return: display string
    """
    MAGIC_SIZE = 50  # Content repeats after chunks this big - used by echo client, too
    if len(raw_bytes) > 2 * MAGIC_SIZE:
        result = repr(raw_bytes[:MAGIC_SIZE]) + " ... " + repr(raw_bytes[-MAGIC_SIZE:])
    else:
        result = repr(raw_bytes)
    return result


class TcpEchoClient:
    def __init__(self, prefix, host, port, size, count,
                 timeout=TIMEOUT,
                 logger=None,
                 ssl_info=None):
        """
        :param host: connect to this host
        :param port: connect to this port
        :param size: size of individual payload chunks in bytes
        :param count: number of payload chunks
        :param strategy: "1" Send one payload;  # TODO more strategies
                             Recv one payload
        :param logger: Logger() object
        :return:
        """
        # Start up
        self.sock = None
        self.ssl_sock = None
        self.prefix = prefix
        self.host = host
        self.port = int(port)
        self.size = size
        self.count = count
        self.timeout = timeout
        self.logger = logger
        self.keep_running = True
        self.is_running = False
        self.exit_status = None
        self.error = None
        self.ssl_info = ssl_info
        self._thread = Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self):
        self.logger.log("%s Client is starting up" % self.prefix)
        try:
            start_time = time.time()
            self.is_running = True
            self.logger.log('%s Connecting to host:%s, port:%d, size:%d, count:%d' %
                            (self.prefix, self.host, self.port, self.size, self.count))
            total_sent = 0
            total_rcvd = 0

            if self.count > 0 and self.size > 0:
                # outbound payload only if count and size both greater than zero
                payload_out = []
                out_list_idx = 0  # current _out array being sent
                out_byte_idx = 0  # next-to-send in current array
                out_ready_to_send = True
                # Generate unique content for each message so you can tell where the message
                # or fragment belongs in the whole stream. Chunks look like:
                #    b'[localhost:33333:6:0]ggggggggggggggggggggggggggggg'
                #    host: localhost
                #    port: 33333
                #    index: 6
                #    offset into message: 0
                CONTENT_CHUNK_SIZE = 50  # Content repeats after chunks this big - used by echo server, too
                for idx in range(self.count):
                    body_msg = ""
                    padchar = "abcdefghijklmnopqrstuvwxyz@#$%"[idx % 30]
                    while len(body_msg) < self.size:
                        chunk = "[%s:%d:%d:%d]" % (self.host, self.port, idx, len(body_msg))
                        padlen = CONTENT_CHUNK_SIZE - len(chunk)
                        chunk += padchar * padlen
                        body_msg += chunk
                    if len(body_msg) > self.size:
                        body_msg = body_msg[:self.size]
                    payload_out.append(bytearray(body_msg.encode()))
                # incoming payloads
                payload_in = []
                in_list_idx = 0  # current _in array being received
                for i in range(self.count):
                    payload_in.append(bytearray())
            else:
                # when count or size .LE. zero then just connect-disconnect
                self.keep_running = False

            # Set up connection.  If the TCPConnectors have not yet finished
            # coming up then it is possible to get a ConnectionRefusedError.
            # This is not necessarly an error, so retry

            host_address = (self.host, self.port)
            if self.ssl_info:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.load_verify_locations(cafile=self.ssl_info['CA_CERT'])

                # If a client cert is provided, use it. If not, we will try connecting to the router
                # with the ca-certificate.pem
                if self.ssl_info.get('CLIENT_CERTIFICATE'):
                    context.load_cert_chain(certfile=self.ssl_info['CLIENT_CERTIFICATE'],
                                            keyfile=self.ssl_info['CLIENT_PRIVATE_KEY'],
                                            password=self.ssl_info['CLIENT_PRIVATE_KEY_PASSWORD'])
                sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock = context.wrap_socket(sck, server_hostname=self.host, server_side=False)
                self.logger.log("%s Socket wrapped with TLS self.host=%s, self.port=%s" % (self.prefix, self.host, self.port))
            else:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_timeout = time.time() + TIMEOUT
            while True:
                try:
                    self.sock.connect(host_address)
                    break
                except ConnectionRefusedError as err:
                    if time.time() > conn_timeout:
                        self.logger.log('%s Failed to connect to host:%s port:%d - Connection Refused!'
                                        % (self.prefix, self.host, self.port))
                        raise
                    time.sleep(0.1)
                    self.logger.log('%s Failed to connect to host:%s port:%d - Retrying...'
                                    % (self.prefix, self.host, self.port))

            self.sock.setblocking(False)

            # set up selector
            sel = selectors.DefaultSelector()
            sel.register(self.sock,
                         selectors.EVENT_READ | selectors.EVENT_WRITE)

            # event loop
            while self.keep_running:
                if self.timeout > 0.0:
                    elapsed = time.time() - start_time
                    if elapsed > self.timeout:
                        self.exit_status = "%s Exiting due to timeout. Total sent= %d, total rcvd= %d" % \
                                           (self.prefix, total_sent, total_rcvd)
                        break
                for key, mask in sel.select(timeout=1):
                    sock = key.fileobj
                    if mask & selectors.EVENT_READ:
                        ssl_want_read = False
                        self.logger.log("ECHO CLIENT selectors.EVENT_READ")
                        if self.ssl_info:
                            try:
                                recv_data = b''
                                while True:
                                    self.logger.log("ECHO CLIENT selectors.EVENT_READ...receiving SSL data")
                                    rcv_data = sock.recv(1024)
                                    if rcv_data and len(rcv_data) > 0:
                                        self.logger.log("ECHO CLIENT selectors.EVENT_READ "
                                                        "received SSL data length=%d" % len(rcv_data))
                                        recv_data += rcv_data
                                    else:
                                        self.logger.log("ECHO CLIENT selectors.EVENT_READ...no more SSL data "
                                                        "to receive, breaking")
                                        break
                            except ssl.SSLWantReadError:
                                ssl_want_read = True
                                len_recv_data = len(recv_data) if recv_data else 0
                                self.logger.log("ECHO CLIENT ssl.SSLWantReadError, recv_data=%d" % len_recv_data)
                        else:
                            recv_data = sock.recv(1024)

                        if recv_data:
                            total_rcvd = len(recv_data)
                            payload_in[in_list_idx].extend(recv_data)
                            self.logger.log("%s Received bytes len=%d, bytes received so far=%d" %
                                            (self.prefix, total_rcvd, len(payload_in[in_list_idx])))

                            if len(payload_in[in_list_idx]) == self.size:
                                self.logger.log("%s Rcvd message %d" % (self.prefix, in_list_idx))
                                in_list_idx += 1
                                if in_list_idx == self.count:
                                    # Received all bytes of all chunks - done.
                                    self.keep_running = False
                                    # Verify the received data
                                    if payload_in != payload_out:
                                        for idxc in range(self.count):
                                            if not payload_in[idxc] == payload_out[idxc]:
                                                for idxs in range(self.size):
                                                    ob = payload_out[idxc][idxs]
                                                    ib = payload_in[idxc][idxs]
                                                    if ob != ib:
                                                        self.error = "%s ERROR Rcvd message verify fail. row:%d, col:%d, " \
                                                                     "expected:%s, actual:%s" \
                                                                     % (self.prefix, idxc, idxs, repr(ob), repr(ib))
                                                        break
                                else:
                                    out_ready_to_send = True
                                    sel.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
                            elif len(payload_in[in_list_idx]) > self.size:
                                self.error = "ERROR Received message too big. Expected:%d, actual:%d" % \
                                             (self.size, len(payload_in[in_list_idx]))
                                break
                            else:
                                pass  # still accumulating a message
                        else:
                            if not ssl_want_read:
                                # In the non SSL case, if a read event came
                                self.keep_running = False
                                if not in_list_idx == self.count:
                                    self.error = "ERROR server closed. Echoed %d of %d messages." % \
                                                 (in_list_idx, self.count)

                    if self.keep_running and mask & selectors.EVENT_WRITE:
                        if out_ready_to_send:
                            payload_to_send = payload_out[out_list_idx][out_byte_idx:]
                            n_sent = 0
                            while True:
                                try:
                                    n_sent = self.sock.send(payload_to_send[n_sent:])
                                    break
                                except ssl.SSLWantWriteError:
                                    n_sent += n_sent
                            self.logger.log("%s Sent payload of length=%d" % (self.prefix, n_sent if n_sent else 0))
                            if n_sent:
                                total_sent += n_sent
                            out_byte_idx += n_sent
                            if out_byte_idx == self.size:
                                self.logger.log("%s Sent message %d" % (self.prefix, out_list_idx))
                                out_byte_idx = 0
                                out_list_idx += 1
                                sel.modify(self.sock, selectors.EVENT_READ)  # turn off write events
                                out_ready_to_send = False  # turn on when rcvr receives
                        else:
                            pass  # logger.log("DEBUG: ignoring EVENT_WRITE")

            # shut down
            sel.unregister(self.sock)
            self.sock.close()

        except Exception:
            self.error = "ERROR: exception : '%s'" % traceback.format_exc()
            self.sock.close()

        self.is_running = False

    def wait(self, timeout=TIMEOUT * 2):
        self.logger.log("%s Client is shutting down" % self.prefix)
        self.keep_running = False
        self._thread.join(timeout)


def main(argv):
    retval = 0
    # parse args
    p = argparse.ArgumentParser()
    p.add_argument('--host', '-b',
                   help='Required target host')
    p.add_argument('--port', '-p', type=int,
                   help='Required target port number')
    p.add_argument('--size', '-s', type=int, default=100, const=1, nargs='?',
                   help='Size of payload in bytes must be >= 0. Size of zero connects and disconnects with no data traffic.')
    p.add_argument('--count', '-c', type=int, default=1, const=1, nargs='?',
                   help='Number of payloads to process must be >= 0. Count of zero connects and disconnects with no data traffic.')
    p.add_argument('--name',
                   help='Optional logger prefix')
    p.add_argument('--timeout', '-t', type=float, default=0.0, const=1, nargs="?",
                   help='Timeout in seconds. Default value "0" disables timeouts')
    p.add_argument('--log', '-l',
                   action='store_true',
                   help='Write activity log to console')
    del argv[0]
    args = p.parse_args(argv)

    # host
    if args.host is None:
        raise Exception("User must specify a host")
    host = args.host

    # port
    if args.port is None:
        raise Exception("User must specify a port number")
    port = args.port

    # size
    if args.size < 0:
        raise Exception("Size must be greater than or equal to zero")
    size = args.size

    # count
    if args.count < 0:
        raise Exception("Count must be greater than or equal to zero")
    count = args.count

    # name / prefix
    prefix = args.name if args.name is not None else "ECHO_CLIENT (%d_%d_%d)" % \
                                                     (port, size, count)

    # timeout
    if args.timeout < 0.0:
        raise Exception("Timeout must be greater than or equal to zero")

    signaller = GracefulExitSignaler()
    logger = None

    try:
        # logging
        logger = Logger(title="%s host:%s port %d size:%d count:%d" % (prefix, host, port, size, count),
                        print_to_console=args.log,
                        save_for_dump=False)

        client = TcpEchoClient(prefix, host, port, size, count, args.timeout, logger)

        keep_running = True
        while keep_running:
            time.sleep(0.1)
            if client.error is not None:
                logger.log("ECHO CLIENT %s stopped with error: %s" % (prefix, client.error))
                keep_running = False
                retval = 1
            if client.exit_status is not None:
                logger.log("ECHO CLIENT %s stopped with status: %s" % (prefix, client.exit_status))
                keep_running = False
            if signaller.kill_now:
                logger.log("ECHO CLIENT %s Process killed with signal" % prefix)
                keep_running = False
            if keep_running and not client.is_running:
                logger.log("ECHO CLIENT %s Client stopped with no error or status" % prefix)
                keep_running = False

    except Exception:
        client.error = "ECHO CLIENT ERROR: exception : '%s'" % traceback.format_exc()
        if logger is not None:
            logger.log("ECHO CLIENT %s Exception: %s" % (prefix, traceback.format_exc()))
        retval = 1

    if client.error is not None:
        # write client errors to stderr
        def eprint(*args, **kwargs):
            print(*args, file=sys.stderr, **kwargs)

        elines = client.error.split("\n")
        for line in elines:
            eprint("ERROR:", prefix, line)

    return retval


if __name__ == "__main__":
    sys.exit(main(sys.argv))

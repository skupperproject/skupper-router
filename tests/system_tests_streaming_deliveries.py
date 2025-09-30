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

from proton import Message, symbol
from proton.handlers import MessagingHandler
from proton.reactor import Container

from system_test import unittest
from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, MgmtMsgProxy, TestTimeout, PollTimeout


class AddrTimer:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.check_address()


class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, mode, connection1, connection2=None, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port()}),
                ('address', {'prefix': 'cl', 'distribution': 'closest'}),
                connection1
            ]

            if connection2:
                config.append(connection2)
            if extra:
                if extra.__class__ == list:
                    for e in extra:
                        config.append(e)
                else:
                    config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        inter_router_port_A = cls.tester.get_port()
        inter_router_port_B = cls.tester.get_port()
        edge_port_A         = cls.tester.get_port()
        edge_port_B         = cls.tester.get_port()

        router('INT.A', 'interior',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_A}),
               ('listener', {'role': 'edge', 'port': edge_port_A}))
        router('INT.B', 'interior',
               ('listener', {'role': 'inter-router', 'port': inter_router_port_B}),
               ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port_A}),
               ('listener', {'role': 'edge', 'port': edge_port_B}))
        router('EA1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EA2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EB1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))
        router('EB2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')

        cls.inta = cls.routers[0].addresses[0]
        cls.intb = cls.routers[1].addresses[0]
        cls.ea1  = cls.routers[2].addresses[0]
        cls.ea2  = cls.routers[3].addresses[0]
        cls.eb1  = cls.routers[4].addresses[0]
        cls.eb2  = cls.routers[5].addresses[0]

    def test_010_balanced_same_interior_one_stream(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0)], self.inta, 'streaming.010')
        test.run()
        self.assertIsNone(test.error)

    def test_020_balanced_same_interior_two_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0)], self.inta, 'streaming.020')
        test.run()
        self.assertIsNone(test.error)

    def test_030_balanced_different_interiors_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.intb, 0, 1), (self.intb, 0, 1)], self.inta, 'streaming.030')
        test.run()
        self.assertIsNone(test.error)

    def test_040_balanced_interior_local_edge_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.inta, 'streaming.040')
        test.run()
        self.assertIsNone(test.error)

    def test_050_balanced_local_edge_interior_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.ea1, 'streaming.050')
        test.run()
        self.assertIsNone(test.error)

    def test_060_balanced_remote_edge_interior_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 0, 1), (self.inta, 0, 1), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.eb1, 'streaming.060')
        test.run()
        self.assertIsNone(test.error)

    def test_110_closest_same_interior_one_stream(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0)], self.inta, 'cl.streaming.110')
        test.run()
        self.assertIsNone(test.error)

    def test_120_closest_same_interior_two_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0)], self.inta, 'cl.streaming.120')
        test.run()
        self.assertIsNone(test.error)

    def test_130_closest_different_interiors_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.intb, 0, 1), (self.intb, 0, 1)], self.inta, 'cl.streaming.130')
        test.run()
        self.assertIsNone(test.error)

    def test_140_closest_interior_local_edge_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.inta, 'cl.streaming.140')
        test.run()
        self.assertIsNone(test.error)

    def test_150_closest_local_edge_interior_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.ea1, 'cl.streaming.150')
        test.run()
        self.assertIsNone(test.error)

    def test_160_closest_remote_edge_interior_four_streams(self):
        test = InterleavedStreamsTest([(self.inta, 0, 1), (self.inta, 0, 1), (self.ea1, 1, 0), (self.ea2, 1, 0)], self.eb1, 'cl.streaming.160')
        test.run()
        self.assertIsNone(test.error)


class RouterTestAutoLink(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTestAutoLink, cls).setUpClass()

        def router(name, mode, connection1, connection2=None, extra=None, args=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port()}),
                ('address', {'prefix': 'cl', 'distribution': 'closest'}),
                connection1
            ]

            if connection2:
                config.append(connection2)
            if extra:
                if extra.__class__ == list:
                    for e in extra:
                        config.append(e)
                else:
                    config.append(extra)
            config = Qdrouterd.Config(config)
            #print(name)
            #print(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=args or []))

        cls.routers = []

        normal_port_A = cls.tester.get_port()

        router('INT.A', 'interior',
               ('listener', {'role': 'normal', 'port': normal_port_A, 'stripAnnotations': 'no'}))
        router('INT.B', 'interior',
               ('connector', {'name': 'autoconnect', 'role': 'route-container', 'port': normal_port_A, 'stripAnnotations': 'no'}),
               ('autoLink', {'address': 'streaming.auto.a-to-b', 'direction': 'in', 'connection': 'autoconnect'}),
               ('autoLink', {'address': 'streaming.auto.b-to-a', 'direction': 'out', 'connection': 'autoconnect'}))

        cls.inta = cls.routers[0].addresses[0]
        cls.intb = cls.routers[1].addresses[0]

    def test_010_balanced_one_stream_autolink_in(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0)], self.intb, 'streaming.auto.a-to-b')
        test.run()
        self.assertIsNone(test.error)

    def test_011_balanced_two_streams_autolink_in(self):
        test = InterleavedStreamsTest([(self.inta, 1, 0), (self.inta, 1, 0)], self.intb, 'streaming.auto.a-to-b')
        test.run()
        self.assertIsNone(test.error)

    def test_020_balanced_one_stream_autolink_out(self):
        test = InterleavedStreamsTest([(self.intb, 1, 0)], self.inta, 'streaming.auto.b-to-a')
        test.run()
        self.assertIsNone(test.error)

    def test_021_balanced_two_streams_autolink_out(self):
        test = InterleavedStreamsTest([(self.intb, 1, 0), (self.intb, 1, 0)], self.inta, 'streaming.auto.b-to-a')
        test.run()
        self.assertIsNone(test.error)


class StreamSender:
    def __init__(self, container, host_tuple, addr):
        self.container    = container
        self.host         = host_tuple[0]
        self.wait_local   = host_tuple[1]
        self.wait_remote  = host_tuple[2]
        self.addr         = addr
        self.proxy        = None
        self.ready_query  = False
        self.ready_stream = False

        #print("  NEW STREAM SENDER (%d, %d)" % (self.wait_local, self.wait_remote))

        self.conn           = self.container.connect(self.host)
        self.query_sender   = self.container.create_sender(self.conn, "$management")
        self.reply_receiver = self.container.create_receiver(self.conn, None, dynamic=True)
        self.stream_sender  = self.container.create_sender(self.conn, self.addr)
        self.stream_sender.target.capabilities.put_object(symbol("qd.streaming-deliveries"))
        self.fragment_id    = 2

        self.conn.stream_sender = self

    def close(self):
        self.conn.close()

    def is_ready_to_query(self):
        result = self.ready_query
        self.ready_query = False
        return result

    def is_ready_to_stream(self):
        return self.ready_stream

    def find_stats(self, response):
        for record in response.results:
            if record.name == 'M' + self.addr:
                return (record.subscriberCount, record.remoteCount)
        return (0, 0)

    def query_stats(self):
        if self.proxy:
            msg = self.proxy.query_addresses()
            self.query_sender.send(msg)
            #print("  SENT QUERY")

    def start_stream(self):
        msg = Message(body=None)
        self.delivery = self.stream_sender.delivery(self.stream_sender.delivery_tag())
        encoded = msg.encode()
        self.stream_sender.stream(encoded)

    def continue_stream(self, eos=False):
        fragment = "Stream fragment %d" % self.fragment_id
        self.fragment_id += 1
        self.stream_sender.stream(bytes(fragment, 'ascii'))
        if eos:
            self.stream_sender.advance()

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.reply_addr  = event.receiver.remote_source.address
            self.proxy       = MgmtMsgProxy(self.reply_addr)
            self.ready_query = True

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            local, remote = self.find_stats(response)
            #print("  RECEIVED REPLY (%d, %d)" % (local, remote))
            if local == self.wait_local and remote == self.wait_remote:
                #print("    READY")
                self.ready_stream = True


class InterleavedStreamsTest(MessagingHandler):
    def __init__(self, sender_hosts, receiver_host, addr, fragments=10):
        super(InterleavedStreamsTest, self).__init__()
        self.sender_hosts   = sender_hosts  # List of (host, wait_local, wait_remote)
        self.receiver_host  = receiver_host
        self.addr           = addr
        self.fragment_count = fragments

        self.stream_senders     = []
        self.stream_receivers   = {}    # {receiver => fragment count}
        self.receiver_conn      = None
        self.receiver           = None
        self.error              = None
        self.poll_timer         = None
        self.n_ready_to_query   = 0
        self.n_ready_to_stream  = 0
        self.n_sent_fragments   = 0
        self.n_received_streams = 0

    def timeout(self):
        self.error = "Timeout Expired - n_ready_to_query=%d, n_ready_to_stream=%d, n_sent_fragments=%d, n_received_streams=%d" % \
            (self.n_ready_to_query, self.n_ready_to_stream, self.n_sent_fragments, self.n_received_streams)
        self.receiver_conn.close()
        for ss in self.stream_senders:
            ss.close()
        if self.poll_timer:
            self.poll_timer.cancel()

    def fail(self, error):
        self.error = error
        self.receiver_conn.close()
        for ss in self.stream_senders:
            ss.close()
        if self.poll_timer:
            self.poll_timer.cancel()
        self.timer.cancel()

    def query_stats(self):
        for ss in self.stream_senders:
            ss.query_stats()
        self.poll_timer = self.reactor.schedule(0.5, PollTimeout(self))

    def start_stream(self):
        for ss in self.stream_senders:
            ss.start_stream()
        self.n_sent_fragments = 1

    def continue_stream(self, eos=False):
        for ss in self.stream_senders:
            #print("SENDING FRAGMENT")
            ss.continue_stream(eos)

    def on_start(self, event):
        #print("STARTING")
        self.container = event.container
        self.reactor   = event.reactor
        self.timer     = self.reactor.schedule(TIMEOUT, TestTimeout(self))

        self.receiver_conn = self.container.connect(self.receiver_host, offered_capabilities=symbol("qd.streaming-links"))
        self.receiver      = self.container.create_receiver(self.receiver_conn, self.addr)

        for host in self.sender_hosts:
            self.stream_senders.append(StreamSender(self.container, host, self.addr))

    def on_link_opened(self, event):
        try:
            event.connection.stream_sender.on_link_opened(event)
            if event.connection.stream_sender.is_ready_to_query():
                self.n_ready_to_query += 1
                if self.n_ready_to_query == len(self.stream_senders):
                    #print("QUERYING STATS")
                    self.query_stats()
            return
        except:
            pass

        if event.connection == self.receiver_conn:
            if event.receiver:
                if event.link.name not in self.stream_receivers:
                    #print("NEW INCOMING STREAM LINK: %s" % event.link.name)
                    self.stream_receivers[event.link.name] = 0

    def on_delivery(self, event):
        if event.sender or event.link.name not in self.stream_receivers:
            return
        self.stream_receivers[event.link.name] += 1
        stuff = event.link.recv(1000)
        #print("RECEIVED FRAGMENT - %s%s" % (stuff, " (EOS)" if not event.delivery.partial else ""))
        was_complete = False
        if not event.delivery.partial:
            event.link.advance()
            self.n_received_streams += 1
            was_complete = True
        caught_up = 0
        for num in self.stream_receivers.values():
            if num == self.n_sent_fragments:
                caught_up += 1
        if caught_up == len(self.stream_senders):
            if self.n_sent_fragments < self.fragment_count:
                self.continue_stream(self.n_sent_fragments == self.fragment_count - 1)
                self.n_sent_fragments += 1
            else:
                if was_complete:
                    if self.n_received_streams == len(self.stream_senders):
                        self.fail(None)
                else:
                    self.fail("Expected end of stream after sent fragment count")

    def on_message(self, event):
        try:
            event.connection.stream_sender.on_message(event)
            if event.connection.stream_sender.is_ready_to_stream():
                self.n_ready_to_stream += 1
                if self.n_ready_to_stream == len(self.stream_senders):
                    #print("STARTING STREAMS")
                    self.poll_timer.cancel()
                    self.poll_timer = None
                    self.start_stream()
            return
        except Exception as e:
            print("EXCEPTION: %r" % e)
            pass

    def poll_timeout(self):
        self.query_stats()

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

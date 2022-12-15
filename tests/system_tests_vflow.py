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

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, unittest, TestTimeout
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Message

IDENTITY    = 1
START_TIME  = 3
RECORD_TYPE = 0

RT_ROUTER = 1

class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, connection, connection2=None, mode='interior'):

            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port()}),
                connection
            ]

            if connection2:
                config.append(connection2)

            config = Qdrouterd.Config(config)

            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True, cl_args=["-T"]))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port         = cls.tester.get_port()

        router('A', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
                    ('listener', {'role': 'edge', 'port': edge_port}))
        router('B', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}))
        router('C', ('connector', {'name': 'connectorToA', 'role': 'edge', 'port': edge_port}), mode='edge')

        cls.routers[0].wait_router_connected('B')
        cls.routers[1].wait_router_connected('A')
        cls.routers[0].is_edge_routers_connected()

    def test_01_attach_far_interior(self):
        test = VFlowTest(self.routers[1])
        test.run()
        self.assertIsNone(test.error)

    def test_02_attach_mid_interior(self):
        test = VFlowTest(self.routers[0])
        test.run()
        self.assertIsNone(test.error)

    def test_03_attach_edge(self):
        test = VFlowTest(self.routers[2])
        test.run()
        self.assertIsNone(test.error)


class VFlowTest(MessagingHandler):
    '''
    Open a receiver for BEACON messages on the indicated router
    Use BEACONS to find the 3 event sources (routers).  When discovered, attach sender and receiver links to the addresses for the source
    Wait for HEARTBEATS from each source.  On heartbeat, send a FLUSH
    Look for ROUTER record updates that include the startTime attribute (only included in a flush output)
    '''
    def __init__(self, host):
        super(VFlowTest, self).__init__()
        self.host   = host
        self.conn   = None
        self.error  = None
        self.beacon_receiver = None
        self.sources = {}
        self.sources_seen = 0
        self.flushes_sent = 0
        self.flushed_seen = 0

    def timeout(self):
        self.error = "Timeout Expired - sources_seen: %d, flushes_sent: %d, flushed_seen: %d" % (self.sources_seen, self.flushes_sent, self.flushed_seen)
        if self.conn:
            self.conn.close()

    def fail(self, reason):
        self.error = reason
        self.conn.close()
        self.timer.cancel()

    def setup_source(self, source_id):
        self.sources[source_id]['receiver'] = self.container.create_receiver(self.conn, self.sources[source_id]['address'])
        self.sources[source_id]['sender']   = self.container.create_sender(self.conn, self.sources[source_id]['direct'])

    def handle_records(self, body):
        for record in body:
            id          = record.get(IDENTITY, None)
            start_time  = record.get(START_TIME, None)
            record_type = record.get(RECORD_TYPE, None)
            if id in self.sources and start_time and record_type == RT_ROUTER and not self.sources[id]['sawFlushed']:
                self.sources[id]['sawFlushed'] = True
                self.flushed_seen += 1
                if self.flushed_seen == 3:
                    self.fail(None)

    def on_start(self, event):
        self.container = event.container
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.conn  = event.container.connect(self.host.addresses[0])
        self.beacon_receiver = event.container.create_receiver(self.conn, 'mc/sfe.all')

    def on_message(self, event):
        try:
            ap      = event.message.properties
            subject = event.message.subject
            source_id = ap.get('id', None) if ap else None
            if subject == 'BEACON':
                if source_id not in self.sources:
                    event_address  = ap['address']
                    direct_address = ap['direct']
                    self.sources[source_id] = {
                        'address'    : event_address,
                        'direct'     : direct_address,
                        'flushed'    : False,
                        'sawFlushed' : False,
                    }
                    self.setup_source(source_id)
                    self.sources_seen += 1
            elif subject == 'HEARTBEAT':
                if source_id in self.sources and not self.sources[source_id]['flushed']:
                    self.sources[source_id]['sender'].send(Message(subject='FLUSH'))
                    self.sources[source_id]['flushed'] = True
                    self.flushes_sent += 1
            elif subject == 'RECORD':
                self.handle_records(event.message.body)
        except Exception as reason:
            self.fail("on_message exception: %r" % reason)

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

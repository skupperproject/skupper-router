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

#
# Various codepoints from the VanFlow specification. These are
# defined in vanflow.h
#

# Attribute constants from the VanFlow specification
IDENTITY    = 1
START_TIME  = 3
END_TIME    = 3
RECORD_TYPE = 0
LOG_SEVERITY = 48
LOG_TEXT = 49
SOURCE_FILE = 50
SOURCE_LINE = 51

# Record identifier constants from the VanFlow specification
RT_ROUTER = 0x01
RT_LOG = 0x0E

# Log event severity from the VanFlow specification
SEVERITY_DEBUG = 0
SEVERITY_INFO = 1
SEVERITY_WARNING = 2
SEVERITY_ERROR = 3
SEVERITY_CRITICAL = 4


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


class VFlowEventTest(TestCase):
    """
    Verify log messages of high severity cause VFLOW_RECORD_EVENT
    """
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(VFlowEventTest, cls).setUpClass()

        config = Qdrouterd.Config([
            ('router', {'mode': 'interior', 'id': "vflowEventRouter"}),
            ('listener', {'port': cls.tester.get_port()}),
        ])

        cls.router = cls.tester.qdrouterd("vflowEventRouter", config, wait=True, cl_args=["-T"])

    def test_log_events(self):
        """
        Force the router to issue logs at various severity levels and verify
        events are generated as expected
        """
        test = VFlowEventsGrabber(self.router)
        test.run()
        self.assertIsNone(test.error)


class VFlowEventsGrabber(MessagingHandler):
    '''
    Open a receiver for BEACON messages on the indicated router
    Use BEACONS to find the 3 event sources (routers).  When discovered, attach sender and receiver links to the addresses for the source
    Wait for HEARTBEATS from each source.  On heartbeat, send a FLUSH
    Look for ROUTER record updates that include the startTime attribute (only included in a flush output)
    '''
    def __init__(self, host):
        super(VFlowEventsGrabber, self).__init__()
        self.host   = host
        self.conn   = None
        self.error  = None
        self.beacon_receiver = None
        self.event_receiver = None
        self.log_sender = None
        self.logs_issued = 0
        self.received_events = set()

    def timeout(self):
        self.error = f"Timeout Expired: issued={self.logs_issued} received={self.received_events}"
        if self.conn:
            self.conn.close()

    def fail(self, reason):
        self.error = reason
        self.conn.close()
        self.timer.cancel()

    def on_start(self, event):
        self.container = event.container
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.conn  = event.container.connect(self.host.addresses[0])
        self.beacon_receiver = event.container.create_receiver(self.conn, 'mc/sfe.all')

    def on_link_opened(self, event):
        if event.link == self.event_receiver:
            self.log_sender = event.container.create_sender(self.conn,
                                                            "io.skupper.router.router/test/log")

    def on_sendable(self, event):
        msg = None
        # Do not change this order! The test expects that DEBUG and INFO are
        # the first logs issued.
        if self.logs_issued == 0:
            msg = Message(subject="DEBUG")
        elif self.logs_issued == 1:
            msg = Message(subject="INFO")
        elif self.logs_issued == 2:
            msg = Message(subject="WARNING")
        elif self.logs_issued == 3:
            msg = Message(subject="ERROR")
        elif self.logs_issued == 4:
            msg = Message(subject="CRITICAL")

        if msg is not None:
            self.logs_issued += 1
            self.log_sender.send(msg)

    def on_message(self, event):
        try:
            subject = event.message.subject
            if subject == 'BEACON':
                if self.event_receiver is None:
                    addr = f"{event.message.properties['address']}.logs"
                    self.event_receiver = self.container.create_receiver(self.conn, addr)
            elif subject == 'RECORD':
                self.handle_records(event.message.body)
        except Exception as reason:
            self.fail("on_message exception: %r" % reason)

    def handle_records(self, body):
        for record in body:
            record_type = record.get(RECORD_TYPE, None)
            if record_type == RT_LOG:
                severity = record.get(LOG_SEVERITY, None)
                if severity is None:
                    self.fail("Severity field not set!")
                    return

                # currently only those logs with severity <= WARNING generate
                # vanflow events. This test expects that the other severities
                # are filtered
                if severity not in [SEVERITY_WARNING, SEVERITY_ERROR,
                                    SEVERITY_CRITICAL]:
                    self.fail(f"Unexpected log event severity: {severity}")
                    return

                if severity in self.received_events:
                    self.fail(f"Duplicate severity received: {severity}")
                    return

                self.received_events.add(severity)
                for attribute in [LOG_TEXT, SOURCE_FILE, SOURCE_LINE,
                                  START_TIME, END_TIME]:
                    # these attributes are mandatory since the default log
                    # configuration includes timestamps and source file info
                    if record.get(attribute, None) is None:
                        self.fail(f"attribute {attribute} not found")
                        return

                # expect begin and end timestamps are the same
                if record[START_TIME] != record[END_TIME]:
                    self.fail("expected timestamps to be equal")
                    return

        if len(self.received_events) == 3:
            self.fail(None)  # test succeeded

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

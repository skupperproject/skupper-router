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

# must include interrouter_msg BEFORE any proton modules because it
# monkey-patches them
from interrouter_msg import InterRouterMessage, RouterAnnotationsSection
from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, unittest
from system_test import TestTimeout

from proton import Data
from proton.handlers import MessagingHandler
from proton.reactor import Container


class MessageAnnotations(MessagingHandler):
    """Simple client for sending messages across the router network"""

    def __init__(self, msg, in_router, out_router, source, target=None):
        super(MessageAnnotations, self).__init__()
        self.in_router = in_router
        self.out_router = out_router
        self.send_msg   = msg
        self.source = source
        self.target = target
        self.test_timer = None
        self.sender     = None
        self.receiver   = None
        self.recv_msg   = None
        self.error      = None
        self.msg_sent   = False

    def run(self) :
        Container(self).run()

    def bail(self, error):
        self.error = error
        if self.error:
            print("message annotations failure: %s" % self.error, flush=True)
        self.send_conn.close()
        self.recv_conn.close()
        self.test_timer.cancel()

    def timeout(self):
        self.bail("Timeout Expired")

    def on_start(self, event):
        self.test_timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_conn = event.container.connect(self.in_router)
        self.recv_conn = event.container.connect(self.out_router)
        self.receiver = event.container.create_receiver(self.recv_conn,
                                                        source=self.source)

    def on_link_opened(self, event):
        if event.link == self.receiver:
            self.sender = event.container.create_sender(self.send_conn,
                                                        target=self.target)

    def send(self):
        if not self.msg_sent:
            self.sender.send(self.send_msg)
            self.msg_sent = True

    def on_sendable(self, event):
        self.send()

    def on_message(self, event):
        if self.recv_msg is None:
            self.recv_msg = event.message
            self.bail(None)

    def on_released(self, event):
        # routes not yet stable?
        self.msg_sent = False
        if self.sender.credit > 0:
            self.send()

    def on_modified(self, event):
        # routes not yet stable?
        self.msg_sent = False
        if self.sender.credit > 0:
            self.send()

    def on_rejected(self, event):
        self.bail("message unexpectantly REJECTED")


class RouterAnnotationsTest(TestCase):
    """Verify the proper handling of per-message router annotations"""
    @classmethod
    def setUpClass(cls):
        """
        """
        super(RouterAnnotationsTest, cls).setUpClass()

        def router(name, mode, extra_config):
            config = [

                ('router', {'id': name,
                            'mode': mode}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port(),
                              'stripAnnotations': 'no',
                              'maxFrameSize': '2048'}),

                ('address', {'prefix': 'closest', 'distribution': 'closest'}),
                ('address', {'prefix': 'balanced', 'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ] + extra_config

            config = Qdrouterd.Config(config)
            return cls.tester.qdrouterd(name, config, wait=True)

        inter_router_port_A = cls.tester.get_port()
        inter_router_port_B = cls.tester.get_port()
        inter_router_port_C = cls.tester.get_port()
        edge_port_A = cls.tester.get_port()
        edge_port_C = cls.tester.get_port()

        cls.RouterA = router('RouterA', 'interior',
                             [('listener', {'role': 'inter-router', 'port': inter_router_port_A}),
                              ('listener', {'role': 'edge', 'port': edge_port_A})])

        cls.RouterB = router('RouterB', 'interior',
                             [('listener', {'role': 'inter-router', 'port': inter_router_port_B}),
                              ('connector', {'role': 'inter-router', 'port': inter_router_port_A})])

        cls.RouterC = router('RouterC', 'interior',
                             [('listener', {'role': 'edge', 'port': edge_port_C}),
                              ('connector', {'role': 'inter-router', 'port': inter_router_port_B})])

        cls.EdgeA = router('EdgeA', 'edge',
                           [('connector', {'role': 'edge', 'port': edge_port_A})])

        cls.EdgeC = router('EdgeC', 'edge',
                           [('connector', {'role': 'edge', 'port': edge_port_C})])

        cls.RouterB.wait_router_connected('RouterA')
        cls.RouterB.wait_router_connected('RouterC')
        cls.RouterA.wait_router_connected('RouterC')
        cls.RouterC.wait_router_connected('RouterA')

    def test_01_no_ra_interior_interior(self):
        """Validate RA between routers"""
        _name = "test_01_no_ra_interior_interior"
        msg = InterRouterMessage(body=_name)
        test = MessageAnnotations(msg,
                                  self.RouterA.addresses[0],
                                  self.RouterC.addresses[0],
                                  "closest/%s" % _name,
                                  "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertEqual("0/RouterA", ra.ingress_router)
        expected = ["0/RouterA", "0/RouterB", "0/RouterC"]
        self.assertEqual(expected, ra.trace)

    def test_02_no_ra_interior_edge(self):
        """Validate RA between interior and edge"""
        _name = "test_02_no_ra_interior_edge"
        msg = InterRouterMessage(body=_name)
        test = MessageAnnotations(msg,
                                  self.RouterA.addresses[0],
                                  self.EdgeC.addresses[0],
                                  "closest/%s" % _name,
                                  "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertIsNone(ra.ingress_router)
        self.assertEqual([], ra.trace)

    def test_03_no_ra_edge_interior(self):
        """Validate RA between edge and interior"""
        _name = "test_02_no_ra_edge_interior"
        msg = InterRouterMessage(body=_name)
        test = MessageAnnotations(msg,
                                  self.EdgeC.addresses[0],
                                  self.RouterA.addresses[0],
                                  "closest/%s" % _name,
                                  "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertEqual("0/RouterC", ra.ingress_router)
        expected = ['0/RouterC', '0/RouterB', '0/RouterA']
        self.assertEqual(expected, ra.trace)

    def test_04_to_override(self):
        """Validate RA to-override"""
        _name = "test_04_to_override"
        ra = RouterAnnotationsSection(flags=0x80,
                                      to_override="closest/%s" % _name,
                                      ingress_router="0/DoesNotExist")
        msg = InterRouterMessage(router_annotations=ra,
                                 body=_name)
        msg.address = "closest/NoSuchAddress"
        # anonymous sender:
        test = MessageAnnotations(msg,
                                  self.RouterA.addresses[0],
                                  self.RouterC.addresses[0],
                                  "closest/%s" % _name,
                                  None)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0x80, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertEqual("0/DoesNotExist", ra.ingress_router)
        expected = ["0/RouterA", "0/RouterB", "0/RouterC"]
        self.assertEqual(expected, ra.trace)

    def test_05_to_override_edge(self):
        """Validate RA to-override across edges"""
        _name = "test_05_to_override_edge"
        ra = RouterAnnotationsSection(flags=0x40,
                                      to_override="closest/%s" % _name,
                                      ingress_router="0/ThisAlsoDoesNotExist")
        msg = InterRouterMessage(router_annotations=ra,
                                 body=_name)
        msg.address = "closest/NoSuchAddressAtAll"
        # anonymous sender:
        test = MessageAnnotations(msg,
                                  self.EdgeA.addresses[0],
                                  self.EdgeC.addresses[0],
                                  "closest/%s" % _name,
                                  None)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0x40, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertIsNone(ra.ingress_router)
        self.assertEqual([], ra.trace)

    def test_06_default_ra_interior_interior(self):
        """Validate default RA between routers"""
        _name = "test_06_default_ra_interior_interior"
        ra = RouterAnnotationsSection()
        msg = InterRouterMessage(router_annotations=ra,
                                 body=_name)
        test = MessageAnnotations(msg,
                                  self.RouterA.addresses[0],
                                  self.RouterC.addresses[0],
                                  "closest/%s" % _name,
                                  "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)
        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertEqual("0/RouterA", ra.ingress_router)
        expected = ["0/RouterA", "0/RouterB", "0/RouterC"]
        self.assertEqual(expected, ra.trace)

    def test_07_ra_big_interior_interior(self):
        """Pad out the RA and verify msg headers"""
        _name = "test_07_ra_big_interior_interior"
        in_ra = RouterAnnotationsSection()
        in_ra.ingress_router = "O/NameTooLong" + "X" * 256
        da = {"DA-KEY": "DA-VALUE"}
        ma = {"MA-KEY": "MA-VALUE"}
        props = {"PROPS-KEY": "PROPS-VALUE"}
        subject = "amqp:/dummy/subject/field"

        msg = InterRouterMessage(router_annotations=in_ra,
                                 body=_name)
        msg.instructions = da
        msg.annotations = ma
        msg.properties = props
        msg.subject = subject
        test = MessageAnnotations(msg,
                                  self.RouterA.addresses[0],
                                  self.RouterC.addresses[0],
                                  "closest/%s" % _name,
                                  "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual(_name, test.recv_msg.body)

        self.assertEqual(subject, test.recv_msg.subject)
        self.assertEqual(da, test.recv_msg.instructions)
        self.assertEqual(ma, test.recv_msg.annotations)
        self.assertEqual(props, test.recv_msg.properties)

        ra = test.recv_msg.router_annotations
        self.assertIsNotNone(ra, "No router annotations present!")
        self.assertEqual(0, ra.flags)
        self.assertEqual("closest/%s" % _name, ra.to_override)
        self.assertEqual(in_ra.ingress_router, ra.ingress_router)
        expected = ["0/RouterA", "0/RouterB", "0/RouterC"]
        self.assertEqual(expected, ra.trace)


class InvalidMessageAnnotations(MessagingHandler):
    """Simple client for sending invalid router annotations"""

    def __init__(self, encoded_msg, in_router, out_router, address):
        super(InvalidMessageAnnotations, self).__init__()
        self.in_router = in_router
        self.out_router = out_router
        self.encoded_msg = encoded_msg
        self.address = address
        self.test_timer = None
        self.sender = None
        self.receiver = None
        self.error = None
        self.msg_sent = False

    def run(self) :
        Container(self).run()

    def bail(self, error):
        self.error = error
        if self.error:
            print("message annotations failure: %s" % self.error, flush=True)
        self.send_conn.close()
        self.recv_conn.close()
        self.test_timer.cancel()

    def timeout(self):
        self.bail("Timeout Expired")

    def on_start(self, event):
        self.test_timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.send_conn = event.container.connect(self.in_router)
        self.recv_conn = event.container.connect(self.out_router)
        self.receiver = event.container.create_receiver(self.recv_conn,
                                                        source=self.address)

    def on_link_opened(self, event):
        if event.link == self.receiver:
            self.sender = event.container.create_sender(self.send_conn,
                                                        target=self.address)

    def send(self):
        if not self.msg_sent:
            dlv = self.sender.delivery(self.sender.delivery_tag())
            self.sender.stream(self.encoded_msg)
            self.sender.advance()
            self.msg_sent = True

    def on_sendable(self, event):
        self.send()

    def on_message(self, event):
        self.bail("invalid message was not rejected as expected")

    def on_released(self, event):
        # routes not yet stable?
        self.msg_sent = False
        if self.sender.credit > 0:
            self.send()

    def on_modified(self, event):
        # routes not yet stable?
        self.msg_sent = False
        if self.sender.credit > 0:
            self.send()

    def on_rejected(self, event):
        # success!
        disp = event.delivery.remote
        self.reject_name = disp.condition.name
        self.reject_desc = disp.condition.description
        self.bail(None)


class InvalidRouterAnnotationsTest(TestCase):
    """Try to crash the router!"""
    @classmethod
    def setUpClass(cls):
        """
        """
        super(InvalidRouterAnnotationsTest, cls).setUpClass()

        def router(name, mode, extra_config):
            config = [

                ('router', {'id': name,
                            'mode': mode}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port(),
                              'stripAnnotations': 'no',
                              'maxFrameSize': '2048'}),

                ('address', {'prefix': 'closest', 'distribution': 'closest'}),
                ('address', {'prefix': 'balanced', 'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ] + extra_config

            config = Qdrouterd.Config(config)
            return cls.tester.qdrouterd(name, config, wait=True)

        inter_router_port = cls.tester.get_port()

        cls.RouterA = router('RouterA', 'interior',
                             [('listener', {'role': 'inter-router', 'port': inter_router_port})])

        cls.RouterB = router('RouterB', 'interior',
                             [('connector', {'role': 'inter-router', 'port': inter_router_port})])

    def test_01_bogus_ra(self):
        """Invalid ra section"""
        _name = "test_01_bogus_ra"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_map()
        ras.enter()
        ras.put_symbol("Bad")
        ras.put_string("RA")
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:decode-error", test.reject_name)

    def test_02_bogus_flags(self):
        """Invalid flags field"""
        _name = "test_02_bogus_flags"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_list()
        ras.enter()
        ras.put_symbol("Bad flag")
        ras.put_null()
        ras.put_null()
        ras.put_sequence([])
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:invalid-field", test.reject_name)

    def test_03_bogus_to_override(self):
        """Invalid to override field"""
        _name = "test_03_bogus_to_override"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_list()
        ras.enter()
        ras.put_uint(0)
        ras.put_int(-1)
        ras.put_null()
        ras.put_sequence([])
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:invalid-field", test.reject_name)

    def test_04_bogus_ingress_router(self):
        """Invalid ingress router field"""
        _name = "test_04_bogus_ingress_router"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_list()
        ras.enter()
        ras.put_uint(0)
        ras.put_string("to-override")
        ras.put_int(-1)
        ras.put_sequence([])
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:invalid-field", test.reject_name)

    def test_05_bogus_trace(self):
        """Invalid trace field"""
        _name = "test_05_bogus_trace"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_list()
        ras.enter()
        ras.put_uint(0)
        ras.put_string("to-override")
        ras.put_string("0/ingress-router")
        ras.put_string("not a trace list")
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:invalid-field", test.reject_name)

    def test_06_bogus_ra_too_long(self):
        """Invalid size of RA"""
        _name = "test_06_bogus_ra_too_long"
        encoded = RouterAnnotationsSection.SECTION_HEADER
        ras = Data()
        ras.put_list()
        ras.enter()
        ras.put_uint(0)
        ras.put_string("to-override")
        ras.put_string("0/ingress-router")
        ras.put_sequence([])
        ras.put_null()
        ras.exit()
        encoded += ras.encode()
        # body holding uint0
        encoded += b'\x00\x53\x77\x43'

        test = InvalidMessageAnnotations(encoded,
                                         self.RouterA.addresses[0],
                                         self.RouterB.addresses[0],
                                         "closest/%s" % _name)
        test.run()
        self.assertIsNone(test.error)
        self.assertEqual("amqp:invalid-field", test.reject_name)


if __name__ == '__main__':
    unittest.main(main_module())

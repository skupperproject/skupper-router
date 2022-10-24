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
# This file is a library of test classes to be imported and used by external test cases.
# No test cases shall be poaced in this file.
#

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from system_test import TestTimeout, Logger
#from system_test import TIMEOUT
TIMEOUT = 10.0

class CustomTimeout:
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        message = Message(body="Test Message")
        message.address = self.parent.address
        self.parent.sender.send(message)
        self.parent.cancel_custom()


class DynamicAddressTest(MessagingHandler):
    '''
    Open connections to a sender and receiver host.
    Create a receiver link on the receiver host with a dynamic address.
    Create a sender link on the sender host using the dynamically generated address.
    Send messages on the sender link.
    Verify reception of all of the addresses on the receiver link.
    '''
    def __init__(self, receiver_host, sender_host, anon_sender=False, message_count=300):
        super(DynamicAddressTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host

        self.receiver_conn = None
        self.sender_conn   = None
        self.receiver      = None
        self.address       = None
        self.anon_sender   = anon_sender
        self.count         = message_count
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.error         = None

    def timeout(self):
        self.error = "Timeout Expired - n_sent=%d n_rcvd=%d addr=%s" % (self.n_sent, self.n_rcvd, self.address)
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer         = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.receiver:
            self.address = self.receiver.remote_source.address
            self.sender  = event.container.create_sender(self.sender_conn, None if self.anon_sender else self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            msg = Message(body="Message %d" % self.n_sent)
            if self.anon_sender:
                msg.address = self.address
            self.sender.send(msg)
            self.n_sent += 1

    def on_message(self, event):
        self.n_rcvd += 1
        if self.n_rcvd == self.count:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileAddressAnonymousTest(MessagingHandler):
    """
    Attach a receiver to the interior and an anonymous sender to the edge router
    In a non-anonymous sender scenario, the sender will never be given credit
    to send until a receiver on the same address shows up . Since this
    is an anonymous sender, credit is given instantly and the sender starts
    sending immediately.

    This test will first send 3 messages with a one second interval to make
    sure receiver is available. Then it will fire off 300 messages
    After dispositions are received for the 300 messages, it will close the
    receiver and send 50 more messages. These 50 messages should be released
    or modified.
    """

    def __init__(self, receiver_host, sender_host, address, large_msg=False):
        super(MobileAddressAnonymousTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host = sender_host
        self.receiver_conn = None
        self.sender_conn = None
        self.receiver = None
        self.sender = None
        self.error = None
        self.n_sent = 0
        self.n_rcvd = 0
        self.address = address
        self.ready = False
        self.custom_timer = None
        self.num_msgs = 100
        self.extra_msgs = 50
        self.n_accepted = 0
        self.n_modified = 0
        self.n_released = 0
        self.error = None
        self.max_attempts = 10
        self.num_attempts = 0
        self.test_started = False
        self.large_msg = large_msg
        if self.large_msg:
            self.body = "0123456789101112131415" * 5000
            self.properties = {'big field': 'X' * 3200}

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, self.address)
        # This is an anonymous sender.
        self.sender        = event.container.create_sender(self.sender_conn)

    def cancel_custom(self):
        self.custom_timer.cancel()

    def timeout(self):
        if self.ready:
            self.error = "Timeout Expired - n_sent=%d n_accepted=%d n_modified=%d n_released=%d" % (
                self.n_sent,  self.n_accepted, self.n_modified, self.n_released)
        else:
            self.error = "Did not get a settlement from the receiver. The test cannot be started until " \
                         "a settlement to a test message is received"
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_sendable(self, event):
        if not self.test_started and event.sender == self.sender:
            message = Message(body="Test Message")
            message.address = self.address
            self.sender.send(message)
            self.num_attempts += 1
            self.test_started = True

    def on_message(self, event):
        if event.receiver == self.receiver:
            if self.ready:
                self.n_rcvd += 1

    def on_link_closed(self, event):
        # The receiver has closed. We will send messages again and
        # make sure they are released.
        if event.receiver == self.receiver:
            for i in range(self.extra_msgs):
                if self.large_msg:
                    message = Message(body=self.body, properties=self.properties)
                else:
                    message = Message(body="Message %d" % self.n_sent)
                message.address = self.address
                self.sender.send(message)
                self.n_sent += 1

    def on_settled(self, event):
        rdisp = str(event.delivery.remote_state)
        if rdisp == "RELEASED" and not self.ready:
            if self.num_attempts < self.max_attempts:
                self.custom_timer = event.reactor.schedule(1, CustomTimeout(self))
                self.num_attempts += 1
        elif rdisp == "ACCEPTED" and not self.ready:
            self.ready = True
            for i in range(self.num_msgs):
                if self.large_msg:
                    message = Message(body=self.body, properties=self.properties)
                else:
                    message = Message(body="Message %d" % self.n_sent)
                message.address = self.address
                self.sender.send(message)
                self.n_sent += 1
        elif rdisp == "ACCEPTED" and self.ready:
            self.n_accepted += 1
            if self.n_accepted == self.num_msgs:
                # Close the receiver after sending 300 messages
                self.receiver.close()
        elif rdisp == "RELEASED" and self.ready:
            self.n_released += 1
        elif rdisp == "MODIFIED" and self.ready:
            self.n_modified += 1

        if self.num_msgs == self.n_accepted and self.extra_msgs == self.n_released + self.n_modified:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileAddressTest(MessagingHandler):
    """
    From a single container create a sender and a receiver connection.
    Send a batch of normal messages that should be accepted by the receiver.
    Close the receiver but not the receiver connection and then
      send an extra batch of messages that should be released or modified.
    Success is when message disposition counts add up correctly.
    """

    def __init__(self, receiver_host, sender_host, address):
        super(MobileAddressTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host
        self.address       = address

        self.receiver_conn = None
        self.sender_conn   = None

        self.receiver      = None
        self.sender        = None

        self.logger        = Logger()

        self.normal_count  = 300
        self.extra_count   = 50
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.n_accepted    = 0
        self.n_rel_or_mod  = 0
        self.error         = None
        self.warning       = False

    def fail_exit(self, title):
        self.error = title
        self.logger.log("MobileAddressTest result:ERROR: %s" % title)
        self.logger.log("address %s     " % self.address)
        self.logger.log("n_sent       = %d. Expected total:%d normal=%d, extra=%d" %
                        (self.n_sent, (self.normal_count + self.extra_count), self.normal_count, self.extra_count))
        self.logger.log("n_rcvd       = %d. Expected %d" % (self.n_rcvd,       self.normal_count))
        self.logger.log("n_accepted   = %d. Expected %d" % (self.n_accepted,   self.normal_count))
        self.logger.log("n_rel_or_mod = %d. Expected %d" % (self.n_rel_or_mod, self.extra_count))
        self.timer.cancel()
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_timer_task(self, event):
        self.fail_exit("Timeout Expired")

    def on_start(self, event):
        self.logger.log("on_start address=%s" % self.address)
        self.timer         = event.reactor.schedule(TIMEOUT, self)
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, self.address)
        self.sender        = event.container.create_sender(self.sender_conn, self.address)

    def on_sendable(self, event):
        self.logger.log("on_sendable")
        if event.sender == self.sender:
            self.logger.log("on_sendable sender")
            while self.n_sent < self.normal_count:
                # send the normal messages
                message = Message(body="Message %d" % self.n_sent)
                self.sender.send(message)
                self.logger.log("on_sendable sender: send message %d: %s" % (self.n_sent, message))
                self.n_sent += 1
        elif event.receiver == self.receiver:
            self.logger.log("on_sendable receiver: WARNING unexpected callback for receiver")
            self.warning = True
        else:
            self.fail_exit("on_sendable not for sender nor for receiver")

    def on_message(self, event):
        self.logger.log("on_message")
        if event.receiver == self.receiver:
            self.n_rcvd += 1
            self.logger.log("on_message receiver: receiver message %d" % (self.n_rcvd))
        else:
            self.logger.log("on_message: WARNING callback not for test receiver.")

    def on_settled(self, event):
        # Expect all settlement events at sender as remote state
        self.logger.log("on_settled")
        rdisp = str(event.delivery.remote_state)
        ldisp = str(event.delivery.local_state)
        if event.sender == self.sender:
            if rdisp is None:
                self.logger.log("on_settled: WARNING: sender remote delivery state is None. Local state = %s." % ldisp)
            elif rdisp == "ACCEPTED":
                self.n_accepted += 1
                self.logger.log("on_settled sender: ACCEPTED %d (of %d)" %
                                (self.n_accepted, self.normal_count))
            elif rdisp in ('RELEASED', 'MODIFIED'):
                self.n_rel_or_mod += 1
                self.logger.log("on_settled sender: %s %d (of %d)" %
                                (rdisp, self.n_rel_or_mod, self.extra_count))
            else:
                self.logger.log("on_settled sender: WARNING unexpected settlement: %s, n_accepted: %d, n_rel_or_mod: %d" %
                                (rdisp, self.n_accepted, self.n_rel_or_mod))
                self.warning = True

            if self.n_sent == self.normal_count and self.n_accepted == self.normal_count:
                # All normal messages are accounted.
                # Close receiver and launch extra messages into the router network.
                self.logger.log("on_settled sender: normal messages all accounted. receiver.close() then send extra messages")
                self.receiver.close()
                for i in range(self.extra_count):
                    message = Message(body="Message %d" % self.n_sent)
                    self.sender.send(message)
                    # Messages must be blasted to get them into the network before news
                    # of the receiver closure is propagated back to EA1.
                    # self.logger.log("on_settled sender: send extra message %d: %s" % (self.n_sent, message))
                    self.n_sent += 1

            if self.n_accepted > self.normal_count:
                self.fail_exit("Too many messages were accepted")
            if self.n_rel_or_mod > self.extra_count:
                self.fail_exit("Too many messages were released or modified")

            if self.n_rel_or_mod == self.extra_count:
                # All extra messages are accounted. Exit with success.
                result = "SUCCESS" if not self.warning else "WARNING"
                self.logger.log("MobileAddressTest result:%s" % result)
                self.timer.cancel()
                self.receiver_conn.close()
                self.sender_conn.close()

        elif event.receiver == self.receiver:
            self.logger.log("on_settled receiver: WARNING unexpected on_settled. remote: %s, local: %s" % (rdisp, ldisp))
            self.warning = True

    def run(self):
        Container(self).run()


class MobileAddressOneSenderTwoReceiversTest(MessagingHandler):
    def __init__(self, receiver1_host, receiver2_host, sender_host, address):
        super(MobileAddressOneSenderTwoReceiversTest, self).__init__()
        self.receiver1_host = receiver1_host
        self.receiver2_host = receiver2_host
        self.sender_host = sender_host
        self.address = address

        # One sender connection and two receiver connections
        self.receiver1_conn = None
        self.receiver2_conn = None
        self.sender_conn   = None

        self.receiver1 = None
        self.receiver2 = None
        self.sender = None

        self.count = 300
        self.rel_count = 50
        self.n_rcvd1 = 0
        self.n_rcvd2 = 0
        self.n_sent = 0
        self.n_settled = 0
        self.n_released = 0
        self.error = None
        self.timer = None
        self.all_msgs_received = False
        self.recvd_msg_bodies = dict()
        self.dup_msg = None

    def timeout(self):
        if self.dup_msg:
            self.error = "Duplicate message %s received " % self.dup_msg
        else:
            self.error = "Timeout Expired - n_sent=%d n_rcvd=%d n_settled=%d n_released=%d addr=%s" % \
                         (self.n_sent, (self.n_rcvd1 + self.n_rcvd2), self.n_settled, self.n_released, self.address)

        self.receiver1_conn.close()
        self.receiver2_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))

        # Create two receivers
        self.receiver1_conn = event.container.connect(self.receiver1_host)
        self.receiver2_conn = event.container.connect(self.receiver2_host)
        self.receiver1 = event.container.create_receiver(self.receiver1_conn,
                                                         self.address)
        self.receiver2 = event.container.create_receiver(self.receiver2_conn,
                                                         self.address)

        # Create one sender
        self.sender_conn = event.container.connect(self.sender_host)
        self.sender = event.container.create_sender(self.sender_conn,
                                                    self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            self.sender.send(Message(body="Message %d" % self.n_sent))
            self.n_sent += 1

    def on_message(self, event):
        if self.recvd_msg_bodies.get(event.message.body):
            self.dup_msg = event.message.body
            self.timeout()
        else:
            self.recvd_msg_bodies[event.message.body] = event.message.body

        if event.receiver == self.receiver1:
            self.n_rcvd1 += 1
        if event.receiver == self.receiver2:
            self.n_rcvd2 += 1

        if self.n_sent == self.n_rcvd1 + self.n_rcvd2:
            self.all_msgs_received = True

    def on_settled(self, event):
        self.n_settled += 1
        if self.n_settled == self.count:
            self.receiver1.close()
            self.receiver2.close()
            for i in range(self.rel_count):
                self.sender.send(Message(body="Message %d" % self.n_sent))
                self.n_sent += 1

    def on_released(self, event):
        self.n_released += 1
        if self.n_released == self.rel_count and self.all_msgs_received:
            self.receiver1_conn.close()
            self.receiver2_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileAddressMulticastTest(MessagingHandler):
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=False,
                 anon_sender=False):
        super(MobileAddressMulticastTest, self).__init__()
        self.receiver1_host = receiver1_host
        self.receiver2_host = receiver2_host
        self.receiver3_host = receiver3_host
        self.sender_host = sender_host
        self.address = address
        self.anon_sender = anon_sender

        # One sender connection and two receiver connections
        self.receiver1_conn = None
        self.receiver2_conn = None
        self.receiver3_conn = None
        self.sender_conn = None

        self.receiver1 = None
        self.receiver2 = None
        self.receiver3 = None
        self.sender = None

        self.count = 100
        self.n_rcvd1 = 0
        self.n_rcvd2 = 0
        self.n_rcvd3 = 0
        self.n_sent = 0
        self.n_settled = 0
        self.n_released = 0
        self.error = None
        self.timer = None
        self.all_msgs_received = False
        self.recvd1_msgs = dict()
        self.recvd2_msgs = dict()
        self.recvd3_msgs = dict()
        self.dup_msg_rcvd = False
        self.dup_msg = None
        self.receiver_name = None
        self.large_msg = large_msg
        self.body = ""
        self.r_attaches = 0
        self.reactor = None
        self.addr_timer = None
        self.n_released = 0
        # The maximum number of times we are going to try to check if the
        # address  has propagated.
        self.max_attempts = 5
        self.num_attempts = 0
        self.container = None
        self.test_msg_received_r1 = False
        self.test_msg_received_r2 = False
        self.test_msg_received_r3 = False
        self.initial_msg_sent = False
        self.n_accepted = 0

        if self.large_msg:
            self.body = "0123456789101112131415" * 5000
            self.properties = {'big field': 'X' * 3200}

    def on_released(self, event):
        self.n_released += 1
        self.send_test_message()

    def timeout(self):
        if self.dup_msg:
            self.error = "%s received duplicate message %s" % \
                         (self.receiver_name, self.dup_msg)
        else:
            if not self.error:
                self.error = "Timeout Expired - n_sent=%d n_rcvd1=%d, " \
                             "n_rcvd2=%d, n_rcvd3=%d, n_released=%d, addr=%s" % \
                             (self.n_sent, self.n_rcvd1, self.n_rcvd2,
                              self.n_rcvd3, self.n_released, self.address)
        self.receiver1_conn.close()
        self.receiver2_conn.close()
        self.receiver3_conn.close()
        if self.sender_conn:
            self.sender_conn.close()

    def on_start(self, event):
        self.reactor = event.reactor
        self.timer = event.reactor.schedule(TIMEOUT, TestTimeout(self))
        self.receiver1_conn = event.container.connect(self.receiver1_host)
        self.receiver2_conn = event.container.connect(self.receiver2_host)
        self.receiver3_conn = event.container.connect(self.receiver3_host)

        # Create receivers and sender all in one shot, no need to check for any address table
        # before creating sender
        self.receiver1 = event.container.create_receiver(self.receiver1_conn,
                                                         self.address)
        self.receiver2 = event.container.create_receiver(self.receiver2_conn,
                                                         self.address)
        self.receiver3 = event.container.create_receiver(self.receiver3_conn,
                                                         self.address)
        self.sender_conn = event.container.connect(self.sender_host)
        if self.anon_sender:
            self.sender = event.container.create_sender(self.sender_conn)
        else:
            self.sender = event.container.create_sender(self.sender_conn,
                                                        self.address)

    def send_test_message(self):
        msg = Message(body="Test Message")
        if self.anon_sender:
            msg.address = self.address
        self.sender.send(msg)

    def send(self):
        if self.large_msg:
            msg = Message(body=self.body, properties=self.properties)
        else:
            msg = Message(body="Message %d" % self.n_sent)
        if self.anon_sender:
            msg.address = self.address
        msg.correlation_id = self.n_sent
        self.sender.send(msg)

    def on_accepted(self, event):
        if self.test_msg_received_r1 and self.test_msg_received_r2 and self.test_msg_received_r3:
            # All receivers have received the test message.
            # Now fire off 100 messages to see if the message was multicasted to all
            # receivers.
            self.n_accepted += 1
            while self.n_sent < self.count:
                self.send()
                self.n_sent += 1
        else:
            self.send_test_message()

    def on_sendable(self, event):
        if not self.initial_msg_sent:
            # First send a single test message. This message
            # could be accepted or released based on if
            # some receiver is already online to receive the message
            self.send_test_message()
            self.initial_msg_sent = True

    def on_message(self, event):
        if event.receiver == self.receiver1:
            if event.message.body == "Test Message":
                self.test_msg_received_r1 = True
            else:
                if self.recvd1_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 1"
                    self.timeout()
                self.n_rcvd1 += 1
                self.recvd1_msgs[event.message.correlation_id] = event.message.correlation_id
        if event.receiver == self.receiver2:
            if event.message.body == "Test Message":
                self.test_msg_received_r2 = True
            else:
                if self.recvd2_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 2"
                    self.timeout()
                self.n_rcvd2 += 1
                self.recvd2_msgs[event.message.correlation_id] = event.message.correlation_id
        if event.receiver == self.receiver3:
            if event.message.body == "Test Message":
                self.test_msg_received_r3 = True
            else:
                if self.recvd3_msgs.get(event.message.correlation_id):
                    self.dup_msg = event.message.correlation_id
                    self.receiver_name = "Receiver 3"
                    self.timeout()
                self.n_rcvd3 += 1
                self.recvd3_msgs[event.message.correlation_id] = event.message.correlation_id

        if self.n_rcvd1 == self.count and self.n_rcvd2 == self.count and \
                self.n_rcvd3 == self.count:
            self.timer.cancel()
            self.receiver1_conn.close()
            self.receiver2_conn.close()
            self.receiver3_conn.close()
            self.sender_conn.close()

    def run(self):
        Container(self).run()


class MobileAddrMcastDroppedRxTest(MobileAddressMulticastTest):
    # failure scenario - cause some receiving clients to close while a large
    # message is in transit
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=True, anon_sender=False):
        super(MobileAddrMcastDroppedRxTest, self).__init__(receiver1_host,
                                                           receiver2_host,
                                                           receiver3_host,
                                                           sender_host,
                                                           address,
                                                           large_msg=large_msg,
                                                           anon_sender=anon_sender)

        self.n_released = 0
        self.recv1_closed = False
        self.recv2_closed = False

    def _check_done(self):
        if self.n_accepted + self.n_released == self.count:
            self.receiver3_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def on_message(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_message(event)

        # start closing receivers
        if self.n_rcvd1 == 50:
            if not self.recv1_closed:
                self.receiver1_conn.close()
                self.recv1_closed = True
        if self.n_rcvd2 == 75:
            if not self.recv2_closed:
                self.recv2_closed = True
                self.receiver2_conn.close()

    def on_accepted(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_accepted(event)
        self._check_done()

    def on_released(self, event):
        super(MobileAddrMcastDroppedRxTest, self).on_released(event)
        self.n_released += 1
        self._check_done()


class MobileAddrMcastAnonSenderDroppedRxTest(MobileAddrMcastDroppedRxTest):
    # failure scenario - cause some receiving clients to close while a large
    # message is in transit
    def __init__(self, receiver1_host, receiver2_host, receiver3_host,
                 sender_host, address, large_msg=True, anon_sender=True):
        super(MobileAddrMcastAnonSenderDroppedRxTest, self).__init__(receiver1_host,
                                                                     receiver2_host,
                                                                     receiver3_host,
                                                                     sender_host,
                                                                     address,
                                                                     large_msg=large_msg,
                                                                     anon_sender=anon_sender)
        self.n_released = 0
        self.recv1_closed = False
        self.recv2_closed = False

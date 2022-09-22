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
from system_test import TIMEOUT, TestTimeout


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

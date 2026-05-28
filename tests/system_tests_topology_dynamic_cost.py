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

import time

# must include interrouter_msg BEFORE any proton modules because it
# monkey-patches proton.Message so we can get the message trace
# annotation
import interrouter_msg  # noqa: F401  # pylint: disable=unused-import

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from system_test import TestCase, Qdrouterd, main_module
from system_test import TIMEOUT, AMQP_CONNECTOR_TYPE
from system_test import unittest

# ------------------------------------------------
# Helper classes for all tests.
# ------------------------------------------------


class Timeout:
    """
    Named timeout object can handle multiple simultaneous
    timers, by telling the parent which one fired.
    """

    def __init__(self, parent, name):
        self.parent = parent
        self.name   = name

    def on_timer_task(self, event):
        self.parent.timeout(self.name)


class ManagementMessageHelper:
    """
    Format management messages.
    """

    def __init__(self, reply_addr):
        self.reply_addr = reply_addr

    def make_connector_query(self, connector_name):
        props = {'operation': 'READ', 'type': AMQP_CONNECTOR_TYPE, 'name' : connector_name}
        msg = Message(properties=props, reply_to=self.reply_addr)
        return msg

    def make_connector_update_command(self, connector_name, new_cost):
        props = {'operation': 'UPDATE', 'type': AMQP_CONNECTOR_TYPE, 'name' : connector_name}
        msg_body = {'cost': new_cost}
        msg = Message(body=msg_body, properties=props, reply_to=self.reply_addr)
        return msg


# ------------------------------------------------
# END Helper classes for all tests.
# ------------------------------------------------


# ================================================================
#     Setup
# ================================================================

class TopologyTests (TestCase):

    @classmethod
    def setUpClass(cls):
        super(TopologyTests, cls).setUpClass()

        def router(name, more_config):

            config = [('router',  {'mode': 'interior', 'id': name}),
                      ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                      ('address', {'prefix': 'balanced',  'distribution': 'balanced'}),
                      ('address', {'prefix': 'multicast', 'distribution': 'multicast'})
                      ]    \
                + more_config

            config = Qdrouterd.Config(config)

            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        A_client_port = cls.tester.get_port()
        B_client_port = cls.tester.get_port()
        C_client_port = cls.tester.get_port()
        D_client_port = cls.tester.get_port()

        A_inter_router_port = cls.tester.get_port()
        B_inter_router_port = cls.tester.get_port()
        C_inter_router_port = cls.tester.get_port()

        #
        #
        #  Topology of the 4-mesh, with costs of connections marked.
        #  Tail of arrow indicates initiator of connection.
        #
        #                1
        #         D ----------> A
        #         | \         > ^
        #         | 20\   50/   |
        #         |     \ /     |
        #      1  |     / \     | 100
        #         |   /     \   |
        #         v /         > |
        #         C ----------> B (listener cost: 5)
        #                10
        #
        #  Test 1 TopologyCostUpdate Notes
        #
        #       1. Messages are always sent from A, and go to B.
        #       2. First route should be ADCB (cost: 1 + 1 + 10)
        #       3. Then change A_B_cost to 11
        #       4. Next route should be AB (cost: 11)
        #       5. Then change B_D_cost to 1
        #       6. Next route should be ADB (cost: 1 + 5 due to larger listener cost)
        #       7. Then change A_B_cost to 5
        #       8. Final route should be AB (cost: 5)

        # Initial connector costs
        cls.A_B_cost = 100
        cls.A_C_cost =  50
        cls.A_D_cost =   1
        cls.B_C_cost =  10
        cls.B_D_cost =  20
        cls.C_D_cost =   1

        # Listener costs
        cls.B_listener_cost = 5

        router('A',
               [
                   ('listener',
                    {'port': A_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': A_inter_router_port,
                     'stripAnnotations': 'no'
                     }
                    )
               ]
               )

        router('B',
               [
                   ('listener',
                    {'port': B_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': B_inter_router_port,
                     'stripAnnotations': 'no',
                     'cost': cls.B_listener_cost
                     }
                    ),
                   ('connector',
                    {'name': 'AB_connector',
                     'role': 'inter-router',
                     'port': A_inter_router_port,
                     'cost': cls.A_B_cost,
                     'stripAnnotations': 'no'
                     }
                    )
               ]
               )

        router('C',
               [
                   ('listener',
                    {'port': C_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('listener',
                    {'role': 'inter-router',
                     'port': C_inter_router_port,
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('connector',
                    {'name': 'AC_connector',
                     'role': 'inter-router',
                     'port': A_inter_router_port,
                     'cost' : cls.A_C_cost,
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('connector',
                    {'name': 'BC_connector',
                     'role': 'inter-router',
                     'port': B_inter_router_port,
                     'cost' : cls.B_C_cost,
                     'stripAnnotations': 'no'
                     }
                    )
               ]
               )

        router('D',
               [
                   ('listener',
                    {'port': D_client_port,
                     'role': 'normal',
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('connector',
                    {'name': 'AD_connector',
                     'role': 'inter-router',
                     'port': A_inter_router_port,
                     'cost' : cls.A_D_cost,
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('connector',
                    {'name': 'BD_connector',
                     'role': 'inter-router',
                     'port': B_inter_router_port,
                     'cost' : cls.B_D_cost,
                     'stripAnnotations': 'no'
                     }
                    ),
                   ('connector',
                    {'name': 'CD_connector',
                     'role': 'inter-router',
                     'port': C_inter_router_port,
                     'cost' : cls.C_D_cost,
                     'stripAnnotations': 'no'
                     }
                    )
               ]
               )

        router_A = cls.routers[0]
        router_B = cls.routers[1]
        router_C = cls.routers[2]
        router_D = cls.routers[3]

        # Make sure every router is connected to every other router
        router_A.wait_router_connected('B')
        router_A.wait_router_connected('C')
        router_A.wait_router_connected('D')

        router_B.wait_router_connected('A')
        router_B.wait_router_connected('C')
        router_B.wait_router_connected('D')

        router_C.wait_router_connected('A')
        router_C.wait_router_connected('B')
        router_C.wait_router_connected('D')

        router_D.wait_router_connected('A')
        router_D.wait_router_connected('B')
        router_D.wait_router_connected('C')

        # Additional reinforcements. This wait_connectors() queries for connections.
        # We can now be absolutely sure that the router network has been established.
        router_A.wait_connectors()
        router_B.wait_connectors()
        router_C.wait_connectors()
        router_D.wait_connectors()

        cls.client_addrs = (router_A.addresses[0],
                            router_B.addresses[0],
                            router_C.addresses[0],
                            router_D.addresses[0]
                            )

        # 1 means skip that test.
        cls.skip = {'test_01' : 0
                    }

    def test_01_topology_cost_update(self):
        name = 'test_01'
        if self.skip[name]:
            self.skipTest("Test skipped during development.")
        test = TopologyCostUpdate(self.client_addrs,
                                  "closest/01"
                                  )
        test.run()
        self.assertIsNone(test.error)


# ================================================================
#     Tests
# ================================================================

class TopologyCostUpdate (MessagingHandler):
    """
    Test that the lowest-cost route is always chosen in a 4-mesh
    network topology, as costs of connectors changes dynamically.

    """

    def __init__(self, client_addrs, destination):
        super(TopologyCostUpdate, self).__init__(prefetch=0)
        self.client_addrs     = client_addrs
        self.dest       = destination
        self.error      = None
        self.sender     = None
        self.receiver   = None
        self.test_timer = None
        self.send_timer = None
        self.n_sent     = 0
        self.n_received = 0
        self.n_accepted = 0
        self.n_released = 0
        self.reactor    = None
        self.state      = None
        self.send_conn  = None
        self.recv_conn  = None
        self.nap_time   = 2
        self.debug      = True
        self.trace_count = 0

        # Holds the management sender, receiver, and 'helper'
        # associated with each router.
        self.routers = {
            'A' : dict(),
            'B' : dict(),
            'C' : dict(),
            'D' : dict()
        }

        # These are the expected routing traces, in the order we
        # expect to receive them.
        self.expected_traces = [
            ['0/A', '0/D', '0/C', '0/B'],
            ['0/A', '0/B'],
            ['0/A', '0/D', '0/B'],
            ['0/A', '0/B']
        ]

        # This tells the system in what order to update connector costs.
        self.update_list = (
            ('B', 'AB_connector', 11),
            ('D', 'BD_connector', 1),
            ('B', 'AB_connector', 5),
        )

        # Use this to keep track of which connectors we have found
        # when the test is first getting started and we are checking
        # the topology.
        self.connectors_map = {
            'AB_connector' : 0,
            'AC_connector' : 0,
            'AD_connector' : 0,
            'BC_connector' : 0,
            'BD_connector' : 0,
            'CD_connector' : 0
        }

        # Number of inter-router connections per router at the start of the test
        self.connections_map = {
            'A' : 3,
            'B' : 3,
            'C' : 3,
            'D' : 3
        }

    # The simple state machine transitions when certain events happen,
    # if certain conditions are met.  The conditions are checked for
    # by the callbacks for the events.
    # The normal sequence of states in the state machine is:
    #  1. starting              -- doesn't do anything
    #  2. checking              -- checks initial topology
    #  3. examine_trace         -- look at routing trace of first message
    #  4. update_connector_cost -- updates the first connector cost (BA)
    #  5. examine_trace         -- checks routing trace of next message(s)
    #  6. update_connector_cost -- updates the next connector cost (BD)
    #  7. examine_trace         -- checks routing trace of next message
    #  8. update_connector_cost -- updates the next connector cost (BA)
    #  9. examine_trace         -- checks routing trace of final message
    # 10. bailing               -- bails out with success

    def state_transition(self, message, new_state) :
        if self.state == new_state :
            return
        self.state = new_state
        self.debug_print("state transition to : %s -- because %s" % (self.state, message))

    def debug_print(self, text) :
        if self.debug:
            print("%s %s" % (time.time(), text))

    # Shut down everything and exit.
    def bail(self, text):
        self.error = text

        self.send_conn.close()
        self.recv_conn.close()

        self.routers['B']['mgmt_conn'].close()
        self.routers['C']['mgmt_conn'].close()
        self.routers['D']['mgmt_conn'].close()

        self.test_timer.cancel()
        self.send_timer.cancel()

    # ------------------------------------------------------------------------
    # I want some behavior from this test that is a little too complex
    # to be governed by the usual callback functions. The way I do this
    # is by making a simple state machine that checks some conditions
    # during some callback, and then either steps forward or terminates
    # the test.
    # The callbacks that activate the state machine are mostly on_message,
    # or timeout.  But there are two different timers: the one-second
    # timer that mostly runs the test, and the 60-second timer that, if it
    # fires, will terminate the test with a timeout error.
    # ------------------------------------------------------------------------
    def timeout(self, name):
        if name == 'test':
            self.state_transition('Timeout Expired', 'bailing')
            self.bail("Timeout Expired: n_sent=%d n_received=%d n_accepted=%d" %
                      (self.n_sent, self.n_received, self.n_accepted))
        elif name == 'sender':
            if self.state == 'examine_trace' :
                self.send_messages()
            self.send_timer = self.reactor.schedule(1, Timeout(self, "sender"))

    def on_start(self, event):
        self.state_transition('on_start', 'starting')
        self.reactor = event.reactor
        self.test_timer = event.reactor.schedule(TIMEOUT, Timeout(self, "test"))
        self.send_timer = event.reactor.schedule(1, Timeout(self, "sender"))
        self.send_conn  = event.container.connect(self.client_addrs[0])  # A
        self.recv_conn  = event.container.connect(self.client_addrs[1])  # B

        self.sender     = event.container.create_sender(self.send_conn, self.dest)
        self.receiver   = event.container.create_receiver(self.recv_conn, self.dest)
        self.receiver.flow(100)

        # I will only send management messages to B, C, and D, because
        # they are the owners of the connectirs that I will want to check and update.
        self.routers['B']['mgmt_conn'] = event.container.connect(self.client_addrs[1])
        self.routers['C']['mgmt_conn'] = event.container.connect(self.client_addrs[2])
        self.routers['D']['mgmt_conn'] = event.container.connect(self.client_addrs[3])

        self.routers['B']['mgmt_receiver'] = event.container.create_receiver(self.routers['B']['mgmt_conn'], dynamic=True)
        self.routers['C']['mgmt_receiver'] = event.container.create_receiver(self.routers['C']['mgmt_conn'], dynamic=True)
        self.routers['D']['mgmt_receiver'] = event.container.create_receiver(self.routers['D']['mgmt_conn'], dynamic=True)

        self.routers['B']['mgmt_sender']   = event.container.create_sender(self.routers['B']['mgmt_conn'], "$management")
        self.routers['C']['mgmt_sender']   = event.container.create_sender(self.routers['C']['mgmt_conn'], "$management")
        self.routers['D']['mgmt_sender']   = event.container.create_sender(self.routers['D']['mgmt_conn'], "$management")

    # -----------------------------------------------------------------
    # At start-time, as the links to the three managed routers
    # open, check each one to make sure that it has all the expected
    # connections.
    # -----------------------------------------------------------------

    def on_link_opened(self, event) :
        self.state_transition('on_link_opened', 'checking')
        # The B mgmt link has opened. Check its connections. --------------------------
        if event.receiver == self.routers['B']['mgmt_receiver'] :
            event.receiver.flow(1000)
            self.routers['B']['mgmt_helper'] = ManagementMessageHelper(event.receiver.remote_source.address)
            for connector in ['AB_connector'] :
                self.connector_check('B', connector)
        # The C mgmt link has opened. Check its connections. --------------------------
        elif event.receiver == self.routers['C']['mgmt_receiver'] :
            event.receiver.flow(1000)
            self.routers['C']['mgmt_helper'] = ManagementMessageHelper(event.receiver.remote_source.address)
            for connector in ['AC_connector', 'BC_connector'] :
                self.connector_check('C', connector)
        # The D mgmt link has opened. Check its connections. --------------------------
        elif event.receiver == self.routers['D']['mgmt_receiver']:
            event.receiver.flow(1000)
            self.routers['D']['mgmt_helper'] = ManagementMessageHelper(event.receiver.remote_source.address)
            for connector in ['AD_connector', 'BD_connector', 'CD_connector'] :
                self.connector_check('D', connector)

    def send_messages(self):
        n_sent_this_time = 0
        if self.sender.credit <= 0:
            self.receiver.flow(100)
            self.debug_print("receiver sends flow of 100")
            return
        # Send messages one at a time.
        if self.sender.credit > 0 :
            msg = Message(body=self.n_sent)
            self.sender.send(msg)
            n_sent_this_time += 1
            self.n_sent += 1
        else:
            self.debug_print("No credit yet, not sending")
        self.debug_print("sent: %d" % self.n_sent)

    def on_message(self, event):
        if event.receiver in (
                self.routers['B']['mgmt_receiver'],
                self.routers['C']['mgmt_receiver'],
                self.routers['D']['mgmt_receiver']):

            if event.receiver == self.routers['B']['mgmt_receiver']:
                router = 'B'
            elif event.receiver == self.routers['C']['mgmt_receiver']:
                router = 'C'
            elif event.receiver == self.routers['D']['mgmt_receiver']:
                router = 'D'
            else:
                raise Exception("Unexpected event receiver")

            # ----------------------------------------------------------------
            # This is a management message.
            # ----------------------------------------------------------------
            if self.state == 'checking' :
                connection_name = event.message.body['name']

                if connection_name in self.connectors_map :
                    self.connectors_map[connection_name] = 1
                else :
                    self.state_transition("bad connection name: %s" % connection_name, 'bailing')
                    self.bail("bad connection name: %s" % connection_name)

                n_connections = sum(self.connectors_map.values())
                if n_connections == 6 :
                    self.state_transition("all %d connections found" % n_connections, 'examine_trace')
            elif self.state == 'update_connector_cost':
                if event.message.properties['statusDescription'] == 'OK':
                    self.state_transition('connector cost update done', 'examine_trace')
                else :
                    self.state_transition(f"Connector cost update request failed,  reply msg: {event.message}", 'bailing')
                    self.bail("Connector cost update request failed")
        else:
            # ----------------------------------------------------------------
            # This is a payload message.
            # ----------------------------------------------------------------
            self.n_received += 1
            if self.state == 'examine_trace' :
                trace    = event.message.router_annotations.trace
                expected = self.expected_traces[self.trace_count]
                if trace == expected :
                    if self.trace_count == len(self.expected_traces) - 1 :
                        self.state_transition('final expected trace %s observed' % expected, 'bailing')
                        self.bail(None)
                        return
                    self.state_transition("expected trace %d observed successfully %s" % (self.trace_count, expected), 'update_connector_cost')
                    self.update_connector_cost(self.update_list[self.trace_count])
                    self.trace_count += 1
                else:
                    self.debug_print("expected trace %s but got %s -- trying again" % (expected, trace))

    def on_accepted(self, event):
        self.n_accepted += 1

    def on_released(self, event) :
        self.n_released += 1

    def connector_check(self, router, connector) :
        self.debug_print("checking connector for router %s" % router)
        mgmt_helper = self.routers[router]['mgmt_helper']
        mgmt_sender = self.routers[router]['mgmt_sender']
        msg = mgmt_helper.make_connector_query(connector)
        mgmt_sender.send(msg)

    def update_connector_cost(self, target) :
        router = target[0]
        connector = target[1]
        new_cost = target[2]

        self.debug_print("Updating cost of connector %s to %d on router %s" % (connector, new_cost, router))
        mgmt_helper = self.routers[router]['mgmt_helper']
        mgmt_sender = self.routers[router]['mgmt_sender']
        msg = mgmt_helper.make_connector_update_command(connector, new_cost)
        mgmt_sender.send(msg)

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

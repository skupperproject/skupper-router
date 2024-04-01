#!/usr/bin/env python3
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
import json
import logging
import sys
from copy import deepcopy
from os import path
from threading import Thread
from threading import Lock

from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Message


# These mappings come directly from the vanflow.h source. They will need to be
# updated as new records/attributes/etc are added
#

record_types = {
    0: "SITE",
    1: "ROUTER",
    2: "LINK",
    3: "CONTROLLER",
    4: "LISTENER",
    5: "CONNECTOR",
    6: "FLOW",
    7: "PROCESS",
    8: "IMAGE",
    9: "INGRESS",
    10: "EGRESS",
    11: "COLLECTOR",
    12: "PROCESS_GROUP",
    13: "HOST",
    14: "LOG",
    15: "ACCESS_POINT"
}

RECORD_TYPE = 0
IDENTITY = 1

attribute_types = {
    RECORD_TYPE: "RECORD_TYPE",
    IDENTITY: "IDENTITY",
    2: "PARENT",
    3: "START_TIME",
    4: "END_TIME",
    5: "COUNTERFLOW",
    6: "PEER",
    7: "PROCESS",
    8: "SIBLING_ORDINAL",
    9: "LOCATION",
    10: "PROVIDER",
    11: "PLATFORM",
    12: "NAMESPACE",
    13: "MODE",
    14: "SOURCE_HOST",
    15: "DESTINATION_HOST",
    16: "PROTOCOL",
    17: "SOURCE_PORT",
    18: "DESTINATION_PORT",
    19: "VAN_ADDRESS",
    20: "IMAGE_NAME",
    21: "IMAGE_VERSION",
    22: "HOST_NAME",
    23: "OCTETS",
    24: "LATENCY",
    25: "TRANSIT_LATENCY",
    26: "BACKLOG",
    27: "METHOD",
    28: "RESULT",
    29: "REASON",
    30: "NAME",
    31: "TRACE",
    32: "BUILD_VERSION",
    33: "LINK_COST",
    34: "DIRECTION",
    35: "OCTET_RATE",
    36: "OCTETS_OUT",
    37: "OCTETS_UNACKED",
    38: "WINDOW_CLOSURES",
    39: "WINDOW_SIZE",
    40: "FLOW_COUNT_L4",
    41: "FLOW_COUNT_L7",
    42: "FLOW_RATE_L4",
    43: "FLOW_RATE_L7",
    44: "DURATION",
    45: "IMAGE",
    46: "GROUP",
    47: "STREAM_ID",
    48: "LOG_SEVERITY",
    49: "LOG_TEXT",
    50: "SOURCE_FILE",
    51: "SOURCE_LINE",
    52: "LINK_COUNT",
    53: "OPER_STATUS",
    54: "ROLE"
}


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


def id_2_source(identity):
    """
    Identity attributes are in the form <source-id>:#

    This routine simply returns the base source-id from an identity value.
    """
    return identity.split(":")[0]


class EventSource:
    """
    Events from a given source (router). There is an instance of this class for
    each source the snooper has detected. All records emitted by the source is
    gathered into the records map.

    The records map is indexed by the record identity and the value is the
    record's attributes.
    """
    def __init__(self, name, base_address, sender):
        self.name = name
        self.base_address = base_address
        self.sender = sender
        self.records = {}  # indexed by identifier
        self.links_pending = 3  # 1 sender, 1 for events, 1 for flows
        self.idle_heartbeats = 0
        logger.debug("New EventSource name=%s base-addr=%s", name, base_address)

    def get_records(self):
        """
        Return a list of all records, translating attribute and record type
        values to human-friendly names. Each record is a map.
        """
        records = []
        for record_id, attributes in self.records.items():
            record = {}
            for key, value in attributes.items():
                if key not in attribute_types:
                    # need to update attribute_types with new type?
                    raise Exception(f"Unknown VanFlow attribute type '{key}'")
                if key == RECORD_TYPE:
                    if value not in record_types:
                        # need to update record_types with new type?
                        raise Exception(f"Unknown VanFlow record type '{value}'")
                    record['RECORD_TYPE'] = record_types[value]
                else:
                    record[attribute_types[key]] = value
            records.append(record)
        return records


class VFlowSnooper(MessagingHandler):
    '''
    Open a receiver for BEACON messages on the indicated address.

    Use BEACONS to detect event sources (routers).  When discovered,
    instantiate a new EventSource for that source. Open receiver links to
    subscribe to that source's event and flow records.
    '''
    def __init__(self, address, idle_timeout=0):
        super(VFlowSnooper, self).__init__()
        self.address = address  # router address
        self.conn = None
        self.beacon_receiver = None
        self.sources_lock = Lock()
        self.sources = {}
        self._error = None
        self._sources_subscribed = 0
        self._total_records = 0
        self._idle_timeout = idle_timeout

    def on_connection_opened(self, event):
        logger.debug("Connection opened")

    def on_connection_closed(self, event):
        logger.debug("Connection closed")

    def on_link_opened(self, event):
        if event.link.is_sender:
            ltype = "sender"
            addr = event.link.target.address
        else:
            ltype = "receiver"
            addr = event.link.source.address
        logger.debug("%s link opened: %s", ltype, addr)

        with self.sources_lock:
            for name in self.sources.keys():
                if name in addr:
                    self.sources[name].links_pending -= 1
                    assert self.sources[name].links_pending >= 0
                    if self.sources[name].links_pending == 0:
                        self._sources_subscribed += 1
                        logger.debug("%s sources ready", self.sources_ready)

    def on_link_closed(self, event):
        if event.link.is_sender:
            ltype = "sender"
            addr = event.link.target.address
        else:
            ltype = "receiver"
            addr = event.link.source.address
        logger.debug("%s link closed: %s", ltype, addr)

    def on_transport_error(self, event):
        cond = event.transport.condition
        logger.debug("Transport error %s %s %s", cond.name, cond.description, cond.info)
        # ignore connection retries errors
        if "connection refused" not in cond.description.lower():
            self.exit()  # assume router terminated

    def on_start(self, event):
        self.container = event.container
        self.conn  = event.container.connect(self.address)
        self.beacon_receiver = event.container.create_receiver(self.conn, 'mc/sfe.all')

    def on_sendable(self, event):
        return

    def on_message(self, event):
        subject = event.message.subject
        if subject == 'BEACON':
            self.handle_beacon(event.message)
        elif subject == 'HEARTBEAT':
            self.handle_heartbeat(event.message)
        elif subject == 'RECORD':
            self.handle_records(event.message)
        else:
            self.exit(f"Message received with unknown subject '{subject}'")

    @property
    def sources_ready(self):
        """
        Total sources detected
        """
        return self._sources_subscribed

    @property
    def total_records(self):
        """
        Total records received from all sources
        """
        return self._total_records

    @property
    def error(self):
        return self._error

    def exit(self, error=None):
        self._error = error
        self.conn.close()
        if self._error is not None:
            logger.error("exit error: %s", self._error)
            raise Exception(self._error)

    def add_source(self, name, base_address, command_address):
        flow_address = f"{base_address}.flows"
        self.container.create_receiver(self.conn, base_address)
        self.container.create_receiver(self.conn, flow_address)
        sender = self.container.create_sender(self.conn, command_address)
        assert self.sources_lock.locked()
        self.sources[name] = EventSource(name, base_address, sender)

    def handle_beacon(self, message):
        source_id = message.properties['id']
        logger.debug("Beacon from %s", source_id)

        name = id_2_source(source_id)
        with self.sources_lock:
            if name not in self.sources:
                self.add_source(name, message.properties['address'], message.properties['direct'])

    def handle_heartbeat(self, message):
        source_id = message.properties['id']
        logger.debug("Heartbeat from %s", source_id)
        sender = None
        name = id_2_source(source_id)
        with self.sources_lock:
            source = self.sources.get(name)
            if source is not None:
                source.idle_heartbeats += 1
                if self._idle_timeout > 0:
                    # Check all known sources for idle timeout
                    idle = True
                    for src in self.sources.values():
                        if src.idle_heartbeats < self._idle_timeout:
                            idle = False
                            break
                    if idle:
                        logger.debug("Exiting due to idle_timeout")
                        self.exit()
                        return
                sender = source.sender

        if sender is not None and sender.credit > 0:
            sender.send(Message(subject='FLUSH'))

    def handle_records(self, message):
        for record in message.body:
            identity = record.get(IDENTITY)
            if identity is None:
                err = f"ERROR: received record with no id: {record}"
                logger.error(err)
                self.exit(err)
            with self.sources_lock:
                source = self.sources.get(id_2_source(identity))
                if source is None:
                    err = f"ERROR: source {identity} not in sources!!"
                    logger.error(err)
                    self.exit(err)

                if identity not in source.records:
                    logger.debug("New record: %s", identity)
                    self._total_records += 1
                    source.records[identity] = record
                    source.idle_heartbeats = 0  # new record
                else:
                    logger.debug("Record update: %s", identity)
                    source.records[identity].update(record)

    def get_results(self):
        """
        Return a map keyed by source-id. Value is a list of records emitted by
        that source (in no particular order).
        """
        results = {}
        if self.error is None:
            with self.sources_lock:
                for source_id, event in self.sources.items():
                    results[f"{source_id}:0"] = deepcopy(event.get_records())
        return results


class VFlowSnooperThread:
    """
    Run the vanflow snooper as a Python thread
    """
    def __init__(self, address, verbose=False):
        if verbose is True:
            logger.setLevel(logging.DEBUG)
        self.address = address
        self._snooper = VFlowSnooper(self.address)
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        self._snooper = VFlowSnooper(self.address)
        cid = f"vanflow-snooper-{self.address}"
        try:
            Container(self._snooper, container_id=cid).run()
        except Exception as exc:
            pass  # caller must check error property

    def join(self, timeout):
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise Exception("VFlowSnooperThread failed to join!")

    def get_results(self):
        return self._snooper.get_results()

    @property
    def total_records(self):
        """
        Return total number of records received to date.
        """
        return self._snooper.total_records

    @property
    def sources_ready(self):
        """
        Total sources detected
        """
        return self._snooper.sources_ready

    @property
    def error(self):
        return self._snooper.error


def main():
    parser = argparse.ArgumentParser(description="Display Vanflow Records")
    parser.add_argument("-a", "--address",
                        help="Address of the router",
                        type=str,
                        default="localhost:5672")
    parser.add_argument("-d", "--debug", help="Verbose logging",
                        action='store_true')
    it_help = f"""Exit {path.basename(sys.argv[0])} after no new records have
    arrived for this many heartbeats across all sources (0 for no timeout)"""
    parser.add_argument("--idle-timeout", help=it_help, type=int, default=0)

    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    snooper = VFlowSnooper(args.address, idle_timeout=args.idle_timeout)
    cid = f"vanflow-snooper-{args.address}"
    try:
        Container(snooper, container_id=cid).run()
    except KeyboardInterrupt:
        pass

    if snooper.error:
        print(f"ERROR: {snooper.error}", sys.stderr)
        return 1

    results = json.dumps(snooper.get_results(), indent=2, sort_keys=True)
    print(f"{results}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

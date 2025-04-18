<!-- Licensed to the Apache Software Foundation (ASF) under one -->
<!-- or more contributor license agreements.  See the NOTICE file -->
<!-- distributed with this work for additional information -->
<!-- regarding copyright ownership.  The ASF licenses this file -->
<!-- to you under the Apache License, Version 2.0 (the -->
<!-- "License"); you may not use this file except in compliance -->
<!-- with the License.  You may obtain a copy of the License at -->

<!--   http://www.apache.org/licenses/LICENSE-2.0 -->

<!-- Unless required by applicable law or agreed to in writing, -->
<!-- software distributed under the License is distributed on an -->
<!-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY -->
<!-- KIND, either express or implied.  See the License for the -->
<!-- specific language governing permissions and limitations -->
<!-- under the License. -->

Edge/Interior Address Tracking
==============================

This document gives an overview of the address proxying done between
an edge router and its interior router.

This logic is implemented in two core modules. See the *edge\_router*
and *edge\_addr\_tracking* subdirectories in src/router_core/modules.

The *edge\_router* module is active on the edge router only.  The
*edge\_addr\_tracking* module is active on the interior router only.

Note that this document only applies to edge-to-interior router
connections. Documenting inter-edge (edge mesh) connections are TBD.

Overview
========

The purpose of address-proxying is to allow clients attached to an
edge router access to sources/targets in the rest of the router
network.

This proxying mechanism is necessary because the edge router does not
participate in the routing protocol exchange that is run by the
interior routers.  This is by design: by exempting the edge routers
from the routing protocol edge clients can scale without negatively
impacting the routing topology with frequent updates.

However this means we need a different mechanism for discovering
sources/targets on the routing mesh.

When a mobile-addressed link attaches to the edge router the router
needs to know if there are sources or targets for that mobile address
in the routing network.  That means:

 * if the link is incoming (to a target) are there consumers for that
   address in the routing network?
 * if the link is outgoing (from a source) are there producers to that
   address in the routing network?

To do this the edge router creates *proxy links* to the interior
router. These are links that mimics local clients on the interior
router for clients on the edge router. For example:

 * When a consuming link is opened on the edge, the edge will create a
   proxy incoming link *with the same mobile address* to the
   interior.
 * When a producing link is opened on the edge, the edge will create a
   proxy outgoing link *with the same mobile address* to the interior

Note well: in reality only one proxy link is necessary per mobile
address.  For example if three clients attach to the edge to consume
from address "example/service" then only one proxy link for
"example/service" is necessary. This results in less links to the
interior as only one "summary" link is used regardless of the number
of edge clients. This allows better scaling of mobile addresses.

Note well part deux: in the case of incoming edge links we do not want
to start accepting inbound messages unless there is a consumer for
them. This means that the edge router needs to be aware when
destinations become available or go away. To do this the interior
router will send control messages to the edge to notify the edge of
the availablility of destinations for mobile addresses.

The edge router uses these updates to either bind the address to the
client incoming link (destinations available) or unbind the address
(no destinations available). This controls whether or not messages
arriving on the incoming link can be forwarded.

Interior Router
===============

The interior router creates a mobile endpoint for address
"\_$qd.edge\_addr_tracking".  This is the address where the interior
router will send destination availability status messages to the edge
router. The edge router will create a consuming link to this address
when the edge connection is established.

When the consuming link attaches to the address tracking endpoint in
the interior, the interior creates an *endpoint\_state* instance that
holds state about the connection, including the endpoint corresponding
to the edge's link.  The interior maintains a list of these
endpoints - one for each edge router connection.

These endpoint state objects are reference counted. Every incoming
(producing) link from the edge router with a _mobile address_ target
terminus that attaches over the edge router connection associated with
the endpoint state increments this refcount.

Those links hold a back-pointer to the endpoint state instance.

When the interior router detects that there are destinations for an
edge link's mobile target address it will send a message to the edge
router (via the \_$qd.edge\_addr\_tracking endpoint link) to cause the
edge router to bind the target address to that link.  This notifies
the edge that there are destinations available via the interior router
for the mobile address. Once the address is bound on the edge, the
edge can now forward messages destined to that mobile address to the
interior router. The interior router will then forward those messages
to the proper destination.

When the reachability of those mobile addresses change on the interior
router the edge must be notified. The edge must either unbind the
affected address if the address no longer has destinations via the
interior, or the edge must rebind the address if destinations via the
interior become available.

So when the state (reachable/unreachable) of a mobile address changes
at the interior router, the router will spin through all incoming
links associated with that address (addr->inlinks), and if the link is
from an edge router the interior will send a message (via the
\_$qd.edge\_addr_tracking endpoint link to that edge). The message
will indicate whether the address should be bound or unbound to its
link on the edge side.

In addition to establishing an endpoint for edge tracking address, the
interior router subscribes to address and link-related core events.

For link events the interior gets notified when an link on the edge
connection attaches or detaches. Specifically these events are
generated when the link is bound to (attached) or unbound from its
address (link->owning\_addr set or cleared).

The interior router is only interested in link attach/detach events
for incoming (producer) links bound to moble-class addresses. Other
types of links/address classes are ignored.

When such a link attaches (bound to a mobile address) the interior
checks to see if there are currently any destinations (consumers) for
the link's target address. If destinations are reachable via the
interior router the router then sends the command to the edge to bind
the address to the new link (as described above).

A link detach event is used by the interior router to clean up the
state associated with the link.

The interior also registers for address events to track changes in the
availability of destinations associated with the address. These events
are used by the interior to determine whether or not there are
destinations for the given address. The interior only considers those
addresses that are associated with edge incoming links (those same
links that are tracked for attach/detach events). Changes availability
of destinations will cause the interior router to send *bind/unbind*
messages for the address over the edge address tracker link to the
edge. This effectively forces the edge to either stop or start sending
messages to the interior for that address.

Edge Router
===========

The edge router's logic resides in two source files: addr\_proxy.c
and connection\_manager.c.

The edge router's connection\_manager tracks the state of the
edge-to-interior connection. It selects one connection to be the
active edge connection, holding any others in reserve should the
active edge fail.  When the active edge opens or closes it sends a
event to the addr\_proxy subcomponent.

The addr\_proxy implements the lion's share of address management on
the Edge.

It maintains a subscriber link to the interior's
\_$qd.edge\_addr\_tracking source, processing bind/unbind command
messages sent by the interior.

The addr\_proxy also maintains a single outgoing anonymous link to the
interior. This link functions as a "catch-all" for those deliveries
that do not have a known destination (arrive on an anonymous link, do
not have a "to" address field, etc). The forwarder on the edge will
use this link to pass these deliveries to the interior for forwarding.

In addition the addr\_proxy maintains an incoming link bound to the
router's address. This link is known as the *edge downlink* and is
used by the interior when forwarding deliveries addressed to the router
itself.

The addr\_proxy monitors address events and link detach events.

Address events are used to determine changes in the state of local
sources and targets.  These events are triggered when a local consumer
(outgoing) link or producer (incoming) link attaches or detaches from
the router.

In order for these links to consume from or produce to destinations in
the interior the addr\_proxy creates proxy links to the interior for
the targets/sources of the local links.

For example, when a client attaches an outgoing link for source
"foo-addr" to the edge the addr\_proxy creates a proxy link to the
interior using source "foo-addr".  The same process occurs for
incoming links to targets.

When all non-proxy local links to a particular target/source drop an
address event is generated that causes the addr\_proxy to detach the
corresponding proxy link.

When link detach events occur for the non-proxy link the addr\_proxy
deletes the corresponding proxy link to the interior.

Certificate Rotation
====================

The process of rotating in a new TLS edge connection is controlled by
the edge router. When the sslProfile associated with an Edge Connector
is rotated, the connector will create another edge-to-interior
connection.

The connection\_manager will be alerted when the new connection
opens. It will detect that there is already an existing edge
connection present.  This will cause it to check the TLS ordinal
associated with the new connection and if it is greater than the TLS
ordinal of the existing connection it will alert the addr\_proxy that
a new connection has opened.

The addr\_proxy will also notice that there is already an existing
edge connection. This will trigger the connection migration logic.

The addr\_proxy will start by detaching the existing control links -
the anonymous up link and the edge downlink - on the old
connection. It will then detach the \_$qd.edge\_addr\_tracking
endpoint used to receive updates from the interior. Lastly it will
scan all mobile addresses for those that have an associated proxy in
or out link. These links will be detached.

Once this process completes the addr\_proxy will re-create all the
control and proxy links, as well as the address tracking endpoint,
over the new connection.  This completes the rotation process.

One key point to be aware of: the rotation process does _NOT_ tear
down any non-proxy links on the old connection. This means that all
streaming anonymous links using the old connection are left
intact. This is by-design: the rotation process must not terminate any
streaming links that are active on the old connection prior to the
rotation process.

One other point: the old connection is not closed after the rotation
process completes (ignoring the case where oldestValidOrdinal expires
it). The old connection is kept as a hot standby should the active
edge connection fail. The failover logic on the connection\_manager
takes into consideration the TLS ordinal value of the standby
connections when it selects a failover candidate. The connection with
the numerically greatest TLS ordinal is selected as the failover
connection.  This ensures that the connection with the most up-to-date
set of TLS credentials are used.


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

# Inter-Router Connection Upgrade

## Introduction

The router supports a mechanism that allows new inter-router
connections to replace existing connections without disturbing any
message flows in-flight. Once the new connections are established new
message flows will be routed over those new connections only. New
flows will never be forwarded over the older pre-existing connections.
Older connections are not torn down. They are preserved to allow older
message flows to continue without interruption.

This capability is useful for those situations where the properties of
the inter-router connections need to be modified on the fly. For
example this allows an operator to update TLS credentials used for
inter-router connections without disturbing existing traffic.

## Implementation

It is a goal to preserve existing message flows during connection
upgrade. In order to do this the connection upgrade process should
avoid causing topology recomputations. Connection upgrade does not
require a topology change because there is no effective changes to the
routing path: the new connections are connecting to the same peer
router as the connections to be upgraded.

This implementation avoids changing any routing state in the routing
engine. The connection upgrade process is done entirely within the
router core module. The router core basically hot swaps the
connections being upgraded in the forwarding data structures without
notifying the routing engine that the connections have changed.

### Inter-Router Connector

The inter-router connector is responsible for establishing
inter-router connections. A connector will open one connection of role
"inter-router" that serves as the control connection. The control
connection establishes the bi-directional control links that carry
HELLO routing protocol messages. In addition to the inter-router
control connection the connector will establish zero or more
connections of role "inter-router-data". The inter-router-data
connections are dedicated to forwarding streaming message flows.

The connector labels all of its connections with a
"correlation-id". The correlation-id is used by the router core (on
both routers) to group all connections originating from the connector.
Since all of these connections are to the same peer router the core
considers the connection group a single "path" for routing purposes.

The correlation-id is globally unique and does not change for the life
of the connector.

### The Group Ordinal

The connector also labels each connection with an integer value called
the "group ordinal". Initially, all connections in the group have the
same group ordinal value.

However the group ordinal can be advanced by a management operation
(like certificate rotation). When the connector's group ordinal is
changed it triggers the connector to establish a new set of
connections. This new set of connections are labelled with the new
group ordinal value.

Note that the group ordinal value can only be numerically
increased. In other words a group ordinal value will never be set to a
value less than its current value.

The group ordinal is used by the connection upgrade process. During
connection upgrade the router core needs some way of determining that
a new connection replaces another existing connection. To do this the
router core compairs the group ordinal values associated with the
connections.

The group ordinal effectively becomes the connections precedence. New
message flows are always routed over those connections with the
numerically highest group ordinal values within the connection
group. Those connections with lower group ordinal values remain up,
but no longer receive new message flows.

### The Upgrade Process

When an event occurs that requires inter-router connection upgrade -
such as certificate rotation - the connector's group ordinal value is
increased and the connector opens a new set of inter-router control
and data connections. These new connections are labelled with the
latest (and numerically greatest) group ordinal value.

The router core is notified when these connections activate. The core
uses the correlation-id to determine which group the connection
belongs to.  The core then checks the new connection's group
ordinal. If the new connection's ordinal is greater than the ordinals
of other connections in the group the core begins transitioning the
forwarder to the new higher valued connections.

The core cannot perform the transition until:

* a new higher-ordinal connection with the role of inter-router control has opened
* the control links on the new inter-router control connection have attached

Until those two events happen the core will store all new higher
ordinal inter-router connections in a pending state.

On the connector-side router when a new higher-ordinal control
connection has opened the core will initiate all AMQP links carried by
a control connection. This includes the control links and all
priority-based non-streaming message links.

Once the core is notified that both control links are attached the
core performs the upgrade:

* The new control connection uses the same "mask-bit" link identifer as the old control connection.
* The new control connection becomes the group "parent", replacing the old control connection.
* The new control connection replaces the old control connection in the core's next-hop forwarding table.
* The control links on the old control connection are detached.

All new inter-router data connections with the same group ordinal as
the parent control connection are added to the parent's data
connection list.  Since these connections are created asynchronously
their addition to the parent may happen after the upgrade process
completes.

At this point the upgrade process is complete. New forwarding requests
will find the highest ordinal connection in the next hop forwarding
table and be forwarded across these connection.

### Extra Credit: The Inter-router Link Identifier

The link identifer - often referred in the code as the "mask bit" - is
used to identify a logical path between the current router and a
next-hop peer. In this case the term "link" has no relationship to an
AMQP link. For routing purposes the term "link" means the data path
between two entities.  In the case of the router the link identifier
"identifies" the concrete connection group that comprises the message
flow between two routers.

The term "mask bit" relates to an implementation detail: the router
code manages the pool of free link identifiers using a bitmask
utility.

The link identifier serves as an abstraction for the routing
engine. Rather than having the routing engine deal with the complexity
of managing multiple connections to the same next hop (the connection
group), the router simply uses the link identifier to represent the
group as a whole. This is valid since all connections in the group
terminate at the same peer router.

Therefore all path-related state shared between the routing engine and
any other part of the core (like the forwarder) use only the link
identifier.

This is how the connection upgrade process can adopt a new set of
connections between routers without triggering routing topology
re-computation. The new control connection does not get a new link
identifer.  Instead it takes the link identifer from the old control
connection that it replaces. This means that the routing engine sees
no change of state for that link and does not trigger a routing
topology update.



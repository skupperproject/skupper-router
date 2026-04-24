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

# Cross (Inter) Network Communication

Cross-network communication is message transfer between endpoints connected to disjoint Skupper/AMQP networks.  The connectivity between the networks is achieved using connections with the 'inter-network' role and auto-links established over those connections.

The easy part is creating connectivity for a particular mobile address, the address of a service.  More challenging is the need to provide reply-to connectivity using a router-assigned dynamic address on the client-side (for request/response traffic patterns).

## Use Cases

There are a couple of use cases that drive the requirements for cross-network communication.

### Central Management of Networks

In the case where an enterprise maintains a large number of virtual application networks, a good way to provide connectivity for a management plane is to create a management network that maintains cross-network connectivity to each of the managed networks.

In such a network, there is a inter-network connection between at least one router in the managed network and at lease one router in the management network.  Multiple connections may be desired for redundancy and availability.  All message connectivity is strictly between the management network and the individual managed network.  No communication will be possible between two managed networks via the management network, thus preserving the isolation provided by an application network.

### Inter-Network Federation

In the case where an application running on a virtual application network wishes to expose a service for use from within another VAN without creating a general-use ingress, cross-network connectivity can be used to create a secure tunnel between VANs for specifically configured services.

## Setting up Cross-Network Connectivity

To illustrate the configuration of inter-network communication, we'll use the inter-network federation use case with two networks, net-A and net-B.

Both networks are assumed to be composed of multiple interior routers and possibly some edge routers.  The inter-network connection must be between two interior routers, one in each network.

### Router Configuration

All routers (interior and edge) must be configured with the name of the network using the `network` configuration:

```
network {
    networkId: net-A
}
```
### Listener and Connector Configuration

An interior router in each network must be designated to be endpoints of the inter-network connection.  For example, routers A3 (in net-A) and B2 (in net-B) are designated.  A3 is configured with a `listener` and B2 is configured with a `connector`, both using role `inter-network`.  B2 is configured to connect to A3 using standard host/port addressing and security.  Both the listener and connector must be named because the configurations will include `autoLink` entities that refer to those connections.

### Exposing Services Cross-Network

Since the `inter-network` connection does not provide the network-joining functions of an `inter-router` connection, the two networks are not joined and do not share any topology or routing information.  Nothing will flow over the `inter-network` connection until an `autoLink` is created which establishes a unidirectional link between the networks.

The full functionality of auto-links is available for `inter-network` connections.  They behave similarly to `route-container` connections.  However, it is recommended for inter-network use cases that all auto-links be configured as `direction: in` and "pull" from the network hosting a particular destination.

For example, if net-A hosts a service using the address `service-45` anywhere in its network, an auto-link should be configured in the designated router (A3) as follows:

```
autoLink {
    connection: <name of listener/connector>
    address: service-45
    direction: in
}
```

This will cause the `service-45` address to be reachable from all routers in net-B.

Each service that is to be reachable cross-network must have an auto-link created for it in the network in which the service is hosted.

Important Note:  Destinations cannot be load-balanced across different networks.  In other words, there must not be cross-network services in different networks with the same address.  If auto-links are created in both directions for an address, a loop is created that will almost certainly forward deliveries to that address to the wrong places.

### Using Dynamic Addresses

Almost always, service traffic is not a one-way affair.  A user of a service expects to receive a response from the service when a request is sent.  Such a client will first create a receiving link using a dynamic terminus.  The dynamically-allocated address is then placed in the request's `reply-to` header for the server to use as the destination address of the reply or replies.

If a network in a federation relationship is going to host clients using dynamic addresses, it must create an auto-link to carry those replies back from the serving network.  The auto-link looks like this:

```
autoLink {
    connection: <name of listener/connector>
    direction: in
    externalAddress: _topo/<local-network-id>
}
```

There are a couple of notable characteristics of this auto-link.  It does not have an `address` attribute, making it "anonymous".  This is important because it must issue credit to the peer network regardless of the existance of a particular destination locally.

The other important aspect of this auto-link is that its remote address is interpreted at the other end as a "remote-network" address which will match any dynamic address created on the local network.  This means that only one auto-link needs to be created to handle the return traffic of any number of exposed services.

## A Complete Example

The following example shows how to create a simple two-network federation on a single host where each network has one router.

Router A:
```
router {
    id: A
    mode: interior
}

network {
    networkId: net-A
}

listener {
    port: 10001
    role: normal
}

listener {
    name: federation
    port: 11001
    role: inter-network
}

autoLink {
    connection: federation
    direction: in
    externalAddress: _topo/net-A
}

autoLink {
    connection: federation
    direction: in
    address: service-A-to-B
}
```

Router B:
```
router {
    id: B
    mode: interior
}

network {
    networkId: net-B
}

listener {
    port: 10002
    role: normal
}

connector {
    name: federation
    port: 11001
    role: inter-network
}

autoLink {
    connection: federation
    direction: in
    externalAddress: _topo/net-B
}

autoLink {
    connection: federation
    direction: in
    address: service-B-to-A
}
```

## Using Cross-Network Connectivity as an Endpoint

It is the intent of the implementation that cross-network operations are transparent to the endpoints involved.  No changes to endpoint (producer, consumer, client, server) code need to be made to use this feature.

## Backward Compatibility and Breaking Changes

This feature represents an incompatible change from the version 3 router because the format of dynamic addresses has been changed to include a field for the network ID.  This feature must be introduced in a major release (version 4).  Until that time, the project team shall maintain a development branch that tracks main and carries a patch for this feature.

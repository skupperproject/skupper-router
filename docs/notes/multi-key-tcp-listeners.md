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

# Multi address tcp listeners

An ordinary tcp listener has exactly one service address associated with it. The address is defined at listener creation time and cannot be changed afterwards. The service address determines the potential target connectors for each new client connection of the tcp listener.

A multi address listener can have more than one service addresses. No address is defined at creation time. Instead, addresses can be assigned to or removed from the multi address listener dynamically during its entire lifespan. A new client connection always picks an optimal target address from the current set of addresses. There is a new listener property to define an address selection strategy. This strategy determines the criteria for selecting the optimal address for a new client connection.

## Router Configuration

The router management schema is extended as follows.

* A new field called `multiAddressStrategy` is added to the tcpListener  configuration entity. It defines the address selection strategy.
* A new configuration entity called `listenerAddress` is added to the schema. This entity associates a new service address with an existing multi key listener.

### Address selection strategy

The new `multiAddressStrategy` field in the tcpListener entity can take the following values:

* `none`: No address selection strategy is defined for the listener. This value indicates that it is an ordinary listener which has exactly one address. The address is defined in the address field of the listener entity. This is the default value.
* `priority`: Indicates a multi address listener. The `address` field of the listener entity is ignored. This address selection strategy always picks the address with the highest priority value from the current set of reachable addresses. Note that this is the only supported address selection strategy currently.

### New listener address entity

The new listenerAddress entity has the following fields.

* `address`: The service address (a.k.a. routing key) value.
* `value`: An integer parameter. The priority address selection strategy interprets this value as priority. Larger values represent higher priority.
* `listener`: The name of the listener which this service address is to be associated with. The referred listener must already exist.

The `listenerAddress` entity supports `create` and `delete` operations. Service address entities can be created and deleted dynamically during the lifespan of the corresponding multi address listener.

## Implementation notes

* Deletion of a service address entity does not affect already existing client connections targeting that address.
* If multiple service addresses share the same value, those addresses will be ordered non-deterministically by the router. I.e. if there are two addresses with the same value, and that value is the highest priority, and both addresses are reachable, the router will arbitrarily choose one of those addresses to be treated as the currently highest priority address.
* Each listener address holds a reference to the parent listener adaptor hence it is not allowed to delete a multi-address listener if it has any associated address.
* A vanflow record of type `VFLOW_RECORD_LISTENER`is created for each individual address of a multi-address listener. The record begins and ends when the address is created and deleted, respectively. Vanflow records of the same multi-address listener have the same listener name.
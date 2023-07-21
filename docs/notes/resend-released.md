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

Resend-Released specification

# Overview

Resend-released is an optional feature that causes deliveries with a RELEASED disposition to be re-forwarded in an attempt to find another valid destination.

When a delivery is released, it was not successfully delivered to the intended destination.  Released disposition can occur under the following circumstances:

* The delivery was sent using a stale route to a remote router that no longer has a valid destination for the delivery.
* The consumer to which the message/stream was delivered returned a RELEASED disposition indicating that the payload was not used.
* A protocol adaptor was configured with a connector for which there was no backing server (i.e. a TCP connection was dropped due to no-route-to-destination).

Most of the above cases are caused by transitions in a network including changes in network topology and the Skupper control plane reacting to the shut-down of a service for which it has a configured connector.

This feature is intended to dramatically reduce, if not close, the window of opportunity for delivery failure during the above mentioned transitions.  As long as there is at least one reachable destination for the delivery, the network should not return a RELEASED disposition back to the original sender.

# How the feature works

This feature is only relevant for anycast routing.  It will be ignored on deliveries that use a multicast address.

This feature is enabled on a per-delivery basis.  A bit, MSG_FLAG_RESEND_RELEASED, in the ra_flags field within the router-annotations of the message/stream is used to indicate that resend-released is to be performed for the delivery.

In the process of forwarding a message/stream, each router in the path receives the delivery, examines the possible outbound links, chooses the _best_ link from the possibilities, and forwards the delivery on that link.  The chosen link may be a link direct to a destination or it may be a link to the next-hop router.

From the perspective of a single router in the path, if the outbound delivery is updated with a disposition of RELEASED, the last-used outbound path (link or neighbor router) is invalidated and the message is posted for re-delivery.  The forwarders have been modified to exclude invalided paths as options for selection.  This means that the delivery will be re-forwarded to the _next_ best available option.  It there are no remaining available options, the delivery will have a fanout of zero and will be released back to the sender.

This mechanism results in a depth-first search for an available destination.  If a message was delivered to a remote router on which there are now no available destinations, the remote router will release the message back to the previous router in the path which will then work through all of its possible outbount paths to destinations.

When resend-released is enabled for a delivery, the buffers in the message content are not freed.  This allows the message content to be re-transmitted on the new path(s).  Buffer reclaimation is suppressed as long as the resend-released bit is set in the message's router-annotations.

When a disposition other than RELEASED is received on a delivery, the resend-released flag is cleared in the router-annotations and re-forwarding will no longer occur.

For a resend-released stream, the receiver MUST send a non-terminal disposition of RECEIVED as soon as it is determined that the stream can be delivered to a valid destination.  This stops the re-forwarding and allows the routers in the path to free content buffers on the flowing stream.

# How to access the feature

The resend-released feature is selected individually per-delivery.

## As a messaging client

As an external messaging client, the router-annotations are not accessible.  To enable this feature, the `qd.resend-released` capability must be set in the sending terminus.  This will cause the ingress router to set the appropriate flag in the router-annotations for propagation through the network.

All deliveries via a terminus with the above capability set will be enabled for resend-released.

## As an in-process adaptor

An in-process message source can invoke the `qd_message_set_resend_released_annotation` method on the outgoing message if it wishes to use the feature.

For protocol adaptors, resend-released is appropriate for the client-to-server stream.  Resend-released should _not_ be used for the reply/from-server stream.
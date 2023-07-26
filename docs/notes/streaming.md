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

Streaming Deliveries

# Overview
AMQP messages are not limited in size.  A message body can be very large or even indeterminate in length.  If messages were completely received by a router before they were forwarded, there would be unbounded memory consumption to hold the message content and the latency of delivery would be unacceptably large.

Streaming deliveries are used to solve these problems.  A streaming delivery occurs when a large message is being transmitted downrange toward its destination(s) at the same time that new content is being received.  This allows for long-running indeterminate-sized streams to be sent without memory consumption or storage latency.

# Technical Considerations

## Link allocation for streaming deliveries
An AMQP _link_ represents an uninterrupted sequence of message content.  Messages cannot be interleaved over an AMQP link.  One message must be completely transferred before the next message can begin.

Because of the non-interleaved nature of links, concurrent streaming message deliveries must each be allocated to their own dedicated link.  Since link allocation is dependent on streaming state (non-streamed deliveries are sent over a set of prioritized inter-router data links), whether or not a delivery is streaming must be determined before that delivery can be forwarded onto an outbound link.

Connections with the `qd.streaming-links` capability can be used to carry concurrent streaming deliveries over links that are created specifically for a single stream.

During the forwarding process when the best outgoing link is chosen for a particular delivery, if the delivery is streaming, a new parallel link will be created (or re-used from a pool) for that stream.  Refer to the `get_outgoing_streaming_link` function in forwarder.c

## Determining when a delivery must be streamed
A number of conditions must be met in order for an incoming delivery that has not yet been forwarded to be considered streaming:

* The message must not be receive-complete.  A completely received message that has not yet been forwarded will never be considered streaming.
* An incoming receive-incomplete message will be treated as streaming if any of the following conditions are met:
    * Enough payload has been received to cause Q2-flow-control to be applied.  In this case, in order for the message to not be blocked indefinitely, the delivery becomes a streaming delivery so that the payload can be flushed outbound.
    * The Router-Annotations ra_flags has the STREAMING bit (0x01) set.  This is an indication from either an upstream router or an in-process message source that the delivery is to be treated as a streaming delivery.
    * (new since [#1173](https://github.com/skupperproject/skupper-router/issues/1173)) The incoming link on which the delivery arrived has a target capability of `qd.streaming-deliveries`

## Flow control for streaming deliveries
It is important that the amount of memory used to buffer streaming messages in a router be limited so as to avoid exhausing memory when the producer is faster than the consumer.

In the version 2.4.1 timeframe, this is accomplished by limiting the number of buffers held by a streaming message to the "Q2" limit.  This limit is 64 buffers, each of which can consume up to 4K octets.  This limits the memory consumption of a single stream to 256 KBytes.  Once the streaming message has Q2_max buffers allocated to it, that message will stop reading from its incoming link, thus placing back-pressure on the upstream producer.

### A proposed change to flow control
A proposed change to the approach to streaming-flow-control is to remove the Q2 algorith and instead use an AMQP session for every streaming link on an inter-router connection.  This would allow a session window to be used in lieu of the non-standard Q2 window.

Challenges with this include the fact that an AMQP connection can only accommodate 65535 sessions (32767 with the Proton library).  This can be addressed by taking advantage of the inter-router connection trunking feature to use multiple connections for streaming deliveries.

In this case, it may be advantageous to configure the streaming connections to use throughput-oriented socket settings (TCP_QUICKACK) and to use TCP_NODELAY, a latency-oriented setting for the main connection used for control messages and non-streamed deliveries.

# Controlling Streaming Behavior

## As a messaging client
A normal messaging client, connected to a router, that wishes to produce streaming deliveries should use the `qd.streaming-deliveries` capability on the sending target for said deliveries.

Here is an example code snippet for Proton Python that illustrates how to set up such a link:

```
from proton import symbol

...

sender = container.create_sender(connection, address)
sender.target.capabilities.put_object(symbol("qd.streaming-deliveries"))
```

A normal receiver that wishes to receive streaming deliveries must indicate the `qd.streaming-links` capability for its AMQP connection.  It must also be prepared to accept incoming link-attaches for the streamed deliveries.

## As an in-process adaptor

An in-process stream source (typically a protocol adaptor) can designate messages as streaming by invoking the `qd_message_set_streaming_annotation(message)` function on the message before it is sent.
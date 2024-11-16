#ifndef __dispatch_message_h__
#define __dispatch_message_h__ 1
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "qpid/dispatch/buffer.h"
#include "qpid/dispatch/compose.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/iterator.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/parse.h"

#include <proton/raw_connection.h>

typedef struct qdr_delivery_t qdr_delivery_t;
typedef struct qd_link_t      qd_link_t;

/**@file
 * Message representation. 
 *
 * @defgroup message message
 *
 * Message representation.
 * @{
 */

// DISPATCH-807 Queue depth limits
// upper and lower limits for bang bang hysteresis control
//
// Q2 defines the maximum number of buffers allowed in a message's buffer chain.  This limits the number of bytes that
// will be read from an incoming link for the current message. Once Q2 is enabled no further input data will be read
// from the link. Q2 remains in effect until enough bytes have been consumed by the outgoing link(s) to drop the number
// of buffered bytes below the lower threshold.

#define QD_QLIMIT_Q2_LOWER 32                        // Re-enable link receive
#define QD_QLIMIT_Q2_UPPER (QD_QLIMIT_Q2_LOWER * 2)  // Disable link receive

// Callback for status change (confirmed persistent, loaded-in-memory, etc.)

typedef struct qd_message_t             qd_message_t;

/** Amount of message to be parsed.  */
typedef enum {
    QD_DEPTH_NONE,
    QD_DEPTH_ROUTER_ANNOTATIONS,
    QD_DEPTH_HEADER,
    QD_DEPTH_DELIVERY_ANNOTATIONS,
    QD_DEPTH_MESSAGE_ANNOTATIONS,
    QD_DEPTH_PROPERTIES,
    QD_DEPTH_APPLICATION_PROPERTIES,
    QD_DEPTH_BODY,
    QD_DEPTH_RAW_BODY,
    QD_DEPTH_ALL
} qd_message_depth_t;


/** Message fields */
typedef enum {
    QD_FIELD_NONE,   // reserved

    //
    // Message Sections
    //
    QD_FIELD_ROUTER_ANNOTATION,
    QD_FIELD_HEADER,
    QD_FIELD_DELIVERY_ANNOTATION,
    QD_FIELD_MESSAGE_ANNOTATION,
    QD_FIELD_PROPERTIES,
    QD_FIELD_APPLICATION_PROPERTIES,
    QD_FIELD_BODY,
    QD_FIELD_FOOTER,

    //
    // Fields of the Header Section
    // Ordered by list position
    //
    QD_FIELD_DURABLE,
    QD_FIELD_PRIORITY,
    QD_FIELD_TTL,
    QD_FIELD_FIRST_ACQUIRER,
    QD_FIELD_DELIVERY_COUNT,

    //
    // Fields of the Properties Section
    // Ordered by list position
    //
    QD_FIELD_MESSAGE_ID,
    QD_FIELD_USER_ID,
    QD_FIELD_TO,
    QD_FIELD_SUBJECT,
    QD_FIELD_REPLY_TO,
    QD_FIELD_CORRELATION_ID,
    QD_FIELD_CONTENT_TYPE,
    QD_FIELD_CONTENT_ENCODING,
    QD_FIELD_ABSOLUTE_EXPIRY_TIME,
    QD_FIELD_CREATION_TIME,
    QD_FIELD_GROUP_ID,
    QD_FIELD_GROUP_SEQUENCE,
    QD_FIELD_REPLY_TO_GROUP_ID
} qd_message_field_t;


/**
 * Allocate a new message.
 *
 * @return A pointer to a qd_message_t that is the sole reference to a newly allocated
 *         message.
 */
qd_message_t *qd_message(void);

/**
 * Free a message reference.  If this is the last reference to the message, free the
 * message as well.
 *
 * @param msg A pointer to a qd_message_t that is no longer needed.
 */
void qd_message_free(qd_message_t *msg);

/**
 * Make a new reference to an existing message.
 *
 * @param msg A pointer to a qd_message_t referencing a message.
 * @return A new pointer to the same referenced message.
 */
qd_message_t *qd_message_copy(qd_message_t *msg);

/**
 * Parse the router annotations section from a message and place them in
 * the qd_message_t/qd_message_content_t data structures.
 *
 * @param msg Pointer to a received message.
 * @return 0 on success, else an error message
 */
const char *qd_message_parse_router_annotations(qd_message_t *msg);

/**
 * Set the value for the QD_MA_TO field in the outgoing message annotations for
 * the message.
 *
 * @param msg Pointer to an outgoing message.
 * @param to_field Pointer to a c string holding the to override address that
 * will be used as the value for the outgoing QD_MA_TO annotations map entry.
 * If null, the message will not have a QA_MA_TO message annotation field.
 */
void qd_message_set_to_override_annotation(qd_message_t *msg, const char *to_field);

/**
 * Set the value for the ingress_mesh annotation for this message.
 *
 * @param msg Pointer to an outgoing message.
 * @param mesh_identifier Pointer to a character string holding ascii characters and of
 * a length equal to QD_DISCRIMINATOR_BYTES.
 */
void qd_message_set_ingress_mesh(qd_message_t *msg, const char *mesh_identifier);

/**
 * Classify the message as streaming.
 *
 * Marking a message as streaming will prevent downstream routers from manually
 * determining if this message should be sent on an inter-router streaming
 * link. Once a message is classified as streaming it retains the
 * classification until it is delivered to an endpoint
 *
 * @param msg Pointer to an outgoing message.
 *
 */
void qd_message_set_streaming_annotation(qd_message_t *msg);

/**
 * Test whether received message should be considered to be streaming.
 *
 * @param msg Pointer to an outgoing message.
 * @return true if the received message has the streaming annotation set, else false.
 *
 */
int qd_message_is_streaming(const qd_message_t *msg);

/**
 * Classify the message as resend-released.
 *
 * This classification is used when a message is to be re-routed in the event it is RELEASED.
 *
 * @param msg Pointer to an outgoing message.
 * @param value Boolean value to set or clear the resend-released state.
 */
void qd_message_set_resend_released_annotation(qd_message_t *msg, bool value);

/**
 * Test whether a received message is marked as resend-released.
 *
 * @param msg Pointer to an incoming message.
 * @return true if the received message has the resend-released flag set.
 */
bool qd_message_is_resend_released(const qd_message_t *msg);

/**
 * Mark this message so Q2 will be disabled on this router and all downstream routers.
 *
 * This annotation can be used to disable Q2 backpressure along the entire path this message travels through the router
 * network. It should only be used if there is some other form of flow-control in effect, such as the TCP adaptor
 * windowing algorithm.
 *
 * This function will invoke qd_message_Q2_holdoff_disable() on the message to disable Q2 backpressure on the current
 * router. Note that this may invoke the Q2 unblock handler if the message is currently blocked by Q2.
 *
 * @param msg Pointer to a message.
 */
void qd_message_set_Q2_disabled_annotation(qd_message_t *msg);

/**
 * Returns true if the "disable Q2" flag is set in the message annotation section. See
 * qd_message_set_Q2_disabled_annotation()
 *
 * It is expected that the caller will use this function to check if an incoming message needs to have Q2 backpressure
 * disabled. If true the caller should invoke qd_message_Q2_holdoff_disable() on the given message prior to forwarding
 * it.
 *
 * @param msg Pointer to a message.
 */
bool qd_message_is_Q2_disabled_annotation(const qd_message_t *msg);

/**
 * Prevent the router from doing any transformations to the message annotations
 * section of the message.
 *
 * Used by link-routing to completely skip all MA handling, including parsing
 * MA on receive and restoring/composing MA on send.
 */
void qd_message_disable_router_annotations(qd_message_t *in_msg);

/**
 * Receive message data frame by frame via a delivery.  This function may be called more than once on the same
 * delivery if the message spans multiple frames. Always returns a message. The message buffers are filled up to the point with the data that was been received so far.
 * The buffer keeps filling up on successive calls to this function.
 *
 * @param delivery An incoming delivery from a link
 * @param octes_received Output of the number of octets received from the delivery.
 * @return A pointer to the complete message or 0 if the message is not yet complete.
 */
qd_message_t *qd_message_receive(pn_delivery_t *delivery, ssize_t *octets_received);

/**
 * Returns the PN_DELIVERY_CTX record from the attachments
 *
 * @param delivery An incoming delivery from a link
 * @return - pointer to qd_message_t object
 */
qd_message_t * qd_get_message_context(pn_delivery_t *delivery);

/**
 * Returns true if there is at least one non-empty buffer at the head of the content->buffers list
 * or if the content->pending buffer is non-empty.
 *
 * @param msg A pointer to a message.
 */
bool qd_message_has_data_in_content_or_pending_buffers(qd_message_t   *msg);

/**
 * Send the message outbound on an outgoing link.
 *
 * @param msg A pointer to a message to be sent.
 * @param link The outgoing link on which to send the message.
 * @param ra_flags [in] outbound router annotations control flag
 * @param q3_stalled [out] indicates that the link is stalled due to proton-buffer-full
 */
#define QD_MESSAGE_RA_STRIP_NONE    0x00  // send all router annotations
#define QD_MESSAGE_RA_STRIP_INGRESS 0x01
#define QD_MESSAGE_RA_STRIP_TRACE   0x02
#define QD_MESSAGE_RA_STRIP_ALL     0xFF  // no router annotations section sent
ssize_t qd_message_send(qd_message_t *msg, qd_link_t *link, unsigned int ra_flags, bool *q3_stalled);

/**
 * Check that the message is well-formed up to a certain depth.  Any part of the message that is
 * beyond the specified depth is not checked for validity.
 *
 * Note: some message sections are optional - QD_MESSAGE_OK is returned if the
 * optional section is not present, as that is valid.
 */
typedef enum {
    QD_MESSAGE_DEPTH_INVALID,     // corrupt or malformed message detected
    QD_MESSAGE_DEPTH_OK,          // valid up to depth, including 'depth' if not optional
    QD_MESSAGE_DEPTH_INCOMPLETE   // have not received up to 'depth', or partial depth
} qd_message_depth_status_t;

qd_message_depth_status_t qd_message_check_depth(const qd_message_t *msg, qd_message_depth_t depth);

/**
 * Return an iterator for the requested message field.  If the field is not in the message,
 * return NULL.
 *
 * @param msg A pointer to a message.
 * @param field The field to be returned via iterator.
 * @return A field iterator that spans the requested field.
 */
qd_iterator_t *qd_message_field_iterator_typed(qd_message_t *msg, qd_message_field_t field);
qd_iterator_t *qd_message_field_iterator(qd_message_t *msg, qd_message_field_t field);

ssize_t qd_message_field_length(qd_message_t *msg, qd_message_field_t field);
ssize_t qd_message_field_copy(qd_message_t *msg, qd_message_field_t field, char *buffer, size_t *hdr_length);

/**
 * Return a pointer to the first octet of non-cutthrough body data in the message.
 *
 * This is intended to be used by the message-consuming end of a cut through message. The returned pointer is to the
 * data that arrived in the message prior to cut through activation. Prior to cut through activation buffers are
 * appended to the traditional content buffer queue. Once cut through has been activated new buffers are no longer
 * appended to the content buffer queue but are instead added to the cut through buffer slots. This function allows the
 * message consumer to access these initial buffers before consuming from the cut through buffer slots.  Return NULL if
 * there is no raw body buffers available.
 *
 * @param msg A pointer to a stream
 * @param buf [out] pointer to the buffer containing the first octet of the raw body (or 0)
 * @param offset [out] The offset in the buffer to the first octet of the raw body
 */
void qd_message_get_raw_body_data(qd_message_t *msg, qd_buffer_t **buf, size_t *offset);

/**
 * This is called when the raw body has been completely consumed by a cut-through consumer.
 * In this function, the function _should_ free any buffers that purely contain body content.
 *
 * @param msg A pointer to a stream
 */
void qd_message_release_raw_body(qd_message_t *msg);

// Create a message using composed fields to supply content.
//
// This message constructor will create a new message using each fields buffers
// concatenated in order (f1 first, f2 second, etc). There is no need to
// provide all three fields: concatenation stops at the first null fx pointer.
//
// Note well that while this constructor can support up to three separate
// composed fields it is more efficent to chain as many message sections as
// possible into as few separate composed fields as possible.  This means that
// any passed composed field can contain several message sections.
//
// This constructor takes ownership of the composed fields - the caller must
// not reference them after the call.
//
qd_message_t *qd_message_compose(qd_composed_field_t *f1,
                                 qd_composed_field_t *f2,
                                 qd_composed_field_t *f3,
                                 bool receive_complete);

// The following qd_message_compose_X are deprecated: Please use the
// qd_message_compose() to create locally generated messages instead
void qd_message_compose_1(qd_message_t *msg, const char *to, qd_buffer_list_t *buffers);
void qd_message_compose_2(qd_message_t *msg, qd_composed_field_t *content, bool receive_complete);
void qd_message_compose_3(qd_message_t *msg, qd_composed_field_t *content1, qd_composed_field_t *content2, bool receive_complete);
void qd_message_compose_4(qd_message_t *msg, qd_composed_field_t *content1, qd_composed_field_t *content2, qd_composed_field_t *content3, bool receive_complete);
void qd_message_compose_5(qd_message_t *msg, qd_composed_field_t *field1, qd_composed_field_t *field2, qd_composed_field_t *field3, qd_composed_field_t *field4, bool receive_complete);

/**
 * qd_message_extend
 *
 * Extend the content of a streaming message with more buffers.
 *
 * @param msg Pointer to a message
 * @param field A composed field to be appended to the end of the message's stream
 * @param q2_blocked Set to true if this call caused Q2 to block
 * @return The number of buffers stored in the message's content
 */
int qd_message_extend(qd_message_t *msg, qd_composed_field_t *field, bool *q2_blocked);


/** Put string representation of a message suitable for logging in buffer. Note that log message text is limited to
 * QD_LOG_TEXT_MAX bytes which includes the terminating null byte.
 *
 * Invoke qd_message_repr_flags() with a string representation of the message header fields desired to be included in
 * the string. See the description of messageLoggingComponents in the router schema. This will compile the string to a
 * set of field flags.  Pass these field flags to message_repr() to specify the set of fields to include in the
 * string.
 *
 * @return buffer
 */
uint32_t qd_message_repr_flags(const char *msg_fields);
char* qd_message_repr(qd_message_t *msg, char* buffer, size_t len, uint32_t message_repr_flags);

qd_log_source_t *qd_message_log_source(void);

/**
 * Accessor for incoming messages ingress router annotation
 *
 * @param msg A pointer to the message
 * @return the parsed field or 0 if no ingress present in msg
 */
qd_parsed_field_t *qd_message_get_ingress_router(qd_message_t *msg);

/**
 * Accessor for message field to_override
 *
 * @param msg A pointer to the message
 * @return the parsed field or 0 if no to_override present
 */
qd_parsed_field_t *qd_message_get_to_override(qd_message_t *msg);

/**
 * Accessor for incoming messages trace annotation
 *
 * @param msg A pointer to the received message
 * @return the parsed field
 */
qd_parsed_field_t *qd_message_get_trace(qd_message_t *msg);

/**
 * Accessor for ingress edge-mesh annotation
 *
 * @param msg A pointer to the received message
 * @return the parsed field
 */
qd_parsed_field_t *qd_message_get_ingress_mesh(qd_message_t *msg);

/**
 * Should the message be discarded.
 * A message can be discarded if the disposition is released or rejected.
 *
 * @param msg A pointer to the message.
 **/
bool qd_message_is_discard(qd_message_t *msg);

/**
 *Set the discard field on the message to to the passed in boolean value.
 *
 * @param msg A pointer to the message.
 * @param discard - the boolean value of discard.
 */
void qd_message_set_discard(qd_message_t *msg, bool discard);

/**
 * Has the message been completely received?
 * Return true if the message is fully received
 * Returns false if only the partial message has been received, if there is more of the message to be received.
 *
 * @param msg A pointer to the message.
 */
bool qd_message_receive_complete(qd_message_t *msg);

/**
 * Returns true if the message has been completely received AND the message has been completely sent.
 */
bool qd_message_send_complete(qd_message_t *msg);

/**
 * Flag the message as being send-complete.
 */
void qd_message_set_send_complete(qd_message_t *msg);


/**
 * Flag the message as being receive-complete.
 */
void qd_message_set_receive_complete(qd_message_t *msg);


/**
 * Returns true if the delivery tag has already been sent.
 */
bool qd_message_tag_sent(qd_message_t *msg);


/**
 * Sets if the delivery tag has already been sent out or not.
 */
void qd_message_set_tag_sent(qd_message_t *msg, bool tag_sent);

/**
 * Increase the fanout of the message by 1.
 *
 * @param out_msg A pointer to the message to be sent outbound or to a local
 * subscriber.
 */
void qd_message_add_fanout(qd_message_t *out_msg);

/**
 * Disable the Q2-holdoff for this message.
 *
 * Note: this call may invoke the Q2 unblock handler routine associated with
 * this message.  See qd_message_set_q2_unblocked_handler().
 *
 * @param msg A pointer to the message
 */
void qd_message_Q2_holdoff_disable(qd_message_t *msg);

/**
 * Check if a message has hit its Q2 limit and is currently blocked.
 * When blocked no further message data will be read from the link.
 *
 * @param msg A pointer to the message
 */
bool qd_message_is_Q2_blocked(const qd_message_t *msg);


/**
 * Register a callback that will be invoked when the message has exited the Q2
 * blocking state. Note that the callback can be invoked on any I/O thread.
 * The callback must be thread safe.
 *
 * @param msg The message to monitor.
 * @param callback The method to invoke
 * @param context safe pointer holding the context
 */

typedef void (*qd_message_q2_unblocked_handler_t)(qd_alloc_safe_ptr_t context);
void qd_message_set_q2_unblocked_handler(qd_message_t *msg,
                                         qd_message_q2_unblocked_handler_t callback,
                                         qd_alloc_safe_ptr_t context);
void qd_message_clear_q2_unblocked_handler(qd_message_t *msg);

/**
 * Return message aborted state
 * @param msg A pointer to the message
 * @return true if the message has been aborted
 */
bool qd_message_aborted(const qd_message_t *msg);

/**
 * Set the aborted flag on the message.
 * @param msg A pointer to the message
 */
void qd_message_set_aborted(qd_message_t *msg);

/**
 * Return message priority
 * @param msg A pointer to the message
 * @return The message priority value. Default if not present.
 */
uint8_t qd_message_get_priority(qd_message_t *msg);

/**
 * True if message is larger that maxMessageSize
 * @param msg A pointer to the message
 * @return 
 */
bool qd_message_oversize(const qd_message_t *msg);

//=====================================================================================================
// Unicast/Cut-through API
//
// This is an optimization for the case where the message is streaming and is being delivered to
// exactly one destination.
//=====================================================================================================

#define UCT_SLOT_COUNT       8
#define UCT_RESUME_THRESHOLD 4
#define UCT_SLOT_BUF_LIMIT   16  // per slot maximum qd_buffer_t list length

/**
 * Transition this message to unicast/cut-through operation.  This action cannot be reversed for a message.
 *
 * Once this mode is set for a message stream, the conventional methods for accessing the message body will
 * no longer work.
 *
 * @param stream Pointer to the message
 */
void qd_message_start_unicast_cutthrough(qd_message_t *stream);

/**
 * Indicate whether this message stream is in unicast/cut-through mode.
 *
 * @param stream Pointer to the message
 * @return true if the message is in unicast/cut-through mode
 * @return false if not
 */
bool qd_message_is_unicast_cutthrough(const qd_message_t *stream);

/**
 * Indicate whether there is capacity to produce buffers into the stream.
 *
 * @param stream Pointer to the message
 * @return true Yes, there is capacity to produce buffers
 * @return false No, do not attempt to produce buffers
 */
bool qd_message_can_produce_buffers(const qd_message_t *stream);

/**
 * Indicate whether there are buffers to consume from the stream.
 *
 * @param stream Pointer to the message
 * @return true Yes, there are buffers to consume
 * @return false No, there are no buffers to consumer
 */
bool qd_message_can_consume_buffers(const qd_message_t *stream);

/**
 * Return the number of cut-through slots that are filled
 * 
 * @param stream Pointer to the message
 * @return int The number of slots that contain produced content
 */
int qd_message_full_slot_count(const qd_message_t *stream);

/**
 * Produce a list of buffers into the message stream.  The pn_message_can_produce_buffers must be
 * called prior to calling this function to determine whether there is capacity to produce a list
 * of buffers into the stream.  If there is no capacity, this function must not be called.
 *
 * There is no scenario in which this function will partially consume the buffer list.
 *
 * @param stream Pointer to the message
 * @param buffers Pointer to a list of buffers to be appended to the message stream
 */
void qd_message_produce_buffers(qd_message_t *stream, qd_buffer_list_t *buffers);

/**
 * Consume buffers from a message stream.
 *
 * @param stream Pointer to the message
 * @param buffers Pointer to a list of buffers to fill.  Must be empty at the call.
 * @param limit The maximum number of buffers that should be consumed.
 * @return int The number of buffers actually consumed.
 */
int qd_message_consume_buffers(qd_message_t *stream, qd_buffer_list_t *buffers, int limit);


typedef enum {
    QD_ACTIVATION_NONE = 0,
    QD_ACTIVATION_AMQP,
    QD_ACTIVATION_TCP
} qd_message_activation_type_t;

typedef struct {
    qd_message_activation_type_t  type;
    qd_alloc_safe_ptr_t           safeptr;
    qdr_delivery_t               *delivery;
} qd_message_activation_t;

/**
 * Tell the message stream which connection is consuming its buffers.
 *
 * @param stream Pointer to the message
 * @param activation Parameters for activating the consuming I/O thread
 */
void qd_message_set_consumer_activation(qd_message_t *stream, qd_message_activation_t *activation);

/**
 * Cancel the activation. No further activations will be occur on return from this call.
 *
 * @param stream Pointer to the message
 */
void qd_message_cancel_consumer_activation(qd_message_t *stream);

/**
 * Tell the message stream which connection is producing its buffers.
 *
 * @param stream Pointer to the message
 * @param activation Parameters for activating the producing I/O thread
 */
void qd_message_set_producer_activation(qd_message_t *stream, qd_message_activation_t *activation);

/**
 * Cancel the activation. No further activations will occur on return from this call.
 *
 * @param stream Pointer to the message
 */
void qd_message_cancel_producer_activation(qd_message_t *stream);

///@}

#endif

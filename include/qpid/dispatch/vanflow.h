#ifndef __vanflow_h__
#define __vanflow_h__ 1
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

#include "stdint.h"
#include "proton/codec.h"
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/iterator.h"

typedef struct vflow_record_t vflow_record_t;

/**
 * Record hierarchy for parent-child relationships (CHILD ---> PARENT)
 *
 *   PROCESS ---> SITE
 *   LINK ---> ROUTER ---> SITE
 *   [FLOW --->]* FLOW ---> LISTENER ---> ROUTER ---> SITE
 *   [FLOW --->]* FLOW ---> CONNECTOR ---> ROUTER ---> SITE
 */
typedef enum vflow_record_type {
    // Note: these values are shared with the Skupper control plane - do not re-use or change them without updating the
    // control plane component!
    VFLOW_RECORD_SITE          = 0x00,  // A cloud site involved in a VAN
    VFLOW_RECORD_ROUTER        = 0x01,  // A VAN router deployed in a site
    VFLOW_RECORD_LINK          = 0x02,  // An inter-router link to a different router
    VFLOW_RECORD_CONTROLLER    = 0x03,  // A VAN controller
    VFLOW_RECORD_LISTENER      = 0x04,  // An ingress listener for an application protocol
    VFLOW_RECORD_CONNECTOR     = 0x05,  // An egress connector for an application protocol
    VFLOW_RECORD_FLOW          = 0x06,  // An application flow between a ingress and an egress
    VFLOW_RECORD_PROCESS       = 0x07,  // A running process/pod/container that uses application protocols
    VFLOW_RECORD_IMAGE         = 0x08,  // A process-type or container image
    VFLOW_RECORD_INGRESS       = 0x09,  // An access point for external access into the VAN
    VFLOW_RECORD_EGRESS        = 0x0a,  // An entity from which external services are accessed from within the VAN
    VFLOW_RECORD_COLLECTOR     = 0x0b,  // An event collector
    VFLOW_RECORD_PROCESS_GROUP = 0x0c,  // A grouping of PROCESS
    VFLOW_RECORD_HOST          = 0x0d,  // Host (or Kubernetes Node) on which a process runs
    VFLOW_RECORD_LOG           = 0x0e,  // A notable router log event such as an error or warning
    VFLOW_RECORD_ROUTER_ACCESS = 0x0f,  // An access point for inter-router connections
} vflow_record_type_t;

// clang-format off
typedef enum vflow_attribute {
    // Note: these values are shared with the Skupper control plane - do not re-use or change them without updating the
    // control plane component!
    VFLOW_ATTRIBUTE_RECORD_TYPE      = 0,   // uint
    VFLOW_ATTRIBUTE_IDENTITY         = 1,   // Reference (cannot be set by the user)
    VFLOW_ATTRIBUTE_PARENT           = 2,   // Reference (set during object creation only)
    VFLOW_ATTRIBUTE_START_TIME       = 3,   // uint

    VFLOW_ATTRIBUTE_END_TIME         = 4,   // uint
    VFLOW_ATTRIBUTE_COUNTERFLOW      = 5,   // Reference
    VFLOW_ATTRIBUTE_PEER             = 6,   // Reference to another record that represents the same flow
    VFLOW_ATTRIBUTE_PROCESS          = 7,   // Reference

    VFLOW_ATTRIBUTE_SIBLING_ORDINAL  = 8,   // uint
    VFLOW_ATTRIBUTE_LOCATION         = 9,   // String
    VFLOW_ATTRIBUTE_PROVIDER         = 10,  // String
    VFLOW_ATTRIBUTE_PLATFORM         = 11,  // String

    VFLOW_ATTRIBUTE_NAMESPACE        = 12,  // String
    VFLOW_ATTRIBUTE_MODE             = 13,  // String
    VFLOW_ATTRIBUTE_SOURCE_HOST      = 14,  // String
    VFLOW_ATTRIBUTE_DESTINATION_HOST = 15,  // String

    VFLOW_ATTRIBUTE_PROTOCOL         = 16,  // String
    VFLOW_ATTRIBUTE_SOURCE_PORT      = 17,  // String
    VFLOW_ATTRIBUTE_DESTINATION_PORT = 18,  // String
    VFLOW_ATTRIBUTE_VAN_ADDRESS      = 19,  // String

    VFLOW_ATTRIBUTE_IMAGE_NAME       = 20,  // String
    VFLOW_ATTRIBUTE_IMAGE_VERSION    = 21,  // String
    VFLOW_ATTRIBUTE_HOST_NAME        = 22,  // String
    VFLOW_ATTRIBUTE_OCTETS           = 23,  // uint/counter

    VFLOW_ATTRIBUTE_LATENCY          = 24,  // uint
    VFLOW_ATTRIBUTE_TRANSIT_LATENCY  = 25,  // uint
    VFLOW_ATTRIBUTE_BACKLOG          = 26,  // uint
    VFLOW_ATTRIBUTE_METHOD           = 27,  // String

    VFLOW_ATTRIBUTE_RESULT           = 28,  // String
    VFLOW_ATTRIBUTE_REASON           = 29,  // String
    VFLOW_ATTRIBUTE_NAME             = 30,  // String
    VFLOW_ATTRIBUTE_TRACE            = 31,  // List of Strings (use vflow_set_trace function)

    VFLOW_ATTRIBUTE_BUILD_VERSION    = 32,  // String
    VFLOW_ATTRIBUTE_LINK_COST        = 33,  // uint
    VFLOW_ATTRIBUTE_DIRECTION        = 34,  // String
    VFLOW_ATTRIBUTE_OCTET_RATE       = 35,  // uint

    VFLOW_ATTRIBUTE_OCTETS_OUT       = 36,  // uint/counter
    VFLOW_ATTRIBUTE_OCTETS_UNACKED   = 37,  // uint
    VFLOW_ATTRIBUTE_WINDOW_CLOSURES  = 38,  // uint/counter
    VFLOW_ATTRIBUTE_WINDOW_SIZE      = 39,  // uint

    VFLOW_ATTRIBUTE_FLOW_COUNT_L4    = 40,  // uint/counter
    VFLOW_ATTRIBUTE_FLOW_COUNT_L7    = 41,  // uint/counter
    VFLOW_ATTRIBUTE_FLOW_RATE_L4     = 42,  // uint
    VFLOW_ATTRIBUTE_FLOW_RATE_L7     = 43,  // uint

    VFLOW_ATTRIBUTE_DURATION         = 44,  // uint
    VFLOW_ATTRIBUTE_IMAGE            = 45,  // Reference
    VFLOW_ATTRIBUTE_GROUP            = 46,  // Reference
    VFLOW_ATTRIBUTE_STREAM_ID        = 47,  // uint

    VFLOW_ATTRIBUTE_LOG_SEVERITY     = 48,  // uint
    VFLOW_ATTRIBUTE_LOG_TEXT         = 49,  // String
    VFLOW_ATTRIBUTE_SOURCE_FILE      = 50,  // String
    VFLOW_ATTRIBUTE_SOURCE_LINE      = 51,  // uint

    VFLOW_ATTRIBUTE_LINK_COUNT       = 52,  // uint/counter
    VFLOW_ATTRIBUTE_OPER_STATUS      = 53,  // String
    VFLOW_ATTRIBUTE_ROLE             = 54,  // String
    VFLOW_ATTRIBUTE_UP_TIMESTAMP     = 55,  // uint     Timestamp of last transition to oper-status up
} vflow_attribute_t;
// clang-format on

#define VALID_REF_ATTRS     0x00006000000000e6
#define VALID_UINT_ATTRS    0x00999ffa07800119
#define VALID_COUNTER_ATTRS 0x0010035000800000
#define VALID_STRING_ATTRS  0x00660005787ffe00
#define VALID_TRACE_ATTRS   0x0000000080000000

typedef enum vflow_log_severity {
    // Note: these values are shared with the Skupper control plane - do not re-use or change them without updating the
    // control plane component!
    VFLOW_LOG_SEVERITY_DEBUG    = 0,  // debugging events
    VFLOW_LOG_SEVERITY_INFO     = 1,  // normal operational events
    VFLOW_LOG_SEVERITY_WARNING  = 2,  // recoverable or transient failure with minor or no loss of service
    VFLOW_LOG_SEVERITY_ERROR    = 3,  // service affecting failure that requires intervention
    VFLOW_LOG_SEVERITY_CRITICAL = 4   // catastrophic loss of routing service
} vflow_log_severity_t;

/**
 * vflow_start_record
 * 
 * Open a new protocol-log record, specifying the parent record that the new record is
 * a child of.
 * 
 * @param record_type The type for the newly opened record
 * @param parent Pointer to the parent record.  If NULL, it will reference the local SOURCE record.
 * @return Pointer to the new record
 */
vflow_record_t *vflow_start_record(vflow_record_type_t record_type, vflow_record_t *parent);

/**
 * vflow_end_record
 * 
 * Close a record when it is no longer needed.  After a record is closed, it cannot be referenced
 * or accessed in any way thereafter.
 * 
 * @param record The record pointer returned by vflow_start_record
 */
void vflow_end_record(vflow_record_t *record);

/**
 * vflow_serialize_identity
 * 
 * Encode the identity of the indicated record into the supplied composed-field.
 * 
 * @param record Pointer to the record from which to obtain the identity
 * @param field Pointer to the composed-field into which to serialize the identity
 */
void vflow_serialize_identity(const vflow_record_t *record, qd_composed_field_t *field);

/**
 * vflow_serialize_identity_pn
 * 
 * Encode the identity of the indicated record into the supplied Proton pn_data
 * 
 * @param record Pointer to the record from which to obtain the identity
 * @param field Pointer to the pn_data into which to serialize the identity
 */
void vflow_serialize_identity_pn(const vflow_record_t *record, pn_data_t *data);

/**
 * vflow_set_ref_from_record
 * 
 * Set a reference-typed attribute in a record from the ID of another record.
 * 
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param record Pointer to the referenced record.
 */
void vflow_set_ref_from_record(vflow_record_t *record, vflow_attribute_t attribute_type, vflow_record_t *referenced_record);


/**
 * vflow_set_ref_from_parsed
 * 
 * Set a reference-typed attribute in a record from a parsed field (a serialized identity).
 * 
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param field Pointer to a parsed field containing the serialized form of a record identity
 */
void vflow_set_ref_from_parsed(vflow_record_t *record, vflow_attribute_t attribute_type, qd_parsed_field_t *field);


/**
 * vflow_set_ref_from_iter
 *
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param iter Pointer to an iterator containing the serialized form of a record identity
 */
void vflow_set_ref_from_iter(vflow_record_t *record, vflow_attribute_t attribute_type, qd_iterator_t *iter);

/**
 * vflow_set_ref_from_pn
 *
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param iter Pointer to a Proton pn_data
 */
void vflow_set_ref_from_pn(vflow_record_t *record, vflow_attribute_t attribute_type, pn_data_t *data);

/**
 * vflow_set_string
 * 
 * Set a string-typed attribute in a record.
 * 
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param value The string value to be set
 */
void vflow_set_string(vflow_record_t *record, vflow_attribute_t attribute_type, const char *value);

/**
 * vflow_set_uint64
 * 
 * Set a uint64-typed attribute in a record.
 * 
 * @param record The record pointer returned by vflow_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param value The unsigned integer value to be set
 */
void vflow_set_uint64(vflow_record_t *record, vflow_attribute_t attribute_type, uint64_t value);


/**
 * vflow_set_trace
 * 
 * Set the VFLOW_ATTRIBUTE_TRACE attribute of the record using the trace annotation from the
 * referenced message.
 * 
 * @param record The record pointer returned by vflow_start_record
 * @param msg Pointer to a message from which to extract the path trace
 */
void vflow_set_trace(vflow_record_t *record, qd_message_t *msg);


/**
 * @brief Make and record a latency measurement for this record.
 * Call vflow_latency_start at the beginning of the latency interval.
 * Call vflow_latency_end at the end of the latency interval.  This will set the latency attribute.
 *
 * vflow_latency_start may be called multiple times.  vflow_latency_end will only measure the time
 * interval back to the most recent call to vflow_latency_start.
 *
 * If vflow_latency_end is called without a prior call to vflow_latency_start, the call will do nothing.
 *
 * @param record The record pointer returned by vflow_start_record
 */
void vflow_latency_start(vflow_record_t *record);
void vflow_latency_end(vflow_record_t *record);

/**
 * @brief Request that the rate of change of a counter attribute be stored in a rate attribute.
 *
 * @param record The record pointer returned by plg_start_record
 * @param count_attribute The attribute identifier of the counter to be measured
 * @param rate_attribute The attribute identifier for the rate to be stored
 */
void vflow_add_rate(vflow_record_t *record, vflow_attribute_t count_attribute, vflow_attribute_t rate_attribute);

#endif

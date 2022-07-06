#ifndef __protocol_log_h__
#define __protocol_log_h__ 1
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
#include "qpid/dispatch/message.h"
#include "qpid/dispatch/iterator.h"

typedef struct plog_record_t plog_record_t;

/**
 * Record hierarchy for parent-child relationships (CHILD ---> PARENT)
 *
 *   PROCESS ---> SITE
 *   LINK ---> ROUTER ---> SITE
 *   [FLOW --->]* FLOW ---> LISTENER ---> ROUTER ---> SITE
 *   [FLOW --->]* FLOW ---> CONNECTOR ---> ROUTER ---> SITE
 */
typedef enum plog_record_type {
    PLOG_RECORD_SITE       = 0x00,  // A cloud site involved in a VAN
    PLOG_RECORD_ROUTER     = 0x01,  // A VAN router deployed in a site
    PLOG_RECORD_LINK       = 0x02,  // An inter-router link to a different router
    PLOG_RECORD_CONTROLLER = 0x03,  // A VAN controller
    PLOG_RECORD_LISTENER   = 0x04,  // An ingress listener for an application protocol
    PLOG_RECORD_CONNECTOR  = 0x05,  // An egress connector for an application protocol
    PLOG_RECORD_FLOW       = 0x06,  // An application flow between a ingress and an egress
    PLOG_RECORD_PROCESS    = 0x07,  // A running process/pod/container that uses application protocols
    PLOG_RECORD_IMAGE      = 0x08,  // A process-type or container image
    PLOG_RECORD_INGRESS    = 0x09,  // An access point for external access into the VAN
    PLOG_RECORD_EGRESS     = 0x0a,  // An entity from which external services are accessed from within the VAN
    PLOG_RECORD_COLLECTOR  = 0x0b,  // An event collector
} plog_record_type_t;

typedef enum plog_attribute {
    PLOG_ATTRIBUTE_RECORD_TYPE      = 0,   // uint
    PLOG_ATTRIBUTE_IDENTITY         = 1,   // Reference (cannot be set by the user)
    PLOG_ATTRIBUTE_PARENT           = 2,   // Reference (set during object creation only)
    PLOG_ATTRIBUTE_START_TIME       = 3,   // uint

    PLOG_ATTRIBUTE_END_TIME         = 4,   // uint
    PLOG_ATTRIBUTE_COUNTERFLOW      = 5,   // Reference
    PLOG_ATTRIBUTE_PEER             = 6,   // Reference to another record that represents the same flow
    PLOG_ATTRIBUTE_PROCESS          = 7,   // Reference

    PLOG_ATTRIBUTE_SIBLING_ORDINAL  = 8,   // uint
    PLOG_ATTRIBUTE_LOCATION         = 9,   // String
    PLOG_ATTRIBUTE_PROVIDER         = 10,  // String
    PLOG_ATTRIBUTE_PLATFORM         = 11,  // String

    PLOG_ATTRIBUTE_NAMESPACE        = 12,  // String
    PLOG_ATTRIBUTE_MODE             = 13,  // String
    PLOG_ATTRIBUTE_SOURCE_HOST      = 14,  // String
    PLOG_ATTRIBUTE_DESTINATION_HOST = 15,  // String

    PLOG_ATTRIBUTE_PROTOCOL         = 16,  // String
    PLOG_ATTRIBUTE_SOURCE_PORT      = 17,  // String
    PLOG_ATTRIBUTE_DESTINATION_PORT = 18,  // String
    PLOG_ATTRIBUTE_VAN_ADDRESS      = 19,  // String

    PLOG_ATTRIBUTE_IMAGE_NAME       = 20,  // String
    PLOG_ATTRIBUTE_IMAGE_VERSION    = 21,  // String
    PLOG_ATTRIBUTE_HOST_NAME        = 22,  // String
    PLOG_ATTRIBUTE_OCTETS           = 23,  // uint

    PLOG_ATTRIBUTE_LATENCY          = 24,  // uint
    PLOG_ATTRIBUTE_TRANSIT_LATENCY  = 25,  // uint
    PLOG_ATTRIBUTE_BACKLOG          = 26,  // uint
    PLOG_ATTRIBUTE_METHOD           = 27,  // String

    PLOG_ATTRIBUTE_RESULT           = 28,  // String
    PLOG_ATTRIBUTE_REASON           = 29,  // String
    PLOG_ATTRIBUTE_NAME             = 30,  // String
    PLOG_ATTRIBUTE_TRACE            = 31,  // List of Strings (use plog_set_trace function)

    PLOG_ATTRIBUTE_BUILD_VERSION    = 32,  // String
    PLOG_ATTRIBUTE_LINK_COST        = 33,  // uint
    PLOG_ATTRIBUTE_DIRECTION        = 34,  // String
    PLOG_ATTRIBUTE_OCTET_RATE       = 35,  // uint

    PLOG_ATTRIBUTE_OCTETS_OUT       = 36,  // uint
    PLOG_ATTRIBUTE_OCTETS_UNACKED   = 37,  // uint
    PLOG_ATTRIBUTE_WINDOW_CLOSURES  = 38,  // uint
    PLOG_ATTRIBUTE_WINDOW_SIZE      = 39,  // uint
} plog_attribute_t;

#define VALID_REF_ATTRS    0x00000000000000e6
#define VALID_UINT_ATTRS   0x000000fa07800119
#define VALID_STRING_ATTRS 0x00000005787ffe00
#define VALID_TRACE_ATTRS  0x0000000080000000


/**
 * plog_start_record
 * 
 * Open a new protocol-log record, specifying the parent record that the new record is
 * a child of.
 * 
 * @param record_type The type for the newly opened record
 * @param parent Pointer to the parent record.  If NULL, it will reference the local SOURCE record.
 * @return Pointer to the new record
 */
plog_record_t *plog_start_record(plog_record_type_t record_type, plog_record_t *parent);

/**
 * plog_end_record
 * 
 * Close a record when it is no longer needed.  After a record is closed, it cannot be referenced
 * or accessed in any way thereafter.
 * 
 * @param record The record pointer returned by plog_start_record
 */
void plog_end_record(plog_record_t *record);

/**
 * plog_serialize_identity
 * 
 * Encode the identity of the indicated record into the supplied composed-field.
 * 
 * @param record Pointer to the record from which to obtain the identity
 * @param field Pointer to the composed-field into which to serialize the identity
 */
void plog_serialize_identity(const plog_record_t *record, qd_composed_field_t *field);


/**
 * plog_set_ref_from_record
 * 
 * Set a reference-typed attribute in a record from the ID of another record.
 * 
 * @param record The record pointer returned by plog_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param record Pointer to the referenced record.
 */
void plog_set_ref_from_record(plog_record_t *record, plog_attribute_t attribute_type, plog_record_t *referenced_record);


/**
 * plog_set_ref_from_parsed
 * 
 * Set a reference-typed attribute in a record from a parsed field (a serialized identity).
 * 
 * @param record The record pointer returned by plog_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param field Pointer to a parsed field containing the serialized form of a record identity
 */
void plog_set_ref_from_parsed(plog_record_t *record, plog_attribute_t attribute_type, qd_parsed_field_t *field);

/**
 * plog_set_string
 * 
 * Set a string-typed attribute in a record.
 * 
 * @param record The record pointer returned by plog_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param value The string value to be set
 */
void plog_set_string(plog_record_t *record, plog_attribute_t attribute_type, const char *value);

/**
 * plog_set_uint64
 * 
 * Set a uint64-typed attribute in a record.
 * 
 * @param record The record pointer returned by plog_start_record
 * @param attribute_type The type of the attribute (see enumerated above) to be set
 * @param value The unsigned integer value to be set
 */
void plog_set_uint64(plog_record_t *record, plog_attribute_t attribute_type, uint64_t value);


/**
 * plog_set_trace
 * 
 * Set the PLOG_ATTRIBUTE_TRACE attribute of the record using the trace annotation from the
 * referenced message.
 * 
 * @param record The record pointer returned by plog_start_record
 * @param msg Pointer to a message from which to extract the path trace
 */
void plog_set_trace(plog_record_t *record, qd_message_t *msg);


/**
 * @brief Make and record a latency measurement for this record.
 * Call plog_latency_start at the beginning of the latency interval.
 * Call plog_latency_end at the end of the latency interval.  This will set the latency attribute.
 *
 * plog_latency_start may be called multiple times.  plog_latency_end will only measure the time
 * interval back to the most recent call to plog_latency_start.
 *
 * If plog_latency_end is called without a prior call to plog_latency_start, the call will do nothing.
 *
 * @param record The record pointer returned by plog_start_record
 */
void plog_latency_start(plog_record_t *record);
void plog_latency_end(plog_record_t *record);

/**
 * @brief Request that the rate of change of a counter attribute be stored in a rate attribute.
 *
 * @param record The record pointer returned by plg_start_record
 * @param count_attribute The attribute identifier of the counter to be measured
 * @param rate_attribute The attribute identifier for the rate to be stored
 */
void plog_add_rate(plog_record_t *record, plog_attribute_t count_attribute, plog_attribute_t rate_attribute);

#endif

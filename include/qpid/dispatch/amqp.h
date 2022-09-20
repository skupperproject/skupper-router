#ifndef __dispatch_amqp_h__
#define __dispatch_amqp_h__ 1
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

#include <stdint.h>

/**@file
 * AMQP constants.
 *
 *@defgroup amqp amqp
 *
 * AMQP related constant definitions.
 */
/// @{

/**
 * Skupper type domain identifier "SKPR"
 */
#define QD_SKUPPER_DOMAIN_ID UINT32_C(0x534B5052)


/**
 * AMQP Constants
 */
typedef enum {
    QD_AMQP_MIN_MAX_FRAME_SIZE = 512,
    QD_AMQP_PORT_INT = 5672,
    QD_AMQPS_PORT_INT = 5671
} qd_amqp_constants_t;

extern const char * const QD_AMQP_PORT_STR;
extern const char * const QD_AMQPS_PORT_STR;

/* Returns -1 if string is not a number or recognized symbolic port name */
int qd_port_int(const char* port_str);

/**
 * AMQP Performative Tags
 */
typedef uint64_t qd_amqp_performative_t;
#define QD_PERFORMATIVE_ROUTER_ANNOTATIONS     UINT64_C(0x534B50522D2D5241) // "SKPR--RA"
#define QD_PERFORMATIVE_HEADER                 UINT64_C(0x70)
#define QD_PERFORMATIVE_DELIVERY_ANNOTATIONS   UINT64_C(0x71)
#define QD_PERFORMATIVE_MESSAGE_ANNOTATIONS    UINT64_C(0x72)
#define QD_PERFORMATIVE_PROPERTIES             UINT64_C(0x73)
#define QD_PERFORMATIVE_APPLICATION_PROPERTIES UINT64_C(0x74)
#define QD_PERFORMATIVE_BODY_DATA              UINT64_C(0x75)
#define QD_PERFORMATIVE_BODY_AMQP_SEQUENCE     UINT64_C(0x76)
#define QD_PERFORMATIVE_BODY_AMQP_VALUE        UINT64_C(0x77)
#define QD_PERFORMATIVE_FOOTER                 UINT64_C(0x78)

/**
 * AMQP Type Tags
 */
typedef enum {
    QD_AMQP_NULL = 0x40,
    QD_AMQP_BOOLEAN = 0x56,
    QD_AMQP_TRUE = 0x41,
    QD_AMQP_FALSE = 0x42,
    QD_AMQP_UBYTE = 0x50,
    QD_AMQP_USHORT = 0x60,
    QD_AMQP_UINT = 0x70,
    QD_AMQP_SMALLUINT = 0x52,
    QD_AMQP_UINT0 = 0x43,
    QD_AMQP_ULONG = 0x80,
    QD_AMQP_SMALLULONG = 0x53,
    QD_AMQP_ULONG0 = 0x44,
    QD_AMQP_BYTE = 0x51,
    QD_AMQP_SHORT = 0x61,
    QD_AMQP_INT = 0x71,
    QD_AMQP_SMALLINT = 0x54,
    QD_AMQP_LONG = 0x81,
    QD_AMQP_SMALLLONG = 0x55,
    QD_AMQP_FLOAT = 0x72,
    QD_AMQP_DOUBLE = 0x82,
    QD_AMQP_DECIMAL32 = 0x74,
    QD_AMQP_DECIMAL64 = 0x84,
    QD_AMQP_DECIMAL128 = 0x94,
    QD_AMQP_UTF32 = 0x73,
    QD_AMQP_TIMESTAMP = 0x83,
    QD_AMQP_UUID = 0x98,
    QD_AMQP_VBIN8 = 0xa0,
    QD_AMQP_VBIN32 = 0xb0,
    QD_AMQP_STR8_UTF8 = 0xa1,
    QD_AMQP_STR32_UTF8 = 0xb1,
    QD_AMQP_SYM8 = 0xa3,
    QD_AMQP_SYM32 = 0xb3,
    QD_AMQP_LIST0 = 0x45,
    QD_AMQP_LIST8 = 0xc0,
    QD_AMQP_LIST32 = 0xd0,
    QD_AMQP_MAP8 = 0xc1,
    QD_AMQP_MAP32 = 0xd1,
    QD_AMQP_ARRAY8 = 0xe0,
    QD_AMQP_ARRAY32 = 0xf0,
} qd_amqp_type_t;

// Router annotations: maximum supported version
//
#define QD_ROUTER_ANNOTATIONS_VERSION 2

/** @name Application Propertiy Names */
/// @{
extern const char * const QD_AP_FLOW_ID;  ///< Flow-ID for correlating flow and counter-flow records
/// @}

/** @name Container Capabilities */
/// @{
extern const char * const QD_CAPABILITY_ANONYMOUS_RELAY;
extern const char * const QD_CAPABILITY_STREAMING_LINKS;
/// @}

/** @name Link Terminus Capabilities */
/// @{
extern const char * const QD_CAPABILITY_ROUTER_CONTROL;
extern const char * const QD_CAPABILITY_ROUTER_DATA;
extern const char * const QD_CAPABILITY_EDGE_DOWNLINK;
/// @}

/** @name Dynamic Node Properties */
/// @{
extern const char * const QD_DYNAMIC_NODE_PROPERTY_ADDRESS;  ///< Address for routing dynamic sources
/// @}

/** @name Connection Properties */
/// @{
extern const char * const QD_CONNECTION_PROPERTY_PRODUCT_KEY;
extern const char * const QD_CONNECTION_PROPERTY_PRODUCT_VALUE;
extern const char * const QD_CONNECTION_PROPERTY_VERSION_KEY;
extern const char * const QD_CONNECTION_PROPERTY_COST_KEY;
extern const char * const QD_CONNECTION_PROPERTY_ROLE_KEY;
extern const char * const QD_CONNECTION_PROPERTY_CONN_ID;
extern const char * const QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY;
extern const char * const QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY;
extern const char * const QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY;
extern const char * const QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY;
extern const char * const QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY;
extern const char * const QD_CONNECTION_PROPERTY_ADAPTOR_KEY;
extern const char * const QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE;
extern const char * const QD_CONNECTION_PROPERTY_ANNOTATIONS_VERSION_KEY;
/// @}

/** @name Terminus Addresses */
/// @{
extern const char * const QD_TERMINUS_EDGE_ADDRESS_TRACKING;
extern const char * const QD_TERMINUS_ADDRESS_LOOKUP;
extern const char * const QD_TERMINUS_HEARTBEAT;
/// @}

/** @name AMQP error codes. */
/// @{
/** AMQP management error status code and string description (HTTP-style)  */
typedef struct qd_amqp_error_t { int status; const char* description; } qd_amqp_error_t;
extern const qd_amqp_error_t QD_AMQP_OK;
extern const qd_amqp_error_t QD_AMQP_CREATED;
extern const qd_amqp_error_t QD_AMQP_NO_CONTENT;
extern const qd_amqp_error_t QD_AMQP_FORBIDDEN;
extern const qd_amqp_error_t QD_AMQP_BAD_REQUEST;
extern const qd_amqp_error_t QD_AMQP_NOT_FOUND;
extern const qd_amqp_error_t QD_AMQP_NOT_IMPLEMENTED;
/// @}

/** @name Standard AMQP error condition names. */
/// @{
extern const char * const QD_AMQP_COND_INTERNAL_ERROR;
extern const char * const QD_AMQP_COND_NOT_FOUND;
extern const char * const QD_AMQP_COND_UNAUTHORIZED_ACCESS;
extern const char * const QD_AMQP_COND_DECODE_ERROR;
extern const char * const QD_AMQP_COND_RESOURCE_LIMIT_EXCEEDED;
extern const char * const QD_AMQP_COND_NOT_ALLOWED;
extern const char * const QD_AMQP_COND_INVALID_FIELD;
extern const char * const QD_AMQP_COND_NOT_IMPLEMENTED;
extern const char * const QD_AMQP_COND_RESOURCE_LOCKED;
extern const char * const QD_AMQP_COND_PRECONDITION_FAILED;
extern const char * const QD_AMQP_COND_RESOURCE_DELETED;
extern const char * const QD_AMQP_COND_ILLEGAL_STATE;
extern const char * const QD_AMQP_COND_FRAME_SIZE_TOO_SMALL;

extern const char * const QD_AMQP_COND_CONNECTION_FORCED;

extern const char * const QD_AMQP_COND_MESSAGE_SIZE_EXCEEDED;
/// @};

/** @name AMQP link endpoint role. */
/// @{
#define QD_AMQP_LINK_ROLE_SENDER   false
#define QD_AMQP_LINK_ROLE_RECEIVER true
/// @};

/** @name AMQP Message priority. */
/// @{
#define QDR_N_PRIORITIES     10
#define QDR_MAX_PRIORITY     (QDR_N_PRIORITIES - 1)
#define QDR_DEFAULT_PRIORITY  4
/// @};

#endif

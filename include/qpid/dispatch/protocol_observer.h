#ifndef __protocol_observer_h__
#define __protocol_observer_h__ 1
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

#include <qpid/dispatch/buffer.h>
#include <qpid/dispatch/vanflow.h>

/**
 * Callback type to indicate VAN address for cross-VAN transport.
 *
 * @param transport_context The context provided in protocol_observer_first.
 * @param address The VAN address to be used to encapsulate this connection.
 */
typedef void (*protocol_observer_use_address_t) (void *transport_context, const char *address);

typedef struct protocol_observer_config_t;

/**
 * Create a new protocol observer context
 *
 * @param use_address Callback address for use-address indications.
 * @param allow_all_protocols If true, allow all protocols and deny exceptions.  If false, deny all and allow exceptions.
 * @return protocol_observer_config_t* Newly allocated config record.
 */
protocol_observer_config_t *protocol_observer_config(protocol_observer_use_address_t use_address, bool allow_all_protocols);

/**
 * Free an allocated protocol observer config.
 *
 * @param config Configuration returned by protocol_observer_config
 */
void protocol_observer_config_free(protocol_observer_config_t *config);

/**
 * Add an exception to the list of protocols allowed or denied
 *
 * @param config Configuration returned by protocol_observer_config
 * @param protocol The name of an application protocol (HTTPv1, TLSv3, ...)
 */
void protocol_observer_config_add_exception_protocol(protocol_observer_config_t *config, const char *protocol);

/**
 * Add an address to be mapped from a protocol field.
 *
 * @param config Configuration returned by protocol_observer_config
 * @param field The name of the field used for the mapping
 * @param value The value of the above field to map
 * @param address The address mapped to this field value
 */
void protocol_observer_config_add_address(protocol_observer_config_t *config, const char *field, const char *value, const char *address);


typedef struct protocol_observer_t;

/**
 * Create a new protocol observer.  Protocol observers take raw octets and attempt to detect the application protocol
 * in use.
 *
 * @param base Indicates which protocol is the base for detection (i.e. TCP, UDP, SCTP)
 * @param config Configuration returned by protocol_observer_config
 * @return protocol_observer_t* Newly allocated observer
 */
protocol_observer_t *protocol_observer(const char *base, protocol_observer_config_t *config);

/**
 * Free an allocated observer
 *
 * @param observer The observer returned by protocol_observer
 */
void protocol_observer_free(protocol_observer_t *observer);

/**
 * Provide the first buffer for a new connection.  This is payload sent from the client of the connection.
 *
 * @param observer The observer returned by protocol_observer
 * @param vflow The vanflow record for the client-side transport flow
 * @param transport_context A context unique to this connections transport (used in callbacks)
 * @param buf The buffer containing the protocol payload
 * @param offset The offset into the buffer where the first protocol octet is found
 * @return void* A connection handle that references the observer's state for this connection
 */
void *protocol_observer_first(protocol_observer_t *observer, vflow_record_t *vflow, void *transport_context, qd_buffer_t *buf, size_t offset);

/**
 * Provide subsequent payload data to an already established connection.
 *
 * @param connection_handle The handle returned by protocol_observer_first
 * @param from_client True if this payload is from the client, false if from the server
 * @param buf The buffer containing the protocol payload
 * @param offset The offset into the buffer where the next protocol octet is found
 */
void protocol_observer_data(void *connection_handle, bool from_client, qd_buffer_t *buf, size_t offset);

/**
 * Indicate the end of a connection.
 *
 * @param connection_handle The handle returned by protocol_observer_first.  This handle
 *                          should not be used after making this call.
 */
void protocol_observer_end(void *connection_handle);

#endif

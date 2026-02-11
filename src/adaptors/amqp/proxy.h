#ifndef __proxy_h__
#define __proxy_h__ 1
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

typedef struct qd_proxy_profile_t    qd_proxy_profile_t;
typedef struct qd_proxy_context_t    qd_proxy_context_t;
typedef struct qd_proxy_setup_info_t qd_proxy_setup_info_t;

/* One for each negotiation attempt with the proxy server intermediary.
 * Holds info for separate thread startup and for completion timer callback.
 */
struct qd_proxy_setup_info_t {
    qd_connector_t     *connector;
    qd_connection_t    *qd_conn;  // Lives independently of connector until proxy negotiated
    const char         *target_host;
    const char         *target_port;
    qd_proxy_profile_t *profile;
    sys_thread_t       *proxy_thread;
    qd_timer_t         *callback_timer;
    int                 proxy_socket;
};

void qd_proxy_initialize(void);
void qd_proxy_finalize(void);
void qd_proxy_free(qd_proxy_setup_info_t *info);


typedef void (*qd_timer_cb_t)(void *context);
void  qd_proxy_setup_lh(qd_connector_t *c, qd_connection_t *qd_conn, const char *host, const char *port,
                        qd_proxy_context_t *ctx, qd_timer_cb_t cb);
void *qd_configure_proxy_profile(qd_dispatch_t *qd, qd_entity_t *entity);

QD_EXPORT void                qd_delete_proxy_profile(qd_dispatch_t *qd, void *impl);
QD_EXPORT qd_error_t          qd_entity_refresh_proxyProfile(qd_entity_t *entity, void *impl);
QD_EXPORT qd_proxy_context_t *qd_proxy_context(const char *proxy_profile_name);
QD_EXPORT void                qd_proxy_context_incref(qd_proxy_context_t *cntx);
QD_EXPORT void                qd_proxy_context_decref(qd_proxy_context_t *cntx);

#endif

#ifndef __adaptor_common_h__
#define __adaptor_common_h__ 1

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

#include "entity.h"

#include "qpid/dispatch/dispatch.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/log.h"
#include "qpid/dispatch/alloc_pool.h"

#include <proton/tls.h>

typedef enum {
    QD_AGGREGATION_NONE,
    QD_AGGREGATION_JSON,
    QD_AGGREGATION_MULTIPART
} qd_http_aggregation_t;

typedef struct qd_adaptor_config_t qd_adaptor_config_t;

struct qd_adaptor_config_t
{
    char              *name;
    char              *host;
    char              *port;
    char              *address;
    char              *site_id;
    char              *host_port;

    //TLS related info
    char              *ssl_profile_name;
    bool               require_tls;
    bool               authenticate_peer;
    bool               verify_host_name;
};


ALLOC_DECLARE(qd_adaptor_config_t);

qd_error_t qd_load_adaptor_config(qd_dispatch_t *qd, qd_adaptor_config_t *config, qd_entity_t* entity, qd_log_source_t *log_source);
void qd_free_adaptor_config(qd_adaptor_config_t *config);

/**
 * Configure a connection's pn_tls objects
 *     Info log describes objects being configured
 * On success:
 *     tls_config and tls_session are set up
 * On failure:
 *     Error log is written
 *     All in-progress pn_tls objects are destroyed
 */
bool qd_initial_tls_setup(qd_adaptor_config_t *config,
                          qd_dispatch_t       *qd,
                          pn_tls_config_t     **tls_config,
                          pn_tls_t            **tls_session,
                          qd_log_source_t     *log_source,
                          uint64_t             conn_id,
                          bool                 is_listener,
                          bool                *tls_has_output,
                          const char          *protocols[]);

#endif // __adaptor_common_h__

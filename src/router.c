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

#include "router_private.h"

const char *QD_ROUTER_NODE_TYPE = "router.node";
const char *QD_ROUTER_ADDRESS_TYPE = "router.address";
const char *QD_ROUTER_LINK_TYPE = "router.link";

static char *node_id;

static void qd_router_timer_handler(void *context)
{
    qd_router_t *router = (qd_router_t*) context;

    //
    // Periodic processing.
    //
    qd_pyrouter_tick(router);

    // This sends a tick into the core and this happens every second.
    qdr_process_tick(router->router_core);
    qd_timer_schedule(router->timer, 1000);
}


// not api, but needed by unit tests
void qd_router_id_initialize(const char *area, const char *id)
{
    size_t dplen = 2 + strlen(area) + strlen(id);
    node_id = (char*) qd_malloc(dplen);
    strcpy(node_id, area);
    strcat(node_id, "/");
    strcat(node_id, id);
}


// not api, but needed by unit tests
void qd_router_id_finalize(void)
{
    free(node_id);
    node_id = 0;
}


const char *qd_router_id(void)
{
    assert(node_id);
    return node_id;
}


qd_router_t *qd_router(qd_dispatch_t *qd, qd_router_mode_t mode, const char *area, const char *id)
{
    qd_router_id_initialize(area, id);

    qd_router_t *router = NEW(qd_router_t);
    ZERO(router);

    qd->router = router;
    router->qd           = qd;
    router->router_core  = 0;
    router->router_mode  = mode;
    router->router_area  = area;
    router->router_id    = id;
    router->van_id       = qd->van_id;

    sys_mutex_init(&router->lock);
    router->timer = qd_timer(qd, qd_router_timer_handler, (void*) router);

    //
    // Inform the field iterator module of this router's mode, id, and area.  The field iterator
    // uses this to offload some of the address-processing load from the router.
    //
    qd_iterator_set_address(mode == QD_ROUTER_MODE_EDGE, area, id);

    switch (router->router_mode) {
        case QD_ROUTER_MODE_STANDALONE:
            qd_log(LOG_ROUTER, QD_LOG_INFO, "Router started in Standalone mode");
            break;
        case QD_ROUTER_MODE_INTERIOR:
            qd_log(LOG_ROUTER, QD_LOG_INFO, "Router started in Interior mode, area=%s id=%s", area, id);
            break;
        case QD_ROUTER_MODE_EDGE:
            qd_log(LOG_ROUTER, QD_LOG_INFO, "Router started in Edge mode");
            break;
        case QD_ROUTER_MODE_ENDPOINT:
            qd_log(LOG_ROUTER, QD_LOG_INFO, "Router started in Endpoint mode");
            break;
    }

    qd_log(LOG_ROUTER, QD_LOG_INFO, "Version: %s", QPID_DISPATCH_VERSION);

    return router;
}


void qd_router_free(qd_router_t *router)
{
    if (!router) return;

    //
    // The char* router->router_id and router->router_area are owned by qd->router_id and qd->router_area respectively
    // We will set them to zero here just in case anybody tries to use these fields.
    //
    router->router_id = 0;
    router->router_area = 0;
    router->van_id = 0;

    qd_router_python_free(router);
    qdr_core_free(router->router_core);
    qd_tracemask_free(router->tracemask);
    qd_timer_free(router->timer);
    sys_mutex_free(&router->lock);
    qd_router_configure_free(router);

    free(router);
    qd_router_id_finalize();
}

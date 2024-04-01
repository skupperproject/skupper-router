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

#include "config.h"
#include "http.h"
#include "server_private.h"

// KAG: todo: fix these layering violations:
#include "adaptors/amqp/qd_connection.h"
#include "adaptors/amqp/qd_listener.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/amqp.h"
#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "qpid/dispatch/threading.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/connection_counters.h"

#include <proton/connection_driver.h>
#include <proton/object.h>

#include <ctype.h>
#include <inttypes.h>
#include <libwebsockets.h>

static const char *CIPHER_LIST = "ALL:aNULL:!eNULL:@STRENGTH"; /* Default */
static const char *IGNORED = "ignore-this-log-message";

/* Log for LWS messages. For dispatch server messages use qd_http_server_t::log */

static qd_log_level_t qd_level(int lll) {
    switch (lll) {
    case LLL_ERR: return QD_LOG_ERROR;
    case LLL_WARN: return QD_LOG_WARNING;
    /* LWS is noisy compared to dispatch on the informative levels, downgrade */
    case LLL_NOTICE: return QD_LOG_DEBUG;
    default: return QD_LOG_DEBUG; /* Everything else to debug  */
    }
}

static void logger(int lll, const char *line)  {
    if (strstr(line, IGNORED)) return;
    size_t  len = strlen(line);
    while (len > 1 && isspace(line[len-1])) { /* Strip trailing newline */
        --len;
    }
    qd_log(LOG_HTTP, qd_level(lll), "%.*s", (int) len, line);
}

static void log_init(void)
{
    int levels = 0;
    for (int i = 0; i < LLL_COUNT; ++i) {
        int lll = 1<<i;
        levels |= qd_log_enabled(LOG_HTTP, qd_level(lll)) ? lll : 0;
    }
    lws_set_log_level(levels, logger);
}

/* Intermediate write buffer: LWS needs extra header space on write.  */
typedef struct buffer_t {
    char *start;
    size_t size, cap;
} buffer_t;

/* Ensure size bytes in buffer, make buf empty if alloc fails */
static void buffer_set_size(buffer_t *buf, size_t size) {
    if (size > buf->cap) {
        buf->cap = (size > buf->cap * 2) ? size : buf->cap * 2;
        buf->start = realloc(buf->start, buf->cap);
    }
    if (buf->start) {
        buf->size = size;
    } else {
        buf->size = buf->cap = 0;
    }
}

/* AMQPWS connection: set as lws user data and qd_conn->context */
typedef struct connection_t {
    pn_connection_driver_t driver;
    qd_connection_t* qd_conn;
    buffer_t wbuf;   /* LWS requires allocated header space at start of buffer */
    struct lws *wsi;
} connection_t;

// Instantiated for every HTTP request, this holds the statistics to be written in the response
//
typedef struct stats_request_state_t {
    bool callback_completed;  // T: the core has written the global statistics to the stats field
    bool wsi_deleted;         // T: client has closed, may release this state instance
    qdr_global_stats_t stats;
    qd_http_server_t *server;
    struct lws *wsi;
    size_t buffer_size;       // extra octets past lws_prefix[LWS_PRE] for HTTP output
    uint8_t lws_prefix[LWS_PRE];
    // buffer_size extra octets are appended to this structure when it is allocated. This space is used for the HTTP
    // response. See new_stats_request_state(), Use &lws_prefix[LWS_PRE] as the start of output buffer.
} stats_request_state_t;
static stats_request_state_t *new_stats_request_state(size_t buffer_size);
static void free_stats_request_state(stats_request_state_t *);

// Context passed to metrics and healthz protocol callbacks. Instantiated by the LWS thread, represents a single HTTP
// request transaction.
//
typedef struct stats_t {
    stats_request_state_t *state;
    bool response_complete;  // T: HTTP response sent
} stats_t;

/* Navigating from WSI pointer to qd objects */
static qd_http_server_t *wsi_server(struct lws *wsi);
static qd_lws_listener_t *wsi_listener(struct lws *wsi);

/* Declare LWS callbacks and protocol list */
inline static void finalize_http(struct lws_vhost *vh, void *arg);

static int callback_http(struct lws *wsi, enum lws_callback_reasons reason,
                         void *user, void *in, size_t len);
static int callback_amqpws(struct lws *wsi, enum lws_callback_reasons reason,
                           void *user, void *in, size_t len);
static int callback_metrics(struct lws *wsi, enum lws_callback_reasons reason,
                               void *user, void *in, size_t len);
static int callback_healthz(struct lws *wsi, enum lws_callback_reasons reason,
                               void *user, void *in, size_t len);

static struct lws_protocols protocols[] = {
    /* HTTP only protocol comes first */
    {
        "http-only",
        callback_http,
        0,
    },
    /* "amqp" is the official oasis AMQP over WebSocket protocol name */
    {
        "amqp",
        callback_amqpws,
        sizeof(connection_t),
    },
    /* "binary" is an alias for "amqp", for compatibility with clients designed
     * to work with a WebSocket proxy
     */
    {
        "binary",
        callback_amqpws,
        sizeof(connection_t),
    },
    {
        "http",
        callback_metrics,
        sizeof(stats_t),
    },
    {
        "healthz",
        callback_healthz,
        sizeof(stats_t),
    },
    { NULL, NULL, 0, 0 } /* terminator */
};


static inline int unexpected_close(struct lws *wsi, const char *msg) {
    lws_close_reason(wsi, LWS_CLOSE_STATUS_UNEXPECTED_CONDITION,
                     (unsigned char*)msg, strlen(msg));
    char peer[64];
    lws_get_peer_simple(wsi, peer, sizeof(peer));
    qd_log(LOG_HTTP, QD_LOG_ERROR, "Error on HTTP connection from %s: %s", peer, msg);
    return -1;
}

static int handle_events(connection_t* c) {
    if (!c->qd_conn) {
        return unexpected_close(c->wsi, "not-established");
    }
    pn_event_t *e;
    while ((e = pn_connection_driver_next_event(&c->driver)) != 0) {
        if (c->qd_conn) {
            if (!qd_amqpws_handle_connection_event(c->qd_conn, e)) {
                c->qd_conn = 0;  // connection closed
            }
        }
    }
    if (c->qd_conn)
        qd_amqpws_end_of_batch(c->qd_conn);

    if (pn_connection_driver_write_buffer(&c->driver).size) {
        lws_callback_on_writable(c->wsi);
    }
    if (pn_connection_driver_write_closed(&c->driver)) {
        lws_close_reason(c->wsi, LWS_CLOSE_STATUS_NORMAL, NULL, 0);
        return -1;
    }
    return 0;
}

/* The server has a bounded, thread-safe queue for external work */
typedef struct work_t {
    enum { W_NONE, W_LISTEN, W_CLOSE, W_WAKE, W_STOP, W_HANDLE_STATS } type;
    void *value;
} work_t;

#define WORK_MAX 8              /* Just decouple threads, not a big buffer */

typedef struct work_queue_t {
    sys_mutex_t lock;
    sys_cond_t  cond;
    work_t work[WORK_MAX];
    size_t head, len;          /* Ring buffer */
} work_queue_t;

/* HTTP Server runs in a single thread, communication from other threads via work_queue */
struct qd_http_server_t {
    qd_server_t *server;
    qdr_core_t *core;
    sys_thread_t *thread;
    work_queue_t work;
    qd_log_source_t *log;
    struct lws_context *context;
    qd_timestamp_t now;         /* Cache current time in thread_run */
    qd_timestamp_t next_tick;   /* Next requested tick service */
};

static void work_queue_destroy(work_queue_t *wq) {
    sys_mutex_free(&wq->lock);
    sys_cond_free(&wq->cond);
}

static void work_queue_init(work_queue_t *wq) {
    sys_mutex_init(&wq->lock);
    sys_cond_init(&wq->cond);
}

 /* Block till there is space */
static void work_push(qd_http_server_t *hs, work_t w) {
    work_queue_t *wq = &hs->work;
    sys_mutex_lock(&wq->lock);
    while (wq->len == WORK_MAX) {
        lws_cancel_service(hs->context); /* Wake up the run thread to clear space */
        sys_cond_wait(&wq->cond, &wq->lock);
    }
    wq->work[(wq->head + wq->len) % WORK_MAX] = w;
    ++wq->len;
    sys_mutex_unlock(&wq->lock);
    lws_cancel_service(hs->context); /* Wake up the run thread to handle my work */
}

/* Non-blocking, return { W_NONE, NULL } if empty */
static work_t work_pop(qd_http_server_t *hs) {
    work_t w = { W_NONE, NULL };
    work_queue_t *wq = &hs->work;
    sys_mutex_lock(&wq->lock);
    if (wq->len > 0) {
        w = wq->work[wq->head];
        wq->head = (wq->head + 1) % WORK_MAX;
        --wq->len;
        sys_cond_signal(&wq->cond);
    }
    sys_mutex_unlock(&wq->lock);
    return w;
}

/* Each qd_lws_listener_t is associated with an lws_vhost */
struct qd_lws_listener_t {
    qd_listener_t *listener;
    qd_http_server_t *server;
    struct lws_vhost *vhost;
    struct lws_http_mount mount;
    struct lws_http_mount metrics;
    struct lws_http_mount healthz;
};

void qd_lws_listener_free(qd_lws_listener_t *hl) {
    if (!hl) return;
    if (hl->listener) {
        hl->listener->http = NULL;
        qd_listener_decref(hl->listener);
    }
    free(hl);
}

static qd_lws_listener_t *qd_lws_listener(qd_http_server_t *hs, qd_listener_t *li) {
    qd_lws_listener_t *hl = calloc(1, sizeof(*hl));
    if (hl) {
        hl->server = hs;
        hl->listener = li;
        li->http = hl;
        sys_atomic_inc(&li->ref_count); /* Keep it around till qd_http_server_free() */
    } else {
        qd_log(LOG_HTTP, QD_LOG_CRITICAL, "No memory for HTTP listen on %s", li->config.host_port);
    }
    return hl;
}

/* Linked list: first entry on each line should point to next, last line should be the 
 * octet-stream default.
 */
static const struct lws_protocol_vhost_options mime_types[] = {
    { &mime_types[1], NULL, ".json", "application/json" },
    { &mime_types[2], NULL, ".woff2", "font/woff2" },
    { NULL, NULL, "*", "application/octet-stream" }
};

static int is_ipv6_address(qd_http_server_t *hs, const char* host, const char* port)
{
    int result = 0;
    struct addrinfo *addr;
    struct addrinfo hints = {0, AF_UNSPEC, SOCK_STREAM};
    int code = getaddrinfo(host, port, &hints, &addr);
    if (code) {
        qd_log(LOG_HTTP, QD_LOG_ERROR, "getaddrinfo(%s, %s) failed with %s", host, port, gai_strerror(code));
    } else {
        result = addr->ai_family == AF_INET6;
        freeaddrinfo(addr);
    }
    return result;
}

static void listener_start(qd_lws_listener_t *hl, qd_http_server_t *hs) {
    log_init();                 /* Update log flags at each listener */

    qd_server_config_t *config = &hl->listener->config;

    int port = qd_port_int(config->port);
    if (port < 0) {
        qd_log(LOG_HTTP, QD_LOG_ERROR, "HTTP listener %s has invalid port %s", config->host_port,
               config->port);
        goto error;
    }
    struct lws_http_mount *m = &hl->mount;
    m->mountpoint = "/";    /* URL mount point */
    m->mountpoint_len = strlen(m->mountpoint); /* length of the mountpoint */
    m->origin = (config->http_root_dir && *config->http_root_dir) ? /* File system root */
        config->http_root_dir : QPID_DISPATCH_HTTP_ROOT_DIR;
    m->def = "index.html";  /* Default file name */
    m->origin_protocol = LWSMPRO_FILE; /* mount type is a directory in a filesystem */
    m->extra_mimetypes = mime_types;
    struct lws_http_mount *tail = m;
    if (config->metrics) {
        struct lws_http_mount *metrics = &hl->metrics;
        tail->mount_next = metrics;
        tail = metrics;
        metrics->mountpoint = "/metrics";
        metrics->mountpoint_len = strlen(metrics->mountpoint);
        metrics->origin_protocol = LWSMPRO_CALLBACK;
        metrics->protocol = "http";
        metrics->origin = IGNORED;
    }
    if (config->healthz) {
        struct lws_http_mount *healthz = &hl->healthz;
        tail->mount_next = healthz;
        healthz->mountpoint = "/healthz";
        healthz->mountpoint_len = strlen(healthz->mountpoint);
        healthz->origin_protocol = LWSMPRO_CALLBACK;
        healthz->protocol = "healthz";
        healthz->origin = IGNORED;
    }

    struct lws_context_creation_info info = {0};
    info.mounts = m;
    info.port = port;
    info.protocols = protocols;
    info.keepalive_timeout = 1;
    info.ssl_cipher_list = CIPHER_LIST;
    info.options |= LWS_SERVER_OPTION_VALIDATE_UTF8;
    if (!is_ipv6_address(hs, strlen(config->host) == 0 ? 0 : config->host, config->port)) {
        qd_log(LOG_HTTP, QD_LOG_INFO, "Disabling ipv6 on %s", config->host_port);
        info.options |= LWS_SERVER_OPTION_DISABLE_IPV6;
    }
    if (config->ssl_profile) {
        info.ssl_cert_filepath = config->ssl_certificate_file;
        info.ssl_private_key_filepath = config->ssl_private_key_file;
        info.ssl_private_key_password = config->ssl_password;
        info.ssl_ca_filepath = config->ssl_trusted_certificate_db;
        info.ssl_cipher_list = config->ssl_ciphers;

        info.options |=
            LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT |
            (config->ssl_required ? 0 : LWS_SERVER_OPTION_ALLOW_NON_SSL_ON_SSL_PORT | LWS_SERVER_OPTION_ALLOW_HTTP_ON_HTTPS_LISTENER) |
            ((config->requireAuthentication && info.ssl_ca_filepath) ? LWS_SERVER_OPTION_REQUIRE_VALID_OPENSSL_CLIENT_CERT : 0);
    }
    info.vhost_name = hl->listener->config.host_port;
    info.finalize = finalize_http;
    info.finalize_arg = hl;
    hl->vhost = lws_create_vhost(hs->context, &info);
    if (!hl->vhost) {
        qd_log(LOG_HTTP, QD_LOG_INFO, "Error listening for HTTP on %s", config->host_port);
        goto error;
    }

    /* Store hl pointer in vhost */
    void *vp = lws_protocol_vh_priv_zalloc(hl->vhost, &protocols[0], sizeof(hl));
    memcpy(vp, &hl, sizeof(hl));

    if (port == 0) {
        // If a 0 (zero) is specified for a port, get the actual listening port from the listener.
        const int resolved_port = lws_get_vhost_port(hl->vhost);
        assert(resolved_port != -1); // already checked the vhost is successfully started
        if (config->name)
            qd_log(LOG_HTTP, QD_LOG_INFO, "Listening for HTTP on %s:%d (%s)", config->host, resolved_port,
                   config->name);
        else
            qd_log(LOG_HTTP, QD_LOG_INFO, "Listening for HTTP on %s:%d", config->host, resolved_port);
    } else {
        qd_log(LOG_HTTP, QD_LOG_INFO, "Listening for HTTP on %s", config->host_port);
    }
    return;

  error:
    if (hl->listener->exit_on_error) {
        qd_log(LOG_HTTP, QD_LOG_CRITICAL, "Shutting down, required listener failed %s", config->host_port);
        exit(1);
    }
    qd_lws_listener_free(hl);
}

static void listener_close(qd_lws_listener_t *hl, qd_http_server_t *hs) {
    qd_server_config_t *config = &hl->listener->config;
    qd_log(LOG_HTTP, QD_LOG_INFO, "Stopped listening for HTTP on %s", config->host_port);
    lws_vhost_destroy(hl->vhost);
}

/*
 * LWS callback for un-promoted HTTP connections.
 * Note main HTTP file serving is handled by the "mount" struct below.
 */
static int callback_http(struct lws *wsi, enum lws_callback_reasons reason,
                         void *user, void *in, size_t len)
{
    /* Do default HTTP handling for all the cases we don't care about. */
    return lws_callback_http_dummy(wsi, reason, user, in, len);
}

inline static void finalize_http(struct lws_vhost *vh /*unused*/, void *arg) {
    qd_lws_listener_t *listener = (qd_lws_listener_t*) arg;
    qd_lws_listener_free(listener);
}

/* Wake up a connection managed by the http server thread */
static void connection_wake(qd_connection_t *qd_conn)
{
    connection_t *c = qd_conn->context;
    if (c && qd_conn->listener->http) {
        qd_http_server_t *hs = qd_conn->listener->http->server;
        work_t w = { W_WAKE, c };
        work_push(hs, w);
    }
}

//
// Metrics
//
// Metrics are reported via an HTTP get request on the url "http://<router>/metrics". The metrics in the HTTP response
// are formatted for consumption by Prometheus - see the description "exposition formats" at the Prometheus website for
// details.
//
// Each metric is rendered as ASCII text. Two lines of text are generated for each metric. The format of these lines
// are (minus quotes):
//
// "# TYPE <metric-name> <metric-type>\n"
// "<metric-name> <value>\n"
//
// Currently all metric values are uint64_t integers, and the metric-type is either "counter" or "gauge". Counters are
// those metrics that only increase (may allow reset to zero). Gauges are those metrics whose values may increase or
// decrease over time.
//
// The current http-response body buffering implementation is... interesting. When a request arrives all counters are
// fetched then rendered into an output buffer. After all metrics have been written to the buffer the buffer is written
// to the LWS internal network buffer(s). Metrics can either be fetch synchronously (alloc-pool metrics) or require an
// asynchronous callback (router core metrics). The output buffer rendering and writing all occur on the http thread -
// see callback_metrics().
//
// Given this implementation it is necessary to ensure that the output buffer is large enough to hold all metrics. The
// following definitions are used to compute the necessary buffer size. These values may need updating occasionally
// should metric be added/removed. I've added many debug asserts to prevent accidental buffer overflow should the
// metrics not be updated properly.
//
// TODO(kgiusti): refactor this to use a smaller buffer with a chunked-output approach if possible with LWS.
#define MAX_METRIC_NAME_LEN  48
#define MAX_METRIC_VALUE_LEN 20  // uint64_t in decimal
#define MAX_METRIC_TYPE_LEN  7   // strlen("counter")
#define PER_METRIC_BUF_SIZE ((2 * MAX_METRIC_NAME_LEN) + MAX_METRIC_VALUE_LEN + MAX_METRIC_TYPE_LEN + 11)
#define PER_ALLOC_METRIC_COUNT 4  // 4 metrics per alloc type

#define HTTP_HEADER_LEN 128  // reserve space for headers added by LWS (128 is a guess, asserted in callback).
#define HEALTHZ_BUF_SIZE 2048 // for /healthz url response data


/**
 * Called on router worker thread: passes latest router stats to the http thread for processing
 */
static void handle_stats_results(void *context, bool discard)
{
    stats_request_state_t* state = (stats_request_state_t*) context;
    if (state->wsi_deleted || discard) {
        free_stats_request_state(state);
    } else {
        qd_http_server_t *hs = state->server;
        if (hs) {
            work_t w = { W_HANDLE_STATS, state };
            work_push(hs, w);
        }
    }
}

/**
 * Called on http thread: process the stats arriving from the router thread
 */
static void handle_stats_result_HT(stats_request_state_t* state)
{
    if (state->wsi_deleted) {
        free_stats_request_state(state);
    } else {
        state->callback_completed = true;
        lws_callback_on_writable(state->wsi);
    }
}

typedef uint64_t (*uint64_metric) (const qdr_global_stats_t *stats);
typedef struct metric_definition {
    const char* name;
    const char* type;
    uint64_metric get_value;
} metric_definition;

static uint64_t stats_get_connections(const qdr_global_stats_t *stats) { return stats->connections; }
static uint64_t stats_get_links(const qdr_global_stats_t *stats) { return stats->links; }
static uint64_t stats_get_addrs(const qdr_global_stats_t *stats) { return stats->addrs; }
static uint64_t stats_get_routers(const qdr_global_stats_t *stats) { return stats->routers; }
static uint64_t stats_get_presettled_deliveries(const qdr_global_stats_t *stats) { return stats->presettled_deliveries; }
static uint64_t stats_get_dropped_presettled_deliveries(const qdr_global_stats_t *stats) { return stats->dropped_presettled_deliveries; }
static uint64_t stats_get_accepted_deliveries(const qdr_global_stats_t *stats) { return stats->accepted_deliveries; }
static uint64_t stats_get_released_deliveries(const qdr_global_stats_t *stats) { return stats->released_deliveries; }
static uint64_t stats_get_rejected_deliveries(const qdr_global_stats_t *stats) { return stats->rejected_deliveries; }
static uint64_t stats_get_modified_deliveries(const qdr_global_stats_t *stats) { return stats->modified_deliveries; }
static uint64_t stats_get_deliveries_ingress(const qdr_global_stats_t *stats) { return stats->deliveries_ingress; }
static uint64_t stats_get_deliveries_egress(const qdr_global_stats_t *stats) { return stats->deliveries_egress; }
static uint64_t stats_get_deliveries_transit(const qdr_global_stats_t *stats) { return stats->deliveries_transit; }
static uint64_t stats_get_deliveries_delayed_1sec(const qdr_global_stats_t *stats) { return stats->deliveries_delayed_1sec; }
static uint64_t stats_get_deliveries_delayed_10sec(const qdr_global_stats_t *stats) { return stats->deliveries_delayed_10sec; }
static uint64_t stats_get_deliveries_stuck(const qdr_global_stats_t *stats) { return stats->deliveries_stuck; }
static uint64_t stats_get_links_blocked(const qdr_global_stats_t *stats) { return stats->links_blocked; }

static const struct metric_definition metrics[] = {
    {"qdr_connections_total", "gauge", stats_get_connections},
    {"qdr_links_total", "gauge", stats_get_links},
    {"qdr_addresses_total", "gauge", stats_get_addrs},
    {"qdr_routers_total", "gauge", stats_get_routers},
    {"qdr_presettled_deliveries_total", "counter", stats_get_presettled_deliveries},
    {"qdr_dropped_presettled_deliveries_total", "counter", stats_get_dropped_presettled_deliveries},
    {"qdr_accepted_deliveries_total", "counter", stats_get_accepted_deliveries},
    {"qdr_released_deliveries_total", "counter", stats_get_released_deliveries},
    {"qdr_rejected_deliveries_total", "counter", stats_get_rejected_deliveries},
    {"qdr_modified_deliveries_total", "counter", stats_get_modified_deliveries},
    {"qdr_deliveries_ingress_total", "counter", stats_get_deliveries_ingress},
    {"qdr_deliveries_egress_total", "counter", stats_get_deliveries_egress},
    {"qdr_deliveries_transit_total", "counter", stats_get_deliveries_transit},
    {"qdr_deliveries_delayed_1sec_total", "counter", stats_get_deliveries_delayed_1sec},
    {"qdr_deliveries_delayed_10sec_total", "counter", stats_get_deliveries_delayed_10sec},
    {"qdr_deliveries_stuck_total", "gauge", stats_get_deliveries_stuck},
    {"qdr_links_blocked_total", "gauge", stats_get_links_blocked},
};
static const size_t metrics_length = sizeof(metrics)/sizeof(metrics[0]);

//
// Metrics provided by the alloc_pool memory object cache.
//
// The alloc_pool module will register a name and descriptor for each memory object maintained by the pool during
// initialization. This information can be used to gather the metrics associated with the given object. The alloc_pool
// will deregister these on shutdown. See qd_http_add/remove_alloc_metric().
//
typedef struct allocator_metric_definition_t allocator_metric_definition_t;
struct allocator_metric_definition_t {
    DEQ_LINKS(allocator_metric_definition_t);
    const char *name;
    const qd_alloc_type_desc_t *desc;
};
DEQ_DECLARE(allocator_metric_definition_t, allocator_metric_definition_list_t);
static allocator_metric_definition_list_t allocator_metrics = DEQ_EMPTY;

// Write a single metric to the output buffer. Advance (*start) past the written data (to the null terminator) and
// return the total octets written (not including null terminator). Return zero on error (abort() if debug build).
//
static size_t _write_metric(uint8_t **start, size_t available, const char *name, const char *type, uint64_t value)
{
    // if you modify this please update any buffer sizing info above

    int rc1 = snprintf((char *) *start, available, "# TYPE %s %s\n", name, type);
    if (rc1 < 0 || rc1 >= available) { // overrun!
        assert(false);  // you need to increase the output_buffer size!
        return 0;
    }
    *start += rc1;
    available -= rc1;

    int rc2 = snprintf((char *) *start, available, "%s %" PRIu64 "\n", name, value);
    if (rc2 < 0 || rc2 >= available) { // overrun!
        assert(false);  // you need to increase the output_buffer size!
        return 0;
    }
    *start += rc2;

    return rc1 + rc2;
}

// Write all the per-protocol connection counters. Return the total octets written (not including null terminator) or
// zero on error.
//
// On successful return (*start) will be advanced to the terminating null byte.
//
static size_t _write_conn_counter_metrics(uint8_t **start, size_t available)
{
    char name_buffer[MAX_METRIC_NAME_LEN + 1];
    const size_t save = available;

    for (int proto = 0; proto < QD_PROTOCOL_TOTAL; ++proto) {
        const char *proto_name = qd_protocol_name(proto);
        assert(proto_name);
        int ct = snprintf(name_buffer, sizeof(name_buffer), "qdr_%s_service_connections", proto_name);
        if (ct < 0 || ct >= sizeof(name_buffer)) {  // overrun!
            assert(false);  // you need to increase the output_buffer size!
            return 0;
        }

        // Prometheus restricts metric names to the following character set: [a-zA-Z_:]. Protocol names may include
        // other characters, like 'http/1'. Convert any illegal characters to '_'

        for (char *ptr = name_buffer; *ptr; ++ptr) {
            if (!isalnum(*ptr) && *ptr != '_' && *ptr != ':')
                *ptr = '_';
        }

        size_t rc = _write_metric(start, available, name_buffer, "gauge", qd_connection_count(proto));
        if (rc == 0) {
            return 0;  // error writing, close the connection
        }
        available -= rc;
    }

    return save - available;
}

// Write all the router global metrics to the output buffer. Return the total octets written (not including null
// terminator) or zero on error.
//
// On successful return (*start) will be advanced to the terminating null byte.
//
static size_t _write_global_metrics(const stats_request_state_t *state, uint8_t **start, size_t available)
{
    assert(state && state->callback_completed);

    const size_t save = available;

    for (int index = 0; index < metrics_length; ++index) {
        const metric_definition *metric = &metrics[index];
        size_t rc = _write_metric(start, available, metric->name, metric->type, metric->get_value(&state->stats));
        if (rc == 0) {
            return 0;  // error writing, close the connection
        }
        available -= rc;
    }

    return save - available;
}


// Write a single allocator metric to the output buffer. Generate the metric name using the name and subname. Return the
// total octets written (not including null terminator) or zero on error.
//
// On successful return (*start) will be advanced to the terminating null byte.
//
static size_t _write_allocator_metric(uint8_t **start, size_t available, const char *name, const char *subname, uint64_t value)
{
    char name_buffer[MAX_METRIC_NAME_LEN + 1];
    int rc = snprintf(name_buffer, sizeof(name_buffer), "%s:%s", name, subname);
    if (rc < 0 || rc >= sizeof(name_buffer)) {  // overrun!
        assert(false);  // you need to increase the output_buffer size!
        return 0;
    }

    return _write_metric(start, available, name_buffer, "gauge", value);
}

// Write all the allocator metrics to the output buffer. Return the total octets written (not including null terminator)
// or zero on error.
//
// On successful return (*start) will be advanced to the terminating null byte.
//
static size_t _write_allocator_metrics(uint8_t **start, size_t available)
{
    const size_t save = available;
    uint64_t pool_total_bytes = 0;  // total memory allocated across all types

    allocator_metric_definition_t *metric = DEQ_HEAD(allocator_metrics);
    assert(metric);  // unexpected if null no metrics?

    while (metric) {
        qd_alloc_stats_t stats = qd_alloc_desc_stats(metric->desc);
        uint64_t total_allocated = stats.total_alloc_from_heap - stats.total_free_to_heap;
        uint64_t total_in_use = stats.held_by_threads;
        uint64_t total_in_cache = total_allocated - total_in_use;
        uint64_t total_bytes = total_allocated * qd_alloc_type_size(metric->desc);

        pool_total_bytes += total_bytes;

        size_t rc = _write_allocator_metric(start, available, metric->name, "allocated", total_allocated);
        if (rc == 0) return 0;
        available -= rc;

        rc = _write_allocator_metric(start, available, metric->name, "in_use", total_in_use);
        if (rc == 0) return 0;
        available -= rc;

        rc = _write_allocator_metric(start, available, metric->name, "cached", total_in_cache);
        if (rc == 0) return 0;
        available -= rc;

        rc = _write_allocator_metric(start, available, metric->name, "bytes", total_bytes);
        if (rc == 0) return 0;
        available -= rc;

        metric = DEQ_NEXT(metric);
    }

    size_t rc = _write_metric(start, available, "qdr_alloc_pool_bytes", "gauge", pool_total_bytes);
    if (rc == 0) return 0;
    available -= rc;

    return save - available;
}

// Write the router process memory use metrics to the output buffer. Return the total octets written (not including null
// terminator) or zero on error.
//
// On successful return (*start) will be advanced to the terminating null byte.
//
static size_t _write_memory_metrics(uint8_t **start, size_t available)
{
    const size_t save = available;
    uint64_t   vmsize = qd_router_virtual_memory_usage();
    uint64_t      rss = qd_router_rss_memory_usage();
    size_t         rc = 0;

    if (vmsize > 0) {  // 0 means not available
        rc = _write_metric(start, available, "qdr_router_vmsize_bytes", "gauge", vmsize);
        if (rc == 0) {
            return 0;
        }
        available -= rc;
    }

    if (rss > 0) {
        rc = _write_metric(start, available, "qdr_router_rss_bytes", "gauge", rss);
        if (rc == 0) {
            return 0;
        }
        available -= rc;
    }

    return save - available;
}

// Gather the current metrics and write them to the output buffer. Return the total bytes written to the buffer (not
// including null terminator) or zero on error.
//
// On successful return *start is advanced to the terminating null byte
//
static size_t _generate_metrics_response(stats_request_state_t *state, uint8_t **start, const uint8_t * const end)
{
    if (_write_global_metrics(state, start, end - *start) == 0
        || _write_allocator_metrics(start, end - *start) == 0
        || _write_memory_metrics(start, end - *start) == 0
        || _write_conn_counter_metrics(start, end - *start) == 0) {
        // error, close the connection
        return 0;
    }

    return end - *start;
}

static int add_header_by_name(struct lws *wsi, const char* name, const char* value, uint8_t** position, uint8_t* end)
{
    return lws_add_http_header_by_name(wsi, (unsigned char*) name, (unsigned char*) value, strlen(value), position, end);
}

static int callback_metrics(struct lws *wsi, enum lws_callback_reasons reason,
                            void *user, void *in, size_t len)
{
    qd_http_server_t *hs = wsi_server(wsi);
    stats_t *stats = (stats_t*) user;

    if (!stats)   // ignore any non-http request events
        return 0;

    switch (reason) {

    case LWS_CALLBACK_HTTP: {
        // New HTTP request received, setup per-request state with output buffer
        assert(!stats->state);
        // see the comments above regarding output buffer size for metrics:
        size_t buf_size = HTTP_HEADER_LEN
            // router global metrics:
            + (metrics_length * PER_METRIC_BUF_SIZE)
            // alloc_pool metrics (+ 1 for qdr_alloc_pool_bytes):
            + (DEQ_SIZE(allocator_metrics) * PER_METRIC_BUF_SIZE * PER_ALLOC_METRIC_COUNT)
            + PER_METRIC_BUF_SIZE
            // qdr_router_vmsize_bytes and qdr_router_rss_bytes:
            + (2 * PER_METRIC_BUF_SIZE)
            // connection counters by protocol:
            + (QD_PROTOCOL_TOTAL * PER_METRIC_BUF_SIZE)
            // 1 terminating null
            + 1;
        stats->state = new_stats_request_state(buf_size);
        stats->state->wsi = wsi;
        stats->state->server = hs;
        //request stats from core thread
        qdr_request_global_stats(hs->core, &stats->state->stats, handle_stats_results, (void*) stats->state);
        return 0;
    }

    case LWS_CALLBACK_HTTP_WRITEABLE: {
        // LWS HTTP server ready to send to HTTP response data
        assert(stats->state);  // expect LWS_CALLBACK_HTTP event occurs first!

        if (stats->response_complete) {  // ignore spurious WRITABLE events once response complete
            return 0;
        }

        if (!stats->state->callback_completed) {
            // the asynchronous request for global metrics has not yet completed. When it does another
            // LWS_CALLBACK_HTTP_WRITABLE event will be generated and then we can send the response.
            return 0;
        }

        uint8_t *start = &stats->state->lws_prefix[LWS_PRE];
        uint8_t *end = start + stats->state->buffer_size;  // first byte past buffer

        // encode stats into buffer

        if (lws_add_http_header_status(wsi, HTTP_STATUS_OK, &start, end)
            || add_header_by_name(wsi, "content-type:", "text/plain", &start, end)
            || add_header_by_name(wsi, "connection:", "close", &start, end)
            || lws_finalize_http_header(wsi, &start, end)) {

            qd_log(LOG_HTTP, QD_LOG_WARNING, "Metrics request failed: cannot send headers");
            return 1;
        }

        // if this fails make HTTP_HEADER_LEN larger (LWS does not document the required size)
        assert(HTTP_HEADER_LEN >= (start - &stats->state->lws_prefix[LWS_PRE]));

        if (_generate_metrics_response(stats->state, &start, end) == 0) {
            // Failed to generate output. This is not expected. Terminate the connection
            qd_log(LOG_HTTP, QD_LOG_WARNING, "Metrics request failed: cannot access metrics");
            return 1;
        }

        // Write the entire output buffer to LWS in one call. Best I can tell from the docs this should not fail
        // unless the connection has closed.

        size_t available = (size_t) (start - &stats->state->lws_prefix[LWS_PRE]);
        size_t amount = lws_write(wsi, (unsigned char *) &stats->state->lws_prefix[LWS_PRE],
                                  available, LWS_WRITE_HTTP_FINAL);

        if (amount < available) {
            // according to the lws_write header, this is an error. It may return more than available, which is ok
            qd_log(LOG_HTTP, QD_LOG_WARNING, "Metrics request failed: connection closed while writing");
            return 1;
        }

        stats->response_complete = true;

        if (lws_http_transaction_completed(wsi)) {
            // I do not think this is an error, but according to the examples we close the connection when this happens
            return 1;
        }
        return 0;
    }

    case LWS_CALLBACK_HTTP_DROP_PROTOCOL:
    case LWS_CALLBACK_CLOSED_HTTP: {
        // request complete (added DROP_PROTOCOL since we do not get CLOSED_HTTP from curl clients (?))
        if (stats->state) {
            stats->state->wsi_deleted = true;
            // if the callback is still running then we cannot free the state since the callback will access it. We rely
            // on the callback to free the state in this case. See handle_stats_result_HT().
            if (stats->state->callback_completed) {
                free_stats_request_state(stats->state);
                stats->state = 0;
            }
        }
        return 0;
    }

    default:
        return 0;
    }
}

static int callback_healthz(struct lws *wsi, enum lws_callback_reasons reason,
                               void *user, void *in, size_t len)
{
    qd_http_server_t *hs = wsi_server(wsi);
    stats_t *stats = (stats_t*) user;

    if (!stats)   // ignore any non-http request events
        return 0;

    switch (reason) {

    case LWS_CALLBACK_HTTP: {
        assert(!stats->state);
        stats->state = new_stats_request_state(HEALTHZ_BUF_SIZE);
        stats->state->wsi = wsi;
        stats->state->server = hs;
        //make dummy request for stats (pass in null ptr); this still exercises the
        //path through core thread and back through callback on io thread which is
        //a reasonable initial liveness check
        qdr_request_global_stats(hs->core, 0, handle_stats_results, (void*) stats->state);
        return 0;
    }

    case LWS_CALLBACK_HTTP_WRITEABLE: {
        assert(stats->state);  // expect LWS_CALLBACK_HTTP event occurs first!

        if (stats->response_complete) {  // ignore spurious WRITABLE events once response complete
            return 0;
        }

        if (!stats->state->callback_completed) {
            // the asynchronous request for global metrics has not yet completed. When it does another
            // LWS_CALLBACK_HTTP_WRITABLE event will be generated and then we can send the response.
            return 0;
        }

        uint8_t *start = &stats->state->lws_prefix[LWS_PRE];
        uint8_t *end = start + HEALTHZ_BUF_SIZE;  // first byte past buffer

        // encode stats into buffer

        if (lws_add_http_header_status(wsi, HTTP_STATUS_OK, &start, end)
            || add_header_by_name(wsi, "content-type:", "text/plain", &start, end)
            || lws_add_http_header_content_length(wsi, 3, &start, end)
            || lws_finalize_http_header(wsi, &start, end)) {

            qd_log(LOG_HTTP, QD_LOG_WARNING, "Healthz request failed: cannot send headers");
            return 1;
        }

        // if this fails make HTTP_HEADER_LEN larger (LWS does not document the required size)
        assert(HTTP_HEADER_LEN >= (start - &stats->state->lws_prefix[LWS_PRE]));

        start += lws_snprintf((char*) start, end - start, "OK\n");

        size_t available = (size_t) (start - &stats->state->lws_prefix[LWS_PRE]);
        size_t amount = lws_write(wsi, (unsigned char *) &stats->state->lws_prefix[LWS_PRE],
                                  available, LWS_WRITE_HTTP_FINAL);
        if (amount < available) {
            // according to the lws_write header, this is an error. It may return more than available, which is ok
            qd_log(LOG_HTTP, QD_LOG_WARNING, "Healthz request failed: connection closed while writing");
            return 1;
        }

        stats->response_complete = true;

        if (lws_http_transaction_completed(wsi)) {
            // I do not think this is an error, but according to the examples we close the connection when this happens
            return 1;
        }

        return 0;
    }

    case LWS_CALLBACK_HTTP_DROP_PROTOCOL:  // won't get CLOSED_HTTP from curl (?)
    case LWS_CALLBACK_CLOSED_HTTP: {
        if (stats->state) {
            stats->state->wsi_deleted = true;
            if (stats->state->callback_completed) {
                free_stats_request_state(stats->state);
                stats->state = 0;
            }
        }
        return 0;
    }

    default:
        return 0;
    }
}

/* Callbacks for promoted AMQP over WS connections. */
static int callback_amqpws(struct lws *wsi, enum lws_callback_reasons reason,
                           void *user, void *in, size_t len)
{
    qd_http_server_t *hs = wsi_server(wsi);
    connection_t *c = (connection_t*)user;

    switch (reason) {

    case LWS_CALLBACK_ESTABLISHED: {
        /* Upgrade accepted HTTP connection to AMQPWS */
        memset(c, 0, sizeof(*c));
        c->wsi = wsi;
        qd_lws_listener_t *hl = wsi_listener(wsi);
        if (hl == NULL || !hl->listener->config.websockets) {
            return unexpected_close(c->wsi, "cannot-upgrade");
        }

        c->qd_conn = new_qd_connection_t();
        ZERO(c->qd_conn);
        qd_connection_init(c->qd_conn, hs->server, &hl->listener->config, 0, hl->listener);
        c->qd_conn->context = c;
        c->qd_conn->wake = connection_wake;

        lws_get_peer_simple(wsi, c->qd_conn->rhost, sizeof(c->qd_conn->rhost));
        int err = pn_connection_driver_init(&c->driver, c->qd_conn->pn_conn, NULL);
        if (err) {
            return unexpected_close(c->wsi, pn_code(err));
        }
        strncpy(c->qd_conn->rhost_port, c->qd_conn->rhost, sizeof(c->qd_conn->rhost_port));
        qd_log(LOG_HTTP, QD_LOG_DEBUG, "[%" PRIu64 "] upgraded HTTP connection from %s to AMQPWS",
               qd_connection_connection_id(c->qd_conn), qd_connection_name(c->qd_conn));
        return handle_events(c);
    }

    case LWS_CALLBACK_SERVER_WRITEABLE: {
        if (handle_events(c)) return -1;
        pn_bytes_t dbuf = pn_connection_driver_write_buffer(&c->driver);
        if (dbuf.size) {
            /* lws_write() demands LWS_PRE bytes of free space before the data,
             * so we must copy from the driver's buffer to larger temporary wbuf
             */
            buffer_set_size(&c->wbuf, LWS_PRE + dbuf.size);
            if (c->wbuf.start == NULL) {
                return unexpected_close(c->wsi, "out-of-memory");
            }
            unsigned char* buf = (unsigned char*)c->wbuf.start + LWS_PRE;
            memcpy(buf, dbuf.start, dbuf.size);
            ssize_t wrote = lws_write(wsi, buf, dbuf.size, LWS_WRITE_BINARY);
            if (wrote < 0) {
                pn_connection_driver_write_close(&c->driver);
                return unexpected_close(c->wsi, "write-error");
            } else {
                pn_connection_driver_write_done(&c->driver, wrote);
            }
        }
        return handle_events(c);
    }

    case LWS_CALLBACK_RECEIVE: {
        while (len > 0) {
            if (handle_events(c)) return -1;
            pn_rwbytes_t dbuf = pn_connection_driver_read_buffer(&c->driver);
            if (dbuf.size == 0) {
                return unexpected_close(c->wsi, "unexpected-data");
            }
            size_t copy = (len < dbuf.size) ? len : dbuf.size;
            memcpy(dbuf.start, in, copy);
            pn_connection_driver_read_done(&c->driver, copy);
            len -= copy;
            in = (char*)in + copy;
        }
        return handle_events(c);
    }

    case LWS_CALLBACK_USER: {
        pn_timestamp_t next_tick = pn_transport_tick(c->driver.transport, hs->now);
        if (next_tick && next_tick > hs->now && next_tick < hs->next_tick) {
            hs->next_tick = next_tick;
        }
        return handle_events(c);
    }

    case LWS_CALLBACK_WS_PEER_INITIATED_CLOSE: {
        pn_connection_driver_read_close(&c->driver);
        return handle_events(c);
    }

    case LWS_CALLBACK_CLOSED: {
        if (c->driver.transport) {
            pn_connection_driver_close(&c->driver);
            handle_events(c);
        }
        pn_connection_driver_destroy(&c->driver);
        free(c->wbuf.start);
        return -1;
    }

    default:
        return 0;
    }
}

#define DEFAULT_TICK 1000

#ifndef NDEBUG
static int threads_running;
#endif

static void* http_thread_run(void* v)
{
    qd_http_server_t *hs = v;
    qd_log(LOG_HTTP, QD_LOG_INFO, "HTTP server thread running");
    int result = 0;

#ifndef NDEBUG
    ++threads_running;
#endif

    while(result >= 0) {
        /* Send a USER event to run transport ticks, may decrease hs->next_tick. */
        hs->now = qd_timer_now();
        hs->next_tick = hs->now + DEFAULT_TICK;
        lws_callback_all_protocol(hs->context, &protocols[1], LWS_CALLBACK_USER);
        lws_callback_all_protocol(hs->context, &protocols[2], LWS_CALLBACK_USER);
        pn_millis_t timeout = (hs->next_tick > hs->now) ? hs->next_tick - hs->now : 1;
        result = lws_service(hs->context, timeout);

        /* Process any work items on the queue */
        for (work_t w = work_pop(hs); w.type != W_NONE; w = work_pop(hs)) {
            switch (w.type) {
            case W_NONE:
                break;
            case W_STOP:
                result = -1;
                break;
            case W_LISTEN:
                listener_start((qd_lws_listener_t*)w.value, hs);
                break;
            case W_CLOSE:
                listener_close((qd_lws_listener_t*)w.value, hs);
                break;
            case W_HANDLE_STATS:
                handle_stats_result_HT((stats_request_state_t*) w.value);
                break;
            case W_WAKE: {
                connection_t *c = w.value;
                pn_collector_put_object(c->driver.collector, c->driver.connection,
                                        PN_CONNECTION_WAKE);
                handle_events(c);
                break;
            }
            }
        }
    }

#ifndef NDEBUG
    --threads_running;
#endif

    qd_log(LOG_HTTP, QD_LOG_INFO, "HTTP server thread exit");
    return NULL;
}

void qd_http_server_stop(qd_http_server_t *hs) {
    if (!hs) return;
    if (hs->thread) {
        /* Thread safe, stop via work queue then clean up */
        work_t work = { W_STOP, NULL };
        work_push(hs, work);
        sys_thread_join(hs->thread);
        sys_thread_free(hs->thread);
        hs->thread = NULL;
    }
}

void qd_http_server_free(qd_http_server_t *hs) {
    if (!hs) return;
    qd_http_server_stop(hs);
    work_queue_destroy(&hs->work);
    if (hs->context) lws_context_destroy(hs->context);
    free(hs);
}

qd_http_server_t *qd_http_server(qd_server_t *s)
{
    log_init();
    qd_http_server_t *hs = calloc(1, sizeof(*hs));
    if (hs) {
        work_queue_init(&hs->work);
        struct lws_context_creation_info info = {0};
        info.gid = info.uid = -1;
        info.user = hs;
        info.server_string = QD_CONNECTION_PROPERTY_PRODUCT_VALUE;
        info.options = LWS_SERVER_OPTION_EXPLICIT_VHOSTS |
            LWS_SERVER_OPTION_SKIP_SERVER_CANONICAL_NAME |
            LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
        info.max_http_header_pool = 32;
        info.timeout_secs = 1;

        hs->context = lws_create_context(&info);
        hs->server  = s;
        hs->core = 0; // not yet available
        if (!hs->context) {
            qd_log(LOG_HTTP, QD_LOG_CRITICAL, "No memory starting HTTP server");
            qd_http_server_free(hs);
            hs = NULL;
        }
    }
    return hs;
}

/* Thread safe calls that put items on work queue */

qd_lws_listener_t *qd_http_server_listen(qd_http_server_t *hs, qd_listener_t *li)
{
    hs->core = qd_dispatch_router_core(qd_server_dispatch(hs->server));
    sys_mutex_lock(&hs->work.lock);
    if (!hs->thread) {
        hs->thread = sys_thread(SYS_THREAD_LWS_HTTP, http_thread_run, hs);
    }
    bool ok = hs->thread;
    sys_mutex_unlock(&hs->work.lock);
    if (!ok) return NULL;

    qd_lws_listener_t *hl = qd_lws_listener(hs, li);
    if (hl) {
        work_t w = { W_LISTEN, hl };
        work_push(hs, w);
    }

    return hl;
}

void qd_lws_listener_close(qd_lws_listener_t *hl)
{
    work_t w = { W_CLOSE, hl };
    work_push(hl->server, w);
}

static qd_http_server_t *wsi_server(struct lws *wsi) {
    return (qd_http_server_t*)lws_context_user(lws_get_context(wsi));
}

static qd_lws_listener_t *wsi_listener(struct lws *wsi) {
    qd_lws_listener_t *hl = NULL;
    struct lws_vhost *vhost = lws_get_vhost(wsi);
    if (vhost) {                /* Get qd_lws_listener from vhost data */
        void *vp = lws_protocol_vh_priv_get(vhost, &protocols[0]);
        memcpy(&hl, vp, sizeof(hl));
    }
    return hl;
}

void qd_http_add_alloc_metric(const char *name, const qd_alloc_type_desc_t *desc)
{
    allocator_metric_definition_t *md = qd_malloc(sizeof(allocator_metric_definition_t));
    ZERO(md);
    DEQ_ITEM_INIT(md);
    // name and desc remain valid until qd_http_remove_alloc_metric() is called
    md->name = name;
    md->desc = desc;
    DEQ_INSERT_TAIL(allocator_metrics, md);

#ifdef NDEBUG
    // Attempting to add a metric after the server threads have started will crash stuff. If you hit this assert then
    // qd_alloc_initialize() has not been called. qd_alloc_initialize() MUST be called before starting the http threads!
    assert(threads_running == 0);
#endif
}

void qd_http_remove_alloc_metric(const char *name)
{
    allocator_metric_definition_t *md = DEQ_HEAD(allocator_metrics);
    DEQ_FIND(md, strcmp(md->name, name) == 0);
    if (md) {
        DEQ_REMOVE(allocator_metrics, md);
        free(md);
    }

#ifndef NDEBUG
    // Attempting to remove a metric while the server threads are running will crash stuff. If you hit this assert then
    // qd_alloc_finalize() has been called prior to stopping all http threads. qd_alloc_finalize() MUST NOT be called
    // while http threads are running!
    assert(threads_running == 0);
#endif
}

// allocate a new stats_request_state_t instance, include buffer_size additional octets past the structure for rendering
// the HTML response
//
static stats_request_state_t *new_stats_request_state(size_t buffer_size)
{
    stats_request_state_t *state = qd_malloc(sizeof(stats_request_state_t) + buffer_size);
    ZERO(state);  // do not bother initializing buffer space - it will be overwritten
    state->buffer_size = buffer_size;
    return state;
}

static void free_stats_request_state(stats_request_state_t *state)
{
    free(state);
}

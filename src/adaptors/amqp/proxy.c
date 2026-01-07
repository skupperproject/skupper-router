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

#include "qd_connector.h"
#include "qd_connection.h"
#include "proxy.h"
#include "private.h"
#include "entity.h"

#include "qpid/dispatch/alloc_pool.h"
#include "qpid/dispatch/timer.h"
#include "qpid/dispatch/vanflow.h"
#include "qpid/dispatch/dispatch.h"

#include <proton/proactor.h>

#include <inttypes.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/err.h>


// proxyProfile configuration record
struct qd_proxy_profile_t {
    char *host;
    char *port;
    char *username;
    char *password;
};

// per connector reference to a proxyProfile
struct qd_proxy_context_t {
    DEQ_LINKS(qd_proxy_context_t);
    char                   *proxy_profile_name;
    qd_proxy_profile_t      profile;
    sys_atomic_t            ref_count;
};

#define ASSERT_MGMT_THREAD assert(sys_thread_role(0) == SYS_THREAD_MAIN || sys_thread_proactor_mode() == SYS_THREAD_PROACTOR_MODE_TIMER)


static int connect_to_proxy(const char *host, const char *port, char *errmsg, size_t errmsg_len)
{
    struct addrinfo *ai;

    int err = getaddrinfo(host, port, &(struct addrinfo){.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM}, &ai);
    if (err != 0) {
        strncpy(errmsg, gai_strerror(err), errmsg_len);
        return -1;
    }
    
    struct addrinfo *ai_ = ai;
    while (true) {
        int s = socket(ai_->ai_family, ai_->ai_socktype, ai_->ai_protocol);
        if (s != -1) {
            err = connect(s, ai_->ai_addr, ai_->ai_addrlen);
            if (err == 0) {
                freeaddrinfo(ai);
                return s;
            }
        }
        ai_ = ai_->ai_next;
        if (ai_) {
            close(s);
            continue;
        }
        snprintf(errmsg, errmsg_len, "connect error %s", strerror(errno));
        close(s);
        freeaddrinfo(ai);
        return -1;
    }
}

// Check inbound bytes for http1.1 validity.  Save bytes in line buffer or discard if line is NULL.
// Return length of line, excluding CRNL, or -1 if invalid.
static int read_response_line(int sockfd, char *line, size_t line_len, char *errmsg, size_t errmsg_len)
{
    unsigned char c;
    unsigned char prev = 0;
    int len = 0;
    while (1) {
        int rv = read(sockfd, &c, 1);
        if (rv == 0) {
            strncpy(errmsg, "proxy response EOF", errmsg_len);
            return -1;
        } else if (rv < 0) {
            snprintf(errmsg, errmsg_len, "proxy response read error %s", strerror(errno));
            return -1;
        }
        if (prev == '\r') {
            if (c == '\n')
                return len;  //  Valid line detected.
            strncpy(errmsg, "proxy malformed response CR", errmsg_len);
            return -1;
        }
        if (c == '\n') {
            strncpy(errmsg, "proxy malformed response NL without CR", errmsg_len);
            return -1;
        }
        if (c == '\r') {
            prev = c;
            continue;
        }
        if ((c < ' ') || (c >= 0x7F)) {
            strncpy(errmsg, "proxy response invalid character", errmsg_len);
            return -1;
        }
        if (len == line_len) {
            snprintf(errmsg, errmsg_len, "proxy response header > %lu chars", line_len);
            return -1;
        }
        if (line)
            line[len] = c;
        len++;
        prev = c;
    }
}

/* Send HTTP CONNECT request to proxy over sockfd.
 * sockfd is connected to the proxy and no bytes have yet been read or written.
 * errmsg to hold success/fail log message.
 *
 * Return true if a valid HTTP response is received and the proxy relay is in place.  All future
 * bytes sent and received on the socket are relayed unaltered by the proxy.
 */
static bool proxy_negotiate(int sockfd,
                            const char *host,
                            const char *port,
                            const char *user,
                            const char *pw,
                            char *errmsg,
                            size_t errmsg_len)
{
    // RFC 7231, authorization line (with base64 content) optional:
    //   CONNECT server.example.com:80 HTTP/1.1
    //   Host: server.example.com:80
    //   Proxy-Authorization: basic aGVsbG86d29ybGQ=
    //   [ empty line ]

    BIO *b64 = NULL;
    BIO *membio = BIO_new(BIO_s_mem());
    size_t host_len = host ? strlen(host) : 0;
    size_t port_len = port ? strlen(port) : 0;

    BIO_write(membio, "CONNECT ", 8);
    BIO_write(membio, host, host_len);
    BIO_write(membio, ":", 1);
    BIO_write(membio, port, port_len);

    BIO_write(membio, " HTTP/1.1\r\nHost: ", 17);
    BIO_write(membio, host, host_len);
    BIO_write(membio, ":", 1);
    BIO_write(membio, port, port_len);
    BIO_write(membio, "\r\n", 2);
    
    if (!!user) {
        BIO_write(membio, "Proxy-Authorization: basic ", 27);
        // use base64 BIO filter for user:pw
        b64 = BIO_new(BIO_f_base64());
        BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
        BIO_push(b64, membio);
        BIO_puts(b64, user);
        if (!!pw) {
            BIO_puts(b64, ":");
            BIO_puts(b64, pw);
        }
        BIO_flush(b64);
        // Turn off base64 filtering
        BIO_pop(b64);
        BIO_free(b64);
        // Finish auth line
        BIO_write(membio, "\r\n", 2);
    }

    // Last empty line completes CONNECT header
    BIO_write(membio, "\r\n", 2);

    // Access the header we just constructed
    char *header;
    long header_len = BIO_get_mem_data(membio, &header);
    if (header_len <= 0) {
        snprintf(errmsg, errmsg_len, "proxy setup error %lu", ERR_get_error());
        BIO_free(membio);
        return false;
    }
    ssize_t len = send(sockfd, header, header_len, 0);
    if (len != (ssize_t) header_len) {
        snprintf(errmsg, errmsg_len, "proxy handshake send error %zu bytes: %s", len, strerror(errno));
        BIO_free(membio);
        return false;
    }
    BIO_free(membio);
    
    // Parse returned response without consuming bytes beyond response end.  We only care about the
    // first status line (success/fail); we are allowed to ignore (and do) all other header lines.
    // There is no specified max line length.  Expect 20-50 total on success, doubt >200 reason-phrase
    // chars will be supplied or be helpful in log message.

    // status-line = HTTP/x.x SP status-code SP reason-phrase CRLF
    char response_line[256];
    len = read_response_line(sockfd, response_line, sizeof(response_line), errmsg, errmsg_len);
    if (len < 0) {
        return false;
    }
    // all chars in response line are printable.
    if (len == sizeof(response_line))
        len--;
    response_line[len] = '\0';
    ssize_t response_len = len;
    if (strncasecmp(response_line, "HTTP/1.1 ", 9) != 0) {
        strncpy(errmsg, "proxy malformed header version", errmsg_len);
        return false;
    }
    if (!(len >= 13 &&
          isdigit(response_line[9]) &&
          isdigit(response_line[10]) &&
          isdigit(response_line[11]) &&
          response_line[12] == ' ')) {
        strncpy(errmsg, "proxy malformed header status code", errmsg_len);
        return false;
    }
    // Any 2XX code is success
    if (response_line[9] != '2') {
        snprintf(errmsg, errmsg_len, "proxy request denied %s", &response_line[9]);
        return false;
    }
    do {
        // Keep reading until we see an empty line.
        len = read_response_line(sockfd, NULL, 16384, errmsg, errmsg_len);
        if (len < 0) {
            // Proper end of header not found.
            strncpy(errmsg, "proxy invalid header content", errmsg_len);
            return false;
        }
        response_len += len;
        if (response_len > 16384) {
            // No specific limit on metadata that could be in response,
            // but proxy not expected to be needlessly verbose.  Presume invalid.
            strncpy(errmsg, "proxy response not terminated", errmsg_len);
            return false;
        }
    } while (len != 0);

    strncpy(errmsg, response_line, errmsg_len);
    return true;
}

// Separate thread to create socket and do the proxy negotiation handshake.
// Blocks on getaddrinfo and socket io.
static void *proxy_setup_thread(void *arg)
{
    ASSERT_THREAD_IS(SYS_THREAD_PROXY);
    qd_proxy_setup_info_t *info = (qd_proxy_setup_info_t *) arg;
    qd_connector_t *ctor = info->connector;
    qd_connector_config_t *ctor_config = ctor->ctor_config;
    qd_proxy_profile_t *proxy_profile = &ctor_config->proxy_context->profile;
    char errmsg[256];
    int sockfd = connect_to_proxy(proxy_profile->host, proxy_profile->port, errmsg, sizeof(errmsg));
    if (sockfd != -1) {
        if (proxy_negotiate(sockfd, info->target_host, info->target_port, proxy_profile->username, proxy_profile->password, errmsg, sizeof(errmsg))) {
            info->proxy_socket = sockfd;
            qd_log(LOG_SERVER, QD_LOG_DEBUG, "Proxy connection negotiated through %s:%s for %s:%s, %s", proxy_profile->host, proxy_profile->port, info->target_host, info->target_port, errmsg);
        } else {
            close(sockfd);
            qd_log(LOG_SERVER, QD_LOG_ERROR, "Proxy connection negotiation failed through %s:%s for %s:%s, %s", proxy_profile->host, proxy_profile->port, info->target_host, info->target_port, errmsg);
        }
    } else {
        qd_log(LOG_SERVER, QD_LOG_ERROR, "Proxy connection not established to %s:%s, %s", proxy_profile->host, proxy_profile->port, errmsg);
    }
    // Resume connector try_open processing on the timer thread.
    qd_timer_schedule(info->callback_timer, 0);
    return NULL;
}

ALLOC_DECLARE(qd_proxy_setup_info_t);
ALLOC_DEFINE(qd_proxy_setup_info_t);

void qd_proxy_free(qd_proxy_setup_info_t *info)
{
    free_qd_proxy_setup_info_t(info);
}


/* Obtain an HTTP relay from a forward proxy using HTTP CONNECT and start an AMQP connection from
 * the resulting socket.
 *
 * Currently only used when creating inter-router links.
 * qd_conn->connector->lock held
 */
void qd_proxy_setup_lh(qd_connector_t *c, qd_connection_t *qd_conn, const char *host, const char *port, qd_proxy_context_t *ctx, qd_timer_cb_t cb)
{
    qd_proxy_setup_info_t *info = new_qd_proxy_setup_info_t();

    ZERO(info);
    info->proxy_socket = -1;
    info->connector = c;
    info->qd_conn = qd_conn;
    info->target_host = host;
    info->target_port = port;
    info->profile = &ctx->profile;
    info->callback_timer = qd_timer(amqp_adaptor.dispatch, cb, info);

    info->proxy_thread = sys_thread(SYS_THREAD_PROXY, proxy_setup_thread, info);
}

void qd_proxy_cleanup(qd_proxy_setup_info_t *info)
{
    qd_timer_free(info->callback_timer);
    free_qd_proxy_setup_info_t(info);
}

// Management

/**
 * Top-level proxy context.  One per proxyProfile record.
 */

DEQ_DECLARE(qd_proxy_context_t, qd_proxy_context_list_t);

/**
 * Master list of all active proxy context instances. Only accessed by the management thread so no locking necessary.
 */
static qd_proxy_context_list_t context_list;


ALLOC_DECLARE(qd_proxy_context_t);
ALLOC_DEFINE(qd_proxy_context_t);

/** Release the contents of a configuration instance
 */
static void _cleanup_proxy_profile(qd_proxy_profile_t *profile)
{
    free(profile->host);
    free(profile->port);
    free(profile->username);
    free(profile->password);
    ZERO(profile);
}

/** Free the proxy context. Assumes context is no longer on context_list
 */
static void _proxy_context_free(qd_proxy_context_t *ctxt)
{
    if (ctxt) {
        free(ctxt->proxy_profile_name);
        _cleanup_proxy_profile(&ctxt->profile);
        sys_atomic_destroy(&ctxt->ref_count);
        free_qd_proxy_context_t(ctxt);
    }
}

/** Find the proxy context associated with the given proxyProfile name
 */
static qd_proxy_context_t *_find_proxy_context(const char *profile_name)
{
    ASSERT_MGMT_THREAD;

    qd_proxy_context_t *ctxt = DEQ_HEAD(context_list);
    while (ctxt) {
        if (strcmp(ctxt->proxy_profile_name, profile_name) == 0)
            return ctxt;
        ctxt = DEQ_NEXT(ctxt);
    }
    return 0;
}

void qd_proxy_initialize(void)
{
    DEQ_INIT(context_list);
}

void qd_proxy_finalize(void)
{
    qd_proxy_context_t *ctxt = DEQ_HEAD(context_list);
    while (ctxt) {
        DEQ_REMOVE_HEAD(context_list);
        _proxy_context_free(ctxt);
        ctxt = DEQ_HEAD(context_list);
    }
}

/**
 * Read the proxyProfile configuration record from entity
 */
static qd_error_t _read_proxy_profile(qd_entity_t *entity, qd_proxy_profile_t *profile)
{
    ZERO(profile);

    profile->host                = qd_entity_opt_string(entity, "proxyHost", 0);
    if (qd_error_code()) goto error;
    profile->port                = qd_entity_opt_string(entity, "proxyPort", 0);
    if (qd_error_code()) goto error;
    profile->username = qd_entity_opt_string(entity, "proxyUserName", 0);
    if (qd_error_code()) goto error;
    profile->password       = qd_entity_opt_string(entity, "proxyPassword", 0);

    return QD_ERROR_NONE;

error:
    _cleanup_proxy_profile(profile);
    return qd_error_code();
}

/**
 * Handle proxyProfile record create request from management
 */
QD_EXPORT void *qd_configure_proxy_profile(qd_dispatch_t *qd, qd_entity_t *entity)
{
    ASSERT_MGMT_THREAD;

    qd_error_clear();
    char *name = qd_entity_opt_string(entity, "name", 0);
    if (!name || qd_error_code()) {
        free(name);
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Unable to create proxyProfile: %s", qd_error_message());
        return 0;
    }

    qd_proxy_context_t *proxy_context = new_qd_proxy_context_t();
    ZERO(proxy_context);
    DEQ_ITEM_INIT(proxy_context);
    proxy_context->proxy_profile_name = name;

    if (_read_proxy_profile(entity, &proxy_context->profile) != QD_ERROR_NONE) {
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Unable to create proxyProfile '%s': %s", name, qd_error_message());
        _proxy_context_free(proxy_context);
        return 0;
    }

    sys_atomic_init(&proxy_context->ref_count, 1);  // for caller
    DEQ_INSERT_HEAD(context_list, proxy_context);   // if name collision, last inserted found first
    qd_log(LOG_AGENT, QD_LOG_INFO, "Created proxyProfile %s", proxy_context->proxy_profile_name);
    return proxy_context;
}


/**
 * Handle proxyProfile record delete request from management.
 */
QD_EXPORT void qd_delete_proxy_profile(qd_dispatch_t *qd, void *impl)
{
    ASSERT_MGMT_THREAD;

    qd_proxy_context_t *proxy_context = (qd_proxy_context_t *) impl;
    assert(proxy_context);

    DEQ_REMOVE(context_list, proxy_context);

    qd_log(LOG_AGENT, QD_LOG_INFO, "Deleted proxyProfile %s", proxy_context->proxy_profile_name);

    _proxy_context_free(proxy_context);
}

/**
 * Dummy stub since the Python agent expects a "qd_entity_refresh_BLAH" for every
 * entity that has a C implementation (see CImplementation in agent.py)
 */
QD_EXPORT qd_error_t qd_entity_refresh_proxyProfile(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}

QD_EXPORT qd_proxy_context_t *qd_proxy_context(const char *proxy_profile_name)
{
    ASSERT_MGMT_THREAD;  // called from listener/connector create callback

    qd_error_clear();
    qd_proxy_context_t *proxy_context = _find_proxy_context(proxy_profile_name);
    if (!proxy_context) {
        qd_error(QD_ERROR_NOT_FOUND, "proxyProfile '%s' not found", proxy_profile_name);
        return 0;
    }

    return proxy_context;
}

QD_EXPORT void qd_proxy_context_incref(qd_proxy_context_t *cntx)
{
    if (cntx) {
        sys_atomic_inc(&cntx->ref_count);
    }
}

QD_EXPORT void qd_proxy_context_decref(qd_proxy_context_t *cntx)
{
    if (cntx) {
        if (sys_atomic_dec(&cntx->ref_count) == 1) {
            _proxy_context_free(cntx);
        }
    }
}


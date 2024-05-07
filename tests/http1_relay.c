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
 *
 */

/*
 * HTTP/1.x test proxy for the HTTP/1.x protocol observer.
 *
 * Sits between an HTTP/1.x client(s) and a server and invokes the protocol observer codec on the proxied TCP traffic.
 */

#include "decoders/http1/http1_decoder.h"
#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/buffer.h"

#include "proton/proactor.h"
#include "proton/raw_connection.h"
#include "proton/listener.h"
#include "proton/netaddr.h"

#include <arpa/inet.h>
#include <assert.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>


#define BOOL2STR(b) ((b)?"true":"false")

bool running = true;   // exit main loop when false
bool verbose = false;

const char *listener_host = "localhost";
const char *listener_port = "8888";
char        listener_address[1024];

const char *server_host   = "localhost";
const char *server_port   = "8800";
char        server_address[1024];

pn_proactor_t *proactor;
pn_listener_t *listener;

uintptr_t next_request_id;
uintptr_t next_conn_id;

// State for one side of the stream
//
typedef struct endpoint_state_t endpoint_state_t;
struct endpoint_state_t {
    pn_raw_connection_t *raw_conn;
    qd_buffer_list_t     outgoing;  // buffers staged to send out raw_conn

    bool close_read:1;
    bool close_write:1;
};

typedef struct h1_stream_t h1_stream_t;
struct h1_stream_t {
    DEQ_LINKS(h1_stream_t);

    qd_http1_decoder_connection_t *decoder;

    uintptr_t               conn_id;
    endpoint_state_t        client;   // client-facing endpoint
    endpoint_state_t        server;   // server-facing endpoint
};

ALLOC_DECLARE(h1_stream_t);
ALLOC_DEFINE(h1_stream_t);
DEQ_DECLARE(h1_stream_t, h1_stream_list_t);


h1_stream_list_t stream_list = DEQ_EMPTY;


__attribute__((format(printf, 1, 2))) void debug(const char *format, ...)
{
    va_list args;

    if (!verbose) return;

    struct timeval tv_time = {0};
    static const char time_fmt[] = "%Y-%m-%d %H:%M:%S";

    if (gettimeofday(&tv_time, NULL) != 0) {
        perror("gettimeofday failed");
        exit(1);
    }

    struct tm tm_time;
    char temp[100];

    if (localtime_r(&tv_time.tv_sec, &tm_time) != &tm_time) {
        perror("localtime_r failed");
        exit(1);
    }

    if (strftime(temp, sizeof(temp), time_fmt, &tm_time) == 0) {
        perror("strftime failed");
        exit(1);
    }

    fprintf(stdout, "DEBUG: [%s.%03lld] ", temp, (long long) (tv_time.tv_usec / 1000));
    va_start(args, format);
    vfprintf(stdout, format, args);
    va_end(args);
    fflush(stdout);
}


static void signal_handler(int signum)
{
    signal(signum, SIG_IGN);
    if (proactor)
        pn_proactor_interrupt(proactor);
}


/*
 * HTTP/1.x decoder callbacks
 */


int rx_request(qd_http1_decoder_connection_t *hconn, const char *method, const char *target, uint32_t version_major, uint32_t version_minor, uintptr_t *request_context)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    *request_context = ++next_request_id;
    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] RX-REQ METHOD=%s TARGET=%s VMAJOR=%" PRIu32 " VMINOR=%" PRIu32 "\n",
            stream->conn_id, *request_context, method, target, version_major, version_minor);
    return 0;
}


int rx_response(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, int status_code, const char *reason_phrase, uint32_t version_major, uint32_t version_minor)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] RX-RESP STATUS=%d VMAJOR=%" PRIu32 " VMINOR=%" PRIu32 " REASON=%s\n",
            stream->conn_id, request_context, status_code, version_major, version_minor, reason_phrase);
    return 0;
}


int rx_header(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client, const char *key, const char *value)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    const char *cmd = from_client ? "CLIENT-HEADER" : "SERVER-HEADER";
    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] %s KEY=%s VALUE=%s\n", stream->conn_id, request_context, cmd, key, value);
    return 0;
}


int rx_headers_done(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    const char *cmd = from_client ? "CLIENT-HEADER-DONE" : "SERVER-HEADER-DONE";
    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] %s\n", stream->conn_id, request_context, cmd);
    return 0;
}


int rx_body(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client, const unsigned char *body, size_t length)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    const char *cmd = from_client ? "CLIENT-BODY" : "SERVER-BODY";
    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] %s LENGTH=%zu\n", stream->conn_id, request_context, cmd, length);
    return 0;
}


int message_done(qd_http1_decoder_connection_t *hconn, uintptr_t request_context, bool from_client)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    const char *cmd = from_client ? "CLIENT-MSG-DONE" : "SERVER-MSG-DONE";
    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] %s\n", stream->conn_id, request_context, cmd);
    return 0;
}


int transaction_complete(qd_http1_decoder_connection_t *hconn, uintptr_t request_context)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    fprintf(stdout, "[C%" PRIuPTR ":R%" PRIuPTR "] TRANSACTION-COMPLETE\n", stream->conn_id, request_context);
    return 0;
}


void protocol_error(qd_http1_decoder_connection_t *hconn, const char *reason)
{
    h1_stream_t *stream = (h1_stream_t *) qd_http1_decoder_connection_get_context(hconn);
    assert(stream);

    fprintf(stdout, "[C%" PRIuPTR "] PROTOCOL-ERROR REASON=%s\n", stream->conn_id, reason);
}


qd_http1_decoder_config_t decoder_config = {
    .rx_request = rx_request,
    .rx_response = rx_response,
    .rx_header = rx_header,
    .rx_headers_done = rx_headers_done,
    .rx_body = rx_body,
    .message_done = message_done,
    .transaction_complete = transaction_complete,
    .protocol_error = protocol_error
};


/**
 * h1_stream_t constructor
 */
h1_stream_t *h1_stream(void)
{
    h1_stream_t *stream = new_h1_stream_t();
    ZERO(stream);

    stream->conn_id = ++next_conn_id;
    stream->decoder = qd_http1_decoder_connection(&decoder_config, (uintptr_t) stream);

    stream->client.raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(stream->client.raw_conn, stream);
    DEQ_INIT(stream->client.outgoing);

    stream->server.raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(stream->server.raw_conn, stream);
    DEQ_INIT(stream->server.outgoing);

    return stream;
}


/**
 * h1_stream_t destructor
 */
void h1_stream_free(h1_stream_t *stream)
{
    if (stream) {
        qd_http1_decoder_connection_free(stream->decoder);
        qd_buffer_list_free_buffers(&stream->client.outgoing);
        qd_buffer_list_free_buffers(&stream->server.outgoing);
        free_h1_stream_t(stream);
    }
}


// fill the raw_conn with empty buffers for receiving input
//
static void grant_read_buffers(pn_raw_connection_t *raw_conn)
{
    pn_raw_buffer_t pn_desc;

    size_t count = pn_raw_connection_read_buffers_capacity(raw_conn);
    debug("Granting  %zu read buffers\n", count);

    while (count--) {
        qd_buffer_t *buf = qd_buffer();

        pn_desc.context  = (uintptr_t) buf;
        pn_desc.bytes    = (char *) qd_buffer_base(buf);
        pn_desc.capacity = qd_buffer_capacity(buf);
        pn_desc.size     = 0;
        pn_desc.offset   = 0;

        if (pn_raw_connection_give_read_buffers(raw_conn, &pn_desc, 1) != 1) {
            fprintf(stderr, "raw give read buffer failed\n");
            exit(1);
        }
    }
}


// Free used write buffers
//
static void release_written_buffers(pn_raw_connection_t *raw_conn)
{
    pn_raw_buffer_t pn_desc = {0};

    size_t taken = pn_raw_connection_take_written_buffers(raw_conn, &pn_desc, 1);
    while (taken) {
        qd_buffer_t *buf = (qd_buffer_t *)pn_desc.context;
        assert(buf);
        qd_buffer_free(buf);
        taken = pn_raw_connection_take_written_buffers(raw_conn, &pn_desc, 1);
    }
}


// Move full read buffers from src endpoint to the write channel of dst.
// Pass the buffers through the decoder as they are forwarded
//
static void forward_read_buffers(h1_stream_t *stream, bool is_client, endpoint_state_t *src, endpoint_state_t *dst)
{
    pn_raw_buffer_t pn_desc = {0};
    unsigned int    count   = 0;
    size_t          amount  = 0;

    size_t taken = pn_raw_connection_take_read_buffers(src->raw_conn, &pn_desc, 1);
    while (taken == 1) {
        qd_buffer_t *buf = (qd_buffer_t *) pn_desc.context;
        assert(buf);

        if (pn_desc.size > 0) {
            // move to peer
            buf->size = pn_desc.size;
            DEQ_INSERT_TAIL(dst->outgoing, buf);
            amount += pn_desc.size;
            count += 1;

            // Pass the data to the decoder. There is no recovery if the decoder fails.
            //
            if (stream->decoder) {
                int rc = qd_http1_decoder_connection_rx_data(stream->decoder, is_client, qd_buffer_base(buf), qd_buffer_size(buf));
                if (rc) {
                    debug("Decoder failed! rc=%d\n", rc);
                    qd_http1_decoder_connection_free(stream->decoder);
                    stream->decoder = 0;
                }
            }

        } else {
            qd_buffer_free(buf);
        }

        taken = pn_raw_connection_take_read_buffers(src->raw_conn, &pn_desc, 1);
    }

    if (count && dst->raw_conn) {
        debug("Forwarded %u buffers (%zu octets)\n", count, amount);
        pn_raw_connection_wake(dst->raw_conn);
    }

}


// send outgoing buffers out the endpoint
//
static void send_buffers(endpoint_state_t *endpoint)
{
    pn_raw_buffer_t pn_desc = {0};
    unsigned int    count   = 0;
    size_t          amount  = 0;

    size_t capacity = pn_raw_connection_write_buffers_capacity(endpoint->raw_conn);
    while (capacity > 0) {
        qd_buffer_t *buf = DEQ_HEAD(endpoint->outgoing);
        if (!buf)
            break;

        DEQ_REMOVE_HEAD(endpoint->outgoing);

        pn_desc.context = (uintptr_t) buf;
        pn_desc.bytes   = (char *) qd_buffer_base(buf);
        pn_desc.size    = qd_buffer_size(buf);

        size_t given = pn_raw_connection_write_buffers(endpoint->raw_conn, &pn_desc, 1);
        if (given != 1)
            break;

        amount += pn_desc.size;
        count += 1;
        capacity -= 1;
    }

    if (count) {
        debug("Wrote %u buffers (%zu octets)\n", count, amount);
    }

    if (DEQ_IS_EMPTY(endpoint->outgoing) && endpoint->close_write) {
        endpoint->close_write = false;
        pn_raw_connection_write_close(endpoint->raw_conn);
        debug("endpoint write closed\n");
    }
}


/**
 * discard any buffers held by raw_conn
 */
static void flush_buffers(pn_raw_connection_t *raw_conn)
{
    pn_raw_buffer_t pn_desc = {0};

    release_written_buffers(raw_conn);
    size_t taken = pn_raw_connection_take_read_buffers(raw_conn, &pn_desc, 1);
    while (taken == 1) {
        assert(pn_desc.context);
        qd_buffer_free((qd_buffer_t *) pn_desc.context);
        taken = pn_raw_connection_take_read_buffers(raw_conn, &pn_desc, 1);
    }
}


/**
 * Raw connection event handler - dispatch to the handlers for client or server.
 */
static void raw_event_handler(pn_event_t *event)
{
    pn_event_type_t      etype    = pn_event_type(event);
    pn_raw_connection_t *raw_conn = pn_event_raw_connection(event);
    assert(raw_conn);
    h1_stream_t         *stream   = pn_raw_connection_get_context(raw_conn);
    assert(stream);

    endpoint_state_t *endpoint;
    endpoint_state_t *peer;
    bool              is_client;

    if (raw_conn == stream->client.raw_conn) {
        debug("client connection (%p) event=%s\n", (void *) raw_conn, pn_event_type_name(etype));
        is_client = true;
        endpoint  = &stream->client;
        peer      = &stream->server;
    } else {
        debug("server connection (%p) event=%s\n", (void *) raw_conn, pn_event_type_name(etype));
        assert(raw_conn == stream->server.raw_conn);
        is_client = false;
        endpoint  = &stream->server;
        peer      = &stream->client;
    }

    switch (etype) {
        case PN_RAW_CONNECTION_CONNECTED: {
            const pn_netaddr_t *local_addr = pn_raw_connection_local_addr(raw_conn);
            const pn_netaddr_t *remote_addr = pn_raw_connection_remote_addr(raw_conn);
            char remote_host[200];
            char local_host[200];
            char remote_port[32];
            char local_port[32];
            if (pn_netaddr_host_port(remote_addr, remote_host, sizeof(remote_host), remote_port, sizeof(remote_port)) == 0
                && pn_netaddr_host_port(local_addr, local_host, sizeof(local_host), local_port, sizeof(local_port)) == 0) {
                debug("%s connected, local=%s:%s remote=%s:%s\n", (is_client) ? "client" : "server",
                      local_host, local_port, remote_host, remote_port);
            }
        } break;

        case PN_RAW_CONNECTION_CLOSED_READ:
            if (peer->raw_conn) {
                debug("Closing peer write side\n");
                peer->close_write = true;
                pn_raw_connection_wake(peer->raw_conn);
            }
            break;

        case PN_RAW_CONNECTION_CLOSED_WRITE:
            if (peer->raw_conn) {
                debug("Closing peer read side\n");
                peer->close_read = true;
                pn_raw_connection_wake(peer->raw_conn);
            }
            break;

        case PN_RAW_CONNECTION_DISCONNECTED: {
            flush_buffers(endpoint->raw_conn);
            pn_raw_connection_set_context(endpoint->raw_conn, 0);
            endpoint->raw_conn = 0;
            if (peer->raw_conn == 0) {
                debug("Connection %" PRIuPTR " closed.\n", stream->conn_id);
                DEQ_REMOVE(stream_list, stream);
                h1_stream_free(stream);
            } else {
                debug("force-closing peer connection\n");
                peer->close_read = true;
                peer->close_write = true;
                pn_raw_connection_wake(peer->raw_conn);
            }
            return;  // endpoint.raw_conn no longer valid
        }

        case PN_RAW_CONNECTION_NEED_READ_BUFFERS:
            grant_read_buffers(endpoint->raw_conn);
            break;

        case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS:
            send_buffers(endpoint);
            break;

        case PN_RAW_CONNECTION_READ:
            forward_read_buffers(stream, is_client, endpoint, peer);
            break;

        case PN_RAW_CONNECTION_WRITTEN:
            release_written_buffers(endpoint->raw_conn);
            send_buffers(endpoint);
            break;

        case PN_RAW_CONNECTION_WAKE:
            send_buffers(endpoint);

            if (endpoint->close_read) {
                endpoint->close_read = false;
                pn_raw_connection_read_close(endpoint->raw_conn);
                debug("endpoint read closed\n");
            }
            break;

        case PN_RAW_CONNECTION_DRAIN_BUFFERS:
            flush_buffers(endpoint->raw_conn);
            break;

        default:
            break;
    }
}


/**
 * Listener event handler
 */
static void listener_event_handler(pn_event_t *event)
{
    const pn_event_type_t etype = pn_event_type(event);
    debug("new listener event=%s\n", pn_event_type_name(etype));

    switch (etype) {

        case PN_LISTENER_ACCEPT: {
            h1_stream_t *stream = h1_stream();
            DEQ_INSERT_TAIL(stream_list, stream);

            // raw connection events for these connections will arrive after these calls
            debug("Accepting new client connection, initiating server connection to %s\n", server_address);
            pn_listener_raw_accept(listener, stream->client.raw_conn);
            pn_proactor_raw_connect(proactor, stream->server.raw_conn, server_address);
        } break;

        case PN_LISTENER_OPEN:
            break;

        case PN_LISTENER_CLOSE:
            break;

        default:
            break;
    }
}


/**
 * Handle generic (non-raw-connection) proactor events
 */
static void proactor_event_handler(pn_event_t *event)
{
    const pn_event_type_t etype = pn_event_type(event);
    debug("new proactor event=%s\n", pn_event_type_name(etype));

    switch (etype) {

        case PN_PROACTOR_INTERRUPT: {
            running = false;
        } break;

        case PN_PROACTOR_TIMEOUT:
        default:
            break;
    }
}


static void usage(const char *prog)
{
    printf("Usage: %s <options>\n", prog);
    printf("-l \tThe listener address for incoming client connections [%s:%s]\n", listener_host, listener_port);
    printf("-s \tThe server address to connect to [%s:%s]\n", server_host, server_port);
    printf("-v \tEnable debug output\n");
    exit(1);
}


int main_loop(void )
{
    void (*event_handler)(pn_event_t *event);

    proactor = pn_proactor();
    listener = pn_listener();

    int rc = pn_proactor_addr(listener_address, sizeof(listener_address), listener_host, listener_port);
    if (rc >= sizeof(listener_address)) {
        fprintf(stderr, "listener address too long!\n");
        exit(1);
    }

    rc = pn_proactor_addr(server_address, sizeof(server_address), server_host, server_port);
    if (rc >= sizeof(server_address)) {
        fprintf(stderr, "server address too long!\n");
        exit(1);
    }

    debug("Creating listener on address %s\n", listener_address);
    pn_proactor_listen(proactor, listener, listener_address, 10);

    while (running) {
        debug("Waiting for proactor event...\n");
        pn_event_batch_t *events = pn_proactor_wait(proactor);
        debug("Start new proactor batch\n");

        if (pn_event_batch_raw_connection(events)) {
            event_handler = raw_event_handler;
        } else if (pn_event_batch_listener(events)) {
            event_handler = listener_event_handler;
        } else {
            event_handler = proactor_event_handler;
        }

        pn_event_t *event = pn_event_batch_next(events);
        while (event) {
            event_handler(event);
            event = pn_event_batch_next(events);
        }

        debug("Proactor batch processing done\n");
        pn_proactor_done(proactor, events);
    }

    debug("Send complete!\n");
    pn_proactor_free(proactor);

    return 0;
}


int main(int argc, char** argv)
{
    /* command line options */
    opterr = 0;
    int c;
    while ((c = getopt(argc, argv, "hl:s:v")) != -1) {
        switch(c) {
            case 'h': usage(argv[0]); break;
            case 'v': verbose = true; break;
            case 'l': {
                char *colon = strrchr(optarg, ':');
                if (!colon) {
                    fprintf(stderr, "Error: invalid listener address format: missing port\n");
                    usage(argv[0]);
                } else {
                    *colon = '\0';
                    listener_port = ++colon;
                    listener_host = optarg;
                }
            } break;
            case 's': {
                char *colon = strrchr(optarg, ':');
                if (!colon) {
                    fprintf(stderr, "Error: invalid server address format: missing port\n");
                    usage(argv[0]);
                } else {
                    *colon = '\0';
                    server_port = ++colon;
                    server_host = optarg;
                }
            } break;
        default:
            usage(argv[0]);
            break;
        }
    }

    signal(SIGQUIT, signal_handler);
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    qd_alloc_initialize();

    int rc = main_loop();

    h1_stream_t *stream = DEQ_HEAD(stream_list);
    while (stream) {
        DEQ_REMOVE_HEAD(stream_list);
        h1_stream_free(stream);
        stream = DEQ_HEAD(stream_list);
    }

    qd_alloc_finalize();

    return rc;
}


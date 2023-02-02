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

#include "http_common.h"

#include "adaptor_common.h"
#include "adaptor_tls.h"

#include <proton/listener.h>
#include <proton/tls.h>

#include <stdio.h>

ALLOC_DECLARE(qd_http_listener_t);
ALLOC_DEFINE(qd_http_listener_t);
ALLOC_DECLARE(qd_http_connector_t);
ALLOC_DEFINE(qd_http_connector_t);
ALLOC_DEFINE(qd_http_adaptor_config_t);

//
// HTTP Listener Management (HttpListenerEntity)
//

#define CHECK() if (qd_error_code()) goto error

static qd_http_adaptor_config_t *qd_http_adaptor_config(void)
{
    qd_http_adaptor_config_t *http_config = new_qd_http_adaptor_config_t();
    assert(http_config);
    ZERO(http_config);

    qd_adaptor_config_t *adaptor_config = new_qd_adaptor_config_t();
    assert(adaptor_config);
    ZERO(adaptor_config);

    http_config->adaptor_config = adaptor_config;

    return http_config;
}

static void qd_free_http_adaptor_config(qd_http_adaptor_config_t *config)
{
    if (!config)
        return;

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_INFO,
           "Deleted HTTP adaptor configuration '%s' for address %s, %s, siteId %s.", config->adaptor_config->name,
           config->adaptor_config->address, config->adaptor_config->host_port, config->adaptor_config->site_id);

    //
    // Free the common adaptor configuration
    //
    qd_free_adaptor_config(config->adaptor_config);

    //
    // Free the adaptor config that is common to http1 and http2
    //
    free_qd_http_adaptor_config_t(config);
}

static qd_error_t qd_load_http_adaptor_config(qdr_core_t *core, qd_http_adaptor_config_t *config, qd_entity_t *entity)
{
    assert(config);
    //
    // First load the common config data (common to all adaptors)
    //
    qd_error_t qd_error = qd_load_adaptor_config(core, config->adaptor_config, entity);
    if (qd_error != QD_ERROR_NONE) {
        return qd_error;
    }

    //
    // Now, load http1/http2 specific config.
    //
    char *aggregation_str = 0;
    char *version_str     = qd_entity_opt_string(entity, "protocolVersion", "HTTP1");
    CHECK();
    if (strcmp(version_str, "HTTP2") == 0) {
        config->http_version = HTTP2;
    } else if (strcmp(version_str, "HTTP1") == 0) {
        config->http_version = HTTP1;
    } else {
        qd_error(QD_ERROR_CONFIG, "Invalid value for HTTP version: %s, expected 'HTTP1' or 'HTTP2'", version_str);
        goto error;
    }
    free(version_str);
    version_str = 0;


    if (config->http_version == HTTP1) {
        aggregation_str = qd_entity_opt_string(entity, "aggregation", 0);  CHECK();
        config->event_channel     = qd_entity_opt_bool(entity, "eventChannel", false);     CHECK();
        config->host_override     = qd_entity_opt_string(entity, "hostOverride", 0);       CHECK();

        if (aggregation_str && strcmp(aggregation_str, "json") == 0) {
            config->aggregation = QD_AGGREGATION_JSON;
        } else if (aggregation_str && strcmp(aggregation_str, "multipart") == 0) {
            config->aggregation = QD_AGGREGATION_MULTIPART;
        } else {
            config->aggregation = QD_AGGREGATION_NONE;
        }
        free(aggregation_str);
        aggregation_str = 0;
    }

    return QD_ERROR_NONE;

error:
    free(version_str);
    free(aggregation_str);
    return qd_error_code();
}


qd_http_listener_t *qd_dispatch_configure_http_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_http_listener_t *listener = qd_http_listener(qd->server);
    assert(listener);

    if (qd_load_http_adaptor_config(qd->router->router_core, listener->config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "Unable to configure a new httpListener: %s",
               qd_error_message());
        qd_http_listener_decref(listener);
        return 0;
    }

    switch (listener->config->http_version) {
        case HTTP1:
            listener = qd_http1_configure_listener(listener, qd, entity);
            break;
        case HTTP2:
            listener = qd_http2_configure_listener(listener, qd, entity);
            break;
        default:
            assert(false);  // qd_load_http_adaptor_config will detect bad version
            break;
    }

    return listener;
}


void qd_dispatch_delete_http_listener(qd_dispatch_t *qd, void *impl)
{
    qd_http_listener_t *listener = (qd_http_listener_t*) impl;
    if (listener) {
        switch (listener->config->http_version) {
        case HTTP1:
            qd_http1_delete_listener(qd, listener);
            break;
        case HTTP2:
            qd_http2_delete_listener(qd, listener);
            break;
        default:
            assert(false);
            break;
        }
        // it is expected that the calls above have properly decremented the listener reference count
    }
}


qd_error_t qd_entity_refresh_httpListener(qd_entity_t* entity, void *impl)
{
    qd_http_listener_t *listener = (qd_http_listener_t*) impl;
    qd_listener_oper_status_t os = qd_adaptor_listener_oper_status(listener->adaptor_listener);
    if (qd_entity_set_string(entity, "operStatus",
                             os == QD_LISTENER_OPER_UP ? "up" : "down") == 0)
        return QD_ERROR_NONE;
    return qd_error_code();
}


//
// HTTP Connector Management (HttpConnectorEntity)
//


qd_http_connector_t *qd_dispatch_configure_http_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_http_connector_t *conn = qd_http_connector(qd->server);
    assert(conn);

    if (qd_load_http_adaptor_config(qd->router->router_core, conn->config, entity) != QD_ERROR_NONE) {
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_ERROR, "Unable to configure a new httpConnector: %s",
               qd_error_message());
        qd_http_connector_decref(conn);
        return 0;
    }

    switch (conn->config->http_version) {
        case HTTP1:
            conn = qd_http1_configure_connector(conn, qd, entity);
            break;
        case HTTP2:
            conn = qd_http2_configure_connector(conn, qd, entity);
            break;
        default:
            assert(false);  // qd_load_http_adaptor_config will detect bad version
            break;
    }

    return conn;
}


void qd_dispatch_delete_http_connector(qd_dispatch_t *qd, void *impl)
{
    qd_http_connector_t *conn = (qd_http_connector_t*) impl;

    if (conn) {
        switch (conn->config->http_version) {
        case HTTP1:
            qd_http1_delete_connector(qd, conn);
            break;
        case HTTP2:
            qd_http2_delete_connector(qd, conn);
            break;
        default:
            assert(false);
            break;
        }
        // it is expected that the calls above have properly decremented the connector reference count
    }
}

qd_error_t qd_entity_refresh_httpConnector(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}

//
// qd_http_listener_t constructor
//

qd_http_listener_t *qd_http_listener(qd_server_t *server)
{
    qd_http_listener_t *li = new_qd_http_listener_t();
    assert(li);
    ZERO(li);
    sys_atomic_init(&li->ref_count, 1);
    li->server = server;
    li->config = qd_http_adaptor_config();
    DEQ_ITEM_INIT(li);
    return li;
}

void qd_http_listener_decref(qd_http_listener_t *li)
{
    if (li && sys_atomic_dec(&li->ref_count) == 1) {
        vflow_end_record(li->vflow);
        qd_free_http_adaptor_config(li->config);
        qd_tls_domain_decref(li->tls_domain);
        sys_atomic_destroy(&li->ref_count);
        free_qd_http_listener_t(li);
    }
}

//
// qd_http_connector_t constructor
//

qd_http_connector_t *qd_http_connector(qd_server_t *server)
{
    qd_http_connector_t *c = new_qd_http_connector_t();
    assert(c);
    ZERO(c);
    sys_atomic_init(&c->ref_count, 1);
    c->server = server;
    c->config = qd_http_adaptor_config();
    DEQ_ITEM_INIT(c);

    return c;
}

void qd_http_connector_decref(qd_http_connector_t* c)
{
    if (c && sys_atomic_dec(&c->ref_count) == 1) {
        vflow_end_record(c->vflow);
        qd_free_http_adaptor_config(c->config);
        qd_tls_domain_decref(c->tls_domain);
        sys_atomic_destroy(&c->ref_count);
        free_qd_http_connector_t(c);
    }
}


typedef struct qdr_http_method_status_t  qdr_http_method_status_t;

struct qdr_http_method_status_t {
    DEQ_LINKS(qdr_http_method_status_t);

    char     *key;
    uint64_t requests;
};
DEQ_DECLARE(qdr_http_method_status_t, qdr_http_method_status_list_t);

typedef struct qdr_http_request_info_t  qdr_http_request_info_t;

struct qdr_http_request_info_t {
    DEQ_LINKS(qdr_http_request_info_t);

    char     *key;
    char     *address;
    char     *host;
    char     *site;
    bool      ingress;
    uint64_t  requests;
    uint64_t  bytes_in;
    uint64_t  bytes_out;
    uint64_t  max_latency;
    qdr_http_method_status_list_t detail;
};
DEQ_DECLARE(qdr_http_request_info_t, qdr_http_request_info_list_t);

#define QDR_HTTP_REQUEST_INFO_NAME                   0
#define QDR_HTTP_REQUEST_INFO_IDENTITY               1
#define QDR_HTTP_REQUEST_INFO_ADDRESS                2
#define QDR_HTTP_REQUEST_INFO_HOST                   3
#define QDR_HTTP_REQUEST_INFO_SITE                   4
#define QDR_HTTP_REQUEST_INFO_DIRECTION              5
#define QDR_HTTP_REQUEST_INFO_REQUESTS               6
#define QDR_HTTP_REQUEST_INFO_BYTES_IN               7
#define QDR_HTTP_REQUEST_INFO_BYTES_OUT              8
#define QDR_HTTP_REQUEST_INFO_MAX_LATENCY            9
#define QDR_HTTP_REQUEST_INFO_DETAIL                10


const char * const QDR_HTTP_REQUEST_INFO_DIRECTION_IN  = "in";
const char * const QDR_HTTP_REQUEST_INFO_DIRECTION_OUT = "out";

const char *qdr_http_request_info_columns[] =
    {"name",
     "identity",
     "address",
     "host",
     "site",
     "direction",
     "requests",
     "bytesIn",
     "bytesOut",
     "maxLatency",
     "details",
     0};

const char *HTTP_REQUEST_INFO_TYPE = "io.skupper.router.httpRequestInfo";

typedef struct {
    qdr_http_request_info_list_t records;
} http_request_info_records_t;

static http_request_info_records_t* request_info = 0;

static http_request_info_records_t *_get_request_info(void)
{
    if (!request_info) {
        request_info = NEW(http_request_info_records_t);
        DEQ_INIT(request_info->records);
    }
    return request_info;
}

static void insert_column(qdr_core_t *core, qdr_http_request_info_t *record, int col, qd_composed_field_t *body)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "Insert column %i for %p", col, (void *) record);

    if (!record)
        return;

    switch(col) {
    case QDR_HTTP_REQUEST_INFO_NAME:
        qd_compose_insert_string(body, record->key);
        break;

    case QDR_HTTP_REQUEST_INFO_IDENTITY: {
        qd_compose_insert_string(body, record->key);
        break;
    }

    case QDR_HTTP_REQUEST_INFO_ADDRESS:
        qd_compose_insert_string(body, record->address);
        break;

    case QDR_HTTP_REQUEST_INFO_HOST:
        qd_compose_insert_string(body, record->host);
        break;

    case QDR_HTTP_REQUEST_INFO_SITE:
        qd_compose_insert_string(body, record->site);
        break;

    case QDR_HTTP_REQUEST_INFO_DIRECTION:
        if (record->ingress)
            qd_compose_insert_string(body, QDR_HTTP_REQUEST_INFO_DIRECTION_IN);
        else
            qd_compose_insert_string(body, QDR_HTTP_REQUEST_INFO_DIRECTION_OUT);
        break;

    case QDR_HTTP_REQUEST_INFO_REQUESTS:
        qd_compose_insert_uint(body, record->requests);
        break;

    case QDR_HTTP_REQUEST_INFO_BYTES_IN:
        qd_compose_insert_uint(body, record->bytes_in);
        break;

    case QDR_HTTP_REQUEST_INFO_BYTES_OUT:
        qd_compose_insert_uint(body, record->bytes_out);
        break;

    case QDR_HTTP_REQUEST_INFO_MAX_LATENCY:
        qd_compose_insert_uint(body, record->max_latency);
        break;

    case QDR_HTTP_REQUEST_INFO_DETAIL:
        qd_compose_start_map(body);
        for (qdr_http_method_status_t *item = DEQ_HEAD(record->detail); item; item = DEQ_NEXT(item)) {
            qd_compose_insert_string(body, item->key);
            qd_compose_insert_int(body, item->requests);
        }
        qd_compose_end_map(body);
        break;

    }
}


static void write_list(qdr_core_t *core, qdr_query_t *query,  qdr_http_request_info_t *record)
{
    qd_composed_field_t *body = query->body;

    qd_compose_start_list(body);

    if (record) {
        int i = 0;
        while (query->columns[i] >= 0) {
            insert_column(core, record, query->columns[i], body);
            i++;
        }
    }
    qd_compose_end_list(body);
}

static void write_map(qdr_core_t           *core,
                      qdr_http_request_info_t *record,
                      qd_composed_field_t  *body,
                      const char           *qdr_connection_columns[])
{
    qd_compose_start_map(body);

    for(int i = 0; i < QDR_HTTP_REQUEST_INFO_COLUMN_COUNT; i++) {
        qd_compose_insert_string(body, qdr_connection_columns[i]);
        insert_column(core, record, i, body);
    }

    qd_compose_end_map(body);
}

static void advance(qdr_query_t *query, qdr_http_request_info_t *record)
{
    if (record) {
        query->next_offset++;
        record = DEQ_NEXT(record);
        query->more = !!record;
    }
    else {
        query->more = false;
    }
}

static qdr_http_request_info_t *find_by_identity(qdr_core_t *core, qd_iterator_t *identity)
{
    if (!identity)
        return 0;

    qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records);
    while (record) {
        // Convert the passed in identity to a char*
        if (qd_iterator_equal(identity, (const unsigned char*) record->key))
            break;
        record = DEQ_NEXT(record);
    }

    return record;

}

void qdra_http_request_info_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset)
{
    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "query for first http request info (%i)", offset);
    query->status = QD_AMQP_OK;

    if (offset >= DEQ_SIZE(_get_request_info()->records)) {
        query->more = false;
        qdr_agent_enqueue_response_CT(core, query);
        return;
    }

    qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records);
    for (int i = 0; i < offset && record; i++)
        record = DEQ_NEXT(record);
    assert(record);

    if (record) {
        write_list(core, query, record);
        query->next_offset = offset;
        advance(query, record);
    } else {
        query->more = false;
    }

    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_http_request_info_get_next_CT(qdr_core_t *core, qdr_query_t *query)
{
    qdr_http_request_info_t *record = 0;

    if (query->next_offset < DEQ_SIZE(_get_request_info()->records)) {
        record = DEQ_HEAD(_get_request_info()->records);
        for (int i = 0; i < query->next_offset && record; i++)
            record = DEQ_NEXT(record);
    }

    if (record) {
        write_list(core, query, record);
        advance(query, record);
    } else {
        query->more = false;
    }
    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_http_request_info_get_CT(qdr_core_t          *core,
                               qd_iterator_t       *name,
                               qd_iterator_t       *identity,
                               qdr_query_t         *query,
                               const char          *qdr_http_request_info_columns[])
{
    qdr_http_request_info_t *record = 0;

    if (!identity) {
        query->status = QD_AMQP_BAD_REQUEST;
        query->status.description = "Name not supported. Identity required";
        qd_log(LOG_AGENT, QD_LOG_ERROR, "Error performing READ of %s: %s", HTTP_REQUEST_INFO_TYPE,
               query->status.description);
    } else {
        record = find_by_identity(core, identity);

        if (record == 0) {
            query->status = QD_AMQP_NOT_FOUND;
        } else {
            write_map(core, record, query->body, qdr_http_request_info_columns);
            query->status = QD_AMQP_OK;
        }
    }
    qdr_agent_enqueue_response_CT(core, query);
}

static const char* UNKNOWN = "unknown";

static qdr_http_method_status_t* _new_qdr_http_method_status_t(const char *const method, int status)
{
    qdr_http_method_status_t* record = NEW(qdr_http_method_status_t);
    ZERO(record);

    if (status >= 600 || status < 100) {
        status = 500;
    }
    if (method) {
        size_t key_len = strlen(method) + 5;
        record->key = malloc(key_len);
        snprintf(record->key, key_len, "%s:%03i", method, status);
    } else {
        record->key = qd_strdup(UNKNOWN);
    }

    return record;
}

static void _free_qdr_http_method_status(qdr_http_method_status_t* record)
{
    free(record->key);
    free(record);
}

static void _free_qdr_http_request_info(qdr_http_request_info_t* record)
{
    if (record->key) {
        free(record->key);
    }
    if (record->address) {
        free(record->address);
    }
    if (record->host) {
        free(record->host);
    }
    if (record->site) {
        free(record->site);
    }
    for (qdr_http_method_status_t *item = DEQ_HEAD(record->detail); item; item = DEQ_HEAD(record->detail)) {
        _free_qdr_http_method_status(item);
    }
    free(record);
}

static void _update_http_method_status_detail(qdr_http_method_status_list_t *detail, qdr_http_method_status_t *addition)
{
    bool updated = false;
    for (qdr_http_method_status_t *item = DEQ_HEAD(*detail); item && !updated; item = DEQ_NEXT(item)) {
        if (strcmp(item->key, addition->key) == 0) {
            item->requests += addition->requests;
            _free_qdr_http_method_status(addition);
            updated = true;
        }
    }
    if (!updated) {
        DEQ_INSERT_TAIL(*detail, addition);
    }
}

static bool _update_qdr_http_request_info(qdr_http_request_info_t* record, qdr_http_request_info_t* additions)
{
    if (strcmp(record->key, additions->key) == 0) {
        record->requests += additions->requests;
        record->bytes_in += additions->bytes_in;
        record->bytes_out += additions->bytes_out;
        if (additions->max_latency > record->max_latency) {
            record->max_latency = additions->max_latency;
        }
        for (qdr_http_method_status_t *item = DEQ_HEAD(additions->detail); item; item = DEQ_HEAD(additions->detail)) {
            DEQ_REMOVE_HEAD(additions->detail);
            _update_http_method_status_detail(&record->detail, item);
        }
        return true;
    } else {
        return false;
    }
}

static void _add_http_request_info_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_http_request_info_t *update = (qdr_http_request_info_t*) action->args.general.context_1;
    bool updated = false;
    for (qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records); record && !updated; record = DEQ_NEXT(record)) {
        if (_update_qdr_http_request_info(record, update)) {
            updated = true;
            _free_qdr_http_request_info(update);
            qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "Updated http request info %s", record->key);
        }
    }
    if (!updated) {
        DEQ_INSERT_TAIL(_get_request_info()->records, update);
        qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "Added http request info %s (%zu)", update->key,
               DEQ_SIZE(_get_request_info()->records));
    }
}

static void _add_http_request_info(qdr_core_t *core, qdr_http_request_info_t* record)
{
    qdr_action_t *action = qdr_action(_add_http_request_info_CT, "add_http_request_info");
    action->args.general.context_1 = record;
    qdr_action_enqueue(core, action);
}

static qdr_http_request_info_t *_new_qdr_http_request_info_t(void)
{
    qdr_http_request_info_t* record = NEW(qdr_http_request_info_t);
    ZERO(record);
    DEQ_INIT(record->detail);
    return record;
}

static char *_record_key(const char *host, const char *address, const char* site, bool ingress)
{
    if (!host)
        return 0;

    size_t hostlen = strlen(host);
    size_t addresslen = address ? strlen(address) + 1 : 0;
    size_t sitelen = site ? strlen(site) + 1 : 0;
    char *key = malloc(hostlen + addresslen + sitelen + 3);
    size_t i = 0;
    key[i++] = ingress ? 'i' : 'o';
    key[i++] = '_';
    strcpy(key+i, host);
    i += hostlen;
    if (address) {
        key[i++] = '_';
        strcpy(key+i, address);
        i += (addresslen-1);
    }
    if (site) {
        key[i++] = '@';
        strcpy(key+i, site);
    }
    return key;
}

void qd_http_record_request(qdr_core_t *core, const char * method, uint32_t status_code, const char *address, const char *host,
                             const char *local_site, const char *remote_site, bool ingress,
                             uint64_t bytes_in, uint64_t bytes_out, uint64_t latency)
{
    //
    // The _record_key() returns zero if there is no host. The qdr_http_request_info_t (record) should not even be created
    // if the passed in host parameter is zero. There is no point in having a record without a key.
    //
    if (!host)
        return;

    qdr_http_request_info_t* record = _new_qdr_http_request_info_t();
    record->ingress = ingress;
    record->address = address ? qd_strdup(address) : 0;
    record->host                    = qd_strdup(host);
    record->site = remote_site ? qd_strdup(remote_site) : 0;
    record->key = _record_key(record->host, record->address, remote_site, record->ingress);
    record->requests = 1;
    record->bytes_in = bytes_in;
    record->bytes_out = bytes_out;
    record->max_latency = latency;

    qdr_http_method_status_t *detail = _new_qdr_http_method_status_t(method, (int) status_code);
    detail->requests = 1;
    DEQ_INSERT_TAIL(record->detail, detail);

    qd_log(LOG_HTTP_ADAPTOR, QD_LOG_DEBUG, "Adding http request info %s", record->key);
    _add_http_request_info(core, record);
}

char *qd_get_host_from_host_port(const char *host_port)
{
    char *end = strchr(host_port, ':');
    if (end == NULL) {
        return 0;
    } else {
        size_t len = end - host_port;
        char *host = malloc(len + 1);
        strncpy(host, host_port, len);
        host[len] = '\0';
        return host;
    }
}

void qd_set_condition_on_vflow(pn_raw_connection_t *raw_conn, vflow_record_t *vflow)
{
    pn_condition_t *cond = pn_raw_connection_condition(raw_conn);
    if (!!cond) {
        const char *cname = pn_condition_get_name(cond);
        const char *cdesc = pn_condition_get_description(cond);

        if (!!cname) {
            vflow_set_string(vflow, VFLOW_ATTRIBUTE_RESULT, cname);
        }
        if (!!cdesc) {
            vflow_set_string(vflow, VFLOW_ATTRIBUTE_REASON, cdesc);
        }
    }
}

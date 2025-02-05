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

#include "python_private.h"
#include "qpid/dispatch/python_embedded.h"

#include "qpid/dispatch.h"

#include "config.h"
#include "dispatch_private.h"
#include "entity.h"
#include "entity_cache.h"
#include "http.h"
#include "log_private.h"
#include "message_private.h"
#include "policy.h"
#include "router_private.h"

#include "qpid/dispatch/alloc.h"
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/discriminator.h"
#include "qpid/dispatch/server.h"
#include "qpid/dispatch/static_assert.h"
#include "qpid/dispatch/tls_common.h"

#include <dlfcn.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/random.h>

/**
 * Private Function Prototypes
 */
qd_server_t    *qd_server(qd_dispatch_t *qd, int tc, const char *router_id,
                          const char *sasl_config_path, const char *sasl_config_name);
void            qd_server_free(qd_server_t *server);
qd_policy_t    *qd_policy(qd_dispatch_t *qd);
void            qd_policy_free(qd_policy_t *policy);
qd_router_t    *qd_router(qd_dispatch_t *qd, qd_router_mode_t mode, const char *area, const char *id);
void            qd_router_setup_late(qd_dispatch_t *qd);
void            qd_router_free(qd_router_t *router);
void            qd_error_initialize(void);
static void qd_dispatch_set_router_id(qd_dispatch_t *qd, char *_id);
static void qd_dispatch_set_router_area(qd_dispatch_t *qd, char *_area);
static void qd_dispatch_set_router_van_id(qd_dispatch_t *qd, char *_van_id);
static void qd_dispatch_policy_c_counts_free(PyObject *capsule);

const char     *CLOSEST_DISTRIBUTION   = "closest";
const char     *MULTICAST_DISTRIBUTION = "multicast";
const char     *BALANCED_DISTRIBUTION  = "balanced";
const char     *UNAVAILABLE_DISTRIBUTION = "unavailable";

sys_atomic_t global_delivery_id;

qd_dispatch_t *qd = 0;

qd_dispatch_t *qd_dispatch_get_dispatch(void)
{
    return qd;
}

/* _test_hooks is set to true when the router is started in test hook mode via the '-T' command line argument. It is a
 * global state. It is set at startup and does not change for the life of the router.
 */
static bool _test_hooks = false;

bool qd_router_test_hooks_enabled(void)
{
    return _test_hooks;
}

qd_dispatch_t *qd_dispatch(const char *python_pkgdir, bool test_hooks)
{
    _test_hooks = test_hooks;

    //
    // Seed the random number generator. The router does not need crypto-grade randomness so a Pseudo-RNG is acceptable.
    //
    unsigned int seed = 0;
#if QD_HAVE_GETRANDOM
    while (getrandom(&seed, sizeof(seed), 0) == -1 && errno == EINTR) {
        // EINTR will occur only if a signal arrives while blocking for
        // the entropy pool to initialize. Non-fatal, try again.
    }
#endif
    if (seed == 0) {  // getrandom() not supported
        struct timespec tspec;
        clock_gettime(CLOCK_MONOTONIC, &tspec);
        // rotate lower (more random) bits to make them more significant
        unsigned int timestamp = (unsigned int) (tspec.tv_sec + tspec.tv_nsec);
        timestamp = (timestamp<<11) | (timestamp>>(sizeof(timestamp) * CHAR_BIT - 11));
        seed = (unsigned int)(getpid() ^ timestamp);
    }
    srandom(seed);

    sys_atomic_init(&global_delivery_id, 1);

    qd = NEW(qd_dispatch_t);
    ZERO(qd);

    qd_entity_cache_initialize();   /* Must be first */
    qd_alloc_initialize();
    qd_log_initialize();
    qd_tls_initialize();
    qd_error_initialize();
    if (qd_error_code()) { qd_dispatch_free(qd); return 0; }

    if (python_pkgdir) {
        struct stat st;
        if (stat(python_pkgdir, &st)) {
            qd_error_errno(errno, "Cannot find Python library path '%s'", python_pkgdir);
            return NULL;
        } else if (!S_ISDIR(st.st_mode)) {
            qd_error(QD_ERROR_RUNTIME, "Python library path '%s' not a directory", python_pkgdir);
            return NULL;
        }
    }

    qd_dispatch_set_router_area(qd, strdup("0"));
    qd_dispatch_set_router_id(qd, strdup("0"));
    qd->router_mode = QD_ROUTER_MODE_ENDPOINT;
    qd->default_treatment = QD_TREATMENT_ANYCAST_BALANCED;

    qd_python_initialize(qd, python_pkgdir);
    if (qd_error_code()) { qd_dispatch_free(qd); return 0; }
    qd_message_initialize();
    if (qd_error_code()) { qd_dispatch_free(qd); return 0; }
    return qd;
}


// We sometimes still pass pointers as longs via the python interface, make sure this is safe.
STATIC_ASSERT(sizeof(long) >= sizeof(void*), pointer_is_bigger_than_long);

qd_error_t qd_dispatch_load_config(qd_dispatch_t *qd, const char *config_path)
{
    qd_python_lock_state_t lock_state = qd_python_lock();
    PyObject *module = PyImport_ImportModule("skupper_router_internal.management.config");
    PyObject *configure_dispatch = module ? PyObject_GetAttrString(module, "configure_dispatch") : NULL;
    Py_XDECREF(module);
    PyObject *result = configure_dispatch ? PyObject_CallFunction(configure_dispatch, "(Ns)", PyLong_FromVoidPtr(qd), config_path)
                                          : NULL;
    Py_XDECREF(configure_dispatch);
    if (!result) qd_error_py();
    Py_XDECREF(result);
    qd_python_unlock(lock_state);
    return qd_error_code();
}

qd_error_t qd_dispatch_validate_config(const char *config_path)
{
	FILE* config_file = NULL;
	char config_data = '\0';
	qd_error_t validation_error = QD_ERROR_CONFIG;

	do {
		if (!config_path) {
			validation_error = qd_error(QD_ERROR_VALUE, "Configuration path value was empty");
			break;
		}

		config_file = fopen(config_path, "r");
		if (!config_file) {
			validation_error = qd_error(QD_ERROR_NOT_FOUND, "Configuration file could not be opened");
			break;
		}

		// TODO Check the actual minimum number of bytes required for the smallest valid configuration file
		if (!fread((void*)&config_data, 1, 1, config_file)) {
			validation_error = qd_error(QD_ERROR_CONFIG, "Configuration file was empty");
			break;
		}

		// TODO Add real validation code

		validation_error = QD_ERROR_NONE;
	} while (false); // do once

	if (config_file)
	{
		fclose(config_file);
	}

	return validation_error;
}

// Takes ownership of distribution string.
static void qd_dispatch_set_router_default_distribution(qd_dispatch_t *qd, char *distribution)
{
    if (distribution) {
        if (strcmp(distribution, MULTICAST_DISTRIBUTION) == 0)
            qd->default_treatment = QD_TREATMENT_MULTICAST_ONCE;
        else if (strcmp(distribution, CLOSEST_DISTRIBUTION) == 0)
            qd->default_treatment = QD_TREATMENT_ANYCAST_CLOSEST;
        else if (strcmp(distribution, BALANCED_DISTRIBUTION) == 0)
            qd->default_treatment = QD_TREATMENT_ANYCAST_BALANCED;
        else if (strcmp(distribution, UNAVAILABLE_DISTRIBUTION) == 0)
            qd->default_treatment = QD_TREATMENT_UNAVAILABLE;
    }
    else
        // The default for the router defaultDistribution field is QD_TREATMENT_ANYCAST_BALANCED
        qd->default_treatment = QD_TREATMENT_ANYCAST_BALANCED;
    free(distribution);
}

qd_error_t qd_dispatch_configure_router(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_dispatch_set_router_default_distribution(qd, qd_entity_opt_string(entity, "defaultDistribution", 0)); QD_ERROR_RET();
    qd_dispatch_set_router_id(qd, qd_entity_opt_string(entity, "id", 0)); QD_ERROR_RET();
    qd_dispatch_set_router_van_id(qd, qd_entity_opt_string(entity, "vanId", 0)); QD_ERROR_RET();
    qd->router_mode = qd_entity_get_long(entity, "mode"); QD_ERROR_RET();
    if (!qd->router_id) {
        char *mode = 0;
        switch (qd->router_mode) {
        case QD_ROUTER_MODE_STANDALONE: mode = "Standalone_"; break;
        case QD_ROUTER_MODE_INTERIOR:   mode = "Interior_";   break;
        case QD_ROUTER_MODE_EDGE:       mode = "Edge_";       break;
        case QD_ROUTER_MODE_ENDPOINT:   mode = "Endpoint_";   break;
        }

        qd->router_id = (char*) malloc(strlen(mode) + QD_DISCRIMINATOR_SIZE + 2);
        strcpy(qd->router_id, mode);
        qd_generate_discriminator(qd->router_id + strlen(qd->router_id));
    }
    qd->thread_count = qd_entity_opt_long(entity, "workerThreads", 4); QD_ERROR_RET();
    char *data_conn_count_str = qd_entity_opt_string(entity, "dataConnectionCount", "auto"); QD_ERROR_RET();

    if (!strcmp("auto", data_conn_count_str)) {
        // The user has explicitly requested 'auto'.
        qd->data_connection_count = (qd->thread_count + 1) / 2;
        qd_log(LOG_ROUTER, QD_LOG_INFO, "Inter-router data connections calculated at %d ", qd->data_connection_count);
    } else if (1 == sscanf(data_conn_count_str, "%u", &qd->data_connection_count)) {
        // The user has requested a specific number of connections.
        qd_log(LOG_ROUTER, QD_LOG_INFO, "Inter-router data connections set to %d ", qd->data_connection_count);
    } else {
        // The user has entered a non-numeric value that is not 'auto'.
        // This is not a legal value. Default to 'auto' and mention it.
        qd_log(LOG_ROUTER, QD_LOG_INFO, "Bad value \"%s\" for dataConnectionCount ", data_conn_count_str);
        qd->data_connection_count = (qd->thread_count + 1) / 2;
        qd_log(LOG_ROUTER, QD_LOG_INFO, "Inter-router data connections calculated at %d ", qd->data_connection_count);
    }
    free(data_conn_count_str);

    qd->timestamps_in_utc = qd_entity_opt_bool(entity, "timestampsInUTC", false); QD_ERROR_RET();
    qd->timestamp_format = qd_entity_opt_string(entity, "timestampFormat", 0); QD_ERROR_RET();
    qd->metadata = qd_entity_opt_string(entity, "metadata", 0); QD_ERROR_RET();
    qd->terminate_tcp_conns   = qd_entity_opt_bool(entity, "dropTcpConnections", true);
    QD_ERROR_RET();

    if (! qd->sasl_config_path) {
        qd->sasl_config_path = qd_entity_opt_string(entity, "saslConfigDir", 0); QD_ERROR_RET();
    }
    if (! qd->sasl_config_name) {
        qd->sasl_config_name = qd_entity_opt_string(entity, "saslConfigName", "skrouterd"); QD_ERROR_RET();
    }

    char *dump_file = qd_entity_opt_string(entity, "debugDumpFile", 0); QD_ERROR_RET();
    if (dump_file) {
        qd_alloc_debug_dump(dump_file); QD_ERROR_RET();
        free(dump_file);
    }

    return QD_ERROR_NONE;

}

qd_error_t qd_dispatch_configure_address(qd_dispatch_t *qd, qd_entity_t *entity) {
    if (!qd->router) return qd_error(QD_ERROR_NOT_FOUND, "No router available");
    qd_router_configure_address(qd->router, entity);
    return qd_error_code();
}

QD_EXPORT qd_error_t qd_dispatch_configure_auto_link(qd_dispatch_t *qd, qd_entity_t *entity) {
    if (!qd->router) return qd_error(QD_ERROR_NOT_FOUND, "No router available");
    qd_router_configure_auto_link(qd->router, entity);
    return qd_error_code();
}

QD_EXPORT qd_error_t qd_dispatch_configure_policy(qd_dispatch_t *qd, qd_entity_t *entity)
{
    qd_error_t err;
    err = qd_entity_configure_policy(qd->policy, entity);
    if (err)
        return err;
    return QD_ERROR_NONE;
}


QD_EXPORT qd_error_t qd_dispatch_register_policy_manager(qd_dispatch_t *qd, qd_entity_t *entity)
{
    return qd_register_policy_manager(qd->policy, entity);
}


QD_EXPORT PyObject *qd_dispatch_policy_c_counts_alloc(void)
{
    return PyCapsule_New(qd_policy_c_counts_alloc(), "qd_policy_c_counts", qd_dispatch_policy_c_counts_free);
}

static void qd_dispatch_policy_c_counts_free(PyObject *capsule)
{
    void *ccounts = PyCapsule_GetPointer(capsule, "qd_policy_c_counts");
    qd_policy_c_counts_free(ccounts);
}

QD_EXPORT void qd_dispatch_policy_c_counts_refresh(PyObject *ccounts_capsule, qd_entity_t *entity)
{
    assert(PyCapsule_CheckExact(ccounts_capsule));
    const char * name = PyCapsule_GetName(ccounts_capsule);
    assert(PyCapsule_IsValid(ccounts_capsule, name));
    void* ccounts = PyCapsule_GetPointer(ccounts_capsule, name);
    qd_error_py();
    qd_policy_c_counts_refresh(ccounts, entity);
}

QD_EXPORT bool qd_dispatch_policy_host_pattern_add(qd_dispatch_t *qd, void *py_obj)
{
    char *hostPattern = py_string_2_c(py_obj);
    bool rc = qd_policy_host_pattern_add(qd->policy, hostPattern);
    free(hostPattern);
    return rc;
}

QD_EXPORT void qd_dispatch_policy_host_pattern_remove(qd_dispatch_t *qd, void *py_obj)
{
    char *hostPattern = py_string_2_c(py_obj);
    qd_policy_host_pattern_remove(qd->policy, hostPattern);
    free(hostPattern);
}

QD_EXPORT char * qd_dispatch_policy_host_pattern_lookup(qd_dispatch_t *qd, void *py_obj)
{
    char *hostPattern = py_string_2_c(py_obj);
    char *rc = qd_policy_host_pattern_lookup(qd->policy, hostPattern);
    free(hostPattern);
    return rc;
}

qd_error_t qd_dispatch_prepare(qd_dispatch_t *qd)
{
    qd_log_global_options(qd->timestamp_format, qd->timestamps_in_utc);
    qd->server             = qd_server(qd, qd->thread_count, qd->router_id, qd->sasl_config_path, qd->sasl_config_name);
    qd->router             = qd_router(qd, qd->router_mode, qd->router_area, qd->router_id);
    qd->connection_manager = qd_connection_manager(qd);
    qd->policy             = qd_policy(qd);
    return qd_error_code();
}

void qd_dispatch_set_agent(qd_dispatch_t *qd, void *agent) {
    assert(agent);
    assert(!qd->agent);
    Py_IncRef(agent);
    qd->agent = agent;
}

// Takes ownership of _id
static void qd_dispatch_set_router_id(qd_dispatch_t *qd, char *_id) {
    if (qd->router_id) {
        free(qd->router_id);
    }
    qd->router_id = _id;
}

// Takes ownership of _area
static void qd_dispatch_set_router_area(qd_dispatch_t *qd, char *_area) {
    if (qd->router_area) {
        free(qd->router_area);
    }
    qd->router_area = _area;
}

// Takes ownership of _van_id
static void qd_dispatch_set_router_van_id(qd_dispatch_t *qd, char *_van_id) {
    if (qd->van_id) {
        free(qd->van_id);
    }
    qd->van_id = _van_id;
}

void qd_dispatch_free(qd_dispatch_t *qd)
{
    if (!qd) return;

    /* Stop HTTP threads immediately */
    qd_http_server_free(qd_server_http(qd->server));

    free(qd->sasl_config_path);
    free(qd->sasl_config_name);
    qd_connection_manager_free(qd->connection_manager);
    qd_policy_free(qd->policy);
    Py_XDECREF((PyObject*) qd->agent);
    qd_router_free(qd->router);
    qd_server_free(qd->server);
    qd_tls_finalize();
    qd_log_finalize();
    qd_alloc_finalize();
    qd_python_finalize();
    qd_dispatch_set_router_id(qd, NULL);
    qd_dispatch_set_router_area(qd, NULL);
    qd_dispatch_set_router_van_id(qd, NULL);
    qd_iterator_finalize();
    free(qd->timestamp_format);
    free(qd->metadata);
    sys_atomic_destroy(&global_delivery_id);

    free(qd);
}


QD_EXPORT void qd_dispatch_router_lock(qd_dispatch_t *qd) TA_ACQ(qd->router->lock) { sys_mutex_lock(&qd->router->lock); }
QD_EXPORT void qd_dispatch_router_unlock(qd_dispatch_t *qd) TA_REL(qd->router->lock) { sys_mutex_unlock(&qd->router->lock); }

qdr_core_t* qd_dispatch_router_core(const qd_dispatch_t *qd)
{
    return qd->router->router_core;
}

/**
 * Return a reference to connection_manager
 */
qd_connection_manager_t *qd_dispatch_connection_manager(const qd_dispatch_t *qd) {
    return qd->connection_manager;
}


QD_EXPORT void qd_router_setup_late(qd_dispatch_t *qd)
{
    qd->router->tracemask   = qd_tracemask();
    qd->router->router_core = qdr_core(qd, qd->router->router_mode, qd->router->router_area, qd->router->router_id, qd->router->van_id);
    qd_router_python_setup(qd->router);
    qd_timer_schedule(qd->router->timer, 1000);
}


qdr_core_t *qd_router_core(const qd_dispatch_t *qd)
{
    return qd->router->router_core;
}


qd_policy_t *qd_dispatch_get_policy(const qd_dispatch_t *dispatch)
{
    return qd->policy;
}

uint32_t qd_dispatch_get_data_connection_count(const qd_dispatch_t *dispatch)
{
    return qd->data_connection_count;
}

qd_server_t *qd_dispatch_get_server(const qd_dispatch_t *dispatch)
{
    return qd->server;
}

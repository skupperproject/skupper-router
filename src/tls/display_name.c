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

#include "qpid/dispatch/tls.h"
#include "private.h"

#include "qpid/dispatch/internal/export.h"
#include "qpid/dispatch/python_embedded.h"

/*
 * API for accessing the Python DisplayNameService instance. The DisplayNameService is used to access the contents of
 * the uidNameMappingFile configured in the sslProfile.
 */

static void *py_displayname_obj;  // Reference to the Python DisplayNameService singleton


/**
 * Store address of display name service Python object for C code use.
 * Called by Python during the router configuration file load. The qd_python_lock() is held during this call.
 *
 * @param display_name_service address of python object
 */
QD_EXPORT qd_error_t qd_tls_register_display_name_service(void *display_name_service)
{
    if (display_name_service) {
        py_displayname_obj = display_name_service;
        Py_XINCREF((PyObject *)py_displayname_obj);
        return QD_ERROR_NONE;
    }
    else {
        return qd_error(QD_ERROR_VALUE, "Display Name Service is not set");
    }
}


void tls_private_release_display_name_service(void)
{
    qd_python_lock_state_t ls = qd_python_lock();
    Py_XDECREF((PyObject *)py_displayname_obj);
    qd_python_unlock(ls);
    py_displayname_obj = 0;
}


/**
 * Look up the display name corresponding to user_id in the given sslProfile's uidNameMappingFile
 * @param ssl_profile_name name of the sslProfile record instance
 * @param user_id user identifier used as lookup key
 * @return a null-terminated user name string on success else 0. The caller must free() the user name string when done
 * using it.
 */
char *tls_private_lookup_display_name(const char *ssl_profile_name, const char *user_id)
{
    char *user_name = 0;

    assert(py_displayname_obj);

    // Translate extracted id into display name
    qd_python_lock_state_t lock_state = qd_python_lock();
    PyObject *result = PyObject_CallMethod((PyObject *)py_displayname_obj, "query", "(ss)", ssl_profile_name, user_id );
    if (result) {
        user_name = py_string_2_c(result);
        Py_XDECREF(result);
    }
    qd_python_unlock(lock_state);

    return user_name;
}

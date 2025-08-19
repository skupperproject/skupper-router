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

#include "./qdr_doctest.hpp"
#include "./helpers.hpp"  // must come after ./qdr_doctest.hpp

#include <proton/listener.h>

#include <Python.h>

#include <regex>
#include <thread>

extern "C" {
#include "adaptors/amqp/qd_listener.h"
#include "qpid/dispatch/entity.h"
    qd_listener_t *qd_dispatch_configure_listener(qd_dispatch_t *qd, qd_entity_t *entity);
    void qd_connection_manager_delete_listener(qd_dispatch_t *qd, void *impl);
}


/// GCC 4.8 made a questionable choice to implement std::regex_search to always
/// return false. Meaning that tests cannot use regex on RHEL 7
static bool regex_is_broken() {
    return !std::regex_search("", std::regex(""));
}

void check_amqp_listener_startup_log_message(qd_entity_t *entity, std::string listen, std::string stop)
{
    QDR qdr{};
    CaptureCStream css(stderr);
    qdr.initialize("./minimal_trace.conf");

    qd_listener_t *li = qd_listener_create(qdr.qd, entity);

    CHECK(qd_listener_listen(li));
    pn_listener_close(li->pn_listener);
    {
        /* AMQP socket is opened (and closed) only when proactor loop runs; meaning router has to be started */
        auto timer = qdr.schedule_stop(0);
        qdr.run();
    }

    qd_listener_decref(li);
    qdr.deinitialize();

    std::string logging = css.str();
    CHECK_MESSAGE(std::regex_search(logging, std::regex(listen)),
                  listen, " not found in ", logging);
    CHECK_MESSAGE(std::regex_search(logging, std::regex(stop)),
                  stop, " not found in ", logging);
}

void check_http_listener_startup_log_message(qd_entity_t *entity, std::string listen, std::string stop, std::string failed)
{
    QDR qdr{};
    CaptureCStream css(stderr);
    qdr.initialize("./minimal_trace.conf");

    qd_listener_t *li = qd_listener_create(qdr.qd, entity);

    const bool http_supported = qd_server_http(qdr.qd->server) != nullptr;

    CHECK(qd_listener_listen(li) == http_supported);
    qdr.wait();
    qd_lws_listener_close(li->http);
    qd_listener_decref(li);
    {
        auto timer = qdr.schedule_stop(0);
        qdr.run();
    }

    qdr.deinitialize();

    std::string logging = css.str();
    const std::string unavailable = "HTTP (warning) HTTP support is not available";
    CHECK_MESSAGE((logging.find(unavailable) == std::string::npos) == http_supported,
                  unavailable, " (not) found in ", logging);

    CHECK_MESSAGE(std::regex_search(logging, std::regex(listen)) == http_supported,
                  listen, " (not) found in ", logging);
    CHECK_MESSAGE(std::regex_search(logging, std::regex(stop)) == http_supported,
                  stop, " (not) found in ", logging);

    CHECK_MESSAGE(std::regex_search(logging, std::regex(failed)) != http_supported,
                  failed, " (not) found in ", logging);

}

TEST_CASE("Start AMQP listener with zero port" * doctest::skip(regex_is_broken()))
{
    std::thread([] {
        PyObject *pyObject = PyDict_New();
        PyObject *port = PyUnicode_FromString("0");
        PyObject *host = PyUnicode_FromString("localhost");

        PyDict_SetItemString(pyObject, "port", port);
        PyDict_SetItemString(pyObject, "host", host);

        qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);

        check_amqp_listener_startup_log_message(
            entity,
            R"EOS(SERVER \(info\) Listening on (127.0.0.1)|(::1):(\d\d+))EOS",
            R"EOS(SERVER \(debug\) Listener closed on localhost:0)EOS"
        );

        Py_DECREF(port);
        Py_DECREF(host);
        Py_DECREF(pyObject);
    }).join();
}

TEST_CASE("Start AMQP listener with zero port and a name" * doctest::skip(regex_is_broken()))
{
    std::thread([] {

        PyObject *pyObject = PyDict_New();
        PyObject *name = PyUnicode_FromString("pepa");
        PyObject *port = PyUnicode_FromString("0");
        PyObject *host = PyUnicode_FromString("localhost");

        PyDict_SetItemString(pyObject, "name", name);
        PyDict_SetItemString(pyObject, "port", port);
        PyDict_SetItemString(pyObject, "host", host);

        qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);

        check_amqp_listener_startup_log_message(
            entity,
            R"EOS(SERVER \(info\) Listening on (127.0.0.1)|(::1):(\d\d+) \(pepa\))EOS",
            R"EOS(SERVER \(debug\) Listener closed on localhost:0)EOS"
        );

        Py_DECREF(name);
        Py_DECREF(port);
        Py_DECREF(host);
        Py_DECREF(pyObject);

    }).join();
}

TEST_CASE("Start HTTP listener with zero port" * doctest::skip(regex_is_broken()))
{
    std::thread([] {
        PyObject *pyObject = PyDict_New();
        PyObject *port = PyUnicode_FromString("0");
        PyObject *host = PyUnicode_FromString("localhost");

        PyDict_SetItemString(pyObject, "port", port);
        PyDict_SetItemString(pyObject, "host", host);
        PyDict_SetItemString(pyObject, "http", Py_True);

        qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);

        check_http_listener_startup_log_message(
            entity,
            R"EOS(HTTP \(info\) Listening for HTTP on localhost:(\d\d+))EOS",
            R"EOS(HTTP \(info\) Stopped listening for HTTP on localhost:0)EOS",

            R"EOS(HTTP \(error\) No HTTP support to listen on localhost:0)EOS"
        );

        Py_DECREF(port);
        Py_DECREF(host);
        Py_DECREF(pyObject);

    }).join();
}

TEST_CASE("Start HTTP listener with zero port and a name" * doctest::skip(regex_is_broken()))
{
    std::thread([] {

        PyObject *pyObject = PyDict_New();
        PyObject *name = PyUnicode_FromString("pepa");
        PyObject *port = PyUnicode_FromString("0");
        PyObject *host = PyUnicode_FromString("localhost");

        PyDict_SetItemString(pyObject, "name", port);
        PyDict_SetItemString(pyObject, "port", port);
        PyDict_SetItemString(pyObject, "host", host);
        PyDict_SetItemString(pyObject, "http", Py_True);

        qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);

        check_http_listener_startup_log_message(
            entity,
            R"EOS(HTTP \(info\) Listening for HTTP on localhost:(\d\d+))EOS",
            R"EOS(HTTP \(info\) Stopped listening for HTTP on localhost:0)EOS",

            R"EOS(HTTP \(error\) No HTTP support to listen on localhost:0)EOS"
        );

        Py_DECREF(name);
        Py_DECREF(port);
        Py_DECREF(host);
        Py_DECREF(pyObject);

    }).join();
}

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

#include <Python.h>

#include "./qdr_doctest.hpp"

#include "./helpers.hpp"  // must come after ./qdr_doctest.hpp

#include <map>
#include <thread>
#include <variant>

extern "C" {
#include "adaptors/http_common.h"
}

using entity_value_t = std::variant<std::string, const char *, bool>;

/// Builds a std::unique_ptr<qd_entity_t> instance from a C++ map with std::string-typed keys.
/// Caller must hold the Python GIL (qd_entity_t is a PyObject).
static auto create_entity_from_config_map(std::map<std::string, entity_value_t> const &config)
{
    PyObject *pyObject = PyDict_New();
    for (auto [k, v] : config) {
        PyObject *item = nullptr;
        if (std::holds_alternative<std::string>(v)) {
            item = PyUnicode_FromString(std::get<std::string>(v).c_str());
        } else if (std::holds_alternative<const char *>(v)) {
            // gcc 8.5. 0 (on RHEL 8) prefers to cast const char * to bool and not std::string
            item = PyUnicode_FromString(std::get<const char *>(v));
        } else if (std::holds_alternative<bool>(v)) {
            item = std::get<bool>(v) ? Py_True : Py_False;
            Py_IncRef(item);
        }
        REQUIRE_MESSAGE(item != nullptr, "Test utils cannot handle value of a given type");

        PyDict_SetItemString(pyObject, k.c_str(), item);
        Py_DECREF(item);
    }

    // return a std::unique_ptr with a deleter the runs Py_DECREF when the value goes out of scope
    qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);
    return qd_make_unique(entity, [](qd_entity_t *entity) { Py_DECREF(entity); });
}

TEST_CASE("Configure and start HTTP_ADAPTOR listener")
{
    std::thread([] {
        QDR qdr{};
        qdr.initialize("./minimal_trace.conf");
        qdr.wait();
        qd_dispatch_t *qd = qdr.qd;

        CaptureCStream css(stderr);

        // previous functions dropped Python GIL
        auto state = PyGILState_Ensure();

        SUBCASE("empty config should fail but not crash")
        {
            std::map<std::string, entity_value_t> config = {
                // empty map
            };

            // build entity
            auto entity = create_entity_from_config_map(config);

            // perform test
            CHECK(qd_dispatch_configure_http_listener(qd, entity.get()) == nullptr);

            // check logging
            std::string       logging = css.str();
            const std::string unable  = R"EOS(HTTP_ADAPTOR (error) Unable to load config information)EOS";
            CHECK_MESSAGE(logging.find(unable) != std::string::npos, unable, " not found in ", logging);
        }

        SUBCASE("minimal config should create listener")
        {
            std::map<std::string, entity_value_t> config = {
                {"protocolVersion", "HTTP1"},

                {"name", "my_config"},      //
                {"host", "localhost"},      //
                {"port", "0"},              //
                {"address", "my_address"},  //
            };

            // build entity
            auto entity = create_entity_from_config_map(config);

            // perform test
            qd_http_listener_t *listener = qd_dispatch_configure_http_listener(qd, entity.get());
            REQUIRE(listener != nullptr);

            // clean up
            qd_dispatch_delete_http_listener(qd, listener);

            // check logging
            std::string       logging = css.str();
            const std::string configured =
                R"EOS(HTTP_ADAPTOR (info) Configured HTTP_ADAPTOR listener on localhost:0)EOS";
            CHECK_MESSAGE(logging.find(configured) != std::string::npos, configured, " not found in ", logging);
        }
        PyGILState_Release(state);

        qdr.deinitialize();
    }).join();
}

TEST_CASE("Configure and start HTTP connector")
{
    QDR qdr{};
    qdr.initialize("./minimal_trace.conf");
    qdr.wait();
    qd_dispatch_t *qd = qdr.qd;

    CaptureCStream css(stderr);

    // previous functions dropped Python GIL
    auto state = PyGILState_Ensure();

    SUBCASE("empty config should fail but not crash")
    {
        std::map<std::string, entity_value_t> config = {
            // empty map
        };

        // build entity
        auto entity = create_entity_from_config_map(config);

        // perform test
        CHECK(qd_dispatch_configure_http_connector(qd, entity.get()) == nullptr);

        // check logging
        std::string       logging = css.str();
        const std::string unable  = R"EOS(HTTP_ADAPTOR (error) Unable to load config information)EOS";
        CHECK_MESSAGE(logging.find(unable) != std::string::npos, unable, " not found in ", logging);
    }

    PyGILState_Release(state);

    qdr.deinitialize();
}

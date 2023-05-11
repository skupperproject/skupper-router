#ifndef __dispatch_log_h__
#define __dispatch_log_h__ 1
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
#include <stdarg.h>
#include <stdbool.h>

/**@file
 * Sending debug/administrative log messages.
 */

/** Logging levels */
typedef enum {
    QD_LOG_NONE     =0x00, ///< No logging
    QD_LOG_TRACE    =0x01, ///< High volume messages, o(n) or more for n message transfers.
    QD_LOG_DEBUG    =0x02, ///< Debugging messages useful to developers.
    QD_LOG_INFO     =0x04, ///< Information messages useful to users
    QD_LOG_NOTICE   =0x08, ///< Notice of important but non-error events.
    QD_LOG_WARNING  =0x10, ///< Warning of event that may be a problem.
    QD_LOG_ERROR    =0x20, ///< Error, definitely a problem
    QD_LOG_CRITICAL =0x40, ///< Critical error, data loss or process shut-down.
} qd_log_level_t;

/** Logging modules */
typedef enum {
    QD_LOG_MODULE_ROUTER,
    QD_LOG_MODULE_ROUTER_CORE,
    QD_LOG_MODULE_ROUTER_HELLO,
    QD_LOG_MODULE_ROUTER_LS,
    QD_LOG_MODULE_ROUTER_MA,
    QD_LOG_MODULE_MESSAGE,
    QD_LOG_MODULE_SERVER,
    QD_LOG_MODULE_AGENT,
    QD_LOG_MODULE_CONTAINER,
    QD_LOG_MODULE_ERROR,
    QD_LOG_MODULE_POLICY,
    QD_LOG_MODULE_HTTP,
    QD_LOG_MODULE_CONN_MGR,
    QD_LOG_MODULE_PYTHON,
    QD_LOG_MODULE_PROTOCOL,
    QD_LOG_MODULE_TCP_ADAPTOR,
    QD_LOG_MODULE_HTTP_ADAPTOR,
    QD_LOG_MODULE_FLOW_LOG,
    QD_LOG_MODULE_ADDRESS_WATCH,
    QD_LOG_MODULE_DEFAULT
} qd_log_module_t;

typedef struct qd_log_source_t qd_log_source_t;

/**
 * Get the log module from module name.
 *
 * @param module_name whose corresponding log module is required.
 */
qd_log_module_t get_log_module_from_module_name(char *module_name);

/**
 * Get the log source record from the passed in log module.
 *
 * @param qd_log_module_t module - the log module whose log source is required.
 */
qd_log_source_t *qd_log_source(qd_log_module_t module);

bool qd_log_enabled(qd_log_module_t module, qd_log_level_t level);
/**@internal*/
void qd_log_impl(qd_log_module_t module, qd_log_level_t level, const char *file, int line, const char *fmt, ...)
    __attribute__((format(printf, 5, 6)));

/**
 * Another version of the qd_log_impl function. This function unconditionally writes the the message to the log file.
 * It does not check to see if the passed in log level is enabled.
 */
void qd_log_impl_v1(qd_log_module_t module, qd_log_level_t level, const char *file, int line, const char *fmt, ...)
    __attribute__((format(printf, 5, 6)));
void qd_vlog_impl(qd_log_module_t module, qd_log_level_t level, bool check_level, const char *file, int line,
                  const char *fmt, va_list ap) __attribute__((format(printf, 6, 0)));

/** Log a message
 * Note: does not evaluate the format args unless the log message is enabled.
 * @param module qd_log_module_t module of log message.
 * @param level qd_log_level_t log level of message.
 * @param ... printf style format string and arguments.
 */
#define qd_log(module, level, ...)                                       \
    do {                                                                 \
        if (qd_log_enabled(module, level))                               \
            qd_log_impl(module, level, __FILE__, __LINE__, __VA_ARGS__); \
    } while (0)

/** Log a message, using a va_list.
 * Note: does not evaluate the format args unless the log message is enabled.
 * @param module qd_log_module_t module of log message..
 * @param level qd_log_level_t log level of message.
 * @param ap va_list argument pack.
 */
#define qd_vlog(module, level, fmt, ap)                                     \
    do {                                                                    \
        if (qd_log_enabled(module, level))                                  \
            qd_vlog_impl(module, level, true, __FILE__, __LINE__, fmt, ap); \
    } while (0)

/** Maximum length for a log message */
int qd_log_max_len(void);

void qd_format_string(char *buf, int buf_size, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

#endif

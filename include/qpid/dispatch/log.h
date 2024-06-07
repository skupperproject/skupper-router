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
    QD_LOG_DEBUG    =0x01, ///< High volume messages, o(n) or more for n message transfers.
    QD_LOG_INFO     =0x02, ///< Debugging messages useful to developers.
    QD_LOG_WARNING  =0x04, ///< Recoverable or transient failures, minor or no service loss
    QD_LOG_ERROR    =0x08, ///< Service affecting failure, likely requires intervention
    QD_LOG_CRITICAL =0x10, ///< Catastrophic router failure, service loss, requires intervention
} qd_log_level_t;

/** Logging modules */
typedef enum {
    LOG_ROUTER,
    LOG_ROUTER_CORE,
    LOG_ROUTER_HELLO,
    LOG_ROUTER_LS,
    LOG_ROUTER_MA,
    LOG_MESSAGE,
    LOG_SERVER,
    LOG_AGENT,
    LOG_CONTAINER,
    LOG_ERROR,
    LOG_POLICY,
    LOG_HTTP,
    LOG_CONN_MGR,
    LOG_PYTHON,
    LOG_PROTOCOL,
    LOG_TCP_ADAPTOR,
    LOG_HTTP_ADAPTOR,
    LOG_FLOW_LOG,
    LOG_ADDRESS_WATCH,
    LOG_HTTP1_OBSERVER,
    LOG_DEFAULT
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

/** Maximum length for a log message (including null terminator byte!) */
#define QD_LOG_TEXT_MAX 2048  // note: keep this small to allow stack-based buffers

void qd_format_string(char *buf, int buf_size, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

// used by vanflow to indicate it is ready to accept vanflow events for log messages
void qd_log_enable_events(void);
void qd_log_disable_events(void);

#endif

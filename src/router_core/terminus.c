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

#include "router_core_private.h"
#include "terminus_private.h"

#include <inttypes.h>
#include <strings.h>

ALLOC_DEFINE(qdr_terminus_t);

qdr_terminus_t *qdr_terminus(pn_terminus_t *pn)
{
    qdr_terminus_t *term = new_qdr_terminus_t();
    ZERO(term);

    term->properties   = pn_data(0);
    term->filter       = pn_data(0);
    term->outcomes     = pn_data(0);
    term->capabilities = pn_data(0);

    if (pn) {
        const char *addr = pn_terminus_get_address(pn);
        if (addr && *addr)
            term->address = qdr_field(addr);

        term->durability        = pn_terminus_get_durability(pn);
        term->expiry_policy     = pn_terminus_get_expiry_policy(pn);
        term->timeout           = pn_terminus_get_timeout(pn);
        term->dynamic           = pn_terminus_is_dynamic(pn);
        term->distribution_mode = pn_terminus_get_distribution_mode(pn);

        pn_data_copy(term->properties,   pn_terminus_properties(pn));
        pn_data_copy(term->filter,       pn_terminus_filter(pn));
        pn_data_copy(term->outcomes,     pn_terminus_outcomes(pn));
        pn_data_copy(term->capabilities, pn_terminus_capabilities(pn));
    }

    return term;
}


void qdr_terminus_free(qdr_terminus_t *term)
{
    if (term == 0)
        return;

    qdr_field_free(term->address);
    pn_data_free(term->properties);
    pn_data_free(term->filter);
    pn_data_free(term->outcomes);
    pn_data_free(term->capabilities);

    free_qdr_terminus_t(term);
}

void qdr_terminus_format(qdr_terminus_t *term, char *output, size_t *size)
{
    size_t len = safe_snprintf(output, *size, "{");

    output += len;
    *size  -= len;
    len     = 0;

    do {
        if (term == 0)
            break;

        if (term->dynamic) {
            len = safe_snprintf(output, *size, "(dyn)");
            output += len;
            *size  -= len;
        }

        if (term->address && term->address->iterator) {
            qd_iterator_reset_view(term->address->iterator, ITER_VIEW_ALL);
            len = qd_iterator_ncopy(term->address->iterator, (unsigned char*) output, *size - 1);
            output[len] = 0;
        } else
            len = safe_snprintf(output, *size, "<none>");

        output += len;
        *size  -= len;

        char *text = "";
        switch (term->durability) {
        case PN_NONDURABLE:                              break;
        case PN_CONFIGURATION: text = " dur:config";     break;
        case PN_DELIVERIES:    text = " dur:deliveries"; break;
        }

        len     = safe_snprintf(output, *size, "%s", text);
        output += len;
        *size  -= len;

        switch (term->expiry_policy) {
        case PN_EXPIRE_WITH_LINK:       text = " expire:link";  break;
        case PN_EXPIRE_WITH_SESSION:    text = " expire:sess";  break;
        case PN_EXPIRE_WITH_CONNECTION: text = " expire:conn";  break;
        case PN_EXPIRE_NEVER:           text = "";              break;
        }

        len     = safe_snprintf(output, *size, "%s", text);
        output += len;
        *size  -= len;

        switch (term->distribution_mode) {
        case PN_DIST_MODE_UNSPECIFIED: text = "";             break;
        case PN_DIST_MODE_COPY:        text = " dist:copy";   break;
        case PN_DIST_MODE_MOVE:        text = " dist:move";   break;
        }

        len     = safe_snprintf(output, *size, "%s", text);
        output += len;
        *size  -= len;

        if (term->timeout > 0) {
            len     = safe_snprintf(output, *size, " timeout:%"PRIu32, term->timeout);
            output += len;
            *size  -= len;
        }

        if (term->capabilities && pn_data_size(term->capabilities) > 0) {
            len     = safe_snprintf(output, *size, " caps:");
            output += len;
            *size  -= len;

            len = *size;
            pn_data_format(term->capabilities, output, &len);
            output += len;
            *size  -= len;
        }

        if (term->filter && pn_data_size(term->filter) > 0) {
            len     = safe_snprintf(output, *size, " flt:");
            output += len;
            *size  -= len;

            len = *size;
            pn_data_format(term->filter, output, &len);
            output += len;
            *size  -= len;
        }

        if (term->outcomes && pn_data_size(term->outcomes) > 0) {
            len     = safe_snprintf(output, *size, " outcomes:");
            output += len;
            *size  -= len;

            len = *size;
            pn_data_format(term->outcomes, output, &len);
            output += len;
            *size  -= len;
        }

        if (term->properties && pn_data_size(term->properties) > 0) {
            len     = safe_snprintf(output, *size, " props:");
            output += len;
            *size  -= len;

            len = *size;
            pn_data_format(term->properties, output, &len);
            output += len;
            *size  -= len;
        }

        len = 0;
    } while (false);

    output += len;
    *size  -= len;

    len = safe_snprintf(output, *size, "}");
    *size -= len;
}


void qdr_terminus_copy(qdr_terminus_t *from, pn_terminus_t *to)
{
    if (!from) {
        pn_terminus_set_type(to, PN_UNSPECIFIED);
        return;
    }

    if (from->address) {
        qd_iterator_reset_view(from->address->iterator, ITER_VIEW_ALL);
        unsigned char *addr = qd_iterator_copy(from->address->iterator);
        pn_terminus_set_address(to, (char*) addr);
        free(addr);
    }

    pn_terminus_set_durability(to,        from->durability);
    pn_terminus_set_expiry_policy(to,     from->expiry_policy);
    pn_terminus_set_timeout(to,           from->timeout);
    pn_terminus_set_dynamic(to,           from->dynamic);
    pn_terminus_set_distribution_mode(to, from->distribution_mode);

    pn_data_copy(pn_terminus_properties(to),   from->properties);
    pn_data_copy(pn_terminus_filter(to),       from->filter);
    pn_data_copy(pn_terminus_outcomes(to),     from->outcomes);
    pn_data_copy(pn_terminus_capabilities(to), from->capabilities);
}


void qdr_terminus_add_capability(qdr_terminus_t *term, const char *capability)
{
    pn_data_put_symbol(term->capabilities, pn_bytes(strlen(capability), capability));
}


bool qdr_terminus_has_capability(qdr_terminus_t *term, const char *capability)
{
    size_t     cap_len = strlen(capability);
    pn_data_t *cap     = term->capabilities;

    if (!cap) {
        return false;
    }

    pn_data_rewind(cap);
    pn_data_next(cap);

    if (pn_data_type(cap) == PN_SYMBOL) {
        pn_bytes_t sym = pn_data_get_symbol(cap);
        if (sym.size == cap_len && strncmp(sym.start, capability, cap_len) == 0) {
            return true;
        }
    } else if (pn_data_type(cap) == PN_ARRAY && pn_data_get_array_type(cap) == PN_SYMBOL) {
        size_t count = pn_data_get_array(cap);
        pn_data_enter(cap);
        for (size_t i = 0; i < count; i++) {
            pn_data_next(cap);
            pn_bytes_t sym = pn_data_get_symbol(cap);
            if (sym.size == cap_len && strncmp(sym.start, capability, cap_len) == 0) {
                pn_data_exit(cap);
                return true;
            }
        }
        pn_data_exit(cap);
    }

    return false;
}


bool qdr_terminus_is_anonymous(qdr_terminus_t *term)
{
    return term == 0 || (term->address == 0 && !term->dynamic);
}


void qdr_terminus_set_dynamic(qdr_terminus_t *term)
{
    term->dynamic = true;
}


bool qdr_terminus_is_dynamic(qdr_terminus_t *term)
{
    return term->dynamic;
}


void qdr_terminus_set_address(qdr_terminus_t *term, const char *addr)
{
    qdr_field_free(term->address);
    term->address = qdr_field(addr);
}


void qdr_terminus_set_address_iterator(qdr_terminus_t *term, qd_iterator_t *addr)
{
    qdr_field_t *old = term->address;
    term->address = qdr_field_from_iter(addr);
    qdr_field_free(old);
}


qd_iterator_t *qdr_terminus_get_address(qdr_terminus_t *term)
{
    if (term->address == 0)
        return 0;

    return term->address->iterator;
}

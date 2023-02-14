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

#include "qpid/dispatch/platform.h"

#include "config.h"

#include "qpid/dispatch/ctools.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/resource.h>

static uintmax_t computed_memory_size = 0;

uintmax_t qd_platform_memory_size(void)
{
    if (computed_memory_size > 0) {
        return computed_memory_size;
    }

    bool found = false;
    uintmax_t rlimit = UINTMAX_MAX;

#if QD_HAVE_GETRLIMIT
    {
        // determine if this process has a hard or soft limit set for its total
        // virtual address space
        struct rlimit rl = {0};
        // note rlim_max >= rlim_cur (see man getrlimit) use smallest value
        if (getrlimit(RLIMIT_AS, &rl) == 0) {
            if (rl.rlim_cur != RLIM_INFINITY) {
                rlimit = (uintmax_t)rl.rlim_cur;
                found = true;
            } else if (rl.rlim_max != RLIM_INFINITY) {
                rlimit = (uintmax_t)rl.rlim_max;
                found = true;
            }
        }
    }
#endif // QD_HAVE_GETRLIMIT

    // although a resource limit may be set be sure it does not exceed the
    // available "fast" memory.

    // @TODO(kgiusti) this is linux-specific (see man proc)
    uintmax_t mlimit = UINTMAX_MAX;
    FILE *minfo_fp = fopen("/proc/meminfo", "r");
    if (minfo_fp) {
        size_t buflen = 0;
        char *buffer = 0;
        uintmax_t tmp;
        while (getline(&buffer, &buflen, minfo_fp) != -1) {
            if (sscanf(buffer, "MemTotal: %"SCNuMAX, &tmp) == 1) {
                mlimit = tmp * 1024;  // MemTotal is in KiB
                found = true;
                break;
            }
        }
        free(buffer);  // allocated by getline
        fclose(minfo_fp);
    }

    // and if the router is running within a container check the cgroups memory
    // controller. Hard and soft memory limits can be set.

    uintmax_t climit = UINTMAX_MAX;
    {
        uintmax_t soft = UINTMAX_MAX;
        uintmax_t hard = UINTMAX_MAX;
        bool c_set = false;

        FILE *cg_fp = fopen("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
        if (cg_fp) {
            if (fscanf(cg_fp, "%"SCNuMAX, &hard) == 1) {
                c_set = true;
            }
            fclose(cg_fp);
        }

        cg_fp = fopen("/sys/fs/cgroup/memory/memory.soft_limit_in_bytes", "r");
        if (cg_fp) {
            if (fscanf(cg_fp, "%"SCNuMAX, &soft) == 1) {
                c_set = true;
            }
            fclose(cg_fp);
        }

        if (c_set) {
            climit = MIN(soft, hard);
            found = true;
        }
    }

    if (found) {
        uintmax_t tmp = MIN(mlimit, climit);
        computed_memory_size = MIN(rlimit, tmp);
        return computed_memory_size;
    }

    return 0;
}


double normalize_memory_size(const uint64_t bytes, const char **suffix)
{
    static const char * const units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    const int units_ct = 5;
    const double base = 1024.0;

    double value = (double)bytes;
    for (int i = 0; i < units_ct; ++i) {
        if (value < base) {
            if (suffix)
                *suffix = units[i];
            return value;
        }
        value /= base;
    }
    if (suffix)
        *suffix = units[units_ct - 1];
    return value;
}


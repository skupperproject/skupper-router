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
#if QD_HAVE_GETRLIMIT
#include <sys/resource.h>
#endif

static uintmax_t computed_memory_size = 0;

// Return the total amount of RAM memory available for use by the router.
//
// The heuristic involves detecting the amount of physical memory on the platform then checking for any other memory
// limits that may be placed on the process.
//
uintmax_t qd_platform_memory_size(void)
{
    if (computed_memory_size > 0) {
        return computed_memory_size;
    }

    bool found = false;
    uintmax_t mlimit = UINTMAX_MAX;  // physical memory limit
    uintmax_t rlimit = UINTMAX_MAX;  // resource limit (rlimit)
    uintmax_t climit = UINTMAX_MAX;  // cgroups max memory limit

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

    // Check the cgroups memory controller.

    {
        uintmax_t max = 0;

        // There are two versions of cgroups: v1 and v2. Check for v2 first

        FILE *cg_fp = fopen("/sys/fs/cgroup/memory.max", "r");
        if (cg_fp) {
            // memory.max may be set to the string "max", which means no limit has been set. "max" will cause fscanf() to
            // return 0 and we'll ignore the setting
            if (fscanf(cg_fp, "%"SCNuMAX, &max) == 1 && max != 0) {
                climit = max;
                found = true;
            }
            fclose(cg_fp);

        } else {  // check for v1 cgroups configuration

            // v1 allows both soft and hard limits

            FILE *cg_fp = fopen("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
            if (cg_fp) {
                if (fscanf(cg_fp, "%"SCNuMAX, &max) == 1 && max != 0) {
                    climit = max;
                    found = true;
                }
                fclose(cg_fp);
            }

            cg_fp = fopen("/sys/fs/cgroup/memory/memory.soft_limit_in_bytes", "r");
            if (cg_fp) {
                if (fscanf(cg_fp, "%"SCNuMAX, &max) == 1 && max != 0) {
                    climit = MIN(climit, max);
                    found = true;
                }
                fclose(cg_fp);
            }
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

// Parse the /proc/self/status file for the line matching sscanf_pattern and return the corresponding metric.
// Linux-specific. Thread safe.
//
static uint64_t _parse_proc_memory_metric(const char *sscanf_pattern)
{
    FILE *fp = fopen("/proc/self/status", "r");
    if (!fp) {
        // possible - if not on linux
        return 0;
    }

    // the format of the /proc/self/status file is documented in the linux man
    // pages (man proc)

    size_t   buflen = 0;
    char    *buffer = 0;
    uint64_t metric = 0;

    while (getline(&buffer, &buflen, fp) != -1) {
        if (sscanf(buffer, sscanf_pattern, &metric) == 1) {
            break;  // matched
        }
    }
    free(buffer);
    fclose(fp);

    return metric;
}

uint64_t qd_router_virtual_memory_usage(void)
{
    // VmSize is in kB
    return _parse_proc_memory_metric("VmSize: %" SCNu64) * 1024;
}

uint64_t qd_router_rss_memory_usage(void)
{
    // VmRSS is in kB
    return _parse_proc_memory_metric("VmRSS: %" SCNu64) * 1024;
}

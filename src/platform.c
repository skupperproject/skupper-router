
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
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#if QD_HAVE_GETRLIMIT
#include <sys/resource.h>
#endif

static uintmax_t computed_memory_size = 0;

/*
 * Walking up the cgroup tree to find memory limit.
 *
 * Cgroups are stored as a tree. To find out the most constraining
 * memory limit, you start at the cgroup node for that process
 ^ and then walk up the tree, parent to parent, until you reach
 * the root, remembering the smallest limit along the way.
 * 
 * Docker, Podman, and Kubernetes all store the cgroups and their limits 
 * as a tree like this:
 *     /sys/fs/cgroup/
 *     └── user.slice/
 *         └── user-1000.slice/
 *             └── user@1000.service/
 *                 └── app.slice/
 *                     └── run-r84a00efcca804a60af4b0afcbac230b3.scope/   ← our process lives here
 *                         └── memory.max   ← the 1 GiB limit for my test is here
 *
 * So, we:
 *   1. Read /proc/self/cgroup to obtain the relative path of the current
 *      process’s cgroup for the memory controller.
 *   2. Start at that path under /sys/fs/cgroup and walk upward through
 *      all parent directories.
 *   3. At every level we check for a memory limit:
 *        - cgroup v2: memory.max
 *        - cgroup v1: memory.limit_in_bytes (and memory.soft_limit_in_bytes)
 *   4. We take the most restrictive limit we find.
 *
 *
 * If no limit is discovered anywhere in the hierarchy we fall back 
 * to top-level cgroup checks, finally to /proc/meminfo.
 */


/*
 * Parse /proc/self/cgroup and return the relative cgroup path for the
 * current process (for the memory controller).
 * Works for both cgroup v2 unified hierarchy ("0::/path") and v1.
 * Caller must free() the returned string.
 */
static char *get_process_cgroup_path(void)
{
    FILE *f = fopen("/proc/self/cgroup", "r");
    if (!f) return NULL;

    char *line = NULL;
    size_t len = 0;
    char *result = NULL;

    while (getline(&line, &len, f) != -1) {
        // cgroup v2 unified hierarchy: "0::/user.slice/.../run-xxx.scope"
        if (strncmp(line, "0::", 3) == 0) {
            char *p = line + 3;
            char *nl = strchr(p, '\n');
            if (nl) *nl = '\0';
            result = strdup(p);
            break;
        }

        // cgroup v1 memory controller: "memory:/path" or "8:memory:/path"
        char ctrl[64], path[PATH_MAX];
        if (sscanf(line, "%*d:%[^:]:%s", ctrl, path) == 2) {
            if (strstr(ctrl, "memory") != NULL) {
                char *nl = strchr(path, '\n');
                if (nl) *nl = '\0';
                result = strdup(path);
                break;
            }
        }
    }

    free(line);
    fclose(f);
    return result;
}

/*
 * Try to read a memory limit from a specific cgroup directory.
 * Checks v2 (memory.max) and v1 (memory.limit_in_bytes + soft) in that order.
 * Returns true and sets *out if a valid numeric limit was found.
 */
static bool read_cgroup_memory_limit(const char *dir, uintmax_t *out)
{
    char path[PATH_MAX];
    uintmax_t val = 0;
    FILE *f;

    // cgroup v2
    snprintf(path, sizeof(path), "%s/memory.max", dir);
    f = fopen(path, "r");
    if (f) {
        char buf[64] = {0};
        if (fgets(buf, sizeof(buf), f) && strncmp(buf, "max", 3) != 0) {
            if (sscanf(buf, "%" SCNuMAX, &val) == 1 && val > 0) {
                *out = val;
                fclose(f);
                return true;
            }
        }
        fclose(f);
    }

    // cgroup v1 hard limit
    snprintf(path, sizeof(path), "%s/memory.limit_in_bytes", dir);
    f = fopen(path, "r");
    if (f) {
        if (fscanf(f, "%" SCNuMAX, &val) == 1 && val > 0) {
            *out = val;
            fclose(f);
            return true;
        }
        fclose(f);
    }

    // cgroup v1 soft limit (use if tighter)
    snprintf(path, sizeof(path), "%s/memory.soft_limit_in_bytes", dir);
    f = fopen(path, "r");
    if (f) {
        if (fscanf(f, "%" SCNuMAX, &val) == 1 && val > 0) {
            *out = val;
            fclose(f);
            return true;
        }
        fclose(f);
    }

    return false;
}

/*
 * Walk the cgroup hierarchy starting from the process's own cgroup
 * and return the most restrictive (smallest) memory limit found.
 * Returns UINTMAX_MAX if no limit was found anywhere in the tree.
 */
static uintmax_t find_effective_cgroup_memory_limit(void)
{
    char *rel_path = get_process_cgroup_path();
    if (!rel_path) return UINTMAX_MAX;

    uintmax_t best_limit = UINTMAX_MAX;
    char current_dir[PATH_MAX];

    // Start at the process's own cgroup directory
    snprintf(current_dir, sizeof(current_dir), "/sys/fs/cgroup%s", rel_path);

    // Walk upward through all parent cgroups
    while (strlen(current_dir) > strlen("/sys/fs/cgroup")) {
        uintmax_t limit = 0;
        if (read_cgroup_memory_limit(current_dir, &limit)) {
            if (limit < best_limit) {
                best_limit = limit;
            }
        }

        // Move to parent directory
        char *last_slash = strrchr(current_dir, '/');
        if (!last_slash || last_slash == current_dir) break;
        *last_slash = '\0';

        // Stop if we've reached the root cgroup
        if (strcmp(current_dir, "/sys/fs/cgroup") == 0) break;
    }

    free(rel_path);
    return best_limit;
}

// Return the total amount of RAM memory available for use by the router.
//
// Detect the amount of physical memory on the platform then checking for 
// any other memory limits that may be placed on the process.
//
uintmax_t qd_platform_memory_size(void)
{
    if (computed_memory_size > 0) {
        return computed_memory_size;
    }

    bool found = false;
    uintmax_t mlimit = UINTMAX_MAX;  // physical memory limit from /proc/meminfo
    uintmax_t rlimit = UINTMAX_MAX;  // from getrlimit(RLIMIT_AS)
    uintmax_t climit = UINTMAX_MAX;  // effective cgroup limit (now walks hierarchy)

#if QD_HAVE_GETRLIMIT
    {
        struct rlimit rl = {0};
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

    // Read MemTotal from /proc/meminfo (Linux-specific)
    FILE *minfo_fp = fopen("/proc/meminfo", "r");
    if (minfo_fp) {
        size_t buflen = 0;
        char *buffer = NULL;
        uintmax_t tmp;
        while (getline(&buffer, &buflen, minfo_fp) != -1) {
            if (sscanf(buffer, "MemTotal: %" SCNuMAX, &tmp) == 1) {
                mlimit = tmp * 1024;  // KiB → bytes
                found = true;
                break;
            }
        }
        free(buffer);
        fclose(minfo_fp);
    }

    // === NEW: Check cgroup memory limit by walking the actual hierarchy ===
    // This correctly handles systemd --user --scope, containers, Kubernetes, etc.
    climit = find_effective_cgroup_memory_limit();
    if (climit != UINTMAX_MAX) {
        found = true;
    } else {
        // Fallback: original root-only cgroup checks (for very old systems)
        uintmax_t max = 0;
        FILE *cg_fp = fopen("/sys/fs/cgroup/memory.max", "r");
        if (cg_fp) {
            if (fscanf(cg_fp, "%" SCNuMAX, &max) == 1 && max != 0) {
                climit = max;
                found = true;
            }
            fclose(cg_fp);
        } else {
            // v1 root
            cg_fp = fopen("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
            if (cg_fp) {
                if (fscanf(cg_fp, "%" SCNuMAX, &max) == 1 && max != 0) {
                    climit = max;
                    found = true;
                }
                fclose(cg_fp);
            }
            cg_fp = fopen("/sys/fs/cgroup/memory/memory.soft_limit_in_bytes", "r");
            if (cg_fp) {
                if (fscanf(cg_fp, "%" SCNuMAX, &max) == 1 && max != 0) {
                    if (max < climit) climit = max;
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

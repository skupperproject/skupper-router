#include "../../src/server.c"

#include <stdint.h>

__attribute__((visibility("default"))) double testonly_normalize_memory_size(const uint64_t bytes, const char **suffix)
{
    return normalize_memory_size(bytes, suffix);
}

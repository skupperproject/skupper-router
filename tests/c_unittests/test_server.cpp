#include "cpp_stub.h"
#include "qdr_doctest.hpp"
// helpers.hpp goes after qdr_doctest.hpp
#include "helpers.hpp"

#include <stdint.h>

extern "C" {
double testonly_normalize_memory_size(const uint64_t bytes, const char **suffix);
}

static void test_normalize_memory_size(uint64_t bytes, double expected_value, const char *expected_suffix)
{
    const char *suffix = NULL;
    double value       = testonly_normalize_memory_size(bytes, &suffix);
    CHECK(value == expected_value);
    CHECK(suffix == expected_suffix);
}

TEST_CASE("normalize_memory_size")
{
    test_normalize_memory_size(0, 0.0, "B");
    test_normalize_memory_size(1023, 1023.0, "B");
    test_normalize_memory_size(1024, 1.0, "KiB");
    test_normalize_memory_size(1024 + 1024 / 2, 1.5, "KiB");
    test_normalize_memory_size(1024 * 1024, 1.0, "MiB");
    test_normalize_memory_size(1024 * 1024 * 1024, 1.0, "GiB");
    test_normalize_memory_size(1024ul * 1024 * 1024 * 1024, 1.0, "TiB");
    test_normalize_memory_size(UINT64_MAX, 16384.0, "TiB");
}

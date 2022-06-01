# 3. Integrate with the jemalloc allocator

Date: 2022-03-12

## Status

Proposed

## Context

Memory pools are an important technique to improve application performance.
The router already uses a custom memory pool (`src/alloc_pool.c`).
Proton currently does not use any such mechanism.

The `jemalloc` library is a thread-safe pooled memory allocator, able to seamlessly replace malloc/free calls with a more optimized, tunable, and observable implementation.
It turns out that linking the router with `jemalloc` shared library (without any additional tuning) is sufficient to noticeably improve the router's performance.
This improvement is larger than what is provided by linking either `tcmalloc` or `mimalloc`, two other similar well-known libraries.

Integrating `jemalloc` can be accomplished by setting `LD_PRELOAD=/usr/lib64/libjemalloc.so.2`.
However, this procedure is hard to discover, easy to get wrong (e.g. make a typo), and in general not user-friendly.

## Decision

Router shall offer a CMake configuration option `-DUSE_JEMALLOC=ON` to link with `jemalloc` as part of the regular build.
Interested users are to be encouraged to experiment with this option, and to try various `jemalloc` tuning parameters, which are plentiful.

## Consequences

This change should make the router instantaneously slightly more performant.

In the future, tighter integration with the library can be considered, (refer to `man 3 jemalloc` for description of its additional nonstandard allocation API).
Thorough performance evaluation should be performed beforehand, to establish whether careful tuning can unseat `jemalloc` as the fastest allocation library choice among the considered alternatives (`tcmalloc` and `mimalloc`.

The router could possibly get out of the business of doing its own memory pooling, or implement special additional pooling strategies only for the important types (such as `qd_message_t` and `qd_buffer_t`).
This would require reimplementing the weak pointers so that they do not rely on the current properties of the router memory pool.

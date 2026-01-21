# 0004. Use static Clang Thread Safety Analysis annotations

Date: 2024-01-10

## Status

Accepted

## Context

Thread Safety Analysis [1] is an extension to C/C++'s type system to annotate functions with clang-specific attributes to express their requirements regarding thread safety.
Specifically, these annotations capture the requirements on locks held and contexts the function can be safely called from.

Any checking happens statically, when compiling with clang and passing the `-Wthread-safety` parameter.
Violations are reported as compiler warnings, which then fails the router's build as we enable the warnings-as-errors option by default.
IDE's that use `clangd` language server benefit automatically from these warnings.

This differs from various dynamic approaches, either Thread Sanitizer [2] (already in use on GCC and clang) or lock priorities [3] (proposed, not implemented), or thread context checking (implemented in [4]).
the static approach proposed here is less expressive, so some requirements cannot be annotated, but the constrains that can be expressed can be checked quickly and reliably in the compiler, or in the IDE (if based on clangd).

The way it works is that functions are annotated with what locks they require to be held (`TA_REQ(lock)`), and the compiler then checks these conditions can be proven to be satisfied at the places the function is called.

Some additional features of the clang solution, namely, the `TA_GUARDED()` annotation for struct members, are available only in C++ [5].
Another limitation is that C does not have the `this` pointer and classes with methods, so it is more likely that the lock that needs to be held will not be in scope for the function called.
In that case (when lock that needs to be held is not accessible (from function parameters, or as global variable) it is impossible to annotate the lock.

* [1] https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
* [2] https://clang.llvm.org/docs/ThreadSanitizer.html
* [3] assign each lock a number, when acquiring a lock in debug mode, check that its number is not lesser than the number of the highest numbered lock so far on this thread (of execution, not necessarily OS thread)
* [4] [https://github.com/skupperproject/skupper-router/issues/1015](Runtime Debug tests to validate proper thread execution #1015)
* [5] https://github.com/llvm/llvm-project/issues/20777

## Decision

1. Annotate all functions that acquire, release, or require locks with the Clang Thread Safety Analysis annotations.
2. Run a CI job that checks these annotations.

## Consequences

Header file defining the annotations needs to be included.
There are multiple possibilities, either the Clang one (`mutex.h` given in feature documentation), or the Zircon one (`thread_annotations.h` in the Fuchsia project).
They are functionally equivalent, differing only in the particular license and how they name the helper macros.

## Disadvantages and limitations

Annotation checking (the compiler warning option) is only implemented in clang.
GCC implementation has been pursued first but later abandoned in favor of implementing this in clang.

The lock order checking is not implemented (not even in clang).
The respective annotations are currently being ignored.

Python code is not visible to clang, so locks cannot be tracked through the C/Python boundary.

C support is limited and some features (like `TA_GUARDED` can only be used in C++). 

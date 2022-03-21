# XXXX. Use static Thread Safety Analysis annotations

Date: 2022-03-21

## Status

Proposed

## Context

Some mistakes in lock usage can be detected statically.
As opposed to dynamic approaches, either TSan (already in use) or lock priorities (proposed in [2], not implemented), the static approach proposed now can be checked reliably in the compiler, or in the IDE (if based on clangd).

The way it works is that functions are annotated with what locks they require to be held, and the compiler then checks these conditions are satisfied.

Some additional features of the Clang solution, namely, the TA_GUARDED() annotation for struct members, are available only in C++ [3]; meaning these features may not be used in skupper-router. 

[1] https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
[2] 

## Decision

Annotate all functions that acquire, release, or require locks with the Clang Thread Safety Analysis annotations.
Run a CI job that will check these annotations.

## Consequences

Header file defining the annotations needs to be included.
There are multiple possibilities, either the Clang one(mutex.h), or the Zircon one (thread_annotations.h).
They are functionally equivalent, differing only in license and how they name the helper macros.

Annotation checking (-W) is only implemented in Clang, the GCC implementation was abandoned.

The lock order checking () is not implemented (not even in Clang)



Disadvantages and limitations

Python code is not visible to Clang

limited C support

requires clang

? can also annotate for coverity ?

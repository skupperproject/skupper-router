# 3. Move per message router annotations

Date: 2022-03-16

## Status

Accepted

## Context

The skupper-router includes routing and state meta-data in each message transferred between interior routers. This meta-data is merged into the message annotations section. This implementation is both expensive and complicated. In the interest of removing complexity from the router and increasing inter-router throughput, it has been decided to move this meta-data into a custom message section that is prepended to the message.

## Decision

The per-message router meta-data will no longer appear in the message annotations section. It will be moved to a new custom message section as described in docs/notes/router-annotations.adoc.

## Consequences

This is a breaking change: routers that have not been updated to the new implementation will not route messages properly. In addition any existing tests that need to access the per-message meta-data will need to use the extended Proton message class InterRouterMessage defined in tests/interrouter_msg.py

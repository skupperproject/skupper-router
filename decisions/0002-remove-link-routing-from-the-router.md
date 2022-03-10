# 2. Remove Link-Routing from the router

Date: 2022-02-11

## Status

Accepted

## Context

In the interest of removing complexity from the router and in removing an entire dimension of testing, it has been decided that link-routing shall be removed from the router code base.

## Decision

Link routing, including the transaction coordinator, shall be removed from the router.

## Consequences

The _interesting_ capabilities of link routing can be replicated using streamed messages and intermediate delivery dispositions.

Link routing is not needed for Skupper.

This also has the effect of removing the asynchronous address lookup in edge routers that was needed to determine if an address was a link-routed address.  This will greatly improve the latency experienced in local traffic in an edge cloud.

# ADR 0002: Own buffering pipeline instead of Serilog's BatchingSink

- **Status:** Accepted (implemented)
- **Date:** 2026-11-07

## Context

The main goal is reliable delivery and automatic resume across broker outages of any length.
Serilog 4's built-in `BatchingSink` (`IBatchedLogEventSink` + `BatchingOptions`):

- drops a batch after `RetryTimeLimit` and, after 10 consecutive dropped batches, drains the whole queue on every failure;
- drops new events **silently** when the queue is full;
- retries the whole batch, re-sending messages already accepted by the broker;
- retries on a blind exponential schedule (up to 1 min) instead of reacting to connection readiness.

## Decision

The sink implements its own pipeline: a bounded in-memory `EventBuffer` (channel) plus a single
`PublishLoop` that batches, publishes concurrently, retries only not-yet-accepted messages, and waits on
connection readiness. Transient failures are retried without a time limit; events leave the pipeline only
when accepted by the broker or reported to `ILoggingFailureListener` (`Permanent`/`Final`).

Memory is bounded by `QueueLimit`; overflow follows a configurable policy and is always reported.

The buffer is **in memory only — by design, not as a v1 shortcut**. The sink targets containerised /
cluster deployments (Kubernetes etc.) where pods usually have no persistent or writable local disk, so a
disk buffer is not planned. Events still in memory when the process dies are lost; graceful shutdown
flushes with a timeout and hands the rest to the failure listener (fallback sink, e.g. console/stdout
collected by the cluster's log agent).

## Consequences

- More code to own and test (batching, back-pressure, shutdown) — covered by unit tests with a fake transport.
- `AGENTS.md` rule "use Serilog built-in batching" is replaced by this ADR.
- Users get no-silent-loss semantics and can add `WriteTo.FallbackChain(...)` with a file sink.
- No disk I/O: works on read-only root file systems and ephemeral pods.
- Durability across process restarts is not provided by the sink; resilience comes from the in-memory
  buffer (outages up to `QueueLimit` events) plus a replicated broker (quorum queues / streams).

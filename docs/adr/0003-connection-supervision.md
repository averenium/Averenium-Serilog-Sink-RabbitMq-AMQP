# ADR 0003: Connection supervision on top of the client's automatic recovery

- **Status:** Accepted (implemented; verified by integration tests)
- **Date:** 2026-11-07

## Context

`RabbitMQ.AMQP.Client` 1.4.x recovers an open connection that drops and reconnects through the configured
`Uris` (selector asked on every attempt). Gaps found in its source:

1. No retries when the **initial** connection fails (broker down at start-up).
2. Default back-off gives up after 12 attempts → connection permanently `Closed`.
3. Publishers are re-attached with 2 attempts only; failures are just traced. The connection reports
   `Open` before publishers are back.
4. `PublishAsync` ignores cancellation; outcome can be lost when the link breaks, leaving the task pending.

## Decision

- Keep client recovery **enabled** (fast path for short failures and node failover) with an endless,
  capped, jittered back-off policy and a round-robin `IUriSelector` over all configured nodes.
- Add a `ConnectionSupervisor` that owns environment → connection → publisher, tracks their
  `ChangeState` events, and **rebuilds** them from scratch when the client gives up or a publisher is stuck
  (`Closed`, or not `Open` for longer than a grace period while the connection is `Open`).
- Readiness for publishing = connection `Open` **and** publisher `Open`.
- Every publish is bounded by the sink's own `PublishTimeout` via `Task.WaitAsync`; timeouts are treated
  as unknown outcome → retry (at-least-once) and as a health signal for the supervisor.
- Connecting is lazy and in the background; logger creation never blocks or throws because the broker is down.

## Verification (RabbitMQ 4 in containers, `ConnectionResilienceTests`)

| Scenario | Observed |
|---|---|
| Broker down at start-up | supervisor retries (`Faulted` ↔ `Connecting`), Ready once the broker starts |
| Broker restart | client recovery → Ready again; quorum queue keeps earlier messages |
| Connections force-closed (`rabbitmqctl close_all_connections`) | client recovery → Ready |
| Two nodes, current node stopped | client recovery picks the next node (round-robin), Ready in ~1 s |
| Target exchange deleted | link stays attached, messages `Rejected` `amqp:not-found` → `ReportTargetMissing` → rebuild; attach fails (`Faulted`) until the exchange is declared again |

## Consequences

- Behaviour under failures is deterministic and testable through an internal transport abstraction.
- At-least-once delivery: duplicates possible after timeouts/link loss; each message carries a unique `message-id`.
- When the client fixes these gaps upstream, the supervisor's rebuild path simply triggers less often.

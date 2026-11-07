# Architecture

> Status: **Implemented** — agreed goals, design derived from the actual behaviour of
> `RabbitMQ.AMQP.Client` 1.4.x and `Serilog` 4.4 (source reviewed 2026-11-07).

## Goals (in priority order)

1. **Reliable delivery.** A log event is either confirmed by the broker (`Accepted`) or explicitly reported as
   dropped to a failure listener (so it can go to a fallback sink). No silent loss.
2. **Resume after broker failures and failovers.** Node crash, restart, rolling upgrade, network partition,
   broker down at application start-up — the sink keeps buffering and resumes publishing automatically,
   for as long as the outage lasts.
3. **Multi-node clusters.** The sink is configured with several node endpoints and moves to a healthy node
   when the current one goes away.
4. **Never harm the host application.** Logging calls never block on the network and never throw;
   memory use is bounded.

Delivery semantics: **at-least-once**. Duplicates are possible after a connection loss with in-flight
messages (the broker may have accepted a message whose confirmation was lost). Every message carries a
unique `message-id` so consumers can de-duplicate.

## Facts about the underlying libraries that drive the design

### RabbitMQ.AMQP.Client 1.4.x

| # | Behaviour | Consequence for the sink |
|---|---|---|
| C1 | Multi-node: `ConnectionSettingsBuilder.Uris(...)` + `IUriSelector` (default `RandomUriSelector`). The selector is asked **on every (re)connect attempt**. | Failover between nodes is free; we supply a round-robin selector so every node is tried in turn. |
| C2 | Automatic recovery runs only when an **already open** connection drops unexpectedly. | A broker that is down at start-up is *not* retried by the client → the sink must own initial connect retries. |
| C3 | Default `BackOffDelayPolicy` gives up after **12 attempts** (≈1–2 min) and moves the connection to `Closed` forever. | Use our own never-ending back-off policy, and still be ready to rebuild the connection from scratch. |
| C4 | After connection recovery, publishers are re-attached with only **2 attempts**; failures are only traced. Connection state becomes `Open` **before** publishers are re-attached. | Readiness = connection `Open` **and** publisher `Open`. A publisher stuck in `Closed` must be rebuilt by the sink. |
| C5 | `PublishAsync` throws `AmqpNotOpenException` while the publisher is `Reconnecting`/`Closing`/`Closed`. | The sink waits for readiness instead of publishing blindly. |
| C6 | `PublishAsync` **ignores its `CancellationToken`**; `PublisherBuilder.PublishTimeout` is unused. An unknown outcome type throws inside the callback, leaving the task incomplete. | Every publish is bounded by our own timeout (`Task.WaitAsync`); a timeout is treated as "unknown → retry". |
| C7 | Outcomes: `Accepted`, `Released`, `Rejected`. RabbitMQ 4 returns `Released` for an unroutable message (no bound queue). **When the target exchange is deleted the link stays attached** (publisher `Open`) and every message is `Rejected` with `amqp:not-found` (verified by integration test). | `Released` → limited retries. `Rejected`+`amqp:not-found` → keep message, rebuild publisher (`TargetMissing`). Other `Rejected` → permanent. |
| C8 | Messages are durable by default; publishes can be pipelined (many in flight on one link). | Publish a whole batch concurrently and await outcomes together (throughput). |
| C9 | When the broker force-closes the connection, in-flight publishes complete as `Rejected` with `amqp:internal-error` "Connection forced" (client-side settlement, not a broker decision about the message; verified by integration test). | Connection/session/link error codes (`amqp:internal-error`, `amqp:connection:*`, `amqp:session:*`, `amqp:link:detach-forced`, `amqp:link:stolen`) → `ConnectionLost`: keep and retry without limit. `amqp:resource-limit-exceeded` → limited retries like `Released`. |

### Serilog 4.4

| # | Behaviour | Consequence |
|---|---|---|
| S1 | Built-in `BatchingSink` drops a batch after `RetryTimeLimit` (default 10 min) and, after 10 consecutive dropped batches, **drains the whole queue** on every failure. | Incompatible with goal 2 for long outages. |
| S2 | `BatchingSink` drops new events **silently** when its queue is full. | Incompatible with goal 1. |
| S3 | `BatchingSink` retries the **whole** batch, so partially published batches are re-sent. | Extra duplicates. |
| S4 | `ILoggingFailureListener` / `ISetLoggingFailureListener` + `WriteTo.FallbackChain(...)`: events reported as `Permanent`/`Final` are written to the fallback sink. | Our drop path reports through this API → users can fall back to a file sink. |

Conclusion: the sink implements **its own buffering pipeline** (ADR 0002) and **its own connection
supervision on top of the client's recovery** (ADR 0003).

## Component overview

```
 app threads                                   single background worker
 ───────────                                   ─────────────────────────
 Log.Information(...)                           ┌───────────────────────────┐
        │ LogEvent                              │ PublishLoop               │
        ▼                                       │  1. take retry list +     │
 RabbitMqAmqpSink.Emit ──► EventBuffer ────────►│     new events (≤ batch)  │
   (never blocks,         bounded channel,      │  2. format → IMessage     │
    never throws)         overflow policy       │  3. await Ready           │
                          │ overflow            │  4. publish concurrently, │
                          ▼                     │     per-message timeout   │
                 failure listener (Permanent)   │  5. classify outcomes     │
                          │                     └──────┬──────────▲─────────┘
                          ▼                            │ publisher │ Ready/NotReady
                  FallbackChain sink (optional)  ┌─────▼──────────┴─────────┐
                                                 │ ConnectionSupervisor     │
                                                 │ IEnvironment             │
                                                 │  └ IConnection (Uris[])  │
                                                 │     └ IPublisher         │
                                                 └──────────┬───────────────┘
                                                            │ AMQP 1.0
                                              RabbitMQ node A | node B | node C
```

### `RabbitMqAmqpSink` (public entry, internal type)
- Implements `ILogEventSink`, `IDisposable`, `IAsyncDisposable`, `ISetLoggingFailureListener`.
- `Emit` only writes to `EventBuffer`. No I/O, no locks on the hot path beyond the channel.

### `EventBuffer`
- `Channel<LogEvent>` bounded by `QueueLimit` (default 100 000 events).
- Overflow policy (`DropNewest` | `DropOldest`), every dropped event is counted and reported to the
  failure listener as `Permanent` (in small aggregated chunks, so a flood does not explode the listener).

### `PublishLoop`
- Single long-running task; takes up to `BatchSizeLimit` messages per round and publishes them concurrently.
- Keeps a **retry list** of messages not yet accepted; they are sent before new events, so ordering is
  preserved except across retries.
- An event is formatted once; its `IMessage` (with a stable `message-id`) is reused for retries.
- Waits on `ConnectionSupervisor.WaitReadyAsync` instead of blind back-off; while not ready, events
  accumulate in `EventBuffer` (bounded).
- Outcome handling per message:

| Result | Classification | Action |
|---|---|---|
| `Accepted` | success | done |
| `Released` | transient / maybe unroutable | retry up to `MaxReleasedRetries` (default 5), then drop + report `Permanent` |
| `Rejected` with `amqp:not-found` | target missing (exchange/queue deleted) | keep message, `ReportTargetMissing` → supervisor rebuilds (re-declares topology if enabled, otherwise stays not-Ready until the target exists) |
| `Rejected` with a connection/session/link error (C9) | connection lost | keep, retry without limit, signal supervisor |
| `Rejected` with `amqp:resource-limit-exceeded` | transient | like `Released` |
| `Rejected` (other: message size, permissions, invalid fields) | permanent | drop + report `Permanent` (with broker error) |
| `AmqpNotOpenException`, `PublisherException`, connection errors | transient | mark supervisor suspicious, wait ready, retry — **no limit** |
| publish timeout (`PublishTimeout`, default 30 s) | unknown | retry (possible duplicate), signal supervisor |

### `ConnectionSupervisor`
- Owns one `IEnvironment`, one `IConnection` (with all configured `Uris`), one `IPublisher`.
- Connection settings: our `RoundRobinUriSelector`, client recovery **on**, our
  `EndlessBackOffDelayPolicy` (exponential with jitter, capped at `MaxReconnectDelay`, never inactive).
- State machine:

```
          start / rebuild
 ┌──────────────► Connecting ──ok──► Ready ◄───────────── client recovered + publisher Open
 │                    │                │
 │                 failure          connection/publisher Reconnecting
 │                    ▼                ▼
 └── backoff ◄── Faulted ◄── Closed / stuck ── Recovering (client-driven)
```

- Rebuild (dispose publisher → connection, create new ones) when: initial connect fails (C2); connection
  ends in `Closed` with an error (C3); publisher ends `Closed` or stays not-`Open` while the connection is
  `Open` for longer than `PublisherRecoveryGrace` (C4); publish timeouts repeat.
- Exposes `WaitReadyAsync(ct)` (completes when Ready) and the current state for diagnostics.
- Start-up is **lazy and non-blocking**: the logger is usable immediately; connection is established in
  the background.

### Shutdown
`Dispose`/`DisposeAsync`: stop accepting events → drain buffer and retry list until empty or
`ShutdownTimeout` (default 10 s) → report the remainder as `Final` (goes to fallback sink) → close
publisher → connection → environment.

## Multi-node guidance (documentation for users)
- Configure **all** cluster nodes (or a load balancer) in `Uris`.
- Publish to an exchange bound to a **quorum queue** (or a stream) so that stored log messages survive the
  loss of a node; classic queues live on one node.
- Optional later: `Affinity` to connect to the node hosting the queue leader.

## Configuration surface

Implemented as `RabbitMqAmqpSinkOptions`; the full table with defaults is in the README ("Options").
Notes on choices:

- No `BufferingTimeLimit`: the loop publishes whatever is buffered immediately (up to `BatchSizeLimit` in flight);
  batching happens naturally under load and adds no latency when idle.
- Credentials come from the URIs (`amqp://user:pass@host:port/vhost`).
- Application properties `level` and `source-context` are always set; more via `ConfigureMessage`.

## Code map

| Concern | Type |
|---|---|
| Public entry | `Serilog.RabbitMqAmqpLoggerConfigurationExtensions`, `RabbitMqAmqpSinkOptions`, `OverflowPolicy` |
| Sink (composition, Emit, shutdown) | `RabbitMqAmqpSink` |
| Buffer | `Buffering/EventBuffer` |
| Publish loop, message format, outcomes | `Publishing/PublishLoop`, `LogEventMessageFactory`, `OutcomeClassifier` |
| Connection supervision | `Connection/ConnectionSupervisor`, `EndlessBackOffDelayPolicy`, `RoundRobinUriSelector`, `TopologyDeclarer` |
| No-silent-loss exit | `FailureReporter` |

## Decisions

- **No disk buffer, ever** — target environments (Kubernetes / clusters) usually have no writable local
  disk. Buffer is in memory; drops go to the failure listener (ADR 0002).
- **Overflow default = `DropOldest`** — the most recent logs are the most valuable for incident analysis;
  dropped events are reported as `Permanent` → fallback sink.
- **Topology declaration: opt-in, off by default.** When enabled, after every (re)connect the sink
  declares exchange → quorum queue → binding through `IManagement` (`Exchange(...)`, `Queue(...).Quorum()`,
  `Binding()`), idempotently, before the publisher is reported Ready. Requires `configure` permission on the
  vhost; declaration errors are reported via `SelfLog` and retried with the reconnect back-off.
- **Per-event customisation hook** (inspired by the original sink's `ISendMessageEvents`): compute routing key
  and application properties from the `LogEvent`.

Prior art comparison: [prior-art.md](prior-art.md).

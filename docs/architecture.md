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
| C10 | Attaching a publisher to a **missing exchange**: on RabbitMQ 4.0 it does not surface `amqp:not-found` — `BuildAsync` fails with a `TimeoutException` after ~5 s; on newer versions (seen on 4.2–4.4) `amqp:not-found` surfaces right away — the attach is refused, or it succeeds and every publish is `Rejected amqp:not-found` (C7) — verified by the integration tests on every minor. | 4.0: repeated attach failures on a working connection (5 in a row) are reported as a *probable* configuration error; 4.4: `amqp:not-found` at attach is reported as a certain one. |
| C12 | User name and password are fixed when the connection settings are built (a URI → `Address` map); the client's recovery reconnects with them forever. TLS callbacks and the client-certificate collection are handed to every new connection. Serilog never rebuilds sinks on configuration reload. | Rotation (ADR 0005): new credentials → the supervisor reconnects through a new environment (`IEnvironmentSource`); certificate files are re-read in the TLS callbacks at each handshake. |
| C13 | Client recovery attempts are sequential and have **no timeout**; a TCP connect to a node that just vanished can hang for minutes (SYN not answered — verified: connects made while a broker container stops hang, later ones are refused). Recovery then stalls. | The supervisor gives client recovery `ClientRecoveryTimeout` (`MaxReconnectDelay` + 15 s) and then rebuilds itself; its own attempts are bounded by `ConnectTimeout`. |
| C11 | `TlsSettings.AcceptablePolicyErrors` is **not** passed to the TLS stack (only protocols, revocation check, client certificates and the validation callbacks are). SASL `EXTERNAL` is supported. | The sink validates server certificates itself (`TlsConfiguration`): acceptable errors are ignored, chain errors are re-checked against `Tls.CaCertificatePath` with custom root trust. |

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
 app threads        background (ADR 0004)                         background (ADR 0002 / 0003)
 ───────────        ─────────────────────                         ────────────────────────────
 Log.Information
       │ LogEvent
       ▼
 Sink.Emit ──► EventBuffer ──► FormatterLoop ──► OutageQueue ──► PublishLoop (sliding window, per-message retries)
 (enqueue only) burst channel    format once,      outage buffer        │                    ▲
               QueueLimit       release LogEvent   QueueSizeLimitBytes  │ publisher          │ Ready / not ready
               (count)          (+ routing key,    (approx. bytes)      ▼                    │
                  │ overflow     properties)          │ overflow     ConnectionSupervisor ───┘
                  ▼                                   ▼               IEnvironment → IConnection (Uris[]) → IPublisher
            UndeliveredEvents ◄───────────────────────┘                     │ AMQP 1.0
            (own worker, bounded; batches; stdout lines for every path;     ▼
             decodes records only if the listener needs events)    RabbitMQ node A | node B | node C
                  │
                  ▼
            FailureReporter (Diagnostics)
                  │
                  ▼
     failure listener / FallbackChain
```

### `RabbitMqAmqpSink` (composition root, internal type)
- Implements `ILogEventSink`, `IDisposable`, `IAsyncDisposable`, `ISetLoggingFailureListener`.
- `Emit` only writes to `EventBuffer`. No I/O, no formatting, no locks on the hot path beyond the channel.

### `EventBuffer` (burst channel)
- `Channel<LogEvent>` bounded by `QueueLimit` (default 10 000 events). Stays almost empty, because the formatter never
  waits for the broker; it only absorbs bursts the formatter has not caught up with yet.
- Overflow follows `OverflowPolicy`; dropped events are reported as `Permanent` (they are still `LogEvent`s, no parsing).

### `FormatterLoop` + `LogEventFormatter` (ADR 0004)
- Formats each event once into a `FormattedEvent` (UTF-8 body, message-id, routing key, `MessageProperties`, level,
  timestamp, source context) and releases the `LogEvent` — including any `Exception` graph it references.
- `RoutingKeySelector` / `MessageProperties` run here, once; unsupported property values fail here (event dropped and
  reported), never later on every publish attempt.
- Reports overflow and formatting failures in batches (once per chunk of up to 1 000 events).
- Also evaluates the correlation id (`CorrelationIdSelector`, else the trace id) and stamps `FormattedAt` from the
  `TimeProvider` — the age `FallbackAfter` is measured by.

### `SizeBoundedQueue<FormattedEvent>` (outage buffer)
- Lock + queue + byte counter, bounded by `QueueSizeLimitBytes` (default 15 MiB) using `FormattedEvent.EstimatedSize`
  (payload + approximate per-record overhead): a predictable memory bound, not an exact heap size.
- `DropOldest` evicts until the new record fits; `DropNewest` drops the new record; a record larger than the whole
  limit is dropped. Dropped records go back to the formatter loop, which reports them in one batch.

### `UndeliveredEvents` + `ClefDecoder` (Formatting) and `FailureReporter` (Diagnostics)
- Every undelivered event — burst overflow, formatting failure, outage overflow, rejection, shutdown, `Emit` after
  dispose — goes to `UndeliveredEvents`, split into chunks of ≤ 1 000. It runs the fallbacks on its own worker:
  `Emit` and the formatter never wait for a slow fallback. While running, at most 20 000 events wait for the fallback
  (the excess is counted and reported as lost); `Final` (shutdown) batches are exempt.
- Raw output (stdout) gets a line for every event; records are decoded back into `LogEvent`s only when the listener
  consumes events. `ClefDecoder` is our own `System.Text.Json` CLEF reader (template, properties, exception text,
  trace/span ids; no Newtonsoft dependency); non-CLEF formatters are handed over as `{Payload}`.
- `FailureReporter` is the bottom layer: listener calls and the aggregated SelfLog trace; it knows nothing about records.
  The sink closes it as the last step of `DisposeAsync`; afterwards reports (only possible from a fallback worker that
  was stuck past the budget) go to SelfLog, never to the listener — Serilog closes the fallbacks right after.
- Events emitted after `Dispose` are not handed to any fallback (Serilog closes the fallbacks together with this
  sink): they are counted (`EmittedAfterDispose`) and noted once in SelfLog — the `Emit` thread never blocks or writes.
- If the fallback worker is still writing a chunk when the shutdown hand-over budget ends, `CompleteAsync` returns
  without touching stdout; the worker finishes that chunk, counts the rest as lost (`Final`) and writes nothing more.

### `PublishLoop`
- Event-driven: finished attempts arrive through a completion queue, retries wait in a priority queue ordered by
  their own due time, one reusable timer wakes the loop for the next due retry, and at most one waiter each watches
  readiness and new records — O(log n) work per message regardless of the window size.
- Single long-running task with a **sliding window**: in flight + waiting for retry ≤ `BatchSizeLimit`. A new
  message is sent as soon as a slot frees up, so one message without an outcome never stalls the others, and the
  loop never pulls more from the outage queue than the window holds.
- Every message has its own retry schedule (`NotBefore`, attempt counters); progress of other messages never
  resets it. Due retries are sent before new events, so **order is not preserved**.
- Works on formatted records (body + stable `message-id`); `AmqpMessageBuilder` creates a fresh `IMessage` for every
  attempt, because a message object must not be re-sent while a previous send may still be in flight. A failure while
  building drops only that record (`Permanent`) — it is not a connection problem.
- Waits on `ConnectionSupervisor.WaitReadyAsync` instead of blind back-off; while not ready, events
  accumulate in the outage queue (bounded by bytes).
- On abort every finished attempt is still evaluated individually, so accepted messages are not reported as
  undelivered.
- `FallbackAfter` (optional): at most every quarter of the limit (≤ 1 s) the loop hands records older than the limit
  to `UndeliveredEvents` as `Permanent` — from the retry queue and from the head of the outage queue
  (`SizeBoundedQueue.TryDequeueIf`, FIFO by formatting time). In-flight attempts are left alone (the broker may still
  accept them). While not ready the timer keeps ticking at that interval, since nothing else wakes the loop.
- Outcome handling per message:

| Result | Classification | Action |
|---|---|---|
| `Accepted` | success | done |
| `Released` | transient / maybe unroutable | retry up to `MaxReleasedRetries` (default 5) spaced 1 s, 2 s, 4 s… (≈ 31 s), then drop + report `Permanent` |
| `Rejected` with `amqp:not-found` | target missing (exchange/queue deleted) | keep message, `ReportTargetMissing` → supervisor rebuilds (re-declares topology if enabled, otherwise stays not-Ready until the target exists) |
| `Rejected` with a connection/session/link error (C9) | connection lost | keep, retry without limit, signal supervisor |
| `Rejected` with `amqp:resource-limit-exceeded` | throttled (temporary broker state) | retry **without limit**, spaced 1 s → 30 s |
| `Rejected` (other: message size, permissions, invalid fields) | permanent | drop + report `Permanent` (with broker error) |
| `AmqpNotOpenException`, `PublisherException`, connection errors | transient | signal supervisor, retry spaced 0.1 s → 5 s — **no limit** |
| building the message throws | invalid record | drop + report `Permanent` |
| publish timeout (`PublishTimeout`, default 30 s) | unknown | retry (possible duplicate), signal supervisor |

### `ConnectionSupervisor`
- Owns one `IEnvironment`, one `IConnection` (with all configured hosts as URIs), one `IPublisher`.
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
- Leaves `Ready` (→ `Faulted`) **before** tearing down, so `WaitReadyAsync` never hands out a publisher that is
  being closed.
- Configuration errors (C10, `ConfigurationErrorDetector`: unauthorized access, precondition failed, bad request,
  not found, or 3 attach failures in a row on a working connection) are reported once per distinct reason through
  `OnConfigurationError` → failure listener (`Permanent`, no events) and `SelfLog`; retrying continues.
- Exposes `WaitReadyAsync(ct)` (completes when Ready) and the current state for diagnostics.
- Start-up is **lazy and non-blocking**: the logger is usable immediately; connection is established in
  the background.

### Shutdown
`Dispose`/`DisposeAsync` uses **one deadline** (`ShutdownTimeout`, default 10 s), split into three phases so none can
starve the others:

| Phase | Budget (10 s default) |
|---|---|
| deliver buffered events (ends early when everything is delivered, or immediately when idle) | rest (5 s) |
| hand the undelivered rest (`Final`) to the fallbacks — `UndeliveredEvents.CompleteAsync` | min(5 s, 30 %) (3 s) |
| close publisher → connection → environment | min(2 s, 25 %) (2 s) |

When the broker is down the deliver phase runs out, and the fallback still gets its own time — that is exactly when
it matters most. `Final` batches are not subject to the fallback's pending limit (the records are already in memory)
and are handed over **before** any earlier overflow backlog; what still does not make it is reported as lost with
kind `Final`. Each phase keeps its minimum share and the in-flight settle / lock waits add up to ~1–2 s, so
`ShutdownTimeout` is a target rather than a hard limit.
After its budget the fallback worker is stopped (nothing is written after `Dispose` returns) and the rest is reported
as lost. Size `terminationGracePeriodSeconds` in Kubernetes above `ShutdownTimeout`.

## Multi-node guidance (documentation for users)
- Configure **all** cluster nodes (or a load balancer) in `Hostnames`.
- Publish to an exchange bound to a **quorum queue** (or a stream) so that stored log messages survive the
  loss of a node; classic queues live on one node.
- Optional later: `Affinity` to connect to the node hosting the queue leader.

## Configuration surface

Implemented as `RabbitMqAmqpSinkOptions`; the full table with defaults is in [guide/options.md](guide/options.md).
Notes on choices:

- No `BufferingTimeLimit`: the loop publishes whatever is buffered immediately (up to `BatchSizeLimit` in flight);
  batching happens naturally under load and adds no latency when idle.
- `Hostnames` accepts plain hosts (`host`, `host:port`, `[ipv6]:port`) and `amqp(s)://` URIs; credentials, virtual host,
  port and TLS can be separate options. `EndpointResolver` turns everything into canonical
  `amqp(s)://user:pass@host:port/vhost` URIs (user name and password escaped), because that is how the client takes
  credentials for several nodes. Conflicting duplicates are rejected; passwords never appear in error messages.
- The client reads the virtual host from the single URI path segment and unescapes it, so a virtual host with `/`
  (e.g. `/data`) must be `%2F`-encoded in the URI. `EndpointResolver` does that for `VirtualHost` and rejects URI paths
  with more than one segment (`//data`, `a/b`, trailing `/`) — .NET would otherwise drop the extra slash silently.
  Verified against a real broker with a `/data` virtual host (`VirtualHostTests`).
- Application properties `level` and `source-context` are always set; more via `MessageProperties`.
- `correlation-id` defaults to the W3C trace id; `DurableMessages` controls the durable header (default on).
- Secret rotation (ADR 0005, event-driven): `RabbitMqAmqpCredentialsSource.Update` → `IEnvironmentSource.Changed` →
  the supervisor tears the connection down (also while the client is reconnecting, and cutting a rebuild back-off short),
  replaces the environment and reconnects. The `appsettings` overload gets `IConfiguration`, finds its own `Args`
  section (`ConfigurationCredentials`) and updates the source on reload tokens. Certificates: `ReloadingFiles<T>` in the
  TLS callbacks re-reads the files at each handshake and reparses only changed bytes.
- TLS: certificate files (CA, client PFX/PEM) are read once at logger creation — typically mounted secrets; nothing is
  written to disk (PEM keys are round-tripped through PKCS#12 in memory). `TlsConfiguration` builds `TlsSettings` and
  the SASL mechanism.
- TLS settings are the one nested options object (`RabbitMqAmqpSinkOptions.Tls`, `RabbitMqAmqpTlsOptions`): ten
  related settings would otherwise crowd the flat options and the `appsettings` argument list. Serilog.Settings.Configuration
  binds the `"tls": { ... }` object argument by property names (verified by a test). Everything else stays flat, as is
  usual for Serilog sinks.
- `hostnames` in `appsettings` is a `string[]`: `Serilog.Settings.Configuration` cannot bind a JSON array to a
  `string` argument nor a plain string to an array, so the array wins; elements are also split by `,`/`;` for
  environment variables.

## Code map

| Concern | Type |
|---|---|
| Public entry (root) | `Serilog.RabbitMqAmqpLoggerConfigurationExtensions` |
| Public models (`Options`, root namespace) | `RabbitMqAmqpSinkOptions`, `RabbitMqAmqpTlsOptions`, `RabbitMqAmqpCredentialsSource`, `OverflowPolicy` |
| Composition root, Emit, shutdown (root) | `RabbitMqAmqpSink` |
| `Buffering` | `EventBuffer` (burst), `SizeBoundedQueue<T>` (outage, size) |
| `Formatting` | `FormatterLoop`, `LogEventFormatter`, `ClefDecoder`, `UndeliveredEvents` |
| `Formatting/Models` | `FormattedEvent`, `TextException` |
| `Publishing` (actions) | `PublishLoop`, `AmqpMessageBuilder`, `OutcomeClassifier`, `IPublisherProvider` |
| `Publishing/Models` | `PendingMessage`, `OutcomeKind`, `PublishLoopOptions` |
| `Connection` (actions) | `ConnectionSupervisor` (implements `IPublisherProvider`), `IEnvironmentSource` (`RotatingEnvironmentSource`, `FixedEnvironmentSource`), `ConfigurationCredentials`, `ReloadingFiles<T>`, `AmqpConnectionSettingsFactory`, `TlsConfiguration`, `EndpointResolver`, `ConfigurationErrorDetector`, `EndlessBackOffDelayPolicy`, `RoundRobinUriSelector` |
| `Connection/Models` | `SupervisorState`, `ConnectionSupervisorOptions`, `ConnectionStage`, `ConfigurationError` |
| `Topology` | `TopologyDeclarer` |
| `Resilience` | `ExponentialBackoff` |
| `Diagnostics` | `FailureReporter` (no-silent-loss exit) |

Dependencies point one way (enforced by `FolderDependencyTests`):
`Connection` → `Publishing` → `Formatting` → `Buffering` / `Diagnostics`, and `Resilience` for both loops;
`Diagnostics`, `Resilience` and `Buffering` depend on no other folder. `Connection` sees `Publishing` only through the
`IPublisherProvider` interface owned by `Publishing`.

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

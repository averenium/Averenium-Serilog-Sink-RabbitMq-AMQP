# Prior art: Serilog.Sinks.RabbitMQ

Reviewed: [ArieGato/serilog-sinks-rabbitmq](https://github.com/ArieGato/serilog-sinks-rabbitmq), `main` @ `3cf1d2f`
(2026-10-27, package 9.x). Targets `netstandard2.0;net8.0;net10.0`, `RabbitMQ.Client` 7.2 (AMQP 0-9-1), `Serilog` 4.4.

## How it works

| Area | Behaviour |
|---|---|
| Batching | Serilog's built-in `BatchingSink` (`BatchPostingLimit` 50, `BufferingTimeLimit` 2 s, `RetryTimeLimit` 10 min, `QueueLimit` optional). |
| Publishing | Events of a batch are published **one by one** (`await` each) through a pool of 64 channels. |
| Delivery guarantee | `BasicPublishAsync` **without publisher confirms** and without `mandatory` → a publish "succeeds" once written to the socket. Messages in flight when the connection drops, or unroutable messages, are lost silently. |
| Durability | `DeliveryMode` default **NonDurable**. |
| Connection | `RabbitMQ.Client` automatic recovery (`NetworkRecoveryInterval` 2 s). One cached connection, created lazily; if creation fails it is retried on the next publish. |
| Multi-node | `Hostnames` list → `AmqpTcpEndpoint`s; the client picks an endpoint on connect/recovery. |
| Channel pool health | Warm-up with back-off 0.5 s → 30 s; after `WarmUpMaxRetries` (10) failures a **circuit breaker** opens for 60 s, publishes fail fast ("pool exhausted"), then a single probe. |
| Failure reporting | Implements `ISetLoggingFailureListener`; after `RetryTimeLimit` failed batches go to `WriteTo.FallbackChain(...)`. |
| Audit mode | `AuditTo.RabbitMQ(...)` — synchronous, sync-over-async (`RunSync`), exceptions propagate. |
| Topology | Only `AutoCreateExchange` (declares the exchange, durable if `DeliveryMode` durable). **No queue / binding declaration.** |
| Extensibility | `ISendMessageEvents`: per-event hook to set message properties and compute the routing key. |
| Formatting | `CompactJsonFormatter` by default, pooled `RecyclableMemoryStream`. |

## Lessons for this project

**Adopt**
- `ISetLoggingFailureListener` + `FallbackChain` integration (already in our design).
- Per-event hook for routing key and message properties (e.g. route by level / source context) —
  our equivalent: `IMessageCustomizer`/delegate, applied when the event is formatted.
- Lazy, non-blocking connection creation.
- Compact JSON as the default formatter; pooled buffers to limit allocations.
- Opt-in exchange declaration — and go further (quorum queue + binding), since the AMQP client supports it.

**Do differently** (gaps relative to our reliability goal)
- No confirms → we use AMQP 1.0 settlement outcomes (`Accepted`/`Released`/`Rejected`) for every message.
- Silent loss of unroutable messages → `Released` is detected, retried a few times, then reported.
- Serilog `BatchingSink` limits (10-min retry window, queue drain, silent overflow) → own pipeline (ADR 0002).
- Sequential publish per batch → pipelined concurrent publish on one link.
- Durable messages by default (the AMQP client already defaults to durable).
- Circuit breaker with 60 s fail-fast window adds latency after recovery → readiness-driven waiting
  on supervisor state instead (ADR 0003).
- Sync-over-async audit mode → not planned for v1.

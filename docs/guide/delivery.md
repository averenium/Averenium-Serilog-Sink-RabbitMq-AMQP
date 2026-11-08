# Delivery, failures and shutdown

[← README](../../README.md) · [Options reference → Delivery](options.md#delivery-and-fallback)

## Guarantees

- **Never blocks the app.** `Log.*` calls only enqueue; logger creation does not wait or fail when the broker is down —
  the sink connects in the background.
- **No silent loss.** An event leaves the sink only when RabbitMQ accepted it, or when it is handed to the Serilog
  failure listener, so `WriteTo.FallbackChain(...)` receives it.
- **At-least-once.** After a connection loss a message may arrive twice; `message-id` identifies duplicates.
- **Order is not guaranteed.** Messages are published concurrently, retries go before newer events.
- **Outages of any length** — broker restarts, node crashes, failovers — are handled automatically; events wait in a
  memory buffer (`QueueSizeLimitBytes`, then `OverflowPolicy`). No disk is used.

## How it works

1. `Emit` puts the event into a small burst channel (`QueueLimit`).
2. A background formatter turns it into message bytes and releases the event.
3. The bytes wait in the outage buffer, bounded by `QueueSizeLimitBytes` (approximate memory).
4. A background loop publishes through a sliding window (`BatchSizeLimit` in flight or waiting for a retry) and waits
   for the broker outcome of **each** message; one slow message never stalls the others.

## Retries and failures

| Situation | What happens |
|---|---|
| connection lost, broker down, publish timeout | retried without limit (reconnect back-off `MinReconnectDelay` → `MaxReconnectDelay`) |
| target exchange/queue deleted | messages kept; the sink reconnects and re-declares the topology if `DeclareTopology` is on |
| `amqp:resource-limit-exceeded` (e.g. queue full with reject-publish) | retried without limit, spaced 1 s → 30 s |
| message *released* (unroutable — no binding) | retried `MaxReleasedRetries` times over ≈ 31 s, then handed to the fallback |
| rejected for the message itself (too large, invalid) | handed to the fallback at once |
| waiting longer than `FallbackAfter` | handed to the fallback (off by default) |

**`FallbackAfter`** bounds how long an event may wait (from formatting, across the buffer and retries). Use it when a
fallback should take over during long outages, not only at overflow or shutdown. A publish already in flight is not
interrupted, so an event can — rarely — reach both the broker and the fallback.

**Configuration problems** (wrong credentials or permissions, missing virtual host, a queue that exists with another
type) are reported once to the failure listener and as `CONFIGURATION ERROR` in `SelfLog`; the sink keeps retrying, so
fixing the broker side is enough. A missing exchange without `DeclareTopology` can only be inferred, so it is reported
as *probable* after 5 failed attaches on a working connection.

## Fallbacks

- `WriteTo.FallbackChain(sink, fallback)` — the fallback receives every undelivered event (buffer overflow, rejection,
  `FallbackAfter`, shutdown), also when the sink has `restrictedToMinimumLevel`.
- `WriteUndeliveredToStandardOutput = true` — also writes every undelivered event as its formatted line to stdout
  (collected by Fluent Bit / Vector), with any formatter and without an extra sink.
- Fallbacks run on their own worker: a slow fallback never slows logging; if it cannot keep up, the excess is counted
  and reported as lost.

## Shutdown

Call `await Log.CloseAndFlushAsync()` (or dispose the logger) on shutdown. `ShutdownTimeout` (default 10 s) bounds the
**whole** shutdown, split into delivering what is buffered (≈ 50 %), handing the rest to the fallbacks (≈ 30 %, max 5 s)
and closing the connection (≈ 20 %, max 2 s). So a broker that is down never takes the time the fallback needs. An idle
sink stops immediately.

Serilog passes no deadline to sinks, and the host's `ShutdownTimeout` does not apply to logger disposal — that is why the
sink has its own. In Kubernetes keep `terminationGracePeriodSeconds` above `ShutdownTimeout` plus your app's own
shutdown work.

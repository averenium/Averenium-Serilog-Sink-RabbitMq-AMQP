# ADR 0004: Background formatting stage and a byte-bounded outage buffer

- **Status:** Accepted (implemented)
- **Date:** 2026-11-07

## Context

The in-memory buffer was limited by event **count** (`QueueLimit`, 100 000). Events with stack traces can be 10+ KB,
so a long outage could use 100 MB – 1 GB+ and get the pod OOM-killed — losing the whole buffer. A byte limit needs the
size of what is buffered:

- the retained size of a `LogEvent` graph (properties, the `Exception` object and everything it references) cannot be
  measured cheaply in .NET;
- formatting in `Emit` would give exact payload bytes, but moves CPU to application threads and — because
  `FallbackChain` needs `LogEvent`s — would keep the event *and* its bytes (double memory).

## Decision

```
Emit → [burst channel of LogEvent, QueueLimit (count)] → FormatterLoop (background)
     → [SizeBoundedQueue of formatted records, QueueSizeLimitBytes] → PublishLoop
```

- `Emit` only enqueues, as before; nothing is formatted on application threads.
- The background `FormatterLoop` never waits for the broker: it turns each `LogEvent` into a `FormattedEvent`
  (UTF-8 body, message-id, routing key, application properties, level, timestamp) and releases the event. The burst
  channel therefore stays almost empty (default 10 000 events); the long outage buffer consists of bytes.
- `SizeBoundedQueue<T>` (lock + queue + size counter — `Channel` cannot limit by size; generic, so buffering knows
  nothing about formatting) holds the records and is bounded by
  `QueueSizeLimitBytes` (default 15 MiB). `DropOldest` evicts until the new record fits, `DropNewest` drops the new
  one, a record larger than the whole limit is dropped and reported. The size is **payload + an approximate per-record
  overhead**: a predictable bound, documented as approximate memory, not an exact heap size.
- `RoutingKeySelector` and `MessageProperties` (replacing `ConfigureMessage`, which needed the `LogEvent` on every
  publish attempt) run once on the formatter thread; property values are validated there, so user code can never fail
  later on every retry.
- Drops are the **main** path during a long outage, so every undelivered event goes to `UndeliveredEvents`, which runs
  the fallbacks on **its own worker** with a bounded queue: neither `Emit` nor the formatter waits for a slow fallback;
  if it cannot keep up, the excess is counted and reported as lost. Reports are batched and coalesced, and records are
  turned back into `LogEvent`s **only when the listener consumes events** — the default SelfLog listener gets the count.
- Decoding uses our own small CLEF reader on `System.Text.Json` (`ClefDecoder`). The official
  `Serilog.Formatting.Compact.Reader` was rejected: it pulls in Newtonsoft.Json as a runtime dependency for a rare path.
  Non-CLEF formatters are handed over as level + timestamp + raw body (`{Payload}`).
- Optional raw fallback `WriteUndeliveredToStandardOutput`: every undelivered event, one formatted line each, to stdout
  — the natural fallback in Kubernetes (Fluent Bit / Vector read CLEF/JSON). Records are written as they are; events
  that never got formatted are formatted on the fallback worker (a minimal CLEF line if the formatter itself failed).

## Consequences

- Memory during an outage is bounded by `QueueSizeLimitBytes` (approximately) plus a small burst channel and the
  publish window; the `LogEvent` graph (including exceptions and what they reference) is released right after formatting.
- Formatting stays off application threads; `Serilog.Sinks.Async` is not needed.
- Public API: `QueueSizeLimitBytes`, `MessageProperties`, `WriteUndeliveredToStandardOutput` added, `ConfigureMessage`
  removed, `QueueLimit` now means the burst buffer (default lowered to 10 000).
- Events restored for the failure listener carry the exception as text (`ToString()` of the original), not the
  original exception object.

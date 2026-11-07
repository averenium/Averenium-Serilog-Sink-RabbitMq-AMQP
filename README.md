# Averenium.Serilog.Sinks.RabbitMQ.Amqp

A [Serilog](https://serilog.net) sink that writes log events to **RabbitMQ 4.x over AMQP 1.0**,
built on the official [`RabbitMQ.AMQP.Client`](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client).

> **Status:** pre-release (0.x) — the public API may still change and no package is published yet.

## Why another RabbitMQ sink?

Existing Serilog RabbitMQ sinks use `RabbitMQ.Client`, which speaks AMQP 0-9-1.
RabbitMQ 4.0 made AMQP 1.0 a core protocol, and RabbitMQ ships a dedicated AMQP 1.0 client for .NET.
This sink uses that client, so you get:

- AMQP 1.0 addressing (`/exchanges/{name}/{key}`, `/queues/{name}`) and per-message publish outcomes;
- automatic connection recovery from the client;
- no dependency on `RabbitMQ.Client`.

## Requirements

- .NET 10
- RabbitMQ **4.0 or newer** (AMQP 1.0 enabled by default)

## Installation

```bash
dotnet add package Averenium.Serilog.Sinks.RabbitMQ.Amqp
```

## Usage

```csharp
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.FallbackChain(
        wt => wt.RabbitMQAmqp(options =>
        {
            // every cluster node — the sink fails over between them round-robin
            options.Uris.Add(new Uri("amqp://app:secret@rabbit-1:5672/"));
            options.Uris.Add(new Uri("amqp://app:secret@rabbit-2:5672/"));
            options.Uris.Add(new Uri("amqp://app:secret@rabbit-3:5672/"));
            options.Exchange = "logs";
            options.RoutingKey = "billing";
        }),
        // optional: whatever could not be delivered (buffer overflow, shutdown timeout) goes here
        wt => wt.Console())
    .CreateLogger();

Log.Information("Hello from {Protocol}", "AMQP 1.0");

await Log.CloseAndFlushAsync(); // delivers buffered events (up to ShutdownTimeout)
```

From `appsettings.json` (via `Serilog.Settings.Configuration`):

```json
{
  "Serilog": {
    "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "uris": "amqp://app:secret@rabbit-1:5672/;amqp://app:secret@rabbit-2:5672/",
          "exchange": "logs",
          "routingKey": "billing",
          "declareTopology": true,
          "declareQueue": "logs.billing"
        }
      }
    ]
  }
}
```

### How delivery works

- `Log.*` calls only put the event into a bounded in-memory buffer — they never block on the network and never throw.
  Logger creation does not fail or wait when the broker is down; the sink connects in the background.
- A background loop publishes messages concurrently and waits for the broker outcome of **each** message.
  A message leaves the buffer only when RabbitMQ accepted it, or when it is reported to the Serilog failure listener
  (so `WriteTo.FallbackChain(...)` receives it). Nothing is dropped silently.
- **At-least-once**: after a connection loss a message may be delivered twice. Every message carries a unique
  `message-id` for de-duplication.
- Broker restarts, node crashes, network blips and failovers are handled automatically and for as long as they last;
  events accumulate in the buffer meanwhile (up to `QueueLimit`, then `OverflowPolicy`).
- If the target exchange/queue disappears, messages are kept and the sink reconnects (re-declaring the topology when
  `DeclareTopology` is on) instead of dropping logs.
- No disk is used, by design (containers / Kubernetes).

### Message format

| AMQP field | Value |
|---|---|
| body | event formatted by `Formatter` (default compact JSON / CLEF), UTF-8 |
| `message-id` | unique per event, stable across retries |
| `content-type` | `ContentType` (default `application/json`) |
| `creation-time` | event timestamp (UTC) |
| application property `level` | `Information`, `Error`, ... |
| application property `source-context` | `SourceContext` of the event, if any |
| durable | yes |

Use `RoutingKeySelector` to route by event (e.g. by level) and `ConfigureMessage` to add your own properties.

### Options

| Option | Default | Description |
|---|---|---|
| `Uris` | — | one or more `amqp://` / `amqps://` URIs (same vhost) |
| `Exchange` + `RoutingKey`, or `Queue` | — | publish target |
| `RoutingKeySelector` | — | per-event routing key (needs `Exchange`) |
| `ConfigureMessage` | — | adjust each outgoing message |
| `Formatter` / `ContentType` | compact JSON / `application/json` | body format |
| `ConnectionName` | `<entry assembly>@<machine>` | shown in the management UI |
| `QueueLimit` | 100 000 | events buffered in memory |
| `OverflowPolicy` | `DropOldest` | `DropOldest` keeps recent logs, `DropNewest` keeps the start of an incident |
| `BatchSizeLimit` | 500 | messages in flight at once |
| `PublishTimeout` | 30 s | wait for one broker outcome before retrying |
| `MaxReleasedRetries` | 5 | retries for messages the broker released (e.g. unroutable) |
| `MinReconnectDelay` / `MaxReconnectDelay` | 0.5 s / 30 s | reconnect back-off; reconnecting never stops |
| `ShutdownTimeout` | 10 s | time to deliver buffered events on dispose |
| `DeclareTopology` | `false` | declare exchange (`ExchangeType`, default `topic`) and quorum queue `DeclareQueue` bound with `BindingKey` after every connect |

### Recommendations for clusters

- List all nodes in `Uris` (or put a load balancer in front).
- Store logs in a **quorum queue** (or a stream): classic queues live on a single node.
- Add a fallback sink (e.g. console / stdout collected by your log agent) via `WriteTo.FallbackChain`.

## Building from source

```bash
dotnet build -c Release
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests
```

Tests use [TUnit](https://tunit.dev). Integration tests start RabbitMQ 4 in containers and need Docker (or Podman):

```bash
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests
```

## Repository layout

| Path | Contents |
|---|---|
| `src/` | The library (NuGet package) |
| `tests/` | Unit tests and Testcontainers-based integration tests |
| `docs/` | Architecture notes and decision records |
| `AGENTS.md` | Contribution rules for humans and AI agents |

## Contributing

Read [AGENTS.md](AGENTS.md) before contributing — it covers conventions, public API tracking and test requirements.
Changes are recorded in [CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0.

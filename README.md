# Averenium.Serilog.Sinks.RabbitMQ.Amqp

[Serilog](https://serilog.net) sink for **RabbitMQ 4.x over AMQP 1.0** (official
[`RabbitMQ.AMQP.Client`](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client)). Survives broker restarts, failovers
and outages of any length without losing logs, never blocks your app, needs no disk.

> **Status:** pre-release (0.x) — the public API may still change and no package is published yet.

## Install

```bash
dotnet add package Averenium.Serilog.Sinks.RabbitMQ.Amqp
```

Requires .NET 10 and RabbitMQ 4.0+.

## Quick start — code

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.FallbackChain(
        wt => wt.RabbitMQAmqp(o =>
        {
            o.Hostnames.Add("rabbit-1");            // add every cluster node
            o.Hostnames.Add("rabbit-2");
            o.Username = "app";
            o.Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD");
            o.Exchange = "logs";
            o.RoutingKey = "billing";
        }),
        wt => wt.Console())                         // gets whatever could not be delivered
    .CreateLogger();

// on shutdown — delivers what is buffered (bounded by ShutdownTimeout)
await Log.CloseAndFlushAsync();
```

## Quick start — appsettings.json

Needs [Serilog.Settings.Configuration](https://github.com/serilog/serilog-settings-configuration) (included in
`Serilog.AspNetCore`): `builder.Services.AddSerilog((s, lc) => lc.ReadFrom.Configuration(builder.Configuration));`

```json
{
  "Serilog": {
    "Using": [ "Serilog.Sinks.Console", "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "FallbackChain",
        "Args": {
          "configureSink": [
            {
              "Name": "RabbitMQAmqp",
              "Args": {
                "hostnames": [ "rabbit-1", "rabbit-2" ],
                "username": "app",
                "exchange": "logs",
                "routingKey": "billing"
              }
            }
          ],
          "configureFallback": [ { "Name": "Console" } ]
        }
      }
    ]
  }
}
```

Set the password from a secret, e.g. `Serilog__WriteTo__0__Args__configureSink__0__Args__password`. Without a fallback,
use `"Name": "RabbitMQAmqp"` directly in `WriteTo`. `hostnames` must be a JSON array.

## Options you will most likely set

| Option | Default | |
|---|---|---|
| `Hostnames` | — | cluster nodes: `host`, `host:port` or `amqp(s)://` URIs |
| `Username` / `Password` | — | rotated values from the configuration are picked up without a restart |
| `VirtualHost` | `/` | |
| `Exchange` + `RoutingKey`, or `Queue` | — | publish target |
| `DeclareTopology` + `DeclareQueue` | off | create exchange and quorum queue on connect |
| `UseTls`, `Tls.CaCertificatePath` | off | TLS with a private CA — see [TLS](docs/guide/tls.md) |
| `Formatter` | compact JSON (CLEF) | see [Message format](docs/guide/message-format.md) |
| `QueueSizeLimitBytes` | 15 MiB | memory for logs buffered during an outage |
| `FallbackAfter` | off | hand events to the fallback if not delivered within this time |
| `ShutdownTimeout` | 10 s | upper bound for the whole shutdown |

All options: [Options reference](docs/guide/options.md).

## Production checklist

- List **all** cluster nodes and publish to a **quorum queue**.
- Add a fallback (`WriteTo.FallbackChain`) or `WriteUndeliveredToStandardOutput = true`.
- Call `Log.CloseAndFlushAsync()` on shutdown; in Kubernetes keep `terminationGracePeriodSeconds` > `ShutdownTimeout` + 2 s.
- Size `QueueSizeLimitBytes` for the outage you want to ride out (memory only, no disk).
- Consumers: de-duplicate by `message-id` (at-least-once), sort by timestamp (order is not guaranteed).

## Features

- **No lost logs** — buffers during outages of any length, retries until RabbitMQ accepts each message, hands the rest
  to a fallback sink; at-least-once with a stable `message-id` → [Delivery](docs/guide/delivery.md)
- **Clusters and failover** — all nodes in `Hostnames`, round-robin reconnects, endless recovery
  → [Configuration](docs/guide/configuration.md#hosts)
- **Never blocks the app** — logging only enqueues; the broker may be down at start-up
  → [Delivery](docs/guide/delivery.md#guarantees)
- **Bounded memory, no disk** — byte-limited buffer (`QueueSizeLimitBytes`), built for containers
  → [Kubernetes](docs/guide/kubernetes.md)
- **Secret rotation without a restart** — passwords changed in `IConfiguration` and renewed certificate files are picked
  up automatically → [Password rotation](docs/guide/configuration.md#rotating-the-password-without-a-restart),
  [certificate rotation](docs/guide/tls.md#certificate-files-and-rotation)
- **TLS** — private CA, mutual TLS, SASL EXTERNAL → [TLS](docs/guide/tls.md)
- **Fallbacks** — `WriteTo.FallbackChain`, stdout output, `FallbackAfter` for long outages
  → [Delivery](docs/guide/delivery.md#fallbacks)
- **Graceful shutdown** — one deadline for delivering, handing over and closing → [Shutdown](docs/guide/delivery.md#shutdown)
- **Loud configuration errors** — wrong credentials, permissions or missing targets are reported once, retrying goes on
  → [Delivery](docs/guide/delivery.md#retries-and-failures)
- **Any message format** — CLEF by default, any Serilog formatter, trace id as `correlation-id`, routing per event
  → [Message format](docs/guide/message-format.md)
- **Code or `appsettings.json`** — every common option in both → [Options reference](docs/guide/options.md)

## Guides

| | |
|---|---|
| [Configuration](docs/guide/configuration.md) | hosts, credentials, virtual hosts, `appsettings.json`, password rotation |
| [TLS](docs/guide/tls.md) | private CA, mutual TLS, SASL EXTERNAL, certificate rotation |
| [Message format](docs/guide/message-format.md) | what consumers receive, formatters, routing, custom properties |
| [Delivery](docs/guide/delivery.md) | guarantees, retries, fallbacks, `FallbackAfter`, shutdown |
| [Kubernetes](docs/guide/kubernetes.md) | pods, memory, secrets, cluster recommendations |
| [Options reference](docs/guide/options.md) | every option with defaults and validation rules |

## Try it locally

```bash
docker compose up -d
dotnet run --project samples/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Sample            # appsettings.json
dotnet run --project samples/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Sample -- code    # code
```

Management UI: http://localhost:15672 (`sample` / `sample`), queue `logs.sample`.

---

## Why another RabbitMQ sink?

Existing Serilog RabbitMQ sinks use `RabbitMQ.Client` (AMQP 0-9-1). RabbitMQ 4.0 made AMQP 1.0 a core protocol and
ships a dedicated AMQP 1.0 client for .NET; this sink uses it — per-message publish outcomes, AMQP 1.0 addressing,
client connection recovery, no `RabbitMQ.Client` dependency. Comparison with existing sinks:
[docs/prior-art.md](docs/prior-art.md); design: [docs/architecture.md](docs/architecture.md).

## Building from source

```bash
dotnet build -c Release
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests   # needs Docker or Podman
```

| Path | Contents |
|---|---|
| `src/` | the library |
| `tests/` | unit tests (TUnit) and Testcontainers integration tests |
| `samples/`, `docker-compose.yml` | runnable sample and a local RabbitMQ |
| `docs/guide/` | user guides |
| `docs/architecture.md`, `docs/adr/` | design and decision records |

Releases: pushing a `vX.Y.Z` tag publishes to nuget.org (see [AGENTS.md → Releasing](AGENTS.md#releasing)). Contributing: read [AGENTS.md](AGENTS.md). Changes: [CHANGELOG.md](CHANGELOG.md). License: [MIT](LICENSE).

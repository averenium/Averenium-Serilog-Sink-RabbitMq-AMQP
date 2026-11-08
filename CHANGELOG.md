# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0-preview-01] - 2026-11-08

First preview. The public API may still change before 0.1.0.

### Added

- `WriteTo.RabbitMQAmqp(...)` — a Serilog sink for **RabbitMQ 4.x over AMQP 1.0** on the official
  `RabbitMQ.AMQP.Client`, configured in code or from `appsettings.json` (Serilog.Settings.Configuration).
- **Reliable delivery:** at-least-once with a stable `message-id`, a per-message broker outcome for every event and
  retries without a time limit for connection problems; nothing is dropped silently — whatever cannot be delivered is
  handed to the Serilog failure listener, so `WriteTo.FallbackChain(...)` receives it (also with
  `restrictedToMinimumLevel`).
- **Outages and clusters:** lazy background connect (the broker may be down at start-up), endless client recovery with
  round-robin over all `Hostnames`, and a supervisor that rebuilds connection/publisher when the client gives up, a
  publisher is stuck, or a recovery attempt hangs (e.g. TCP connect to a vanished node).
- **Bounded memory, no disk:** events are formatted on a background thread into a byte-limited outage buffer
  (`QueueSizeLimitBytes`, default 15 MiB) with `OverflowPolicy`; `FallbackAfter` hands events to the fallback when they
  wait too long; `WriteUndeliveredToStandardOutput` writes undelivered events to stdout.
- **Graceful shutdown** within one `ShutdownTimeout`, split between delivering, handing over to the fallbacks and
  closing; an idle sink stops immediately.
- **Connection options:** plain hosts or `amqp(s)://` URIs, separate `Username`/`Password`/`VirtualHost`/`Port`,
  virtual hosts with `/`, conflicting settings rejected at logger creation, passwords never in error messages.
- **TLS** (`Tls`): private CA, mutual TLS (PFX or PEM), SASL EXTERNAL, protocols, tolerated certificate errors.
- **Secret rotation without a restart:** changed `username`/`password` in `IConfiguration` (or
  `RabbitMqAmqpCredentialsSource.Update` in code) reconnect the sink; renewed certificate files are used on the next
  connection.
- **Message format:** compact JSON (CLEF) by default or any Serilog formatter, `level`/`source-context` properties,
  trace id as `correlation-id`, per-event routing key and custom application properties, durable or non-durable
  messages.
- **Topology declaration** (opt-in): exchange, quorum queue and binding after every connect.
- **Loud configuration errors:** wrong credentials or permissions, missing virtual host or target, conflicting queue
  types are reported once to the failure listener and SelfLog while the sink keeps retrying.
- Sample (`samples/`), `docker-compose.yml`, guides in `docs/guide/`.
- Tested against RabbitMQ 4.0–4.4 (latest patch of each minor).

### Known limitations

- Targets .NET 10 only.
- Message order is not guaranteed (concurrent publishing, retries go first).
- Changing hosts, virtual host or TLS options at runtime needs a restart (credentials and certificate files do not).

[Unreleased]: https://github.com/averenium/Averenium-Serilog-Sink-RabbitMq-AMQP/compare/v0.1.0-preview-01...HEAD
[0.1.0-preview-01]: https://github.com/averenium/Averenium-Serilog-Sink-RabbitMq-AMQP/releases/tag/v0.1.0-preview-01

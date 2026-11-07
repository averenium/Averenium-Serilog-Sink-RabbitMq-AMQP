# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Repository scaffold: .NET 10 solution, library and test projects (TUnit), Central Package Management, contribution rules.
- Connection supervision (internal): lazy background connect with endless retries, client recovery with an
  endless back-off and round-robin node selection, rebuild of connection/publisher when the client gives up,
  a publisher is stuck or publishes keep timing out (ADR 0003).
- `WriteTo.RabbitMQAmqp(...)` sink (code and `Serilog.Settings.Configuration` overloads) with
  `RabbitMqAmqpSinkOptions`: bounded in-memory buffer with `OverflowPolicy` (default `DropOldest`), concurrent
  publishing with per-message broker outcomes, at-least-once delivery with stable `message-id`, no silent loss
  (drops go to the failure listener / `FallbackChain`), graceful shutdown with `ShutdownTimeout`, optional topology
  declaration (exchange + quorum queue + binding), per-event routing key and message customisation.
- Messages rejected with `amqp:not-found` (target exchange/queue deleted) are kept and trigger a reconnect instead
  of being dropped.
- In-flight messages settled as `Rejected` because the connection was lost (`amqp:internal-error`
  "Connection forced", connection/session/link errors) are retried instead of dropped.
- Every event reported to the failure listener is also traced to `SelfLog`, even when a fallback sink is used.
- Publishes that get no broker outcome within `PublishTimeout` are traced to `SelfLog` before being retried.

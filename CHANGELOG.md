# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **Breaking (configuration):** the `appsettings` argument `hostnames` is now a JSON array (`["rabbit-1", "rabbit-2"]`);
  each element may still hold several nodes separated by `,` or `;` (e.g. from an environment variable
  `Serilog__WriteTo__0__Args__hostnames__0=rabbit-1;rabbit-2`). A plain string is no longer accepted.
- New pipeline (ADR 0004): `Emit` → burst channel → background formatter → byte-bounded outage buffer → publisher.
  Formatting never happens on application threads and the `LogEvent` is released right after formatting.
- **Breaking (behaviour):** `QueueLimit` no longer bounds the outage buffer — it now bounds only the short burst channel
  before formatting (default lowered from 100 000 to 10 000). The outage buffer is bounded by the new
  `QueueSizeLimitBytes` (default 15 MiB, approximate memory).
- `ConfigureMessage` replaced by `MessageProperties` (evaluated once per event on the formatter thread; values are
  validated there).
- Undelivered records are decoded back to `LogEvent`s only when the failure listener consumes events, in batches;
  decoding uses a built-in `System.Text.Json` CLEF reader (no Newtonsoft dependency).
- Publishing uses a sliding window instead of rounds: a message without an outcome no longer stalls the others.
- Retry spacing is tracked per message; `Released` messages are retried over ~31 s (default) instead of within
  milliseconds, and `amqp:resource-limit-exceeded` is retried without limit.
- `ShutdownTimeout` now bounds the whole shutdown, including closing publisher, connection and environment.
- Stricter option validation (ambiguous targets, `DeclareQueue`/`BindingKey` without their prerequisites, unknown
  exchange type); options are copied when the sink is created.
- `SelfLog` traces of dropped events are aggregated (one line per reason every 10 s).
- The `appsettings` overload accepts `contentType`, `publishTimeout`, `maxReleasedRetries`, `minReconnectDelay`,
  `maxReconnectDelay` and `shutdownTimeout`.

### Fixed

- A client recovery attempt that hangs (TCP connect to a node that vanished can take minutes) no longer stalls delivery:
  after `MaxReconnectDelay` + 15 s in `Reconnecting` the supervisor rebuilds the connection with its own bounded
  attempts. Found by flaky broker-restart tests on RabbitMQ 4.0–4.4.
- Integration test for a missing exchange failed on RabbitMQ 4.4, which refuses the attach with `amqp:not-found` (reported
  as a certain configuration error) instead of letting it time out like 4.0; both outcomes are accepted.
- A failed sink start no longer closes an environment passed in from outside (internal/test constructor).
- Client certificates replaced by a rotation are released one renewal later (on Windows this removes their key
  containers) instead of staying alive until garbage collection.
- The sink's configuration section is also found when `hostnames` is written as one string.

- A password could appear in an exception message: a plain host entry with credentials (`guest:secret@rabbit`) was
  reported as an invalid port with the whole entry, and a URI password with an unescaped `@` was redacted only up to the
  first `@`. Plain entries with credentials are now rejected with a redacted message; redaction hides everything up to
  the last `@`.
- Splitting `hostnames` elements by `,`/`;` no longer cuts a URI whose user name or password contains an unescaped
  separator.
- `Tls.UseExternalAuthentication` together with credentials in a host URI is rejected (it was only checked against
  `Username`/`Password`).
- The failure listener is no longer called after `DisposeAsync` returned (when a fallback was still stuck after the
  shutdown budget): late reports go to `SelfLog` only, because Serilog has closed the fallbacks by then.
- XML docs: `ShutdownTimeout` describes the whole-shutdown budget; the endpoint rules state that `Port` is a default
  and a port given with the host wins.

- On shutdown with the broker down, the whole buffered backlog reaches the fallbacks: `Final` batches are chunked and
  exempt from the fallback's pending limit, and the shutdown budget reserves time for the hand-over
  (deliver / hand over / close).
- An idle shutdown with the broker down returns immediately instead of waiting for the deadline.
- The fallback worker is stopped when its shutdown budget runs out, so nothing is written after `Dispose` returns.
- Events emitted after the sink was disposed no longer call listeners or write stdout on the application thread
  (and can no longer block it for up to 1 s): they are counted and noted once in `SelfLog`.
- When the fallback worker is stuck in a slow fallback at the end of shutdown, nothing is written without the output
  lock and the remaining events are still counted and reported as lost.
- Shutdown (`Final`) events are handed to the fallbacks before an earlier overflow backlog; events that still do not
  make it are reported as lost with kind `Final` (not `Permanent`); a late report after dispose that cannot get the
  output lock is counted as lost instead of writing unsynchronized.

- `WriteUndeliveredToStandardOutput` now covers every path (burst overflow, formatting failures, unformatted events at
  shutdown, `Emit` after dispose), not only formatted records.
- Fallbacks (failure listener, stdout) run on their own bounded worker, so a slow fallback no longer slows the
  formatter down or pushes drops onto application threads.
- Publishing work per message is O(log n) (completion queue, priority queue of retries, one reusable timer) instead of
  scanning the whole window on every completed publish.
- Configuration-error detection is stage-aware and ignores stack traces (a missing virtual host is no longer reported as
  a missing exchange); the attach heuristic needs 5 failures and is reported as probable (`Temporary`).
- Restored fallback events keep TraceId/SpanId; `decimal`/`char` message properties are sent as strings.
- Folder dependency cycles removed and guarded by an architecture test.

- An exception thrown by `ConfigureMessage` caused endless retries; such events are now dropped and reported.
- Right after a rebuild a closed publisher could be handed out while the old connection was being torn down.
- On shutdown abort, messages already accepted by the broker were also reported as undelivered (duplicates in the
  fallback sink).
- The environment created by the sink was not closed if construction failed.

### Added

- MIT license (`LICENSE`); the package license expression is now `MIT` (was Apache-2.0).
- Integration tests against the latest patch of each RabbitMQ minor: the current minors (4.3, 4.4) on every push and
  pull request, every supported minor (4.0–4.4) on demand (`integration-tests.yml`) and before every release; the
  broker image is selected with `RABBITMQ_IMAGE`. Brokers are shared by tests that do not disrupt them, disruptive tests run
  three at a time, fixed delays became condition waits and CI builds once: a full run per version takes ~1.5 min
  instead of 6–8.
- GitHub Actions: `ci.yml` (build with warnings as errors, unit and integration tests, pack) and `release.yml` (tag
  `vX.Y.Z` → tests → package with SourceLink symbols → nuget.org via Trusted Publishing → GitHub release).

- Secret rotation without a restart (ADR 0005): when this sink's `username`/`password` change on an `IConfiguration`
  reload, the sink reconnects with the new values (the `appsettings` overload receives `IConfiguration` from
  Serilog.Settings.Configuration and follows its own section); in code, `RabbitMqAmqpCredentialsSource` +
  `Credentials` do the same on `Update`. Renewed TLS client certificates and CA files are re-read at every TLS
  handshake. Event-driven, nothing is polled; verified against a real broker with the old secret revoked first.
- Dependency on `Microsoft.Extensions.Configuration.Abstractions` (for `IConfiguration` and change tokens).

- TLS settings, grouped in `Tls` (`RabbitMqAmqpTlsOptions`; in `appsettings.json` a `"tls": { ... }` object):
  `CaCertificatePath` (trust a private CA for this connection only), client certificates for mutual TLS
  (`ClientCertificatePath` + `ClientCertificateKeyPath`/`ClientCertificatePassword`, or `ClientCertificate` in code),
  `UseExternalAuthentication` (SASL EXTERNAL), `Protocols`, `AcceptablePolicyErrors`, `CheckCertificateRevocation` and
  `CertificateValidation` (code only). Broker certificates are validated by the sink because the client ignores
  `TlsSettings.AcceptablePolicyErrors`. Verified against a TLS broker (private CA, mutual TLS + EXTERNAL, untrusted CA).
- README reorganised for people adding the package: install, quick starts (code and `appsettings.json`), the most
  used options and a production checklist first; explanations moved to `docs/guide/` (configuration, TLS, message
  format, delivery, Kubernetes, options reference with all options in groups).
- `FallbackAfter`: events not accepted within that time are handed to the fallback instead of waiting for the broker.
- AMQP `correlation-id` set to the event's trace id (`CorrelationIdFromTraceId`, on by default) or to a value from
  `CorrelationIdSelector`.
- `DurableMessages` (default `true`) to publish non-durable messages.
- README: `WriteTo.FallbackChain` in `appsettings.json`; a fallback chain keeps working with `restrictedToMinimumLevel`
  (verified by tests).
- `samples/` console sample (code and `appsettings.json`) and `docker-compose.yml` with a local RabbitMQ 4.

- README: how to configure the sink from `appsettings.json` (`Serilog.Settings.Configuration`, host wiring) and how to
  choose the message format (type-name and object syntax for `formatter`, verified by tests).

- `Hostnames` (replaces `Uris`) accepts plain hosts (`rabbit-1`, `rabbit-1:5673`, `[::1]:5672`) as well as
  `amqp://` / `amqps://` URIs; credentials and connection details are separate options: `Username`, `Password`,
  `VirtualHost`, `Port`, `UseTls` (also in the `appsettings` overload as `hostnames`, `username`, `password`,
  `virtualHost`, `port`, `useTls`). Conflicting duplicates fail at logger creation; passwords never appear in messages.
  Virtual hosts containing `/` (e.g. `/data`) work: `VirtualHost` takes the name as is, URIs need `%2F`
  (`amqp://host/%2Fdata`), and ambiguous paths like `amqp://host//data` are rejected instead of silently becoming `data`.

- `WriteUndeliveredToStandardOutput`: undelivered events are also written as their formatted lines to stdout.
- Configuration errors (credentials/permissions, conflicting queue type, missing exchange, invalid exchange type) are
  recognised, reported once to the failure listener and as `CONFIGURATION ERROR` in `SelfLog`; retrying continues.

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

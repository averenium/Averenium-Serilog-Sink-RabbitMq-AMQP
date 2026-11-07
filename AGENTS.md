# AGENTS.md

Guide for AI coding agents (and humans) working in this repository.

## Project

**Averenium.Serilog.Sinks.RabbitMQ.Amqp** — a [Serilog](https://serilog.net) sink that publishes log events to
**RabbitMQ 4.x over AMQP 1.0** using the official
[`RabbitMQ.AMQP.Client`](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client).

Why it exists: the existing Serilog RabbitMQ sinks are built on `RabbitMQ.Client` (AMQP 0-9-1).
This library targets the AMQP 1.0 client exclusively. **Never add a dependency on `RabbitMQ.Client`.**

**Primary goal: reliable delivery** — resume after broker crashes/restarts/failovers of any length and
work with multi-node clusters. Read [docs/architecture.md](docs/architecture.md) before changing the
pipeline, connection or publishing code; it lists verified client/Serilog behaviours (C1–C8, S1–S4) the
design depends on.

## Naming

- Package ID, assembly and root namespace: **`Averenium.Serilog.Sinks.RabbitMQ.Amqp`**.
  It follows the Serilog `Serilog.Sinks.<Name>` convention, prefixed with the owner `Averenium.`
  (the bare `Serilog.` prefix belongs to the Serilog project on nuget.org).
- Configuration extension methods live in namespace `Serilog` so users need no extra `using`.
- New companion packages (if any) follow the same pattern, e.g. `Averenium.Serilog.Sinks.RabbitMQ.Amqp.<Feature>`.

## Layout

```
Averenium.Serilog.Sinks.RabbitMQ.Amqp.slnx      # solution (SLNX format)
global.json                               # pinned .NET SDK (10.0.x, latestFeature roll-forward)
Directory.Build.props                     # shared MSBuild settings (TFM, nullable, warnings-as-errors, package metadata)
Directory.Packages.props                  # Central Package Management — ALL package versions live here
src/
  Averenium.Serilog.Sinks.RabbitMQ.Amqp/        # the NuGet package
    PublicAPI.Shipped.txt                 # public API released in a published version
    PublicAPI.Unshipped.txt               # public API added since the last release
tests/
  Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests/              # fast unit tests, no broker, no Docker
  Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests/   # real RabbitMQ 4.x via Testcontainers (needs Docker)
docs/
  architecture.md                         # design of the sink
  adr/                                    # architecture decision records (NNNN-title.md)
CHANGELOG.md                              # Keep a Changelog format
```

## Commands

```bash
dotnet restore
dotnet build -c Release
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests              # unit tests (always run)
dotnet test --project tests/Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests   # needs Docker or Podman running
dotnet test --project <proj> --treenode-filter "/*/*/ConnectionSupervisorTests/*"   # filter (TUnit syntax, not VSTest --filter)
dotnet pack src/Averenium.Serilog.Sinks.RabbitMQ.Amqp -c Release -o artifacts
```

A change is done only when `dotnet build -c Release` has **zero warnings** (warnings are errors) and the unit tests pass.
Run integration tests whenever you touch connection, publishing, recovery or topology code; if Docker is
unavailable, say so explicitly instead of claiming they pass.

## Tech baseline

| Item | Value |
|---|---|
| Target framework | `net10.0` (single TFM; widen only via an ADR) |
| Language | latest C#, `Nullable` enabled, `ImplicitUsings` enabled |
| Logging core | `Serilog` 4.x — own buffering pipeline (ADR 0002); do **not** use Serilog's `BatchingSink`/`IBatchedLogEventSink` or `Serilog.Sinks.PeriodicBatching` |
| Transport | `RabbitMQ.AMQP.Client` 1.x (AMQP 1.0, RabbitMQ ≥ 4.0) |
| Tests | **TUnit** on Microsoft.Testing.Platform (set in `global.json`, no VSTest packages), awaited `Assert.That(...)` assertions, NSubstitute, `FakeTimeProvider`, Testcontainers.RabbitMq |
| Analyzers | `AnalysisLevel=latest-recommended`, code style enforced in build; test-only relaxations in `tests/.editorconfig` |
| Packages | Central Package Management — `<PackageReference>` **without** `Version` |

## Coding conventions

- File-scoped namespaces; namespace = folder path under `Averenium.Serilog.Sinks.RabbitMQ.Amqp`.
- One top-level type per file, file named after the type.
- `_camelCase` private fields, `PascalCase` for everything public, `I` prefix for interfaces, `Async` suffix for async methods.
- Types are `sealed` and `internal` by default. Make something `public` only when users need it.
- All I/O is async (`Task`/`ValueTask`), with `CancellationToken` where the caller can cancel. No `.Result`, `.Wait()`, `GetAwaiter().GetResult()` except in `Dispose()` where no async alternative exists (prefer `IAsyncDisposable`).
- `ConfigureAwait(false)` on every `await` in library code (not required in tests).
- XML doc comments on every public member (`GenerateDocumentationFile` is on; missing docs fail the build).
- No new analyzer suppressions without a justification comment. Prefer fixing the code.

## Public API rules

- The public surface is tracked by `Microsoft.CodeAnalysis.PublicApiAnalyzers`.
  Adding/changing a public member → update `PublicAPI.Unshipped.txt` (the analyzer code fix does it).
- Breaking changes to anything in `PublicAPI.Shipped.txt` require a major version bump and a CHANGELOG entry under **Breaking**.
- Entry points follow Serilog conventions:
  - `LoggerSinkConfiguration` extension `WriteTo.RabbitMQAmqp(...)` in namespace `Serilog`.
  - Options type(s) bindable from `Serilog.Settings.Configuration` (`appsettings.json`) — plain settable properties, no required ctor args.
- Don't leak `RabbitMQ.AMQP.Client` implementation types (`Amqp*` classes) through our public API. Interfaces
  from the client (`IConnection`, `IEnvironment`) may be accepted as optional inputs for advanced scenarios.

## Sink behaviour rules (non-negotiable)

1. **Never crash or block the app.** `Emit` only enqueues; no I/O, no throwing. Logger creation must not
   block or fail because the broker is down (connect lazily, in the background).
2. **No silent loss.** An event leaves the pipeline only when the broker returns `Accepted`, or when it is
   reported to `ILoggingFailureListener` as `Permanent`/`Final` (also `SelfLog`). This makes
   `WriteTo.FallbackChain(...)` work.
3. **At-least-once.** Transient failures (not open, timeouts, connection errors) are retried with **no time
   limit**; only not-yet-accepted messages are re-sent. Each message has a unique `message-id`.
   `Released` → limited retries; `Rejected` with `amqp:not-found` → keep + rebuild (target missing);
   `Rejected` with connection/session/link errors (e.g. `amqp:internal-error` "Connection forced") → retry
   without limit; only rejections about the message itself are permanent. Classification lives in `OutcomeClassifier`.
   When unsure, prefer retry (possible duplicate) over drop (lost log).
4. **Bounded memory.** `QueueLimit` caps buffered events; overflow follows `OverflowPolicy` and is reported.
   **No disk I/O** — target is containers/Kubernetes without writable local disk; never add a disk buffer or temp files.
5. **Recovery = client recovery + supervisor (ADR 0003).** Client recovery stays on with an endless back-off
   and round-robin node selection; `ConnectionSupervisor` rebuilds connection/publisher when the client gives
   up or a publisher is stuck. Ready = connection `Open` **and** publisher `Open`.
6. **Never trust the client to complete a publish.** Every `PublishAsync` is bounded by our own timeout
   (`Task.WaitAsync`) — the client ignores cancellation.
7. **Graceful shutdown.** Drain until empty or `ShutdownTimeout`, report the rest as `Final`, then close
   publisher → connection → environment.
8. **Thread safety.** `Emit` is called concurrently; all publishing happens on the single `PublishLoop`.
9. **Topology declaration is opt-in.** By default the sink only publishes to an existing address.

## Testing rules

- Bug fix → first a failing test reproducing it, then the fix.
- New feature → unit tests for logic (formatting, options validation, batching/outcome handling via fakes)
  plus at least one integration test against a real broker.
- Unit tests must not touch the network or Docker. Abstract the transport behind an internal interface so it can be faked.
- Use TUnit: `[Test]`, `[Arguments]`, `[Before/After(Test)]`, `ClassDataSource` for shared fixtures; get the
  test `CancellationToken` as a test-method parameter and pass it on. Tests run in parallel by default —
  no shared mutable state; mark broker-disrupting classes `[NotInParallel]`.
- Integration tests use `RabbitMqNode` (image `rabbitmq:4-management`, fixed host port so it can be stopped/started)
  and `SupervisorHarness`; each test uses `Topology.Unique()` names and owns the brokers it disrupts.
  Podman works: its machine exposes the Docker API pipe Testcontainers uses.
- Resilience is a feature and must be tested: broker stop/start, broker down at start-up, connection
  force-closed (management API), multi-node failover (3-node cluster fixture), publisher-only failure,
  overflow, shutdown with pending events. Assert "no silent loss": accepted + reported-dropped = emitted.
- Time-dependent logic takes `TimeProvider` so unit tests use `FakeTimeProvider` instead of real delays.
- Test names: a plain-English sentence with underscores; one behaviour per test.

## Documentation rules

- User-facing change → update `README.md` (usage) and `CHANGELOG.md` (`## [Unreleased]`).
- Design decision that is hard to reverse (dependency, TFM, delivery semantics, message format) → new ADR in `docs/adr/`.
- Keep `docs/architecture.md` in sync with the code.

## Git

- Default branch `main`. Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `build:`, `chore:`).
- Don't commit, push or tag unless asked. Never commit secrets or broker credentials; tests use container-generated credentials.

## Reference

- AMQP client repo and its agent guide: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client (see `AGENTS.md`, `docs/Examples/`).
  Key concepts: `AmqpEnvironment.Create(settings)` is the entry point → `IConnection` → `PublisherBuilder()` → `IPublisher.PublishAsync(message)` returning a `PublishResult` with an outcome.
- Serilog sink authoring: https://github.com/serilog/serilog/wiki/Developing-a-sink

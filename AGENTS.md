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
.github/workflows/                        # ci.yml (standard check), integration-tests.yml (broker matrix), release.yml (tag → nuget.org)
global.json                               # pinned .NET SDK (10.0.x, latestFeature roll-forward)
Directory.Build.props                     # shared MSBuild settings (TFM, nullable, warnings-as-errors, package metadata)
Directory.Packages.props                  # Central Package Management — ALL package versions live here
src/
  Averenium.Serilog.Sinks.RabbitMQ.Amqp/        # the NuGet package
    RabbitMqAmqpLoggerConfigurationExtensions.cs  # public: WriteTo.RabbitMQAmqp (namespace Serilog — the one exception)
    RabbitMqAmqpSink.cs                   # internal composition root (wires the folders below)
    Options/                              # public models: RabbitMqAmqpSinkOptions, RabbitMqAmqpTlsOptions, RabbitMqAmqpCredentialsSource, OverflowPolicy (root namespace!)
    Buffering/                            # EventBuffer (burst channel, count) + SizeBoundedQueue<T> (outage buffer, size)
    Formatting/                           # FormatterLoop, LogEventFormatter, ClefDecoder, UndeliveredEvents (fallbacks)
      Models/                             # data: FormattedEvent, TextException
    Publishing/                           # actions: PublishLoop, AmqpMessageBuilder, OutcomeClassifier; IPublisherProvider
      Models/                             # data: PendingMessage, OutcomeKind, PublishLoopOptions
    Connection/                           # actions: ConnectionSupervisor, settings factory, TLS configuration, endless back-off, URI selector
      Models/                             # data: SupervisorState, ConnectionSupervisorOptions
    Topology/                             # TopologyDeclarer (exchange / quorum queue / binding)
    Resilience/                           # ExponentialBackoff (generic, used by Connection and Publishing)
    Diagnostics/                          # FailureReporter: the single "no silent loss" exit
    PublicAPI.Shipped.txt                 # public API released in a published version
    PublicAPI.Unshipped.txt               # public API added since the last release
tests/
  Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests/              # fast unit tests, no broker, no Docker
    <Folder>/<Type>Tests.cs               # mirrors src: one test class per production class
    TestDoubles/                          # fakes and collectors (FakeEnvironment, FakeConnection, Collecting*)
    Support/                              # helpers (TestEvents, Eventually)
  Averenium.Serilog.Sinks.RabbitMQ.Amqp.IntegrationTests/   # real RabbitMQ 4.x via Testcontainers (needs Docker/Podman)
    *Tests.cs                             # scenario test classes
    Infrastructure/                       # RabbitMqNode, TestTopology, SupervisorHarness, Eventually
samples/
  Averenium.Serilog.Sinks.RabbitMQ.Amqp.Sample/   # runnable console sample (code + appsettings.json); builds with the solution
docker-compose.yml                        # local RabbitMQ 4 for the sample (not used by tests)
docs/
  guide/                                  # user guides linked from README (configuration, tls, message-format, delivery, kubernetes, options)
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
| Configuration | `Microsoft.Extensions.Configuration.Abstractions` (only `IConfiguration` + change tokens, ADR 0005) |
| Tests | **TUnit** on Microsoft.Testing.Platform (set in `global.json`, no VSTest packages), awaited `Assert.That(...)` assertions, NSubstitute, `FakeTimeProvider`, Testcontainers.RabbitMq |
| Analyzers | `AnalysisLevel=latest-recommended`, code style enforced in build; test-only relaxations in `tests/.editorconfig` |
| Packages | Central Package Management — `<PackageReference>` **without** `Version` |

## Coding conventions

- File-scoped namespaces; **namespace = root namespace + folder path** (enforced in build by `IDE0130`).
  Exceptions (each marked with `#pragma` + comment), don't add others:
  `RabbitMqAmqpLoggerConfigurationExtensions` is in namespace `Serilog` (Serilog convention);
  `Options/*` keep the root namespace so users need no extra `using`.
- **Models are separated from actions.** Data types (records/DTOs, options, enums, state) go to a `Models/`
  subfolder of their feature (`Publishing/Models`, `Connection/Models`, …) with namespace `…<Feature>.Models`;
  classes with behaviour stay in the feature folder. Public models go to `Options/`. Interfaces describing
  behaviour (e.g. `IPublisherProvider`) are contracts, not models — they stay next to the actions.
- One top-level type per file, file named after the type (tests included; nested private helpers are fine).
- Folders are layers with one-way dependencies: `Connection` → `Publishing` → `Formatting` → `Buffering`/`Diagnostics`
  (+ `Resilience`). `Diagnostics`, `Resilience` and `Buffering` depend on no other folder — pass callbacks or make types
  generic instead (see `EventBuffer`, `SizeBoundedQueue<T>`). `Connection` may implement interfaces owned by
  `Publishing` (`IPublisherProvider`), never the other way round. **`FolderDependencyTests` enforces this** — the
  compiler and `IDE0130` do not.
  Folders may use the public contract types from the root (`RabbitMqAmqpSinkOptions`, `OverflowPolicy`),
  never the composition root `RabbitMqAmqpSink`.
- The src root folder holds only the configuration extensions and the composition root `RabbitMqAmqpSink`.
- Avoid type names equal to a namespace segment (`Topology`, `Connection`, …) — they shadow each other.
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
4. **Bounded memory.** The outage buffer holds formatted bytes, bounded by `QueueSizeLimitBytes` (ADR 0004); the burst
   channel (`QueueLimit`) only absorbs spikes before formatting. Never hold a `LogEvent` and its bytes at the same
   time, never format on application threads. Overflow follows `OverflowPolicy` and is reported in batches.
   **No disk I/O** — target is containers/Kubernetes without writable local disk; never add a disk buffer or temp files.
   The only allowed file access is *reading* configured TLS certificate files (mounted secrets) at logger creation and
   at TLS handshakes, so renewed certificates are picked up (ADR 0005). Never poll files, never watch with timers.
5. **Recovery = client recovery + supervisor (ADR 0003).** Client recovery stays on with an endless back-off
   and round-robin node selection; `ConnectionSupervisor` rebuilds connection/publisher when the client gives
   up or a publisher is stuck. Ready = connection `Open` **and** publisher `Open`.
6. **Never trust the client to complete a publish.** Every `PublishAsync` is bounded by our own timeout
   (`Task.WaitAsync`) — the client ignores cancellation.
7. **Graceful shutdown within one deadline.** `ShutdownTimeout` is the target for the whole shutdown, split into
   deliver / hand over to fallbacks / close, so a down broker never starves the fallback hand-over (`Final` batches go
   first). Never add per-step timeouts on top. An idle shutdown must return immediately.
8. **Thread safety.** `Emit` is called concurrently; all publishing happens on the single `PublishLoop`.
9. **Topology declaration is opt-in.** By default the sink only publishes to an existing address.
10. **Retry state is per message.** Spacing comes from the message's own schedule, never from a loop-wide back-off
   that other messages' success can reset. The publish window bounds memory (in flight + retry ≤ `BatchSizeLimit`).
11. **Configuration errors are loud, not fatal.** Recognise them (`ConfigurationErrorDetector`, stage-aware, messages
   only — never stack traces), report once per distinct reason (heuristics as *probable*), keep retrying.
12. **Fallbacks never slow the pipeline.** Every undelivered event goes through `UndeliveredEvents` (own worker,
   bounded); never call a failure listener or write stdout on the `Emit` or formatter thread — not even after
   dispose: events emitted after `Dispose` are only counted (`EmittedAfterDispose`, one `SelfLog` line), because the
   fallbacks are closed together with the sink. Never write stdout without the output lock.
13. **Message property values are validated where user code runs** (formatter thread); only types covered by
   `MessagePropertyEncodingTests` (client encoding) and the broker e2e test are allowed.

## Testing rules

- Bug fix → first a failing test reproducing it, then the fix.
- New feature → unit tests for logic (formatting, options validation, batching/outcome handling via fakes)
  plus at least one integration test against a real broker.
- Unit tests must not touch the network or Docker. Abstract the transport behind an internal interface so it can be faked.
- Use TUnit: `[Test]`, `[Arguments]`, `[Before/After(Test)]`, `ClassDataSource` for shared fixtures; get the
  test `CancellationToken` as a test-method parameter and pass it on. Tests run in parallel by default —
  no shared mutable state. Integration tests that only use their own unique topology run on the session-wide
  `SharedRabbitMq` / `SharedTlsRabbitMq` (`ClassDataSource`, `SharedType.PerTestSession`); tests that stop, restart or
  otherwise disrupt a broker own their `RabbitMqNode` and are marked `[ParallelLimiter<DisruptiveBrokerTests>]` (at most
  3 at once). Wait for conditions (a message count, a state, a closed connection), not fixed delays.
- Integration tests use `RabbitMqNode` (image from `RABBITMQ_IMAGE`, default `rabbitmq:4-management`; fixed host port
  so it can be stopped/started). Two levels, both through `.github/workflows/integration-tests.yml` (latest patch of
  each listed minor):
  - **standard** — every push / pull request (`ci.yml`): the current minors only (4.3, 4.4);
  - **extended** — every supported minor (4.0–4.4): run it by hand (Actions → *Integration tests (extended)* → Run
    workflow) before rolling out a new feature; every release runs it too (`release.yml`).
  Broker behaviour differs between minors (e.g. C10), so a test must pass on all of them. Run one minor locally with
  `RABBITMQ_IMAGE=rabbitmq:4.1-management dotnet test --project …IntegrationTests`.
  A new RabbitMQ minor → add it to the standard list in `ci.yml`, the extended lists in `integration-tests.yml` and
  `release.yml`; move the oldest minor out of the standard list.
  and `SupervisorHarness`; each test uses `Topology.Unique()` names and owns the brokers it disrupts.
  Podman works: its machine exposes the Docker API pipe Testcontainers uses.
- Resilience is a feature and must be tested: broker stop/start, broker down at start-up, connection
  force-closed (management API), multi-node failover (3-node cluster fixture), publisher-only failure,
  overflow, shutdown with pending events. Assert "no silent loss": accepted + reported-dropped = emitted.
- Time-dependent logic takes `TimeProvider` so unit tests use `FakeTimeProvider` instead of real delays.
- Test names: a plain-English sentence with underscores; one behaviour per test.

## Documentation rules

- User-facing change → update the matching guide in `docs/guide/` and `CHANGELOG.md` (`## [Unreleased]`).
  `README.md` holds only what someone adding the package needs first (features with links to their guides, install,
  quick starts, the most used options, production checklist); explanations go into `docs/guide/*.md`, background to
  the bottom of the README. A new feature gets one line in the README's **Features** list, linking to its guide.
- Design decision that is hard to reverse (dependency, TFM, delivery semantics, message format) → new ADR in `docs/adr/`.
- Keep `docs/architecture.md` in sync with the code.

## Git

- Default branch `main`. Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `build:`, `chore:`).
- Don't commit, push or tag unless asked. Never commit secrets or broker credentials; tests use container-generated credentials.

## Releasing

Releases are published by `.github/workflows/release.yml` when a `vX.Y.Z` (or `vX.Y.Z-preview.N`) tag is pushed;
`ci.yml` builds and tests every push and pull request. Before tagging a commit on `main`:

1. `CHANGELOG.md`: rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and add a new empty `[Unreleased]`.
2. Move the lines of `PublicAPI.Unshipped.txt` into `PublicAPI.Shipped.txt` (keep `#nullable enable` at the top).
3. Commit, then `git tag vX.Y.Z && git push origin vX.Y.Z` — only when the maintainer asks for it.

The workflow takes the version from the tag, builds and runs all tests again, rewrites the README's relative links to
the tag on GitHub (nuget.org does not resolve them), packs `.nupkg` + `.snupkg` and pushes them with nuget.org
Trusted Publishing (OIDC — no API key secret), then creates the GitHub release. One-time setup: a Trusted Publishing
policy on nuget.org for this repository and `release.yml`, the repository variable `NUGET_USER`, and optionally
required reviewers on the `nuget-org` environment.

## Reference

- AMQP client repo and its agent guide: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client (see `AGENTS.md`, `docs/Examples/`).
  Key concepts: `AmqpEnvironment.Create(settings)` is the entry point → `IConnection` → `PublisherBuilder()` → `IPublisher.PublishAsync(message)` returning a `PublishResult` with an outcome.
- Serilog sink authoring: https://github.com/serilog/serilog/wiki/Developing-a-sink

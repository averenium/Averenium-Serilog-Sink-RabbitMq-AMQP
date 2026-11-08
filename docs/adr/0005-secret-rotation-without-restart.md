# ADR 0005: Rotating credentials and certificates without a restart

- **Status:** Accepted (implemented)
- **Date:** 2026-11-08

## Context

In Kubernetes, secrets rotate while pods run: a password changes in a secret (surfaced to the app through
`IConfiguration` — key-per-file, JSON with `reloadOnChange`, Key Vault / Vault providers) and cert-manager renews
mounted certificates. A logging sink that only reads them at start-up stops delivering after the next reconnect with an
outdated secret, until someone restarts the pod — the opposite of the sink's primary goal.

Verified client behaviour (C12, RabbitMQ.AMQP.Client 1.4.1 source):

- User name and password are baked into the `Address` objects when the connection settings are built
  (`ClusterConnectionSettings` keeps a URI → `Address` map). The client's own recovery reconnects with them forever.
- `TlsSettings.ClientCertificates`, `LocalCertificateSelectionCallback` and `RemoteCertificateValidationCallback` are
  handed to every new connection, so callbacks run at each TLS handshake.
- Serilog (and Serilog.Settings.Configuration) never rebuild sinks on a configuration reload.

## Decision

- **Credentials are pushed, not polled.** `RabbitMqAmqpCredentialsSource` (public) holds user name and password;
  `Update` raises an event only when the values really change. The supervisor reacts by tearing down the connection,
  replacing the environment (`IEnvironmentSource`) with one built from the new credentials and reconnecting — also when
  the client is stuck reconnecting with the old ones, and cutting a pending rebuild back-off short.
- **`appsettings.json` works without any user code.** The flat overload takes `IConfiguration` (filled in by
  Serilog.Settings.Configuration, not a configuration argument), finds its own `Args` section (an entry with
  `Name = RabbitMQAmqp` whose user name, password, hosts and target equal the values it was created with — also inside
  `FallbackChain`) and subscribes to the reload token (`ChangeToken.OnChange`). If the section is not found or is
  ambiguous, rotation via configuration is off and SelfLog says so.
- **Certificates are re-read at each TLS handshake.** The CA file and the client certificate (`ReloadingFiles<T>`) are
  read again when a connection is made and parsed only if the bytes changed; a file caught mid-update keeps the previous
  value. No file watchers, no timers. An established TLS connection keeps working; the renewed certificate is used on
  the next (re)connect — which is when it matters.
- New dependency: **Microsoft.Extensions.Configuration.Abstractions** (lowest 10.x), for `IConfiguration` and change
  tokens. It is what Serilog.Settings.Configuration users already have; code-only users get a small abstractions package.

## Consequences

- A rotation reconnects once; buffered events are kept and delivered afterwards (at-least-once, no loss — verified by
  integration tests that revoke the old secret on the broker first).
- Ordering of a rotation is the operator's choice: if the new password reaches the app before the broker, the sink
  reconnects, is refused (reported once as a configuration error) and connects as soon as the broker accepts it.
- Reading secret files at handshakes is the only file access besides start-up; nothing is ever written (AGENTS.md rule 4).
- Other settings (hosts, virtual host, TLS options) still require a restart when they change.

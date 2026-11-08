# Options reference

Every option is a property of `RabbitMqAmqpSinkOptions` in code and a camelCase argument in `appsettings.json`
(`QueueSizeLimitBytes` → `queueSizeLimitBytes`). TLS settings are one object: `o.Tls.CaCertificatePath` in code,
`"tls": { "caCertificatePath": ... }` in JSON. Options marked *code only* have no `appsettings` argument.
Time spans are written as `hh:mm:ss[.fff]`, flag enums as comma-separated names (`"Tls12, Tls13"`).

[← README](../../README.md)

## Connection

| Option | Default | Description |
|---|---|---|
| `Hostnames` | — | nodes: `host`, `host:port` or `amqp(s)://` URIs (same virtual host); a JSON array in `appsettings` — see [Configuration](configuration.md#hosts) |
| `Username` / `Password` | — | credentials for hosts without their own; followed on configuration reload |
| `Credentials` | — | `RabbitMqAmqpCredentialsSource` for rotating credentials in code (*code only*) |
| `VirtualHost` | `/` | virtual host |
| `Port` | 5672 (5671 with TLS) | default port for hosts given without one |
| `UseTls` | `false` | `amqps` for hosts given without a scheme |
| `ConnectionName` | `<entry assembly>@<machine>` | shown in the management UI |
| `MinReconnectDelay` / `MaxReconnectDelay` | 0.5 s / 30 s | reconnect back-off; reconnecting never stops |

## Target and routing

| Option | Default | Description |
|---|---|---|
| `Exchange` + `RoutingKey`, or `Queue` | — | publish target (exactly one of `Exchange` / `Queue`) |
| `RoutingKeySelector` | — | per-event routing key, needs `Exchange` (*code only*) |
| `DeclareTopology` | `false` | declare the exchange (and `DeclareQueue`) after every connect |
| `ExchangeType` | `topic` | type of the declared exchange |
| `DeclareQueue` / `BindingKey` | — / `RoutingKey` or `#` | quorum queue to declare and bind to the exchange |

## Message

| Option | Default | Description |
|---|---|---|
| `Formatter` / `ContentType` | compact JSON / `application/json` | body format — see [Message format](message-format.md) |
| `MessageProperties` | — | extra application properties per event (*code only*) |
| `CorrelationIdFromTraceId` | `true` | AMQP `correlation-id` = the event's trace id |
| `CorrelationIdSelector` | — | your own `correlation-id` per event (*code only*) |
| `DurableMessages` | `true` | durable (persistent) messages; `false` trades safety on broker restart for speed with classic queues |

## Buffering

| Option | Default | Description |
|---|---|---|
| `QueueSizeLimitBytes` | 15 MiB | outage buffer: approximate memory for formatted events waiting for delivery |
| `QueueLimit` | 10 000 | burst buffer: events waiting for the background formatter (stays almost empty) |
| `OverflowPolicy` | `DropOldest` | `DropOldest` keeps recent logs, `DropNewest` keeps the start of an incident |

## Delivery and fallback

| Option | Default | Description |
|---|---|---|
| `BatchSizeLimit` | 500 | sliding window: messages in flight or waiting for a retry |
| `PublishTimeout` | 30 s | wait for one broker outcome before retrying |
| `MaxReleasedRetries` | 5 | retries for messages the broker released (e.g. unroutable) |
| `FallbackAfter` | — (wait for the broker) | hand events not accepted within this time to the fallback |
| `WriteUndeliveredToStandardOutput` | `false` | also write undelivered events as formatted lines to stdout |

## Shutdown

| Option | Default | Description |
|---|---|---|
| `ShutdownTimeout` | 10 s | target for the **whole** shutdown — see [Delivery → Shutdown](delivery.md#shutdown) |

## TLS (`Tls`)

| Option | Default | Description |
|---|---|---|
| `Tls.CaCertificatePath` | — | CA certificate(s) that issued the broker certificate; trusted for this connection only |
| `Tls.ClientCertificatePath` | — | client certificate for mutual TLS: `.pfx`/`.p12` or PEM |
| `Tls.ClientCertificateKeyPath` | — | PEM key, when it is not in the certificate file |
| `Tls.ClientCertificatePassword` | — | password of the `.pfx` or of an encrypted PEM key |
| `Tls.ClientCertificate` | — | client certificate as `X509Certificate2` (*code only*) |
| `Tls.UseExternalAuthentication` | `false` | SASL EXTERNAL: authenticate with the client certificate |
| `Tls.Protocols` | OS default | allowed TLS versions, e.g. `Tls12, Tls13` |
| `Tls.AcceptablePolicyErrors` | `None` | certificate errors to tolerate, e.g. `RemoteCertificateNameMismatch` when connecting by IP |
| `Tls.CheckCertificateRevocation` | `false` | check the broker certificate's revocation status |
| `Tls.CertificateValidation` | — | your own validation callback (*code only*; replaces `CaCertificatePath` and `AcceptablePolicyErrors`) |

## Validation

Checked at logger creation, with a clear message (passwords are never included):

- exactly one of `Exchange` / `Queue`; `RoutingKey` and `RoutingKeySelector` only with `Exchange`;
- `DeclareQueue` needs `DeclareTopology` and `Exchange`; `BindingKey` needs `DeclareQueue`;
- `ExchangeType` is `topic`, `direct`, `fanout`, `headers` or a plugin type (`x-...`);
- `FallbackAfter` is positive;
- hosts, credentials and virtual host do not conflict — see [Configuration](configuration.md#hosts);
- `Credentials` is not combined with `Username`/`Password`;
- `Tls` settings need a TLS host, certificate files exist, `Tls.UseExternalAuthentication` needs a client certificate
  and no user/password.

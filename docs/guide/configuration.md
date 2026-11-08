# Configuration

[← README](../../README.md) · [Options reference](options.md)

- [Hosts](#hosts)
- [Credentials and virtual host](#credentials-and-virtual-host)
- [Rotating the password without a restart](#rotating-the-password-without-a-restart)
- [appsettings.json](#appsettingsjson)

## Hosts

`Hostnames` entries may be plain hosts or full URIs, mixed freely:

| Entry | Means |
|---|---|
| `rabbit-1` | `amqp://rabbit-1:5672/` (`amqps` and port 5671 with `UseTls`; port from `Port` if set) |
| `rabbit-1:5673`, `10.0.0.5:5672`, `[::1]:5672` | host with its own port |
| `amqp://rabbit-1:5672/vhost`, `amqps://...` | a full URI; may carry `user:pass@` |

List every cluster node (or a load balancer): the sink fails over between them round-robin. `Port` is only the
**default** for hosts written without a port — a port given with the host wins, so nodes may listen on different ports.

## Credentials and virtual host

Credentials, virtual host, port and TLS are separate options (`Username`, `Password`, `VirtualHost`, `Port`, `UseTls`)
and apply to every host that does not specify them itself. Any characters are allowed in user name and password.

Conflicting duplicates fail at logger creation with a clear message (passwords are never included): credentials both in
a URI and in `Username`, a virtual host in a URI path that differs from `VirtualHost`, `UseTls` with an `amqp://` URI,
different virtual hosts across hosts. Credentials go into `Username`/`Password` or a full URI — a plain entry such as
`user:pass@rabbit-1` is rejected.

**Virtual hosts with `/` in the name** (e.g. `/data`): set `VirtualHost = "/data"` — the name is used as is and encoded
for you. In a URI a `/` inside the virtual host must be written as `%2F`: `amqp://rabbit-1/%2Fdata` is `/data`, while
`amqp://rabbit-1/data` is `data` (AMQP URI rules). Paths that would silently lose a slash, such as
`amqp://rabbit-1//data`, are rejected with a hint.

## Rotating the password without a restart

- **`appsettings.json` / `IConfiguration`** — nothing to do. When the configuration reloads (JSON with
  `reloadOnChange`, a Kubernetes secret through a key-per-file provider, Azure Key Vault / Vault providers with reload)
  and this sink's `username` or `password` changed, the sink reconnects with the new values. Buffered events are kept
  and delivered afterwards. The sink finds its own entry in the configuration; if it cannot (e.g. two identical
  RabbitMQ sinks) it writes a SelfLog line and the change needs a restart.
- **In code** — use a `RabbitMqAmqpCredentialsSource` instead of `Username`/`Password` and call `Update` when the
  secret changes. Nothing is polled; the sink reacts to the call:

```csharp
var credentials = new RabbitMqAmqpCredentialsSource(settings.CurrentValue.User, settings.CurrentValue.Password);
settings.OnChange(s => credentials.Update(s.User, s.Password));   // IOptionsMonitor<RabbitSettings>

.WriteTo.RabbitMQAmqp(o =>
{
    o.Hostnames.Add("rabbit-1");
    o.Credentials = credentials;
    o.Exchange = "logs";
})
```

The order of a rotation does not matter for logging: RabbitMQ keeps existing connections open when a password
changes; if the app gets the new password before the broker does, the sink reconnects, is refused (reported once as a
configuration error) and connects as soon as the broker accepts it.

## appsettings.json

Configuration files are read by [Serilog.Settings.Configuration](https://github.com/serilog/serilog-settings-configuration)
(also included in `Serilog.AspNetCore` and `Serilog.Extensions.Hosting`):

```csharp
// plain console app / worker
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)          // IConfiguration with the "Serilog" section
    .CreateLogger();

// ASP.NET Core / generic host
builder.Services.AddSerilog((services, logger) => logger
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services));
```

- `"Using"` must list `Averenium.Serilog.Sinks.RabbitMQ.Amqp`.
- Arguments are the [options](options.md) in camelCase, plus Serilog's `restrictedToMinimumLevel`.
- `hostnames` is a **JSON array**. An element may hold several nodes separated by `,` or `;` (a separator inside the
  user name or password of a URI is kept), handy for environment variables:
  `Serilog__WriteTo__0__Args__hostnames__0=rabbit-1;rabbit-2;rabbit-3`. A plain string (`"hostnames": "rabbit-1"`) is not
  accepted by Serilog.Settings.Configuration for an array argument — logger creation fails with
  "Invalid cast from 'System.String' to 'System.String[]'".
- Secrets: set `password` from the environment (`Serilog__WriteTo__0__Args__password`) or a secret provider, not in
  the file.

All arguments in one example:

```json
{
  "Serilog": {
    "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "hostnames": [ "rabbit-1", "rabbit-2", "rabbit-3" ],
          "username": "app",
          "virtualHost": "/",
          "exchange": "logs",
          "routingKey": "billing",
          "declareTopology": true,
          "declareQueue": "logs.billing",
          "formatter": "Serilog.Formatting.Compact.RenderedCompactJsonFormatter, Serilog.Formatting.Compact",
          "queueSizeLimitBytes": 15728640,
          "fallbackAfter": "00:01:00",
          "writeUndeliveredToStandardOutput": true,
          "shutdownTimeout": "00:00:15",
          "useTls": true,
          "tls": { "caCertificatePath": "/etc/rabbitmq-ca/ca.crt" }
        }
      }
    ]
  }
}
```

`WriteTo.FallbackChain` in JSON (Serilog 4.1+, Serilog.Settings.Configuration 9.0+) is shown in the
[README](../../README.md#quick-start--appsettingsjson); the fallback receives every undelivered event, also when the
sink has `restrictedToMinimumLevel`.

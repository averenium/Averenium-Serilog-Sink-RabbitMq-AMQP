# Message format

[← README](../../README.md) · [Options reference → Message](options.md#message)

## What a consumer receives

One event per message:

| AMQP field | Value |
|---|---|
| body | the event formatted by `Formatter` (default compact JSON / CLEF), UTF-8, trailing line break removed |
| `message-id` | unique per event, stable across retries — use it to de-duplicate (at-least-once delivery) |
| `content-type` | `ContentType` (default `application/json`) |
| `creation-time` | event timestamp (UTC) |
| `correlation-id` | the event's W3C trace id when it has one, or `CorrelationIdSelector`; off with `CorrelationIdFromTraceId = false` |
| application property `level` | `Information`, `Error`, ... |
| application property `source-context` | `SourceContext` of the event, if any |
| durable | `DurableMessages` (default yes) |

Order is not guaranteed — sort by the event timestamp (`@t` or `creation-time`) if it matters.

## Choosing the formatter

Without `formatter` the sink uses **compact JSON (CLEF)** — `Serilog.Formatting.Compact.CompactJsonFormatter`.
In `appsettings.json` a formatter is an assembly-qualified **type name**, or an **object** with `type` plus constructor
arguments. Complete examples:

**CLEF with the rendered message** (`@m` instead of the template `@mt`):

```json
{
  "Serilog": {
    "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "hostnames": [ "rabbit-1", "rabbit-2" ],
          "username": "app",
          "exchange": "logs",
          "routingKey": "billing",
          "formatter": "Serilog.Formatting.Compact.RenderedCompactJsonFormatter, Serilog.Formatting.Compact"
        }
      }
    ]
  }
}
```

**Serilog's classic JSON**, with the rendered message (constructor argument `renderMessage`):

```json
{
  "Serilog": {
    "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "hostnames": [ "rabbit-1", "rabbit-2" ],
          "username": "app",
          "exchange": "logs",
          "routingKey": "billing",
          "formatter": {
            "type": "Serilog.Formatting.Json.JsonFormatter, Serilog",
            "renderMessage": true
          }
        }
      }
    ]
  }
}
```

**Your own JSON shape** — needs the `Serilog.Expressions` package:

```json
{
  "Serilog": {
    "Using": [ "Serilog.Expressions", "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "hostnames": [ "rabbit-1", "rabbit-2" ],
          "username": "app",
          "exchange": "logs",
          "routingKey": "billing",
          "formatter": {
            "type": "Serilog.Templates.ExpressionTemplate, Serilog.Expressions",
            "template": "{ {timestamp: @t, level: @l, message: @m, exception: @x, ..@p} }"
          },
          "contentType": "application/json"
        }
      }
    ]
  }
}
```

**Plain text** — e.g. for consumers that only display lines:

```json
{
  "Serilog": {
    "Using": [ "Serilog.Expressions", "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
    "WriteTo": [
      {
        "Name": "RabbitMQAmqp",
        "Args": {
          "hostnames": [ "rabbit-1", "rabbit-2" ],
          "username": "app",
          "exchange": "logs",
          "routingKey": "billing",
          "formatter": {
            "type": "Serilog.Templates.ExpressionTemplate, Serilog.Expressions",
            "template": "{@t:yyyy-MM-dd HH:mm:ss.fff} [{@l:u3}] {SourceContext}: {@m}{#if @x is not null} {@x}{#end}"
          },
          "contentType": "text/plain"
        }
      }
    ]
  }
}
```

The same in code:

```csharp
using Serilog.Templates;

Log.Logger = new LoggerConfiguration()
    .WriteTo.RabbitMQAmqp(o =>
    {
        o.Hostnames.Add("rabbit-1");
        o.Hostnames.Add("rabbit-2");
        o.Username = "app";
        o.Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD");
        o.Exchange = "logs";
        o.RoutingKey = "billing";
        o.Formatter = new ExpressionTemplate("{ {timestamp: @t, level: @l, message: @m, exception: @x, ..@p} }");
        o.ContentType = "application/json";
    })
    .CreateLogger();
```

- Set `contentType` to match the formatter.
- With the CLEF formatters, events handed to a fallback sink keep their properties; with any other formatter the
  fallback receives level, timestamp and the formatted text as `{Payload}`. `writeUndeliveredToStandardOutput` writes
  the formatted body as is, with any formatter.

## Routing and custom properties (code only)

- `RoutingKeySelector` — routing key per event (e.g. by level); needs `Exchange`.
- `MessageProperties` — your own application properties: string, integer and floating-point numbers, bool, Guid,
  DateTime/DateTimeOffset, enum; `decimal` and `char` are sent as strings.

Both run once per event on the background formatter; an exception or an unsupported value drops only that event
(reported to the fallback).

```csharp
o.RoutingKeySelector = e => e.Level.ToString().ToLowerInvariant();
o.MessageProperties = e => [new("service", "billing"), new("tenant", Tenant.Current)];
```

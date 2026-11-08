using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;
using Serilog.Parsing;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;

/// <summary>
/// Restores a <see cref="LogEvent"/> from a formatted record, only for handing undelivered events to a failure
/// listener (e.g. <c>WriteTo.FallbackChain</c>). Uses <see cref="System.Text.Json"/> — no extra dependency
/// (the official CLEF reader would pull in Newtonsoft.Json; ADR 0004).
/// <list type="bullet">
/// <item>CLEF bodies (the default compact formatters): template, level, timestamp, exception text and properties.</item>
/// <item>Anything else (custom formatter, unparsable body): level, timestamp and the raw body as <c>{Payload}</c>.</item>
/// </list>
/// </summary>
internal sealed class ClefDecoder
{
    private static readonly MessageTemplateParser Parser = new();
    private static readonly MessageTemplate PayloadTemplate = Parser.Parse("{Payload}");

    private readonly bool _isClef;

    public ClefDecoder(ITextFormatter formatter)
    {
        _isClef = formatter is CompactJsonFormatter or RenderedCompactJsonFormatter;
    }

    public LogEvent Decode(FormattedEvent record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (_isClef && TryDecodeClef(record.Body, out var logEvent))
        {
            return logEvent;
        }

        return new LogEvent(
            record.Timestamp,
            record.Level,
            exception: null,
            PayloadTemplate,
            [new LogEventProperty("Payload", new ScalarValue(Encoding.UTF8.GetString(record.Body)))]);
    }

    private static bool TryDecodeClef(byte[] body, out LogEvent logEvent)
    {
        logEvent = null!;
        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            var timestamp = DateTimeOffset.UtcNow;
            var level = LogEventLevel.Information;
            string? template = null;
            string? rendered = null;
            Exception? exception = null;
            ActivityTraceId traceId = default;
            ActivitySpanId spanId = default;
            var properties = new List<LogEventProperty>();

            foreach (var field in root.EnumerateObject())
            {
                switch (field.Name)
                {
                    case "@t" when field.Value.TryGetDateTimeOffset(out var t):
                        timestamp = t;
                        break;
                    case "@l" when Enum.TryParse<LogEventLevel>(field.Value.GetString(), out var l):
                        level = l;
                        break;
                    case "@mt":
                        template = field.Value.GetString();
                        break;
                    case "@m":
                        rendered = field.Value.GetString();
                        break;
                    case "@x" when field.Value.GetString() is { } text:
                        exception = new TextException(text);
                        break;
                    case "@tr" when field.Value.GetString() is { Length: 32 } trace:
                        traceId = ActivityTraceId.CreateFromString(trace);
                        break;
                    case "@sp" when field.Value.GetString() is { Length: 16 } span:
                        spanId = ActivitySpanId.CreateFromString(span);
                        break;
                    default:
                        if (!field.Name.StartsWith('@') || field.Name.StartsWith("@@", StringComparison.Ordinal))
                        {
                            var name = field.Name.StartsWith("@@", StringComparison.Ordinal) ? field.Name[1..] : field.Name;
                            properties.Add(new LogEventProperty(name, ToPropertyValue(field.Value)));
                        }

                        break;
                }
            }

            var messageTemplate = template is not null
                ? Parser.Parse(template)
                : new MessageTemplate([new TextToken(rendered ?? string.Empty)]);

            logEvent = new LogEvent(timestamp, level, exception, messageTemplate, properties, traceId, spanId);
            return true;
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException or FormatException)
        {
            return false; // not CLEF after all (or an invalid property name): fall back to the raw payload
        }
    }

    private static LogEventPropertyValue ToPropertyValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => new ScalarValue(value.GetString()),
        JsonValueKind.Number when value.TryGetInt64(out var integer) => new ScalarValue(integer),
        JsonValueKind.Number when value.TryGetDecimal(out var number) => new ScalarValue(number),
        JsonValueKind.Number => new ScalarValue(value.GetDouble()),
        JsonValueKind.True => new ScalarValue(true),
        JsonValueKind.False => new ScalarValue(false),
        JsonValueKind.Array => new SequenceValue(value.EnumerateArray().Select(ToPropertyValue).ToList()),
        JsonValueKind.Object => ToStructure(value),
        _ => new ScalarValue(null),
    };

    private static StructureValue ToStructure(JsonElement value)
    {
        string? typeTag = null;
        var properties = new List<LogEventProperty>();
        foreach (var member in value.EnumerateObject())
        {
            if (member.Name == "$type")
            {
                typeTag = member.Value.ToString();
                continue;
            }

            properties.Add(new LogEventProperty(member.Name, ToPropertyValue(member.Value)));
        }

        return new StructureValue(properties, typeTag);
    }
}

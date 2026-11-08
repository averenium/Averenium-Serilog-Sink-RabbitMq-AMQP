using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Serilog.Formatting.Display;
using Serilog.Parsing;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Formatting;

public sealed class ClefDecoderTests
{
    [Test]
    public async Task Restores_template_level_timestamp_properties_and_exception_from_clef()
    {
        var formatter = new CompactJsonFormatter();
        var original = new LogEvent(
            new DateTimeOffset(2026, 11, 7, 10, 0, 0, TimeSpan.Zero),
            LogEventLevel.Error,
            new InvalidOperationException("boom"),
            new MessageTemplateParser().Parse("Order {OrderId} failed for {User}"),
            [
                new LogEventProperty("OrderId", new ScalarValue(42)),
                new LogEventProperty("User", new StructureValue([new LogEventProperty("Name", new ScalarValue("ann"))], "User")),
                new LogEventProperty("Tags", new SequenceValue([new ScalarValue("a"), new ScalarValue("b")])),
            ]);
        var record = new LogEventFormatter(formatter).Format(original);

        var restored = new ClefDecoder(formatter).Decode(record);

        await Assert.That(restored.MessageTemplate.Text).IsEqualTo("Order {OrderId} failed for {User}");
        await Assert.That(restored.Level).IsEqualTo(LogEventLevel.Error);
        await Assert.That(restored.Timestamp).IsEqualTo(original.Timestamp);
        await Assert.That(((ScalarValue)restored.Properties["OrderId"]).Value).IsEqualTo(42L);
        await Assert.That(restored.Properties["User"]).IsTypeOf<StructureValue>();
        await Assert.That(((StructureValue)restored.Properties["User"]).TypeTag).IsEqualTo("User");
        await Assert.That(restored.Properties["Tags"]).IsTypeOf<SequenceValue>();
        await Assert.That(restored.Exception!.ToString()).Contains("boom");
        await Assert.That(restored.RenderMessage(System.Globalization.CultureInfo.InvariantCulture)).Contains("Order 42 failed");
    }

    [Test]
    public async Task Restores_trace_and_span_ids()
    {
        var formatter = new CompactJsonFormatter();
        var traceId = System.Diagnostics.ActivityTraceId.CreateRandom();
        var spanId = System.Diagnostics.ActivitySpanId.CreateRandom();
        var original = new LogEvent(DateTimeOffset.UtcNow, LogEventLevel.Information, null,
            new MessageTemplateParser().Parse("Traced"), [], traceId, spanId);

        var restored = new ClefDecoder(formatter).Decode(new LogEventFormatter(formatter).Format(original));

        await Assert.That(restored.TraceId).IsEqualTo(traceId);
        await Assert.That(restored.SpanId).IsEqualTo(spanId);
    }

    [Test]
    public async Task Custom_formatter_output_is_handed_over_as_raw_payload()
    {
        var formatter = new MessageTemplateTextFormatter("{Timestamp:o} {Message}");
        var original = TestEvents.Create(7, LogEventLevel.Warning);
        var record = new LogEventFormatter(formatter).Format(original);

        var restored = new ClefDecoder(formatter).Decode(record);

        await Assert.That(restored.Level).IsEqualTo(LogEventLevel.Warning);
        await Assert.That(restored.Timestamp).IsEqualTo(original.Timestamp);
        await Assert.That(restored.RenderMessage(System.Globalization.CultureInfo.InvariantCulture)).Contains("Event 7");
    }
}

using System.Diagnostics;
using System.Text;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Events;
using Microsoft.Extensions.Time.Testing;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Formatting;

public sealed class LogEventFormatterTests
{
    [Test]
    public async Task Formats_compact_json_once_with_metadata_and_a_stable_message_id()
    {
        var formatter = new LogEventFormatter(new CompactJsonFormatter());
        var logEvent = TestEvents.Create(42, LogEventLevel.Warning, "My.App.Service");

        var record = formatter.Format(logEvent);
        var body = Encoding.UTF8.GetString(record.Body);

        await Assert.That(body).StartsWith("{\"@t\":");
        await Assert.That(body).Contains("\"N\":42");
        await Assert.That(body.EndsWith('\n')).IsFalse();
        await Assert.That(record.MessageId.Length).IsEqualTo(32);
        await Assert.That(record.Level).IsEqualTo(LogEventLevel.Warning);
        await Assert.That(record.Timestamp).IsEqualTo(logEvent.Timestamp);
        await Assert.That(record.SourceContext).IsEqualTo("My.App.Service");
    }

    [Test]
    public async Task Estimated_size_is_payload_plus_overhead()
    {
        var record = new LogEventFormatter(new CompactJsonFormatter()).Format(TestEvents.Create(1));

        await Assert.That(record.EstimatedSize).IsGreaterThanOrEqualTo(record.Body.Length + FormattedEvent.OverheadBytes);
        await Assert.That(record.EstimatedSize).IsLessThan(record.Body.Length + 1024);
    }

    [Test]
    public async Task Routing_key_and_message_properties_are_evaluated_once()
    {
        var calls = 0;
        var formatter = new LogEventFormatter(
            new CompactJsonFormatter(),
            e => e.Level.ToString().ToLowerInvariant(),
            e =>
            {
                calls++;
                return [new("tenant", "acme"), new("attempt", 3), new("at", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc))];
            });

        var record = formatter.Format(TestEvents.Create(1, LogEventLevel.Error));

        await Assert.That(record.RoutingKey).IsEqualTo("error");
        await Assert.That(record.Properties!.Length).IsEqualTo(3);
        await Assert.That(calls).IsEqualTo(1);
    }

    [Test]
    public async Task Unsupported_property_values_fail_at_formatting_not_on_every_publish()
    {
        var formatter = new LogEventFormatter(new CompactJsonFormatter(),
            messageProperties: _ => [new("payload", new { Nested = true })]);

        await Assert.That(() => formatter.Format(TestEvents.Create(1))).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Empty_routing_key_is_rejected()
    {
        var formatter = new LogEventFormatter(new CompactJsonFormatter(), _ => "");

        await Assert.That(() => formatter.Format(TestEvents.Create(1))).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Correlation_id_defaults_to_the_trace_id_when_enabled()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var formatter = new LogEventFormatter(new CompactJsonFormatter(), correlationId: LogEventFormatter.CorrelationIdFromTraceId);

        var traced = formatter.Format(TestEvents.WithTrace(1, traceId));
        var untraced = formatter.Format(TestEvents.Create(2));

        await Assert.That(traced.CorrelationId).IsEqualTo(traceId.ToHexString());
        await Assert.That(untraced.CorrelationId).IsNull();
    }

    [Test]
    public async Task Correlation_id_selector_is_evaluated_once_on_formatting()
    {
        var calls = 0;
        var formatter = new LogEventFormatter(new CompactJsonFormatter(), correlationId: e =>
        {
            calls++;
            return "order-" + e.N();
        });

        var record = formatter.Format(TestEvents.Create(7));

        await Assert.That(record.CorrelationId).IsEqualTo("order-7");
        await Assert.That(calls).IsEqualTo(1);
    }

    [Test]
    public async Task Formatting_time_is_taken_from_the_time_provider()
    {
        var time = new FakeTimeProvider();
        var formatter = new LogEventFormatter(new CompactJsonFormatter(), timeProvider: time);

        var first = formatter.Format(TestEvents.Create(1));
        time.Advance(TimeSpan.FromSeconds(3));
        var second = formatter.Format(TestEvents.Create(2));

        await Assert.That(time.GetElapsedTime(first.FormattedAt, second.FormattedAt)).IsEqualTo(TimeSpan.FromSeconds(3));
    }
}

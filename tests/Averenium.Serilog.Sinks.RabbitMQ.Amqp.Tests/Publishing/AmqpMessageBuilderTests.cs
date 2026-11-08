using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Serilog.Events;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

public sealed class AmqpMessageBuilderTests
{
    [Test]
    public async Task Builds_a_fresh_message_per_attempt_with_the_same_id_and_metadata()
    {
        var logEvent = TestEvents.Create(1, LogEventLevel.Warning, "My.App.Service");
        var record = new LogEventFormatter(new CompactJsonFormatter(), messageProperties: _ => [new("app", "billing")]).Format(logEvent);
        var builder = new AmqpMessageBuilder("application/json");

        var first = builder.Build(record);
        var second = builder.Build(record);

        await Assert.That(first).IsNotSameReferenceAs(second);
        await Assert.That(first.MessageId()).IsEqualTo(record.MessageId);
        await Assert.That(second.MessageId()).IsEqualTo(record.MessageId);
        await Assert.That(first.ContentType()).IsEqualTo("application/json");
        await Assert.That(first.Property(AmqpMessageBuilder.LevelProperty)).IsEqualTo("Warning");
        await Assert.That(first.Property(AmqpMessageBuilder.SourceContextProperty)).IsEqualTo("My.App.Service");
        await Assert.That(first.Property("app")).IsEqualTo("billing");
        await Assert.That(first.CreationTime()).IsEqualTo(logEvent.Timestamp.UtcDateTime);
        await Assert.That(first.Durable()).IsTrue();
    }

    [Test]
    public async Task Per_event_routing_key_addresses_the_message()
    {
        var record = new LogEventFormatter(new CompactJsonFormatter(), e => e.Level.ToString().ToLowerInvariant())
            .Format(TestEvents.Create(1, LogEventLevel.Error));

        var message = new AmqpMessageBuilder("application/json", "logs").Build(record);

        await Assert.That(message.To()).IsEqualTo("/exchanges/logs/error");
    }

    [Test]
    public async Task Correlation_id_is_set_only_when_the_record_has_one()
    {
        var formatter = new LogEventFormatter(new CompactJsonFormatter(), correlationId: e => e.N() == 1 ? "abc" : null);
        var builder = new AmqpMessageBuilder("application/json");

        var with = builder.Build(formatter.Format(TestEvents.Create(1)));
        var without = builder.Build(formatter.Format(TestEvents.Create(2)));

        await Assert.That(with.CorrelationId()).IsEqualTo("abc");
        await Assert.That(without.CorrelationId()).IsNull();
    }

    [Test]
    public async Task Messages_can_be_published_non_durable()
    {
        var record = new LogEventFormatter(new CompactJsonFormatter()).Format(TestEvents.Create(1));

        var message = new AmqpMessageBuilder("application/json", durable: false).Build(record);

        await Assert.That(message.Durable()).IsFalse();
    }
}

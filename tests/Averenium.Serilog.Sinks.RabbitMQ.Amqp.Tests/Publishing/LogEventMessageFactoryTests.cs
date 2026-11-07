using System.Text;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Serilog.Events;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

public sealed class LogEventMessageFactoryTests
{
    [Test]
    public async Task Prepare_formats_compact_json_once_with_a_stable_message_id()
    {
        var factory = new LogEventMessageFactory(new CompactJsonFormatter(), "application/json");

        var pending = factory.Prepare(TestEvents.Create(42));
        var body = Encoding.UTF8.GetString(pending.Body);

        await Assert.That(body).StartsWith("{\"@t\":");
        await Assert.That(body).Contains("\"N\":42");
        await Assert.That(body.EndsWith('\n')).IsFalse();
        await Assert.That(pending.MessageId.Length).IsEqualTo(32);
    }

    [Test]
    public async Task Build_creates_a_fresh_message_per_attempt_with_the_same_id_and_metadata()
    {
        var factory = new LogEventMessageFactory(new CompactJsonFormatter(), "application/json");
        var logEvent = TestEvents.Create(1, LogEventLevel.Warning, "My.App.Service");
        var pending = factory.Prepare(logEvent);

        var first = factory.Build(pending);
        var second = factory.Build(pending);

        await Assert.That(first).IsNotSameReferenceAs(second);
        await Assert.That(first.MessageId()).IsEqualTo(pending.MessageId);
        await Assert.That(second.MessageId()).IsEqualTo(pending.MessageId);
        await Assert.That(first.ContentType()).IsEqualTo("application/json");
        await Assert.That(first.Property(LogEventMessageFactory.LevelProperty)).IsEqualTo("Warning");
        await Assert.That(first.Property(LogEventMessageFactory.SourceContextProperty)).IsEqualTo("My.App.Service");
        await Assert.That(first.CreationTime()).IsEqualTo(logEvent.Timestamp.UtcDateTime);
        await Assert.That(first.Durable()).IsTrue();
    }

    [Test]
    public async Task Routing_key_selector_addresses_each_message_individually()
    {
        var factory = new LogEventMessageFactory(
            new CompactJsonFormatter(), "application/json", "logs", e => e.Level.ToString().ToLowerInvariant());

        var message = factory.Build(factory.Prepare(TestEvents.Create(1, LogEventLevel.Error)));

        await Assert.That(message.To()).IsEqualTo("/exchanges/logs/error");
    }

    [Test]
    public async Task Configure_message_hook_is_applied()
    {
        var factory = new LogEventMessageFactory(
            new CompactJsonFormatter(), "application/json",
            configureMessage: (e, m) => m.Property("app", "billing"));

        var message = factory.Build(factory.Prepare(TestEvents.Create(1)));

        await Assert.That(message.Property("app")).IsEqualTo("billing");
    }
}

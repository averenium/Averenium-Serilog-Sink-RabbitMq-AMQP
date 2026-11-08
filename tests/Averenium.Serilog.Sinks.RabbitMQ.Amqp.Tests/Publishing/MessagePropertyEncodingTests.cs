using Amqp;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

/// <summary>
/// Every value type accepted by <c>MessageProperties</c> must be encodable by the AMQP client — otherwise the send
/// would fail on every retry. This encodes the real AMQP message and decodes it back.
/// </summary>
public sealed class MessagePropertyEncodingTests
{
    private static readonly Dictionary<string, Func<object>> Samples = new()
    {
        ["string"] = () => "text",
        ["bool"] = () => true,
        ["byte"] = () => (byte)1,
        ["sbyte"] = () => (sbyte)-1,
        ["short"] = () => (short)-2,
        ["ushort"] = () => (ushort)2,
        ["int"] = () => -3,
        ["uint"] = () => 3u,
        ["long"] = () => -4L,
        ["ulong"] = () => 4UL,
        ["float"] = () => 1.5f,
        ["double"] = () => 2.5d,
        ["Guid"] = () => Guid.NewGuid(),
        ["DateTime"] = () => new DateTime(2026, 11, 7, 10, 0, 0, DateTimeKind.Utc),
        ["DateTimeOffset"] = () => DateTimeOffset.UtcNow,
        ["decimal"] = () => 12.34m,
        ["char"] = () => 'x',
        ["enum"] = () => DayOfWeek.Friday,
    };

    public static IEnumerable<string> AllowedTypes() => Samples.Keys;

    [Test]
    [MethodDataSource(nameof(AllowedTypes))]
    public async Task Allowed_property_values_survive_amqp_encoding(string type)
    {
        var value = Samples[type]();
        var record = new LogEventFormatter(new CompactJsonFormatter(), messageProperties: _ => [new("p", value)])
            .Format(TestEvents.Create(1));
        var message = (AmqpMessage)new AmqpMessageBuilder("application/json").Build(record);

        var decoded = Message.Decode(message.NativeMessage.Encode());

        await Assert.That(decoded.ApplicationProperties["p"]).IsNotNull();
    }
}

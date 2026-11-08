using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// Creates a fresh <see cref="IMessage"/> from a formatted record for every publish attempt (a message object is never
/// re-sent while a previous send of it may still be in flight). Only already-validated data is used here, so building
/// does not run user code.
/// </summary>
internal sealed class AmqpMessageBuilder
{
    public const string LevelProperty = "level";
    public const string SourceContextProperty = "source-context";

    private readonly string _contentType;
    private readonly string? _exchange;
    private readonly bool _durable;

    public AmqpMessageBuilder(string contentType, string? exchange = null, bool durable = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(contentType);
        _contentType = contentType;
        _exchange = exchange;
        _durable = durable;
    }

    public IMessage Build(FormattedEvent record)
    {
        ArgumentNullException.ThrowIfNull(record);

        IMessage message = new AmqpMessage(record.Body)
            .MessageId(record.MessageId)
            .ContentType(_contentType)
            .CreationTime(record.Timestamp.UtcDateTime)
            .Property(LevelProperty, record.Level.ToString())
            .Durable(_durable);

        if (record.CorrelationId is not null)
        {
            message.CorrelationId(record.CorrelationId);
        }

        if (record.SourceContext is not null)
        {
            message.Property(SourceContextProperty, record.SourceContext);
        }

        if (record.Properties is not null)
        {
            foreach (var (key, value) in record.Properties)
            {
                message.Property(key, value);
            }
        }

        if (record.RoutingKey is not null)
        {
            if (_exchange is null)
            {
                throw new InvalidOperationException("A per-event routing key requires an exchange.");
            }

            message = message.ToAddress().Exchange(_exchange).Key(record.RoutingKey).Build();
        }

        return message;
    }
}

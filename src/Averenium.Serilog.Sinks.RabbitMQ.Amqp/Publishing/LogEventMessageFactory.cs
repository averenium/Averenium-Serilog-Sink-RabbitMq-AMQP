using System.Globalization;
using System.Text;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Events;
using Serilog.Formatting;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// Turns log events into AMQP messages. <see cref="Prepare"/> formats once; <see cref="Build"/> creates a fresh
/// <see cref="IMessage"/> for every publish attempt (a message object is never re-sent while a previous send
/// of it may still be in flight).
/// </summary>
internal sealed class LogEventMessageFactory
{
    public const string LevelProperty = "level";
    public const string SourceContextProperty = "source-context";

    private readonly ITextFormatter _formatter;
    private readonly string _contentType;
    private readonly string? _exchange;
    private readonly Func<LogEvent, string>? _routingKeySelector;
    private readonly Action<LogEvent, IMessage>? _configureMessage;

    public LogEventMessageFactory(
        ITextFormatter formatter,
        string contentType,
        string? exchange = null,
        Func<LogEvent, string>? routingKeySelector = null,
        Action<LogEvent, IMessage>? configureMessage = null)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        if (routingKeySelector is not null)
        {
            ArgumentException.ThrowIfNullOrEmpty(exchange);
        }

        _formatter = formatter;
        _contentType = contentType;
        _exchange = exchange;
        _routingKeySelector = routingKeySelector;
        _configureMessage = configureMessage;
    }

    /// <summary>Formats the event. Throws if the formatter or routing-key selector throws.</summary>
    public PendingMessage Prepare(LogEvent logEvent)
    {
        using var writer = new StringWriter(CultureInfo.InvariantCulture);
        _formatter.Format(logEvent, writer);
        var body = Encoding.UTF8.GetBytes(writer.ToString().TrimEnd('\r', '\n'));
        var routingKey = _routingKeySelector?.Invoke(logEvent);
        return new PendingMessage(logEvent, body, Guid.NewGuid().ToString("N"), routingKey);
    }

    public IMessage Build(PendingMessage pending)
    {
        var logEvent = pending.LogEvent;
        IMessage message = new AmqpMessage(pending.Body)
            .MessageId(pending.MessageId)
            .ContentType(_contentType)
            .CreationTime(logEvent.Timestamp.UtcDateTime)
            .Property(LevelProperty, logEvent.Level.ToString());

        if (logEvent.Properties.TryGetValue("SourceContext", out var sourceContext)
            && sourceContext is ScalarValue { Value: string source })
        {
            message.Property(SourceContextProperty, source);
        }

        if (pending.RoutingKey is not null)
        {
            message = message.ToAddress().Exchange(_exchange!).Key(pending.RoutingKey).Build();
        }

        _configureMessage?.Invoke(logEvent, message);
        return message;
    }
}

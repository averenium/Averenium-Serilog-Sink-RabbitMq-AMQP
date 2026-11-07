using System.Reflection;
using RabbitMQ.AMQP.Client;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;

/// <summary>Configuration of the RabbitMQ AMQP 1.0 sink.</summary>
public sealed class RabbitMqAmqpSinkOptions
{
    /// <summary>
    /// Broker endpoints, e.g. <c>amqp://user:pass@rabbit-1:5672/vhost</c>. Configure every cluster node (or a load
    /// balancer): the sink fails over between them round-robin. All URIs must use the same virtual host.
    /// </summary>
    public IList<Uri> Uris { get; } = [];

    /// <summary>Exchange to publish to. Either <see cref="Exchange"/> or <see cref="Queue"/> is required.</summary>
    public string? Exchange { get; set; }

    /// <summary>Routing key used with <see cref="Exchange"/>. Ignored when <see cref="RoutingKeySelector"/> is set.</summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Queue to publish to directly when no <see cref="Exchange"/> is set. Use a quorum queue (or a stream) so
    /// stored logs survive the loss of a node.
    /// </summary>
    public string? Queue { get; set; }

    /// <summary>
    /// Computes the routing key per event (e.g. by level). Requires <see cref="Exchange"/>; messages are then
    /// addressed individually through an anonymous publisher.
    /// </summary>
    public Func<LogEvent, string>? RoutingKeySelector { get; set; }

    /// <summary>
    /// Last chance to adjust each outgoing message (application properties, subject, ...). Called for every publish
    /// attempt, so it must be idempotent. The sink already sets message-id, content-type, creation-time and the
    /// <c>level</c> / <c>source-context</c> application properties.
    /// </summary>
    public Action<LogEvent, IMessage>? ConfigureMessage { get; set; }

    /// <summary>Formats the message body. Default: compact JSON (CLEF), one event per message.</summary>
    public ITextFormatter Formatter { get; set; } = new CompactJsonFormatter();

    /// <summary>AMQP content-type of the body. Default <c>application/json</c>.</summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>AMQP container-id, shown as the connection name in the management UI. Default: entry assembly name @ machine name.</summary>
    public string? ConnectionName { get; set; }

    /// <summary>Maximum events buffered in memory while the broker is unavailable. Default 100 000.</summary>
    public int QueueLimit { get; set; } = 100_000;

    /// <summary>What to drop when <see cref="QueueLimit"/> is reached. Default <see cref="OverflowPolicy.DropOldest"/>.</summary>
    public OverflowPolicy OverflowPolicy { get; set; } = OverflowPolicy.DropOldest;

    /// <summary>Maximum messages published concurrently per round. Default 500.</summary>
    public int BatchSizeLimit { get; set; } = 500;

    /// <summary>How long to wait for the broker outcome of one message before retrying it. Default 30 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often a message the broker released (e.g. unroutable) is retried before it is reported as dropped. Default 5.</summary>
    public int MaxReleasedRetries { get; set; } = 5;

    /// <summary>First delay between reconnect attempts. Default 500 ms.</summary>
    public TimeSpan MinReconnectDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Upper bound of the delay between reconnect attempts; reconnecting never stops. Default 30 s.</summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long disposing the logger may spend delivering buffered events. Default 10 s.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Declare the topology after every (re)connect: the exchange (<see cref="ExchangeType"/>) and, if
    /// <see cref="DeclareQueue"/> is set, a quorum queue bound with <see cref="BindingKey"/>; or the quorum
    /// <see cref="Queue"/> when publishing directly to a queue. Needs the <c>configure</c> permission. Default off.
    /// </summary>
    public bool DeclareTopology { get; set; }

    /// <summary>Exchange type for <see cref="DeclareTopology"/>: <c>topic</c> (default), <c>direct</c>, <c>fanout</c> or <c>headers</c>.</summary>
    public string ExchangeType { get; set; } = "topic";

    /// <summary>Quorum queue to declare and bind to <see cref="Exchange"/> when <see cref="DeclareTopology"/> is on.</summary>
    public string? DeclareQueue { get; set; }

    /// <summary>Binding key for <see cref="DeclareQueue"/>. Default: <see cref="RoutingKey"/>, or <c>#</c> when empty.</summary>
    public string? BindingKey { get; set; }

    internal string EffectiveConnectionName =>
        string.IsNullOrWhiteSpace(ConnectionName)
            ? $"{Assembly.GetEntryAssembly()?.GetName().Name ?? "serilog"}@{Environment.MachineName}"
            : ConnectionName;

    internal string EffectiveBindingKey =>
        !string.IsNullOrEmpty(BindingKey) ? BindingKey
        : !string.IsNullOrEmpty(RoutingKey) ? RoutingKey
        : "#";

    /// <summary>Throws <see cref="ArgumentException"/> for an unusable configuration (at logger creation time).</summary>
    internal void Validate()
    {
        if (Uris.Count == 0)
        {
            throw new ArgumentException("At least one RabbitMQ URI is required.", nameof(Uris));
        }

        if (string.IsNullOrWhiteSpace(Exchange) && string.IsNullOrWhiteSpace(Queue))
        {
            throw new ArgumentException("Either Exchange or Queue must be set.", nameof(Exchange));
        }

        if (RoutingKeySelector is not null && string.IsNullOrWhiteSpace(Exchange))
        {
            throw new ArgumentException("RoutingKeySelector requires Exchange.", nameof(RoutingKeySelector));
        }

        ArgumentNullException.ThrowIfNull(Formatter);
        ArgumentException.ThrowIfNullOrWhiteSpace(ContentType);
        ArgumentOutOfRangeException.ThrowIfLessThan(QueueLimit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSizeLimit, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxReleasedRetries);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(PublishTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MinReconnectDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxReconnectDelay, MinReconnectDelay);
        ArgumentOutOfRangeException.ThrowIfLessThan(ShutdownTimeout, TimeSpan.Zero);
    }
}

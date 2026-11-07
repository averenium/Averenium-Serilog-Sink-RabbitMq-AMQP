namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>What the sink does with a message after a publish attempt.</summary>
internal enum OutcomeKind
{
    /// <summary>Broker accepted the message — done.</summary>
    Accepted,

    /// <summary>
    /// Broker released the message (e.g. unroutable) or refused it for a resource limit — retry a limited
    /// number of times, then drop and report.
    /// </summary>
    Released,

    /// <summary>
    /// The publish target (exchange/queue) does not exist — keep the message and rebuild the publisher;
    /// RabbitMQ 4 keeps the link attached and rejects every message with <c>amqp:not-found</c>.
    /// </summary>
    TargetMissing,

    /// <summary>
    /// "Rejected" that is really a connection/session/link failure (e.g. the client settles in-flight messages
    /// with <c>amqp:internal-error</c> "Connection forced" when the broker shuts down) — keep and retry without limit.
    /// </summary>
    ConnectionLost,

    /// <summary>Broker rejected this particular message — drop and report.</summary>
    Rejected,
}

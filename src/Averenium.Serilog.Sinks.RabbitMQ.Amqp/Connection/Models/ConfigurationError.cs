namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;

/// <summary>
/// A failure caused by configuration rather than an outage. <see cref="IsCertain"/> is <see langword="false"/> for
/// heuristics (repeated attach timeouts), which are reported as "probable" so a slow broker is not mislabelled.
/// </summary>
internal sealed record ConfigurationError(string Reason, bool IsCertain);

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;

/// <summary>What the supervisor was doing when an attempt failed; the same error means different things per stage.</summary>
internal enum ConnectionStage
{
    /// <summary>Opening the connection (credentials, virtual host).</summary>
    Connect,

    /// <summary>Declaring topology after connecting (configure permission, conflicting definitions).</summary>
    Declare,

    /// <summary>Attaching the publisher to its target (target exists, write permission).</summary>
    Attach,
}

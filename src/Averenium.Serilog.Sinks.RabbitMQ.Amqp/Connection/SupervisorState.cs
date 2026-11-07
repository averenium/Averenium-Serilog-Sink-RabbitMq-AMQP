namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>State of <see cref="ConnectionSupervisor"/>.</summary>
internal enum SupervisorState
{
    /// <summary>Not started yet.</summary>
    Idle,

    /// <summary>Creating connection, declaring topology, attaching the publisher.</summary>
    Connecting,

    /// <summary>Connection and publisher are open; publishing is possible.</summary>
    Ready,

    /// <summary>The client is recovering the connection or re-attaching the publisher.</summary>
    Recovering,

    /// <summary>Connection attempt failed or was torn down; waiting for back-off before rebuilding.</summary>
    Faulted,

    /// <summary>Disposed; terminal.</summary>
    Disposed,
}

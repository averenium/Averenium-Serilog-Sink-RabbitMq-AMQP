using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Creates the AMQP environment the supervisor connects through. The client fixes user name and password when the
/// connection settings are built (C12), so rotated credentials need a new environment. Nothing is polled:
/// <see cref="Changed"/> tells the supervisor, which then reconnects through a new environment.
/// </summary>
internal interface IEnvironmentSource
{
    /// <summary>Creates an environment with the current credentials (no network I/O).</summary>
    IEnvironment Create();

    /// <summary>
    /// <see langword="true"/> when <paramref name="environment"/> was created with credentials that are no longer current.
    /// Cheap (compares in-memory values) and never throws.
    /// </summary>
    bool IsOutdated(IEnvironment environment);

    /// <summary>Raised when the credentials changed (on the thread that changed them).</summary>
    event Action? Changed;
}

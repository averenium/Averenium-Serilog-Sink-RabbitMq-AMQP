using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;

/// <summary>Tuning knobs for <see cref="ConnectionSupervisor"/>.</summary>
internal sealed class ConnectionSupervisorOptions
{
    /// <summary>Configures the publisher address (exchange/key or queue). Default: anonymous publisher.</summary>
    public Func<IPublisherBuilder, IPublisherBuilder> ConfigurePublisher { get; init; } = static b => b;

    /// <summary>
    /// Optional hook run after every successful connect and before the publisher is attached,
    /// e.g. to declare topology. Failures cause a rebuild with back-off.
    /// </summary>
    public Func<IConnection, CancellationToken, Task>? OnConnectedAsync { get; init; }

    /// <summary>
    /// Called once per distinct configuration error (wrong credentials/permissions, conflicting topology, missing
    /// target) — see <c>ConfigurationErrorDetector</c>. The supervisor keeps retrying regardless.
    /// </summary>
    public Action<ConfigurationError, Exception>? OnConfigurationError { get; init; }

    /// <summary>First delay between rebuild attempts.</summary>
    public TimeSpan MinReconnectDelay { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Upper bound for the delay between rebuild attempts.</summary>
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Bound for connect, topology hook and publisher attach — the client may ignore cancellation.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long the publisher may stay not-open while the connection is open before the supervisor
    /// rebuilds everything (the client gives up re-attaching publishers after 2 attempts).
    /// </summary>
    public TimeSpan PublisherRecoveryGrace { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How long the connection may stay <c>Reconnecting</c> (client-driven recovery) before the supervisor stops waiting
    /// and rebuilds it itself. The client's recovery attempts run one after another without a timeout of their own, and
    /// one attempt can hang for minutes in TCP connect to a node that vanished (C13); the supervisor's own attempts are
    /// bounded by <see cref="ConnectTimeout"/>. Default (<see langword="null"/>): <see cref="MaxReconnectDelay"/> + 15 s,
    /// i.e. one full back-off step plus a generous connect + handshake.
    /// </summary>
    public TimeSpan? ClientRecoveryTimeout { get; init; }

    internal TimeSpan EffectiveClientRecoveryTimeout => ClientRecoveryTimeout ?? MaxReconnectDelay + TimeSpan.FromSeconds(15);

    /// <summary>Periodic re-evaluation of connection/publisher state, in addition to state-change events.</summary>
    public TimeSpan HealthCheckInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Bound for closing a publisher/connection during teardown.</summary>
    public TimeSpan CloseTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Consecutive publish timeouts on one publisher that trigger a rebuild.</summary>
    public int MaxConsecutivePublishTimeouts { get; init; } = 3;
}

// Public options live in the Options folder but keep the root namespace, so users need no extra using
// (documented exception to "namespace = folder", AGENTS.md).
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;
#pragma warning restore IDE0130

/// <summary>
/// Credentials that can change while the application runs (rotated secrets). Assign it to
/// <see cref="RabbitMqAmqpSinkOptions.Credentials"/> and call <see cref="Update"/> when the secret changes, e.g. from
/// <c>IOptionsMonitor.OnChange</c>; the sink then reconnects with the new values. Nothing is polled. With
/// <c>appsettings.json</c> the sink does this by itself for its <c>username</c>/<c>password</c> arguments.
/// Thread-safe; one source may be shared by several sinks.
/// </summary>
public sealed class RabbitMqAmqpCredentialsSource
{
    private Snapshot _current;

    /// <summary>Creates the source with the initial credentials.</summary>
    /// <param name="username">User name.</param>
    /// <param name="password">Password; any characters are allowed (they are escaped).</param>
    public RabbitMqAmqpCredentialsSource(string username, string? password)
    {
        ArgumentException.ThrowIfNullOrEmpty(username);
        _current = new Snapshot(username, password);
    }

    /// <summary>Raised after <see cref="Update"/> changed the credentials (on the caller's thread).</summary>
    internal event Action? Changed;

    /// <summary>Current user name.</summary>
    public string Username => Volatile.Read(ref _current).Username;

    internal Snapshot Current => Volatile.Read(ref _current);

    /// <summary>
    /// Sets new credentials. If they differ from the current ones, every sink using this source closes its connection
    /// and connects again with them (buffered events are kept and delivered afterwards). Same values: nothing happens.
    /// </summary>
    /// <param name="username">User name.</param>
    /// <param name="password">Password.</param>
    public void Update(string username, string? password)
    {
        ArgumentException.ThrowIfNullOrEmpty(username);
        var next = new Snapshot(username, password);
        if (Interlocked.Exchange(ref _current, next) != next)
        {
            Changed?.Invoke();
        }
    }

    /// <summary>Never prints the password.</summary>
    /// <returns>The user name.</returns>
    public override string ToString() => $"RabbitMqAmqpCredentialsSource {{ Username = {Username} }}";

    /// <summary>Immutable pair; value equality tells whether the credentials changed.</summary>
    internal sealed record Snapshot(string Username, string? Password)
    {
        public override string ToString() => $"Snapshot {{ Username = {Username} }}";
    }
}

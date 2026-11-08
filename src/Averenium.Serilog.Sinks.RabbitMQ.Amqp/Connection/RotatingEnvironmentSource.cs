using System.Runtime.CompilerServices;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Builds real AMQP environments from the sink options with the credentials current at creation time, and forwards
/// changes of <c>Credentials</c> (a <see cref="RabbitMqAmqpCredentialsSource"/> — updated by the user, or by the
/// <c>appsettings</c> overload when its configuration reloads). TLS settings are built once and shared: certificate files
/// are reloaded by their own callbacks on every handshake (<see cref="TlsConfiguration"/>).
/// </summary>
internal sealed class RotatingEnvironmentSource : IEnvironmentSource
{
    private readonly RabbitMqAmqpSinkOptions _options;
    private readonly TlsSettings? _tls;
    private readonly SaslMechanism? _sasl;
    private readonly ConditionalWeakTable<IEnvironment, CredentialsHolder> _createdWith = [];

    /// <param name="options">A validated snapshot.</param>
    public RotatingEnvironmentSource(RabbitMqAmqpSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        (_tls, _sasl) = TlsConfiguration.Create(options.Tls);
    }

    public event Action? Changed
    {
        add
        {
            if (_options.Credentials is { } credentials)
            {
                credentials.Changed += value;
            }
        }

        remove
        {
            if (_options.Credentials is { } credentials)
            {
                credentials.Changed -= value;
            }
        }
    }

    public IEnvironment Create()
    {
        var credentials = _options.CurrentCredentials();
        var settings = AmqpConnectionSettingsFactory.Create(
            _options.ResolveEndpoints(credentials),
            _options.EffectiveConnectionName,
            _options.MinReconnectDelay,
            _options.MaxReconnectDelay,
            _tls,
            _sasl);
        var environment = AmqpEnvironment.Create(settings);
        _createdWith.AddOrUpdate(environment, new CredentialsHolder(credentials));
        return environment;
    }

    public bool IsOutdated(IEnvironment environment) =>
        _options.Credentials is { } credentials
        && _createdWith.TryGetValue(environment, out var used)
        && credentials.Current != used.Credentials;

    /// <summary><see cref="ConditionalWeakTable{TKey,TValue}"/> needs a reference type that may hold <see langword="null"/>.</summary>
    private sealed record CredentialsHolder(RabbitMqAmqpCredentialsSource.Snapshot? Credentials);
}

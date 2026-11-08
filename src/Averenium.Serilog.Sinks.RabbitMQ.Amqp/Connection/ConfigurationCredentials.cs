using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;
using Serilog.Debugging;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Follows the <c>username</c>/<c>password</c> arguments of this sink in the application's <see cref="IConfiguration"/>
/// (handed to the <c>appsettings</c> overload by Serilog.Settings.Configuration). Serilog does not rebuild sinks when the
/// configuration reloads, so the sink finds its own <c>Args</c> section and subscribes to the configuration's reload
/// token: a rotated secret (JSON with <c>reloadOnChange</c>, a key-per-file Kubernetes secret, Key Vault / Vault
/// providers with reload) updates a <see cref="RabbitMqAmqpCredentialsSource"/>, and the sink reconnects. No polling.
/// </summary>
internal static class ConfigurationCredentials
{
    private const string SinkName = "RabbitMQAmqp";

    /// <summary>
    /// Finds the <c>Args</c> section of this sink: an entry with <c>Name = RabbitMQAmqp</c> anywhere in the configuration
    /// (also inside <c>FallbackChain</c> or similar wrappers) whose user name, password, hosts and target are exactly the
    /// values the sink was created with. <see langword="null"/> when there is no such entry or more than one.
    /// </summary>
    public static IConfigurationSection? FindArgs(
        IConfiguration configuration, string username, string? password, IReadOnlyList<string> hostnames, string? exchange, string? queue)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        IConfigurationSection? found = null;
        foreach (var (key, value) in configuration.AsEnumerable())
        {
            if (!key.EndsWith(":Name", StringComparison.OrdinalIgnoreCase)
                || !string.Equals(value, SinkName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var args = configuration.GetSection(key[..^":Name".Length] + ":Args");
            if (!Matches(args, username, password, hostnames, exchange, queue))
            {
                continue;
            }

            if (found is not null)
            {
                return null; // ambiguous: two identical sinks — don't guess
            }

            found = args;
        }

        return found;
    }

    /// <summary>
    /// Subscribes to configuration reloads; every reload re-reads <paramref name="args"/> and updates
    /// <paramref name="credentials"/> (which only notifies the sink when the values really changed).
    /// </summary>
    public static IDisposable Watch(IConfiguration configuration, IConfigurationSection args, RabbitMqAmqpCredentialsSource credentials)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(credentials);

        return ChangeToken.OnChange(configuration.GetReloadToken, () =>
        {
            var username = args["username"];
            if (string.IsNullOrEmpty(username))
            {
                SelfLog.WriteLine("[RabbitMQAmqp] The reloaded configuration has no username for the sink; keeping the current credentials.");
                return;
            }

            credentials.Update(username, args["password"]);
        });
    }

    private static bool Matches(
        IConfigurationSection args, string username, string? password, IReadOnlyList<string> hostnames, string? exchange, string? queue) =>
        string.Equals(args["username"], username, StringComparison.Ordinal)
        && string.Equals(args["password"], password, StringComparison.Ordinal)
        && string.Equals(NullIfEmpty(args["exchange"]), NullIfEmpty(exchange), StringComparison.Ordinal)
        && string.Equals(NullIfEmpty(args["queue"]), NullIfEmpty(queue), StringComparison.Ordinal)
        && HostEntries(args.GetSection("hostnames"))
            .SelectMany(EndpointResolver.SplitList)
            .SequenceEqual(hostnames, StringComparer.Ordinal);

    /// <summary>A JSON array (children) or a single string — whichever form the configuration uses.</summary>
    private static IEnumerable<string> HostEntries(IConfigurationSection hostnames) =>
        hostnames.Value is { } single ? [single] : hostnames.GetChildren().Select(child => child.Value ?? string.Empty);

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}

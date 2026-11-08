using System.Diagnostics.CodeAnalysis;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Microsoft.Extensions.Configuration;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;

// Serilog convention: configuration extensions live in the Serilog namespace so `WriteTo.RabbitMQAmqp(...)`
// needs no extra using. This is the only file allowed to break "namespace = folder" (AGENTS.md).
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Serilog;
#pragma warning restore IDE0130

/// <summary>Adds <c>WriteTo.RabbitMQAmqp(...)</c> to the logger configuration.</summary>
public static class RabbitMqAmqpLoggerConfigurationExtensions
{
    /// <summary>
    /// Writes log events to RabbitMQ 4.x over AMQP 1.0, configured in code.
    /// </summary>
    /// <param name="sinkConfiguration">Logger sink configuration.</param>
    /// <param name="configure">Sets up <see cref="RabbitMqAmqpSinkOptions"/>.</param>
    /// <param name="restrictedToMinimumLevel">Minimum level for events passed to the sink.</param>
    /// <param name="levelSwitch">Optional switch controlling the minimum level at runtime.</param>
    /// <returns>Configuration object allowing method chaining.</returns>
    [SuppressMessage("ApiDesign", "RS0026:Do not add multiple public overloads with optional parameters",
        Justification = "Serilog convention: Serilog.Settings.Configuration binds sink methods by optional parameter names.")]
    public static LoggerConfiguration RabbitMQAmqp(
        this LoggerSinkConfiguration sinkConfiguration,
        Action<RabbitMqAmqpSinkOptions> configure,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch = null)
    {
        ArgumentNullException.ThrowIfNull(sinkConfiguration);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RabbitMqAmqpSinkOptions();
        configure(options);
        return sinkConfiguration.Sink(new RabbitMqAmqpSink(options), restrictedToMinimumLevel, levelSwitch);
    }

    /// <summary>
    /// Writes log events to RabbitMQ 4.x over AMQP 1.0; flat parameters for <c>Serilog.Settings.Configuration</c>.
    /// </summary>
    /// <param name="sinkConfiguration">Logger sink configuration.</param>
    /// <param name="hostnames">
    /// Cluster nodes: <c>host</c>, <c>host:port</c> or <c>amqp(s)://</c> URIs. A JSON array in <c>appsettings.json</c>;
    /// each element may also hold several nodes separated by <c>,</c> or <c>;</c> (handy for environment variables).
    /// </param>
    /// <param name="username">User name for hosts without credentials in a URI.</param>
    /// <param name="password">Password for <paramref name="username"/>.</param>
    /// <param name="virtualHost">Virtual host; default <c>/</c>.</param>
    /// <param name="port">Port for hosts given without one; default 5672 (5671 with TLS).</param>
    /// <param name="useTls">Connect with TLS to hosts given without a scheme.</param>
    /// <param name="exchange">Exchange to publish to.</param>
    /// <param name="routingKey">Routing key used with <paramref name="exchange"/>.</param>
    /// <param name="queue">Queue to publish to when no exchange is set.</param>
    /// <param name="connectionName">AMQP container-id shown in the management UI.</param>
    /// <param name="queueLimit">Burst buffer: events waiting for the background formatter.</param>
    /// <param name="queueSizeLimitBytes">Outage buffer: approximate memory for formatted events waiting for delivery.</param>
    /// <param name="overflowPolicy">What to drop when the buffer is full.</param>
    /// <param name="batchSizeLimit">Maximum messages published concurrently.</param>
    /// <param name="declareTopology">Declare exchange / quorum queue / binding after every connect.</param>
    /// <param name="exchangeType">Exchange type used when declaring.</param>
    /// <param name="declareQueue">Quorum queue to declare and bind to the exchange.</param>
    /// <param name="bindingKey">Binding key for <paramref name="declareQueue"/>.</param>
    /// <param name="formatter">Body formatter; default compact JSON.</param>
    /// <param name="contentType">AMQP content-type of the body.</param>
    /// <param name="publishTimeout">Wait for one broker outcome before retrying (e.g. <c>00:00:30</c>); default 30 s.</param>
    /// <param name="maxReleasedRetries">Retries for messages the broker released (e.g. unroutable).</param>
    /// <param name="minReconnectDelay">First reconnect delay; default 500 ms.</param>
    /// <param name="maxReconnectDelay">Maximum reconnect delay; default 30 s.</param>
    /// <param name="writeUndeliveredToStandardOutput">Also write undelivered events as formatted lines to stdout.</param>
    /// <param name="shutdownTimeout">Upper bound for the whole shutdown (delivering buffered events and closing); default 10 s.</param>
    /// <param name="tls">
    /// TLS details as an object, e.g. <c>"tls": { "caCertificatePath": "/etc/ca.crt" }</c>; see <see cref="RabbitMqAmqpTlsOptions"/>.
    /// </param>
    /// <param name="correlationIdFromTraceId">Set the AMQP correlation-id to the event's trace id.</param>
    /// <param name="durableMessages">Publish durable (persistent) messages.</param>
    /// <param name="fallbackAfter">Hand events not accepted within this time to the fallback (e.g. <c>00:01:00</c>); default: keep retrying.</param>
    /// <param name="restrictedToMinimumLevel">Minimum level for events passed to the sink.</param>
    /// <param name="levelSwitch">Optional switch controlling the minimum level at runtime.</param>
    /// <param name="configuration">
    /// Filled in by Serilog.Settings.Configuration (not an argument in the configuration file): when this sink's
    /// <c>username</c>/<c>password</c> change on a configuration reload, the sink reconnects with the new values.
    /// </param>
    /// <returns>Configuration object allowing method chaining.</returns>
    [SuppressMessage("ApiDesign", "RS0026:Do not add multiple public overloads with optional parameters",
        Justification = "Serilog convention: Serilog.Settings.Configuration binds sink methods by optional parameter names.")]
    public static LoggerConfiguration RabbitMQAmqp(
        this LoggerSinkConfiguration sinkConfiguration,
        string[] hostnames,
        string? username = null,
        string? password = null,
        string? virtualHost = null,
        int? port = null,
        bool useTls = false,
        string? exchange = null,
        string? routingKey = null,
        string? queue = null,
        string? connectionName = null,
        int queueLimit = 10_000,
        long queueSizeLimitBytes = 15L * 1024 * 1024,
        OverflowPolicy overflowPolicy = OverflowPolicy.DropOldest,
        int batchSizeLimit = 500,
        bool declareTopology = false,
        string exchangeType = "topic",
        string? declareQueue = null,
        string? bindingKey = null,
        ITextFormatter? formatter = null,
        string contentType = "application/json",
        TimeSpan? publishTimeout = null,
        int maxReleasedRetries = 5,
        TimeSpan? minReconnectDelay = null,
        TimeSpan? maxReconnectDelay = null,
        bool writeUndeliveredToStandardOutput = false,
        TimeSpan? shutdownTimeout = null,
        RabbitMqAmqpTlsOptions? tls = null,
        bool correlationIdFromTraceId = true,
        bool durableMessages = true,
        TimeSpan? fallbackAfter = null,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch = null,
        IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(hostnames);
        var hosts = hostnames
            .Where(entry => entry is not null)
            .SelectMany(Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.EndpointResolver.SplitList)
            .ToList();
        if (hosts.Count == 0)
        {
            throw new ArgumentException("At least one host is required.", nameof(hostnames));
        }

        // Follow credential rotation in the configuration (Serilog itself does not rebuild sinks on reload).
        RabbitMqAmqpCredentialsSource? liveCredentials = null;
        IDisposable? watch = null;
        if (configuration is not null && !string.IsNullOrEmpty(username))
        {
            if (ConfigurationCredentials.FindArgs(configuration, username, password, hosts, exchange, queue) is { } args)
            {
                liveCredentials = new RabbitMqAmqpCredentialsSource(username, password);
                watch = ConfigurationCredentials.Watch(configuration, args, liveCredentials);
            }
            else
            {
                global::Serilog.Debugging.SelfLog.WriteLine(
                    "[RabbitMQAmqp] Could not find this sink's section in the configuration; credential changes require a restart.");
            }
        }

        try
        {
            return sinkConfiguration.RabbitMQAmqp(
                options =>
                {
                    foreach (var host in hosts)
                    {
                        options.Hostnames.Add(host);
                    }

                    if (liveCredentials is not null)
                    {
                        options.Credentials = liveCredentials;
                        options.CredentialsWatch = watch;
                    }
                    else
                    {
                        options.Username = username;
                        options.Password = password;
                    }

                    options.VirtualHost = virtualHost;
                    options.Port = port;
                    options.UseTls = useTls;
                    if (tls is not null)
                    {
                        options.Tls = tls;
                    }

                    options.CorrelationIdFromTraceId = correlationIdFromTraceId;
                    options.DurableMessages = durableMessages;
                    options.FallbackAfter = fallbackAfter;

                    options.Exchange = exchange;
                    options.RoutingKey = routingKey;
                    options.Queue = queue;
                    options.ConnectionName = connectionName;
                    options.QueueLimit = queueLimit;
                    options.QueueSizeLimitBytes = queueSizeLimitBytes;
                    options.WriteUndeliveredToStandardOutput = writeUndeliveredToStandardOutput;
                    options.OverflowPolicy = overflowPolicy;
                    options.BatchSizeLimit = batchSizeLimit;
                    options.DeclareTopology = declareTopology;
                    options.ExchangeType = exchangeType;
                    options.DeclareQueue = declareQueue;
                    options.BindingKey = bindingKey;
                    options.ContentType = contentType;
                    options.MaxReleasedRetries = maxReleasedRetries;
                    if (formatter is not null)
                    {
                        options.Formatter = formatter;
                    }

                    if (publishTimeout is { } publish)
                    {
                        options.PublishTimeout = publish;
                    }

                    if (minReconnectDelay is { } minDelay)
                    {
                        options.MinReconnectDelay = minDelay;
                    }

                    if (maxReconnectDelay is { } maxDelay)
                    {
                        options.MaxReconnectDelay = maxDelay;
                    }

                    if (shutdownTimeout is { } shutdown)
                    {
                        options.ShutdownTimeout = shutdown;
                    }
                },
                restrictedToMinimumLevel,
                levelSwitch);
        }
        catch
        {
            watch?.Dispose();
            throw;
        }
    }
}

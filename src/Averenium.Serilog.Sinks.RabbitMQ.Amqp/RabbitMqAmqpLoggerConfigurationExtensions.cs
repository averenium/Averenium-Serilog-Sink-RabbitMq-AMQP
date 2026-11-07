using System.Diagnostics.CodeAnalysis;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;

namespace Serilog;

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
    /// <param name="uris">One or more broker URIs separated by <c>,</c> or <c>;</c>.</param>
    /// <param name="exchange">Exchange to publish to.</param>
    /// <param name="routingKey">Routing key used with <paramref name="exchange"/>.</param>
    /// <param name="queue">Queue to publish to when no exchange is set.</param>
    /// <param name="connectionName">AMQP container-id shown in the management UI.</param>
    /// <param name="queueLimit">Maximum events buffered in memory.</param>
    /// <param name="overflowPolicy">What to drop when the buffer is full.</param>
    /// <param name="batchSizeLimit">Maximum messages published concurrently.</param>
    /// <param name="declareTopology">Declare exchange / quorum queue / binding after every connect.</param>
    /// <param name="exchangeType">Exchange type used when declaring.</param>
    /// <param name="declareQueue">Quorum queue to declare and bind to the exchange.</param>
    /// <param name="bindingKey">Binding key for <paramref name="declareQueue"/>.</param>
    /// <param name="formatter">Body formatter; default compact JSON.</param>
    /// <param name="restrictedToMinimumLevel">Minimum level for events passed to the sink.</param>
    /// <param name="levelSwitch">Optional switch controlling the minimum level at runtime.</param>
    /// <returns>Configuration object allowing method chaining.</returns>
    [SuppressMessage("ApiDesign", "RS0026:Do not add multiple public overloads with optional parameters",
        Justification = "Serilog convention: Serilog.Settings.Configuration binds sink methods by optional parameter names.")]
    public static LoggerConfiguration RabbitMQAmqp(
        this LoggerSinkConfiguration sinkConfiguration,
        string uris,
        string? exchange = null,
        string? routingKey = null,
        string? queue = null,
        string? connectionName = null,
        int queueLimit = 100_000,
        OverflowPolicy overflowPolicy = OverflowPolicy.DropOldest,
        int batchSizeLimit = 500,
        bool declareTopology = false,
        string exchangeType = "topic",
        string? declareQueue = null,
        string? bindingKey = null,
        ITextFormatter? formatter = null,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(uris);

        return sinkConfiguration.RabbitMQAmqp(
            options =>
            {
                foreach (var uri in uris.Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    options.Uris.Add(new Uri(uri));
                }

                options.Exchange = exchange;
                options.RoutingKey = routingKey;
                options.Queue = queue;
                options.ConnectionName = connectionName;
                options.QueueLimit = queueLimit;
                options.OverflowPolicy = overflowPolicy;
                options.BatchSizeLimit = batchSizeLimit;
                options.DeclareTopology = declareTopology;
                options.ExchangeType = exchangeType;
                options.DeclareQueue = declareQueue;
                options.BindingKey = bindingKey;
                if (formatter is not null)
                {
                    options.Formatter = formatter;
                }
            },
            restrictedToMinimumLevel,
            levelSwitch);
    }
}

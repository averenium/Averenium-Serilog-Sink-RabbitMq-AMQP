using System.Diagnostics.CodeAnalysis;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;

/// <summary>
/// Serilog sink publishing to RabbitMQ 4.x over AMQP 1.0. <see cref="Emit"/> only enqueues; delivery,
/// retries and reconnects happen in the background (docs/architecture.md).
/// </summary>
internal sealed class RabbitMqAmqpSink : ILogEventSink, IDisposable, IAsyncDisposable, ISetLoggingFailureListener
{
    private readonly FailureReporter _reporter;
    private readonly EventBuffer _buffer;
    private readonly ConnectionSupervisor _supervisor;
    private readonly PublishLoop _loop;
    private readonly TimeSpan _shutdownTimeout;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _abort = new();
    private readonly Task _loopTask;
    private int _disposed;

    public RabbitMqAmqpSink(RabbitMqAmqpSinkOptions options)
        : this(options, CreateEnvironment(options), TimeProvider.System)
    {
    }

    internal RabbitMqAmqpSink(RabbitMqAmqpSinkOptions options, IEnvironment environment, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        options.Validate();

        _time = timeProvider;
        _shutdownTimeout = options.ShutdownTimeout;
        _reporter = new FailureReporter(this);
        _buffer = new EventBuffer(options.QueueLimit, options.OverflowPolicy, _reporter);

        _supervisor = new ConnectionSupervisor(
            environment,
            new ConnectionSupervisorOptions
            {
                ConfigurePublisher = CreatePublisherConfiguration(options),
                OnConnectedAsync = options.DeclareTopology
                    ? (connection, _) => TopologyDeclarer.DeclareAsync(connection, options)
                    : null,
                MinReconnectDelay = options.MinReconnectDelay,
                MaxReconnectDelay = options.MaxReconnectDelay,
            },
            timeProvider);

        var factory = new LogEventMessageFactory(
            options.Formatter,
            options.ContentType,
            options.Exchange,
            options.RoutingKeySelector,
            options.ConfigureMessage);

        _loop = new PublishLoop(
            _buffer.Reader,
            _supervisor,
            factory,
            _reporter,
            new PublishLoopOptions
            {
                BatchSizeLimit = options.BatchSizeLimit,
                PublishTimeout = options.PublishTimeout,
                MaxReleasedRetries = options.MaxReleasedRetries,
            },
            timeProvider);

        _supervisor.Start();
        _loopTask = Task.Run(() => _loop.RunAsync(_abort.Token));
    }

    internal ConnectionSupervisor Supervisor => _supervisor;

    internal PublishLoop Loop => _loop;

    internal EventBuffer Buffer => _buffer;

    public void Emit(LogEvent logEvent)
    {
        ArgumentNullException.ThrowIfNull(logEvent);
        if (!_buffer.TryWrite(logEvent))
        {
            _reporter.Report(LoggingFailureKind.Final, "RabbitMQ sink is disposed; event not delivered", [logEvent]);
        }
    }

    public void SetFailureListener(ILoggingFailureListener failureListener) => _reporter.SetListener(failureListener);

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Disposal of a logging sink must never throw into the host application.")]
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _buffer.Complete();

            using (var deadline = new CancellationTokenSource(_shutdownTimeout, _time))
            {
                try
                {
                    await _loopTask.WaitAsync(deadline.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Deadline reached: abort; the loop reports what is left as Final.
                    await _abort.CancelAsync().ConfigureAwait(false);
                    await _loopTask.ConfigureAwait(false);
                }
            }

            await _supervisor.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("[RabbitMQAmqp] Error while disposing the sink: {0}", ex);
        }
        finally
        {
            _abort.Dispose();
        }
    }

    private static IEnvironment CreateEnvironment(RabbitMqAmqpSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var settings = AmqpConnectionSettingsFactory.Create(
            [.. options.Uris],
            options.EffectiveConnectionName,
            options.MinReconnectDelay,
            options.MaxReconnectDelay);
        return AmqpEnvironment.Create(settings);
    }

    private static Func<IPublisherBuilder, IPublisherBuilder> CreatePublisherConfiguration(RabbitMqAmqpSinkOptions options)
    {
        if (options.RoutingKeySelector is not null)
        {
            return static builder => builder; // anonymous publisher; each message carries its own address
        }

        if (!string.IsNullOrWhiteSpace(options.Exchange))
        {
            var exchange = options.Exchange;
            var routingKey = options.RoutingKey;
            return builder =>
            {
                builder = builder.Exchange(exchange);
                return string.IsNullOrEmpty(routingKey) ? builder : builder.Key(routingKey);
            };
        }

        var queue = options.Queue!;
        return builder => builder.Queue(queue);
    }
}

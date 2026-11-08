using System.Diagnostics.CodeAnalysis;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Topology;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp;

/// <summary>
/// Serilog sink publishing to RabbitMQ 4.x over AMQP 1.0 (docs/architecture.md, ADR 0004):
/// <c>Emit</c> → burst channel (<c>QueueLimit</c>) → <see cref="FormatterLoop"/> → size-bounded outage queue
/// (<c>QueueSizeLimitBytes</c>) → <see cref="PublishLoop"/>; every undelivered event goes through
/// <see cref="UndeliveredEvents"/> (fallbacks on their own worker).
/// <see cref="Emit"/> only enqueues; formatting, delivery, retries and reconnects happen in the background.
/// </summary>
internal sealed class RabbitMqAmqpSink : ILogEventSink, IDisposable, IAsyncDisposable, ISetLoggingFailureListener
{
    private readonly FailureReporter _reporter;
    private readonly UndeliveredEvents _undelivered;
    private readonly EventBuffer _buffer;
    private readonly SizeBoundedQueue<FormattedEvent> _records;
    private readonly ConnectionSupervisor _supervisor;
    private readonly PublishLoop _loop;
    private readonly TimeSpan _shutdownTimeout;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _abort = new();
    private readonly Task _formatterTask;
    private readonly Task _loopTask;
    private static readonly TimeSpan MaxCloseReserve = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxHandoverReserve = TimeSpan.FromSeconds(5);

    private readonly IDisposable? _credentialsWatch;
    private int _disposed;
    private long _emittedAfterDispose;

    public RabbitMqAmqpSink(RabbitMqAmqpSinkOptions options)
        : this(options, environment: null, TimeProvider.System)
    {
    }

    /// <param name="options">Copied: later changes to the caller's object do not affect the sink.</param>
    /// <param name="environment">For tests; <see langword="null"/> creates (and owns) a real AMQP environment.</param>
    /// <param name="timeProvider">Time source for delays and timeouts.</param>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Rethrown after closing the environment created here.")]
    internal RabbitMqAmqpSink(RabbitMqAmqpSinkOptions options, IEnvironment? environment, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        options = options.Snapshot();
        options.Validate();

        _time = timeProvider;
        _shutdownTimeout = options.ShutdownTimeout;
        _reporter = new FailureReporter(this, timeProvider);
        _undelivered = new UndeliveredEvents(
            _reporter,
            options.Formatter,
            options.WriteUndeliveredToStandardOutput ? Console.OpenStandardOutput() : null);

        _credentialsWatch = options.CredentialsWatch;
        IEnvironmentSource environments = environment is null
            ? new RotatingEnvironmentSource(options)
            : new FixedEnvironmentSource(environment);
        try
        {
            var burstOverflow =
                $"RabbitMQ sink burst buffer (QueueLimit) is full; event dropped before formatting ({options.OverflowPolicy})";
            _buffer = new EventBuffer(options.QueueLimit, options.OverflowPolicy,
                dropped => _undelivered.Report(LoggingFailureKind.Permanent, burstOverflow, [dropped]));
            _records = new SizeBoundedQueue<FormattedEvent>(options.QueueSizeLimitBytes, options.OverflowPolicy, r => r.EstimatedSize);
            var formatterLoop = new FormatterLoop(
                _buffer.Reader,
                _records,
                new LogEventFormatter(
                    options.Formatter,
                    options.RoutingKeySelector,
                    options.MessageProperties,
                    options.CorrelationIdSelector
                        ?? (options.CorrelationIdFromTraceId ? LogEventFormatter.CorrelationIdFromTraceId : null),
                    timeProvider),
                _undelivered,
                options.OverflowPolicy);

            _supervisor = new ConnectionSupervisor(
                environments,
                new ConnectionSupervisorOptions
                {
                    ConfigurePublisher = CreatePublisherConfiguration(options),
                    OnConnectedAsync = options.DeclareTopology
                        ? (connection, _) => TopologyDeclarer.DeclareAsync(connection, options)
                        : null,
                    OnConfigurationError = (error, exception) => _reporter.Notify(
                        error.IsCertain ? LoggingFailureKind.Permanent : LoggingFailureKind.Temporary,
                        $"RabbitMQ configuration error{(error.IsCertain ? string.Empty : " (probable)")} — the sink keeps retrying: {error.Reason}",
                        exception),
                    MinReconnectDelay = options.MinReconnectDelay,
                    MaxReconnectDelay = options.MaxReconnectDelay,
                    ClientRecoveryTimeout = options.ClientRecoveryTimeout,
                },
                timeProvider);

            _loop = new PublishLoop(
                _records,
                _supervisor,
                new AmqpMessageBuilder(options.ContentType, options.Exchange, options.DurableMessages),
                _undelivered,
                new PublishLoopOptions
                {
                    BatchSizeLimit = options.BatchSizeLimit,
                    PublishTimeout = options.PublishTimeout,
                    MaxReleasedRetries = options.MaxReleasedRetries,
                    FallbackAfter = options.FallbackAfter,
                },
                timeProvider);

            _undelivered.Start();
            _supervisor.Start();
            _formatterTask = Task.Run(() => formatterLoop.RunAsync(_abort.Token));
            _loopTask = Task.Run(() => _loop.RunAsync(_abort.Token));
        }
        catch (Exception)
        {
            CleanUpFailedStart(_supervisor, ownsEnvironment: environment is null, _credentialsWatch);
            throw;
        }
    }

    /// <summary>
    /// After a constructor failure: release what the sink created. The supervisor closes its environment when disposed,
    /// so it is only disposed when the sink created that environment; one passed in (tests) is left to its owner — the
    /// supervisor has not been started at this point, so nothing else needs stopping.
    /// </summary>
    internal static void CleanUpFailedStart(ConnectionSupervisor? supervisor, bool ownsEnvironment, IDisposable? credentialsWatch)
    {
        credentialsWatch?.Dispose();
        if (!ownsEnvironment)
        {
            return;
        }

        supervisor?.DisposeAsync().AsTask().ContinueWith(
            static t => SelfLog.WriteLine("[RabbitMQAmqp] Closing the environment after a failed start failed: {0}", t.Exception),
            CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
    }

    internal ConnectionSupervisor Supervisor => _supervisor;

    internal PublishLoop Loop => _loop;

    internal EventBuffer Buffer => _buffer;

    internal SizeBoundedQueue<FormattedEvent> Records => _records;

    internal UndeliveredEvents Undelivered => _undelivered;

    /// <summary>Events emitted after <see cref="DisposeAsync"/>; dropped because the fallbacks are closed too.</summary>
    internal long EmittedAfterDispose => Interlocked.Read(ref _emittedAfterDispose);

    public void Emit(LogEvent logEvent)
    {
        ArgumentNullException.ThrowIfNull(logEvent);
        if (!_buffer.TryWrite(logEvent) && Interlocked.Increment(ref _emittedAfterDispose) == 1)
        {
            // The fallbacks are closed together with this sink, so there is nobody to hand the event to. Never block
            // or call listeners on the application thread: count it and say so once.
            SelfLog.WriteLine("[RabbitMQAmqp] Events emitted after the sink was disposed are dropped (counted in EmittedAfterDispose).");
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

        // One deadline for the whole shutdown, split into three phases so none can starve the others:
        //   deliver buffered events | hand the undelivered rest to the fallbacks | close publisher → connection → environment
        // e.g. 10 s = 5 s + 3 s + 2 s. When the broker is down the deliver phase runs out, and the fallback still gets
        // its own time — that is exactly when it matters most.
        var started = _time.GetTimestamp();
        var closeReserve = TimeSpan.FromTicks(Math.Min(MaxCloseReserve.Ticks, _shutdownTimeout.Ticks / 4));
        var handoverReserve = TimeSpan.FromTicks(Math.Min(MaxHandoverReserve.Ticks, (long)(_shutdownTimeout.Ticks * 0.3)));
        try
        {
            _buffer.Complete();

            // Completing the burst channel lets the formatter drain it and complete the record queue,
            // which in turn lets the publish loop finish once everything is delivered.
            var pipeline = Task.WhenAll(_formatterTask, _loopTask);
            using (var deliverDeadline = new CancellationTokenSource(_shutdownTimeout - closeReserve - handoverReserve, _time))
            {
                try
                {
                    await pipeline.WaitAsync(deliverDeadline.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Deadline reached: abort; the publish loop reports its records as Final.
                    await _abort.CancelAsync().ConfigureAwait(false);
                    await pipeline.ConfigureAwait(false);
                }
            }

            ReportLeftovers();

            // Hand everything undelivered to the fallbacks (at least the handover reserve), keeping the close reserve.
            var handover = _shutdownTimeout - _time.GetElapsedTime(started) - closeReserve;
            await _undelivered.CompleteAsync(handover > handoverReserve ? handover : handoverReserve).ConfigureAwait(false);

            var remaining = _shutdownTimeout - _time.GetElapsedTime(started);
            await _supervisor.DisposeAsync(remaining > closeReserve ? remaining : closeReserve).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("[RabbitMQAmqp] Error while disposing the sink: {0}", ex);
        }
        finally
        {
            _credentialsWatch?.Dispose();
            _reporter.Flush();
            _reporter.Close(); // the fallbacks are closed after this; late reports go to SelfLog only
            _abort.Dispose();
        }
    }

    /// <summary>After an abort: events never formatted, and records the formatter added after the publish loop stopped.</summary>
    private void ReportLeftovers()
    {
        var unformatted = new List<LogEvent>();
        while (_buffer.Reader.TryRead(out var logEvent))
        {
            unformatted.Add(logEvent);
        }

        if (unformatted.Count > 0)
        {
            _undelivered.Report(LoggingFailureKind.Final, "RabbitMQ sink stopped before these log events were delivered", unformatted);
        }

        var records = _records.DrainAll();
        if (records.Count > 0)
        {
            _undelivered.Report(LoggingFailureKind.Final, "RabbitMQ sink stopped before these log events were delivered", records);
        }
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

using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Resilience;
using RabbitMQ.AMQP.Client;
using Serilog.Debugging;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Owns connection → publisher on top of an <see cref="IEnvironment"/> and keeps them usable for as long as
/// the sink lives (ADR 0003):
/// <list type="bullet">
/// <item>connects lazily in the background and retries the initial connect forever (the client does not);</item>
/// <item>lets the client's automatic recovery handle short outages and node failover;</item>
/// <item>rebuilds connection and publisher when the client gives up, its recovery does not finish in time (C13),
/// a publisher is stuck, or publishes keep timing out;</item>
/// <item>reconnects through a new environment when the credentials changed (<see cref="IEnvironmentSource.Changed"/>,
/// no polling) — the client keeps the credentials it was built with (C12).</item>
/// </list>
/// Readiness for publishing = connection <see cref="State.Open"/> and publisher <see cref="State.Open"/>.
/// </summary>
internal sealed class ConnectionSupervisor : IPublisherProvider, IAsyncDisposable
{
    private const string LogPrefix = "[RabbitMQAmqp] ";

    private readonly IEnvironmentSource _source;
    private readonly ConnectionSupervisorOptions _options;
    private readonly TimeProvider _time;
    private readonly ExponentialBackoff _backoff;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Channel<bool> _signals =
        Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
    private readonly Lock _gate = new();

    private TaskCompletionSource<IPublisher> _ready = NewReadySource();
    private SupervisorState _state = SupervisorState.Idle;
    private Task? _runTask;
    private IPublisher? _currentPublisher;
    private int _consecutiveTimeouts;
    private int _rebuildRequested;
    private int _disposed;
    private long _closeDeadline; // Stopwatch-style timestamp; 0 = no shared deadline
    private string? _lastConfigurationError;
    private int _consecutiveAttachFailures;
    private int _credentialsChanged;
    private IEnvironment _environment;

    public ConnectionSupervisor(IEnvironment environment, ConnectionSupervisorOptions options, TimeProvider? timeProvider = null)
        : this(new FixedEnvironmentSource(environment ?? throw new ArgumentNullException(nameof(environment))), options, timeProvider)
    {
    }

    /// <summary>Creates the first environment right away (no network I/O).</summary>
    public ConnectionSupervisor(IEnvironmentSource source, ConnectionSupervisorOptions options, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);

        _source = source;
        _environment = source.Create();
        _source.Changed += OnCredentialsChanged;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _backoff = new ExponentialBackoff(options.MinReconnectDelay, options.MaxReconnectDelay);
    }

    /// <summary>Raised after every state transition (on the supervisor's background thread).</summary>
    public event Action<SupervisorState>? StateChanged;

    public SupervisorState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>Starts the background loop. Idempotent; never blocks on the network.</summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        lock (_gate)
        {
            _runTask ??= Task.Run(RunAsync);
        }
    }

    /// <summary>
    /// Completes with an open publisher once the supervisor is <see cref="SupervisorState.Ready"/>.
    /// The publisher may break right after it is returned — callers must still handle publish failures
    /// and report them via <see cref="ReportPublishTimeout"/>.
    /// </summary>
    /// <exception cref="OperationCanceledException">Cancelled, or the supervisor was disposed.</exception>
    public Task<IPublisher> WaitReadyAsync(CancellationToken cancellationToken = default)
    {
        Task<IPublisher> ready;
        lock (_gate)
        {
            ready = _ready.Task;
        }

        return ready.WaitAsync(cancellationToken);
    }

    /// <summary>A publish on <paramref name="publisher"/> got no outcome in time.</summary>
    public void ReportPublishTimeout(IPublisher publisher)
    {
        if (!ReferenceEquals(publisher, Volatile.Read(ref _currentPublisher)))
        {
            return;
        }

        if (Interlocked.Increment(ref _consecutiveTimeouts) >= _options.MaxConsecutivePublishTimeouts)
        {
            Volatile.Write(ref _rebuildRequested, 1);
        }

        Signal();
    }

    /// <summary>
    /// The broker rejected a publish with <c>amqp:not-found</c>: the target exchange/queue is gone while the link
    /// stays attached. Rebuild now — this re-runs the topology hook, or keeps failing to attach (not Ready)
    /// until the target exists again.
    /// </summary>
    public void ReportTargetMissing(IPublisher publisher)
    {
        if (!ReferenceEquals(publisher, Volatile.Read(ref _currentPublisher)))
        {
            return;
        }

        Volatile.Write(ref _rebuildRequested, 1);
        Signal();
    }

    /// <summary>A publish on <paramref name="publisher"/> received an outcome.</summary>
    public void ReportPublishSucceeded(IPublisher publisher)
    {
        if (ReferenceEquals(publisher, Volatile.Read(ref _currentPublisher)))
        {
            Interlocked.Exchange(ref _consecutiveTimeouts, 0);
        }
    }

    /// <summary>Asks the supervisor to re-check states now (e.g. after a publish failed with "not open").</summary>
    public void Signal() => _signals.Writer.TryWrite(true);

    private void OnCredentialsChanged()
    {
        Volatile.Write(ref _credentialsChanged, 1);
        Signal();
    }

    public ValueTask DisposeAsync() => DisposeAsync(null);

    /// <summary>
    /// Stops the loop and closes publisher → connection → environment. With <paramref name="closeBudget"/> all
    /// closes share one deadline (so the sink's ShutdownTimeout bounds the whole shutdown); otherwise each close
    /// is bounded by <see cref="ConnectionSupervisorOptions.CloseTimeout"/>.
    /// </summary>
    public async ValueTask DisposeAsync(TimeSpan? closeBudget)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (closeBudget is { } budget)
        {
            var ticks = (long)(Math.Max(0, budget.TotalSeconds) * _time.TimestampFrequency);
            Volatile.Write(ref _closeDeadline, _time.GetTimestamp() + Math.Max(1, ticks));
        }

        await _shutdown.CancelAsync().ConfigureAwait(false);

        Task? runTask;
        lock (_gate)
        {
            runTask = _runTask;
        }

        if (runTask is not null)
        {
            await runTask.ConfigureAwait(false);
        }

        _source.Changed -= OnCredentialsChanged;
        SetState(SupervisorState.Disposed);
        await CloseQuietlyAsync(Volatile.Read(ref _environment).CloseAsync, "environment").ConfigureAwait(false);
        _shutdown.Dispose();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "The supervisor loop must survive any client failure; errors go to SelfLog and trigger a rebuild.")]
    private async Task RunAsync()
    {
        var ct = _shutdown.Token;
        while (!ct.IsCancellationRequested)
        {
            IConnection? connection = null;
            IPublisher? publisher = null;
            var stage = ConnectionStage.Connect;
            try
            {
                SetState(SupervisorState.Connecting);
                await RenewEnvironmentIfOutdatedAsync().ConfigureAwait(false);

                connection = await WithTimeoutAsync(_environment.CreateConnectionAsync, "connect", ct).ConfigureAwait(false);
                connection.ChangeState += OnEntityStateChanged;

                if (_options.OnConnectedAsync is { } onConnected)
                {
                    stage = ConnectionStage.Declare;
                    var c = connection;
                    await WithTimeoutAsync(
                        async token => { await onConnected(c, token).ConfigureAwait(false); return c; },
                        "on-connected hook",
                        ct,
                        disposeLateResult: false).ConfigureAwait(false);
                }

                stage = ConnectionStage.Attach;
                var builder = _options.ConfigurePublisher(connection.PublisherBuilder());
                publisher = await WithTimeoutAsync(builder.BuildAsync, "publisher attach", ct).ConfigureAwait(false);
                publisher.ChangeState += OnEntityStateChanged;
                _consecutiveAttachFailures = 0;

                _backoff.Reset();
                _lastConfigurationError = null;
                await MonitorAsync(connection, publisher, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                ReportFailure(ex, stage);
            }
            finally
            {
                // Leave Ready *before* the (possibly slow) teardown so WaitReadyAsync never hands out
                // a publisher that is being closed.
                Volatile.Write(ref _currentPublisher, null);
                if (!ct.IsCancellationRequested)
                {
                    SetState(SupervisorState.Faulted);
                }

                await TeardownAsync(connection, publisher).ConfigureAwait(false);
            }

            if (ct.IsCancellationRequested)
            {
                break;
            }

            SetState(SupervisorState.Faulted);
            var delay = _backoff.Next();
            SelfLog.WriteLine(LogPrefix + "Rebuilding connection in {0} (attempt {1}).", delay, _backoff.Attempt);
            try
            {
                await DelayUnlessCredentialsChangeAsync(delay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>The rebuild back-off, cut short when new credentials arrive (the old ones are what failed).</summary>
    private async Task DelayUnlessCredentialsChangeAsync(TimeSpan delay, CancellationToken ct)
    {
        var until = _time.GetTimestamp() + (long)(delay.TotalSeconds * _time.TimestampFrequency);
        while (Volatile.Read(ref _credentialsChanged) == 0)
        {
            var remaining = TimeSpan.FromSeconds((double)(until - _time.GetTimestamp()) / _time.TimestampFrequency);
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            await WaitForSignalAsync(remaining, ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
        }
    }

    /// <summary>New credentials: close the old environment (its connection is already torn down) and build a new one.</summary>
    private async Task RenewEnvironmentIfOutdatedAsync()
    {
        Volatile.Write(ref _credentialsChanged, 0);
        var current = _environment;
        if (!_source.IsOutdated(current))
        {
            return;
        }

        var renewed = _source.Create(); // throws for invalid new credentials → reported, retried with back-off
        Volatile.Write(ref _environment, renewed);
        SelfLog.WriteLine(LogPrefix + "Credentials changed; connecting with the new ones.");
        await CloseQuietlyAsync(current.CloseAsync, "previous environment").ConfigureAwait(false);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A throwing configuration-error callback must not stop the supervisor.")]
    private void ReportFailure(Exception ex, ConnectionStage stage)
    {
        var configurationError = ConfigurationErrorDetector.Describe(ex, stage);

        // The client reports an attach to a missing exchange (or without write permission) only as a timeout.
        // The connection itself works, so repeated attach failures are probably configuration, not an outage.
        if (configurationError is null && stage == ConnectionStage.Attach
            && ++_consecutiveAttachFailures >= ConfigurationErrorDetector.AttachFailuresBeforeReport)
        {
            configurationError = ConfigurationErrorDetector.ProbableAttachFailure();
        }
        if (configurationError is null)
        {
            SelfLog.WriteLine(LogPrefix + "Connection attempt failed: {0}", ex);
            return;
        }

        SelfLog.WriteLine(LogPrefix + "CONFIGURATION ERROR (will keep retrying): {0}. {1}", configurationError.Reason, ex);
        if (string.Equals(configurationError.Reason, _lastConfigurationError, StringComparison.Ordinal))
        {
            return; // already reported; don't flood the listener on every retry
        }

        _lastConfigurationError = configurationError.Reason;
        try
        {
            _options.OnConfigurationError?.Invoke(configurationError, ex);
        }
        catch (Exception callbackError)
        {
            SelfLog.WriteLine(LogPrefix + "Configuration error callback failed: {0}", callbackError);
        }
    }

    /// <summary>Returns when connection and publisher must be rebuilt; throws on shutdown.</summary>
    private async Task MonitorAsync(IConnection connection, IPublisher publisher, CancellationToken ct)
    {
        Interlocked.Exchange(ref _consecutiveTimeouts, 0);
        Volatile.Write(ref _rebuildRequested, 0);
        Volatile.Write(ref _currentPublisher, publisher);

        long? publisherNotOpenSince = null;
        long? reconnectingSince = null;
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            if (Volatile.Read(ref _rebuildRequested) != 0)
            {
                SelfLog.WriteLine(LogPrefix + "Publish target missing or publishes keep timing out; rebuilding connection.");
                return;
            }

            if (Volatile.Read(ref _credentialsChanged) != 0)
            {
                if (_source.IsOutdated(_environment))
                {
                    // Also while the client is reconnecting: it would keep retrying with the old credentials forever.
                    // The flag stays set, so the rebuild skips its back-off; RenewEnvironmentIfOutdatedAsync clears it.
                    SelfLog.WriteLine(LogPrefix + "Credentials changed; reconnecting with the new ones.");
                    return;
                }

                Volatile.Write(ref _credentialsChanged, 0); // changed back to what this connection uses
            }

            var connectionState = connection.State;
            var publisherState = publisher.State;

            if (connectionState is global::RabbitMQ.AMQP.Client.State.Closed or global::RabbitMQ.AMQP.Client.State.Closing)
            {
                SelfLog.WriteLine(LogPrefix + "Connection is {0} and will not recover by itself; rebuilding.", connectionState);
                return;
            }

            if (publisherState is global::RabbitMQ.AMQP.Client.State.Closed or global::RabbitMQ.AMQP.Client.State.Closing)
            {
                SelfLog.WriteLine(LogPrefix + "Publisher is {0}; rebuilding.", publisherState);
                return;
            }

            if (connectionState == global::RabbitMQ.AMQP.Client.State.Reconnecting)
            {
                // Client-driven recovery (endless back-off, round-robin over nodes) — but not for ever: one of its
                // attempts can hang in TCP connect for minutes (C13), and it has no timeout of its own.
                publisherNotOpenSince = null;
                SetState(SupervisorState.Recovering);
                reconnectingSince ??= _time.GetTimestamp();
                if (_time.GetElapsedTime(reconnectingSince.Value) >= _options.EffectiveClientRecoveryTimeout)
                {
                    SelfLog.WriteLine(LogPrefix + "Client recovery has not finished within {0}; rebuilding the connection.",
                        _options.EffectiveClientRecoveryTimeout);
                    return;
                }
            }
            else if (publisherState == global::RabbitMQ.AMQP.Client.State.Open)
            {
                publisherNotOpenSince = null;
                reconnectingSince = null;
                SetState(SupervisorState.Ready, publisher);
            }
            else
            {
                // Connection is open but the publisher is still re-attaching (C4).
                reconnectingSince = null;
                SetState(SupervisorState.Recovering);
                publisherNotOpenSince ??= _time.GetTimestamp();
                if (_time.GetElapsedTime(publisherNotOpenSince.Value) >= _options.PublisherRecoveryGrace)
                {
                    SelfLog.WriteLine(LogPrefix + "Publisher not open for {0} while connection is open; rebuilding.",
                        _options.PublisherRecoveryGrace);
                    return;
                }
            }

            await WaitForSignalAsync(_options.HealthCheckInterval, ct).ConfigureAwait(false);
        }
    }

    private async Task WaitForSignalAsync(TimeSpan timeout, CancellationToken ct)
    {
        if (_signals.Reader.TryRead(out _))
        {
            return;
        }

        using var timeoutCts = new CancellationTokenSource(timeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            await _signals.Reader.WaitToReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Health-check tick.
        }

        _signals.Reader.TryRead(out _);
    }

    /// <summary>
    /// The client may ignore cancellation (C6), so operations are bounded by <see cref="ConnectionSupervisorOptions.ConnectTimeout"/>.
    /// If an operation completes after we gave up on it, its result is closed so nothing leaks.
    /// </summary>
    private async Task<T> WithTimeoutAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        string what,
        CancellationToken ct,
        bool disposeLateResult = true)
        where T : class
    {
        using var operationCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = operation(operationCts.Token);
        try
        {
            return await task.WaitAsync(_options.ConnectTimeout, _time, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (!task.IsCompleted)
        {
            await operationCts.CancelAsync().ConfigureAwait(false);
            if (disposeLateResult)
            {
                _ = CloseWhenCompletedAsync(task);
            }

            if (ex is TimeoutException)
            {
                throw new TimeoutException($"RabbitMQ {what} did not complete within {_options.ConnectTimeout}.", ex);
            }

            throw;
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Best-effort clean-up of an abandoned operation.")]
    private async Task CloseWhenCompletedAsync<T>(Task<T> task)
    {
        try
        {
            var result = await task.ConfigureAwait(false);
            if (result is ILifeCycle lifeCycle)
            {
                await CloseQuietlyAsync(lifeCycle.CloseAsync, "late " + lifeCycle.GetType().Name).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // The abandoned operation failed after all — nothing to clean up.
        }
    }

    private async Task TeardownAsync(IConnection? connection, IPublisher? publisher)
    {
        if (publisher is not null)
        {
            publisher.ChangeState -= OnEntityStateChanged;
            await CloseQuietlyAsync(publisher.CloseAsync, "publisher").ConfigureAwait(false);
        }

        if (connection is not null)
        {
            connection.ChangeState -= OnEntityStateChanged;
            await CloseQuietlyAsync(connection.CloseAsync, "connection").ConfigureAwait(false);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Closing is best-effort; failures must not stop the supervisor.")]
    private async Task CloseQuietlyAsync(Func<Task> close, string what)
    {
        var timeout = _options.CloseTimeout;
        var deadline = Volatile.Read(ref _closeDeadline);
        if (deadline != 0)
        {
            var remaining = TimeSpan.FromSeconds((double)(deadline - _time.GetTimestamp()) / _time.TimestampFrequency);
            timeout = remaining <= TimeSpan.Zero ? TimeSpan.Zero : (remaining < timeout ? remaining : timeout);
        }

        try
        {
            await close().WaitAsync(timeout, _time).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine(LogPrefix + "Failed to close {0}: {1}", what, ex.Message);
        }
    }

    private void OnEntityStateChanged(object sender, State previousState, State currentState, Error? failureCause)
    {
        if (failureCause is not null)
        {
            SelfLog.WriteLine(LogPrefix + "{0}: {1} ({2})", sender.GetType().Name, $"{previousState} -> {currentState}", failureCause);
        }

        Signal();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A throwing diagnostics subscriber must not break the supervisor.")]
    private void SetState(SupervisorState state, IPublisher? readyPublisher = null)
    {
        lock (_gate)
        {
            if (_state == state || _state == SupervisorState.Disposed)
            {
                return;
            }

            _state = state;
            if (state == SupervisorState.Ready)
            {
                _ready.TrySetResult(readyPublisher!);
            }
            else if (state == SupervisorState.Disposed)
            {
                _ready.TrySetCanceled();
            }
            else if (_ready.Task.IsCompleted)
            {
                _ready = NewReadySource();
            }
        }

        try
        {
            StateChanged?.Invoke(state);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine(LogPrefix + "StateChanged handler failed: {0}", ex);
        }
    }

    private static TaskCompletionSource<IPublisher> NewReadySource() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}

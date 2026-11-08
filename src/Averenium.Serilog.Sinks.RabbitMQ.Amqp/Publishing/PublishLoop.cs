using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Resilience;
using RabbitMQ.AMQP.Client;
using Serilog.Core;
using Serilog.Debugging;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// Single background loop that moves formatted records from the outage queue to the broker (ADR 0002, ADR 0004)
/// using a sliding window: up to <see cref="PublishLoopOptions.BatchSizeLimit"/> messages are in flight or waiting for
/// a retry, and a new message is sent as soon as a slot frees up — one slow message never stalls the others.
/// <para>
/// Event-driven, so the cost per message is O(log n) regardless of the window size: finished attempts arrive through a
/// completion queue, retries wait in a priority queue ordered by their own <see cref="PendingMessage.NotBefore"/>, one
/// reusable timer wakes the loop for the next due retry, and at most one waiter each watches readiness and new data.
/// </para>
/// An event leaves the loop only when <see cref="OutcomeState.Accepted"/> or handed to <see cref="UndeliveredEvents"/>.
/// </summary>
internal sealed class PublishLoop
{
    private const string LogPrefix = "[RabbitMQAmqp] ";
    private const double Jitter = 0.2;
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxExpiryCheckInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MinExpiryCheckInterval = TimeSpan.FromMilliseconds(10);

    private readonly SizeBoundedQueue<FormattedEvent> _queue;
    private readonly IPublisherProvider _provider;
    private readonly AmqpMessageBuilder _builder;
    private readonly UndeliveredEvents _undelivered;
    private readonly PublishLoopOptions _options;
    private readonly TimeProvider _time;
    private readonly PriorityQueue<PendingMessage, long> _retry = new();
    private readonly Dictionary<PendingMessage, Task> _inFlight = [];
    private readonly ConcurrentQueue<Completion> _completions = new();
    private readonly Channel<bool> _wake =
        Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true });
    private Task<IPublisher>? _readyWatch;
    private Task<bool>? _queueWatch;
    private int _pendingCount;
    private long _accepted;
    private long _nextExpiryCheck;

    public PublishLoop(
        SizeBoundedQueue<FormattedEvent> queue,
        IPublisherProvider provider,
        AmqpMessageBuilder builder,
        UndeliveredEvents undelivered,
        PublishLoopOptions options,
        TimeProvider? timeProvider = null)
    {
        _queue = queue;
        _provider = provider;
        _builder = builder;
        _undelivered = undelivered;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        if (options.FallbackAfter is { } limit)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(limit, TimeSpan.Zero, nameof(options));
            _expiryCheckInterval = TimeSpan.FromTicks(Math.Clamp(limit.Ticks / 4, MinExpiryCheckInterval.Ticks, MaxExpiryCheckInterval.Ticks));
        }
    }

    private readonly TimeSpan _expiryCheckInterval;

    private enum Attempt
    {
        Accepted,
        Released,
        Throttled,
        TargetMissing,
        NotOpen,
        Timeout,
        Rejected,
        Invalid,
    }

    /// <summary>Messages taken from the queue and not yet accepted or dropped (in flight + waiting for retry).</summary>
    public int PendingCount => Volatile.Read(ref _pendingCount);

    /// <summary>Messages accepted by the broker so far.</summary>
    public long AcceptedCount => Interlocked.Read(ref _accepted);

    /// <summary>
    /// Runs until the queue is completed and everything is delivered or dropped (graceful), or until
    /// <paramref name="abortToken"/> fires. Whatever is undelivered at the end is reported as
    /// <see cref="LoggingFailureKind.Final"/>; messages accepted before the abort are not. Never throws.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "The loop must survive unexpected failures; they go to SelfLog and the loop continues.")]
    public async Task RunAsync(CancellationToken abortToken)
    {
        using var timer = _time.CreateTimer(static state => ((PublishLoop)state!).Wake(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        // Learn about the end of the input even while not ready (no publisher → no queue watcher), so an idle
        // shutdown with the broker down finishes immediately instead of waiting for the deadline.
        _ = _queue.Completion.ContinueWith(static (_, state) => ((PublishLoop)state!).Wake(), this,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        try
        {
            while (true)
            {
                try
                {
                    if (Iterate(timer, abortToken))
                    {
                        return; // completed and drained
                    }

                    await _wake.Reader.WaitToReadAsync(abortToken).ConfigureAwait(false);
                    _wake.Reader.TryRead(out _);
                }
                catch (OperationCanceledException) when (abortToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine(LogPrefix + "Publish loop failure: {0}", ex);
                    await DelayIgnoringCancellationAsync(_options.RetryMaxDelay, abortToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            await SettleInFlightAsync().ConfigureAwait(false);
            DrainCompletions();
            ReportUndelivered();
        }
    }

    /// <summary>
    /// After an abort the in-flight attempts complete promptly (their wait is cancelled). Waiting for them before the
    /// final report means attempts the broker accepted in the meantime are counted, not reported as undelivered.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Best effort: whatever has not settled is reported as undelivered.")]
    private async Task SettleInFlightAsync()
    {
        if (_inFlight.Count == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(_inFlight.Values).WaitAsync(SettleTimeout).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Timeout: the rest stays in flight and is reported as undelivered.
        }
    }

    private void Wake() => _wake.Writer.TryWrite(true);

    /// <summary>Collect → dispatch → arm waiters. Returns <see langword="true"/> when there is nothing left to do.</summary>
    private bool Iterate(ITimer timer, CancellationToken abortToken)
    {
        DrainCompletions();
        ExpireOverdue();

        var publisher = TryGetReadyPublisher(abortToken, out var recheckProvider);
        if (publisher is not null)
        {
            Dispatch(publisher, abortToken);
        }

        UpdatePendingCount();
        if (_inFlight.Count == 0 && _retry.Count == 0 && _queue.IsCompletedAndEmpty)
        {
            return true;
        }

        TimeSpan? wakeIn = recheckProvider ? _options.RetryMinDelay : null;
        if (publisher is not null && _inFlight.Count < _options.BatchSizeLimit)
        {
            if (_retry.TryPeek(out _, out var due))
            {
                wakeIn = Earliest(wakeIn, Until(due));
            }

            WatchQueue(abortToken);
        }

        // Waiting records must reach the fallback on time even while the broker is down: then nothing else wakes the
        // loop (no publisher → no queue watcher), so tick at the check interval.
        if (_options.FallbackAfter is not null && (publisher is null || _retry.Count > 0 || _queue.Count > 0))
        {
            wakeIn = Earliest(wakeIn, Until(_nextExpiryCheck));
        }

        if (wakeIn is { } delay)
        {
            timer.Change(delay, Timeout.InfiniteTimeSpan);
        }

        return false;
    }

    private static TimeSpan Earliest(TimeSpan? current, TimeSpan candidate) =>
        current is { } value && value <= candidate ? value : candidate;

    /// <summary>
    /// <c>FallbackAfter</c>: records that waited longer than the limit since formatting (in the queue or for a retry)
    /// are handed to the fallback. In-flight attempts are left alone — the broker may still accept them. Checked at
    /// most every quarter of the limit (≤ 1 s), so the cost stays independent of the event rate.
    /// </summary>
    private void ExpireOverdue()
    {
        if (_options.FallbackAfter is not { } limit)
        {
            return;
        }

        var now = _time.GetTimestamp();
        if (now < _nextExpiryCheck)
        {
            return;
        }

        _nextExpiryCheck = now + ToTimestampTicks(_expiryCheckInterval);
        var cutoff = now - ToTimestampTicks(limit);
        List<FormattedEvent>? expired = null;

        if (_retry.Count > 0)
        {
            List<PendingMessage>? overdue = null;
            foreach (var (message, _) in _retry.UnorderedItems)
            {
                if (message.Record.FormattedAt <= cutoff)
                {
                    (overdue ??= []).Add(message);
                }
            }

            if (overdue is not null)
            {
                foreach (var message in overdue)
                {
                    _retry.Remove(message, out _, out _);
                    (expired ??= []).Add(message.Record);
                }
            }
        }

        // The queue is FIFO by formatting time, so only its head can be overdue.
        while (_queue.TryDequeueIf(record => record.FormattedAt <= cutoff, out var record))
        {
            (expired ??= []).Add(record);
        }

        if (expired is not null)
        {
            UpdatePendingCount();
            _undelivered.Report(LoggingFailureKind.Permanent,
                $"RabbitMQ did not accept these log events within FallbackAfter ({limit}); handed to the fallback",
                expired);
        }
    }

    private long ToTimestampTicks(TimeSpan span) => (long)(span.TotalSeconds * _time.TimestampFrequency);

    /// <summary>The current open publisher, or <see langword="null"/> (then one waiter wakes the loop when ready).</summary>
    private IPublisher? TryGetReadyPublisher(CancellationToken abortToken, out bool recheck)
    {
        recheck = false;
        if (_readyWatch is { IsCompleted: false })
        {
            return null;
        }

        var ready = _provider.WaitReadyAsync(abortToken); // completed task when ready: no allocation, no registration
        if (ready.IsCompletedSuccessfully)
        {
            _readyWatch = null;
            return ready.Result;
        }

        if (ready.IsCompleted)
        {
            // Provider shutting down (cancelled): look again shortly.
            _readyWatch = null;
            recheck = true;
            return null;
        }

        _readyWatch = ready;
        ready.ContinueWith(static (_, state) => ((PublishLoop)state!).Wake(), this,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return null;
    }

    /// <summary>One waiter for new records while the window has room.</summary>
    private void WatchQueue(CancellationToken abortToken)
    {
        if (!HasRoomForNewMessages() || Volatile.Read(ref _queueWatch) is not null || _queue.IsCompletedAndEmpty)
        {
            return;
        }

        var watch = _queue.WaitToDequeueAsync(abortToken);
        if (watch.IsCompleted)
        {
            Wake();
            return;
        }

        Volatile.Write(ref _queueWatch, watch);
        watch.ContinueWith(static (_, state) =>
        {
            var loop = (PublishLoop)state!;
            Volatile.Write(ref loop._queueWatch, null);
            loop.Wake();
        }, this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    /// <summary>Fills free window slots: due retries first, then new records from the queue.</summary>
    private void Dispatch(IPublisher publisher, CancellationToken abortToken)
    {
        var now = _time.GetTimestamp();
        while (_inFlight.Count < _options.BatchSizeLimit)
        {
            PendingMessage next;
            if (_retry.TryPeek(out _, out var notBefore) && notBefore <= now)
            {
                next = _retry.Dequeue();
            }
            else if (HasRoomForNewMessages() && _queue.TryDequeue(out var record))
            {
                next = new PendingMessage(record);
            }
            else
            {
                return;
            }

            _inFlight.Add(next, RunAttemptAsync(publisher, next, abortToken));
        }
    }

    /// <summary>Memory bound: in flight + waiting for retry never exceed the window.</summary>
    private bool HasRoomForNewMessages() => _inFlight.Count + _retry.Count < _options.BatchSizeLimit;

    private async Task RunAttemptAsync(IPublisher publisher, PendingMessage message, CancellationToken ct)
    {
        AttemptResult? result;
        try
        {
            result = await PublishOneAsync(publisher, message, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            result = null; // aborted: undelivered
        }

        _completions.Enqueue(new Completion(message, publisher, result));
        Wake();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Build failures concern this record only (permanent); publish failures mean 'not delivered yet'.")]
    private async Task<AttemptResult> PublishOneAsync(IPublisher publisher, PendingMessage pending, CancellationToken ct)
    {
        IMessage message;
        try
        {
            message = _builder.Build(pending.Record);
        }
        catch (Exception ex)
        {
            // Retrying would fail forever and must not look like "not open".
            return new AttemptResult(Attempt.Invalid, ex.Message);
        }

        try
        {
            var result = await publisher.PublishAsync(message, ct)
                .WaitAsync(_options.PublishTimeout, _time, ct)
                .ConfigureAwait(false);

            var detail = result.Outcome.Error?.ToString();
            return OutcomeClassifier.Classify(result.Outcome) switch
            {
                OutcomeKind.Accepted => new AttemptResult(Attempt.Accepted, null),
                OutcomeKind.Released => new AttemptResult(Attempt.Released, null),
                OutcomeKind.Throttled => new AttemptResult(Attempt.Throttled, detail),
                OutcomeKind.TargetMissing => new AttemptResult(Attempt.TargetMissing, detail),
                OutcomeKind.ConnectionLost => new AttemptResult(Attempt.NotOpen, detail),
                _ => new AttemptResult(Attempt.Rejected, detail),
            };
        }
        catch (TimeoutException)
        {
            return new AttemptResult(Attempt.Timeout, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new AttemptResult(Attempt.NotOpen, ex.Message);
        }
    }

    /// <summary>Handles every finished attempt individually (also on abort, so accepted messages are counted).</summary>
    private void DrainCompletions()
    {
        List<FormattedEvent>? rejected = null, invalid = null, overReleased = null;
        string? rejectDetail = null, invalidDetail = null;
        var timeouts = 0;

        while (_completions.TryDequeue(out var completion))
        {
            var message = completion.Message;
            _inFlight.Remove(message);
            if (completion.Result is not { } result)
            {
                _retry.Enqueue(message, 0); // aborted: stays undelivered for the final report
                continue;
            }

            switch (result.Kind)
            {
                case Attempt.Accepted:
                    Interlocked.Increment(ref _accepted);
                    _provider.ReportPublishSucceeded(completion.Publisher);
                    break;

                case Attempt.Released:
                    message.ReleasedCount++;
                    if (message.ReleasedCount > _options.MaxReleasedRetries)
                    {
                        (overReleased ??= []).Add(message.Record);
                    }
                    else
                    {
                        ScheduleRetry(message, _options.SlowRetryMinDelay, _options.SlowRetryMaxDelay, message.ReleasedCount - 1);
                    }

                    break;

                case Attempt.Throttled:
                    ScheduleRetry(message, _options.SlowRetryMinDelay, _options.SlowRetryMaxDelay, message.FailedAttempts++);
                    break;

                case Attempt.TargetMissing:
                    ScheduleRetry(message, _options.RetryMinDelay, _options.RetryMaxDelay, message.FailedAttempts++);
                    _provider.ReportTargetMissing(completion.Publisher);
                    break;

                case Attempt.NotOpen:
                    ScheduleRetry(message, _options.RetryMinDelay, _options.RetryMaxDelay, message.FailedAttempts++);
                    _provider.Signal();
                    break;

                case Attempt.Timeout:
                    message.FailedAttempts++;
                    message.NotBefore = 0; // already waited PublishTimeout
                    _retry.Enqueue(message, 0);
                    timeouts++;
                    _provider.ReportPublishTimeout(completion.Publisher);
                    break;

                case Attempt.Invalid:
                    (invalid ??= []).Add(message.Record);
                    invalidDetail ??= result.Detail;
                    break;

                default:
                    (rejected ??= []).Add(message.Record);
                    rejectDetail ??= result.Detail;
                    break;
            }
        }

        UpdatePendingCount();

        if (timeouts > 0)
        {
            SelfLog.WriteLine(LogPrefix + "{0} publish(es) got no broker outcome within {1}; retrying.", timeouts, _options.PublishTimeout);
        }

        if (rejected is not null)
        {
            _undelivered.Report(LoggingFailureKind.Permanent, $"RabbitMQ rejected log events: {rejectDetail}", rejected);
        }

        if (invalid is not null)
        {
            _undelivered.Report(LoggingFailureKind.Permanent, $"Failed to build the AMQP message: {invalidDetail}", invalid);
        }

        if (overReleased is not null)
        {
            _undelivered.Report(LoggingFailureKind.Permanent,
                $"RabbitMQ released log events more than {_options.MaxReleasedRetries} times (unroutable? check bindings)",
                overReleased);
        }
    }

    private void ScheduleRetry(PendingMessage message, TimeSpan min, TimeSpan max, int attempt)
    {
        var delay = ExponentialBackoff.Compute(min, max, attempt, Jitter, Random.Shared.NextDouble());
        message.NotBefore = _time.GetTimestamp() + ToTimestampTicks(delay);
        _retry.Enqueue(message, message.NotBefore);
    }

    private TimeSpan Until(long timestamp)
    {
        var remaining = timestamp - _time.GetTimestamp();
        return remaining <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds((double)remaining / _time.TimestampFrequency);
    }

    private void UpdatePendingCount() => Volatile.Write(ref _pendingCount, _inFlight.Count + _retry.Count);

    private void ReportUndelivered()
    {
        var undelivered = new List<FormattedEvent>(_inFlight.Count + _retry.Count);
        undelivered.AddRange(_inFlight.Keys.Select(m => m.Record));
        undelivered.AddRange(_retry.UnorderedItems.Select(item => item.Element.Record));
        _inFlight.Clear();
        _retry.Clear();
        undelivered.AddRange(_queue.DrainAll());

        UpdatePendingCount();
        if (undelivered.Count > 0)
        {
            _undelivered.Report(LoggingFailureKind.Final, "RabbitMQ sink stopped before these log events were delivered", undelivered);
        }
    }

    private async Task DelayIgnoringCancellationAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, _time, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutdown — the outer loop observes the token.
        }
    }

    private readonly record struct AttemptResult(Attempt Kind, string? Detail);

    private readonly record struct Completion(PendingMessage Message, IPublisher Publisher, AttemptResult? Result);
}

using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// Single background loop that moves events from the buffer to the broker (ADR 0002):
/// keeps a retry list of not-yet-accepted messages (sent before new events), publishes a round concurrently,
/// waits for readiness instead of blind back-off, and lets an event leave only when it is
/// <see cref="OutcomeState.Accepted"/> or reported to the failure listener.
/// </summary>
internal sealed class PublishLoop
{
    private const string LogPrefix = "[RabbitMQAmqp] ";

    private readonly ChannelReader<LogEvent> _reader;
    private readonly IPublisherProvider _provider;
    private readonly LogEventMessageFactory _factory;
    private readonly FailureReporter _reporter;
    private readonly PublishLoopOptions _options;
    private readonly TimeProvider _time;
    private readonly ExponentialBackoff _retryBackoff;
    private readonly List<PendingMessage> _pending = [];
    private int _pendingCount;
    private long _accepted;

    public PublishLoop(
        ChannelReader<LogEvent> reader,
        IPublisherProvider provider,
        LogEventMessageFactory factory,
        FailureReporter reporter,
        PublishLoopOptions options,
        TimeProvider? timeProvider = null)
    {
        _reader = reader;
        _provider = provider;
        _factory = factory;
        _reporter = reporter;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _retryBackoff = new ExponentialBackoff(options.RetryMinDelay, options.RetryMaxDelay);
    }

    private enum Attempt
    {
        Accepted,
        Released,
        TargetMissing,
        Rejected,
        NotOpen,
        Timeout,
    }

    /// <summary>Messages taken from the buffer but not yet accepted or dropped.</summary>
    public int PendingCount => Volatile.Read(ref _pendingCount);

    /// <summary>Messages accepted by the broker so far.</summary>
    public long AcceptedCount => Interlocked.Read(ref _accepted);

    /// <summary>
    /// Runs until the buffer is completed and drained (graceful) or <paramref name="abortToken"/> fires.
    /// Whatever is undelivered at the end is reported as <see cref="LoggingFailureKind.Final"/>. Never throws.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "The loop must survive unexpected failures; they go to SelfLog and the loop continues.")]
    public async Task RunAsync(CancellationToken abortToken)
    {
        try
        {
            while (true)
            {
                try
                {
                    if (_pending.Count == 0 && !await _reader.WaitToReadAsync(abortToken).ConfigureAwait(false))
                    {
                        return; // completed and drained
                    }

                    Fill();
                    if (_pending.Count == 0)
                    {
                        continue;
                    }

                    var publisher = await _provider.WaitReadyAsync(abortToken).ConfigureAwait(false);
                    if (await PublishRoundAsync(publisher, abortToken).ConfigureAwait(false))
                    {
                        _retryBackoff.Reset();
                    }
                    else if (_pending.Count > 0)
                    {
                        await Task.Delay(_retryBackoff.Next(), _time, abortToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (abortToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine(LogPrefix + "Publish loop failure: {0}", ex);
                    await DelayIgnoringCancellationAsync(_retryBackoff.Next(), abortToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            ReportUndelivered();
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A formatter or routing-key selector failure drops only that event, reported to the listener.")]
    private void Fill()
    {
        while (_pending.Count < _options.BatchSizeLimit && _reader.TryRead(out var logEvent))
        {
            try
            {
                _pending.Add(_factory.Prepare(logEvent));
            }
            catch (Exception ex)
            {
                _reporter.Report(LoggingFailureKind.Permanent, "Failed to format log event for RabbitMQ", [logEvent], ex);
            }
        }

        Volatile.Write(ref _pendingCount, _pending.Count);
    }

    /// <summary>Publishes all pending messages concurrently. Returns whether any message left the pipeline.</summary>
    private async Task<bool> PublishRoundAsync(IPublisher publisher, CancellationToken ct)
    {
        var attempts = new Task<(Attempt Result, string? Detail)>[_pending.Count];
        for (var i = 0; i < attempts.Length; i++)
        {
            attempts[i] = PublishOneAsync(publisher, _pending[i], ct);
        }

        await Task.WhenAll(attempts).ConfigureAwait(false);

        List<LogEvent>? rejected = null;
        List<LogEvent>? overReleased = null;
        string? rejectDetail = null;
        var accepted = 0;
        var targetMissing = false;
        var timeouts = 0;
        var notOpen = false;
        var kept = new List<PendingMessage>(_pending.Count);

        for (var i = 0; i < attempts.Length; i++)
        {
            var message = _pending[i];
            var (result, detail) = attempts[i].Result;
            switch (result)
            {
                case Attempt.Accepted:
                    accepted++;
                    break;
                case Attempt.Released when ++message.ReleasedCount > _options.MaxReleasedRetries:
                    (overReleased ??= []).Add(message.LogEvent);
                    break;
                case Attempt.Rejected:
                    (rejected ??= []).Add(message.LogEvent);
                    rejectDetail ??= detail;
                    break;
                case Attempt.TargetMissing:
                    targetMissing = true;
                    kept.Add(message);
                    break;
                case Attempt.Timeout:
                    timeouts++;
                    kept.Add(message);
                    break;
                case Attempt.NotOpen:
                    notOpen = true;
                    kept.Add(message);
                    break;
                default: // Released, still within the retry budget
                    kept.Add(message);
                    break;
            }
        }

        _pending.Clear();
        _pending.AddRange(kept);
        Volatile.Write(ref _pendingCount, _pending.Count);
        Interlocked.Add(ref _accepted, accepted);

        if (accepted > 0)
        {
            _provider.ReportPublishSucceeded(publisher);
        }

        if (targetMissing)
        {
            _provider.ReportTargetMissing(publisher);
        }

        if (timeouts > 0)
        {
            SelfLog.WriteLine(LogPrefix + "{0} publish(es) got no broker outcome within {1}; retrying.",
                timeouts, _options.PublishTimeout);
            _provider.ReportPublishTimeout(publisher);
        }

        if (notOpen)
        {
            _provider.Signal();
        }

        if (rejected is not null)
        {
            _reporter.Report(LoggingFailureKind.Permanent, $"RabbitMQ rejected log events: {rejectDetail}", rejected);
        }

        if (overReleased is not null)
        {
            _reporter.Report(LoggingFailureKind.Permanent,
                $"RabbitMQ released log events more than {_options.MaxReleasedRetries} times (unroutable? check bindings)",
                overReleased);
        }

        return accepted > 0 || rejected is not null || overReleased is not null;
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "Any publish failure other than shutdown means 'not delivered yet' and is retried.")]
    private async Task<(Attempt Result, string? Detail)> PublishOneAsync(IPublisher publisher, PendingMessage pending, CancellationToken ct)
    {
        try
        {
            var result = await publisher.PublishAsync(_factory.Build(pending), ct)
                .WaitAsync(_options.PublishTimeout, _time, ct)
                .ConfigureAwait(false);

            return OutcomeClassifier.Classify(result.Outcome) switch
            {
                OutcomeKind.Accepted => (Attempt.Accepted, null),
                OutcomeKind.Released => (Attempt.Released, null),
                OutcomeKind.TargetMissing => (Attempt.TargetMissing, result.Outcome.Error?.ToString()),
                OutcomeKind.ConnectionLost => (Attempt.NotOpen, result.Outcome.Error?.ToString()),
                _ => (Attempt.Rejected, result.Outcome.Error?.ToString()),
            };
        }
        catch (TimeoutException)
        {
            return (Attempt.Timeout, null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (Attempt.NotOpen, ex.Message);
        }
    }

    private void ReportUndelivered()
    {
        var undelivered = new List<LogEvent>(_pending.Count);
        undelivered.AddRange(_pending.Select(p => p.LogEvent));
        _pending.Clear();
        while (_reader.TryRead(out var logEvent))
        {
            undelivered.Add(logEvent);
        }

        Volatile.Write(ref _pendingCount, 0);
        if (undelivered.Count > 0)
        {
            _reporter.Report(LoggingFailureKind.Final,
                "RabbitMQ sink stopped before these log events were delivered", undelivered);
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
}

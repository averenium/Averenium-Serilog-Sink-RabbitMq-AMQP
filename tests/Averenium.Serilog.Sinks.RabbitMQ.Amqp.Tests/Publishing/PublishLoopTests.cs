using System.Collections.Concurrent;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Core;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

public sealed class PublishLoopTests
{
    private readonly SizeBoundedQueue<FormattedEvent> _queue = new(long.MaxValue, OverflowPolicy.DropOldest, r => r.EstimatedSize);
    private LogEventFormatter _formatter = new(new CompactJsonFormatter());
    private readonly CollectingFailureListener _failures = new();
    private readonly FakePublisherProvider _provider = new();
    private UndeliveredEvents? _undelivered;

    [Test]
    public async Task Delivers_all_events_and_stops_when_the_buffer_is_completed(CancellationToken ct)
    {
        var loop = CreateLoop();
        Write(1, 2, 3, 4, 5);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(5L);
        await Assert.That(_provider.AcceptedMessageIds.Count).IsEqualTo(5);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
        await Assert.That(_provider.Succeeded).IsGreaterThan(0);
    }

    [Test]
    public async Task Released_messages_are_retried_until_accepted(CancellationToken ct)
    {
        _provider.Script = attempt => attempt == 0 ? Outcome(OutcomeState.Released) : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop();
        Write(1, 2);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(2L);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Messages_released_too_often_are_dropped_and_reported(CancellationToken ct)
    {
        _provider.Script = _ => Outcome(OutcomeState.Released);
        var loop = CreateLoop(maxReleasedRetries: 2);
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(_provider.PublishCalls).IsEqualTo(3);
        await Assert.That(_failures.Events.Count).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Kind).IsEqualTo(LoggingFailureKind.Permanent);
        await Assert.That(_failures.Events[0].Message).Contains("released");
    }

    [Test]
    public async Task Rejected_messages_are_dropped_and_reported_without_retry(CancellationToken ct)
    {
        _provider.Script = _ => Outcome(OutcomeState.Rejected, "amqp:link:message-size-exceeded");
        var loop = CreateLoop();
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(_provider.PublishCalls).IsEqualTo(1);
        await Assert.That(_failures.Events.Count).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Message).Contains("message-size-exceeded");
    }

    [Test]
    public async Task Missing_target_keeps_messages_and_asks_for_rebuild(CancellationToken ct)
    {
        _provider.Script = attempt => attempt < 2 ? Outcome(OutcomeState.Rejected, OutcomeClassifier.NotFound) : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop();
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(1L);
        await Assert.That(_provider.TargetMissing).IsEqualTo(2);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Publish_exceptions_are_retried_and_signal_the_supervisor(CancellationToken ct)
    {
        _provider.Script = attempt => attempt == 0
            ? Task.FromException<PublishOutcome>(new AmqpNotOpenException("Resource is reconnecting"))
            : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop();
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(1L);
        await Assert.That(_provider.Signals).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task Rejection_caused_by_a_lost_connection_is_retried_not_dropped(CancellationToken ct)
    {
        // Seen in integration tests: in-flight messages are settled as Rejected/amqp:internal-error
        // "Connection forced" when the broker shuts down.
        _provider.Script = attempt => attempt == 0 ? Outcome(OutcomeState.Rejected, "amqp:internal-error") : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop();
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(1L);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
        await Assert.That(_provider.Signals).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task Timed_out_publish_is_retried_with_a_fresh_message_and_the_same_id(CancellationToken ct)
    {
        _provider.Script = attempt => attempt == 0 ? new TaskCompletionSource<PublishOutcome>().Task : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop(publishTimeout: TimeSpan.FromMilliseconds(100));
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(1L);
        await Assert.That(_provider.Timeouts).IsEqualTo(1);
        await Assert.That(_provider.Messages.Count).IsEqualTo(2);
        await Assert.That(_provider.Messages[0]).IsNotSameReferenceAs(_provider.Messages[1]);
        await Assert.That(_provider.Messages[0].MessageId()).IsEqualTo(_provider.Messages[1].MessageId());
    }

    [Test]
    public async Task Abort_reports_undelivered_events_as_final(CancellationToken ct)
    {
        _provider.NeverReady = true;
        var loop = CreateLoop();
        Write(1, 2, 3);
        using var abort = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await loop.RunAsync(abort.Token).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(_failures.Events.Count).IsEqualTo(3);
        await Assert.That(_failures.Events.All(e => e.Kind == LoggingFailureKind.Final)).IsTrue();
        await Assert.That(loop.PendingCount).IsEqualTo(0);
    }

    [Test]
    public async Task Batches_are_limited_to_the_batch_size(CancellationToken ct)
    {
        var loop = CreateLoop(batchSizeLimit: 3);
        Write(1, 2, 3, 4, 5, 6, 7);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(7L);
        await Assert.That(_provider.MaxConcurrent).IsLessThanOrEqualTo(3);
    }

    [Test]
    public async Task Message_build_failure_is_permanent_and_not_retried(CancellationToken ct)
    {
        var loop = CreateLoop(exchange: null); // a per-event routing key needs an exchange: Build throws
        _queue.TryEnqueue(new LogEventFormatter(new CompactJsonFormatter(), _ => "key").Format(TestEvents.Create(2)), []);
        Write(1, 3);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(2L);
        await Assert.That(_failures.Events.Count).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Event.N()).IsEqualTo(2);
        await Assert.That(_failures.Events[0].Kind).IsEqualTo(LoggingFailureKind.Permanent);
        await Assert.That(_failures.Events[0].Message).Contains("Failed to build");
        await Assert.That(_provider.Signals).IsEqualTo(0).Because("a build failure is not a connection problem");
    }

    [Test]
    public async Task Released_retries_are_spaced_in_time_even_while_other_messages_are_accepted(CancellationToken ct)
    {
        var time = new FakeTimeProvider();
        _provider.MessageScript = (m, _) => IsEvent(m, 1) ? Outcome(OutcomeState.Released) : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop(maxReleasedRetries: 5, timeProvider: time, slowRetryMinDelay: TimeSpan.FromSeconds(1));
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);

        Write(1);
        for (var n = 2; n <= 30; n++)
        {
            Write(n);
            await Task.Delay(5, ct); // a steady stream of accepted messages; no time passes on the fake clock
        }

        await Eventually.TrueAsync(() => loop.AcceptedCount == 29, "the other messages flow", ct);
        await Assert.That(_provider.AttemptsFor(m => IsEvent(m, 1))).IsEqualTo(1)
            .Because("progress of other messages must not reset this message's back-off");

        await Eventually.TrueWhileAdvancingAsync(time, TimeSpan.FromMilliseconds(500), () => _failures.Events.Count == 1,
            "released 6 times over ~31 s of fake time, then dropped", ct, TimeSpan.FromSeconds(10));
        await Assert.That(_provider.AttemptsFor(m => IsEvent(m, 1))).IsEqualTo(6);

        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
    }

    [Test]
    public async Task Resource_limit_rejections_are_retried_without_limit(CancellationToken ct)
    {
        _provider.Script = attempt => attempt < 10
            ? Outcome(OutcomeState.Rejected, OutcomeClassifier.ResourceLimitExceeded)
            : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop(maxReleasedRetries: 1);
        Write(1);
        _queue.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(10), ct);

        await SettleAsync();

        await Assert.That(loop.AcceptedCount).IsEqualTo(1L);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
        await Assert.That(_provider.PublishCalls).IsEqualTo(11);
    }

    [Test]
    public async Task A_message_without_outcome_does_not_stall_the_others(CancellationToken ct)
    {
        _provider.MessageScript = (m, _) => IsEvent(m, 1) ? new TaskCompletionSource<PublishOutcome>().Task : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop(publishTimeout: TimeSpan.FromMinutes(5), batchSizeLimit: 5);
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);

        Write(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        await Eventually.TrueAsync(() => loop.AcceptedCount == 9, "the sliding window keeps sending around the stuck message", ct);
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
    }

    [Test]
    public async Task Abort_reports_only_undelivered_messages_and_counts_the_accepted_ones(CancellationToken ct)
    {
        _provider.MessageScript = (m, _) => IsEvent(m, 1) ? new TaskCompletionSource<PublishOutcome>().Task : Outcome(OutcomeState.Accepted);
        var loop = CreateLoop(publishTimeout: TimeSpan.FromMinutes(5));
        using var abort = new CancellationTokenSource();
        var run = loop.RunAsync(abort.Token);
        Write(1, 2, 3, 4, 5);
        await Eventually.TrueAsync(() => loop.AcceptedCount == 4, "all but the stuck message accepted", ct);

        await abort.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();

        await Assert.That(_failures.Events.Count).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Event.N()).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Kind).IsEqualTo(LoggingFailureKind.Final);
        await Assert.That(loop.AcceptedCount).IsEqualTo(4L);
    }

    [Test]
    public async Task The_window_bounds_how_much_is_taken_from_the_buffer_while_publishes_fail(CancellationToken ct)
    {
        _provider.Script = _ => Task.FromException<PublishOutcome>(new AmqpNotOpenException("Resource is reconnecting"));
        var loop = CreateLoop(batchSizeLimit: 3);
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);
        Write(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        await Eventually.TrueAsync(() => _provider.PublishCalls >= 20, "retrying", ct);

        await Assert.That(loop.PendingCount).IsLessThanOrEqualTo(3);
        await Assert.That(_queue.Count).IsEqualTo(7).Because("the rest stays in the bounded buffer");
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
        await Assert.That(_failures.Events.Count).IsEqualTo(10);
    }

    [Test]
    public async Task Events_waiting_longer_than_fallback_after_are_handed_over_while_the_broker_is_down(CancellationToken ct)
    {
        var time = new FakeTimeProvider();
        _provider.NeverReady = true;
        var loop = CreateLoop(timeProvider: time, fallbackAfter: TimeSpan.FromMinutes(1));
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);
        Write(1, 2);
        time.Advance(TimeSpan.FromSeconds(30));
        Write(3);

        await Eventually.TrueWhileAdvancingAsync(time, TimeSpan.FromSeconds(1), () => _failures.Events.Count >= 2,
            "the two older events reach the fallback after one minute", ct);
        await Task.Delay(50, ct);

        await Assert.That(string.Join(",", _failures.Events.Select(e => e.Event.N()).Order())).IsEqualTo("1,2")
            .Because("the younger event has not waited long enough yet");
        await Assert.That(_failures.Events.All(e => e.Kind == LoggingFailureKind.Permanent)).IsTrue();
        await Assert.That(_failures.Events[0].Message).Contains("FallbackAfter");
        await Assert.That(_queue.Count).IsEqualTo(1);

        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
        await Assert.That(_failures.Events.Count).IsEqualTo(3).Because("the rest is reported as final, nothing is lost");
    }

    [Test]
    public async Task Events_retried_longer_than_fallback_after_are_handed_over(CancellationToken ct)
    {
        var time = new FakeTimeProvider();
        _provider.Script = _ => Task.FromException<PublishOutcome>(new AmqpNotOpenException("Resource is reconnecting"));
        var loop = CreateLoop(timeProvider: time, fallbackAfter: TimeSpan.FromSeconds(10));
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);
        Write(1, 2, 3);

        await Eventually.TrueWhileAdvancingAsync(time, TimeSpan.FromMilliseconds(500), () => _failures.Events.Count == 3,
            "retried events reach the fallback", ct);

        await Assert.That(_failures.Events.All(e => e.Kind == LoggingFailureKind.Permanent)).IsTrue();
        await Assert.That(loop.PendingCount).IsEqualTo(0);
        await Assert.That(_provider.PublishCalls).IsGreaterThan(3).Because("they were retried before the hand-over");

        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
        await Assert.That(_failures.Events.Count).IsEqualTo(3);
    }

    [Test]
    public async Task Without_fallback_after_events_wait_for_the_broker_indefinitely(CancellationToken ct)
    {
        var time = new FakeTimeProvider();
        _provider.NeverReady = true;
        var loop = CreateLoop(timeProvider: time);
        using var stop = new CancellationTokenSource();
        var run = loop.RunAsync(stop.Token);
        Write(1);

        time.Advance(TimeSpan.FromHours(5));
        await Task.Delay(100, ct);

        await Assert.That(_failures.Events.Count).IsEqualTo(0);
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await SettleAsync();
    }

    private static Task<PublishOutcome> Outcome(OutcomeState state, string? errorCode = null) =>
        Task.FromResult(new PublishOutcome(state, errorCode is null ? null : new Error(errorCode, "test")));

    private PublishLoop CreateLoop(
        int maxReleasedRetries = 5,
        TimeSpan? publishTimeout = null,
        int batchSizeLimit = 100,
        TimeProvider? timeProvider = null,
        TimeSpan? slowRetryMinDelay = null,
        string? exchange = "logs",
        TimeSpan? fallbackAfter = null)
    {
        _formatter = new LogEventFormatter(new CompactJsonFormatter(), timeProvider: timeProvider);
        var reporter = new FailureReporter(this);
        reporter.SetListener(_failures);
        _undelivered = new UndeliveredEvents(reporter, new CompactJsonFormatter(), rawOutput: null);
        _undelivered.Start();
        var slow = slowRetryMinDelay ?? TimeSpan.FromMilliseconds(1);
        return new PublishLoop(
            _queue,
            _provider,
            new AmqpMessageBuilder("application/json", exchange),
            _undelivered,
            new PublishLoopOptions
            {
                BatchSizeLimit = batchSizeLimit,
                PublishTimeout = publishTimeout ?? TimeSpan.FromSeconds(5),
                MaxReleasedRetries = maxReleasedRetries,
                RetryMinDelay = TimeSpan.FromMilliseconds(1),
                RetryMaxDelay = TimeSpan.FromMilliseconds(10),
                SlowRetryMinDelay = slow,
                SlowRetryMaxDelay = slow * 32,
                FallbackAfter = fallbackAfter,
            },
            timeProvider);
    }

    private static bool IsEvent(IMessage message, int n) =>
        message.BodyAsString().EndsWith("\"N\":" + n + "}", StringComparison.Ordinal);

    /// <summary>Failures are handed over on a background worker; wait until it delivered everything.</summary>
    private Task SettleAsync() => _undelivered?.CompleteAsync(TimeSpan.FromSeconds(5)) ?? Task.CompletedTask;

    private void Write(params int[] numbers)
    {
        foreach (var n in numbers)
        {
            _queue.TryEnqueue(_formatter.Format(TestEvents.Create(n)), []);
        }
    }

    /// <summary>
    /// Provider with one fake publisher. <see cref="Script"/> decides the outcome per message attempt
    /// (attempt index counted per message-id).
    /// </summary>
    private sealed class FakePublisherProvider : IPublisherProvider
    {
        private readonly ConcurrentDictionary<object, int> _attempts = new();
        private readonly Lock _gate = new();
        private readonly List<IMessage> _messages = [];
        private int _inFlight;
        private int _maxConcurrent;
        private int _succeeded;
        private int _timeouts;
        private int _targetMissing;
        private int _signals;

        public FakePublisherProvider()
        {
            Publisher.PublishAsync(Arg.Any<IMessage>(), Arg.Any<CancellationToken>())
                .Returns(ci => PublishAsync(ci.Arg<IMessage>()));
        }

        public IPublisher Publisher { get; } = Substitute.For<IPublisher>();

        public Func<int, Task<PublishOutcome>> Script { get; set; } = _ => Outcome(OutcomeState.Accepted);

        /// <summary>When set, decides per message and attempt (overrides <see cref="Script"/>).</summary>
        public Func<IMessage, int, Task<PublishOutcome>>? MessageScript { get; set; }

        public int AttemptsFor(Func<IMessage, bool> predicate) => Messages.Count(predicate);

        public bool NeverReady { get; set; }

        public ConcurrentBag<object> AcceptedMessageIds { get; } = [];

        public IReadOnlyList<IMessage> Messages
        {
            get
            {
                lock (_gate)
                {
                    return [.. _messages];
                }
            }
        }

        public int PublishCalls => Messages.Count;

        public int MaxConcurrent => Volatile.Read(ref _maxConcurrent);

        public int Succeeded => Volatile.Read(ref _succeeded);

        public int Timeouts => Volatile.Read(ref _timeouts);

        public int TargetMissing => Volatile.Read(ref _targetMissing);

        public int Signals => Volatile.Read(ref _signals);

        public Task<IPublisher> WaitReadyAsync(CancellationToken cancellationToken) =>
            NeverReady ? Task.Delay(Timeout.Infinite, cancellationToken).ContinueWith(_ => Publisher, cancellationToken) : Task.FromResult(Publisher);

        public void ReportPublishSucceeded(IPublisher publisher) => Interlocked.Increment(ref _succeeded);

        public void ReportPublishTimeout(IPublisher publisher) => Interlocked.Increment(ref _timeouts);

        public void ReportTargetMissing(IPublisher publisher) => Interlocked.Increment(ref _targetMissing);

        public void Signal() => Interlocked.Increment(ref _signals);

        private async Task<PublishResult> PublishAsync(IMessage message)
        {
            lock (_gate)
            {
                _messages.Add(message);
            }

            var attempt = _attempts.AddOrUpdate(message.MessageId(), 0, (_, n) => n + 1);
            var inFlight = Interlocked.Increment(ref _inFlight);
            InterlockedMax(ref _maxConcurrent, inFlight);
            try
            {
                await Task.Yield();
                var outcome = await (MessageScript?.Invoke(message, attempt) ?? Script(attempt));
                if (outcome.State == OutcomeState.Accepted)
                {
                    AcceptedMessageIds.Add(message.MessageId());
                }

                return new PublishResult(message, outcome);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        private static void InterlockedMax(ref int target, int value)
        {
            int current;
            while ((current = Volatile.Read(ref target)) < value
                   && Interlocked.CompareExchange(ref target, value, current) != current)
            {
            }
        }
    }
}

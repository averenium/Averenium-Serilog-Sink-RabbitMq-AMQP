using System.Collections.Concurrent;
using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using NSubstitute;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

public sealed class PublishLoopTests
{
    private readonly Channel<LogEvent> _channel = Channel.CreateUnbounded<LogEvent>();
    private readonly CollectingFailureListener _failures = new();
    private readonly FakePublisherProvider _provider = new();

    [Test]
    public async Task Delivers_all_events_and_stops_when_the_buffer_is_completed(CancellationToken ct)
    {
        var loop = CreateLoop();
        Write(1, 2, 3, 4, 5);
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(loop.AcceptedCount).IsEqualTo(2L);
        await Assert.That(_failures.Events.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Messages_released_too_often_are_dropped_and_reported(CancellationToken ct)
    {
        _provider.Script = _ => Outcome(OutcomeState.Released);
        var loop = CreateLoop(maxReleasedRetries: 2);
        Write(1);
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

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

        await Assert.That(_failures.Events.Count).IsEqualTo(3);
        await Assert.That(_failures.Events.All(e => e.Kind == LoggingFailureKind.Final)).IsTrue();
        await Assert.That(loop.PendingCount).IsEqualTo(0);
    }

    [Test]
    public async Task Formatter_failure_drops_only_that_event(CancellationToken ct)
    {
        var loop = CreateLoop(formatter: new ThrowingOnNFormatter(2));
        Write(1, 2, 3);
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(loop.AcceptedCount).IsEqualTo(2L);
        await Assert.That(_failures.Events.Count).IsEqualTo(1);
        await Assert.That(_failures.Events[0].Event.N()).IsEqualTo(2);
    }

    [Test]
    public async Task Batches_are_limited_to_the_batch_size(CancellationToken ct)
    {
        var loop = CreateLoop(batchSizeLimit: 3);
        Write(1, 2, 3, 4, 5, 6, 7);
        _channel.Writer.Complete();

        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(loop.AcceptedCount).IsEqualTo(7L);
        await Assert.That(_provider.MaxConcurrent).IsLessThanOrEqualTo(3);
    }

    private static Task<PublishOutcome> Outcome(OutcomeState state, string? errorCode = null) =>
        Task.FromResult(new PublishOutcome(state, errorCode is null ? null : new Error(errorCode, "test")));

    private PublishLoop CreateLoop(
        int maxReleasedRetries = 5,
        TimeSpan? publishTimeout = null,
        int batchSizeLimit = 100,
        ITextFormatter? formatter = null)
    {
        var reporter = new FailureReporter(this);
        reporter.SetListener(_failures);
        return new PublishLoop(
            _channel.Reader,
            _provider,
            new LogEventMessageFactory(formatter ?? new CompactJsonFormatter(), "application/json"),
            reporter,
            new PublishLoopOptions
            {
                BatchSizeLimit = batchSizeLimit,
                PublishTimeout = publishTimeout ?? TimeSpan.FromSeconds(5),
                MaxReleasedRetries = maxReleasedRetries,
                RetryMinDelay = TimeSpan.FromMilliseconds(1),
                RetryMaxDelay = TimeSpan.FromMilliseconds(10),
            });
    }

    private void Write(params int[] numbers)
    {
        foreach (var n in numbers)
        {
            _channel.Writer.TryWrite(TestEvents.Create(n));
        }
    }

    private sealed class ThrowingOnNFormatter(int failOn) : ITextFormatter
    {
        private readonly CompactJsonFormatter _inner = new();

        public void Format(LogEvent logEvent, TextWriter output)
        {
            if (logEvent.N() == failOn)
            {
                throw new FormatException("boom");
            }

            _inner.Format(logEvent, output);
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
                var outcome = await Script(attempt);
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

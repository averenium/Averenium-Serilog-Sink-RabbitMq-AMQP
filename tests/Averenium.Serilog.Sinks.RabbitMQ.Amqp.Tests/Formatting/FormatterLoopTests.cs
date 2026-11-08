using System.Threading.Channels;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Buffering;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Formatting;

public sealed class FormatterLoopTests
{
    private readonly Channel<LogEvent> _input = Channel.CreateUnbounded<LogEvent>();
    private readonly CollectingFailureListener _failures = new();

    [Test]
    public async Task Formats_everything_and_completes_the_output(CancellationToken ct)
    {
        var output = Queue(long.MaxValue);
        Write(1, 2, 3);
        _input.Writer.Complete();

        var (loop, _) = CreateLoop(output, new CompactJsonFormatter());
        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

        await Assert.That(output.Count).IsEqualTo(3);
        output.DrainAll();
        await Assert.That(output.IsCompletedAndEmpty).IsTrue();
    }

    [Test]
    public async Task Formatter_failure_drops_only_that_event(CancellationToken ct)
    {
        var output = Queue(long.MaxValue);
        Write(1, 2, 3);
        _input.Writer.Complete();

        var (loop, undelivered) = CreateLoop(output, new ThrowingOnNFormatter(2));
        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));

        await Assert.That(output.Count).IsEqualTo(2);
        await Assert.That(_failures.Events.Single().Event.N()).IsEqualTo(2);
        await Assert.That(_failures.Events.Single().Kind).IsEqualTo(LoggingFailureKind.Permanent);
    }

    [Test]
    public async Task Byte_limit_overflow_is_reported_in_batches_with_decoded_events(CancellationToken ct)
    {
        var output = Queue(4_096);
        var listener = new CountingListener();
        for (var n = 1; n <= 200; n++)
        {
            Write(n);
        }

        _input.Writer.Complete();
        var (loop, undelivered) = CreateLoop(output, new CompactJsonFormatter(), listener);
        await loop.RunAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));

        await Assert.That(listener.Events + output.Count).IsEqualTo(200).Because("every event is queued or reported");
        await Assert.That(listener.Reports).IsLessThan(5).Because("drops are reported per chunk, not per event");
        await Assert.That(listener.Numbers.All(n => n > 0)).IsTrue().Because("events are decoded with their properties");
    }

    private static SizeBoundedQueue<FormattedEvent> Queue(long limit) => new(limit, OverflowPolicy.DropOldest, r => r.EstimatedSize);

    private (FormatterLoop Loop, UndeliveredEvents Undelivered) CreateLoop(
        SizeBoundedQueue<FormattedEvent> output, ITextFormatter formatter, ILoggingFailureListener? listener = null)
    {
        var reporter = new FailureReporter(this);
        reporter.SetListener(listener ?? _failures);
        var undelivered = new UndeliveredEvents(reporter, formatter, rawOutput: null);
        undelivered.Start();
        return (new FormatterLoop(_input.Reader, output, new LogEventFormatter(formatter), undelivered, OverflowPolicy.DropOldest), undelivered);
    }

    private void Write(params int[] numbers)
    {
        foreach (var n in numbers)
        {
            _input.Writer.TryWrite(TestEvents.Create(n));
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

    private sealed class CountingListener : ILoggingFailureListener
    {
        private readonly Lock _gate = new();
        private readonly List<int> _numbers = [];
        private int _events;
        private int _reports;

        public int Events => Volatile.Read(ref _events);

        public int Reports => Volatile.Read(ref _reports);

        public IReadOnlyList<int> Numbers
        {
            get
            {
                lock (_gate)
                {
                    return [.. _numbers];
                }
            }
        }

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            Interlocked.Increment(ref _reports);
            Interlocked.Add(ref _events, events?.Count ?? 0);
            lock (_gate)
            {
                _numbers.AddRange((events ?? []).Select(e => e.N()));
            }
        }
    }
}

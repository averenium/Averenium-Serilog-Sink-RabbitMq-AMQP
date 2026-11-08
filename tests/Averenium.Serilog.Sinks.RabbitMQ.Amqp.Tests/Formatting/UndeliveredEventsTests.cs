using System.Text;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Compact;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Formatting;

public sealed class UndeliveredEventsTests
{
    private readonly CompactJsonFormatter _formatter = new();
    private readonly CollectingFailureListener _listener = new();

    [Test]
    public async Task Raw_output_receives_every_undelivered_event_from_every_path()
    {
        using var raw = new MemoryStream();
        var undelivered = Create(raw, listener: null);

        undelivered.Report(LoggingFailureKind.Permanent, "burst overflow", [TestEvents.Create(1)]);
        undelivered.Report(LoggingFailureKind.Permanent, "outage overflow", Records(2, 3));
        undelivered.Report(LoggingFailureKind.Final, "shutdown", [TestEvents.Create(4)]);
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));

        var lines = Lines(raw);
        await Assert.That(lines.Length).IsEqualTo(4).Because("LogEvent paths are formatted for stdout too");
        await Assert.That(string.Join(",", lines.Select(N).Order())).IsEqualTo("1,2,3,4")
            .Because("every path reaches stdout; Final batches are handed over first, so the order differs");
    }

    [Test]
    public async Task Raw_output_falls_back_to_a_minimal_line_when_the_formatter_is_what_failed()
    {
        using var raw = new MemoryStream();
        var reporter = new FailureReporter(this);
        var undelivered = new UndeliveredEvents(reporter, new AlwaysThrowingFormatter(), raw);
        undelivered.Start();

        undelivered.Report(LoggingFailureKind.Permanent, "format failed", [TestEvents.Create(1)]);
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));

        var line = Lines(raw).Single();
        await Assert.That(line).Contains("\"@mt\":\"Event {N}\"");
        await Assert.That(line).Contains("\"@l\":\"Information\"");
    }

    [Test]
    public async Task Records_are_decoded_only_when_the_listener_consumes_events()
    {
        var silent = Create(rawOutput: null, listener: null);
        silent.Report(LoggingFailureKind.Permanent, "outage overflow", Records(1, 2));
        await silent.CompleteAsync(TimeSpan.FromSeconds(5));
        await Assert.That(_listener.Events.Count).IsEqualTo(0);

        var withFallback = Create(rawOutput: null, _listener);
        withFallback.Report(LoggingFailureKind.Permanent, "outage overflow", Records(1, 2));
        await withFallback.CompleteAsync(TimeSpan.FromSeconds(5));
        await Assert.That(string.Join(",", _listener.Events.Select(e => e.Event.N()))).IsEqualTo("1,2");
    }

    [Test]
    public async Task Single_event_drops_are_coalesced_into_few_reports()
    {
        var reports = 0;
        var undelivered = Create(rawOutput: null, new CountingListener(() => Interlocked.Increment(ref reports)));

        for (var n = 0; n < 500; n++)
        {
            undelivered.Report(LoggingFailureKind.Permanent, "burst overflow", [TestEvents.Create(n)]);
        }

        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));
        await Assert.That(reports).IsLessThan(500);
    }

    [Test]
    public async Task A_slow_fallback_never_blocks_the_caller_and_the_excess_is_counted(CancellationToken ct)
    {
        using var gate = new ManualResetEventSlim(false);
        var slow = new CountingListener(() => gate.Wait(TimeSpan.FromSeconds(10)));
        var reporter = new FailureReporter(this);
        reporter.SetListener(slow);
        var undelivered = new UndeliveredEvents(reporter, _formatter, rawOutput: null, maxPendingEvents: 100);
        undelivered.Start();

        var started = DateTime.UtcNow;
        for (var n = 0; n < 1_000; n++)
        {
            undelivered.Report(LoggingFailureKind.Permanent, "outage overflow", Records(n));
        }

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(1))
            .Because("reporting only enqueues; the fallback runs on its own worker");
        gate.Set();
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(5));
        await Assert.That(slow.Events).IsLessThan(1_000).Because("the bounded queue dropped what the fallback could not take");
    }

    [Test]
    public async Task A_huge_final_batch_reaches_the_fallbacks_completely()
    {
        using var raw = new MemoryStream();
        var reporter = new FailureReporter(this);
        reporter.SetListener(_listener);
        var undelivered = new UndeliveredEvents(reporter, _formatter, raw, maxPendingEvents: 100);
        undelivered.Start();
        var records = Records([.. Enumerable.Range(1, 5_000)]);

        undelivered.Report(LoggingFailureKind.Final, "shutdown", records);
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(10));

        await Assert.That(_listener.Events.Count).IsEqualTo(5_000).Because("shutdown batches are exempt from the pending limit");
        await Assert.That(Lines(raw).Length).IsEqualTo(5_000);
    }

    [Test]
    public async Task After_the_completion_budget_the_worker_writes_nothing_more()
    {
        var calls = 0;
        var slow = new CountingListener(() =>
        {
            Interlocked.Increment(ref calls);
            Thread.Sleep(200);
        });
        var reporter = new FailureReporter(this);
        reporter.SetListener(slow);
        var undelivered = new UndeliveredEvents(reporter, _formatter, rawOutput: null);
        undelivered.Start();
        undelivered.Report(LoggingFailureKind.Final, "shutdown", Records([.. Enumerable.Range(1, 5_000)]));

        await undelivered.CompleteAsync(TimeSpan.FromMilliseconds(100));
        var callsAtReturn = Volatile.Read(ref calls);
        await Task.Delay(700);

        await Assert.That(Volatile.Read(ref calls)).IsEqualTo(callsAtReturn).Because("the worker stopped when the budget ran out");
        await Assert.That(callsAtReturn).IsLessThan(6);
    }

    [Test]
    public async Task Final_batches_are_handed_over_before_an_earlier_overflow_backlog()
    {
        var order = new List<LoggingFailureKind>();
        var listener = new KindRecordingListener(order, delay: TimeSpan.FromMilliseconds(30));
        var reporter = new FailureReporter(this);
        reporter.SetListener(listener);
        var undelivered = new UndeliveredEvents(reporter, _formatter, rawOutput: null);
        for (var n = 0; n < 10; n++)
        {
            undelivered.Report(LoggingFailureKind.Permanent, "outage overflow", Records([.. Enumerable.Range(n * 1_000, 1_000)]));
        }

        undelivered.Report(LoggingFailureKind.Final, "shutdown", Records(1, 2, 3));
        undelivered.Start(); // everything is queued before the worker looks at it
        await undelivered.CompleteAsync(TimeSpan.FromSeconds(10));

        await Assert.That(order[0]).IsEqualTo(LoggingFailureKind.Final).Because("shutdown events must not wait behind overflow drops");
    }

    [Test]
    public async Task Events_lost_at_shutdown_are_reported_as_final()
    {
        var listener = new KindRecordingListener([], delay: TimeSpan.FromMilliseconds(200));
        var reporter = new FailureReporter(this);
        reporter.SetListener(listener);
        var undelivered = new UndeliveredEvents(reporter, _formatter, rawOutput: null);
        undelivered.Start();
        undelivered.Report(LoggingFailureKind.Final, "shutdown", Records([.. Enumerable.Range(1, 5_000)]));

        await undelivered.CompleteAsync(TimeSpan.FromMilliseconds(100));

        await Assert.That(listener.Notifications.Any(n => n.Kind == LoggingFailureKind.Final && n.Message.Contains("shutdown time ran out", StringComparison.Ordinal)))
            .IsTrue();
        await Assert.That(listener.Notifications.Any(n => n.Kind == LoggingFailureKind.Permanent)).IsFalse();
    }

    [Test]
    public async Task When_the_worker_is_stuck_in_a_slow_fallback_completion_returns_and_the_worker_counts_the_rest()
    {
        using var raw = new MemoryStream();
        var listener = new KindRecordingListener([], delay: TimeSpan.FromSeconds(2));
        var traces = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var reporter = new FailureReporter(this, trace: traces.Enqueue);
        reporter.SetListener(listener);
        var undelivered = new UndeliveredEvents(reporter, _formatter, raw);
        undelivered.Start();
        undelivered.Report(LoggingFailureKind.Final, "shutdown", Records([.. Enumerable.Range(1, 3_000)]));
        await Task.Delay(100); // the worker is now inside the first (slow) chunk

        var started = DateTime.UtcNow;
        await undelivered.CompleteAsync(TimeSpan.FromMilliseconds(50));
        reporter.Close(); // what the sink does at the end of DisposeAsync: the fallbacks are closed after this
        var elapsed = DateTime.UtcNow - started;
        var linesAtReturn = Lines(raw).Length;
        var notificationsAtReturn = listener.Notifications.Count;

        await Assert.That(elapsed).IsLessThan(TimeSpan.FromSeconds(1.8)).Because("completion does not wait for the stuck chunk");
        await Eventually.TrueAsync(
            () => traces.Any(t => t.Contains("shutdown time ran out", StringComparison.Ordinal)),
            "the worker still accounts for what it could not hand over (SelfLog)", CancellationToken.None, TimeSpan.FromSeconds(5));
        await Assert.That(listener.Notifications.Count).IsEqualTo(notificationsAtReturn)
            .Because("the listener is not called after the sink was disposed");
        await Assert.That(Lines(raw).Length).IsEqualTo(linesAtReturn).Because("nothing more is written after completion");
    }

    private UndeliveredEvents Create(Stream? rawOutput, ILoggingFailureListener? listener)
    {
        var reporter = new FailureReporter(this);
        if (listener is not null)
        {
            reporter.SetListener(listener);
        }

        var undelivered = new UndeliveredEvents(reporter, _formatter, rawOutput);
        undelivered.Start();
        return undelivered;
    }

    private List<FormattedEvent> Records(params int[] numbers) =>
        [.. numbers.Select(n => new LogEventFormatter(_formatter).Format(TestEvents.Create(n)))];

    private static string[] Lines(MemoryStream raw) =>
        Encoding.UTF8.GetString(raw.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);

    private static int N(string line)
    {
        var start = line.IndexOf("\"N\":", StringComparison.Ordinal) + 4;
        var end = line.IndexOfAny([',', '}'], start);
        return int.Parse(line.AsSpan(start, end - start), System.Globalization.CultureInfo.InvariantCulture);
    }

    private sealed class AlwaysThrowingFormatter : ITextFormatter
    {
        public void Format(LogEvent logEvent, TextWriter output) => throw new FormatException("boom");
    }

    private sealed class KindRecordingListener(List<LoggingFailureKind> order, TimeSpan delay) : ILoggingFailureListener
    {
        private readonly Lock _gate = new();
        private readonly List<(LoggingFailureKind Kind, string Message)> _notifications = [];

        public IReadOnlyList<(LoggingFailureKind Kind, string Message)> Notifications
        {
            get
            {
                lock (_gate)
                {
                    return [.. _notifications];
                }
            }
        }

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            lock (_gate)
            {
                if (events is null)
                {
                    _notifications.Add((kind, message));
                }
                else
                {
                    order.Add(kind);
                }
            }

            if (events is not null)
            {
                Thread.Sleep(delay);
            }
        }
    }

    private sealed class CountingListener(Action onReport) : ILoggingFailureListener
    {
        private int _events;

        public int Events => Volatile.Read(ref _events);

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            onReport();
            Interlocked.Add(ref _events, events?.Count ?? 0);
        }
    }
}

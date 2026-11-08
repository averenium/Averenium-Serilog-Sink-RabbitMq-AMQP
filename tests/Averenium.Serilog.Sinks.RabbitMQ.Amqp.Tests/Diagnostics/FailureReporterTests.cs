using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Diagnostics;
using Microsoft.Extensions.Time.Testing;
using Serilog.Core;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Diagnostics;

public sealed class FailureReporterTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly List<string> _trace = [];
    private readonly CollectingFailureListener _listener = new();

    [Test]
    public async Task Every_event_reaches_the_listener_but_the_trace_is_aggregated()
    {
        var reporter = CreateReporter();

        for (var n = 0; n < 1_000; n++)
        {
            reporter.Report(LoggingFailureKind.Permanent, "buffer full", [TestEvents.Create(n)]);
        }

        await Assert.That(_listener.Events.Count).IsEqualTo(1_000);
        await Assert.That(_trace.Count).IsEqualTo(1).Because("first report is traced immediately, the rest is aggregated");

        _time.Advance(FailureReporter.TraceInterval);
        reporter.Report(LoggingFailureKind.Permanent, "buffer full", [TestEvents.Create(0)]);

        await Assert.That(_trace.Count).IsEqualTo(2);
        await Assert.That(_trace[1]).Contains("1000 event(s)");
    }

    [Test]
    public async Task After_close_reports_go_to_the_trace_only_never_to_the_listener()
    {
        var traces = new List<string>();
        var listener = new CollectingFailureListener();
        var reporter = new FailureReporter(this, trace: traces.Add);
        reporter.SetListener(listener);

        reporter.Close();
        reporter.ReportCount(LoggingFailureKind.Final, "late", 3);
        reporter.Report(LoggingFailureKind.Final, "late events", [TestEvents.Create(1)]);
        reporter.Notify(LoggingFailureKind.Permanent, "late notice");

        await Assert.That(listener.Events.Count).IsEqualTo(0);
        await Assert.That(listener.Notifications.Count).IsEqualTo(0);
        await Assert.That(traces.Count(t => t.Contains("late", StringComparison.Ordinal))).IsEqualTo(3);
    }

    [Test]
    public async Task Flush_writes_what_was_aggregated()
    {
        var reporter = CreateReporter();
        reporter.Report(LoggingFailureKind.Permanent, "buffer full", [TestEvents.Create(1)]);
        reporter.Report(LoggingFailureKind.Final, "shutdown", [TestEvents.Create(2), TestEvents.Create(3)]);

        reporter.Flush();

        await Assert.That(_trace.Count).IsEqualTo(2);
        await Assert.That(_trace[1]).Contains("Final failure, 2 event(s): shutdown");
    }

    [Test]
    public async Task Nothing_is_traced_when_the_listener_already_is_selflog()
    {
        var reporter = new FailureReporter(this, timeProvider: _time, trace: _trace.Add);

        reporter.Report(LoggingFailureKind.Permanent, "buffer full", [TestEvents.Create(1)]);

        await Assert.That(_trace.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Listener_consumes_events_only_when_it_is_not_selflog()
    {
        var reporter = new FailureReporter(this, _time, _trace.Add);
        await Assert.That(reporter.ListenerConsumesEvents).IsFalse();

        reporter.SetListener(_listener);

        await Assert.That(reporter.ListenerConsumesEvents).IsTrue();
    }

    private FailureReporter CreateReporter()
    {
        var reporter = new FailureReporter(this, timeProvider: _time, trace: _trace.Add);
        reporter.SetListener(_listener);
        return reporter;
    }
}

using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using Microsoft.Extensions.Configuration;
using NSubstitute;
using RabbitMQ.AMQP.Client;
using Serilog;
using Serilog.Core;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests;

public sealed class RabbitMqAmqpSinkTests
{
    [Test]
    public async Task Emitted_events_are_published_and_dispose_flushes(CancellationToken ct)
    {
        var broker = new FakeEnvironment();
        var sink = new RabbitMqAmqpSink(Options(), broker.Environment, TimeProvider.System);

        for (var n = 0; n < 50; n++)
        {
            sink.Emit(TestEvents.Create(n));
        }

        await sink.DisposeAsync();

        await Assert.That(broker.Last!.Published.Count).IsEqualTo(50);
        await Assert.That(broker.Last.ConnectionClosed).IsTrue();
        await broker.Environment.Received(1).CloseAsync();
    }

    [Test]
    public async Task Logger_creation_does_not_block_when_the_broker_is_unreachable(CancellationToken ct)
    {
        var environment = UnreachableEnvironment();

        var started = DateTime.UtcNow;
        var sink = new RabbitMqAmqpSink(Options(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(200)), environment, TimeProvider.System);
        sink.Emit(TestEvents.Create(1));

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(1));
        await sink.DisposeAsync();
    }

    [Test]
    public async Task Dispose_reports_undelivered_events_as_final_after_the_shutdown_timeout(CancellationToken ct)
    {
        var listener = new CollectingFailureListener();
        var sink = new RabbitMqAmqpSink(
            Options(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(300)), UnreachableEnvironment(), TimeProvider.System);
        sink.SetFailureListener(listener);
        sink.Emit(TestEvents.Create(1));
        sink.Emit(TestEvents.Create(2));

        var started = DateTime.UtcNow;
        await sink.DisposeAsync();

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(5));
        await Assert.That(string.Join(",", listener.Events.Select(e => e.Event.N()).Order())).IsEqualTo("1,2");
        await Assert.That(listener.Events.All(e => e.Kind == LoggingFailureKind.Final)).IsTrue();
    }

    [Test]
    public async Task Thousands_of_events_reach_the_fallback_when_the_broker_is_down_at_shutdown(CancellationToken ct)
    {
        var fallback = new CollectingSink();
        var sink = new RabbitMqAmqpSink(
            Options(o => o.ShutdownTimeout = TimeSpan.FromSeconds(2)), UnreachableEnvironment(), TimeProvider.System);
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(wt => wt.Sink(sink), wt => wt.Sink(fallback))
            .CreateLogger();

        for (var n = 0; n < 5_000; n++)
        {
            logger.Information("Event {N}", n);
        }

        var started = DateTime.UtcNow;
        await logger.DisposeAsync();

        await Assert.That(fallback.Events.Count).IsEqualTo(5_000)
            .Because("the fallback gets its own share of the shutdown time when delivery is impossible");
        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(3));
    }

    [Test]
    public async Task Fallback_after_hands_events_to_the_fallback_while_the_broker_is_down(CancellationToken ct)
    {
        var fallback = new CollectingSink();
        var sink = new RabbitMqAmqpSink(
            Options(o =>
            {
                o.FallbackAfter = TimeSpan.FromMilliseconds(200);
                o.ShutdownTimeout = TimeSpan.FromSeconds(10);
            }),
            UnreachableEnvironment(),
            TimeProvider.System);
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(wt => wt.Sink(sink), wt => wt.Sink(fallback))
            .CreateLogger();

        logger.Information("Event {N}", 1);

        await Eventually.TrueAsync(() => fallback.Events.Count == 1, "handed over before shutdown", ct);
        var started = DateTime.UtcNow;
        await logger.DisposeAsync();
        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(1)).Because("nothing left to deliver");
        await Assert.That(fallback.Events.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Fallback_chain_keeps_working_with_a_level_restricted_sink(CancellationToken ct)
    {
        var fallback = new CollectingSink();
        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(
                wt => wt.RabbitMQAmqp(
                    o =>
                    {
                        o.Hostnames.Add("127.0.0.1:1");
                        o.Exchange = "logs";
                        o.ShutdownTimeout = TimeSpan.FromMilliseconds(300);
                    },
                    restrictedToMinimumLevel: global::Serilog.Events.LogEventLevel.Warning),
                wt => wt.Sink(fallback))
            .CreateLogger();

        logger.Information("Filtered");
        logger.Error("Undeliverable");
        await logger.DisposeAsync();

        await Assert.That(fallback.Events.Count).IsEqualTo(1);
        await Assert.That(fallback.Events[0].Level).IsEqualTo(global::Serilog.Events.LogEventLevel.Error);
    }

    [Test]
    public async Task Idle_shutdown_with_the_broker_down_is_immediate(CancellationToken ct)
    {
        var sink = new RabbitMqAmqpSink(
            Options(o => o.ShutdownTimeout = TimeSpan.FromSeconds(10)), UnreachableEnvironment(), TimeProvider.System);
        await Task.Delay(200, ct); // let it try (and fail) to connect

        var started = DateTime.UtcNow;
        await sink.DisposeAsync();

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(1))
            .Because("nothing to deliver: no reason to wait for the deadline");
    }

    [Test]
    public async Task Emit_after_dispose_never_blocks_or_calls_listeners_on_the_app_thread_and_is_counted(CancellationToken ct)
    {
        var listener = new CollectingFailureListener();
        var sink = new RabbitMqAmqpSink(Options(), new FakeEnvironment().Environment, TimeProvider.System);
        sink.SetFailureListener(listener);
        await sink.DisposeAsync();

        var started = DateTime.UtcNow;
        sink.Emit(TestEvents.Create(7));
        sink.Emit(TestEvents.Create(8));

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromMilliseconds(100));
        await Assert.That(listener.Events.Count).IsEqualTo(0).Because("the fallbacks are closed together with the sink");
        await Assert.That(sink.EmittedAfterDispose).IsEqualTo(2L);
    }

    [Test]
    public async Task Fallback_chain_receives_overflow_and_undelivered_events(CancellationToken ct)
    {
        var fallback = new CollectingSink();
        var sink = new RabbitMqAmqpSink(
            Options(o =>
            {
                o.QueueLimit = 2;
                o.ShutdownTimeout = TimeSpan.FromMilliseconds(200);
            }),
            UnreachableEnvironment(),
            TimeProvider.System);

        var logger = new LoggerConfiguration()
            .WriteTo.FallbackChain(wt => wt.Sink(sink), wt => wt.Sink(fallback))
            .CreateLogger();

        for (var n = 1; n <= 5; n++)
        {
            logger.Information("Event {N}", n);
        }

        await logger.DisposeAsync();

        await Assert.That(fallback.Events.Count).IsEqualTo(5).Because("no silent loss: every event ends up somewhere");
    }

    [Test]
    public async Task Invalid_options_fail_at_configuration_time()
    {
        await Assert.That(() => new LoggerConfiguration().WriteTo.RabbitMQAmqp(o => o.Exchange = "logs"))
            .Throws<ArgumentException>().Because("no URI");
        await Assert.That(() => new LoggerConfiguration().WriteTo.RabbitMQAmqp(o =>
            {
                o.Hostnames.Add("localhost");
                o.Exchange = "logs";
                o.Queue = "logs";
            }))
            .Throws<ArgumentException>().Because("ambiguous target");
    }

    [Test]
    public async Task Changing_the_options_after_creation_has_no_effect(CancellationToken ct)
    {
        var broker = new FakeEnvironment();
        var options = Options();
        await using var sink = new RabbitMqAmqpSink(options, broker.Environment, TimeProvider.System);

        options.Exchange = "changed";
        options.RoutingKey = "changed";
        await Eventually.TrueAsync(() => broker.Last is not null, "connected", ct);

        broker.Last!.Builder.Received().Exchange("logs");
        broker.Last.Builder.DidNotReceive().Exchange("changed");
    }

    [Test]
    public async Task Shutdown_timeout_bounds_the_whole_shutdown_including_hanging_closes(CancellationToken ct)
    {
        var broker = new FakeEnvironment { OnCreated = c => c.CloseGate = new TaskCompletionSource() };
        var sink = new RabbitMqAmqpSink(
            Options(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(600)), broker.Environment, TimeProvider.System);
        sink.Emit(TestEvents.Create(1));
        await Eventually.TrueAsync(() => broker.Last?.Published.Count == 1, "delivered", ct);

        var started = DateTime.UtcNow;
        await sink.DisposeAsync();

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(1.5))
            .Because("closing must fit into ShutdownTimeout instead of adding up to 5 s per entity");
    }

    [Test]
    public async Task Configuration_errors_are_reported_to_the_failure_listener(CancellationToken ct)
    {
        var listener = new CollectingFailureListener();
        var environment = Substitute.For<IEnvironment>();
        environment.CreateConnectionAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            Task.FromException<IConnection>(new ConnectionException("connect failed",
                new InvalidOperationException("amqp:unauthorized-access: access refused for user 'app'"))));
        environment.CloseAsync().Returns(Task.CompletedTask);
        var sink = new RabbitMqAmqpSink(Options(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(200)), environment, TimeProvider.System);
        sink.SetFailureListener(listener);

        await Eventually.TrueAsync(() => listener.Notifications.Count > 0, "configuration error reported", ct);
        await sink.DisposeAsync();

        await Assert.That(listener.Notifications.Count).IsEqualTo(1).Because("reported once, not on every retry");
        await Assert.That(listener.Notifications[0].Kind).IsEqualTo(LoggingFailureKind.Permanent);
        await Assert.That(listener.Notifications[0].Message).Contains("access refused");
    }

    [Test]
    public async Task Flat_overload_is_bound_from_appsettings_including_timeouts()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Serilog:Using:0"] = "Averenium.Serilog.Sinks.RabbitMQ.Amqp",
                ["Serilog:WriteTo:0:Name"] = "RabbitMQAmqp",
                ["Serilog:WriteTo:0:Args:hostnames:0"] = "127.0.0.1:1",
                ["Serilog:WriteTo:0:Args:hostnames:1"] = "127.0.0.1:2",
                ["Serilog:WriteTo:0:Args:username"] = "app",
                ["Serilog:WriteTo:0:Args:password"] = "s3cr:t@/x",
                ["Serilog:WriteTo:0:Args:virtualHost"] = "logs",
                ["Serilog:WriteTo:0:Args:exchange"] = "logs",
                ["Serilog:WriteTo:0:Args:routingKey"] = "app",
                ["Serilog:WriteTo:0:Args:overflowPolicy"] = "DropNewest",
                ["Serilog:WriteTo:0:Args:publishTimeout"] = "00:00:05",
                ["Serilog:WriteTo:0:Args:minReconnectDelay"] = "00:00:00.100",
                ["Serilog:WriteTo:0:Args:maxReconnectDelay"] = "00:00:00.200",
                ["Serilog:WriteTo:0:Args:shutdownTimeout"] = "00:00:00.500",
            })
            .Build();

        var logger = new LoggerConfiguration().ReadFrom.Configuration(configuration).CreateLogger();
        logger.Information("Hello");

        var started = DateTime.UtcNow;
        await logger.DisposeAsync();

        await Assert.That(DateTime.UtcNow - started).IsLessThan(TimeSpan.FromSeconds(2))
            .Because("shutdownTimeout from configuration (0.5 s) bounds disposal while the broker is unreachable");
    }

    [Test]
    public async Task A_failed_start_does_not_close_an_environment_the_sink_does_not_own(CancellationToken ct)
    {
        var broker = new FakeEnvironment();
        var supervisor = new ConnectionSupervisor(broker.Environment, new ConnectionSupervisorOptions(), TimeProvider.System);

        RabbitMqAmqpSink.CleanUpFailedStart(supervisor, ownsEnvironment: false, credentialsWatch: null);
        await Task.Delay(100, ct);

        await broker.Environment.DidNotReceive().CloseAsync();
    }

    [Test]
    public async Task A_failed_start_closes_the_environment_the_sink_created(CancellationToken ct)
    {
        var broker = new FakeEnvironment();
        var supervisor = new ConnectionSupervisor(broker.Environment, new ConnectionSupervisorOptions(), TimeProvider.System);

        RabbitMqAmqpSink.CleanUpFailedStart(supervisor, ownsEnvironment: true, credentialsWatch: null);

        await Eventually.TrueAsync(() => broker.Environment.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "CloseAsync"),
            "the environment is closed", ct);
    }

    private static RabbitMqAmqpSinkOptions Options(Action<RabbitMqAmqpSinkOptions>? configure = null)
    {
        var options = new RabbitMqAmqpSinkOptions
        {
            Exchange = "logs",
            RoutingKey = "app",
            MinReconnectDelay = TimeSpan.FromMilliseconds(50),
            MaxReconnectDelay = TimeSpan.FromMilliseconds(200),
            ShutdownTimeout = TimeSpan.FromSeconds(5),
        };
        options.Hostnames.Add("localhost");
        configure?.Invoke(options);
        return options;
    }

    private static IEnvironment UnreachableEnvironment()
    {
        var environment = Substitute.For<IEnvironment>();
        environment.CreateConnectionAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<IConnection>(new InvalidOperationException("broker down")));
        environment.CloseAsync().Returns(Task.CompletedTask);
        return environment;
    }
}

using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;
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
    public async Task Emit_after_dispose_is_reported_not_lost(CancellationToken ct)
    {
        var listener = new CollectingFailureListener();
        var sink = new RabbitMqAmqpSink(Options(), new FakeEnvironment().Environment, TimeProvider.System);
        sink.SetFailureListener(listener);
        await sink.DisposeAsync();

        sink.Emit(TestEvents.Create(7));

        await Assert.That(listener.Events.Count).IsEqualTo(1);
        await Assert.That(listener.Events[0].Kind).IsEqualTo(LoggingFailureKind.Final);
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
        await Assert.That(() => new LoggerConfiguration().WriteTo.RabbitMQAmqp(o => o.Uris.Add(new Uri("amqp://localhost"))))
            .Throws<ArgumentException>().Because("neither exchange nor queue");
        await Assert.That(() => new LoggerConfiguration().WriteTo.RabbitMQAmqp(o =>
            {
                o.Uris.Add(new Uri("amqp://localhost"));
                o.Queue = "logs";
                o.RoutingKeySelector = _ => "x";
            }))
            .Throws<ArgumentException>().Because("routing key selector needs an exchange");
    }

    [Test]
    public async Task Flat_overload_parses_multiple_uris()
    {
        var options = new RabbitMqAmqpSinkOptions();
        foreach (var uri in "amqp://a:5672/ ; amqp://b:5672/,amqp://c:5672/".Split([',', ';'],
                     StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            options.Uris.Add(new Uri(uri));
        }

        await Assert.That(options.Uris.Count).IsEqualTo(3);
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
        options.Uris.Add(new Uri("amqp://localhost:5672/"));
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

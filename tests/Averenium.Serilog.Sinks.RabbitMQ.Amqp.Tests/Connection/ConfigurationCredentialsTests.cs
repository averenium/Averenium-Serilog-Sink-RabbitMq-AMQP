using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Microsoft.Extensions.Configuration;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class ConfigurationCredentialsTests
{
    [Test]
    public async Task Finds_the_sinks_own_args_section()
    {
        var configuration = Build(new()
        {
            ["Serilog:WriteTo:0:Name"] = "Console",
            ["Serilog:WriteTo:1:Name"] = "RabbitMQAmqp",
            ["Serilog:WriteTo:1:Args:hostnames:0"] = "rabbit-1",
            ["Serilog:WriteTo:1:Args:username"] = "app",
            ["Serilog:WriteTo:1:Args:password"] = "pw",
            ["Serilog:WriteTo:1:Args:exchange"] = "logs",
        });

        var args = ConfigurationCredentials.FindArgs(configuration, "app", "pw", ["rabbit-1"], "logs", null);

        await Assert.That(args?.Path).IsEqualTo("Serilog:WriteTo:1:Args");
    }

    [Test]
    public async Task Finds_the_section_inside_a_fallback_chain()
    {
        var configuration = Build(new()
        {
            ["Serilog:WriteTo:0:Name"] = "FallbackChain",
            ["Serilog:WriteTo:0:Args:configureSink:0:Name"] = "RabbitMQAmqp",
            ["Serilog:WriteTo:0:Args:configureSink:0:Args:hostnames:0"] = "rabbit-1;rabbit-2",
            ["Serilog:WriteTo:0:Args:configureSink:0:Args:username"] = "app",
            ["Serilog:WriteTo:0:Args:configureSink:0:Args:password"] = "pw",
            ["Serilog:WriteTo:0:Args:configureSink:0:Args:queue"] = "logs",
        });

        var args = ConfigurationCredentials.FindArgs(configuration, "app", "pw", ["rabbit-1", "rabbit-2"], null, "logs");

        await Assert.That(args?.Path).IsEqualTo("Serilog:WriteTo:0:Args:configureSink:0:Args");
    }

    [Test]
    public async Task Two_identical_sinks_are_ambiguous_and_not_followed()
    {
        var settings = new Dictionary<string, string?>();
        foreach (var i in new[] { 0, 1 })
        {
            settings[$"Serilog:WriteTo:{i}:Name"] = "RabbitMQAmqp";
            settings[$"Serilog:WriteTo:{i}:Args:hostnames:0"] = "rabbit-1";
            settings[$"Serilog:WriteTo:{i}:Args:username"] = "app";
            settings[$"Serilog:WriteTo:{i}:Args:exchange"] = "logs";
        }

        var args = ConfigurationCredentials.FindArgs(Build(settings), "app", null, ["rabbit-1"], "logs", null);

        await Assert.That(args).IsNull();
    }

    [Test]
    public async Task A_reload_with_a_new_password_updates_the_credentials()
    {
        var configuration = Build(Sink("old"));
        var args = ConfigurationCredentials.FindArgs(configuration, "app", "old", ["rabbit-1"], "logs", null)!;
        var credentials = new RabbitMqAmqpCredentialsSource("app", "old");
        var changes = 0;
        credentials.Changed += () => changes++;
        using var watch = ConfigurationCredentials.Watch(configuration, args, credentials);

        configuration["Serilog:WriteTo:0:Args:password"] = "new";
        configuration.Reload();

        await Assert.That(changes).IsEqualTo(1);
        await Assert.That(credentials.Current.Password).IsEqualTo("new");
    }

    [Test]
    public async Task A_reload_that_changes_something_else_does_not_reconnect()
    {
        var configuration = Build(Sink("pw"));
        configuration["Serilog:MinimumLevel"] = "Information";
        var args = ConfigurationCredentials.FindArgs(configuration, "app", "pw", ["rabbit-1"], "logs", null)!;
        var credentials = new RabbitMqAmqpCredentialsSource("app", "pw");
        var changes = 0;
        credentials.Changed += () => changes++;
        using var watch = ConfigurationCredentials.Watch(configuration, args, credentials);

        configuration["Serilog:MinimumLevel"] = "Warning";
        configuration.Reload();

        await Assert.That(changes).IsEqualTo(0);
    }

    [Test]
    public async Task After_dispose_reloads_are_ignored()
    {
        var configuration = Build(Sink("old"));
        var args = ConfigurationCredentials.FindArgs(configuration, "app", "old", ["rabbit-1"], "logs", null)!;
        var credentials = new RabbitMqAmqpCredentialsSource("app", "old");
        ConfigurationCredentials.Watch(configuration, args, credentials).Dispose();

        configuration["Serilog:WriteTo:0:Args:password"] = "new";
        configuration.Reload();

        await Assert.That(credentials.Current.Password).IsEqualTo("old");
    }

    [Test]
    public async Task Hostnames_given_as_one_string_are_matched_too()
    {
        var configuration = Build(new()
        {
            ["Serilog:WriteTo:0:Name"] = "RabbitMQAmqp",
            ["Serilog:WriteTo:0:Args:hostnames"] = "rabbit-1;rabbit-2",
            ["Serilog:WriteTo:0:Args:username"] = "app",
            ["Serilog:WriteTo:0:Args:password"] = "pw",
            ["Serilog:WriteTo:0:Args:exchange"] = "logs",
        });

        var args = ConfigurationCredentials.FindArgs(configuration, "app", "pw", ["rabbit-1", "rabbit-2"], "logs", null);

        await Assert.That(args?.Path).IsEqualTo("Serilog:WriteTo:0:Args");
    }

    private static Dictionary<string, string?> Sink(string password) => new()
    {
        ["Serilog:WriteTo:0:Name"] = "RabbitMQAmqp",
        ["Serilog:WriteTo:0:Args:hostnames:0"] = "rabbit-1",
        ["Serilog:WriteTo:0:Args:username"] = "app",
        ["Serilog:WriteTo:0:Args:password"] = password,
        ["Serilog:WriteTo:0:Args:exchange"] = "logs",
    };

    private static IConfigurationRoot Build(Dictionary<string, string?> settings) =>
        new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
}

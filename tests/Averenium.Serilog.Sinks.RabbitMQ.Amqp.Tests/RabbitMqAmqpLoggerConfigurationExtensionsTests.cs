using System.Text;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests;

/// <summary>
/// The <c>appsettings.json</c> examples from the README and docs/guide, bound through the real <c>Serilog.Settings.Configuration</c>.
/// The broker is unreachable on purpose: formatting happens in the background regardless.
/// </summary>
public sealed class RabbitMqAmqpLoggerConfigurationExtensionsTests
{
    private static readonly string RecordingFormatterType = typeof(RecordingFormatter).AssemblyQualifiedName!;

    [Test]
    public async Task Formatter_given_as_a_type_name_is_used(CancellationToken ct)
    {
        var logger = Create(new()
        {
            ["Serilog:WriteTo:0:Args:formatter"] = RecordingFormatterType,
        });

        logger.Information("Hello");

        await Eventually.TrueAsync(() => RecordingFormatter.HasFormatted("default"), "the configured formatter formats the event", ct);
        await logger.DisposeAsync();
    }

    [Test]
    public async Task Formatter_given_as_an_object_with_constructor_arguments_is_used(CancellationToken ct)
    {
        var tag = Guid.NewGuid().ToString("N");
        var logger = Create(new()
        {
            ["Serilog:WriteTo:0:Args:formatter:type"] = RecordingFormatterType,
            ["Serilog:WriteTo:0:Args:formatter:tag"] = tag,
        });

        logger.Information("Hello");

        await Eventually.TrueAsync(() => RecordingFormatter.HasFormatted(tag), "the formatter got its constructor argument", ct);
        await logger.DisposeAsync();
    }

    [Test]
    [Arguments("Serilog.Formatting.Compact.RenderedCompactJsonFormatter, Serilog.Formatting.Compact", null)]
    [Arguments("Serilog.Formatting.Json.JsonFormatter, Serilog", "renderMessage=true")]
    [Arguments("Serilog.Templates.ExpressionTemplate, Serilog.Expressions", "template={ {timestamp: @t, level: @l, message: @m, exception: @x, ..@p} }")]
    public async Task The_formatters_from_the_readme_can_be_configured(string type, string? argument)
    {
        var settings = new Dictionary<string, string?>();
        if (argument is null)
        {
            settings["Serilog:WriteTo:0:Args:formatter"] = type;
        }
        else
        {
            var separator = argument.IndexOf('=', StringComparison.Ordinal);
            settings["Serilog:WriteTo:0:Args:formatter:type"] = type;
            settings[$"Serilog:WriteTo:0:Args:formatter:{argument[..separator]}"] = argument[(separator + 1)..];
        }

        var logger = Create(settings);
        logger.Information("Hello {Name}", "world");

        await Assert.That(logger).IsNotNull();
        await logger.DisposeAsync();
    }

    [Test]
    public async Task Hostnames_are_bound_from_a_json_array_and_elements_may_hold_several_nodes(CancellationToken ct)
    {
        var logger = FromJson("""
            {
              "Serilog": {
                "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
                "WriteTo": [
                  {
                    "Name": "RabbitMQAmqp",
                    "Args": {
                      "hostnames": [ "127.0.0.1:1", "127.0.0.1:2;127.0.0.1:3" ],
                      "exchange": "logs",
                      "shutdownTimeout": "00:00:00.300"
                    }
                  }
                ]
              }
            }
            """);

        logger.Information("Hello");

        await Assert.That(logger).IsNotNull();
        await logger.DisposeAsync();
    }

    [Test]
    public async Task Every_hostnames_element_is_validated()
    {
        var settings = new Dictionary<string, string?>
        {
            ["Serilog:WriteTo:0:Args:hostnames:1"] = "127.0.0.1:2;http://wrong-scheme",
        };

        await Assert.That(() => Create(settings)).Throws<Exception>();
    }

    [Test]
    public async Task Fallback_chain_from_json_receives_undelivered_events_of_a_level_restricted_sink(CancellationToken ct)
    {
        var name = Guid.NewGuid().ToString("N");
        var logger = FromJson($$"""
            {
              "Serilog": {
                "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp", "Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests" ],
                "WriteTo": [
                  {
                    "Name": "FallbackChain",
                    "Args": {
                      "configureSink": [
                        {
                          "Name": "RabbitMQAmqp",
                          "Args": {
                            "hostnames": [ "127.0.0.1:1" ],
                            "exchange": "logs",
                            "shutdownTimeout": "00:00:00.500",
                            "restrictedToMinimumLevel": "Warning"
                          }
                        }
                      ],
                      "configureFallback": [
                        { "Name": "Collector", "Args": { "name": "{{name}}" } }
                      ]
                    }
                  }
                ]
              }
            }
            """);

        logger.Information("Filtered by the sink's minimum level");
        logger.Warning("Undeliverable {N}", 1);
        await logger.DisposeAsync();

        var fallback = CollectorSinkExtensions.Get(name).Events;
        await Assert.That(fallback.Count).IsEqualTo(1)
            .Because("the failure listener reaches the sink through the level-restricting wrapper");
        await Assert.That(fallback[0].Level).IsEqualTo(global::Serilog.Events.LogEventLevel.Warning);
    }

    [Test]
    public async Task Tls_correlation_durability_and_fallback_after_arguments_are_bound()
    {
        var caPath = Path.Combine(Path.GetTempPath(), $"amqp-sink-ca-{Guid.NewGuid():N}.pem");
        using (var ca = TestCertificates.CreateCa())
        {
            await File.WriteAllTextAsync(caPath, ca.ExportCertificatePem());
        }

        try
        {
            var logger = Create(new()
            {
                ["Serilog:WriteTo:0:Args:useTls"] = "true",
                ["Serilog:WriteTo:0:Args:tls:protocols"] = "Tls12, Tls13",
                ["Serilog:WriteTo:0:Args:tls:acceptablePolicyErrors"] = "RemoteCertificateNameMismatch",
                ["Serilog:WriteTo:0:Args:tls:checkCertificateRevocation"] = "false",
                ["Serilog:WriteTo:0:Args:tls:caCertificatePath"] = caPath,
                ["Serilog:WriteTo:0:Args:correlationIdFromTraceId"] = "false",
                ["Serilog:WriteTo:0:Args:durableMessages"] = "false",
                ["Serilog:WriteTo:0:Args:fallbackAfter"] = "00:01:00",
            });

            logger.Information("Hello");
            await Assert.That(logger).IsNotNull();
            await logger.DisposeAsync();
        }
        finally
        {
            File.Delete(caPath);
        }
    }

    [Test]
    public async Task Tls_object_from_json_reaches_the_sink_options()
    {
        // A missing CA file fails validation — proof that the nested "tls" object was bound, not silently ignored.
        var missing = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.pem").Replace("\\", "/", StringComparison.Ordinal);

        var thrown = await Assert.That(() => FromJson($$"""
            {
              "Serilog": {
                "Using": [ "Averenium.Serilog.Sinks.RabbitMQ.Amqp" ],
                "WriteTo": [
                  {
                    "Name": "RabbitMQAmqp",
                    "Args": {
                      "hostnames": [ "127.0.0.1:1" ],
                      "useTls": true,
                      "exchange": "logs",
                      "tls": { "caCertificatePath": "{{missing}}", "protocols": "Tls12, Tls13" }
                    }
                  }
                ]
              }
            }
            """)).Throws<System.Reflection.TargetInvocationException>();

        await Assert.That(thrown!.InnerException?.Message).Contains("Tls.CaCertificatePath");
    }

    private static global::Serilog.Core.Logger FromJson(string json)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder().AddJsonStream(stream).Build();
        return new LoggerConfiguration().ReadFrom.Configuration(configuration).CreateLogger();
    }

    private static global::Serilog.Core.Logger Create(Dictionary<string, string?> settings)
    {
        settings["Serilog:Using:0"] = "Averenium.Serilog.Sinks.RabbitMQ.Amqp";
        settings["Serilog:WriteTo:0:Name"] = "RabbitMQAmqp";
        settings.TryAdd("Serilog:WriteTo:0:Args:hostnames:0", "127.0.0.1:1");
        settings["Serilog:WriteTo:0:Args:exchange"] = "logs";
        settings["Serilog:WriteTo:0:Args:shutdownTimeout"] = "00:00:00.300";

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return new LoggerConfiguration().ReadFrom.Configuration(configuration).CreateLogger();
    }
}

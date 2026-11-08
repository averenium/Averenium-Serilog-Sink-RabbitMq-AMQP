using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class ConfigurationErrorDetectorTests
{
    [Test]
    public async Task Recognises_typed_management_errors_as_certain()
    {
        var precondition = ConfigurationErrorDetector.Describe(
            new PreconditionFailedException("queue type mismatch, response code: 409"), ConnectionStage.Declare);
        var badRequest = ConfigurationErrorDetector.Describe(
            new BadRequestException("invalid exchange type, response code: 400"), ConnectionStage.Declare);

        await Assert.That(precondition!.Reason).Contains("precondition failed");
        await Assert.That(precondition.IsCertain).IsTrue();
        await Assert.That(badRequest!.Reason).Contains("bad request");
    }

    [Test]
    [Arguments("Connect", "virtual host not found")]
    [Arguments("Attach", "publish target not found")]
    public async Task Not_found_means_different_things_per_stage(string stage, string expected)
    {
        var error = new ConnectionException("failed", new InvalidOperationException("Code: amqp:not-found - Description: ..."));

        var described = ConfigurationErrorDetector.Describe(error, Enum.Parse<ConnectionStage>(stage));

        await Assert.That(described!.Reason).Contains(expected);
    }

    [Test]
    [Arguments("Connect", "credentials")]
    [Arguments("Declare", "'configure' permission")]
    [Arguments("Attach", "'write' permission")]
    public async Task Access_refused_points_to_the_right_permission(string stage, string expected)
    {
        var error = new ConnectionException("failed", new InvalidOperationException("amqp:unauthorized-access: access refused"));

        var described = ConfigurationErrorDetector.Describe(error, Enum.Parse<ConnectionStage>(stage));

        await Assert.That(described!.Reason).Contains(expected);
    }

    [Test]
    public async Task Markers_in_stack_traces_are_ignored()
    {
        Exception thrown;
        try
        {
            AMethodNamedAmqpNotFound();
            throw new InvalidOperationException("unreachable");
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        await Assert.That(thrown.ToString()).Contains("AMethodNamedAmqpNotFound");
        await Assert.That(ConfigurationErrorDetector.Describe(thrown, ConnectionStage.Attach)).IsNull();
    }

    [Test]
    public async Task Ordinary_outages_are_not_configuration_errors()
    {
        var refused = new ConnectionException("connect failed", new System.Net.Sockets.SocketException(10061));

        await Assert.That(ConfigurationErrorDetector.Describe(refused, ConnectionStage.Connect)).IsNull();
        await Assert.That(ConfigurationErrorDetector.Describe(new TimeoutException("connect timed out"), ConnectionStage.Attach)).IsNull();
    }

    [Test]
    public async Task The_attach_heuristic_is_reported_as_probable()
    {
        await Assert.That(ConfigurationErrorDetector.ProbableAttachFailure().IsCertain).IsFalse();
    }

    private static void AMethodNamedAmqpNotFound() => throw new InvalidOperationException("broker went away");

    [Test]
    public async Task Unusable_new_credentials_are_a_certain_configuration_error_when_connecting()
    {
        var error = ConfigurationErrorDetector.Describe(new ArgumentException("Password is set but Username is not."), ConnectionStage.Connect);

        await Assert.That(error).IsNotNull();
        await Assert.That(error!.IsCertain).IsTrue();
        await Assert.That(error.Reason).Contains("Username");
    }
}

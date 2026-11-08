using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;
using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing.Models;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Publishing;

public sealed class OutcomeClassifierTests
{
    [Test]
    [Arguments(OutcomeState.Accepted, null, nameof(OutcomeKind.Accepted))]
    [Arguments(OutcomeState.Released, null, nameof(OutcomeKind.Released))]
    [Arguments(OutcomeState.Rejected, "amqp:not-found", nameof(OutcomeKind.TargetMissing))]
    [Arguments(OutcomeState.Rejected, "amqp:link:message-size-exceeded", nameof(OutcomeKind.Rejected))]
    [Arguments(OutcomeState.Rejected, null, nameof(OutcomeKind.Rejected))]
    [Arguments(OutcomeState.Rejected, "amqp:unauthorized-access", nameof(OutcomeKind.Rejected))]
    [Arguments(OutcomeState.Rejected, "amqp:resource-limit-exceeded", nameof(OutcomeKind.Throttled))]
    [Arguments(OutcomeState.Rejected, "amqp:internal-error", nameof(OutcomeKind.ConnectionLost))]
    [Arguments(OutcomeState.Rejected, "amqp:connection:forced", nameof(OutcomeKind.ConnectionLost))]
    [Arguments(OutcomeState.Rejected, "amqp:session:window-violation", nameof(OutcomeKind.ConnectionLost))]
    [Arguments(OutcomeState.Rejected, "amqp:link:detach-forced", nameof(OutcomeKind.ConnectionLost))]
    public async Task Maps_broker_outcomes_to_sink_actions(OutcomeState state, string? errorCode, string expected)
    {
        var outcome = new PublishOutcome(state, errorCode is null ? null : new Error(errorCode, "test"));

        await Assert.That(OutcomeClassifier.Classify(outcome)).IsEqualTo(Enum.Parse<OutcomeKind>(expected));
    }
}

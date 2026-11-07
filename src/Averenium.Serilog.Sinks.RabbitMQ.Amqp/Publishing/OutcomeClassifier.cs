using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Publishing;

/// <summary>
/// Maps broker publish outcomes to sink actions (docs/architecture.md, C7/C9). Only rejections that concern the
/// message itself are permanent; anything that smells of a lost connection is retried, because dropping a log
/// event is worse than a possible duplicate.
/// </summary>
internal static class OutcomeClassifier
{
    /// <summary>AMQP 1.0 error condition RabbitMQ uses when the target exchange or queue does not exist.</summary>
    public const string NotFound = "amqp:not-found";

    public const string InternalError = "amqp:internal-error";
    public const string ResourceLimitExceeded = "amqp:resource-limit-exceeded";

    public static OutcomeKind Classify(PublishOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);

        return outcome.State switch
        {
            OutcomeState.Accepted => OutcomeKind.Accepted,
            OutcomeState.Released => OutcomeKind.Released,
            _ => ClassifyRejection(outcome.Error?.ErrorCode),
        };
    }

    private static OutcomeKind ClassifyRejection(string? errorCode) => errorCode switch
    {
        NotFound => OutcomeKind.TargetMissing,
        ResourceLimitExceeded => OutcomeKind.Released,
        InternalError => OutcomeKind.ConnectionLost,
        "amqp:link:detach-forced" or "amqp:link:stolen" => OutcomeKind.ConnectionLost,
        not null when errorCode.StartsWith("amqp:connection:", StringComparison.Ordinal)
                      || errorCode.StartsWith("amqp:session:", StringComparison.Ordinal) => OutcomeKind.ConnectionLost,
        _ => OutcomeKind.Rejected,
    };
}

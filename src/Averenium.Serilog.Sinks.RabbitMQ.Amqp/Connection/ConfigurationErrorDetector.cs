using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection.Models;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// Recognises connect / declare / attach failures caused by configuration (credentials or permissions, missing virtual
/// host or target, conflicting definitions, invalid exchange type). The supervisor still keeps retrying — an operator may
/// fix the broker side — but reports these loudly instead of making them look like an ordinary outage.
/// <para>
/// Only exception <em>messages</em> are inspected (never stack traces), and the meaning depends on the
/// <see cref="ConnectionStage"/>: <c>amqp:not-found</c> while connecting is a missing virtual host, while attaching a
/// missing exchange/queue.
/// </para>
/// </summary>
internal static class ConfigurationErrorDetector
{
    /// <summary>
    /// Consecutive attach failures on a working connection after which a <em>probable</em> configuration error is
    /// reported (the client surfaces a missing exchange only as an attach timeout). Five, so that a broker that is merely
    /// slow under load is unlikely to be mislabelled.
    /// </summary>
    public const int AttachFailuresBeforeReport = 5;

    public const string AttachFailureReason =
        "the publisher cannot attach to its target although the connection works: the exchange/queue is probably missing " +
        "(declare it or enable DeclareTopology) or the user has no write permission";

    public static ConfigurationError? Describe(Exception exception, ConnectionStage stage)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (stage == ConnectionStage.Connect && exception is ArgumentException invalidSettings)
        {
            // New credentials from CredentialsProvider / the configuration that cannot be used (messages are redacted).
            return Certain($"invalid connection settings: {invalidSettings.Message}");
        }

        foreach (var current in Chain(exception))
        {
            switch (current)
            {
                case PreconditionFailedException:
                    return Certain("precondition failed: an existing exchange/queue has a different type or arguments");
                case BadRequestException:
                    return Certain("bad request: check the exchange type, names and arguments");
                case ResourceNotFoundException:
                    return Certain("resource not found: a declared binding refers to a missing exchange or queue");
            }

            var message = current.Message;
            if (Contains(message, "amqp:unauthorized-access"))
            {
                return Certain(stage switch
                {
                    ConnectionStage.Connect => "access refused: check the credentials and the user's access to the virtual host",
                    ConnectionStage.Declare => "access refused: the user needs the 'configure' permission to declare topology",
                    _ => "access refused: the user needs the 'write' permission on the publish target",
                });
            }

            if (Contains(message, "amqp:not-found"))
            {
                return Certain(stage switch
                {
                    ConnectionStage.Connect => "virtual host not found: check the path of the broker URI",
                    ConnectionStage.Declare => "not found while declaring topology: check exchange and queue names",
                    _ => "publish target not found: declare the exchange/queue or enable DeclareTopology",
                });
            }

            if (Contains(message, "amqp:precondition-failed"))
            {
                return Certain("precondition failed: an existing exchange/queue has a different type or arguments");
            }

            if (Contains(message, "amqp:invalid-field"))
            {
                return Certain("invalid field: check exchange/queue names and the exchange type");
            }
        }

        return null;
    }

    public static ConfigurationError ProbableAttachFailure() => new(AttachFailureReason, IsCertain: false);

    private static ConfigurationError Certain(string reason) => new(reason, IsCertain: true);

    private static bool Contains(string text, string marker) => text.Contains(marker, StringComparison.OrdinalIgnoreCase);

    private static IEnumerable<Exception> Chain(Exception exception)
    {
        var pending = new Stack<Exception>();
        pending.Push(exception);
        while (pending.Count > 0)
        {
            var current = pending.Pop();
            yield return current;
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    pending.Push(inner);
                }
            }
            else if (current.InnerException is { } inner)
            {
                pending.Push(inner);
            }
        }
    }
}

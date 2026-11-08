namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Formatting.Models;

/// <summary>
/// An exception restored from its formatted text (<c>@x</c> in CLEF) when an undelivered event is handed to the
/// failure listener. <see cref="ToString"/> returns the original text including the stack trace.
/// </summary>
internal sealed class TextException : Exception
{
    private readonly string _text;

    public TextException(string text)
        : base(FirstLine(text))
    {
        _text = text;
    }

    public TextException()
        : this(string.Empty)
    {
    }

    public TextException(string message, Exception innerException)
        : base(message, innerException)
    {
        _text = message;
    }

    public override string ToString() => _text;

    private static string FirstLine(string text)
    {
        var end = text.AsSpan().IndexOfAny('\r', '\n');
        return end < 0 ? text : text[..end];
    }
}

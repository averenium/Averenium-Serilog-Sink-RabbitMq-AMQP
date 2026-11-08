using System.Diagnostics.CodeAnalysis;
using Serilog.Debugging;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;

/// <summary>
/// A value loaded from files (a certificate, a CA bundle) that is reloaded when the file contents change — e.g. when
/// cert-manager renews a mounted secret. <see cref="Current"/> re-reads the bytes (small files, only at TLS handshakes)
/// and parses again only if they differ. A file that cannot be read or parsed right now (a secret in the middle of an
/// update) keeps the previous value; the next handshake tries again. Files are only read, never written.
/// <para>
/// A replaced value is released (e.g. a certificate's key container removed) one reload <em>later</em>, not at once: a
/// handshake that obtained it just before the reload may still be using it.
/// </para>
/// </summary>
internal sealed class ReloadingFiles<T>
    where T : class
{
    private readonly string[] _paths;
    private readonly Func<T> _load;
    private readonly Action<T>? _release;
    private readonly Lock _gate = new();
    private byte[][] _loadedFrom;
    private T _current;
    private T? _retired;

    /// <summary>Loads eagerly, so a missing or invalid file fails at logger creation.</summary>
    /// <param name="paths">Files whose contents decide whether to reload.</param>
    /// <param name="load">Parses the files.</param>
    /// <param name="release">Frees a value that is no longer used (called one reload after it was replaced).</param>
    public ReloadingFiles(IEnumerable<string> paths, Func<T> load, Action<T>? release = null)
    {
        _paths = [.. paths];
        _load = load;
        _release = release;
        _loadedFrom = ReadAll();
        _current = load();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification = "A file being swapped must not fail the handshake; the previous value stays in use.")]
    public T Current
    {
        get
        {
            lock (_gate)
            {
                try
                {
                    var bytes = ReadAll();
                    if (!SameContent(bytes, _loadedFrom))
                    {
                        var loaded = _load();
                        if (_retired is not null)
                        {
                            _release?.Invoke(_retired);
                        }

                        _retired = _current;
                        _current = loaded;
                        _loadedFrom = bytes;
                        SelfLog.WriteLine("[RabbitMQAmqp] Reloaded {0} after it changed.", string.Join(", ", _paths));
                    }
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine("[RabbitMQAmqp] Could not reload {0}; keeping the previous one: {1}",
                        string.Join(", ", _paths), ex.Message);
                }

                return _current;
            }
        }
    }

    private byte[][] ReadAll() => [.. _paths.Select(File.ReadAllBytes)];

    private static bool SameContent(byte[][] a, byte[][] b) =>
        a.Length == b.Length && a.Zip(b).All(pair => pair.First.AsSpan().SequenceEqual(pair.Second));
}

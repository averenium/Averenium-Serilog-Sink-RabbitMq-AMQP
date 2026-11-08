using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests;

/// <summary>
/// Every complete <c>appsettings.json</c> example in the README and in <c>docs/guide</c> is bound through the real
/// Serilog.Settings.Configuration, so the documentation cannot drift from the code. A <c>caCertificatePath</c> is pointed
/// at a generated CA file; examples with client certificate files are skipped (covered by the TLS tests).
/// </summary>
public sealed partial class DocumentationExamplesTests
{
    public static IEnumerable<string> Examples() => LoadExamples().Keys;

    [Test]
    [MethodDataSource(nameof(Examples))]
    public async Task Configuration_example_from_the_docs_creates_a_logger(string example)
    {
        var caFile = Path.Combine(Path.GetTempPath(), $"docs-ca-{Guid.NewGuid():N}.pem");
        using (var ca = TestCertificates.CreateCa())
        {
            await File.WriteAllTextAsync(caFile, ca.ExportCertificatePem());
        }

        var json = CaPath().Replace(LoadExamples()[example], $"\"caCertificatePath\": \"{caFile.Replace("\\", "/", StringComparison.Ordinal)}\"");
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder().AddJsonStream(stream).Build();

        var logger = new LoggerConfiguration().ReadFrom.Configuration(configuration).CreateLogger();

        await Assert.That(logger).IsNotNull();
        await logger.DisposeAsync(); // nothing logged: an idle sink stops at once, the hosts are never contacted
        File.Delete(caFile);
    }

    [Test]
    public async Task The_docs_contain_configuration_examples() =>
        await Assert.That(LoadExamples().Count).IsGreaterThanOrEqualTo(6);

    private static Dictionary<string, string> LoadExamples()
    {
        var root = RepositoryRoot();
        var files = new[] { Path.Combine(root, "README.md") }
            .Concat(Directory.GetFiles(Path.Combine(root, "docs", "guide"), "*.md").Order(StringComparer.Ordinal));

        var examples = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var file in files)
        {
            var index = 0;
            foreach (Match block in JsonBlock().Matches(File.ReadAllText(file)))
            {
                var json = block.Groups[1].Value;
                index++;
                if (!json.TrimStart().StartsWith('{') || !json.Contains("\"Serilog\"", StringComparison.Ordinal)
                    || json.Contains("clientCertificatePath", StringComparison.Ordinal))
                {
                    continue;
                }

                examples[$"{Path.GetRelativePath(root, file).Replace('\\', '/')} #{index}"] = json;
            }
        }

        return examples;
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Averenium.Serilog.Sinks.RabbitMQ.Amqp.slnx")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("Repository root not found.");
    }

    [GeneratedRegex("\"caCertificatePath\":\\s*\"[^\"]*\"")]
    private static partial Regex CaPath();

    [GeneratedRegex("```json\\r?\\n(.*?)```", RegexOptions.Singleline)]
    private static partial Regex JsonBlock();
}

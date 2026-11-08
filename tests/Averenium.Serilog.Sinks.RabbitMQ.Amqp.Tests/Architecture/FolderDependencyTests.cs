using System.Text.RegularExpressions;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Architecture;

/// <summary>
/// Enforces the layering rules from AGENTS.md on the source code (the compiler does not): no cycles between feature
/// folders, and the bottom layers depend on no other feature folder. A folder's <c>Models</c> sub-namespace counts as
/// part of the folder. The root namespace (public contracts + composition root) is not a node.
/// </summary>
public sealed partial class FolderDependencyTests
{
    private const string RootNamespace = "Averenium.Serilog.Sinks.RabbitMQ.Amqp";

    private static readonly string[] BottomLayers = ["Diagnostics", "Resilience", "Buffering"];

    [Test]
    public async Task There_are_no_cycles_between_folders()
    {
        var graph = BuildGraph();

        var cycle = FindCycle(graph);

        await Assert.That(cycle).IsNull().Because($"folder cycle: {(cycle is null ? "" : string.Join(" -> ", cycle))}");
    }

    [Test]
    public async Task Bottom_layers_depend_on_no_other_folder()
    {
        var graph = BuildGraph();

        var violations = BottomLayers
            .Where(graph.ContainsKey)
            .SelectMany(layer => graph[layer].Select(dependency => $"{layer} -> {dependency}"))
            .ToList();

        await Assert.That(violations).IsEmpty();
    }

    private static Dictionary<string, HashSet<string>> BuildGraph()
    {
        var source = Path.Combine(FindRepositoryRoot(), "src", RootNamespace);
        var graph = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(source, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            var ns = NamespacePattern().Match(text);
            if (!ns.Success || FolderOf(ns.Groups[1].Value) is not { } folder)
            {
                continue;
            }

            var dependencies = graph.TryGetValue(folder, out var existing) ? existing : graph[folder] = [];
            foreach (Match match in UsingPattern().Matches(text))
            {
                if (FolderOf(match.Groups[1].Value) is { } dependency && dependency != folder)
                {
                    dependencies.Add(dependency);
                }
            }
        }

        return graph;
    }

    /// <summary>"…Amqp.Publishing.Models" → "Publishing"; the root namespace → null.</summary>
    private static string? FolderOf(string ns)
    {
        if (!ns.StartsWith(RootNamespace + ".", StringComparison.Ordinal))
        {
            return null;
        }

        return ns[(RootNamespace.Length + 1)..].Split('.')[0];
    }

    private static List<string>? FindCycle(Dictionary<string, HashSet<string>> graph)
    {
        var state = new Dictionary<string, int>(StringComparer.Ordinal); // 1 = visiting, 2 = done
        var path = new Stack<string>();

        List<string>? Visit(string node)
        {
            state[node] = 1;
            path.Push(node);
            foreach (var next in graph.GetValueOrDefault(node) ?? [])
            {
                if (state.GetValueOrDefault(next) == 1)
                {
                    return [.. path.Reverse().SkipWhile(n => n != next), next];
                }

                if (state.GetValueOrDefault(next) == 0 && Visit(next) is { } cycle)
                {
                    return cycle;
                }
            }

            path.Pop();
            state[node] = 2;
            return null;
        }

        foreach (var node in graph.Keys)
        {
            if (state.GetValueOrDefault(node) == 0 && Visit(node) is { } cycle)
            {
                return cycle;
            }
        }

        return null;
    }

    private static string FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (directory.EnumerateFiles("*.slnx").Any())
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("Repository root (*.slnx) not found.");
    }

    [GeneratedRegex(@"^namespace\s+([\w.]+)\s*;", RegexOptions.Multiline)]
    private static partial Regex NamespacePattern();

    [GeneratedRegex(@"^using\s+([\w.]+)\s*;", RegexOptions.Multiline)]
    private static partial Regex UsingPattern();
}

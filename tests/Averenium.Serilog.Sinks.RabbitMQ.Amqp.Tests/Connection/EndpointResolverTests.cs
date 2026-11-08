using Averenium.Serilog.Sinks.RabbitMQ.Amqp.Connection;
using RabbitMQ.AMQP.Client;

namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Connection;

public sealed class EndpointResolverTests
{
    [Test]
    [Arguments("rabbit-1", "amqp://rabbit-1:5672/")]
    [Arguments("rabbit-1:5673", "amqp://rabbit-1:5673/")]
    [Arguments("10.0.0.5", "amqp://10.0.0.5:5672/")]
    [Arguments("[::1]:5673", "amqp://[::1]:5673/")]
    [Arguments("::1", "amqp://[::1]:5672/")]
    [Arguments("amqp://rabbit-1", "amqp://rabbit-1:5672/")]
    [Arguments("amqps://rabbit-1:5999/", "amqps://rabbit-1:5999/")]
    public async Task Accepts_plain_hosts_and_amqp_uris(string entry, string expected)
    {
        var uri = Resolve([entry]).Single();

        await Assert.That(uri.ToString()).IsEqualTo(expected);
    }

    [Test]
    public async Task Tls_and_default_port_apply_to_plain_hosts()
    {
        await Assert.That(Resolve(["rabbit-1"], useTls: true).Single().ToString()).IsEqualTo("amqps://rabbit-1:5671/");
        await Assert.That(Resolve(["rabbit-1", "rabbit-2:6000"], port: 5999).Select(u => u.Port)).IsEquivalentTo([5999, 6000]);
    }

    [Test]
    public async Task Separate_credentials_with_any_characters_reach_the_client_unchanged()
    {
        const string password = "p@ss:w/rd%?#";
        var uris = Resolve(["rabbit-1"], username: "app user", password: password, virtualHost: "logs/prod");

        var settings = ConnectionSettingsBuilder.Create().Uri(uris.Single()).Build();

        await Assert.That(settings.User).IsEqualTo("app user");
        await Assert.That(settings.Password).IsEqualTo(password);
        await Assert.That(settings.VirtualHost).IsEqualTo("logs/prod");
    }

    [Test]
    public async Task Credentials_from_a_uri_are_kept()
    {
        var settings = ConnectionSettingsBuilder.Create().Uri(Resolve(["amqp://u:p@rabbit-1/vh"]).Single()).Build();

        await Assert.That(settings.User).IsEqualTo("u");
        await Assert.That(settings.Password).IsEqualTo("p");
        await Assert.That(settings.VirtualHost).IsEqualTo("vh");
    }

    [Test]
    [Arguments("option", "/data", "/data")]
    [Arguments("option", "a/b", "a/b")]
    [Arguments("option", "/", "/")]
    [Arguments("uri", "amqp://h/%2Fdata", "/data")]
    [Arguments("uri", "amqp://h/%2fdata", "/data")]
    [Arguments("uri", "amqp://h/%2F", "/")]
    [Arguments("uri", "amqp://h/data", "data")]
    [Arguments("uri", "amqp://h", "/")]
    public async Task Virtual_hosts_with_slashes_reach_the_client_exactly(string kind, string value, string expected)
    {
        var uri = kind == "option" ? Resolve(["h"], virtualHost: value).Single() : Resolve([value]).Single();

        var settings = ConnectionSettingsBuilder.Create().Uri(uri).Build();

        await Assert.That(settings.VirtualHost).IsEqualTo(expected);
    }

    [Test]
    [Arguments("amqp://h//data")]
    [Arguments("amqp://h/a/b")]
    [Arguments("amqp://h/data/")]
    public async Task Uri_paths_that_would_silently_lose_a_slash_are_rejected(string entry)
    {
        var error = Assert.Throws<ArgumentException>(() => Resolve([entry]));

        await Assert.That(error.Message).Contains("%2F");
    }

    [Test]
    public async Task A_uri_with_an_encoded_slash_matches_the_same_virtual_host_option()
    {
        var uris = Resolve(["amqp://h/%2Fdata"], virtualHost: "/data");

        await Assert.That(ConnectionSettingsBuilder.Create().Uri(uris.Single()).Build().VirtualHost).IsEqualTo("/data");
    }

    [Test]
    [Arguments("credentials twice")]
    [Arguments("virtual host conflict")]
    [Arguments("different virtual hosts")]
    [Arguments("tls with amqp scheme")]
    [Arguments("password without user")]
    [Arguments("bad port")]
    [Arguments("bad scheme")]
    [Arguments("empty entry")]
    [Arguments("no hosts")]
    public async Task Ambiguous_or_malformed_endpoints_are_rejected(string scenario)
    {
        Action resolve = scenario switch
        {
            "credentials twice" => () => Resolve(["amqp://u:p@rabbit-1"], username: "other"),
            "virtual host conflict" => () => Resolve(["amqp://rabbit-1/a"], virtualHost: "b"),
            "different virtual hosts" => () => Resolve(["amqp://rabbit-1/a", "amqp://rabbit-2/b"]),
            "tls with amqp scheme" => () => Resolve(["amqp://rabbit-1"], useTls: true),
            "password without user" => () => Resolve(["rabbit-1"], password: "x"),
            "bad port" => () => Resolve(["rabbit-1:99999"]),
            "bad scheme" => () => Resolve(["http://rabbit-1"]),
            "empty entry" => () => Resolve([" "]),
            _ => () => Resolve([]),
        };

        await Assert.That(resolve).Throws<ArgumentException>();
    }

    [Test]
    public async Task Passwords_never_appear_in_error_messages()
    {
        var error = Assert.Throws<ArgumentException>(() => Resolve(["amqp://user:TopSecret@rabbit-1"], username: "other"));

        await Assert.That(error.Message).DoesNotContain("TopSecret");
    }

    [Test]
    [Arguments("guest:Top@Secret@rabbit-1")]
    [Arguments("guest:TopSecret@rabbit-1")]
    [Arguments("guest:TopSecret@rabbit-1:5672")]
    public async Task Credentials_in_a_plain_host_entry_are_rejected_without_revealing_the_password(string entry)
    {
        var error = Assert.Throws<ArgumentException>(() => Resolve([entry]));

        await Assert.That(error.Message).DoesNotContain("Secret");
        await Assert.That(error.Message).Contains("amqp://");
    }

    [Test]
    public async Task An_unescaped_at_sign_in_a_uri_password_is_redacted_completely()
    {
        var error = Assert.Throws<ArgumentException>(() => Resolve(["amqp://user:Top@Secret@rabbit-1"], username: "other"));

        await Assert.That(error.Message).DoesNotContain("Secret");
    }

    [Test]
    public async Task Port_option_is_a_default_and_a_port_given_with_the_host_wins()
    {
        var uris = Resolve(["rabbit-1", "rabbit-2:5673", "amqp://rabbit-3:5674"], port: 5000);

        await Assert.That(string.Join(" ", uris.Select(u => u.Port))).IsEqualTo("5000 5673 5674");
    }

    [Test]
    [Arguments("rabbit-1;rabbit-2, rabbit-3", "rabbit-1|rabbit-2|rabbit-3")]
    [Arguments("amqp://rabbit-1;amqps://rabbit-2", "amqp://rabbit-1|amqps://rabbit-2")]
    [Arguments("amqp://user:p;a,ss@rabbit-1;rabbit-2", "amqp://user:p;a,ss@rabbit-1|rabbit-2")]
    [Arguments("amqp://us;er:pass@rabbit-1,amqp://user:x;y@rabbit-2", "amqp://us;er:pass@rabbit-1|amqp://user:x;y@rabbit-2")]
    public async Task A_host_list_is_split_by_separators_but_not_inside_uri_credentials(string list, string expected)
    {
        var hosts = EndpointResolver.SplitList(list);

        await Assert.That(string.Join("|", hosts)).IsEqualTo(expected);
    }

    private static IReadOnlyList<Uri> Resolve(
        string[] hosts, string? username = null, string? password = null, string? virtualHost = null, int? port = null, bool useTls = false) =>
        EndpointResolver.Resolve(hosts, username, password, virtualHost, port, useTls);
}

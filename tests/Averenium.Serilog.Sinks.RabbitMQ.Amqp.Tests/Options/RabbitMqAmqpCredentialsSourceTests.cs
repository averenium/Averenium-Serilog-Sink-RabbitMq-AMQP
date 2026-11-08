namespace Averenium.Serilog.Sinks.RabbitMQ.Amqp.Tests.Options;

public sealed class RabbitMqAmqpCredentialsSourceTests
{
    [Test]
    public async Task Update_with_new_values_notifies_once()
    {
        var source = new RabbitMqAmqpCredentialsSource("app", "old");
        var changes = 0;
        source.Changed += () => changes++;

        source.Update("app", "new");

        await Assert.That(changes).IsEqualTo(1);
        await Assert.That(source.Current.Password).IsEqualTo("new");
    }

    [Test]
    public async Task Update_with_the_same_values_does_not_notify()
    {
        var source = new RabbitMqAmqpCredentialsSource("app", "same");
        var changes = 0;
        source.Changed += () => changes++;

        source.Update("app", "same");

        await Assert.That(changes).IsEqualTo(0);
    }

    [Test]
    public async Task The_password_never_appears_in_text()
    {
        var source = new RabbitMqAmqpCredentialsSource("app", "TopSecret");

        await Assert.That(source.ToString()).DoesNotContain("TopSecret");
        await Assert.That(source.Current.ToString()).DoesNotContain("TopSecret");
    }

    [Test]
    public async Task An_empty_user_name_is_rejected() =>
        await Assert.That(() => new RabbitMqAmqpCredentialsSource("", "x")).Throws<ArgumentException>();

    [Test]
    public async Task Credentials_and_username_together_are_rejected()
    {
        var options = new RabbitMqAmqpSinkOptions { Exchange = "logs", Username = "app", Credentials = new("app", "x") };
        options.Hostnames.Add("rabbit");

        await Assert.That(options.Validate).Throws<ArgumentException>();
    }

    [Test]
    public async Task Endpoints_use_the_current_credentials_of_the_source()
    {
        var options = new RabbitMqAmqpSinkOptions { Exchange = "logs", Credentials = new("app", "old") };
        options.Hostnames.Add("rabbit");

        options.Credentials.Update("app", "new");

        await Assert.That(options.ResolveEndpoints().Single().UserInfo).IsEqualTo("app:new");
    }
}

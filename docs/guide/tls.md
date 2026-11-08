# TLS

[← README](../../README.md) · [Options reference → TLS](options.md#tls-tls)

`UseTls = true` (or `amqps://` hosts) is enough when the broker certificate is trusted by the operating system.
Inside a cluster the broker usually has a certificate from a private CA — point the sink at it instead of changing the
OS trust store. All TLS details live in one object, `Tls`:

```csharp
.WriteTo.RabbitMQAmqp(o =>
{
    o.Hostnames.Add("rabbit-1");
    o.UseTls = true;                                        // amqps, port 5671
    o.Tls.CaCertificatePath = "/etc/rabbitmq-ca/ca.crt";    // PEM (bundle) or DER, e.g. a mounted secret
    o.Username = "app";
    o.Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD");
    o.Exchange = "logs";
})
```

```json
"Args": {
  "hostnames": [ "rabbit-1" ],
  "useTls": true,
  "tls": {
    "caCertificatePath": "/etc/rabbitmq-ca/ca.crt",
    "clientCertificatePath": "/etc/rabbitmq-client/client.pfx",
    "clientCertificatePassword": "set me from a secret",
    "useExternalAuthentication": true
  },
  "exchange": "logs"
}
```

## Mutual TLS and SASL EXTERNAL

Add a client certificate: a `.pfx`/`.p12` file (`ClientCertificatePassword`), a PEM certificate with its key
(`ClientCertificateKeyPath`, or both in one file; an encrypted key uses the password), or an `X509Certificate2` in code
(`ClientCertificate`).

With `UseExternalAuthentication = true` the broker authenticates the client by that certificate (SASL EXTERNAL, needs
the `rabbitmq_auth_mechanism_ssl` plugin; the user is the certificate's common name by default). `Username`/`Password`
and credentials in host URIs must then not be set.

## Certificate files and rotation

- Files are read when the logger is created (a missing or unreadable file fails fast) and again at every TLS
  handshake. `Tls` settings without any TLS host are rejected.
- **Rotation needs no restart.** When cert-manager (or anything else) renews a mounted client certificate or CA bundle,
  the next connection uses the new files; they are reloaded only if their contents changed, and a file caught in the
  middle of an update keeps the previous certificate. An established connection keeps working with the certificate it
  was opened with. Nothing is polled and nothing is written.
- When the broker's CA is rotated, ship a bundle with both the old and the new CA in `Tls.CaCertificatePath` during
  the transition.
- `Tls.AcceptablePolicyErrors` is applied by the sink itself (the AMQP client ignores that setting).

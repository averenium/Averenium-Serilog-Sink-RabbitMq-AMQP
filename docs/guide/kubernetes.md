# Running in Kubernetes and clusters

[← README](../../README.md)

## Pods

- **Shutdown** — set `terminationGracePeriodSeconds` above `ShutdownTimeout` plus ~2 s (and your app's own shutdown
  work) and call `Log.CloseAndFlushAsync()`, otherwise buffered events are lost when the pod is killed. Details:
  [Delivery → Shutdown](delivery.md#shutdown).
- **Memory** — during an outage the sink holds formatted bytes, bounded by `QueueSizeLimitBytes` (default 15 MiB,
  roughly 10–15 thousand typical events, fewer with stack traces), plus a small burst channel and the publish window.
  Raise it if pods have memory to spare; an OOM kill would lose the whole buffer.
- **Fallback** — `WriteUndeliveredToStandardOutput = true` writes every undelivered event to stdout, where your log
  agent picks it up.
- **Secrets** — rotated passwords (through `IConfiguration`) and renewed certificate files are picked up without a
  restart: [Configuration → Rotating the password](configuration.md#rotating-the-password-without-a-restart),
  [TLS → rotation](tls.md#certificate-files-and-rotation).
- **No disk** is used; read-only root file systems are fine. Certificate files are only read.

## RabbitMQ cluster

- List all nodes in `Hostnames` (or put a load balancer in front); the sink fails over round-robin.
- Store logs in a **quorum queue** (or a stream) — classic queues live on a single node.
- Add a fallback sink via `WriteTo.FallbackChain`.

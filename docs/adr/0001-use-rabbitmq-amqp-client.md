# ADR 0001: Use RabbitMQ.AMQP.Client (AMQP 1.0) as the only transport

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

Available Serilog sinks for RabbitMQ are built on `RabbitMQ.Client` (AMQP 0-9-1). RabbitMQ 4.0 made AMQP 1.0
a core protocol and provides `RabbitMQ.AMQP.Client`, a dedicated .NET client with connection recovery,
publish outcomes and a management API over AMQP 1.0. No Serilog sink built on it was found.

## Decision

The sink uses `RabbitMQ.AMQP.Client` exclusively. `RabbitMQ.Client` is not referenced.
The minimum supported broker is RabbitMQ 4.0. The library targets `net10.0`.

## Consequences

- RabbitMQ 3.x is not supported.
- Recovery, addressing and outcomes follow the AMQP 1.0 client model.
- Widening target frameworks (e.g. `net8.0`, `netstandard2.0`, both supported by the client) requires a new ADR.

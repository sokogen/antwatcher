# ADR 0002: Bus abstraction

Status: accepted (2026-09)

## Context

The bus is the durability boundary of the system (ADR 0001). Different deployments want
different brokers: an embedded one for small installations, NATS JetStream, later Kafka,
Google Pub/Sub, AMQP, SQL, or Redis Streams. Watermill offers a Pub/Sub implementation for
each, but the implementations differ in what they can promise: whether a publish is
durable, whether a consumer's position survives a restart, whether history can be
replayed, whether duplicates are collapsed, and how retention is bounded.

We do not want the receiver, the router, or the sinks to know which broker is in use, and
we do not want a broker's weakness to silently weaken the guarantees stated in
`docs/operations.md`.

## Decision

**One contract.** `internal/bus` defines:

```
Bus
├── Publish(ctx, msg)               nil only under the driver's durability guarantee
├── Subscribe(ctx, consumer, opts)  independent consumer "sink-<name>", own position, every message
├── Topic()
├── Capabilities()                  declared once per driver (+config), shown in /status
└── Close()
LagReporter (optional)              Lag(ctx, consumer) for antwatcher_bus_consumer_lag
```

Every consumer receives every message (fan-out); consumers never share work. A
`SubscribeOptions.StartFrom` of `Earliest` or `Now` is mandatory in configuration because
the two differ by a potentially huge replay.

**Capabilities, declared once.** A driver reports what it guarantees with its
configuration:

| Capability | Meaning |
|---|---|
| `DurablePublish` | `Publish` returns nil only once the broker has stored the message. Without it a webhook `2xx` does not mean the event is safe. |
| `DurableConsumers` | A consumer position survives restarts of antwatcher and of the broker. |
| `FanOut` | Every consumer receives every message. Required by the architecture. |
| `HistoricalReplay` | A consumer created with `Earliest` receives the retained history first. |
| `Deduplicates` | The broker collapses a re-published UUID inside its window. |
| `ReportsLag` | The driver implements `LagReporter`. |
| `Retention` | `none`, `time`, `size`, or `until_acked`. |

There is deliberately no `Ordered` capability: nothing in antwatcher relies on ordering,
because GitHub itself does not deliver in order.

**One policy.** `bus.ResolveIngress` and `bus.ResolveConsumer` are the only places that
compare capabilities with needs, under `bus.on_missing_capability: fail | degrade`:

| Missing | `fail` | `degrade` |
|---|---|---|
| `DurablePublish` (ingress) | refuse to start | start, warning "2xx does not mean durable, dev only" |
| `FanOut` | refuse | refuse (cannot be degraded) |
| `HistoricalReplay` with `earliest` | refuse | downgrade to `now`, warning |
| `DurableConsumers` | warning | warning |
| `Deduplicates` | informational | informational |

Warnings are logged at startup and listed in `/status` next to the bus and the affected
sink, so an operator can see exactly which guarantee a deployment lacks.

**Conformance suite.** `internal/bus/bustest.Run` exercises every driver: publish and
subscribe, nack and redelivery, consumer name validation, topic binding, close semantics,
and, for each declared capability, a test that proves it (fan-out to several consumers,
replay for a late `Earliest` consumer, position kept across subscriber restarts,
deduplication of a repeated UUID, lag accounting, and a durable publish that survives a
broker outage injected through `WithOutage`). A driver that declares a capability it does
not deliver fails its own test suite.

**Driver registry.** Drivers register a factory and a config describer under their name
from an `init` function; `cmd/antwatcher/drivers.go` is the one file that imports them.
The describer decodes the driver's block strictly (unknown keys fail), masks its secrets
for `-check` and `/status`, and can validate against the core configuration (JetStream
enforces `ack_wait > router.process_timeout + 10s`). An architecture test ensures no
package outside `cmd` depends on a concrete driver.

**Watermill driver matrix.** Declared capabilities for the two drivers that exist and the
expected ones for future drivers:

| Driver | DurablePublish | DurableConsumers | FanOut | Replay | Dedup | Retention |
|---|---|---|---|---|---|---|
| `gochannel` | no | no | yes | in-process | no | none |
| `nats-jetstream` | yes (PubAck) | yes | yes | yes | window | time |
| kafka (future) | yes (`acks=all`) | consumer groups | yes | yes (earliest offset) | no | time/size |
| googlecloud (future) | yes | subscription per sink | yes | yes with topic retention (up to 31d, seek) | no | up to 31d |
| amqp (future) | with publisher confirms | durable queue per sink | exchange fan-out | no | no | until acked |
| sql (future) | transaction commit | consumer groups | yes | yes | no | until pruned |
| redisstream (future) | yes | groups | yes | yes | no | maxlen |
| aws sns+sqs (future) | yes | yes | SNS fan-out | no | FIFO 5m | up to 14d |

**JetStream specifics.** `Publish` uses the context-aware `jetstream.PublishMsg` of
nats.go directly and returns after the PubAck. The subscriber is an in-house
`message.Subscriber` over a nats.go durable pull consumer, not the watermill-nats
JetStream subscriber: that package is beta, binds the stream name to the topic, ignores
consumer configuration for grouped consumers, drains the shared connection on close, and
holds one unacknowledged message per subscription. The choice stays inside
`internal/bus/natsjs`; the driver's public surface is the contract above. Consumers use
`MaxDeliver: -1` (unlimited) and a nack delay growing from `nak_delay_min` to
`nak_delay_max` with the delivery count.

## How to add a driver

1. Create `internal/bus/<name>` with a `Config` struct (yaml tags, `config.Secret` for
   secrets), `DefaultConfig`, `Describe` (strict decode on top of the defaults), and `Open`
   matching `bus.Factory`.
2. Implement `bus.Bus`: `Publish` must honor `ctx` and return nil only under the durability
   guarantee you declare; `Subscribe` must call `bus.ValidateConsumerName`, create an
   independent consumer per name, and return a Subscriber bound to the bus topic
   (`bus.ErrWrongTopic` otherwise); `Close` must be idempotent and make later calls return
   `bus.ErrClosed`. Implement `bus.LagReporter` if you declare `ReportsLag`.
3. Declare `Capabilities()` truthfully from the configuration (for example JetStream declares
   `Deduplicates` only when `dedup_window > 0`).
4. Register in `init`: `bus.Register(Name, bus.Driver{Factory: Open, Describer: config.DescriberFunc(Describe)})`.
5. Write `driver_test.go` with `bustest.Run(t, open)`; pass `bustest.WithOutage` when you
   declare `DurablePublish`. Add capability and policy assertions as in the gochannel test.
6. Import the package in `cmd/antwatcher/drivers.go`, add it to `busDrivers` in
   `internal/archtest`, and add a row to the matrices in `docs/configuration.md` and
   this ADR.

## Consequences

- Core code is broker-agnostic and stays that way by test, not by discipline.
- A weaker broker cannot pretend: either the configuration refuses it, or `/status` says
  which guarantee is missing.
- A new driver costs one package and one conformance run; the suite is the acceptance
  test and the documentation of the contract.
- Capabilities are coarse. A broker with an unusual mix (for example durable publish but
  no fan-out) is simply refused; we accept that instead of adding policy branches.

# ADR 0004: Errors, retries, and stalled sinks

Status: accepted (2026-09)

## Context

A destination can be unreachable for seconds (a rolling restart), hours (an outage), or
forever (a revoked credential, a schema mismatch, a payload the destination refuses).
Each sink processes messages from a durable bus that redelivers whatever is not
acknowledged. We need one rule set that never loses an event because of a processing
failure, never blocks other sinks, never hides a broken sink, and does not duplicate the
retry logic the broker already provides.

Watermill offers `Retry` and `Poison` middleware; brokers offer redelivery with backoff
and dead-letter queues. Using all of them at once creates layered, unpredictable timing
and quietly discards messages into queues nobody watches.

## Decision

**Two error classes, decided by the driver.** A driver returns either an error wrapped by
`sink.Permanent` (retrying cannot help: authentication, authorization, schema mismatch,
invalid argument, malformed input) or any other error (retryable: network, timeout, 5xx,
rate limit, unknown). When in doubt the error is retryable: a wrongly permanent error
stalls a message until an operator acts, a wrongly retryable one only costs redeliveries.
`ErrSkipped` means the projection produced nothing (a trace sink seeing a queued job) and
is acknowledged like success. The architecture test requires every driver to classify.

**Short retries belong to the destination client.** A driver may retry a transient failure
a bounded number of times with a short backoff (the OTLP client: two retries, 500 ms
doubling), inside the `router.process_timeout` deadline of one `Process` call. Then it
returns the error.

**Long retries belong to the broker.** The router acknowledges on success or skip and
nacks on any error; it never sleeps, never retries, and never acknowledges a failed
message. On JetStream the nack carries a delay that grows from `nak_delay_min` doubling
per delivery up to `nak_delay_max`, `MaxDeliver` is unlimited, and `ack_wait` is validated
to exceed `process_timeout` by 10 s so a slow call times out and nacks before a concurrent
redelivery of the same message. The backlog stays on the bus for the retention window and
drains at the destination's pace once it is back. No Watermill `Retry` or `Poison`
middleware is used; the architecture test forbids them along with `time.Sleep`, `Ack`, and
`Nack` calls anywhere under the sink tree.

**Permanent errors stall, they do not discard.** A permanent error, including a message
the sink cannot decode, is recorded as stalled for that sink under the message UUID (with
reason, first and last seen, attempts) and the message is nacked at once. The broker keeps
re-attempting it at its pace; the entry is cleared when the same UUID later succeeds. While
any entry exists the sink reports `antwatcher_sink_stalled{sink}=1`,
`antwatcher_sink_stalled_messages{sink}` counts them, and `/status` lists them. Other
messages of the same sink continue to flow around the stalled ones, and other sinks are
unaffected. Nothing is ever dropped by antwatcher: the operator fixes the cause (rotate the
credential, migrate the table, correct the endpoint) and the stalled messages go through.

**No dead-letter queue.** A DLQ is a second store with its own retention, monitoring, and
replay tooling, and in practice it is where events go to be forgotten. The bus already
retains the message; the stall record already points at it; the fix is to repair the
destination, not to move the message. If the message cannot ever succeed (an undecodable
message, which should not happen because the receiver validated it), the operator's
remedy is explicit: delete the consumer or purge the UUID, a visible action rather than a
silent one.

**Protocol-level rejections are the only loss, and they are counted.** OTLP partial
success means the collector accepted the batch but rejected some items; retrying would
duplicate the accepted ones. The export counts as success, the rejected items are counted
in `antwatcher_otlp_rejected_total{signal}`, and the README says so.

**Startup never depends on a destination.** Sink factories do not dial. An unreachable
destination surfaces from the first `Process` as a retryable error, so the sink starts
degraded and the webhook path is unaffected. Only invalid static configuration fails
startup.

**Health is expressed as metrics, not as process state.** `antwatcher_bus_consumer_lag`
per consumer, `antwatcher_sink_events_total{result}` (`ok`, `skipped`, `error`,
`permanent`), `antwatcher_sink_last_success_timestamp_seconds`, and the stalled gauges are
the operational surface. `/readyz` depends on the bus only; a failing sink never takes the
receiver out of a load balancer, because that would turn a destination outage into an
ingress outage.

## Consequences

- Timing is predictable: one bounded retry inside the driver, then the broker's schedule.
  There is one place to tune (`nak_delay_min`, `nak_delay_max`, `process_timeout`,
  `ack_wait`).
- Permanent failures are loud and localized. An alert on `antwatcher_sink_stalled` names the
  sink; `/status` names the message and the reason.
- Because failed messages are never acknowledged, a stalled message is re-attempted for as
  long as it stays within retention. On a destination that rejects it permanently this is
  one request every `nak_delay_max` per stalled message; that is the accepted cost of
  never discarding.
- Sinks must be idempotent, which they are through deterministic IDs (ADR 0003), because
  redelivery after a partial failure is the normal path.
- Drivers carry the responsibility of classification. A misclassification is a bug in the
  driver, found by the tests each driver ships with, not a runtime policy question.

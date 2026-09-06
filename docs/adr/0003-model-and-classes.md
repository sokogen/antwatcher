# ADR 0003: Canonical model and destination classes

Status: accepted (2026-09)

## Context

Five kinds of destination want the same GitHub events in different shapes: spans for a
trace backend, log records, warehouse rows, raw archives, and re-published copies. Each
destination technology (Tempo, Loki, BigQuery, a filesystem, another bus) has its own
client, credentials, and error semantics, and more will be added (ClickHouse, S3, GCS,
Parquet, HTTP forward). Without structure, GitHub payload parsing would be repeated per
destination and drift between them.

## Decision

**One canonical model.** `internal/model` is the only package that understands GitHub
payloads. `model.Normalize(envelope)` returns an `Execution` of kind `run`, `job`, or
`none`: a `workflow_run` event yields a `Run`, a `workflow_job` event yields a `Job` with
its `Steps`, any other event yields `none` with the envelope only. A known event whose
payload does not decode returns `ErrMalformed`, which is permanent by definition. The model
is sparse: fields a payload may lack are pointers and stay nil; nothing is fetched to fill
them (ADR 0001).

**Deterministic identifiers** live in the model and are the basis of idempotency
everywhere:

```
trace_id     = sha256("antwatcher:trace:" + repository_id + ":" + run_id + ":" + run_attempt)[:16]
run_span_id  = sha256("antwatcher:run:"   + run_id + ":" + run_attempt)[:8]
job_span_id  = sha256("antwatcher:job:"   + job_id)[:8]
step_span_id = sha256("antwatcher:step:"  + job_id + ":" + step_number)[:8]
record_id    = hex(sha256("antwatcher:record:" + kind + ":" + repository_id + ":" + run_id + ":" + run_attempt [+ ":" + job_id [+ ":" + step_number]]))
```

The same delivery, processed twice or by two replicas, produces byte-identical spans,
records, and archive lines.

**Five classes, each owning a projection and a small driver contract.** A class is a
package under `internal/sink/<class>` with a `Sink` implementing `sink.Sink` and an
interface its drivers implement. Drivers live under `internal/sink/<class>/drivers/<name>`
and only move bytes.

| Class | Projection | Driver contract | Drivers |
|---|---|---|---|
| `trace` | completed run → run span; completed job → job span + step spans; earlier states → `ErrSkipped` | `Exporter{ExportSpans(ctx, []otlp.Span); Close()}` | `otlp` |
| `log` | every event → one OTLP `LogRecord` with envelope attributes and, for runs and jobs, the trace and span IDs | `Writer{Write(ctx, otlp.LogRecord); Close()}` | `stdout`, `otlp` |
| `analytics` | run, job, and step `Record`s with `record_id`, `status_rank`, `event_time`; a portable `Schema`; `CurrentViewSQL` | `Writer{EnsureSchema(ctx, Schema); Write(ctx, []Record); Close()}` | `bigquery` |
| `archive` | the raw envelope as one JSON document (envelope fields plus nested payload) | `Writer{Append(ctx, event.Envelope); Close()}` | `filesystem` |
| `forward` | the raw message under a new transport UUID `sha256(delivery_guid + sink + topic)`, delivery GUID kept in metadata, hop markers, `max_hops` | `Publisher{Publish(ctx, *message.Message); Close()}` (`bus.Bus` satisfies it) | `bus` |

The trace and log projections share attribute names so a query written for spans also
finds the log lines. The OTLP client (`internal/otlp`: gRPC and HTTP transports, TLS,
headers, gzip, bounded retry, partial-success accounting) is shared by the trace and log
`otlp` drivers.

**Analytics is at-least-once with a current view.** The base table receives every
projected row through the BigQuery Storage Write API default stream: no `insertId`, no
offsets, no exactly-once. The table is DAY partitioned on `event_time` and clustered by
`repository, kind`. Consumers read `<table>_current`, a view that keeps, per `record_id`,
the row with the highest `status_rank`, then the latest `event_time`, then the latest
`received_at`. Duplicates and out-of-order deliveries therefore never reach a dashboard.
Job and step rows are sparse (no workflow path, trigger, or actor); they join run rows on
`repository_id, run_id, run_attempt`.

**Configuration model.** A sink is `name`, `class`, `driver`, a mandatory `start_from`,
and an opaque `config` block. The registry maps `(class, driver)` to a factory and a
describer; the describer decodes the block strictly and masks secrets for `-check` and
`/status`. A factory validates and constructs but never performs a network round trip:
credentials are read, files are opened, connections are made on the first `Process`. An
unreachable destination thus starts the sink degraded (retryable errors, redelivered by
the bus) instead of failing startup. The forward driver additionally refuses a target equal
to the ingress bus and topic, which would be a loop.

**Every sink is its own consumer.** The router binds each sink to the consumer
`sink-<name>` with the position resolved by the bus policy (ADR 0002). Renaming a sink
creates a new consumer and a fresh position; with limits retention the orphaned consumer
pins no data.

**Metrics as a destination class is deferred.** Emitting GitHub Actions metrics (durations,
queue times, success rates) to a metrics backend is a natural sixth class. It is not built
here because the analytics class already carries the same facts in queryable form, and the
aggregation semantics (which histogram buckets, which labels, cardinality of repositories
and workflows) deserve their own design. Service metrics of antwatcher itself are a
separate concern (`internal/metrics`) and exist.

## How to add a sink driver

1. Pick the class. If the destination fits an existing projection (spans, log records,
   analytics records, raw envelopes, raw messages), implement that class's driver
   interface. A new projection is a new class and gets its own plan.
2. Create `internal/sink/<class>/drivers/<name>` with a `Config` (yaml tags,
   `config.Secret` for secrets), `DefaultConfig`, a `Describe` function, and a `Factory`
   matching `sink.DriverFactory` that decodes with `config.DecodeStrict`, validates, and
   returns `<class>.New(name, yourDriver)`. Do not dial in the factory.
3. Classify errors in the driver: wrap failures a retry cannot fix (authentication,
   authorization, schema mismatch, invalid argument, malformed input) with
   `sink.Permanent`; leave network, timeout, 5xx, rate-limit, and unknown errors
   retryable. When in doubt, retryable. The architecture test requires each driver package
   (or the shared client or class package it delegates to) to call `sink.Permanent`.
4. Keep any retry short and bounded (the OTLP client does two retries with 500 ms
   backoff). Long-term retry belongs to the bus.
5. Register in `init`: `sink.RegisterDriver(sink.Class<Class>, Name, Driver())`.
6. Write tests: the class's projection is already tested, so test the driver against a
   fake or in-memory destination, cover a retryable and a permanent error, and prove the
   factory builds with an unreachable endpoint.
7. Import the package in `cmd/antwatcher/drivers.go` and add it to the README matrix and
   `antwatcher.example.yml`.

## Consequences

- GitHub payload changes are absorbed in one package with fixtures; sinks never see JSON.
- Adding a destination is a driver, typically a few hundred lines plus tests, with no
  changes to the router, the receiver, or the other classes.
- Sinks are cheap to add and to remove; each is an independent consumer, so a broken or
  slow one costs nothing to the others.
- The model's sparseness is visible to analytics users, who join run and job rows. That
  trade is documented rather than hidden behind API calls.

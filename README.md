# antwatcher

antwatcher turns GitHub Actions webhooks into traces, logs, analytics records, archives,
and forwarded events, without losing events across destination outages, restarts, or
short receiver outages.

It is a single Go binary. GitHub posts `workflow_run` and `workflow_job` webhooks to it,
every delivery is stored on a durable bus before GitHub gets a `2xx`, and each configured
destination consumes the bus independently at its own pace.

> **Webhooks are the source of truth. The bus provides durability and replay. The GitHub
> deliveries API repairs missed ingress. Destinations consume independently.**

The Actions REST API is never used to reconstruct runs, jobs, or steps.

## Architecture

```
                 HMAC     publish (durable)      2xx only after PubAck
GitHub ─POST──▶ receiver ───────────────▶ bus ────────────────────────▶ GitHub
                 :8080                    │  topic antwatcher.events
                                          │  one message per delivery
                                          │  (envelope metadata + raw payload)
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │ consumer sink-tempo       │ consumer sink-bq          │ consumer sink-raw ...
              ▼                           ▼                           ▼
        ┌───────────┐               ┌───────────┐               ┌───────────┐
        │ Normalize │               │ Normalize │               │  raw env  │
        │  ▼ trace  │               │  ▼ analyt.│               │  ▼ archive│
        │ otlp drv  │               │ bigquery  │               │ filesystem│
        └─────┬─────┘               └─────┬─────┘               └─────┬─────┘
              ▼                           ▼                           ▼
            Tempo                      BigQuery                   ./data/archive

        ack on success · nack on error → broker redelivery with growing delay
        permanent error → message stalled for that sink, re-attempted, never dropped

recovery (optional): GitHub deliveries API ──▶ redeliver GUIDs without a 2xx ──▶ receiver
admin :9090: /metrics /status /healthz /readyz
```

Every message on the bus is one webhook delivery: the Watermill UUID is the GitHub
delivery GUID, the metadata carries the envelope (`delivery_guid`, `event`, `action`,
`hook_id`, `received_at`, `repository_id`, `repository`, `schema_version`), and the
payload is the GitHub JSON byte for byte. The retained log of the bus is therefore the raw
history for its retention window.

Each sink is an independent named consumer (`sink-<name>`) with its own position. A slow
or broken sink never affects another. Sinks are idempotent through deterministic IDs
derived from GitHub identifiers, so redelivery is always safe.

The design decisions are recorded in the ADRs under [`docs/adr`](docs/adr):
[architecture](docs/adr/0001-architecture.md), [bus abstraction](docs/adr/0002-bus-abstraction.md),
[model and classes](docs/adr/0003-model-and-classes.md), [errors and retries](docs/adr/0004-errors-and-retries.md).

## Guarantees

The contract, stated honestly. At-least-once delivery to every sink, bounded by the bus
retention.

| Situation | Behavior |
|---|---|
| Sink temporarily unavailable | Backlog is kept on the bus and redelivered with a growing delay while the event is inside bus retention. Lag is visible per sink. |
| Sink down longer than bus retention | The oldest part of its backlog is gone. Retention is a stream limit; size it for the longest outage you want to survive. |
| Sink hits a permanent error or an undecodable message | That message is recorded as stalled for that sink (by UUID) and re-attempted at the broker's pace. Nothing is discarded; other sinks continue. There is no dead-letter queue. |
| Destination rejects part of an OTLP export | Counted in `antwatcher_otlp_rejected_total`, not retried (OTLP partial-success rule). The export counts as success. |
| Destination unreachable at startup | The sink starts degraded and catches up when the destination is back. Startup and the webhook path are unaffected. |
| Process restart | Each sink resumes from its durable consumer position. Acked messages are not delivered again. |
| Receiver unavailable or publish slow | GitHub gets a `503` (or a timeout) and records a failed delivery. Nothing was accepted, so nothing is lost silently. |
| Receiver back within 3 days, recovery enabled | A full scan of the deliveries API at start, incremental scans afterwards. Every GUID without a `2xx` is redelivered by GitHub through the normal receiver path. |
| GitHub API down or token invalid | Recovery is degraded (metric and `/status` reason). Ingress continues. |
| Duplicate webhook | Accepted. JetStream collapses repeats inside `dedup_window`; deterministic IDs make trace, log, and archive projections idempotent; analytics is deduplicated in the `_current` view. |
| New sink with `start_from: earliest` | Receives the retained history first, when the bus can replay. Otherwise the policy decides (fail, or downgrade to `now` with a warning). |
| Bus without a required capability | Startup fails, or starts degraded with the warning shown in `/status`, according to `bus.on_missing_capability`. A non-durable bus is dev-only and says so. |
| Secrets | `serve -check` and `/status` never print secret values. |
| History older than bus retention | Not recoverable. By design. |
| Missed ingress older than GitHub's 3-day delivery window | Considered lost. |

## Quick start

The smallest useful deployment: embedded NATS JetStream for durability, one log sink to
stdout, one archive sink to disk. No external services.

1. Build (Go 1.27 or newer):

   ```sh
   make build          # ./bin/antwatcher
   ```

2. Write `antwatcher.yml`:

   ```yaml
   server:
     webhook_secret: ${GITHUB_WEBHOOK_SECRET}
   bus:
     driver: nats-jetstream
     nats-jetstream:
       embedded: true
       store_dir: ./data/nats
   sinks:
     - name: events
       class: log
       driver: stdout
       start_from: now
     - name: raw
       class: archive
       driver: filesystem
       start_from: earliest
       config:
         dir: ./data/archive
   ```

3. Validate and run:

   ```sh
   export GITHUB_WEBHOOK_SECRET='choose-a-long-random-string'
   ./bin/antwatcher serve -config antwatcher.yml -check   # prints the effective config, secrets masked
   ./bin/antwatcher serve -config antwatcher.yml
   ```

4. Send a signed test delivery from another shell:

   ```sh
   body='{"zen":"Keep it logically awesome.","hook_id":1}'
   sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$GITHUB_WEBHOOK_SECRET" | sed 's/^.* //')
   curl -sS -X POST http://localhost:8080/webhook \
     -H 'Content-Type: application/json' \
     -H 'X-GitHub-Event: ping' \
     -H 'X-GitHub-Delivery: 00000000-0000-0000-0000-000000000001' \
     -H "X-Hub-Signature-256: sha256=$sig" \
     -d "$body"
   ```

   A `ping` is acknowledged without being published. Point a real webhook at the receiver
   (see [GitHub webhook setup](#github-webhook-setup)) and every `workflow_run` and
   `workflow_job` event appears as a JSON line on stdout and in `./data/archive`.

5. Look at the service:

   ```sh
   curl -s http://127.0.0.1:9090/status | jq .
   curl -s http://127.0.0.1:9090/metrics | grep antwatcher_
   ```

The container image does the same with `docker compose -f docker-compose.example.yml up`,
adding Grafana Tempo and Loki behind an OpenTelemetry collector. See [Docker](#docker).

## Configuration reference

The full reference is `antwatcher.example.yml`, reproduced here verbatim. Every field is
shown with its default where one has one. `${VAR}` and `${VAR:-default}` are expanded on
scalar values; an undefined variable without a default fails loading. Unknown keys fail
loading. `serve -check` validates a file and prints it with secrets masked.

<!-- config-reference:begin (generated from antwatcher.example.yml by `make readme`) -->
```yaml
# antwatcher example configuration.
#
# Every field is shown with its default where one exists. Values may reference
# environment variables as ${VAR} or ${VAR:-default}; a reference to an undefined
# variable without a default fails loading. There is no escape for a literal "${".
#
# Check a configuration without starting the service:
#   antwatcher serve -config antwatcher.yml -check

server:
  # Address of the webhook listener. Only the webhook path and GET /healthz are
  # served here; metrics and status live on the admin listener.
  listen: ":8080"
  # Path GitHub posts deliveries to.
  webhook_path: /webhook
  # Secret configured on the GitHub webhook; every delivery is verified with
  # HMAC-SHA256 over the raw body. Required.
  webhook_secret: ${GITHUB_WEBHOOK_SECRET}
  # Externally visible webhook URL. Informational, shown in /status.
  public_url: https://antwatcher.example.com/webhook
  # Maximum accepted request body (25 MiB, the GitHub payload limit).
  max_body_bytes: 26214400
  # Upper bound for publishing one delivery to the bus. GitHub gives up after
  # 10s, so keep this below it; on timeout the receiver answers 503 and GitHub
  # records a failed delivery that recovery can redeliver.
  publish_timeout: 8s

admin:
  # Address of the admin listener: /metrics, /status, /healthz, /readyz.
  # Must differ from server.listen. Do not expose it publicly.
  listen: "127.0.0.1:9090"
  # How often consumer lag is polled from the bus into antwatcher_bus_consumer_lag.
  lag_interval: 15s

bus:
  # Bus driver: gochannel (in-process, non-durable, development only) or
  # nats-jetstream (embedded or external, durable).
  driver: nats-jetstream
  # Topic (JetStream subject) carrying every webhook delivery.
  topic: antwatcher.events
  # What to do when the driver lacks a capability the configuration needs:
  #   fail    - refuse to start (recommended in production)
  #   degrade - start with a warning shown in /status (for example a non-durable
  #             bus, or start_from: earliest on a bus without history)
  on_missing_capability: fail
  # Driver blocks are keyed by driver name; only the selected one is used.
  nats-jetstream:
    # Run an in-process nats-server with JetStream (single replica only).
    embedded: true
    # JetStream storage directory for the embedded server.
    store_dir: ./data/nats
    # External server URL when embedded is false, e.g. nats://nats:4222.
    url: ""
    # Credentials for an external server (contents of a .creds file or a token).
    credentials: ${NATS_CREDENTIALS:-}
    # Stream name.
    stream: ANTWATCHER
    # Retention window of the raw history. A sink that is down longer than this
    # loses the oldest part of its backlog; size disk accordingly.
    retention: 168h
    # Window in which a redelivered webhook with the same delivery GUID is
    # collapsed by the broker.
    dedup_window: 2h
    # Time the broker waits for an ack before redelivering. Must exceed
    # router.process_timeout by at least 10s (validated).
    ack_wait: 90s
    # Redelivery delay after a failed attempt grows from min to max with the
    # delivery count.
    nak_delay_min: 5s
    nak_delay_max: 5m

router:
  # Time allowed for in-flight handlers to finish on shutdown.
  close_timeout: 30s
  # Deadline for one sink.Process call. Slower destinations time out and the
  # message is redelivered later instead of racing a concurrent redelivery.
  process_timeout: 60s

# Each sink is an independent named consumer of the bus ("sink-<name>") with its
# own position. Renaming a sink creates a new consumer.
#
#   name        unique, [a-z0-9-]
#   class       trace | log | analytics | archive | forward
#   driver      class-specific driver
#   start_from  required, no default:
#                 earliest - replay the retained history on first start
#                 now      - start with the next event
#   config      driver block, documented per driver
sinks:
  # Traces: one span tree per workflow run (workflow -> jobs -> steps), exported
  # over OTLP to Tempo, Jaeger, or an OpenTelemetry collector.
  - name: tempo
    class: trace
    driver: otlp
    start_from: earliest
    config:
      endpoint: "tempo:4317"
      protocol: grpc            # grpc | http
      insecure: true            # plaintext; set false and configure tls for TLS
      timeout: 10s
      compression: none         # none | gzip
      headers: {}               # extra headers, values may be secrets

  # Logs to stdout: one JSON line per webhook event, useful for development
  # and for platforms that ship container output.
  - name: events
    class: log
    driver: stdout
    start_from: now
    config:
      stream: stdout            # stdout | stderr
      pretty: false

  # Logs over OTLP (for Loki through an OpenTelemetry collector). Records carry
  # trace and span IDs so they correlate with the traces above.
  - name: loki
    class: log
    driver: otlp
    start_from: now
    config:
      endpoint: "otel-collector:4318"
      protocol: http
      headers:
        Authorization: ${OTLP_AUTH:-}

  # Analytics: one row per run, job, and step state, appended to BigQuery.
  # The base table is at-least-once; query the <table>_current view.
  - name: bq
    class: analytics
    driver: bigquery
    start_from: earliest
    config:
      project: my-proj
      dataset: github
      table: actions
      ensure_table: true        # create the table on first use
      ensure_view: true         # create the deduplicating <table>_current view

  # Archive: raw webhook payloads as JSON lines with rotation and compression.
  - name: raw
    class: archive
    driver: filesystem
    start_from: earliest
    config:
      dir: ./data/archive
      compress: gzip            # none | gzip
      rotate_every: 1h
      rotate_size: 128MiB

  # Forward: re-publish every raw event to another bus (same drivers as `bus`).
  - name: downstream
    class: forward
    driver: bus
    start_from: now
    config:
      driver: nats-jetstream
      topic: github.events
      max_hops: 3               # drop copies that already hopped this many times
      nats-jetstream:
        embedded: false
        url: nats://other:4222
        credentials: ${DOWNSTREAM_NATS_CREDENTIALS:-}

recovery:
  # Scan the GitHub webhook deliveries API for deliveries without a successful
  # attempt and ask GitHub to redeliver them. Repairs receiver outages shorter
  # than GitHub's 3-day window. Requires auth and at least one target.
  enabled: false
  auth:
    type: token                 # token (github_app is reserved for a later version)
    # Token with admin:repo_hook / admin:org_hook scope.
    token: ${GITHUB_TOKEN:-}
  # GitHub Enterprise API base URL; empty for github.com.
  api_base_url: ""
  # Interval between scans.
  interval: 10m
  # How far back the first scan after start looks (max 72h). Later scans are
  # incremental from the previous scan minus overlap.
  lookback: 72h
  overlap: 15m
  # Deliveries younger than grace, or redelivered within grace, are left alone.
  grace: 5m
  # Upper bound of redelivery requests per scan (rate-limit protection).
  max_per_scan: 100
  # Webhooks to scan: repository webhooks by repo, organization webhooks by org
  # and hook id.
  targets:
    - repo: owner/name
    - org: name
      hook_id: 987654
```
<!-- config-reference:end -->

Command line:

```
antwatcher serve -config antwatcher.yml [-check] [-log-level info] [-log-format json]
antwatcher version
```

Startup fails only on invalid static configuration: unknown keys, an unknown bus or sink
driver, a policy error in `fail` mode, a sink constructor rejecting its block, or an
ingress bus that cannot be opened (it is the durability boundary). An unreachable
destination, a GitHub API error, or a failed hook discovery never stops startup: the sink
or recovery target starts degraded and the webhook path serves.

## Sink classes and drivers

A class owns the projection from the canonical model and a small driver contract. Drivers
of the same class share the projection. Every driver classifies its errors as retryable or
permanent, and no driver constructor touches the network.

| Class | What it emits | Driver | Destination | Notes |
|---|---|---|---|---|
| `trace` | One span tree per workflow run: run span, job spans, step spans. Only completed runs and jobs produce spans; earlier states are skipped. | `otlp` | Any OTLP trace receiver (Tempo, Jaeger, an OpenTelemetry collector) over gRPC or HTTP/protobuf | TLS, mTLS, headers, gzip, short in-client retry |
| `log` | One log record per delivery, every event and action included, with the trace and span IDs of the run or job for correlation | `stdout` | Process stdout or stderr, one JSON document per line | For local use and container log collectors |
| | | `otlp` | Any OTLP log receiver (Loki through a collector, or a collector directly) | Same client as the trace driver |
| `analytics` | Sparse run, job, and step rows with a deterministic `record_id` and a `status_rank` | `bigquery` | BigQuery table through the Storage Write API, plus a `<table>_current` view with the latest state per entity | Base table is at-least-once by design; query the view. Table is DAY partitioned on `event_time`, clustered by `repository, kind` |
| `archive` | The raw envelope and payload as one JSON line per event | `filesystem` | A directory tree `YYYY/MM/DD/<sink>-<ts>-<seq>.jsonl[.gz]`, fsynced before ack | Rotation by size and age, optional gzip, crash repair on start |
| `forward` | The raw event re-published under a new transport UUID, with hop markers | `bus` | Any registered bus driver and topic, publisher only | Refuses to publish back into the ingress topic; `max_hops` bounds chains |

Deterministic identifiers, shared by every class:

```
trace_id     = sha256("antwatcher:trace:" + repository_id + ":" + run_id + ":" + run_attempt)[:16]
run_span_id  = sha256("antwatcher:run:"   + run_id + ":" + run_attempt)[:8]
job_span_id  = sha256("antwatcher:job:"   + job_id)[:8]
step_span_id = sha256("antwatcher:step:"  + job_id + ":" + step_number)[:8]
record_id    = hex(sha256("antwatcher:record:" + kind + ":" + repository_id + ":" + run_id + ":" + run_attempt [+ ":" + job_id [+ ":" + step_number]]))
```

## Bus drivers and capabilities

One contract over any Watermill Pub/Sub. Broker differences are declared once as
capabilities and resolved by one policy (`bus.on_missing_capability`); the receiver, the
router, and the sinks never branch on the driver. A driver must pass the conformance suite
in `internal/bus/bustest` for every capability it declares.

| Driver | DurablePublish | DurableConsumers | FanOut | HistoricalReplay | Deduplicates | ReportsLag | Retention |
|---|---|---|---|---|---|---|---|
| `nats-jetstream` (embedded or external) | yes, after PubAck | yes | yes | yes | yes, inside `dedup_window` | yes | time (`retention`) |
| `gochannel` (dev and tests only) | no | no | yes | in-process only | no | no | none |

What the policy does with a missing capability:

| Missing | Effect |
|---|---|
| `DurablePublish` on the ingress bus | `fail`: refuse to start. `degrade`: start with the warning "2xx does not mean durable", shown in `/status`. |
| `FanOut` | Always refuse: every sink must receive every message. |
| `HistoricalReplay` with `start_from: earliest` | `fail`: refuse. `degrade`: downgrade that sink to `now` with a warning. |
| `DurableConsumers` | Warning: positions are lost on restart. |
| `Deduplicates` | Informational: sinks are idempotent anyway. |

Adding a bus driver means implementing the `bus.Bus` interface, declaring its
capabilities, and passing `bustest.Run`. See [ADR 0002](docs/adr/0002-bus-abstraction.md)
and `CLAUDE.md`.

## GitHub webhook setup

1. Expose the receiver over HTTPS. The reverse proxy must forward the raw body unchanged
   (the HMAC covers the exact bytes) and allow bodies of 25 MiB. Do not expose the admin
   listener.
2. In the repository or organization settings, add a webhook:
   - Payload URL: your `server.public_url`, for example `https://antwatcher.example.com/webhook`
   - Content type: `application/json`
   - Secret: the value of `server.webhook_secret`
   - Events: "Let me select individual events", then **Workflow runs** and **Workflow jobs**
3. GitHub sends a `ping`. The receiver answers `200` without publishing it; the metric
   `antwatcher_webhooks_total{result="ping"}` increments.
4. Trigger a workflow and watch `antwatcher_webhooks_total{result="published"}` and your sinks.

Other event types are accepted, published, logged, and archived, but produce no trace or
analytics rows.

The receiver answers `401` for a missing or wrong signature, `413` above
`max_body_bytes`, `400` for a malformed delivery, and `503` when the bus did not confirm
the publish within `publish_timeout`. Only `503` and timeouts are failed deliveries from
GitHub's point of view, and only those are candidates for recovery.

## Recovery and token permissions

Recovery repairs receiver outages. It lists the deliveries of each configured webhook
through the GitHub API, groups attempts by delivery GUID, and asks GitHub to redeliver
every GUID that never got a `2xx`. Redeliveries re-enter the receiver like any other
webhook, so nothing bypasses the bus. GitHub keeps deliveries for 3 days; anything older
is lost.

Token requirements (`recovery.auth.type: token`):

| Target | Classic personal access token | Fine-grained personal access token |
|---|---|---|
| `repo: owner/name` | `admin:repo_hook` (or `write:repo_hook`) | Repository permission **Webhooks: Read and write** on that repository |
| `org: name` | `admin:org_hook` | Organization permission **Webhooks: Read and write** |

Listing deliveries needs read access; requesting a redelivery needs write access, so
read-only scopes let the scans succeed and the redeliveries fail. GitHub App
authentication is reserved for a later version.

A repository target discovers its hook ID by matching `server.public_url` against the
repository's webhooks; set `hook_id` explicitly when several hooks share the URL or when
`public_url` is empty. Organization targets should always pin `hook_id`.

Tuning:

| Field | Meaning |
|---|---|
| `interval` | Time between scans. Each scan of a target costs at least one API call per page of deliveries. |
| `lookback` | Window of the first scan after start, at most `72h`. Later scans are incremental from the previous scan minus `overlap`. |
| `grace` | Deliveries younger than this, or redelivered within this, are left alone; it must exceed the time GitHub takes to retry a delivery on its own. |
| `max_per_scan` | Upper bound of redelivery requests per scan; the loop also stops early on a rate limit and resumes on the next tick. |

## Sizing notes

**Retention is the outage budget.** `bus.nats-jetstream.retention` (default 7 days) is a
stream max-age. A sink that stays down longer than that loses the oldest part of its
backlog. Pick it from the longest destination outage you want to survive plus the time it
takes you to notice.

**Disk.** Each delivery is stored once regardless of the number of sinks. GitHub Actions
payloads are typically 5 to 30 KiB; `workflow_job` payloads grow with the number of
steps. A repository producing 10 000 deliveries a day at 15 KiB average retains roughly
1 GiB per week of retention. Renaming a sink leaves an orphaned consumer that pins no data
(limits retention), so old consumers do not grow the disk.

**Dedup window.** `dedup_window` (default 2 hours) is the window in which the broker
collapses a re-published delivery GUID. It bounds a broker-side index, not correctness:
recovery redeliveries arriving later than the window are accepted again and handled by the
idempotent sinks. Keep it longer than `recovery.grace` and shorter than `retention`.

**Ack wait versus process timeout.** `ack_wait` must exceed `router.process_timeout` by
at least 10 seconds (validated at load). A slow destination then times out and nacks
before the broker redelivers the same message concurrently.

**Redelivery delay.** After a nack, the delay grows from `nak_delay_min` doubling per
delivery up to `nak_delay_max`. A stalled message with the defaults is re-attempted every
5 minutes after the first few attempts, forever, until it succeeds or the sink is fixed.

**Consumer lag.** `admin.lag_interval` controls how often `antwatcher_bus_consumer_lag`
is refreshed. It is one JetStream consumer-info call per sink per interval.

## Single-replica notes

antwatcher is designed to run as one replica.

- **Embedded NATS** stores its stream under `store_dir` on the local disk of the one
  process. Give it a persistent volume. A second replica with `embedded: true` would have
  its own empty stream and its own consumer positions.
- **Recovery** keeps its scan state in memory and assumes it is the only scanner of a hook.
  Two replicas scanning the same hook would both request redeliveries of the same GUIDs.
  The result is safe (dedup window and idempotent sinks) but wasteful.
- **Restart** is cheap: acked positions are on disk, unacked messages are redelivered after
  `ack_wait`, in-flight handlers get `router.close_timeout` to finish. During a restart
  GitHub records failed deliveries, which recovery repairs.

For more than one replica, run an external NATS cluster (`embedded: false`, `url`) shared
by all replicas, and enable recovery on exactly one instance. Consumers are durable and
named per sink, so replicas of the same configuration share each sink's work, which is
acceptable because the pipeline never relies on ordering.

## Operations

### Endpoints

| Listener | Path | Purpose |
|---|---|---|
| `server.listen` | `POST <webhook_path>` | GitHub deliveries |
| `server.listen` | `GET /healthz` | Liveness, always `200` while the process runs |
| `admin.listen` | `GET /metrics` | Prometheus exposition |
| `admin.listen` | `GET /status` | JSON: version, bus driver with capabilities and policy warnings, every sink with requested and effective `start_from`, last success, and stalled UUIDs, recovery targets with hook IDs and degraded reasons, uptime. Never contains secrets. |
| `admin.listen` | `GET /healthz` | Liveness |
| `admin.listen` | `GET /readyz` | `200` while the bus is open and connected and no shutdown has started. Sinks never affect readiness. |

### Metrics

All series are prefixed `antwatcher_`. Watermill router metrics, Go runtime, and process
collectors are exported as well.

| Metric | Labels | Meaning |
|---|---|---|
| `build_info` | `version`, `commit` | Constant 1 |
| `webhooks_total` | `event`, `result` | `published`, `ping`, `rejected_signature`, `bad_request`, `publish_failed`, `publish_timeout` |
| `webhook_publish_seconds` | | Histogram of the bus publish latency |
| `webhooks_inflight` | | Deliveries currently being handled |
| `bus_connected` | | 1 while the bus connection is up |
| `bus_consumer_lag` | `consumer` | Messages the consumer `sink-<name>` has not acked yet |
| `sink_events_total` | `sink`, `class`, `result` | `ok`, `skipped`, `error` (retryable), `permanent` |
| `sink_process_seconds` | `sink`, `class` | Histogram of one `Process` call |
| `sink_last_success_timestamp_seconds` | `sink` | Unix time of the last successful event |
| `sink_stalled` | `sink` | 1 while at least one message is stalled for the sink |
| `sink_stalled_messages` | `sink` | Number of stalled messages |
| `otlp_rejected_total` | `signal` | Items rejected by an OTLP partial success |
| `recovery_scans_total` | `target`, `result` | `ok`, `error`, `rate_limited` |
| `recovery_redeliveries_total` | `target` | Redeliveries requested from GitHub |
| `recovery_pending` | `target` | GUIDs without a successful attempt |
| `recovery_degraded` | `target` | 1 while the target cannot be scanned |
| `recovery_last_scan_timestamp_seconds` | `target` | Unix time of the last completed listing |

### Example Prometheus alert rules

```yaml
groups:
  - name: antwatcher
    rules:
      - alert: AntwatcherSinkLagGrowing
        expr: |
          antwatcher_bus_consumer_lag > 100
          and deriv(antwatcher_bus_consumer_lag[15m]) > 0
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.consumer }} backlog is growing ({{ $value }} messages)"
          description: "The destination is slower than the inflow or is failing. Events stay on the bus for the retention window."

      - alert: AntwatcherSinkStalled
        expr: antwatcher_sink_stalled == 1
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: "sink {{ $labels.sink }} has stalled messages"
          description: "A permanent error (auth, schema, invalid argument, undecodable message) blocks specific messages. See /status for the UUIDs and reasons; other sinks continue."

      - alert: AntwatcherSinkNoRecentSuccess
        expr: |
          (time() - antwatcher_sink_last_success_timestamp_seconds > 3600)
          and on (sink)
          label_replace(antwatcher_bus_consumer_lag > 0, "sink", "$1", "consumer", "sink-(.*)")
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: "sink {{ $labels.sink }} has a backlog but no success for an hour"

      - alert: AntwatcherWebhookPublishFailures
        expr: |
          sum(rate(antwatcher_webhooks_total{result=~"publish_failed|publish_timeout"}[5m])) > 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "webhooks are being refused with 503"
          description: "The bus does not confirm publishes. GitHub records failed deliveries; enable recovery to repair them once the bus is back."

      - alert: AntwatcherBusDisconnected
        expr: antwatcher_bus_connected == 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "antwatcher lost its bus connection"

      - alert: AntwatcherRecoveryDegraded
        expr: antwatcher_recovery_degraded == 1
        for: 30m
        labels: {severity: warning}
        annotations:
          summary: "recovery target {{ $labels.target }} cannot be scanned"
          description: "Token, API, or hook discovery problem; see /status for the reason. Ingress is unaffected."
```

## Docker

`Dockerfile` builds a static binary in a multi-stage build and ships it in a distroless
image running as a non-root user. `/data` is a volume for the embedded NATS store and the
filesystem archive; the configuration is read from `/etc/antwatcher/antwatcher.yml` by
default.

The build stage runs on the build platform and cross-compiles for the target, so a
multi-platform build never falls back to emulation. It needs BuildKit — the default
builder in current Docker, and the deprecated legacy builder cannot expand
`$BUILDPLATFORM`.

```sh
docker build -t antwatcher .
# or, for both architectures at once:
# docker buildx build --platform linux/amd64,linux/arm64 -t antwatcher .
docker run --rm -p 8080:8080 -p 127.0.0.1:9090:9090 \
  -e GITHUB_WEBHOOK_SECRET=... \
  -v "$PWD/antwatcher.yml:/etc/antwatcher/antwatcher.yml:ro" \
  -v antwatcher-data:/data \
  antwatcher
```

Set `store_dir` and archive `dir` under `/data` and bind the admin listener to
`0.0.0.0:9090` inside the container; publish it to the host loopback only, as above.

`docker-compose.example.yml` runs antwatcher with embedded NATS next to an OpenTelemetry
collector, Grafana Tempo, Loki, and Grafana with both data sources provisioned. Traces and
logs flow antwatcher → collector → Tempo and Loki; the supporting configuration lives in
`deploy/compose/`.

```sh
export GITHUB_WEBHOOK_SECRET=...
docker compose -f docker-compose.example.yml up --build
# webhook  http://localhost:8080/webhook
# grafana  http://localhost:3000   (anonymous admin, Tempo and Loki data sources)
# status   http://127.0.0.1:9090/status
```

## Development

```sh
make build      # ./bin/antwatcher
make test       # go test -race -cover ./...
make lint       # golangci-lint, installed into ./bin on first use
make readme     # regenerate the configuration reference above from antwatcher.example.yml
make check CONFIG=antwatcher.example.yml
```

The test suite needs no network and no external services: unit tests use the in-process
`gochannel` bus, integration tests start an embedded JetStream server in a temporary
directory, and the end-to-end test drives a signed webhook through the receiver, the bus,
and one capturing sink of every class. Architecture rules (no driver imports outside
`cmd`, no Actions REST API, no handler-side retry or sleep, error classification in every
driver) are enforced by tests in `internal/archtest`.

`CLAUDE.md` describes the layout and the conventions for adding a bus or sink driver.

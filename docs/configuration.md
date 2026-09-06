# Configuration reference

Every configuration field of antwatcher, the driver matrices behind the `class` and
`driver` fields, the webhook GitHub has to be pointed at, and the token a recovery
target needs. The [README](../README.md) is the introduction; this file is the
reference. For which options to pick in a given deployment and how to verify the
result, see [agent-operations](agent-operations.md); for endpoints, metrics and
sizing, see [operations](operations.md).

## Every field

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
    # External server URL when embedded is false, e.g. nats://nats:4222, or a
    # comma-separated list for a cluster. Prefer credentials below over
    # userinfo in the URL; a userinfo that is present is masked in -check,
    # /status and the logs, but the credentials field is the intended place.
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
      tls:                      # used when insecure is false
        ca_file: ""             # PEM bundle replacing the system roots
        cert_file: ""           # client certificate for mTLS, with key_file
        key_file: ""
        server_name: ""         # overrides the verified certificate name
      retry:                    # short in-client retry; the bus retries after
        attempts: 2             # retries after the first attempt, 0 disables
        backoff: 500ms          # doubles per retry, server delay wins

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
      credentials_file: ""      # service account JSON key; empty uses ADC
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
        # A stream belongs to one subject. Sharing a broker with the ingress
        # bus needs a stream name of its own, or ensuring the stream fails.
        stream: ANTWATCHER_DOWNSTREAM
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

## Command line

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
| `forward` | The raw event re-published under a new transport UUID, with hop markers | `bus` | Any registered bus driver and topic, publisher only | Refuses to publish back into the ingress topic; `max_hops` bounds chains; needs a destination of its own, see below |

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

A forward sink configures a second bus, and its block inherits the driver's defaults
when a key is omitted -- `embedded: true`, `store_dir: ./data/nats`, `stream: ANTWATCHER`.
Two collisions are refused rather than papered over, both at first use:

- **One stream, one subject.** A stream's subject list is its identity, so a bus whose
  `stream` already carries another topic is an error, not an update. A forward target on
  the ingress broker needs its own `stream`; rewriting the subject would take it away from
  whoever publishes into it and leave the ingress bus publishing into nothing.
- **One embedded server per `store_dir`.** nats-server takes no lock on its store
  directory, and two servers over one JetStream file store corrupt each other's stream and
  consumer state -- losing events a webhook 2xx already promised. Starting a second
  embedded server on a directory already in use in the process fails.

Adding a bus driver means implementing the `bus.Bus` interface, declaring its
capabilities, and passing `bustest.Run`. See [ADR 0002](adr/0002-bus-abstraction.md)
and `AGENTS.md`.

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


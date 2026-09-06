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
   (see [GitHub webhook setup](docs/configuration.md#github-webhook-setup)) and every
   `workflow_run` and `workflow_job` event appears as a JSON line on stdout and in
   `./data/archive`.

5. Look at the service:

   ```sh
   curl -s http://127.0.0.1:9090/status | jq .
   curl -s http://127.0.0.1:9090/metrics | grep antwatcher_
   ```

The container image does the same with `docker compose -f docker-compose.example.yml up`,
adding Grafana Tempo and Loki behind an OpenTelemetry collector. See [Docker](#docker).

## Configuration

antwatcher reads one YAML file: a `server` block, one `bus`, any number of `sinks`, and
an optional `recovery` block. `serve -config antwatcher.yml -check` validates it and
prints the effective configuration with secrets masked.

Every field with its default, the driver matrices, the GitHub webhook setup and the token
a recovery target needs are in the [configuration reference](docs/configuration.md).

Where a sink can send events:

| Class | What it emits | Drivers | Destination |
|---|---|---|---|
| `trace` | One span tree per workflow run | `otlp` | Any OTLP trace receiver: Tempo, Jaeger, an OpenTelemetry collector |
| `log` | One record per delivery, correlated with the trace | `stdout`, `otlp` | Process stdout, or any OTLP log receiver such as Loki behind a collector |
| `analytics` | Sparse run, job and step rows | `bigquery` | A BigQuery table through the Storage Write API, plus a `_current` view |
| `archive` | The raw envelope and payload, one JSON line each | `filesystem` | A dated directory tree, fsynced before ack |
| `forward` | The raw event, re-published with hop markers | `bus` | Another bus topic, for a downstream consumer |

The bus underneath is `nats-jetstream`, embedded in the process or external, for anything
that has to survive a restart; `gochannel` is in-process and non-durable, for development
and tests. What each bus guarantees, and what happens when it cannot, is the capability
matrix in the [configuration reference](docs/configuration.md#bus-drivers-and-capabilities).

## Operations

The admin listener serves `/metrics`, `/status`, `/healthz` and `/readyz`. Those
endpoints, every `antwatcher_` metric, example Prometheus alert rules, retention and
sizing, and the single-replica notes are in [operations](docs/operations.md).

Installing, configuring and debugging an instance in someone else's project, by
observable signal, is [agent-operations](docs/agent-operations.md).

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

The compose build passes `VERSION`, `COMMIT` and `DATE` through as build arguments,
taken from the environment and falling back to `dev`. Export them to stamp the image
the way `make build` stamps a local binary:

```sh
export VERSION=$(git describe --tags --always --dirty) \
       COMMIT=$(git rev-parse --short HEAD) \
       DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

## Development

```sh
make build      # ./bin/antwatcher
make test       # go test -race -cover ./...
make lint       # golangci-lint, installed into ./bin on first use
make readme     # regenerate docs/configuration.md from antwatcher.example.yml
make check CONFIG=antwatcher.example.yml
```

The test suite needs no network and no external services: unit tests use the in-process
`gochannel` bus, integration tests start an embedded JetStream server in a temporary
directory, and the end-to-end test drives a signed webhook through the receiver, the bus,
and one capturing sink of every class. Architecture rules (no driver imports outside
`cmd`, no Actions REST API, no handler-side retry or sleep, error classification in every
driver) are enforced by tests in `internal/archtest`.

`AGENTS.md` describes the layout and the conventions for adding a bus or sink driver.

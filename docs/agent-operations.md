# Running antwatcher — an operator's guide for agents

For an agent that installs, configures and debugs antwatcher **in someone else's
project**. The contributor's view — building, testing, adding drivers — is
`AGENTS.md` at the repository root; this file assumes you are not changing
antwatcher's code, you are making it work somewhere.

The complete field-by-field configuration reference and the metric and alert
tables are in the [README](../README.md). This file is the part that is not a
reference: what to choose, what to verify, and how to read the signals when it
misbehaves.

## What you are deploying

One binary, one configuration file, two listeners:

| Listener | Default | Exposure |
|---|---|---|
| `server.listen` | `:8080` | Public, behind HTTPS. Serves `POST <webhook_path>` and `GET /healthz`, nothing else. |
| `admin.listen` | `127.0.0.1:9090` | Private. Serves `/metrics`, `/status`, `/healthz`, `/readyz`. |

Validation refuses a configuration where the two addresses are equal, because
admin endpoints are never served on the webhook listener.

**antwatcher terminates no TLS.** There is no `tls` block under `server`; the
process speaks plain HTTP. Put a reverse proxy, ingress, or load balancer in
front, and make sure it:

- forwards the **raw request body unchanged** — the HMAC covers the exact bytes,
  so a proxy that re-encodes, pretty-prints, or strips whitespace from JSON turns
  every delivery into a `401`;
- allows bodies up to 25 MiB (`server.max_body_bytes`, GitHub's payload limit);
- does not add its own timeout below `server.publish_timeout` (default `8s`);
- does not expose `admin.listen` to the internet.

Plan for **one replica**. Embedded NATS stores the stream on the local disk of
that one process, and recovery assumes it is the only scanner of a hook. See
"Single-replica notes" in the README before scaling out.

## Choosing a bus

`bus.driver` decides what a `2xx` to GitHub means.

| Driver | Use it when | Consequence |
|---|---|---|
| `nats-jetstream` | Anything you care about. `embedded: true` needs a persistent volume at `store_dir`; `embedded: false` points at an existing cluster with `url`. | Publish is acknowledged only after a JetStream `PubAck`, so an accepted delivery is stored. Durable consumers, history replay, lag reporting. |
| `gochannel` | Development, demos, tests. No configuration block at all. | Non-durable, in-process. Events accepted from GitHub are lost on restart. |

`bus.on_missing_capability` decides what happens when the driver cannot do what
the configuration asks:

- `fail` (the default and the right answer in production) refuses to start.
- `degrade` starts and records a warning in `/status` — for example
  `"2xx does not mean durable: events accepted from GitHub can be lost on
  restart, dev only"` for `gochannel`, or a sink whose `start_from: earliest` was
  downgraded to `now` because the bus keeps no history.

`bus.nats-jetstream.retention` (default `168h`) is your **outage budget**: a sink
that stays down longer than the retention window loses the oldest part of its
backlog. Size it from the longest destination outage you want to survive plus
the time it takes someone to notice.

## Choosing sinks

Each entry in `sinks` is an independent consumer named `sink-<name>` with its own
position on the bus. Adding one never affects the others; a sink that is down
does not slow ingress and does not affect `/readyz`.

| Goal | Class | Driver | Needs |
|---|---|---|---|
| Trace waterfall per workflow run | `trace` | `otlp` | An OTLP trace endpoint: Tempo, Jaeger, or a collector |
| Log records correlated with the traces | `log` | `otlp` | An OTLP log endpoint (Loki through a collector) |
| Same, but into the container log stream | `log` | `stdout` | Nothing |
| Queryable rows per run/job/step | `analytics` | `bigquery` | A GCP project and an existing dataset; `credentials_file`, or Application Default Credentials when it is empty |
| Raw payloads kept on disk | `archive` | `filesystem` | A writable directory, ideally a volume |
| Re-publish everything to another broker | `forward` | `bus` | A second bus; it refuses to publish back into the ingress topic |

`start_from` is **required and has no default**, because the two answers differ by
a potentially large replay:

- `earliest` — replay everything the bus still retains on the sink's first start.
  Right for `trace`, `analytics` and `archive`, which are idempotent through
  deterministic IDs.
- `now` — begin with the next event. Right for `log` and `forward`, where a
  replay produces a burst of duplicated noise downstream.

Only the **first** start of a given consumer honours `start_from`; afterwards the
position is durable. Renaming a sink therefore creates a *new* consumer that
starts from `start_from` again, and leaves the old one behind (it pins no data,
so it costs no disk).

## Writing the configuration

Values may reference environment variables as `${VAR}` or `${VAR:-default}`. A
reference to an undefined variable **without** a default fails loading; there is
no escape for a literal `${`. Keep every secret in the environment, never in the
file.

A working minimum — everything not mentioned takes its default:

```yaml
server:
  webhook_secret: ${GITHUB_WEBHOOK_SECRET}
  public_url: https://antwatcher.example.com/webhook

bus:
  driver: nats-jetstream
  nats-jetstream:
    embedded: true
    store_dir: /var/lib/antwatcher/nats

sinks:
  - name: tempo
    class: trace
    driver: otlp
    start_from: earliest
    config:
      endpoint: "tempo:4317"
      protocol: grpc
      insecure: true
```

Start from `antwatcher.example.yml`, which shows every field with its default,
and delete what you do not need.

## Validate before you start it

```sh
antwatcher serve -config /etc/antwatcher/antwatcher.yml -check
```

`-check` loads the file, expands the environment, decodes every driver block
strictly, validates, prints the effective configuration with secrets masked, and
exits. Exit codes: `0` valid, `1` invalid configuration or runtime failure, `2`
usage error.

It is a real check, not a smoke test — driver blocks are decoded by the drivers
themselves, so a typo inside `sinks[].config` is caught here:

```
configuration is invalid:
sinks[0] "tempo": decode: yaml: unmarshal errors:
  line 2: field protocoll not found in type otlp.Config
```

and so is a missing secret:

```
configuration is invalid:
antwatcher.yml: undefined environment variables: GITHUB_WEBHOOK_SECRET
```

What `-check` cannot tell you: whether the destination is reachable. No
constructor dials the network — that is deliberate, so an unreachable backend
starts degraded instead of blocking startup — so an endpoint typo surfaces later
as sink errors, not as a validation failure.

## Setting up the webhook

1. Publish the receiver over HTTPS and set `server.public_url` to that URL.
2. Repository or organization → Settings → Webhooks → Add webhook:
   - **Payload URL**: `server.public_url`
   - **Content type**: `application/json`
   - **Secret**: the same value as `server.webhook_secret`
   - **Events**: "Let me select individual events" → **Workflow runs** and
     **Workflow jobs**
3. GitHub immediately sends a `ping`. A healthy receiver answers `200` without
   publishing it, and `antwatcher_webhooks_total{result="ping"}` increments.
4. Trigger a workflow and watch `antwatcher_webhooks_total{result="published"}`.

Other event types are accepted, published, logged, and archived, but produce no
spans and no analytics rows — the trace and analytics classes only understand
`workflow_run` and `workflow_job`.

If you also want antwatcher to repair its own downtime, enable `recovery` and
give it a token with `admin:repo_hook` (repository targets) or `admin:org_hook`
(organization targets). Read access alone lets the scans succeed and every
redelivery fail. GitHub keeps deliveries for 3 days; older ones are gone.

## What a healthy start looks like

```sh
curl -s localhost:9090/readyz            # {"ready":true}
curl -s localhost:9090/status | jq .     # the document below
curl -s localhost:9090/metrics | grep antwatcher_webhooks_total
```

In `/status`, check in this order:

- `ready` is `true` and there is no `not_ready_reason`;
- `bus.connected` is `true` and `bus.warnings` is absent — a warning here means
  `on_missing_capability: degrade` silently accepted something you may not want;
- every sink's `effective_start_from` equals its `requested_start_from` (if not,
  the bus could not replay history and the sink was downgraded to `now`);
- no sink has a `stalled` array;
- `version` and `commit` are the build you think you deployed — a `dev` here
  means the image was built without the build arguments.

`/status` is rendered from typed values and the same redaction `-check` uses, so
it never contains a secret. It is still an admin endpoint: do not expose it.

## Debugging by signal

### The webhook response code is the first fact

| Code | Metric result | What it means | What to do |
|---|---|---|---|
| `200` | `published` | The bus confirmed the publish. GitHub is happy. | Nothing; if data is still missing, the problem is downstream of the bus. |
| `200` | `ping` | The webhook's `ping` event, never published. | Normal on creation and on "Redeliver" of the ping. |
| `401` | `rejected_signature` | The HMAC did not verify, or the header was missing. | Almost always a wrong secret, sometimes a proxy that rewrote the body. Not a bug in antwatcher. |
| `413` | `bad_request` | The body exceeded `max_body_bytes`. | Raise it, and check the proxy's own body limit. |
| `400` | `bad_request` | A malformed delivery: missing headers, undecodable payload. | Look at the log line, it carries the delivery GUID. |
| `503` | `publish_failed` / `publish_timeout` | The bus did not confirm within `publish_timeout`. **Nothing was accepted.** | Fix the bus. GitHub records these as failed deliveries; they are the only ones recovery can repair. |

`404` on the webhook path means `server.webhook_path` and the GitHub webhook URL
disagree; `405` means something sent `GET` where GitHub sends `POST`.

The receiver logs every rejection with the delivery GUID (and, for a `401`,
whether a signature header was present at all), so a rejection can be matched
with the entry in GitHub's "Recent Deliveries" tab.

### Ingress works but the destination is empty

Follow the pipeline in order — each step has its own signal.

1. **Did it arrive?** `antwatcher_webhooks_total{result="published"}` increasing.
   If not, the problem is between GitHub and the receiver; check GitHub's Recent
   Deliveries tab for the response it got.
2. **Did the sink see it?** `antwatcher_sink_events_total{sink="…"}` by `result`:
   - `ok` — handed to the destination successfully;
   - `skipped` — the projection deliberately produced nothing (the trace class
     emits spans only for *completed* runs and jobs, so `in_progress` events are
     skipped by design, not lost);
   - `error` — retryable; the message was nacked and the broker will redeliver
     it with a growing delay (`nak_delay_min` → `nak_delay_max`);
   - `permanent` — the driver classified the failure as one a retry cannot fix
     (auth, schema, invalid argument, malformed input).
3. **Is it falling behind?** `antwatcher_bus_consumer_lag{consumer="sink-<name>"}`
   is the unacked backlog. Growing lag with `result="error"` climbing means the
   destination is failing; growing lag with `ok` climbing means it is merely
   slower than the inflow.
4. **Is it stuck?** `antwatcher_sink_stalled{sink="…"} == 1` means at least one
   message fails permanently. `/status` lists the message UUIDs, the first and
   last occurrence, the attempt count, and the reason text. The message stays on
   the bus and is retried at the broker's pace forever; other sinks are
   unaffected. Fix the cause (credentials, schema, endpoint) and it clears
   itself.
5. **When did it last work?** `antwatcher_sink_last_success_timestamp_seconds`.
   A stale timestamp *with* a non-zero lag is the honest "this sink is broken"
   signal; a stale timestamp with zero lag just means no events arrived.

### Readiness and liveness

`/readyz` depends on the **bus only**: it is `200` while the bus is open and
connected and no shutdown has started. A failing sink never makes the process
unready — that is intentional, so a broken destination cannot stop antwatcher
from accepting webhooks. Use `/healthz` for liveness and `/readyz` for traffic
gating; wiring a sink's health into readiness would defeat the design.

### Nothing retries anywhere else

Handlers never sleep and never retry: a failed `Process` is nacked and the broker
redelivers. If you are looking for a retry knob in antwatcher, there isn't one
beyond the short in-driver retries (`retry.attempts`, `retry.backoff` on the OTLP
drivers) and the broker's `nak_delay_*`.

## Failure modes seen in practice

**A signature mismatch is a wrong secret, not a bug.** `401` with
`rejected_signature` means the HMAC over the exact bytes did not match. In order
of likelihood: the secret in GitHub differs from `server.webhook_secret` (a
trailing newline in a Kubernetes Secret is the classic); the environment variable
was not exported into the process; a proxy modified the body. Verify by reading
`/status` for the deployed configuration and re-entering the secret on both
sides. Do not go looking in the code.

**Events reach the log backend but the query shows nothing.** Log records and
spans are timestamped with the **event's** time — the transition GitHub reported —
not with the time antwatcher processed them. A sink starting with
`start_from: earliest` replays a week of retained history in a few seconds, and
every record it writes lands at its original timestamp. Grafana's default "last
15 minutes" window then shows an empty panel while the data is very much there.
Widen the query window to the retention period before concluding anything is
lost.

**`service.version` reads `dev` in the exported telemetry.** The image was built
without the `VERSION`/`COMMIT`/`DATE` build arguments. `/status` shows the same
value; rebuild passing them (`docker compose` does this from the environment).

**A renamed sink replays everything.** The consumer name is derived from the sink
name, so a rename creates a fresh consumer that honours `start_from` again. If
that is not what you wanted, rename it back.

**The first start after a long stop looks like a flood.** Unacked messages are
redelivered after `ack_wait` and the sinks work through the backlog. Lag falling
steadily is the healthy shape; watch `sink_events_total{result="ok"}` climb with
it.

**A sink is missing everything before its creation, and editing `start_from`
changes nothing.** The start position applies when the consumer is created and
never again: an existing consumer keeps the deliver policy it was born with, and
the log says so ("consumer exists: keeping its original start position"). To
replay history into a sink that was created with `now`, give it a new name — that
creates a new consumer, which starts from `earliest` — or delete the consumer on
the broker.

## Reading further

- [README](../README.md) — configuration reference, metric and endpoint tables,
  example Prometheus alert rules, sizing, single-replica notes
- [ADR 0001](adr/0001-architecture.md) — why the pipeline is shaped this way
- [ADR 0002](adr/0002-bus-abstraction.md) — the bus contract and capabilities
- [ADR 0003](adr/0003-model-and-classes.md) — the model and the five classes
- [ADR 0004](adr/0004-errors-and-retries.md) — permanent versus retryable
- `AGENTS.md` — for changing antwatcher itself

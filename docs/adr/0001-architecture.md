# ADR 0001: Architecture

Status: accepted (2026-09)

## Context

We want traces, logs, analytics rows, archives, and forwarded copies of GitHub Actions
activity, and we want them to survive destination outages, process restarts, and short
receiver outages. Three sources of run data exist: webhooks, the Actions REST API, and the
webhook deliveries API. Each destination has its own failure modes, and destinations are
added over time.

Webhooks are pushed once per state change, carry the complete object, and are signed. The
Actions REST API is rate limited, lags behind the webhook, paginates, and reconstructs
history from a mutable view that forgets old runs. The deliveries API lists every delivery
attempt of a webhook for 3 days, with its response code, and can trigger a redelivery.

## Decision

**Webhooks are the source of truth.** Every `workflow_run` and `workflow_job` delivery is
the event. Nothing is fetched from the Actions REST API, not to fill gaps, not to enrich,
not to reconcile. An architecture test (`internal/archtest`) fails the build if any
`Actions` selector of the GitHub client appears anywhere in the module.

**Publish before acknowledge.** The receiver verifies the HMAC, builds an envelope, and
publishes the raw delivery on the bus. It writes `200` only after `Publish` returned
without error on a bus that declares `DurablePublish`. Any publish error or a timeout of
`publish_timeout` yields `503`, which GitHub records as a failed delivery. There is no
buffer in the receiver, no in-memory queue, no background flush: what GitHub saw
acknowledged is on disk.

**The raw envelope is the message.** The bus message is the webhook: Watermill UUID equals
the GitHub delivery GUID, metadata equals the envelope (`delivery_guid`, `event`,
`action`, `hook_id`, `received_at`, `repository_id`, `repository`, `schema_version`, plus
forward markers), payload equals the GitHub JSON byte for byte. Projections are computed
by each consumer, not once at ingress. The retained stream is therefore the raw history for
the retention window, and a new destination class or a fixed projection can replay it.

**At-least-once plus idempotent sinks.** Every sink is an independent durable consumer that
receives every message at least once. Deduplication is the sinks' job through
deterministic identifiers (trace, span, and record IDs derived from GitHub IDs), assisted
but never guaranteed by the broker's dedup window. The analytics base table stays
at-least-once and is read through a deduplicating view.

**Recovery through the deliveries API.** Missed ingress (receiver down, `503`s, timeouts)
is repaired by an optional loop that lists deliveries per hook, groups the attempts by
GUID, and asks GitHub to redeliver every GUID without a `2xx`. The redelivery re-enters the
receiver like any other webhook. Recovery is a repair path only: it never publishes to the
bus directly and it never fails startup; a broken token degrades recovery and is visible
in metrics and `/status`.

**Bounded by retention, stated honestly.** A sink's backlog lives on the bus for the
configured retention. A sink down longer than that loses the oldest part of its backlog.
History older than retention is not recoverable, and ingress missed longer than GitHub's
3-day delivery window is lost. These limits are documented in the README, shown in
`/status`, and not papered over by any secondary store.

**Single replica by default.** Embedded NATS and the recovery loop assume one process. A
multi-replica deployment uses an external NATS and runs recovery on one instance.

## Consequences

- The event model is sparse: a job event has no workflow path, trigger, or actor; a run
  event has no runner or steps. Consumers join run and job rows on repository, run ID, and
  attempt when they need both. This is the price of not calling the Actions API, and it is
  paid deliberately.
- Every projection is computed per sink. CPU cost scales with the number of sinks; it is
  negligible next to the network calls the sinks make.
- Ordering is never relied upon. GitHub delivers out of order, brokers redeliver out of
  order, and every projection carries enough state (`status_rank`, timestamps) to make
  order irrelevant.
- Recovery needs a token with webhook administration rights, and only repairs what GitHub
  still remembers. It is a repair mechanism for outages measured in hours, not a
  synchronization protocol.
- Storage is bounded and predictable: one copy of every delivery for the retention window,
  independent of the number of sinks.

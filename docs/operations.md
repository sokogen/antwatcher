# Operations

What antwatcher exposes once it runs — endpoints, metrics, alert rules — and the
numbers to size it by. The configuration fields these sections refer to are in the
[configuration reference](configuration.md); the procedure for deploying and
debugging an instance is in [agent-operations](agent-operations.md).

## Endpoints

| Listener | Path | Purpose |
|---|---|---|
| `server.listen` | `POST <webhook_path>` | GitHub deliveries |
| `server.listen` | `GET /healthz` | Liveness, always `200` while the process runs |
| `admin.listen` | `GET /metrics` | Prometheus exposition |
| `admin.listen` | `GET /status` | JSON: version, bus driver with capabilities and policy warnings, every sink with requested and effective `start_from`, last success, and stalled UUIDs, recovery targets with hook IDs and degraded reasons, uptime. Never contains secrets. |
| `admin.listen` | `GET /healthz` | Liveness |
| `admin.listen` | `GET /readyz` | `200` while the bus is open and connected and no shutdown has started. Sinks never affect readiness. |

## Metrics

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

## Example Prometheus alert rules

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


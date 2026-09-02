# Antwatcher core pipeline: webhook → bus → canonical model → destination classes

## Overview

Antwatcher turns GitHub Actions webhooks into traces, logs, analytics records, archives,
and forwarded events without losing events across destination outages, restarts, or short
receiver outages.

Architecture slogan: **Webhooks are the source of truth. The bus provides durability and
replay. GitHub Delivery API repairs missed ingress. Destinations consume independently.**

Key properties this plan implements:
- Webhook receiver answers GitHub `2xx` only after `Bus.Publish` returned success on a bus
  that declares `DurablePublish`. Non-durable buses are dev-only and say so in `/status`.
- Every message carries envelope metadata plus the raw GitHub payload; the retained log of
  the bus is the raw history for its retention window.
- **Bus abstraction**: one contract over any Watermill Pub/Sub, driver chosen by config.
  Broker differences are explicit `Capabilities`, resolved by one documented policy.
  Receiver, router, and sinks never branch on the driver.
- **Canonical model**: `model.Normalize(envelope)` yields Run / Job / Step entities with
  deterministic trace/span/record IDs. Classes project the model; GitHub payload semantics
  live in one package.
- **Destination classes** own the projection and a small driver contract. Each configured
  sink is an independent named consumer. At-least-once is the contract; sinks are
  idempotent through deterministic IDs.
- **Errors**: drivers classify errors as retryable or permanent. Retryable → short bounded
  retry in the driver, then Nack → broker redelivery with growing delay. Permanent (including
  an undecodable bus message) → the message is recorded as `stalled` for that sink (by UUID),
  Nacked, and re-attempted at the broker's pace; the sink reports stalled while any such
  message remains; other sinks continue. Antwatcher never discards an event because of a
  processing failure; the only losses are protocol-level terminal rejections (OTLP partial
  success), which are surfaced explicitly. No DLQ.
- **Bounded by retention**: a sink's backlog is kept and redelivered while the event is
  inside the configured bus retention; a sink down longer than retention loses the oldest
  part of its backlog. This is the contract, stated honestly in docs and `/status`.
- Optional recovery through the GitHub webhook deliveries API (group attempts by GUID,
  redeliver GUIDs with no successful attempt; full 72h scan at startup, incremental scans
  afterwards). Recovery re-enters the normal receiver and never blocks ingress: GitHub API
  problems degrade recovery, they do not fail startup.
- The Actions REST API is **never** used to reconstruct runs/jobs/steps.
- The service exposes Prometheus metrics for its own health on an admin listener: ingress,
  publish latency, consumer lag per sink, failures, stalled sinks, last success, recovery.

## Context (from discovery)

- Repository is empty (no commits, no files). Everything below is greenfield.
- Local toolchain: Go 1.27.1, docker, ralphex. `golangci-lint` and `nats-server` are not
  installed; tests use the embedded NATS server, lint is installed via a Makefile target.
- Dependencies (pin latest stable at implementation time):
  - `github.com/ThreeDotsLabs/watermill` (`message.Router`, `middleware.Recoverer`,
    `pubsub/gochannel`, `components/metrics`)
  - `github.com/nats-io/nats.go` (`jetstream` package: stream provisioning, **context-aware
    `PublishMsg(ctx, ...)` used directly for `Bus.Publish`**, consumer info for lag) and
    `github.com/nats-io/nats-server/v2` (embedded server)
  - `github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream` **subscriber only**
    (`SubscriberConfig{Conn, AckWaitTimeout, ResourceInitializer, NakDelay, ConfigureConsumer,
    Unmarshaler}`, `GroupedConsumer`). The package is documented as beta; its publisher uses
    `context.Background()` and is not used. If the subscriber fails the conformance suite,
    the fallback is a small in-house `message.Subscriber` over a nats.go pull consumer —
    either way the instability stays inside `internal/bus/natsjs`
  - `go.opentelemetry.io/proto/otlp` (generated OTLP protobuf + gRPC service stubs),
    `google.golang.org/grpc`, `google.golang.org/protobuf`
  - `cloud.google.com/go/bigquery` (+ `storage/managedwriter`, `storage/managedwriter/adapt`)
  - `github.com/google/go-github` (latest supported major at implementation time)
  - `gopkg.in/yaml.v3`, `github.com/stretchr/testify`, `github.com/prometheus/client_golang`
- Watermill fit: one message = one webhook delivery; `message.UUID` = delivery GUID on the
  ingress bus (also the broker dedup key where supported); envelope fields in metadata
  (including `delivery_guid` explicitly, so the GUID survives re-publishing under a new
  transport UUID); payload = raw GitHub JSON. Each sink instance = one Router handler bound
  to its own Subscriber. Handler error → Nack → broker redelivery.
- Layout:
  ```
  cmd/antwatcher/              main: serve (-config, -check), version
  internal/config/             YAML, ${ENV} on scalar nodes, Secret type, defaults, validation, redaction
  internal/event/              Envelope ⇄ watermill message, fixtures
  internal/model/              canonical Run/Job/Step, Normalize, deterministic IDs
  internal/metrics/            Prometheus registry, build info, lag poller
  internal/admin/              admin HTTP: /metrics, /status, /healthz, /readyz
  internal/bus/                Bus contract, Capabilities, policy, driver registry
  internal/bus/bustest/        conformance suite every driver must pass
  internal/bus/gochannel/      dev/test driver (non-durable)
  internal/bus/natsjs/         JetStream driver: embedded server, stream, pub/sub, lag
  internal/receiver/           webhook HTTP handler, HMAC, publish timeout
  internal/sink/               Sink interface, classes, errors, driver registry, config
                               model, router assembly, stalled handling, metrics
  internal/sink/trace/         projection (model → Span) + drivers/otlp
  internal/sink/log/           projection (model → LogRecord) + drivers/otlp, drivers/stdout
  internal/sink/analytics/     projection (model → Record), Schema, view + drivers/bigquery
  internal/sink/archive/       JSONL append/rotate + drivers/filesystem
  internal/sink/forward/       re-publish + drivers/bus
  internal/otlp/               OTLP client: config, TLS/auth, gRPC + HTTP, retry, classification
  internal/ghclient/           go-github wrapper for hook deliveries
  internal/recovery/           scan/group/redeliver loop
  docs/adr/                    0001 architecture, 0002 bus, 0003 model & classes, 0004 errors & retries
  ```

## Development Approach
- **Testing approach**: Regular (code first, then tests in the same task)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change (`make test`)
- Core packages (receiver, sink root, recovery, serve) depend only on `internal/bus`,
  `internal/sink`, `internal/model` interfaces, never on a concrete driver package
- Unit tests use the `gochannel` bus driver; integration tests use the embedded NATS server
  in a temp dir; nothing in the suite requires network, Tempo, BigQuery, or GitHub

## Testing Strategy
- **Unit tests**: required for every task (see Development Approach above)
- **Conformance tests**: `internal/bus/bustest` runs against every bus driver with the
  driver's declared `Capabilities`; a driver that declares a capability must prove it
- **Integration tests**: `internal/bus/natsjs`, `internal/sink` router, and the final
  `serve` test run against an embedded JetStream server
- **E2E tests**: Task 18 drives an `httptest` receiver through the embedded bus and the
  router into capturing sinks of every class
- External systems (real Tempo/Loki, BigQuery, GitHub) are verified manually in
  Post-Completion, not in the automated suite

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope
- Keep plan in sync with actual work done

## What Goes Where
- **Implementation Steps** (`[ ]` checkboxes): tasks achievable within this codebase - code changes, tests, documentation updates
- **Post-Completion** (no checkboxes): items requiring external action - manual testing, changes in consuming projects, deployment configs, third-party verifications
- **Checkbox placement**: Checkboxes belong only in Task sections. Do not put checkboxes in Success criteria, Overview, or Context — they cause extra loop iterations.

## Implementation Steps

### Task 1: Project skeleton, config core, build tooling
- [x] `go mod init github.com/sokogen/antwatcher`; `.gitignore` (binaries, `data/`, `coverage.out`), `.golangci.yml` (govet, staticcheck, errcheck, revive, gocritic, gosec, testifylint), `Makefile` with `build`, `test` (`go test -race -cover ./...`), `lint` (installs golangci-lint into `./bin` if missing), `run`
- [x] `cmd/antwatcher/main.go`: `serve` subcommand with `-config` (default `antwatcher.yml`) and `-check` (load, validate, print redacted config, exit), `version` subcommand, `slog` JSON/text logging with `-log-level`; `serve` only loads config and logs it for now
- [x] `internal/config/secret.go`: `type Secret string` with `UnmarshalYAML`, `String()/MarshalYAML()/MarshalJSON()` → `"***"` (empty stays empty), `Reveal() string`; every credential field in core and driver configs uses it
- [x] `internal/config/load.go`: `Load(path)` parses YAML into `yaml.Node` first, then walks **scalar string nodes** expanding `${VAR}` / `${VAR:-default}` (undefined without default → error listing the variable), then decodes into `Config{Server, Admin, Bus, Router, Sinks []SinkConfig, Recovery}`; driver blocks (`Bus.Drivers map[string]yaml.Node`, `SinkConfig.Config yaml.Node`) stay opaque and are decoded by drivers with `KnownFields(true)`
- [x] `internal/config/redact.go`: `Redacted(cfg, describers)` renders core config via typed structs (Secret masks itself) and driver blocks through a `Describer` (a driver that decodes its block into its typed config and returns it, so `Secret` fields mask); fallback for undescribed nodes masks values under keys matching `(?i)(token|secret|password|passwd|authorization|api[-_]?key|credential)`
- [x] `Validate()`: webhook secret required; sink names unique and `[a-z0-9-]`; class/driver non-empty; `start_from` **required** for every sink and ∈ {earliest, now} (no default: a missing value must not silently choose between a huge replay and no history); `bus.on_missing_capability` ∈ {fail, degrade}; `router.process_timeout` > 0; admin and server listeners differ; `recovery.auth.type` ∈ {token} (schema reserves `github_app`); recovery enabled requires auth and ≥1 target; drivers get `ValidateWith(router config)` so a broker driver can enforce `ack_wait > process_timeout + margin`
- [x] `antwatcher.example.yml` with every field documented inline, one sink per class
- [x] write tests for `Load` (defaults, env expansion incl. a value containing `: #` that must not change structure, undefined var error, missing file, invalid YAML, opaque blocks preserved), `Secret` (yaml/json/String masking, Reveal), `Redacted` (typed describer masks, fallback masks by key, non-secret values visible), `Validate` (each rule)
- [x] run tests - must pass before next task

### Task 2: Event envelope and Watermill message mapping
- [x] `internal/event/envelope.go`: `Envelope{SchemaVersion, DeliveryGUID, Event, Action, HookID, ReceivedAt, RepositoryID, Repository, Payload json.RawMessage}`; `FromWebhook(hdr http.Header, body []byte, now time.Time) (Envelope, error)` extracting `X-GitHub-Delivery`, `X-GitHub-Event`, `X-GitHub-Hook-ID`, and `action`/`repository.id`/`repository.full_name` (missing repository allowed, e.g. org-level `ping`)
- [x] `internal/event/message.go`: `ToMessage(env) *message.Message` (UUID = DeliveryGUID; metadata `delivery_guid`, `schema_version`, `event`, `action`, `hook_id`, `received_at` RFC3339Nano, `repository_id`, `repository`; payload = raw JSON) and `FromMessage(*message.Message) (Envelope, error)` reading the GUID from metadata (falls back to UUID only when metadata is absent, for messages written by older versions); missing/invalid required metadata → error
- [x] `internal/event/testdata/` fixtures: `workflow_run.requested.json`, `workflow_run.in_progress.json`, `workflow_run.completed.json`, `workflow_job.queued.json`, `workflow_job.in_progress.json`, `workflow_job.completed.json` (≥3 steps incl. one skipped step without timestamps and one failed step), `ping.json`; helper `event.LoadFixture(t, name)`
- [x] write tests for `FromWebhook` (each fixture, missing GUID → error, invalid JSON → error, org ping without repository → ok) and `ToMessage`/`FromMessage` round-trip (payload bytes unchanged, GUID from metadata, legacy UUID fallback, metadata errors)
- [x] run tests - must pass before next task

### Task 3: Canonical execution model
- [x] `internal/model/model.go`: `Run{RepositoryID, Repository, WorkflowID, WorkflowName, WorkflowPath, RunID, RunNumber, RunAttempt, Status, Conclusion, CreatedAt, StartedAt, UpdatedAt, TriggerEvent, HeadBranch, HeadSHA, Actor, HTMLURL}`, `Job{RepositoryID, Repository, RunID, RunAttempt, JobID, Name, WorkflowName, Status, Conclusion, CreatedAt, StartedAt, CompletedAt, RunnerName, RunnerGroup, Labels, HeadBranch, HeadSHA, HTMLURL, Steps []Step}`, `Step{Number, Name, Status, Conclusion, StartedAt, CompletedAt}`; `Execution{Kind Kind (KindNone|KindRun|KindJob); Run *Run; Job *Job; Envelope event.Envelope}`; the model is **sparse**: optional fields are pointers/`sql.Null*`-style (e.g. `workflow_job` carries no `workflow_id`, `workflow_path`, or trigger actor), documented per field; run and job/step entities join on `repository_id + run_id + run_attempt`; no API enrichment ever
- [x] `internal/model/normalize.go`: `Normalize(env event.Envelope) (Execution, error)` — `workflow_run` → Run (all actions), `workflow_job` → Job with Steps (all actions); every other event → `KindNone` without error; a known event with a malformed payload → `ErrMalformed` (permanent)
- [x] `internal/model/ids.go`: `TraceID(repoID, runID, runAttempt) [16]byte`, `RunSpanID(runID, runAttempt) [8]byte`, `JobSpanID(jobID) [8]byte`, `StepSpanID(jobID, stepNumber) [8]byte`, `RecordID(kind, ids...) string` (sha256 hex over namespaced strings); methods `Run.TraceID()`, `Run.SpanID()`, `Job.TraceID()`, `Job.SpanID()`, `Job.ParentSpanID()`, `Step.SpanID(job)`; `EventTime(exec)` = the timestamp of the state transition the event reports, by explicit table — Run: completed → `updated_at`, in_progress → `run_started_at`, requested/other → `created_at`; Job: completed → `completed_at`, in_progress → `started_at`, queued/waiting/other → `created_at`; Step (inside a job event): completed → `completed_at`, in_progress → `started_at`, else the job's EventTime; missing field → `received_at`
- [x] `internal/model/status.go`: `StatusRank(status)` (completed=3, in_progress=2, queued/waiting/requested/pending=1, unknown=0) shared by analytics dedup and log severity
- [x] write tests: each fixture normalizes to the expected entities (golden structs), sparse fields nil on job fixtures, skipped step keeps nil timestamps, unknown event → KindNone, malformed → ErrMalformed, IDs deterministic/distinct/pinned golden values, EventTime table (every row incl. fallback) and StatusRank table
- [x] run tests - must pass before next task

### Task 4: Bus contract, capabilities, policy, registry, conformance suite, gochannel driver
- [x] `internal/bus/bus.go`: `Bus` interface — `Publish(ctx, *message.Message) error` (returns success only when the driver's durability guarantee holds; see `DurablePublish`), `Subscribe(ctx, consumer string, opts SubscribeOptions) (message.Subscriber, error)` (independent named consumer receiving every message, own position), `Capabilities() Capabilities`, `Close() error`; optional `LagReporter{Lag(ctx, consumer) (int64, error)}`; `SubscribeOptions{StartFrom StartPosition}`, `StartPosition` {Earliest, Now}
- [x] `internal/bus/capabilities.go`: `Capabilities{DurablePublish, DurableConsumers, FanOut, HistoricalReplay, Deduplicates, ReportsLag bool; Retention RetentionKind}` (`None|Time|Size|UntilAcked`); no `Ordered` — the architecture never relies on ordering (GitHub itself delivers out of order); `String()` for logs and `/status`
- [x] `internal/bus/policy.go`: `ResolveIngress(caps, mode) (warnings, err)` — `DurablePublish=false` → error in `fail`, warning "2xx does not mean durable, dev only" in `degrade`; `ResolveConsumer(caps, requested, mode) (effective, warnings, err)` — `FanOut=false` → always error; `Earliest` without `HistoricalReplay` → error in `fail`, downgrade to `Now` with warning in `degrade`; `DurableConsumers=false` → warning "positions lost on restart"; `Deduplicates=false` → informational
- [x] `internal/bus/registry.go`: `Register(name, Factory)`, `Open(ctx, cfg config.Bus, logger, metrics) (Bus, error)`, `Factory func(ctx, raw yaml.Node, topic string, logger, metrics) (Bus, error)`; unknown driver error lists registered drivers; drivers implement `config.Describer` for redaction
- [x] `internal/bus/bustest/suite.go`: `Run(t, open func(t) bus.Bus)` gated by declared capabilities: publish→subscribe delivers UUID/metadata/payload intact; Nack redelivers; two consumers each receive everything and progress independently (FanOut); re-opened consumer with the same name resumes after the last Ack (DurableConsumers); consumer created after publishing gets history with `Earliest` and not with `Now` (HistoricalReplay); duplicate UUID collapsed (Deduplicates); `Lag` counts unacked backlog and drops after Ack (ReportsLag); `Publish` returns an error when the broker is unreachable (DurablePublish, driver supplies a "break" hook); publish after `Close` errors; `Close` idempotent
- [x] `internal/bus/gochannel/driver.go`: registered as `gochannel`; `gochannel.NewGoChannel(Config{Persistent: true})`; `Now` emulated by dropping messages whose publish sequence precedes the subscribe call (wrapper subscriber); declares `FanOut, HistoricalReplay` true, `DurablePublish, DurableConsumers, Deduplicates, ReportsLag` false, `Retention: None`
- [x] write tests for both `Resolve*` functions (table: every capability × mode), registry (register/open/unknown/duplicate register panics), and `bustest.Run` against the gochannel driver
- [x] run tests - must pass before next task
- ➕ `Bus` also exposes `Topic() string` (read-only, from config) so the conformance suite, the router, and `/status` can name the topic without a side channel; the returned `message.Subscriber` is bound to that topic and wraps `ErrWrongTopic` for any other
- ➕ `Register(name, Driver{Factory, Describer})`: the Describer travels with the factory so `Describers()` feeds `config.Describers.Bus`; `Factory` takes `metrics prometheus.Registerer` (nil allowed) because `internal/metrics` will import `bus` for `LagReporter` and cannot be imported back; policy warnings are typed `Warning{Capability, Severity (warning|info), Message}` for `/status` JSON
- ➕ `bustest.Run(t, open, ...Option)`: `WithOutage(hook)` supplies the broker "break" for the DurablePublish check and is mandatory when the driver declares it; messages are tagged per subtest so a shared durable stream (Task 5) does not leak between subtests; the DurableConsumers check closes the first subscriber and bus before reopening the consumer on a second bus from `open`

### Task 5: NATS JetStream bus driver
- [x] `internal/bus/natsjs/embedded.go`: `StartEmbedded(cfg) (*server.Server, error)` in-process nats-server with JetStream and `store_dir`, no TCP listener by default (client via `nats.InProcessServer`); `natsjs.NewTestServer(t)` for tests (temp dir, cleanup, `Stop/Start` helpers to simulate an outage for the DurablePublish conformance check)
- [x] `internal/bus/natsjs/stream.go`: `EnsureStream(ctx, js, cfg)` creates/updates stream `cfg.Stream` with subjects `[topic]`, `MaxAge = retention`, `Duplicates = dedup_window`, file storage, **limits** retention (so orphaned consumers of renamed sinks never pin data); idempotent
- [x] `internal/bus/natsjs/driver.go`: registered as `nats-jetstream`; typed config `{embedded, store_dir, url, credentials Secret, stream, retention, dedup_window, ack_wait, nak_delay_min, nak_delay_max}`; `ValidateWith(router)` enforces `ack_wait > router.process_timeout + 10s`; `Open` connects and calls `EnsureStream`
- [x] `Publish(ctx, msg)` implemented **directly** on `nats.go/jetstream`: build a `nats.Msg` on the topic subject with headers from metadata plus `Nats-Msg-Id = msg.UUID`, call `js.PublishMsg(ctx, ...)` and return only after the PubAck (ctx deadline/cancel is honored by the client; no Watermill publisher, whose `Publish` ignores ctx)
- [x] `Subscribe(name, opts)` builds the Watermill `jetstream.NewSubscriber(SubscriberConfig{Conn, AckWaitTimeout: ack_wait, NakDelay: exponential by delivery count between nak_delay_min and nak_delay_max (implement the `Delay` interface if the package lacks one), ResourceInitializer: GroupedConsumer(name), ConfigureConsumer: → ConsumerConfig{Durable: name, AckPolicy: explicit, AckWait, MaxDeliver: -1, DeliverPolicy: All|New}})` with the header-based unmarshaler so payload stays raw JSON; ⚠️ decision point: if the beta subscriber fails `bustest`, replace it with an in-house `message.Subscriber` over a nats.go pull consumer (Fetch → message → wait Ack/Nack → `Ack`/`NakWithDelay`) without changing the driver's public surface
- [x] `Capabilities()`: `DurablePublish, DurableConsumers, FanOut, HistoricalReplay, ReportsLag` true, `Deduplicates = dedup_window > 0`, `Retention: Time`; `Lag(consumer)` = `NumPending + NumAckPending` from consumer info
- [x] existing durable consumer keeps its original deliver policy (start position applies on first creation only); logged at `Subscribe`
- [x] write test running `bustest.Run` against the driver on `NewTestServer` (all capability checks execute, including publish failure while the server is stopped)
- [x] write driver-specific tests: stream created with retention/dedup/limits, `EnsureStream` idempotent, external `url` mode, `Publish` honors ctx cancellation/deadline (returns promptly while the server is stopped), exponential nak delay values, `Lag` reflects unacked messages, `ValidateWith` rejects `ack_wait ≤ process_timeout + margin`, redacted config masks credentials
- [x] run tests - must pass before next task
- ➕ The subscriber is in-house over a nats.go pull consumer; the decision point was exercised before wiring watermill-nats: in `watermill-nats/v2` v2.2.0 `pkg/jetstream` is documented as beta, `GroupedConsumer` hardcodes a work-queue stream configurator and the default consumer configurator (so `ConfigureConsumer` is ignored for named consumers), the subscriber creates its own stream named after the topic, and `Close` drains the shared connection. The public surface is the one planned; watermill-nats is not a dependency
- ➕ Signatures: `StartEmbedded(cfg, logger)` (server log lines routed to slog), `EnsureStream(ctx, js, cfg, topic)` and exported `StreamConfig(cfg, topic)` (the topic comes from `bus.topic`, not the driver block), `New(ctx, cfg, topic, logger, ...Option)` with `WithInProcess(provider)` and `WithReconnectWait(d)` for tests; `TestServer` implements `nats.InProcessConnProvider` so a bus reconnects to the restarted server on its own; `Bus.Connected()` exposes the connection state for `/status`
- ➕ Publish: the client runs with `ReconnectBufSize(-1)`, so a publish during a broker outage fails at once instead of being buffered and flushed after the receiver already answered 503; metadata keys with the `Nats-` prefix are rejected; the caller's message is not modified
- ➕ Existing consumers: deliver policy and start sequence are kept, `ack_wait` and `max_deliver` are updated to the current configuration; the subscriber forwards a Nack as `NakWithDelay` with the exponential delay computed from the broker's delivery count, and `Close` waits up to 5s for in-flight acks/nacks before handing the rest back to the broker

### Task 6: Service metrics and admin server
- [x] `internal/metrics/metrics.go`: `Metrics` owning a `prometheus.Registry` with Go runtime and process collectors, `antwatcher_build_info{version,commit}`, and constructors for every metric family listed in Technical Details so components share instances
- [x] `internal/metrics/lag.go`: `LagPoller` polling `bus.LagReporter` for registered consumers every `admin.lag_interval` (default 15s) into `antwatcher_bus_consumer_lag{consumer}`; logs once and skips when the bus does not report lag
- [x] `internal/admin/server.go`: admin HTTP server on `admin.listen` (default `127.0.0.1:9090`): `/metrics`, `/healthz` (200), `/readyz` (from a `Readiness` func), `/status` (JSON from a `StatusProvider` func; filled in by Task 18); never mounted on the webhook listener
- [x] write tests: build info exposed; lag poller updates gauge from a fake reporter and tolerates errors; admin endpoints respond; readiness reflects the provider
- [x] run tests - must pass before next task

### Task 7: Webhook receiver
- [x] `internal/receiver/handler.go`: `New(cfg, b bus.Bus, topic, metrics, logger) http.Handler` mounting only `POST <webhook_path>` and `GET /healthz` (for load balancers); readiness lives on the admin server and depends on the bus only (sinks being down must not fail ingress)
- [x] webhook flow: reject non-POST; `http.MaxBytesReader` (default 25 MiB); verify `X-Hub-Signature-256` with HMAC-SHA256 over the raw body via `hmac.Equal` (401 on mismatch/missing); `ping` → 200 without publishing; `event.FromWebhook` (400 on error) → `b.Publish(ctx with server.publish_timeout, default 8s — under GitHub's 10s limit, event.ToMessage(env))` (503 on error/timeout, body includes GUID) → 200 with `{"delivery": guid}`; info log with guid/event/action/repo/latency
- [x] metrics: `antwatcher_webhooks_total{event,result}` (published, ping, rejected_signature, bad_request, publish_failed, publish_timeout), `antwatcher_webhook_publish_seconds`, `antwatcher_webhooks_inflight`
- [x] `internal/receiver/server.go`: `Run(ctx, cfg, handler) error` with `http.Server` timeouts and graceful shutdown
- [x] write tests with `httptest` + gochannel bus + failing/slow bus stubs: valid signature → 200 and message on the bus; bad/missing signature → 401 without publish; ping → 200 without publish; oversize → 413; invalid JSON → 400; publish error → 503; publish slower than timeout → 503 within the timeout; ordering test proving 2xx is written only after `Publish` returned nil; `/status`/`/metrics` are not served on this listener
- [x] run tests - must pass before next task
- ➕ note: `New` takes no separate `topic` argument; the bus already carries its topic (`bus.Topic()`), so the receiver cannot publish to a different one. Requests rejected before signature verification (401, 413) are counted with `event="unknown"` so an unauthenticated caller cannot create metric series.

### Task 8: Sink core: classes, errors, driver registry, router assembly, stalled handling
- [x] `internal/sink/sink.go`: `Class` enum {trace, log, analytics, archive, forward}; `Sink` interface `Name() string`, `Class() Class`, `Process(ctx, event.Envelope) error`, `Close() error`; `Instance{Sink; StartFrom bus.StartPosition}`
- [x] `internal/sink/errors.go`: `Permanent(err) error` wrapper and `IsPermanent(err) bool`; `ErrSkipped` sentinel returned by projections that produce nothing (router counts `skipped`, acks); rule documented in code: drivers wrap non-retryable failures (auth, schema mismatch, invalid argument, malformed input) in `Permanent`, everything else is retryable
- [x] `internal/sink/registry.go`: `RegisterDriver(class, driver, DriverFactory)`; `DriverFactory func(ctx, name string, raw yaml.Node, deps Deps) (Sink, error)`, `Deps{Logger, Metrics, BusDrivers}`; `Build(ctx, cfgs, deps) ([]Instance, error)` — unknown class/driver lists what is registered; drivers decode their block with `KnownFields(true)` and implement `config.Describer`
- [x] `internal/sink/router.go`: `BuildRouter(ctx, b, routerCfg, mode, instances, metrics, logger)` — per instance `bus.ResolveConsumer` (warnings logged, errors abort startup), `b.Subscribe("sink-"+name, effective)`, `router.AddNoPublisherHandler(...)`; handler: derive `ctx` with `router.process_timeout` (invariant: process deadline < broker ack wait, enforced by driver validation); `event.FromMessage` undecodable → treated as **permanent** (see stalled handling, never acked); `sink.Process` → nil/`ErrSkipped` → ack; retryable error → return error (Watermill Nacks, broker redelivers with growing delay); permanent error → stalled handling; middleware: `middleware.Recoverer` and Watermill Prometheus metrics only, **no `Retry`, no `Poison`, no sleeping in handlers**
- [x] stalled handling (`internal/sink/stalled.go`): per sink a `map[uuid]StalledInfo{reason, firstSeen, attempts}`; on permanent error: upsert the entry, log at error on first occurrence and then once per N attempts, return the error so the message is **Nacked immediately** (the broker's nak delay paces re-attempts; no handler-side delay because Watermill stops waiting for Ack/Nack at `AckWaitTimeout` and sends no `InProgress`); on success of the **same UUID** delete the entry; `antwatcher_sink_stalled{sink}` = 1 while the map is non-empty, `antwatcher_sink_stalled_messages{sink}` = len; `/status` lists stalled UUIDs per sink (operator runbook: fix config/code/destination, the message then succeeds on its next redelivery, no restart needed)
- [x] per-sink metrics: `antwatcher_sink_events_total{sink,class,result=ok|error|permanent|skipped}`, `antwatcher_sink_process_seconds{sink,class}`, `antwatcher_sink_last_success_timestamp_seconds{sink}`, `antwatcher_sink_stalled{sink}`, `antwatcher_sink_stalled_messages{sink}`
- [x] write tests for errors helpers, registry (register/build, unknown class/driver listing, unknown config key rejected), stalled map (success of a different UUID does not clear an older stalled UUID; success of the same UUID does), `BuildRouter` on the gochannel bus: delivery; retryable error → Nack → redelivery; permanent error → stalled entry, Nack without delay in the handler, gauge set; undecodable → stalled, never acked; skipped counted and acked; process timeout cancels a hanging sink and Nacks; `Earliest` on a non-replay bus fails in `fail` and degrades in `degrade`; clean stop on ctx cancel
- [x] write integration test on `natsjs.NewTestServer`: two capturing sinks, one failing with retryable errors; lag grows for the failing sink only; after recovery the backlog drains from its own durable position
- [x] run tests - must pass before next task
- ➕ deviations: `RegisterDriver(class, driver, Driver{Factory, Describer})` mirrors the bus registry so `Describers()` feeds `config.Describers.Sinks`; `Instance` also carries `Driver` for `/status`; handlers are added with `AddConsumerHandler` (`AddNoPublisherHandler` is deprecated in Watermill 1.5); `Permanentf` added; a skipped event updates `last_success` (a trace sink seeing only queued jobs is healthy); `Router.Status()` / `Router.Consumers()` expose the `/status` view and the consumer names for the lag poller (wired in Task 18); the Watermill logger adapter demotes the router's own "Handler returned error" line to debug (the handler already logged the classified failure) except for recovered panics

### Task 9: OTLP client (shared by trace and log drivers)
- [x] `internal/otlp/config.go`: `Config{Endpoint, Protocol grpc|http, Insecure bool, TLS{CAFile, CertFile, KeyFile, ServerName}, Headers map[string]config.Secret, Timeout, Compression none|gzip, Retry{Attempts (default 2), Backoff (default 500ms)}}` with validation; implements `config.Describer`
- [x] `internal/otlp/types.go`: own transport-neutral types — `Span{TraceID [16]byte, SpanID, ParentSpanID [8]byte, Name, Kind, Start, End time.Time, Attributes map[string]any, Status{Code, Message}}`, `LogRecord{Time, ObservedTime time.Time, Severity, SeverityText, Body string, Attributes map[string]any, TraceID [16]byte, SpanID [8]byte}`, `Resource{Attributes}`; converters to `go.opentelemetry.io/proto/otlp` `ResourceSpans` / `ResourceLogs` (scope `antwatcher`, resource `service.name=antwatcher`, `service.version`)
- [x] `internal/otlp/client.go`: `Client{ExportSpans(ctx, []Span) error; ExportLogs(ctx, []LogRecord) error; Close() error}`; gRPC via generated `TraceServiceClient`/`LogsServiceClient` with a **non-blocking dial** (constructing a client never requires the endpoint to be reachable; connection is established on first export), HTTP via POST `/v1/traces`, `/v1/logs` with `application/x-protobuf` (+gzip); headers/TLS applied to both; partial-success responses are the documented exception to "nothing dropped": the OTLP spec forbids retrying them, so rejected spans/records are a terminal loss for that destination, logged with the server message and counted in `antwatcher_otlp_rejected_total{signal}`, and the export is treated as success
- [x] `internal/otlp/retry.go`: classification per OTLP spec — retryable gRPC codes (Unavailable, DeadlineExceeded, Aborted, OutOfRange, DataLoss, Cancelled, ResourceExhausted with retry info) and HTTP 429/502/503/504 (honor `Retry-After`); short bounded retry (`Retry.Attempts`) inside the client, then the error is returned; other codes/4xx → `sink.Permanent`
- [x] write tests: config validation and redaction; in-process gRPC stubs for both services (records requests, programmable status); HTTP stub via `httptest`; conversions produce expected proto (IDs, parent, timestamps, attributes, severity); retryable status retried then surfaced; permanent status wrapped as permanent; partial success counted not retried; headers and gzip applied
- [x] run tests - must pass before next task
- ➕ Implementation notes: endpoint scheme decides TLS on its own (`http://` plaintext, `https://` TLS, scheme-less obeys `insecure`), and TLS settings on a plaintext endpoint are rejected; backoff doubles per retry (capped at 1m) unless the server supplies a delay (gRPC `RetryInfo`, HTTP `Retry-After`); a server delay longer than `timeout` or than the caller's remaining deadline is not waited for in-client (error returned, bus redelivers); HTTP 500 is permanent per the OTLP spec (only 429/502/503/504 retry); `New(cfg, Options{Logger, Metrics, Version, Resource})` reads TLS files but never dials; TLS/mTLS covered by tests with a generated test CA for both protocols

### Task 10: Trace class projection and otlp driver
- [x] `internal/sink/trace/project.go`: `Project(exec model.Execution) ([]otlp.Span, error)` — `KindRun` with status `completed` → root span `workflow:<name>` from `Run.StartedAt` to `Run.UpdatedAt` (attributes `github.repository`, `github.workflow.name`, `github.workflow.path`, `github.run_id`, `github.run_number`, `github.run_attempt`, `github.event`, `github.head_branch`, `github.head_sha`, `github.actor`, `github.conclusion`, `github.html_url`; status Error for failure/timed_out, Ok for success, Unset otherwise); `KindJob` with `completed` → job span `job:<name>` (parent = `Job.ParentSpanID()`, `StartedAt`→`CompletedAt`, `github.job_id`, `github.runner_name`, `github.runner_group`, `github.labels`, `github.conclusion`) plus one child span per step having both timestamps (`step:<name>`, `github.step.number`, `github.step.conclusion`); everything else → `ErrSkipped`
- [x] `internal/sink/trace/sink.go`: `Exporter` interface `ExportSpans(ctx, []otlp.Span) error; Close() error` (driver contract); `New(name, exporter)` implementing `Sink`: normalize (malformed → `Permanent`) → project → export
- [x] `internal/sink/trace/drivers/otlp`: registered as `otlp` for class `trace`; decodes `otlp.Config`, wraps `otlp.Client` (constructor performs no network call; an unreachable endpoint surfaces as retryable errors on `Process`)
- [x] write tests from fixtures: same trace ID across run and its jobs; job parent = run span; steps parented to job; timestamps/attributes exact; skipped step omitted; failed step status Error; `requested`/`queued`/`in_progress` → skipped; malformed → permanent; determinism; sink calls the exporter only for non-empty projections and propagates errors; driver builds from config and exports to the gRPC stub
- [x] run tests - must pass before next task
- ➕ Implementation notes: `Project` returns `sink.ErrSkipped` (no class-local error); job spans additionally carry `github.repository`, `github.run_id`, `github.run_attempt`, `github.workflow.name`, `github.head_branch`, `github.head_sha`, `github.html_url` so a job trace is searchable even when its run event was never received; nil optional fields are omitted from attributes; a run with no `run_started_at` starts at `created_at`, a workflow with a null name is named by its path; a completed job with a missing `started_at`/`completed_at` falls back to `created_at` / `model.EventTime`; Error status carries the conclusion as its message; `sink.Deps` gained `Version` so drivers can report `service.version`; the driver package name is `otlp` (imported blank; the client is aliased `otlpclient` inside it); `cmd` does not import the driver yet — Task 18 wires blank imports for all drivers

### Task 11: Log class projection, stdout driver, otlp driver
- [x] `internal/sink/log/project.go`: `Project(exec model.Execution) otlp.LogRecord` for **every** event: `Time` = `model.EventTime` (else `received_at`), `Body` = `"<event> <action>: <workflow or job name>"`, `Severity` (Error for failure/timed_out, Warn for cancelled, Info otherwise), attributes = envelope fields + entity fields (ids, names, status, conclusion, branch, sha, actor, runner, html_url, step summary counts), `TraceID`/`SpanID` from the model when present (so logs correlate with traces in Grafana); `KindNone` events get envelope attributes only
- [x] `internal/sink/log/sink.go`: `Writer` interface `Write(ctx, otlp.LogRecord) error; Close() error`; `New(name, writer)`
- [x] `internal/sink/log/drivers/stdout`: registered as `stdout`; JSON line per record to stdout or stderr (`stream`), `pretty` option; write errors are retryable
- [x] `internal/sink/log/drivers/otlp`: registered as `otlp`; `Write` → `otlp.Client.ExportLogs` synchronously (success → ack); no batch processor, no `ForceFlush`; batching is a later optimisation behind the same `Writer` contract
- [x] write tests: projection per fixture (severity, body, correlation IDs present/absent, unknown event), stdout output shape, otlp driver exports one record with trace/span IDs to the logs gRPC stub and surfaces errors with the right classification
- [x] run tests - must pass before next task
- ➕ Implementation notes: `Body` drops empty parts (`ping` has no action, an event without a modelled entity has no name part, a nameless workflow is named by its path); `ObservedTime` is `received_at`; every record carries `antwatcher.kind`, `antwatcher.received_at`, `antwatcher.schema_version` next to the envelope fields, and run/job records reuse the trace-class attribute spelling so one query matches spans and lines; step summary counts are `github.steps.total/completed/failed/skipped` (failed = `failure` + `timed_out`); the stdout document shape is `{time, observed_time, severity, severity_number, body, trace_id, span_id, attributes}` with hex ids omitted when zero, writes serialised under a mutex, an unencodable record (a projection bug) is permanent, and `Close` keeps the process stream open; the log otlp driver exports one record per request, its package is named `otlp` with the client aliased `otlpclient`; `cmd` still imports no driver (Task 18)

### Task 12: Analytics class projection, schema, and deduplicating view
- [x] `internal/sink/analytics/record.go`: `Record` with columns in Technical Details (`record_id`, `kind`, ids, names, status, conclusion, timestamps, `queued_ms`, `duration_ms`, trigger/branch/sha/actor, runner fields, `labels`, `html_url`, `event_time`, `status_rank`, `received_at`, `delivery_guid`, `schema_version`); all columns except identity, `kind`, `event_time`, `received_at`, `delivery_guid` are **nullable** because the model is sparse (job/step rows have no `workflow_id`, `workflow_path`, `trigger_event`, `actor`; run rows have no runner fields); `Project(exec) ([]Record, error)` — run event → one `run` record; job event → one `job` record plus one `step` record per step (steps carry the job's current status when their own is missing); `KindNone` → `ErrSkipped`; `record_id = model.RecordID(kind, repository_id, run_id, run_attempt[, job_id[, step_number]])` (entity identity, **not** per event); consumers join run ↔ job/step on `repository_id, run_id, run_attempt`
- [x] `internal/sink/analytics/schema.go`: portable `Schema` (ordered columns with types string/int64/timestamp/bool/string_array, required flags, `PartitionBy: event_time (day)`, `ClusterBy: repository, kind`) and `CurrentViewSQL(table)` producing the deduplicating view: `SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY status_rank DESC, event_time DESC, received_at DESC) = 1` (later, newer state wins; replays of older events never overwrite)
- [x] `internal/sink/analytics/sink.go`: `Writer` interface `EnsureSchema(ctx, Schema) error; Write(ctx, []Record) error; Close() error`; `New(name, writer, ensure bool)`; `EnsureSchema` runs **lazily on the first `Process`** (retried on later calls until it succeeds; its errors are classified like writes), never in the constructor, so an unreachable warehouse degrades this sink instead of failing startup; contract stated in doc comment: at-least-once, duplicates are expected in the base table, consumers query the `_current` view
- [x] write tests: projection per fixture (record ids stable across queued/in_progress/completed events of the same entity, nullable sparse fields, durations, queued time, step records), schema and view SQL golden, sink calls `EnsureSchema` lazily on first `Process`, retries it after failure, runs it once after success, and propagates writer errors
- [x] run tests - must pass before next task
- ➕ Implementation notes: `Record` uses pointer fields for nullable columns and exposes `Fields()` (column name → string/int64/time.Time/[]string, NULLs absent) so drivers encode rows without knowing the struct; `status_rank` and `schema_version` are required next to the plan's list (they are always derivable and the view order must be total), as are `repository_id`, `run_id`, `run_attempt` (the join key, never absent after `Normalize`); empty strings and zero timestamps become NULL; `queued_ms` exists only once the entity started (GitHub sends `started_at = created_at` while queued) and `duration_ms` only once completed; a run's `completed_at` is its `updated_at` when completed; step rows copy `job_name`, runner fields, labels, branch and sha from the job (no join needed) and have no `created_at`/`html_url`; a defensive `Kind` without its entity is `ErrSkipped`; `Schema` carries column descriptions, `Validate()`, `ColumnNames()`, `Column()`; `ViewName(table)` = `<table>_current`; `CurrentViewSQL` wraps the given table in backquotes; `EnsureSchema` runs only when there are records to write (a skipped event does not touch the warehouse), under a mutex so concurrent first calls share one attempt; package is at 100% coverage; no driver registered yet (Task 13)

### Task 13: Analytics bigquery driver (Storage Write API)
- [x] `internal/sink/analytics/drivers/bigquery`: registered as `bigquery`; config `{project, dataset, table, credentials_file, ensure_table, ensure_view}`; the constructor only validates config and creates clients (no network round-trip); `EnsureSchema` (called lazily by the class) creates the table from `Schema` (DAY partitioning on `event_time`, clustering, nullable columns) and the `<table>_current` view when missing; the managed stream is opened on first write; proto descriptor derived from the table schema via `adapt.StorageSchemaToProto2Descriptor`, rows encoded as `dynamicpb` messages
- [x] `Write` uses `managedwriter` with the **default stream** (`WithType(DefaultStream)`, `EnableWriteRetries(true)` for the client's own transient retries), waits for the append result, returns errors; schema/permission errors → `sink.Permanent`, transport errors retryable; no offsets, no application streams (exactly-once explicitly out of scope)
- [x] `appender` interface so the managed stream is injectable in unit tests
- [x] write tests: schema → BigQuery schema and proto descriptor conversion (golden), record → dynamic proto encoding round-trip, fake appender receives encoded rows, error classification, `EnsureSchema` creates table and view through a fake metadata client and skips when disabled
- [x] run tests - must pass before next task
- ➕ Implementation notes: the injectable seams are exported (`Tables`/`Table` for the metadata API, `Appender` + `StreamOpener` for the managed stream) so the external test package can drive them; `NewWriter(cfg, tables, opener, logger)` builds the writer over fakes and `New(cfg, Options)` over real clients. `credentials_file` is loaded with the typed service-account loader (`option.WithCredentialsFile` is deprecated for a security risk), so only service account keys are accepted; without it Application Default Credentials are used and their absence fails construction (static config). Both `ensure_table` and `ensure_view` default to true; the class is built with `ensure = ensure_table || ensure_view`. An existing table is verified against `Schema` (missing column, type, repetition, or a REQUIRED column the rows may leave NULL → permanent); a 409 on create is treated as created concurrently. Opening the default managed stream performs a `GetWriteStream` call and the library keeps the same context as the stream's lifetime, so the opener runs it under a child of the writer's base context that is cancelled only if the per-write context ends during setup (an unreachable service gives up with `process_timeout`, retryable), and the stream itself outlives the write; `ManagedStream.Close` returning `io.EOF` is success. Classification: gRPC InvalidArgument / PermissionDenied / Unauthenticated / NotFound / FailedPrecondition / OutOfRange / AlreadyExists / Unimplemented and REST 400 / 401 / 403 / 404 / 409 / 412 are permanent, everything else retryable; row-level errors from the append response are appended to the message. Tests also run the real client against an in-process gRPC `BigQueryWrite` stub (rows and descriptor as received by the service, embedded errors and row errors, stream reuse after rejection) and against an unroutable endpoint; package coverage 92.7%.

### Task 14: Archive class and filesystem driver
- [x] `internal/sink/archive/sink.go`: `Writer` interface `Append(ctx, event.Envelope) error; Close() error` (driver contract); `New(name, writer)`; `Process` passes the raw envelope (no normalization)
- [x] `internal/sink/archive/jsonl.go`: shared helper for file-based drivers — the open file is always **plain JSONL** `current.jsonl.part`: append one envelope JSON line, flush userspace buffers, `fsync`, then return (ack only after bytes are durable); rotation by `rotate_size` / `rotate_every`: close, rename to `YYYY/MM/DD/<sink>-<first_ts>-<seq>.jsonl`, then optionally gzip the **finalized** file (`compress: gzip` writes `.jsonl.gz` and removes the plain file only after the gzip is fsynced); unfinished `.part` files are finalized at startup (plain JSONL is always readable, so a crash never leaves a truncated gzip stream)
- [x] `internal/sink/archive/drivers/filesystem`: registered as `filesystem`; config `{dir, compress, rotate_size, rotate_every}`; no spool/uploader (object-store drivers add their own later)
- [x] write tests: append + fsync produces valid JSONL readable back into identical envelopes; rotation by size and by time (fake clock); post-rotation gzip valid and plain file removed only after; simulated crash mid-file (kill without close) leaves a readable `.part` that startup finalizes; write error → retryable error and no ack; sink passes envelope verbatim
- [x] run tests - must pass before next task
- ➕ Implementation notes (Task 14): the open file is `<dir>/<sink>-current.jsonl.part` (sink-prefixed, so a directory may hold several sinks and a renamed sink starts a new series); finalized files are `YYYY/MM/DD/<sink>-<first_ts>-<seq>.jsonl[.gz]` where first_ts is the received_at of the first record in UTC and the dated path follows it; rotation is evaluated on Append and at Close (an idle sink keeps its readable `.part` open until the next event or shutdown); the record line nests the raw payload as a JSON value with insignificant whitespace removed (values byte-identical, pretty-printing not preserved) and no HTML escaping; a write or fsync failure cuts the partial line, closes the file, and returns a retryable error; startup also cuts a torn trailing line, removes interrupted `.gz.tmp` files, and compresses plain finalized files when `compress: gzip`; `config.ByteSize` added for `rotate_size` (`128MiB`, `1GB`, bare bytes); durations must be spelled with a unit (`0s`, not `0`) as everywhere in the config.

### Task 15: Forward class and bus driver
- [x] `internal/sink/forward/sink.go`: `Publisher` interface `Publish(ctx, *message.Message) error; Close() error`; `New(name, publisher, topic, maxHops)`; `Process` builds a **new** message: UUID = `sha256(delivery_guid + sink name + topic)` (never the original UUID, so a broker dedup window cannot swallow it), metadata copied plus `antwatcher.forward.by=<sink>` and `antwatcher.forward.hops` incremented; hops ≥ `max_hops` (default 3) → `Permanent` (loop detected); payload raw
- [x] `internal/sink/forward/drivers/bus`: registered as `bus`; config `{driver, topic, <driver block>}` opens a publisher-only bus through the bus registry (NATS today, Kafka/Pub/Sub tomorrow with no forward-specific code); validation rejects same driver+topic as the ingress bus
- [x] write tests with a capturing publisher: new UUID deterministic and different from the GUID, metadata and payload preserved, markers added, hop limit → permanent, publisher error propagates; driver test forwarding from a gochannel ingress into a `natsjs.NewTestServer` topic and reading it back; loop validation error
- [x] run tests - must pass before next task
- ➕ Implementation notes (Task 15): the forward markers are typed envelope fields (`event.Envelope.ForwardedBy`, `ForwardHops`) mapped to metadata keys `antwatcher.forward.by` / `antwatcher.forward.hops` by `ToMessage`/`FromMessage` (omitted when zero; a non-integer or negative hop count is `ErrInvalidMetadata`, so an undecodable copy stalls like any other), and the archive record keeps them as `forwarded_by` / `forward_hops`; the copy's metadata is rebuilt from the envelope by `event.ToMessage`, so every known envelope key is copied and `delivery_guid` stays explicit; the UUID is `hex(sha256("antwatcher:forward:" + delivery_guid + ":" + sink + ":" + topic))`; the loop rule is `hops >= max_hops` on the incoming envelope, checked before publishing. The `bus` driver block is `{driver, topic, max_hops, <bus driver name>: {...}}` with the same driver-block convention as the top-level `bus:` mapping; `Describe` renders every driver block through the bus driver's own `Describer` (secrets mask themselves, unknown bus drivers are a describe error, the selected driver's block is always shown with defaults) and the typed config implements `config.DriverValidator` (own rules plus the target block's `ValidateWith`) and the new `config.IngressValidator` (`ValidateIngress(config.Bus)`), which `config.ValidateDrivers` now runs for sink blocks so `-check` rejects a target with the ingress driver+topic; `sink.Deps.Ingress` carries the same pair so `sink.Build` rejects it too. The factory never opens the target: `Publisher` opens the bus through the registry on the first `Publish` under a mutex, a failed open is a retryable error (unreachable target = degraded sink, bus redelivers), a non-durable target logs a warning, and the target bus registers no metrics (its collectors would collide with the ingress bus on the shared registry); `DriverWith(registry)` binds a custom bus registry for tests and embedders.

### Task 16: GitHub hook deliveries client
- [x] `internal/ghclient/auth.go`: `Auth{Type string; Token config.Secret}`; `type: token` builds an authenticated go-github client; `type: github_app` is recognized by validation and returns `ErrUnsupportedAuth` (schema reserved, implementation later)
- [x] `internal/ghclient/client.go`: `Target{Owner, Repo string; Org string; HookID int64}`; `ListDeliveries(ctx, target, since)` paginating by cursor until `delivered_at < since`; `Redeliver(ctx, target, deliveryID)`; `DiscoverHookID(ctx, target, webhookURL)` matching `config.url` in `ListHooks`; `Delivery{ID, GUID, DeliveredAt, StatusCode, Redelivery, Event, Action}`; typed `ErrRateLimited{RetryAfter}`
- [x] write tests with an `httptest` GitHub API stub: pagination across 3 pages stops at `since`; repo and org endpoints; discovery found/absent/ambiguous; rate limit → `ErrRateLimited`; redeliver POSTs to the right path; unsupported auth type error
- [x] run tests - must pass before next task

### Task 17: Recovery loop
- [x] `internal/recovery/decide.go`: pure `Plan(deliveries, now, grace) []int64` — group by GUID; skip GUIDs with any `2xx` attempt; skip GUIDs whose latest attempt is younger than `grace`; return the latest delivery ID per remaining GUID
- [x] `internal/recovery/recovery.go`: `Run(ctx)` per target keeps in-memory state `{hookID, lastScan time.Time, pending map[guid]pendingInfo{latestDeliveryID, requestedAt}}`; **first scan** after start covers `now − lookback .. now`; **subsequent ticks** scan `lastScan − overlap .. now` (`overlap` default 15m) and merge into `pending`: a GUID enters `pending` when it has no 2xx attempt, leaves when a later attempt (incl. a redelivery) is 2xx; redeliver `pending` entries whose latest attempt is older than `grace` and which were not requested within `grace`, up to `max_per_scan` with short delays; `ErrRateLimited` ends the scan until next tick; a restart simply repeats the full scan (RAM state is a cache, never a correctness dependency)
- [x] recovery is **never fatal**: a bad token, unreachable API, rate limit, or failed hook discovery marks the target degraded (`antwatcher_recovery_degraded{target}=1`, reason in `/status`, retried next tick) while the receiver keeps running; only invalid static config (auth type, empty targets, bad intervals) fails startup
- [x] metrics `antwatcher_recovery_scans_total{target,result}`, `antwatcher_recovery_redeliveries_total{target}`, `antwatcher_recovery_pending{target}`, `antwatcher_recovery_degraded{target}`, `antwatcher_recovery_last_scan_timestamp_seconds{target}`; documented in code and ADR together with the single-replica note (run recovery on one instance)
- [x] write table tests for `Plan` (all failed → latest; one OK → skip; redelivery OK → skip; inside grace → skip; mixed; empty) and `Run` with a fake client and fake clock: first scan uses the full lookback, the second uses `lastScan − overlap`; pending GUID cleared by a later 2xx; exact IDs redelivered; cap; no re-request within grace; rate limit and API errors → degraded, not returned; discovery retried after failure; ctx cancel
- [x] run tests - must pass before next task
- ➕ note: metrics and the single-replica rule are documented in the `internal/recovery` package doc; the ADR text is written in Task 20 (`0001-architecture.md` recovery section)

### Task 18: `serve` wiring, status endpoint, end-to-end test
- [x] `cmd/antwatcher/serve.go`: logger → config → metrics → bus (`bus.Open`, `ResolveIngress`) → admin server (readiness = bus open) → sinks (`sink.Build`) → `sink.BuildRouter` → lag poller → recovery (if enabled) → receiver; `errgroup` with signal-aware root ctx
- [x] ordered shutdown: receiver `Shutdown` first, then `router.Close()` (in-flight handlers finish within `router.close_timeout`, unacked messages are redelivered later), then sink `Close`, then bus / embedded server, admin server last
- [x] `/status` JSON: version, bus driver + capabilities + retention + startup policy warnings, sinks (name, class, driver, requested/effective `start_from`, last success, stalled UUIDs), recovery (targets with hook IDs, auth type, degraded reason), uptime; rendered through `config.Redacted` describers, secrets never included
- [x] startup failure rule: **invalid static config only** — unknown bus/sink driver, unknown config keys, ingress or consumer policy error in `fail` mode, sink constructor rejecting its config; the ingress bus itself must open (it is the durability boundary). Unreachable destinations, GitHub API errors, and failed hook discovery never stop startup: the sink/recovery starts degraded and the webhook path serves
- [x] write end-to-end test: embedded NATS + `httptest` receiver + one capturing sink per class (trace with stub exporter, log with buffer writer, analytics with fake writer, archive with temp dir, forward into a second topic); POST signed `workflow_job.completed` → 200; every sink receives it; restart the router → no duplicates for acked messages; a sink added later with `Earliest` receives history; `/status` reflects effective positions and no secrets
- [x] write tests for shutdown ordering, startup failure paths (static config errors fail; an OTLP endpoint that refuses connections and a recovery target with a failing API both start degraded while the receiver answers 200)
- [x] run tests - must pass before next task
- ➕ [x] `errgroup` replaced by per-component tasks with detached contexts: an errgroup cancels every component at once on the first error, which contradicts the ordered shutdown; `run` reacts to the root context or the first failure and stops the components one by one (`internal/sink` handler contexts are likewise detached from the router close so in-flight `Process` calls finish within `close_timeout`; `receiver.Server.Close` added to simulate a listener failure)

### Task 19: Verify acceptance criteria
- [ ] verify every guarantee in the Technical Details table has a test or an explicit Post-Completion note
- [ ] verify receiver never returns 2xx before `Bus.Publish` succeeded (code review + Task 7 test) and that `serve` refuses a non-durable bus in `fail` mode
- [ ] verify core packages import no concrete driver (`go list -deps` check as a test: `internal/receiver`, `internal/sink` root, `internal/recovery`, `cmd` wiring excluded, must not depend on `internal/bus/natsjs` or any `drivers/*`)
- [ ] verify no code path calls the Actions REST API (grep for `Actions.` in go-github usage)
- [ ] verify no Watermill `Retry`/`Poison` middleware, no `time.Sleep` in router handlers, no unconditional `Ack` of failed messages, and `MaxDeliver` unlimited in the NATS driver; verify every driver classifies errors (grep for `Permanent(` in each driver package); verify no sink constructor performs a network call (constructors run in tests with unroutable endpoints)
- [ ] verify `-check` and `/status` output contain no secret values (test feeds known secrets and asserts absence)
- [ ] run full test suite with `-race`; run `make lint` - all issues must be fixed
- [ ] verify test coverage meets project standard (80%+ per package excluding `cmd/`)
- [ ] verify `antwatcher.example.yml` passes `serve -check`

### Task 20: [Final] Documentation and packaging
- [ ] `README.md`: what it is, architecture diagram (text), guarantees table, quick start (embedded NATS + log/stdout + archive/filesystem), config reference generated from `antwatcher.example.yml`, sink classes and drivers matrix, bus drivers and capability matrix, GitHub webhook setup, recovery token permissions, sizing notes (retention, dedup window), single-replica notes (embedded NATS, recovery), example Prometheus alert rules (lag growing, stalled sink, last-success age, publish failures)
- [ ] ADRs: `0001-architecture.md` (webhooks as source of truth, publish-before-ack, raw envelope, at-least-once + idempotent sinks, delivery-API recovery, no Actions API), `0002-bus-abstraction.md` (contract, capabilities incl. DurablePublish, policy, Watermill driver matrix, how to add a driver: implement + pass `bustest`), `0003-model-and-classes.md` (canonical model, projections, five classes, driver contracts, config model, analytics at-least-once + current view, metrics destination deferred), `0004-errors-and-retries.md` (retryable vs permanent, driver-level short retry, broker redelivery, stalled sinks, no DLQ)
- [ ] `Dockerfile` (multi-stage, distroless, `data/` volume) and `docker-compose.example.yml` with antwatcher + Grafana Tempo + Loki via an OTel collector
- [ ] update `CLAUDE.md` with build, test, layout, and "how to add a bus driver / sink driver" conventions

## Technical Details

### Bus contract and capability policy
```
Bus
├── Publish(ctx, msg)           success only under the driver's durability guarantee
├── Subscribe(ctx, name, opts)  independent consumer "sink-<name>", own position, all messages
├── Capabilities()              declared once per driver (+config), shown in /status
└── Lag(name) (optional)        backlog per consumer → antwatcher_bus_consumer_lag

Capabilities: DurablePublish, DurableConsumers, FanOut, HistoricalReplay, Deduplicates,
              ReportsLag, Retention{None|Time|Size|UntilAcked}   (no Ordered: never relied on)

Policy (one place, internal/bus/policy.go):
  ingress   DurablePublish=false            → fail | degrade→warn "2xx is not durable, dev only"
  consumer  FanOut=false                    → always fail
            Earliest && !HistoricalReplay   → fail | degrade→Now with warning
            DurableConsumers=false          → warn "positions lost on restart"
            Deduplicates=false              → informational (sinks are idempotent anyway)
```

Watermill driver matrix (declared capabilities; only the first two exist in this plan):
| Driver | DurablePublish | DurableConsumers | FanOut | Replay | Dedup | Retention |
|---|---|---|---|---|---|---|
| gochannel | no | no | yes | in-process | no | none |
| nats-jetstream | yes (PubAck) | yes | yes | yes | window | time/size |
| kafka (future) | yes (acks=all) | consumer groups | yes | yes (earliest offset) | no | time/size |
| googlecloud (future) | yes | subscription per sink | yes | yes with topic retention (≤ 31d, seek) | no | ≤ 31d |
| amqp (future) | with publisher confirms | durable queue per sink | exchange fan-out | no | no | until acked |
| sql (future) | tx commit | consumer groups | yes | yes | no | until pruned |
| redisstream (future) | yes | groups | yes | yes | no | maxlen |
| aws sns+sqs (future) | yes | yes | SNS fan-out | no | FIFO 5m | ≤ 14d |

### Message (Watermill ⇄ broker)
```
message.UUID      = X-GitHub-Delivery on the ingress bus (broker dedup key where supported)
message.Metadata  = delivery_guid, schema_version, event, action, hook_id, received_at,
                    repository_id, repository            → broker headers
message.Payload   = original GitHub webhook JSON, byte-for-byte
topic             = bus.topic (default antwatcher.events)
consumers         = sink-<sink name>       (renaming a sink = new consumer; limits retention
                                            means orphaned consumers pin no data)
forwarded copies  = new UUID sha256(delivery_guid + sink + topic), hops counter, same payload
```

### Canonical model and projections
```
Envelope ──Normalize──▶ Execution{Kind: run|job|none, Run, Job{Steps}}   (one implementation,
                            │                                              called by each consumer)
                            ├── trace     completed run → workflow span; completed job → job + step spans
                            ├── log       every event → one LogRecord with trace/span correlation
                            └── analytics run/job/step records, record_id = entity identity
Envelope (raw) ─────────────┬── archive   JSONL append + fsync + rotate
                            └── forward   re-publish with new transport UUID
```

### Deterministic IDs
```
trace_id      = sha256("antwatcher:trace:"  + repository_id + ":" + run_id + ":" + run_attempt)[:16]
run_span_id   = sha256("antwatcher:run:"    + run_id + ":" + run_attempt)[:8]
job_span_id   = sha256("antwatcher:job:"    + job_id)[:8]
step_span_id  = sha256("antwatcher:step:"   + job_id + ":" + step_number)[:8]
record_id     = hex(sha256("antwatcher:record:" + kind + ":" + repository_id + ":" + run_id + ":" + run_attempt [+ ":" + job_id [+ ":" + step_number]]))
```

### Analytics record columns
```
record_id, kind(run|job|step)
repository_id, repository, workflow_id, workflow_name, workflow_path
run_id, run_number, run_attempt, job_id, job_name, step_number, step_name
status, conclusion, status_rank
created_at, started_at, completed_at, queued_ms, duration_ms
trigger_event, head_branch, head_sha, actor
runner_name, runner_group, labels[]
html_url, event_time, received_at, delivery_guid, schema_version
partition: event_time (day)   cluster: repository, kind
view <table>_current: latest state per record_id (status_rank DESC, event_time DESC, received_at DESC)
event_time: Run completed→updated_at, in_progress→run_started_at, else created_at;
            Job completed→completed_at, in_progress→started_at, else created_at;
            Step completed→completed_at, in_progress→started_at, else job event_time; fallback received_at
```
Rows are sparse (nullable columns): job/step rows never carry workflow_id, workflow_path,
trigger_event, actor; join to run rows on repository_id + run_id + run_attempt. Base table is
at-least-once by design; BigQuery `insertId` is not used as a guarantee.

### Processing flow, ack, and retry semantics
```
GitHub ──POST──▶ receiver ──HMAC──▶ Bus.Publish (≤ publish_timeout) ──▶ 2xx
                                          │ error / timeout
                                          └──▶ 503 (GitHub records failed delivery)

bus ──▶ subscriber sink-<name> ──▶ router handler (ctx deadline = process_timeout) ──▶ sink.Process
   nil / ErrSkipped      → Ack
   retryable error       → driver already did ≤ N short retries → Nack → broker redelivery,
                           delay grows with delivery count (nak_delay_min → nak_delay_max),
                           while the event is inside bus retention
   permanent error       → stalled[uuid] recorded for this sink (metric, /status) → Nack at once;
   undecodable message     broker paces re-attempts; cleared on success of the same uuid;
                           other sinks unaffected; no DLQ; nothing discarded by antwatcher
   OTLP partial success  → counted as rejected, not retried (protocol rule), export = success
```
Rules: short transient retries belong to the destination client; long-term retry belongs to
the broker; handlers never sleep; `process_timeout` < broker ack wait (validated), so a slow
destination times out and Nacks instead of racing a concurrent redelivery.

### Recovery algorithm (per target)
```
first scan:   since = now - lookback (≤ 72h, GitHub window)
next scans:   since = lastScan - overlap
deliveries = ListDeliveries(since)
for each GUID group:
    any attempt 2xx                          → delete pending[GUID]
    else                                     → pending[GUID] = latest delivery id
for each pending GUID:
    latest attempt age < grace               → skip (in flight / just redelivered)
    requested within grace                   → skip
    else Redeliver(id); requestedAt = now
stop early on max_per_scan or ErrRateLimited; API errors → target degraded, retry next tick
restart → RAM state gone → first-scan again (cache, not correctness)
```
Redelivered events re-enter the receiver with the same GUID. With the default 2h dedup
window on JetStream, a redelivery whose original did reach the stream is usually collapsed
by the broker; sink idempotency covers the rest.

### Service metrics (Prometheus, admin listener)
```
antwatcher_build_info{version,commit}
antwatcher_webhooks_total{event,result}   antwatcher_webhook_publish_seconds   antwatcher_webhooks_inflight
antwatcher_bus_connected                  antwatcher_bus_consumer_lag{consumer}
antwatcher_sink_events_total{sink,class,result}   antwatcher_sink_process_seconds{sink,class}
antwatcher_sink_last_success_timestamp_seconds{sink}   antwatcher_sink_stalled{sink}   antwatcher_sink_stalled_messages{sink}
antwatcher_otlp_rejected_total{signal}
antwatcher_recovery_scans_total{target,result}   antwatcher_recovery_redeliveries_total{target}
antwatcher_recovery_pending{target}   antwatcher_recovery_degraded{target}   antwatcher_recovery_last_scan_timestamp_seconds{target}
+ Watermill router metrics, Go runtime, process collectors
```

### Config (fields and defaults)
```yaml
server:
  listen: ":8080"               # webhook only (+ /healthz)
  webhook_path: /webhook
  webhook_secret: ${GITHUB_WEBHOOK_SECRET}
  public_url: https://antwatcher.example.com/webhook
  max_body_bytes: 26214400
  publish_timeout: 8s
admin:
  listen: "127.0.0.1:9090"      # /metrics /status /healthz /readyz
  lag_interval: 15s
bus:
  driver: nats-jetstream        # gochannel | nats-jetstream | (future) kafka, googlecloud, amqp, sql, redisstream, aws
  topic: antwatcher.events
  on_missing_capability: fail   # fail | degrade
  nats-jetstream:
    embedded: true
    store_dir: ./data/nats
    url: ""
    credentials: ""             # Secret
    stream: ANTWATCHER
    retention: 168h             # backlog for a down sink survives only within this window
    dedup_window: 2h
    ack_wait: 90s               # must exceed router.process_timeout + 10s (validated)
    nak_delay_min: 5s
    nak_delay_max: 5m
router:
  close_timeout: 30s
  process_timeout: 60s          # deadline for one sink.Process call
sinks:                          # start_from is required on every sink
  - name: tempo
    class: trace
    driver: otlp
    start_from: earliest
    config: {endpoint: "tempo:4317", protocol: grpc, insecure: true}
  - name: events
    class: log
    driver: stdout
    start_from: now
    config: {stream: stdout}
  - name: loki
    class: log
    driver: otlp
    start_from: now
    config: {endpoint: "otel-collector:4318", protocol: http, headers: {Authorization: "${OTLP_AUTH}"}}
  - name: bq
    class: analytics
    driver: bigquery
    start_from: earliest
    config: {project: my-proj, dataset: github, table: actions, ensure_table: true, ensure_view: true}
  - name: raw
    class: archive
    driver: filesystem
    start_from: earliest
    config: {dir: ./data/archive, compress: gzip, rotate_every: 1h, rotate_size: 128MiB}
  - name: downstream
    class: forward
    driver: bus
    start_from: now
    config: {driver: nats-jetstream, topic: github.events, max_hops: 3, nats-jetstream: {url: nats://other:4222}}
recovery:
  enabled: false
  auth:
    type: token                 # github_app reserved
    token: ${GITHUB_TOKEN}
  api_base_url: ""
  interval: 10m
  lookback: 72h                 # first scan after start; later scans are incremental
  overlap: 15m
  grace: 5m
  max_per_scan: 100
  targets:
    - repo: owner/name
    - org: name
      hook_id: 987654
```

### Guarantees
| Situation | Behavior | Verified by |
|---|---|---|
| Sink temporarily unavailable | backlog kept on the bus and redelivered with growing delay **while the event is inside bus retention**; lag visible | Task 8 integration test |
| Sink down longer than bus retention | oldest part of its backlog is gone; stated in docs and `/status` | ADR 0001, Task 20 docs |
| Sink hits a permanent error or an undecodable message | that message is stalled for that sink (by UUID), re-attempted at broker pace, nothing discarded, others continue | Task 8 tests |
| Destination rejects part of an OTLP export | counted as rejected for that destination, not retried (protocol rule) | Task 9 tests |
| Destination unreachable at startup | sink starts degraded; webhook path unaffected | Task 18 tests |
| Process restart | each sink resumes from its durable consumer position | Task 5 conformance, Task 18 e2e |
| Receiver unavailable or publish slow | GitHub records failed delivery (503 / timeout) | Task 7 tests |
| Receiver back within 3 days, recovery enabled | full scan at start, incremental scans after; redelivery of GUIDs without a 2xx | Task 17 tests + Post-Completion drill |
| GitHub API down or token invalid | recovery degraded with metric and status; ingress continues | Task 17, 18 tests |
| Duplicate webhook | accepted; broker dedup where available; deterministic IDs make projections idempotent; analytics dedups in the view | Task 5, 10, 12 tests |
| New sink | earliest retained message when the bus replays; policy otherwise | Task 4 policy, Task 18 e2e |
| Bus without a required capability | startup fails, or degrades with a warning shown in `/status` | Task 4, 8, 18 |
| Secrets in output | `-check` and `/status` never print secret values | Task 1, 19 |
| History older than bus retention | not recoverable by design | ADR 0001 |
| Missed ingress older than GitHub 3-day window | considered lost | ADR 0001 |
| Actions REST API | never used for reconstruction | Task 19 grep check |
| Service degradation | lag, stalled, failures, last-success age exported | Task 6, 8, 17 metric tests |

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- Register a repo or org webhook pointing at a deployed receiver (events `workflow_run`,
  `workflow_job`, JSON, secret); trigger a workflow; confirm the trace in Grafana Tempo
  (workflow → job → step) and the correlated log records in Loki via the OTel collector.
- Point the BigQuery driver at a real project with Application Default Credentials; confirm
  rows land through the Storage Write API, the `_current` view returns one row per entity,
  and Looker can model runs/jobs/steps from it.
- Recovery drill: stop the receiver, trigger workflows, wait past `grace`, start with
  recovery enabled and a token holding `admin:repo_hook` / `admin:org_hook`; confirm GitHub
  shows redelivery attempts and every sink processes the events (duplicates only in the
  analytics base table, none in the view).
- Outage drill: stop Tempo longer than `ack_wait`; confirm lag grows for the trace sink
  only, redelivery delay backs off, and the backlog drains after Tempo returns.
- Stalled drill: point a trace sink at an endpoint returning `InvalidArgument`; confirm the
  sink stalls, the alert fires, other sinks continue, and fixing the endpoint clears it.

**External system updates**:
- Size `retention` and disk for the expected event volume; the bus is the raw history.
- `dedup_window` collapses repeats inside the window only; deterministic IDs are the real guarantee.
- Embedded NATS and recovery assume a single replica; multi-replica deployments need an
  external NATS and recovery enabled on one instance only.
- GitHub App webhooks need `recovery.auth.type: github_app` (App JWT), a later plan.
- Reverse proxy must forward the raw body unchanged (HMAC over exact bytes) and allow 25 MiB;
  the admin listener must not be exposed publicly.
- Future drivers (bus: Kafka, Google Pub/Sub, AMQP, SQL, Redis Streams, AWS; sinks:
  ClickHouse, S3, GCS, Parquet, HTTP forward, GitHub App recovery auth) each get their own
  plan: implement the contract, pass the conformance/class tests, add to the docs matrices.

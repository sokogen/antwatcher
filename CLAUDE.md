# antwatcher

GitHub Actions webhooks → durable bus → traces, logs, analytics, archives, forwarded
events. One Go binary, one module `github.com/sokogen/antwatcher`, Go 1.27.

Read the ADRs in `docs/adr/` before changing anything structural. The README is the
operator's view; this file is the contributor's view.

## Build, test, lint

```sh
make build                     # ./bin/antwatcher, version stamped from git
make test                      # go test -race -cover ./...
make lint                      # golangci-lint v2 (installed into ./bin on first use)
make check CONFIG=antwatcher.example.yml
make readme                    # regenerate the README config reference from antwatcher.example.yml
go test ./internal/archtest    # architecture rules only (fast)
```

Everything must pass before a task is done: `make test` and `make lint` with zero issues.
The suite needs no network and no external services (embedded NATS in a temp dir, fakes
for OTLP, BigQuery, and GitHub). Coverage target is 80 percent per package outside `cmd/`;
`internal/bus/bustest` is the conformance suite itself and has no tests of its own.

`golangci-lint` config is `.golangci.yml`: govet (all but fieldalignment and shadow),
staticcheck, errcheck, revive, gocritic, gosec, testifylint, gofmt, goimports with the
module as local prefix. Tests are linted too.

## Layout

```
cmd/antwatcher/            main: serve (-config, -check, -log-level, -log-format), version
  drivers.go               the ONLY place concrete drivers are imported (blank imports)
  serve.go                 wiring, ordered shutdown, /status view
internal/config/           YAML load, ${ENV} expansion, Secret, defaults, validation, redaction, Describer
internal/event/            Envelope ⇄ Watermill message, fixtures
internal/model/            canonical Run/Job/Step, Normalize, deterministic IDs (only GitHub payload parser)
internal/metrics/          Prometheus registry, families, lag poller
internal/admin/            admin HTTP: /metrics /status /healthz /readyz
internal/bus/              Bus contract, Capabilities, policy, driver registry
internal/bus/bustest/      conformance suite every bus driver must pass
internal/bus/gochannel/    in-process driver (dev, tests, non-durable)
internal/bus/natsjs/       JetStream driver: embedded server, stream, publish, pull subscriber, lag
internal/receiver/         webhook HTTP handler, HMAC, publish-before-2xx
internal/sink/             Sink contract, classes, Permanent errors, driver registry, router, stalled
internal/sink/<class>/     projection + Sink for trace, log, analytics, archive, forward
internal/sink/<class>/drivers/<name>/   drivers: otlp, stdout, bigquery, filesystem, bus
internal/otlp/             OTLP client shared by trace/otlp and log/otlp
internal/ghclient/         go-github wrapper: hook deliveries, hook discovery, redeliver
internal/recovery/         scan / group / redeliver loop
internal/archtest/         architecture rules as tests (see below)
docs/adr/                  0001 architecture, 0002 bus, 0003 model & classes, 0004 errors & retries
docs/plans/                ralphex plans
deploy/compose/            supporting config for docker-compose.example.yml
scripts/                   readme-config.sh
```

## Rules enforced by tests

`internal/archtest` fails the build when a rule is broken. Do not work around it.

- No package outside `cmd/` may depend on `internal/bus/gochannel`, `internal/bus/natsjs`,
  or any `.../drivers/...` package. Core code talks to registries and interfaces only.
- The Actions REST API is never called. go-github is confined to `internal/ghclient` and
  its call surface is `ListHookDeliveries`, `RedeliverHookDelivery`, `ListHooks`.
- Handlers never retry, sleep, or ack: the only Watermill middleware is `Recoverer`; no
  `time.Sleep`, `.Ack()`, or `.Nack()` under `internal/sink/**`, `internal/receiver`,
  `internal/recovery`.
- Every sink driver package (or the client or class package it delegates to) calls
  `sink.Permanent`: drivers classify their errors.
- The README configuration reference equals `antwatcher.example.yml` (run `make readme`).

## Conventions

- **Errors**: drivers wrap failures a retry cannot fix in `sink.Permanent` (auth, schema,
  invalid argument, malformed input); everything else stays retryable. When in doubt,
  retryable. See ADR 0004.
- **No network in constructors**: bus and sink factories validate and construct; they
  connect on first use so an unreachable destination starts degraded, never fails startup.
  Only the ingress bus must open at startup.
- **Config blocks** are opaque `yaml.Node`s decoded with `config.DecodeStrict` (unknown keys
  fail) on top of a `DefaultConfig()`. Secrets are `config.Secret` so `-check` and
  `/status` mask them. Every driver ships a `Describe` function used for validation and
  redaction; implement `config.DriverValidator` for cross-checks against the core config.
- **Registration** happens in `init()` of the driver package; `cmd/antwatcher/drivers.go`
  imports it. Nothing else references a driver by import path.
- **Tests** go in the same task as the code. Unit tests use `gochannel`; integration tests
  use `natsjs.StartEmbedded`-style helpers in a `t.TempDir()`. Use testify (`require` for
  preconditions, `assert` for checks); testifylint runs with every check enabled.
- **Logging** is `log/slog`; loggers are passed down, never global. Metrics come through
  `*metrics.Metrics`, which may be nil in tests.
- **Ordering is never assumed.** GitHub delivers out of order; brokers redeliver out of
  order. Projections carry `status_rank` and timestamps instead.
- Keep docs in sync: a new field goes into `antwatcher.example.yml` (then `make readme`),
  a new driver into the README matrices, a changed decision into the ADR it belongs to.

## How to add a bus driver

1. `internal/bus/<name>/`: `Config` (yaml tags, `config.Secret`), `DefaultConfig`,
   `Describe`, `Open` matching `bus.Factory`.
2. Implement `bus.Bus` (and `bus.LagReporter` if you declare `ReportsLag`). `Publish`
   honors ctx and returns nil only under the guarantee you declare; `Subscribe` validates
   the consumer name with `bus.ValidateConsumerName`, creates an independent consumer per
   name, and returns a Subscriber bound to the bus topic; `Close` is idempotent, later
   calls return `bus.ErrClosed`.
3. `Capabilities()` tells the truth for the given configuration.
4. `init()`: `bus.Register(Name, bus.Driver{Factory: Open, Describer: config.DescriberFunc(Describe)})`.
5. `driver_test.go`: `bustest.Run(t, open)`, with `bustest.WithOutage` when you declare
   `DurablePublish`; assert capabilities and their policy consequences.
6. Import in `cmd/antwatcher/drivers.go`; add to `busDrivers` in `internal/archtest`; add a
   row to the README and ADR 0002 matrices; add a driver block to `antwatcher.example.yml`.

## How to add a sink driver

1. Choose the class whose projection fits (trace: `Exporter`, log: `Writer`, analytics:
   `Writer`, archive: `Writer`, forward: `Publisher`). A new projection is a new class and
   its own plan.
2. `internal/sink/<class>/drivers/<name>/`: `Config`, `DefaultConfig`, `Describe`, and a
   `Factory` matching `sink.DriverFactory` that decodes strictly, validates, builds the
   driver without dialing, and returns `<class>.New(name, driver)`.
3. Classify errors with `sink.Permanent`; keep in-driver retries short and bounded.
4. `init()`: `sink.RegisterDriver(sink.Class<Class>, Name, Driver())`.
5. Tests against a fake destination: success, a retryable error, a permanent error, and a
   factory that builds with an unreachable endpoint.
6. Import in `cmd/antwatcher/drivers.go`; add to the README matrix and
   `antwatcher.example.yml`; mention it in ADR 0003 if the driver contract changed.

## Plans

Implementation plans live in `docs/plans/` and are executed with ralphex. Each task lands
as one commit `feat: <description>` with its tests, after `make test` and `make lint` pass.

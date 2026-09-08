# CI, releases, agent instructions, README refactor

## Overview

antwatcher has no CI, no release artifacts, no instructions for AI agents, and a
README that has grown into a manual. This plan adds the missing delivery
infrastructure and reshapes the documentation so a consumer can see what the
tool does and try it, while the depth moves into `docs/`.

Five independent strands, in the order they land:

1. **CI** — one workflow running the checks contributors already run locally,
   plus a coverage gate that turns the 80 percent target from a sentence in
   CLAUDE.md into a rule.
2. **Releases** — on a tag, a multi-arch container image in ghcr.io and binaries
   attached to the GitHub Release. The service alone: no collector, no Tempo, no
   Grafana.
3. **Agent instructions** — `AGENTS.md` for an agent contributing to this repo,
   `docs/agent-operations.md` for an agent installing and debugging the service
   somewhere else.
4. **README refactor** — consumer-facing: what it gives you, how to try it, how
   to configure it, licence. Everything else moves under `docs/` behind links.
5. **Span prefix** — `workflow:` becomes `run:`, matching both GitHub's own
   terminology and this codebase's `model.Run`.

Nothing here changes the runtime behaviour of the pipeline except strand 5,
which changes one span name prefix. (No longer true as written: the review
iterations that followed the fifteen tasks landed several behavioural fixes on
top. They are listed under Review Follow-Up.)

## Context (from discovery)

Verified against the working tree at commit `aeafa20`.

**Build and check surface that CI will call:**

- `make lint` installs golangci-lint into `./bin` and runs it. `Makefile:11` pins
  the version to `latest`, so a new linter release can fail the build with no
  code change. Locally installed today: `v2.13.2`, reporting 0 issues.
- `make test` runs `go test -race -cover ./...`, no coverage profile is written.
- `make check` builds and validates a config; it needs `GITHUB_WEBHOOK_SECRET`
  set or it exits non-zero on the example config.
- `go.mod` declares `go 1.27.1` — the single source of truth for the Go version.

**Coverage today** (from a full run): every package outside `cmd/` is at or above
86.2 percent, the minimum being `internal/sink/archive`. Two packages report no
coverage by design: `internal/bus/bustest` is the conformance suite itself, and
`internal/archtest` contains no statements. The gate can be switched on without
fixing anything.

**Two constraints the README refactor must respect** — both are enforced tests
that will fail if ignored:

- `internal/archtest/docs_test.go:24` `TestREADMEConfigReferenceMatchesExample`
  requires the config-reference markers and the verbatim contents of
  `antwatcher.example.yml` to live in `README.md`.
- `internal/archtest/docs_test.go:54` `TestREADMEListsEveryShippedDriver`
  requires `README.md` to mention every registered driver name in backticks.

Moving either block into `docs/` means updating the test to read the new file and
updating `scripts/readme-config.sh`, which writes the config block between the
markers in `README.md`.

**The Dockerfile does not cross-compile.** `Dockerfile:2` is a plain
`FROM golang:1.27-alpine` with no `--platform=$BUILDPLATFORM` and no
`GOOS`/`GOARCH`. Under buildx multi-platform this builds each architecture under
emulation, which is slow. Cross-compiling from the build platform is the fix.

**The compose example drops the version stamp.** `docker-compose.example.yml:20`
is a bare `build: .` with no build args, so the image keeps `version = "dev"`
from `cmd/antwatcher/main.go:27` instead of the value `Dockerfile:9-13` would
stamp. This is why `service.version` reads `dev` in exported telemetry.

**Span names** are built in `internal/sink/trace/project.go:27-29` from three
prefix constants and used at lines 118, 152 and 168.

## Development Approach

- **Testing approach**: Regular, following CLAUDE.md — tests land in the same
  task as the code they cover.
- Complete each task fully before moving to the next.
- **CRITICAL: every task that changes code MUST include new or updated tests.**
  Tasks that add CI or documentation instead state their own verification, which
  is `actionlint`, `make test`, or a manual read; a YAML workflow gets no
  invented unit test.
- **CRITICAL: `make test` and `make lint` must pass before starting the next
  task.**
- **CRITICAL: update this plan file when scope changes during implementation.**
- Each task lands as one commit `feat:`/`fix:`/`chore: <description>` per
  CLAUDE.md.

## Testing Strategy

- **Unit tests**: required for every code change; testify, `require` for
  preconditions, `assert` for checks.
- **Architecture tests**: `go test ./internal/archtest` must stay green; the
  README refactor will need its doc tests updated in the same task.
- **Workflow linting**: `actionlint` on `.github/workflows/*.yml`.
- **No e2e/UI tests**: this project has none.

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Keep plan in sync with actual work done

## Implementation Steps

### Task 1: Pin the linter and emit a coverage profile

- [x] set `GOLANGCI_LINT_VERSION ?= v2.13.2` in `Makefile` so local and CI agree,
      keeping `?=` so it stays overridable
- [x] add `-coverprofile=coverage.out` to the `test` target (`coverage.out` is
      already in `.gitignore` and already removed by `clean`)
- [x] verify `make lint` still reports 0 issues with the pinned version
- [x] verify `make test` writes `coverage.out` and still passes
- [x] run tests - must pass before next task
- [x] ➕ install the linter through a version stamp
      (`bin/.golangci-lint-<version>`) instead of the bare binary path, so
      changing the pin reinstalls rather than reusing whatever `bin/` holds —
      without this the pin has no effect on a machine that already ran `latest`

### Task 2: Coverage threshold script

- [x] create `scripts/check-coverage.sh` reading `coverage.out` and failing when
      any package falls below a threshold given as `$1` (default 80)
- [x] exclude `cmd/...` (CLAUDE.md scopes the target to packages outside `cmd/`),
      `internal/bus/bustest` (conformance suite, no tests by design) and
      `internal/archtest` (no statements)
- [x] print a per-package table so a failure names the package and its percentage
- [x] verify it passes at 80 against the current profile
- [x] verify it *fails* when invoked with an impossible threshold, so the script
      is proven able to fail and not merely to stay silent
- [x] add a `make coverage` target invoking it
- [x] run tests - must pass before next task

### Task 3: CI workflow

- [x] create `.github/workflows/ci.yml` triggered on `push` to `main` and on
      `pull_request`, one job on `ubuntu-latest`
- [x] use `actions/setup-go` with `go-version-file: go.mod` and module caching
      so the Go version has one source of truth
- [x] run `make lint`, `make test`, `make coverage`, and `make check` with a
      dummy `GITHUB_WEBHOOK_SECRET`, calling the same targets contributors run
- [x] do NOT add a separate README drift step: `TestREADMEConfigReferenceMatchesExample`
      already covers it inside `make test`
      (that test is now `TestConfigDocReferenceMatchesExample` and guards
      `docs/configuration.md`; renamed by Task 11, see its notes)
- [x] verify the workflow parses with `actionlint`
- [x] run tests - must pass before next task

### Task 4: Cross-compiling Dockerfile

- [x] change the build stage to `FROM --platform=$BUILDPLATFORM golang:1.27-alpine`
      and pass `TARGETOS`/`TARGETARCH` into `go build` so buildx cross-compiles
      instead of emulating
- [x] keep the existing `VERSION`/`COMMIT`/`DATE` build args and ldflags stamping
- [x] verify `docker build` still produces a working image for the host platform
- [x] verify `antwatcher version` inside the image reports the stamped version,
      not `dev`
- [x] run tests - must pass before next task
- [x] ➕ note the BuildKit requirement in the README Docker section: the
      deprecated legacy builder cannot expand `$BUILDPLATFORM` and fails at
      `FROM`, so the image now needs BuildKit (the default in current Docker)
- ⚠️ this machine's Docker CLI had no buildx plugin, so `make docker` ran on the
  legacy builder and failed at `FROM`. Verified instead with the buildx binary
  installed via Homebrew (`docker-buildx` 0.37.0, not wired into
  `~/.docker/config.json`): a host-platform build reported
  `antwatcher v0.0.0-test (commit abc1234, ...)`, and `--platform linux/amd64`
  cross-compiled with `GOOS=linux GOARCH=amd64` on the native arm64 builder in
  17s, producing a `linux/amd64` image with no emulation.

### Task 5: Release workflow — binaries

- [x] create `.github/workflows/release.yml` triggered on tags matching `v*`
- [x] build binaries for linux and darwin on amd64 and arm64, stamping the
      version from the tag through the existing ldflags
- [x] produce one archive per target plus a `checksums.txt`
- [x] attach the artifacts to the GitHub Release, using `permissions: contents: write`
- [x] verify the workflow parses with `actionlint`
- [x] run tests - must pass before next task
- ➕ the job calls `make build` with `VERSION`/`COMMIT`/`DATE` and a per-target
  `BINARY`, so the release ldflags are the Makefile's, not a second copy.
- ➕ `gh release create` fails when the tag was published through the UI, so the
  publish step falls back to `gh release upload --clobber` on an existing
  release. It runs on the built-in `GITHUB_TOKEN`; no repository secret.
- ⚠️ `actionlint` is not installed on this machine; verified with v1.7.12
  installed into a temp `GOBIN` (go.mod and go.sum untouched): 0 errors over
  both workflows. Its `shellcheck` rule stayed disabled, shellcheck being
  absent too. The build loop was also run locally end to end: four archives
  plus `checksums.txt`, `antwatcher version` inside the darwin/arm64 archive
  reporting `v0.0.0-test (commit 0123456, ...)`, and the linux/amd64 binary
  reported by `file` as a statically linked x86-64 ELF.

### Task 6: Release workflow — container image

- [x] add a job publishing a multi-arch image (linux/amd64, linux/arm64) to
      `ghcr.io`, using `permissions: packages: write` and the built-in
      `GITHUB_TOKEN` so no extra secret is needed
- [x] derive image tags from the git tag with `docker/metadata-action`
      (`v1.2.3`, `1.2.3`, `1.2`, `latest`)
- [x] pass `VERSION`/`COMMIT`/`DATE` build args so the image reports its version
- [x] verify the workflow parses with `actionlint`
- [x] run tests - must pass before next task
- ➕ `latest` is not listed explicitly: `docker/metadata-action` adds it under its
  default `latest=auto` flavour for a semver tag that is not a prerelease, which
  is exactly the wanted behaviour and keeps `v1.2.3-rc1` off `latest`.
- ➕ the image job stamps the *short* commit through a `stamp` step rather than
  passing `github.sha` straight through, so an image and an archive built from
  one tag report identical build metadata.
- ➕ no `setup-qemu-action`: Task 4 made the Dockerfile cross-compile, so
  linux/arm64 is built natively on the amd64 runner. GitHub Actions cache
  (`type=gha`) backs the module download layer.
- ⚠️ `actionlint` is still not installed on this machine; verified again with
  v1.7.12 in a temp `GOBIN` (go.mod and go.sum untouched): 0 errors over both
  workflows, with the `shellcheck` rule inactive because shellcheck is absent.
  Pushing a real tag is the only way to exercise the push to ghcr.io; that is
  already recorded under Post-Completion.

### Task 7: Rename the run span prefix

- [x] rename `PrefixWorkflow` to `PrefixRun` with value `run:` in
      `internal/sink/trace/project.go:27`
- [x] update the doc comment on `Project` (`project.go:63-66`) which spells the
      old name out
- [x] leave `job:` and `step:` unchanged, leave every attribute on `github.*`,
      and do not add `antwatcher.kind` — this task is the prefix only
- [x] update trace projection tests asserting the old prefix
- [x] grep README and `docs/` for `workflow:` used as a span name and update
- [x] run tests - must pass before next task
- ➕ no literal `workflow:` span name appears in README or `docs/`; the two
  places that *name* the top-level span in prose now say "run span" instead of
  "workflow span" (`README.md` driver matrix, ADR 0003 class table). The
  generated config block was left alone: it describes GitHub's hierarchy
  ("one span tree per workflow run"), not a span name, and `make readme`
  reports no drift.
- ➕ `internal/otlp/types_test.go` used `"workflow:ci"` as a fixture span name.
  It is unrelated to the trace class constant, but it was renamed to `"run:ci"`
  so a grep for the old prefix comes back empty.

### Task 8: Compose passes the version through

- [x] add `build.args` to `docker-compose.example.yml` so `VERSION`, `COMMIT` and
      `DATE` reach the image instead of defaulting to `dev`
- [x] document in the file header that the values come from the environment and
      fall back to `dev` for a plain `docker compose up`
- [x] verify a compose build reports a stamped version at `/status`
- [x] run tests - must pass before next task
- ➕ the fallbacks are `dev`/`unknown`/`unknown`, matching the ldflag defaults in
  `cmd/antwatcher/main.go`, and `docker compose config` renders them both ways:
  stamped when `VERSION`/`COMMIT`/`DATE` are exported, defaulted when they are not.
- ➕ README's compose section now states that the build passes the three arguments
  through and shows the exports that stamp an image the way `make build` does.
- Verified end to end on this machine: `docker compose build antwatcher` with
  `VERSION=v0.0.0-test COMMIT=abc1234 DATE=2026-09-06T00:00:00Z`, then
  `docker run antwatcher:local version` reporting
  `antwatcher v0.0.0-test (commit abc1234, built 2026-09-06T00:00:00Z)`, and
  `/status` on the started container reporting
  `"version":"v0.0.0-test","commit":"abc1234"`. Compose used BuildKit, so the
  Task 4 `$BUILDPLATFORM` builder worked without the buildx wiring problem noted
  there.

### Task 9: AGENTS.md for repository work

- [x] create `AGENTS.md` at the repository root covering build/test/lint
      commands, the layout, the architecture rules enforced by `internal/archtest`,
      the error and registration conventions, and how to add a bus or sink driver
- [x] keep it factual and current: every command in it must actually work
- [x] replace the body of `CLAUDE.md` with a pointer to `AGENTS.md` plus anything
      genuinely Claude-specific, so the two cannot drift apart
- [x] verify every command quoted in `AGENTS.md` by running it
- [x] run tests - must pass before next task
- ➕ `CLAUDE.md` pulls the shared text in with an `@AGENTS.md` import rather than
  a prose "see AGENTS.md", so Claude loads the same words other agents read and a
  second copy cannot appear. Its only other section says what belongs there:
  Claude-only instructions, of which there are none today.
- ➕ the two README lines that pointed contributors at `CLAUDE.md` now point at
  `AGENTS.md`.
- ➕ each architecture rule now names the test that enforces it, so a failure
  message maps back to the rule without reading `internal/archtest`.
- Verified by running every command quoted in the file: `make build`, `make test`,
  `make coverage` (all packages at or above 80%), `make lint` (0 issues),
  `GITHUB_WEBHOOK_SECRET=dummy make check CONFIG=antwatcher.example.yml`,
  `make readme` (no drift), `go test ./internal/archtest`, and `make docker`,
  which now succeeds because buildx is installed as a CLI plugin on this machine,
  unlike when Task 4 was written. `make check` without the secret was also run, to
  confirm the documented failure.
- ➕ `AGENTS.md` deliberately does not link `docs/agent-operations.md` yet; Task 10
  creates that file, and a dead link in a document whose point is being current
  would be self-defeating.

### Task 10: docs/agent-operations.md for running the service

- [x] create `docs/agent-operations.md` for an agent that installs, configures
      and debugs antwatcher in someone else's project
- [x] cover: choosing a bus and sinks, writing a config, `-check`, the webhook
      setup on GitHub, and the fact that TLS must be terminated in front
- [x] cover debugging by observable signal: `/status`, `/readyz`, the
      `antwatcher_webhooks_total` result labels, `antwatcher_sink_events_total`,
      consumer lag, and what a `401` versus a `503` on the webhook path means
- [x] include the failure modes seen in practice: a signature mismatch is a
      wrong secret, not a bug; events reaching a log backend but not appearing
      means the query window predates the event timestamps
- [x] run tests - must pass before next task
- ➕ `AGENTS.md` now links the new file, both in its opening paragraph and in the
  layout tree; Task 9 had deliberately left that link out until the file existed.
- ➕ the document does not repeat the README's reference tables (fields, metrics,
  alert rules); it links to them and holds only what a reference cannot say —
  which option to choose, what to verify after a deploy, and how to read a signal.
  Tasks 11 and 12 move those tables into `docs/`, and the links here move with them.
- ➕ two further failure modes were added from the code rather than invented:
  `start_from` applies only when the consumer is created (`natsjs/driver.go:283`
  logs "consumer exists: keeping its original start position"), so editing it on a
  live sink does nothing; and `result="skipped"` on the trace class is by design
  for events that are not a completed run or job, not a lost event.
- Verified by running what the document tells an operator to run: the minimal
  config in it passes `-check` (with only defaults filled in), and both quoted
  failures are the real output — `undefined environment variables:
  GITHUB_WEBHOOK_SECRET` with the secret unset, and the strict-decode error
  `field protocoll not found in type otlp.Config` for a typo inside
  `sinks[].config`. Every endpoint, metric name, result label and HTTP status in
  the file was read off `internal/admin`, `internal/metrics` and
  `internal/receiver` rather than from the README.

### Task 11: Move the reference material out of README

- [x] create `docs/configuration.md` holding the full configuration reference,
      and `docs/operations.md` holding endpoints, metrics, alert rules, sizing
      and single-replica notes
- [x] update `scripts/readme-config.sh` to write the config block into
      `docs/configuration.md` instead of `README.md`
- [x] update `TestREADMEConfigReferenceMatchesExample` to read the new file, and
      rename it to match — the test is the reason this cannot be done by moving
      text alone
- [x] decide and implement how `TestREADMEListsEveryShippedDriver` is satisfied:
      either keep a compact driver table in README, or point the test at
      `docs/configuration.md`; record the choice in the test's doc comment
- [x] run `make readme` and confirm no drift
- [x] run tests - must pass before next task
- ➕ the driver-name check stays on `README.md`, with a compact class → driver
  table kept there. The reason is recorded in the test's doc comment: the
  generated block in `docs/configuration.md` already names every driver, because
  a new driver must appear in `antwatcher.example.yml`, so drift is impossible
  there; the README table is hand-written and is the only place a driver can be
  forgotten. The config-reference test was renamed
  `TestConfigDocReferenceMatchesExample` and reads `docs/configuration.md`.
- ➕ four more sections moved with the two the task names — "Sink classes and
  drivers", "Bus drivers and capabilities", "GitHub webhook setup" and "Recovery
  and token permissions" — into `docs/configuration.md`. They are reference
  material by the same argument, and Task 12 rewrites README without creating a
  place to put them.
- ➕ `scripts/readme-config.sh` and the `make readme` target keep their names, as
  the task spells the script out by name; both now act on `docs/configuration.md`
  and say so. Renaming them is a Task 15 question, not a Task 11 one.
- ➕ the cross-references that pointed at the moved text were updated in the same
  task: `docs/agent-operations.md` (three), ADRs 0001, 0002, 0003 and 0004 (one
  each), `AGENTS.md` (layout, architecture rules, the two driver recipes and the
  `make readme` line), and the README's own `#github-webhook-setup` anchor.
- Verified: `make readme` twice over, byte-identical the second time (no drift);
  `TestConfigDocReferenceMatchesExample` proven able to fail by drifting one line
  of the generated block, then restored; `make test`, `make lint` (0 issues) and
  `make coverage` (every package at or above 80%) green. README went from 680 to
  248 lines.

### Task 12: Rewrite README for consumers

- [x] rewrite `README.md` around: what problem it solves, what you get, quick
      start, configuration pointer, and licence/authorship
- [x] keep the architecture section short — a paragraph and a diagram, with the
      detail linked to the ADRs
- [x] link to `docs/configuration.md`, `docs/operations.md`,
      `docs/agent-operations.md` and `docs/adr/`
- [x] add a `LICENSE` file at the repository root: MIT, copyright Gennady
      Sokolachko, year 2026
- [x] add a short licence section to README pointing at `LICENSE`, and set the
      SPDX identifier `MIT` where the packaging metadata needs one
- [x] run tests - must pass before next task
- ➕ the "Guarantees" table moved to `docs/operations.md` as "Failure behaviour".
  It is the last reference-sized block Task 11 left in the README; what a
  consumer needs from it is now five bullets under "What you get", each linking
  to the table. The two ADRs that cited "the guarantees stated in the README"
  (0001, 0002) now cite the new location, and `AGENTS.md`'s layout line for
  `docs/operations.md` names the section.
- ➕ the architecture section became "How it works": one paragraph plus the
  diagram, with the three paragraphs of message-format detail folded into that
  paragraph and the depth left to the ADR links.
- ➕ the SPDX identifier went into the `Dockerfile` as OCI image labels
  (`org.opencontainers.image.licenses="MIT"` with title, description and
  source), the only packaging metadata this repository has — there is no
  `package.json` or equivalent. `docker/metadata-action` adds its tag-derived
  labels on top at release time.
- ➕ `LICENSE` also ships inside the image (`/LICENSE`) and inside every release
  archive, since MIT requires the notice to travel with copies of the software.
- Verified: `make test`, `make lint` (0 issues), `make coverage` (every package
  at or above 80%), `make readme` (no drift), `go test ./internal/archtest`
  (both doc tests green, so the compact driver table still names every shipped
  driver), `actionlint` v1.7.12 in a temp `GOBIN` over both workflows (0 errors,
  go.mod and go.sum untouched), and a `docker build` whose image reports the
  four labels, runs `antwatcher version`, and carries `/LICENSE`.

### Task 13: Screenshots

- [x] bring up `docker-compose.example.yml` and feed it the recorded fixtures so
      Grafana has data
- [x] capture the trace waterfall for one run in Tempo, and a Loki view showing
      the correlated records
- [x] store them under `docs/images/` and reference them from README
- [x] keep the files small enough that cloning the repository stays cheap
- [x] run tests - must pass before next task
- ⚠️ **the recorded fixtures cannot be fed verbatim.** They are dated
  `2026-09-02`; Loki accepted the OTLP push (`antwatcher_sink_events_total{
  sink="loki",result="ok"} 6`, no error in the collector or in Loki) and its
  label API then reported `service_name="antwatcher"`, but no `query_range` or
  `series` call over any window returned a line. Re-feeding the same fixtures
  with every ISO timestamp shifted into the present made all six queryable
  immediately. This is the failure mode `docs/agent-operations.md` describes from
  the other side: the backend has the events and the query window is not the
  problem — the backend dropped them for age. The shift was a throwaway script
  over a copy in `/tmp`; the fixtures themselves are untouched.
- ⚠️ trace IDs are deterministic from the run ID, so feeding the fixtures twice
  merges both sets of spans into one trace and the waterfall shows each step
  twice. The screenshots were taken after `docker compose down -v` and exactly
  one feed: `/api/search` reports one trace, `7 spans`.
- ➕ the six deliveries were posted with `openssl dgst -sha256 -hmac` signatures
  to the running receiver, in GitHub's order (run requested, run in_progress, job
  queued, job in_progress, job completed, run completed). The pipeline reported
  `webhooks_total{result="published"} 6` and, on the trace class, `ok 2` with
  `skipped 4` — the four events that are not a completed run or job, skipped by
  design as Task 10 recorded.
- ➕ both images are captured at 1440 CSS px on a 2× viewport and downsampled to
  1440 px wide: `trace-waterfall.png` 171 KB, `logs-correlated.png` 307 KB. The
  Grafana chrome around the data (nav menu, query editor, logs-volume panel) is
  collapsed so the frame is the data, not the UI.
- ➕ the Loki shot expands the *oldest* record rather than the newest, so all six
  records stay visible above the expansion, which shows the derived `trace` field
  linking to Tempo and the trace ID it resolves to. That is the correlation the
  README claims, in one frame.
- ➕ no feed script was committed. Putting one under `scripts/` would add a Go
  main package with no tests (`make coverage` gates every package outside `cmd/`
  at 80%) or a Python dependency this repository does not otherwise have; the
  procedure is recorded here instead. Regenerating the screenshots is a manual
  job by design — Post-Completion already notes they are checked by no test.
- Verified: `make test`, `make lint` (0 issues), `make coverage` (every package at
  or above 80%) and `make readme` (no drift) all green with the README change.

### Task 14: Verify acceptance criteria

- [x] verify every requirement in the Overview is implemented
- [x] run the full test suite
- [x] run `make lint` — zero issues
- [x] run `make coverage` — threshold met
- [x] run `actionlint` over every workflow
- [x] confirm `make readme` produces no drift
- The five strands of the Overview, checked against the tree rather than against
  the task list: **CI** is `.github/workflows/ci.yml`, one `ubuntu-latest` job on
  push-to-`main` and pull requests, calling `make lint`, `make test`,
  `make coverage` and `make check` with a dummy secret, with the Go version read
  from `go.mod`. **Releases** are `.github/workflows/release.yml` on `v*`: a
  `binaries` job with `contents: write` producing four archives plus
  `checksums.txt`, each archive carrying `README.md`, `LICENSE` and
  `antwatcher.example.yml`, and an `image` job with `packages: write` pushing a
  linux/amd64 + linux/arm64 image to ghcr.io with `docker/metadata-action` tags.
  **Agent instructions** are `AGENTS.md` and `docs/agent-operations.md`, with
  `CLAUDE.md` importing the former. **README** is consumer-facing at 260 lines
  and links `docs/configuration.md`, `docs/operations.md`,
  `docs/agent-operations.md` and all four ADRs, closing with an MIT licence
  section pointing at `LICENSE`. **Span prefix** is `PrefixRun = "run:"`
  (`internal/sink/trace/project.go:27`), used at line 115; a repository-wide grep
  for a `"workflow:` literal in Go and Markdown matches only this plan's own
  Task 7 note.
- Verified in one pass on 2026-09-06: `make test` — all 27 packages ok, race
  detector on; `make lint` — 0 issues on the pinned v2.13.2; `make coverage` —
  "every package at or above 80%", the minimum still `internal/sink/archive` at
  86.2%; `make readme` — regenerated and `git status` clean afterwards, so no
  drift; `actionlint` v1.7.12 over both workflows — exit 0, no output.
- ⚠️ `actionlint` is still not installed on this machine, so it was again run
  from a temp `GOBIN` with `GOFLAGS=-mod=mod`; `git status` was clean after the
  install, confirming `go.mod` and `go.sum` were untouched. Its `shellcheck`
  rule stays inactive, shellcheck being absent — the release workflow's shell
  blocks are therefore unlinted here, and CI is where they first run for real.

### Task 15: [Final] Documentation sweep

- [x] confirm README, `AGENTS.md`, `CLAUDE.md` and `docs/` agree with the code
- [x] add a CI status badge to README once the workflow has run at least once
- The sweep checked claims against the tree, not prose against prose: every
  markdown link in `README.md`, `AGENTS.md`, `CLAUDE.md`, `docs/*.md` and
  `docs/adr/*.md` resolves relative to its own file; every backticked path exists;
  every `antwatcher_` metric named in the docs is one of the seventeen declared in
  `internal/metrics/metrics.go`; every `make` target named anywhere is in the
  `Makefile`; the `/status` fields `docs/agent-operations.md` tells an agent to
  read (`ready`, `not_ready_reason`, `bus.connected`, `bus.warnings`,
  `requested_start_from`, `effective_start_from`, `stalled`, `version`, `commit`)
  and the six `antwatcher_webhooks_total{result}` values in its response-code
  table all exist verbatim; the README driver table, the `docs/configuration.md`
  matrices and `cmd/antwatcher/drivers.go` list the same two bus and six sink
  drivers; the flags, `:8080`, `127.0.0.1:9090`, `/webhook`, `antwatcher.events`
  and `sink-<name>` in the README match `internal/config/config.go` and
  `internal/sink/sink.go`; and AGENTS.md's linter, coverage-exclusion and
  go-github-surface claims match `.golangci.yml`, `scripts/check-coverage.sh` and
  `TestActionsRESTAPIIsNeverUsed`.
- Three disagreements found and fixed: `.github/workflows/ci.yml` credited README
  drift to `TestREADMEConfigReferenceMatchesExample`, which does not exist — the
  test is `TestConfigDocReferenceMatchesExample` and it guards
  `docs/configuration.md`, not the README; the README said all three compose build
  arguments fall back to `dev`, but `docker-compose.example.yml:35-37` falls back
  to `dev` only for `VERSION` and to `unknown` for `COMMIT` and `DATE`; and the
  AGENTS.md layout block had no `docs/images/`, added by Task 13.
- ⚠️ The badge was added before its precondition was met. `gh api
  repos/sokogen/antwatcher/actions/runs` reports `total_count: 0` and no
  registered workflows: `origin/antwatcher-core-pipeline` is still at `dbca8f9`,
  from before `.github/` existed, so CI has never run. The badge renders "no
  status" until the first push, and the repository is private, so it needs a
  logged-in viewer either way. Nothing else can be done from here — merging the
  branch to `main` is what resolves it.
- Verified: `make test` (all packages ok), `make lint` (0 issues), `make coverage`
  ("every package at or above 80%") and `make readme` (no drift — only the three
  files above are modified). Both workflows still parse as YAML; the ci.yml change
  is comment-only.

## Review Follow-Up

*Landed after Task 15, in the review iterations. Recorded here because the
Overview above promised no runtime behaviour change and these are exceptions to
it — none of them is scope the fifteen tasks planned.*

- `internal/config/url.go`: a `config.URL` type that masks userinfo on every
  render path (`-check`, `/status`, the startup log). `natsjs.Config.URL` changed
  from `string` to it. Scheme-less URLs are masked too — `url.Parse` reads
  `admin:pass@host:4222` as an opaque scheme and reports no userinfo.
- `internal/config/validate.go`: three `server.webhook_path` rules, rejecting
  what would panic or over-match on the `net/http` mux.
- `internal/bus/natsjs/stream.go`: `EnsureStream` refuses a stream that already
  carries other subjects instead of rewriting its subject list.
- `internal/bus/natsjs/embedded.go`: a process-wide reservation on `store_dir`,
  so two embedded servers cannot corrupt one JetStream file store.
- `internal/sink/stalled.go`: a 1000-entry cap on per-sink stalled tracking.
- `internal/ghclient/client.go`: `DiscoverHookID` stops on exhausted pagination.
- `internal/recovery/decide.go`: the exported `Plan` was removed; `group` stayed.
- `internal/sink/router.go`: `antwatcher_sink_last_success_timestamp_seconds` is
  created at zero with the other sink series, so the "no recent success" alert in
  `docs/operations.md` can fire for a sink that has never succeeded.
- `internal/receiver/handler.go`: the `503` response body no longer includes the
  driver's error text (broker addresses, stream names) — only `"publish failed"`
  and the delivery GUID. The detail moved exclusively to the log line, because
  the body reaches GitHub's delivery log, a wider audience than the operator.
- `internal/config/redact.go`: `secretKeyPattern` widened from
  `password|passwd` to `pass|pwd|bearer`, changing which keys the fallback
  redaction masks for driver blocks with no `Describer`.
- `internal/sink/archive/jsonl.go`: `sweep` matches finalized filenames by full
  grammar instead of a bare prefix test, fixing a bug where a sink named `raw`
  sharing a directory with `raw-2` would compress and delete `raw-2`'s files.
- `internal/bus/bustest/suite.go`: proving `DurablePublish` replays the broker's
  history, so a driver declaring it must declare `HistoricalReplay` too; the
  suite now says so instead of failing on the replay assertion.
- `internal/event/message.go`: `FromMessage` rejects a negative
  `repository_id`, not just a non-numeric one.
- `cmd/antwatcher/serve.go`: `closeBuilt` and `run` close `s.receiver`
  explicitly when shutdown beats `router.Running()`, so the webhook listener's
  socket is released instead of held until the process exits.

## Technical Details

**Coverage exclusions and why each is legitimate.** `cmd/...` because CLAUDE.md
scopes the 80 percent target to packages outside it. `internal/bus/bustest`
because it is the conformance suite every driver runs, and has no tests of its
own by design. `internal/archtest` because it declares no statements, so a
percentage is meaningless rather than low.

**Release permissions.** Both release jobs run on the built-in `GITHUB_TOKEN`:
`contents: write` to attach release artifacts, `packages: write` to push to
ghcr.io. No repository secret has to be created, which is the reason ghcr.io was
chosen over Docker Hub.

**Version stamping has three call sites** that must agree: `Makefile:8` for local
builds, `Dockerfile:9-13` for images, and the release workflow for tagged
artifacts. All three feed the same `-X main.version=...` ldflags into
`cmd/antwatcher/main.go:27-29`.

**Span prefix change is deliberately narrow.** Renaming the prefix does not
address the fact that every exported span is `SPAN_KIND_INTERNAL`, including the
run span. The OpenTelemetry CI/CD convention models a pipeline run as `SERVER`
and its tasks as `INTERNAL`, which would carry the level natively instead of
encoding it in the name. That is a larger change — it also pulls in
`cicd.pipeline.result` with its own value set, and `error.type` on failures — and
it is explicitly out of scope here. See Post-Completion.

## Post-Completion

*Items requiring manual intervention, a decision, or external systems — no
checkboxes, informational only.*

**Deferred design questions:**

- **Span kind and OpenTelemetry CI/CD semantics.** Exported spans currently
  declare no kind, so everything is `INTERNAL`. Adopting `cicd.*` and `vcs.*`
  attribute names, `SERVER` for the run span, `cicd.pipeline.result` and
  `error.type` would make antwatcher's traces legible to any tool that
  understands the convention. It requires deciding how GitHub conclusions with
  no standard equivalent — `cancelled`, `neutral`, `action_required`, `stale` —
  are mapped. The `cicd` namespace is at development stability, so names may
  still shift. Deferred by decision, not by oversight.
- **`service.name` is a hardcoded constant** (`internal/otlp/types.go:18`). Two
  antwatcher instances exporting to one backend are indistinguishable. Making it
  configurable is a small change waiting for a reason.

**Verification that cannot run in CI:**

- The release workflow is only exercised by pushing a real tag. Verify the first
  release manually: pull the published image, run `antwatcher version` inside it,
  and check the reported version matches the tag.
- Screenshots go stale as the UI changes; they are not checked by any test.

**External steps:**

- Enable GitHub Actions for the repository if it is not already enabled.
- The first push of this branch will trigger CI; expect to iterate once on
  runner-specific failures that do not reproduce locally.

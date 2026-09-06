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
which changes one span name prefix.

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
- [x] verify the workflow parses with `actionlint`
- [x] run tests - must pass before next task

### Task 4: Cross-compiling Dockerfile

- [ ] change the build stage to `FROM --platform=$BUILDPLATFORM golang:1.27-alpine`
      and pass `TARGETOS`/`TARGETARCH` into `go build` so buildx cross-compiles
      instead of emulating
- [ ] keep the existing `VERSION`/`COMMIT`/`DATE` build args and ldflags stamping
- [ ] verify `docker build` still produces a working image for the host platform
- [ ] verify `antwatcher version` inside the image reports the stamped version,
      not `dev`
- [ ] run tests - must pass before next task

### Task 5: Release workflow — binaries

- [ ] create `.github/workflows/release.yml` triggered on tags matching `v*`
- [ ] build binaries for linux and darwin on amd64 and arm64, stamping the
      version from the tag through the existing ldflags
- [ ] produce one archive per target plus a `checksums.txt`
- [ ] attach the artifacts to the GitHub Release, using `permissions: contents: write`
- [ ] verify the workflow parses with `actionlint`
- [ ] run tests - must pass before next task

### Task 6: Release workflow — container image

- [ ] add a job publishing a multi-arch image (linux/amd64, linux/arm64) to
      `ghcr.io`, using `permissions: packages: write` and the built-in
      `GITHUB_TOKEN` so no extra secret is needed
- [ ] derive image tags from the git tag with `docker/metadata-action`
      (`v1.2.3`, `1.2.3`, `1.2`, `latest`)
- [ ] pass `VERSION`/`COMMIT`/`DATE` build args so the image reports its version
- [ ] verify the workflow parses with `actionlint`
- [ ] run tests - must pass before next task

### Task 7: Rename the run span prefix

- [ ] rename `PrefixWorkflow` to `PrefixRun` with value `run:` in
      `internal/sink/trace/project.go:27`
- [ ] update the doc comment on `Project` (`project.go:63-66`) which spells the
      old name out
- [ ] leave `job:` and `step:` unchanged, leave every attribute on `github.*`,
      and do not add `antwatcher.kind` — this task is the prefix only
- [ ] update trace projection tests asserting the old prefix
- [ ] grep README and `docs/` for `workflow:` used as a span name and update
- [ ] run tests - must pass before next task

### Task 8: Compose passes the version through

- [ ] add `build.args` to `docker-compose.example.yml` so `VERSION`, `COMMIT` and
      `DATE` reach the image instead of defaulting to `dev`
- [ ] document in the file header that the values come from the environment and
      fall back to `dev` for a plain `docker compose up`
- [ ] verify a compose build reports a stamped version at `/status`
- [ ] run tests - must pass before next task

### Task 9: AGENTS.md for repository work

- [ ] create `AGENTS.md` at the repository root covering build/test/lint
      commands, the layout, the architecture rules enforced by `internal/archtest`,
      the error and registration conventions, and how to add a bus or sink driver
- [ ] keep it factual and current: every command in it must actually work
- [ ] replace the body of `CLAUDE.md` with a pointer to `AGENTS.md` plus anything
      genuinely Claude-specific, so the two cannot drift apart
- [ ] verify every command quoted in `AGENTS.md` by running it
- [ ] run tests - must pass before next task

### Task 10: docs/agent-operations.md for running the service

- [ ] create `docs/agent-operations.md` for an agent that installs, configures
      and debugs antwatcher in someone else's project
- [ ] cover: choosing a bus and sinks, writing a config, `-check`, the webhook
      setup on GitHub, and the fact that TLS must be terminated in front
- [ ] cover debugging by observable signal: `/status`, `/readyz`, the
      `antwatcher_webhooks_total` result labels, `antwatcher_sink_events_total`,
      consumer lag, and what a `401` versus a `503` on the webhook path means
- [ ] include the failure modes seen in practice: a signature mismatch is a
      wrong secret, not a bug; events reaching a log backend but not appearing
      means the query window predates the event timestamps
- [ ] run tests - must pass before next task

### Task 11: Move the reference material out of README

- [ ] create `docs/configuration.md` holding the full configuration reference,
      and `docs/operations.md` holding endpoints, metrics, alert rules, sizing
      and single-replica notes
- [ ] update `scripts/readme-config.sh` to write the config block into
      `docs/configuration.md` instead of `README.md`
- [ ] update `TestREADMEConfigReferenceMatchesExample` to read the new file, and
      rename it to match — the test is the reason this cannot be done by moving
      text alone
- [ ] decide and implement how `TestREADMEListsEveryShippedDriver` is satisfied:
      either keep a compact driver table in README, or point the test at
      `docs/configuration.md`; record the choice in the test's doc comment
- [ ] run `make readme` and confirm no drift
- [ ] run tests - must pass before next task

### Task 12: Rewrite README for consumers

- [ ] rewrite `README.md` around: what problem it solves, what you get, quick
      start, configuration pointer, and licence/authorship
- [ ] keep the architecture section short — a paragraph and a diagram, with the
      detail linked to the ADRs
- [ ] link to `docs/configuration.md`, `docs/operations.md`,
      `docs/agent-operations.md` and `docs/adr/`
- [ ] add a `LICENSE` file at the repository root: MIT, copyright Gennady
      Sokolachko, year 2026
- [ ] add a short licence section to README pointing at `LICENSE`, and set the
      SPDX identifier `MIT` where the packaging metadata needs one
- [ ] run tests - must pass before next task

### Task 13: Screenshots

- [ ] bring up `docker-compose.example.yml` and feed it the recorded fixtures so
      Grafana has data
- [ ] capture the trace waterfall for one run in Tempo, and a Loki view showing
      the correlated records
- [ ] store them under `docs/images/` and reference them from README
- [ ] keep the files small enough that cloning the repository stays cheap
- [ ] run tests - must pass before next task

### Task 14: Verify acceptance criteria

- [ ] verify every requirement in the Overview is implemented
- [ ] run the full test suite
- [ ] run `make lint` — zero issues
- [ ] run `make coverage` — threshold met
- [ ] run `actionlint` over every workflow
- [ ] confirm `make readme` produces no drift

### Task 15: [Final] Documentation sweep

- [ ] confirm README, `AGENTS.md`, `CLAUDE.md` and `docs/` agree with the code
- [ ] add a CI status badge to README once the workflow has run at least once

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

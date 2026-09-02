// Package model is the canonical execution model of antwatcher: the Run, Job,
// and Step entities every destination class projects from, the Normalize
// function that builds them from a webhook envelope, and the deterministic
// trace, span, and record identifiers that make sinks idempotent.
//
// GitHub payload semantics live only here. Sinks never parse GitHub JSON and
// never branch on event names; they receive an Execution and project it.
//
// The model is sparse on purpose. A workflow_run event and a workflow_job event
// describe different objects and carry different fields: a job event has no
// workflow_id, workflow_path, trigger event, or actor, and a run event has no
// runner or step information. Nothing is ever fetched from the Actions REST API
// to fill the gaps. Fields that a payload may legitimately lack are pointers and
// stay nil when absent; consumers join run and job/step entities on
// RepositoryID + RunID + RunAttempt when they need both sides.
package model

import (
	"time"

	"github.com/sokogen/antwatcher/internal/event"
)

// Kind tells which entity an Execution carries.
type Kind int

// Execution kinds.
const (
	// KindNone: the event is not a workflow_run or workflow_job event. Run and
	// Job are nil; only the envelope is available.
	KindNone Kind = iota
	// KindRun: a workflow_run event. Run is set.
	KindRun
	// KindJob: a workflow_job event. Job is set, including its Steps.
	KindJob
)

// String returns the kind as the lowercase word used in logs and attributes.
func (k Kind) String() string {
	switch k {
	case KindNone:
		return "none"
	case KindRun:
		return "run"
	case KindJob:
		return "job"
	default:
		return "unknown"
	}
}

// Execution is the normalized view of one webhook delivery: the entity the
// event describes (by Kind) plus the envelope it came from. Exactly one of Run
// and Job is non-nil for KindRun and KindJob; both are nil for KindNone.
type Execution struct {
	Kind     Kind
	Run      *Run
	Job      *Job
	Envelope event.Envelope
}

// Run is one attempt of a workflow run as reported by a workflow_run event.
//
// Identity is RepositoryID + RunID + RunAttempt; every workflow_run event for
// the same attempt (requested, in_progress, completed) maps to the same Run
// identity with a later state. A re-run produces a new RunAttempt and therefore
// a new identity and a new trace.
type Run struct {
	// RepositoryID and Repository ("owner/name") come from the payload's
	// repository object. Always present.
	RepositoryID int64
	Repository   string

	// WorkflowID is the numeric id of the workflow definition. Always present.
	WorkflowID int64
	// WorkflowName is the workflow's display name. GitHub documents it as
	// nullable, so it is nil when the payload carries null.
	WorkflowName *string
	// WorkflowPath is the workflow file path inside the repository, for example
	// ".github/workflows/ci.yml". Always present.
	WorkflowPath string

	// RunID, RunNumber, and RunAttempt identify the run. RunNumber is the
	// human-visible counter; RunAttempt starts at 1 and increments on re-run.
	// Always present.
	RunID      int64
	RunNumber  int64
	RunAttempt int64

	// Status is the run status ("queued", "in_progress", "completed", ...).
	// Empty when the payload carries null; StatusRank treats it as unknown.
	Status string
	// Conclusion is set only once the run completed ("success", "failure",
	// "cancelled", "timed_out", ...). Nil before completion.
	Conclusion *string

	// CreatedAt and UpdatedAt are GitHub's timestamps for the run object.
	// Always present in GitHub payloads; a zero value only occurs for
	// defensive fallback when the field is missing.
	CreatedAt time.Time
	UpdatedAt time.Time
	// StartedAt is run_started_at: when the attempt actually started. Nil when
	// the payload lacks it.
	StartedAt *time.Time

	// TriggerEvent is the event that triggered the run ("push",
	// "pull_request", "schedule", ...). Always present.
	TriggerEvent string
	// HeadBranch is nullable in GitHub payloads (for example for some
	// workflow_dispatch runs on a tag) and nil in that case.
	HeadBranch *string
	// HeadSHA is the commit the run was executed against. Always present.
	HeadSHA string
	// Actor is the login of the user who created the run (payload actor.login).
	// Nil when the payload carries no actor.
	Actor *string
	// HTMLURL is the run page on GitHub. Always present.
	HTMLURL string
}

// Job is one job of a workflow run attempt as reported by a workflow_job event,
// including the steps reported in that same event.
//
// Identity is JobID; the job joins its run on RepositoryID + RunID + RunAttempt.
// A job event never carries workflow_id, workflow_path, the trigger event, or
// the actor: those fields exist only on Run.
type Job struct {
	// RepositoryID and Repository ("owner/name") come from the payload's
	// repository object. Always present.
	RepositoryID int64
	Repository   string

	// RunID and RunAttempt identify the run attempt the job belongs to and are
	// the join key to Run. Always present.
	RunID      int64
	RunAttempt int64

	// JobID is the job's numeric id (the check run id). Always present.
	JobID int64
	// Name is the job's display name. Always present.
	Name string
	// WorkflowName is the workflow's display name as carried on the job
	// (workflow_name). Nullable in GitHub payloads, nil in that case.
	WorkflowName *string

	// Status is the job status ("queued", "waiting", "in_progress",
	// "completed", ...). Empty when the payload carries null.
	Status string
	// Conclusion is set only once the job completed. Nil before completion.
	Conclusion *string

	// CreatedAt is when the job was created. Always present in GitHub
	// payloads; zero only for defensive fallback.
	CreatedAt time.Time
	// StartedAt is when the job started. GitHub sends it on every action
	// (equal to CreatedAt while queued); nil only when the payload lacks it.
	StartedAt *time.Time
	// CompletedAt is set only once the job completed. Nil before completion.
	CompletedAt *time.Time

	// RunnerName and RunnerGroup are set once a runner picked the job up
	// (runner_name, runner_group_name). Nil while queued.
	RunnerName  *string
	RunnerGroup *string
	// Labels are the runner labels requested by the job. Nil when absent.
	Labels []string

	// HeadBranch is nullable in GitHub payloads and nil in that case.
	HeadBranch *string
	// HeadSHA is the commit the job runs against. Always present.
	HeadSHA string
	// HTMLURL is the job page on GitHub. Always present.
	HTMLURL string

	// Steps are the steps reported in this event, in payload order. Empty
	// while queued; grows as the job progresses.
	Steps []Step
}

// Step is one step of a job as reported inside a workflow_job event. Steps have
// no identity of their own beyond (JobID, Number).
type Step struct {
	// Number is the 1-based step index within the job. Always present.
	Number int64
	// Name is the step's display name. Always present.
	Name string
	// Status is the step status ("queued", "in_progress", "completed").
	// Empty when the payload carries null.
	Status string
	// Conclusion is set once the step completed ("success", "failure",
	// "skipped", ...). Nil before completion.
	Conclusion *string
	// StartedAt and CompletedAt are nil until the step reached that state. A
	// skipped step completes without ever starting and keeps both nil.
	StartedAt   *time.Time
	CompletedAt *time.Time
}

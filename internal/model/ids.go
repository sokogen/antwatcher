package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Deterministic identifiers.
//
// Every id is a SHA-256 over a namespaced string built from GitHub's numeric
// identifiers, so the same entity yields the same id in every process, on every
// redelivery, and after a replay. That determinism is what makes at-least-once
// delivery safe: a trace backend receives the same span twice and stores one,
// the analytics view collapses records on record_id.
//
//	trace_id     = sha256("antwatcher:trace:"  + repository_id + ":" + run_id + ":" + run_attempt)[:16]
//	run_span_id  = sha256("antwatcher:run:"    + run_id + ":" + run_attempt)[:8]
//	job_span_id  = sha256("antwatcher:job:"    + job_id)[:8]
//	step_span_id = sha256("antwatcher:step:"   + job_id + ":" + step_number)[:8]
//	record_id    = hex(sha256("antwatcher:record:" + kind + ":" + id [+ ":" + id ...]))

const (
	nsTrace  = "antwatcher:trace:"
	nsRun    = "antwatcher:run:"
	nsJob    = "antwatcher:job:"
	nsStep   = "antwatcher:step:"
	nsRecord = "antwatcher:record:"
)

// RecordKind is the analytics entity kind used in RecordID. It differs from
// Kind because steps are separate records while they are not separate events.
type RecordKind string

// Analytics record kinds.
const (
	RecordRun  RecordKind = "run"
	RecordJob  RecordKind = "job"
	RecordStep RecordKind = "step"
)

// TraceID is the trace identifier shared by a run attempt and all its jobs.
func TraceID(repositoryID, runID, runAttempt int64) [16]byte {
	sum := sha256.Sum256([]byte(nsTrace + join(repositoryID, runID, runAttempt)))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

// RunSpanID is the span identifier of the workflow (root) span of a run attempt.
func RunSpanID(runID, runAttempt int64) [8]byte {
	return spanID(nsRun + join(runID, runAttempt))
}

// JobSpanID is the span identifier of a job span.
func JobSpanID(jobID int64) [8]byte {
	return spanID(nsJob + join(jobID))
}

// StepSpanID is the span identifier of a step span inside a job.
func StepSpanID(jobID, stepNumber int64) [8]byte {
	return spanID(nsStep + join(jobID, stepNumber))
}

// RecordID is the analytics identity of an entity: hex SHA-256 over the kind
// and the ids that identify it. Callers pass the ids in join order:
//
//	RecordID(RecordRun,  repositoryID, runID, runAttempt)
//	RecordID(RecordJob,  repositoryID, runID, runAttempt, jobID)
//	RecordID(RecordStep, repositoryID, runID, runAttempt, jobID, stepNumber)
//
// The id names the entity, not the event: every event about the same entity
// produces the same record id, which is what the deduplicating view relies on.
func RecordID(kind RecordKind, ids ...int64) string {
	sum := sha256.Sum256([]byte(nsRecord + string(kind) + ":" + join(ids...)))
	return hex.EncodeToString(sum[:])
}

func spanID(s string) [8]byte {
	sum := sha256.Sum256([]byte(s))
	var id [8]byte
	copy(id[:], sum[:8])
	return id
}

func join(ids ...int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(parts, ":")
}

// TraceID returns the trace the run's spans belong to.
func (r *Run) TraceID() [16]byte { return TraceID(r.RepositoryID, r.RunID, r.RunAttempt) }

// SpanID returns the id of the run's root span.
func (r *Run) SpanID() [8]byte { return RunSpanID(r.RunID, r.RunAttempt) }

// RecordID returns the run's analytics record identity.
func (r *Run) RecordID() string { return RecordID(RecordRun, r.RepositoryID, r.RunID, r.RunAttempt) }

// TraceID returns the trace the job's spans belong to: the trace of its run
// attempt, so run and job spans line up without any lookup.
func (j *Job) TraceID() [16]byte { return TraceID(j.RepositoryID, j.RunID, j.RunAttempt) }

// SpanID returns the id of the job span.
func (j *Job) SpanID() [8]byte { return JobSpanID(j.JobID) }

// ParentSpanID returns the id of the run's root span, the parent of the job
// span. It is computed, not looked up, so a job can be exported before its run.
func (j *Job) ParentSpanID() [8]byte { return RunSpanID(j.RunID, j.RunAttempt) }

// RecordID returns the job's analytics record identity.
func (j *Job) RecordID() string {
	return RecordID(RecordJob, j.RepositoryID, j.RunID, j.RunAttempt, j.JobID)
}

// SpanID returns the id of the step span within job.
func (s *Step) SpanID(job *Job) [8]byte { return StepSpanID(job.JobID, s.Number) }

// RecordID returns the step's analytics record identity within job.
func (s *Step) RecordID(job *Job) string {
	return RecordID(RecordStep, job.RepositoryID, job.RunID, job.RunAttempt, job.JobID, s.Number)
}

// EventTime is the time of the state transition the event reports, chosen by
// the entity's status:
//
//	Run:  completed → UpdatedAt, in_progress → StartedAt, anything else → CreatedAt
//	Job:  completed → CompletedAt, in_progress → StartedAt, anything else → CreatedAt
//	none: the envelope's ReceivedAt
//
// When the chosen field is missing (nil or zero) the envelope's ReceivedAt is
// used, so the result is never zero. Analytics orders duplicates by it and logs
// use it as the record time.
func EventTime(exec Execution) time.Time {
	received := exec.Envelope.ReceivedAt
	switch exec.Kind {
	case KindRun:
		if exec.Run != nil {
			return runEventTime(exec.Run, received)
		}
	case KindJob:
		if exec.Job != nil {
			return jobEventTime(exec.Job, received)
		}
	case KindNone:
	}
	return received
}

// StepEventTime is the time of the transition a step reports inside a job
// event: completed → CompletedAt, in_progress → StartedAt, anything else (or a
// missing field) → the job event's EventTime. For a non-job execution it is
// EventTime(exec).
func StepEventTime(exec Execution, step Step) time.Time {
	jobTime := EventTime(exec)
	if exec.Kind != KindJob || exec.Job == nil {
		return jobTime
	}
	switch step.Status {
	case StatusCompleted:
		return orTime(step.CompletedAt, jobTime)
	case StatusInProgress:
		return orTime(step.StartedAt, jobTime)
	default:
		return jobTime
	}
}

func runEventTime(r *Run, received time.Time) time.Time {
	switch r.Status {
	case StatusCompleted:
		return orZero(r.UpdatedAt, received)
	case StatusInProgress:
		return orTime(r.StartedAt, received)
	default:
		return orZero(r.CreatedAt, received)
	}
}

func jobEventTime(j *Job, received time.Time) time.Time {
	switch j.Status {
	case StatusCompleted:
		return orTime(j.CompletedAt, received)
	case StatusInProgress:
		return orTime(j.StartedAt, received)
	default:
		return orZero(j.CreatedAt, received)
	}
}

func orTime(t *time.Time, fallback time.Time) time.Time {
	if t == nil || t.IsZero() {
		return fallback
	}
	return *t
}

func orZero(t, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}
	return t
}

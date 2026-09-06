// Package trace is the trace destination class: it projects completed runs
// and jobs of the canonical model into OTLP spans and hands them to a driver
// through the Exporter contract.
//
// One workflow run attempt is one trace. The run's root span covers the whole
// attempt, each job is a child span of the root, and each step that ran is a
// child span of its job. Every id is derived from GitHub's numeric identifiers
// (see the model package), so a job exported before its run, or a run
// redelivered twice, still lines up into the same trace without any lookup.
//
// Only completed entities become spans: a span needs an end time, and GitHub
// reports the final timestamps and conclusions only with the completed event.
// Earlier events (requested, queued, in_progress) are skipped; the log class
// covers them.
package trace

import (
	"time"

	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Span name prefixes.
const (
	PrefixRun  = "run:"
	PrefixJob  = "job:"
	PrefixStep = "step:"
)

// Attribute keys set on the exported spans.
const (
	AttrRepository     = "github.repository"
	AttrWorkflowName   = "github.workflow.name"
	AttrWorkflowPath   = "github.workflow.path"
	AttrRunID          = "github.run_id"
	AttrRunNumber      = "github.run_number"
	AttrRunAttempt     = "github.run_attempt"
	AttrEvent          = "github.event"
	AttrHeadBranch     = "github.head_branch"
	AttrHeadSHA        = "github.head_sha"
	AttrActor          = "github.actor"
	AttrConclusion     = "github.conclusion"
	AttrHTMLURL        = "github.html_url"
	AttrJobID          = "github.job_id"
	AttrRunnerName     = "github.runner_name"
	AttrRunnerGroup    = "github.runner_group"
	AttrLabels         = "github.labels"
	AttrStepNumber     = "github.step.number"
	AttrStepConclusion = "github.step.conclusion"
)

// GitHub conclusions that map to a span status.
const (
	conclusionSuccess  = "success"
	conclusionFailure  = "failure"
	conclusionTimedOut = "timed_out"
)

// Project turns an execution into the spans it contributes to its trace.
//
//   - A completed run yields its root span "run:<name>" from the run's
//     start (run_started_at, or created_at when GitHub omitted it) to its
//     updated_at, with the run attributes.
//   - A completed job yields the job span "job:<name>" parented to the run's
//     root span (computed, not looked up), from started_at to completed_at,
//     followed by one child span "step:<name>" per step that has both
//     timestamps. A skipped step never started and produces no span.
//   - Everything else (earlier lifecycle events, non-workflow events) yields
//     sink.ErrSkipped.
//
// Span status is Error for a "failure" or "timed_out" conclusion, Ok for
// "success", and Unset for anything else (cancelled, skipped, neutral, ...).
// The result is deterministic: the same execution always yields the same
// spans in the same order, which is what makes redelivery idempotent.
func Project(exec model.Execution) ([]otlp.Span, error) {
	switch exec.Kind {
	case model.KindRun:
		if exec.Run != nil && exec.Run.Status == model.StatusCompleted {
			return []otlp.Span{runSpan(exec.Run)}, nil
		}
	case model.KindJob:
		if exec.Job != nil && exec.Job.Status == model.StatusCompleted {
			return jobSpans(exec), nil
		}
	case model.KindNone:
	}
	return nil, sink.ErrSkipped
}

func runSpan(r *model.Run) otlp.Span {
	attrs := map[string]any{
		AttrRepository:   r.Repository,
		AttrWorkflowPath: r.WorkflowPath,
		AttrRunID:        r.RunID,
		AttrRunNumber:    r.RunNumber,
		AttrRunAttempt:   r.RunAttempt,
		AttrEvent:        r.TriggerEvent,
		AttrHeadSHA:      r.HeadSHA,
		AttrHTMLURL:      r.HTMLURL,
	}
	setString(attrs, AttrWorkflowName, r.WorkflowName)
	setString(attrs, AttrHeadBranch, r.HeadBranch)
	setString(attrs, AttrActor, r.Actor)
	setString(attrs, AttrConclusion, r.Conclusion)

	name := r.WorkflowPath
	if r.WorkflowName != nil {
		name = *r.WorkflowName
	}
	return otlp.Span{
		TraceID:    r.TraceID(),
		SpanID:     r.SpanID(),
		Name:       PrefixRun + name,
		Kind:       otlp.SpanKindInternal,
		Start:      orTime(r.StartedAt, r.CreatedAt),
		End:        r.UpdatedAt,
		Attributes: attrs,
		Status:     status(r.Conclusion),
	}
}

func jobSpans(exec model.Execution) []otlp.Span {
	j := exec.Job
	attrs := map[string]any{
		AttrRepository: j.Repository,
		AttrRunID:      j.RunID,
		AttrRunAttempt: j.RunAttempt,
		AttrJobID:      j.JobID,
		AttrHeadSHA:    j.HeadSHA,
		AttrHTMLURL:    j.HTMLURL,
	}
	setString(attrs, AttrWorkflowName, j.WorkflowName)
	setString(attrs, AttrHeadBranch, j.HeadBranch)
	setString(attrs, AttrRunnerName, j.RunnerName)
	setString(attrs, AttrRunnerGroup, j.RunnerGroup)
	setString(attrs, AttrConclusion, j.Conclusion)
	if len(j.Labels) > 0 {
		attrs[AttrLabels] = append([]string(nil), j.Labels...)
	}

	traceID := j.TraceID()
	jobSpanID := j.SpanID()
	spans := make([]otlp.Span, 0, 1+len(j.Steps))
	spans = append(spans, otlp.Span{
		TraceID:      traceID,
		SpanID:       jobSpanID,
		ParentSpanID: j.ParentSpanID(),
		Name:         PrefixJob + j.Name,
		Kind:         otlp.SpanKindInternal,
		Start:        orTime(j.StartedAt, j.CreatedAt),
		End:          orTime(j.CompletedAt, model.EventTime(exec)),
		Attributes:   attrs,
		Status:       status(j.Conclusion),
	})
	for i := range j.Steps {
		s := &j.Steps[i]
		if s.StartedAt == nil || s.CompletedAt == nil {
			continue
		}
		stepAttrs := map[string]any{AttrStepNumber: s.Number}
		setString(stepAttrs, AttrStepConclusion, s.Conclusion)
		spans = append(spans, otlp.Span{
			TraceID:      traceID,
			SpanID:       s.SpanID(j),
			ParentSpanID: jobSpanID,
			Name:         PrefixStep + s.Name,
			Kind:         otlp.SpanKindInternal,
			Start:        *s.StartedAt,
			End:          *s.CompletedAt,
			Attributes:   stepAttrs,
			Status:       status(s.Conclusion),
		})
	}
	return spans
}

// status maps a GitHub conclusion to a span status. Error carries the
// conclusion as its message so a trace UI shows why.
func status(conclusion *string) otlp.Status {
	if conclusion == nil {
		return otlp.Status{}
	}
	switch *conclusion {
	case conclusionSuccess:
		return otlp.Status{Code: otlp.StatusOk}
	case conclusionFailure, conclusionTimedOut:
		return otlp.Status{Code: otlp.StatusError, Message: *conclusion}
	default:
		return otlp.Status{}
	}
}

func setString(attrs map[string]any, key string, v *string) {
	if v != nil {
		attrs[key] = *v
	}
}

func orTime(t *time.Time, fallback time.Time) time.Time {
	if t == nil || t.IsZero() {
		return fallback
	}
	return *t
}

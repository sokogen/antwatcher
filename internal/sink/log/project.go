// Package log is the log destination class: it projects every event of the
// canonical model into one OTLP log record and hands it to a driver through
// the Writer contract.
//
// Unlike the trace class, nothing is skipped: a queued job, a requested run,
// a ping, or an event antwatcher does not model at all still yields a record,
// so the log stream is a complete timeline of what GitHub delivered. Records
// of runs and jobs carry the deterministic trace and span ids of the model,
// which is what lets a log UI jump from a line to its trace and back.
package log

import (
	"strings"
	"time"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
)

// Attribute keys carried by every record: the envelope fields.
const (
	AttrDeliveryGUID  = "github.delivery_guid"
	AttrHookID        = "github.hook_id"
	AttrWebhookEvent  = "github.webhook.event"
	AttrWebhookAction = "github.webhook.action"
	AttrRepositoryID  = "github.repository_id"
	AttrRepository    = "github.repository"
	AttrKind          = "antwatcher.kind"
	AttrReceivedAt    = "antwatcher.received_at"
	AttrSchemaVersion = "antwatcher.schema_version"
)

// Attribute keys carried by run and job records. The spelling matches the
// trace class so a query written for spans also finds the log lines.
const (
	AttrWorkflowID   = "github.workflow.id"
	AttrWorkflowName = "github.workflow.name"
	AttrWorkflowPath = "github.workflow.path"
	AttrRunID        = "github.run_id"
	AttrRunNumber    = "github.run_number"
	AttrRunAttempt   = "github.run_attempt"
	AttrJobID        = "github.job_id"
	AttrJobName      = "github.job.name"
	AttrStatus       = "github.status"
	AttrConclusion   = "github.conclusion"
	AttrEvent        = "github.event"
	AttrHeadBranch   = "github.head_branch"
	AttrHeadSHA      = "github.head_sha"
	AttrActor        = "github.actor"
	AttrRunnerName   = "github.runner_name"
	AttrRunnerGroup  = "github.runner_group"
	AttrLabels       = "github.labels"
	AttrHTMLURL      = "github.html_url"
)

// Attribute keys summarising the steps reported in a job event.
const (
	AttrStepsTotal     = "github.steps.total"
	AttrStepsCompleted = "github.steps.completed"
	AttrStepsFailed    = "github.steps.failed"
	AttrStepsSkipped   = "github.steps.skipped"
)

// Severity texts written next to the severity numbers.
const (
	SeverityTextInfo  = "INFO"
	SeverityTextWarn  = "WARN"
	SeverityTextError = "ERROR"
)

// GitHub conclusions that change the severity.
const (
	conclusionFailure   = "failure"
	conclusionTimedOut  = "timed_out"
	conclusionCancelled = "cancelled"
	conclusionSkipped   = "skipped"
)

// Project turns an execution into its log record. It never fails and never
// skips: every event has a record.
//
//   - Time is the model's event time (the transition the event reports, or
//     the envelope's received_at when the payload lacks it); ObservedTime is
//     received_at.
//   - Body is "<event> <action>: <name>", where name is the workflow name
//     (or its path when GitHub sent a null name) for runs and the job name
//     for jobs. Events without an entity have no name part, and events
//     without an action have no action part ("ping").
//   - Severity is Error for a failure or timed_out conclusion, Warn for
//     cancelled, Info otherwise (including everything not yet concluded).
//   - Attributes are the envelope fields for every record, plus the entity
//     fields for runs and jobs; nil optional fields are omitted. Job records
//     add step counts so a failing job is visible without its step spans.
//   - TraceID and SpanID are the model's ids for runs and jobs and zero
//     otherwise, so a record correlates with the span the trace class emits
//     for the same entity.
//
// The result is deterministic: the same execution always yields the same
// record, which keeps redelivery idempotent for destinations that dedupe.
func Project(exec model.Execution) otlp.LogRecord {
	env := exec.Envelope
	rec := otlp.LogRecord{
		Time:         model.EventTime(exec),
		ObservedTime: env.ReceivedAt,
		Attributes:   envelopeAttributes(env, exec.Kind),
	}
	rec.Severity, rec.SeverityText = severity(nil)

	var name string
	switch exec.Kind {
	case model.KindRun:
		if r := exec.Run; r != nil {
			name = runName(r)
			runAttributes(rec.Attributes, r)
			rec.Severity, rec.SeverityText = severity(r.Conclusion)
			rec.TraceID = r.TraceID()
			rec.SpanID = r.SpanID()
		}
	case model.KindJob:
		if j := exec.Job; j != nil {
			name = j.Name
			jobAttributes(rec.Attributes, j)
			rec.Severity, rec.SeverityText = severity(j.Conclusion)
			rec.TraceID = j.TraceID()
			rec.SpanID = j.SpanID()
		}
	case model.KindNone:
	}
	rec.Body = body(env, name)
	return rec
}

// body renders "<event> <action>: <name>" and drops the parts that are empty.
func body(env event.Envelope, name string) string {
	var b strings.Builder
	b.WriteString(env.Event)
	if env.Action != "" {
		b.WriteByte(' ')
		b.WriteString(env.Action)
	}
	if name != "" {
		b.WriteString(": ")
		b.WriteString(name)
	}
	return b.String()
}

func runName(r *model.Run) string {
	if r.WorkflowName != nil {
		return *r.WorkflowName
	}
	return r.WorkflowPath
}

func envelopeAttributes(env event.Envelope, kind model.Kind) map[string]any {
	attrs := map[string]any{
		AttrDeliveryGUID:  env.DeliveryGUID,
		AttrWebhookEvent:  env.Event,
		AttrKind:          kind.String(),
		AttrReceivedAt:    env.ReceivedAt.UTC().Format(time.RFC3339Nano),
		AttrSchemaVersion: int64(env.SchemaVersion),
	}
	if env.Action != "" {
		attrs[AttrWebhookAction] = env.Action
	}
	if env.HookID != "" {
		attrs[AttrHookID] = env.HookID
	}
	if env.RepositoryID != 0 {
		attrs[AttrRepositoryID] = env.RepositoryID
	}
	if env.Repository != "" {
		attrs[AttrRepository] = env.Repository
	}
	return attrs
}

func runAttributes(attrs map[string]any, r *model.Run) {
	attrs[AttrRepositoryID] = r.RepositoryID
	attrs[AttrRepository] = r.Repository
	attrs[AttrWorkflowID] = r.WorkflowID
	attrs[AttrWorkflowPath] = r.WorkflowPath
	attrs[AttrRunID] = r.RunID
	attrs[AttrRunNumber] = r.RunNumber
	attrs[AttrRunAttempt] = r.RunAttempt
	attrs[AttrStatus] = r.Status
	attrs[AttrEvent] = r.TriggerEvent
	attrs[AttrHeadSHA] = r.HeadSHA
	attrs[AttrHTMLURL] = r.HTMLURL
	setString(attrs, AttrWorkflowName, r.WorkflowName)
	setString(attrs, AttrConclusion, r.Conclusion)
	setString(attrs, AttrHeadBranch, r.HeadBranch)
	setString(attrs, AttrActor, r.Actor)
}

func jobAttributes(attrs map[string]any, j *model.Job) {
	attrs[AttrRepositoryID] = j.RepositoryID
	attrs[AttrRepository] = j.Repository
	attrs[AttrRunID] = j.RunID
	attrs[AttrRunAttempt] = j.RunAttempt
	attrs[AttrJobID] = j.JobID
	attrs[AttrJobName] = j.Name
	attrs[AttrStatus] = j.Status
	attrs[AttrHeadSHA] = j.HeadSHA
	attrs[AttrHTMLURL] = j.HTMLURL
	setString(attrs, AttrWorkflowName, j.WorkflowName)
	setString(attrs, AttrConclusion, j.Conclusion)
	setString(attrs, AttrHeadBranch, j.HeadBranch)
	setString(attrs, AttrRunnerName, j.RunnerName)
	setString(attrs, AttrRunnerGroup, j.RunnerGroup)
	if len(j.Labels) > 0 {
		attrs[AttrLabels] = append([]string(nil), j.Labels...)
	}

	var completed, failed, skipped int64
	for i := range j.Steps {
		s := &j.Steps[i]
		if s.Status == model.StatusCompleted {
			completed++
		}
		if s.Conclusion == nil {
			continue
		}
		switch *s.Conclusion {
		case conclusionFailure, conclusionTimedOut:
			failed++
		case conclusionSkipped:
			skipped++
		}
	}
	attrs[AttrStepsTotal] = int64(len(j.Steps))
	attrs[AttrStepsCompleted] = completed
	attrs[AttrStepsFailed] = failed
	attrs[AttrStepsSkipped] = skipped
}

// severity maps a conclusion to the record severity: Error for failure and
// timed_out, Warn for cancelled, Info for anything else (including no
// conclusion yet).
func severity(conclusion *string) (otlp.Severity, string) {
	if conclusion == nil {
		return otlp.SeverityInfo, SeverityTextInfo
	}
	switch *conclusion {
	case conclusionFailure, conclusionTimedOut:
		return otlp.SeverityError, SeverityTextError
	case conclusionCancelled:
		return otlp.SeverityWarn, SeverityTextWarn
	default:
		return otlp.SeverityInfo, SeverityTextInfo
	}
}

func setString(attrs map[string]any, key string, v *string) {
	if v != nil {
		attrs[key] = *v
	}
}

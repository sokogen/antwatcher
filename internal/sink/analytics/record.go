// Package analytics is the analytics destination class: it projects the
// canonical model into flat, sparse records for a warehouse (one row per run,
// job, or step per event) and hands them to a driver through the Writer
// contract together with the portable Schema they conform to.
//
// The class is at-least-once by design. Every event appends its records to a
// base table; the same entity therefore appears once per lifecycle event
// (queued, in_progress, completed) and once more per redelivery. Records are
// keyed by entity identity (RecordID), never by delivery, so consumers read
// the deduplicating "<table>_current" view (CurrentViewSQL), which keeps the
// most advanced state per record_id and lets a replayed older event never
// overwrite a newer one.
package analytics

import (
	"time"

	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/sink"
)

// Column names of Record, in table order (see Current for the schema).
const (
	ColRecordID      = "record_id"
	ColKind          = "kind"
	ColRepositoryID  = "repository_id"
	ColRepository    = "repository"
	ColWorkflowID    = "workflow_id"
	ColWorkflowName  = "workflow_name"
	ColWorkflowPath  = "workflow_path"
	ColRunID         = "run_id"
	ColRunNumber     = "run_number"
	ColRunAttempt    = "run_attempt"
	ColJobID         = "job_id"
	ColJobName       = "job_name"
	ColStepNumber    = "step_number"
	ColStepName      = "step_name"
	ColStatus        = "status"
	ColConclusion    = "conclusion"
	ColStatusRank    = "status_rank"
	ColCreatedAt     = "created_at"
	ColStartedAt     = "started_at"
	ColCompletedAt   = "completed_at"
	ColQueuedMs      = "queued_ms"
	ColDurationMs    = "duration_ms"
	ColTriggerEvent  = "trigger_event"
	ColHeadBranch    = "head_branch"
	ColHeadSHA       = "head_sha"
	ColActor         = "actor"
	ColRunnerName    = "runner_name"
	ColRunnerGroup   = "runner_group"
	ColLabels        = "labels"
	ColHTMLURL       = "html_url"
	ColEventTime     = "event_time"
	ColReceivedAt    = "received_at"
	ColDeliveryGUID  = "delivery_guid"
	ColSchemaVersion = "schema_version"
)

// Record is one analytics row. Field names map to the columns of Current; a
// nil pointer or nil slice is a NULL.
//
// The model is sparse, so most columns are nullable and a row carries only
// what its event reported:
//
//   - run rows (Kind "run") carry workflow_id, workflow_path, run_number,
//     trigger_event, and actor and never job_id, job_name, step_*, runner_*,
//     or labels;
//   - job rows (Kind "job") carry job_id, job_name, runner_name, runner_group,
//     and labels and never workflow_id, workflow_path, run_number,
//     trigger_event, or actor;
//   - step rows (Kind "step") add step_number and step_name to the job's
//     fields (job_id, job_name, runner_*, labels are copied from the job so a
//     step is filterable without a join) and have no created_at or html_url.
//
// Run rows and job/step rows describe the same run attempt when they share
// RepositoryID, RunID, and RunAttempt; that is the join key for consumers
// that need both sides. Those three ids, RecordID, Kind, StatusRank,
// EventTime, ReceivedAt, DeliveryGUID, and SchemaVersion are set on every
// row.
type Record struct {
	// RecordID is the entity identity (model.RecordID): the same value for
	// every event and redelivery of the same run attempt, job, or step.
	RecordID string
	// Kind is "run", "job", or "step".
	Kind model.RecordKind

	RepositoryID int64
	Repository   *string
	WorkflowID   *int64
	WorkflowName *string
	WorkflowPath *string

	RunID      int64
	RunNumber  *int64
	RunAttempt int64
	JobID      *int64
	JobName    *string
	StepNumber *int64
	StepName   *string

	// Status is the entity's status as GitHub reported it; nil when the
	// payload carried null. A step without a status of its own carries the
	// job's status. StatusRank is model.StatusRank of that status and orders
	// rows of the same entity in the current view.
	Status     *string
	Conclusion *string
	StatusRank int64

	// CreatedAt, StartedAt, and CompletedAt are the entity's lifecycle
	// timestamps as far as the event reported them. A run's CompletedAt is
	// its updated_at once completed (GitHub has no completed_at for runs).
	CreatedAt   *time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	// QueuedMs is StartedAt minus CreatedAt once the entity has started (GitHub
	// sends started_at equal to created_at while a job is queued, so it is nil
	// until then). DurationMs is CompletedAt minus StartedAt once completed.
	QueuedMs   *int64
	DurationMs *int64

	TriggerEvent *string
	HeadBranch   *string
	HeadSHA      *string
	Actor        *string
	RunnerName   *string
	RunnerGroup  *string
	Labels       []string
	HTMLURL      *string

	// EventTime is the model's event time: when the reported transition
	// happened, or ReceivedAt when the payload lacks it. It is the partition
	// column and the second sort key of the current view.
	EventTime time.Time
	// ReceivedAt, DeliveryGUID, and SchemaVersion come from the envelope and
	// identify the delivery the row was projected from.
	ReceivedAt    time.Time
	DeliveryGUID  string
	SchemaVersion int64
}

// Fields returns the record as column name → value for the columns that are
// not NULL. Values are string, int64, time.Time, or []string, matching the
// column types of Current. Drivers encode rows from it without knowing the
// struct.
func (r *Record) Fields() map[string]any {
	f := map[string]any{
		ColRecordID:      r.RecordID,
		ColKind:          string(r.Kind),
		ColRepositoryID:  r.RepositoryID,
		ColRunID:         r.RunID,
		ColRunAttempt:    r.RunAttempt,
		ColStatusRank:    r.StatusRank,
		ColEventTime:     r.EventTime,
		ColReceivedAt:    r.ReceivedAt,
		ColDeliveryGUID:  r.DeliveryGUID,
		ColSchemaVersion: r.SchemaVersion,
	}
	setStr(f, ColRepository, r.Repository)
	setInt(f, ColWorkflowID, r.WorkflowID)
	setStr(f, ColWorkflowName, r.WorkflowName)
	setStr(f, ColWorkflowPath, r.WorkflowPath)
	setInt(f, ColRunNumber, r.RunNumber)
	setInt(f, ColJobID, r.JobID)
	setStr(f, ColJobName, r.JobName)
	setInt(f, ColStepNumber, r.StepNumber)
	setStr(f, ColStepName, r.StepName)
	setStr(f, ColStatus, r.Status)
	setStr(f, ColConclusion, r.Conclusion)
	setTime(f, ColCreatedAt, r.CreatedAt)
	setTime(f, ColStartedAt, r.StartedAt)
	setTime(f, ColCompletedAt, r.CompletedAt)
	setInt(f, ColQueuedMs, r.QueuedMs)
	setInt(f, ColDurationMs, r.DurationMs)
	setStr(f, ColTriggerEvent, r.TriggerEvent)
	setStr(f, ColHeadBranch, r.HeadBranch)
	setStr(f, ColHeadSHA, r.HeadSHA)
	setStr(f, ColActor, r.Actor)
	setStr(f, ColRunnerName, r.RunnerName)
	setStr(f, ColRunnerGroup, r.RunnerGroup)
	if r.Labels != nil {
		f[ColLabels] = append([]string(nil), r.Labels...)
	}
	setStr(f, ColHTMLURL, r.HTMLURL)
	return f
}

// Project turns an execution into its analytics records.
//
//   - A run event yields one "run" record.
//   - A job event yields one "job" record followed by one "step" record per
//     step in payload order. A step whose payload carried no status takes the
//     job's status.
//   - An event without a modelled entity yields sink.ErrSkipped.
//
// Record ids are entity identities, so the three lifecycle events of one job
// produce three rows with the same record_id and increasing status_rank; the
// current view keeps the last. The result is deterministic: the same
// execution always yields the same records in the same order.
func Project(exec model.Execution) ([]Record, error) {
	switch exec.Kind {
	case model.KindRun:
		if r := exec.Run; r != nil {
			return []Record{runRecord(exec, r)}, nil
		}
	case model.KindJob:
		if j := exec.Job; j != nil {
			out := make([]Record, 0, 1+len(j.Steps))
			out = append(out, jobRecord(exec, j))
			for i := range j.Steps {
				out = append(out, stepRecord(exec, j, &j.Steps[i]))
			}
			return out, nil
		}
	case model.KindNone:
	}
	return nil, sink.ErrSkipped
}

func runRecord(exec model.Execution, r *model.Run) Record {
	rec := Record{
		RecordID:     r.RecordID(),
		Kind:         model.RecordRun,
		RepositoryID: r.RepositoryID,
		Repository:   nonEmpty(r.Repository),
		WorkflowID:   ptr(r.WorkflowID),
		WorkflowName: r.WorkflowName,
		WorkflowPath: nonEmpty(r.WorkflowPath),
		RunID:        r.RunID,
		RunNumber:    ptr(r.RunNumber),
		RunAttempt:   r.RunAttempt,
		Status:       nonEmpty(r.Status),
		Conclusion:   r.Conclusion,
		StatusRank:   int64(model.StatusRank(r.Status)),
		CreatedAt:    nonZero(r.CreatedAt),
		StartedAt:    r.StartedAt,
		TriggerEvent: nonEmpty(r.TriggerEvent),
		HeadBranch:   r.HeadBranch,
		HeadSHA:      nonEmpty(r.HeadSHA),
		Actor:        r.Actor,
		HTMLURL:      nonEmpty(r.HTMLURL),
	}
	if r.Status == model.StatusCompleted {
		rec.CompletedAt = nonZero(r.UpdatedAt)
	}
	rec.QueuedMs, rec.DurationMs = timings(r.Status, rec.CreatedAt, rec.StartedAt, rec.CompletedAt)
	envelopeFields(&rec, exec, model.EventTime(exec))
	return rec
}

func jobRecord(exec model.Execution, j *model.Job) Record {
	rec := Record{
		RecordID:     j.RecordID(),
		Kind:         model.RecordJob,
		RepositoryID: j.RepositoryID,
		Repository:   nonEmpty(j.Repository),
		WorkflowName: j.WorkflowName,
		RunID:        j.RunID,
		RunAttempt:   j.RunAttempt,
		JobID:        ptr(j.JobID),
		JobName:      nonEmpty(j.Name),
		Status:       nonEmpty(j.Status),
		Conclusion:   j.Conclusion,
		StatusRank:   int64(model.StatusRank(j.Status)),
		CreatedAt:    nonZero(j.CreatedAt),
		StartedAt:    j.StartedAt,
		CompletedAt:  j.CompletedAt,
		HeadBranch:   j.HeadBranch,
		HeadSHA:      nonEmpty(j.HeadSHA),
		RunnerName:   j.RunnerName,
		RunnerGroup:  j.RunnerGroup,
		Labels:       cloneLabels(j.Labels),
		HTMLURL:      nonEmpty(j.HTMLURL),
	}
	rec.QueuedMs, rec.DurationMs = timings(j.Status, rec.CreatedAt, rec.StartedAt, rec.CompletedAt)
	envelopeFields(&rec, exec, model.EventTime(exec))
	return rec
}

func stepRecord(exec model.Execution, j *model.Job, s *model.Step) Record {
	status := s.Status
	if status == "" {
		status = j.Status
	}
	rec := Record{
		RecordID:     s.RecordID(j),
		Kind:         model.RecordStep,
		RepositoryID: j.RepositoryID,
		Repository:   nonEmpty(j.Repository),
		WorkflowName: j.WorkflowName,
		RunID:        j.RunID,
		RunAttempt:   j.RunAttempt,
		JobID:        ptr(j.JobID),
		JobName:      nonEmpty(j.Name),
		StepNumber:   ptr(s.Number),
		StepName:     nonEmpty(s.Name),
		Status:       nonEmpty(status),
		Conclusion:   s.Conclusion,
		StatusRank:   int64(model.StatusRank(status)),
		StartedAt:    s.StartedAt,
		CompletedAt:  s.CompletedAt,
		HeadBranch:   j.HeadBranch,
		HeadSHA:      nonEmpty(j.HeadSHA),
		RunnerName:   j.RunnerName,
		RunnerGroup:  j.RunnerGroup,
		Labels:       cloneLabels(j.Labels),
	}
	_, rec.DurationMs = timings(status, nil, rec.StartedAt, rec.CompletedAt)
	envelopeFields(&rec, exec, model.StepEventTime(exec, *s))
	return rec
}

func envelopeFields(rec *Record, exec model.Execution, eventTime time.Time) {
	env := exec.Envelope
	rec.EventTime = eventTime.UTC()
	rec.ReceivedAt = env.ReceivedAt.UTC()
	rec.DeliveryGUID = env.DeliveryGUID
	rec.SchemaVersion = int64(env.SchemaVersion)
}

// timings derives queued_ms and duration_ms. Queued time exists once the
// entity started (rank in_progress or later) and both created and started
// are known; duration exists once it completed and both started and
// completed are known.
func timings(status string, created, started, completed *time.Time) (queued, duration *int64) {
	rank := model.StatusRank(status)
	if rank >= model.RankInProgress && created != nil && started != nil {
		queued = ptr(started.Sub(*created).Milliseconds())
	}
	if rank >= model.RankCompleted && started != nil && completed != nil {
		duration = ptr(completed.Sub(*started).Milliseconds())
	}
	return queued, duration
}

func ptr[T any](v T) *T { return &v }

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nonZero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func cloneLabels(labels []string) []string {
	if labels == nil {
		return nil
	}
	return append([]string(nil), labels...)
}

func setStr(f map[string]any, key string, v *string) {
	if v != nil {
		f[key] = *v
	}
}

func setInt(f map[string]any, key string, v *int64) {
	if v != nil {
		f[key] = *v
	}
}

func setTime(f map[string]any, key string, v *time.Time) {
	if v != nil {
		f[key] = *v
	}
}

package analytics_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

const (
	headSHA = "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e"
	runURL  = "https://github.com/sokogen/antwatcher/actions/runs/15000000001"
	jobURL  = runURL + "/job/42000000001"
)

// runRecord is the golden run row shared by the three workflow_run fixtures.
func runRecord(fixture, status string, rank int64, eventTime time.Time) analytics.Record {
	return analytics.Record{
		RecordID:      model.RecordID(model.RecordRun, repoID, runID, 1),
		Kind:          model.RecordRun,
		RepositoryID:  repoID,
		Repository:    str("sokogen/antwatcher"),
		WorkflowID:    i64(12345678),
		WorkflowName:  str("CI"),
		WorkflowPath:  str(".github/workflows/ci.yml"),
		RunID:         runID,
		RunNumber:     i64(42),
		RunAttempt:    1,
		Status:        str(status),
		StatusRank:    rank,
		CreatedAt:     tp(10, 0, 0),
		StartedAt:     tp(10, 0, 0),
		TriggerEvent:  str("push"),
		HeadBranch:    str("main"),
		HeadSHA:       str(headSHA),
		Actor:         str("sokogen"),
		HTMLURL:       str(runURL),
		EventTime:     eventTime,
		ReceivedAt:    received,
		DeliveryGUID:  "guid-" + fixture,
		SchemaVersion: int64(event.SchemaVersion),
	}
}

// jobRecord is the golden job row shared by the three workflow_job fixtures.
func jobRecord(fixture, status string, rank int64, eventTime time.Time) analytics.Record {
	return analytics.Record{
		RecordID:      model.RecordID(model.RecordJob, repoID, runID, 1, jobID),
		Kind:          model.RecordJob,
		RepositoryID:  repoID,
		Repository:    str("sokogen/antwatcher"),
		WorkflowName:  str("CI"),
		RunID:         runID,
		RunAttempt:    1,
		JobID:         i64(jobID),
		JobName:       str("test"),
		Status:        str(status),
		StatusRank:    rank,
		CreatedAt:     tp(10, 0, 2),
		HeadBranch:    str("main"),
		HeadSHA:       str(headSHA),
		Labels:        []string{"ubuntu-latest"},
		HTMLURL:       str(jobURL),
		EventTime:     eventTime,
		ReceivedAt:    received,
		DeliveryGUID:  "guid-" + fixture,
		SchemaVersion: int64(event.SchemaVersion),
	}
}

// stepRecord is a golden step row of the started job.
func stepRecord(fixture string, number int64, name, status string, eventTime time.Time) analytics.Record {
	return analytics.Record{
		RecordID:      model.RecordID(model.RecordStep, repoID, runID, 1, jobID, number),
		Kind:          model.RecordStep,
		RepositoryID:  repoID,
		Repository:    str("sokogen/antwatcher"),
		WorkflowName:  str("CI"),
		RunID:         runID,
		RunAttempt:    1,
		JobID:         i64(jobID),
		JobName:       str("test"),
		StepNumber:    i64(number),
		StepName:      str(name),
		Status:        str(status),
		StatusRank:    int64(model.StatusRank(status)),
		HeadBranch:    str("main"),
		HeadSHA:       str(headSHA),
		RunnerName:    str("GitHub Actions 12"),
		RunnerGroup:   str("GitHub Actions"),
		Labels:        []string{"ubuntu-latest"},
		EventTime:     eventTime,
		ReceivedAt:    received,
		DeliveryGUID:  "guid-" + fixture,
		SchemaVersion: int64(event.SchemaVersion),
	}
}

func completedStep(fixture string, number int64, name, conclusion string, started, completed *time.Time, eventTime time.Time) analytics.Record {
	rec := stepRecord(fixture, number, name, model.StatusCompleted, eventTime)
	rec.Conclusion = str(conclusion)
	rec.StartedAt, rec.CompletedAt = started, completed
	if started != nil && completed != nil {
		rec.DurationMs = i64(completed.Sub(*started).Milliseconds())
	}
	return rec
}

func TestProject_RunFixtures(t *testing.T) {
	requested := runRecord("workflow_run.requested", model.StatusQueued, model.RankScheduled, ts(10, 0, 0))

	inProgress := runRecord("workflow_run.in_progress", model.StatusInProgress, model.RankInProgress, ts(10, 0, 0))
	inProgress.QueuedMs = i64(0)

	completed := runRecord("workflow_run.completed", model.StatusCompleted, model.RankCompleted, ts(10, 3, 20))
	completed.Conclusion = str("failure")
	completed.CompletedAt = tp(10, 3, 20)
	completed.QueuedMs = i64(0)
	completed.DurationMs = i64(200_000)

	tests := []struct {
		fixture string
		want    analytics.Record
	}{
		{"workflow_run.requested", requested},
		{"workflow_run.in_progress", inProgress},
		{"workflow_run.completed", completed},
	}
	for _, tc := range tests {
		t.Run(tc.fixture, func(t *testing.T) {
			got, err := analytics.Project(fixtureExecution(t, tc.fixture))
			require.NoError(t, err)
			require.Len(t, got, 1, "a run event is exactly one record")
			assert.Equal(t, tc.want, got[0])
		})
	}
}

func TestProject_JobQueued(t *testing.T) {
	want := jobRecord("workflow_job.queued", model.StatusQueued, model.RankScheduled, ts(10, 0, 2))
	want.StartedAt = tp(10, 0, 2)

	got, err := analytics.Project(fixtureExecution(t, "workflow_job.queued"))
	require.NoError(t, err)
	require.Len(t, got, 1, "no steps while queued")
	assert.Equal(t, want, got[0])
	assert.Nil(t, got[0].QueuedMs, "queued time is unknown until the job starts")
	assert.Nil(t, got[0].RunnerName, "no runner while queued")
	assert.Nil(t, got[0].DurationMs)
}

func TestProject_JobInProgress(t *testing.T) {
	const fixture = "workflow_job.in_progress"
	job := jobRecord(fixture, model.StatusInProgress, model.RankInProgress, ts(10, 0, 10))
	job.StartedAt = tp(10, 0, 10)
	job.QueuedMs = i64(8_000)
	job.RunnerName = str("GitHub Actions 12")
	job.RunnerGroup = str("GitHub Actions")

	step1 := stepRecord(fixture, 1, "Set up job", model.StatusInProgress, ts(10, 0, 10))
	step1.StartedAt = tp(10, 0, 10)
	want := []analytics.Record{
		job,
		step1,
		stepRecord(fixture, 2, "Run actions/checkout@v4", model.StatusQueued, ts(10, 0, 10)),
		stepRecord(fixture, 3, "Run make test", model.StatusQueued, ts(10, 0, 10)),
		stepRecord(fixture, 4, "Upload coverage", model.StatusQueued, ts(10, 0, 10)),
	}

	got, err := analytics.Project(fixtureExecution(t, fixture))
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestProject_JobCompleted(t *testing.T) {
	const fixture = "workflow_job.completed"
	job := jobRecord(fixture, model.StatusCompleted, model.RankCompleted, ts(10, 3, 15))
	job.Conclusion = str("failure")
	job.StartedAt = tp(10, 0, 10)
	job.CompletedAt = tp(10, 3, 15)
	job.QueuedMs = i64(8_000)
	job.DurationMs = i64(185_000)
	job.RunnerName = str("GitHub Actions 12")
	job.RunnerGroup = str("GitHub Actions")

	want := []analytics.Record{
		job,
		completedStep(fixture, 1, "Set up job", "success", tp(10, 0, 10), tp(10, 0, 12), ts(10, 0, 12)),
		completedStep(fixture, 2, "Run actions/checkout@v4", "success", tp(10, 0, 12), tp(10, 0, 14), ts(10, 0, 14)),
		completedStep(fixture, 3, "Run make test", "failure", tp(10, 0, 14), tp(10, 3, 10), ts(10, 3, 10)),
		completedStep(fixture, 4, "Upload coverage", "skipped", nil, nil, ts(10, 3, 15)),
		completedStep(fixture, 5, "Post Run actions/checkout@v4", "success", tp(10, 3, 10), tp(10, 3, 11), ts(10, 3, 11)),
		completedStep(fixture, 6, "Complete job", "success", tp(10, 3, 11), tp(10, 3, 12), ts(10, 3, 12)),
	}

	got, err := analytics.Project(fixtureExecution(t, fixture))
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, i64(176_000), got[3].DurationMs, "failed step duration")
	skipped := got[4]
	assert.Nil(t, skipped.DurationMs, "a skipped step never ran")
	assert.Equal(t, ts(10, 3, 15), skipped.EventTime, "a step without timestamps takes the job's event time")
}

func TestProject_KindNoneIsSkipped(t *testing.T) {
	for _, exec := range []model.Execution{
		fixtureExecution(t, "ping"),
		{Kind: model.KindNone, Envelope: rawEnvelope("push", `{}`)},
		{Kind: model.KindRun, Envelope: rawEnvelope("workflow_run", `{}`)}, // defensive: kind without entity
		{Kind: model.KindJob, Envelope: rawEnvelope("workflow_job", `{}`)},
	} {
		got, err := analytics.Project(exec)
		require.ErrorIs(t, err, sink.ErrSkipped)
		assert.Nil(t, got)
	}
}

func TestProject_RecordIDsAreEntityIdentity(t *testing.T) {
	var runIDs, jobIDs, stepIDs []string
	for _, fixture := range []string{"workflow_run.requested", "workflow_run.in_progress", "workflow_run.completed"} {
		recs, err := analytics.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		runIDs = append(runIDs, recs[0].RecordID)
	}
	for _, fixture := range []string{"workflow_job.queued", "workflow_job.in_progress", "workflow_job.completed"} {
		recs, err := analytics.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		jobIDs = append(jobIDs, recs[0].RecordID)
		if len(recs) > 1 {
			stepIDs = append(stepIDs, recs[1].RecordID)
		}
	}

	assert.Equal(t, []string{runIDs[0], runIDs[0], runIDs[0]}, runIDs, "same run attempt, same id across events")
	assert.Equal(t, []string{jobIDs[0], jobIDs[0], jobIDs[0]}, jobIDs, "same job, same id across events")
	assert.Equal(t, []string{stepIDs[0], stepIDs[0]}, stepIDs, "same step, same id across events")

	assert.Equal(t, model.RecordID(model.RecordRun, repoID, runID, 1), runIDs[0])
	assert.Equal(t, model.RecordID(model.RecordJob, repoID, runID, 1, jobID), jobIDs[0])
	assert.Equal(t, model.RecordID(model.RecordStep, repoID, runID, 1, jobID, 1), stepIDs[0])
	assert.NotEqual(t, runIDs[0], jobIDs[0], "kinds never collide")
	assert.NotEqual(t, jobIDs[0], stepIDs[0])
	assert.Len(t, runIDs[0], 64, "hex sha256")
}

func TestProject_StatusRankOrdersLifecycle(t *testing.T) {
	var ranks []int64
	for _, fixture := range []string{"workflow_job.queued", "workflow_job.in_progress", "workflow_job.completed"} {
		recs, err := analytics.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		ranks = append(ranks, recs[0].StatusRank)
	}
	assert.Equal(t, []int64{model.RankScheduled, model.RankInProgress, model.RankCompleted}, ranks)
}

func TestProject_RowsAreSparse(t *testing.T) {
	runs, err := analytics.Project(fixtureExecution(t, "workflow_run.completed"))
	require.NoError(t, err)
	run := runs[0]
	assert.Nil(t, run.JobID)
	assert.Nil(t, run.JobName)
	assert.Nil(t, run.StepNumber)
	assert.Nil(t, run.StepName)
	assert.Nil(t, run.RunnerName)
	assert.Nil(t, run.RunnerGroup)
	assert.Nil(t, run.Labels)

	jobs, err := analytics.Project(fixtureExecution(t, "workflow_job.completed"))
	require.NoError(t, err)
	job, step := jobs[0], jobs[1]
	for _, rec := range []analytics.Record{job, step} {
		assert.Nil(t, rec.WorkflowID, rec.Kind)
		assert.Nil(t, rec.WorkflowPath, rec.Kind)
		assert.Nil(t, rec.RunNumber, rec.Kind)
		assert.Nil(t, rec.TriggerEvent, rec.Kind)
		assert.Nil(t, rec.Actor, rec.Kind)
	}
	assert.Nil(t, job.StepNumber)
	assert.Nil(t, job.StepName)
	assert.Nil(t, step.CreatedAt, "steps have no created_at")
	assert.Nil(t, step.HTMLURL, "steps have no page of their own")
	assert.Nil(t, step.QueuedMs)

	joinKey := func(r analytics.Record) [3]int64 { return [3]int64{r.RepositoryID, r.RunID, r.RunAttempt} }
	assert.Equal(t, joinKey(run), joinKey(job), "run and job rows join on repository_id, run_id, run_attempt")
	assert.Equal(t, joinKey(job), joinKey(step))
}

func TestProject_NullFieldsStayNull(t *testing.T) {
	exec, err := model.Normalize(rawEnvelope(model.EventWorkflowRun, `{
		"workflow_run": {"id": 1, "run_attempt": 2, "status": null, "name": null, "head_branch": null, "actor": null,
			"conclusion": null, "path": "", "html_url": ""},
		"repository": {"id": 5, "full_name": "o/r"}}`))
	require.NoError(t, err)
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	rec := recs[0]

	assert.Nil(t, rec.Status)
	assert.Equal(t, int64(model.RankUnknown), rec.StatusRank)
	assert.Nil(t, rec.WorkflowName)
	assert.Nil(t, rec.WorkflowPath, "empty strings are NULL, not \"\"")
	assert.Nil(t, rec.HeadBranch)
	assert.Nil(t, rec.Actor)
	assert.Nil(t, rec.HTMLURL)
	assert.Nil(t, rec.CreatedAt, "missing timestamps are NULL")
	assert.Nil(t, rec.StartedAt)
	assert.Nil(t, rec.CompletedAt)
	assert.Nil(t, rec.QueuedMs)
	assert.Nil(t, rec.DurationMs)
	assert.Equal(t, received, rec.EventTime, "falls back to received_at")
	assert.Equal(t, int64(2), rec.RunAttempt)
}

func TestProject_StepInheritsJobStatus(t *testing.T) {
	exec, err := model.Normalize(rawEnvelope(model.EventWorkflowJob, `{
		"workflow_job": {"id": 7, "run_id": 8, "run_attempt": 1, "status": "in_progress", "name": "build",
			"created_at": "2026-09-02T10:00:00Z", "started_at": "2026-09-02T10:00:03Z",
			"steps": [{"number": 1, "name": "no status", "status": null},
			          {"number": 2, "name": "own status", "status": "queued"}]},
		"repository": {"id": 5, "full_name": "o/r"}}`))
	require.NoError(t, err)
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	require.Len(t, recs, 3)

	assert.Equal(t, str(model.StatusInProgress), recs[1].Status, "missing step status is the job's")
	assert.Equal(t, int64(model.RankInProgress), recs[1].StatusRank)
	assert.Equal(t, str(model.StatusQueued), recs[2].Status, "an own status is kept")
	assert.Equal(t, int64(model.RankScheduled), recs[2].StatusRank)
	assert.Equal(t, i64(3_000), recs[0].QueuedMs)
	assert.Nil(t, recs[0].Labels, "absent labels stay NULL")
}

func TestProject_CompletedRunWithoutStartHasNoDuration(t *testing.T) {
	exec, err := model.Normalize(rawEnvelope(model.EventWorkflowRun, `{
		"workflow_run": {"id": 1, "run_attempt": 1, "status": "completed", "conclusion": "cancelled",
			"created_at": "2026-09-02T10:00:00Z", "updated_at": "2026-09-02T10:00:30Z"},
		"repository": {"id": 5, "full_name": "o/r"}}`))
	require.NoError(t, err)
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	rec := recs[0]
	assert.Equal(t, tp(10, 0, 30), rec.CompletedAt, "a completed run's completed_at is its updated_at")
	assert.Nil(t, rec.StartedAt)
	assert.Nil(t, rec.QueuedMs)
	assert.Nil(t, rec.DurationMs)
	assert.Equal(t, str("cancelled"), rec.Conclusion)
}

func TestProject_TimesAreUTC(t *testing.T) {
	exec, err := model.Normalize(rawEnvelope(model.EventWorkflowJob, `{
		"workflow_job": {"id": 7, "run_id": 8, "run_attempt": 1, "status": "completed", "conclusion": "success",
			"created_at": "2026-09-02T12:00:00+02:00", "started_at": "2026-09-02T12:00:03+02:00", "completed_at": "2026-09-02T12:01:03+02:00"},
		"repository": {"id": 5, "full_name": "o/r"}}`))
	require.NoError(t, err)
	exec.Envelope.ReceivedAt = received.In(time.FixedZone("x", 3600))
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	rec := recs[0]
	assert.Equal(t, time.UTC, rec.EventTime.Location())
	assert.Equal(t, time.UTC, rec.ReceivedAt.Location())
	assert.Equal(t, ts(10, 1, 3), rec.EventTime)
	assert.Equal(t, i64(3_000), rec.QueuedMs)
	assert.Equal(t, i64(60_000), rec.DurationMs)
}

func TestProject_IsDeterministic(t *testing.T) {
	for _, fixture := range event.Fixtures() {
		exec := fixtureExecution(t, fixture)
		a, errA := analytics.Project(exec)
		b, errB := analytics.Project(exec)
		require.Equal(t, errA, errB, fixture)
		assert.Equal(t, a, b, fixture)
	}
}

func TestProject_DoesNotAliasModelLabels(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	recs[0].Labels[0] = "changed"
	assert.Equal(t, []string{"ubuntu-latest"}, exec.Job.Labels, "records own their label slices")
	assert.Equal(t, []string{"ubuntu-latest"}, recs[1].Labels, "step rows own theirs too")
}

func TestRecord_FieldsMatchSchema(t *testing.T) {
	schema := analytics.Current
	for _, fixture := range []string{"workflow_run.completed", "workflow_job.completed", "workflow_job.queued"} {
		recs, err := analytics.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		for _, rec := range recs {
			fields := rec.Fields()
			for name, v := range fields {
				col, ok := schema.Column(name)
				require.True(t, ok, "%s: field %q is not a schema column", fixture, name)
				switch col.Type {
				case analytics.TypeString:
					assert.IsType(t, "", v, name)
				case analytics.TypeInt64:
					assert.IsType(t, int64(0), v, name)
				case analytics.TypeTimestamp:
					assert.IsType(t, time.Time{}, v, name)
				case analytics.TypeStringArray:
					assert.IsType(t, []string{}, v, name)
				case analytics.TypeBool:
					assert.IsType(t, false, v, name)
				}
			}
			for _, col := range schema.Columns {
				if col.Required {
					assert.Contains(t, fields, col.Name, "%s %s: required column missing", fixture, rec.Kind)
				}
			}
		}
	}
}

func TestRecord_FieldsValues(t *testing.T) {
	recs, err := analytics.Project(fixtureExecution(t, "workflow_job.completed"))
	require.NoError(t, err)
	job := recs[0]
	fields := job.Fields()

	assert.Equal(t, job.RecordID, fields[analytics.ColRecordID])
	assert.Equal(t, "job", fields[analytics.ColKind])
	assert.Equal(t, jobID, fields[analytics.ColJobID])
	assert.Equal(t, "failure", fields[analytics.ColConclusion])
	assert.Equal(t, int64(185_000), fields[analytics.ColDurationMs])
	assert.Equal(t, ts(10, 3, 15), fields[analytics.ColCompletedAt])
	assert.Equal(t, []string{"ubuntu-latest"}, fields[analytics.ColLabels])
	assert.NotContains(t, fields, analytics.ColWorkflowID, "NULL columns are absent")
	assert.NotContains(t, fields, analytics.ColActor)
	assert.NotContains(t, fields, analytics.ColStepNumber)

	fields[analytics.ColLabels].([]string)[0] = "changed"
	assert.Equal(t, []string{"ubuntu-latest"}, job.Labels, "Fields copies slices")
}

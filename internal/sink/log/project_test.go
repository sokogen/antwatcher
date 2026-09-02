package log_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink/log"
	"github.com/sokogen/antwatcher/internal/sink/trace"
)

const (
	repoName = "sokogen/antwatcher"
	headSHA  = "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e"
	runURL   = "https://github.com/sokogen/antwatcher/actions/runs/15000000001"
	jobURL   = runURL + "/job/42000000001"
)

var receivedText = received.Format(time.RFC3339Nano)

func TestProject_CompletedRun(t *testing.T) {
	rec := log.Project(fixtureExecution(t, "workflow_run.completed"))
	assert.Equal(t, otlp.LogRecord{
		Time:         ts(10, 3, 20),
		ObservedTime: received,
		Severity:     otlp.SeverityError,
		SeverityText: log.SeverityTextError,
		Body:         "workflow_run completed: CI",
		TraceID:      model.TraceID(repoID, runID, 1),
		SpanID:       model.RunSpanID(runID, 1),
		Attributes: map[string]any{
			log.AttrDeliveryGUID:  "guid-workflow_run.completed",
			log.AttrWebhookEvent:  "workflow_run",
			log.AttrWebhookAction: "completed",
			log.AttrKind:          "run",
			log.AttrReceivedAt:    receivedText,
			log.AttrSchemaVersion: int64(event.SchemaVersion),
			log.AttrRepositoryID:  repoID,
			log.AttrRepository:    repoName,
			log.AttrWorkflowID:    int64(12345678),
			log.AttrWorkflowName:  "CI",
			log.AttrWorkflowPath:  ".github/workflows/ci.yml",
			log.AttrRunID:         runID,
			log.AttrRunNumber:     int64(42),
			log.AttrRunAttempt:    int64(1),
			log.AttrStatus:        "completed",
			log.AttrConclusion:    "failure",
			log.AttrEvent:         "push",
			log.AttrHeadBranch:    "main",
			log.AttrHeadSHA:       headSHA,
			log.AttrActor:         "sokogen",
			log.AttrHTMLURL:       runURL,
		},
	}, rec)
}

func TestProject_CompletedJob(t *testing.T) {
	rec := log.Project(fixtureExecution(t, "workflow_job.completed"))
	assert.Equal(t, otlp.LogRecord{
		Time:         ts(10, 3, 15),
		ObservedTime: received,
		Severity:     otlp.SeverityError,
		SeverityText: log.SeverityTextError,
		Body:         "workflow_job completed: test",
		TraceID:      model.TraceID(repoID, runID, 1),
		SpanID:       model.JobSpanID(jobID),
		Attributes: map[string]any{
			log.AttrDeliveryGUID:   "guid-workflow_job.completed",
			log.AttrWebhookEvent:   "workflow_job",
			log.AttrWebhookAction:  "completed",
			log.AttrKind:           "job",
			log.AttrReceivedAt:     receivedText,
			log.AttrSchemaVersion:  int64(event.SchemaVersion),
			log.AttrRepositoryID:   repoID,
			log.AttrRepository:     repoName,
			log.AttrWorkflowName:   "CI",
			log.AttrRunID:          runID,
			log.AttrRunAttempt:     int64(1),
			log.AttrJobID:          jobID,
			log.AttrJobName:        "test",
			log.AttrStatus:         "completed",
			log.AttrConclusion:     "failure",
			log.AttrHeadBranch:     "main",
			log.AttrHeadSHA:        headSHA,
			log.AttrRunnerName:     "GitHub Actions 12",
			log.AttrRunnerGroup:    "GitHub Actions",
			log.AttrLabels:         []string{"ubuntu-latest"},
			log.AttrHTMLURL:        jobURL,
			log.AttrStepsTotal:     int64(6),
			log.AttrStepsCompleted: int64(6),
			log.AttrStepsFailed:    int64(1),
			log.AttrStepsSkipped:   int64(1),
		},
	}, rec)
}

func TestProject_EarlierLifecycleEventsAreRecorded(t *testing.T) {
	t.Run("run requested", func(t *testing.T) {
		rec := log.Project(fixtureExecution(t, "workflow_run.requested"))
		assert.Equal(t, ts(10, 0, 0), rec.Time, "created_at while queued")
		assert.Equal(t, "workflow_run requested: CI", rec.Body)
		assert.Equal(t, otlp.SeverityInfo, rec.Severity)
		assert.Equal(t, log.SeverityTextInfo, rec.SeverityText)
		assert.Equal(t, "queued", rec.Attributes[log.AttrStatus])
		assert.NotContains(t, rec.Attributes, log.AttrConclusion)
		assert.Equal(t, model.TraceID(repoID, runID, 1), rec.TraceID)
		assert.Equal(t, model.RunSpanID(runID, 1), rec.SpanID)
	})
	t.Run("run in progress", func(t *testing.T) {
		rec := log.Project(fixtureExecution(t, "workflow_run.in_progress"))
		assert.Equal(t, ts(10, 0, 0), rec.Time, "run_started_at while in progress")
		assert.Equal(t, "workflow_run in_progress: CI", rec.Body)
		assert.Equal(t, otlp.SeverityInfo, rec.Severity)
		assert.Equal(t, "in_progress", rec.Attributes[log.AttrStatus])
	})
	t.Run("job queued", func(t *testing.T) {
		rec := log.Project(fixtureExecution(t, "workflow_job.queued"))
		assert.Equal(t, ts(10, 0, 2), rec.Time, "created_at while queued")
		assert.Equal(t, "workflow_job queued: test", rec.Body)
		assert.Equal(t, otlp.SeverityInfo, rec.Severity)
		assert.Equal(t, "queued", rec.Attributes[log.AttrStatus])
		assert.NotContains(t, rec.Attributes, log.AttrConclusion)
		assert.NotContains(t, rec.Attributes, log.AttrRunnerName, "no runner while queued")
		assert.NotContains(t, rec.Attributes, log.AttrRunnerGroup)
		assert.Equal(t, []string{"ubuntu-latest"}, rec.Attributes[log.AttrLabels])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsTotal])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsCompleted])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsFailed])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsSkipped])
		assert.Equal(t, model.JobSpanID(jobID), rec.SpanID)
	})
	t.Run("job in progress", func(t *testing.T) {
		rec := log.Project(fixtureExecution(t, "workflow_job.in_progress"))
		assert.Equal(t, ts(10, 0, 10), rec.Time, "started_at while in progress")
		assert.Equal(t, "workflow_job in_progress: test", rec.Body)
		assert.Equal(t, "GitHub Actions 12", rec.Attributes[log.AttrRunnerName])
		assert.Equal(t, int64(4), rec.Attributes[log.AttrStepsTotal])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsCompleted])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsFailed])
		assert.Equal(t, int64(0), rec.Attributes[log.AttrStepsSkipped])
	})
}

func TestProject_PingHasEnvelopeAttributesOnly(t *testing.T) {
	rec := log.Project(fixtureExecution(t, "ping"))
	assert.Equal(t, otlp.LogRecord{
		Time:         received,
		ObservedTime: received,
		Severity:     otlp.SeverityInfo,
		SeverityText: log.SeverityTextInfo,
		Body:         "ping",
		Attributes: map[string]any{
			log.AttrDeliveryGUID:  "guid-ping",
			log.AttrWebhookEvent:  "ping",
			log.AttrKind:          "none",
			log.AttrReceivedAt:    receivedText,
			log.AttrSchemaVersion: int64(event.SchemaVersion),
		},
	}, rec)
	assert.Equal(t, [16]byte{}, rec.TraceID, "no trace to correlate with")
	assert.Equal(t, [8]byte{}, rec.SpanID)
}

func TestProject_UnknownEventWithActionAndRepository(t *testing.T) {
	env := event.Envelope{
		SchemaVersion: event.SchemaVersion,
		DeliveryGUID:  "guid-issues",
		Event:         "issues",
		Action:        "opened",
		HookID:        "570000001",
		ReceivedAt:    received,
		RepositoryID:  repoID,
		Repository:    repoName,
		Payload:       []byte(`{"action":"opened","issue":{"number":1}}`),
	}
	exec, err := model.Normalize(env)
	require.NoError(t, err)
	require.Equal(t, model.KindNone, exec.Kind)

	rec := log.Project(exec)
	assert.Equal(t, "issues opened", rec.Body)
	assert.Equal(t, received, rec.Time)
	assert.Equal(t, otlp.SeverityInfo, rec.Severity)
	assert.Equal(t, map[string]any{
		log.AttrDeliveryGUID:  "guid-issues",
		log.AttrWebhookEvent:  "issues",
		log.AttrWebhookAction: "opened",
		log.AttrHookID:        "570000001",
		log.AttrKind:          "none",
		log.AttrReceivedAt:    receivedText,
		log.AttrSchemaVersion: int64(event.SchemaVersion),
		log.AttrRepositoryID:  repoID,
		log.AttrRepository:    repoName,
	}, rec.Attributes)
	assert.Equal(t, [16]byte{}, rec.TraceID)
}

func TestProject_SeverityFromConclusion(t *testing.T) {
	cases := []struct {
		conclusion *string
		severity   otlp.Severity
		text       string
	}{
		{str("success"), otlp.SeverityInfo, log.SeverityTextInfo},
		{str("failure"), otlp.SeverityError, log.SeverityTextError},
		{str("timed_out"), otlp.SeverityError, log.SeverityTextError},
		{str("cancelled"), otlp.SeverityWarn, log.SeverityTextWarn},
		{str("skipped"), otlp.SeverityInfo, log.SeverityTextInfo},
		{str("neutral"), otlp.SeverityInfo, log.SeverityTextInfo},
		{str("action_required"), otlp.SeverityInfo, log.SeverityTextInfo},
		{nil, otlp.SeverityInfo, log.SeverityTextInfo},
	}
	for _, tc := range cases {
		name := "nil"
		if tc.conclusion != nil {
			name = *tc.conclusion
		}
		t.Run("run "+name, func(t *testing.T) {
			exec := fixtureExecution(t, "workflow_run.completed")
			exec.Run.Conclusion = tc.conclusion
			rec := log.Project(exec)
			assert.Equal(t, tc.severity, rec.Severity)
			assert.Equal(t, tc.text, rec.SeverityText)
			if tc.conclusion == nil {
				assert.NotContains(t, rec.Attributes, log.AttrConclusion)
			} else {
				assert.Equal(t, *tc.conclusion, rec.Attributes[log.AttrConclusion])
			}
		})
		t.Run("job "+name, func(t *testing.T) {
			exec := fixtureExecution(t, "workflow_job.completed")
			exec.Job.Conclusion = tc.conclusion
			rec := log.Project(exec)
			assert.Equal(t, tc.severity, rec.Severity)
			assert.Equal(t, tc.text, rec.SeverityText)
		})
	}
}

func TestProject_StepCountsIncludeTimedOut(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	exec.Job.Steps[1].Conclusion = str("timed_out")
	exec.Job.Steps[2].Status = model.StatusInProgress
	exec.Job.Steps[2].Conclusion = nil
	rec := log.Project(exec)
	assert.Equal(t, int64(6), rec.Attributes[log.AttrStepsTotal])
	assert.Equal(t, int64(5), rec.Attributes[log.AttrStepsCompleted])
	assert.Equal(t, int64(1), rec.Attributes[log.AttrStepsFailed], "timed_out counts as failed, the running step does not")
	assert.Equal(t, int64(1), rec.Attributes[log.AttrStepsSkipped])
}

func TestProject_OptionalFieldsOmitted(t *testing.T) {
	t.Run("run without name, branch, actor", func(t *testing.T) {
		exec := fixtureExecution(t, "workflow_run.completed")
		exec.Run.WorkflowName = nil
		exec.Run.HeadBranch = nil
		exec.Run.Actor = nil
		rec := log.Project(exec)
		assert.Equal(t, "workflow_run completed: .github/workflows/ci.yml", rec.Body, "path names a nameless workflow")
		assert.NotContains(t, rec.Attributes, log.AttrWorkflowName)
		assert.NotContains(t, rec.Attributes, log.AttrHeadBranch)
		assert.NotContains(t, rec.Attributes, log.AttrActor)
	})
	t.Run("job without workflow name, branch, labels", func(t *testing.T) {
		exec := fixtureExecution(t, "workflow_job.completed")
		exec.Job.WorkflowName = nil
		exec.Job.HeadBranch = nil
		exec.Job.Labels = nil
		rec := log.Project(exec)
		assert.Equal(t, "workflow_job completed: test", rec.Body)
		assert.NotContains(t, rec.Attributes, log.AttrWorkflowName)
		assert.NotContains(t, rec.Attributes, log.AttrHeadBranch)
		assert.NotContains(t, rec.Attributes, log.AttrLabels)
	})
	t.Run("labels are copied", func(t *testing.T) {
		exec := fixtureExecution(t, "workflow_job.completed")
		rec := log.Project(exec)
		exec.Job.Labels[0] = "changed"
		assert.Equal(t, []string{"ubuntu-latest"}, rec.Attributes[log.AttrLabels])
	})
}

func TestProject_MissingEventTimeFallsBackToReceivedAt(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	exec.Job.CompletedAt = nil
	rec := log.Project(exec)
	assert.Equal(t, received, rec.Time)
	assert.Equal(t, received, rec.ObservedTime)
}

func TestProject_DefensiveNilEntities(t *testing.T) {
	env := fixtureEnvelope(t, "workflow_run.completed")
	for _, exec := range []model.Execution{
		{Kind: model.KindRun, Envelope: env},
		{Kind: model.KindJob, Envelope: env},
		{Kind: model.Kind(99), Envelope: env},
	} {
		rec := log.Project(exec)
		assert.Equal(t, "workflow_run completed", rec.Body, "no entity, no name part")
		assert.Equal(t, received, rec.Time)
		assert.Equal(t, otlp.SeverityInfo, rec.Severity)
		assert.Equal(t, [16]byte{}, rec.TraceID)
		assert.Equal(t, [8]byte{}, rec.SpanID)
		assert.NotContains(t, rec.Attributes, log.AttrRunID)
		assert.Equal(t, repoID, rec.Attributes[log.AttrRepositoryID], "envelope repository survives")
	}
}

func TestProject_CorrelatesWithTraceSpans(t *testing.T) {
	for _, fixture := range []string{"workflow_run.completed", "workflow_job.completed"} {
		t.Run(fixture, func(t *testing.T) {
			spans, err := trace.Project(fixtureExecution(t, fixture))
			require.NoError(t, err)
			rec := log.Project(fixtureExecution(t, fixture))
			assert.Equal(t, spans[0].TraceID, rec.TraceID, "same trace as the span of the entity")
			assert.Equal(t, spans[0].SpanID, rec.SpanID, "same span as the entity's span")
		})
	}
	run := log.Project(fixtureExecution(t, "workflow_run.completed"))
	job := log.Project(fixtureExecution(t, "workflow_job.queued"))
	assert.Equal(t, run.TraceID, job.TraceID, "a job record shares the trace of its run even before the trace class emits anything")
	assert.NotEqual(t, run.SpanID, job.SpanID)
}

func TestProject_Deterministic(t *testing.T) {
	for _, fixture := range event.Fixtures() {
		t.Run(fixture, func(t *testing.T) {
			a := log.Project(fixtureExecution(t, fixture))
			b := log.Project(fixtureExecution(t, fixture))
			assert.Equal(t, a, b)
		})
	}
}

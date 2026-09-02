package trace_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/trace"
)

const (
	repoID = int64(987654321)
	runID  = int64(15000000001)
	jobID  = int64(42000000001)
)

func TestProject_CompletedRunIsRootSpan(t *testing.T) {
	spans, err := trace.Project(fixtureExecution(t, "workflow_run.completed"))
	require.NoError(t, err)
	require.Len(t, spans, 1)

	want := otlp.Span{
		TraceID: model.TraceID(repoID, runID, 1),
		SpanID:  model.RunSpanID(runID, 1),
		Name:    "workflow:CI",
		Kind:    otlp.SpanKindInternal,
		Start:   ts(10, 0, 0),
		End:     ts(10, 3, 20),
		Attributes: map[string]any{
			trace.AttrRepository:   "sokogen/antwatcher",
			trace.AttrWorkflowName: "CI",
			trace.AttrWorkflowPath: ".github/workflows/ci.yml",
			trace.AttrRunID:        runID,
			trace.AttrRunNumber:    int64(42),
			trace.AttrRunAttempt:   int64(1),
			trace.AttrEvent:        "push",
			trace.AttrHeadBranch:   "main",
			trace.AttrHeadSHA:      "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e",
			trace.AttrActor:        "sokogen",
			trace.AttrConclusion:   "failure",
			trace.AttrHTMLURL:      "https://github.com/sokogen/antwatcher/actions/runs/15000000001",
		},
		Status: otlp.Status{Code: otlp.StatusError, Message: "failure"},
	}
	assert.Equal(t, want, spans[0])
	assert.Equal(t, [8]byte{}, spans[0].ParentSpanID, "root span has no parent")
}

func TestProject_CompletedJobIsJobAndStepSpans(t *testing.T) {
	spans, err := trace.Project(fixtureExecution(t, "workflow_job.completed"))
	require.NoError(t, err)
	// six steps in the payload, step 4 was skipped and never ran
	require.Len(t, spans, 1+5)

	traceID := model.TraceID(repoID, runID, 1)
	jobSpanID := model.JobSpanID(jobID)

	job := spans[0]
	assert.Equal(t, otlp.Span{
		TraceID:      traceID,
		SpanID:       jobSpanID,
		ParentSpanID: model.RunSpanID(runID, 1),
		Name:         "job:test",
		Kind:         otlp.SpanKindInternal,
		Start:        ts(10, 0, 10),
		End:          ts(10, 3, 15),
		Attributes: map[string]any{
			trace.AttrRepository:   "sokogen/antwatcher",
			trace.AttrWorkflowName: "CI",
			trace.AttrRunID:        runID,
			trace.AttrRunAttempt:   int64(1),
			trace.AttrJobID:        jobID,
			trace.AttrHeadBranch:   "main",
			trace.AttrHeadSHA:      "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e",
			trace.AttrRunnerName:   "GitHub Actions 12",
			trace.AttrRunnerGroup:  "GitHub Actions",
			trace.AttrLabels:       []string{"ubuntu-latest"},
			trace.AttrConclusion:   "failure",
			trace.AttrHTMLURL:      "https://github.com/sokogen/antwatcher/actions/runs/15000000001/job/42000000001",
		},
		Status: otlp.Status{Code: otlp.StatusError, Message: "failure"},
	}, job)

	type stepWant struct {
		number     int64
		name       string
		conclusion string
		start, end [3]int
		status     otlp.Status
	}
	ok := otlp.Status{Code: otlp.StatusOk}
	wants := []stepWant{
		{1, "Set up job", "success", [3]int{10, 0, 10}, [3]int{10, 0, 12}, ok},
		{2, "Run actions/checkout@v4", "success", [3]int{10, 0, 12}, [3]int{10, 0, 14}, ok},
		{3, "Run make test", "failure", [3]int{10, 0, 14}, [3]int{10, 3, 10}, otlp.Status{Code: otlp.StatusError, Message: "failure"}},
		{5, "Post Run actions/checkout@v4", "success", [3]int{10, 3, 10}, [3]int{10, 3, 11}, ok},
		{6, "Complete job", "success", [3]int{10, 3, 11}, [3]int{10, 3, 12}, ok},
	}
	for i, w := range wants {
		got := spans[1+i]
		assert.Equal(t, otlp.Span{
			TraceID:      traceID,
			SpanID:       model.StepSpanID(jobID, w.number),
			ParentSpanID: jobSpanID,
			Name:         "step:" + w.name,
			Kind:         otlp.SpanKindInternal,
			Start:        ts(w.start[0], w.start[1], w.start[2]),
			End:          ts(w.end[0], w.end[1], w.end[2]),
			Attributes: map[string]any{
				trace.AttrStepNumber:     w.number,
				trace.AttrStepConclusion: w.conclusion,
			},
			Status: w.status,
		}, got, "step %d", w.number)
	}
	for _, s := range spans[1:] {
		assert.NotEqual(t, int64(4), s.Attributes[trace.AttrStepNumber], "skipped step must not produce a span")
	}
}

func TestProject_RunAndJobShareTraceAndParent(t *testing.T) {
	runSpans, err := trace.Project(fixtureExecution(t, "workflow_run.completed"))
	require.NoError(t, err)
	jobSpans, err := trace.Project(fixtureExecution(t, "workflow_job.completed"))
	require.NoError(t, err)

	root := runSpans[0]
	for _, s := range jobSpans {
		assert.Equal(t, root.TraceID, s.TraceID)
	}
	assert.Equal(t, root.SpanID, jobSpans[0].ParentSpanID, "job parent is the run root span")
	for _, s := range jobSpans[1:] {
		assert.Equal(t, jobSpans[0].SpanID, s.ParentSpanID, "steps are parented to the job")
	}
}

func TestProject_EarlierLifecycleAndOtherEventsAreSkipped(t *testing.T) {
	for _, fixture := range []string{
		"workflow_run.requested",
		"workflow_run.in_progress",
		"workflow_job.queued",
		"workflow_job.in_progress",
		"ping",
	} {
		t.Run(fixture, func(t *testing.T) {
			spans, err := trace.Project(fixtureExecution(t, fixture))
			require.ErrorIs(t, err, sink.ErrSkipped)
			assert.Nil(t, spans)
		})
	}
}

func TestProject_DefensiveNilEntities(t *testing.T) {
	for _, exec := range []model.Execution{
		{Kind: model.KindRun},
		{Kind: model.KindJob},
		{Kind: model.KindNone},
		{Kind: model.Kind(99)},
	} {
		_, err := trace.Project(exec)
		require.ErrorIs(t, err, sink.ErrSkipped)
	}
}

func TestProject_Deterministic(t *testing.T) {
	for _, fixture := range []string{"workflow_run.completed", "workflow_job.completed"} {
		a, err := trace.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		b, err := trace.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		assert.Equal(t, a, b, fixture)
	}
}

func TestProject_StatusFromConclusion(t *testing.T) {
	cases := []struct {
		conclusion *string
		want       otlp.Status
	}{
		{str("success"), otlp.Status{Code: otlp.StatusOk}},
		{str("failure"), otlp.Status{Code: otlp.StatusError, Message: "failure"}},
		{str("timed_out"), otlp.Status{Code: otlp.StatusError, Message: "timed_out"}},
		{str("cancelled"), otlp.Status{}},
		{str("skipped"), otlp.Status{}},
		{str("neutral"), otlp.Status{}},
		{str("action_required"), otlp.Status{}},
		{nil, otlp.Status{}},
	}
	for _, tc := range cases {
		name := "nil"
		if tc.conclusion != nil {
			name = *tc.conclusion
		}
		t.Run(name, func(t *testing.T) {
			exec := fixtureExecution(t, "workflow_run.completed")
			exec.Run.Conclusion = tc.conclusion
			spans, err := trace.Project(exec)
			require.NoError(t, err)
			assert.Equal(t, tc.want, spans[0].Status)
			if tc.conclusion == nil {
				assert.NotContains(t, spans[0].Attributes, trace.AttrConclusion)
			} else {
				assert.Equal(t, *tc.conclusion, spans[0].Attributes[trace.AttrConclusion])
			}
		})
	}
}

func TestProject_RunFallbacks(t *testing.T) {
	exec := fixtureExecution(t, "workflow_run.completed")
	exec.Run.StartedAt = nil
	exec.Run.WorkflowName = nil
	exec.Run.HeadBranch = nil
	exec.Run.Actor = nil

	spans, err := trace.Project(exec)
	require.NoError(t, err)
	got := spans[0]
	assert.Equal(t, "workflow:.github/workflows/ci.yml", got.Name, "path names the span when the workflow has no name")
	assert.Equal(t, exec.Run.CreatedAt, got.Start, "created_at stands in for a missing run_started_at")
	assert.NotContains(t, got.Attributes, trace.AttrWorkflowName)
	assert.NotContains(t, got.Attributes, trace.AttrHeadBranch)
	assert.NotContains(t, got.Attributes, trace.AttrActor)
}

func TestProject_JobFallbacks(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	exec.Job.StartedAt = nil
	exec.Job.CompletedAt = nil
	exec.Job.WorkflowName = nil
	exec.Job.HeadBranch = nil
	exec.Job.RunnerName = nil
	exec.Job.RunnerGroup = nil
	exec.Job.Labels = nil
	exec.Job.Steps = nil

	spans, err := trace.Project(exec)
	require.NoError(t, err)
	require.Len(t, spans, 1)
	got := spans[0]
	assert.Equal(t, exec.Job.CreatedAt, got.Start, "created_at stands in for a missing started_at")
	assert.Equal(t, received, got.End, "the envelope time stands in for a missing completed_at")
	for _, key := range []string{trace.AttrWorkflowName, trace.AttrHeadBranch, trace.AttrRunnerName, trace.AttrRunnerGroup, trace.AttrLabels} {
		assert.NotContains(t, got.Attributes, key)
	}
}

func TestProject_StepWithOneTimestampIsOmitted(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	exec.Job.Steps = []model.Step{
		{Number: 1, Name: "started only", Status: "in_progress", StartedAt: tp(10, 0, 10)},
		{Number: 2, Name: "completed only", Status: "completed", Conclusion: str("success"), CompletedAt: tp(10, 0, 12)},
		{Number: 3, Name: "both", Status: "completed", Conclusion: str("success"), StartedAt: tp(10, 0, 12), CompletedAt: tp(10, 0, 14)},
	}
	spans, err := trace.Project(exec)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	assert.Equal(t, "step:both", spans[1].Name)
}

func TestProject_LabelsAreCopied(t *testing.T) {
	exec := fixtureExecution(t, "workflow_job.completed")
	spans, err := trace.Project(exec)
	require.NoError(t, err)
	labels := spans[0].Attributes[trace.AttrLabels].([]string)
	labels[0] = "mutated"
	assert.Equal(t, []string{"ubuntu-latest"}, exec.Job.Labels, "projection must not alias the model's slice")
}

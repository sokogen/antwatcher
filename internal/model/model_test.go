package model

import (
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func ts(h, m, s int) time.Time { return time.Date(2026, 9, 2, h, m, s, 0, time.UTC) }

func tp(h, m, s int) *time.Time {
	t := ts(h, m, s)
	return &t
}

func str(s string) *string { return &s }

// fixtureEnvelope builds the envelope the receiver would produce for a fixture.
func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

// rawEnvelope builds an envelope for an arbitrary payload without going through
// FromWebhook, so malformed bodies can be injected.
func rawEnvelope(eventName, payload string) event.Envelope {
	return event.Envelope{
		SchemaVersion: event.SchemaVersion,
		DeliveryGUID:  "guid-raw",
		Event:         eventName,
		ReceivedAt:    received,
		Payload:       []byte(payload),
	}
}

func goldenRun(status string, conclusion *string, updated time.Time) *Run {
	return &Run{
		RepositoryID: 987654321,
		Repository:   "sokogen/antwatcher",
		WorkflowID:   12345678,
		WorkflowName: str("CI"),
		WorkflowPath: ".github/workflows/ci.yml",
		RunID:        15000000001,
		RunNumber:    42,
		RunAttempt:   1,
		Status:       status,
		Conclusion:   conclusion,
		CreatedAt:    ts(10, 0, 0),
		UpdatedAt:    updated,
		StartedAt:    tp(10, 0, 0),
		TriggerEvent: "push",
		HeadBranch:   str("main"),
		HeadSHA:      "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e",
		Actor:        str("sokogen"),
		HTMLURL:      "https://github.com/sokogen/antwatcher/actions/runs/15000000001",
	}
}

func goldenJob(status string, conclusion *string, started, completed *time.Time, runner bool, steps []Step) *Job {
	j := &Job{
		RepositoryID: 987654321,
		Repository:   "sokogen/antwatcher",
		RunID:        15000000001,
		RunAttempt:   1,
		JobID:        42000000001,
		Name:         "test",
		WorkflowName: str("CI"),
		Status:       status,
		Conclusion:   conclusion,
		CreatedAt:    ts(10, 0, 2),
		StartedAt:    started,
		CompletedAt:  completed,
		Labels:       []string{"ubuntu-latest"},
		HeadBranch:   str("main"),
		HeadSHA:      "3f1c9e2b7a6d5c4e8f0a1b2c3d4e5f6a7b8c9d0e",
		HTMLURL:      "https://github.com/sokogen/antwatcher/actions/runs/15000000001/job/42000000001",
		Steps:        steps,
	}
	if runner {
		j.RunnerName = str("GitHub Actions 12")
		j.RunnerGroup = str("GitHub Actions")
	}
	return j
}

func TestNormalize_Fixtures(t *testing.T) {
	tests := []struct {
		fixture string
		kind    Kind
		run     *Run
		job     *Job
	}{
		{"ping", KindNone, nil, nil},
		{"workflow_run.requested", KindRun, goldenRun(StatusQueued, nil, ts(10, 0, 0)), nil},
		{"workflow_run.in_progress", KindRun, goldenRun(StatusInProgress, nil, ts(10, 0, 5)), nil},
		{"workflow_run.completed", KindRun, goldenRun(StatusCompleted, str("failure"), ts(10, 3, 20)), nil},
		{"workflow_job.queued", KindJob, nil, goldenJob(StatusQueued, nil, tp(10, 0, 2), nil, false, nil)},
		{"workflow_job.in_progress", KindJob, nil, goldenJob(StatusInProgress, nil, tp(10, 0, 10), nil, true, []Step{
			{Number: 1, Name: "Set up job", Status: StatusInProgress, StartedAt: tp(10, 0, 10)},
			{Number: 2, Name: "Run actions/checkout@v4", Status: StatusQueued},
			{Number: 3, Name: "Run make test", Status: StatusQueued},
			{Number: 4, Name: "Upload coverage", Status: StatusQueued},
		})},
		{"workflow_job.completed", KindJob, nil, goldenJob(StatusCompleted, str("failure"), tp(10, 0, 10), tp(10, 3, 15), true, []Step{
			{Number: 1, Name: "Set up job", Status: StatusCompleted, Conclusion: str("success"), StartedAt: tp(10, 0, 10), CompletedAt: tp(10, 0, 12)},
			{Number: 2, Name: "Run actions/checkout@v4", Status: StatusCompleted, Conclusion: str("success"), StartedAt: tp(10, 0, 12), CompletedAt: tp(10, 0, 14)},
			{Number: 3, Name: "Run make test", Status: StatusCompleted, Conclusion: str("failure"), StartedAt: tp(10, 0, 14), CompletedAt: tp(10, 3, 10)},
			{Number: 4, Name: "Upload coverage", Status: StatusCompleted, Conclusion: str("skipped")},
			{Number: 5, Name: "Post Run actions/checkout@v4", Status: StatusCompleted, Conclusion: str("success"), StartedAt: tp(10, 3, 10), CompletedAt: tp(10, 3, 11)},
			{Number: 6, Name: "Complete job", Status: StatusCompleted, Conclusion: str("success"), StartedAt: tp(10, 3, 11), CompletedAt: tp(10, 3, 12)},
		})},
	}
	require.Len(t, tests, len(event.Fixtures()), "every fixture must be covered")

	for _, tc := range tests {
		t.Run(tc.fixture, func(t *testing.T) {
			env := fixtureEnvelope(t, tc.fixture)
			exec, err := Normalize(env)
			require.NoError(t, err)
			assert.Equal(t, Execution{Kind: tc.kind, Run: tc.run, Job: tc.job, Envelope: env}, exec)
		})
	}
}

func TestNormalize_JobIsSparse(t *testing.T) {
	exec, err := Normalize(fixtureEnvelope(t, "workflow_job.queued"))
	require.NoError(t, err)
	require.Equal(t, KindJob, exec.Kind)
	job := exec.Job

	assert.Nil(t, exec.Run, "a job event never carries a run")
	assert.Nil(t, job.RunnerName, "no runner while queued")
	assert.Nil(t, job.RunnerGroup)
	assert.Nil(t, job.Conclusion)
	assert.Nil(t, job.CompletedAt)
	assert.Nil(t, job.Steps)
}

func TestNormalize_SkippedStepKeepsNilTimestamps(t *testing.T) {
	exec, err := Normalize(fixtureEnvelope(t, "workflow_job.completed"))
	require.NoError(t, err)
	require.Len(t, exec.Job.Steps, 6)

	skipped := exec.Job.Steps[3]
	assert.Equal(t, "Upload coverage", skipped.Name)
	assert.Equal(t, str("skipped"), skipped.Conclusion)
	assert.Equal(t, StatusCompleted, skipped.Status)
	assert.Nil(t, skipped.StartedAt)
	assert.Nil(t, skipped.CompletedAt)

	failed := exec.Job.Steps[2]
	assert.Equal(t, str("failure"), failed.Conclusion)
	assert.NotNil(t, failed.StartedAt)
	assert.NotNil(t, failed.CompletedAt)
}

func TestNormalize_UnknownEventIsKindNone(t *testing.T) {
	for _, name := range []string{"push", "check_run", "pull_request", "workflow_dispatch"} {
		t.Run(name, func(t *testing.T) {
			env := rawEnvelope(name, `{"not":"parsed at all", "workflow_run": 12}`)
			exec, err := Normalize(env)
			require.NoError(t, err)
			assert.Equal(t, Execution{Kind: KindNone, Envelope: env}, exec)
		})
	}
}

func TestNormalize_TimestampsAreUTC(t *testing.T) {
	env := rawEnvelope(EventWorkflowJob, `{
		"workflow_job": {"id": 7, "run_id": 8, "run_attempt": 2,
			"created_at": "2026-09-02T12:00:02+02:00", "started_at": "2026-09-02T12:00:10+02:00",
			"steps": [{"number": 1, "name": "s", "status": "in_progress", "started_at": "2026-09-02T12:00:11+02:00"}]},
		"repository": {"id": 5, "full_name": "o/r"}}`)
	exec, err := Normalize(env)
	require.NoError(t, err)
	assert.Equal(t, ts(10, 0, 2), exec.Job.CreatedAt)
	assert.Equal(t, time.UTC, exec.Job.CreatedAt.Location())
	assert.Equal(t, tp(10, 0, 10), exec.Job.StartedAt)
	assert.Equal(t, time.UTC, exec.Job.StartedAt.Location())
	assert.Equal(t, tp(10, 0, 11), exec.Job.Steps[0].StartedAt)
}

func TestNormalize_NullStatusIsEmpty(t *testing.T) {
	env := rawEnvelope(EventWorkflowRun, `{
		"workflow_run": {"id": 1, "run_attempt": 1, "status": null, "name": null, "head_branch": null, "actor": null},
		"repository": {"id": 5, "full_name": "o/r"}}`)
	exec, err := Normalize(env)
	require.NoError(t, err)
	assert.Empty(t, exec.Run.Status)
	assert.Equal(t, RankUnknown, StatusRank(exec.Run.Status))
	assert.Nil(t, exec.Run.WorkflowName)
	assert.Nil(t, exec.Run.HeadBranch)
	assert.Nil(t, exec.Run.Actor)
	assert.Nil(t, exec.Run.StartedAt)
}

func TestNormalize_RepositoryFallsBackToEnvelope(t *testing.T) {
	env := rawEnvelope(EventWorkflowRun, `{"workflow_run": {"id": 1, "run_attempt": 1}}`)
	env.RepositoryID, env.Repository = 55, "from/envelope"
	exec, err := Normalize(env)
	require.NoError(t, err)
	assert.Equal(t, int64(55), exec.Run.RepositoryID)
	assert.Equal(t, "from/envelope", exec.Run.Repository)
}

func TestNormalize_Malformed(t *testing.T) {
	tests := []struct {
		name    string
		event   string
		payload string
	}{
		{"run empty payload", EventWorkflowRun, ""},
		{"run invalid json", EventWorkflowRun, `{"workflow_run": `},
		{"run missing object", EventWorkflowRun, `{"action": "completed", "repository": {"id": 1}}`},
		{"run wrong type", EventWorkflowRun, `{"workflow_run": "nope", "repository": {"id": 1}}`},
		{"run id zero", EventWorkflowRun, `{"workflow_run": {"run_attempt": 1}, "repository": {"id": 1}}`},
		{"run attempt zero", EventWorkflowRun, `{"workflow_run": {"id": 1}, "repository": {"id": 1}}`},
		{"run missing repository", EventWorkflowRun, `{"workflow_run": {"id": 1, "run_attempt": 1}}`},
		{"run bad timestamp", EventWorkflowRun, `{"workflow_run": {"id": 1, "run_attempt": 1, "created_at": "yesterday"}, "repository": {"id": 1}}`},
		{"job empty payload", EventWorkflowJob, ""},
		{"job invalid json", EventWorkflowJob, `[`},
		{"job missing object", EventWorkflowJob, `{"action": "queued", "repository": {"id": 1}}`},
		{"job wrong type", EventWorkflowJob, `{"workflow_job": [], "repository": {"id": 1}}`},
		{"job id zero", EventWorkflowJob, `{"workflow_job": {"run_id": 1, "run_attempt": 1}, "repository": {"id": 1}}`},
		{"job run id zero", EventWorkflowJob, `{"workflow_job": {"id": 1, "run_attempt": 1}, "repository": {"id": 1}}`},
		{"job attempt zero", EventWorkflowJob, `{"workflow_job": {"id": 1, "run_id": 1}, "repository": {"id": 1}}`},
		{"job missing repository", EventWorkflowJob, `{"workflow_job": {"id": 1, "run_id": 1, "run_attempt": 1}}`},
		{"job bad step", EventWorkflowJob, `{"workflow_job": {"id": 1, "run_id": 1, "run_attempt": 1, "steps": [1]}, "repository": {"id": 1}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := Normalize(rawEnvelope(tc.event, tc.payload))
			require.ErrorIs(t, err, ErrMalformed)
			assert.Contains(t, err.Error(), "guid-raw", "error names the delivery")
			assert.Equal(t, Execution{}, exec)
		})
	}
}

func TestKind_String(t *testing.T) {
	assert.Equal(t, "none", KindNone.String())
	assert.Equal(t, "run", KindRun.String())
	assert.Equal(t, "job", KindJob.String())
	assert.Equal(t, "unknown", Kind(99).String())
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func TestIDs_PinnedGoldenValues(t *testing.T) {
	trace := TraceID(987654321, 15000000001, 1)
	assert.Equal(t, "6b70ae3e8410ccd2cdfa9226a52b32aa", hexOf(trace[:]))

	run := RunSpanID(15000000001, 1)
	assert.Equal(t, "d86ec47ef6d13b92", hexOf(run[:]))

	job := JobSpanID(42000000001)
	assert.Equal(t, "62806c909b4fdcb9", hexOf(job[:]))

	step := StepSpanID(42000000001, 3)
	assert.Equal(t, "43dc0641dc1d2762", hexOf(step[:]))

	assert.Equal(t, "33a3323170eea63bfa2d9d2a6522d3b33bf48b56eb16820e061336bb2874ca08",
		RecordID(RecordRun, 987654321, 15000000001, 1))
	assert.Equal(t, "8dc1a2e3d15e9133f5ab4a8334a3d5ed9fbaa02a3b61ce5330af18c8753075fe",
		RecordID(RecordJob, 987654321, 15000000001, 1, 42000000001))
	assert.Equal(t, "82720684789b1ecd40e836f117c0ba050e620583923dc3795864bcea80b8cd4d",
		RecordID(RecordStep, 987654321, 15000000001, 1, 42000000001, 3))
}

func TestIDs_DeterministicAndDistinct(t *testing.T) {
	// Determinism across processes is proven by the pinned values above; this
	// checks that repeated calls in one process agree too.
	trace, run, record := TraceID(1, 2, 3), RunSpanID(2, 3), RecordID(RecordRun, 1, 2, 3)
	for range 3 {
		assert.Equal(t, trace, TraceID(1, 2, 3))
		assert.Equal(t, run, RunSpanID(2, 3))
		assert.Equal(t, record, RecordID(RecordRun, 1, 2, 3))
	}

	assert.NotEqual(t, TraceID(1, 2, 3), TraceID(1, 2, 4), "a re-run attempt is a new trace")
	assert.NotEqual(t, TraceID(1, 2, 3), TraceID(9, 2, 3), "same run id in another repository is a different trace")

	assert.NotEqual(t, RunSpanID(2, 3), RunSpanID(2, 4))
	assert.NotEqual(t, RunSpanID(7, 1), JobSpanID(7), "namespaces keep run and job spans apart")
	assert.NotEqual(t, JobSpanID(7), StepSpanID(7, 1))
	assert.NotEqual(t, StepSpanID(7, 1), StepSpanID(7, 2))

	assert.NotEqual(t, RecordID(RecordRun, 1, 2, 3), RecordID(RecordJob, 1, 2, 3), "kind is part of the identity")
	assert.NotEqual(t, RecordID(RecordJob, 1, 2, 3, 4), RecordID(RecordStep, 1, 2, 3, 4))
	assert.NotEqual(t, RecordID(RecordRun, 1, 23), RecordID(RecordRun, 12, 3), "ids are delimited, not concatenated")
	assert.Len(t, RecordID(RecordRun, 1), 64)
}

func TestIDs_EntityMethodsLineUp(t *testing.T) {
	runExec, err := Normalize(fixtureEnvelope(t, "workflow_run.completed"))
	require.NoError(t, err)
	jobExec, err := Normalize(fixtureEnvelope(t, "workflow_job.completed"))
	require.NoError(t, err)
	run, job := runExec.Run, jobExec.Job

	assert.Equal(t, run.TraceID(), job.TraceID(), "job spans join the run's trace")
	assert.Equal(t, TraceID(987654321, 15000000001, 1), run.TraceID())
	assert.Equal(t, run.SpanID(), job.ParentSpanID(), "job span's parent is the run span")
	assert.Equal(t, RunSpanID(15000000001, 1), run.SpanID())
	assert.Equal(t, JobSpanID(42000000001), job.SpanID())
	assert.NotEqual(t, run.SpanID(), job.SpanID())

	step := job.Steps[2]
	assert.Equal(t, StepSpanID(42000000001, 3), step.SpanID(job))

	assert.Equal(t, RecordID(RecordRun, 987654321, 15000000001, 1), run.RecordID())
	assert.Equal(t, RecordID(RecordJob, 987654321, 15000000001, 1, 42000000001), job.RecordID())
	assert.Equal(t, RecordID(RecordStep, 987654321, 15000000001, 1, 42000000001, 3), step.RecordID(job))

	// Record ids name the entity, not the event: every event of the same
	// entity yields the same id.
	queued, err := Normalize(fixtureEnvelope(t, "workflow_job.queued"))
	require.NoError(t, err)
	assert.Equal(t, job.RecordID(), queued.Job.RecordID())
	requested, err := Normalize(fixtureEnvelope(t, "workflow_run.requested"))
	require.NoError(t, err)
	assert.Equal(t, run.RecordID(), requested.Run.RecordID())
}

func TestEventTime_Table(t *testing.T) {
	env := event.Envelope{ReceivedAt: received}
	created, started, updated, completed := ts(1, 0, 0), ts(2, 0, 0), ts(3, 0, 0), ts(4, 0, 0)

	runExec := func(r Run) Execution { return Execution{Kind: KindRun, Run: &r, Envelope: env} }
	jobExec := func(j Job) Execution { return Execution{Kind: KindJob, Job: &j, Envelope: env} }

	tests := []struct {
		name string
		exec Execution
		want time.Time
	}{
		{"run completed → updated_at", runExec(Run{Status: StatusCompleted, CreatedAt: created, StartedAt: &started, UpdatedAt: updated}), updated},
		{"run completed, updated_at missing → received_at", runExec(Run{Status: StatusCompleted, CreatedAt: created}), received},
		{"run in_progress → run_started_at", runExec(Run{Status: StatusInProgress, CreatedAt: created, StartedAt: &started, UpdatedAt: updated}), started},
		{"run in_progress, run_started_at missing → received_at", runExec(Run{Status: StatusInProgress, CreatedAt: created, UpdatedAt: updated}), received},
		{"run queued → created_at", runExec(Run{Status: StatusQueued, CreatedAt: created, StartedAt: &started, UpdatedAt: updated}), created},
		{"run requested → created_at", runExec(Run{Status: StatusRequested, CreatedAt: created, UpdatedAt: updated}), created},
		{"run pending → created_at", runExec(Run{Status: StatusPending, CreatedAt: created, UpdatedAt: updated}), created},
		{"run unknown status → created_at", runExec(Run{Status: "", CreatedAt: created, UpdatedAt: updated}), created},
		{"run other, created_at missing → received_at", runExec(Run{Status: StatusQueued, UpdatedAt: updated}), received},

		{"job completed → completed_at", jobExec(Job{Status: StatusCompleted, CreatedAt: created, StartedAt: &started, CompletedAt: &completed}), completed},
		{"job completed, completed_at missing → received_at", jobExec(Job{Status: StatusCompleted, CreatedAt: created, StartedAt: &started}), received},
		{"job in_progress → started_at", jobExec(Job{Status: StatusInProgress, CreatedAt: created, StartedAt: &started}), started},
		{"job in_progress, started_at missing → received_at", jobExec(Job{Status: StatusInProgress, CreatedAt: created}), received},
		{"job queued → created_at", jobExec(Job{Status: StatusQueued, CreatedAt: created, StartedAt: &started}), created},
		{"job waiting → created_at", jobExec(Job{Status: StatusWaiting, CreatedAt: created, StartedAt: &started}), created},
		{"job unknown status → created_at", jobExec(Job{Status: "", CreatedAt: created}), created},
		{"job other, created_at missing → received_at", jobExec(Job{Status: StatusQueued, StartedAt: &started}), received},

		{"none → received_at", Execution{Kind: KindNone, Envelope: env}, received},
		{"run kind without run → received_at", Execution{Kind: KindRun, Envelope: env}, received},
		{"job kind without job → received_at", Execution{Kind: KindJob, Envelope: env}, received},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, EventTime(tc.exec))
		})
	}
}

func TestStepEventTime_Table(t *testing.T) {
	env := event.Envelope{ReceivedAt: received}
	created, started, completed := ts(1, 0, 0), ts(2, 0, 0), ts(4, 0, 0)
	// The job is in_progress, so the job's EventTime is started.
	job := Job{Status: StatusInProgress, CreatedAt: created, StartedAt: &started}
	jobExec := Execution{Kind: KindJob, Job: &job, Envelope: env}
	stepStarted, stepCompleted := ts(2, 30, 0), ts(3, 0, 0)

	tests := []struct {
		name string
		exec Execution
		step Step
		want time.Time
	}{
		{"completed → completed_at", jobExec, Step{Status: StatusCompleted, StartedAt: &stepStarted, CompletedAt: &stepCompleted}, stepCompleted},
		{"completed, completed_at missing (skipped) → job event time", jobExec, Step{Status: StatusCompleted}, started},
		{"in_progress → started_at", jobExec, Step{Status: StatusInProgress, StartedAt: &stepStarted}, stepStarted},
		{"in_progress, started_at missing → job event time", jobExec, Step{Status: StatusInProgress}, started},
		{"queued → job event time", jobExec, Step{Status: StatusQueued, StartedAt: &stepStarted}, started},
		{"unknown → job event time", jobExec, Step{Status: ""}, started},
		{"job event time falls back to received_at", Execution{Kind: KindJob, Job: &Job{Status: StatusInProgress}, Envelope: env}, Step{Status: StatusQueued}, received},
		{"non-job execution → EventTime", Execution{Kind: KindRun, Run: &Run{Status: StatusCompleted, UpdatedAt: completed}, Envelope: env}, Step{Status: StatusCompleted, CompletedAt: &stepCompleted}, completed},
		{"none execution → received_at", Execution{Kind: KindNone, Envelope: env}, Step{Status: StatusCompleted, CompletedAt: &stepCompleted}, received},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, StepEventTime(tc.exec, tc.step))
		})
	}
}

func TestEventTime_Fixtures(t *testing.T) {
	tests := []struct {
		fixture string
		want    time.Time
	}{
		{"ping", received},
		{"workflow_run.requested", ts(10, 0, 0)},
		{"workflow_run.in_progress", ts(10, 0, 0)},
		{"workflow_run.completed", ts(10, 3, 20)},
		{"workflow_job.queued", ts(10, 0, 2)},
		{"workflow_job.in_progress", ts(10, 0, 10)},
		{"workflow_job.completed", ts(10, 3, 15)},
	}
	require.Len(t, tests, len(event.Fixtures()))
	for _, tc := range tests {
		t.Run(tc.fixture, func(t *testing.T) {
			exec, err := Normalize(fixtureEnvelope(t, tc.fixture))
			require.NoError(t, err)
			assert.Equal(t, tc.want, EventTime(exec))
		})
	}
}

func TestStatusRank_Table(t *testing.T) {
	tests := []struct {
		status string
		want   int
	}{
		{StatusCompleted, 3},
		{StatusInProgress, 2},
		{StatusQueued, 1},
		{StatusWaiting, 1},
		{StatusRequested, 1},
		{StatusPending, 1},
		{"", 0},
		{"cancelled", 0},
		{"COMPLETED", 0},
	}
	for _, tc := range tests {
		t.Run("status="+tc.status, func(t *testing.T) {
			assert.Equal(t, tc.want, StatusRank(tc.status))
		})
	}
	assert.Greater(t, StatusRank(StatusCompleted), StatusRank(StatusInProgress))
	assert.Greater(t, StatusRank(StatusInProgress), StatusRank(StatusQueued))
	assert.Greater(t, StatusRank(StatusQueued), StatusRank(""))
}

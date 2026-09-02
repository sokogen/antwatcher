package analytics_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
)

// fakeWriter records every call and answers with programmable errors.
type fakeWriter struct {
	mu        sync.Mutex
	ensures   int
	ensureErr error
	ensured   []analytics.Schema
	inEnsure  int // concurrent EnsureSchema calls observed
	maxEnsure int
	writes    [][]analytics.Record
	writeErr  error
	closeErr  error
	closed    int
	lastCtx   context.Context
}

func (f *fakeWriter) EnsureSchema(ctx context.Context, schema analytics.Schema) error {
	f.mu.Lock()
	f.ensures++
	f.inEnsure++
	if f.inEnsure > f.maxEnsure {
		f.maxEnsure = f.inEnsure
	}
	f.ensured = append(f.ensured, schema)
	f.lastCtx = ctx
	err := f.ensureErr
	f.mu.Unlock()

	time.Sleep(2 * time.Millisecond) // give concurrent callers a chance to overlap

	f.mu.Lock()
	f.inEnsure--
	f.mu.Unlock()
	return err
}

func (f *fakeWriter) Write(ctx context.Context, records []analytics.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, records)
	f.lastCtx = ctx
	return f.writeErr
}

func (f *fakeWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func (f *fakeWriter) setEnsureErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureErr = err
}

func (f *fakeWriter) counts() (ensures, writes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensures, len(f.writes)
}

func TestSink_NameAndClass(t *testing.T) {
	s := analytics.New("bq", &fakeWriter{}, true)
	assert.Equal(t, "bq", s.Name())
	assert.Equal(t, sink.ClassAnalytics, s.Class())
}

func TestNew_NilWriterPanics(t *testing.T) {
	assert.PanicsWithValue(t, "analytics: New(bq) with nil writer", func() { analytics.New("bq", nil, true) })
}

func TestNew_DoesNotEnsureSchema(t *testing.T) {
	w := &fakeWriter{}
	analytics.New("bq", w, true)
	ensures, writes := w.counts()
	assert.Equal(t, 0, ensures, "constructors never touch the warehouse")
	assert.Equal(t, 0, writes)
}

func TestSink_ProcessWritesProjection(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, false)
	for _, fixture := range []string{"workflow_run.completed", "workflow_job.completed", "workflow_job.queued"} {
		require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, fixture)), fixture)
	}
	ensures, writes := w.counts()
	assert.Equal(t, 0, ensures, "ensure disabled")
	require.Equal(t, 3, writes, "one Write per event")
	for i, fixture := range []string{"workflow_run.completed", "workflow_job.completed", "workflow_job.queued"} {
		want, err := analytics.Project(fixtureExecution(t, fixture))
		require.NoError(t, err)
		assert.Equal(t, want, w.writes[i], fixture)
	}
	assert.Len(t, w.writes[1], 7, "job + six steps in a single write")
}

func TestSink_ProcessSkipsWithoutTouchingWriter(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	err := s.Process(context.Background(), fixtureEnvelope(t, "ping"))
	require.ErrorIs(t, err, sink.ErrSkipped)
	ensures, writes := w.counts()
	assert.Equal(t, 0, ensures, "a skipped event does not trigger schema management")
	assert.Equal(t, 0, writes)
}

func TestSink_ProcessMalformedIsPermanent(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	cases := map[string]string{
		"invalid json":     `{"workflow_run": `,
		"missing entity":   `{"action":"completed","repository":{"id":1}}`,
		"missing identity": `{"workflow_run":{"name":"CI"},"repository":{"id":1}}`,
		"missing repo":     `{"workflow_run":{"id":1,"run_attempt":1}}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			err := s.Process(context.Background(), rawEnvelope("workflow_run", payload))
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), "%v", err)
			assert.ErrorIs(t, err, model.ErrMalformed)
		})
	}
	ensures, writes := w.counts()
	assert.Equal(t, 0, ensures)
	assert.Equal(t, 0, writes)
}

func TestSink_EnsureSchemaLazilyOnFirstProcess(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	env := fixtureEnvelope(t, "workflow_run.completed")

	require.NoError(t, s.Process(context.Background(), env))
	ensures, writes := w.counts()
	assert.Equal(t, 1, ensures, "ensured before the first write")
	assert.Equal(t, 1, writes)
	assert.Equal(t, analytics.Current, w.ensured[0], "the class passes its own schema")

	require.NoError(t, s.Process(context.Background(), env))
	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.completed")))
	ensures, writes = w.counts()
	assert.Equal(t, 1, ensures, "ensured once after success")
	assert.Equal(t, 3, writes)
}

func TestSink_EnsureSchemaRetriedUntilSuccess(t *testing.T) {
	boom := errors.New("connection refused")
	w := &fakeWriter{ensureErr: boom}
	s := analytics.New("bq", w, true)
	env := fixtureEnvelope(t, "workflow_job.completed")

	for i := range 3 {
		err := s.Process(context.Background(), env)
		require.ErrorIs(t, err, boom, "attempt %d", i)
		assert.False(t, sink.IsPermanent(err), "the driver's classification is kept")
		assert.Contains(t, err.Error(), "ensure analytics schema")
		assert.Contains(t, err.Error(), "guid-workflow_job.completed")
	}
	ensures, writes := w.counts()
	assert.Equal(t, 3, ensures, "retried on every event")
	assert.Equal(t, 0, writes, "nothing is written while the schema is not ensured")

	w.setEnsureErr(nil)
	require.NoError(t, s.Process(context.Background(), env))
	require.NoError(t, s.Process(context.Background(), env))
	ensures, writes = w.counts()
	assert.Equal(t, 4, ensures, "one more attempt, then done")
	assert.Equal(t, 2, writes)
}

func TestSink_EnsureSchemaPermanentStaysPermanent(t *testing.T) {
	w := &fakeWriter{ensureErr: sink.Permanentf("permission denied")}
	s := analytics.New("bq", w, true)
	err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
	_, writes := w.counts()
	assert.Equal(t, 0, writes)
}

func TestSink_EnsureSchemaRunsOnceUnderConcurrency(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	env := fixtureEnvelope(t, "workflow_run.completed")

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.Process(context.Background(), env)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
	}
	ensures, writes := w.counts()
	assert.Equal(t, 1, ensures, "concurrent first calls share one EnsureSchema")
	assert.Equal(t, 1, w.maxEnsure, "never overlapping")
	assert.Equal(t, n, writes)
}

func TestSink_ProcessPropagatesWriterErrors(t *testing.T) {
	t.Run("retryable stays retryable", func(t *testing.T) {
		w := &fakeWriter{writeErr: errors.New("deadline exceeded")}
		s := analytics.New("bq", w, true)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.completed"))
		require.Error(t, err)
		require.ErrorIs(t, err, w.writeErr)
		assert.False(t, sink.IsPermanent(err))
		require.NotErrorIs(t, err, sink.ErrSkipped)
		assert.Contains(t, err.Error(), "write 7 analytics record(s) of workflow_job")
		assert.Contains(t, err.Error(), "guid-workflow_job.completed")
	})
	t.Run("permanent stays permanent", func(t *testing.T) {
		w := &fakeWriter{writeErr: sink.Permanentf("schema mismatch")}
		s := analytics.New("bq", w, false)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
	})
}

func TestSink_ProcessPassesContext(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	require.NoError(t, s.Process(ctx, fixtureEnvelope(t, "workflow_run.completed")))
	assert.Equal(t, "v", w.lastCtx.Value(key{}))
}

func TestSink_CloseClosesWriter(t *testing.T) {
	w := &fakeWriter{closeErr: errors.New("boom")}
	s := analytics.New("bq", w, true)
	require.EqualError(t, s.Close(), "boom")
	assert.Equal(t, 1, w.closed)
}

func TestSink_EveryFixtureIsHandled(t *testing.T) {
	w := &fakeWriter{}
	s := analytics.New("bq", w, true)
	var written int
	for _, fixture := range event.Fixtures() {
		err := s.Process(context.Background(), fixtureEnvelope(t, fixture))
		if errors.Is(err, sink.ErrSkipped) {
			continue
		}
		require.NoError(t, err, fixture)
		written++
	}
	assert.Equal(t, len(event.Fixtures())-1, written, "everything but ping is written")
}

// compile-time check that the analytics sink satisfies the class contract
var _ sink.Sink = (*analytics.Sink)(nil)

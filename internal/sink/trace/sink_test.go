package trace_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/trace"
)

// fakeExporter records every export and answers with a programmable error.
type fakeExporter struct {
	mu       sync.Mutex
	calls    [][]otlp.Span
	err      error
	closeErr error
	closed   int
}

func (f *fakeExporter) ExportSpans(_ context.Context, spans []otlp.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, spans)
	return f.err
}

func (f *fakeExporter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func (f *fakeExporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestSink_NameAndClass(t *testing.T) {
	s := trace.New("tempo", &fakeExporter{})
	assert.Equal(t, "tempo", s.Name())
	assert.Equal(t, sink.ClassTrace, s.Class())
}

func TestNew_NilExporterPanics(t *testing.T) {
	assert.PanicsWithValue(t, "trace: New(tempo) with nil exporter", func() { trace.New("tempo", nil) })
}

func TestSink_ProcessExportsProjection(t *testing.T) {
	exp := &fakeExporter{}
	s := trace.New("tempo", exp)

	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.completed")))
	require.Equal(t, 1, exp.count())
	want, err := trace.Project(fixtureExecution(t, "workflow_job.completed"))
	require.NoError(t, err)
	assert.Equal(t, want, exp.calls[0])
}

func TestSink_ProcessSkipsWithoutExporting(t *testing.T) {
	exp := &fakeExporter{}
	s := trace.New("tempo", exp)
	for _, fixture := range []string{"workflow_run.requested", "workflow_job.queued", "ping"} {
		err := s.Process(context.Background(), fixtureEnvelope(t, fixture))
		require.ErrorIs(t, err, sink.ErrSkipped, fixture)
	}
	assert.Equal(t, 0, exp.count(), "skipped events never reach the exporter")
}

func TestSink_ProcessMalformedIsPermanent(t *testing.T) {
	exp := &fakeExporter{}
	s := trace.New("tempo", exp)
	cases := map[string]string{
		"invalid json":      `{"workflow_run": `,
		"missing entity":    `{"action":"completed","repository":{"id":1}}`,
		"missing identity":  `{"workflow_run":{"name":"CI"},"repository":{"id":1}}`,
		"missing repo":      `{"workflow_run":{"id":1,"run_attempt":1}}`,
		"wrong field types": `{"workflow_run":{"id":"x"}}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			err := s.Process(context.Background(), rawEnvelope("workflow_run", payload))
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), "%v", err)
			assert.ErrorIs(t, err, model.ErrMalformed)
		})
	}
	assert.Equal(t, 0, exp.count())
}

func TestSink_ProcessPropagatesExporterErrors(t *testing.T) {
	t.Run("retryable stays retryable", func(t *testing.T) {
		exp := &fakeExporter{err: errors.New("connection refused")}
		s := trace.New("tempo", exp)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		require.ErrorIs(t, err, exp.err)
		assert.False(t, sink.IsPermanent(err))
		assert.Contains(t, err.Error(), "guid-workflow_run.completed")
	})
	t.Run("permanent stays permanent", func(t *testing.T) {
		exp := &fakeExporter{err: sink.Permanentf("unauthenticated")}
		s := trace.New("tempo", exp)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
	})
}

func TestSink_ProcessPassesContext(t *testing.T) {
	var seen context.Context
	exp := &ctxExporter{fn: func(ctx context.Context) { seen = ctx }}
	s := trace.New("tempo", exp)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	require.NoError(t, s.Process(ctx, fixtureEnvelope(t, "workflow_run.completed")))
	assert.Equal(t, "v", seen.Value(key{}))
}

type ctxExporter struct {
	fn func(ctx context.Context)
}

func (c *ctxExporter) ExportSpans(ctx context.Context, _ []otlp.Span) error {
	c.fn(ctx)
	return nil
}

func (c *ctxExporter) Close() error { return nil }

func TestSink_CloseClosesExporter(t *testing.T) {
	exp := &fakeExporter{closeErr: errors.New("boom")}
	s := trace.New("tempo", exp)
	require.EqualError(t, s.Close(), "boom")
	assert.Equal(t, 1, exp.closed)
}

// compile-time check that the trace sink satisfies the class contract
var _ sink.Sink = (*trace.Sink)(nil)

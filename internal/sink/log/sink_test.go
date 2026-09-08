package log_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/log"
)

// fakeWriter records every record and answers with a programmable error.
type fakeWriter struct {
	mu       sync.Mutex
	records  []otlp.LogRecord
	err      error
	closeErr error
	closed   int
	lastCtx  context.Context
}

func (f *fakeWriter) Write(ctx context.Context, rec otlp.LogRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	f.lastCtx = ctx
	return f.err
}

func (f *fakeWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func (f *fakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func TestSink_NameAndClass(t *testing.T) {
	s := log.New("loki", &fakeWriter{})
	assert.Equal(t, "loki", s.Name())
	assert.Equal(t, sink.ClassLog, s.Class())
}

func TestNew_NilWriterPanics(t *testing.T) {
	assert.PanicsWithValue(t, "log: New(loki) with nil writer", func() { log.New("loki", nil) })
}

func TestSink_ProcessWritesEveryFixture(t *testing.T) {
	w := &fakeWriter{}
	s := log.New("loki", w)
	fixtures := event.Fixtures()
	for _, fixture := range fixtures {
		require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, fixture)), fixture)
	}
	require.Equal(t, len(fixtures), w.count(), "nothing is skipped by the log class")
	for i, fixture := range fixtures {
		assert.Equal(t, log.Project(fixtureExecution(t, fixture)), w.records[i], fixture)
	}
}

func TestSink_ProcessMalformedIsPermanent(t *testing.T) {
	w := &fakeWriter{}
	s := log.New("loki", w)
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
	assert.Equal(t, 0, w.count())
}

func TestSink_ProcessPropagatesWriterErrors(t *testing.T) {
	t.Run("retryable stays retryable", func(t *testing.T) {
		w := &fakeWriter{err: errors.New("broken pipe")}
		s := log.New("loki", w)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		require.ErrorIs(t, err, w.err)
		assert.False(t, sink.IsPermanent(err))
		require.NotErrorIs(t, err, sink.ErrSkipped)
		assert.Contains(t, err.Error(), "workflow_run")
		assert.Contains(t, err.Error(), "guid-workflow_run.completed")
	})
	t.Run("permanent stays permanent", func(t *testing.T) {
		w := &fakeWriter{err: sink.Permanentf("unauthenticated")}
		s := log.New("loki", w)
		err := s.Process(context.Background(), fixtureEnvelope(t, "ping"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
	})
}

func TestSink_ProcessPassesContext(t *testing.T) {
	w := &fakeWriter{}
	s := log.New("loki", w)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	require.NoError(t, s.Process(ctx, fixtureEnvelope(t, "workflow_run.completed")))
	assert.Equal(t, "v", w.lastCtx.Value(key{}))
}

func TestSink_CloseClosesWriter(t *testing.T) {
	w := &fakeWriter{closeErr: errors.New("boom")}
	s := log.New("loki", w)
	require.EqualError(t, s.Close(), "boom")
	assert.Equal(t, 1, w.closed)
}

// compile-time check that the log sink satisfies the class contract
var _ sink.Sink = (*log.Sink)(nil)

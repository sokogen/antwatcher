package archive_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/archive"
)

type fakeWriter struct {
	mu       sync.Mutex
	envs     []event.Envelope
	err      error
	closeErr error
	closed   int
	lastCtx  context.Context
}

func (f *fakeWriter) Append(ctx context.Context, env event.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envs = append(f.envs, env)
	f.lastCtx = ctx
	return f.err
}

func (f *fakeWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return f.closeErr
}

func TestSink_NameAndClass(t *testing.T) {
	s := archive.New("raw", &fakeWriter{})
	assert.Equal(t, "raw", s.Name())
	assert.Equal(t, sink.ClassArchive, s.Class())
}

func TestNew_NilWriterPanics(t *testing.T) {
	assert.PanicsWithValue(t, "archive: New(raw) with nil writer", func() { archive.New("raw", nil) })
}

func TestSink_ProcessPassesEnvelopeVerbatim(t *testing.T) {
	w := &fakeWriter{}
	s := archive.New("raw", w)
	fixtures := event.Fixtures()
	for _, fixture := range fixtures {
		require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, fixture)), fixture)
	}
	require.Len(t, w.envs, len(fixtures), "nothing is skipped by the archive class")
	for i, fixture := range fixtures {
		want := fixtureEnvelope(t, fixture)
		assert.Equal(t, want, w.envs[i], fixture)
		assert.Equal(t, string(want.Payload), string(w.envs[i].Payload), "payload bytes untouched")
	}
}

func TestSink_ProcessDoesNotParsePayload(t *testing.T) {
	w := &fakeWriter{}
	s := archive.New("raw", w)
	env := fixtureEnvelope(t, "workflow_run.completed")
	env.Payload = []byte(`{"workflow_run": {"id": "not-a-number"}}`)
	require.NoError(t, s.Process(context.Background(), env), "the archive keeps what the model cannot read")
	require.Len(t, w.envs, 1)
	assert.Equal(t, env, w.envs[0])
}

func TestSink_ProcessPropagatesWriterErrors(t *testing.T) {
	t.Run("retryable stays retryable", func(t *testing.T) {
		w := &fakeWriter{err: errors.New("disk full")}
		s := archive.New("raw", w)
		err := s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		require.ErrorIs(t, err, w.err)
		assert.False(t, sink.IsPermanent(err))
		require.NotErrorIs(t, err, sink.ErrSkipped)
		assert.Contains(t, err.Error(), "workflow_run")
		assert.Contains(t, err.Error(), "guid-workflow_run.completed")
	})
	t.Run("permanent stays permanent", func(t *testing.T) {
		w := &fakeWriter{err: sink.Permanentf("bad record")}
		s := archive.New("raw", w)
		err := s.Process(context.Background(), fixtureEnvelope(t, "ping"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
	})
}

func TestSink_ProcessPassesContext(t *testing.T) {
	w := &fakeWriter{}
	s := archive.New("raw", w)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	require.NoError(t, s.Process(ctx, fixtureEnvelope(t, "ping")))
	assert.Equal(t, "v", w.lastCtx.Value(key{}))
}

func TestSink_CloseClosesWriter(t *testing.T) {
	w := &fakeWriter{closeErr: errors.New("close failed")}
	s := archive.New("raw", w)
	require.ErrorIs(t, s.Close(), w.closeErr)
	assert.Equal(t, 1, w.closed)
}

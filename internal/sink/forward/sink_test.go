package forward_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/forward"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

// capturePublisher records every published message and answers with err.
type capturePublisher struct {
	mu       sync.Mutex
	msgs     []*message.Message
	err      error
	closeErr error
	closed   int
	lastCtx  context.Context
}

func (p *capturePublisher) Publish(ctx context.Context, msg *message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	p.lastCtx = ctx
	return p.err
}

func (p *capturePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed++
	return p.closeErr
}

func (p *capturePublisher) published() []*message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*message.Message(nil), p.msgs...)
}

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, "workflow_job")
	hdr.Set(event.HeaderHookID, "570000001")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

func TestSink_NameClassTopicMaxHops(t *testing.T) {
	s := forward.New("downstream", &capturePublisher{}, "github.events", 5)
	assert.Equal(t, "downstream", s.Name())
	assert.Equal(t, sink.ClassForward, s.Class())
	assert.Equal(t, "github.events", s.Topic())
	assert.Equal(t, 5, s.MaxHops())
	assert.Equal(t, forward.DefaultMaxHops, forward.New("d", &capturePublisher{}, "t", 0).MaxHops(), "0 means the default")
	assert.Equal(t, 3, forward.DefaultMaxHops)
}

func TestNew_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "forward: New(d) with nil publisher", func() { forward.New("d", nil, "t", 1) })
	assert.PanicsWithValue(t, "forward: New(d) with empty topic", func() { forward.New("d", &capturePublisher{}, "", 1) })
	assert.PanicsWithValue(t, "forward: New(d) with negative max hops -1", func() { forward.New("d", &capturePublisher{}, "t", -1) })
}

func TestForwardUUID(t *testing.T) {
	want := sha256.Sum256([]byte("antwatcher:forward:guid:downstream:github.events"))
	got := forward.ForwardUUID("guid", "downstream", "github.events")
	assert.Equal(t, hex.EncodeToString(want[:]), got, "documented formula")
	assert.Equal(t, got, forward.ForwardUUID("guid", "downstream", "github.events"), "deterministic")
	assert.NotEqual(t, "guid", got, "never the delivery GUID")
	assert.NotEqual(t, got, forward.ForwardUUID("guid", "other", "github.events"), "sink name is part of the identity")
	assert.NotEqual(t, got, forward.ForwardUUID("guid", "downstream", "other.topic"), "topic is part of the identity")
	assert.NotEqual(t, got, forward.ForwardUUID("guid2", "downstream", "github.events"))
}

func TestSink_ProcessPublishesCopy(t *testing.T) {
	p := &capturePublisher{}
	s := forward.New("downstream", p, "github.events", 3)
	env := fixtureEnvelope(t, "workflow_job.completed")

	require.NoError(t, s.Process(context.Background(), env))
	msgs := p.published()
	require.Len(t, msgs, 1)
	msg := msgs[0]

	assert.Equal(t, forward.ForwardUUID(env.DeliveryGUID, "downstream", "github.events"), msg.UUID)
	assert.NotEqual(t, env.DeliveryGUID, msg.UUID, "the copy never travels under the original UUID")
	assert.Equal(t, string(env.Payload), string(msg.Payload), "payload is raw and byte-identical")

	original := event.ToMessage(env).Metadata
	for k, v := range original {
		assert.Equal(t, v, msg.Metadata[k], "metadata %s copied", k)
	}
	assert.Equal(t, env.DeliveryGUID, msg.Metadata[event.MetaDeliveryGUID], "the GUID stays explicit in metadata")
	assert.Equal(t, "downstream", msg.Metadata[event.MetaForwardedBy])
	assert.Equal(t, "1", msg.Metadata[event.MetaForwardHops])
	assert.Len(t, msg.Metadata, len(original)+2, "only the two markers are added")

	// The copy decodes into the same envelope plus the markers, so a
	// downstream antwatcher reads it as any other event.
	out, err := event.FromMessage(msg)
	require.NoError(t, err)
	want := env
	want.ForwardedBy, want.ForwardHops = "downstream", 1
	assert.Equal(t, want, out)
	built := s.Message(env)
	assert.Equal(t, msg.UUID, built.UUID, "Message builds the same copy")
	assert.Equal(t, msg.Metadata, built.Metadata)
	assert.Equal(t, msg.Payload, built.Payload)
}

func TestSink_ProcessEveryFixtureDeterministic(t *testing.T) {
	p := &capturePublisher{}
	s := forward.New("downstream", p, "github.events", 3)
	fixtures := event.Fixtures()
	for _, fixture := range fixtures {
		env := fixtureEnvelope(t, fixture)
		require.NoError(t, s.Process(context.Background(), env), fixture)
		require.NoError(t, s.Process(context.Background(), env), fixture)
	}
	msgs := p.published()
	require.Len(t, msgs, 2*len(fixtures), "nothing is skipped by the forward class")
	seen := map[string]bool{}
	for i := 0; i < len(msgs); i += 2 {
		assert.Equal(t, msgs[i].UUID, msgs[i+1].UUID, "a redelivery produces the same copy")
		assert.False(t, seen[msgs[i].UUID], "distinct deliveries produce distinct copies")
		seen[msgs[i].UUID] = true
	}
}

func TestSink_ProcessIncrementsHops(t *testing.T) {
	p := &capturePublisher{}
	s := forward.New("second", p, "t", 3)
	env := fixtureEnvelope(t, "ping")
	env.ForwardedBy, env.ForwardHops = "first", 1

	require.NoError(t, s.Process(context.Background(), env))
	msg := p.published()[0]
	assert.Equal(t, "second", msg.Metadata[event.MetaForwardedBy], "the last forwarder wins")
	assert.Equal(t, "2", msg.Metadata[event.MetaForwardHops])
	assert.Equal(t, forward.ForwardUUID(env.DeliveryGUID, "second", "t"), msg.UUID, "hops do not change the copy identity")
}

func TestSink_ProcessHopLimitIsPermanent(t *testing.T) {
	p := &capturePublisher{}
	s := forward.New("downstream", p, "github.events", 3)
	env := fixtureEnvelope(t, "ping")

	env.ForwardedBy, env.ForwardHops = "upstream", 2
	require.NoError(t, s.Process(context.Background(), env), "below the limit forwards")

	env.ForwardHops = 3
	err := s.Process(context.Background(), env)
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err), "a loop cannot be fixed by retrying")
	require.NotErrorIs(t, err, sink.ErrSkipped)
	assert.Contains(t, err.Error(), "loop detected")
	assert.Contains(t, err.Error(), `"upstream"`)
	assert.Contains(t, err.Error(), "max_hops 3")
	assert.Contains(t, err.Error(), env.DeliveryGUID)

	env.ForwardHops = 10
	assert.True(t, sink.IsPermanent(s.Process(context.Background(), env)), "far past the limit as well")
	assert.Len(t, p.published(), 1, "nothing was published for the looping copies")

	one := forward.New("d", p, "t", 1)
	require.NoError(t, one.Process(context.Background(), fixtureEnvelope(t, "ping")), "max_hops 1 forwards originals")
	env.ForwardHops = 1
	assert.True(t, sink.IsPermanent(one.Process(context.Background(), env)), "max_hops 1 refuses any copy")
}

func TestSink_ProcessPropagatesPublisherErrors(t *testing.T) {
	t.Run("retryable stays retryable", func(t *testing.T) {
		p := &capturePublisher{err: errors.New("connection refused")}
		s := forward.New("downstream", p, "github.events", 3)
		env := fixtureEnvelope(t, "workflow_run.completed")
		err := s.Process(context.Background(), env)
		require.Error(t, err)
		require.ErrorIs(t, err, p.err)
		assert.False(t, sink.IsPermanent(err))
		require.NotErrorIs(t, err, sink.ErrSkipped)
		assert.Contains(t, err.Error(), "workflow_run")
		assert.Contains(t, err.Error(), env.DeliveryGUID)
		assert.Contains(t, err.Error(), `"github.events"`)
	})
	t.Run("permanent stays permanent", func(t *testing.T) {
		p := &capturePublisher{err: sink.Permanentf("reserved metadata key")}
		s := forward.New("downstream", p, "github.events", 3)
		err := s.Process(context.Background(), fixtureEnvelope(t, "ping"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
	})
	t.Run("context error", func(t *testing.T) {
		p := &capturePublisher{err: context.DeadlineExceeded}
		s := forward.New("downstream", p, "github.events", 3)
		require.ErrorIs(t, s.Process(context.Background(), fixtureEnvelope(t, "ping")), context.DeadlineExceeded)
	})
}

func TestSink_ProcessPassesContext(t *testing.T) {
	p := &capturePublisher{}
	s := forward.New("downstream", p, "github.events", 3)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	require.NoError(t, s.Process(ctx, fixtureEnvelope(t, "ping")))
	assert.Equal(t, "v", p.lastCtx.Value(key{}))
}

func TestSink_CloseClosesPublisher(t *testing.T) {
	p := &capturePublisher{closeErr: errors.New("close failed")}
	s := forward.New("downstream", p, "github.events", 3)
	require.ErrorIs(t, s.Close(), p.closeErr)
	assert.Equal(t, 1, p.closed)
}

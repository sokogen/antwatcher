package sink_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/bus/gochannel"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
	"github.com/sokogen/antwatcher/internal/sink"
)

const topic = "antwatcher.events"

var routerCfg = config.Router{CloseTimeout: 5 * time.Second, ProcessTimeout: 5 * time.Second}

// captureSink records every envelope it receives and answers with whatever
// process is set to (nil means success).
type captureSink struct {
	name   string
	class  sink.Class
	closed atomic.Bool

	mu        sync.Mutex
	process   func(ctx context.Context, env event.Envelope) error
	envelopes []event.Envelope
	calls     int
}

func newCaptureSink(name string, class sink.Class) *captureSink {
	return &captureSink{name: name, class: class}
}

func (s *captureSink) Name() string      { return s.name }
func (s *captureSink) Class() sink.Class { return s.class }
func (s *captureSink) Close() error      { s.closed.Store(true); return nil }

func (s *captureSink) Process(ctx context.Context, env event.Envelope) error {
	s.mu.Lock()
	s.calls++
	s.envelopes = append(s.envelopes, env)
	fn := s.process
	s.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, env)
}

func (s *captureSink) setProcess(fn func(ctx context.Context, env event.Envelope) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.process = fn
}

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *captureSink) received() []event.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]event.Envelope, len(s.envelopes))
	copy(out, s.envelopes)
	return out
}

// guids returns the distinct delivery GUIDs received, in first-seen order.
func (s *captureSink) guids() []string {
	seen := map[string]bool{}
	var out []string
	for _, env := range s.received() {
		if !seen[env.DeliveryGUID] {
			seen[env.DeliveryGUID] = true
			out = append(out, env.DeliveryGUID)
		}
	}
	return out
}

func envelope(t *testing.T, guid string) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, guid)
	hdr.Set(event.HeaderEvent, "workflow_job")
	hdr.Set(event.HeaderHookID, "570000001")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, "workflow_job.completed"), time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	return env
}

// capsBus overrides the declared capabilities of a bus for policy tests.
type capsBus struct {
	bus.Bus
	caps bus.Capabilities
}

func (c capsBus) Capabilities() bus.Capabilities { return c.caps }

// failingSubscribeBus fails Subscribe after n successful calls.
type failingSubscribeBus struct {
	bus.Bus
	allow int32
}

func (f *failingSubscribeBus) Subscribe(ctx context.Context, consumer string, opts bus.SubscribeOptions) (message.Subscriber, error) {
	if atomic.AddInt32(&f.allow, -1) < 0 {
		return nil, errors.New("broker refused")
	}
	return f.Bus.Subscribe(ctx, consumer, opts)
}

// syncBuffer is a bytes.Buffer safe for the router's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness runs a router over a gochannel bus for one test.
type harness struct {
	t      *testing.T
	bus    bus.Bus
	m      *metrics.Metrics
	router *sink.Router
	cancel context.CancelFunc
	done   chan error
	logs   *syncBuffer
	once   sync.Once
}

func start(t *testing.T, b bus.Bus, cfg config.Router, mode bus.Mode, instances ...sink.Instance) *harness {
	t.Helper()
	h := &harness{t: t, bus: b, m: metrics.New("v", "c"), logs: &syncBuffer{}}
	logger := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	r, err := sink.BuildRouter(context.Background(), b, cfg, mode, instances, h.m, logger)
	require.NoError(t, err)
	h.router = r
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	go func() { h.done <- r.Run(ctx) }()
	select {
	case <-r.Running():
	case <-time.After(10 * time.Second):
		t.Fatal("router did not start")
	}
	t.Cleanup(h.stop)
	return h
}

// stop closes the router and waits for Run to return. Idempotent.
func (h *harness) stop() {
	h.t.Helper()
	h.once.Do(func() {
		require.NoError(h.t, h.router.Close())
		h.cancel()
		select {
		case err := <-h.done:
			require.NoError(h.t, err)
		case <-time.After(10 * time.Second):
			h.t.Fatal("router did not stop")
		}
	})
}

func (h *harness) publish(env event.Envelope) {
	h.t.Helper()
	require.NoError(h.t, h.bus.Publish(context.Background(), event.ToMessage(env)))
}

func (h *harness) events(name string, class sink.Class, result string) float64 {
	return testutil.ToFloat64(h.m.SinkEventsTotal.WithLabelValues(name, class.String(), result))
}

func (h *harness) stalledGauges(name string) (stalled, count float64) {
	return testutil.ToFloat64(h.m.SinkStalled.WithLabelValues(name)), testutil.ToFloat64(h.m.SinkStalledMessages.WithLabelValues(name))
}

func (h *harness) stalledAttempts(name string) int {
	for _, st := range h.router.Status() {
		if st.Name == name && len(st.Stalled) == 1 {
			return st.Stalled[0].Attempts
		}
	}
	return 0
}

func (h *harness) status(name string) sink.SinkStatus {
	h.t.Helper()
	for _, st := range h.router.Status() {
		if st.Name == name {
			return st
		}
	}
	h.t.Fatalf("no status for sink %q", name)
	return sink.SinkStatus{}
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, 5*time.Millisecond, msg)
}

func TestRouter_DeliversAndAcks(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest, Driver: "capture"})

	before := time.Now()
	env := envelope(t, "guid-1")
	h.publish(env)
	eventually(t, func() bool { return s.count() == 1 }, "sink receives the event")
	assert.Equal(t, env, s.received()[0], "envelope arrives intact")

	assert.InDelta(t, 1, h.events("a", sink.ClassLog, metrics.SinkOK), 0)
	assert.InDelta(t, 0, h.events("a", sink.ClassLog, metrics.SinkError), 0)
	cnt, err := testutil.GatherAndCount(h.m.Registry, "antwatcher_sink_process_seconds")
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	last := testutil.ToFloat64(h.m.SinkLastSuccessTimestamp.WithLabelValues("a"))
	assert.GreaterOrEqual(t, last, float64(before.Unix()))

	st := h.status("a")
	assert.Equal(t, "a", st.Name)
	assert.Equal(t, sink.ClassLog, st.Class)
	assert.Equal(t, "capture", st.Driver)
	assert.Equal(t, "sink-a", st.Consumer)
	assert.Equal(t, bus.Earliest, st.RequestedStartFrom)
	assert.Equal(t, bus.Earliest, st.EffectiveStartFrom)
	require.NotNil(t, st.LastSuccess)
	assert.False(t, st.LastSuccess.Before(before.Truncate(time.Second)))
	assert.Empty(t, st.Stalled)
	assert.Equal(t, []string{"sink-a"}, h.router.Consumers())

	// Acked: the message is not redelivered.
	h.publish(envelope(t, "guid-2"))
	eventually(t, func() bool { return s.count() == 2 }, "second event")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, s.count(), "acked messages are not redelivered")
	assert.Contains(t, h.logs.String(), "sink bound")
}

func TestRouter_RetryableErrorNacksAndRedelivers(t *testing.T) {
	s := newCaptureSink("a", sink.ClassTrace)
	var failures atomic.Int32
	failures.Store(2)
	s.setProcess(func(context.Context, event.Envelope) error {
		if failures.Add(-1) >= 0 {
			return errors.New("tempo unavailable")
		}
		return nil
	})
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

	h.publish(envelope(t, "guid-1"))
	eventually(t, func() bool { return h.events("a", sink.ClassTrace, metrics.SinkOK) == 1 }, "succeeds after redeliveries")
	assert.Equal(t, 3, s.count(), "two failed attempts plus the success")
	assert.InDelta(t, 2, h.events("a", sink.ClassTrace, metrics.SinkError), 0)
	assert.InDelta(t, 0, h.events("a", sink.ClassTrace, metrics.SinkPermanent), 0)
	stalled, count := h.stalledGauges("a")
	assert.InDelta(t, 0, stalled, 0, "retryable errors never stall")
	assert.InDelta(t, 0, count, 0)
	assert.Contains(t, h.logs.String(), "event failed; the broker redelivers")
	assert.Contains(t, h.logs.String(), "tempo unavailable")
}

func TestRouter_PermanentErrorStallsAndClearsOnSuccess(t *testing.T) {
	s := newCaptureSink("a", sink.ClassAnalytics)
	var fail atomic.Bool
	fail.Store(true)
	s.setProcess(func(context.Context, event.Envelope) error {
		if fail.Load() {
			time.Sleep(time.Millisecond) // throttle the in-memory bus, which redelivers at once
			return sink.Permanent(errors.New("schema mismatch"))
		}
		return nil
	})
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

	started := time.Now()
	h.publish(envelope(t, "guid-1"))
	eventually(t, func() bool { return h.stalledAttempts("a") >= 5 }, "re-attempted at the bus's pace")
	assert.Less(t, time.Since(started), 3*time.Second, "the handler adds no delay of its own before the Nack")

	stalled, count := h.stalledGauges("a")
	assert.InDelta(t, 1, stalled, 0)
	assert.InDelta(t, 1, count, 0, "the same message counts once however often it fails")
	st := h.status("a")
	require.Len(t, st.Stalled, 1)
	assert.Equal(t, "guid-1", st.Stalled[0].UUID)
	assert.GreaterOrEqual(t, st.Stalled[0].Attempts, 5)
	assert.Contains(t, st.Stalled[0].Reason, "schema mismatch")
	assert.False(t, st.Stalled[0].FirstSeen.IsZero())
	assert.False(t, st.Stalled[0].LastSeen.Before(st.Stalled[0].FirstSeen))
	assert.Nil(t, st.LastSuccess)
	assert.GreaterOrEqual(t, h.events("a", sink.ClassAnalytics, metrics.SinkPermanent), 5.0)
	assert.InDelta(t, 0, h.events("a", sink.ClassAnalytics, metrics.SinkError), 0)
	assert.Contains(t, h.logs.String(), "message stalled on a permanent error")

	// Fixing the cause clears the entry on the next redelivery, no restart.
	fail.Store(false)
	eventually(t, func() bool { return h.events("a", sink.ClassAnalytics, metrics.SinkOK) == 1 }, "succeeds after the fix")
	eventually(t, func() bool { s, c := h.stalledGauges("a"); return s == 0 && c == 0 }, "gauges cleared")
	assert.Empty(t, h.status("a").Stalled)
	assert.Contains(t, h.logs.String(), "stalled message processed")
	time.Sleep(20 * time.Millisecond)
	assert.InDelta(t, 1, h.events("a", sink.ClassAnalytics, metrics.SinkOK), 0, "acked once fixed")
}

func TestRouter_UndecodableMessageStallsAndIsNeverAcked(t *testing.T) {
	s := newCaptureSink("a", sink.ClassArchive)
	b := gochannel.New(topic, nil)
	h := start(t, b, routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

	// A message from a newer schema: metadata is present but unsupported.
	msg := event.ToMessage(envelope(t, "future"))
	msg.Metadata.Set(event.MetaSchemaVersion, "99")
	require.NoError(t, b.Publish(context.Background(), msg))

	eventually(t, func() bool {
		st := h.status("a")
		return len(st.Stalled) == 1 && st.Stalled[0].Attempts >= 3
	}, "re-attempted, never acked")
	assert.Equal(t, 0, s.count(), "the sink never sees an undecodable message")
	st := h.status("a")
	assert.Equal(t, "future", st.Stalled[0].UUID)
	assert.Contains(t, st.Stalled[0].Reason, "decode message")
	assert.Contains(t, st.Stalled[0].Reason, "schema_version 99 is not supported")
	stalled, count := h.stalledGauges("a")
	assert.InDelta(t, 1, stalled, 0)
	assert.InDelta(t, 1, count, 0)
	assert.GreaterOrEqual(t, h.events("a", sink.ClassArchive, metrics.SinkPermanent), 3.0)

	// Stopping the router ends the re-attempts; the message was never acked,
	// so a durable broker would hand it to the next start.
	h.stop()
	assert.Equal(t, 0, s.count())
	assert.Len(t, h.status("a").Stalled, 1, "the entry survives until the same message succeeds")
}

func TestRouter_OtherSinksContinueWhileOneIsStalled(t *testing.T) {
	good := newCaptureSink("good", sink.ClassLog)
	bad := newCaptureSink("bad", sink.ClassTrace)
	bad.setProcess(func(context.Context, event.Envelope) error {
		time.Sleep(time.Millisecond)
		return sink.Permanent(errors.New("invalid argument"))
	})
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade,
		sink.Instance{Sink: good, StartFrom: bus.Earliest}, sink.Instance{Sink: bad, StartFrom: bus.Earliest})

	for _, guid := range []string{"g1", "g2", "g3"} {
		h.publish(envelope(t, guid))
	}
	eventually(t, func() bool { return len(good.guids()) == 3 }, "the healthy sink receives everything")
	assert.ElementsMatch(t, []string{"g1", "g2", "g3"}, good.guids())
	eventually(t, func() bool { _, n := h.stalledGauges("bad"); return n == 3 }, "every message is stalled on the failing sink")
	eventually(t, func() bool { return bad.count() >= 6 }, "the stalled sink keeps being re-attempted")
	assert.ElementsMatch(t, []string{"g1", "g2", "g3"}, bad.guids())
	assert.InDelta(t, 3, h.events("good", sink.ClassLog, metrics.SinkOK), 0)
	assert.InDelta(t, 0, h.events("good", sink.ClassLog, metrics.SinkPermanent), 0)
	stalled, _ := h.stalledGauges("good")
	assert.InDelta(t, 0, stalled, 0, "the healthy sink is not affected")
	stalled, _ = h.stalledGauges("bad")
	assert.InDelta(t, 1, stalled, 0)
	assert.Len(t, h.status("bad").Stalled, 3)
	assert.Empty(t, h.status("good").Stalled)
	assert.Equal(t, []string{"sink-good", "sink-bad"}, h.router.Consumers())
}

func TestRouter_SkippedIsCountedAndAcked(t *testing.T) {
	s := newCaptureSink("a", sink.ClassTrace)
	s.setProcess(func(context.Context, event.Envelope) error { return sink.ErrSkipped })
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

	h.publish(envelope(t, "guid-1"))
	eventually(t, func() bool { return h.events("a", sink.ClassTrace, metrics.SinkSkipped) == 1 }, "skipped counted")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, s.count(), "acked: not redelivered")
	assert.InDelta(t, 0, h.events("a", sink.ClassTrace, metrics.SinkOK), 0)
	assert.NotNil(t, h.status("a").LastSuccess, "a skip is a successful decision")
	stalled, _ := h.stalledGauges("a")
	assert.InDelta(t, 0, stalled, 0)
}

func TestRouter_ProcessTimeoutCancelsAndNacks(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	var hang atomic.Bool
	hang.Store(true)
	var firstElapsed atomic.Int64
	s.setProcess(func(ctx context.Context, _ event.Envelope) error {
		if !hang.Swap(false) {
			return nil
		}
		start := time.Now()
		<-ctx.Done()
		firstElapsed.Store(int64(time.Since(start)))
		return ctx.Err()
	})
	cfg := config.Router{CloseTimeout: 5 * time.Second, ProcessTimeout: 50 * time.Millisecond}
	h := start(t, gochannel.New(topic, nil), cfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

	h.publish(envelope(t, "guid-1"))
	eventually(t, func() bool { return h.events("a", sink.ClassLog, metrics.SinkOK) == 1 }, "redelivered after the timeout and processed")
	assert.Equal(t, 2, s.count())
	assert.InDelta(t, 1, h.events("a", sink.ClassLog, metrics.SinkError), 0, "a timeout is a retryable error")
	elapsed := time.Duration(firstElapsed.Load())
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
	assert.Contains(t, h.logs.String(), "process_timeout=50ms")
}

func TestRouter_EarliestOnBusWithoutReplay(t *testing.T) {
	noReplay := func() capsBus {
		return capsBus{Bus: gochannel.New(topic, nil), caps: bus.Capabilities{FanOut: true}}
	}

	t.Run("fail mode refuses", func(t *testing.T) {
		b := noReplay()
		s := newCaptureSink("a", sink.ClassLog)
		_, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeFail, []sink.Instance{{Sink: s, StartFrom: bus.Earliest}}, metrics.New("v", "c"), nil)
		require.ErrorIs(t, err, bus.ErrMissingCapability)
		assert.Contains(t, err.Error(), `sink "a"`)
		assert.Contains(t, err.Error(), "HistoricalReplay")
	})

	t.Run("degrade mode starts from now with a warning", func(t *testing.T) {
		b := noReplay()
		require.NoError(t, b.Publish(context.Background(), event.ToMessage(envelope(t, "before"))))
		s := newCaptureSink("a", sink.ClassLog)
		h := start(t, b, routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})

		st := h.status("a")
		assert.Equal(t, bus.Earliest, st.RequestedStartFrom)
		assert.Equal(t, bus.Now, st.EffectiveStartFrom)
		var caps []string
		for _, w := range st.Warnings {
			caps = append(caps, w.Capability)
		}
		assert.Contains(t, caps, "HistoricalReplay")
		assert.Contains(t, caps, "DurableConsumers")
		assert.Contains(t, caps, "Deduplicates")
		assert.Contains(t, h.logs.String(), "consumer policy")
		assert.Contains(t, h.logs.String(), "downgraded to now")

		h.publish(envelope(t, "after"))
		eventually(t, func() bool { return s.count() == 1 }, "post-subscription message delivered")
		assert.Equal(t, []string{"after"}, s.guids(), "history was not replayed")
	})

	t.Run("no fan-out always fails", func(t *testing.T) {
		b := capsBus{Bus: gochannel.New(topic, nil), caps: bus.Capabilities{HistoricalReplay: true}}
		_, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: newCaptureSink("a", sink.ClassLog), StartFrom: bus.Now}}, metrics.New("v", "c"), nil)
		require.ErrorIs(t, err, bus.ErrMissingCapability)
		assert.Contains(t, err.Error(), "FanOut")
	})
}

func TestRouter_BuildErrors(t *testing.T) {
	b := gochannel.New(topic, nil)
	m := metrics.New("v", "c")
	inst := sink.Instance{Sink: newCaptureSink("a", sink.ClassLog), StartFrom: bus.Now}

	_, err := sink.BuildRouter(context.Background(), nil, routerCfg, bus.ModeFail, nil, m, nil)
	require.ErrorContains(t, err, "needs a bus")
	_, err = sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeFail, nil, nil, nil)
	require.ErrorContains(t, err, "needs metrics")
	_, err = sink.BuildRouter(context.Background(), b, config.Router{CloseTimeout: time.Second}, bus.ModeFail, nil, m, nil)
	require.ErrorContains(t, err, "process_timeout")
	_, err = sink.BuildRouter(context.Background(), b, config.Router{ProcessTimeout: time.Second}, bus.ModeFail, nil, m, nil)
	require.ErrorContains(t, err, "close_timeout")
	_, err = sink.BuildRouter(context.Background(), b, routerCfg, bus.Mode(0), []sink.Instance{inst}, m, nil)
	require.ErrorContains(t, err, "invalid policy mode")
	_, err = sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeFail, []sink.Instance{{Sink: inst.Sink}}, m, nil)
	require.ErrorContains(t, err, "invalid start position")

	t.Run("subscribe failure closes the consumers created before it", func(t *testing.T) {
		fb := &failingSubscribeBus{Bus: gochannel.New(topic, nil), allow: 1}
		_, err := sink.BuildRouter(context.Background(), fb, routerCfg, bus.ModeDegrade, []sink.Instance{
			{Sink: newCaptureSink("first", sink.ClassLog), StartFrom: bus.Now},
			{Sink: newCaptureSink("second", sink.ClassLog), StartFrom: bus.Now},
		}, m, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `sink "second": create consumer "sink-second": broker refused`)
		require.NoError(t, fb.Close())
	})

	t.Run("closed bus", func(t *testing.T) {
		closed := gochannel.New(topic, nil)
		require.NoError(t, closed.Close())
		_, err := sink.BuildRouter(context.Background(), closed, routerCfg, bus.ModeDegrade, []sink.Instance{inst}, m, nil)
		require.ErrorIs(t, err, bus.ErrClosed)
	})
}

func TestRouter_CleanStopOnContextCancel(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	b := gochannel.New(topic, nil)
	r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: s, StartFrom: bus.Earliest}}, metrics.New("v", "c"), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-r.Running()
	require.NoError(t, b.Publish(ctx, event.ToMessage(envelope(t, "g"))))
	eventually(t, func() bool { return s.count() == 1 }, "delivered")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "cancellation is a clean stop")
	case <-time.After(10 * time.Second):
		t.Fatal("router did not stop on ctx cancel")
	}
	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "idempotent")
	require.NoError(t, b.Close())
}

// TestRouter_CloseDoesNotCancelInFlightProcess pins the shutdown contract: a
// Process call that honors its context still finishes and is acked when the
// router closes underneath it, so a graceful stop never turns a slow but
// healthy destination into a redelivery.
func TestRouter_CloseDoesNotCancelInFlightProcess(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	entered := make(chan struct{})
	s.setProcess(func(ctx context.Context, _ event.Envelope) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
			return nil
		}
	})
	b := gochannel.New(topic, nil)
	m := metrics.New("v", "c")
	r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: s, StartFrom: bus.Earliest}}, m, nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	<-r.Running()
	require.NoError(t, b.Publish(context.Background(), event.ToMessage(envelope(t, "g"))))
	<-entered
	require.NoError(t, r.Close())
	require.NoError(t, <-done)
	assert.InDelta(t, 1, testutil.ToFloat64(m.SinkEventsTotal.WithLabelValues("a", "log", metrics.SinkOK)), 0, "finished and acked")
	assert.InDelta(t, 0, testutil.ToFloat64(m.SinkEventsTotal.WithLabelValues("a", "log", metrics.SinkError)), 0, "not aborted by the close")
	require.NoError(t, b.Close())
}

func TestRouter_CloseWithoutRunReleasesConsumers(t *testing.T) {
	b := gochannel.New(topic, nil)
	r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: newCaptureSink("a", sink.ClassLog), StartFrom: bus.Now}}, metrics.New("v", "c"), nil)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.NoError(t, b.Close())
}

func TestRouter_CloseWaitsForInFlightProcess(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.setProcess(func(context.Context, event.Envelope) error {
		close(entered)
		<-release
		return nil
	})
	b := gochannel.New(topic, nil)
	m := metrics.New("v", "c")
	r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: s, StartFrom: bus.Earliest}}, m, nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	<-r.Running()
	require.NoError(t, b.Publish(context.Background(), event.ToMessage(envelope(t, "g"))))
	<-entered

	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while a Process call was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the handler finished")
	}
	require.NoError(t, <-done)
	assert.InDelta(t, 1, testutil.ToFloat64(m.SinkEventsTotal.WithLabelValues("a", "log", metrics.SinkOK)), 0)
	require.NoError(t, b.Close())
}

func TestRouter_PanicInSinkIsRecoveredAndRedelivered(t *testing.T) {
	s := newCaptureSink("a", sink.ClassLog)
	var once atomic.Bool
	s.setProcess(func(context.Context, event.Envelope) error {
		if once.CompareAndSwap(false, true) {
			panic("sink bug")
		}
		return nil
	})
	h := start(t, gochannel.New(topic, nil), routerCfg, bus.ModeDegrade, sink.Instance{Sink: s, StartFrom: bus.Earliest})
	h.publish(envelope(t, "g"))
	eventually(t, func() bool { return h.events("a", sink.ClassLog, metrics.SinkOK) == 1 }, "redelivered after the panic")
	assert.Equal(t, 2, s.count())
	assert.Contains(t, h.logs.String(), "level=ERROR")
	assert.Contains(t, h.logs.String(), "sink bug")
}

func TestRouter_WatermillMetricsRegisteredOnce(t *testing.T) {
	// Two routers on the same registry (a restart in the same process) must
	// not fail on duplicate Watermill collectors.
	m := metrics.New("v", "c")
	b := gochannel.New(topic, nil)
	for range 2 {
		r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade, []sink.Instance{{Sink: newCaptureSink("a", sink.ClassLog), StartFrom: bus.Now}}, m, nil)
		require.NoError(t, err)
		require.NoError(t, r.Close())
	}
	cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_sink_events_total")
	require.NoError(t, err)
	assert.Equal(t, 4, cnt, "every result series exists as zero from the start")
	require.NoError(t, b.Close())
}

// TestRouter_LastSuccessExistsBeforeTheFirstSuccess pins what the
// AntwatcherSinkNoRecentSuccess alert in docs/operations.md depends on: a sink
// that has never succeeded must still publish the series, at zero. An absent
// series makes `time() - antwatcher_sink_last_success_timestamp_seconds` an
// empty vector, so the alert stays silent for exactly the sink it is meant to
// catch -- one misconfigured at deploy time that never processes anything.
func TestRouter_LastSuccessExistsBeforeTheFirstSuccess(t *testing.T) {
	m := metrics.New("v", "c")
	b := gochannel.New(topic, nil)
	r, err := sink.BuildRouter(context.Background(), b, routerCfg, bus.ModeDegrade,
		[]sink.Instance{{Sink: newCaptureSink("a", sink.ClassLog), StartFrom: bus.Now}}, m, nil)
	require.NoError(t, err)

	cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_sink_last_success_timestamp_seconds")
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "the series must exist before the first success")
	assert.InDelta(t, 0, testutil.ToFloat64(m.SinkLastSuccessTimestamp.WithLabelValues("a")), 0,
		"zero means never succeeded")

	require.NoError(t, r.Close())
	require.NoError(t, b.Close())
}

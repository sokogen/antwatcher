package metrics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/bus"
)

// fakeBus is a bus.Bus that optionally reports lag through a programmable func.
type fakeBus struct {
	reportsLag bool

	mu    sync.Mutex
	lag   map[string]int64
	err   map[string]error
	calls map[string]int
}

func newFakeBus(reportsLag bool) *fakeBus {
	return &fakeBus{reportsLag: reportsLag, lag: map[string]int64{}, err: map[string]error{}, calls: map[string]int{}}
}

func (f *fakeBus) Publish(context.Context, *message.Message) error { return nil }
func (f *fakeBus) Subscribe(context.Context, string, bus.SubscribeOptions) (message.Subscriber, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeBus) Topic() string { return "t" }
func (f *fakeBus) Close() error  { return nil }
func (f *fakeBus) Capabilities() bus.Capabilities {
	return bus.Capabilities{FanOut: true, ReportsLag: f.reportsLag}
}

func (f *fakeBus) Lag(_ context.Context, consumer string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[consumer]++
	if err := f.err[consumer]; err != nil {
		return 0, err
	}
	return f.lag[consumer], nil
}

func (f *fakeBus) set(consumer string, lag int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lag[consumer] = lag
	f.err[consumer] = err
}

func (f *fakeBus) callCount(consumer string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[consumer]
}

// noLagBus declares ReportsLag but does not implement bus.LagReporter (no
// embedding, so no promoted Lag method): the poller must not panic.
type noLagBus struct{ inner *fakeBus }

func (b noLagBus) Publish(ctx context.Context, m *message.Message) error {
	return b.inner.Publish(ctx, m)
}
func (b noLagBus) Subscribe(ctx context.Context, c string, o bus.SubscribeOptions) (message.Subscriber, error) {
	return b.inner.Subscribe(ctx, c, o)
}
func (b noLagBus) Topic() string                  { return b.inner.Topic() }
func (b noLagBus) Close() error                   { return b.inner.Close() }
func (b noLagBus) Capabilities() bus.Capabilities { return bus.Capabilities{ReportsLag: true} }

func gauge(t *testing.T, m *Metrics, consumer string) float64 {
	t.Helper()
	g, err := m.BusConsumerLag.GetMetricWithLabelValues(consumer)
	require.NoError(t, err)
	return testutil.ToFloat64(g)
}

func TestLagPoller_UpdatesGaugeFromReporter(t *testing.T) {
	fb := newFakeBus(true)
	fb.set("sink-a", 7, nil)
	fb.set("sink-b", 0, nil)
	m := New("v", "c")
	p := NewLagPoller(fb, m, 10*time.Millisecond, nil)
	require.True(t, p.Supported())
	p.Register("sink-a")
	p.Register("sink-b")
	p.Register("sink-a") // duplicate is a no-op

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	assert.Eventually(t, func() bool { return gauge(t, m, "sink-a") == 7 }, time.Second, 5*time.Millisecond)
	assert.InDelta(t, 0, gauge(t, m, "sink-b"), 0)

	fb.set("sink-a", 2, nil)
	assert.Eventually(t, func() bool { return gauge(t, m, "sink-a") == 2 }, time.Second, 5*time.Millisecond)
	assert.Greater(t, fb.callCount("sink-a"), 1, "polled repeatedly")

	cancel()
	require.NoError(t, <-done)
}

func TestLagPoller_ToleratesErrorsAndLogsOncePerStreak(t *testing.T) {
	fb := newFakeBus(true)
	fb.set("good", 4, nil)
	fb.set("bad", 0, errors.New("consumer not found"))
	m := New("v", "c")
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := NewLagPoller(fb, m, 5*time.Millisecond, logger)
	p.Register("good")
	p.Register("bad")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	assert.Eventually(t, func() bool { return fb.callCount("bad") >= 3 }, time.Second, time.Millisecond)
	assert.InDelta(t, 4, gauge(t, m, "good"), 0, "the healthy consumer keeps being sampled")
	_, err := m.BusConsumerLag.GetMetricWithLabelValues("bad")
	require.NoError(t, err)
	cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_bus_consumer_lag")
	require.NoError(t, err)
	assert.Equal(t, 2, cnt, "the failing consumer never got a series; GetMetricWithLabelValues creates it lazily")

	// warn once, then debug
	assert.Equal(t, 1, strings.Count(logs.String(), "level=WARN"), logs.String())
	assert.GreaterOrEqual(t, strings.Count(logs.String(), "level=DEBUG"), 1)

	// recovery: gauge set and an info line
	fb.set("bad", 9, nil)
	assert.Eventually(t, func() bool { return gauge(t, m, "bad") == 9 }, time.Second, time.Millisecond)
	assert.Contains(t, logs.String(), "consumer lag sampling recovered")

	// a new failure streak warns again and keeps the last value
	fb.set("bad", 0, errors.New("timeout"))
	before := fb.callCount("bad")
	assert.Eventually(t, func() bool { return fb.callCount("bad") >= before+2 }, time.Second, time.Millisecond)
	assert.InDelta(t, 9, gauge(t, m, "bad"), 0, "last value kept on error")
	assert.Equal(t, 2, strings.Count(logs.String(), "level=WARN"))

	cancel()
	require.NoError(t, <-done)
}

func TestLagPoller_UnregisterDropsSeries(t *testing.T) {
	fb := newFakeBus(true)
	fb.set("a", 1, nil)
	m := New("v", "c")
	p := NewLagPoller(fb, m, time.Hour, nil)
	p.Register("a")
	p.Poll(context.Background())
	cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_bus_consumer_lag")
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	p.Unregister("a")
	cnt, err = testutil.GatherAndCount(m.Registry, "antwatcher_bus_consumer_lag")
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)

	before := fb.callCount("a")
	p.Poll(context.Background())
	assert.Equal(t, before, fb.callCount("a"), "unregistered consumer is not sampled")
}

func TestLagPoller_BusWithoutLagLogsOnceAndSkips(t *testing.T) {
	for name, b := range map[string]bus.Bus{
		"capability false": newFakeBus(false),
		"interface absent": noLagBus{inner: newFakeBus(true)},
	} {
		t.Run(name, func(t *testing.T) {
			m := New("v", "c")
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			p := NewLagPoller(b, m, time.Millisecond, logger)
			assert.False(t, p.Supported())
			p.Register("sink-a")

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			start := time.Now()
			require.NoError(t, p.Run(ctx))
			assert.Less(t, time.Since(start), 100*time.Millisecond, "returns without waiting for ctx")
			assert.Equal(t, 1, strings.Count(logs.String(), "does not report consumer lag"))

			p.Poll(context.Background()) // no-op, no panic
			cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_bus_consumer_lag")
			require.NoError(t, err)
			assert.Equal(t, 0, cnt)
		})
	}
}

func TestLagPoller_NilBus(t *testing.T) {
	p := NewLagPoller(nil, New("v", "c"), time.Second, nil)
	assert.False(t, p.Supported())
	require.NoError(t, p.Run(context.Background()))
}

func TestLagPoller_CancelledContextIsNotAFailure(t *testing.T) {
	fb := newFakeBus(true)
	fb.set("a", 0, context.Canceled)
	m := New("v", "c")
	var logs bytes.Buffer
	p := NewLagPoller(fb, m, time.Hour, slog.New(slog.NewTextHandler(&logs, nil)))
	p.Register("a")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.Poll(ctx)
	assert.Empty(t, logs.String(), "no warning during shutdown")
}

// syncBuffer is a bytes.Buffer safe for the poller's background goroutine.
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

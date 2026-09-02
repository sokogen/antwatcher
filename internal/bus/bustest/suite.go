// Package bustest is the conformance suite every bus driver must pass. Run
// exercises the bus.Bus contract and, for each capability the driver declares,
// a test that proves it. A driver that declares a capability it cannot prove
// fails here rather than in production.
package bustest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/bus"
)

const (
	// receiveTimeout bounds one expected delivery. It is generous so drivers
	// with redelivery delays (JetStream nak delay, ack wait) pass unchanged.
	receiveTimeout = 30 * time.Second
	// quietWindow is how long the suite watches a channel that must stay silent.
	quietWindow = 500 * time.Millisecond
	// metaRun tags every message with the id of the subtest that published it,
	// so a broker shared between subtests (a durable stream) does not leak
	// messages between them. Foreign messages are acked and ignored.
	metaRun = "bustest_run"
	// metaN carries the publish index so tests can name what they expect.
	metaN = "bustest_n"
)

// Option configures Run.
type Option func(*options)

type options struct {
	outage func(t *testing.T) (restore func())
}

// WithOutage supplies the hook a driver needs to prove DurablePublish: it makes
// the broker unreachable for every bus returned by open until restore is
// called. Required when the driver declares DurablePublish.
func WithOutage(outage func(t *testing.T) (restore func())) Option {
	return func(o *options) { o.outage = outage }
}

// Run runs the conformance suite. open returns a fresh bus for one subtest and
// may be called several times; when the driver declares DurableConsumers every
// bus returned by open must share the same broker so a consumer created on one
// can be reopened on another. Buses are closed by the suite.
func Run(t *testing.T, open func(t *testing.T) bus.Bus, opts ...Option) {
	t.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	caps := openBus(t, open).Capabilities()
	t.Logf("capabilities: %s", caps)

	t.Run("PublishSubscribe", func(t *testing.T) { testPublishSubscribe(t, open) })
	t.Run("NackRedelivers", func(t *testing.T) { testNackRedelivers(t, open) })
	t.Run("SubscriberBoundToTopic", func(t *testing.T) { testSubscriberBoundToTopic(t, open) })
	t.Run("InvalidConsumerName", func(t *testing.T) { testInvalidConsumerName(t, open) })
	t.Run("PublishAfterClose", func(t *testing.T) { testPublishAfterClose(t, open) })
	t.Run("SubscribeAfterClose", func(t *testing.T) { testSubscribeAfterClose(t, open) })
	t.Run("CloseIdempotent", func(t *testing.T) { testCloseIdempotent(t, open) })

	if caps.FanOut {
		t.Run("FanOut", func(t *testing.T) { testFanOut(t, open) })
	}
	if caps.HistoricalReplay {
		t.Run("HistoricalReplay", func(t *testing.T) { testHistoricalReplay(t, open) })
	}
	if caps.DurableConsumers {
		t.Run("DurableConsumers", func(t *testing.T) { testDurableConsumers(t, open) })
	}
	if caps.Deduplicates {
		t.Run("Deduplicates", func(t *testing.T) { testDeduplicates(t, open) })
	}
	if caps.ReportsLag {
		t.Run("ReportsLag", func(t *testing.T) { testReportsLag(t, open) })
	}
	if caps.DurablePublish {
		t.Run("DurablePublish", func(t *testing.T) {
			if o.outage == nil {
				t.Fatal("driver declares DurablePublish but the test supplies no bustest.WithOutage hook to prove it")
			}
			testDurablePublish(t, open, o.outage)
		})
	}
}

// run is the per-subtest state: the bus under test and the tag on its messages.
type run struct {
	t  *testing.T
	b  bus.Bus
	id string
	n  int
}

func newRun(t *testing.T, open func(t *testing.T) bus.Bus) *run {
	t.Helper()
	return &run{t: t, b: openBus(t, open), id: watermill.NewUUID()}
}

func openBus(t *testing.T, open func(t *testing.T) bus.Bus) bus.Bus {
	t.Helper()
	b := open(t)
	require.NotNil(t, b, "open returned a nil bus")
	t.Cleanup(func() { assert.NoError(t, b.Close(), "closing the bus") })
	return b
}

// message builds a tagged message with a fresh UUID.
func (r *run) message() *message.Message {
	r.n++
	msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf(`{"run":%q,"n":%d}`, r.id, r.n)))
	msg.Metadata.Set(metaRun, r.id)
	msg.Metadata.Set(metaN, strconv.Itoa(r.n))
	msg.Metadata.Set("event", "workflow_run")
	return msg
}

func (r *run) publish(msg *message.Message) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
	defer cancel()
	require.NoError(r.t, r.b.Publish(ctx, msg), "publish %s", msg.UUID)
}

// consumer creates a uniquely named consumer with the given start position and
// returns its message channel. The subscriber is closed at test end.
func (r *run) consumer(name string, from bus.StartPosition) (string, <-chan *message.Message) {
	r.t.Helper()
	full := name + "-" + r.id[:8]
	ch, _ := r.subscribe(full, from)
	return full, ch
}

// subscribe opens (or reopens) the named consumer and returns its channel and
// an idempotent close function; the subscriber is also closed at test end.
func (r *run) subscribe(name string, from bus.StartPosition) (<-chan *message.Message, func()) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r.t.Cleanup(cancel)
	sub, err := r.b.Subscribe(ctx, name, bus.SubscribeOptions{StartFrom: from})
	require.NoError(r.t, err, "subscribe %s", name)
	var once sync.Once
	closeSub := func() {
		once.Do(func() { assert.NoError(r.t, sub.Close(), "closing subscriber %s", name) })
	}
	r.t.Cleanup(closeSub)
	ch, err := sub.Subscribe(ctx, r.b.Topic())
	require.NoError(r.t, err, "subscriber.Subscribe %s", name)
	return ch, closeSub
}

// receive returns the next message of this run, acking and skipping messages
// that belong to other runs on a shared broker.
func (r *run) receive(ch <-chan *message.Message) *message.Message {
	r.t.Helper()
	deadline := time.After(receiveTimeout)
	for {
		select {
		case msg, ok := <-ch:
			require.True(r.t, ok, "subscriber channel closed while waiting for a message")
			if msg.Metadata.Get(metaRun) != r.id {
				msg.Ack()
				continue
			}
			return msg
		case <-deadline:
			r.t.Fatalf("no message within %s", receiveTimeout)
			return nil
		}
	}
}

// receiveAll receives n messages of this run and returns them keyed by UUID.
func (r *run) receiveAll(ch <-chan *message.Message, n int) map[string]*message.Message {
	r.t.Helper()
	got := make(map[string]*message.Message, n)
	for len(got) < n {
		msg := r.receive(ch)
		if _, dup := got[msg.UUID]; dup {
			// At-least-once allows a redelivery of an unacked message; keep the
			// first copy and ack the duplicate so it does not loop.
			msg.Ack()
			continue
		}
		got[msg.UUID] = msg
	}
	return got
}

// expectQuiet fails when a message of this run arrives within quietWindow.
func (r *run) expectQuiet(ch <-chan *message.Message, why string) {
	r.t.Helper()
	deadline := time.After(quietWindow)
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				r.t.Fatalf("subscriber channel closed while expecting silence (%s)", why)
			}
			if msg.Metadata.Get(metaRun) != r.id {
				msg.Ack()
				continue
			}
			msg.Ack()
			r.t.Fatalf("unexpected message %s (n=%s): %s", msg.UUID, msg.Metadata.Get(metaN), why)
		case <-deadline:
			return
		}
	}
}

func testPublishSubscribe(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	_, ch := r.consumer("ps", bus.Now)

	sent := r.message()
	sent.Metadata.Set("action", "completed")
	sent.Metadata.Set("repository", "octo/repo")
	r.publish(sent)

	got := r.receive(ch)
	assert.Equal(t, sent.UUID, got.UUID, "UUID")
	assert.Equal(t, sent.Metadata, got.Metadata, "metadata")
	assert.Equal(t, sent.Payload, got.Payload, "payload")
	assert.True(t, got.Ack(), "ack")
	r.expectQuiet(ch, "an acked message must not be redelivered")
}

func testNackRedelivers(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	_, ch := r.consumer("nack", bus.Now)

	sent := r.message()
	r.publish(sent)

	first := r.receive(ch)
	require.Equal(t, sent.UUID, first.UUID)
	require.True(t, first.Nack(), "nack")

	second := r.receive(ch)
	assert.Equal(t, sent.UUID, second.UUID, "the nacked message must be redelivered")
	assert.Equal(t, sent.Payload, second.Payload, "redelivery keeps the payload")
	assert.True(t, second.Ack())
	r.expectQuiet(ch, "after the ack nothing else is pending")
}

func testSubscriberBoundToTopic(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	sub, err := r.b.Subscribe(context.Background(), "bound-"+r.id[:8], bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sub.Close()) })

	_, err = sub.Subscribe(context.Background(), r.b.Topic()+".other")
	require.Error(t, err)
	assert.ErrorIs(t, err, bus.ErrWrongTopic)
}

func testInvalidConsumerName(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	for _, name := range []string{"", "with space", "dots.are.bad", "star*"} {
		_, err := r.b.Subscribe(context.Background(), name, bus.SubscribeOptions{StartFrom: bus.Now})
		require.Error(t, err, "name %q", name)
		assert.ErrorIs(t, err, bus.ErrInvalidConsumerName, "name %q", name)
	}
}

func testPublishAfterClose(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	require.NoError(t, r.b.Close())
	err := r.b.Publish(context.Background(), r.message())
	require.Error(t, err)
	assert.ErrorIs(t, err, bus.ErrClosed)
}

func testSubscribeAfterClose(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	require.NoError(t, r.b.Close())
	_, err := r.b.Subscribe(context.Background(), "late-"+r.id[:8], bus.SubscribeOptions{StartFrom: bus.Now})
	require.Error(t, err)
	assert.ErrorIs(t, err, bus.ErrClosed)
}

func testCloseIdempotent(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	require.NoError(t, r.b.Close())
	require.NoError(t, r.b.Close(), "second Close")
}

func testFanOut(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	_, a := r.consumer("fan-a", bus.Now)
	_, b := r.consumer("fan-b", bus.Now)

	const n = 5
	want := map[string]struct{}{}
	for range n {
		msg := r.message()
		want[msg.UUID] = struct{}{}
		r.publish(msg)
	}

	// a acks everything while b has acked nothing: positions are independent.
	gotA := r.receiveAll(a, n)
	for _, msg := range gotA {
		require.True(t, msg.Ack())
	}
	assert.Equal(t, want, uuids(gotA), "consumer a receives every message")

	gotB := r.receiveAll(b, n)
	assert.Equal(t, want, uuids(gotB), "consumer b receives every message regardless of a")
	for _, msg := range gotB {
		require.True(t, msg.Ack())
	}
	r.expectQuiet(a, "a has acked everything")
	r.expectQuiet(b, "b has acked everything")
}

func testHistoricalReplay(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)

	const n = 3
	want := map[string]struct{}{}
	for range n {
		msg := r.message()
		want[msg.UUID] = struct{}{}
		r.publish(msg)
	}

	_, early := r.consumer("replay-earliest", bus.Earliest)
	got := r.receiveAll(early, n)
	assert.Equal(t, want, uuids(got), "Earliest replays the history")
	for _, msg := range got {
		require.True(t, msg.Ack())
	}
	r.expectQuiet(early, "history delivered once")

	_, late := r.consumer("replay-now", bus.Now)
	r.expectQuiet(late, "Now must not replay history")

	next := r.message()
	r.publish(next)
	gotLate := r.receive(late)
	assert.Equal(t, next.UUID, gotLate.UUID, "Now receives what is published afterwards")
	require.True(t, gotLate.Ack())
	gotEarly := r.receive(early)
	assert.Equal(t, next.UUID, gotEarly.UUID, "Earliest keeps receiving new messages after the history")
	require.True(t, gotEarly.Ack())
}

func testDurableConsumers(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	name := "durable-" + r.id[:8]
	ch, closeSub := r.subscribe(name, bus.Now)

	first, second := r.message(), r.message()
	r.publish(first)
	r.publish(second)

	got := r.receiveAll(ch, 2)
	require.Len(t, got, 2)
	require.True(t, got[first.UUID].Ack(), "ack the first")
	require.True(t, got[second.UUID].Nack(), "leave the second unacked")

	// Stop this process's view of the consumer, then reopen the same consumer
	// on a fresh bus, as after a restart. Earliest is requested to prove the
	// stored position wins over the start position given on a reopen.
	closeSub()
	require.NoError(t, r.b.Close())
	r2 := &run{t: t, b: openBus(t, open), id: r.id}
	ch2, _ := r2.subscribe(name, bus.Earliest)
	redelivered := r2.receive(ch2)
	assert.Equal(t, second.UUID, redelivered.UUID, "only the unacked message is redelivered")
	require.True(t, redelivered.Ack())
	r2.expectQuiet(ch2, "the acked message must not come back after a reopen")
}

func testDeduplicates(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	_, ch := r.consumer("dedup", bus.Now)

	msg := r.message()
	r.publish(msg)
	r.publish(msg.Copy())

	got := r.receive(ch)
	assert.Equal(t, msg.UUID, got.UUID)
	require.True(t, got.Ack())
	r.expectQuiet(ch, "a re-published UUID inside the dedup window is collapsed")
}

func testReportsLag(t *testing.T, open func(t *testing.T) bus.Bus) {
	r := newRun(t, open)
	reporter, ok := r.b.(bus.LagReporter)
	require.True(t, ok, "driver declares ReportsLag but does not implement bus.LagReporter")

	name, ch := r.consumer("lag", bus.Now)
	lagOf := func() int64 {
		ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
		defer cancel()
		lag, err := reporter.Lag(ctx, name)
		require.NoError(t, err)
		return lag
	}

	const n = 3
	for range n {
		r.publish(r.message())
	}
	require.Eventually(t, func() bool { return lagOf() == n }, receiveTimeout, 50*time.Millisecond,
		"lag must count the unacked backlog (last seen %d)", lagOf())

	got := r.receiveAll(ch, n)
	assert.Equal(t, int64(n), lagOf(), "received but unacked messages still count")
	for _, msg := range got {
		require.True(t, msg.Ack())
	}
	require.Eventually(t, func() bool { return lagOf() == 0 }, receiveTimeout, 50*time.Millisecond,
		"lag must drop after ack (last seen %d)", lagOf())
}

func testDurablePublish(t *testing.T, open func(t *testing.T) bus.Bus, outage func(t *testing.T) (restore func())) {
	r := newRun(t, open)
	r.publish(r.message())

	restore := outage(t)
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := r.b.Publish(ctx, r.message())
	require.Error(t, err, "publish must not report success while the broker is unreachable")
	assert.Less(t, time.Since(start), 10*time.Second, "publish must give up by the ctx deadline")

	restore()
	restored = true
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return r.b.Publish(ctx, r.message()) == nil
	}, receiveTimeout, 200*time.Millisecond, "publish must succeed again once the broker is back")
}

func uuids(msgs map[string]*message.Message) map[string]struct{} {
	out := make(map[string]struct{}, len(msgs))
	for uuid := range msgs {
		out[uuid] = struct{}{}
	}
	return out
}

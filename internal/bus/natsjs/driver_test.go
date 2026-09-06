package natsjs_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/bus/bustest"
	"github.com/sokogen/antwatcher/internal/bus/natsjs"
	"github.com/sokogen/antwatcher/internal/config"
)

const topic = "antwatcher.events"

// testReconnectWait keeps the DurablePublish check fast: the client retries
// the in-process connection this often while the test server is stopped.
const testReconnectWait = 50 * time.Millisecond

// openOn opens a bus on the shared test server with the test configuration.
func openOn(t *testing.T, ts *natsjs.TestServer, cfg natsjs.Config, logger *slog.Logger) *natsjs.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := natsjs.New(ctx, cfg, topic, logger, natsjs.WithInProcess(ts), natsjs.WithReconnectWait(testReconnectWait))
	require.NoError(t, err)
	return b
}

// jsClient is an independent JetStream client on the test server used to
// inspect what the driver created.
func jsClient(t *testing.T, ts *natsjs.TestServer) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(ts))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

func TestConformance(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	open := func(t *testing.T) bus.Bus { return openOn(t, ts, cfg, nil) }
	outage := func(_ *testing.T) (restore func()) {
		ts.Stop()
		return func() { ts.Start() }
	}
	bustest.Run(t, open, bustest.WithOutage(outage))
}

func TestCapabilities(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	assert.Equal(t, bus.Capabilities{
		DurablePublish:   true,
		DurableConsumers: true,
		FanOut:           true,
		HistoricalReplay: true,
		Deduplicates:     true,
		ReportsLag:       true,
		Retention:        bus.RetentionTime,
	}, b.Capabilities())
	assert.Equal(t, topic, b.Topic())
	assert.True(t, b.Connected())

	_, isReporter := any(b).(bus.LagReporter)
	assert.True(t, isReporter, "ReportsLag requires bus.LagReporter")

	warnings, err := bus.ResolveIngress(b.Capabilities(), bus.ModeFail)
	require.NoError(t, err, "accepted as ingress in fail mode")
	assert.Empty(t, warnings)
	effective, warnings, err := bus.ResolveConsumer(b.Capabilities(), bus.Earliest, bus.ModeFail)
	require.NoError(t, err)
	assert.Equal(t, bus.Earliest, effective)
	assert.Empty(t, warnings, "nothing is lost on this driver")
}

func TestDeduplicatesFollowsDedupWindow(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	cfg.DedupWindow = 0
	b := openOn(t, ts, cfg, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	assert.False(t, b.Capabilities().Deduplicates, "dedup_window 0 leaves the server default and declares no Deduplicates")
}

func TestStreamCreatedWithRetentionDedupAndLimits(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	cfg.Retention = 2 * time.Hour
	cfg.DedupWindow = 3 * time.Minute
	b := openOn(t, ts, cfg, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	js := jsClient(t, ts)
	s, err := js.Stream(context.Background(), cfg.Stream)
	require.NoError(t, err)
	info := s.CachedInfo()
	assert.Equal(t, []string{topic}, info.Config.Subjects)
	assert.Equal(t, 2*time.Hour, info.Config.MaxAge, "retention")
	assert.Equal(t, 3*time.Minute, info.Config.Duplicates, "dedup window")
	assert.Equal(t, jetstream.LimitsPolicy, info.Config.Retention, "limits retention so orphaned consumers pin no data")
	assert.Equal(t, jetstream.FileStorage, info.Config.Storage)
}

func TestEnsureStreamIdempotentAndUpdates(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	js := jsClient(t, ts)
	ctx := context.Background()
	cfg := ts.Config()

	s1, err := natsjs.EnsureStream(ctx, js, cfg, topic)
	require.NoError(t, err)
	created := s1.CachedInfo().Created

	s2, err := natsjs.EnsureStream(ctx, js, cfg, topic)
	require.NoError(t, err, "second run with the same configuration")
	assert.Equal(t, created, s2.CachedInfo().Created, "the stream is reused, not recreated")
	assert.Equal(t, cfg.Retention, s2.CachedInfo().Config.MaxAge)

	cfg.Retention = 4 * time.Hour
	s3, err := natsjs.EnsureStream(ctx, js, cfg, topic)
	require.NoError(t, err, "changed configuration updates the stream")
	assert.Equal(t, created, s3.CachedInfo().Created, "still the same stream")
	assert.Equal(t, 4*time.Hour, s3.CachedInfo().Config.MaxAge, "retention updated")

	names := js.StreamNames(ctx)
	var count int
	for range names.Name() {
		count++
	}
	require.NoError(t, names.Err())
	assert.Equal(t, 1, count, "exactly one stream")
}

func TestEnsureStreamRefusesAStreamCarryingAnotherTopic(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	js := jsClient(t, ts)
	ctx := context.Background()
	cfg := ts.Config()

	_, err := natsjs.EnsureStream(ctx, js, cfg, topic)
	require.NoError(t, err)

	// A second bus with the default stream name on the same broker — a forward
	// target beside the ingress bus — must not take the subject away.
	_, err = natsjs.EnsureStream(ctx, js, cfg, "other.topic")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already carries subjects")

	s, err := js.Stream(ctx, cfg.Stream)
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{topic}, info.Config.Subjects, "the original subject is untouched")

	cfg.Stream = "OTHER"
	other, err := natsjs.EnsureStream(ctx, js, cfg, "other.topic")
	require.NoError(t, err, "its own stream name works")
	assert.Equal(t, []string{"other.topic"}, other.CachedInfo().Config.Subjects)
}

func TestStreamConfig(t *testing.T) {
	cfg := natsjs.DefaultConfig()
	sc := natsjs.StreamConfig(cfg, topic)
	assert.Equal(t, cfg.Stream, sc.Name)
	assert.Equal(t, []string{topic}, sc.Subjects)
	assert.Equal(t, jetstream.LimitsPolicy, sc.Retention)
	assert.Equal(t, jetstream.FileStorage, sc.Storage)
	assert.Equal(t, cfg.Retention, sc.MaxAge)
	assert.Equal(t, cfg.DedupWindow, sc.Duplicates)
}

// TestExternalURLMode connects to a nats-server over TCP as an operator would
// with an external deployment.
func TestExternalURLMode(t *testing.T) {
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	require.True(t, srv.ReadyForConnections(10*time.Second))

	cfg := natsjs.DefaultConfig()
	cfg.Embedded = false
	cfg.StoreDir = ""
	cfg.URL = config.URL(srv.ClientURL())
	cfg.Retention = time.Hour
	cfg.DedupWindow = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := natsjs.New(ctx, cfg, topic, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	assert.True(t, b.Connected())

	ch := subscribe(t, b, "url-mode", bus.Now)
	sent := newMessage("m1")
	require.NoError(t, b.Publish(ctx, sent))
	got := receive(t, ch)
	assert.Equal(t, sent.UUID, got.UUID)
	assert.Equal(t, sent.Metadata, got.Metadata)
	assert.Equal(t, sent.Payload, got.Payload)
	require.True(t, got.Ack())
}

func TestExternalURLUnreachable(t *testing.T) {
	cfg := natsjs.DefaultConfig()
	cfg.Embedded = false
	cfg.URL = "nats://127.0.0.1:1" // nothing listens on port 1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := natsjs.New(ctx, cfg, topic, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect")
}

// TestEmbeddedMode starts the driver's own in-process server through the
// registry, as the default configuration does.
func TestEmbeddedMode(t *testing.T) {
	assert.Contains(t, bus.Names(), natsjs.Name)

	dir := t.TempDir()
	var cfg config.Bus
	require.NoError(t, yaml.Unmarshal([]byte(
		"driver: nats-jetstream\ntopic: "+topic+"\nnats-jetstream:\n  store_dir: "+dir+"\n  retention: 1h\n  dedup_window: 1m\n"), &cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b, err := bus.Open(ctx, cfg, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, topic, b.Topic())
	assert.True(t, b.Capabilities().DurablePublish)

	ch := subscribe(t, b, "embedded", bus.Now)
	sent := newMessage("m1")
	require.NoError(t, b.Publish(ctx, sent))
	got := receive(t, ch)
	assert.Equal(t, sent.UUID, got.UUID)
	require.True(t, got.Ack())
	require.NoError(t, b.Close())
	assert.DirExists(t, dir+"/jetstream", "JetStream data lives under store_dir")

	// Unknown keys in the driver block are rejected through the registry.
	require.NoError(t, yaml.Unmarshal([]byte("driver: nats-jetstream\ntopic: t\nnats-jetstream:\n  store_dir: "+dir+"\n  port: 4222\n"), &cfg))
	_, err = bus.Open(ctx, cfg, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "port")
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	ctx := context.Background()
	_, err := natsjs.New(ctx, natsjs.DefaultConfig(), "", nil)
	require.Error(t, err, "topic is required")

	cfg := natsjs.DefaultConfig()
	cfg.Stream = ""
	_, err = natsjs.New(ctx, cfg, topic, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream is required")

	ts := natsjs.NewTestServer(t)
	cfg = ts.Config()
	cfg.AckWait = 0
	_, err = natsjs.New(ctx, cfg, topic, nil, natsjs.WithInProcess(ts))
	require.Error(t, err, "broker settings are validated even with an injected server")
	assert.Contains(t, err.Error(), "ack_wait")
}

func TestPublishHonorsContextWhileServerIsStopped(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.Publish(context.Background(), newMessage("before")))

	ts.Stop()
	require.Eventually(t, func() bool { return !b.Connected() }, 10*time.Second, 10*time.Millisecond)

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := b.Publish(ctx, newMessage("deadline"))
		require.Error(t, err)
		assert.Less(t, time.Since(start), 5*time.Second, "returns promptly")
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		err := b.Publish(ctx, newMessage("cancel"))
		require.Error(t, err)
		assert.Less(t, time.Since(start), 5*time.Second, "returns promptly")
	})

	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, b.Publish(ctx, newMessage("cancelled")), context.Canceled)
	})

	ts.Start()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return b.Publish(ctx, newMessage("after")) == nil
	}, 30*time.Second, 100*time.Millisecond, "publish succeeds again once the server is back")
}

// TestSubscriberSurvivesBrokerOutage proves the fetch loop keeps retrying
// while the broker is down and resumes on the same durable consumer once it
// is back, without the sink reopening anything.
func TestSubscriberSurvivesBrokerOutage(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()

	ch := subscribe(t, b, "survivor", bus.Now)
	require.NoError(t, b.Publish(ctx, newMessage("before")))
	require.True(t, receive(t, ch).Ack())

	ts.Stop()
	require.Eventually(t, func() bool { return !b.Connected() }, 10*time.Second, 10*time.Millisecond)
	time.Sleep(1500 * time.Millisecond) // long enough for at least one failed fetch and its retry pause
	ts.Start()

	require.Eventually(t, func() bool {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return b.Publish(pctx, newMessage("after")) == nil
	}, 30*time.Second, 100*time.Millisecond)
	got := receive(t, ch)
	assert.Equal(t, "after", got.UUID, "the same subscription delivers after the outage")
	require.True(t, got.Ack())
}

func TestPublishValidation(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()

	require.Error(t, b.Publish(ctx, nil), "nil message")
	require.Error(t, b.Publish(ctx, message.NewMessage("", []byte("{}"))), "empty UUID")

	reserved := newMessage("reserved")
	reserved.Metadata.Set("Nats-Msg-Id", "other")
	err := b.Publish(ctx, reserved)
	require.Error(t, err, "metadata must not spoof NATS headers")
	assert.Contains(t, err.Error(), "reserved")

	msg := newMessage("intact")
	require.NoError(t, b.Publish(ctx, msg))
	assert.Equal(t, message.Metadata{"k": "v"}, msg.Metadata, "the caller's message is not modified")
}

func TestNakDelay(t *testing.T) {
	minD, maxD := 5*time.Second, 5*time.Minute
	tests := []struct {
		delivered uint64
		want      time.Duration
	}{
		{0, 5 * time.Second},
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, 80 * time.Second},
		{6, 160 * time.Second},
		{7, 5 * time.Minute},
		{8, 5 * time.Minute},
		{100, 5 * time.Minute},
		{1 << 40, 5 * time.Minute},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, natsjs.NakDelay(minD, maxD, tc.delivered), "delivered=%d", tc.delivered)
	}
	assert.Equal(t, time.Second, natsjs.NakDelay(time.Second, time.Second, 50), "min == max")
}

// TestNakDelayGrowsBetweenRedeliveries checks the delay computed from the
// broker's delivery count is applied: the second redelivery of a nacked
// message takes longer than the first.
func TestNakDelayGrowsBetweenRedeliveries(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	cfg.NakDelayMin = 200 * time.Millisecond
	cfg.NakDelayMax = 2 * time.Second
	b := openOn(t, ts, cfg, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	ch := subscribe(t, b, "nak-growth", bus.Now)
	require.NoError(t, b.Publish(context.Background(), newMessage("m")))

	first := receive(t, ch)
	t1 := time.Now()
	require.True(t, first.Nack())
	second := receive(t, ch)
	gap1 := time.Since(t1)
	t2 := time.Now()
	require.True(t, second.Nack())
	third := receive(t, ch)
	gap2 := time.Since(t2)
	require.True(t, third.Ack())

	assert.GreaterOrEqual(t, gap1, 200*time.Millisecond, "first redelivery waits nak_delay_min")
	assert.GreaterOrEqual(t, gap2, 400*time.Millisecond, "second redelivery waits twice as long")
}

func TestLagReflectsUnackedMessages(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()

	_, err := b.Lag(ctx, "missing")
	require.Error(t, err, "unknown consumer")

	sub, err := b.Subscribe(ctx, "lag", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Close()) })
	lag, err := b.Lag(ctx, "lag")
	require.NoError(t, err)
	assert.Zero(t, lag)

	for i := range 3 {
		require.NoError(t, b.Publish(ctx, newMessage("m"+string(rune('0'+i)))))
	}
	lag, err = b.Lag(ctx, "lag")
	require.NoError(t, err)
	assert.Equal(t, int64(3), lag, "pending messages count before anything is delivered")

	ch, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	first := receive(t, ch)
	lag, err = b.Lag(ctx, "lag")
	require.NoError(t, err)
	assert.Equal(t, int64(3), lag, "a delivered but unacked message still counts")

	require.True(t, first.Ack())
	require.Eventually(t, func() bool {
		lag, err := b.Lag(ctx, "lag")
		return err == nil && lag == 2
	}, 5*time.Second, 20*time.Millisecond, "ack lowers the lag")
	for range 2 {
		require.True(t, receive(t, ch).Ack())
	}
	require.Eventually(t, func() bool {
		lag, err := b.Lag(ctx, "lag")
		return err == nil && lag == 0
	}, 5*time.Second, 20*time.Millisecond)
}

func TestExistingConsumerKeepsDeliverPolicy(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := openOn(t, ts, ts.Config(), logger)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()

	require.NoError(t, b.Publish(ctx, newMessage("before-creation")))
	sub, err := b.Subscribe(ctx, "keep", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	require.NoError(t, sub.Close())
	assert.Contains(t, logs.String(), "consumer created")

	require.NoError(t, b.Publish(ctx, newMessage("after-creation")))
	logs.Reset()
	ch := subscribe(t, b, "keep", bus.Earliest)
	assert.Contains(t, logs.String(), "keeping its original start position", "the change of start position is logged")

	got := receive(t, ch)
	assert.Equal(t, "after-creation", got.UUID, "the stored Now policy wins over the Earliest requested on reopen")
	require.True(t, got.Ack())
	expectQuiet(t, ch)

	js := jsClient(t, ts)
	c, err := js.Consumer(ctx, ts.Config().Stream, "keep")
	require.NoError(t, err)
	info := c.CachedInfo()
	assert.Equal(t, jetstream.DeliverNewPolicy, info.Config.DeliverPolicy)
	assert.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)
	assert.Equal(t, -1, info.Config.MaxDeliver, "unlimited deliveries: nothing is dropped by the broker")
	assert.Equal(t, ts.Config().AckWait, info.Config.AckWait)
}

func TestAckWaitUpdatedOnExistingConsumer(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	cfg := ts.Config()
	b := openOn(t, ts, cfg, nil)
	sub, err := b.Subscribe(context.Background(), "ackwait", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	require.NoError(t, sub.Close())
	require.NoError(t, b.Close())

	cfg.AckWait = 25 * time.Second
	b2 := openOn(t, ts, cfg, nil)
	t.Cleanup(func() { require.NoError(t, b2.Close()) })
	sub2, err := b2.Subscribe(context.Background(), "ackwait", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	require.NoError(t, sub2.Close())

	c, err := jsClient(t, ts).Consumer(context.Background(), cfg.Stream, "ackwait")
	require.NoError(t, err)
	assert.Equal(t, 25*time.Second, c.CachedInfo().Config.AckWait, "ack_wait follows the configuration")
}

func TestSubscribeErrors(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	_, err := b.Subscribe(context.Background(), "zero", bus.SubscribeOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid start position")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Subscribe(ctx, "cancelled", bus.SubscribeOptions{StartFrom: bus.Now})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSubscriberLifecycle(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	t.Run("close ends channel", func(t *testing.T) {
		sub, err := b.Subscribe(context.Background(), "lc-close", bus.SubscribeOptions{StartFrom: bus.Now})
		require.NoError(t, err)
		ch, err := sub.Subscribe(context.Background(), topic)
		require.NoError(t, err)
		require.NoError(t, sub.Close())
		expectClosed(t, ch)
		require.NoError(t, sub.Close(), "idempotent")
		_, err = sub.Subscribe(context.Background(), topic)
		require.Error(t, err, "a closed subscriber cannot subscribe again")
	})

	t.Run("ctx ends channel", func(t *testing.T) {
		sub, err := b.Subscribe(context.Background(), "lc-ctx", bus.SubscribeOptions{StartFrom: bus.Now})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sub.Close()) })
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := sub.Subscribe(ctx, topic)
		require.NoError(t, err)
		cancel()
		expectClosed(t, ch)
	})

	t.Run("wrong topic", func(t *testing.T) {
		sub, err := b.Subscribe(context.Background(), "lc-topic", bus.SubscribeOptions{StartFrom: bus.Now})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sub.Close()) })
		_, err = sub.Subscribe(context.Background(), "other")
		require.ErrorIs(t, err, bus.ErrWrongTopic)
	})
}

func TestBusCloseEndsSubscriptions(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	ch := subscribe(t, b, "close-all", bus.Now)
	require.NoError(t, b.Close())
	expectClosed(t, ch)
	require.NoError(t, b.Close(), "idempotent")
	require.ErrorIs(t, b.Publish(context.Background(), newMessage("late")), bus.ErrClosed)
	_, err := b.Subscribe(context.Background(), "late", bus.SubscribeOptions{StartFrom: bus.Now})
	require.ErrorIs(t, err, bus.ErrClosed)
	_, err = b.Lag(context.Background(), "close-all")
	require.ErrorIs(t, err, bus.ErrClosed)
}

// TestUnackedMessageRedeliveredAfterSubscriberClose proves a message handed
// to a consumer that goes away without acking is not lost.
func TestUnackedMessageRedeliveredAfterSubscriberClose(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	b := openOn(t, ts, ts.Config(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.Background()

	sub, err := b.Subscribe(ctx, "redeliver", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	ch, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, b.Publish(ctx, newMessage("m")))
	got := receive(t, ch)
	require.Equal(t, "m", got.UUID)
	// The handler gives up on the message only after the subscriber is closing:
	// Close waits for the in-flight nack and forwards it, so the broker
	// redelivers at once instead of after ack_wait.
	go func() {
		time.Sleep(100 * time.Millisecond)
		got.Nack()
	}()
	require.NoError(t, sub.Close())

	ch2 := subscribe(t, b, "redeliver", bus.Now)
	again := receive(t, ch2)
	assert.Equal(t, "m", again.UUID, "redelivered to the reopened consumer")
	require.True(t, again.Ack())
}

func TestValidateWith(t *testing.T) {
	router := config.Router{ProcessTimeout: 60 * time.Second}
	cfg := natsjs.DefaultConfig()
	cfg.AckWait = 90 * time.Second
	require.NoError(t, cfg.ValidateWith(router), "90s > 60s + 10s")

	for _, ackWait := range []time.Duration{70 * time.Second, 60 * time.Second, 10 * time.Second} {
		cfg.AckWait = ackWait
		err := cfg.ValidateWith(router)
		require.Error(t, err, "ack_wait %s", ackWait)
		assert.Contains(t, err.Error(), "ack_wait")
		assert.Contains(t, err.Error(), "process_timeout")
	}

	// Through the configuration machinery, as the -check command runs it.
	full := config.Default()
	var b config.Bus
	require.NoError(t, yaml.Unmarshal([]byte("driver: nats-jetstream\ntopic: t\nnats-jetstream:\n  ack_wait: 30s\n"), &b))
	full.Bus = b
	err := config.ValidateDrivers(full, config.Describers{Bus: bus.Describers()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bus.nats-jetstream")
	assert.Contains(t, err.Error(), "ack_wait")

	require.NoError(t, yaml.Unmarshal([]byte("driver: nats-jetstream\ntopic: t\nnats-jetstream:\n  ack_wait: 2m\n"), &b))
	full.Bus = b
	require.NoError(t, config.ValidateDrivers(full, config.Describers{Bus: bus.Describers()}))
}

func TestRedactedConfigMasksCredentials(t *testing.T) {
	full := config.Default()
	var b config.Bus
	require.NoError(t, yaml.Unmarshal([]byte(
		"driver: nats-jetstream\ntopic: t\nnats-jetstream:\n  embedded: false\n  url: nats://admin:s3cret@broker:4222\n  credentials: hunter2\n"), &b))
	full.Bus = b

	red, err := config.Redacted(full, config.Describers{Bus: bus.Describers()})
	require.NoError(t, err)
	typed, ok := red.Bus.Drivers["nats-jetstream"].(natsjs.Config)
	require.True(t, ok, "the block is rendered as the typed driver config")
	assert.Equal(t, "nats://admin:s3cret@broker:4222", typed.URL.Reveal(), "the value itself is intact for the driver")
	assert.False(t, typed.Embedded)

	out, err := yaml.Marshal(red)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "hunter2")
	assert.NotContains(t, string(out), "s3cret", "a password in the url's userinfo is a credential too")
	assert.Contains(t, string(out), "nats://***@broker:4222", "the host stays visible")
	assert.Contains(t, string(out), config.Mask)
	assert.Equal(t, config.Mask, typed.Credentials.String())
	assert.Equal(t, "hunter2", typed.Credentials.Reveal(), "the value itself is intact for the driver")
}

func TestDescribe(t *testing.T) {
	v, err := natsjs.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, natsjs.DefaultConfig(), v, "an absent block yields the defaults")

	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("retention: 48h\nnak_delay_min: 1s\n"), &node))
	v, err = natsjs.Describe(*node.Content[0])
	require.NoError(t, err)
	cfg := v.(natsjs.Config)
	assert.Equal(t, 48*time.Hour, cfg.Retention)
	assert.Equal(t, time.Second, cfg.NakDelayMin)
	assert.Equal(t, natsjs.DefaultConfig().Stream, cfg.Stream, "other fields keep their defaults")

	require.NoError(t, yaml.Unmarshal([]byte("cluster: x\n"), &node))
	_, err = natsjs.Describe(*node.Content[0])
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster")
}

func newMessage(uuid string) *message.Message {
	msg := message.NewMessage(uuid, []byte(`{}`))
	msg.Metadata.Set("k", "v")
	return msg
}

func subscribe(t *testing.T, b bus.Bus, name string, from bus.StartPosition) <-chan *message.Message {
	t.Helper()
	sub, err := b.Subscribe(context.Background(), name, bus.SubscribeOptions{StartFrom: from})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Close()) })
	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	return ch
}

func receive(t *testing.T, ch <-chan *message.Message) *message.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		require.True(t, ok, "channel closed")
		return msg
	case <-time.After(15 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func expectQuiet(t *testing.T, ch <-chan *message.Message) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("unexpected message %s", msg.UUID)
	case <-time.After(500 * time.Millisecond):
	}
}

func expectClosed(t *testing.T, ch <-chan *message.Message) {
	t.Helper()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must be closed")
	case <-time.After(10 * time.Second):
		t.Fatal("channel not closed")
	}
}

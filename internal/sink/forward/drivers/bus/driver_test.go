package bus_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/bus/gochannel"
	"github.com/sokogen/antwatcher/internal/bus/natsjs"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/forward"
	fwdbus "github.com/sokogen/antwatcher/internal/sink/forward/drivers/bus"
)

const ingressTopic = "antwatcher.events"

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func block(t *testing.T, text string) yaml.Node {
	t.Helper()
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(text), &n))
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		return *n.Content[0]
	}
	return n
}

func envelope(t *testing.T, guid string) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, guid)
	hdr.Set(event.HeaderEvent, "workflow_job")
	hdr.Set(event.HeaderHookID, "570000001")
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, "workflow_job.completed"), received)
	require.NoError(t, err)
	return env
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, 5*time.Millisecond, msg)
}

// fakeBus is a target bus for publisher tests: it records publishes and
// counts closes.
type fakeBus struct {
	topic string
	caps  bus.Capabilities
	err   error

	mu     sync.Mutex
	msgs   []*message.Message
	closed int
}

func (f *fakeBus) Publish(_ context.Context, msg *message.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, msg)
	return f.err
}

func (f *fakeBus) Subscribe(context.Context, string, bus.SubscribeOptions) (message.Subscriber, error) {
	return nil, errors.New("publisher-only")
}
func (f *fakeBus) Topic() string                  { return f.topic }
func (f *fakeBus) Capabilities() bus.Capabilities { return f.caps }
func (f *fakeBus) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeBus) published() []*message.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*message.Message(nil), f.msgs...)
}

// fakeDriver registers "fake" in a registry: every Open returns the same bus
// after a configurable number of failures.
type fakeDriver struct {
	bus   *fakeBus
	fail  atomic.Int32 // remaining opens that fail
	opens atomic.Int32
}

type fakeConfig struct {
	Token config.Secret `yaml:"token"`
	Level string        `yaml:"level"`
}

func (c fakeConfig) ValidateWith(config.Router) error {
	if c.Level == "bad" {
		return errors.New("level is bad")
	}
	return nil
}

func describeFake(raw yaml.Node) (any, error) {
	var c fakeConfig
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

func (d *fakeDriver) open(_ context.Context, raw yaml.Node, topic string, _ *slog.Logger, _ prometheus.Registerer) (bus.Bus, error) {
	if _, err := describeFake(raw); err != nil {
		return nil, err
	}
	d.opens.Add(1)
	if d.fail.Add(-1) >= 0 {
		return nil, errors.New("connection refused")
	}
	d.bus.topic = topic
	return d.bus, nil
}

func fakeRegistry(t *testing.T, failures int32) (*bus.Registry, *fakeDriver) {
	t.Helper()
	d := &fakeDriver{bus: &fakeBus{caps: bus.Capabilities{DurablePublish: true, FanOut: true}}}
	d.fail.Store(failures)
	reg := bus.NewRegistry()
	reg.Register("fake", bus.Driver{Factory: d.open, Describer: config.DescriberFunc(describeFake)})
	reg.Register(gochannel.Name, bus.Driver{Factory: gochannel.Open, Describer: config.DescriberFunc(gochannel.Describe)})
	return reg, d
}

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassForward), fwdbus.Name)
	d, ok := sink.Describers()[config.SinkKey("forward", "bus")]
	require.True(t, ok)

	typed, err := d.Describe(block(t, "driver: nats-jetstream\ntopic: github.events\nmax_hops: 2\nnats-jetstream:\n  embedded: false\n  url: nats://other:4222\n  credentials: hunter2\n"))
	require.NoError(t, err)
	cfg, ok := typed.(fwdbus.Config)
	require.True(t, ok)
	assert.Equal(t, "nats-jetstream", cfg.Driver)
	assert.Equal(t, "github.events", cfg.Topic)
	assert.Equal(t, 2, cfg.MaxHops)
	nats, ok := cfg.Drivers["nats-jetstream"].(natsjs.Config)
	require.True(t, ok, "the target block is described by the bus driver: %T", cfg.Drivers["nats-jetstream"])
	assert.Equal(t, "nats://other:4222", nats.URL)
	assert.False(t, nats.Embedded)
	assert.Equal(t, "ANTWATCHER", nats.Stream, "bus driver defaults apply to the target block")

	out, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "hunter2", "secrets in the target block mask themselves")
	assert.Contains(t, string(out), "driver: nats-jetstream\ntopic: github.events\nmax_hops: 2\nnats-jetstream:\n")
	assert.Contains(t, string(out), "  url: nats://other:4222\n")

	typed, err = d.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, fwdbus.DefaultConfig(), typed, "absent block yields defaults")
	assert.Equal(t, fwdbus.Config{MaxHops: 3}, fwdbus.DefaultConfig())
}

func TestDescribeWith(t *testing.T) {
	reg, _ := fakeRegistry(t, 0)

	t.Run("selected driver block described even when absent", func(t *testing.T) {
		typed, err := fwdbus.DescribeWith(reg, block(t, "driver: fake\ntopic: out"))
		require.NoError(t, err)
		cfg := typed.(fwdbus.Config) //nolint:errcheck // DescribeWith returns Config
		assert.Equal(t, fwdbus.Config{Driver: "fake", Topic: "out", MaxHops: 3, Drivers: map[string]any{"fake": fakeConfig{}}}, cfg)
	})

	t.Run("every present block is described", func(t *testing.T) {
		typed, err := fwdbus.DescribeWith(reg, block(t, "driver: fake\ntopic: out\nfake: {level: hi}\ngochannel: {}"))
		require.NoError(t, err)
		cfg := typed.(fwdbus.Config) //nolint:errcheck // DescribeWith returns Config
		assert.Equal(t, map[string]any{"fake": fakeConfig{Level: "hi"}, "gochannel": gochannel.Config{}}, cfg.Drivers)
	})

	t.Run("null block", func(t *testing.T) {
		typed, err := fwdbus.DescribeWith(reg, block(t, "~"))
		require.NoError(t, err)
		assert.Equal(t, fwdbus.DefaultConfig(), typed)
	})

	t.Run("null driver block uses defaults", func(t *testing.T) {
		typed, err := fwdbus.DescribeWith(reg, block(t, "driver: fake\ntopic: out\nfake:"))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"fake": fakeConfig{}}, typed.(fwdbus.Config).Drivers) //nolint:errcheck // DescribeWith returns Config
	})

	errs := map[string]struct{ text, want string }{
		"unknown scalar key":      {"driver: fake\ntopic: out\nbogus: 1", `unknown field "bogus"`},
		"unknown driver block":    {"driver: fake\ntopic: out\nkafka: {}", `unknown bus driver "kafka" (registered: fake, gochannel)`},
		"unknown selected driver": {"driver: kafka\ntopic: out", `unknown bus driver "kafka"`},
		"bad key inside block":    {"driver: fake\ntopic: out\nfake: {port: 1}", `fake: `},
		"wrong type":              {"driver: fake\ntopic: out\nmax_hops: many", "max_hops:"},
		"duplicate key":           {"driver: fake\ntopic: out\ntopic: other", `duplicate key "topic"`},
		"not a mapping":           {"- x", "config must be a mapping"},
	}
	for name, tc := range errs {
		t.Run(name, func(t *testing.T) {
			_, err := fwdbus.DescribeWith(reg, block(t, tc.text))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	ok := fwdbus.Config{Driver: "fake", Topic: "out", MaxHops: 1}
	require.NoError(t, ok.Validate())
	require.NoError(t, ok.ValidateWith(config.Router{}))

	err := fwdbus.Config{MaxHops: 0}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "driver is required")
	assert.Contains(t, err.Error(), "topic is required")
	assert.Contains(t, err.Error(), "max_hops must be >= 1 (got 0)")

	t.Run("target block cross-check", func(t *testing.T) {
		c := fwdbus.Config{Driver: "fake", Topic: "out", MaxHops: 3, Drivers: map[string]any{"fake": fakeConfig{Level: "bad"}}}
		err := c.ValidateWith(config.Router{})
		require.Error(t, err)
		assert.Equal(t, "fake: level is bad", err.Error())

		c.MaxHops = 0
		err = c.ValidateWith(config.Router{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_hops must be >= 1")
		assert.Contains(t, err.Error(), "fake: level is bad")
	})

	t.Run("nats target ack_wait rule applies", func(t *testing.T) {
		typed, err := fwdbus.Describe(block(t, "driver: nats-jetstream\ntopic: out\nnats-jetstream: {embedded: false, url: nats://x, ack_wait: 5s}"))
		require.NoError(t, err)
		err = typed.(config.DriverValidator).ValidateWith(config.Router{ProcessTimeout: 60 * time.Second}) //nolint:errcheck // Config implements it
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nats-jetstream: ack_wait (5s) must exceed")
	})
}

func TestConfig_ValidateIngress(t *testing.T) {
	ingress := config.Bus{Driver: "nats-jetstream", Topic: "antwatcher.events"}
	err := fwdbus.Config{Driver: "nats-jetstream", Topic: "antwatcher.events", MaxHops: 3}.ValidateIngress(ingress)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target (driver "nats-jetstream", topic "antwatcher.events") is the ingress bus`)
	assert.Contains(t, err.Error(), "loop")

	require.NoError(t, fwdbus.Config{Driver: "nats-jetstream", Topic: "github.events"}.ValidateIngress(ingress), "another topic")
	require.NoError(t, fwdbus.Config{Driver: "gochannel", Topic: "antwatcher.events"}.ValidateIngress(ingress), "another driver")
	require.NoError(t, fwdbus.Config{Driver: "nats-jetstream", Topic: "antwatcher.events"}.ValidateIngress(config.Bus{}), "no ingress known")
	require.NoError(t, fwdbus.Config{}.ValidateIngress(config.Bus{}), "empty everywhere is reported by Validate, not as a loop")
}

// TestValidateDrivers_LoopRejected proves the rule end to end through the
// config package with the real describers, as `-check` runs it.
func TestValidateDrivers_LoopRejected(t *testing.T) {
	busReg, _ := fakeRegistry(t, 0)
	sinkReg := sink.NewRegistry()
	sinkReg.RegisterDriver(sink.ClassForward, fwdbus.Name, fwdbus.DriverWith(busReg))
	describers := config.Describers{Bus: busReg.Describers(), Sinks: sinkReg.Describers()}

	text := "server:\n  webhook_secret: s\nbus:\n  driver: fake\n  topic: in\nsinks:\n  - name: fwd\n    class: forward\n    driver: bus\n    start_from: now\n    config: {driver: fake, topic: %s}\n"
	cfg, err := config.Parse(fmt.Appendf(nil, text, "in"))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	err = config.ValidateDrivers(cfg, describers)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `sinks[0] "fwd": target (driver "fake", topic "in") is the ingress bus`)

	cfg, err = config.Parse(fmt.Appendf(nil, text, "out"))
	require.NoError(t, err)
	require.NoError(t, config.ValidateDrivers(cfg, describers))

	redacted, err := config.Redacted(cfg, describers)
	require.NoError(t, err)
	assert.Equal(t, fwdbus.Config{Driver: "fake", Topic: "out", MaxHops: 3, Drivers: map[string]any{"fake": fakeConfig{}}}, redacted.Sinks[0].Config)
}

func TestFactory(t *testing.T) {
	reg, d := fakeRegistry(t, 0)
	deps := func() sink.Deps { return sink.Deps{Logger: slog.New(slog.DiscardHandler), BusDrivers: reg} }

	t.Run("builds without opening the target", func(t *testing.T) {
		s, err := fwdbus.Factory(context.Background(), "fwd", block(t, "driver: fake\ntopic: out\nmax_hops: 2"), deps())
		require.NoError(t, err)
		fs, ok := s.(*forward.Sink)
		require.True(t, ok)
		assert.Equal(t, "fwd", fs.Name())
		assert.Equal(t, sink.ClassForward, fs.Class())
		assert.Equal(t, "out", fs.Topic())
		assert.Equal(t, 2, fs.MaxHops())
		assert.Zero(t, d.opens.Load(), "the factory never opens the target")

		require.NoError(t, s.Process(context.Background(), envelope(t, "g1")))
		assert.Equal(t, int32(1), d.opens.Load())
		assert.Equal(t, "out", d.bus.topic)
		require.Len(t, d.bus.published(), 1)
		assert.Equal(t, forward.ForwardUUID("g1", "fwd", "out"), d.bus.published()[0].UUID)
		require.NoError(t, s.Close())
		assert.Equal(t, 1, d.bus.closed)
	})

	t.Run("default registry when deps carry none", func(t *testing.T) {
		s, err := fwdbus.Factory(context.Background(), "fwd", block(t, "driver: gochannel\ntopic: out"), sink.Deps{})
		require.NoError(t, err)
		require.NoError(t, s.Close())
	})

	t.Run("DriverWith binds the registry", func(t *testing.T) {
		drv := fwdbus.DriverWith(reg)
		s, err := drv.Factory(context.Background(), "fwd", block(t, "driver: fake\ntopic: out"), sink.Deps{})
		require.NoError(t, err)
		require.NoError(t, s.Close())
		_, err = drv.Describer.Describe(block(t, "driver: fake\ntopic: out"))
		require.NoError(t, err)
		_, err = fwdbus.Driver().Describer.Describe(block(t, "driver: fake\ntopic: out"))
		require.Error(t, err, "the default registry does not know the fake driver")
	})

	t.Run("loop against deps.Ingress", func(t *testing.T) {
		dp := deps()
		dp.Ingress = config.Bus{Driver: "fake", Topic: "in"}
		_, err := fwdbus.Factory(context.Background(), "fwd", block(t, "driver: fake\ntopic: in"), dp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is the ingress bus")
		_, err = fwdbus.Factory(context.Background(), "fwd", block(t, "driver: fake\ntopic: out"), dp)
		require.NoError(t, err)
	})

	errs := map[string]string{
		"decode":           "driver: fake\ntopic: out\nbogus: 1",
		"unknown driver":   "driver: kafka\ntopic: out",
		"missing topic":    "driver: fake",
		"missing driver":   "topic: out",
		"max_hops zero":    "driver: fake\ntopic: out\nmax_hops: 0",
		"bad target block": "driver: fake\ntopic: out\nfake: {port: 1}",
		"empty":            "",
		"not a mapping":    "- x",
	}
	for name, text := range errs {
		t.Run(name, func(t *testing.T) {
			s, err := fwdbus.Factory(context.Background(), "fwd", block(t, text), deps())
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}

	t.Run("through the sink registry", func(t *testing.T) {
		sinkReg := sink.NewRegistry()
		sinkReg.RegisterDriver(sink.ClassForward, fwdbus.Name, fwdbus.Driver())
		instances, err := sinkReg.Build(context.Background(), []config.SinkConfig{
			{Name: "fwd", Class: "forward", Driver: "bus", StartFrom: "now", Config: block(t, "driver: fake\ntopic: out")},
		}, deps())
		require.NoError(t, err)
		require.Len(t, instances, 1)
		assert.Equal(t, "bus", instances[0].Driver)
		require.NoError(t, sink.CloseAll(instances))
	})
}

func TestFactory_UnreachableTargetStartsDegraded(t *testing.T) {
	s, err := fwdbus.Factory(context.Background(), "fwd",
		block(t, "driver: nats-jetstream\ntopic: out\nnats-jetstream: {embedded: false, url: 'nats://127.0.0.1:1'}"),
		sink.Deps{Logger: slog.New(slog.DiscardHandler)})
	require.NoError(t, err, "an unreachable target never fails the build")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = s.Process(ctx, envelope(t, "g1"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), "unreachable is retryable: the bus redelivers")
	assert.Contains(t, err.Error(), "open target bus")
	require.NoError(t, s.Close())
}

func TestPublisher_LazyOpen(t *testing.T) {
	reg, d := fakeRegistry(t, 2)
	p := fwdbus.NewPublisher(reg, config.Bus{Driver: "fake", Topic: "out"}, nil)
	assert.False(t, p.Opened())

	msg := message.NewMessage("u1", []byte("{}"))
	for i := range 2 {
		err := p.Publish(context.Background(), msg)
		require.Error(t, err, "attempt %d", i)
		require.ErrorContains(t, err, "open target bus: ")
		require.ErrorContains(t, err, "connection refused")
		assert.False(t, sink.IsPermanent(err), "a failed open is retried by the broker")
		assert.False(t, p.Opened())
	}
	assert.Equal(t, int32(2), d.opens.Load())

	require.NoError(t, p.Publish(context.Background(), msg))
	assert.True(t, p.Opened())
	require.NoError(t, p.Publish(context.Background(), message.NewMessage("u2", []byte("{}"))))
	assert.Equal(t, int32(3), d.opens.Load(), "opened once, then reused")
	require.Len(t, d.bus.published(), 2)

	d.bus.err = errors.New("publish failed")
	err := p.Publish(context.Background(), msg)
	require.ErrorIs(t, err, d.bus.err)
	assert.Contains(t, err.Error(), "publish u1: ")
	d.bus.err = nil

	require.NoError(t, p.Close())
	assert.Equal(t, 1, d.bus.closed)
	assert.False(t, p.Opened())
	require.NoError(t, p.Close(), "idempotent")
	assert.Equal(t, 1, d.bus.closed)
	require.ErrorIs(t, p.Publish(context.Background(), msg), bus.ErrClosed)
	assert.Equal(t, int32(3), d.opens.Load(), "no reopen after close")
}

func TestPublisher_CloseBeforeOpenAndConcurrentOpen(t *testing.T) {
	reg, d := fakeRegistry(t, 0)
	p := fwdbus.NewPublisher(reg, config.Bus{Driver: "fake", Topic: "out"}, nil)
	require.NoError(t, p.Close())
	assert.Zero(t, d.opens.Load(), "closing an unopened publisher opens nothing")
	assert.Zero(t, d.bus.closed)

	reg, d = fakeRegistry(t, 0)
	p = fwdbus.NewPublisher(reg, config.Bus{Driver: "fake", Topic: "out"}, nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, p.Publish(context.Background(), message.NewMessage(string(rune('a'+i)), []byte("{}"))))
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), d.opens.Load(), "concurrent publishes share one open")
	assert.Len(t, d.bus.published(), 8)
	require.NoError(t, p.Close())
}

func TestPublisher_NonDurableTargetWarns(t *testing.T) {
	reg, d := fakeRegistry(t, 0)
	d.bus.caps = bus.Capabilities{FanOut: true}
	logs := &syncBuffer{}
	p := fwdbus.NewPublisher(reg, config.Bus{Driver: "fake", Topic: "out"}, slog.New(slog.NewTextHandler(logs, nil)))
	require.NoError(t, p.Publish(context.Background(), message.NewMessage("u", []byte("{}"))))
	assert.Contains(t, logs.String(), "forward target is not durable")
	assert.Contains(t, logs.String(), "forward target opened")
	require.NoError(t, p.Close())
}

// TestDriver_GochannelIngressIntoJetStream is the plan's integration test: a
// forward sink bound to a gochannel ingress publishes into a JetStream topic
// on an embedded server, and a reader on that topic gets the copies.
func TestDriver_GochannelIngressIntoJetStream(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A bus registry whose nats-jetstream driver connects to the test server
	// in process, with the described block deciding the stream name.
	busReg := bus.NewRegistry()
	busReg.Register(natsjs.Name, bus.Driver{
		Factory: func(ctx context.Context, raw yaml.Node, topic string, logger *slog.Logger, _ prometheus.Registerer) (bus.Bus, error) {
			v, err := natsjs.Describe(raw)
			if err != nil {
				return nil, err
			}
			cfg := ts.Config()
			cfg.Stream = v.(natsjs.Config).Stream //nolint:errcheck // Describe returns Config
			return natsjs.New(ctx, cfg, topic, logger, natsjs.WithInProcess(ts))
		},
		Describer: config.DescriberFunc(natsjs.Describe),
	})
	busReg.Register(gochannel.Name, bus.Driver{Factory: gochannel.Open, Describer: config.DescriberFunc(gochannel.Describe)})
	sinkReg := sink.NewRegistry()
	sinkReg.RegisterDriver(sink.ClassForward, fwdbus.Name, fwdbus.DriverWith(busReg))

	ingress := gochannel.New(ingressTopic, nil)
	t.Cleanup(func() { require.NoError(t, ingress.Close()) })
	m := metrics.New("v", "c")
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	instances, err := sinkReg.Build(ctx, []config.SinkConfig{{
		Name: "downstream", Class: "forward", Driver: "bus", StartFrom: "earliest",
		Config: block(t, "driver: nats-jetstream\ntopic: github.events\nmax_hops: 2\nnats-jetstream: {stream: DOWNSTREAM}"),
	}}, sink.Deps{Logger: logger, Metrics: m, BusDrivers: busReg, Ingress: config.Bus{Driver: gochannel.Name, Topic: ingressTopic}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sink.CloseAll(instances)) })

	router, err := sink.BuildRouter(ctx, ingress, config.Router{CloseTimeout: 5 * time.Second, ProcessTimeout: 10 * time.Second}, bus.ModeDegrade, instances, m, logger)
	require.NoError(t, err)
	done := make(chan error, 1)
	runCtx, stop := context.WithCancel(ctx)
	go func() { done <- router.Run(runCtx) }()
	t.Cleanup(func() {
		require.NoError(t, router.Close())
		stop()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("router did not stop")
		}
	})
	<-router.Running()

	ok := func() float64 {
		return testutil.ToFloat64(m.SinkEventsTotal.WithLabelValues("downstream", "forward", metrics.SinkOK))
	}
	first, second := envelope(t, "guid-1"), envelope(t, "guid-2")
	require.NoError(t, ingress.Publish(ctx, event.ToMessage(first)))
	require.NoError(t, ingress.Publish(ctx, event.ToMessage(second)))
	eventually(t, func() bool { return ok() == 2 }, "both events forwarded")
	assert.Contains(t, logs.String(), "forward target opened")

	// Read the copies back from the downstream stream.
	reader, err := natsjs.New(ctx, func() natsjs.Config { c := ts.Config(); c.Stream = "DOWNSTREAM"; return c }(), "github.events", nil, natsjs.WithInProcess(ts))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	sub, err := reader.Subscribe(ctx, "reader", bus.SubscribeOptions{StartFrom: bus.Earliest})
	require.NoError(t, err)
	msgs, err := sub.Subscribe(ctx, "github.events")
	require.NoError(t, err)

	got := map[string]*message.Message{}
	for len(got) < 2 {
		select {
		case msg := <-msgs:
			got[msg.Metadata[event.MetaDeliveryGUID]] = msg
			msg.Ack()
		case <-time.After(10 * time.Second):
			t.Fatalf("downstream copies not received; have %d", len(got))
		}
	}
	for _, env := range []event.Envelope{first, second} {
		msg := got[env.DeliveryGUID]
		require.NotNil(t, msg, env.DeliveryGUID)
		assert.Equal(t, forward.ForwardUUID(env.DeliveryGUID, "downstream", "github.events"), msg.UUID)
		assert.Equal(t, string(env.Payload), string(msg.Payload))
		assert.Equal(t, "downstream", msg.Metadata[event.MetaForwardedBy])
		assert.Equal(t, "1", msg.Metadata[event.MetaForwardHops])
		out, err := event.FromMessage(msg)
		require.NoError(t, err)
		want := env
		want.ForwardedBy, want.ForwardHops = "downstream", 1
		assert.Equal(t, want, out, "the copy decodes downstream as the same event")
	}

	// A redelivery of the same event produces the same copy, which the
	// target's dedup window collapses: no third message reaches the reader.
	require.NoError(t, ingress.Publish(ctx, event.ToMessage(first)))
	eventually(t, func() bool { return ok() == 3 }, "redelivery forwarded again")
	lag, err := reader.Lag(ctx, "reader")
	require.NoError(t, err)
	assert.Zero(t, lag, "the duplicate copy was collapsed by the target broker")
	select {
	case msg := <-msgs:
		t.Fatalf("unexpected third copy %s", msg.UUID)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, sub.Close())
}

// syncBuffer is a bytes.Buffer safe for the router's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

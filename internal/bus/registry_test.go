package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

// fakeBus records what the registry handed to its factory.
type fakeBus struct {
	topic string
	raw   yaml.Node
}

func (f *fakeBus) Publish(context.Context, *message.Message) error { return nil }
func (f *fakeBus) Subscribe(context.Context, string, SubscribeOptions) (message.Subscriber, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeBus) Topic() string              { return f.topic }
func (f *fakeBus) Capabilities() Capabilities { return Capabilities{} }
func (f *fakeBus) Close() error               { return nil }

type fakeConfig struct {
	URL   string        `yaml:"url"`
	Token config.Secret `yaml:"token"`
}

func fakeDriver(t *testing.T, opened *[]*fakeBus) Driver {
	t.Helper()
	return Driver{
		Factory: func(_ context.Context, raw yaml.Node, topic string, logger *slog.Logger, _ prometheus.Registerer) (Bus, error) {
			require.NotNil(t, logger, "logger is never nil")
			b := &fakeBus{topic: topic, raw: raw}
			*opened = append(*opened, b)
			return b, nil
		},
		Describer: config.DescriberFunc(func(raw yaml.Node) (any, error) {
			var c fakeConfig
			if err := config.DecodeStrict(raw, &c); err != nil {
				return nil, err
			}
			return c, nil
		}),
	}
}

func TestRegistry_RegisterAndOpen(t *testing.T) {
	r := NewRegistry()
	var opened []*fakeBus
	r.Register("fake", fakeDriver(t, &opened))
	r.Register("other", fakeDriver(t, &opened))
	assert.Equal(t, []string{"fake", "other"}, r.Names())

	cfg := parseBus(t, "driver: fake\ntopic: events\nfake:\n  url: nats://x\n  token: s3cret\nother:\n  url: unused\n")
	b, err := r.Open(context.Background(), cfg, nil, nil)
	require.NoError(t, err)
	require.Len(t, opened, 1, "only the selected driver is opened")
	assert.Same(t, opened[0], b)
	assert.Equal(t, "events", opened[0].topic)

	var got fakeConfig
	require.NoError(t, opened[0].raw.Decode(&got))
	assert.Equal(t, fakeConfig{URL: "nats://x", Token: "s3cret"}, got, "the selected driver block is passed through")
}

func TestRegistry_OpenWithoutDriverBlock(t *testing.T) {
	r := NewRegistry()
	var opened []*fakeBus
	r.Register("fake", fakeDriver(t, &opened))

	cfg := parseBus(t, "driver: fake\ntopic: events\n")
	_, err := r.Open(context.Background(), cfg, nil, nil)
	require.NoError(t, err)
	require.Len(t, opened, 1)
	assert.Equal(t, 0, int(opened[0].raw.Kind), "absent block is a zero node")
}

func TestRegistry_OpenUnknownDriverListsRegistered(t *testing.T) {
	r := NewRegistry()
	var opened []*fakeBus
	r.Register("beta", fakeDriver(t, &opened))
	r.Register("alpha", fakeDriver(t, &opened))

	_, err := r.Open(context.Background(), config.Bus{Driver: "kafka", Topic: "t"}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown bus driver "kafka"`)
	assert.Contains(t, err.Error(), "registered: alpha, beta")
	assert.Empty(t, opened)

	_, err = NewRegistry().Open(context.Background(), config.Bus{Driver: "kafka", Topic: "t"}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "registered: none")
}

func TestRegistry_OpenRequiresDriverAndTopic(t *testing.T) {
	r := NewRegistry()
	var opened []*fakeBus
	r.Register("fake", fakeDriver(t, &opened))

	_, err := r.Open(context.Background(), config.Bus{Topic: "t"}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bus.driver is required")

	_, err = r.Open(context.Background(), config.Bus{Driver: "fake"}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bus.topic is required")
	assert.Empty(t, opened)
}

func TestRegistry_OpenWrapsFactoryError(t *testing.T) {
	r := NewRegistry()
	boom := errors.New("boom")
	r.Register("broken", Driver{
		Factory: func(context.Context, yaml.Node, string, *slog.Logger, prometheus.Registerer) (Bus, error) {
			return nil, boom
		},
		Describer: config.DescriberFunc(func(yaml.Node) (any, error) { return nil, nil }),
	})
	_, err := r.Open(context.Background(), config.Bus{Driver: "broken", Topic: "t"}, nil, nil)
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), `open bus "broken"`)
}

func TestRegistry_LoggerCarriesDriverName(t *testing.T) {
	r := NewRegistry()
	var buf bytes.Buffer
	r.Register("fake", Driver{
		Factory: func(_ context.Context, _ yaml.Node, _ string, logger *slog.Logger, _ prometheus.Registerer) (Bus, error) {
			logger.Info("opened")
			return &fakeBus{}, nil
		},
		Describer: config.DescriberFunc(func(yaml.Node) (any, error) { return nil, nil }),
	})
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	_, err := r.Open(context.Background(), config.Bus{Driver: "fake", Topic: "t"}, logger, nil)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "bus=fake")
}

func TestRegistry_RegisterPanics(t *testing.T) {
	var opened []*fakeBus
	valid := fakeDriver(t, &opened)

	r := NewRegistry()
	r.Register("fake", valid)
	assert.PanicsWithValue(t, `bus: driver "fake" registered twice`, func() { r.Register("fake", valid) })
	assert.PanicsWithValue(t, "bus: Register with empty driver name", func() { r.Register("", valid) })
	assert.PanicsWithValue(t, `bus: Register("x") with nil Factory`, func() {
		r.Register("x", Driver{Describer: valid.Describer})
	})
	assert.PanicsWithValue(t, `bus: Register("x") with nil Describer`, func() {
		r.Register("x", Driver{Factory: valid.Factory})
	})
	assert.Equal(t, []string{"fake"}, r.Names(), "a panicking Register leaves the registry unchanged")
}

func TestRegistry_DescribersRedactSecrets(t *testing.T) {
	r := NewRegistry()
	var opened []*fakeBus
	r.Register("fake", fakeDriver(t, &opened))

	describers := config.Describers{Bus: r.Describers()}
	require.Contains(t, describers.Bus, "fake")

	cfg := parseBus(t, "driver: fake\ntopic: events\nfake:\n  url: nats://x\n  token: s3cret\n")
	red, err := config.Redacted(config.Config{Bus: cfg}, describers)
	require.NoError(t, err)
	out, err := yaml.Marshal(red.Bus)
	require.NoError(t, err)
	assert.Contains(t, string(out), "token: '***'")
	assert.NotContains(t, string(out), "s3cret")

	bad := parseBus(t, "driver: fake\ntopic: events\nfake:\n  urll: typo\n")
	require.Error(t, config.ValidateDrivers(config.Config{Bus: bad}, describers), "unknown keys are rejected through the Describer")
}

func TestDefaultRegistryFunctions(t *testing.T) {
	// Unique per run: DefaultRegistry is process-wide and rejects duplicates.
	name := fmt.Sprintf("default-registry-test-%d", time.Now().UnixNano())
	var opened []*fakeBus
	Register(name, fakeDriver(t, &opened))
	assert.Contains(t, Names(), name)
	assert.Contains(t, Describers(), name)

	b, err := Open(context.Background(), config.Bus{Driver: name, Topic: "t"}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "t", b.Topic())
}

func parseBus(t *testing.T, doc string) config.Bus {
	t.Helper()
	var b config.Bus
	require.NoError(t, yaml.Unmarshal([]byte(doc), &b))
	return b
}

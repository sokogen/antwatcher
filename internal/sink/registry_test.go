package sink_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/sink"
)

// fakeConfig is the driver block of the fake driver.
type fakeConfig struct {
	Endpoint string        `yaml:"endpoint"`
	Token    config.Secret `yaml:"token"`
}

func describeFake(raw yaml.Node) (any, error) {
	var c fakeConfig
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// fakeDriver registers a driver whose factory builds capture sinks and
// records the dependencies it was given.
type fakeDriver struct {
	class   sink.Class
	fail    error
	built   []*captureSink
	lastCfg fakeConfig
	deps    sink.Deps
}

func (d *fakeDriver) factory(_ context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
	typed, err := describeFake(raw)
	if err != nil {
		return nil, err
	}
	d.lastCfg = typed.(fakeConfig)
	d.deps = deps
	if d.fail != nil {
		return nil, d.fail
	}
	s := newCaptureSink(name, d.class)
	d.built = append(d.built, s)
	return s, nil
}

func registerFake(t *testing.T, r *sink.Registry, class sink.Class, name string) *fakeDriver {
	t.Helper()
	d := &fakeDriver{class: class}
	r.RegisterDriver(class, name, sink.Driver{Factory: d.factory, Describer: config.DescriberFunc(describeFake)})
	return d
}

func block(t *testing.T, text string) yaml.Node {
	t.Helper()
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(text), &n))
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		return *n.Content[0]
	}
	return n
}

func TestRegistry_RegisterDriverPanics(t *testing.T) {
	ok := sink.Driver{
		Factory:   func(context.Context, string, yaml.Node, sink.Deps) (sink.Sink, error) { return nil, nil },
		Describer: config.DescriberFunc(describeFake),
	}
	tests := []struct {
		name  string
		class sink.Class
		drv   string
		d     sink.Driver
		msg   string
	}{
		{"invalid class", sink.Class(0), "x", ok, "invalid class"},
		{"empty name", sink.ClassLog, "", ok, "empty driver name"},
		{"nil factory", sink.ClassLog, "x", sink.Driver{Describer: ok.Describer}, "nil Factory"},
		{"nil describer", sink.ClassLog, "x", sink.Driver{Factory: ok.Factory}, "nil Describer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := sink.NewRegistry()
			assert.Contains(t, panicValue(t, func() { r.RegisterDriver(tc.class, tc.drv, tc.d) }), tc.msg)
		})
	}

	t.Run("duplicate", func(t *testing.T) {
		r := sink.NewRegistry()
		r.RegisterDriver(sink.ClassLog, "x", ok)
		r.RegisterDriver(sink.ClassTrace, "x", ok) // same name in another class is fine
		assert.Contains(t, panicValue(t, func() { r.RegisterDriver(sink.ClassLog, "x", ok) }), "registered twice")
	})
}

func panicValue(t *testing.T, fn func()) (v string) {
	t.Helper()
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected a panic")
		v = r.(string)
	}()
	fn()
	return ""
}

func TestRegistry_DriversAndDescribers(t *testing.T) {
	r := sink.NewRegistry()
	registerFake(t, r, sink.ClassLog, "stdout")
	registerFake(t, r, sink.ClassLog, "otlp")
	registerFake(t, r, sink.ClassTrace, "otlp")

	assert.Equal(t, []string{"otlp", "stdout"}, r.Drivers(sink.ClassLog))
	assert.Equal(t, []string{"otlp"}, r.Drivers(sink.ClassTrace))
	assert.Empty(t, r.Drivers(sink.ClassArchive))

	ds := r.Describers()
	assert.Len(t, ds, 3)
	for _, key := range []string{config.SinkKey("log", "stdout"), config.SinkKey("log", "otlp"), config.SinkKey("trace", "otlp")} {
		require.Contains(t, ds, key)
	}
	typed, err := ds["log/otlp"].Describe(block(t, "endpoint: e\ntoken: s"))
	require.NoError(t, err)
	assert.Equal(t, fakeConfig{Endpoint: "e", Token: "s"}, typed)
}

func TestRegistry_Build(t *testing.T) {
	r := sink.NewRegistry()
	logDriver := registerFake(t, r, sink.ClassLog, "stdout")
	traceDriver := registerFake(t, r, sink.ClassTrace, "otlp")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	busReg := bus.NewRegistry()

	cfgs := []config.SinkConfig{
		{Name: "events", Class: "log", Driver: "stdout", StartFrom: "now"},
		{Name: "tempo", Class: "trace", Driver: "otlp", StartFrom: "earliest", Config: block(t, "endpoint: tempo:4317\ntoken: secret")},
	}
	instances, err := r.Build(context.Background(), cfgs, sink.Deps{Logger: logger, BusDrivers: busReg})
	require.NoError(t, err)
	require.Len(t, instances, 2)

	assert.Equal(t, "events", instances[0].Name())
	assert.Equal(t, sink.ClassLog, instances[0].Class())
	assert.Equal(t, bus.Now, instances[0].StartFrom)
	assert.Equal(t, "stdout", instances[0].Driver)
	assert.Equal(t, "tempo", instances[1].Name())
	assert.Equal(t, sink.ClassTrace, instances[1].Class())
	assert.Equal(t, bus.Earliest, instances[1].StartFrom)
	assert.Equal(t, "otlp", instances[1].Driver)

	assert.Equal(t, fakeConfig{Endpoint: "tempo:4317", Token: "secret"}, traceDriver.lastCfg)
	assert.Equal(t, fakeConfig{}, logDriver.lastCfg, "absent block leaves defaults")
	assert.Same(t, busReg, traceDriver.deps.BusDrivers)
	require.NotNil(t, traceDriver.deps.Logger)
	traceDriver.deps.Logger.Info("hello")
	assert.Contains(t, logs.String(), "sink=tempo")
	assert.Contains(t, logs.String(), "class=trace")
	assert.Contains(t, logs.String(), "driver=otlp")

	require.NoError(t, sink.CloseAll(instances))
	assert.True(t, logDriver.built[0].closed.Load())
	assert.True(t, traceDriver.built[0].closed.Load())
}

func TestRegistry_BuildNilLoggerIsFine(t *testing.T) {
	r := sink.NewRegistry()
	d := registerFake(t, r, sink.ClassLog, "stdout")
	_, err := r.Build(context.Background(), []config.SinkConfig{{Name: "a", Class: "log", Driver: "stdout", StartFrom: "now"}}, sink.Deps{})
	require.NoError(t, err)
	require.NotNil(t, d.deps.Logger)
}

func TestRegistry_BuildErrors(t *testing.T) {
	newReg := func(t *testing.T) (*sink.Registry, *fakeDriver) {
		t.Helper()
		r := sink.NewRegistry()
		registerFake(t, r, sink.ClassLog, "stdout")
		registerFake(t, r, sink.ClassLog, "otlp")
		d := registerFake(t, r, sink.ClassTrace, "otlp")
		return r, d
	}
	good := config.SinkConfig{Name: "ok", Class: "log", Driver: "stdout", StartFrom: "now"}

	tests := []struct {
		name string
		cfg  config.SinkConfig
		want []string
	}{
		{"unknown class", config.SinkConfig{Name: "a", Class: "metrics", Driver: "x", StartFrom: "now"},
			[]string{`sinks[1] "a"`, `unknown sink class "metrics"`, "trace, log, analytics, archive, forward"}},
		{"unknown driver lists registered", config.SinkConfig{Name: "a", Class: "log", Driver: "syslog", StartFrom: "now"},
			[]string{`unknown driver "syslog" for class log`, "registered: otlp, stdout"}},
		{"unknown driver in class without drivers", config.SinkConfig{Name: "a", Class: "archive", Driver: "s3", StartFrom: "now"},
			[]string{`unknown driver "s3" for class archive`, "registered: none"}},
		{"empty driver", config.SinkConfig{Name: "a", Class: "log", StartFrom: "now"},
			[]string{"driver is required for class log", "registered: otlp, stdout"}},
		{"invalid start_from", config.SinkConfig{Name: "a", Class: "log", Driver: "stdout", StartFrom: "yesterday"},
			[]string{"start_from", `"earliest" or "now"`}},
		{"missing start_from", config.SinkConfig{Name: "a", Class: "log", Driver: "stdout"},
			[]string{"start_from"}},
		{"invalid name", config.SinkConfig{Name: "bad name", Class: "log", Driver: "stdout", StartFrom: "now"},
			[]string{"invalid consumer name"}},
		{"empty name", config.SinkConfig{Class: "log", Driver: "stdout", StartFrom: "now"},
			[]string{"sinks[1]", "name is required"}},
		{"unknown config key rejected", config.SinkConfig{Name: "a", Class: "log", Driver: "stdout", StartFrom: "now", Config: block(t, "endpoint: e\nendpoitn: typo")},
			[]string{"driver log/stdout", "endpoitn", "not found"}},
		{"duplicate name", config.SinkConfig{Name: "ok", Class: "log", Driver: "otlp", StartFrom: "now"},
			[]string{`sinks[1] "ok"`, "duplicate sink name (also sinks[0])"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReg(t)
			instances, err := r.Build(context.Background(), []config.SinkConfig{good, tc.cfg}, sink.Deps{})
			require.Error(t, err)
			assert.Nil(t, instances)
			for _, want := range tc.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}

	t.Run("factory error closes earlier sinks", func(t *testing.T) {
		r, traceDriver := newReg(t)
		traceDriver.fail = errors.New("boom")
		stdout := &fakeDriver{class: sink.ClassLog}
		r.RegisterDriver(sink.ClassLog, "capture", sink.Driver{Factory: stdout.factory, Describer: config.DescriberFunc(describeFake)})
		_, err := r.Build(context.Background(), []config.SinkConfig{
			{Name: "first", Class: "log", Driver: "capture", StartFrom: "now"},
			{Name: "second", Class: "trace", Driver: "otlp", StartFrom: "now"},
		}, sink.Deps{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `sinks[1] "second": driver trace/otlp: boom`)
		require.Len(t, stdout.built, 1)
		assert.True(t, stdout.built[0].closed.Load(), "the sink built before the failure is closed")
	})

	t.Run("duplicate closes the duplicate", func(t *testing.T) {
		r, _ := newReg(t)
		capture := &fakeDriver{class: sink.ClassLog}
		r.RegisterDriver(sink.ClassLog, "capture", sink.Driver{Factory: capture.factory, Describer: config.DescriberFunc(describeFake)})
		_, err := r.Build(context.Background(), []config.SinkConfig{
			{Name: "same", Class: "log", Driver: "capture", StartFrom: "now"},
			{Name: "same", Class: "log", Driver: "capture", StartFrom: "now"},
		}, sink.Deps{})
		require.Error(t, err)
		require.Len(t, capture.built, 2)
		assert.True(t, capture.built[0].closed.Load())
		assert.True(t, capture.built[1].closed.Load())
	})

	t.Run("nil sink from factory", func(t *testing.T) {
		r := sink.NewRegistry()
		r.RegisterDriver(sink.ClassLog, "nil", sink.Driver{
			Factory:   func(context.Context, string, yaml.Node, sink.Deps) (sink.Sink, error) { return nil, nil },
			Describer: config.DescriberFunc(describeFake),
		})
		_, err := r.Build(context.Background(), []config.SinkConfig{{Name: "a", Class: "log", Driver: "nil", StartFrom: "now"}}, sink.Deps{})
		require.ErrorContains(t, err, "returned no sink")
	})

	t.Run("sink identity mismatch", func(t *testing.T) {
		r := sink.NewRegistry()
		var built *captureSink
		r.RegisterDriver(sink.ClassLog, "liar", sink.Driver{
			Factory: func(_ context.Context, _ string, _ yaml.Node, _ sink.Deps) (sink.Sink, error) {
				built = newCaptureSink("other", sink.ClassTrace)
				return built, nil
			},
			Describer: config.DescriberFunc(describeFake),
		})
		_, err := r.Build(context.Background(), []config.SinkConfig{{Name: "a", Class: "log", Driver: "liar", StartFrom: "now"}}, sink.Deps{})
		require.ErrorContains(t, err, `built sink trace/"other" instead of log/"a"`)
		assert.True(t, built.closed.Load())
	})
}

func TestDefaultRegistryFunctions(t *testing.T) {
	// The default registry is shared by the process; use a unique class/name
	// pair and only assert what the wrappers forward.
	const name = "registry-test-driver"
	if !contains(sink.Drivers(sink.ClassForward), name) {
		d := &fakeDriver{class: sink.ClassForward}
		sink.RegisterDriver(sink.ClassForward, name, sink.Driver{Factory: d.factory, Describer: config.DescriberFunc(describeFake)})
	}
	assert.Contains(t, sink.Drivers(sink.ClassForward), name)
	assert.Contains(t, sink.Describers(), config.SinkKey("forward", name))
	instances, err := sink.Build(context.Background(), []config.SinkConfig{{Name: "fwd", Class: "forward", Driver: name, StartFrom: "now"}}, sink.Deps{})
	require.NoError(t, err)
	require.Len(t, instances, 1)
	require.NoError(t, sink.CloseAll(instances))
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

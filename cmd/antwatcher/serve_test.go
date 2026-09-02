package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/receiver"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	"github.com/sokogen/antwatcher/internal/sink/archive/drivers/filesystem"
	"github.com/sokogen/antwatcher/internal/sink/forward"
	"github.com/sokogen/antwatcher/internal/sink/log"
	"github.com/sokogen/antwatcher/internal/sink/trace"
	traceotlp "github.com/sokogen/antwatcher/internal/sink/trace/drivers/otlp"
)

const (
	testSecret = "e2e-webhook-secret"
	testGUID   = "1f4d2c3e-e2e0-4a11-9c2b-0123456789ab"
)

// captureStore holds every capturing destination by sink name. It outlives a
// service so a restart with the same sink names keeps counting into the
// same buckets, which is how the tests see duplicates. It also records the
// lifecycle: every stage reported by the wiring, every sink Close, and the
// start of every Process call.
type captureStore struct {
	delay time.Duration // artificial Process latency, set before the service starts

	mu       sync.Mutex
	spans    map[string]int                // trace sink -> spans exported
	exports  map[string]int                // trace sink -> export calls
	logs     map[string]int                // log sink -> records
	records  map[string]int                // analytics sink -> records
	schemas  map[string]int                // analytics sink -> EnsureSchema calls
	forwards map[string][]*message.Message // forward sink -> published copies
	events   []string                      // ordered lifecycle log
	hook     func(string)                  // called with every lifecycle event, outside the lock
}

func newCaptureStore() *captureStore {
	return &captureStore{
		spans: map[string]int{}, exports: map[string]int{}, logs: map[string]int{},
		records: map[string]int{}, schemas: map[string]int{}, forwards: map[string][]*message.Message{},
	}
}

func (c *captureStore) note(ev string) {
	c.mu.Lock()
	c.events = append(c.events, ev)
	hook := c.hook
	c.mu.Unlock()
	if hook != nil {
		hook(ev)
	}
}

func (c *captureStore) setHook(fn func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hook = fn
}

// snapshot returns the lifecycle events, without the process markers when
// stagesOnly is set.
func (c *captureStore) snapshot(stagesOnly bool) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.events))
	for _, ev := range c.events {
		if stagesOnly && strings.HasPrefix(ev, "process:") {
			continue
		}
		out = append(out, ev)
	}
	return out
}

func (c *captureStore) has(ev string) bool {
	for _, got := range c.snapshot(false) {
		if got == ev {
			return true
		}
	}
	return false
}

func (c *captureStore) count(m map[string]int, name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return m[name]
}

func (c *captureStore) forwardsOf(name string) []*message.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*message.Message(nil), c.forwards[name]...)
}

// processing marks the start of a Process call and applies the configured
// latency.
func (c *captureStore) processing(ctx context.Context, name string) error {
	c.note("process:" + name)
	if c.delay == 0 {
		return nil
	}
	select {
	case <-time.After(c.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type captureExporter struct {
	c    *captureStore
	name string
}

func (e *captureExporter) ExportSpans(ctx context.Context, spans []otlp.Span) error {
	if err := e.c.processing(ctx, e.name); err != nil {
		return err
	}
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	e.c.spans[e.name] += len(spans)
	e.c.exports[e.name]++
	return nil
}

func (e *captureExporter) Close() error { e.c.note("close:" + e.name); return nil }

type captureLogWriter struct {
	c    *captureStore
	name string
}

func (w *captureLogWriter) Write(ctx context.Context, _ otlp.LogRecord) error {
	if err := w.c.processing(ctx, w.name); err != nil {
		return err
	}
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	w.c.logs[w.name]++
	return nil
}

func (w *captureLogWriter) Close() error { w.c.note("close:" + w.name); return nil }

type captureAnalyticsWriter struct {
	c    *captureStore
	name string
}

func (w *captureAnalyticsWriter) EnsureSchema(context.Context, analytics.Schema) error {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	w.c.schemas[w.name]++
	return nil
}

func (w *captureAnalyticsWriter) Write(ctx context.Context, records []analytics.Record) error {
	if err := w.c.processing(ctx, w.name); err != nil {
		return err
	}
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	w.c.records[w.name] += len(records)
	return nil
}

func (w *captureAnalyticsWriter) Close() error { w.c.note("close:" + w.name); return nil }

type capturePublisher struct {
	c    *captureStore
	name string
}

func (p *capturePublisher) Publish(ctx context.Context, msg *message.Message) error {
	if err := p.c.processing(ctx, p.name); err != nil {
		return err
	}
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	p.c.forwards[p.name] = append(p.c.forwards[p.name], msg)
	return nil
}

func (p *capturePublisher) Close() error { p.c.note("close:" + p.name); return nil }

// registry builds a sink registry with a "capture" driver for every class
// that has an external destination, the real filesystem archive driver, and
// the real OTLP trace driver (for the degraded-destination test).
func (c *captureStore) registry() *sink.Registry {
	noConfig := config.DescriberFunc(func(yaml.Node) (any, error) { return struct{}{}, nil })
	r := sink.NewRegistry()
	r.RegisterDriver(sink.ClassTrace, "capture", sink.Driver{
		Factory: func(_ context.Context, name string, _ yaml.Node, _ sink.Deps) (sink.Sink, error) {
			return trace.New(name, &captureExporter{c: c, name: name}), nil
		},
		Describer: noConfig,
	})
	r.RegisterDriver(sink.ClassLog, "capture", sink.Driver{
		Factory: func(_ context.Context, name string, _ yaml.Node, _ sink.Deps) (sink.Sink, error) {
			return log.New(name, &captureLogWriter{c: c, name: name}), nil
		},
		Describer: noConfig,
	})
	r.RegisterDriver(sink.ClassAnalytics, "capture", sink.Driver{
		Factory: func(_ context.Context, name string, _ yaml.Node, _ sink.Deps) (sink.Sink, error) {
			return analytics.New(name, &captureAnalyticsWriter{c: c, name: name}, true), nil
		},
		Describer: noConfig,
	})
	r.RegisterDriver(sink.ClassForward, "capture", sink.Driver{
		Factory: func(_ context.Context, name string, _ yaml.Node, _ sink.Deps) (sink.Sink, error) {
			return forward.New(name, &capturePublisher{c: c, name: name}, "github.events", 3), nil
		},
		Describer: noConfig,
	})
	r.RegisterDriver(sink.ClassArchive, filesystem.Name, filesystem.Driver())
	r.RegisterDriver(sink.ClassTrace, traceotlp.Name, traceotlp.Driver())
	return r
}

func (c *captureStore) options() serveOptions {
	return serveOptions{
		busDrivers:  bus.DefaultRegistry,
		sinkDrivers: c.registry(),
		version:     "test",
		commit:      "abc",
		stage:       c.note,
	}
}

// Configuration snippets. Listeners use distinct hosts on port 0 because
// validation refuses the same host:port for both.
const listeners = `
server:
  listen: "localhost:0"
  webhook_path: /webhook
  webhook_secret: ` + testSecret + `
  publish_timeout: 2s
admin:
  listen: "127.0.0.1:0"
  lag_interval: 100ms
router:
  close_timeout: 5s
  process_timeout: 1s
`

const gochannelBus = `
bus:
  driver: gochannel
  topic: antwatcher.events
  on_missing_capability: degrade
`

const oneLogSink = "sinks:\n  - {name: events, class: log, driver: capture, start_from: now}\n"

func natsBus(dir string) string {
	return fmt.Sprintf(`
bus:
  driver: nats-jetstream
  topic: antwatcher.events
  on_missing_capability: fail
  nats-jetstream:
    embedded: true
    store_dir: %q
    credentials: super-secret-nats-creds
    retention: 1h
    dedup_window: 1m
    ack_wait: 12s
    nak_delay_min: 50ms
    nak_delay_max: 500ms
`, dir)
}

func parseConfig(t *testing.T, text string) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(text))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	return cfg
}

// running is a service started by startService.
type running struct {
	svc     *service
	cancel  context.CancelFunc
	stopped chan struct{} // closed when run returned
	err     error         // run's result, valid after stopped
	logs    *syncWriter
}

// wait blocks until run returned and reports its result.
func (r *running) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.stopped:
		return r.err
	case <-time.After(30 * time.Second):
		t.Fatalf("service did not stop; logs:\n%s", r.logs.String())
		return nil
	}
}

func (r *running) webhookURL() string {
	return "http://" + r.svc.receiver.Addr().String() + "/webhook"
}

func (r *running) adminURL(path string) string {
	return "http://" + r.svc.admin.Addr().String() + path
}

// stop triggers the signal path and waits for run to return.
func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.wait(t)
}

// launch runs svc under ctx and returns the handle tests wait on.
func launch(t *testing.T, svc *service, ctx context.Context, cancel context.CancelFunc, logs *syncWriter) *running {
	t.Helper()
	r := &running{svc: svc, cancel: cancel, stopped: make(chan struct{}), logs: logs}
	go func() {
		r.err = svc.run(ctx)
		close(r.stopped)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.stopped:
		case <-time.After(30 * time.Second):
			t.Errorf("service did not stop at cleanup")
		}
	})
	return r
}

func startService(t *testing.T, cfg config.Config, opts serveOptions) *running {
	t.Helper()
	logs := newSyncWriter()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, cancel := context.WithCancel(context.Background())
	svc, err := newService(ctx, cfg, logger, opts)
	if err != nil {
		cancel()
		t.Fatalf("newService: %v\nlogs:\n%s", err, logs.String())
	}
	r := launch(t, svc, ctx, cancel, logs)
	waitReady(t, r)
	return r
}

// syncWriter serializes log writes from the service goroutines with the
// test's reads.
type syncWriter struct {
	mu sync.Mutex
	w  bytes.Buffer
}

func newSyncWriter() *syncWriter { return &syncWriter{} }

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.String()
}

func waitReady(t *testing.T, r *running) {
	t.Helper()
	eventually(t, func() bool { return probe(r.adminURL("/readyz")) == http.StatusOK }, "service ready")
	// The receiver serves only after the router runs; wait for its listener
	// to answer instead of racing it.
	eventually(t, func() bool { return probe("http://"+r.svc.receiver.Addr().String()+"/healthz") == http.StatusOK }, "receiver serving")
}

// probe returns the status code of a GET, or 0 when the request fails.
func probe(url string) int {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// postWebhook signs body like GitHub and posts it as a workflow_job delivery.
func postWebhook(t *testing.T, url, guid string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(event.HeaderDelivery, guid)
	req.Header.Set(event.HeaderEvent, "workflow_job")
	req.Header.Set(event.HeaderHookID, "570000001")
	req.Header.Set(receiver.HeaderSignature, receiver.Sign(testSecret, body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

func getBody(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(out)
}

// statusDoc is the part of the /status document the tests assert on, with
// enums as strings so the JSON decodes without the production types.
type statusDoc struct {
	Version        string `json:"version"`
	Ready          bool   `json:"ready"`
	NotReadyReason string `json:"not_ready_reason"`
	Bus            struct {
		Driver       string `json:"driver"`
		Capabilities struct {
			DurablePublish bool   `json:"durable_publish"`
			Retention      string `json:"retention"`
		} `json:"capabilities"`
		Connected *bool          `json:"connected"`
		Config    map[string]any `json:"config"`
		Warnings  []struct {
			Capability string `json:"capability"`
			Severity   string `json:"severity"`
		} `json:"warnings"`
	} `json:"bus"`
	Router map[string]any `json:"router"`
	Sinks  []struct {
		Name               string     `json:"name"`
		Class              string     `json:"class"`
		Driver             string     `json:"driver"`
		RequestedStartFrom string     `json:"requested_start_from"`
		EffectiveStartFrom string     `json:"effective_start_from"`
		LastSuccess        *time.Time `json:"last_success"`
		Stalled            []any      `json:"stalled"`
	} `json:"sinks"`
	Recovery struct {
		Enabled  bool   `json:"enabled"`
		AuthType string `json:"auth_type"`
		Targets  []struct {
			Target         string `json:"target"`
			HookID         int64  `json:"hook_id"`
			Degraded       bool   `json:"degraded"`
			DegradedReason string `json:"degraded_reason"`
		} `json:"targets"`
	} `json:"recovery"`
}

func getStatus(t *testing.T, r *running) (statusDoc, string) {
	t.Helper()
	code, body := getBody(t, r.adminURL("/status"))
	require.Equal(t, http.StatusOK, code, body)
	var st statusDoc
	require.NoError(t, json.Unmarshal([]byte(body), &st), body)
	return st, body
}

// metricValue reads one sample from /metrics by its full series text.
func metricValue(t *testing.T, r *running, series string) (float64, bool) {
	t.Helper()
	_, body := getBody(t, r.adminURL("/metrics"))
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, series+" ") {
			var v float64
			_, err := fmt.Sscanf(strings.TrimPrefix(line, series+" "), "%g", &v)
			require.NoError(t, err, line)
			return v, true
		}
	}
	return 0, false
}

func TestServe_EndToEnd(t *testing.T) {
	store := newCaptureStore()
	storeDir := t.TempDir()
	archiveDir := t.TempDir()
	sinksYAML := fmt.Sprintf(`
sinks:
  - {name: tempo, class: trace, driver: capture, start_from: earliest}
  - {name: events, class: log, driver: capture, start_from: earliest}
  - {name: bq, class: analytics, driver: capture, start_from: earliest}
  - {name: raw, class: archive, driver: filesystem, start_from: earliest, config: {dir: %q, compress: none}}
  - {name: downstream, class: forward, driver: capture, start_from: earliest}
`, archiveDir)
	cfg := parseConfig(t, listeners+natsBus(storeDir)+sinksYAML)
	body := event.LoadFixture(t, "workflow_job.completed")

	// First life: one delivery reaches every class.
	r := startService(t, cfg, store.options())
	code, resp := postWebhook(t, r.webhookURL(), testGUID, body)
	require.Equal(t, http.StatusOK, code, resp)
	assert.Contains(t, resp, testGUID)

	eventually(t, func() bool {
		return store.count(store.exports, "tempo") == 1 && store.count(store.logs, "events") == 1 &&
			store.count(store.records, "bq") > 0 && len(store.forwardsOf("downstream")) == 1
	}, "every capturing sink processed the delivery")
	assert.Positive(t, store.count(store.spans, "tempo"), "a completed job yields spans")
	assert.Equal(t, 1, store.count(store.schemas, "bq"), "schema ensured once before the first write")
	fwd := store.forwardsOf("downstream")[0]
	assert.Equal(t, testGUID, fwd.Metadata.Get(event.MetaDeliveryGUID), "forwarded copy keeps the delivery GUID")
	assert.NotEqual(t, testGUID, fwd.UUID, "forwarded copy travels under a new transport UUID")
	assert.Equal(t, "1", fwd.Metadata.Get(event.MetaForwardHops))
	assert.Equal(t, string(body), string(fwd.Payload), "payload forwarded byte for byte")
	eventually(t, func() bool {
		v, ok := metricValue(t, r, `antwatcher_sink_events_total{class="archive",result="ok",sink="raw"}`)
		return ok && v == 1
	}, "archive sink acked")

	st, raw := getStatus(t, r)
	assert.Equal(t, "test", st.Version)
	assert.True(t, st.Ready)
	assert.Equal(t, "nats-jetstream", st.Bus.Driver)
	assert.True(t, st.Bus.Capabilities.DurablePublish)
	assert.Equal(t, "time", st.Bus.Capabilities.Retention)
	assert.Empty(t, st.Bus.Warnings)
	require.NotNil(t, st.Bus.Connected)
	assert.True(t, *st.Bus.Connected)
	assert.Equal(t, "1h0m0s", st.Bus.Config["retention"], "the driver block shows the retention window")
	assert.Equal(t, "***", st.Bus.Config["credentials"])
	assert.Equal(t, "1s", st.Router["process_timeout"])
	require.Len(t, st.Sinks, 5)
	for _, s := range st.Sinks {
		assert.Equal(t, "earliest", s.RequestedStartFrom, s.Name)
		assert.Equal(t, "earliest", s.EffectiveStartFrom, s.Name)
		assert.Empty(t, s.Stalled, s.Name)
		assert.NotNil(t, s.LastSuccess, s.Name)
	}
	assert.Equal(t, "filesystem", st.Sinks[3].Driver)
	assert.Equal(t, "archive", st.Sinks[3].Class)
	assert.False(t, st.Recovery.Enabled)
	for _, secret := range []string{testSecret, "super-secret-nats-creds"} {
		assert.NotContains(t, raw, secret)
	}
	eventually(t, func() bool {
		v, ok := metricValue(t, r, `antwatcher_bus_consumer_lag{consumer="sink-tempo"}`)
		return ok && v == 0
	}, "lag exported and drained")
	v, ok := metricValue(t, r, "antwatcher_bus_connected")
	assert.True(t, ok)
	assert.InDelta(t, 1, v, 0)

	require.NoError(t, r.stop(t))
	assertArchived(t, archiveDir, testGUID)

	// Second life on the same store: acked messages are not redelivered, a
	// sink added later with earliest receives the history.
	lateYAML := "  - {name: late, class: log, driver: capture, start_from: earliest}\n"
	cfg2 := parseConfig(t, listeners+natsBus(storeDir)+sinksYAML+lateYAML)
	r2 := startService(t, cfg2, store.options())
	eventually(t, func() bool { return store.count(store.logs, "late") == 1 }, "late sink replays history")
	time.Sleep(300 * time.Millisecond) // give any wrongful redelivery a chance to show
	assert.Equal(t, 1, store.count(store.exports, "tempo"), "trace sink saw nothing twice after restart")
	assert.Equal(t, 1, store.count(store.logs, "events"), "log sink saw nothing twice after restart")
	assert.Len(t, store.forwardsOf("downstream"), 1, "forward sink saw nothing twice after restart")

	// A second delivery in the second life reaches old and new sinks once.
	code, _ = postWebhook(t, r2.webhookURL(), "second-"+testGUID, body)
	require.Equal(t, http.StatusOK, code)
	eventually(t, func() bool {
		return store.count(store.logs, "late") == 2 && store.count(store.logs, "events") == 2 && store.count(store.exports, "tempo") == 2
	}, "second delivery processed once per sink")
	st2, _ := getStatus(t, r2)
	require.Len(t, st2.Sinks, 6)
	assert.Equal(t, "late", st2.Sinks[5].Name)
	require.NoError(t, r2.stop(t))
}

// assertArchived finds guid in one of the JSONL files under dir.
func assertArchived(t *testing.T, dir, guid string) {
	t.Helper()
	found := false
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(guid)) {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found, "archive under %s contains delivery %s", dir, guid)
}

func TestServe_ShutdownOrder(t *testing.T) {
	store := newCaptureStore()
	store.delay = 300 * time.Millisecond
	cfg := parseConfig(t, listeners+gochannelBus+`
sinks:
  - {name: tempo, class: trace, driver: capture, start_from: now}
  - {name: events, class: log, driver: capture, start_from: now}
`)
	r := startService(t, cfg, store.options())
	body := event.LoadFixture(t, "workflow_job.completed")
	code, _ := postWebhook(t, r.webhookURL(), testGUID, body)
	require.Equal(t, http.StatusOK, code)
	eventually(t, func() bool { return store.has("process:tempo") && store.has("process:events") }, "both sinks in flight")

	// Stop while Process is still sleeping: the in-flight handlers finish
	// before the sinks close, and nothing runs twice.
	receiverAddr := r.svc.receiver.Addr().String()
	adminAddr := r.svc.admin.Addr().String()
	require.NoError(t, r.stop(t))
	assert.Equal(t, 1, store.count(store.exports, "tempo"))
	assert.Equal(t, 1, store.count(store.logs, "events"))

	want := []string{
		stageBusOpen, stageAdminStart, stageRouterStart, stageReceiverOpen,
		stageReceiverStop, stageRouterClose, stageSinksClose, "close:tempo", "close:events", stageBusClose, stageAdminStop,
	}
	assert.Equal(t, want, store.snapshot(true))

	for _, addr := range []string{receiverAddr, adminAddr} {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
		}
		require.Error(t, err, "listener %s released", addr)
	}
	assert.Contains(t, r.logs.String(), "antwatcher stopped")
}

func TestServe_ReadinessDuringShutdown(t *testing.T) {
	store := newCaptureStore()
	cfg := parseConfig(t, listeners+gochannelBus+oneLogSink)
	r := startService(t, cfg, store.options())
	st, _ := getStatus(t, r)
	assert.True(t, st.Ready)
	require.Len(t, st.Bus.Warnings, 1, "non-durable bus in degrade mode warns")
	assert.Equal(t, "DurablePublish", st.Bus.Warnings[0].Capability)
	assert.Equal(t, "warning", st.Bus.Warnings[0].Severity)
	assert.Nil(t, st.Bus.Connected, "gochannel reports no connection state")
	assert.Contains(t, r.logs.String(), "2xx does not mean durable")

	// While the router is closing, the admin listener still answers and
	// reports the shutdown.
	// The hook runs on the service goroutine, so it only records; the
	// assertions happen on the test goroutine afterwards.
	var readyCode int
	var readyBody, statusBody string
	var hookErr error
	observed := make(chan struct{})
	store.setHook(func(ev string) {
		if ev != stageRouterClose {
			return
		}
		defer close(observed)
		readyCode, readyBody, hookErr = fetch(r.adminURL("/readyz"))
		if hookErr != nil {
			return
		}
		_, statusBody, hookErr = fetch(r.adminURL("/status"))
	})
	require.NoError(t, r.stop(t))
	<-observed
	require.NoError(t, hookErr, "admin listener answers while the router closes")
	assert.Equal(t, http.StatusServiceUnavailable, readyCode)
	assert.Contains(t, readyBody, "shutting down")
	var statusDuring statusDoc
	require.NoError(t, json.Unmarshal([]byte(statusBody), &statusDuring), statusBody)
	assert.False(t, statusDuring.Ready)
	assert.Equal(t, "shutting down", statusDuring.NotReadyReason)
}

// fetch GETs url without test assertions, for use off the test goroutine.
func fetch(url string) (int, string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out), err
}

func TestServe_StartupFailures(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
		// closed lists the sinks that must have been closed on the way out.
		closed []string
	}{
		{
			name: "unknown bus driver",
			yaml: listeners + "bus: {driver: kafka, topic: t, on_missing_capability: fail}\nsinks: []\n",
			want: `unknown bus driver "kafka"`,
		},
		{
			name: "non-durable bus in fail mode",
			yaml: listeners + "bus: {driver: gochannel, topic: t, on_missing_capability: fail}\nsinks: []\n",
			want: "DurablePublish",
		},
		{
			name:   "unknown sink driver",
			yaml:   listeners + gochannelBus + oneLogSink + "  - {name: x, class: log, driver: nope, start_from: now}\n",
			want:   `unknown driver "nope" for class log`,
			closed: []string{"close:events"},
		},
		{
			name: "sink constructor rejects its config",
			yaml: listeners + gochannelBus + "sinks:\n  - {name: x, class: archive, driver: filesystem, start_from: now, config: {dir: /tmp/x, bogus: 1}}\n",
			want: "bogus",
		},
		{
			name:   "recovery target cannot be discovered",
			yaml:   listeners + gochannelBus + oneLogSink + "recovery:\n  enabled: true\n  auth: {type: token, token: ghp_x}\n  targets: [{repo: o/r}]\n",
			want:   "hook_id is not set and server.public_url is empty",
			closed: []string{"close:events"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newCaptureStore()
			cfg := parseConfig(t, tc.yaml)
			svc, err := newService(context.Background(), cfg, nil, store.options())
			require.Error(t, err)
			assert.Nil(t, svc)
			assert.Contains(t, err.Error(), tc.want)
			var closed []string
			for _, ev := range store.snapshot(true) {
				if strings.HasPrefix(ev, "close:") {
					closed = append(closed, ev)
				}
			}
			assert.Equal(t, tc.closed, closed, "sinks built before the failure are closed")
		})
	}
}

func TestServe_StartupFailure_PortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	store := newCaptureStore()
	cfg := parseConfig(t, strings.Replace(listeners, `"127.0.0.1:0"`, fmt.Sprintf("%q", ln.Addr().String()), 1)+gochannelBus+oneLogSink)
	_, err = newService(context.Background(), cfg, nil, store.options())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin: listen")
	assert.Equal(t, []string{stageBusOpen, "close:events"}, store.snapshot(true), "what was built is closed")
}

func TestServe_DegradedDestinationsDoNotFailStartup(t *testing.T) {
	// A GitHub API that fails every call.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	}))
	defer api.Close()
	// A port that refuses connections for the OTLP exporter.
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	refusedAddr := refused.Addr().String()
	require.NoError(t, refused.Close())

	store := newCaptureStore()
	cfg := parseConfig(t, listeners+gochannelBus+fmt.Sprintf(`
sinks:
  - name: tempo
    class: trace
    driver: otlp
    start_from: now
    config: {endpoint: %q, protocol: grpc, insecure: true, timeout: 300ms, retry: {attempts: 1, backoff: 10ms}, headers: {Authorization: "Bearer otlp-secret-token"}}
  - {name: events, class: log, driver: capture, start_from: now}
recovery:
  enabled: true
  auth: {type: token, token: ghp_recovery_secret}
  api_base_url: %q
  interval: 1h
  targets: [{repo: octo/repo, hook_id: 42}, {org: octo, hook_id: 43}]
`, refusedAddr, api.URL))
	r := startService(t, cfg, store.options())

	body := event.LoadFixture(t, "workflow_job.completed")
	code, _ := postWebhook(t, r.webhookURL(), testGUID, body)
	assert.Equal(t, http.StatusOK, code, "ingress serves while destinations are down")
	eventually(t, func() bool { return store.count(store.logs, "events") == 1 }, "healthy sink processes")
	eventually(t, func() bool {
		v, ok := metricValue(t, r, `antwatcher_sink_events_total{class="trace",result="error",sink="tempo"}`)
		return ok && v >= 1
	}, "trace sink degraded with retryable errors")
	eventually(t, func() bool {
		st, _ := getStatus(t, r)
		return len(st.Recovery.Targets) == 2 && st.Recovery.Targets[0].Degraded && st.Recovery.Targets[1].Degraded
	}, "recovery targets degraded")

	st, raw := getStatus(t, r)
	assert.True(t, st.Ready)
	assert.True(t, st.Recovery.Enabled)
	assert.Equal(t, "token", st.Recovery.AuthType)
	assert.Equal(t, "octo/repo", st.Recovery.Targets[0].Target)
	assert.Equal(t, int64(42), st.Recovery.Targets[0].HookID)
	assert.Contains(t, st.Recovery.Targets[0].DegradedReason, "list deliveries")
	require.Len(t, st.Sinks, 2)
	assert.Equal(t, "tempo", st.Sinks[0].Name)
	assert.Nil(t, st.Sinks[0].LastSuccess, "no success yet on the unreachable destination")
	assert.NotNil(t, st.Sinks[1].LastSuccess)
	for _, secret := range []string{testSecret, "ghp_recovery_secret", "otlp-secret-token"} {
		assert.NotContains(t, raw, secret)
	}
	v, ok := metricValue(t, r, `antwatcher_recovery_degraded{target="octo/repo"}`)
	assert.True(t, ok)
	assert.InDelta(t, 1, v, 0)
	require.NoError(t, r.stop(t))
}

func TestServe_ComponentFailureShutsDown(t *testing.T) {
	store := newCaptureStore()
	cfg := parseConfig(t, listeners+gochannelBus+oneLogSink)
	r := startService(t, cfg, store.options())

	// Kill the receiver's listener underneath it: its Run fails, and run
	// must shut everything else down in order and report the cause.
	require.NoError(t, r.svc.receiver.Close())
	err := r.wait(t)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "receiver: serve")
	assert.Contains(t, r.logs.String(), "component failed")
	assert.Equal(t, []string{
		stageBusOpen, stageAdminStart, stageRouterStart, stageReceiverOpen,
		stageReceiverStop, stageRouterClose, stageSinksClose, "close:events", stageBusClose, stageAdminStop,
	}, store.snapshot(true))
}

// TestRunServe drives the real CLI entry point with the process seams: the
// service starts from a file, is observed through serveStarted, and stops
// on the root context like it would on SIGTERM.
func TestRunServe(t *testing.T) {
	store := newCaptureStore()
	path := filepath.Join(t.TempDir(), "antwatcher.yml")
	require.NoError(t, os.WriteFile(path, []byte(listeners+gochannelBus+oneLogSink), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan *service, 1)
	serveBaseContext = func() context.Context { return ctx }
	serveOptionsFn = store.options
	serveStarted = func(s *service) { started <- s }
	t.Cleanup(func() {
		serveBaseContext = context.Background
		serveOptionsFn = defaultServeOptions
		serveStarted = nil
	})

	var stdout bytes.Buffer
	stderr := newSyncWriter()
	done := make(chan int, 1)
	go func() { done <- run([]string{"serve", "-config", path, "-log-format", "text"}, &stdout, stderr) }()
	var svc *service
	select {
	case svc = <-started:
	case <-time.After(30 * time.Second):
		t.Fatalf("service did not start; logs:\n%s", stderr.String())
	}
	r := &running{svc: svc, cancel: cancel, stopped: make(chan struct{}), logs: stderr}
	waitReady(t, r)
	code, _ := postWebhook(t, r.webhookURL(), testGUID, event.LoadFixture(t, "workflow_job.completed"))
	assert.Equal(t, http.StatusOK, code)
	eventually(t, func() bool { return store.count(store.logs, "events") == 1 }, "delivery processed")

	cancel()
	select {
	case exit := <-done:
		assert.Equal(t, 0, exit, stderr.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("serve did not exit; logs:\n%s", stderr.String())
	}
	logs := stderr.String()
	assert.Contains(t, logs, "configuration loaded")
	assert.Contains(t, logs, "antwatcher started")
	assert.Contains(t, logs, "antwatcher stopped")
	assert.Contains(t, logs, "***")
	assert.NotContains(t, logs, testSecret)
	assert.Empty(t, stdout.String())
}

func TestRunServe_StartupFailureExits1(t *testing.T) {
	store := newCaptureStore()
	path := filepath.Join(t.TempDir(), "antwatcher.yml")
	// Valid static config whose bus refuses the ingress policy.
	require.NoError(t, os.WriteFile(path, []byte(listeners+"bus: {driver: gochannel, topic: t, on_missing_capability: fail}\nsinks: []\n"), 0o600))
	serveOptionsFn = store.options
	t.Cleanup(func() { serveOptionsFn = defaultServeOptions })

	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "-config", path, "-log-format", "text"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "startup failed")
	assert.Contains(t, stderr.String(), "DurablePublish")
}

func TestReadable(t *testing.T) {
	assert.Nil(t, readable(nil))
	got := readable(config.Router{CloseTimeout: 30 * time.Second, ProcessTimeout: time.Minute})
	assert.Equal(t, map[string]any{"close_timeout": "30s", "process_timeout": "1m0s"}, got)
	type withSecret struct {
		Token config.Secret `yaml:"token"`
	}
	assert.Equal(t, map[string]any{"token": "***"}, readable(withSecret{Token: "hunter2"}))
	bad := readable(unrenderable{})
	assert.Equal(t, map[string]string{"error": "render: no"}, bad)
}

// unrenderable fails YAML rendering.
type unrenderable struct{}

func (unrenderable) MarshalYAML() (any, error) { return nil, errors.New("no") }

package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
)

const (
	secret = "top-secret"
	topic  = "antwatcher.events"
	guid   = "72d3162e-cc78-11e3-81ab-4c9367dc0958"
)

// stubBus records Publish calls and delegates to a programmable func.
type stubBus struct {
	publish func(ctx context.Context, msg *message.Message) error

	mu    sync.Mutex
	calls []*message.Message
}

func (s *stubBus) Publish(ctx context.Context, msg *message.Message) error {
	s.mu.Lock()
	s.calls = append(s.calls, msg)
	s.mu.Unlock()
	if s.publish == nil {
		return nil
	}
	return s.publish(ctx, msg)
}

func (s *stubBus) Subscribe(context.Context, string, bus.SubscribeOptions) (message.Subscriber, error) {
	return nil, errors.New("not implemented")
}
func (s *stubBus) Topic() string                  { return topic }
func (s *stubBus) Capabilities() bus.Capabilities { return bus.Capabilities{FanOut: true} }
func (s *stubBus) Close() error                   { return nil }

func (s *stubBus) published() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func testConfig() config.Server {
	cfg := config.Default().Server
	cfg.WebhookSecret = secret
	cfg.PublishTimeout = 200 * time.Millisecond
	return cfg
}

// post builds a signed POST to the webhook path; sign=false omits the header.
func post(cfg config.Server, eventName string, body []byte, sign bool) *http.Request {
	r := httptest.NewRequest(http.MethodPost, cfg.WebhookPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(event.HeaderDelivery, guid)
	r.Header.Set(event.HeaderEvent, eventName)
	r.Header.Set(event.HeaderHookID, "570000001")
	if sign {
		r.Header.Set(HeaderSignature, Sign(secret, body))
	}
	return r
}

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func counter(t *testing.T, m *metrics.Metrics, eventName, result string) float64 {
	t.Helper()
	c, err := m.WebhooksTotal.GetMetricWithLabelValues(eventName, result)
	require.NoError(t, err)
	return testutil.ToFloat64(c)
}

func bodyJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())
	return out
}

func TestSign_MatchesGitHubFormat(t *testing.T) {
	// Example from GitHub's "Validating webhook deliveries" documentation.
	got := Sign("It's a Secret to Everybody", []byte("Hello, World!"))
	assert.Equal(t, "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17", got)
	assert.True(t, verifySignature("It's a Secret to Everybody", got, []byte("Hello, World!")))
	assert.False(t, verifySignature("It's a Secret to Everybody", got, []byte("Hello, World")))
	assert.False(t, verifySignature("", got, []byte("Hello, World!")), "empty secret never verifies")
	assert.False(t, verifySignature("s", "sha256=zz", []byte("x")), "non-hex digest")
	assert.False(t, verifySignature("s", "sha1=abcd", []byte("x")), "wrong algorithm prefix")
}

func TestWebhook_ValidSignaturePublishesToBus(t *testing.T) {
	b := gochannel.New(topic, nil)
	defer func() { _ = b.Close() }()
	sub, err := b.Subscribe(context.Background(), "sink-test", bus.SubscribeOptions{StartFrom: bus.Earliest})
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	m := metrics.New("v", "c")
	cfg := testConfig()
	h := New(cfg, b, m, nil)
	body := event.LoadFixture(t, "workflow_job.completed")

	rec := serve(h, post(cfg, "workflow_job", body, true))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, map[string]any{"delivery": guid}, bodyJSON(t, rec))

	select {
	case msg := <-ch:
		msg.Ack()
		assert.Equal(t, guid, msg.UUID)
		assert.Equal(t, body, []byte(msg.Payload), "payload byte-for-byte")
		env, err := event.FromMessage(msg)
		require.NoError(t, err)
		assert.Equal(t, "workflow_job", env.Event)
		assert.Equal(t, "completed", env.Action)
		assert.Equal(t, "570000001", env.HookID)
		assert.NotEmpty(t, env.Repository)
		assert.WithinDuration(t, time.Now(), env.ReceivedAt, 5*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("message not published")
	}

	assert.InDelta(t, 1, counter(t, m, "workflow_job", metrics.WebhookPublished), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.WebhooksInflight), 0)
	cnt, err := testutil.GatherAndCount(m.Registry, "antwatcher_webhook_publish_seconds")
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "publish latency observed")
}

func TestWebhook_SignatureRejectedWithoutPublish(t *testing.T) {
	body := event.LoadFixture(t, "workflow_run.completed")
	cfg := testConfig()

	cases := map[string]func(r *http.Request){
		"missing":       func(r *http.Request) { r.Header.Del(HeaderSignature) },
		"wrong secret":  func(r *http.Request) { r.Header.Set(HeaderSignature, Sign("other", body)) },
		"tampered body": func(r *http.Request) { r.Header.Set(HeaderSignature, Sign(secret, []byte(`{"x":1}`))) },
		"not hex":       func(r *http.Request) { r.Header.Set(HeaderSignature, "sha256=nothex") },
		"sha1 only":     func(r *http.Request) { r.Header.Set(HeaderSignature, "sha1=deadbeef") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			sb := &stubBus{}
			m := metrics.New("v", "c")
			h := New(cfg, sb, m, nil)
			r := post(cfg, "workflow_run", body, true)
			mutate(r)
			rec := serve(h, r)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Contains(t, bodyJSON(t, rec)["error"], HeaderSignature)
			assert.Equal(t, 0, sb.published(), "nothing published")
			assert.InDelta(t, 1, counter(t, m, unknownEvent, metrics.WebhookRejectedSignature), 0,
				"unauthenticated requests never label metrics with their event header")
			assert.InDelta(t, 0, counter(t, m, "workflow_run", metrics.WebhookRejectedSignature), 0)
		})
	}
}

func TestWebhook_PingAnswersWithoutPublish(t *testing.T) {
	sb := &stubBus{}
	m := metrics.New("v", "c")
	cfg := testConfig()
	h := New(cfg, sb, m, nil)

	rec := serve(h, post(cfg, EventPing, event.LoadFixture(t, "ping"), true))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, map[string]any{"delivery": guid, "ping": true}, bodyJSON(t, rec))
	assert.Equal(t, 0, sb.published())
	assert.InDelta(t, 1, counter(t, m, EventPing, metrics.WebhookPing), 0)

	t.Run("ping still needs a valid signature", func(t *testing.T) {
		rec := serve(h, post(cfg, EventPing, event.LoadFixture(t, "ping"), false))
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Equal(t, 0, sb.published())
	})
}

func TestWebhook_OversizeBodyIs413(t *testing.T) {
	sb := &stubBus{}
	m := metrics.New("v", "c")
	cfg := testConfig()
	cfg.MaxBodyBytes = 64
	h := New(cfg, sb, m, nil)

	body := event.LoadFixture(t, "workflow_job.queued")
	require.Greater(t, len(body), 64)
	rec := serve(h, post(cfg, "workflow_job", body, true))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Contains(t, bodyJSON(t, rec)["error"], "max_body_bytes")
	assert.Equal(t, 0, sb.published())
	assert.InDelta(t, 1, counter(t, m, unknownEvent, metrics.WebhookBadRequest), 0)

	small := []byte(`{"action":"queued"}`)
	rec = serve(h, post(cfg, "workflow_job", small, true))
	assert.Equal(t, http.StatusOK, rec.Code, "a body at or under the limit is accepted")
	assert.Equal(t, 1, sb.published())
}

func TestWebhook_BadRequests(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name    string
		event   string
		body    string
		mutate  func(r *http.Request)
		label   string
		errPart string
	}{
		{"invalid json", "workflow_run", `{"action": "completed"`, nil, "workflow_run", "JSON"},
		{"not an object", "workflow_run", `[1,2]`, nil, "workflow_run", "JSON object"},
		{"empty body", "workflow_run", ``, nil, "workflow_run", "JSON object"},
		{"missing delivery header", "workflow_run", `{"action":"x"}`,
			func(r *http.Request) { r.Header.Del(event.HeaderDelivery) }, "workflow_run", event.HeaderDelivery},
		{"missing event header", "", `{"action":"x"}`,
			func(r *http.Request) { r.Header.Del(event.HeaderEvent) }, unknownEvent, event.HeaderEvent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &stubBus{}
			m := metrics.New("v", "c")
			h := New(cfg, sb, m, nil)
			r := post(cfg, tc.event, []byte(tc.body), true)
			if tc.mutate != nil {
				tc.mutate(r)
			}
			rec := serve(h, r)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, bodyJSON(t, rec)["error"], tc.errPart)
			assert.Equal(t, 0, sb.published())
			assert.InDelta(t, 1, counter(t, m, tc.label, metrics.WebhookBadRequest), 0)
		})
	}
}

func TestWebhook_PublishErrorIs503WithGUID(t *testing.T) {
	sb := &stubBus{publish: func(context.Context, *message.Message) error { return errors.New("broker unavailable") }}
	m := metrics.New("v", "c")
	cfg := testConfig()
	h := New(cfg, sb, m, nil)

	rec := serve(h, post(cfg, "workflow_job", event.LoadFixture(t, "workflow_job.in_progress"), true))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	got := bodyJSON(t, rec)
	assert.Equal(t, guid, got["delivery"], "GUID in the body so GitHub's delivery log can be matched")
	assert.Equal(t, "publish failed", got["error"], "the driver's error text stays out of GitHub's delivery log")
	assert.NotContains(t, rec.Body.String(), "broker unavailable")
	assert.Equal(t, 1, sb.published())
	assert.InDelta(t, 1, counter(t, m, "workflow_job", metrics.WebhookPublishFailed), 0)
	assert.InDelta(t, 0, counter(t, m, "workflow_job", metrics.WebhookPublishTimeout), 0)
}

func TestWebhook_SlowPublishTimesOutWithin503(t *testing.T) {
	released := make(chan struct{})
	sb := &stubBus{publish: func(ctx context.Context, _ *message.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-released:
			return nil
		}
	}}
	m := metrics.New("v", "c")
	cfg := testConfig()
	cfg.PublishTimeout = 100 * time.Millisecond
	h := New(cfg, sb, m, nil)

	start := time.Now()
	rec := serve(h, post(cfg, "workflow_run", event.LoadFixture(t, "workflow_run.requested"), true))
	elapsed := time.Since(start)
	close(released)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, guid, bodyJSON(t, rec)["delivery"])
	assert.GreaterOrEqual(t, elapsed, cfg.PublishTimeout, "the timeout was honored")
	assert.Less(t, elapsed, 2*time.Second, "the response is written promptly after the timeout")
	assert.InDelta(t, 1, counter(t, m, "workflow_run", metrics.WebhookPublishTimeout), 0)
	assert.InDelta(t, 0, counter(t, m, "workflow_run", metrics.WebhookPublishFailed), 0)
}

// orderRecorder captures whether Publish had returned when the status code
// was written.
type orderRecorder struct {
	*httptest.ResponseRecorder
	publishReturned *atomic.Bool
	sawPublished    bool
}

func (o *orderRecorder) WriteHeader(code int) {
	o.sawPublished = o.publishReturned.Load()
	o.ResponseRecorder.WriteHeader(code)
}

func (o *orderRecorder) Write(b []byte) (int, error) {
	if o.Code == 0 {
		o.WriteHeader(http.StatusOK)
	}
	return o.ResponseRecorder.Write(b)
}

func TestWebhook_StatusWrittenOnlyAfterPublishReturned(t *testing.T) {
	var returned atomic.Bool
	release := make(chan struct{})
	entered := make(chan struct{})
	sb := &stubBus{publish: func(ctx context.Context, _ *message.Message) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		returned.Store(true)
		return nil
	}}
	m := metrics.New("v", "c")
	cfg := testConfig()
	cfg.PublishTimeout = 5 * time.Second
	h := New(cfg, sb, m, nil)

	rec := &orderRecorder{ResponseRecorder: httptest.NewRecorder(), publishReturned: &returned}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, post(cfg, "workflow_job", event.LoadFixture(t, "workflow_job.completed"), true))
	}()

	<-entered
	assert.Eventually(t, func() bool { return testutil.ToFloat64(m.WebhooksInflight) == 1 }, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("response written while Publish was still blocked")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, rec.sawPublished, "status code written before Publish returned nil")
	assert.InDelta(t, 0, testutil.ToFloat64(m.WebhooksInflight), 0)
}

func TestWebhook_CustomPath(t *testing.T) {
	cfg := testConfig()
	cfg.WebhookPath = "/gh/hooks"
	sb := &stubBus{}
	h := New(cfg, sb, metrics.New("v", "c"), nil)
	body := []byte(`{"action":"completed"}`)
	rec := serve(h, post(cfg, "workflow_run", body, true))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, sb.published())

	rec = serve(h, httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body)))
	assert.Equal(t, http.StatusNotFound, rec.Code, "the default path is not mounted when another is configured")
}

func TestHandler_ListenerServesOnlyWebhookAndHealthz(t *testing.T) {
	cfg := testConfig()
	sb := &stubBus{}
	h := New(cfg, sb, metrics.New("v", "c"), nil)

	rec := serve(h, httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())

	for _, path := range []string{"/status", "/metrics", "/readyz", "/"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			rec := serve(h, httptest.NewRequest(method, path, strings.NewReader("{}")))
			assert.Equal(t, http.StatusNotFound, rec.Code, "%s %s", method, path)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodHead} {
		rec := serve(h, httptest.NewRequest(method, cfg.WebhookPath, http.NoBody))
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code, method)
		assert.Contains(t, rec.Header().Get("Allow"), http.MethodPost)
	}
	assert.Equal(t, http.StatusMethodNotAllowed, serve(h, httptest.NewRequest(http.MethodPost, "/healthz", http.NoBody)).Code)
	assert.Equal(t, 0, sb.published())
}

func TestWebhook_LogsDeliveryFields(t *testing.T) {
	var logs bytes.Buffer
	logger := slogText(&logs)
	cfg := testConfig()
	h := New(cfg, &stubBus{}, metrics.New("v", "c"), logger)
	serve(h, post(cfg, "workflow_job", event.LoadFixture(t, "workflow_job.completed"), true))
	out := logs.String()
	assert.Contains(t, out, "webhook published")
	assert.Contains(t, out, "delivery="+guid)
	assert.Contains(t, out, "event=workflow_job")
	assert.Contains(t, out, "action=completed")
	assert.Contains(t, out, "repository=")
	assert.Contains(t, out, "latency_ms=")
	assert.NotContains(t, out, secret, "the secret is never logged")

	logs.Reset()
	serve(h, post(cfg, "workflow_job", event.LoadFixture(t, "workflow_job.completed"), false))
	assert.Contains(t, logs.String(), "signature")
	assert.NotContains(t, logs.String(), secret)
}

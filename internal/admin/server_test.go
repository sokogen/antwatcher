package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/metrics"
)

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, http.NoBody))
	return rec
}

func TestHandler_Endpoints(t *testing.T) {
	m := metrics.New("1.0.0", "cafe")
	var ready atomic.Bool
	h := Handler(Options{
		Metrics: m.Handler(),
		Readiness: func() error {
			if ready.Load() {
				return nil
			}
			return errors.New("bus not open")
		},
		Status: func() any { return map[string]any{"version": "1.0.0", "sinks": []string{"a"}} },
	})

	rec := get(t, h, http.MethodGet, "/metrics")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `antwatcher_build_info{commit="cafe",version="1.0.0"} 1`)

	rec = get(t, h, http.MethodGet, "/healthz")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	rec = get(t, h, http.MethodGet, "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"ready":false,"reason":"bus not open"}`, rec.Body.String())

	ready.Store(true)
	rec = get(t, h, http.MethodGet, "/readyz")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"ready":true}`, rec.Body.String())

	rec = get(t, h, http.MethodGet, "/status")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"version":"1.0.0","sinks":["a"]}`, rec.Body.String())

	// method and path restrictions
	assert.Equal(t, http.StatusMethodNotAllowed, get(t, h, http.MethodPost, "/status").Code)
	assert.Equal(t, http.StatusNotFound, get(t, h, http.MethodGet, "/webhook").Code)
	assert.Equal(t, http.StatusNotFound, get(t, h, http.MethodPost, "/webhook").Code)
}

func TestHandler_Defaults(t *testing.T) {
	h := Handler(Options{})
	assert.Equal(t, http.StatusOK, get(t, h, http.MethodGet, "/readyz").Code, "nil readiness means ready")
	rec := get(t, h, http.MethodGet, "/status")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{}`, rec.Body.String(), "nil status provider renders an empty object")
	assert.Equal(t, http.StatusNotFound, get(t, h, http.MethodGet, "/metrics").Code, "no metrics handler configured")
}

func TestHandler_StatusRenderError(t *testing.T) {
	h := Handler(Options{Status: func() any { return map[string]any{"bad": make(chan int)} }})
	rec := get(t, h, http.MethodGet, "/status")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "render status")
}

func TestServer_RunAndShutdown(t *testing.T) {
	m := metrics.New("2.0.0", "beef")
	s := New(Options{
		Listen:    "127.0.0.1:0",
		Metrics:   m.Handler(),
		Readiness: func() error { return nil },
		Status:    func() any { return map[string]string{"state": "running"} },
	})
	assert.Nil(t, s.Addr(), "no address before Listen")
	require.NoError(t, s.Listen())
	require.NoError(t, s.Listen(), "Listen is idempotent")
	addr := s.Addr()
	require.NotNil(t, addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	base := "http://" + addr.String()
	client := &http.Client{Timeout: 2 * time.Second}

	var body map[string]string
	fetchJSON(t, client, base+"/status", http.StatusOK, &body)
	assert.Equal(t, "running", body["state"])

	resp, err := client.Get(base + "/metrics")
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(raw), "antwatcher_build_info")

	resp, err = client.Get(base + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after ctx cancel")
	}
	_, err = client.Get(base + "/healthz")
	require.Error(t, err, "listener closed after shutdown")
}

func TestServer_ListenError(t *testing.T) {
	first := New(Options{Listen: "127.0.0.1:0"})
	require.NoError(t, first.Listen())
	second := New(Options{Listen: first.Addr().String()})
	err := second.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin: listen")
}

func fetchJSON(t *testing.T, client *http.Client, url string, wantCode int, into any) {
	t.Helper()
	resp, err := client.Get(url) //nolint:gosec // test URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, wantCode, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(into))
}

package receiver

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
)

func slogText(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestServer_RoundTripAndShutdown(t *testing.T) {
	cfg := testConfig()
	cfg.Listen = "127.0.0.1:0"
	sb := &stubBus{}
	var logs bytes.Buffer
	srv := NewServer(cfg, New(cfg, sb, metrics.New("v", "c"), nil), slogText(&logs))
	require.Nil(t, srv.Addr(), "no address before Listen")
	require.NoError(t, srv.Listen())
	require.NoError(t, srv.Listen(), "idempotent")
	addr := srv.Addr()
	require.NotNil(t, addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	base := "http://" + addr.String()
	body := event.LoadFixture(t, "workflow_run.completed")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+cfg.WebhookPath, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set(event.HeaderDelivery, guid)
	req.Header.Set(event.HeaderEvent, "workflow_run")
	req.Header.Set(HeaderSignature, Sign(secret, body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, string(got))
	assert.Equal(t, 1, sb.published())

	hz, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	_ = hz.Body.Close()
	assert.Equal(t, http.StatusOK, hz.StatusCode)

	st, err := http.Get(base + "/status")
	require.NoError(t, err)
	_ = st.Body.Close()
	assert.Equal(t, http.StatusNotFound, st.StatusCode, "/status is not served on the webhook listener")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop")
	}
	assert.Contains(t, logs.String(), "webhook server listening")
	assert.Contains(t, logs.String(), "webhook server stopped")

	_, err = net.DialTimeout("tcp", addr.String(), 200*time.Millisecond)
	assert.Error(t, err, "listener closed after shutdown")
}

func TestServer_Timeouts(t *testing.T) {
	cfg := testConfig()
	cfg.PublishTimeout = 8 * time.Second
	srv := NewServer(cfg, http.NotFoundHandler(), nil)
	assert.Positive(t, srv.http.ReadHeaderTimeout)
	assert.Positive(t, srv.http.ReadTimeout)
	assert.Positive(t, srv.http.IdleTimeout)
	assert.Greater(t, srv.http.WriteTimeout, srv.http.ReadTimeout+cfg.PublishTimeout,
		"a full body read plus one publish must fit in the write timeout")
}

func TestRun_BindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	cfg := testConfig()
	cfg.Listen = ln.Addr().String()
	err = Run(context.Background(), cfg, http.NotFoundHandler(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "receiver: listen")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	cfg := testConfig()
	cfg.Listen = "127.0.0.1:0"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, http.NotFoundHandler(), nil) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestServer_CloseFailsRun(t *testing.T) {
	cfg := testConfig()
	cfg.Listen = "127.0.0.1:0"
	s := NewServer(cfg, http.NotFoundHandler(), nil)
	require.NoError(t, s.Close(), "Close before Listen is a no-op")
	require.NoError(t, s.Listen())
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, s.Close())
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "receiver: serve")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Close")
	}
}

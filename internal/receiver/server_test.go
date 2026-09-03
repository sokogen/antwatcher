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

// TestServer_ShutdownWaitsForSlowInFlightRequest pins the regression this
// budget change fixes: the shutdown context must give an in-flight request
// at least as long as the server's own WriteTimeout, not just
// PublishTimeout+2s. The handler sleeps longer than the old budget
// (PublishTimeout+2s = 2.2s here) but well under the new one
// (WriteTimeout+2s, tens of seconds), so a regression back to the old
// formula would make this test fail with a shutdown error.
func TestServer_ShutdownWaitsForSlowInFlightRequest(t *testing.T) {
	cfg := testConfig()
	cfg.Listen = "127.0.0.1:0"

	started := make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	})
	srv := NewServer(cfg, slow, nil)
	require.NoError(t, srv.Listen())
	addr := srv.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(ctx) }()

	type result struct {
		resp *http.Response
		err  error
	}
	respDone := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + addr.String()) //nolint:gosec // test URL
		respDone <- result{resp, err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	cancel() // begin shutdown while the slow request is in flight

	select {
	case err := <-runDone:
		require.NoError(t, err, "shutdown must wait for the in-flight request under the enlarged budget")
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	res := <-respDone
	require.NoError(t, res.err)
	assert.Equal(t, http.StatusOK, res.resp.StatusCode, "the slow request completed instead of being cut off")
	_ = res.resp.Body.Close()
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

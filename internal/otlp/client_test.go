package otlp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/metrics"
	"github.com/sokogen/antwatcher/internal/sink"
)

// harness bundles what every client test needs: a client with a recording
// sleep, captured logs, and metrics.
type harness struct {
	client *Client
	logs   *bytes.Buffer
	m      *metrics.Metrics
	mu     sync.Mutex
	delays []time.Duration
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{logs: &bytes.Buffer{}, m: metrics.New("test", "abc")}
	logger := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, err := New(cfg, Options{Logger: logger, Metrics: h.m, Version: "1.0.0"})
	require.NoError(t, err)
	c.sleep = func(ctx context.Context, d time.Duration) error {
		h.mu.Lock()
		h.delays = append(h.delays, d)
		h.mu.Unlock()
		return ctx.Err()
	}
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	h.client = c
	return h
}

func (h *harness) sleeps() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.delays...)
}

func (h *harness) rejected(signal string) float64 {
	return testutil.ToFloat64(h.m.OTLPRejectedTotal.WithLabelValues(signal))
}

func grpcConfig(addr string) Config {
	c := DefaultConfig()
	c.Endpoint = addr
	c.Insecure = true
	c.Retry.Backoff = time.Millisecond
	return c
}

func httpConfig(url string) Config {
	c := DefaultConfig()
	c.Endpoint = url
	c.Protocol = ProtocolHTTP
	c.Retry.Backoff = time.Millisecond
	return c
}

func sampleSpans() []Span {
	return []Span{{
		TraceID: testTraceID, SpanID: testSpanID, ParentSpanID: testParent, Name: "job:build",
		Start: testStart, End: testEnd, Attributes: map[string]any{"github.job_id": int64(7)},
		Status: Status{Code: StatusOk},
	}}
}

func sampleRecords() []LogRecord {
	return []LogRecord{{
		Time: testStart, Severity: SeverityInfo, SeverityText: "INFO", Body: "workflow_run completed: ci",
		TraceID: testTraceID, SpanID: testSpanID, Attributes: map[string]any{"github.event": "workflow_run"},
	}}
}

func partialTrace(rejected int64, msg string) func(int) (*coltracepb.ExportTraceServiceResponse, error) {
	return func(int) (*coltracepb.ExportTraceServiceResponse, error) {
		return &coltracepb.ExportTraceServiceResponse{
			PartialSuccess: &coltracepb.ExportTracePartialSuccess{RejectedSpans: rejected, ErrorMessage: msg},
		}, nil
	}
}

// --- construction -----------------------------------------------------------

func TestNew_InvalidConfig(t *testing.T) {
	_, err := New(Config{}, Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "endpoint is required")

	c := validConfig()
	c.TLS.CAFile = "/nonexistent/ca.pem"
	c.Insecure = false
	_, err = New(c, Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read ca_file")

	c = validConfig()
	c.Insecure = false
	c.TLS.CAFile = writePEM(t, t.TempDir(), "junk.pem", []byte("not a certificate"))
	_, err = New(c, Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no certificates found")

	c = validConfig()
	c.Insecure = false
	c.TLS.CertFile = writePEM(t, t.TempDir(), "c.pem", []byte("x"))
	c.TLS.KeyFile = c.TLS.CertFile
	_, err = New(c, Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load cert_file/key_file")
}

func TestNew_DoesNotDialAndUnreachableIsRetryable(t *testing.T) {
	// reserve a port and close it so nothing listens there
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	for _, proto := range []string{ProtocolGRPC, ProtocolHTTP} {
		t.Run(proto, func(t *testing.T) {
			cfg := grpcConfig(addr)
			cfg.Protocol = proto
			cfg.Retry.Attempts = 1
			start := time.Now()
			h := newHarness(t, cfg)
			assert.Less(t, time.Since(start), time.Second, "constructor must not wait on the network")

			err := h.client.ExportSpans(t.Context(), sampleSpans())
			require.Error(t, err)
			assert.False(t, sink.IsPermanent(err), "an unreachable endpoint is retryable: %v", err)
			assert.Len(t, h.sleeps(), 1, "retried once before giving up")
			assert.Contains(t, err.Error(), "after 2 attempt(s)")

			err = h.client.ExportLogs(t.Context(), sampleRecords())
			require.Error(t, err)
			assert.False(t, sink.IsPermanent(err))
		})
	}
}

func TestExport_EmptyIsNoop(t *testing.T) {
	stub := &grpcStub{}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	require.NoError(t, h.client.ExportSpans(t.Context(), nil))
	require.NoError(t, h.client.ExportLogs(t.Context(), []LogRecord{}))
	assert.Zero(t, stub.traceCalls())
	assert.Zero(t, stub.logCalls())
}

// --- gRPC -------------------------------------------------------------------

func TestGRPC_ExportSpansAndLogs(t *testing.T) {
	stub := &grpcStub{}
	cfg := grpcConfig(startGRPC(t, stub))
	cfg.Headers = map[string]config.Secret{"Authorization": "Bearer tok", "X-Scope-OrgID": "tenant"}
	h := newHarness(t, cfg)

	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	require.Equal(t, 1, stub.traceCalls())
	want := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{ResourceSpans(DefaultResource("1.0.0"), sampleSpans())}}
	assert.True(t, proto.Equal(want, stub.traceReqs[0]), "got %v", stub.traceReqs[0])
	md := stub.lastMetadata()
	assert.Equal(t, []string{"Bearer tok"}, md.Get("authorization"))
	assert.Equal(t, []string{"tenant"}, md.Get("x-scope-orgid"))
	assert.Contains(t, md.Get("user-agent")[0], "antwatcher/1.0.0")
	assert.NotEqual(t, "gzip", stub.lastCompression(), "no compression unless configured")

	require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
	require.Equal(t, 1, stub.logCalls())
	wantLogs := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{ResourceLogs(DefaultResource("1.0.0"), sampleRecords())}}
	assert.True(t, proto.Equal(wantLogs, stub.logReqs[0]), "got %v", stub.logReqs[0])
	assert.Equal(t, []string{"Bearer tok"}, stub.lastMetadata().Get("authorization"))

	assert.Empty(t, h.sleeps())
	assert.Zero(t, h.rejected(SignalTraces))
	assert.Zero(t, h.rejected(SignalLogs))
}

func TestGRPC_Gzip(t *testing.T) {
	stub := &grpcStub{}
	cfg := grpcConfig(startGRPC(t, stub))
	cfg.Compression = CompressionGzip
	h := newHarness(t, cfg)
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	assert.Equal(t, "gzip", stub.lastCompression())
	assert.True(t, proto.Equal(ResourceSpans(DefaultResource("1.0.0"), sampleSpans()), stub.traceReqs[0].ResourceSpans[0]))
}

func TestGRPC_RetryableThenSuccess(t *testing.T) {
	stub := &grpcStub{}
	stub.traceFn = func(n int) (*coltracepb.ExportTraceServiceResponse, error) {
		if n < 2 {
			return nil, status.Error(codes.Unavailable, "warming up")
		}
		return &coltracepb.ExportTraceServiceResponse{}, nil
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	assert.Equal(t, 3, stub.traceCalls())
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, h.sleeps(), "exponential backoff")
	assert.Contains(t, h.logs.String(), "retrying")
}

func TestGRPC_RetryableExhausted(t *testing.T) {
	stub := &grpcStub{}
	stub.logFn = func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return nil, status.Error(codes.DeadlineExceeded, "too slow")
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	err := h.client.ExportLogs(t.Context(), sampleRecords())
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Equal(t, 3, stub.logCalls(), "first attempt plus two retries")
	assert.Contains(t, err.Error(), "otlp export logs to")
	assert.Contains(t, err.Error(), "after 3 attempt(s)")
	assert.Contains(t, err.Error(), "too slow")
}

func TestGRPC_PermanentNotRetried(t *testing.T) {
	stub := &grpcStub{}
	stub.traceFn = func(int) (*coltracepb.ExportTraceServiceResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad span")
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	err := h.client.ExportSpans(t.Context(), sampleSpans())
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
	assert.Equal(t, 1, stub.traceCalls())
	assert.Empty(t, h.sleeps())
	assert.Contains(t, err.Error(), "bad span")
}

func TestGRPC_ResourceExhausted(t *testing.T) {
	t.Run("without RetryInfo is permanent", func(t *testing.T) {
		stub := &grpcStub{}
		stub.traceFn = func(int) (*coltracepb.ExportTraceServiceResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "quota")
		}
		h := newHarness(t, grpcConfig(startGRPC(t, stub)))
		err := h.client.ExportSpans(t.Context(), sampleSpans())
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err))
		assert.Equal(t, 1, stub.traceCalls())
	})

	t.Run("with RetryInfo is retried after the server delay", func(t *testing.T) {
		stub := &grpcStub{}
		stub.traceFn = func(n int) (*coltracepb.ExportTraceServiceResponse, error) {
			if n == 0 {
				st, err := status.New(codes.ResourceExhausted, "quota").WithDetails(
					&errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)})
				if err != nil {
					return nil, err
				}
				return nil, st.Err()
			}
			return &coltracepb.ExportTraceServiceResponse{}, nil
		}
		h := newHarness(t, grpcConfig(startGRPC(t, stub)))
		require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
		assert.Equal(t, 2, stub.traceCalls())
		assert.Equal(t, []time.Duration{2 * time.Second}, h.sleeps(), "RetryInfo overrides the backoff")
	})
}

func TestGRPC_PartialSuccessCountedNotRetried(t *testing.T) {
	stub := &grpcStub{traceFn: partialTrace(2, "spans too old")}
	stub.logFn = func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return &collogspb.ExportLogsServiceResponse{
			PartialSuccess: &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "label limit"},
		}, nil
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))

	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()), "partial success is an export success")
	assert.Equal(t, 1, stub.traceCalls())
	assert.InDelta(t, 2, h.rejected(SignalTraces), 0)
	assert.Contains(t, h.logs.String(), "partially rejected")
	assert.Contains(t, h.logs.String(), "spans too old")

	require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
	assert.Equal(t, 1, stub.logCalls())
	assert.InDelta(t, 1, h.rejected(SignalLogs), 0)
	assert.Contains(t, h.logs.String(), "label limit")
	assert.Empty(t, h.sleeps())
}

func TestGRPC_PartialSuccessMessageOnlyIsLogged(t *testing.T) {
	stub := &grpcStub{traceFn: partialTrace(0, "deprecated attribute")}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	assert.Zero(t, h.rejected(SignalTraces))
	assert.Contains(t, h.logs.String(), "deprecated attribute")
}

func TestGRPC_NilMetricsAndLogger(t *testing.T) {
	stub := &grpcStub{traceFn: partialTrace(3, "x")}
	c, err := New(grpcConfig(startGRPC(t, stub)), Options{})
	require.NoError(t, err)
	defer c.Close() //nolint:errcheck // test cleanup
	require.NoError(t, c.ExportSpans(t.Context(), sampleSpans()))
	assert.Equal(t, "dev", stub.traceReqs[0].ResourceSpans[0].ScopeSpans[0].Scope.Version)
}

func TestGRPC_CustomResource(t *testing.T) {
	stub := &grpcStub{}
	c, err := New(grpcConfig(startGRPC(t, stub)), Options{Resource: Resource{Attributes: map[string]any{"service.name": "custom"}}})
	require.NoError(t, err)
	defer c.Close() //nolint:errcheck // test cleanup
	require.NoError(t, c.ExportLogs(t.Context(), sampleRecords()))
	attrs := stub.logReqs[0].ResourceLogs[0].Resource.Attributes
	require.Len(t, attrs, 1)
	assert.Equal(t, "custom", attrs[0].Value.GetStringValue())
}

func TestGRPC_TLS(t *testing.T) {
	pki := newPKI(t)

	t.Run("ca file and server name", func(t *testing.T) {
		stub := &grpcStub{}
		addr := startGRPC(t, stub, pki.grpcCreds(false))
		cfg := grpcConfig(addr)
		cfg.Insecure = false
		cfg.TLS = TLSConfig{CAFile: pki.caFile, ServerName: "collector.internal"}
		h := newHarness(t, cfg)
		require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
		assert.Equal(t, 1, stub.traceCalls())
	})

	t.Run("mutual tls", func(t *testing.T) {
		stub := &grpcStub{}
		addr := startGRPC(t, stub, pki.grpcCreds(true))
		cfg := grpcConfig(addr)
		cfg.Insecure = false
		cfg.TLS = TLSConfig{CAFile: pki.caFile, CertFile: pki.clientCert, KeyFile: pki.clientKey}
		h := newHarness(t, cfg)
		require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
		assert.Equal(t, 1, stub.logCalls())

		// without the client certificate the handshake fails: retryable, never permanent
		cfg.TLS = TLSConfig{CAFile: pki.caFile}
		cfg.Retry.Attempts = 0
		h2 := newHarness(t, cfg)
		err := h2.client.ExportLogs(t.Context(), sampleRecords())
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
	})

	t.Run("untrusted server is retryable", func(t *testing.T) {
		stub := &grpcStub{}
		addr := startGRPC(t, stub, pki.grpcCreds(false))
		cfg := grpcConfig(addr)
		cfg.Insecure = false
		cfg.Retry.Attempts = 0
		h := newHarness(t, cfg)
		err := h.client.ExportSpans(t.Context(), sampleSpans())
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
		assert.Zero(t, stub.traceCalls())
	})
}

func TestGRPC_ContextCanceled(t *testing.T) {
	stub := &grpcStub{}
	stub.traceFn = func(int) (*coltracepb.ExportTraceServiceResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	ctx, cancel := context.WithCancel(t.Context())
	h.client.sleep = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	err := h.client.ExportSpans(ctx, sampleSpans())
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, stub.traceCalls())

	err = h.client.ExportSpans(ctx, sampleSpans())
	require.ErrorIs(t, err, context.Canceled, "a done ctx is reported before any attempt")
	assert.Equal(t, 1, stub.traceCalls())
}

func TestExport_ServerDelayBeyondBudgetReturnsNow(t *testing.T) {
	stub := &grpcStub{}
	stub.traceFn = func(int) (*coltracepb.ExportTraceServiceResponse, error) {
		st, err := status.New(codes.Unavailable, "later").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Hour)})
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	}
	h := newHarness(t, grpcConfig(startGRPC(t, stub)))
	err := h.client.ExportSpans(t.Context(), sampleSpans())
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.Equal(t, 1, stub.traceCalls())
	assert.Empty(t, h.sleeps(), "a delay longer than the timeout is not waited for in-client")

	// same when the caller's deadline is closer than the backoff
	stub2 := &grpcStub{traceFn: func(int) (*coltracepb.ExportTraceServiceResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}}
	cfg := grpcConfig(startGRPC(t, stub2))
	cfg.Retry.Backoff = 5 * time.Second
	h2 := newHarness(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = h2.client.ExportSpans(ctx, sampleSpans())
	require.Error(t, err)
	assert.Equal(t, 1, stub2.traceCalls())
	assert.Empty(t, h2.sleeps())
}

// --- HTTP -------------------------------------------------------------------

func TestHTTP_ExportSpansAndLogs(t *testing.T) {
	stub := &httpStub{}
	srv := startHTTP(t, stub)
	cfg := httpConfig(srv.URL)
	cfg.Headers = map[string]config.Secret{"Authorization": "Bearer tok"}
	h := newHarness(t, cfg)

	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	require.Equal(t, 1, stub.count())
	call := stub.call(0)
	assert.Equal(t, "/v1/traces", call.path)
	assert.Equal(t, "application/x-protobuf", call.header.Get("Content-Type"))
	assert.Equal(t, "Bearer tok", call.header.Get("Authorization"))
	assert.Equal(t, "antwatcher/1.0.0", call.header.Get("User-Agent"))
	assert.Empty(t, call.header.Get("Content-Encoding"))
	var got coltracepb.ExportTraceServiceRequest
	require.NoError(t, proto.Unmarshal(call.body, &got))
	assert.True(t, proto.Equal(ResourceSpans(DefaultResource("1.0.0"), sampleSpans()), got.ResourceSpans[0]))

	require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
	require.Equal(t, 2, stub.count())
	call = stub.call(1)
	assert.Equal(t, "/v1/logs", call.path)
	var gotLogs collogspb.ExportLogsServiceRequest
	require.NoError(t, proto.Unmarshal(call.body, &gotLogs))
	assert.True(t, proto.Equal(ResourceLogs(DefaultResource("1.0.0"), sampleRecords()), gotLogs.ResourceLogs[0]))
	require.NoError(t, stub.readErr)
}

func TestHTTP_BasePathAndSchemelessEndpoint(t *testing.T) {
	stub := &httpStub{}
	srv := startHTTP(t, stub)
	h := newHarness(t, httpConfig(srv.URL+"/otlp/"))
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	assert.Equal(t, "/otlp/v1/traces", stub.call(0).path)

	cfg := httpConfig(strings.TrimPrefix(srv.URL, "http://"))
	cfg.Insecure = true
	h2 := newHarness(t, cfg)
	require.NoError(t, h2.client.ExportLogs(t.Context(), sampleRecords()))
	assert.Equal(t, "/v1/logs", stub.call(1).path)
}

func TestHTTP_Gzip(t *testing.T) {
	stub := &httpStub{}
	srv := startHTTP(t, stub)
	cfg := httpConfig(srv.URL)
	cfg.Compression = CompressionGzip
	h := newHarness(t, cfg)
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	call := stub.call(0)
	assert.True(t, call.gzip)
	assert.Equal(t, "gzip", call.header.Get("Content-Encoding"))
	require.NoError(t, stub.readErr)
	var got coltracepb.ExportTraceServiceRequest
	require.NoError(t, proto.Unmarshal(call.body, &got))
	assert.True(t, proto.Equal(ResourceSpans(DefaultResource("1.0.0"), sampleSpans()), got.ResourceSpans[0]))
}

func TestHTTP_RetryAfterHonored(t *testing.T) {
	stub := &httpStub{}
	stub.respond = func(n int, w http.ResponseWriter) {
		switch n {
		case 0:
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusServiceUnavailable)
		case 1:
			w.Header().Set("Retry-After", time.Now().Add(4*time.Second).UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}
	srv := startHTTP(t, stub)
	h := newHarness(t, httpConfig(srv.URL))
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	assert.Equal(t, 3, stub.count())
	sleeps := h.sleeps()
	require.Len(t, sleeps, 2)
	assert.Equal(t, 3*time.Second, sleeps[0])
	assert.InDelta(t, float64(4*time.Second), float64(sleeps[1]), float64(1500*time.Millisecond), "HTTP dates have second resolution")
}

func TestHTTP_RetryableWithoutRetryAfterUsesBackoff(t *testing.T) {
	stub := &httpStub{}
	stub.respond = func(n int, w http.ResponseWriter) {
		if n < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
	srv := startHTTP(t, stub)
	h := newHarness(t, httpConfig(srv.URL))
	err := h.client.ExportLogs(t.Context(), sampleRecords())
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	var httpErr *HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusBadGateway, httpErr.StatusCode)
	assert.Equal(t, 3, stub.count())
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, h.sleeps())
}

func TestHTTP_PermanentStatuses(t *testing.T) {
	statusBody, err := proto.Marshal(&rpcstatus.Status{Code: int32(codes.InvalidArgument), Message: "unknown field"})
	require.NoError(t, err)
	cases := []struct {
		name    string
		code    int
		ctype   string
		body    []byte
		message string
	}{
		{"400 with rpc status", http.StatusBadRequest, "application/x-protobuf", statusBody, "unknown field"},
		{"401 with text", http.StatusUnauthorized, "text/plain; charset=utf-8", []byte("bad token\n"), "bad token"},
		{"404 empty", http.StatusNotFound, "", nil, ""},
		{"500 per spec", http.StatusInternalServerError, "text/plain", []byte(strings.Repeat("x", 1000)), strings.Repeat("x", 512) + "…"},
		{"400 undecodable protobuf", http.StatusBadRequest, "application/x-protobuf", []byte{0xff, 0xff}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &httpStub{}
			stub.respond = func(_ int, w http.ResponseWriter) {
				if tc.ctype != "" {
					w.Header().Set("Content-Type", tc.ctype)
				}
				w.WriteHeader(tc.code)
				_, _ = w.Write(tc.body)
			}
			srv := startHTTP(t, stub)
			h := newHarness(t, httpConfig(srv.URL))
			err := h.client.ExportSpans(t.Context(), sampleSpans())
			require.Error(t, err)
			assert.True(t, sink.IsPermanent(err), "%v", err)
			assert.Equal(t, 1, stub.count())
			var httpErr *HTTPError
			require.ErrorAs(t, err, &httpErr)
			assert.Equal(t, tc.code, httpErr.StatusCode)
			assert.Equal(t, tc.message, httpErr.Message)
		})
	}
}

// TestHTTP_NonOKStatusIncludesBodyReadError hijacks the connection after a
// non-2xx status so the client's body read fails, and checks the resulting
// HTTPError surfaces that read failure instead of silently building its
// message from whatever partial bytes were read.
func TestHTTP_NonOKStatusIncludesBodyReadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("short"))
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("ResponseWriter does not support hijacking")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer srv.Close()

	h := newHarness(t, httpConfig(srv.URL))
	err := h.client.ExportSpans(t.Context(), sampleSpans())
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	var httpErr *HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
	assert.Contains(t, httpErr.Message, "response body:")
}

// TestHTTP_OKStatusWithBodyReadErrorIsNotRetried hijacks the connection
// after a 2xx status so the client's body read fails. The status line
// already said the destination accepted the batch, so this must not be
// treated as a retryable failure: retrying would duplicate data the
// destination already has.
func TestHTTP_OKStatusWithBodyReadErrorIsNotRetried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("short"))
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("ResponseWriter does not support hijacking")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer srv.Close()

	h := newHarness(t, httpConfig(srv.URL))
	err := h.client.ExportSpans(t.Context(), sampleSpans())
	assert.NoError(t, err)
}

func TestHTTP_PartialSuccess(t *testing.T) {
	body, err := proto.Marshal(&coltracepb.ExportTraceServiceResponse{
		PartialSuccess: &coltracepb.ExportTracePartialSuccess{RejectedSpans: 4, ErrorMessage: "too many attributes"},
	})
	require.NoError(t, err)
	logBody, err := proto.Marshal(&collogspb.ExportLogsServiceResponse{
		PartialSuccess: &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "stream limit"},
	})
	require.NoError(t, err)
	stub := &httpStub{}
	stub.respond = func(n int, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if n == 0 {
			_, _ = w.Write(body)
			return
		}
		_, _ = w.Write(logBody)
	}
	srv := startHTTP(t, stub)
	h := newHarness(t, httpConfig(srv.URL))
	require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
	assert.Equal(t, 2, stub.count())
	assert.InDelta(t, 4, h.rejected(SignalTraces), 0)
	assert.InDelta(t, 1, h.rejected(SignalLogs), 0)
	assert.Contains(t, h.logs.String(), "too many attributes")
	assert.Contains(t, h.logs.String(), "stream limit")
	assert.Empty(t, h.sleeps())
}

func TestHTTP_SuccessBodyVariants(t *testing.T) {
	t.Run("non-protobuf body ignored", func(t *testing.T) {
		stub := &httpStub{respond: func(_ int, w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"partialSuccess":{}}`))
		}}
		srv := startHTTP(t, stub)
		h := newHarness(t, httpConfig(srv.URL))
		require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
	})
	t.Run("undecodable protobuf body is retryable", func(t *testing.T) {
		stub := &httpStub{respond: func(_ int, w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/x-protobuf")
			_, _ = w.Write([]byte{0xff, 0xff, 0xff})
		}}
		srv := startHTTP(t, stub)
		cfg := httpConfig(srv.URL)
		cfg.Retry.Attempts = 0
		h := newHarness(t, cfg)
		err := h.client.ExportSpans(t.Context(), sampleSpans())
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
		assert.Contains(t, err.Error(), "decode response")
	})
}

func TestHTTP_TLS(t *testing.T) {
	pki := newPKI(t)

	t.Run("ca file", func(t *testing.T) {
		stub := &httpStub{}
		srv := pki.startHTTPS(t, stub, false)
		cfg := httpConfig(srv.URL)
		cfg.TLS = TLSConfig{CAFile: pki.caFile}
		h := newHarness(t, cfg)
		require.NoError(t, h.client.ExportSpans(t.Context(), sampleSpans()))
		assert.Equal(t, 1, stub.count())
	})

	t.Run("mutual tls", func(t *testing.T) {
		stub := &httpStub{}
		srv := pki.startHTTPS(t, stub, true)
		cfg := httpConfig(srv.URL)
		cfg.TLS = TLSConfig{CAFile: pki.caFile, CertFile: pki.clientCert, KeyFile: pki.clientKey}
		h := newHarness(t, cfg)
		require.NoError(t, h.client.ExportLogs(t.Context(), sampleRecords()))
		assert.Equal(t, 1, stub.count())
	})

	t.Run("untrusted server is retryable", func(t *testing.T) {
		stub := &httpStub{}
		srv := pki.startHTTPS(t, stub, false)
		cfg := httpConfig(srv.URL)
		cfg.Retry.Attempts = 0
		h := newHarness(t, cfg)
		err := h.client.ExportSpans(t.Context(), sampleSpans())
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err))
		assert.Zero(t, stub.count())
	})
}

func TestHTTP_TimeoutIsRetryable(t *testing.T) {
	release := make(chan struct{})
	stub := &httpStub{respond: func(_ int, w http.ResponseWriter) {
		<-release
		w.WriteHeader(http.StatusOK)
	}}
	srv := startHTTP(t, stub)
	t.Cleanup(func() { close(release) })
	cfg := httpConfig(srv.URL)
	cfg.Timeout = 50 * time.Millisecond
	cfg.Retry.Attempts = 1
	h := newHarness(t, cfg)
	err := h.client.ExportSpans(t.Context(), sampleSpans())
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline"), "%v", err)
	assert.Equal(t, 2, stub.count())
}

// roundTripperFunc adapts a function to http.RoundTripper, for a fake
// http.DefaultTransport that is not a *http.Transport.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestNewHTTPTransport_FallsBackWhenDefaultTransportIsReplaced simulates
// another package in the process replacing the global http.DefaultTransport
// with something other than *http.Transport (common with instrumentation or
// proxy libraries): the OTLP/HTTP transport must fall back to a fresh
// *http.Transport instead of panicking on the type assertion.
func TestNewHTTPTransport_FallsBackWhenDefaultTransportIsReplaced(t *testing.T) {
	orig := http.DefaultTransport
	http.DefaultTransport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("not used")
	})
	t.Cleanup(func() { http.DefaultTransport = orig })

	tgt := target{hostPort: "example.com:4318", tls: true}
	require.NotPanics(t, func() {
		tr := newHTTPTransport(DefaultConfig(), tgt, nil, "antwatcher-test")
		require.NotNil(t, tr)
		_, ok := tr.client.Transport.(*http.Transport)
		assert.True(t, ok, "falls back to a fresh *http.Transport")
	})
}

func TestSleepCtx(t *testing.T) {
	require.NoError(t, sleepCtx(t.Context(), 0))
	require.NoError(t, sleepCtx(t.Context(), time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, sleepCtx(ctx, time.Hour), context.Canceled)
}

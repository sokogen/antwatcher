package otlp_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	otlpclient "github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/log"
	otlpdriver "github.com/sokogen/antwatcher/internal/sink/log/drivers/otlp"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

// logsStub is an in-process OTLP logs service that records requests and
// answers with a programmable status.
type logsStub struct {
	collogspb.UnimplementedLogsServiceServer
	mu   sync.Mutex
	reqs []*collogspb.ExportLogsServiceRequest
	fn   func(call int) error
}

func (s *logsStub) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	call := len(s.reqs)
	s.reqs = append(s.reqs, req)
	if s.fn != nil {
		if err := s.fn(call); err != nil {
			return nil, err
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s *logsStub) requests() []*collogspb.ExportLogsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*collogspb.ExportLogsServiceRequest(nil), s.reqs...)
}

func startGRPC(t *testing.T, stub *logsStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// unroutable returns a loopback address nothing listens on.
func unroutable(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
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

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

func deps() sink.Deps {
	return sink.Deps{Logger: slog.New(slog.DiscardHandler), Version: "1.2.3"}
}

// singleRecord returns the only log record of the only request, failing
// otherwise.
func singleRecord(t *testing.T, reqs []*collogspb.ExportLogsServiceRequest) (*logspb.ResourceLogs, *logspb.LogRecord) {
	t.Helper()
	require.Len(t, reqs, 1)
	rl := reqs[0].GetResourceLogs()
	require.Len(t, rl, 1)
	require.Len(t, rl[0].GetScopeLogs(), 1)
	records := rl[0].GetScopeLogs()[0].GetLogRecords()
	require.Len(t, records, 1, "one record per export, no batching")
	return rl[0], records[0]
}

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassLog), otlpdriver.Name)
	d, ok := sink.Describers()[config.SinkKey("log", "otlp")]
	require.True(t, ok)
	typed, err := d.Describe(block(t, "endpoint: loki:4317\nheaders:\n  X-Scope-OrgID: tenant"))
	require.NoError(t, err)
	cfg, ok := typed.(otlpclient.Config)
	require.True(t, ok)
	assert.Equal(t, "loki:4317", cfg.Endpoint)
	assert.Equal(t, otlpclient.ProtocolGRPC, cfg.Protocol, "describer applies defaults")
}

func TestDriver_BuildsAndExportsToGRPCStub(t *testing.T) {
	stub := &logsStub{}
	addr := startGRPC(t, stub)

	reg := sink.NewRegistry()
	reg.RegisterDriver(sink.ClassLog, otlpdriver.Name, otlpdriver.Driver())
	instances, err := reg.Build(context.Background(), []config.SinkConfig{{
		Name:      "loki",
		Class:     "log",
		Driver:    "otlp",
		StartFrom: "now",
		Config:    block(t, "endpoint: "+addr+"\ninsecure: true\nretry:\n  attempts: 0"),
	}}, deps())
	require.NoError(t, err)
	require.Len(t, instances, 1)
	s := instances[0]
	t.Cleanup(func() { _ = s.Close() })
	assert.Equal(t, "loki", s.Name())
	assert.Equal(t, sink.ClassLog, s.Class())
	assert.Equal(t, "otlp", s.Driver)

	env := fixtureEnvelope(t, "workflow_job.completed")
	require.NoError(t, s.Process(context.Background(), env))

	exec, err := model.Normalize(env)
	require.NoError(t, err)
	want := log.Project(exec)
	rl, got := singleRecord(t, stub.requests())
	assert.Equal(t, want.TraceID[:], got.GetTraceId(), "trace id reaches the collector")
	assert.Equal(t, want.SpanID[:], got.GetSpanId(), "span id reaches the collector")
	assert.Equal(t, want.Body, got.GetBody().GetStringValue())
	assert.Equal(t, logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, got.GetSeverityNumber())
	assert.Equal(t, log.SeverityTextError, got.GetSeverityText())
	assert.Equal(t, uint64(want.Time.UnixNano()), got.GetTimeUnixNano())
	assert.Equal(t, uint64(received.UnixNano()), got.GetObservedTimeUnixNano())
	attrs := map[string]string{}
	for _, kv := range got.GetAttributes() {
		attrs[kv.GetKey()] = kv.GetValue().String()
	}
	assert.Contains(t, attrs[log.AttrRepository], "sokogen/antwatcher")
	assert.Contains(t, attrs[log.AttrJobID], "42000000001")

	var version string
	for _, kv := range rl.GetResource().GetAttributes() {
		if kv.GetKey() == otlpclient.AttrServiceVersion {
			version = kv.GetValue().GetStringValue()
		}
	}
	assert.Equal(t, "1.2.3", version, "Deps.Version reaches the OTLP resource")

	// a ping is exported too, without correlation ids
	require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "ping")))
	reqs := stub.requests()
	require.Len(t, reqs, 2, "one request per record")
	ping := reqs[1].GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0]
	assert.Equal(t, "ping", ping.GetBody().GetStringValue())
	assert.Empty(t, ping.GetTraceId())
	assert.Empty(t, ping.GetSpanId())
}

func TestDriver_UnreachableEndpointIsRetryableAtProcess(t *testing.T) {
	addr := unroutable(t)
	start := time.Now()
	s, err := otlpdriver.Factory(context.Background(), "loki", block(t, "endpoint: "+addr+"\ninsecure: true\ntimeout: 2s\nretry:\n  attempts: 0"), deps())
	require.NoError(t, err, "constructor never dials")
	assert.Less(t, time.Since(start), time.Second)
	t.Cleanup(func() { _ = s.Close() })

	err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), "unreachable collector is retryable: %v", err)
}

func TestDriver_ErrorClassification(t *testing.T) {
	t.Run("permanent status is permanent and not retried", func(t *testing.T) {
		stub := &logsStub{fn: func(int) error { return status.Error(codes.Unauthenticated, "bad token") }}
		addr := startGRPC(t, stub)
		s, err := otlpdriver.Factory(context.Background(), "loki", block(t, "endpoint: "+addr+"\ninsecure: true"), deps())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed"))
		require.Error(t, err)
		assert.True(t, sink.IsPermanent(err), "%v", err)
		assert.Contains(t, err.Error(), "bad token")
		assert.Len(t, stub.requests(), 1)
	})

	t.Run("retryable status is retried in-client then surfaced retryable", func(t *testing.T) {
		stub := &logsStub{fn: func(int) error { return status.Error(codes.Unavailable, "draining") }}
		addr := startGRPC(t, stub)
		s, err := otlpdriver.Factory(context.Background(), "loki", block(t, "endpoint: "+addr+"\ninsecure: true\nretry:\n  attempts: 1\n  backoff: 1ms"), deps())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.queued"))
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err), "%v", err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
		assert.Len(t, stub.requests(), 2, "one attempt plus one retry")
	})

	t.Run("retryable status then success acks", func(t *testing.T) {
		stub := &logsStub{fn: func(call int) error {
			if call == 0 {
				return status.Error(codes.Unavailable, "warming up")
			}
			return nil
		}}
		addr := startGRPC(t, stub)
		s, err := otlpdriver.Factory(context.Background(), "loki", block(t, "endpoint: "+addr+"\ninsecure: true\nretry:\n  attempts: 1\n  backoff: 1ms"), deps())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		require.NoError(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.queued")))
		assert.Len(t, stub.requests(), 2)
	})
}

func TestDriver_HTTPProtocol(t *testing.T) {
	// protocol http against a closed port: the constructor still succeeds and
	// the failure is a retryable transport error at Process
	addr := unroutable(t)
	s, err := otlpdriver.Factory(context.Background(), "loki", block(t, "endpoint: http://"+addr+"\nprotocol: http\ntimeout: 2s\nretry:\n  attempts: 0"), deps())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	err = s.Process(context.Background(), fixtureEnvelope(t, "ping"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
}

func TestDriver_ConfigErrors(t *testing.T) {
	cases := map[string]string{
		"empty block":    "",
		"unknown key":    "endpoint: loki:4317\nbogus: 1",
		"bad protocol":   "endpoint: loki:4317\nprotocol: udp",
		"missing ca":     "endpoint: loki:4317\ntls:\n  ca_file: /nonexistent/ca.pem",
		"bad timeout":    "endpoint: loki:4317\ntimeout: -1s",
		"invalid scalar": "endpoint: [1, 2]",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := otlpdriver.Factory(context.Background(), "loki", block(t, text), deps())
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestDriver_AbsentBlockFailsValidation(t *testing.T) {
	s, err := otlpdriver.Factory(context.Background(), "loki", yaml.Node{}, deps())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "endpoint is required")
	assert.Nil(t, s)
}

package otlp_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	otlpclient "github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/trace"
	otlpdriver "github.com/sokogen/antwatcher/internal/sink/trace/drivers/otlp"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

// traceStub is an in-process OTLP trace service that records requests and
// answers with a programmable status.
type traceStub struct {
	coltracepb.UnimplementedTraceServiceServer
	mu   sync.Mutex
	reqs []*coltracepb.ExportTraceServiceRequest
	err  error
}

func (s *traceStub) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (s *traceStub) requests() []*coltracepb.ExportTraceServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*coltracepb.ExportTraceServiceRequest(nil), s.reqs...)
}

func startGRPC(t *testing.T, stub *traceStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, stub)
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

func fixtureEnvelope(t *testing.T, fixture, eventName string) event.Envelope {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, eventName)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

func deps() sink.Deps {
	return sink.Deps{Logger: slog.New(slog.DiscardHandler), Version: "1.2.3"}
}

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassTrace), otlpdriver.Name)
	d, ok := sink.Describers()[config.SinkKey("trace", "otlp")]
	require.True(t, ok)
	typed, err := d.Describe(block(t, "endpoint: collector:4317\nheaders:\n  authorization: secret"))
	require.NoError(t, err)
	cfg, ok := typed.(otlpclient.Config)
	require.True(t, ok)
	assert.Equal(t, "collector:4317", cfg.Endpoint)
	assert.Equal(t, otlpclient.ProtocolGRPC, cfg.Protocol, "describer applies defaults")
}

func TestDriver_BuildsAndExportsToGRPCStub(t *testing.T) {
	stub := &traceStub{}
	addr := startGRPC(t, stub)

	reg := sink.NewRegistry()
	reg.RegisterDriver(sink.ClassTrace, otlpdriver.Name, otlpdriver.Driver())
	instances, err := reg.Build(context.Background(), []config.SinkConfig{{
		Name:      "tempo",
		Class:     "trace",
		Driver:    "otlp",
		StartFrom: "now",
		Config:    block(t, "endpoint: "+addr+"\ninsecure: true\nretry:\n  attempts: 0"),
	}}, deps())
	require.NoError(t, err)
	require.Len(t, instances, 1)
	s := instances[0]
	t.Cleanup(func() { _ = s.Close() })
	assert.Equal(t, "tempo", s.Name())
	assert.Equal(t, sink.ClassTrace, s.Class())
	assert.Equal(t, "otlp", s.Driver)

	env := fixtureEnvelope(t, "workflow_job.completed", "workflow_job")
	require.NoError(t, s.Process(context.Background(), env))

	reqs := stub.requests()
	require.Len(t, reqs, 1)
	exec, err := model.Normalize(env)
	require.NoError(t, err)
	want, err := trace.Project(exec)
	require.NoError(t, err)
	rs := reqs[0].GetResourceSpans()
	require.Len(t, rs, 1)
	require.Len(t, rs[0].GetScopeSpans(), 1)
	got := rs[0].GetScopeSpans()[0].GetSpans()
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, want[i].Name, got[i].GetName())
		assert.Equal(t, want[i].TraceID[:], got[i].GetTraceId())
		assert.Equal(t, want[i].SpanID[:], got[i].GetSpanId())
		assert.Equal(t, want[i].ParentSpanID[:], got[i].GetParentSpanId())
	}
	var version string
	for _, kv := range rs[0].GetResource().GetAttributes() {
		if kv.GetKey() == otlpclient.AttrServiceVersion {
			version = kv.GetValue().GetStringValue()
		}
	}
	assert.Equal(t, "1.2.3", version, "Deps.Version reaches the OTLP resource")

	// a skipped event never produces a request
	require.ErrorIs(t, s.Process(context.Background(), fixtureEnvelope(t, "workflow_job.queued", "workflow_job")), sink.ErrSkipped)
	assert.Len(t, stub.requests(), 1)
}

func TestDriver_UnreachableEndpointIsRetryableAtProcess(t *testing.T) {
	addr := unroutable(t)
	s, err := otlpdriver.Factory(context.Background(), "tempo", block(t, "endpoint: "+addr+"\ninsecure: true\ntimeout: 2s\nretry:\n  attempts: 0"), deps())
	require.NoError(t, err, "constructor never dials")
	t.Cleanup(func() { _ = s.Close() })

	err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed", "workflow_run"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err), "unreachable collector is retryable: %v", err)
}

func TestDriver_PermanentStatusIsPermanent(t *testing.T) {
	stub := &traceStub{err: status.Error(codes.Unauthenticated, "bad token")}
	addr := startGRPC(t, stub)
	s, err := otlpdriver.Factory(context.Background(), "tempo", block(t, "endpoint: "+addr+"\ninsecure: true\nretry:\n  attempts: 0"), deps())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed", "workflow_run"))
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err), "%v", err)
	assert.Len(t, stub.requests(), 1, "no retry of a permanent status")
}

func TestDriver_HTTPProtocol(t *testing.T) {
	// protocol http against a closed port: the constructor still succeeds and
	// the failure is a retryable transport error at Process
	addr := unroutable(t)
	s, err := otlpdriver.Factory(context.Background(), "tempo", block(t, "endpoint: http://"+addr+"\nprotocol: http\ntimeout: 2s\nretry:\n  attempts: 0"), deps())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	err = s.Process(context.Background(), fixtureEnvelope(t, "workflow_run.completed", "workflow_run"))
	require.Error(t, err)
	assert.False(t, sink.IsPermanent(err))
}

func TestDriver_ConfigErrors(t *testing.T) {
	cases := map[string]string{
		"empty block":    "",
		"unknown key":    "endpoint: collector:4317\nbogus: 1",
		"bad protocol":   "endpoint: collector:4317\nprotocol: udp",
		"missing ca":     "endpoint: collector:4317\ntls:\n  ca_file: /nonexistent/ca.pem",
		"bad timeout":    "endpoint: collector:4317\ntimeout: -1s",
		"invalid scalar": "endpoint: [1, 2]",
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := otlpdriver.Factory(context.Background(), "tempo", block(t, text), deps())
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestDriver_AbsentBlockFailsValidation(t *testing.T) {
	s, err := otlpdriver.Factory(context.Background(), "tempo", yaml.Node{}, deps())
	require.Error(t, err, "endpoint is required")
	assert.Nil(t, s)
}

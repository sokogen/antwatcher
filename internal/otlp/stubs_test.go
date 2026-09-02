package otlp

import (
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// grpcStub serves both OTLP gRPC services in-process, records every request
// with its metadata and compression, and answers from programmable functions.
type grpcStub struct {
	coltracepb.UnimplementedTraceServiceServer

	mu           sync.Mutex
	traceReqs    []*coltracepb.ExportTraceServiceRequest
	logReqs      []*collogspb.ExportLogsServiceRequest
	metadata     []metadata.MD
	compressions []string

	// traceFn and logFn receive the 0-based call index; nil means success.
	traceFn func(n int) (*coltracepb.ExportTraceServiceResponse, error)
	logFn   func(n int) (*collogspb.ExportLogsServiceResponse, error)
}

func (s *grpcStub) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	n := len(s.traceReqs)
	s.traceReqs = append(s.traceReqs, req)
	md, _ := metadata.FromIncomingContext(ctx)
	s.metadata = append(s.metadata, md)
	fn := s.traceFn
	s.mu.Unlock()
	if fn != nil {
		return fn(n)
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// exportLogs is wired through logsServer so both services can share one struct
// even though the generated interfaces both name the method Export.
func (s *grpcStub) exportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	n := len(s.logReqs)
	s.logReqs = append(s.logReqs, req)
	md, _ := metadata.FromIncomingContext(ctx)
	s.metadata = append(s.metadata, md)
	fn := s.logFn
	s.mu.Unlock()
	if fn != nil {
		return fn(n)
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type logsServer struct {
	collogspb.UnimplementedLogsServiceServer
	stub *grpcStub
}

func (l logsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return l.stub.exportLogs(ctx, req)
}

// TagRPC, HandleRPC, TagConn, HandleConn implement stats.Handler to observe
// the compression negotiated for each incoming RPC.
func (s *grpcStub) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (s *grpcStub) HandleRPC(_ context.Context, st stats.RPCStats) {
	if h, ok := st.(*stats.InHeader); ok {
		s.mu.Lock()
		s.compressions = append(s.compressions, h.Compression)
		s.mu.Unlock()
	}
}
func (s *grpcStub) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (s *grpcStub) HandleConn(context.Context, stats.ConnStats)                       {}

func (s *grpcStub) traceCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.traceReqs)
}

func (s *grpcStub) logCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.logReqs)
}

func (s *grpcStub) lastMetadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.metadata) == 0 {
		return nil
	}
	return s.metadata[len(s.metadata)-1]
}

func (s *grpcStub) lastCompression() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.compressions) == 0 {
		return ""
	}
	return s.compressions[len(s.compressions)-1]
}

// startGRPC serves stub on a loopback port and returns host:port.
func startGRPC(t *testing.T, stub *grpcStub, opts ...grpc.ServerOption) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	opts = append(opts, grpc.StatsHandler(stub))
	srv := grpc.NewServer(opts...)
	coltracepb.RegisterTraceServiceServer(srv, stub)
	collogspb.RegisterLogsServiceServer(srv, logsServer{stub: stub})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// httpCall is one recorded OTLP/HTTP request with its body already decoded.
type httpCall struct {
	path   string
	header http.Header
	body   []byte
	gzip   bool
}

// httpStub records OTLP/HTTP requests and answers from a programmable function.
type httpStub struct {
	mu    sync.Mutex
	calls []httpCall
	// respond receives the 0-based call index; nil answers 200 with no body.
	respond func(n int, w http.ResponseWriter)
	readErr error
}

func (s *httpStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	var err error
	isGzip := r.Header.Get("Content-Encoding") == "gzip"
	if isGzip {
		var zr *gzip.Reader
		zr, err = gzip.NewReader(r.Body)
		if err == nil {
			body, err = io.ReadAll(zr)
		}
	} else {
		body, err = io.ReadAll(r.Body)
	}
	s.mu.Lock()
	n := len(s.calls)
	s.calls = append(s.calls, httpCall{path: r.URL.Path, header: r.Header.Clone(), body: body, gzip: isGzip})
	if err != nil {
		s.readErr = err
	}
	fn := s.respond
	s.mu.Unlock()
	if fn != nil {
		fn(n, w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *httpStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *httpStub) call(i int) httpCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i]
}

func startHTTP(t *testing.T, stub *httpStub) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	return srv
}

// testPKI is a throwaway CA with a loopback server certificate and a client
// certificate, written as PEM files for the TLS config.
type testPKI struct {
	caFile                 string
	clientCert, clientKey  string
	serverTLS              *tls.Config
	serverTLSRequireClient *tls.Config
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	caKey, caCert, caPEM := issue(t, nil, nil, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "antwatcher test CA"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	})
	serverKey, _, serverPEM := issue(t, caKey, caCert, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "collector"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost", "collector.internal"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	})
	clientKey, _, clientPEM := issue(t, caKey, caCert, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "antwatcher"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	})

	p := &testPKI{}
	p.caFile = writePEM(t, dir, "ca.pem", caPEM)
	p.clientCert = writePEM(t, dir, "client.pem", clientPEM)
	p.clientKey = writePEM(t, dir, "client-key.pem", keyPEM(t, clientKey))

	serverCert, err := tls.X509KeyPair(serverPEM, keyPEM(t, serverKey))
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))
	p.serverTLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}
	p.serverTLSRequireClient = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	return p
}

func issue(t *testing.T, parentKey *ecdsa.PrivateKey, parent *x509.Certificate, tmpl *x509.Certificate) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(24 * time.Hour)
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return key, cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func keyPEM(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func (p *testPKI) grpcCreds(requireClient bool) grpc.ServerOption {
	cfg := p.serverTLS
	if requireClient {
		cfg = p.serverTLSRequireClient
	}
	return grpc.Creds(credentials.NewTLS(cfg))
}

func (p *testPKI) startHTTPS(t *testing.T, stub *httpStub, requireClient bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(stub)
	srv.TLS = p.serverTLS
	if requireClient {
		srv.TLS = p.serverTLSRequireClient
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

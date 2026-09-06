package natsjs

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// readyTimeout bounds the embedded server start.
const readyTimeout = 10 * time.Second

// openStores holds the store directory of every embedded server running in
// this process, so a second one cannot be started on a directory already in
// use. nats-server takes no lock on StoreDir: two servers over one JetStream
// file store corrupt each other's stream and consumer state, losing messages
// a webhook 2xx already promised. It is easy to configure by accident — a
// forward sink whose nats-jetstream block is omitted inherits the ingress
// defaults, embedded server and store directory included.
var openStores = struct {
	sync.Mutex
	servers map[string]*server.Server
}{servers: map[string]*server.Server{}}

// reserveStore claims dir for a server about to start and returns its
// absolute form, the key to release it with.
func reserveStore(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("natsjs: store_dir %q: %w", dir, err)
	}
	openStores.Lock()
	defer openStores.Unlock()
	if _, taken := openStores.servers[abs]; taken {
		return "", fmt.Errorf("natsjs: an embedded server is already running on store_dir %q in this process; give each bus its own directory", abs)
	}
	openStores.servers[abs] = nil
	return abs, nil
}

// releaseStore drops the reservation on abs.
func releaseStore(abs string) {
	openStores.Lock()
	defer openStores.Unlock()
	delete(openStores.servers, abs)
}

// StartEmbedded starts an in-process nats-server with JetStream storing data
// in cfg.StoreDir. The server has no TCP listener; clients connect through
// nats.InProcessServer(srv). logger receives the server's own log lines; nil
// discards them. The store directory is held until the server is stopped, and
// starting a second server on it fails. The caller stops the server with
// StopEmbedded.
func StartEmbedded(cfg Config, logger *slog.Logger) (*server.Server, error) {
	if cfg.StoreDir == "" {
		return nil, errors.New("natsjs: store_dir is required for the embedded server")
	}
	if err := os.MkdirAll(cfg.StoreDir, 0o750); err != nil {
		return nil, fmt.Errorf("natsjs: create store_dir: %w", err)
	}
	abs, err := reserveStore(cfg.StoreDir)
	if err != nil {
		return nil, err
	}
	srv, err := startServer(cfg, logger)
	if err != nil {
		releaseStore(abs)
		return nil, err
	}
	openStores.Lock()
	openStores.servers[abs] = srv
	openStores.Unlock()
	return srv, nil
}

// StopEmbedded shuts a server returned by StartEmbedded down, waits for it,
// and releases its store directory for a later server. It is a no-op on nil
// and on a server already stopped.
func StopEmbedded(srv *server.Server) {
	if srv == nil {
		return
	}
	srv.Shutdown()
	srv.WaitForShutdown()
	openStores.Lock()
	defer openStores.Unlock()
	for dir, s := range openStores.servers {
		if s == srv {
			delete(openStores.servers, dir)
			return
		}
	}
}

func startServer(cfg Config, logger *slog.Logger) (*server.Server, error) {
	opts := &server.Options{
		ServerName: "antwatcher",
		DontListen: true,
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("natsjs: embedded server: %w", err)
	}
	if logger != nil {
		srv.SetLoggerV2(serverLogger{logger.With("component", "nats-server")}, false, false, false)
	}
	srv.Start()
	if !srv.ReadyForConnections(readyTimeout) {
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, fmt.Errorf("natsjs: embedded server not ready within %s", readyTimeout)
	}
	return srv, nil
}

// serverLogger routes nats-server log lines to slog (implements server.Logger).
type serverLogger struct{ l *slog.Logger }

// Noticef implements server.Logger at info level.
func (s serverLogger) Noticef(format string, v ...any) { s.l.Info(fmt.Sprintf(format, v...)) }

// Warnf implements server.Logger at warn level.
func (s serverLogger) Warnf(format string, v ...any) { s.l.Warn(fmt.Sprintf(format, v...)) }

// Fatalf implements server.Logger at error level; the server itself decides
// whether to stop.
func (s serverLogger) Fatalf(format string, v ...any) { s.l.Error(fmt.Sprintf(format, v...)) }

// Errorf implements server.Logger at error level.
func (s serverLogger) Errorf(format string, v ...any) { s.l.Error(fmt.Sprintf(format, v...)) }

// Debugf implements server.Logger at debug level.
func (s serverLogger) Debugf(format string, v ...any) { s.l.Debug(fmt.Sprintf(format, v...)) }

// Tracef implements server.Logger at debug level.
func (s serverLogger) Tracef(format string, v ...any) { s.l.Debug(fmt.Sprintf(format, v...)) }

// serverT is the subset of testing.TB NewTestServer needs. Taking an interface
// keeps the "testing" package (and its flags) out of production binaries —
// cmd/antwatcher blank-imports this package for the driver registration —
// while still letting any package's tests call natsjs.NewTestServer(t).
type serverT interface {
	Helper()
	Fatalf(format string, args ...any)
	TempDir() string
	Cleanup(func())
}

// TestServer is an embedded JetStream server for tests, stored in a temporary
// directory, that can be stopped and started again on the same data to
// simulate a broker outage. It implements nats.InProcessConnProvider by
// forwarding to the current server, so a Bus opened with WithInProcess(ts)
// reconnects to the restarted server on its own.
type TestServer struct {
	tb  serverT
	dir string

	mu  sync.Mutex
	srv *server.Server
}

// ErrServerStopped is returned by TestServer.InProcessConn while the server is stopped.
var ErrServerStopped = errors.New("natsjs: test server is stopped")

// NewTestServer starts a TestServer in t.TempDir() and stops it at cleanup.
func NewTestServer(tb serverT) *TestServer {
	tb.Helper()
	ts := &TestServer{tb: tb, dir: tb.TempDir()}
	ts.Start()
	tb.Cleanup(ts.Stop)
	return ts
}

// Start starts the server on the stored data. It fails the test on error and
// is a no-op while the server is running.
func (ts *TestServer) Start() {
	ts.tb.Helper()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.srv != nil {
		return
	}
	srv, err := StartEmbedded(Config{StoreDir: ts.dir}, nil)
	if err != nil {
		ts.tb.Fatalf("start test server: %v", err)
	}
	ts.srv = srv
}

// Stop shuts the server down and waits for it. Data stays on disk. No-op
// while stopped.
func (ts *TestServer) Stop() {
	ts.mu.Lock()
	srv := ts.srv
	ts.srv = nil
	ts.mu.Unlock()
	StopEmbedded(srv)
}

// Running reports whether the server is up.
func (ts *TestServer) Running() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.srv != nil
}

// StoreDir is the data directory.
func (ts *TestServer) StoreDir() string { return ts.dir }

// InProcessConn implements nats.InProcessConnProvider against the current
// server; it fails with ErrServerStopped while the server is stopped so a
// client keeps retrying until Start.
func (ts *TestServer) InProcessConn() (net.Conn, error) {
	ts.mu.Lock()
	srv := ts.srv
	ts.mu.Unlock()
	if srv == nil {
		return nil, ErrServerStopped
	}
	return srv.InProcessConn()
}

// Config returns a driver configuration suited to tests on this server:
// short nak delays so redelivery checks finish quickly, and a dedup window
// short enough that a test never waits for it.
func (ts *TestServer) Config() Config {
	c := DefaultConfig()
	c.StoreDir = ts.dir
	c.Retention = time.Hour
	c.DedupWindow = time.Minute
	c.AckWait = 10 * time.Second
	c.NakDelayMin = 50 * time.Millisecond
	c.NakDelayMax = 500 * time.Millisecond
	return c
}

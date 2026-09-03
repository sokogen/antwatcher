// Package admin serves the operator endpoints of antwatcher on their own
// listener: /metrics, /healthz, /readyz, and /status. Nothing here is ever
// mounted on the webhook listener; config validation rejects a shared address
// and the receiver mounts only its webhook path and /healthz.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Readiness reports whether the service can accept webhooks. A nil error
// means ready; the error message is returned in the /readyz body.
type Readiness func() error

// StatusProvider returns the value rendered as JSON by /status. It must be
// safe to call concurrently and must never include secret values.
type StatusProvider func() any

// Options configures the admin server.
type Options struct {
	// Listen is the address to bind, e.g. "127.0.0.1:9090"; ":0" picks a free port.
	Listen string
	// Metrics serves /metrics (see metrics.Metrics.Handler). Required.
	Metrics http.Handler
	// Readiness backs /readyz. nil means always ready.
	Readiness Readiness
	// Status backs /status. nil renders an empty object.
	Status StatusProvider
	// ShutdownTimeout bounds the graceful shutdown in Serve; default 5s.
	ShutdownTimeout time.Duration
	// Logger may be nil.
	Logger *slog.Logger
}

// Handler builds the admin mux: GET /metrics, GET /healthz, GET /readyz, GET /status.
func Handler(opts Options) http.Handler {
	mux := http.NewServeMux()
	if opts.Metrics != nil {
		mux.Handle("GET /metrics", opts.Metrics)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if opts.Readiness != nil {
			if err := opts.Readiness(); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "reason": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ready": true})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		var status any = map[string]any{}
		if opts.Status != nil {
			status = opts.Status()
		}
		body, err := json.Marshal(status)
		if err != nil {
			http.Error(w, fmt.Sprintf("render status: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Server is the admin HTTP server. Use New.
type Server struct {
	opts   Options
	logger *slog.Logger
	http   *http.Server

	mu       sync.Mutex
	listener net.Listener
}

// New creates a server for opts without binding the address.
func New(opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 5 * time.Second
	}
	return &Server{
		opts:   opts,
		logger: logger,
		http: &http.Server{
			Handler:           Handler(opts),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second, // /metrics on a large registry
			IdleTimeout:       60 * time.Second,
		},
	}
}

// Listen binds the address. It is called by Run; call it directly when the
// bound address is needed before serving (tests, ":0").
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return nil
	}
	ln, err := net.Listen("tcp", s.opts.Listen)
	if err != nil {
		return fmt.Errorf("admin: listen %s: %w", s.opts.Listen, err)
	}
	s.listener = ln
	return nil
}

// Addr returns the bound address, or nil before Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close releases the bound listener at once, without the graceful shutdown
// Run performs when its context ends: Serve then fails and Run returns its
// error. It exists to release the listener when startup fails before Run is
// called; the normal stop path is cancelling the context given to Run.
// No-op before Listen.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Close()
}

// Run binds the address if needed and serves until ctx ends, then shuts down
// gracefully within ShutdownTimeout. It returns nil after a clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	s.logger.Info("admin server listening", "addr", ln.Addr().String())

	errc := make(chan error, 1)
	go func() { errc <- s.http.Serve(ln) }()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("admin: serve: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.opts.ShutdownTimeout)
	defer cancel()
	if err := s.http.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("admin: shutdown: %w", err)
	}
	<-errc
	s.logger.Info("admin server stopped")
	return nil
}

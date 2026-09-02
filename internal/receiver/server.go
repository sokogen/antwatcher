package receiver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sokogen/antwatcher/internal/config"
)

// Server is the webhook HTTP server. Use NewServer, or Run for the common case.
type Server struct {
	cfg    config.Server
	logger *slog.Logger
	http   *http.Server

	mu       sync.Mutex
	listener net.Listener
}

// NewServer creates the webhook server for cfg.Listen without binding the
// address. Timeouts are derived from the configuration: the write timeout
// covers a full body read plus one publish_timeout, and the graceful shutdown
// waits for in-flight publishes, which are bounded by the same timeout.
// logger may be nil.
func NewServer(cfg config.Server, handler http.Handler, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	const readTimeout = 30 * time.Second
	return &Server{
		cfg:    cfg,
		logger: logger,
		http: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       readTimeout,
			WriteTimeout:      readTimeout + cfg.PublishTimeout + 5*time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    64 << 10,
		},
	}
}

// Listen binds the address. Run calls it; call it directly when the bound
// address is needed before serving (tests, ":0").
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return nil
	}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("receiver: listen %s: %w", s.cfg.Listen, err)
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

// Run binds the address if needed and serves until ctx ends, then stops
// accepting connections and waits for in-flight requests up to
// publish_timeout plus a margin. It returns nil after a clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	s.logger.Info("webhook server listening", "addr", ln.Addr().String(), "path", s.cfg.WebhookPath)

	errc := make(chan error, 1)
	go func() { errc <- s.http.Serve(ln) }()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("receiver: serve: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.PublishTimeout+2*time.Second)
	defer cancel()
	if err := s.http.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("receiver: shutdown: %w", err)
	}
	<-errc
	s.logger.Info("webhook server stopped")
	return nil
}

// Run serves handler on cfg.Listen until ctx ends; see Server.Run.
func Run(ctx context.Context, cfg config.Server, handler http.Handler, logger *slog.Logger) error {
	return NewServer(cfg, handler, logger).Run(ctx)
}
